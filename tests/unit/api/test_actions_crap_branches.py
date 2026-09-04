from __future__ import annotations

import json
import sqlite3
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException

from almanak.core.chains import LEGACY_SERIALIZED_CHAIN
from almanak.framework.api import actions
from almanak.framework.local_paths import LocalPathError
from almanak.framework.models.actions import AvailableAction
from almanak.framework.models.operator_card import EventType, PositionSummary, Severity
from almanak.framework.models.stuck_reason import StuckReason


def _create_state_db(path: Path, rows: list[tuple[str, object]]) -> None:
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE strategy_state (deployment_id TEXT PRIMARY KEY, state_data TEXT, updated_at TEXT)")
        conn.executemany(
            "INSERT INTO strategy_state (deployment_id, state_data, updated_at) VALUES (?, ?, ?)",
            [
                (
                    deployment_id,
                    json.dumps(state_data) if state_data is not None else None,
                    "2026-09-03T00:00:00Z",
                )
                for deployment_id, state_data in rows
            ],
        )


def _use_db(monkeypatch: pytest.MonkeyPatch, path: Path) -> None:
    monkeypatch.setattr("almanak.framework.local_paths.local_db_path", lambda: path)


def _strategy_state(**overrides: object) -> actions.StrategyState:
    values = {
        "deployment_id": "deployment:test",
        "status": "running",
        "chain": "base",
        "protocol": "uniswap_v3",
        "current_gas_price_gwei": 1.0,
    }
    values.update(overrides)
    return actions.StrategyState(**values)  # type: ignore[arg-type]


def _patch_config_update_dependencies(
    monkeypatch: pytest.MonkeyPatch,
    state: actions.StrategyState,
) -> tuple[MagicMock, MagicMock]:
    audit = MagicMock()
    add_event = MagicMock()
    monkeypatch.setattr(actions, "get_strategy_state", lambda deployment_id: state)
    monkeypatch.setattr(actions, "emit_audit_event", audit)
    monkeypatch.setattr(actions, "add_event", add_event)
    return audit, add_event


def test_load_state_refuses_local_path_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail_path_resolution() -> Path:
        raise LocalPathError("hosted mode has no local database")

    monkeypatch.setattr("almanak.framework.local_paths.local_db_path", fail_path_resolution)

    assert actions._load_strategy_state_from_db("deployment:hosted") is None


def test_load_state_returns_none_when_database_is_absent(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _use_db(monkeypatch, tmp_path / "missing.db")

    assert actions._load_strategy_state_from_db("deployment:missing") is None


@pytest.mark.parametrize(
    ("setup", "warning"),
    [
        pytest.param(lambda path: path.touch(), "no such table", id="missing-table"),
        pytest.param(lambda path: path.write_text("not sqlite"), "not a database", id="invalid-database"),
    ],
)
def test_load_state_logs_database_failures(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    setup,
    warning: str,
) -> None:
    path = tmp_path / "state.db"
    setup(path)
    _use_db(monkeypatch, path)

    with caplog.at_level("WARNING", logger=actions.__name__):
        result = actions._load_strategy_state_from_db("deployment:test")

    assert result is None
    assert warning in caplog.text


def test_load_state_filters_by_exact_deployment_identity(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    path = tmp_path / "state.db"
    _create_state_db(
        path,
        [
            ("deployment:first", {"chain": "base"}),
            ("deployment:second", {"chain": "arbitrum"}),
        ],
    )
    _use_db(monkeypatch, path)

    state = actions._load_strategy_state_from_db("deployment:second")

    assert state is not None
    assert state.deployment_id == "deployment:second"
    assert state.chain == "arbitrum"
    assert actions._load_strategy_state_from_db("deployment:second' OR 1=1 --") is None


@pytest.mark.parametrize(
    ("state_data", "expected_status"),
    [
        pytest.param(None, "running", id="empty-state"),
        pytest.param({"last_iteration": {"status": "SUCCESS"}}, "running", id="successful-iteration"),
        pytest.param(
            {"last_iteration": {"status": "EXECUTION_FAILED"}},
            "error",
            id="execution-failed",
        ),
        pytest.param(
            {"last_iteration": {"status": "STRATEGY_ERROR"}},
            "error",
            id="strategy-error",
        ),
    ],
)
def test_load_state_derives_iteration_status(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    state_data: object,
    expected_status: str,
) -> None:
    path = tmp_path / "state.db"
    _create_state_db(path, [("deployment:test", state_data)])
    _use_db(monkeypatch, path)

    state = actions._load_strategy_state_from_db("deployment:test")

    assert state is not None
    assert state.status == expected_status
    assert state.chain == LEGACY_SERIALIZED_CHAIN
    assert state.protocol == "unknown"
    assert state.total_value_usd is None


@pytest.mark.parametrize(
    ("reason", "expected_reason", "expected_status", "attention_required"),
    [
        pytest.param(
            StuckReason.GAS_PRICE_BLOCKED.value,
            StuckReason.GAS_PRICE_BLOCKED,
            "stuck",
            True,
            id="known-reason",
        ),
        pytest.param("NOT_A_REASON", None, "running", False, id="unknown-reason"),
        pytest.param("", None, "running", False, id="empty-reason"),
    ],
)
def test_load_state_classifies_stuck_reason(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    reason: str,
    expected_reason: StuckReason | None,
    expected_status: str,
    attention_required: bool,
) -> None:
    path = tmp_path / "state.db"
    _create_state_db(path, [("deployment:test", {"stuck_reason": reason})])
    _use_db(monkeypatch, path)

    state = actions._load_strategy_state_from_db("deployment:test")

    assert state is not None
    assert state.stuck_reason is expected_reason
    assert state.status == expected_status
    assert state.attention_required is attention_required


@pytest.mark.parametrize(
    ("state_data", "expected"),
    [
        pytest.param({"total_value_usd": "0"}, Decimal("0"), id="measured-zero"),
        pytest.param({"total_value_usd": "bad", "total_position_value_usd": "12.50"}, Decimal("12.50"), id="fallback"),
        pytest.param({"total_value_usd": None, "portfolio_value_usd": 7}, Decimal("7"), id="none-fallback"),
        pytest.param({}, None, id="unmeasured"),
    ],
)
def test_load_state_preserves_portfolio_measurement(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    state_data: dict[str, object],
    expected: Decimal | None,
) -> None:
    path = tmp_path / "state.db"
    _create_state_db(path, [("deployment:test", state_data)])
    _use_db(monkeypatch, path)

    state = actions._load_strategy_state_from_db("deployment:test")

    assert state is not None
    assert state.total_value_usd == expected


@pytest.mark.parametrize("nested", [False, True], ids=["legacy-flat", "serialized-nested"])
def test_load_state_restores_hot_reloadable_config(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    nested: bool,
) -> None:
    config = actions.HotReloadableConfig(
        max_slippage=Decimal("0.02"),
        trade_size_usd=Decimal("250"),
        rebalance_threshold=Decimal("0.08"),
        min_health_factor=Decimal("1.8"),
        max_leverage=Decimal("4"),
        daily_loss_limit_usd=Decimal("750"),
    )
    config_data = (
        config.to_dict()
        if nested
        else {
            "max_slippage": "0.02",
            "trade_size_usd": "250",
            "rebalance_threshold": "0.08",
            "min_health_factor": "1.8",
            "max_leverage": "4",
            "daily_loss_limit_usd": "750",
        }
    )
    path = tmp_path / "state.db"
    _create_state_db(path, [("deployment:test", {"config": config_data})])
    _use_db(monkeypatch, path)

    state = actions._load_strategy_state_from_db("deployment:test")

    assert state is not None
    assert state.config.max_slippage == Decimal("0.02")
    assert state.config.trade_size_usd == Decimal("250")
    assert state.config.rebalance_threshold == Decimal("0.08")
    assert state.config.min_health_factor == Decimal("1.8")
    assert state.config.max_leverage == Decimal("4")
    assert state.config.daily_loss_limit_usd == Decimal("750")


@pytest.mark.parametrize(
    "config_data",
    [
        {"max_slippage": "not-a-decimal"},
        {"max_slippage": None},
    ],
)
def test_load_state_invalid_config_uses_defaults(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    config_data: dict[str, object],
) -> None:
    path = tmp_path / "state.db"
    _create_state_db(path, [("deployment:test", {"config": config_data})])
    _use_db(monkeypatch, path)

    state = actions._load_strategy_state_from_db("deployment:test")

    assert state is not None
    assert state.config == actions.HotReloadableConfig()


def test_load_state_malformed_json_logs_and_returns_none(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    path = tmp_path / "state.db"
    _create_state_db(path, [("deployment:test", None)])
    with sqlite3.connect(path) as conn:
        conn.execute(
            "UPDATE strategy_state SET state_data = ? WHERE deployment_id = ?",
            ("{invalid", "deployment:test"),
        )
    _use_db(monkeypatch, path)

    with caplog.at_level("WARNING", logger=actions.__name__):
        result = actions._load_strategy_state_from_db("deployment:test")

    assert result is None
    assert "Failed to load strategy state from DB" in caplog.text


def test_load_state_threads_optional_transaction_fields(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    path = tmp_path / "state.db"
    _create_state_db(
        path,
        [
            (
                "deployment:test",
                {
                    "chain": "base",
                    "protocol": "uniswap_v3",
                    "pending_tx_hash": "0xabc",
                    "portfolio_value_usd": "10",
                },
            )
        ],
    )
    _use_db(monkeypatch, path)

    state = actions._load_strategy_state_from_db("deployment:test")

    assert state is not None
    assert state.chain == "base"
    assert state.protocol == "uniswap_v3"
    assert state.pending_tx_hash == "0xabc"
    assert state.current_gas_price_gwei == 0.1


@pytest.mark.asyncio
async def test_update_config_preserves_strategy_lookup_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    error = HTTPException(status_code=404, detail="Strategy deployment:missing not found")

    def missing_state(deployment_id: str) -> actions.StrategyState:
        raise error

    monkeypatch.setattr(actions, "get_strategy_state", missing_state)

    with pytest.raises(HTTPException) as raised:
        await actions.update_config(
            "deployment:missing",
            actions.ConfigUpdateRequest(updates={"trade_size_usd": "100"}),
            "operator",
        )

    assert raised.value is error


@pytest.mark.asyncio
async def test_update_config_rejects_empty_updates(monkeypatch: pytest.MonkeyPatch) -> None:
    state = _strategy_state()
    audit, add_event = _patch_config_update_dependencies(monkeypatch, state)

    with pytest.raises(HTTPException) as raised:
        await actions.update_config(
            state.deployment_id,
            actions.ConfigUpdateRequest(updates={}),
            "operator",
        )

    assert raised.value.status_code == 400
    assert raised.value.detail == "No configuration updates provided"
    audit.assert_not_called()
    add_event.assert_not_called()


@pytest.mark.asyncio
async def test_update_config_rejects_cold_fields_in_sorted_order(monkeypatch: pytest.MonkeyPatch) -> None:
    state = _strategy_state()
    audit, add_event = _patch_config_update_dependencies(monkeypatch, state)

    with pytest.raises(HTTPException) as raised:
        await actions.update_config(
            state.deployment_id,
            actions.ConfigUpdateRequest(updates={"wallet_address": "0xabc", "chain": "arbitrum"}),
            "operator",
        )

    assert raised.value.status_code == 400
    assert raised.value.detail == (
        "Cannot hot-reload fields: chain, wallet_address. Allowed fields: "
        "daily_loss_limit_usd, max_leverage, max_slippage, min_health_factor, "
        "rebalance_threshold, trade_size_usd"
    )
    audit.assert_not_called()
    add_event.assert_not_called()


@pytest.mark.asyncio
async def test_update_config_returns_structured_risk_guard_block(monkeypatch: pytest.MonkeyPatch) -> None:
    state = _strategy_state()
    audit, add_event = _patch_config_update_dependencies(monkeypatch, state)

    result = await actions.update_config(
        state.deployment_id,
        actions.ConfigUpdateRequest(updates={"max_leverage": "11"}),
        "operator",
    )

    assert set(result) == {"success", "message", "updated_config", "result", "error", "guidance"}
    assert result["success"] is False
    assert result["message"] == "Configuration update blocked by Risk Guard"
    assert result["updated_config"] is None
    assert result["result"] is None
    assert result["error"] == "max_leverage (11) exceeds risk limit (10)"
    assert result["guidance"] == [
        {
            "field_name": "max_leverage",
            "limit_name": "Maximum Leverage",
            "requested_value": "11",
            "limit_value": "10",
            "explanation": (
                "Leverage amplifies both gains and losses. Higher leverage increases liquidation risk and can lead "
                "to rapid loss of capital during adverse market movements."
            ),
            "suggestion": (
                "Reduce the requested leverage value. Consider the current market volatility and your risk "
                "tolerance. If higher leverage is required, contact your system administrator to adjust risk limits."
            ),
        }
    ]
    assert state.config.max_leverage == Decimal("3")
    audit.assert_not_called()
    add_event.assert_not_called()


@pytest.mark.asyncio
async def test_update_config_returns_guidance_for_non_numeric_value(monkeypatch: pytest.MonkeyPatch) -> None:
    state = _strategy_state()
    _patch_config_update_dependencies(monkeypatch, state)

    result = await actions.update_config(
        state.deployment_id,
        actions.ConfigUpdateRequest(updates={"max_slippage": "not-a-number"}),
        "operator",
    )

    assert result["success"] is False
    assert result["error"] == "max_slippage: invalid value format"
    assert result["guidance"][0]["requested_value"] == "0"  # type: ignore[index]
    assert state.config.max_slippage == Decimal("0.005")


@pytest.mark.asyncio
async def test_update_config_translates_config_validation_failure_to_400(monkeypatch: pytest.MonkeyPatch) -> None:
    state = _strategy_state()
    audit, add_event = _patch_config_update_dependencies(monkeypatch, state)

    with pytest.raises(HTTPException) as raised:
        await actions.update_config(
            state.deployment_id,
            actions.ConfigUpdateRequest(updates={"rebalance_threshold": "0"}),
            "operator",
        )

    assert raised.value.status_code == 400
    assert raised.value.detail == "Config validation failed: rebalance_threshold must be between 0.01 and 0.5, got 0"
    assert state.config.rebalance_threshold == Decimal("0.05")
    audit.assert_not_called()
    add_event.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("api_key", "expected_updated_by"),
    [
        pytest.param("short", "api:short", id="short-key"),
        pytest.param("operator-key", "api:operator...", id="long-key"),
    ],
)
async def test_update_config_success_preserves_audit_and_response_schema(
    monkeypatch: pytest.MonkeyPatch,
    api_key: str,
    expected_updated_by: str,
) -> None:
    state = _strategy_state()
    state.config.trade_size_usd = None  # type: ignore[assignment]
    audit, add_event = _patch_config_update_dependencies(monkeypatch, state)

    response = await actions.update_config(
        state.deployment_id,
        actions.ConfigUpdateRequest(
            updates={
                "trade_size_usd": "250",
                "daily_loss_limit_usd": "0",
            }
        ),
        api_key,
    )

    assert set(response) == {"success", "message", "updated_config", "result", "error", "guidance"}
    assert response["success"] is True
    assert response["message"] == (
        "Configuration updated for strategy deployment:test: trade_size_usd, daily_loss_limit_usd"
    )
    assert response["error"] is None
    assert response["guidance"] is None
    assert response["updated_config"]["trading_parameters"]["trade_size_usd"] == "250"  # type: ignore[index]
    assert response["updated_config"]["risk_parameters"]["daily_loss_limit_usd"] == "0"  # type: ignore[index]
    assert response["result"]["success"] is True  # type: ignore[index]
    assert response["result"]["updated_fields"] == ["trade_size_usd", "daily_loss_limit_usd"]  # type: ignore[index]
    assert response["result"]["previous_values"] == {  # type: ignore[index]
        "trade_size_usd": None,
        "daily_loss_limit_usd": "500",
    }
    audit.assert_called_once_with(
        deployment_id="deployment:test",
        action="CONFIG_UPDATE",
        details={
            "updated_fields": ["trade_size_usd", "daily_loss_limit_usd"],
            "previous_values": {"trade_size_usd": None, "daily_loss_limit_usd": "500"},
            "new_values": {"trade_size_usd": "250", "daily_loss_limit_usd": "0"},
        },
        api_key=api_key,
    )
    event = add_event.call_args.args[0]
    assert event.event_type is actions.TimelineEventType.CONFIG_UPDATED
    assert event.description == "Configuration updated: trade_size_usd, daily_loss_limit_usd"
    assert event.deployment_id == "deployment:test"
    assert event.chain == "base"
    assert event.details == {
        "changes": [
            {"field": "trade_size_usd", "old_value": None, "new_value": "250"},
            {"field": "daily_loss_limit_usd", "old_value": "500", "new_value": "0"},
        ],
        "updated_by": expected_updated_by,
    }


@pytest.mark.asyncio
async def test_update_config_preserves_audit_failure_ordering(monkeypatch: pytest.MonkeyPatch) -> None:
    state = _strategy_state()
    error = RuntimeError("audit unavailable")
    add_event = MagicMock()
    monkeypatch.setattr(actions, "get_strategy_state", lambda deployment_id: state)
    monkeypatch.setattr(actions, "emit_audit_event", MagicMock(side_effect=error))
    monkeypatch.setattr(actions, "add_event", add_event)

    with pytest.raises(RuntimeError) as raised:
        await actions.update_config(
            state.deployment_id,
            actions.ConfigUpdateRequest(updates={"trade_size_usd": "250"}),
            "operator",
        )

    assert raised.value is error
    assert state.config.trade_size_usd == Decimal("250")
    add_event.assert_not_called()


@pytest.mark.asyncio
async def test_update_config_preserves_timeline_failure_ordering(monkeypatch: pytest.MonkeyPatch) -> None:
    state = _strategy_state()
    error = RuntimeError("timeline unavailable")
    audit = MagicMock()
    monkeypatch.setattr(actions, "get_strategy_state", lambda deployment_id: state)
    monkeypatch.setattr(actions, "emit_audit_event", audit)
    monkeypatch.setattr(actions, "add_event", MagicMock(side_effect=error))

    with pytest.raises(RuntimeError) as raised:
        await actions.update_config(
            state.deployment_id,
            actions.ConfigUpdateRequest(updates={"trade_size_usd": "250"}),
            "operator",
        )

    assert raised.value is error
    assert state.config.trade_size_usd == Decimal("250")
    audit.assert_called_once()


@pytest.mark.parametrize(
    ("status", "severity", "available_actions", "suggested_action"),
    [
        pytest.param(
            "paused",
            Severity.MEDIUM,
            [AvailableAction.RESUME, AvailableAction.EMERGENCY_UNWIND],
            AvailableAction.RESUME,
            id="paused",
        ),
        pytest.param(
            "running",
            Severity.LOW,
            [AvailableAction.PAUSE, AvailableAction.EMERGENCY_UNWIND],
            AvailableAction.PAUSE,
            id="running",
        ),
        pytest.param(
            "error",
            Severity.LOW,
            [AvailableAction.EMERGENCY_UNWIND],
            AvailableAction.PAUSE,
            id="error-fallback",
        ),
    ],
)
def test_generate_operator_card_status_policy(
    status: str,
    severity: Severity,
    available_actions: list[AvailableAction],
    suggested_action: AvailableAction,
) -> None:
    state = _strategy_state(status=status, total_value_usd=Decimal("1250.50"))

    card = actions.generate_operator_card(
        state,
        EventType.WARNING,
        StuckReason.UNKNOWN,
        "Operator message",
    )

    assert card.severity is severity
    assert card.available_actions == available_actions
    assert [suggestion.action for suggestion in card.suggested_actions] == [suggested_action]
    assert card.suggested_actions[0].is_recommended is (status == "paused")
    assert card.position_summary.total_value_usd == Decimal("1250.50")
    assert card.position_summary.available_balance_usd == Decimal("125.050")
    assert card.risk_description == (f"Strategy deployment:test is currently {status}. Total value at risk: $1,250.50")


def test_generate_operator_card_stuck_gas_policy_and_schema() -> None:
    state = _strategy_state(
        status="stuck",
        stuck_reason=StuckReason.GAS_PRICE_BLOCKED,
        pending_tx_hash="0xabc",
        current_gas_price_gwei=2.0,
        total_value_usd=Decimal("0"),
    )

    card = actions.generate_operator_card(
        state,
        EventType.STUCK,
        StuckReason.GAS_PRICE_BLOCKED,
        "Transaction pending",
    )
    serialized = card.to_dict()

    assert card.severity is Severity.HIGH
    assert card.available_actions == [
        AvailableAction.BUMP_GAS,
        AvailableAction.CANCEL_TX,
        AvailableAction.PAUSE,
        AvailableAction.EMERGENCY_UNWIND,
    ]
    assert len(card.suggested_actions) == 1
    suggestion = card.suggested_actions[0]
    assert suggestion.action is AvailableAction.BUMP_GAS
    assert suggestion.description == "Increase gas price to unstick pending transaction"
    assert suggestion.priority == 1
    assert suggestion.params == {"suggested_gas_gwei": 3.0}
    assert suggestion.is_recommended is True
    assert serialized["deployment_id"] == "deployment:test"
    assert serialized["event_type"] == "STUCK"
    assert serialized["reason"] == "GAS_PRICE_BLOCKED"
    assert serialized["severity"] == "HIGH"
    assert serialized["context"] == {
        "status": "stuck",
        "chain": "base",
        "protocol": "uniswap_v3",
        "message": "Transaction pending",
        "pending_tx_hash": "0xabc",
        "current_gas_price_gwei": 2.0,
    }
    assert serialized["position_summary"]["total_value_usd"] == "0"  # type: ignore[index]
    assert serialized["position_summary"]["available_balance_usd"] == "0.0"  # type: ignore[index]
    assert serialized["risk_description"] == ("Strategy deployment:test is currently stuck. Total value at risk: $0.00")
    assert serialized["suggested_actions"] == [
        {
            "action": "BUMP_GAS",
            "description": "Increase gas price to unstick pending transaction",
            "priority": 1,
            "params": {"suggested_gas_gwei": 3.0},
            "is_recommended": True,
        }
    ]
    assert serialized["available_actions"] == ["BUMP_GAS", "CANCEL_TX", "PAUSE", "EMERGENCY_UNWIND"]
    assert serialized["auto_remediation"] is None
    assert serialized["timestamp"].endswith("+00:00")  # type: ignore[union-attr]


def test_generate_operator_card_stuck_non_gas_uses_default_suggestion() -> None:
    state = _strategy_state(
        status="stuck",
        stuck_reason=StuckReason.NONCE_CONFLICT,
        total_value_usd=Decimal("10"),
    )

    card = actions.generate_operator_card(
        state,
        EventType.ERROR,
        StuckReason.NONCE_CONFLICT,
        "Nonce conflict",
    )

    assert [suggestion.action for suggestion in card.suggested_actions] == [AvailableAction.PAUSE]
    assert card.suggested_actions[0].is_recommended is False


def test_generate_operator_card_preserves_unmeasured_value() -> None:
    state = _strategy_state(total_value_usd=None)

    card = actions.generate_operator_card(
        state,
        EventType.ALERT,
        StuckReason.UNKNOWN,
        "No valuation snapshot",
    )
    position = card.to_dict()["position_summary"]

    assert card.position_summary.total_value_usd is None
    assert card.position_summary.available_balance_usd is None
    assert position["total_value_usd"] is None  # type: ignore[index]
    assert position["available_balance_usd"] is None  # type: ignore[index]
    assert card.risk_description == ("Strategy deployment:test is currently running. Total value at risk: unavailable")


def test_position_summary_serialization_preserves_optional_measured_zero() -> None:
    card = actions.OperatorCard(
        deployment_id="deployment:test",
        timestamp=actions.datetime.now(actions.UTC),
        event_type=EventType.ALERT,
        reason=StuckReason.UNKNOWN,
        context={},
        severity=Severity.LOW,
        position_summary=PositionSummary(
            total_value_usd=Decimal("0"),
            available_balance_usd=Decimal("0"),
            health_factor=Decimal("0"),
            leverage=Decimal("0"),
        ),
        risk_description="Measured zero",
        suggested_actions=[
            actions.SuggestedAction(
                action=AvailableAction.PAUSE,
                description="Pause",
            )
        ],
        available_actions=[AvailableAction.PAUSE],
    )

    position = card.to_dict()["position_summary"]

    assert position["health_factor"] == "0"  # type: ignore[index]
    assert position["leverage"] == "0"  # type: ignore[index]
