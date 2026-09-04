"""Focused branch coverage for the ``strat pnl`` command orchestration."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

import pytest
from click.testing import CliRunner

from almanak.framework.accounting.models import (
    AccountingConfidence,
    AccountingIdentity,
    SwapAccountingEvent,
    SwapEventType,
)
from almanak.framework.accounting.reporting import AccountingData, StrategyClass
from almanak.framework.cli import strat_pnl as pnl_module
from almanak.framework.state.backends.sqlite import SQLiteConfig, SQLiteStore

_DEPLOYMENT_ID = "deployment:0123456789ab"


def _accounting_data(
    *,
    deployment_id: str = _DEPLOYMENT_ID,
    metrics: Any = None,
    ledger_entries: list[Any] | None = None,
    position_events: list[dict[str, Any]] | None = None,
    snapshot: Any = None,
    unavailable_records: list[dict[str, Any]] | None = None,
    parse_errors: int = 0,
    strategy_classes: frozenset[StrategyClass | str] = frozenset({StrategyClass.UNKNOWN}),
) -> AccountingData:
    return AccountingData(
        deployment_id=deployment_id,
        metrics=metrics,
        ledger_entries=ledger_entries or [],
        position_events=position_events or [],
        snapshot=snapshot,
        unavailable_records=unavailable_records or [],
        parse_errors=parse_errors,
        strategy_classes=strategy_classes,
    )


@pytest.mark.parametrize(
    ("option", "message"),
    [
        ("--ledger-limit", "--ledger-limit must be a positive integer.\n"),
        ("--position-limit", "--position-limit must be a positive integer.\n"),
    ],
)
def test_nonpositive_limits_keep_exact_error_and_exit_code(
    monkeypatch: pytest.MonkeyPatch,
    option: str,
    message: str,
) -> None:
    monkeypatch.setattr(pnl_module, "_default_db_path", lambda: pytest.fail("DB resolution must not run"))

    result = CliRunner().invoke(pnl_module.strat_pnl, ["-s", _DEPLOYMENT_ID, option, "0"])

    assert result.exit_code == 1
    assert result.output == message


def test_missing_db_keeps_exact_error_and_exit_code(tmp_path) -> None:
    db_path = tmp_path / "missing.db"

    result = CliRunner().invoke(pnl_module.strat_pnl, ["-s", _DEPLOYMENT_ID, "--db", str(db_path)])

    assert result.exit_code == 1
    assert result.output == (f"State DB not found at {db_path}. Run the strategy at least once (or pass --db).\n")


def test_load_failure_keeps_exact_error_and_exit_code(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    db_path = tmp_path / "state.db"
    db_path.touch()

    async def fail_load(*args: Any, **kwargs: Any) -> AccountingData:
        raise RuntimeError("broken query")

    monkeypatch.setattr(pnl_module, "load_accounting_data", fail_load)

    result = CliRunner().invoke(pnl_module.strat_pnl, ["-s", _DEPLOYMENT_ID, "--db", str(db_path)])

    assert result.exit_code == 1
    assert result.output == "Failed to read state DB: broken query\n"


@pytest.mark.parametrize(
    ("row_count", "truncated"),
    [(2, False), (3, True)],
)
def test_loader_probes_and_trims_both_bounded_queries(
    monkeypatch: pytest.MonkeyPatch,
    row_count: int,
    truncated: bool,
) -> None:
    calls: list[tuple[str, str, dict[str, int]]] = []

    async def load(db_path: str, deployment_id: str, **limits: int) -> AccountingData:
        calls.append((db_path, deployment_id, limits))
        return _accounting_data(
            ledger_entries=list(range(row_count)),
            position_events=[{"row": row} for row in range(row_count)],
        )

    monkeypatch.setattr(pnl_module, "load_accounting_data", load)

    data, positions_truncated, ledger_truncated = pnl_module._load_pnl_accounting_data(
        "/tmp/state.db",
        _DEPLOYMENT_ID,
        ledger_limit=2,
        position_limit=2,
    )

    assert calls == [("/tmp/state.db", _DEPLOYMENT_ID, {"ledger_limit": 3, "position_limit": 3})]
    assert positions_truncated is truncated
    assert ledger_truncated is truncated
    assert len(data.position_events) == 2
    assert len(data.ledger_entries) == 2


@pytest.mark.parametrize("suppressed", [False, True])
def test_prepare_breakdown_applies_fallback_and_both_truncation_warnings(
    monkeypatch: pytest.MonkeyPatch,
    suppressed: bool,
) -> None:
    metrics = SimpleNamespace(
        pnl_before_gas=Decimal("1"),
        pnl_after_gas=Decimal("1"),
        gas_spent_usd=Decimal("0"),
        initial_value_usd=Decimal("0"),
        deposits_usd=Decimal("0"),
        withdrawals_usd=Decimal("0"),
    )
    data = _accounting_data(metrics=metrics)
    monkeypatch.setattr(
        pnl_module,
        "detect_stale_post_teardown_snapshot",
        lambda snapshots, ledger: SimpleNamespace(suppressed=suppressed, reason="stale snapshot"),
    )

    breakdown = pnl_module._prepare_pnl_breakdown(
        data,
        _DEPLOYMENT_ID,
        "/tmp/state.db",
        ledger_limit=7,
        position_limit=5,
        position_events_truncated=True,
        ledger_entries_truncated=True,
    )

    assert breakdown.headline_suppressed is suppressed
    assert breakdown.headline_suppression_reason == ("stale snapshot" if suppressed else None)
    assert any("--position-limit (5)" in warning for warning in breakdown.warnings)
    assert any("--ledger-limit (7)" in warning for warning in breakdown.warnings)


def test_no_reportable_data_keeps_exact_error_and_exit_code(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    db_path = tmp_path / "empty.db"
    db_path.touch()

    async def load(*args: Any, **kwargs: Any) -> AccountingData:
        return _accounting_data()

    monkeypatch.setattr(pnl_module, "load_accounting_data", load)

    result = CliRunner().invoke(pnl_module.strat_pnl, ["-s", _DEPLOYMENT_ID, "--db", str(db_path)])

    assert result.exit_code == 1
    assert result.output == f"No persisted data found for strategy '{_DEPLOYMENT_ID}' in {db_path}.\n"


@pytest.mark.parametrize("as_json", [False, True])
def test_data_quality_only_rows_are_reported_instead_of_treated_as_missing(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    as_json: bool,
) -> None:
    db_path = tmp_path / "quality.db"
    db_path.touch()
    requested_id = "hosted-live-agent-id"
    unavailable = {
        "event_type": "SWAP",
        "position_key": "swap:arbitrum:wallet",
        "timestamp": "2026-09-03T12:00:00+00:00",
        "payload_json": json.dumps({"unavailable_reason": "price missing"}),
        "protocol": "uniswap_v3",
        "chain": "arbitrum",
    }
    calls: list[tuple[str, str]] = []

    async def load(path: str, deployment_id: str, **kwargs: Any) -> AccountingData:
        calls.append((path, deployment_id))
        return _accounting_data(
            deployment_id=deployment_id,
            unavailable_records=[unavailable],
            parse_errors=1,
        )

    monkeypatch.setattr(pnl_module, "load_accounting_data", load)
    monkeypatch.setenv("ALMANAK_IS_HOSTED", "true")
    args = ["-s", requested_id, "--db", str(db_path)]
    if as_json:
        args.append("--json")

    result = CliRunner().invoke(pnl_module.strat_pnl, args)

    assert result.exit_code == 0, result.output
    assert calls == [(str(db_path), requested_id)]
    assert "No persisted data" not in result.output
    if as_json:
        payload = json.loads(result.output)
        assert payload["deployment_id"] == requested_id
        assert payload["gross_pnl_usd"] is None
        assert payload["data_quality"]["unavailable_count"] == 1
        assert payload["data_quality"]["parse_errors"] == 1
    else:
        assert f"Strategy: {requested_id}" in result.output
        assert "Data Quality" in result.output
        assert "1 record(s) with UNAVAILABLE confidence" in result.output
        assert "1 event(s) failed to parse" in result.output


def test_default_db_path_is_passed_to_query_without_rewriting_identity(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "default.db"
    db_path.touch()
    calls: list[tuple[str, str]] = []

    async def load(path: str, deployment_id: str, **kwargs: Any) -> AccountingData:
        calls.append((path, deployment_id))
        return _accounting_data(deployment_id=deployment_id, parse_errors=1)

    monkeypatch.setattr(pnl_module, "_default_db_path", lambda: str(db_path))
    monkeypatch.setattr(pnl_module, "load_accounting_data", load)

    result = CliRunner().invoke(pnl_module.strat_pnl, ["-s", _DEPLOYMENT_ID, "--json"])

    assert result.exit_code == 0, result.output
    assert calls == [(str(db_path), _DEPLOYMENT_ID)]


def test_text_output_renders_known_class_without_empty_sections(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "swap.db"
    db_path.touch()
    metrics = SimpleNamespace(
        pnl_before_gas=Decimal("0"),
        pnl_after_gas=Decimal("0"),
        gas_spent_usd=Decimal("0"),
        initial_value_usd=Decimal("0"),
        deposits_usd=Decimal("0"),
        withdrawals_usd=Decimal("0"),
    )

    async def load(*args: Any, **kwargs: Any) -> AccountingData:
        return _accounting_data(metrics=metrics, strategy_classes=frozenset({StrategyClass.SWAP}))

    monkeypatch.setattr(pnl_module, "load_accounting_data", load)

    result = CliRunner().invoke(pnl_module.strat_pnl, ["-s", _DEPLOYMENT_ID, "--db", str(db_path)])

    assert result.exit_code == 0, result.output
    assert "Strategy class: swap" in result.output
    assert "Data Quality" not in result.output


async def _seed_accounting_only_swap(db_path: str) -> None:
    store = SQLiteStore(SQLiteConfig(db_path=db_path))
    await store.initialize()
    try:
        identity = AccountingIdentity(
            id="accounting-only-swap",
            deployment_id=_DEPLOYMENT_ID,
            cycle_id="cycle-1",
            execution_mode="paper",
            timestamp=datetime(2026, 9, 3, 12, 0, tzinfo=UTC),
            chain="arbitrum",
            protocol="uniswap_v3",
            wallet_address="0x0000000000000000000000000000000000000001",
            tx_hash="0xabc",
            ledger_entry_id="ledger-missing",
        )
        await store.save_accounting_event(
            SwapAccountingEvent(
                identity=identity,
                event_type=SwapEventType.SWAP,
                protocol="uniswap_v3",
                token_in="USDC",
                token_out="WETH",
                amount_in=Decimal("1"),
                amount_out=Decimal("0.0002"),
                amount_in_usd=Decimal("1"),
                amount_out_usd=Decimal("1"),
                effective_price=Decimal("0.0002"),
                slippage_bps=0,
                realized_pnl_usd=Decimal("0"),
                cost_basis_recorded=True,
                gas_usd=Decimal("0"),
                confidence=AccountingConfidence.HIGH,
                unavailable_reason="",
            )
        )
    finally:
        await store.close()


def test_accounting_only_swap_remains_not_found_pending_product_decision(tmp_path) -> None:
    """Preserve the ALM-3528 behavior until report semantics are ratified."""
    db_path = tmp_path / "accounting-only.db"
    asyncio.run(_seed_accounting_only_swap(str(db_path)))

    result = CliRunner().invoke(pnl_module.strat_pnl, ["-s", _DEPLOYMENT_ID, "--db", str(db_path)])

    assert result.exit_code == 1
    assert result.output == f"No persisted data found for strategy '{_DEPLOYMENT_ID}' in {db_path}.\n"
