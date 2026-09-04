from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest

from almanak.framework.portfolio import PortfolioMetrics, PortfolioSnapshot, ValueConfidence
from almanak.framework.runner import runner_state
from almanak.framework.state.exceptions import AccountingPersistenceError, AccountingWriteKind

_NOW = datetime(2026, 9, 3, 12, 30, tzinfo=UTC)


def _snapshot(
    *,
    deployment_id: str = "snapshot-deployment",
    total: str = "100",
    cash: str = "25",
    confidence: ValueConfidence | None = ValueConfidence.HIGH,
    error: str | None = None,
) -> PortfolioSnapshot:
    return PortfolioSnapshot(
        timestamp=_NOW,
        deployment_id=deployment_id,
        total_value_usd=Decimal(total),
        available_cash_usd=Decimal(cash),
        value_confidence=confidence,
        error=error,
        snapshot_metadata={},
    )


def _existing_metrics(*, deployment_id: str = "persisted-deployment") -> PortfolioMetrics:
    return PortfolioMetrics(
        timestamp=datetime(2026, 9, 2, tzinfo=UTC),
        deployment_id=deployment_id,
        total_value_usd=Decimal("90"),
        initial_value_usd=Decimal("80"),
        deposits_usd=None,
        withdrawals_usd=Decimal("0"),
        gas_spent_usd=Decimal("3"),
        positions_json='[{"existing":"record"}]',
        cycle_id="old-cycle",
        execution_mode="dry_run",
        is_complete=False,
    )


def _runner(
    existing: PortfolioMetrics | None = None,
    *,
    deployment_id: str = "runner-deployment",
    cycle_id: str = "runner-cycle",
    dry_run: bool = False,
    paper_mode: bool = True,
) -> SimpleNamespace:
    state_manager = SimpleNamespace(get_portfolio_metrics=AsyncMock(return_value=existing))
    return SimpleNamespace(
        state_manager=state_manager,
        deployment_id=deployment_id,
        _last_cycle_id=cycle_id,
        config=SimpleNamespace(dry_run=dry_run, paper_mode=paper_mode),
    )


_METRICS_GOLDENS = [
    pytest.param(
        False,
        "100",
        "25",
        Decimal("4"),
        {
            "deployment_id": "runner-deployment",
            "timestamp": _NOW.isoformat(),
            "total_value_usd": "4",
            "initial_value_usd": "4",
            "deposits_usd": "0",
            "withdrawals_usd": "0",
            "gas_spent_usd": "0",
            "positions_json": '[{"initial_value_usd":"4","record_type":"accounting_baseline_provenance","schema_version":1,"source":"strategy_allocation_usd"}]',
            "cycle_id": "runner-cycle",
            "execution_mode": "paper",
            "is_complete": True,
        },
        id="new-strategy-allocation",
    ),
    pytest.param(
        False,
        "100",
        "25",
        None,
        {
            "deployment_id": "runner-deployment",
            "timestamp": _NOW.isoformat(),
            "total_value_usd": "100",
            "initial_value_usd": "100",
            "deposits_usd": "0",
            "withdrawals_usd": "0",
            "gas_spent_usd": "0",
            "positions_json": '[{"initial_value_usd":"100","record_type":"accounting_baseline_provenance","schema_version":1,"source":"snapshot_total_value_usd"}]',
            "cycle_id": "runner-cycle",
            "execution_mode": "paper",
            "is_complete": True,
        },
        id="new-snapshot-total",
    ),
    pytest.param(
        False,
        "0",
        "25",
        None,
        {
            "deployment_id": "runner-deployment",
            "timestamp": _NOW.isoformat(),
            "total_value_usd": "25",
            "initial_value_usd": "25",
            "deposits_usd": "0",
            "withdrawals_usd": "0",
            "gas_spent_usd": "0",
            "positions_json": '[{"initial_value_usd":"25","record_type":"accounting_baseline_provenance","schema_version":1,"source":"snapshot_available_cash_usd"}]',
            "cycle_id": "runner-cycle",
            "execution_mode": "paper",
            "is_complete": True,
        },
        id="new-snapshot-cash",
    ),
    pytest.param(
        False,
        "0",
        "0",
        None,
        {
            "deployment_id": "runner-deployment",
            "timestamp": _NOW.isoformat(),
            "total_value_usd": "0",
            "initial_value_usd": "0",
            "deposits_usd": "0",
            "withdrawals_usd": "0",
            "gas_spent_usd": "0",
            "positions_json": '[{"initial_value_usd":"0","record_type":"accounting_baseline_provenance","schema_version":1,"source":"snapshot_available_cash_usd"}]',
            "cycle_id": "runner-cycle",
            "execution_mode": "paper",
            "is_complete": True,
        },
        id="new-measured-zero",
    ),
    pytest.param(
        True,
        "110",
        "25",
        None,
        {
            "deployment_id": "persisted-deployment",
            "timestamp": _NOW.isoformat(),
            "total_value_usd": "110",
            "initial_value_usd": "80",
            "deposits_usd": None,
            "withdrawals_usd": "0",
            "gas_spent_usd": "3",
            "positions_json": '[{"existing":"record"}]',
            "cycle_id": "runner-cycle",
            "execution_mode": "paper",
            "is_complete": False,
        },
        id="existing-position-value",
    ),
    pytest.param(
        True,
        "0",
        "125",
        None,
        {
            "deployment_id": "persisted-deployment",
            "timestamp": _NOW.isoformat(),
            "total_value_usd": "125",
            "initial_value_usd": "80",
            "deposits_usd": None,
            "withdrawals_usd": "0",
            "gas_spent_usd": "3",
            "positions_json": '[{"existing":"record"}]',
            "cycle_id": "runner-cycle",
            "execution_mode": "paper",
            "is_complete": False,
        },
        id="existing-cash-fallback",
    ),
]


@pytest.mark.asyncio
@pytest.mark.parametrize(("has_existing", "total", "cash", "allocation", "expected"), _METRICS_GOLDENS)
async def test_metrics_branch_table_matches_golden_output(
    has_existing: bool,
    total: str,
    cash: str,
    allocation: Decimal | None,
    expected: dict[str, Any],
) -> None:
    existing = _existing_metrics() if has_existing else None
    runner = _runner(existing)
    snapshot = _snapshot(total=total, cash=cash)
    strategy = SimpleNamespace(allocation_usd=allocation)

    with (
        patch.object(runner_state, "_populate_gas_spent_usd", new=AsyncMock()),
        patch.object(runner_state, "_populate_capital_flows", new=AsyncMock()),
    ):
        result = await runner_state._build_metrics_for_snapshot(runner, "requested-deployment", snapshot, strategy)

    assert result is not None
    assert result.to_dict() == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("state_manager", [SimpleNamespace(), SimpleNamespace(get_portfolio_metrics=None)])
async def test_non_callable_metrics_capability_is_skipped_without_warning(state_manager: Any) -> None:
    runner = _runner()
    runner.state_manager = state_manager
    log = MagicMock()

    with patch.object(runner_state, "logger", log):
        result = await runner_state._build_metrics_for_snapshot(runner, "requested-deployment", _snapshot())

    assert result is None
    log.warning.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "snapshot",
    [
        pytest.param(_snapshot(error="valuation failed"), id="error-stamped"),
        pytest.param(_snapshot(confidence=ValueConfidence.UNAVAILABLE), id="confidence-unavailable"),
        pytest.param(_snapshot(confidence=None), id="confidence-unmeasured"),
    ],
)
async def test_unavailable_snapshots_skip_metrics_before_state_reads(snapshot: PortfolioSnapshot) -> None:
    runner = _runner()
    log = MagicMock()

    with patch.object(runner_state, "logger", log):
        result = await runner_state._build_metrics_for_snapshot(runner, "requested-deployment", snapshot)

    assert result is None
    runner.state_manager.get_portfolio_metrics.assert_not_awaited()
    log.info.assert_called_once_with("Skipping portfolio metrics for requested-deployment: snapshot unavailable")


@pytest.mark.parametrize(
    ("runner_id", "snapshot_id", "requested_id", "expected_id"),
    [
        ("runner-id", "snapshot-id", "requested-id", "runner-id"),
        ("", "snapshot-id", "requested-id", "snapshot-id"),
        ("", "", "requested-id", "requested-id"),
    ],
)
def test_metrics_identity_fallback_order_is_stable(
    runner_id: str,
    snapshot_id: str,
    requested_id: str,
    expected_id: str,
) -> None:
    context = runner_state._resolve_metrics_snapshot_context(
        _runner(deployment_id=runner_id),
        requested_id,
        _snapshot(deployment_id=snapshot_id),
    )

    assert context.deployment_id == expected_id


def test_cycle_id_falls_back_to_observability_context() -> None:
    with patch("almanak.framework.observability.context.get_cycle_id", return_value="context-cycle"):
        context = runner_state._resolve_metrics_snapshot_context(
            _runner(cycle_id=""),
            "requested-deployment",
            _snapshot(),
        )

    assert context.cycle_id == "context-cycle"


def test_cycle_id_context_failure_is_debug_logged_and_keeps_empty_id() -> None:
    error = RuntimeError("context unavailable")
    log = MagicMock()
    with (
        patch("almanak.framework.observability.context.get_cycle_id", side_effect=error),
        patch.object(runner_state, "logger", log),
    ):
        context = runner_state._resolve_metrics_snapshot_context(
            _runner(cycle_id=""),
            "requested-deployment",
            _snapshot(),
        )

    assert context.cycle_id == ""
    log.debug.assert_called_once_with("cycle_id context fallback failed: %s", error)


@pytest.mark.parametrize(
    ("allocation", "expected_value", "expected_source"),
    [
        ("bad", Decimal("100"), "snapshot_total_value_usd"),
        (Decimal("NaN"), Decimal("100"), "snapshot_total_value_usd"),
        (Decimal("Infinity"), Decimal("100"), "snapshot_total_value_usd"),
        (Decimal("0"), Decimal("100"), "snapshot_total_value_usd"),
        (Decimal("-1"), Decimal("100"), "snapshot_total_value_usd"),
        (Decimal("0.01"), Decimal("0.01"), "strategy_allocation_usd"),
    ],
)
def test_allocation_resolution_table_preserves_numeric_guard(
    allocation: Any,
    expected_value: Decimal,
    expected_source: str,
) -> None:
    baseline = runner_state._resolve_metrics_baseline(
        SimpleNamespace(allocation_usd=allocation),
        _snapshot(),
        "deployment-id",
    )

    assert (baseline.value, baseline.source) == (expected_value, expected_source)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("dry_run", "paper_mode", "expected_mode", "expected_live"),
    [
        (True, True, "dry_run", False),
        (False, True, "paper", False),
        (False, False, "live", True),
    ],
)
async def test_aggregate_order_and_live_mode_mapping_are_stable(
    dry_run: bool,
    paper_mode: bool,
    expected_mode: str,
    expected_live: bool,
) -> None:
    runner = _runner(dry_run=dry_run, paper_mode=paper_mode)
    snapshot = _snapshot()
    calls = MagicMock()
    gas = AsyncMock()
    capital = AsyncMock()
    calls.attach_mock(gas, "gas")
    calls.attach_mock(capital, "capital")

    with (
        patch.object(runner_state, "_populate_gas_spent_usd", new=gas),
        patch.object(runner_state, "_populate_capital_flows", new=capital),
    ):
        result = await runner_state._build_metrics_for_snapshot(runner, "requested-deployment", snapshot)

    assert result is not None
    assert result.execution_mode == expected_mode
    assert calls.mock_calls == [
        call.gas(
            runner,
            result,
            snapshot,
            deployment_id="runner-deployment",
            is_live=expected_live,
        ),
        call.capital(runner, result, snapshot, deployment_id="runner-deployment"),
    ]


@pytest.mark.asyncio
async def test_aggregated_metrics_preserve_gas_performance_and_empty_not_zero() -> None:
    runner = _runner(_existing_metrics())

    async def populate_gas(_runner: Any, metrics: PortfolioMetrics, _snapshot: Any, **_kwargs: Any) -> None:
        metrics.gas_spent_usd = Decimal("5")

    async def populate_flows(_runner: Any, metrics: PortfolioMetrics, _snapshot: Any, **_kwargs: Any) -> None:
        metrics.deposits_usd = None
        metrics.withdrawals_usd = Decimal("0")

    with (
        patch.object(runner_state, "_populate_gas_spent_usd", side_effect=populate_gas),
        patch.object(runner_state, "_populate_capital_flows", side_effect=populate_flows),
    ):
        result = await runner_state._build_metrics_for_snapshot(
            runner,
            "requested-deployment",
            _snapshot(total="120"),
        )

    assert result is not None
    assert result.gas_spent_usd == Decimal("5")
    assert result.deposits_usd is None
    assert result.withdrawals_usd == Decimal("0")
    assert result.pnl_before_gas is None
    assert result.pnl_after_gas is None
    assert result.roi_percent is None


@pytest.mark.asyncio
@pytest.mark.parametrize("failing_aggregate", ["gas", "capital"])
async def test_typed_accounting_failures_propagate_unchanged(failing_aggregate: str) -> None:
    runner = _runner()
    error = AccountingPersistenceError(
        AccountingWriteKind.METRICS,
        deployment_id="runner-deployment",
        message="accounting failed",
    )
    gas = AsyncMock(side_effect=error if failing_aggregate == "gas" else None)
    capital = AsyncMock(side_effect=error if failing_aggregate == "capital" else None)

    with (
        patch.object(runner_state, "_populate_gas_spent_usd", new=gas),
        patch.object(runner_state, "_populate_capital_flows", new=capital),
        pytest.raises(AccountingPersistenceError) as raised,
    ):
        await runner_state._build_metrics_for_snapshot(runner, "requested-deployment", _snapshot())

    assert raised.value is error
    if failing_aggregate == "gas":
        capital.assert_not_awaited()
    else:
        gas.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_stage", ["context", "read", "aggregate"])
async def test_untyped_failures_keep_warning_and_none_contract(failure_stage: str) -> None:
    runner = _runner()
    error = RuntimeError(f"{failure_stage} failed")
    log = MagicMock()
    context = MagicMock(
        side_effect=error if failure_stage == "context" else runner_state._resolve_metrics_snapshot_context
    )
    if failure_stage == "read":
        runner.state_manager.get_portfolio_metrics.side_effect = error
    aggregate = AsyncMock(side_effect=error if failure_stage == "aggregate" else None)

    with (
        patch.object(runner_state, "logger", log),
        patch.object(runner_state, "_resolve_metrics_snapshot_context", new=context),
        patch.object(runner_state, "_populate_metrics_aggregates", new=aggregate),
    ):
        result = await runner_state._build_metrics_for_snapshot(runner, "requested-deployment", _snapshot())

    assert result is None
    log.warning.assert_called_once_with(f"Failed to build portfolio metrics: {error}")


@pytest.mark.asyncio
async def test_existing_blank_deployment_id_is_backfilled_without_rewriting_baseline() -> None:
    existing = _existing_metrics(deployment_id="")
    runner = _runner(existing)
    original_baseline = existing.initial_value_usd
    original_records = existing.positions_json

    with patch.object(runner_state, "_populate_metrics_aggregates", new=AsyncMock()):
        result = await runner_state._build_metrics_for_snapshot(runner, "requested-deployment", _snapshot())

    assert result is existing
    assert result.deployment_id == "runner-deployment"
    assert result.initial_value_usd == original_baseline
    assert result.positions_json == original_records
