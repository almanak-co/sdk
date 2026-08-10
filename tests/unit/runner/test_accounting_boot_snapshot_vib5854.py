"""VIB-5854 — the accounting interval starts before transaction one."""

from __future__ import annotations

import ast
import inspect
import textwrap
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.observability.context import clear_cycle_id, get_cycle_id, set_cycle_id
from almanak.framework.portfolio.models import PortfolioSnapshot, ValueConfidence
from almanak.framework.runner import _run_loop_helpers
from almanak.framework.runner._run_loop_helpers import capture_boot_snapshot_with_accounting
from almanak.framework.runner.runner_state import (
    UnmeasuredAccountingSnapshotError,
    capture_portfolio_snapshot,
)
from almanak.framework.state.exceptions import AccountingPersistenceError, AccountingWriteKind


def _runner(*, existing=None, live: bool = False):
    state_manager = SimpleNamespace(
        get_first_snapshot=AsyncMock(return_value=existing),
        get_snapshots_since=AsyncMock(return_value=[existing] if existing is not None else []),
        get_latest_snapshot=AsyncMock(return_value=existing),
        get_portfolio_metrics=AsyncMock(return_value=None),
        save_portfolio_snapshot=AsyncMock(return_value=1),
        load_state=AsyncMock(return_value=None),
    )
    runner = SimpleNamespace(
        config=SimpleNamespace(enable_state_persistence=True, dry_run=False, paper_mode=not live),
        state_manager=state_manager,
        _last_cycle_id="outer-run",
        _last_snapshot_time=None,
        _snapshot_interval_seconds=300,
        _portfolio_valuer=SimpleNamespace(),
        _get_gateway_client=lambda: None,
        deployment_id="deployment-1",
        _begin_market_snapshot_iteration=lambda strategy, cycle_id: None,
        _is_live_mode=lambda: live,
    )
    strategy = SimpleNamespace(deployment_id="deployment-1")
    return runner, strategy


def _snapshot(*, confidence: ValueConfidence = ValueConfidence.HIGH) -> PortfolioSnapshot:
    return PortfolioSnapshot(
        timestamp=datetime(2026, 8, 9, tzinfo=UTC),
        deployment_id="deployment-1",
        total_value_usd=Decimal("4"),
        available_cash_usd=Decimal("1"),
        value_confidence=confidence,
    )


@pytest.mark.asyncio
async def test_fresh_deployment_forces_pretrade_snapshot_and_restores_cycle_surfaces(monkeypatch):
    runner, strategy = _runner()
    observed: dict[str, object] = {}
    captured = _snapshot()

    def begin_snapshot(strategy_arg, cycle_id):
        observed["begin"] = (strategy_arg, cycle_id, runner._last_cycle_id, get_cycle_id())

    async def capture(runner_arg, strategy_arg, iteration_number, force_snapshot, require_measured_equity):
        observed["capture"] = (
            runner_arg,
            strategy_arg,
            iteration_number,
            force_snapshot,
            runner._last_cycle_id,
            get_cycle_id(),
            require_measured_equity,
        )
        return captured

    runner._begin_market_snapshot_iteration = begin_snapshot
    monkeypatch.setattr("almanak.framework.runner.runner_state.capture_portfolio_snapshot", capture)
    set_cycle_id("outer-context")
    try:
        result = await capture_boot_snapshot_with_accounting(runner, strategy, "deployment-1")
        assert result is captured
        assert observed["begin"] == (
            strategy,
            "boot-deployment-1",
            "boot-deployment-1",
            "boot-deployment-1",
        )
        assert observed["capture"] == (
            runner,
            strategy,
            0,
            True,
            "boot-deployment-1",
            "boot-deployment-1",
            True,
        )
        assert runner._last_cycle_id == "outer-run"
        assert get_cycle_id() == "outer-context"
    finally:
        clear_cycle_id()


@pytest.mark.asyncio
async def test_existing_snapshot_makes_boot_capture_idempotent(monkeypatch):
    existing = _snapshot()
    runner, strategy = _runner(existing=existing)
    capture = AsyncMock()
    monkeypatch.setattr("almanak.framework.runner.runner_state.capture_portfolio_snapshot", capture)

    result = await capture_boot_snapshot_with_accounting(runner, strategy, "deployment-1")

    assert result is existing
    capture.assert_not_awaited()
    assert runner._last_cycle_id == "outer-run"


@pytest.mark.asyncio
async def test_measured_opening_remains_idempotent_after_later_unavailable_row(monkeypatch):
    """Negative control: restart must never relabel a mid-run row as boot."""
    opening = _snapshot()
    runner, strategy = _runner(existing=opening, live=True)
    runner.state_manager.get_latest_snapshot.return_value = _snapshot(confidence=ValueConfidence.UNAVAILABLE)
    capture = AsyncMock()
    monkeypatch.setattr("almanak.framework.runner.runner_state.capture_portfolio_snapshot", capture)

    result = await capture_boot_snapshot_with_accounting(runner, strategy, "deployment-1")

    assert result is opening
    runner.state_manager.get_snapshots_since.assert_awaited_once()
    assert runner.state_manager.get_snapshots_since.await_args.kwargs == {"limit": 2}
    runner.state_manager.get_first_snapshot.assert_not_awaited()
    runner.state_manager.get_latest_snapshot.assert_awaited_once_with("deployment-1")
    capture.assert_not_awaited()


@pytest.mark.asyncio
async def test_legacy_unavailable_before_measured_boot_remains_idempotent(monkeypatch):
    diagnostic = _snapshot(confidence=ValueConfidence.UNAVAILABLE)
    opening = _snapshot()
    opening.cycle_id = "boot-deployment-1"
    runner, strategy = _runner(existing=diagnostic, live=True)
    runner.state_manager.get_snapshots_since.return_value = [diagnostic, opening]
    runner.state_manager.get_latest_snapshot.return_value = _snapshot(confidence=ValueConfidence.UNAVAILABLE)
    capture = AsyncMock()
    monkeypatch.setattr("almanak.framework.runner.runner_state.capture_portfolio_snapshot", capture)

    result = await capture_boot_snapshot_with_accounting(runner, strategy, "deployment-1")

    assert result is opening
    capture.assert_not_awaited()


@pytest.mark.asyncio
async def test_existing_unavailable_snapshot_does_not_short_circuit_boot_capture(monkeypatch):
    existing = _snapshot(confidence=ValueConfidence.UNAVAILABLE)
    measured = _snapshot()
    runner, strategy = _runner(existing=existing)
    capture = AsyncMock(return_value=measured)
    monkeypatch.setattr("almanak.framework.runner.runner_state.capture_portfolio_snapshot", capture)

    result = await capture_boot_snapshot_with_accounting(runner, strategy, "deployment-1")

    assert result is measured
    capture.assert_awaited_once_with(
        runner,
        strategy,
        iteration_number=0,
        force_snapshot=True,
        require_measured_equity=True,
    )


@pytest.mark.asyncio
async def test_paper_canonical_unavailable_capture_stays_absent(monkeypatch, caplog):
    """Negative control: the real capture path must not persist a zero boot endpoint."""
    runner, strategy = _runner(live=False)
    capture = AsyncMock(return_value=_snapshot(confidence=ValueConfidence.UNAVAILABLE))
    monkeypatch.setattr("almanak.framework.runner.runner_state.capture_portfolio_snapshot", capture)

    result = await capture_boot_snapshot_with_accounting(runner, strategy, "deployment-1")

    assert result is None
    capture.assert_awaited_once_with(
        runner,
        strategy,
        iteration_number=0,
        force_snapshot=True,
        require_measured_equity=True,
    )
    runner.state_manager.save_portfolio_snapshot.assert_not_awaited()
    assert "G6 window coverage will remain unmeasured" in caplog.text


@pytest.mark.asyncio
async def test_live_canonical_unavailable_capture_halts_without_persisting(monkeypatch):
    """Live boot refuses unmeasured equity before transaction one."""
    runner, strategy = _runner(live=True)
    capture = AsyncMock(return_value=_snapshot(confidence=ValueConfidence.UNAVAILABLE))
    monkeypatch.setattr("almanak.framework.runner.runner_state.capture_portfolio_snapshot", capture)

    with pytest.raises(RuntimeError, match="Failed to capture accounting boot snapshot"):
        await capture_boot_snapshot_with_accounting(runner, strategy, "deployment-1")

    capture.assert_awaited_once_with(
        runner,
        strategy,
        iteration_number=0,
        force_snapshot=True,
        require_measured_equity=True,
    )
    runner.state_manager.save_portfolio_snapshot.assert_not_awaited()


@pytest.mark.asyncio
async def test_strict_capture_refuses_unmeasured_equity_at_persistence_chokepoint(monkeypatch):
    """The real capture function must refuse before either durable writer."""
    runner, strategy = _runner(live=True)
    unmeasured = _snapshot(confidence=ValueConfidence.UNAVAILABLE)
    monkeypatch.setattr(
        "almanak.framework.runner.runner_state._value_via_portfolio_valuer",
        lambda runner_arg, strategy_arg, iteration_number: unmeasured,
    )
    fallback = MagicMock(return_value=unmeasured)
    monkeypatch.setattr("almanak.framework.runner.runner_state._value_via_strategy_fallback", fallback)

    with pytest.raises(UnmeasuredAccountingSnapshotError, match="unmeasured equity"):
        await capture_portfolio_snapshot(
            runner,
            strategy,
            iteration_number=0,
            force_snapshot=True,
            require_measured_equity=True,
        )

    fallback.assert_called_once()
    runner.state_manager.save_portfolio_snapshot.assert_not_awaited()


@pytest.mark.asyncio
async def test_paper_boot_snapshot_lookup_failure_stays_absent_for_g6_to_detect(monkeypatch, caplog):
    runner, strategy = _runner(live=False)
    runner.state_manager.get_snapshots_since.side_effect = OSError("state backend unavailable")
    capture = AsyncMock()
    monkeypatch.setattr("almanak.framework.runner.runner_state.capture_portfolio_snapshot", capture)

    result = await capture_boot_snapshot_with_accounting(runner, strategy, "deployment-1")

    assert result is None
    capture.assert_not_awaited()
    assert "G6 window coverage will remain unmeasured" in caplog.text
    assert runner._last_cycle_id == "outer-run"


@pytest.mark.asyncio
async def test_live_boot_snapshot_lookup_failure_halts_before_first_trade(monkeypatch):
    runner, strategy = _runner(live=True)
    runner.state_manager.get_snapshots_since.side_effect = OSError("state backend unavailable")
    capture = AsyncMock()
    monkeypatch.setattr("almanak.framework.runner.runner_state.capture_portfolio_snapshot", capture)

    with pytest.raises(RuntimeError, match="Failed to capture accounting boot snapshot"):
        await capture_boot_snapshot_with_accounting(runner, strategy, "deployment-1")

    capture.assert_not_awaited()
    assert runner._last_cycle_id == "outer-run"


@pytest.mark.asyncio
async def test_paper_boot_failure_stays_absent_for_g6_to_detect(monkeypatch, caplog):
    runner, strategy = _runner(live=False)

    async def fail(*args, **kwargs):
        raise ValueError("oracle unavailable")

    monkeypatch.setattr("almanak.framework.runner.runner_state.capture_portfolio_snapshot", fail)

    result = await capture_boot_snapshot_with_accounting(runner, strategy, "deployment-1")

    assert result is None
    assert "G6 window coverage will remain unmeasured" in caplog.text


@pytest.mark.asyncio
async def test_live_boot_failure_halts_before_first_trade(monkeypatch):
    runner, strategy = _runner(live=True)

    async def fail(*args, **kwargs):
        raise ValueError("oracle unavailable")

    monkeypatch.setattr("almanak.framework.runner.runner_state.capture_portfolio_snapshot", fail)

    with pytest.raises(RuntimeError, match="Failed to capture accounting boot snapshot"):
        await capture_boot_snapshot_with_accounting(runner, strategy, "deployment-1")


@pytest.mark.asyncio
async def test_paper_typed_persistence_failure_logs_and_continues(monkeypatch, caplog):
    runner, strategy = _runner(live=False)
    error = AccountingPersistenceError(
        AccountingWriteKind.SNAPSHOT,
        deployment_id="deployment-1",
        cause=OSError("disk full"),
    )

    async def fail(*args, **kwargs):
        raise error

    monkeypatch.setattr("almanak.framework.runner.runner_state.capture_portfolio_snapshot", fail)

    assert await capture_boot_snapshot_with_accounting(runner, strategy, "deployment-1") is None
    assert "G6 window coverage will remain unmeasured" in caplog.text


@pytest.mark.asyncio
async def test_live_typed_persistence_failure_propagates_unchanged(monkeypatch):
    runner, strategy = _runner(live=True)
    error = AccountingPersistenceError(
        AccountingWriteKind.SNAPSHOT,
        deployment_id="deployment-1",
        cause=OSError("disk full"),
    )

    async def fail(*args, **kwargs):
        raise error

    monkeypatch.setattr("almanak.framework.runner.runner_state.capture_portfolio_snapshot", fail)

    with pytest.raises(AccountingPersistenceError) as raised:
        await capture_boot_snapshot_with_accounting(runner, strategy, "deployment-1")
    assert raised.value is error


def _ordered_calls(function, first: str, second: str) -> bool:
    tree = ast.parse(textwrap.dedent(inspect.getsource(function)))

    def _call_name(node: ast.Call) -> str | None:
        target = node.func
        if isinstance(target, ast.Name):
            return target.id
        if isinstance(target, ast.Attribute):
            return target.attr
        return None

    calls = [node for node in ast.walk(tree) if isinstance(node, ast.Call)]
    first_lines = [node.lineno for node in calls if _call_name(node) == first]
    second_lines = [node.lineno for node in calls if _call_name(node) == second]
    return bool(first_lines and second_lines and min(first_lines) < min(second_lines))


def test_continuous_runner_captures_boot_before_opening_basis_reconstruction():
    assert _ordered_calls(
        _run_loop_helpers.initialize_run_loop,
        "capture_boot_snapshot_with_accounting",
        "reconstruct_lending_basis_store",
    )


def test_all_execution_entry_points_keep_boot_capture_before_reconstruction():
    """Static anti-bypass guard for continuous, --once, and lifecycle lanes."""
    from almanak.framework.cli import _run_modes

    assert _ordered_calls(
        _run_modes._run_once,
        "capture_boot_snapshot_with_accounting",
        "run_iteration",
    )
    assert _ordered_calls(
        _run_modes._run_test_lifecycle,
        "capture_boot_snapshot_with_accounting",
        "run_iteration",
    )

    from almanak.framework.runner.strategy_runner import StrategyRunner

    assert _ordered_calls(
        StrategyRunner.run_loop,
        "initialize_run_loop",
        "run_iteration",
    )
