"""Tests for mode-aware iteration-state persistence (runner_state.update_state).

Contract (blueprint 27 failure-mode table):
- live: a failed durable state write raises AccountingPersistenceError
  (write_kind="state"); the run loop escalates to ACCOUNTING_FAILED.
- paper / dry_run: failures log ERROR and the loop continues.
- StateConflictError (CAS) logs a distinct identity-collision message in
  ALL modes.

Pattern: tests/unit/runner/test_accounting_persistence.py (VIB-3157),
tests/unit/runner/test_run_loop_characterization.py, and
tests/unit/runner/test_accounting_bypass_paths.py (T-3762-3) for the
structlog logger-monkeypatch idiom: runner_state uses structlog (not stdlib
logging), so stdlib caplog cannot capture its output. Patch
runner_state.logger with a MagicMock and assert on call_args_list instead.
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.runner.runner_models import (
    IterationResult,
    IterationStatus,
    RunnerConfig,
)
from almanak.framework.runner.runner_state import update_state
from almanak.framework.runner.strategy_runner import StrategyRunner
from almanak.framework.state.exceptions import (
    AccountingPersistenceError,
    AccountingWriteKind,
)
from almanak.framework.state.state_manager import StateConflictError, StateData

# =============================================================================
# Unit-level: update_state itself
# =============================================================================


class _Runner(StrategyRunner):
    """Bypass StrategyRunner.__init__ -- only the attrs update_state touches."""

    def __init__(self, *, state_manager: Any, config: RunnerConfig | None = None) -> None:
        self.state_manager = state_manager
        self.config = config or RunnerConfig()
        self._total_iterations = 7
        self._successful_iterations = 6
        self._consecutive_errors = 1


def _result() -> IterationResult:
    return IterationResult(status=IterationStatus.SUCCESS, deployment_id="d1", duration_ms=10.0)


def _state_mgr(*, save_side_effect: Exception | None = None) -> MagicMock:
    mgr = MagicMock()
    mgr.load_state = AsyncMock(return_value=StateData(deployment_id="d1", version=3, state={}))
    mgr.save_state = AsyncMock(side_effect=save_side_effect)
    return mgr


@pytest.mark.asyncio
async def test_success_path_saves_with_cas_and_counters() -> None:
    mgr = _state_mgr()
    runner = _Runner(state_manager=mgr, config=RunnerConfig(dry_run=False))

    await update_state(runner, "d1", _result())

    mgr.save_state.assert_awaited_once()
    saved = mgr.save_state.await_args.args[0]
    assert mgr.save_state.await_args.kwargs["expected_version"] == 3
    assert saved.state["total_iterations"] == 7
    assert saved.state["successful_iterations"] == 6
    assert saved.state["consecutive_errors"] == 1
    # NB: IterationStatus values are uppercase ("SUCCESS"), verified at planning time.
    assert saved.state["last_iteration"]["status"] == "SUCCESS"


@pytest.mark.asyncio
async def test_live_mode_save_failure_raises_typed_error() -> None:
    mgr = _state_mgr(save_side_effect=RuntimeError("database is locked"))
    runner = _Runner(state_manager=mgr, config=RunnerConfig(dry_run=False))

    with pytest.raises(AccountingPersistenceError) as exc_info:
        await update_state(runner, "d1", _result())

    assert exc_info.value.write_kind == AccountingWriteKind.STATE
    assert exc_info.value.deployment_id == "d1"
    assert isinstance(exc_info.value.cause, RuntimeError)


@pytest.mark.asyncio
async def test_dry_run_save_failure_logs_error_and_continues(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # runner_state uses structlog (not stdlib logging); stdlib caplog cannot
    # capture its output. Monkeypatch the module-level logger instead — same
    # idiom as test_accounting_bypass_paths.py T-3762-3.
    from almanak.framework.runner import runner_state

    captured_logger = MagicMock()
    monkeypatch.setattr(runner_state, "logger", captured_logger)

    mgr = _state_mgr(save_side_effect=RuntimeError("database is locked"))
    runner = _Runner(state_manager=mgr, config=RunnerConfig(dry_run=True))

    await update_state(runner, "d1", _result())  # must NOT raise

    assert captured_logger.error.called, (
        "expected an ERROR 'Failed to update state' log, got: "
        + repr(captured_logger.error.call_args_list)
    )
    error_messages = [str(call.args[0]) if call.args else "" for call in captured_logger.error.call_args_list]
    assert any("Failed to update state" in m for m in error_messages), (
        f"expected 'Failed to update state' in error log, got: {error_messages}"
    )


@pytest.mark.asyncio
async def test_live_mode_cas_conflict_raises_and_logs_collision_signal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # runner_state uses structlog; monkeypatch module-level logger (T-3762-3 idiom).
    from almanak.framework.runner import runner_state

    captured_logger = MagicMock()
    monkeypatch.setattr(runner_state, "logger", captured_logger)

    conflict = StateConflictError(deployment_id="d1", expected_version=3, actual_version=5)
    mgr = _state_mgr(save_side_effect=conflict)
    runner = _Runner(state_manager=mgr, config=RunnerConfig(dry_run=False))

    with pytest.raises(AccountingPersistenceError) as exc_info:
        await update_state(runner, "d1", _result())

    assert exc_info.value.write_kind == AccountingWriteKind.STATE
    assert exc_info.value.cause is conflict

    assert captured_logger.error.called, (
        "expected an ERROR log for CAS conflict, got: "
        + repr(captured_logger.error.call_args_list)
    )
    error_messages = [str(call.args[0]) if call.args else "" for call in captured_logger.error.call_args_list]
    assert any("State version conflict" in m for m in error_messages), (
        f"expected 'State version conflict' in error log, got: {error_messages}"
    )
    assert any("deployment-identity" in m for m in error_messages), (
        f"expected 'deployment-identity' in error log, got: {error_messages}"
    )


@pytest.mark.asyncio
async def test_non_live_cas_conflict_logs_collision_signal_without_raising(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # runner_state uses structlog; monkeypatch module-level logger (T-3762-3 idiom).
    from almanak.framework.runner import runner_state

    captured_logger = MagicMock()
    monkeypatch.setattr(runner_state, "logger", captured_logger)

    conflict = StateConflictError(deployment_id="d1", expected_version=3, actual_version=5)
    mgr = _state_mgr(save_side_effect=conflict)
    runner = _Runner(state_manager=mgr, config=RunnerConfig(dry_run=True))

    await update_state(runner, "d1", _result())  # must NOT raise

    assert captured_logger.error.called, (
        "expected an ERROR log for CAS conflict, got: "
        + repr(captured_logger.error.call_args_list)
    )
    error_messages = [str(call.args[0]) if call.args else "" for call in captured_logger.error.call_args_list]
    assert any("State version conflict" in m for m in error_messages), (
        f"expected 'State version conflict' in error log, got: {error_messages}"
    )


# =============================================================================
# Loop-level: the strategy_runner call site escalates to ACCOUNTING_FAILED
# =============================================================================


def _make_loop_runner() -> StrategyRunner:
    """Run-loop harness, trimmed copy of test_run_loop_characterization._make_runner."""
    config = RunnerConfig(
        default_interval_seconds=0,
        max_consecutive_errors=10,
        enable_state_persistence=True,
        enable_alerting=False,
    )
    state_mgr = AsyncMock()
    state_mgr.get_accounting_events_sync = MagicMock(return_value=[])
    state_mgr.get_position_events_sync = MagicMock(return_value=[])
    runner = StrategyRunner(
        price_oracle=MagicMock(),
        balance_provider=MagicMock(),
        execution_orchestrator=MagicMock(),
        state_manager=state_mgr,
        alert_manager=MagicMock(),
        config=config,
    )
    runner._register_with_gateway = MagicMock()
    runner._deregister_from_gateway = MagicMock()
    runner._gateway_heartbeat = MagicMock()
    runner._gateway_update_status = MagicMock()
    runner._get_gateway_client = MagicMock(return_value=None)
    runner._recover_incomplete_sessions = AsyncMock(return_value=0)
    runner._lifecycle_write_state = MagicMock()
    runner._lifecycle_heartbeat = MagicMock()
    runner._lifecycle_poll_command = MagicMock(return_value=None)
    runner._lifecycle_handle_stop = MagicMock()
    runner._collect_position_snapshot = MagicMock(return_value=None)
    runner._capture_portfolio_snapshot = AsyncMock(return_value=None)  # snapshot lane succeeds
    runner._alert_accounting_failure = AsyncMock()
    return runner


def _make_loop_strategy() -> MagicMock:
    strategy = MagicMock()
    strategy.deployment_id = "test-strategy"
    strategy.chain = "arbitrum"
    strategy.wallet_address = "0x1234567890abcdef1234567890abcdef12345678"
    strategy.config = {}
    strategy._wallet_activity_provider = None
    del strategy.flush_pending_saves
    return strategy


@pytest.mark.asyncio
async def test_run_loop_state_failure_escalates_to_accounting_failed() -> None:
    runner = _make_loop_runner()
    runner._update_state = AsyncMock(
        side_effect=AccountingPersistenceError(
            AccountingWriteKind.STATE,
            deployment_id="test-strategy",
            cause=RuntimeError("database is locked"),
        )
    )
    strategy = _make_loop_strategy()
    captured: list[IterationResult] = []
    original = IterationResult(
        status=IterationStatus.SUCCESS, deployment_id="test-strategy", duration_ms=10.0
    )
    runner.run_iteration = AsyncMock(return_value=original)

    await asyncio.wait_for(
        runner.run_loop(
            strategy, interval_seconds=0, iteration_callback=captured.append, max_iterations=1
        ),
        timeout=5,
    )

    assert len(captured) == 1
    assert captured[0].status == IterationStatus.ACCOUNTING_FAILED
    assert "state" in (captured[0].error or "")
    assert runner._alert_accounting_failure.await_count == 1
    assert captured[0].timestamp == original.timestamp


@pytest.mark.asyncio
async def test_run_loop_summary_reflects_state_lane_rebuild() -> None:
    """The JSONL iteration summary must report the rebuilt ACCOUNTING_FAILED
    result, not the pre-persistence SUCCESS (issue-#1782 invariant extended
    to the state lane)."""
    runner = _make_loop_runner()
    runner._update_state = AsyncMock(
        side_effect=AccountingPersistenceError(
            AccountingWriteKind.STATE, deployment_id="test-strategy"
        )
    )
    strategy = _make_loop_strategy()
    captured: list[IterationResult] = []
    summary_calls: list[IterationResult] = []
    runner._emit_iteration_summary = MagicMock(
        side_effect=lambda result, chain=None: summary_calls.append(result)
    )
    runner.run_iteration = AsyncMock(
        return_value=IterationResult(
            status=IterationStatus.SUCCESS, deployment_id="test-strategy", duration_ms=10.0
        )
    )

    await asyncio.wait_for(
        runner.run_loop(
            strategy, interval_seconds=0, iteration_callback=captured.append, max_iterations=1
        ),
        timeout=5,
    )

    assert len(summary_calls) == 1
    assert summary_calls[0].status == IterationStatus.ACCOUNTING_FAILED
    assert summary_calls[0] is captured[0]
