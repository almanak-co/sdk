from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.runner.runner_models import ExecutionBarrierPhase, ExecutionProgress, IterationStatus
from almanak.framework.runner.strategy_runner import RunnerConfig, StrategyRunner


def _progress(phase: ExecutionBarrierPhase) -> ExecutionProgress:
    progress = ExecutionProgress(
        execution_id="execution-1",
        deployment_id="deployment-1",
        intents_hash="sealed-plan",
        total_steps=1,
        barrier_phase=phase,
    )
    if phase is ExecutionBarrierPhase.RECONCILIATION_REQUIRED:
        progress.reconciliation_required_step_index = 0
    elif phase is ExecutionBarrierPhase.LANDED_REPAIR_PENDING:
        progress.accounting_pending_step_index = 0
    elif phase in {ExecutionBarrierPhase.RETRYABLE, ExecutionBarrierPhase.RECOMPILE_REQUIRED}:
        progress.failed_at_step_index = 0
    elif phase is ExecutionBarrierPhase.COMPLETED:
        progress.completed_step_index = 0
    return progress


def _runner(saved_progress: object | None) -> StrategyRunner:
    runner = StrategyRunner(
        price_oracle=MagicMock(),
        balance_provider=MagicMock(),
        execution_orchestrator=MagicMock(),
        state_manager=MagicMock(),
        config=RunnerConfig(enable_state_persistence=False, enable_alerting=False),
    )
    runner._load_execution_progress = AsyncMock(return_value=saved_progress)  # type: ignore[method-assign]
    runner._record_failure = MagicMock()  # type: ignore[method-assign]
    return runner


@pytest.mark.asyncio
@pytest.mark.parametrize("saved_progress", [None, _progress(ExecutionBarrierPhase.COMPLETED)])
async def test_absent_or_completed_progress_allows_decide(saved_progress: ExecutionProgress | None) -> None:
    runner = _runner(saved_progress)

    result = await runner._check_and_resume_stuck_execution(
        SimpleNamespace(deployment_id="deployment-1"),
        datetime.now(UTC),
    )

    assert result is None
    runner._load_execution_progress.assert_awaited_once_with("deployment-1")
    runner._record_failure.assert_not_called()


@pytest.mark.asyncio
async def test_reconciliation_marker_fails_closed_with_stable_default_error() -> None:
    runner = _runner(_progress(ExecutionBarrierPhase.RECONCILIATION_REQUIRED))

    result = await runner._check_and_resume_stuck_execution(
        SimpleNamespace(deployment_id="deployment-1"),
        datetime.now(UTC),
    )

    assert result is not None
    assert result.status is IterationStatus.EXECUTION_FAILED
    assert result.error == (
        "BROADCAST_RECONCILIATION_REQUIRED: submitted transaction hashes must be reconciled before execution can resume"
    )
    runner._record_failure.assert_called_once_with()


@pytest.mark.asyncio
async def test_landed_repair_marker_refuses_replay_with_accounting_status() -> None:
    runner = _runner(_progress(ExecutionBarrierPhase.LANDED_REPAIR_PENDING))

    result = await runner._check_and_resume_stuck_execution(
        SimpleNamespace(deployment_id="deployment-1"),
        datetime.now(UTC),
    )

    assert result is not None
    assert result.status is IterationStatus.ACCOUNTING_FAILED
    assert result.error == (
        "LANDED_REPAIR_PENDING: transaction landed but accounting, callback, or strategy-state repair "
        "has not been durably sealed"
    )
    runner._record_failure.assert_called_once_with()


@pytest.mark.asyncio
async def test_recompile_marker_requires_a_fresh_snapshot() -> None:
    runner = _runner(_progress(ExecutionBarrierPhase.RECOMPILE_REQUIRED))

    result = await runner._check_and_resume_stuck_execution(
        SimpleNamespace(deployment_id="deployment-1"),
        datetime.now(UTC),
    )

    assert result is not None
    assert result.status is IterationStatus.EXECUTION_FAILED
    assert result.error == "RECOMPILE_REQUIRED: recovery needs a fresh market snapshot; refusing pre-snapshot execution"


@pytest.mark.asyncio
async def test_recompile_marker_is_forwarded_to_the_snapshot_gated_lane() -> None:
    progress = _progress(ExecutionBarrierPhase.RECOMPILE_REQUIRED)
    runner = _runner(progress)
    iteration_state = SimpleNamespace(recompile_progress=None)

    result = await runner._check_and_resume_stuck_execution(
        SimpleNamespace(deployment_id="deployment-1"),
        datetime.now(UTC),
        iteration_state=iteration_state,
    )

    assert result is None
    assert iteration_state.recompile_progress is progress
    runner._record_failure.assert_not_called()


@pytest.mark.asyncio
async def test_legacy_retry_marker_refuses_automatic_replay() -> None:
    runner = _runner(_progress(ExecutionBarrierPhase.RETRYABLE))

    result = await runner._check_and_resume_stuck_execution(
        SimpleNamespace(deployment_id="deployment-1"),
        datetime.now(UTC),
    )

    assert result is not None
    assert result.status is IterationStatus.EXECUTION_FAILED
    assert result.error == (
        "BROADCAST_RECONCILIATION_REQUIRED: legacy retry marker lacks plan-bound "
        "fresh-compile evidence; refusing automatic replay"
    )
    runner._record_failure.assert_called_once_with()


@pytest.mark.asyncio
async def test_unknown_stuck_phase_raises_instead_of_replaying() -> None:
    unknown_phase = object()
    runner = _runner(
        SimpleNamespace(
            is_stuck=True,
            is_reconciliation_required=False,
            is_accounting_pending=False,
            effective_barrier_phase=unknown_phase,
        )
    )

    with pytest.raises(RuntimeError, match="unsupported execution barrier phase"):
        await runner._check_and_resume_stuck_execution(
            SimpleNamespace(deployment_id="deployment-1"),
            datetime.now(UTC),
        )
