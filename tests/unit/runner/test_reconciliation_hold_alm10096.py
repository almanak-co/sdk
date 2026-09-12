"""ALM-10096: an unresolved submission must never emergency-stop a deployment.

An accepted broadcast whose receipt never becomes observable is an *unresolved*
outcome, not a failed one. The durable replay barrier correctly refuses
automatic replay — a null receipt is not proof of nonexecution, and the
transaction may still mine. What must not follow is the breaker treating that
fail-closed hold as an execution fault and tripping into an emergency stop,
because the deployment then needs a manual redeploy while its funds are
untouched.

``ExecutionLane.SINGLE_CHAIN`` already returns ``EXECUTION_PENDING`` and is
breaker-neutral. The classification seam under test here is lane-independent:
the same evidence reached through the multi-leg, bridge, or legacy/unknown-lane
markers is charged to the breaker instead.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.execution.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitBreakerState,
)
from almanak.framework.execution.reconciliation import reconciliation_required_error
from almanak.framework.runner._run_loop_helpers import handle_iteration_failure
from almanak.framework.runner.failure_kind import FailureKind, kind_for_status
from almanak.framework.runner.runner_models import IterationResult, IterationStatus

# The verbatim production error shape: a real hash was returned by
# ``eth_sendRawTransaction`` and no receipt ever became observable.
INCIDENT_HASH = "0xbde5c7dab07cd25c5cfc2ea019f7129d76d1e3d0aeb1132a2befec8d11d4562c"
INCIDENT_ERROR = reconciliation_required_error(
    SimpleNamespace(
        success=False,
        tx_hashes=[INCIDENT_HASH],
        receipts=[],
        error=f"transaction result 0 ({INCIDENT_HASH[2:]}) has no receipt",
    )
)

# Multi-leg and bridge lanes build their hold error from the same stable prefix
# but never carry a hash: the marker is written *before* the leg broadcasts, so
# a crash in that window leaves an unresolved submission with no identity.
MULTI_LEG_ERROR = (
    "[bsc] BROADCAST_RECONCILIATION_REQUIRED: multi-leg outcome is not durably sealed; "
    "operator reconciliation is required before replay"
)
BRIDGE_RESUME_ERROR = (
    "BROADCAST_RECONCILIATION_REQUIRED: submitted transaction hashes must be reconciled before execution can resume"
)
ORDINARY_FAILURE = "execution reverted: STF"

# The single-chain landed-repair marker reuses the barrier prefix for an
# outcome that RESOLVED: the transaction mined and only its accounting is
# unsealed. Verbatim from ``_single_chain_seal_broadcast_marker``; the live
# producer is exercised in ``TestLandedRepairIsNotAHold``.
LANDED_REPAIR_ERROR = (
    "BROADCAST_RECONCILIATION_REQUIRED: transaction landed but durable accounting and "
    "strategy state are not sealed; operator reconciliation is required before replay"
)


def _breaker(max_consecutive_failures: int = 3) -> CircuitBreaker:
    return CircuitBreaker(
        "deployment:alm-10096",
        CircuitBreakerConfig(max_consecutive_failures=max_consecutive_failures),
    )


def _runner(breaker: CircuitBreaker) -> SimpleNamespace:
    return SimpleNamespace(
        _consecutive_errors=0,
        _first_error_at=None,
        _circuit_breaker=breaker,
        config=SimpleNamespace(max_consecutive_errors=99),
        _maybe_trigger_emergency=AsyncMock(),
        _alert_consecutive_errors=AsyncMock(),
        _alert_execution_pending=AsyncMock(),
        _lifecycle_write_state=MagicMock(),
    )


class TestHoldClassification:
    """``kind_for_status`` is the single seam every returned failure passes."""

    @pytest.mark.parametrize("error", [INCIDENT_ERROR, MULTI_LEG_ERROR, BRIDGE_RESUME_ERROR])
    def test_reconciliation_error_is_an_execution_hold(self, error: str) -> None:
        kind = kind_for_status(IterationStatus.EXECUTION_FAILED, error)
        assert kind is FailureKind.RECONCILIATION_HOLD
        assert kind.is_execution_hold

    def test_hold_is_neither_data_class_nor_a_guard_refusal(self) -> None:
        # A hold is not a data outage (it must not inherit the elevated
        # data-class budget) and not a safety success (a transaction really
        # was broadcast and may still mine).
        assert not FailureKind.RECONCILIATION_HOLD.is_data_class
        assert not FailureKind.RECONCILIATION_HOLD.is_guard_refusal

    @pytest.mark.parametrize(
        "status,error",
        [
            (IterationStatus.EXECUTION_FAILED, ORDINARY_FAILURE),
            (IterationStatus.EXECUTION_FAILED, None),
            (IterationStatus.ACCOUNTING_FAILED, "LANDED_REPAIR_PENDING: accounting write not sealed"),
            (IterationStatus.ACCOUNTING_FAILED, LANDED_REPAIR_ERROR),
            (IterationStatus.RECONCILIATION_FAILED, "balance reconciliation incident"),
            (IterationStatus.RECONCILIATION_FAILED, INCIDENT_ERROR),
            (IterationStatus.STRATEGY_ERROR, MULTI_LEG_ERROR),
        ],
    )
    def test_ordinary_failures_keep_action_class_semantics(self, status: IterationStatus, error: str | None) -> None:
        # Success-path guard: widening the hold classification must not sweep
        # in a landed-but-unreconciled incident or a plain revert. The barrier
        # token is not sufficient on its own. The ACCOUNTING_FAILED and
        # STRATEGY_ERROR rows are pairs production really emits; the
        # RECONCILIATION_FAILED one is defensive — that producer builds its own
        # balance-discrepancy message and has no path to the token today.
        assert kind_for_status(status, error) is FailureKind.UNKNOWN

    def test_data_outage_classification_is_unchanged(self) -> None:
        assert kind_for_status(IterationStatus.DATA_ERROR, "pool quiet") is FailureKind.DATA_UNAVAILABLE
        assert (
            kind_for_status(IterationStatus.DATA_ERROR, "unknown token classification=permanent") is FailureKind.UNKNOWN
        )


class TestBreakerNeutrality:
    def test_hold_streak_never_trips(self) -> None:
        breaker = _breaker()
        for _ in range(10):
            breaker.record_failure(INCIDENT_ERROR, kind=FailureKind.RECONCILIATION_HOLD)
        assert breaker.state is CircuitBreakerState.CLOSED
        assert breaker.check().can_execute
        assert breaker.consecutive_reconciliation_holds == 10

    def test_hold_neither_counts_nor_clears_a_real_failure_streak(self) -> None:
        # Negative control for the fix itself: a hold in the middle of a real
        # action-failure streak must not reset it into an infinite reprieve.
        breaker = _breaker()
        breaker.record_failure(ORDINARY_FAILURE, kind=FailureKind.UNKNOWN)
        breaker.record_failure(ORDINARY_FAILURE, kind=FailureKind.UNKNOWN)
        breaker.record_failure(INCIDENT_ERROR, kind=FailureKind.RECONCILIATION_HOLD)
        assert breaker.state is CircuitBreakerState.CLOSED
        breaker.record_failure(ORDINARY_FAILURE, kind=FailureKind.UNKNOWN)
        assert breaker.state is CircuitBreakerState.OPEN

    def test_hold_records_no_loss_and_no_failure_history(self) -> None:
        breaker = _breaker()
        breaker.record_failure(INCIDENT_ERROR, kind=FailureKind.RECONCILIATION_HOLD, loss_usd=Decimal("25"))
        status = breaker.get_status()
        assert status["cumulative_loss_usd"] == "0"
        assert status["consecutive_failures"] == 0
        assert status["failure_history_count"] == 0
        assert status["consecutive_reconciliation_holds"] == 1

    def test_real_failure_ends_a_hold_streak(self) -> None:
        breaker = _breaker()
        breaker.record_failure(INCIDENT_ERROR, kind=FailureKind.RECONCILIATION_HOLD)
        breaker.record_failure(ORDINARY_FAILURE, kind=FailureKind.UNKNOWN)
        assert breaker.consecutive_reconciliation_holds == 0

    def test_half_open_is_left_untouched_by_a_hold(self) -> None:
        # A hold proves nothing about whether execution works, so it must
        # neither re-trip a probing breaker nor count as a half-open success.
        breaker = _breaker(max_consecutive_failures=1)
        breaker.record_failure(ORDINARY_FAILURE, kind=FailureKind.UNKNOWN)
        assert breaker.state is CircuitBreakerState.OPEN
        breaker._state = CircuitBreakerState.HALF_OPEN
        breaker.record_failure(INCIDENT_ERROR, kind=FailureKind.RECONCILIATION_HOLD)
        assert breaker.state is CircuitBreakerState.HALF_OPEN


class TestRunnerSeam:
    """End-to-end over the returned-failure seam with a real breaker."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("error", [INCIDENT_ERROR, MULTI_LEG_ERROR, BRIDGE_RESUME_ERROR])
    async def test_unresolved_submission_never_emergency_stops(self, error: str) -> None:
        breaker = _breaker()
        runner = _runner(breaker)
        result = IterationResult(status=IterationStatus.EXECUTION_FAILED, error=error)
        for _ in range(5):
            await handle_iteration_failure(runner, SimpleNamespace(), "deployment:alm-10096", result)
        assert breaker.state is CircuitBreakerState.CLOSED
        assert breaker.check().can_execute
        # The iteration still failed, so the error streak — the operator-facing
        # loud signal — keeps running. Only the emergency stop is withheld.
        assert runner._consecutive_errors == 5
        assert isinstance(runner._first_error_at, datetime)

    @pytest.mark.asyncio
    async def test_ordinary_execution_failure_still_trips_and_escalates(self) -> None:
        breaker = _breaker()
        runner = _runner(breaker)
        result = IterationResult(status=IterationStatus.EXECUTION_FAILED, error=ORDINARY_FAILURE)
        for _ in range(3):
            await handle_iteration_failure(runner, SimpleNamespace(), "deployment:alm-10096", result)
        assert breaker.state is CircuitBreakerState.OPEN
        assert runner._maybe_trigger_emergency.await_count == 3

    @pytest.mark.asyncio
    async def test_landed_but_unsealed_accounting_still_trips(self) -> None:
        # The money already moved and the books are wrong. Carrying the barrier
        # token must not buy a reprieve from the action-class threshold.
        breaker = _breaker()
        runner = _runner(breaker)
        result = IterationResult(status=IterationStatus.ACCOUNTING_FAILED, error=LANDED_REPAIR_ERROR)
        for _ in range(3):
            await handle_iteration_failure(runner, SimpleNamespace(), "deployment:alm-10096", result)
        assert breaker.state is CircuitBreakerState.OPEN
        assert breaker.get_status()["consecutive_failures"] == 3
        assert breaker.consecutive_reconciliation_holds == 0

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "error,expect_stop",
        [(INCIDENT_ERROR, False), (ORDINARY_FAILURE, True)],
    )
    async def test_managed_deployment_is_not_killed_while_a_submission_is_unresolved(
        self, error: str, expect_stop: bool
    ) -> None:
        """The full incident chain: trip -> emergency unwind -> pod exit.

        A managed deployment that trips on a non-data kind writes a terminal
        ERROR and shuts the process down, which is the manual redeployment the
        report describes. Worse, the emergency unwind would broadcast
        risk-reducing transactions while the original submission's outcome is
        still unknown.
        """
        from almanak.core.lifecycle import LifecycleState
        from almanak.framework.runner.runner_alerts import RunnerAlerter

        breaker = _breaker()
        emergency_manager = SimpleNamespace(emergency_stop_async=AsyncMock())
        runner = _runner(breaker)
        runner._emergency_manager = emergency_manager
        runner._emergency_triggered_for_open = False
        runner._is_managed_deployment = MagicMock(return_value=True)
        runner._terminal_lifecycle_state = None
        runner._terminal_lifecycle_error_message = None
        runner.request_shutdown = MagicMock()
        strategy = SimpleNamespace(deployment_id="deployment:alm-10096", chain="bsc")
        runner._maybe_trigger_emergency = lambda s, r: RunnerAlerter(runner).maybe_trigger_emergency(s, r)

        result = IterationResult(status=IterationStatus.EXECUTION_FAILED, error=error)
        for _ in range(4):
            await handle_iteration_failure(runner, strategy, "deployment:alm-10096", result)

        assert emergency_manager.emergency_stop_async.await_count == (1 if expect_stop else 0)
        assert runner.request_shutdown.call_count == (1 if expect_stop else 0)
        assert runner._terminal_lifecycle_state is (LifecycleState.ERROR if expect_stop else None)

    @pytest.mark.asyncio
    async def test_pipeline_typed_kind_still_wins_over_the_error_text(self) -> None:
        breaker = _breaker()
        runner = _runner(breaker)
        result = IterationResult(
            status=IterationStatus.EXECUTION_FAILED,
            error=ORDINARY_FAILURE,
            failure_kind=FailureKind.GUARD_REFUSED,
        )
        await handle_iteration_failure(runner, SimpleNamespace(), "deployment:alm-10096", result)
        assert breaker.consecutive_guard_refusals == 1
        assert breaker.consecutive_reconciliation_holds == 0


class TestReplayBarrierHolds:
    """The hold classification must not touch the no-rebroadcast barrier."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "lane,expected_status",
        [
            ("single_chain", IterationStatus.EXECUTION_PENDING),
            ("same_chain_multi_leg", IterationStatus.EXECUTION_FAILED),
            ("bridge", IterationStatus.EXECUTION_FAILED),
            ("unknown", IterationStatus.EXECUTION_FAILED),
        ],
    )
    async def test_stuck_reconciliation_marker_refuses_replay_in_every_lane(
        self, lane: str, expected_status: IterationStatus
    ) -> None:
        from almanak.framework.runner.runner_models import (
            ExecutionBarrierPhase,
            ExecutionLane,
            ExecutionProgress,
        )
        from almanak.framework.runner.strategy_runner import StrategyRunner

        progress = ExecutionProgress(
            execution_id="exec-10096",
            deployment_id="deployment:alm-10096",
            intents_hash="hash",
            total_steps=1,
            execution_lane=ExecutionLane.parse(lane),
            barrier_phase=ExecutionBarrierPhase.RECONCILIATION_REQUIRED,
            reconciliation_required_step_index=0,
            failure_error=INCIDENT_ERROR,
        )
        runner = MagicMock(spec=StrategyRunner)
        runner._load_execution_progress = AsyncMock(return_value=progress)
        runner._calculate_duration_ms = MagicMock(return_value=1)
        runner._record_failure = MagicMock()
        runner._total_iterations = 0
        orchestrator = MagicMock()
        orchestrator.execute = AsyncMock()
        runner.execution_orchestrator = orchestrator
        strategy = SimpleNamespace(deployment_id="deployment:alm-10096")

        result = await StrategyRunner._check_and_resume_stuck_execution(runner, strategy, datetime.now(UTC), None)

        assert result is not None
        assert result.status is expected_status
        assert "BROADCAST_RECONCILIATION_REQUIRED" in (result.error or "")
        assert INCIDENT_HASH in (result.error or "")
        # No replay: the gate returns before any compile or broadcast.
        orchestrator.execute.assert_not_awaited()
        # And whatever the lane, the breaker must read it as a hold.
        assert kind_for_status(result.status, result.error).is_execution_hold or (
            result.status is IterationStatus.EXECUTION_PENDING
        )


class TestLandedRepairIsNotAHold:
    """The producer and the recovery path that make the token ambiguous."""

    @pytest.mark.asyncio
    async def test_production_landed_repair_marker_resumes_as_an_action_failure(self) -> None:
        from almanak.framework.runner.runner_models import (
            ExecutionBarrierPhase,
            ExecutionLane,
            ExecutionProgress,
        )
        from almanak.framework.runner.strategy_runner import StrategyRunner

        marker = ExecutionProgress(
            execution_id="intent-10096",
            deployment_id="deployment:alm-10096",
            intents_hash="broadcast-pending",
            total_steps=1,
            failure_error=INCIDENT_ERROR,
            reconciliation_required_step_index=0,
            execution_lane=ExecutionLane.SINGLE_CHAIN,
            barrier_phase=ExecutionBarrierPhase.PRE_BROADCAST,
        )
        sealer = MagicMock(spec=StrategyRunner)
        sealer._save_execution_progress = AsyncMock()
        landed = SimpleNamespace(
            success=True,
            execution_id=INCIDENT_HASH,
            execution_plan_hash="plan",
            submission_transactions=[],
            tx_hashes=[INCIDENT_HASH],
            receipts=[{"status": 1}],
            error=None,
        )
        await StrategyRunner._single_chain_seal_broadcast_marker(
            sealer,
            deployment_id="deployment:alm-10096",
            marker=marker,
            execution_result=landed,
            submitted_hashes=(INCIDENT_HASH,),
            reconciliation_error=None,
        )
        # The producer really does reuse the barrier token on a landed receipt:
        # that ambiguity is why the status, not the text, decides the kind.
        assert marker.is_accounting_pending
        assert marker.failure_error == LANDED_REPAIR_ERROR

        resumer = MagicMock(spec=StrategyRunner)
        resumer._load_execution_progress = AsyncMock(return_value=marker)
        resumer._calculate_duration_ms = MagicMock(return_value=1)
        resumer._record_failure = MagicMock()
        resumer._total_iterations = 0
        orchestrator = MagicMock()
        orchestrator.execute = AsyncMock()
        resumer.execution_orchestrator = orchestrator
        strategy = SimpleNamespace(deployment_id="deployment:alm-10096")

        result = await StrategyRunner._check_and_resume_stuck_execution(resumer, strategy, datetime.now(UTC), None)

        assert result is not None
        assert result.status is IterationStatus.ACCOUNTING_FAILED
        assert kind_for_status(result.status, result.error) is FailureKind.UNKNOWN
        orchestrator.execute.assert_not_awaited()

        breaker = _breaker()
        runner = _runner(breaker)
        for _ in range(3):
            await handle_iteration_failure(runner, strategy, "deployment:alm-10096", result)
        assert breaker.state is CircuitBreakerState.OPEN
