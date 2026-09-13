"""Unresolved submissions retain replay protection without emergency teardown.

Classification, lifecycle writes and process actions are independent assertions.
Method-level doubles cannot establish actual process liveness or whether an
operator must redeploy. Characterization of ERROR writes is not acceptance of
an unresolved-state/resume contract.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
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
from almanak.framework.runner.runner_models import IterationResult, IterationStatus, RunnerConfig

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


def _runner(
    breaker: CircuitBreaker, *, max_consecutive_errors: int = RunnerConfig.max_consecutive_errors
) -> SimpleNamespace:
    """Runner double using the production error budget."""
    return SimpleNamespace(
        _consecutive_errors=0,
        _first_error_at=None,
        _circuit_breaker=breaker,
        config=SimpleNamespace(max_consecutive_errors=max_consecutive_errors),
        _maybe_trigger_emergency=AsyncMock(),
        _alert_consecutive_errors=AsyncMock(),
        _alert_execution_pending=AsyncMock(),
        _execution_pending_alert_at=None,
        _lifecycle_write_state=MagicMock(),
        request_shutdown=MagicMock(),
    )


def _error_lifecycle_writes(runner: SimpleNamespace) -> int:
    from almanak.core.lifecycle import LifecycleState

    return sum(1 for call in runner._lifecycle_write_state.call_args_list if LifecycleState.ERROR in call.args)


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
        assert runner._consecutive_errors == 0
        assert runner._first_error_at is None
        runner._alert_consecutive_errors.assert_not_awaited()
        runner._maybe_trigger_emergency.assert_not_awaited()

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
        runner._last_emergency_open_episode = None
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
        assert result.execution_pending_since == progress.started_at
        assert result.execution_pending_reason is not None
        if lane != "single_chain":
            assert result.execution_pending_reason == (
                "This execution lane has no automatic receipt recovery; operator reconciliation required"
            )
        bookkeeping = _runner(_breaker())
        bookkeeping._consecutive_errors = 2
        for _ in range(RunnerConfig.max_consecutive_errors + 2):
            await handle_iteration_failure(bookkeeping, strategy, strategy.deployment_id, result)
        assert bookkeeping._consecutive_errors == 2
        assert _error_lifecycle_writes(bookkeeping) == 1
        bookkeeping._alert_consecutive_errors.assert_not_awaited()
        bookkeeping._maybe_trigger_emergency.assert_not_awaited()
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


class TestDeploymentLifecycleEscalation:
    """Diagnostic ERROR remains independent of error budgets and shutdown."""

    @pytest.mark.asyncio
    async def test_a_hold_streak_preserves_the_production_error_budget(self) -> None:
        from almanak.framework.runner.runner_models import RunnerConfig

        budget = RunnerConfig.max_consecutive_errors
        breaker = _breaker()
        runner = _runner(breaker, max_consecutive_errors=budget)
        result = IterationResult(status=IterationStatus.EXECUTION_FAILED, error=INCIDENT_ERROR)
        for _ in range(budget):
            await handle_iteration_failure(runner, SimpleNamespace(), "deployment:alm-10096", result)

        assert breaker.state is CircuitBreakerState.CLOSED
        assert breaker.check().can_execute
        assert runner._consecutive_errors == 0
        assert _error_lifecycle_writes(runner) == 1
        assert runner._alert_execution_pending.await_count == 1
        assert "without recovery timing" in runner._lifecycle_write_state.call_args.kwargs["error_message"]
        runner._alert_consecutive_errors.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_lost_recovery_context_reports_error_on_the_first_pending_iteration(self) -> None:
        # What the single-chain barrier returns once the original execution
        # context is gone: the reason is set, so no grace period applies.
        breaker = _breaker()
        runner = _runner(breaker, max_consecutive_errors=3)
        result = IterationResult(
            status=IterationStatus.EXECUTION_PENDING,
            error=INCIDENT_ERROR,
            execution_pending_since=datetime.now(UTC),
            execution_pending_reason="Original recovery context is unavailable; operator reconciliation required",
        )
        await handle_iteration_failure(runner, SimpleNamespace(), "deployment:alm-10096", result)

        assert _error_lifecycle_writes(runner) == 1
        assert runner._alert_execution_pending.await_count == 1
        # The pending lane returns before the streak and the breaker are touched.
        assert runner._consecutive_errors == 0
        assert breaker.state is CircuitBreakerState.CLOSED

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "pending_age_seconds,expected_error_writes",
        [(30, 0), (400, 1)],
    )
    async def test_an_unresolved_submission_escalates_only_after_the_grace_window(
        self, pending_age_seconds: int, expected_error_writes: int
    ) -> None:
        # With a recovery context still held there is a five-minute window in
        # which a receipt may yet arrive; only past it is an operator told.
        breaker = _breaker()
        runner = _runner(breaker, max_consecutive_errors=3)
        result = IterationResult(
            status=IterationStatus.EXECUTION_PENDING,
            error=INCIDENT_ERROR,
            execution_pending_since=datetime.now(UTC) - timedelta(seconds=pending_age_seconds),
            execution_pending_reason=None,
        )
        for _ in range(4):
            await handle_iteration_failure(runner, SimpleNamespace(), "deployment:alm-10096", result)

        assert _error_lifecycle_writes(runner) == expected_error_writes
        assert runner._alert_execution_pending.await_count == expected_error_writes
        assert runner._consecutive_errors == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [IterationStatus.EXECUTION_FAILED, IterationStatus.EXECUTION_PENDING])
@pytest.mark.parametrize("pending_age_seconds", [30, 400])
@pytest.mark.parametrize("operator_reason", [None, "Original recovery context unavailable; reconcile before replay"])
@pytest.mark.parametrize("existing_failures", [0, 2])
@pytest.mark.parametrize("breaker_state", [CircuitBreakerState.CLOSED, CircuitBreakerState.HALF_OPEN])
async def test_pending_lifecycle_and_safety_are_independent(
    status: IterationStatus,
    pending_age_seconds: int,
    operator_reason: str | None,
    existing_failures: int,
    breaker_state: CircuitBreakerState,
) -> None:
    breaker = _breaker()
    for _ in range(existing_failures):
        breaker.record_failure(ORDINARY_FAILURE, kind=FailureKind.UNKNOWN)
    breaker._state = breaker_state
    runner = _runner(breaker)
    runner._consecutive_errors = existing_failures
    first_error = datetime.now(UTC) - timedelta(seconds=600) if existing_failures else None
    runner._first_error_at = first_error
    before = breaker.get_status()
    result = IterationResult(
        status=status,
        error=INCIDENT_ERROR,
        execution_pending_since=datetime.now(UTC) - timedelta(seconds=pending_age_seconds),
        execution_pending_reason=operator_reason,
    )
    strategy = SimpleNamespace()
    for _ in range(RunnerConfig.max_consecutive_errors + 2):
        await handle_iteration_failure(runner, strategy, "deployment:alm-10096", result)

    expected_writes = int(operator_reason is not None or pending_age_seconds >= 300)
    assert _error_lifecycle_writes(runner) == expected_writes
    assert runner._alert_execution_pending.await_count == expected_writes
    if expected_writes:
        assert runner._lifecycle_write_state.call_args.kwargs["error_message"] == (
            operator_reason or "Receipt recovery exceeded five minutes; operator reconciliation required"
        )
        runner._alert_execution_pending.assert_awaited_once_with(strategy, result)
    assert runner._consecutive_errors == existing_failures
    assert runner._first_error_at == first_error
    assert breaker.state is breaker_state
    after = breaker.get_status()
    for key in ("consecutive_failures", "cumulative_loss_usd", "failure_history_count"):
        assert after[key] == before[key]
    runner._maybe_trigger_emergency.assert_not_awaited()
    runner._alert_consecutive_errors.assert_not_awaited()
    runner.request_shutdown.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status",
    [IterationStatus.ACCOUNTING_FAILED, IterationStatus.RECONCILIATION_FAILED, IterationStatus.EXECUTION_FAILED],
)
async def test_typed_hold_requires_unresolved_execution_evidence(status: IterationStatus) -> None:
    runner = _runner(_breaker())
    result = IterationResult(status=status, error=ORDINARY_FAILURE, failure_kind=FailureKind.RECONCILIATION_HOLD)
    for _ in range(RunnerConfig.max_consecutive_errors):
        await handle_iteration_failure(runner, SimpleNamespace(), "deployment:alm-10096", result)
    assert runner._consecutive_errors == RunnerConfig.max_consecutive_errors
    assert runner._circuit_breaker.state is CircuitBreakerState.OPEN
    runner._alert_execution_pending.assert_not_awaited()


@pytest.mark.asyncio
async def test_authoritative_failure_kind_prevents_marker_from_neutralizing_failure() -> None:
    runner = _runner(_breaker())
    result = IterationResult(
        status=IterationStatus.EXECUTION_FAILED, error=INCIDENT_ERROR, failure_kind=FailureKind.UNKNOWN
    )
    await handle_iteration_failure(runner, SimpleNamespace(), "deployment:alm-10096", result)
    assert runner._consecutive_errors == 1
    assert runner._circuit_breaker.get_status()["consecutive_failures"] == 1
    runner._alert_execution_pending.assert_not_awaited()


def test_recovery_does_not_overwrite_terminal_lifecycle() -> None:
    from almanak.core.lifecycle import LifecycleState
    from almanak.framework.runner._run_loop_helpers import handle_iteration_success

    runner = _runner(_breaker())
    runner._execution_pending_alert_at = datetime.now(UTC)
    runner._shutdown_requested = True
    runner._terminal_lifecycle_state = LifecycleState.TERMINATED
    handle_iteration_success(runner, "deployment:alm-10096", False)
    runner._lifecycle_write_state.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("since", [None, datetime(2026, 1, 1, tzinfo=UTC)])
async def test_pending_operator_card_matches_lifecycle_diagnostic(since: datetime | None) -> None:
    from almanak.framework.runner.runner_alerts import RunnerAlerter

    runner = _runner(_breaker())
    runner.config.enable_alerting = True
    runner.alert_manager = SimpleNamespace(send_alert=AsyncMock())
    runner._alert_execution_pending = lambda strategy, result: RunnerAlerter(runner).alert_execution_pending(
        strategy, result
    )
    strategy = SimpleNamespace(deployment_id="deployment:alm-10096")
    result = IterationResult(
        status=IterationStatus.EXECUTION_FAILED, error=INCIDENT_ERROR, execution_pending_since=since
    )
    await handle_iteration_failure(runner, strategy, strategy.deployment_id, result)
    card = runner.alert_manager.send_alert.call_args.args[0]
    assert card.context["reason"] == runner._lifecycle_write_state.call_args.kwargs["error_message"]
    assert card.context["pending_since"] == (since.isoformat() if since else None)
    if since is None:
        assert "without recovery timing" in card.context["reason"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "intervening_status", [None, IterationStatus.EXECUTION_PENDING, IterationStatus.EXECUTION_FAILED]
)
async def test_emergency_runs_once_for_each_open_episode_across_neutral_holds(intervening_status) -> None:
    from almanak.framework.runner.runner_alerts import RunnerAlerter

    breaker = _breaker(max_consecutive_failures=1)
    runner = _runner(breaker)
    runner._emergency_manager = SimpleNamespace(emergency_stop_async=AsyncMock())
    runner._is_managed_deployment = MagicMock(return_value=False)
    runner._maybe_trigger_emergency = lambda strategy, result: RunnerAlerter(runner).maybe_trigger_emergency(
        strategy, result
    )
    strategy = SimpleNamespace(deployment_id="deployment:alm-10096", chain="bsc")
    failed = IterationResult(status=IterationStatus.EXECUTION_FAILED, error=ORDINARY_FAILURE)
    hold = IterationResult(
        status=intervening_status or IterationStatus.EXECUTION_PENDING,
        error=INCIDENT_ERROR,
        execution_pending_since=datetime.now(UTC),
    )
    await handle_iteration_failure(runner, strategy, strategy.deployment_id, failed)
    first_episode = breaker.open_episode
    await handle_iteration_failure(runner, strategy, strategy.deployment_id, failed)
    assert breaker.open_episode == first_episode
    assert runner._emergency_manager.emergency_stop_async.await_count == 1

    breaker._trip_time = datetime.now(UTC) - timedelta(seconds=breaker.config.cooldown_seconds + 1)
    assert breaker.check().state is CircuitBreakerState.HALF_OPEN
    for _ in range(3 if intervening_status else 0):
        await handle_iteration_failure(runner, strategy, strategy.deployment_id, hold)
    assert breaker.state is CircuitBreakerState.HALF_OPEN
    assert runner._emergency_manager.emergency_stop_async.await_count == 1

    await handle_iteration_failure(runner, strategy, strategy.deployment_id, failed)
    assert breaker.open_episode == first_episode + 1
    assert runner._emergency_manager.emergency_stop_async.await_count == 2
    await handle_iteration_failure(runner, strategy, strategy.deployment_id, failed)
    assert runner._emergency_manager.emergency_stop_async.await_count == 2
