"""Regression tests for issue #1665.

``StrategyRunner._step_teardown_and_cb_gate`` must run the multi-chain
stuck-execution resume path BEFORE the circuit-breaker gate. If a
multi-chain strategy has saved mid-sequence progress (e.g. a partially
completed bridge flow) and the breaker is OPEN or PAUSED, the iteration
MUST continue that already-started work instead of stranding it behind
an ``IterationStatus.CIRCUIT_BREAKER_OPEN`` early-return.

Teardowns already bypass the CB gate (``teardown_mode is None`` guard);
resuming saved progress uses the same rationale -- it is continuation
of in-flight work, not new work.

The single-chain path is unchanged: a tripped breaker still short-circuits
to ``IterationStatus.CIRCUIT_BREAKER_OPEN``.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import ANY, AsyncMock, MagicMock, patch

import pytest

from almanak.framework.execution.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitBreakerState,
)
from almanak.framework.execution.multichain import MultiChainOrchestrator
from almanak.framework.execution.submission import SubmissionProvenance
from almanak.framework.intents.vocabulary import HoldIntent, Intent
from almanak.framework.runner.strategy_runner import (
    ExecutionBarrierPhase,
    ExecutionLane,
    ExecutionProgress,
    IterationResult,
    IterationStatus,
    RunIterationState,
    RunnerConfig,
    StrategyRunner,
)
from tests.unit.runner._state_manager import absent_state_manager

# =============================================================================
# Helpers (same mocking shape as test_run_iteration_steps.py)
# =============================================================================


def _make_runner(
    *,
    circuit_breaker: CircuitBreaker | None = None,
) -> StrategyRunner:
    config = RunnerConfig(
        default_interval_seconds=1,
        enable_state_persistence=False,
        enable_alerting=False,
        dry_run=False,
    )
    return StrategyRunner(
        price_oracle=MagicMock(),
        balance_provider=MagicMock(),
        execution_orchestrator=MagicMock(),
        state_manager=absent_state_manager(),
        config=config,
        circuit_breaker=circuit_breaker,
    )


def _make_strategy(deployment_id: str = "test-strategy") -> MagicMock:
    strategy = MagicMock()
    strategy.deployment_id = deployment_id
    strategy.chain = "arbitrum"
    strategy.wallet_address = "0x1234567890abcdef1234567890abcdef12345678"
    strategy.create_market_snapshot.return_value = MagicMock()
    strategy.create_market_snapshot.return_value.has_critical_data_failures.return_value = False
    strategy.decide.return_value = HoldIntent(reason="unit test hold")
    strategy.generate_teardown_intents.side_effect = NotImplementedError
    del strategy._wallet_activity_provider
    return strategy


def _make_state(strategy: MagicMock) -> RunIterationState:
    return RunIterationState(
        strategy=strategy,
        deployment_id=strategy.deployment_id,
        start_time=datetime.now(UTC),
    )


def _tripped_breaker(deployment_id: str = "test-strategy") -> CircuitBreaker:
    breaker = CircuitBreaker(
        deployment_id=deployment_id,
        config=CircuitBreakerConfig(
            max_consecutive_failures=3,
            max_cumulative_loss_usd=Decimal("1000"),
            cooldown_seconds=60,
        ),
    )
    breaker.record_failure("fail 1")
    breaker.record_failure("fail 2")
    breaker.record_failure("fail 3")
    assert breaker.state == CircuitBreakerState.OPEN
    return breaker


# =============================================================================
# Multi-chain: stuck-resume runs BEFORE the CB gate (#1665)
# =============================================================================


class TestMultiChainStuckResumeBeforeCircuitBreaker:
    """When a multi-chain strategy has saved progress, the resume path
    must fire even if the breaker is OPEN/PAUSED. The fix reorders
    ``_step_teardown_and_cb_gate`` so ``_check_and_resume_stuck_execution``
    is evaluated before the circuit-breaker check.
    """

    @pytest.mark.asyncio
    async def test_open_breaker_does_not_block_multi_chain_stuck_resume(self) -> None:
        """OPEN breaker + multi-chain + stuck state -> resume, NOT CB_OPEN."""
        breaker = _tripped_breaker()
        runner = _make_runner(circuit_breaker=breaker)
        runner._is_multi_chain = True
        strategy = _make_strategy()

        resume_result = IterationResult(
            status=IterationStatus.SUCCESS,
            deployment_id=strategy.deployment_id,
            duration_ms=1,
        )

        resume_mock = AsyncMock(return_value=resume_result)
        with (
            patch.object(runner, "_check_teardown_requested", return_value=None),
            patch.object(runner, "_check_and_resume_stuck_execution", new=resume_mock),
        ):
            result = await runner._step_teardown_and_cb_gate(_make_state(strategy))

        # Resume fired and its IterationResult propagated, bypassing the CB gate.
        resume_mock.assert_awaited_once()
        assert result is resume_result
        assert result.status is IterationStatus.SUCCESS
        assert result.status is not IterationStatus.CIRCUIT_BREAKER_OPEN

    @pytest.mark.asyncio
    async def test_landed_prefix_recompile_bypasses_breaker_until_suffix_finishes(self) -> None:
        breaker = _tripped_breaker()
        runner = _make_runner(circuit_breaker=breaker)
        strategy = _make_strategy()
        state = _make_state(strategy)
        state.recompile_progress = ExecutionProgress(
            execution_id="suffix",
            deployment_id=strategy.deployment_id,
            intents_hash="sealed",
            total_steps=2,
            completed_step_index=0,
            failed_at_step_index=1,
            execution_lane=ExecutionLane.SAME_CHAIN_MULTI_LEG,
            barrier_phase=ExecutionBarrierPhase.RECOMPILE_REQUIRED,
        )

        with (
            patch.object(runner, "_check_teardown_requested", return_value=None),
            patch.object(runner, "_check_and_resume_stuck_execution", new=AsyncMock(return_value=None)),
        ):
            result = await runner._step_teardown_and_cb_gate(state)

        assert result is None

    @pytest.mark.asyncio
    async def test_landed_prefix_builds_snapshot_then_resumes_before_periodic_hooks_or_decide(self) -> None:
        runner = _make_runner()
        strategy = _make_strategy()
        progress = ExecutionProgress(
            execution_id="suffix-order",
            deployment_id=strategy.deployment_id,
            intents_hash="sealed",
            total_steps=2,
            completed_step_index=0,
            failed_at_step_index=1,
            execution_lane=ExecutionLane.SAME_CHAIN_MULTI_LEG,
            barrier_phase=ExecutionBarrierPhase.RECOMPILE_REQUIRED,
        )
        events: list[str] = []
        sentinel = IterationResult(status=IterationStatus.SUCCESS, deployment_id=strategy.deployment_id)

        async def _gate(state: RunIterationState):
            state.recompile_progress = progress
            return None

        async def _snapshot(state: RunIterationState):
            events.append("snapshot")
            state.market = object()
            return None

        async def _resume(state: RunIterationState):
            events.append("resume")
            assert state.market is not None
            return sentinel

        with (
            patch.object(runner, "_step_pause_gate", new=AsyncMock(return_value=None)),
            patch.object(runner, "_step_teardown_and_cb_gate", new=AsyncMock(side_effect=_gate)),
            patch.object(runner, "_step_build_snapshot", new=AsyncMock(side_effect=_snapshot)),
            patch.object(runner, "_resume_recompile_required", new=AsyncMock(side_effect=_resume)),
            patch.object(runner, "_step_periodic_hooks", new=AsyncMock()) as periodic,
        ):
            result = await runner.run_iteration(strategy)

        assert result is sentinel
        assert events == ["snapshot", "resume"]
        periodic.assert_not_awaited()
        strategy.decide.assert_not_called()

    @pytest.mark.asyncio
    async def test_bridge_suffix_recovery_uses_sealed_intent_and_current_market(self) -> None:
        runner = _make_runner()
        runner.execution_orchestrator = MagicMock(spec=MultiChainOrchestrator)
        strategy = _make_strategy()
        bridge = Intent.bridge(
            token="USDC",
            amount=Decimal("5"),
            from_chain="base",
            to_chain="arbitrum",
        )
        intents = [HoldIntent(reason="already completed"), bridge]
        progress = ExecutionProgress(
            execution_id="bridge-suffix",
            deployment_id=strategy.deployment_id,
            intents_hash=runner._compute_intents_hash(intents),
            total_steps=2,
            completed_step_index=0,
            serialized_intents=[intent.serialize() for intent in intents],
            failed_at_step_index=1,
            execution_lane=ExecutionLane.BRIDGE,
            barrier_phase=ExecutionBarrierPhase.RECOMPILE_REQUIRED,
        )
        market = object()
        state = _make_state(strategy)
        state.market = market
        state.recompile_progress = progress
        sentinel = IterationResult(status=IterationStatus.SUCCESS, deployment_id=strategy.deployment_id)
        runner._execute_multi_chain = AsyncMock(return_value=sentinel)  # type: ignore[method-assign]

        result = await runner._resume_recompile_required(state)

        assert result is sentinel
        call = runner._execute_multi_chain.await_args.kwargs
        assert call["market"] is market
        assert call["resume_progress"] is progress
        assert call["intents"][1].serialize() == bridge.serialize()

    @pytest.mark.asyncio
    async def test_intermediate_operator_repair_reenters_snapshot_gated_suffix_recompile(self) -> None:
        runner = _make_runner()
        strategy = _make_strategy()
        progress = ExecutionProgress(
            execution_id="repaired-prefix",
            deployment_id=strategy.deployment_id,
            intents_hash="sealed-plan",
            total_steps=2,
            completed_step_index=-1,
            execution_lane=ExecutionLane.SAME_CHAIN_MULTI_LEG,
        )
        progress.mark_landed_repair_pending(0, "callback/state repair required")
        progress.seal_repaired_step(0)
        assert progress.effective_barrier_phase is ExecutionBarrierPhase.RECOMPILE_REQUIRED

        runner._load_execution_progress = AsyncMock(return_value=progress)  # type: ignore[method-assign]
        state = _make_state(strategy)

        assert (
            await runner._check_and_resume_stuck_execution(
                strategy,
                state.start_time,
                iteration_state=state,
            )
            is None
        )
        assert state.recompile_progress is progress

    @pytest.mark.asyncio
    async def test_teardown_bypasses_reconciliation_resume_gate(self) -> None:
        """A pending risk-reduction request runs before a stuck marker gate."""
        runner = _make_runner()
        runner._is_multi_chain = True
        strategy = _make_strategy()
        teardown_mode = MagicMock()
        teardown_result = IterationResult(
            status=IterationStatus.SUCCESS,
            deployment_id=strategy.deployment_id,
            duration_ms=1,
        )
        resume_mock = AsyncMock(
            return_value=IterationResult(
                status=IterationStatus.EXECUTION_FAILED,
                deployment_id=strategy.deployment_id,
                error="BROADCAST_RECONCILIATION_REQUIRED",
                duration_ms=1,
            )
        )
        teardown_mock = AsyncMock(return_value=teardown_result)
        with (
            patch.object(runner, "_check_teardown_requested", return_value=teardown_mode),
            patch.object(runner, "_check_and_resume_stuck_execution", new=resume_mock),
            patch.object(runner, "_execute_teardown", new=teardown_mock),
        ):
            result = await runner._step_teardown_and_cb_gate(_make_state(strategy))

        assert result is teardown_result
        teardown_mock.assert_awaited_once_with(strategy, teardown_mode, ANY)
        resume_mock.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_teardown_stays_pending_while_attempted_bridge_can_deliver(self) -> None:
        runner = _make_runner()
        strategy = _make_strategy()
        progress = ExecutionProgress(
            execution_id="bridge-1",
            deployment_id=strategy.deployment_id,
            intents_hash="bridge-plan",
            total_steps=1,
            reconciliation_required_step_index=0,
            execution_lane=ExecutionLane.BRIDGE,
        )
        progress.record_submission_evidence(
            step_index=0,
            chain="arbitrum",
            submission_provenance=SubmissionProvenance.ATTEMPTED,
            submitted_transaction_ids=["0xsource"],
        )
        runner._load_execution_progress = AsyncMock(return_value=progress)  # type: ignore[method-assign]
        teardown_mode = MagicMock()
        teardown_mock = AsyncMock()

        with (
            patch.object(runner, "_check_teardown_requested", return_value=teardown_mode),
            patch.object(runner, "_execute_teardown", new=teardown_mock),
        ):
            result = await runner._step_teardown_and_cb_gate(_make_state(strategy))

        assert result is not None
        assert result.status is IterationStatus.EXECUTION_FAILED
        assert "in-flight bridge" in (result.error or "")
        teardown_mock.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_not_attempted_bridge_marker_is_released_before_teardown(self) -> None:
        runner = _make_runner()
        progress = ExecutionProgress(
            execution_id="bridge-2",
            deployment_id="test-strategy",
            intents_hash="bridge-plan",
            total_steps=1,
            reconciliation_required_step_index=0,
            execution_lane=ExecutionLane.BRIDGE,
        )
        progress.record_submission_evidence(
            step_index=0,
            chain="arbitrum",
            submission_provenance=SubmissionProvenance.NOT_ATTEMPTED,
            submitted_transaction_ids=[],
        )
        runner._load_execution_progress = AsyncMock(return_value=progress)  # type: ignore[method-assign]
        runner._clear_execution_progress = AsyncMock()  # type: ignore[method-assign]

        assert await runner._teardown_bridge_barrier_error("test-strategy") is None
        runner._clear_execution_progress.assert_awaited_once_with("test-strategy")

    @pytest.mark.asyncio
    async def test_no_prefix_recompile_bridge_marker_is_cas_consumed_before_teardown(self) -> None:
        runner = _make_runner()
        progress = ExecutionProgress(
            execution_id="bridge-recompile",
            deployment_id="test-strategy",
            intents_hash="bridge-plan",
            total_steps=2,
            completed_step_index=-1,
            failed_at_step_index=0,
            execution_lane=ExecutionLane.BRIDGE,
            barrier_phase=ExecutionBarrierPhase.RECOMPILE_REQUIRED,
        )
        runner._load_execution_progress = AsyncMock(return_value=progress)  # type: ignore[method-assign]
        runner._consume_recompile_progress = AsyncMock()  # type: ignore[method-assign]
        runner._clear_execution_progress = AsyncMock()  # type: ignore[method-assign]

        assert await runner._teardown_bridge_barrier_error("test-strategy") is None
        runner._consume_recompile_progress.assert_awaited_once_with(progress)
        runner._clear_execution_progress.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_paused_breaker_does_not_block_multi_chain_stuck_resume(self) -> None:
        """PAUSED breaker + multi-chain + stuck state -> resume, NOT CB_OPEN."""
        breaker = CircuitBreaker(
            deployment_id="test-strategy",
            config=CircuitBreakerConfig(
                max_consecutive_failures=3,
                max_cumulative_loss_usd=Decimal("1000"),
                cooldown_seconds=60,
            ),
        )
        breaker.pause(reason="operator investigating", operator="ops@test.com")
        assert breaker.state == CircuitBreakerState.PAUSED

        runner = _make_runner(circuit_breaker=breaker)
        runner._is_multi_chain = True
        strategy = _make_strategy()

        resume_result = IterationResult(
            status=IterationStatus.SUCCESS,
            deployment_id=strategy.deployment_id,
            duration_ms=1,
        )

        resume_mock = AsyncMock(return_value=resume_result)
        with (
            patch.object(runner, "_check_teardown_requested", return_value=None),
            patch.object(runner, "_check_and_resume_stuck_execution", new=resume_mock),
        ):
            result = await runner._step_teardown_and_cb_gate(_make_state(strategy))

        resume_mock.assert_awaited_once()
        assert result is resume_result
        assert result.status is not IterationStatus.CIRCUIT_BREAKER_OPEN

    @pytest.mark.asyncio
    async def test_open_breaker_blocks_when_no_stuck_progress(self) -> None:
        """Multi-chain, breaker OPEN, but no saved progress -> CB gate still
        short-circuits new work to CIRCUIT_BREAKER_OPEN.
        """
        breaker = _tripped_breaker()
        runner = _make_runner(circuit_breaker=breaker)
        runner._is_multi_chain = True
        strategy = _make_strategy()

        resume_mock = AsyncMock(return_value=None)  # no saved progress
        with (
            patch.object(runner, "_check_teardown_requested", return_value=None),
            patch.object(runner, "_check_and_resume_stuck_execution", new=resume_mock),
        ):
            result = await runner._step_teardown_and_cb_gate(_make_state(strategy))

        resume_mock.assert_awaited_once()  # resume was evaluated first...
        assert result is not None
        assert result.status is IterationStatus.CIRCUIT_BREAKER_OPEN  # ...then CB gate ran
        assert not result.success

    @pytest.mark.asyncio
    async def test_resume_runs_before_breaker_check(self) -> None:
        """Ordering assertion: with both an OPEN breaker and saved progress,
        the resume path is invoked and its result is returned -- the CB
        check on ``self._circuit_breaker.check()`` must not short-circuit
        the iteration first.
        """
        breaker = _tripped_breaker()
        # Spy on breaker.check to confirm it is not what produced the returned result.
        real_check = breaker.check
        check_spy = MagicMock(side_effect=real_check)
        breaker.check = check_spy  # type: ignore[method-assign]

        runner = _make_runner(circuit_breaker=breaker)
        runner._is_multi_chain = True
        strategy = _make_strategy()

        resume_result = IterationResult(
            status=IterationStatus.SUCCESS,
            deployment_id=strategy.deployment_id,
            duration_ms=1,
        )
        resume_mock = AsyncMock(return_value=resume_result)

        with (
            patch.object(runner, "_check_teardown_requested", return_value=None),
            patch.object(runner, "_check_and_resume_stuck_execution", new=resume_mock),
        ):
            result = await runner._step_teardown_and_cb_gate(_make_state(strategy))

        resume_mock.assert_awaited_once()
        assert result is resume_result
        # Because resume short-circuited, the CB gate's check() must not have run.
        check_spy.assert_not_called()


# =============================================================================
# Single-chain: CB gate unchanged
# =============================================================================


class TestSingleChainCircuitBreakerUnchanged:
    @pytest.mark.asyncio
    async def test_tripped_breaker_still_blocks_single_chain(self) -> None:
        """Single-chain runner: OPEN breaker still returns CIRCUIT_BREAKER_OPEN.
        The multi-chain-only resume reorder must not affect this path.
        """
        breaker = _tripped_breaker()
        runner = _make_runner(circuit_breaker=breaker)
        assert runner._is_multi_chain is False  # default: MagicMock orchestrator
        strategy = _make_strategy()

        # The shared pre-decide gate now inspects terminal reconciliation
        # markers on single-chain runners too; with no marker, the OPEN breaker
        # still blocks new work exactly as before.
        resume_mock = AsyncMock(return_value=None)
        with (
            patch.object(runner, "_check_teardown_requested", return_value=None),
            patch.object(runner, "_check_and_resume_stuck_execution", new=resume_mock),
        ):
            result = await runner._step_teardown_and_cb_gate(_make_state(strategy))

        resume_mock.assert_awaited_once()
        assert result is not None
        assert result.status is IterationStatus.CIRCUIT_BREAKER_OPEN
        assert not result.success

    @pytest.mark.asyncio
    async def test_closed_breaker_single_chain_passes_through(self) -> None:
        """Sanity: single-chain + closed breaker still returns None (proceed)."""
        runner = _make_runner()
        strategy = _make_strategy()

        with patch.object(runner, "_check_teardown_requested", return_value=None):
            result = await runner._step_teardown_and_cb_gate(_make_state(strategy))

        assert result is None
