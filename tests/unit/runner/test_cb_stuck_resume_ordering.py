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
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.execution.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitBreakerState,
)
from almanak.framework.intents.vocabulary import HoldIntent
from almanak.framework.runner.strategy_runner import (
    IterationResult,
    IterationStatus,
    RunIterationState,
    RunnerConfig,
    StrategyRunner,
)


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
        state_manager=MagicMock(),
        config=config,
        circuit_breaker=circuit_breaker,
    )


def _make_strategy(strategy_id: str = "test-strategy") -> MagicMock:
    strategy = MagicMock()
    strategy.strategy_id = strategy_id
    strategy.chain = "arbitrum"
    strategy.wallet_address = "0x1234567890abcdef1234567890abcdef12345678"
    strategy.create_market_snapshot.return_value = MagicMock()
    strategy.decide.return_value = HoldIntent(reason="unit test hold")
    strategy.generate_teardown_intents.side_effect = NotImplementedError
    del strategy._wallet_activity_provider
    return strategy


def _make_state(strategy: MagicMock) -> RunIterationState:
    return RunIterationState(
        strategy=strategy,
        strategy_id=strategy.strategy_id,
        start_time=datetime.now(UTC),
    )


def _tripped_breaker(strategy_id: str = "test-strategy") -> CircuitBreaker:
    breaker = CircuitBreaker(
        strategy_id=strategy_id,
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
            strategy_id=strategy.strategy_id,
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
    async def test_paused_breaker_does_not_block_multi_chain_stuck_resume(self) -> None:
        """PAUSED breaker + multi-chain + stuck state -> resume, NOT CB_OPEN."""
        breaker = CircuitBreaker(
            strategy_id="test-strategy",
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
            strategy_id=strategy.strategy_id,
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
            strategy_id=strategy.strategy_id,
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

        # Resume is gated on _is_multi_chain -- patch it anyway to prove
        # it is NOT invoked on the single-chain path.
        resume_mock = AsyncMock(return_value=None)
        with (
            patch.object(runner, "_check_teardown_requested", return_value=None),
            patch.object(runner, "_check_and_resume_stuck_execution", new=resume_mock),
        ):
            result = await runner._step_teardown_and_cb_gate(_make_state(strategy))

        resume_mock.assert_not_awaited()
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
