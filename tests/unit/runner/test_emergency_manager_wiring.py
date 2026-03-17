"""Tests for EmergencyManager wiring into StrategyRunner (VIB-1256).

Verifies that:
- Runner accepts optional emergency_manager
- Emergency stop auto-triggers when circuit breaker trips to OPEN
- Emergency fires only once per OPEN episode (not every iteration)
- Emergency not triggered without circuit breaker
- Emergency not triggered when breaker stays CLOSED
- Strategy context (chain, error details) passed to emergency_stop_async
"""

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
    IterationStatus,
    RunnerConfig,
    StrategyRunner,
)


# =============================================================================
# Helpers
# =============================================================================

_PAUSE_PATCH = "almanak.framework.runner.strategy_runner.StrategyRunner._is_strategy_paused"
_TEARDOWN_PATCH = "almanak.framework.runner.strategy_runner.StrategyRunner._check_teardown_requested"


def _make_strategy(decide_side_effect=None):
    """Create a mock strategy."""
    strategy = MagicMock()
    strategy.strategy_id = "test_strategy"
    strategy.chain = "arbitrum"
    strategy.wallet_address = "0x1234567890abcdef1234567890abcdef12345678"
    strategy.create_market_snapshot.return_value = MagicMock()
    strategy.generate_teardown_intents.side_effect = NotImplementedError

    if decide_side_effect is not None:
        strategy.decide.side_effect = decide_side_effect
    else:
        strategy.decide.side_effect = RuntimeError("execution failed")

    return strategy


def _make_breaker(max_failures=3):
    """Create a circuit breaker with configurable threshold."""
    config = CircuitBreakerConfig(
        max_consecutive_failures=max_failures,
        max_cumulative_loss_usd=Decimal("1000"),
        cooldown_seconds=3600,
    )
    return CircuitBreaker(strategy_id="test_strategy", config=config)


def _make_runner(circuit_breaker=None, emergency_manager=None):
    """Create a StrategyRunner with optional components."""
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
        emergency_manager=emergency_manager,
    )


# =============================================================================
# Tests: Init
# =============================================================================


class TestEmergencyManagerInit:
    def test_runner_accepts_emergency_manager(self):
        em = MagicMock()
        runner = _make_runner(emergency_manager=em)
        assert runner._emergency_manager is em

    def test_runner_works_without_emergency_manager(self):
        runner = _make_runner()
        assert runner._emergency_manager is None


# =============================================================================
# Tests: Emergency trigger on circuit breaker OPEN
# =============================================================================


class TestEmergencyTrigger:
    @pytest.mark.asyncio
    @patch(_TEARDOWN_PATCH, return_value=None)
    @patch(_PAUSE_PATCH, new_callable=AsyncMock, return_value=(False, None))
    async def test_emergency_triggered_when_breaker_trips(self, _mock_pause, _mock_teardown):
        """Emergency stop should fire when circuit breaker transitions to OPEN."""
        breaker = _make_breaker(max_failures=3)
        em = MagicMock()
        em.emergency_stop_async = AsyncMock()

        runner = _make_runner(circuit_breaker=breaker, emergency_manager=em)
        strategy = _make_strategy()

        # 3 failures will trip the breaker
        await runner.run_loop(strategy, max_iterations=3)

        assert breaker.state == CircuitBreakerState.OPEN
        em.emergency_stop_async.assert_called_once()

    @pytest.mark.asyncio
    @patch(_TEARDOWN_PATCH, return_value=None)
    @patch(_PAUSE_PATCH, new_callable=AsyncMock, return_value=(False, None))
    async def test_emergency_receives_strategy_context(self, _mock_pause, _mock_teardown):
        """Emergency stop should receive strategy_id, chain, and error details."""
        breaker = _make_breaker(max_failures=2)
        em = MagicMock()
        em.emergency_stop_async = AsyncMock()

        runner = _make_runner(circuit_breaker=breaker, emergency_manager=em)
        strategy = _make_strategy(decide_side_effect=RuntimeError("slippage too high"))

        await runner.run_loop(strategy, max_iterations=2)

        call_kwargs = em.emergency_stop_async.call_args[1]
        assert call_kwargs["strategy_id"] == "test_strategy"
        assert call_kwargs["chain"] == "arbitrum"
        assert "slippage too high" in call_kwargs["reason"]
        assert call_kwargs["trigger_context"]["consecutive_failures"] == 2

    @pytest.mark.asyncio
    @patch(_TEARDOWN_PATCH, return_value=None)
    @patch(_PAUSE_PATCH, new_callable=AsyncMock, return_value=(False, None))
    async def test_emergency_fires_only_once_per_open_episode(self, _mock_pause, _mock_teardown):
        """Emergency stop should fire only once, not on every subsequent OPEN iteration."""
        breaker = _make_breaker(max_failures=2)
        em = MagicMock()
        em.emergency_stop_async = AsyncMock()

        runner = _make_runner(circuit_breaker=breaker, emergency_manager=em)
        strategy = _make_strategy()

        # 4 iterations: 2 to trip, 2 more while OPEN
        await runner.run_loop(strategy, max_iterations=4)

        # Should only be called once, not on every iteration after OPEN
        em.emergency_stop_async.assert_called_once()

    @pytest.mark.asyncio
    @patch(_TEARDOWN_PATCH, return_value=None)
    @patch(_PAUSE_PATCH, new_callable=AsyncMock, return_value=(False, None))
    async def test_no_emergency_without_breaker(self, _mock_pause, _mock_teardown):
        """No emergency trigger if circuit breaker not configured."""
        em = MagicMock()
        em.emergency_stop_async = AsyncMock()

        runner = _make_runner(emergency_manager=em)
        strategy = _make_strategy()

        await runner.run_loop(strategy, max_iterations=3)

        em.emergency_stop_async.assert_not_called()

    @pytest.mark.asyncio
    @patch(_TEARDOWN_PATCH, return_value=None)
    @patch(_PAUSE_PATCH, new_callable=AsyncMock, return_value=(False, None))
    async def test_no_emergency_without_manager(self, _mock_pause, _mock_teardown):
        """No crash if emergency_manager is None when breaker trips."""
        breaker = _make_breaker(max_failures=2)
        runner = _make_runner(circuit_breaker=breaker)
        strategy = _make_strategy()

        # Should not raise even though breaker trips
        await runner.run_loop(strategy, max_iterations=2)
        assert breaker.state == CircuitBreakerState.OPEN

    @pytest.mark.asyncio
    @patch(_TEARDOWN_PATCH, return_value=None)
    @patch(_PAUSE_PATCH, new_callable=AsyncMock, return_value=(False, None))
    async def test_no_emergency_when_breaker_stays_closed(self, _mock_pause, _mock_teardown):
        """No emergency when failures are below threshold."""
        breaker = _make_breaker(max_failures=10)
        em = MagicMock()
        em.emergency_stop_async = AsyncMock()

        runner = _make_runner(circuit_breaker=breaker, emergency_manager=em)
        strategy = _make_strategy()

        await runner.run_loop(strategy, max_iterations=3)

        assert breaker.state == CircuitBreakerState.CLOSED
        em.emergency_stop_async.assert_not_called()

    @pytest.mark.asyncio
    @patch(_TEARDOWN_PATCH, return_value=None)
    @patch(_PAUSE_PATCH, new_callable=AsyncMock, return_value=(False, None))
    async def test_emergency_error_does_not_crash_runner(self, _mock_pause, _mock_teardown):
        """If emergency_stop_async raises, the runner should continue gracefully."""
        breaker = _make_breaker(max_failures=2)
        em = MagicMock()
        em.emergency_stop_async = AsyncMock(side_effect=RuntimeError("alert service down"))

        runner = _make_runner(circuit_breaker=breaker, emergency_manager=em)
        strategy = _make_strategy()

        # Should not raise despite emergency manager failure
        await runner.run_loop(strategy, max_iterations=3)
        assert breaker.state == CircuitBreakerState.OPEN


# =============================================================================
# Tests: Emergency with STRATEGY_TIMEOUT
# =============================================================================


class TestEmergencyWithTimeout:
    @pytest.mark.asyncio
    @patch(_TEARDOWN_PATCH, return_value=None)
    @patch(_PAUSE_PATCH, new_callable=AsyncMock, return_value=(False, None))
    async def test_timeout_triggers_emergency_when_breaker_trips(self, _mock_pause, _mock_teardown):
        """STRATEGY_TIMEOUT failures that trip the breaker should also trigger emergency."""
        import time

        breaker = _make_breaker(max_failures=2)
        em = MagicMock()
        em.emergency_stop_async = AsyncMock()

        config = RunnerConfig(
            default_interval_seconds=1,
            enable_state_persistence=False,
            enable_alerting=False,
            dry_run=False,
            decide_timeout_seconds=0.1,
        )
        runner = StrategyRunner(
            price_oracle=MagicMock(),
            balance_provider=MagicMock(),
            execution_orchestrator=MagicMock(),
            state_manager=MagicMock(),
            config=config,
            circuit_breaker=breaker,
            emergency_manager=em,
        )

        def slow_decide(market):
            time.sleep(5)
            return HoldIntent(reason="never reached")

        strategy = _make_strategy(decide_side_effect=slow_decide)

        await runner.run_loop(strategy, max_iterations=2)

        assert breaker.state == CircuitBreakerState.OPEN
        em.emergency_stop_async.assert_called_once()
        # Reason may be the original timeout or the overlap guard message
        reason = em.emergency_stop_async.call_args[1]["reason"]
        assert "timed" in reason.lower()
