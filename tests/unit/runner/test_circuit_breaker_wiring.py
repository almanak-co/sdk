"""Tests for circuit breaker wiring into StrategyRunner.

Verifies that the CircuitBreaker is properly integrated into the runner's
execution path: check() before execution, record_success() on success,
record_failure() on failure, and CIRCUIT_BREAKER_OPEN status when blocked.
"""

from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.execution.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitBreakerState,
)
from almanak.framework.intents.vocabulary import HoldIntent, SwapIntent
from almanak.framework.runner.strategy_runner import (
    IterationStatus,
    RunnerConfig,
    StrategyRunner,
)


# =============================================================================
# Fixtures
# =============================================================================


def _make_strategy(decide_return=None):
    """Create a mock strategy that passes all runner internal checks."""
    strategy = MagicMock()
    strategy.strategy_id = "test_strategy"
    strategy.chain = "arbitrum"
    strategy.wallet_address = "0x1234567890abcdef1234567890abcdef12345678"
    strategy.create_market_snapshot.return_value = MagicMock()

    # Return HoldIntent by default
    if decide_return is None:
        decide_return = HoldIntent(reason="Test hold")
    strategy.decide.return_value = decide_return

    # Teardown: strategy doesn't support teardown
    strategy.supports_teardown.return_value = False
    strategy.generate_teardown_intents.side_effect = NotImplementedError

    return strategy


def _make_runner(circuit_breaker=None):
    """Create a StrategyRunner with common mocks."""
    config = RunnerConfig(
        default_interval_seconds=1,
        enable_state_persistence=False,
        enable_alerting=False,
        dry_run=False,
    )
    runner = StrategyRunner(
        price_oracle=MagicMock(),
        balance_provider=MagicMock(),
        execution_orchestrator=MagicMock(),
        state_manager=MagicMock(),
        config=config,
        circuit_breaker=circuit_breaker,
    )
    return runner


def _make_breaker():
    """Create a circuit breaker with short thresholds for testing."""
    config = CircuitBreakerConfig(
        max_consecutive_failures=3,
        max_cumulative_loss_usd=Decimal("1000"),
        cooldown_seconds=2,
    )
    return CircuitBreaker(strategy_id="test_strategy", config=config)


# =============================================================================
# Patch helper: skip internal runner checks that need real async services
# =============================================================================

# The runner checks for operator pause and teardown requests before decide().
# In unit tests we patch these to skip them since they need a real gateway.
_PAUSE_PATCH = "almanak.framework.runner.strategy_runner.StrategyRunner._is_strategy_paused"
_TEARDOWN_PATCH = "almanak.framework.runner.strategy_runner.StrategyRunner._check_teardown_requested"


# =============================================================================
# Tests: Circuit Breaker Init
# =============================================================================


class TestCircuitBreakerInit:
    def test_runner_accepts_circuit_breaker(self):
        breaker = _make_breaker()
        runner = _make_runner(circuit_breaker=breaker)
        assert runner._circuit_breaker is breaker

    def test_runner_works_without_circuit_breaker(self):
        runner = _make_runner(circuit_breaker=None)
        assert runner._circuit_breaker is None


# =============================================================================
# Tests: record_success
# =============================================================================


class TestCircuitBreakerRecordSuccess:
    def test_execution_proved_resets_failures(self):
        """Only execution_proved=True should reset circuit breaker failures."""
        breaker = _make_breaker()
        runner = _make_runner(circuit_breaker=breaker)

        breaker.record_failure("fail 1")
        breaker.record_failure("fail 2")
        assert breaker._consecutive_failures == 2

        runner._record_success(execution_proved=True)
        assert breaker._consecutive_failures == 0

    def test_hold_success_does_not_reset_failures(self):
        """HOLD (no execution_proved) should not reset circuit breaker failures."""
        breaker = _make_breaker()
        runner = _make_runner(circuit_breaker=breaker)

        breaker.record_failure("fail 1")
        breaker.record_failure("fail 2")
        assert breaker._consecutive_failures == 2

        runner._record_success()  # Default: execution_proved=False
        assert breaker._consecutive_failures == 2

    def test_record_success_without_breaker(self):
        runner = _make_runner(circuit_breaker=None)
        runner._record_success()  # Should not raise
        assert runner._successful_iterations == 1


# =============================================================================
# Tests: record_failure via run_loop
# =============================================================================


class TestCircuitBreakerRecordFailure:
    @pytest.mark.asyncio
    @patch(_TEARDOWN_PATCH, return_value=None)
    @patch(_PAUSE_PATCH, new_callable=AsyncMock, return_value=(False, None))
    async def test_failure_recorded_in_breaker(self, mock_pause, mock_teardown):
        breaker = _make_breaker()
        runner = _make_runner(circuit_breaker=breaker)
        strategy = _make_strategy()
        strategy.decide.side_effect = RuntimeError("boom")

        await runner.run_loop(strategy, max_iterations=1)
        assert breaker._consecutive_failures == 1

    @pytest.mark.asyncio
    @patch(_TEARDOWN_PATCH, return_value=None)
    @patch(_PAUSE_PATCH, new_callable=AsyncMock, return_value=(False, None))
    async def test_three_failures_trip_breaker(self, mock_pause, mock_teardown):
        breaker = _make_breaker()
        runner = _make_runner(circuit_breaker=breaker)
        strategy = _make_strategy()
        strategy.decide.side_effect = RuntimeError("persistent failure")

        await runner.run_loop(strategy, max_iterations=3)
        assert breaker.state == CircuitBreakerState.OPEN
        assert breaker._consecutive_failures == 3


# =============================================================================
# Tests: Circuit breaker blocks execution
# =============================================================================


class TestCircuitBreakerBlocksExecution:
    @pytest.mark.asyncio
    @patch(_TEARDOWN_PATCH, return_value=None)
    @patch(_PAUSE_PATCH, new_callable=AsyncMock, return_value=(False, None))
    async def test_open_breaker_returns_circuit_breaker_open(self, mock_pause, mock_teardown):
        breaker = _make_breaker()
        runner = _make_runner(circuit_breaker=breaker)
        strategy = _make_strategy(
            decide_return=SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("100"))
        )

        # Trip the breaker
        breaker.record_failure("fail 1")
        breaker.record_failure("fail 2")
        breaker.record_failure("fail 3")
        assert breaker.state == CircuitBreakerState.OPEN

        result = await runner.run_iteration(strategy)
        assert result.status == IterationStatus.CIRCUIT_BREAKER_OPEN
        assert not result.success
        assert "Circuit breaker open" in result.error

    @pytest.mark.asyncio
    @patch(_TEARDOWN_PATCH, return_value=None)
    @patch(_PAUSE_PATCH, new_callable=AsyncMock, return_value=(False, None))
    async def test_open_breaker_blocks_even_hold_strategy(self, mock_pause, mock_teardown):
        """When breaker is OPEN, all execution is blocked -- even strategies that
        would return HOLD. The check runs before decide() is called."""
        breaker = _make_breaker()
        runner = _make_runner(circuit_breaker=breaker)
        strategy = _make_strategy(decide_return=HoldIntent(reason="Market uncertain"))

        # Trip the breaker
        breaker.record_failure("fail 1")
        breaker.record_failure("fail 2")
        breaker.record_failure("fail 3")

        result = await runner.run_iteration(strategy)
        assert result.status == IterationStatus.CIRCUIT_BREAKER_OPEN
        assert not result.success
        strategy.decide.assert_not_called()

    @pytest.mark.asyncio
    @patch(_TEARDOWN_PATCH, return_value=None)
    @patch(_PAUSE_PATCH, new_callable=AsyncMock, return_value=(False, None))
    async def test_paused_breaker_blocks_execution(self, mock_pause, mock_teardown):
        breaker = _make_breaker()
        runner = _make_runner(circuit_breaker=breaker)
        strategy = _make_strategy(
            decide_return=SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("100"))
        )

        breaker.pause(reason="Investigating", operator="test@test.com")

        result = await runner.run_iteration(strategy)
        assert result.status == IterationStatus.CIRCUIT_BREAKER_OPEN
        assert not result.success

    @pytest.mark.asyncio
    @patch(_TEARDOWN_PATCH, return_value=None)
    @patch(_PAUSE_PATCH, new_callable=AsyncMock, return_value=(False, None))
    async def test_closed_breaker_allows_execution(self, mock_pause, mock_teardown):
        breaker = _make_breaker()
        runner = _make_runner(circuit_breaker=breaker)
        strategy = _make_strategy(decide_return=HoldIntent(reason="All good"))

        result = await runner.run_iteration(strategy)
        assert result.status == IterationStatus.HOLD
        assert result.success


# =============================================================================
# Tests: CIRCUIT_BREAKER_OPEN not double-counted as failure
# =============================================================================


class TestCircuitBreakerDoubleCount:
    @pytest.mark.asyncio
    @patch(_TEARDOWN_PATCH, return_value=None)
    @patch(_PAUSE_PATCH, new_callable=AsyncMock, return_value=(False, None))
    async def test_cb_open_not_recorded_as_failure(self, mock_pause, mock_teardown):
        """CIRCUIT_BREAKER_OPEN should not increment the failure counter."""
        breaker = _make_breaker()
        runner = _make_runner(circuit_breaker=breaker)
        strategy = _make_strategy(
            decide_return=SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("100"))
        )

        # Trip breaker (3 failures)
        breaker.record_failure("fail 1")
        breaker.record_failure("fail 2")
        breaker.record_failure("fail 3")
        assert breaker._consecutive_failures == 3

        # Run iteration — gets CIRCUIT_BREAKER_OPEN
        result = await runner.run_iteration(strategy)
        assert result.status == IterationStatus.CIRCUIT_BREAKER_OPEN

        # Run through loop to test the run_loop exclusion logic
        await runner.run_loop(strategy, max_iterations=1)

        # Failures should still be 3, not incremented
        assert breaker._consecutive_failures == 3


# =============================================================================
# Tests: Error message content
# =============================================================================


class TestCircuitBreakerErrorContent:
    @pytest.mark.asyncio
    @patch(_TEARDOWN_PATCH, return_value=None)
    @patch(_PAUSE_PATCH, new_callable=AsyncMock, return_value=(False, None))
    async def test_error_contains_reason(self, mock_pause, mock_teardown):
        breaker = _make_breaker()
        runner = _make_runner(circuit_breaker=breaker)
        strategy = _make_strategy(
            decide_return=SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("100"))
        )

        breaker.record_failure("tx reverted")
        breaker.record_failure("tx reverted")
        breaker.record_failure("tx reverted")

        result = await runner.run_iteration(strategy)
        assert result.status == IterationStatus.CIRCUIT_BREAKER_OPEN
        assert "Circuit breaker open" in result.error
