"""Tests for strategy.decide() hard timeout (VIB-1253).

Verifies that:
- decide() is wrapped with asyncio.wait_for timeout
- Timeout produces STRATEGY_TIMEOUT status
- Timeout records a circuit breaker failure
- Normal decide() calls still work within the timeout
- Configurable timeout via RunnerConfig.decide_timeout_seconds
"""

import asyncio
import time
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.execution.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
)
from almanak.framework.intents.vocabulary import HoldIntent, SwapIntent
from almanak.framework.runner.strategy_runner import (
    IterationStatus,
    RunnerConfig,
    StrategyRunner,
)


# =============================================================================
# Helpers
# =============================================================================


def _make_strategy(decide_return=None, decide_side_effect=None):
    """Create a mock strategy."""
    strategy = MagicMock()
    strategy.strategy_id = "test_strategy"
    strategy.chain = "arbitrum"
    strategy.wallet_address = "0x1234567890abcdef1234567890abcdef12345678"
    strategy.create_market_snapshot.return_value = MagicMock()
    strategy.generate_teardown_intents.side_effect = NotImplementedError

    if decide_side_effect is not None:
        strategy.decide.side_effect = decide_side_effect
    elif decide_return is None:
        strategy.decide.return_value = HoldIntent(reason="Test hold")
    else:
        strategy.decide.return_value = decide_return

    return strategy


def _make_runner(circuit_breaker=None, decide_timeout=None):
    """Create a StrategyRunner with optional timeout config."""
    config_kwargs = {
        "default_interval_seconds": 1,
        "enable_state_persistence": False,
        "enable_alerting": False,
        "dry_run": False,
    }
    if decide_timeout is not None:
        config_kwargs["decide_timeout_seconds"] = decide_timeout
    config = RunnerConfig(**config_kwargs)
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
    """Create a circuit breaker with short thresholds."""
    config = CircuitBreakerConfig(
        max_consecutive_failures=3,
        max_cumulative_loss_usd=Decimal("1000"),
        cooldown_seconds=2,
    )
    return CircuitBreaker(strategy_id="test_strategy", config=config)


# =============================================================================
# Tests
# =============================================================================


@pytest.mark.asyncio
@patch.object(StrategyRunner, "_is_strategy_paused", new_callable=AsyncMock, return_value=(False, None))
@patch.object(StrategyRunner, "_check_teardown_requested", return_value=None)
async def test_decide_timeout_returns_strategy_timeout_status(_mock_teardown, _mock_paused):
    """A hanging decide() should return STRATEGY_TIMEOUT after the configured timeout."""

    def slow_decide(market):
        time.sleep(5)  # blocks longer than timeout
        return HoldIntent(reason="never reached")

    strategy = _make_strategy(decide_side_effect=slow_decide)
    runner = _make_runner(decide_timeout=0.1)  # 100ms timeout

    result = await runner.run_iteration(strategy)

    assert result.status == IterationStatus.STRATEGY_TIMEOUT
    assert "timed out" in result.error


@pytest.mark.asyncio
@patch.object(StrategyRunner, "_is_strategy_paused", new_callable=AsyncMock, return_value=(False, None))
@patch.object(StrategyRunner, "_check_teardown_requested", return_value=None)
async def test_decide_timeout_records_circuit_breaker_failure(_mock_teardown, _mock_paused):
    """Timeout should record a failure in the circuit breaker."""
    breaker = _make_breaker()

    def slow_decide(market):
        time.sleep(5)
        return HoldIntent(reason="never reached")

    strategy = _make_strategy(decide_side_effect=slow_decide)
    runner = _make_runner(circuit_breaker=breaker, decide_timeout=0.1)

    result = await runner.run_iteration(strategy)

    assert result.status == IterationStatus.STRATEGY_TIMEOUT
    # Breaker should have recorded one failure
    check = breaker.check()
    assert check.consecutive_failures == 1


@pytest.mark.asyncio
@patch.object(StrategyRunner, "_is_strategy_paused", new_callable=AsyncMock, return_value=(False, None))
@patch.object(StrategyRunner, "_check_teardown_requested", return_value=None)
async def test_decide_within_timeout_succeeds(_mock_teardown, _mock_paused):
    """A fast decide() should work normally within the timeout."""
    strategy = _make_strategy(decide_return=HoldIntent(reason="Fast decision"))
    runner = _make_runner(decide_timeout=5.0)

    result = await runner.run_iteration(strategy)

    assert result.status == IterationStatus.HOLD
    assert result.error is None


@pytest.mark.asyncio
@patch.object(StrategyRunner, "_is_strategy_paused", new_callable=AsyncMock, return_value=(False, None))
@patch.object(StrategyRunner, "_check_teardown_requested", return_value=None)
async def test_decide_exception_still_returns_strategy_error(_mock_teardown, _mock_paused):
    """A decide() that raises should still return STRATEGY_ERROR, not TIMEOUT."""

    def bad_decide(market):
        raise ValueError("strategy bug")

    strategy = _make_strategy(decide_side_effect=bad_decide)
    runner = _make_runner(decide_timeout=5.0)

    result = await runner.run_iteration(strategy)

    assert result.status == IterationStatus.STRATEGY_ERROR
    assert "strategy bug" in result.error


@pytest.mark.asyncio
@patch.object(StrategyRunner, "_is_strategy_paused", new_callable=AsyncMock, return_value=(False, None))
@patch.object(StrategyRunner, "_check_teardown_requested", return_value=None)
async def test_default_timeout_is_30_seconds(_mock_teardown, _mock_paused):
    """Default RunnerConfig should have 30s decide timeout."""
    config = RunnerConfig()
    assert config.decide_timeout_seconds == 30.0


@pytest.mark.asyncio
@patch.object(StrategyRunner, "_is_strategy_paused", new_callable=AsyncMock, return_value=(False, None))
@patch.object(StrategyRunner, "_check_teardown_requested", return_value=None)
async def test_timeout_not_double_counted_in_circuit_breaker(_mock_teardown, _mock_paused):
    """STRATEGY_TIMEOUT should not be double-counted via run_loop's failure handler.

    The timeout handler records the failure inline, so run_loop should skip it.
    We verify this by checking the breaker sees exactly 1 failure after a timeout iteration.
    """
    breaker = _make_breaker()

    def slow_decide(market):
        time.sleep(5)
        return HoldIntent(reason="never reached")

    strategy = _make_strategy(decide_side_effect=slow_decide)
    runner = _make_runner(circuit_breaker=breaker, decide_timeout=0.1)

    # Exercise the real run_loop path to verify no double-counting
    await runner.run_loop(strategy, max_iterations=1)

    # Should be exactly 1 failure (from the timeout handler), not 2
    check = breaker.check()
    assert check.consecutive_failures == 1


@pytest.mark.asyncio
@patch.object(StrategyRunner, "_is_strategy_paused", new_callable=AsyncMock, return_value=(False, None))
@patch.object(StrategyRunner, "_check_teardown_requested", return_value=None)
async def test_three_timeouts_trip_circuit_breaker(_mock_teardown, _mock_paused):
    """Three consecutive timeouts should trip the circuit breaker to OPEN."""
    breaker = _make_breaker()

    def slow_decide(market):
        time.sleep(5)
        return HoldIntent(reason="never reached")

    strategy = _make_strategy(decide_side_effect=slow_decide)
    runner = _make_runner(circuit_breaker=breaker, decide_timeout=0.1)

    for _ in range(3):
        result = await runner.run_iteration(strategy)
        assert result.status == IterationStatus.STRATEGY_TIMEOUT

    # Breaker should be OPEN now
    from almanak.framework.execution.circuit_breaker import CircuitBreakerState

    check = breaker.check()
    assert check.state == CircuitBreakerState.OPEN
    assert not check.can_execute


@pytest.mark.asyncio
@patch.object(StrategyRunner, "_is_strategy_paused", new_callable=AsyncMock, return_value=(False, None))
@patch.object(StrategyRunner, "_check_teardown_requested", return_value=None)
async def test_timeout_error_message_includes_duration(_mock_teardown, _mock_paused):
    """Timeout error message should state the timeout duration."""

    def slow_decide(market):
        time.sleep(5)
        return HoldIntent(reason="never reached")

    strategy = _make_strategy(decide_side_effect=slow_decide)
    runner = _make_runner(decide_timeout=0.2)

    result = await runner.run_iteration(strategy)

    assert result.status == IterationStatus.STRATEGY_TIMEOUT
    assert "0.2s" in result.error


@pytest.mark.asyncio
@patch.object(StrategyRunner, "_is_strategy_paused", new_callable=AsyncMock, return_value=(False, None))
@patch.object(StrategyRunner, "_check_teardown_requested", return_value=None)
async def test_timeout_recovery_after_2x_timeout_elapsed(_mock_teardown, _mock_paused):
    """After a timeout, the runner should recover once 2x timeout has elapsed.

    This prevents the permanent-brick bug where _decide_in_progress stays True
    forever after a single timeout, permanently disabling the runner.
    """
    timeout_seconds = 0.1

    call_count = 0

    def slow_then_fast_decide(market):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            time.sleep(5)  # First call hangs (will be timed out)
            return HoldIntent(reason="never reached")
        return HoldIntent(reason="recovered")

    strategy = _make_strategy(decide_side_effect=slow_then_fast_decide)
    runner = _make_runner(decide_timeout=timeout_seconds)

    # First call: times out, sets _decide_in_progress = True
    result1 = await runner.run_iteration(strategy)
    assert result1.status == IterationStatus.STRATEGY_TIMEOUT
    assert runner._decide_in_progress is True

    # Simulate 2x timeout having elapsed by backdating the timestamp
    import time as time_mod

    runner._decide_timed_out_at = time_mod.monotonic() - (3 * timeout_seconds)

    # Second call: guard should reset, decide() should succeed
    result2 = await runner.run_iteration(strategy)
    assert result2.status == IterationStatus.HOLD
    assert runner._decide_in_progress is False
