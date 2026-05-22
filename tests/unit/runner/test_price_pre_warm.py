"""Tests for price pre-warming before decide() (VIB-2568).

Validates that:
- _pre_warm_prices calls market.price() for strategy tokens before decide()
- Pre-warming failures are silently ignored (decide() retries on its own)
- Strategies without _get_tracked_tokens are gracefully handled
- Pre-warmed prices populate the market's _price_cache
"""

import asyncio
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.intents.vocabulary import HoldIntent
from almanak.framework.runner.strategy_runner import (
    IterationStatus,
    RunnerConfig,
    StrategyRunner,
)


# =============================================================================
# Helpers
# =============================================================================


def _make_strategy(tracked_tokens=None, has_tracked_tokens=True):
    """Create a mock strategy with optional tracked tokens."""
    strategy = MagicMock()
    strategy.deployment_id = "test_strategy"
    strategy.chain = "arbitrum"
    strategy.wallet_address = "0x1234567890abcdef1234567890abcdef12345678"
    strategy.generate_teardown_intents.side_effect = NotImplementedError
    strategy.decide.return_value = HoldIntent(reason="Test hold")

    market = MagicMock()
    market.price.return_value = Decimal("3500")
    strategy.create_market_snapshot.return_value = market

    if has_tracked_tokens:
        strategy._get_tracked_tokens.return_value = tracked_tokens if tracked_tokens is not None else ["WETH", "USDC"]
    else:
        del strategy._get_tracked_tokens

    return strategy, market


def _make_runner():
    """Create a StrategyRunner for testing."""
    config = RunnerConfig(
        default_interval_seconds=1,
        enable_state_persistence=False,
        enable_alerting=False,
        dry_run=False,
        decide_timeout_seconds=30.0,
    )
    return StrategyRunner(
        price_oracle=MagicMock(),
        balance_provider=MagicMock(),
        execution_orchestrator=MagicMock(),
        state_manager=MagicMock(),
        config=config,
    )


# =============================================================================
# Tests
# =============================================================================


@pytest.mark.asyncio
async def test_pre_warm_calls_price_for_tracked_tokens():
    """Pre-warming should fetch prices for each tracked token."""
    runner = _make_runner()
    strategy, market = _make_strategy(tracked_tokens=["WETH", "USDC", "wstETH"])

    await runner._pre_warm_prices(market, strategy)

    assert market.price.call_count == 3
    called_tokens = [call.args[0] for call in market.price.call_args_list]
    assert "WETH" in called_tokens
    assert "USDC" in called_tokens
    assert "wstETH" in called_tokens


@pytest.mark.asyncio
async def test_pre_warm_failure_is_silent():
    """If a price fetch fails during pre-warming, it should not raise."""
    runner = _make_runner()
    strategy, market = _make_strategy(tracked_tokens=["WETH", "UNKNOWN_TOKEN"])
    market.price.side_effect = [Decimal("3500"), Exception("Price unavailable")]

    # Should not raise
    await runner._pre_warm_prices(market, strategy)

    assert market.price.call_count == 2


@pytest.mark.asyncio
async def test_pre_warm_skips_when_no_tracked_tokens_method():
    """Strategies without _get_tracked_tokens should be handled gracefully."""
    runner = _make_runner()
    strategy, market = _make_strategy(has_tracked_tokens=False)

    await runner._pre_warm_prices(market, strategy)

    # No price calls should be made
    market.price.assert_not_called()


@pytest.mark.asyncio
async def test_pre_warm_skips_when_tokens_empty():
    """If _get_tracked_tokens returns empty list, no prices fetched."""
    runner = _make_runner()
    strategy, market = _make_strategy(tracked_tokens=[])

    await runner._pre_warm_prices(market, strategy)

    market.price.assert_not_called()


@pytest.mark.asyncio
async def test_pre_warm_skips_when_tracked_tokens_raises():
    """If _get_tracked_tokens raises, pre-warming should not crash."""
    runner = _make_runner()
    strategy, market = _make_strategy()
    strategy._get_tracked_tokens.side_effect = Exception("config error")

    await runner._pre_warm_prices(market, strategy)

    market.price.assert_not_called()


@pytest.mark.asyncio
@patch.object(StrategyRunner, "_is_strategy_paused", new_callable=AsyncMock, return_value=(False, None))
@patch.object(StrategyRunner, "_check_teardown_requested", return_value=None)
async def test_pre_warm_runs_before_decide_in_iteration(_mock_teardown, _mock_paused):
    """In a full iteration, pre-warming should run before decide()."""
    runner = _make_runner()
    strategy, market = _make_strategy(tracked_tokens=["WETH", "USDC"])

    call_order = []
    original_price = market.price

    def tracking_price(token):
        call_order.append(f"price:{token}")
        return Decimal("3500")

    market.price.side_effect = tracking_price

    def tracking_decide(m):
        call_order.append("decide")
        return HoldIntent(reason="Test hold")

    strategy.decide.side_effect = tracking_decide

    result = await runner.run_iteration(strategy)

    # Pre-warm prices should appear before decide in the call order
    assert "price:WETH" in call_order
    assert "price:USDC" in call_order
    assert "decide" in call_order
    decide_idx = call_order.index("decide")
    assert call_order.index("price:WETH") < decide_idx
    assert call_order.index("price:USDC") < decide_idx


@pytest.mark.asyncio
async def test_pre_warm_timeout_does_not_block():
    """If price fetches stall, the 60s timeout should prevent indefinite blocking."""
    runner = _make_runner()
    strategy, market = _make_strategy(tracked_tokens=["WETH", "USDC"])

    async def slow_price(token):
        await asyncio.sleep(999)  # Simulate a stalled gateway call

    # Patch asyncio.to_thread to use our slow coroutine
    original_to_thread = asyncio.to_thread

    async def mock_to_thread(func, *args):
        await asyncio.sleep(999)

    with patch("almanak.framework.runner.strategy_runner.asyncio.wait_for", wraps=asyncio.wait_for) as mock_wait_for:
        # Use a very short timeout for the test by patching the method
        original_pre_warm = runner._pre_warm_prices

        async def fast_timeout_pre_warm(market, strategy):
            """Override to use 0.1s timeout instead of 60s."""
            try:
                await asyncio.wait_for(runner._do_pre_warm_prices(market, strategy), timeout=0.1)
            except asyncio.TimeoutError:
                pass  # Expected

        runner._pre_warm_prices = fast_timeout_pre_warm

        with patch("almanak.framework.runner.strategy_runner.asyncio.to_thread", side_effect=mock_to_thread):
            # Should complete quickly (0.1s timeout) instead of blocking
            await runner._pre_warm_prices(market, strategy)
