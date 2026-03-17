"""Tests for TeardownManager price threading.

Validates that:
- execute() passes market to generate_teardown_intents
- execute() applies prices to compiler during intent execution
- execute() without market uses placeholder prices (graceful fallback)
- resume() applies prices when regenerating stale intents
- TypeError fallback only catches market-keyword errors, not real bugs
"""

import json
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.teardown.teardown_manager import TeardownManager
from almanak.framework.teardown.models import (
    TeardownMode,
    TeardownPositionSummary,
    TeardownState,
    TeardownStatus,
)
from almanak.framework.teardown.config import TeardownConfig


def _make_strategy(intents=None):
    """Create a mock strategy."""
    strategy = MagicMock()
    strategy.strategy_id = "test_strat"
    strategy.name = "Test Strategy"
    strategy.chain = "arbitrum"
    strategy.uses_safe_wallet = False
    strategy.pause = AsyncMock()

    positions = MagicMock(spec=TeardownPositionSummary)
    positions.positions = []
    positions.total_value_usd = Decimal("10000")
    positions.has_liquidation_risk = False
    positions.chains_involved = {"arbitrum"}
    strategy.get_open_positions.return_value = positions

    if intents is not None:
        strategy.generate_teardown_intents.return_value = intents
    else:
        strategy.generate_teardown_intents.return_value = []

    return strategy


def _make_market(prices=None):
    """Create a mock market snapshot."""
    market = MagicMock()
    market.get_price_oracle_dict.return_value = prices or {"ETH": Decimal("3400"), "USDC": Decimal("1")}
    return market


@pytest.mark.asyncio
async def test_execute_passes_market_to_generate_intents():
    """market is passed to generate_teardown_intents."""
    market = _make_market()
    strategy = _make_strategy(intents=[])

    manager = TeardownManager()

    await manager.execute(strategy=strategy, mode="graceful", market=market)

    strategy.generate_teardown_intents.assert_called_once_with(TeardownMode.SOFT, market=market)


@pytest.mark.asyncio
async def test_execute_applies_prices_to_compiler():
    """compiler.price_oracle is set from market during intent execution."""
    market = _make_market({"ETH": Decimal("3400")})

    # Build an intent mock without max_slippage attribute so slippage
    # cloning is skipped and we can focus on price application
    intent = MagicMock()
    intent.intent_type = "SWAP"
    intent.chain = "arbitrum"
    intent.to_dict.return_value = {"type": "swap"}
    del intent.max_slippage  # remove so hasattr returns False
    strategy = _make_strategy(intents=[intent])

    compiler = MagicMock()
    compiler.price_oracle = None
    compiler._using_placeholders = True

    # Make update_prices/restore_prices functional on the mock
    def _update_prices(prices):
        compiler.price_oracle = prices
        compiler._using_placeholders = False

    def _restore_prices(oracle, placeholders):
        compiler.price_oracle = oracle
        compiler._using_placeholders = placeholders

    compiler.update_prices = _update_prices
    compiler.restore_prices = _restore_prices

    # Track prices during compile
    seen_prices = []

    def mock_compile(intent_arg):
        seen_prices.append(dict(compiler.price_oracle) if compiler.price_oracle else None)
        result = MagicMock()
        result.status.value = "success"
        result.action_bundle = MagicMock()
        return result

    compiler.compile = mock_compile

    orchestrator = MagicMock()
    orchestrator.execute = AsyncMock(return_value=MagicMock(success=True, transaction_results=[], total_gas_used=0))

    manager = TeardownManager(
        orchestrator=orchestrator,
        compiler=compiler,
    )

    # Mock out cancel window and state persistence
    manager.cancel_window.run_cancel_window = AsyncMock(return_value=MagicMock(was_cancelled=False))
    manager.safety_guard.validate_teardown_request = MagicMock(return_value=MagicMock(all_passed=True))
    manager._verify_closure = AsyncMock(return_value=True)

    await manager.execute(strategy=strategy, mode="graceful", market=market)

    # compiler should have had real prices during compile
    assert len(seen_prices) > 0
    assert seen_prices[0] == {"ETH": Decimal("3400")}

    # After execution, compiler should be restored to original state
    assert compiler.price_oracle is None
    assert compiler._using_placeholders is True


@pytest.mark.asyncio
async def test_execute_without_market_uses_placeholders():
    """Graceful fallback when no market is provided."""
    strategy = _make_strategy(intents=[])
    manager = TeardownManager()

    await manager.execute(strategy=strategy, mode="graceful")

    # Should be called without market kwarg in the fallback
    strategy.generate_teardown_intents.assert_called_once_with(TeardownMode.SOFT, market=None)


@pytest.mark.asyncio
async def test_resume_applies_prices_when_regenerating():
    """When resume() regenerates stale intents, it passes market."""
    market = _make_market()
    strategy = _make_strategy(intents=[])

    state_manager = MagicMock()
    state = TeardownState(
        teardown_id="td_123",
        strategy_id="test_strat",
        mode=TeardownMode.SOFT,
        status=TeardownStatus.EXECUTING,
        total_intents=1,
        completed_intents=0,
        current_intent_index=0,
        started_at=datetime.now(UTC),
        updated_at=datetime(2020, 1, 1, tzinfo=UTC),  # very old = stale
        pending_intents_json="[]",
        cancel_window_until=datetime.now(UTC),
        config_json="{}",
    )
    state_manager.get_teardown_state = AsyncMock(return_value=state)

    config = TeardownConfig.default()
    config.staleness_threshold_seconds = 1  # 1 second = always stale

    manager = TeardownManager(state_manager=state_manager, config=config)

    await manager.resume(
        strategy_id="test_strat",
        strategy=strategy,
        market=market,
    )

    # generate_teardown_intents should have been called with market
    strategy.generate_teardown_intents.assert_called_once_with(TeardownMode.SOFT, market=market)


@pytest.mark.asyncio
async def test_execute_empty_dict_oracle_not_coerced_to_none():
    """Empty dict {} from get_price_oracle_dict() must NOT be coerced to None.

    Regression test for VIB-1408: `x or None` converts {} to None, which
    triggers $1 placeholder prices on mainnet teardowns. The fix uses
    `is not None` checks to preserve the semantic distinction.
    """
    market = MagicMock()
    market.get_price_oracle_dict.return_value = {}  # empty but not None
    strategy = _make_strategy(intents=[])

    manager = TeardownManager()

    # Patch _execute_intents to capture the price_oracle arg
    captured_oracle = []
    original_execute = manager._execute_intents

    async def spy_execute(*args, **kwargs):
        captured_oracle.append(kwargs.get("price_oracle"))
        return MagicMock(success=True, results=[], error=None)

    manager._execute_intents = spy_execute
    manager.cancel_window.run_cancel_window = AsyncMock(return_value=MagicMock(was_cancelled=False))
    manager.safety_guard.validate_teardown_request = MagicMock(return_value=MagicMock(all_passed=True))

    # Give it an intent so it reaches _execute_intents
    intent = MagicMock()
    intent.intent_type = "SWAP"
    intent.chain = "arbitrum"
    intent.to_dict.return_value = {"type": "swap"}
    strategy.generate_teardown_intents.return_value = [intent]

    await manager.execute(strategy=strategy, mode="graceful", market=market)

    # price_oracle should be {} (empty dict), NOT None
    assert len(captured_oracle) == 1
    assert captured_oracle[0] == {}
    assert captured_oracle[0] is not None


@pytest.mark.asyncio
async def test_execute_none_oracle_stays_none():
    """get_price_oracle_dict() returning None should remain None."""
    market = MagicMock()
    market.get_price_oracle_dict.return_value = None
    strategy = _make_strategy(intents=[])

    manager = TeardownManager()

    captured_oracle = []

    async def spy_execute(*args, **kwargs):
        captured_oracle.append(kwargs.get("price_oracle"))
        return MagicMock(success=True, results=[], error=None)

    manager._execute_intents = spy_execute
    manager.cancel_window.run_cancel_window = AsyncMock(return_value=MagicMock(was_cancelled=False))
    manager.safety_guard.validate_teardown_request = MagicMock(return_value=MagicMock(all_passed=True))

    intent = MagicMock()
    intent.intent_type = "SWAP"
    intent.chain = "arbitrum"
    intent.to_dict.return_value = {"type": "swap"}
    strategy.generate_teardown_intents.return_value = [intent]

    await manager.execute(strategy=strategy, mode="graceful", market=market)

    assert len(captured_oracle) == 1
    assert captured_oracle[0] is None


@pytest.mark.asyncio
async def test_execute_typeerror_fallback_only_catches_market_keyword():
    """TypeError from real strategy bugs is NOT silently swallowed as a fallback.

    The outer exception handler wraps it in a failed result, but critically
    the code does NOT retry without market (which would hide the bug).
    """
    strategy = _make_strategy()
    # Simulate a real strategy bug: TypeError not related to 'market' keyword
    strategy.generate_teardown_intents.side_effect = TypeError("unsupported operand type(s) for +: 'int' and 'str'")

    manager = TeardownManager()

    result = await manager.execute(strategy=strategy, mode="graceful", market=_make_market())

    # Should fail with the real error, not silently fall back
    assert result.success is False
    assert "unsupported operand" in result.error
    # Should have been called only once (no silent retry without market)
    strategy.generate_teardown_intents.assert_called_once()


@pytest.mark.asyncio
async def test_execute_typeerror_fallback_catches_old_signature():
    """TypeError about 'market' keyword falls back to call without it."""
    strategy = _make_strategy(intents=[])

    call_count = 0

    def fake_generate(mode, **kwargs):
        nonlocal call_count
        call_count += 1
        if "market" in kwargs:
            raise TypeError("generate_teardown_intents() got an unexpected keyword argument 'market'")
        return []

    strategy.generate_teardown_intents = fake_generate

    manager = TeardownManager()
    await manager.execute(strategy=strategy, mode="graceful", market=_make_market())

    # Should have been called twice: once with market (TypeError), once without
    assert call_count == 2
