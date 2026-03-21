"""Tests for the SushiSwap V3 Paper Trade LP on BSC demo strategy.

Validates:
1. Tick-count lifecycle: open -> hold N ticks -> close
2. Intent generation with correct protocol and tick ranges
3. Position tracking via on_intent_executed
4. Teardown support
5. State persistence and restoration

Kitchen Loop iteration 114, VIB-1625.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest


@pytest.fixture
def strategy():
    """Instantiate the strategy with mock config, bypassing IntentStrategy __init__."""
    from strategies.demo.sushiswap_v3_paper_trade_lp_bsc.strategy import (
        SushiSwapV3PaperTradeLPBSCStrategy,
    )

    strat = SushiSwapV3PaperTradeLPBSCStrategy.__new__(SushiSwapV3PaperTradeLPBSCStrategy)
    strat._chain = "bsc"
    strat._wallet_address = "0x" + "0" * 40
    strat._strategy_id = "test-paper-trade-lp-bsc"

    strat.pool = "WBNB/USDT/3000"
    strat.token0_symbol = "WBNB"
    strat.token1_symbol = "USDT"
    strat.fee_tier = 3000
    strat.range_width_pct = Decimal("0.20")
    strat.amount0 = Decimal("0.1")
    strat.amount1 = Decimal("50")
    strat.hold_ticks = 3

    strat._has_position = False
    strat._position_id = None
    strat._ticks_held = 0
    strat._tick_count = 0

    return strat


@pytest.fixture
def mock_market():
    """Mock MarketSnapshot returning BSC token prices."""
    market = MagicMock()
    prices = {"WBNB": Decimal("600"), "USDT": Decimal("1")}
    market.price = MagicMock(side_effect=lambda symbol: prices[symbol])
    return market


class TestLifecycle:
    """Test the tick-count lifecycle: open -> hold -> close."""

    def test_first_tick_opens_lp(self, strategy, mock_market):
        intent = strategy.decide(mock_market)
        assert intent is not None
        assert intent.intent_type.value == "LP_OPEN"
        assert intent.protocol == "sushiswap_v3"

    def test_hold_during_hold_period(self, strategy, mock_market):
        strategy._has_position = True
        strategy._ticks_held = 0
        strategy.hold_ticks = 3

        # Tick 1 of hold: should hold
        intent = strategy.decide(mock_market)
        assert intent.intent_type.value == "HOLD"
        assert "1/3" in intent.reason

    def test_hold_increments_ticks(self, strategy, mock_market):
        strategy._has_position = True
        strategy._ticks_held = 1
        strategy.hold_ticks = 3

        intent = strategy.decide(mock_market)
        assert intent.intent_type.value == "HOLD"
        assert strategy._ticks_held == 2

    def test_close_after_hold_period(self, strategy, mock_market):
        strategy._has_position = True
        strategy._position_id = 12345
        strategy._ticks_held = 3
        strategy.hold_ticks = 3

        intent = strategy.decide(mock_market)
        assert intent.intent_type.value == "LP_CLOSE"
        assert intent.protocol == "sushiswap_v3"

    def test_full_lifecycle(self, strategy, mock_market):
        """Simulate full open -> hold -> hold -> hold -> close lifecycle."""
        # Tick 1: Open
        intent1 = strategy.decide(mock_market)
        assert intent1.intent_type.value == "LP_OPEN"

        # Simulate successful execution
        mock_result = MagicMock()
        mock_result.position_id = 99999
        strategy.on_intent_executed(intent1, success=True, result=mock_result)
        assert strategy._has_position is True

        # Ticks 2-4: Hold
        for i in range(3):
            intent = strategy.decide(mock_market)
            if i < 2:
                assert intent.intent_type.value == "HOLD", f"Expected HOLD at hold tick {i + 1}"
            else:
                # 3rd hold tick reaches hold_ticks=3, triggers close
                assert intent.intent_type.value == "LP_CLOSE"


class TestIntentCreation:
    """Test intent creation details."""

    def test_open_intent_pool(self, strategy, mock_market):
        intent = strategy.decide(mock_market)
        assert intent.pool == "WBNB/USDT/3000"

    def test_open_intent_amounts(self, strategy, mock_market):
        intent = strategy.decide(mock_market)
        assert intent.amount0 == Decimal("0.1")
        assert intent.amount1 == Decimal("50")

    def test_open_intent_range(self, strategy, mock_market):
        intent = strategy.decide(mock_market)
        assert intent.range_lower < intent.range_upper
        # Price=600, +-10% => [540, 660]
        assert Decimal("500") < intent.range_lower < Decimal("600")
        assert Decimal("600") < intent.range_upper < Decimal("700")

    def test_close_intent_with_nft_id(self, strategy, mock_market):
        strategy._has_position = True
        strategy._position_id = 42
        strategy._ticks_held = 3
        strategy.hold_ticks = 3
        intent = strategy.decide(mock_market)
        assert intent.intent_type.value == "LP_CLOSE"
        assert "42" in str(intent.position_id)

    def test_close_intent_without_nft_id(self, strategy, mock_market):
        strategy._has_position = True
        strategy._position_id = None
        strategy._ticks_held = 3
        strategy.hold_ticks = 3
        intent = strategy.decide(mock_market)
        assert intent.intent_type.value == "LP_CLOSE"
        assert "sushiswap-v3-lp" in str(intent.position_id)


class TestOnIntentExecuted:
    """Test on_intent_executed callback."""

    def test_open_success(self, strategy):
        intent = MagicMock()
        intent.intent_type.value = "LP_OPEN"
        result = MagicMock()
        result.position_id = 12345

        strategy.on_intent_executed(intent, success=True, result=result)
        assert strategy._has_position is True
        assert strategy._position_id == 12345
        assert strategy._ticks_held == 0

    def test_close_success(self, strategy):
        strategy._has_position = True
        strategy._position_id = 12345
        strategy._ticks_held = 3

        intent = MagicMock()
        intent.intent_type.value = "LP_CLOSE"
        strategy.on_intent_executed(intent, success=True, result=MagicMock())

        assert strategy._has_position is False
        assert strategy._position_id is None
        assert strategy._ticks_held == 0

    def test_failure_no_state_change(self, strategy):
        strategy._has_position = False
        intent = MagicMock()
        intent.intent_type.value = "LP_OPEN"
        strategy.on_intent_executed(intent, success=False, result=MagicMock())
        assert strategy._has_position is False


class TestTeardown:
    """Test teardown support."""

    def test_no_positions(self, strategy):
        summary = strategy.get_open_positions()
        assert len(summary.positions) == 0

    def test_open_position_reported(self, strategy):
        strategy._has_position = True
        strategy._position_id = 42
        summary = strategy.get_open_positions()
        assert len(summary.positions) == 1
        assert summary.positions[0].protocol == "sushiswap_v3"

    def test_teardown_intents(self, strategy):
        from almanak.framework.teardown import TeardownMode

        strategy._has_position = True
        strategy._position_id = 42
        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 1
        assert intents[0].intent_type.value == "LP_CLOSE"

    def test_teardown_empty_when_no_position(self, strategy):
        from almanak.framework.teardown import TeardownMode

        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 0


class TestStatePersistence:
    """Test state persistence and restoration."""

    def test_roundtrip(self, strategy):
        strategy._has_position = True
        strategy._position_id = 42
        strategy._ticks_held = 2
        strategy._tick_count = 5

        state = strategy.get_persistent_state()
        assert state["has_position"] is True
        assert state["position_id"] == 42
        assert state["ticks_held"] == 2
        assert state["tick_count"] == 5

        # Restore into fresh instance
        new_strat = strategy.__class__.__new__(strategy.__class__)
        new_strat._has_position = False
        new_strat._position_id = None
        new_strat._ticks_held = 0
        new_strat._tick_count = 0
        new_strat.load_persistent_state(state)

        assert new_strat._has_position is True
        assert new_strat._position_id == 42
        assert new_strat._ticks_held == 2
        assert new_strat._tick_count == 5


class TestEdgeCases:
    """Test edge cases."""

    def test_price_zero_holds(self, strategy):
        market = MagicMock()
        market.price = MagicMock(side_effect=lambda s: Decimal("0") if s == "USDT" else Decimal("600"))
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"

    def test_price_error_holds(self, strategy):
        market = MagicMock()
        market.price = MagicMock(side_effect=Exception("API error"))
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"

    def test_tick_count_increments(self, strategy, mock_market):
        assert strategy._tick_count == 0
        strategy.decide(mock_market)
        assert strategy._tick_count == 1
        strategy.decide(mock_market)
        assert strategy._tick_count == 2
