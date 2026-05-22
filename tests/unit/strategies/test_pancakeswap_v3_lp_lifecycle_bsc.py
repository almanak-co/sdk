"""Tests for PancakeSwap V3 LP Full Lifecycle on BSC (VIB-2308).

Validates the tick-based state machine:
Tick 1: LP_OPEN -> Tick 2: HOLD -> Tick 3: LP_CLOSE -> Tick 4+: HOLD

Also tests teardown support, position tracking, and error handling.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest


@pytest.fixture
def strategy():
    from almanak.demo_strategies.pancakeswap_lp_lifecycle_bsc.strategy import (
        PancakeSwapV3LPLifecycleBSCStrategy,
    )

    strat = PancakeSwapV3LPLifecycleBSCStrategy.__new__(PancakeSwapV3LPLifecycleBSCStrategy)
    strat.config = {}
    strat._chain = "bsc"
    strat._wallet_address = "0x" + "0" * 40
    strat._deployment_id = "test-pancakeswap-lp-lifecycle-bsc"
    strat.STRATEGY_NAME = "pancakeswap_lp_lifecycle_bsc"

    # Config values (matching config.json defaults)
    strat.pool = "WBNB/USDT/2500"
    strat.token0_symbol = "WBNB"
    strat.token1_symbol = "USDT"
    strat.fee_tier = 2500
    strat.range_width_pct = Decimal("0.20")
    strat.amount0 = Decimal("0.01")
    strat.amount1 = Decimal("5")

    # State
    strat._current_position_id = None
    strat._tick = 0
    strat._lifecycle_complete = False
    strat._lp_close_attempted = False

    return strat


def _mock_market(
    wbnb_price: float = 600.0,
    usdt_price: float = 1.0,
) -> MagicMock:
    market = MagicMock()

    def price_fn(token):
        prices = {"WBNB": wbnb_price, "USDT": usdt_price, "BNB": wbnb_price}
        if token in prices:
            return Decimal(str(prices[token]))
        raise ValueError(f"Unknown token: {token}")

    market.price = MagicMock(side_effect=price_fn)
    return market


# ===========================================================================
# TICK 1: LP_OPEN
# ===========================================================================


class TestTick1LPOpen:
    """Tick 1 should produce an LP_OPEN intent."""

    def test_tick1_produces_lp_open(self, strategy):
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "LP_OPEN"
        assert intent.protocol == "pancakeswap_v3"
        assert intent.pool == "WBNB/USDT/2500"

    def test_tick1_lp_open_amounts(self, strategy):
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.amount0 == Decimal("0.01")
        assert intent.amount1 == Decimal("5")

    def test_tick1_lp_open_range_centered(self, strategy):
        market = _mock_market(wbnb_price=600.0, usdt_price=1.0)
        intent = strategy.decide(market)
        # Current price = 600/1 = 600; range_width = 20%
        # range_lower = 600 * 0.9 = 540, range_upper = 600 * 1.1 = 660
        assert intent.range_lower == Decimal("600") * Decimal("0.9")
        assert intent.range_upper == Decimal("600") * Decimal("1.1")


# ===========================================================================
# TICK 2: HOLD (after LP_OPEN)
# ===========================================================================


class TestTick2Hold:
    """Tick 2 should HOLD if position was created."""

    def test_tick2_hold_with_position(self, strategy):
        market = _mock_market()
        # Simulate tick 1 (LP_OPEN)
        strategy.decide(market)
        # Simulate successful LP_OPEN callback
        strategy._current_position_id = "123456"
        # Tick 2
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "123456" in intent.reason

    def test_tick2_hold_without_position_aborts(self, strategy):
        market = _mock_market()
        strategy.decide(market)  # tick 1
        # No position_id set (LP_OPEN failed to extract)
        intent = strategy.decide(market)  # tick 2
        assert intent.intent_type.value == "HOLD"
        assert strategy._lifecycle_complete is True


# ===========================================================================
# TICK 3: LP_CLOSE
# ===========================================================================


class TestTick3LPClose:
    """Tick 3 should produce LP_CLOSE if position exists."""

    def test_tick3_produces_lp_close(self, strategy):
        market = _mock_market()
        strategy.decide(market)  # tick 1
        strategy._current_position_id = "789"
        strategy.decide(market)  # tick 2 (hold)
        intent = strategy.decide(market)  # tick 3
        assert intent.intent_type.value == "LP_CLOSE"
        assert intent.position_id == "789"
        assert intent.protocol == "pancakeswap_v3"
        assert intent.collect_fees is True

    def test_tick3_lp_close_only_once(self, strategy):
        market = _mock_market()
        strategy.decide(market)  # tick 1
        strategy._current_position_id = "789"
        strategy.decide(market)  # tick 2
        strategy.decide(market)  # tick 3 (LP_CLOSE)
        # Tick 4 should hold
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"


# ===========================================================================
# TICK 4+: LIFECYCLE COMPLETE
# ===========================================================================


class TestTick4Complete:
    """Tick 4+ should always HOLD."""

    def test_lifecycle_complete_holds(self, strategy):
        strategy._lifecycle_complete = True
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "complete" in intent.reason.lower()


# ===========================================================================
# on_intent_executed CALLBACKS
# ===========================================================================


class TestOnIntentExecuted:
    """Test position tracking via on_intent_executed."""

    def test_lp_open_success_tracks_position(self, strategy):
        intent = MagicMock()
        intent.intent_type.value = "LP_OPEN"
        result = MagicMock()
        result.position_id = 42

        strategy.on_intent_executed(intent, True, result)
        assert strategy._current_position_id == "42"

    def test_lp_open_success_no_position_id(self, strategy):
        intent = MagicMock()
        intent.intent_type.value = "LP_OPEN"
        result = MagicMock(spec=[])  # no position_id attr

        strategy.on_intent_executed(intent, True, result)
        assert strategy._current_position_id is None

    def test_lp_close_success_clears_position(self, strategy):
        strategy._current_position_id = "42"
        intent = MagicMock()
        intent.intent_type.value = "LP_CLOSE"
        result = MagicMock(spec=[])  # no lp_close_data

        strategy.on_intent_executed(intent, True, result)
        assert strategy._current_position_id is None
        assert strategy._lifecycle_complete is True

    def test_lp_close_failure_marks_complete(self, strategy):
        strategy._current_position_id = "42"
        intent = MagicMock()
        intent.intent_type.value = "LP_CLOSE"

        strategy.on_intent_executed(intent, False, None)
        assert strategy._lifecycle_complete is True


# ===========================================================================
# TEARDOWN
# ===========================================================================


class TestTeardown:
    """Test teardown support methods."""

    def test_get_open_positions_with_position(self, strategy):
        strategy._current_position_id = "99"
        positions = strategy.get_open_positions()
        assert len(positions.positions) == 1
        pos = positions.positions[0]
        assert pos.position_id == "99"
        assert pos.protocol == "pancakeswap_v3"
        assert pos.details["pool"] == "WBNB/USDT/2500"

    def test_get_open_positions_empty(self, strategy):
        positions = strategy.get_open_positions()
        assert len(positions.positions) == 0

    def test_generate_teardown_intents_with_position(self, strategy):
        strategy._current_position_id = "99"
        intents = strategy.generate_teardown_intents(mode="hard")
        assert len(intents) == 1
        assert intents[0].intent_type.value == "LP_CLOSE"
        assert intents[0].position_id == "99"
        assert intents[0].collect_fees is True

    def test_generate_teardown_intents_empty(self, strategy):
        intents = strategy.generate_teardown_intents(mode="hard")
        assert len(intents) == 0


# ===========================================================================
# PRICE EDGE CASES
# ===========================================================================


class TestPriceEdgeCases:
    """Test behavior with price failures."""

    def test_price_unavailable_holds(self, strategy):
        market = MagicMock()
        market.price = MagicMock(side_effect=ValueError("No price"))
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "unavailable" in intent.reason.lower()

    def test_price_unavailable_error_holds(self, strategy):
        from almanak.framework.data import PriceUnavailableError

        market = MagicMock()
        market.price = MagicMock(side_effect=PriceUnavailableError("WBNB", "No price data"))
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "unavailable" in intent.reason.lower()

    def test_zero_token1_price_holds(self, strategy):
        market = _mock_market(usdt_price=0.0)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "zero" in intent.reason.lower()
