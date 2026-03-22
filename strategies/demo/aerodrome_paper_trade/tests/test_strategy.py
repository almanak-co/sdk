"""Tests for the Aerodrome Paper Trade — RSI-Based LP on Base demo strategy.

Validates:
1. RSI-gated lifecycle: open when RSI in range, close when extreme, hold otherwise
2. Intent generation with correct protocol and pool encoding
3. Position tracking via on_intent_executed
4. Teardown support
5. State persistence and restoration
6. Edge cases: RSI unavailable, insufficient funds, error handling

Kitchen Loop iteration 117, VIB-1670.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest


@pytest.fixture
def strategy():
    """Instantiate the strategy with mock config, bypassing IntentStrategy __init__."""
    from strategies.demo.aerodrome_paper_trade.strategy import (
        AerodromePaperTradeStrategy,
    )

    strat = AerodromePaperTradeStrategy.__new__(AerodromePaperTradeStrategy)
    strat._chain = "base"
    strat._wallet_address = "0x" + "0" * 40
    strat._strategy_id = "test-aerodrome-paper-trade"

    # Pool configuration (mirrors config.json defaults)
    strat.pool = "WETH/USDC"
    strat.token0 = "WETH"
    strat.token1 = "USDC"
    strat.stable = False

    # LP amounts
    strat.amount0 = Decimal("0.001")
    strat.amount1 = Decimal("3")

    # RSI parameters
    strat.rsi_period = 14
    strat.rsi_oversold = Decimal("35")
    strat.rsi_overbought = Decimal("65")

    # Internal state
    strat._has_position = False
    strat._lp_token_balance = Decimal("0")
    strat._ticks_with_position = 0

    return strat


def _make_market(rsi_value=50, weth_balance=Decimal("1"), usdc_balance=Decimal("10000")):
    """Create a mock MarketSnapshot with configurable RSI and balances."""
    market = MagicMock()

    # RSI mock
    rsi_data = MagicMock()
    rsi_data.value = Decimal(str(rsi_value))
    market.rsi = MagicMock(return_value=rsi_data)

    # Balance mock
    def balance_side_effect(symbol):
        bal = MagicMock()
        if symbol == "WETH":
            bal.balance = weth_balance
        elif symbol == "USDC":
            bal.balance = usdc_balance
        else:
            bal.balance = Decimal("0")
        return bal

    market.balance = MagicMock(side_effect=balance_side_effect)
    return market


@pytest.fixture
def mock_market():
    """Mock MarketSnapshot with RSI=50 (in range) and adequate funds."""
    return _make_market(rsi_value=50)


class TestLifecycle:
    """Test the RSI-gated lifecycle: open -> hold -> close."""

    def test_rsi_in_range_no_position_opens_lp(self, strategy, mock_market):
        intent = strategy.decide(mock_market)
        assert intent is not None
        assert intent.intent_type.value == "LP_OPEN"
        assert intent.protocol == "aerodrome"

    def test_rsi_extreme_low_no_position_holds(self, strategy):
        market = _make_market(rsi_value=25)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "outside range" in intent.reason.lower()

    def test_rsi_extreme_high_no_position_holds(self, strategy):
        market = _make_market(rsi_value=75)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "outside range" in intent.reason.lower()

    def test_has_position_rsi_in_range_holds(self, strategy, mock_market):
        strategy._has_position = True
        strategy._ticks_with_position = 2
        intent = strategy.decide(mock_market)
        assert intent.intent_type.value == "HOLD"
        assert "active" in intent.reason.lower()

    def test_has_position_rsi_extreme_closes(self, strategy):
        strategy._has_position = True
        strategy._ticks_with_position = 5
        market = _make_market(rsi_value=25)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "LP_CLOSE"
        assert intent.protocol == "aerodrome"

    def test_has_position_rsi_overbought_closes(self, strategy):
        strategy._has_position = True
        strategy._ticks_with_position = 3
        market = _make_market(rsi_value=70)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "LP_CLOSE"

    def test_ticks_with_position_increments(self, strategy, mock_market):
        strategy._has_position = True
        strategy._ticks_with_position = 0
        strategy.decide(mock_market)
        assert strategy._ticks_with_position == 1

    def test_full_lifecycle(self, strategy, mock_market):
        """Simulate: open -> hold (RSI in range) -> close (RSI extreme)."""
        # Tick 1: RSI=50, no position -> open
        intent1 = strategy.decide(mock_market)
        assert intent1.intent_type.value == "LP_OPEN"

        # Simulate successful execution
        mock_result = MagicMock()
        mock_result.extracted_data = {"liquidity": "1000000"}
        strategy.on_intent_executed(intent1, success=True, result=mock_result)
        assert strategy._has_position is True

        # Tick 2: RSI=50, has position -> hold
        intent2 = strategy.decide(mock_market)
        assert intent2.intent_type.value == "HOLD"
        assert strategy._ticks_with_position == 1

        # Tick 3: RSI=25, has position -> close
        extreme_market = _make_market(rsi_value=25)
        intent3 = strategy.decide(extreme_market)
        assert intent3.intent_type.value == "LP_CLOSE"

        # Simulate close execution
        strategy.on_intent_executed(intent3, success=True, result=MagicMock())
        assert strategy._has_position is False
        assert strategy._lp_token_balance == Decimal("0")


class TestIntentCreation:
    """Test intent creation details."""

    def test_open_intent_pool_with_type(self, strategy, mock_market):
        intent = strategy.decide(mock_market)
        assert intent.pool == "WETH/USDC/volatile"

    def test_open_intent_amounts(self, strategy, mock_market):
        intent = strategy.decide(mock_market)
        assert intent.amount0 == Decimal("0.001")
        assert intent.amount1 == Decimal("3")

    def test_open_intent_range_values(self, strategy, mock_market):
        intent = strategy.decide(mock_market)
        # Aerodrome uses full range; dummy values
        assert intent.range_lower == Decimal("1")
        assert intent.range_upper == Decimal("1000000")

    def test_open_intent_chain(self, strategy, mock_market):
        intent = strategy.decide(mock_market)
        assert intent.chain == "base"

    def test_close_intent_pool_with_type(self, strategy):
        strategy._has_position = True
        strategy._ticks_with_position = 1
        market = _make_market(rsi_value=80)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "LP_CLOSE"
        assert intent.pool == "WETH/USDC/volatile"
        assert intent.position_id == "WETH/USDC/volatile"

    def test_close_intent_collects_fees(self, strategy):
        strategy._has_position = True
        strategy._ticks_with_position = 1
        market = _make_market(rsi_value=80)
        intent = strategy.decide(market)
        assert intent.collect_fees is True

    def test_stable_pool_encoding(self, strategy, mock_market):
        strategy.stable = True
        intent = strategy.decide(mock_market)
        assert intent.pool == "WETH/USDC/stable"


class TestOnIntentExecuted:
    """Test on_intent_executed callback."""

    def test_open_success_sets_position(self, strategy):
        intent = MagicMock()
        intent.intent_type.value = "LP_OPEN"
        result = MagicMock()
        result.extracted_data = {"liquidity": "5000000"}

        strategy.on_intent_executed(intent, success=True, result=result)
        assert strategy._has_position is True
        assert strategy._ticks_with_position == 0
        assert strategy._lp_token_balance == Decimal("5000000")

    def test_open_success_no_extracted_data(self, strategy):
        intent = MagicMock()
        intent.intent_type.value = "LP_OPEN"
        result = MagicMock()
        result.extracted_data = None

        strategy.on_intent_executed(intent, success=True, result=result)
        assert strategy._has_position is True
        assert strategy._lp_token_balance == Decimal("0")  # no extraction

    def test_close_success_clears_position(self, strategy):
        strategy._has_position = True
        strategy._lp_token_balance = Decimal("5000000")
        strategy._ticks_with_position = 5

        intent = MagicMock()
        intent.intent_type.value = "LP_CLOSE"
        strategy.on_intent_executed(intent, success=True, result=MagicMock())

        assert strategy._has_position is False
        assert strategy._lp_token_balance == Decimal("0")

    def test_failure_no_state_change(self, strategy):
        strategy._has_position = False
        intent = MagicMock()
        intent.intent_type.value = "LP_OPEN"
        strategy.on_intent_executed(intent, success=False, result=MagicMock())
        assert strategy._has_position is False

    def test_unknown_intent_type_ignored(self, strategy):
        strategy._has_position = False
        intent = MagicMock()
        intent.intent_type.value = "SWAP"
        strategy.on_intent_executed(intent, success=True, result=MagicMock())
        assert strategy._has_position is False


class TestTeardown:
    """Test teardown support."""

    def test_no_positions_when_empty(self, strategy):
        summary = strategy.get_open_positions()
        assert len(summary.positions) == 0

    def test_open_position_reported(self, strategy):
        strategy._has_position = True
        strategy._lp_token_balance = Decimal("1000")
        summary = strategy.get_open_positions()
        assert len(summary.positions) == 1
        pos = summary.positions[0]
        assert pos.protocol == "aerodrome"
        assert pos.details["pool"] == "WETH/USDC"

    def test_position_reported_from_balance_only(self, strategy):
        """Even if _has_position is False, nonzero lp_token_balance reports position."""
        strategy._has_position = False
        strategy._lp_token_balance = Decimal("500")
        summary = strategy.get_open_positions()
        assert len(summary.positions) == 1

    def test_teardown_intents_when_position(self, strategy):
        from almanak.framework.teardown import TeardownMode

        strategy._has_position = True
        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 1
        assert intents[0].intent_type.value == "LP_CLOSE"
        assert intents[0].protocol == "aerodrome"

    def test_teardown_intents_from_balance_only(self, strategy):
        """Teardown generated if lp_token_balance > 0, even if _has_position is False."""
        from almanak.framework.teardown import TeardownMode

        strategy._has_position = False
        strategy._lp_token_balance = Decimal("500")
        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 1
        assert intents[0].intent_type.value == "LP_CLOSE"

    def test_teardown_empty_when_no_position(self, strategy):
        from almanak.framework.teardown import TeardownMode

        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 0


class TestStatePersistence:
    """Test state persistence and restoration."""

    def test_get_persistent_state(self, strategy):
        strategy._has_position = True
        strategy._lp_token_balance = Decimal("12345")
        strategy._ticks_with_position = 7

        state = strategy.get_persistent_state()
        assert state["has_position"] is True
        assert state["lp_token_balance"] == "12345"
        assert state["ticks_with_position"] == 7

    def test_roundtrip(self, strategy):
        strategy._has_position = True
        strategy._lp_token_balance = Decimal("99999")
        strategy._ticks_with_position = 3

        state = strategy.get_persistent_state()

        # Restore into fresh instance
        new_strat = strategy.__class__.__new__(strategy.__class__)
        new_strat._has_position = False
        new_strat._lp_token_balance = Decimal("0")
        new_strat._ticks_with_position = 0
        new_strat.load_persistent_state(state)

        assert new_strat._has_position is True
        assert new_strat._lp_token_balance == Decimal("99999")
        assert new_strat._ticks_with_position == 3

    @pytest.mark.parametrize(
        "partial_state, expected_has_position, expected_lp_balance, expected_ticks",
        [
            ({"has_position": True}, True, Decimal("0"), 0),
            ({"lp_token_balance": "123"}, False, Decimal("123"), 0),
            ({"ticks_with_position": 5}, False, Decimal("0"), 5),
            (
                {"has_position": True, "ticks_with_position": 10},
                True,
                Decimal("0"),
                10,
            ),
        ],
    )
    def test_partial_state_restore(
        self, strategy, partial_state, expected_has_position, expected_lp_balance, expected_ticks
    ):
        """Restoring partial state only updates provided fields."""
        strategy._has_position = False
        strategy._lp_token_balance = Decimal("0")
        strategy._ticks_with_position = 0

        strategy.load_persistent_state(partial_state)
        assert strategy._has_position is expected_has_position
        assert strategy._lp_token_balance == expected_lp_balance
        assert strategy._ticks_with_position == expected_ticks


class TestEdgeCases:
    """Test edge cases and error handling."""

    def test_rsi_unavailable_holds(self, strategy):
        market = MagicMock()
        market.rsi = MagicMock(side_effect=ValueError("No RSI data"))
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "unavailable" in intent.reason.lower()

    def test_rsi_attribute_error_holds(self, strategy):
        market = MagicMock()
        market.rsi = MagicMock(side_effect=AttributeError("No attribute"))
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"

    def test_insufficient_funds_holds(self, strategy):
        market = _make_market(rsi_value=50, weth_balance=Decimal("0"), usdc_balance=Decimal("0"))
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "insufficient funds" in intent.reason.lower()

    def test_balance_error_holds(self, strategy):
        market = _make_market(rsi_value=50)
        market.balance = MagicMock(side_effect=ValueError("Balance unavailable"))
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"

    def test_rsi_at_boundary_oversold(self, strategy):
        """RSI exactly at oversold threshold is still in range."""
        market = _make_market(rsi_value=35)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "LP_OPEN"

    def test_rsi_at_boundary_overbought(self, strategy):
        """RSI exactly at overbought threshold is still in range."""
        market = _make_market(rsi_value=65)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "LP_OPEN"

    def test_rsi_just_below_oversold(self, strategy):
        """RSI just below oversold is extreme."""
        market = _make_market(rsi_value=34)
        strategy._has_position = True
        intent = strategy.decide(market)
        assert intent.intent_type.value == "LP_CLOSE"

    def test_rsi_just_above_overbought(self, strategy):
        """RSI just above overbought is extreme."""
        market = _make_market(rsi_value=66)
        strategy._has_position = True
        intent = strategy.decide(market)
        assert intent.intent_type.value == "LP_CLOSE"

    def test_general_exception_holds(self, strategy):
        """Any uncaught exception in decide() returns HOLD."""
        market = MagicMock()
        rsi_data = MagicMock()
        rsi_data.value = Decimal("50")
        market.rsi = MagicMock(return_value=rsi_data)
        # balance raises unexpected error
        market.balance = MagicMock(side_effect=RuntimeError("Unexpected"))
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
