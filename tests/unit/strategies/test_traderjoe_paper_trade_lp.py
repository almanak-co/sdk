"""Tests for the TraderJoe V2 Paper Trade LP demo strategy.

Validates RSI-gated LP open/close decisions, state persistence, teardown,
and intent generation with TraderJoe V2 Liquidity Book on Avalanche.

Kitchen Loop iteration 89, VIB-1428.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest


@pytest.fixture
def strategy():
    from almanak.demo_strategies.traderjoe_paper_trade_lp.strategy import TraderJoePaperTradeLPStrategy

    strat = TraderJoePaperTradeLPStrategy.__new__(TraderJoePaperTradeLPStrategy)
    strat.config = {}
    strat._chain = "avalanche"
    strat._wallet_address = "0x" + "0" * 40
    strat._strategy_id = "test-traderjoe-paper-trade-lp"
    strat.pool = "WAVAX/USDC/20"
    strat.token_x = "WAVAX"
    strat.token_y = "USDC"
    strat.bin_step = 20
    strat.amount_x = Decimal("0.5")
    strat.amount_y = Decimal("10")
    strat.range_width_pct = Decimal("0.10")
    strat.rsi_period = 14
    strat.rsi_oversold = Decimal("35")
    strat.rsi_overbought = Decimal("65")
    strat._has_position = False
    strat._ticks_with_position = 0
    return strat


def _mock_market(
    rsi_value: float | None = 50.0,
    wavax_balance: float = 10.0,
    usdc_balance: float = 500.0,
    wavax_price_usd: float = 25.0,
    usdc_price_usd: float = 1.0,
) -> MagicMock:
    market = MagicMock()

    def rsi_fn(token, period=14):
        if rsi_value is None:
            raise ValueError("RSI unavailable")
        rsi_obj = MagicMock()
        rsi_obj.value = Decimal(str(rsi_value))
        return rsi_obj

    market.rsi = MagicMock(side_effect=rsi_fn)

    def price_fn(token):
        if token == "WAVAX":
            return Decimal(str(wavax_price_usd))
        elif token == "USDC":
            return Decimal(str(usdc_price_usd))
        raise ValueError(f"Unknown token: {token}")

    market.price = MagicMock(side_effect=price_fn)

    def balance_fn(token):
        bal = MagicMock()
        if token == "WAVAX":
            bal.balance = Decimal(str(wavax_balance))
            bal.balance_usd = Decimal(str(wavax_balance)) * Decimal(str(wavax_price_usd))
        elif token == "USDC":
            bal.balance = Decimal(str(usdc_balance))
            bal.balance_usd = Decimal(str(usdc_balance))
        else:
            raise ValueError(f"Unknown token: {token}")
        return bal

    market.balance = MagicMock(side_effect=balance_fn)
    return market


class TestDecision:
    def test_open_lp_when_rsi_in_range(self, strategy):
        """RSI in range + has funds -> LP_OPEN."""
        market = _mock_market(rsi_value=50.0)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "LP_OPEN"
        assert intent.protocol == "traderjoe_v2"

    def test_hold_when_rsi_extreme_no_position(self, strategy):
        """RSI extreme + no position -> HOLD."""
        market = _mock_market(rsi_value=25.0)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "outside range" in intent.reason.lower()

    def test_close_lp_when_rsi_extreme_has_position(self, strategy):
        """RSI extreme + has position -> LP_CLOSE."""
        strategy._has_position = True
        market = _mock_market(rsi_value=25.0)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "LP_CLOSE"
        assert intent.protocol == "traderjoe_v2"

    def test_hold_when_has_position_rsi_neutral(self, strategy):
        """Has position + RSI in range -> HOLD."""
        strategy._has_position = True
        market = _mock_market(rsi_value=50.0)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "active" in intent.reason.lower()

    def test_hold_when_rsi_unavailable(self, strategy):
        market = _mock_market(rsi_value=None)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "unavailable" in intent.reason.lower()

    def test_hold_when_insufficient_funds(self, strategy):
        """RSI in range but insufficient balance -> HOLD."""
        market = _mock_market(rsi_value=50.0, wavax_balance=0.001, usdc_balance=1.0)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "insufficient" in intent.reason.lower()

    def test_open_lp_at_oversold_boundary(self, strategy):
        """RSI exactly at oversold boundary is in range (<=)."""
        market = _mock_market(rsi_value=35.0)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "LP_OPEN"

    def test_open_lp_at_overbought_boundary(self, strategy):
        """RSI exactly at overbought boundary is in range (<=)."""
        market = _mock_market(rsi_value=65.0)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "LP_OPEN"

    def test_close_when_oversold(self, strategy):
        """RSI below oversold with position -> close."""
        strategy._has_position = True
        market = _mock_market(rsi_value=30.0)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "LP_CLOSE"

    def test_close_when_overbought(self, strategy):
        """RSI above overbought with position -> close."""
        strategy._has_position = True
        market = _mock_market(rsi_value=70.0)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "LP_CLOSE"

    def test_ticks_increment_with_position(self, strategy):
        """Ticks counter increments when holding with position."""
        strategy._has_position = True
        market = _mock_market(rsi_value=50.0)
        strategy.decide(market)
        assert strategy._ticks_with_position == 1
        strategy.decide(market)
        assert strategy._ticks_with_position == 2

    def test_lp_open_has_price_range(self, strategy):
        """LP_OPEN intent includes range_lower and range_upper."""
        market = _mock_market(rsi_value=50.0)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "LP_OPEN"
        assert intent.range_lower is not None
        assert intent.range_upper is not None
        assert intent.range_lower < intent.range_upper

    def test_lp_open_pool_format(self, strategy):
        """LP_OPEN uses correct TraderJoe pool format."""
        market = _mock_market(rsi_value=50.0)
        intent = strategy.decide(market)
        assert intent.pool == "WAVAX/USDC/20"


class TestOnIntentExecuted:
    def test_lp_open_sets_position(self, strategy):
        intent = MagicMock()
        intent.intent_type.value = "LP_OPEN"
        strategy.on_intent_executed(intent, success=True, result=MagicMock())
        assert strategy._has_position is True
        assert strategy._ticks_with_position == 0

    def test_lp_close_clears_position(self, strategy):
        strategy._has_position = True
        strategy._ticks_with_position = 5
        intent = MagicMock()
        intent.intent_type.value = "LP_CLOSE"
        strategy.on_intent_executed(intent, success=True, result=MagicMock())
        assert strategy._has_position is False
        assert strategy._ticks_with_position == 0

    def test_failed_intent_no_state_change(self, strategy):
        intent = MagicMock()
        intent.intent_type.value = "LP_OPEN"
        strategy.on_intent_executed(intent, success=False, result=None)
        assert strategy._has_position is False


class TestStatePersistence:
    def test_get_persistent_state(self, strategy):
        strategy._has_position = True
        strategy._ticks_with_position = 3
        state = strategy.get_persistent_state()
        assert state["has_position"] is True
        assert state["ticks_with_position"] == 3

    def test_load_persistent_state(self, strategy):
        strategy.load_persistent_state({
            "has_position": True,
            "ticks_with_position": 7,
        })
        assert strategy._has_position is True
        assert strategy._ticks_with_position == 7

    def test_load_empty_state(self, strategy):
        strategy.load_persistent_state({})
        assert strategy._has_position is False
        assert strategy._ticks_with_position == 0


class TestTeardown:
    def test_supports_teardown(self, strategy):
        assert strategy.supports_teardown() is True

    def test_teardown_with_position(self, strategy):
        strategy._has_position = True
        intents = strategy.generate_teardown_intents(
            mode=MagicMock(value="SOFT"),
            market=None,
        )
        assert len(intents) == 1
        assert intents[0].intent_type.value == "LP_CLOSE"
        assert intents[0].protocol == "traderjoe_v2"

    def test_teardown_without_position(self, strategy):
        strategy._has_position = False
        intents = strategy.generate_teardown_intents(
            mode=MagicMock(value="SOFT"),
            market=None,
        )
        assert len(intents) == 0

    def test_get_open_positions_with_position(self, strategy):
        strategy._has_position = True
        summary = strategy.get_open_positions()
        assert len(summary.positions) == 1
        assert summary.positions[0].protocol == "traderjoe_v2"
        assert summary.positions[0].chain == "avalanche"

    def test_get_open_positions_empty(self, strategy):
        strategy._has_position = False
        summary = strategy.get_open_positions()
        assert len(summary.positions) == 0


class TestStatus:
    def test_get_status_fields(self, strategy):
        status = strategy.get_status()
        assert status["strategy"] == "demo_traderjoe_paper_trade_lp"
        assert status["chain"] == "avalanche"
        assert status["config"]["pool"] == "WAVAX/USDC/20"
        assert status["config"]["bin_step"] == 20
        assert status["state"]["has_position"] is False
        assert status["state"]["ticks_with_position"] == 0
