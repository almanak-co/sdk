"""Tests for the Curve CryptoSwap PnL Backtest demo strategy.

Validates RSI-based buy/sell/hold decisions, teardown, and intent generation
with Curve CryptoSwap protocol on Ethereum.

Kitchen Loop iteration 89, VIB-1429.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest


@pytest.fixture
def strategy():
    from strategies.demo.curve_cryptoswap_pnl.strategy import CurveCryptoSwapPnLStrategy

    strat = CurveCryptoSwapPnLStrategy.__new__(CurveCryptoSwapPnLStrategy)
    strat.config = {}
    strat._chain = "ethereum"
    strat._wallet_address = "0x" + "0" * 40
    strat._strategy_id = "test-curve-cryptoswap-pnl"
    strat.trade_size_usd = Decimal("100")
    strat.rsi_period = 14
    strat.rsi_oversold = Decimal("40")
    strat.rsi_overbought = Decimal("70")
    strat.max_slippage_bps = 100
    strat.base_token = "WETH"
    strat.quote_token = "USDT"
    strat._consecutive_holds = 0
    strat._has_position = False
    return strat


def _mock_market(
    rsi_value: float | None = 50.0,
    quote_usd: float = 50000.0,
    base_balance: float = 1.0,
    base_price: float = 3400.0,
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
        return Decimal(str(base_price))

    market.price = MagicMock(side_effect=price_fn)

    def balance_fn(token):
        bal = MagicMock()
        if token == "USDT":
            bal.balance_usd = Decimal(str(quote_usd))
            bal.balance = Decimal(str(quote_usd))
        elif token == "WETH":
            bal.balance_usd = Decimal(str(base_balance)) * Decimal(str(base_price))
            bal.balance = Decimal(str(base_balance))
        else:
            raise ValueError(f"Unexpected token requested: {token}")
        return bal

    market.balance = MagicMock(side_effect=balance_fn)
    return market


class TestDecision:
    def test_buy_when_oversold(self, strategy):
        market = _mock_market(rsi_value=30.0)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "SWAP"
        assert intent.from_token == "USDT"
        assert intent.to_token == "WETH"

    def test_sell_when_overbought(self, strategy):
        market = _mock_market(rsi_value=80.0)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "SWAP"
        assert intent.from_token == "WETH"
        assert intent.to_token == "USDT"

    def test_hold_when_neutral(self, strategy):
        market = _mock_market(rsi_value=50.0)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "neutral" in intent.reason

    def test_hold_when_price_unavailable(self, strategy):
        market = _mock_market(rsi_value=50.0)
        market.price = MagicMock(side_effect=ValueError("Price unavailable"))
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "unavailable" in intent.reason.lower()

    def test_hold_when_price_zero(self, strategy):
        market = _mock_market(rsi_value=80.0, base_price=0.0)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "invalid" in intent.reason.lower()

    def test_hold_when_rsi_unavailable(self, strategy):
        market = _mock_market(rsi_value=None)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "unavailable" in intent.reason

    def test_protocol_is_curve(self, strategy):
        market = _mock_market(rsi_value=30.0)
        intent = strategy.decide(market)
        assert intent.protocol == "curve"

    def test_slippage_from_bps(self, strategy):
        market = _mock_market(rsi_value=30.0)
        intent = strategy.decide(market)
        assert intent.max_slippage == Decimal("0.01")  # 100 bps

    def test_hold_insufficient_quote_for_buy(self, strategy):
        market = _mock_market(rsi_value=30.0, quote_usd=10.0)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "insufficient" in intent.reason.lower()

    def test_hold_insufficient_base_for_sell(self, strategy):
        market = _mock_market(rsi_value=80.0, base_balance=0.0001, base_price=3400.0)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "insufficient" in intent.reason.lower()

    def test_buy_at_boundary(self, strategy):
        """RSI exactly at oversold threshold triggers buy (<=)."""
        market = _mock_market(rsi_value=40.0)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "SWAP"
        assert intent.from_token == "USDT"

    def test_sell_at_boundary(self, strategy):
        """RSI exactly at overbought threshold triggers sell (>=)."""
        market = _mock_market(rsi_value=70.0)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "SWAP"
        assert intent.from_token == "WETH"

    def test_consecutive_holds_counter(self, strategy):
        market = _mock_market(rsi_value=50.0)
        strategy.decide(market)
        assert strategy._consecutive_holds == 1
        strategy.decide(market)
        assert strategy._consecutive_holds == 2

    def test_trade_resets_consecutive_holds(self, strategy):
        market_neutral = _mock_market(rsi_value=50.0)
        strategy.decide(market_neutral)
        strategy.decide(market_neutral)
        assert strategy._consecutive_holds == 2

        market_buy = _mock_market(rsi_value=30.0)
        strategy.decide(market_buy)
        assert strategy._consecutive_holds == 0


class TestOnIntentExecuted:
    def test_buy_sets_has_position(self, strategy):
        intent = MagicMock()
        intent.intent_type.value = "SWAP"
        intent.from_token = "USDT"
        intent.to_token = "WETH"

        assert strategy._has_position is False
        strategy.on_intent_executed(intent, success=True, result=None)
        assert strategy._has_position is True

    def test_sell_clears_has_position(self, strategy):
        strategy._has_position = True
        intent = MagicMock()
        intent.intent_type.value = "SWAP"
        intent.from_token = "WETH"
        intent.to_token = "USDT"

        strategy.on_intent_executed(intent, success=True, result=None)
        assert strategy._has_position is False

    def test_failed_intent_does_not_change_state(self, strategy):
        intent = MagicMock()
        intent.intent_type.value = "SWAP"
        intent.from_token = "USDT"
        intent.to_token = "WETH"

        strategy.on_intent_executed(intent, success=False, result=None)
        assert strategy._has_position is False


class TestStatePersistence:
    def test_get_persistent_state(self, strategy):
        strategy._has_position = True
        strategy._consecutive_holds = 5
        state = strategy.get_persistent_state()
        assert state["has_position"] is True
        assert state["consecutive_holds"] == 5

    def test_load_persistent_state(self, strategy):
        strategy.load_persistent_state({"has_position": True, "consecutive_holds": 3})
        assert strategy._has_position is True
        assert strategy._consecutive_holds == 3


class TestTeardown:
    def test_supports_teardown(self, strategy):
        assert strategy.supports_teardown() is True

    def test_no_positions_when_no_trades(self, strategy):
        """get_open_positions() returns empty when no position is held."""
        summary = strategy.get_open_positions()
        assert len(summary.positions) == 0

    def test_positions_reported_after_buy(self, strategy):
        """get_open_positions() returns position after a buy."""
        strategy._has_position = True
        summary = strategy.get_open_positions()
        assert len(summary.positions) == 1
        assert summary.positions[0].protocol == "curve"
        assert summary.positions[0].chain == "ethereum"
        assert summary.positions[0].value_usd == Decimal("0")

    def test_teardown_intents_when_has_position(self, strategy):
        from almanak.framework.teardown import TeardownMode

        strategy._has_position = True
        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 1
        assert intents[0].intent_type.value == "SWAP"
        assert intents[0].from_token == "WETH"
        assert intents[0].to_token == "USDT"
        assert intents[0].protocol == "curve"

    def test_teardown_intents_empty_when_no_position(self, strategy):
        from almanak.framework.teardown import TeardownMode

        strategy._has_position = False
        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 0

    def test_teardown_hard_slippage(self, strategy):
        from almanak.framework.teardown import TeardownMode

        strategy._has_position = True
        intents = strategy.generate_teardown_intents(TeardownMode.HARD)
        assert intents[0].max_slippage == Decimal("0.03")

    def test_teardown_soft_slippage(self, strategy):
        from almanak.framework.teardown import TeardownMode

        strategy._has_position = True
        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert intents[0].max_slippage == Decimal("0.01")  # 100 bps


class TestStatus:
    def test_get_status_fields(self, strategy):
        status = strategy.get_status()
        assert status["strategy"] == "demo_curve_cryptoswap_pnl"
        assert status["chain"] == "ethereum"
        assert status["config"]["pair"] == "WETH/USDT"
        assert status["state"]["consecutive_holds"] == 0
