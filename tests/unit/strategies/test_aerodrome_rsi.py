"""Tests for the Aerodrome RSI demo strategy.

Validates RSI-based buy/sell/hold decisions, teardown, and intent generation
with Aerodrome protocol on Base chain.

Kitchen Loop iteration 89, VIB-1398.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest


@pytest.fixture
def strategy():
    from strategies.demo.aerodrome_rsi.strategy import AerodromeRSIStrategy

    strat = AerodromeRSIStrategy.__new__(AerodromeRSIStrategy)
    strat.config = {}
    strat._chain = "base"
    strat._wallet_address = "0x" + "0" * 40
    strat._strategy_id = "test-aerodrome-rsi"
    strat.trade_size_usd = Decimal("3")
    strat.rsi_period = 14
    strat.rsi_oversold = Decimal("40")
    strat.rsi_overbought = Decimal("70")
    strat.max_slippage_bps = 100
    strat.base_token = "WETH"
    strat.quote_token = "USDC"
    strat._consecutive_holds = 0
    return strat


def _mock_market(
    rsi_value: float | None = 50.0,
    quote_usd: float = 10000.0,
    base_balance: float = 1.0,
    base_price: float = 3400.0,
) -> MagicMock:
    market = MagicMock()

    def rsi_fn(token, period=14):
        assert token == "WETH", f"RSI requested for unexpected token: {token}"
        assert period == 14, f"RSI requested with unexpected period: {period}"
        if rsi_value is None:
            raise ValueError("RSI unavailable")
        rsi_obj = MagicMock()
        rsi_obj.value = Decimal(str(rsi_value))
        return rsi_obj

    market.rsi = MagicMock(side_effect=rsi_fn)

    def price_fn(token):
        assert token == "WETH", f"Price requested for unexpected token: {token}"
        return Decimal(str(base_price))

    market.price = MagicMock(side_effect=price_fn)

    def balance_fn(token):
        bal = MagicMock()
        if token == "USDC":
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
        assert intent.from_token == "USDC"
        assert intent.to_token == "WETH"

    def test_sell_when_overbought(self, strategy):
        market = _mock_market(rsi_value=80.0)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "SWAP"
        assert intent.from_token == "WETH"
        assert intent.to_token == "USDC"

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

    def test_hold_when_rsi_unavailable(self, strategy):
        market = _mock_market(rsi_value=None)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "unavailable" in intent.reason

    def test_protocol_is_aerodrome(self, strategy):
        market = _mock_market(rsi_value=30.0)
        intent = strategy.decide(market)
        assert intent.protocol == "aerodrome"

    def test_slippage_from_bps(self, strategy):
        market = _mock_market(rsi_value=30.0)
        intent = strategy.decide(market)
        assert intent.max_slippage == Decimal("0.01")  # 100 bps

    def test_hold_insufficient_quote_for_buy(self, strategy):
        market = _mock_market(rsi_value=30.0, quote_usd=1.0)
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
        assert intent.from_token == "USDC"

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


class TestTeardown:
    def test_supports_teardown(self, strategy):
        assert strategy.supports_teardown() is True

    def test_teardown_intents(self, strategy):
        from almanak.framework.teardown import TeardownMode

        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 1
        assert intents[0].intent_type.value == "SWAP"
        assert intents[0].from_token == "WETH"
        assert intents[0].to_token == "USDC"
        assert intents[0].protocol == "aerodrome"

    def test_teardown_hard_slippage(self, strategy):
        from almanak.framework.teardown import TeardownMode

        intents = strategy.generate_teardown_intents(TeardownMode.HARD)
        assert intents[0].max_slippage == Decimal("0.03")

    def test_teardown_soft_slippage(self, strategy):
        from almanak.framework.teardown import TeardownMode

        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert intents[0].max_slippage == Decimal("0.01")  # 100 bps

    def test_get_open_positions(self, strategy):
        mock_market = MagicMock()
        balance_obj = MagicMock()
        balance_obj.balance = Decimal("0.5")
        balance_obj.balance_usd = Decimal("1700")
        mock_market.balance.return_value = balance_obj
        strategy.create_market_snapshot = MagicMock(return_value=mock_market)

        summary = strategy.get_open_positions()
        assert len(summary.positions) == 1
        assert summary.positions[0].protocol == "aerodrome"
        assert summary.positions[0].chain == "base"

    def test_get_open_positions_no_balance(self, strategy):
        mock_market = MagicMock()
        balance_obj = MagicMock()
        balance_obj.balance = Decimal("0")
        balance_obj.balance_usd = Decimal("0")
        mock_market.balance.return_value = balance_obj
        strategy.create_market_snapshot = MagicMock(return_value=mock_market)

        summary = strategy.get_open_positions()
        assert len(summary.positions) == 0
