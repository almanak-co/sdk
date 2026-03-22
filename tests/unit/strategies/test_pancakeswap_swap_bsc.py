"""Tests for the PancakeSwap V3 Swap BSC demo strategy.

Validates BUY/SELL force_action decisions, balance checks, and teardown
with PancakeSwap V3 on BSC chain.

Kitchen Loop iteration 119, VIB-1710.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest


@pytest.fixture
def strategy():
    from strategies.demo.pancakeswap_swap_bsc.strategy import PancakeSwapSwapBscStrategy

    strat = PancakeSwapSwapBscStrategy.__new__(PancakeSwapSwapBscStrategy)
    strat.config = {}
    strat._chain = "bsc"
    strat._wallet_address = "0x" + "0" * 40
    strat._strategy_id = "test-pancakeswap-swap-bsc"
    strat.trade_size_usd = Decimal("10")
    strat.max_slippage = Decimal("0.01")
    strat.base_token = "WBNB"
    strat.quote_token = "USDT"
    strat.force_action = "buy"
    return strat


def _mock_market(
    quote_usd: float = 50000.0,
    base_balance: float = 5.0,
    base_price: float = 600.0,
    quote_price: float = 1.0,
) -> MagicMock:
    market = MagicMock()

    def price_fn(token):
        if token == "WBNB":
            return Decimal(str(base_price))
        if token == "USDT":
            return Decimal(str(quote_price))
        raise ValueError(f"Unexpected token: {token}")

    market.price = MagicMock(side_effect=price_fn)

    def balance_fn(token):
        bal = MagicMock()
        if token == "USDT":
            bal.balance_usd = Decimal(str(quote_usd))
            bal.balance = Decimal(str(quote_usd))
        elif token == "WBNB":
            bal.balance_usd = Decimal(str(base_balance)) * Decimal(str(base_price))
            bal.balance = Decimal(str(base_balance))
        else:
            raise ValueError(f"Unexpected token: {token}")
        return bal

    market.balance = MagicMock(side_effect=balance_fn)
    return market


class TestBuyPhase:
    def test_buy_emits_swap_intent(self, strategy):
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "SWAP"
        assert intent.from_token == "USDT"
        assert intent.to_token == "WBNB"

    def test_buy_uses_pancakeswap_v3(self, strategy):
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.protocol == "pancakeswap_v3"

    def test_buy_amount_matches_config(self, strategy):
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.amount_usd == Decimal("10")

    def test_buy_slippage_matches_config(self, strategy):
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.max_slippage == Decimal("0.01")

    def test_buy_hold_when_insufficient_quote(self, strategy):
        market = _mock_market(quote_usd=1.0)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "Insufficient" in intent.reason

    def test_buy_proceeds_when_quote_balance_unavailable(self, strategy):
        market = _mock_market()
        market.balance = MagicMock(side_effect=ValueError("balance unavailable"))
        intent = strategy.decide(market)
        assert intent.intent_type.value == "SWAP"
        assert intent.from_token == "USDT"
        assert intent.to_token == "WBNB"


class TestSellPhase:
    def test_sell_emits_swap_intent(self, strategy):
        strategy.force_action = "sell"
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "SWAP"
        assert intent.from_token == "WBNB"
        assert intent.to_token == "USDT"

    def test_sell_uses_pancakeswap_v3(self, strategy):
        strategy.force_action = "sell"
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.protocol == "pancakeswap_v3"

    def test_sell_hold_when_insufficient_base(self, strategy):
        strategy.force_action = "sell"
        market = _mock_market(base_balance=0.001, base_price=600.0)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "Insufficient" in intent.reason

    def test_sell_proceeds_when_base_balance_unavailable(self, strategy):
        strategy.force_action = "sell"
        market = _mock_market()
        market.balance = MagicMock(side_effect=ValueError("balance unavailable"))
        intent = strategy.decide(market)
        assert intent.intent_type.value == "SWAP"
        assert intent.from_token == "WBNB"
        assert intent.to_token == "USDT"


class TestForceAction:
    def test_unknown_force_action_holds(self, strategy):
        strategy.force_action = "invalid"
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "Unknown" in intent.reason


class TestTeardown:
    def test_teardown_empty(self, strategy):
        positions = strategy.get_open_positions()
        assert len(positions.positions) == 0

    def test_teardown_intents_empty(self, strategy):
        intents = strategy.generate_teardown_intents()
        assert intents == []


class TestMetadata:
    def test_strategy_name(self):
        from strategies.demo.pancakeswap_swap_bsc.strategy import PancakeSwapSwapBscStrategy

        assert PancakeSwapSwapBscStrategy.STRATEGY_NAME == "demo_pancakeswap_swap_bsc"

    def test_supported_chains(self):
        from strategies.demo.pancakeswap_swap_bsc.strategy import PancakeSwapSwapBscStrategy

        assert "bsc" in PancakeSwapSwapBscStrategy.STRATEGY_METADATA.supported_chains

    def test_supported_protocols(self):
        from strategies.demo.pancakeswap_swap_bsc.strategy import PancakeSwapSwapBscStrategy

        assert "pancakeswap_v3" in PancakeSwapSwapBscStrategy.STRATEGY_METADATA.supported_protocols
