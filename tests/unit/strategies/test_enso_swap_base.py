"""Tests for the Enso RSI demo strategy on Base chain.

Validates RSI-based buy/sell/hold decisions, teardown intents, and
Enso protocol routing via the enso_rsi demo strategy targeting Base.

This is the first dedicated unit test for the Enso demo strategy on Base,
complementing the intent tests in tests/intents/base/test_enso_swap.py.

Kitchen Loop iteration 145, VIB-2223.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest


@pytest.fixture
def strategy():
    from almanak.demo_strategies.enso_rsi.strategy import EnsoRSIStrategy

    strat = EnsoRSIStrategy.__new__(EnsoRSIStrategy)
    strat.config = {}
    strat._chain = "base"
    strat._wallet_address = "0x" + "0" * 40
    strat._deployment_id = "test-enso-rsi-base"
    strat.trade_size_usd = Decimal("3")
    strat.rsi_oversold = 30
    strat.rsi_overbought = 70
    strat.max_slippage_pct = 0.5
    strat.base_token = "WETH"
    strat.quote_token = "USDC"
    strat.force_action = None
    strat._trades_executed = 0
    return strat


def _mock_market(
    rsi_value: float | None = 50.0,
    quote_usd: float = 10000.0,
    base_balance: float = 1.0,
    base_price: float = 3400.0,
) -> MagicMock:
    market = MagicMock()

    def rsi_fn(token):
        if rsi_value is None:
            raise ValueError("RSI unavailable")
        rsi_obj = MagicMock()
        rsi_obj.value = Decimal(str(rsi_value))
        return rsi_obj

    market.rsi = MagicMock(side_effect=rsi_fn)

    def balance_fn(token):
        bal = MagicMock()
        if token == "USDC":
            bal.balance_usd = Decimal(str(quote_usd))
            bal.balance = Decimal(str(quote_usd))
        elif token == "WETH":
            bal.balance_usd = Decimal(str(base_balance)) * Decimal(str(base_price))
            bal.balance = Decimal(str(base_balance))
        else:
            bal.balance_usd = Decimal("0")
            bal.balance = Decimal("0")
        return bal

    market.balance = MagicMock(side_effect=balance_fn)
    return market


class TestDecision:
    def test_buy_when_oversold(self, strategy):
        market = _mock_market(rsi_value=25.0)
        intent = strategy.decide(market)
        assert intent is not None
        assert intent.intent_type.value == "SWAP"
        assert intent.from_token == "USDC"
        assert intent.to_token == "WETH"
        assert intent.protocol == "enso"

    def test_sell_when_overbought(self, strategy):
        market = _mock_market(rsi_value=75.0)
        intent = strategy.decide(market)
        assert intent is not None
        assert intent.intent_type.value == "SWAP"
        assert intent.from_token == "WETH"
        assert intent.to_token == "USDC"
        assert intent.protocol == "enso"

    def test_hold_when_neutral(self, strategy):
        market = _mock_market(rsi_value=50.0)
        intent = strategy.decide(market)
        assert intent is not None
        assert intent.intent_type.value == "HOLD"

    def test_hold_at_boundary_low(self, strategy):
        market = _mock_market(rsi_value=30.0)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"

    def test_hold_at_boundary_high(self, strategy):
        market = _mock_market(rsi_value=70.0)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"

    def test_buy_just_below_oversold(self, strategy):
        market = _mock_market(rsi_value=29.9)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "SWAP"
        assert intent.from_token == "USDC"
        assert intent.to_token == "WETH"

    def test_sell_just_above_overbought(self, strategy):
        market = _mock_market(rsi_value=70.1)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "SWAP"
        assert intent.from_token == "WETH"
        assert intent.to_token == "USDC"

    def test_rsi_unavailable_defaults_to_hold(self, strategy):
        market = _mock_market(rsi_value=None)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"


class TestForceAction:
    def test_force_buy(self, strategy):
        strategy.force_action = "buy"
        market = _mock_market(rsi_value=50.0)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "SWAP"
        assert intent.from_token == "USDC"
        assert intent.to_token == "WETH"
        assert intent.protocol == "enso"

    def test_force_sell(self, strategy):
        strategy.force_action = "sell"
        market = _mock_market(rsi_value=50.0)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "SWAP"
        assert intent.from_token == "WETH"
        assert intent.to_token == "USDC"
        assert intent.protocol == "enso"

    def test_force_unknown_falls_through(self, strategy):
        strategy.force_action = "invalid"
        market = _mock_market(rsi_value=50.0)
        intent = strategy.decide(market)
        # Falls through to RSI logic, neutral -> HOLD
        assert intent.intent_type.value == "HOLD"


class TestIntentProperties:
    def test_buy_intent_slippage(self, strategy):
        strategy.max_slippage_pct = 1.0
        strategy.force_action = "buy"
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.max_slippage == Decimal("0.01")

    def test_sell_intent_slippage(self, strategy):
        strategy.max_slippage_pct = 0.5
        strategy.force_action = "sell"
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.max_slippage == Decimal("0.005")

    def test_buy_intent_amount(self, strategy):
        strategy.trade_size_usd = Decimal("100")
        strategy.force_action = "buy"
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.amount_usd == Decimal("100")

    def test_trades_counter_increments(self, strategy):
        strategy.force_action = "buy"
        market = _mock_market()
        assert strategy._trades_executed == 0
        strategy.decide(market)
        assert strategy._trades_executed == 1
        strategy.decide(market)
        assert strategy._trades_executed == 2


class TestTeardown:
    def test_teardown_soft_slippage(self, strategy):
        from almanak.framework.teardown import TeardownMode

        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 1
        assert intents[0].protocol == "enso"
        assert intents[0].from_token == "WETH"
        assert intents[0].to_token == "USDC"
        # Soft mode uses configured slippage (0.5% -> 0.005)
        assert intents[0].max_slippage == Decimal("0.005")

    def test_teardown_hard_slippage(self, strategy):
        from almanak.framework.teardown import TeardownMode

        intents = strategy.generate_teardown_intents(TeardownMode.HARD)
        assert len(intents) == 1
        assert intents[0].protocol == "enso"
        # Hard mode uses 3% slippage
        assert intents[0].max_slippage == Decimal("0.03")

    def test_teardown_swaps_all(self, strategy):
        from almanak.framework.teardown import TeardownMode

        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert intents[0].amount == "all"


class TestGetStatus:
    def test_status_contains_config(self, strategy):
        status = strategy.get_status()
        assert status["strategy"] == "demo_enso_rsi"
        assert status["chain"] == "base"
        assert status["config"]["base_token"] == "WETH"
        assert status["config"]["quote_token"] == "USDC"
        assert status["config"]["max_slippage_pct"] == 0.5

    def test_status_tracks_trades(self, strategy):
        strategy._trades_executed = 5
        status = strategy.get_status()
        assert status["state"]["trades_executed"] == 5


class TestChainConfiguration:
    def test_strategy_metadata_supports_base(self):
        from almanak.demo_strategies.enso_rsi.strategy import EnsoRSIStrategy

        meta = EnsoRSIStrategy.STRATEGY_METADATA
        assert "base" in meta.supported_chains

    def test_strategy_default_chain_is_base(self):
        from almanak.demo_strategies.enso_rsi.strategy import EnsoRSIStrategy

        meta = EnsoRSIStrategy.STRATEGY_METADATA
        assert meta.default_chain == "base"

    def test_strategy_uses_enso_protocol(self):
        from almanak.demo_strategies.enso_rsi.strategy import EnsoRSIStrategy

        meta = EnsoRSIStrategy.STRATEGY_METADATA
        assert "enso" in meta.supported_protocols
