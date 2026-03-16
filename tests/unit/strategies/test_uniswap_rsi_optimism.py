"""Tests for the Uniswap RSI Optimism strategy decision logic.

Validates:
1. Strategy config parsing
2. Buy/sell/hold decisions based on RSI thresholds
3. Intent generation with correct protocol and slippage
4. State tracking (buy/sell counts, consecutive holds)
5. Persistence and restoration
6. Teardown support

Kitchen Loop iteration 83, VIB-1359.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest


@pytest.fixture
def rsi_config():
    return {
        "trade_size_usd": "5",
        "rsi_period": 14,
        "rsi_oversold": "35",
        "rsi_overbought": "65",
        "max_slippage_bps": 100,
        "base_token": "WETH",
        "quote_token": "USDC",
    }


@pytest.fixture
def strategy(rsi_config):
    from strategies.incubating.uniswap_rsi_optimism.strategy import (
        UniswapRSIOptimismConfig,
        UniswapRSIOptimismStrategy,
    )

    config = UniswapRSIOptimismConfig(**rsi_config)
    strat = UniswapRSIOptimismStrategy.__new__(UniswapRSIOptimismStrategy)
    strat.config = config
    strat._chain = "optimism"
    strat._wallet_address = "0x" + "0" * 40
    strat._strategy_id = "test-rsi-optimism"

    strat.trade_size_usd = config.trade_size_usd
    strat.rsi_period = config.rsi_period
    strat.rsi_oversold = config.rsi_oversold
    strat.rsi_overbought = config.rsi_overbought
    strat.max_slippage_bps = config.max_slippage_bps
    strat.base_token = config.base_token
    strat.quote_token = config.quote_token
    strat._consecutive_holds = 0
    strat._total_buys = 0
    strat._total_sells = 0
    return strat


def _mock_market(rsi_value: float | None = 50.0, quote_usd: float = 10000.0, base_usd: float = 1000.0) -> MagicMock:
    market = MagicMock()

    def rsi_fn(token, period=14):
        if rsi_value is None:
            raise ValueError("RSI unavailable")
        return rsi_value

    market.rsi = MagicMock(side_effect=rsi_fn)

    # Balance mocks
    def balance_fn(token):
        bal = MagicMock()
        if token == "USDC":
            bal.balance_usd = Decimal(str(quote_usd))
        else:
            bal.balance_usd = Decimal(str(base_usd))
        return bal

    market.balance = MagicMock(side_effect=balance_fn)
    return market


class TestUniswapRSIOptimismConfig:

    def test_string_conversion(self):
        from strategies.incubating.uniswap_rsi_optimism.strategy import UniswapRSIOptimismConfig

        config = UniswapRSIOptimismConfig(
            trade_size_usd="10",
            rsi_oversold="25",
            rsi_overbought="75",
            rsi_period="20",
            max_slippage_bps="50",
        )
        assert config.trade_size_usd == Decimal("10")
        assert config.rsi_oversold == Decimal("25")
        assert config.rsi_overbought == Decimal("75")
        assert config.rsi_period == 20
        assert config.max_slippage_bps == 50

    def test_float_conversion(self):
        from strategies.incubating.uniswap_rsi_optimism.strategy import UniswapRSIOptimismConfig

        config = UniswapRSIOptimismConfig(trade_size_usd=7.5, rsi_oversold=30.0)
        assert config.trade_size_usd == Decimal("7.5")
        assert config.rsi_oversold == Decimal("30.0")

    def test_to_dict(self):
        from strategies.incubating.uniswap_rsi_optimism.strategy import UniswapRSIOptimismConfig

        config = UniswapRSIOptimismConfig()
        d = config.to_dict()
        assert d["base_token"] == "WETH"
        assert d["quote_token"] == "USDC"
        assert isinstance(d["trade_size_usd"], str)


class TestUniswapRSIOptimismDecision:

    def test_buy_when_oversold(self, strategy):
        """RSI below oversold threshold triggers buy."""
        market = _mock_market(rsi_value=25.0)
        intent = strategy.decide(market)
        assert intent is not None
        assert intent.intent_type.value == "SWAP"
        assert intent.from_token == "USDC"
        assert intent.to_token == "WETH"

    def test_sell_when_overbought(self, strategy):
        """RSI above overbought threshold triggers sell."""
        market = _mock_market(rsi_value=75.0)
        intent = strategy.decide(market)
        assert intent is not None
        assert intent.intent_type.value == "SWAP"
        assert intent.from_token == "WETH"
        assert intent.to_token == "USDC"

    def test_hold_when_neutral(self, strategy):
        """RSI in neutral zone triggers hold."""
        market = _mock_market(rsi_value=50.0)
        intent = strategy.decide(market)
        assert intent is not None
        assert intent.intent_type.value == "HOLD"
        assert "neutral zone" in intent.reason

    def test_hold_when_rsi_unavailable(self, strategy):
        """Missing RSI triggers hold with reason."""
        market = _mock_market(rsi_value=None)
        intent = strategy.decide(market)
        assert intent is not None
        assert intent.intent_type.value == "HOLD"
        assert "unavailable" in intent.reason

    def test_buy_at_exact_boundary(self, strategy):
        """RSI exactly at oversold boundary should hold (not oversold yet)."""
        market = _mock_market(rsi_value=35.0)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"

    def test_sell_at_exact_boundary(self, strategy):
        """RSI exactly at overbought boundary should hold (not overbought yet)."""
        market = _mock_market(rsi_value=65.0)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"

    def test_slippage_calculation(self, strategy):
        """Max slippage should be correctly converted from bps."""
        market = _mock_market(rsi_value=25.0)
        intent = strategy.decide(market)
        # 100 bps = 0.01
        assert intent.max_slippage == Decimal("0.01")

    def test_protocol_is_uniswap_v3(self, strategy):
        """Swap intents should target uniswap_v3 protocol."""
        market = _mock_market(rsi_value=25.0)
        intent = strategy.decide(market)
        assert intent.protocol == "uniswap_v3"

    def test_hold_when_insufficient_quote_for_buy(self, strategy):
        """Buy signal with insufficient quote balance triggers hold."""
        market = _mock_market(rsi_value=25.0, quote_usd=1.0)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "insufficient" in intent.reason

    def test_hold_when_insufficient_base_for_sell(self, strategy):
        """Sell signal with insufficient base balance triggers hold."""
        market = _mock_market(rsi_value=75.0, base_usd=1.0)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "insufficient" in intent.reason


class TestUniswapRSIOptimismStateTracking:

    def test_buy_increments_counter(self, strategy):
        market = _mock_market(rsi_value=25.0)
        strategy.decide(market)
        assert strategy._total_buys == 1
        assert strategy._consecutive_holds == 0

    def test_sell_increments_counter(self, strategy):
        market = _mock_market(rsi_value=75.0)
        strategy.decide(market)
        assert strategy._total_sells == 1
        assert strategy._consecutive_holds == 0

    def test_hold_increments_consecutive(self, strategy):
        market = _mock_market(rsi_value=50.0)
        strategy.decide(market)
        assert strategy._consecutive_holds == 1
        strategy.decide(market)
        assert strategy._consecutive_holds == 2

    def test_trade_resets_consecutive_holds(self, strategy):
        # Hold twice
        market_neutral = _mock_market(rsi_value=50.0)
        strategy.decide(market_neutral)
        strategy.decide(market_neutral)
        assert strategy._consecutive_holds == 2

        # Buy resets
        market_oversold = _mock_market(rsi_value=25.0)
        strategy.decide(market_oversold)
        assert strategy._consecutive_holds == 0


class TestUniswapRSIOptimismPersistence:

    def test_get_persistent_state(self, strategy):
        strategy._total_buys = 5
        strategy._total_sells = 3
        strategy._consecutive_holds = 2
        state = strategy.get_persistent_state()
        assert state["total_buys"] == 5
        assert state["total_sells"] == 3
        assert state["consecutive_holds"] == 2

    def test_load_persistent_state(self, strategy):
        state = {"total_buys": 10, "total_sells": 7, "consecutive_holds": 1}
        strategy.load_persistent_state(state)
        assert strategy._total_buys == 10
        assert strategy._total_sells == 7
        assert strategy._consecutive_holds == 1


class TestUniswapRSIOptimismTeardown:

    def test_supports_teardown(self, strategy):
        assert strategy.supports_teardown() is True

    def test_get_open_positions_no_market(self, strategy):
        """Without market context, positions are empty."""
        summary = strategy.get_open_positions()
        assert len(summary.positions) == 0
        assert summary.total_value_usd == Decimal("0")

    def test_generate_teardown_intents_no_market(self, strategy):
        """Without market, no teardown intents generated."""
        from almanak.framework.teardown import TeardownMode

        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 0

    def test_generate_teardown_intents_with_base_holding(self, strategy):
        """Teardown swaps base token back to quote when holdings exist."""
        from almanak.framework.teardown import TeardownMode

        market = _mock_market(base_usd=500.0)
        intents = strategy.generate_teardown_intents(TeardownMode.SOFT, market=market)
        assert len(intents) == 1
        assert intents[0].intent_type.value == "SWAP"
        assert intents[0].from_token == "WETH"
        assert intents[0].to_token == "USDC"

    def test_generate_teardown_intents_empty_when_no_base(self, strategy):
        """Teardown generates no intents when no base token held."""
        from almanak.framework.teardown import TeardownMode

        market = _mock_market(base_usd=0.0)
        intents = strategy.generate_teardown_intents(TeardownMode.SOFT, market=market)
        assert len(intents) == 0
