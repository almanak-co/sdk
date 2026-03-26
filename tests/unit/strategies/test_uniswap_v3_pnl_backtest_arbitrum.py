"""Tests for Uniswap V3 RSI PnL Backtest strategy on Arbitrum.

Validates RSI-based swap decisions, cooldown mechanism, state persistence,
teardown, and intent generation for PnL backtesting.

Kitchen Loop iteration 129, VIB-1926.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest


@pytest.fixture
def strategy():
    from strategies.incubating.uniswap_v3_pnl_backtest_arbitrum.strategy import (
        UniswapV3PnLBacktestArbitrumStrategy,
    )

    strat = UniswapV3PnLBacktestArbitrumStrategy.__new__(UniswapV3PnLBacktestArbitrumStrategy)
    strat.config = {}
    strat._chain = "arbitrum"
    strat._wallet_address = "0x" + "0" * 40
    strat._strategy_id = "test-uniswap-v3-pnl-backtest-arbitrum"
    strat.trade_size_usd = Decimal("50")
    strat.base_token = "WETH"
    strat.quote_token = "USDC"
    strat.rsi_period = 14
    strat.rsi_oversold = Decimal("35")
    strat.rsi_overbought = Decimal("65")
    strat.max_slippage_bps = 100
    strat.max_slippage = Decimal("0.01")
    strat.cooldown_ticks = 2
    strat._consecutive_holds = 0
    strat._total_buys = 0
    strat._total_sells = 0
    strat._ticks_since_last_trade = 2  # No cooldown initially
    strat._last_direction = None
    return strat


def _mock_market(
    rsi_value: float | None = 50.0,
    weth_price: float = 3000.0,
    weth_balance: float = 1.0,
    usdc_balance: float = 10000.0,
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
        if token == "WETH":
            return Decimal(str(weth_price))
        elif token == "USDC":
            return Decimal("1")
        raise ValueError(f"Unknown token: {token}")

    market.price = MagicMock(side_effect=price_fn)

    def balance_fn(token):
        bal = MagicMock()
        if token == "WETH":
            bal.balance = Decimal(str(weth_balance))
            bal.balance_usd = Decimal(str(weth_balance)) * Decimal(str(weth_price))
        elif token == "USDC":
            bal.balance = Decimal(str(usdc_balance))
            bal.balance_usd = Decimal(str(usdc_balance))
        return bal

    market.balance = MagicMock(side_effect=balance_fn)
    return market


class TestDecision:
    def test_buy_when_oversold(self, strategy):
        """Buys when RSI is below oversold threshold."""
        market = _mock_market(rsi_value=30.0)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "SWAP"
        assert intent.from_token == "USDC"
        assert intent.to_token == "WETH"

    def test_sell_when_overbought(self, strategy):
        """Sells when RSI is above overbought threshold."""
        market = _mock_market(rsi_value=70.0)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "SWAP"
        assert intent.from_token == "WETH"
        assert intent.to_token == "USDC"

    def test_hold_in_neutral_zone(self, strategy):
        """Holds when RSI is in neutral zone."""
        market = _mock_market(rsi_value=50.0)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "RSI=50" in intent.reason

    def test_hold_when_on_cooldown(self, strategy):
        """Holds even with signal if cooldown not expired."""
        strategy._ticks_since_last_trade = 0  # Just traded
        market = _mock_market(rsi_value=25.0)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "cooldown" in intent.reason

    def test_trade_after_cooldown(self, strategy):
        """Trades when cooldown has expired."""
        strategy._ticks_since_last_trade = 1  # Will be 2 after increment
        market = _mock_market(rsi_value=25.0)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "SWAP"

    def test_hold_when_insufficient_quote(self, strategy):
        """Holds on buy signal if insufficient USDC."""
        market = _mock_market(rsi_value=25.0, usdc_balance=10.0)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "insufficient" in intent.reason.lower()

    def test_hold_when_insufficient_base(self, strategy):
        """Holds on sell signal if insufficient WETH."""
        market = _mock_market(rsi_value=70.0, weth_balance=0.0001)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "insufficient" in intent.reason.lower()

    def test_hold_on_rsi_unavailable(self, strategy):
        """Holds when RSI data is unavailable."""
        market = _mock_market(rsi_value=None)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"

    def test_hold_on_price_error(self, strategy):
        """Holds when price is unavailable."""
        market = MagicMock()
        market.price = MagicMock(side_effect=ValueError("price unavailable"))
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"

    def test_hold_on_zero_price(self, strategy):
        """Holds when base price is zero."""
        market = _mock_market(weth_price=0.0, rsi_value=25.0)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"

    def test_consecutive_holds_count(self, strategy):
        """Consecutive holds counter increments."""
        market = _mock_market(rsi_value=50.0)
        strategy.decide(market)
        assert strategy._consecutive_holds == 1
        strategy.decide(market)
        assert strategy._consecutive_holds == 2

    def test_buy_resets_holds(self, strategy):
        """Buy signal resets consecutive holds counter."""
        strategy._consecutive_holds = 5
        market = _mock_market(rsi_value=25.0)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "SWAP"
        assert strategy._consecutive_holds == 0

    def test_boundary_rsi_oversold(self, strategy):
        """RSI exactly at oversold threshold triggers buy."""
        market = _mock_market(rsi_value=35.0)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "SWAP"
        assert intent.from_token == "USDC"

    def test_boundary_rsi_overbought(self, strategy):
        """RSI exactly at overbought threshold triggers sell."""
        market = _mock_market(rsi_value=65.0)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "SWAP"
        assert intent.from_token == "WETH"


class TestIntentCreation:
    def test_buy_intent_protocol(self, strategy):
        """Buy intent uses uniswap_v3 protocol."""
        market = _mock_market(rsi_value=25.0)
        intent = strategy.decide(market)
        assert intent.protocol == "uniswap_v3"

    def test_sell_intent_protocol(self, strategy):
        """Sell intent uses uniswap_v3 protocol."""
        market = _mock_market(rsi_value=70.0)
        intent = strategy.decide(market)
        assert intent.protocol == "uniswap_v3"

    def test_slippage_conversion(self, strategy):
        """Max slippage is converted from bps to decimal."""
        market = _mock_market(rsi_value=25.0)
        intent = strategy.decide(market)
        assert intent.max_slippage == Decimal("0.01")  # 100 bps = 1%


class TestOnIntentExecuted:
    def test_buy_tracks_count(self, strategy):
        mock_intent = MagicMock()
        mock_intent.intent_type.value = "SWAP"
        mock_intent.from_token = "USDC"
        mock_intent.to_token = "WETH"
        mock_result = MagicMock()
        mock_result.swap_amounts = None

        strategy.on_intent_executed(mock_intent, True, mock_result)
        assert strategy._total_buys == 1
        assert strategy._last_direction == "buy"
        assert strategy._ticks_since_last_trade == 0

    def test_sell_tracks_count(self, strategy):
        mock_intent = MagicMock()
        mock_intent.intent_type.value = "SWAP"
        mock_intent.from_token = "WETH"
        mock_intent.to_token = "USDC"
        mock_result = MagicMock()
        mock_result.swap_amounts = None

        strategy.on_intent_executed(mock_intent, True, mock_result)
        assert strategy._total_sells == 1
        assert strategy._last_direction == "sell"

    def test_no_update_on_failure(self, strategy):
        mock_intent = MagicMock()
        mock_intent.intent_type.value = "SWAP"
        strategy.on_intent_executed(mock_intent, False, MagicMock())
        assert strategy._total_buys == 0
        assert strategy._total_sells == 0


class TestStatePersistence:
    def test_get_persistent_state(self, strategy):
        strategy._total_buys = 3
        strategy._total_sells = 2
        strategy._consecutive_holds = 5
        strategy._ticks_since_last_trade = 1
        strategy._last_direction = "sell"

        state = strategy.get_persistent_state()
        assert state["total_buys"] == 3
        assert state["total_sells"] == 2
        assert state["consecutive_holds"] == 5
        assert state["ticks_since_last_trade"] == 1
        assert state["last_direction"] == "sell"

    def test_load_persistent_state(self, strategy):
        strategy.load_persistent_state({
            "total_buys": 4,
            "total_sells": 3,
            "consecutive_holds": 10,
            "ticks_since_last_trade": 0,
            "last_direction": "buy",
        })
        assert strategy._total_buys == 4
        assert strategy._total_sells == 3
        assert strategy._consecutive_holds == 10
        assert strategy._ticks_since_last_trade == 0
        assert strategy._last_direction == "buy"

    def test_load_empty_state(self, strategy):
        strategy.load_persistent_state({})
        assert strategy._total_buys == 0
        assert strategy._total_sells == 0
        assert strategy._last_direction is None


class TestTeardown:
    def test_supports_teardown(self, strategy):
        assert strategy.supports_teardown() is True

    def test_teardown_swaps_base_to_quote(self, strategy):
        from almanak.framework.teardown import TeardownMode

        strategy._total_buys = 3
        strategy._total_sells = 1
        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 1
        assert intents[0].intent_type.value == "SWAP"
        assert intents[0].from_token == "WETH"
        assert intents[0].to_token == "USDC"

    def test_teardown_hard_wider_slippage(self, strategy):
        from almanak.framework.teardown import TeardownMode

        strategy._total_buys = 2
        strategy._total_sells = 0
        soft_intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        hard_intents = strategy.generate_teardown_intents(TeardownMode.HARD)
        assert hard_intents[0].max_slippage > soft_intents[0].max_slippage

    def test_teardown_always_emits_when_flat(self, strategy):
        """Teardown always emits swap even when buys == sells (residual exposure possible)."""
        from almanak.framework.teardown import TeardownMode

        strategy._total_buys = 3
        strategy._total_sells = 3
        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 1
        assert intents[0].amount == "all"

    def test_teardown_emits_before_any_trades(self, strategy):
        """Teardown emits swap even before trades (amount=all is safe with zero balance)."""
        from almanak.framework.teardown import TeardownMode

        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 1

    def test_open_positions_with_trades(self, strategy):
        """Positions reported when any buys occurred."""
        strategy._total_buys = 5
        strategy._total_sells = 3
        summary = strategy.get_open_positions()
        assert len(summary.positions) == 1
        assert summary.positions[0].protocol == "uniswap_v3"
        assert summary.positions[0].details["total_buys"] == 5

    def test_open_positions_reported_when_flat(self, strategy):
        """Positions still reported when buys == sells (residual exposure possible)."""
        strategy._total_buys = 3
        strategy._total_sells = 3
        summary = strategy.get_open_positions()
        assert len(summary.positions) == 1

    def test_open_positions_empty_when_no_trades(self, strategy):
        """No positions reported before any trades."""
        summary = strategy.get_open_positions()
        assert len(summary.positions) == 0


class TestStatus:
    def test_get_status(self, strategy):
        strategy._total_buys = 2
        strategy._total_sells = 1
        status = strategy.get_status()
        assert status["strategy"] == "uniswap_v3_pnl_backtest_arbitrum"
        assert status["chain"] == "arbitrum"
        assert status["state"]["total_buys"] == 2
        assert status["state"]["total_sells"] == 1
        assert status["config"]["pair"] == "WETH/USDC"


class TestLifecycle:
    def test_buy_hold_sell_cycle(self, strategy):
        """Simulates buy -> cooldown holds -> sell cycle."""
        # Buy signal
        market_oversold = _mock_market(rsi_value=25.0)
        intent = strategy.decide(market_oversold)
        assert intent.intent_type.value == "SWAP"
        assert intent.from_token == "USDC"

        # Simulate execution
        mock_intent = MagicMock()
        mock_intent.intent_type.value = "SWAP"
        mock_intent.from_token = "USDC"
        mock_intent.to_token = "WETH"
        mock_result = MagicMock()
        mock_result.swap_amounts = None
        strategy.on_intent_executed(mock_intent, True, mock_result)
        assert strategy._total_buys == 1

        # Cooldown hold (still oversold but on cooldown)
        intent = strategy.decide(market_oversold)
        assert intent.intent_type.value == "HOLD"
        assert "cooldown" in intent.reason

        # Neutral hold
        market_neutral = _mock_market(rsi_value=50.0)
        intent = strategy.decide(market_neutral)
        assert intent.intent_type.value == "HOLD"

        # Sell signal (cooldown expired after 2 holds)
        market_overbought = _mock_market(rsi_value=70.0)
        intent = strategy.decide(market_overbought)
        assert intent.intent_type.value == "SWAP"
        assert intent.from_token == "WETH"

        # Simulate execution
        mock_intent.from_token = "WETH"
        mock_intent.to_token = "USDC"
        strategy.on_intent_executed(mock_intent, True, mock_result)
        assert strategy._total_sells == 1
        assert strategy._total_buys == 1
