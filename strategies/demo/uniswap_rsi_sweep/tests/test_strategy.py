"""Unit tests for the Uniswap RSI Sweep Strategy.

Tests validate:
1. Strategy initialization with default and overridden config
2. RSI-driven buy/sell/hold decisions
3. Sweep parameter overrides produce different behavior
4. Teardown interface compliance
5. Edge cases (boundaries, insufficient balance)
"""

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from strategies.demo.uniswap_rsi_sweep.strategy import UniswapRSISweepStrategy


def _create_strategy(config_overrides=None):
    """Create a strategy instance with default config."""
    config = {
        "trade_size_usd": "3",
        "rsi_period": 14,
        "rsi_oversold": "30",
        "rsi_overbought": "70",
        "max_slippage_bps": 100,
        "base_token": "WETH",
        "quote_token": "USDC",
    }
    if config_overrides:
        config.update(config_overrides)
    return UniswapRSISweepStrategy(
        config=config,
        chain="arbitrum",
        wallet_address="0x" + "a" * 40,
    )


@pytest.fixture
def strategy():
    return _create_strategy()


def _make_market(
    rsi_value: Decimal,
    base_price: Decimal = Decimal("3000"),
    quote_balance_usd: Decimal = Decimal("10000"),
    base_balance: Decimal = Decimal("5"),
) -> MagicMock:
    """Create a mock MarketSnapshot with given RSI and balances."""
    market = MagicMock()
    market.price.return_value = base_price

    rsi_mock = MagicMock()
    rsi_mock.value = rsi_value
    market.rsi.return_value = rsi_mock

    quote_bal = MagicMock()
    quote_bal.balance = quote_balance_usd / Decimal("1")  # raw amount
    quote_bal.balance_usd = quote_balance_usd
    base_bal = MagicMock()
    base_bal.balance = base_balance
    base_bal.balance_usd = base_balance * base_price

    def _balance(token):
        if token == "USDC":
            return quote_bal
        return base_bal

    market.balance.side_effect = _balance
    return market


class TestInitialization:
    def test_default_config(self, strategy):
        assert strategy.rsi_period == 14
        assert strategy.rsi_oversold == Decimal("30")
        assert strategy.rsi_overbought == Decimal("70")
        assert strategy.trade_size_usd == Decimal("3")
        assert strategy.base_token == "WETH"
        assert strategy.quote_token == "USDC"

    def test_sweep_parameter_override(self):
        """Sweep engine overrides config — verify they take effect."""
        s = _create_strategy({
            "rsi_period": 20,
            "rsi_oversold": "25",
            "rsi_overbought": "75",
            "trade_size_usd": "10",
        })
        assert s.rsi_period == 20
        assert s.rsi_oversold == Decimal("25")
        assert s.rsi_overbought == Decimal("75")
        assert s.trade_size_usd == Decimal("10")


class TestDecisions:
    def test_buy_when_oversold(self, strategy):
        """RSI below oversold threshold triggers buy."""
        market = _make_market(Decimal("25"))
        intent = strategy.decide(market)
        assert intent.intent_type.value == "SWAP"
        assert intent.from_token == "USDC"
        assert intent.to_token == "WETH"

    def test_buy_when_oversold_does_not_require_price(self, strategy):
        """Oversold buy path does not need base_price — regression for deferred lookup."""
        market = _make_market(Decimal("25"))
        market.price.side_effect = ValueError("No price")
        intent = strategy.decide(market)
        assert intent.intent_type.value == "SWAP"
        assert intent.from_token == "USDC"
        assert intent.to_token == "WETH"

    def test_sell_when_overbought(self, strategy):
        """RSI above overbought threshold triggers sell."""
        market = _make_market(Decimal("75"))
        intent = strategy.decide(market)
        assert intent.intent_type.value == "SWAP"
        assert intent.from_token == "WETH"
        assert intent.to_token == "USDC"

    def test_hold_in_neutral_zone(self, strategy):
        """RSI in neutral zone triggers hold."""
        market = _make_market(Decimal("50"))
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"

    def test_hold_at_exact_oversold_boundary(self, strategy):
        """RSI exactly at oversold threshold triggers buy (<=)."""
        market = _make_market(Decimal("30"))
        intent = strategy.decide(market)
        assert intent.intent_type.value == "SWAP"
        assert intent.from_token == "USDC"

    def test_hold_at_exact_overbought_boundary(self, strategy):
        """RSI exactly at overbought threshold triggers sell (>=)."""
        market = _make_market(Decimal("70"))
        intent = strategy.decide(market)
        assert intent.intent_type.value == "SWAP"
        assert intent.from_token == "WETH"

    def test_hold_when_insufficient_quote_balance(self, strategy):
        """Buy signal but insufficient USDC balance -> hold."""
        market = _make_market(Decimal("25"), quote_balance_usd=Decimal("1"))
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "insufficient" in intent.reason.lower()

    def test_hold_when_insufficient_base_balance(self, strategy):
        """Sell signal but insufficient WETH balance -> hold."""
        market = _make_market(Decimal("75"), base_balance=Decimal("0.0000001"))
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "insufficient" in intent.reason.lower()

    def test_hold_when_rsi_unavailable(self, strategy):
        """Hold when RSI data isn't available."""
        market = MagicMock()
        market.price.return_value = Decimal("3000")
        market.rsi.side_effect = ValueError("Not enough data")
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "unavailable" in intent.reason.lower()


class TestSweepParameterVariation:
    """Verify that different sweep parameters produce meaningfully different behavior."""

    def test_narrow_vs_wide_rsi_range(self):
        """Different RSI thresholds produce different decisions for the same RSI value."""
        narrow = _create_strategy({"rsi_oversold": "40", "rsi_overbought": "60"})
        wide = _create_strategy({"rsi_oversold": "20", "rsi_overbought": "80"})

        # RSI=35: below narrow's oversold(40) -> buy; above wide's oversold(20) -> hold
        market = _make_market(Decimal("35"))
        assert narrow.decide(market).intent_type.value == "SWAP"  # buy signal
        assert wide.decide(market).intent_type.value == "HOLD"  # neutral

        # RSI=65: above narrow's overbought(60) -> sell; below wide's overbought(80) -> hold
        market2 = _make_market(Decimal("65"))
        narrow_intent = narrow.decide(market2)
        assert narrow_intent.intent_type.value == "SWAP"
        assert narrow_intent.from_token == "WETH"  # selling
        assert wide.decide(market2).intent_type.value == "HOLD"

    def test_different_trade_sizes_in_intent(self):
        """Different trade_size_usd values appear in generated intents."""
        small = _create_strategy({"trade_size_usd": "3"})
        large = _create_strategy({"trade_size_usd": "50"})

        market = _make_market(Decimal("25"))

        small_intent = small.decide(market)
        large_intent = large.decide(market)

        assert small_intent.amount_usd == Decimal("3")
        assert large_intent.amount_usd == Decimal("50")

    def test_rsi_period_passed_to_market(self):
        """Verify strategy passes correct RSI period to market.rsi()."""
        s10 = _create_strategy({"rsi_period": 10})
        s20 = _create_strategy({"rsi_period": 20})

        market10 = _make_market(Decimal("50"))
        market20 = _make_market(Decimal("50"))

        s10.decide(market10)
        s20.decide(market20)

        market10.rsi.assert_called_once_with("WETH", period=10)
        market20.rsi.assert_called_once_with("WETH", period=20)


class TestTeardown:
    def test_supports_teardown(self, strategy):
        assert strategy.supports_teardown() is True

    def test_teardown_soft(self, strategy):
        from almanak.framework.teardown import TeardownMode

        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 1
        assert intents[0].intent_type.value == "SWAP"
        assert intents[0].from_token == "WETH"
        assert intents[0].to_token == "USDC"

    def test_teardown_hard_higher_slippage(self, strategy):
        from almanak.framework.teardown import TeardownMode

        soft_intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        hard_intents = strategy.generate_teardown_intents(TeardownMode.HARD)

        assert hard_intents[0].max_slippage > soft_intents[0].max_slippage

    def test_open_positions(self, strategy):
        mock_market = MagicMock()
        balance_obj = MagicMock()
        balance_obj.balance = Decimal("0.5")
        balance_obj.balance_usd = Decimal("1700")
        mock_market.balance.return_value = balance_obj
        strategy.create_market_snapshot = MagicMock(return_value=mock_market)

        summary = strategy.get_open_positions()
        assert len(summary.positions) == 1
        assert summary.positions[0].position_type.value == "TOKEN"
        assert summary.positions[0].chain == "arbitrum"


class TestStatus:
    def test_status_includes_sweep_params(self, strategy):
        status = strategy.get_status()
        assert status["config"]["rsi_period"] == 14
        assert status["config"]["rsi_oversold"] == "30"
        assert status["config"]["rsi_overbought"] == "70"
        assert status["config"]["trade_size_usd"] == "3"

    def test_trade_counter(self, strategy):
        market = _make_market(Decimal("25"))
        strategy.decide(market)
        assert strategy._total_trades == 1
        strategy.decide(market)
        assert strategy._total_trades == 2

        status = strategy.get_status()
        assert status["state"]["total_trades"] == 2
