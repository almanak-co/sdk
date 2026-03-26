"""Unit tests for Uniswap V3 RSI Sweep Arbitrum Strategy.

Tests validate:
1. Strategy initialization with Arbitrum config and production-scale defaults
2. RSI-driven buy/sell/hold decisions
3. Sweep parameter overrides produce different behavior
4. Sell amount calculation at production scale
5. Teardown interface compliance
6. Edge cases (boundaries, insufficient balance, zero price)
"""

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from strategies.demo.uniswap_v3_rsi_sweep_arbitrum.strategy import (
    UniswapV3RSISweepArbitrumStrategy,
)


def _create_strategy(config_overrides=None):
    """Create a strategy instance with default config."""
    config = {
        "base_token": "WETH",
        "quote_token": "USDC",
        "trade_size_usd": "1000",
        "rsi_period": 14,
        "rsi_oversold": "30",
        "rsi_overbought": "70",
        "max_slippage_bps": 50,
        "chain": "arbitrum",
    }
    if config_overrides:
        config.update(config_overrides)
    return UniswapV3RSISweepArbitrumStrategy(
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
    quote_balance_usd: Decimal = Decimal("100000"),
    base_balance: Decimal = Decimal("10"),
) -> MagicMock:
    """Create a mock MarketSnapshot."""
    market = MagicMock()
    market.price.return_value = base_price

    rsi_mock = MagicMock()
    rsi_mock.value = rsi_value
    market.rsi.return_value = rsi_mock

    quote_bal = MagicMock()
    quote_bal.balance = quote_balance_usd
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
        assert strategy.trade_size_usd == Decimal("1000")
        assert strategy.max_slippage_bps == 50
        assert strategy.base_token == "WETH"
        assert strategy.quote_token == "USDC"

    def test_chain_is_arbitrum(self, strategy):
        assert strategy.chain == "arbitrum"

    def test_production_scale_defaults(self, strategy):
        """Default trade size is $1000, not $3 like the tutorial demo."""
        assert strategy.trade_size_usd == Decimal("1000")
        assert strategy.max_slippage_bps == 50  # 0.5% tighter slippage

    def test_sweep_parameter_override(self):
        """Sweep engine overrides config -- verify they take effect."""
        s = _create_strategy({
            "rsi_period": 7,
            "rsi_oversold": "25",
            "rsi_overbought": "75",
            "trade_size_usd": "2000",
        })
        assert s.rsi_period == 7
        assert s.rsi_oversold == Decimal("25")
        assert s.rsi_overbought == Decimal("75")
        assert s.trade_size_usd == Decimal("2000")


class TestDecisions:
    def test_buy_when_oversold(self, strategy):
        """RSI below oversold threshold triggers buy."""
        market = _make_market(Decimal("25"))
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
        assert "neutral zone" in intent.reason.lower()

    def test_hold_at_exact_oversold_boundary(self, strategy):
        """RSI exactly at oversold threshold is NOT oversold (need < 30)."""
        market = _make_market(Decimal("30"))
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"

    def test_hold_at_exact_overbought_boundary(self, strategy):
        """RSI exactly at overbought threshold is NOT overbought (need > 70)."""
        market = _make_market(Decimal("70"))
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"

    def test_hold_insufficient_quote_for_buy(self, strategy):
        """Buy signal but insufficient USDC balance -> hold."""
        market = _make_market(Decimal("25"), quote_balance_usd=Decimal("500"))
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "insufficient" in intent.reason.lower()

    def test_hold_insufficient_base_for_sell(self, strategy):
        """Sell signal but insufficient WETH balance -> hold."""
        # 1000 USD / 3000 = 0.3333 WETH needed; provide less
        market = _make_market(Decimal("75"), base_balance=Decimal("0.1"))
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "insufficient" in intent.reason.lower()

    def test_hold_on_rsi_unavailable(self, strategy):
        market = MagicMock()
        market.rsi.side_effect = ValueError("Not enough data")
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "unavailable" in intent.reason.lower()

    def test_hold_on_balance_unavailable(self, strategy):
        market = MagicMock()
        rsi_mock = MagicMock()
        rsi_mock.value = Decimal("25")
        market.rsi.return_value = rsi_mock
        market.balance.side_effect = ValueError("No balance")
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"

    def test_hold_on_zero_price(self, strategy):
        """Zero price for sell should hold, not crash."""
        market = _make_market(Decimal("75"), base_price=Decimal("0"))
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "invalid price" in intent.reason.lower()

    def test_protocol_is_uniswap_v3(self, strategy):
        market = _make_market(Decimal("25"))
        intent = strategy.decide(market)
        assert intent.protocol == "uniswap_v3"

    def test_chain_in_intent(self, strategy):
        market = _make_market(Decimal("25"))
        intent = strategy.decide(market)
        assert intent.chain == "arbitrum"

    def test_slippage_50bps(self, strategy):
        """Max slippage should be 0.5% (50bps)."""
        market = _make_market(Decimal("25"))
        intent = strategy.decide(market)
        assert intent.max_slippage == Decimal("0.005")

    def test_sell_amount_production_scale(self, strategy):
        """Sell amount at production scale: $1000 / $3000 = 0.3333 WETH."""
        market = _make_market(Decimal("75"), base_price=Decimal("3000"))
        intent = strategy.decide(market)
        assert intent.amount == Decimal("0.3333")

    def test_trade_counter_increments(self, strategy):
        market = _make_market(Decimal("25"))
        strategy.decide(market)
        assert strategy._total_trades == 1
        assert strategy._total_buys == 1
        strategy.decide(market)
        assert strategy._total_trades == 2
        assert strategy._total_buys == 2

    def test_sell_counter_increments(self, strategy):
        market = _make_market(Decimal("75"))
        strategy.decide(market)
        assert strategy._total_sells == 1
        assert strategy._total_buys == 0

    def test_consecutive_holds(self, strategy):
        market = _make_market(Decimal("50"))
        strategy.decide(market)
        assert strategy._consecutive_holds == 1
        strategy.decide(market)
        assert strategy._consecutive_holds == 2

    def test_hold_counter_resets_on_trade(self, strategy):
        market_hold = _make_market(Decimal("50"))
        strategy.decide(market_hold)
        assert strategy._consecutive_holds == 1
        market_buy = _make_market(Decimal("25"))
        strategy.decide(market_buy)
        assert strategy._consecutive_holds == 0


class TestSweepParameterVariation:
    """Verify different sweep parameters produce meaningfully different behavior."""

    def test_narrow_vs_wide_rsi_range(self):
        """Different RSI thresholds produce different decisions for same RSI."""
        narrow = _create_strategy({"rsi_oversold": "35", "rsi_overbought": "65"})
        wide = _create_strategy({"rsi_oversold": "25", "rsi_overbought": "75"})

        # RSI=30: below narrow's oversold(35) -> buy; above wide's oversold(25) -> hold
        market = _make_market(Decimal("30"))
        assert narrow.decide(market).intent_type.value == "SWAP"
        assert wide.decide(market).intent_type.value == "HOLD"

    def test_different_trade_sizes(self):
        """Different trade_size_usd values appear in generated intents."""
        small = _create_strategy({"trade_size_usd": "500"})
        large = _create_strategy({"trade_size_usd": "2000"})

        market = _make_market(Decimal("25"))
        assert small.decide(market).amount_usd == Decimal("500")
        assert large.decide(market).amount_usd == Decimal("2000")

    def test_rsi_period_passed_to_market(self):
        """Verify strategy passes correct RSI period to market.rsi()."""
        s7 = _create_strategy({"rsi_period": 7})
        s21 = _create_strategy({"rsi_period": 21})

        market7 = _make_market(Decimal("50"))
        market21 = _make_market(Decimal("50"))

        s7.decide(market7)
        s21.decide(market21)

        market7.rsi.assert_called_once_with("WETH", period=7)
        market21.rsi.assert_called_once_with("WETH", period=21)

    def test_all_ticket_rsi_periods(self):
        """Ticket specifies [7, 14, 21] -- all should work."""
        for period in [7, 14, 21]:
            s = _create_strategy({"rsi_period": period})
            market = _make_market(Decimal("25"))
            intent = s.decide(market)
            assert intent.intent_type.value == "SWAP"
            market.rsi.assert_called_once_with("WETH", period=period)

    def test_all_ticket_trade_sizes(self):
        """Ticket specifies [500, 1000, 2000] -- all should produce valid intents."""
        for size in [500, 1000, 2000]:
            s = _create_strategy({"trade_size_usd": str(size)})
            market = _make_market(Decimal("25"))
            intent = s.decide(market)
            assert intent.amount_usd == Decimal(str(size))


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
        assert intents[0].max_slippage == Decimal("0.005")  # 50bps

    def test_teardown_hard_wider_slippage(self, strategy):
        from almanak.framework.teardown import TeardownMode

        intents = strategy.generate_teardown_intents(TeardownMode.HARD)
        assert intents[0].max_slippage == Decimal("0.03")

    def test_open_positions(self, strategy):
        summary = strategy.get_open_positions()
        assert len(summary.positions) == 1
        assert summary.positions[0].chain == "arbitrum"
        assert summary.positions[0].protocol == "uniswap_v3"


class TestStatus:
    def test_status_includes_sweep_params(self, strategy):
        status = strategy.get_status()
        assert status["config"]["rsi_period"] == 14
        assert status["config"]["rsi_oversold"] == "30"
        assert status["config"]["rsi_overbought"] == "70"
        assert status["config"]["trade_size_usd"] == "1000"
        assert status["config"]["max_slippage_bps"] == 50

    def test_status_includes_counters(self, strategy):
        market = _make_market(Decimal("25"))
        strategy.decide(market)
        status = strategy.get_status()
        assert status["state"]["total_trades"] == 1
        assert status["state"]["total_buys"] == 1
        assert status["state"]["total_sells"] == 0
