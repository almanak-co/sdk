"""Unit tests for PancakeSwap V3 PnL Swap BSC Strategy.

Tests validate:
1. Strategy initialization with BSC config
2. RSI-gated buy/sell decisions
3. Balance checks before trading
4. Execution callbacks (buy/sell tracking)
5. Teardown interface
6. State persistence round-trip
"""

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from strategies.demo.pancakeswap_pnl_swap_bsc.strategy import PancakeSwapPnLSwapBSCStrategy


def _create_strategy(config_overrides=None):
    """Create a strategy instance with default config."""
    config = {
        "base_token": "WBNB",
        "quote_token": "USDC",
        "trade_size_usd": "100",
        "rsi_period": 14,
        "rsi_oversold": "30",
        "rsi_overbought": "70",
        "max_slippage_bps": 100,
        "chain": "bsc",
    }
    if config_overrides:
        config.update(config_overrides)
    return PancakeSwapPnLSwapBSCStrategy(
        config=config,
        chain="bsc",
        wallet_address="0x" + "c" * 40,
    )


@pytest.fixture
def strategy():
    return _create_strategy()


def _make_market(
    rsi_value: Decimal = Decimal("50"),
    base_price: Decimal = Decimal("600"),
    quote_balance_usd: Decimal = Decimal("10000"),
    base_balance_usd: Decimal = Decimal("5000"),
    base_balance_amount: Decimal | None = None,
) -> MagicMock:
    """Create a mock MarketSnapshot."""
    market = MagicMock()

    rsi_mock = MagicMock()
    rsi_mock.value = rsi_value
    market.rsi.return_value = rsi_mock

    def price_fn(token):
        if token in ("WBNB", "BNB"):
            return base_price
        if token == "USDC":
            return Decimal("1")
        raise ValueError(f"Unknown token: {token}")

    market.price.side_effect = price_fn

    # Default base_balance_amount from USD/price if not explicitly provided
    if base_balance_amount is None and base_price > 0:
        base_balance_amount = base_balance_usd / base_price

    def balance_fn(token):
        bal = MagicMock()
        if token == "USDC":
            bal.balance_usd = quote_balance_usd
            bal.balance = quote_balance_usd  # 1:1 for USDC
        else:
            bal.balance_usd = base_balance_usd
            bal.balance = base_balance_amount if base_balance_amount is not None else Decimal("0")
        return bal

    market.balance.side_effect = balance_fn
    return market


class TestInitialization:
    def test_default_config(self, strategy):
        assert strategy.base_token == "WBNB"
        assert strategy.quote_token == "USDC"
        assert strategy.trade_size_usd == Decimal("100")
        assert strategy.rsi_period == 14
        assert strategy.rsi_oversold == Decimal("30")
        assert strategy.rsi_overbought == Decimal("70")
        assert strategy.max_slippage_bps == 100
        assert strategy._tick_count == 0
        assert strategy._total_buys == 0
        assert strategy._total_sells == 0

    def test_chain_is_bsc(self, strategy):
        assert strategy.chain == "bsc"


class TestDecisionLogic:
    def test_buy_when_oversold(self, strategy):
        """RSI < 30 -> buy WBNB."""
        market = _make_market(rsi_value=Decimal("25"))
        intent = strategy.decide(market)
        assert intent is not None
        assert intent.intent_type.value == "SWAP"
        assert intent.from_token == "USDC"
        assert intent.to_token == "WBNB"

    def test_sell_when_overbought(self, strategy):
        """RSI > 70 -> sell WBNB."""
        market = _make_market(rsi_value=Decimal("75"))
        intent = strategy.decide(market)
        assert intent is not None
        assert intent.intent_type.value == "SWAP"
        assert intent.from_token == "WBNB"
        assert intent.to_token == "USDC"

    def test_hold_in_neutral_zone(self, strategy):
        """RSI between 30-70 -> hold."""
        market = _make_market(rsi_value=Decimal("50"))
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "neutral zone" in intent.reason.lower()

    def test_hold_at_boundary_oversold(self, strategy):
        """RSI == 30 is NOT oversold (need < 30)."""
        market = _make_market(rsi_value=Decimal("30"))
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"

    def test_hold_at_boundary_overbought(self, strategy):
        """RSI == 70 is NOT overbought (need > 70)."""
        market = _make_market(rsi_value=Decimal("70"))
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"

    def test_hold_insufficient_quote_for_buy(self, strategy):
        """Should hold if not enough USDC for buy."""
        market = _make_market(rsi_value=Decimal("25"), quote_balance_usd=Decimal("50"))
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "insufficient" in intent.reason.lower()

    def test_hold_insufficient_base_for_sell(self, strategy):
        """Should hold if not enough WBNB for sell (token amount check)."""
        # trade_size=100 USD / price=600 = 0.1667 WBNB needed; provide less
        market = _make_market(
            rsi_value=Decimal("75"),
            base_balance_amount=Decimal("0.05"),
        )
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "insufficient" in intent.reason.lower()

    def test_hold_on_rsi_unavailable(self, strategy):
        market = MagicMock()
        market.rsi.side_effect = ValueError("No data")
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "unavailable" in intent.reason.lower()

    def test_hold_on_price_unavailable(self, strategy):
        """Price is only needed for SELL (amount calc), so test with overbought RSI."""
        market = MagicMock()
        rsi_mock = MagicMock()
        rsi_mock.value = Decimal("75")
        market.rsi.return_value = rsi_mock
        market.price.side_effect = ValueError("No price")
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"

    def test_hold_on_zero_price(self, strategy):
        """Zero price should not cause DivisionByZero — should hold."""
        market = _make_market(rsi_value=Decimal("75"), base_price=Decimal("0"))
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert "invalid price" in intent.reason.lower()

    def test_sell_amount_calculation(self, strategy):
        """Sell amount should be trade_size_usd / base_price."""
        market = _make_market(rsi_value=Decimal("75"), base_price=Decimal("500"))
        intent = strategy.decide(market)
        # 100 USD / 500 = 0.2000
        assert intent.amount == Decimal("0.2000")

    def test_protocol_is_pancakeswap_v3(self, strategy):
        """Intents should use pancakeswap_v3 protocol."""
        market = _make_market(rsi_value=Decimal("25"))
        intent = strategy.decide(market)
        assert intent.protocol == "pancakeswap_v3"

    def test_slippage_from_config(self, strategy):
        """Max slippage should be derived from config bps."""
        market = _make_market(rsi_value=Decimal("25"))
        intent = strategy.decide(market)
        assert intent.max_slippage == Decimal("0.01")  # 100bps = 1%

    def test_tick_count_increments(self, strategy):
        market = _make_market()
        strategy.decide(market)
        assert strategy._tick_count == 1
        strategy.decide(market)
        assert strategy._tick_count == 2


class TestOnIntentExecuted:
    def test_buy_success_increments_counter(self, strategy):
        intent = MagicMock()
        intent.intent_type.value = "SWAP"
        intent.from_token = "USDC"
        strategy.on_intent_executed(intent, success=True, result=None)
        assert strategy._total_buys == 1
        assert strategy._total_sells == 0

    def test_sell_success_increments_counter(self, strategy):
        intent = MagicMock()
        intent.intent_type.value = "SWAP"
        intent.from_token = "WBNB"
        strategy.on_intent_executed(intent, success=True, result=None)
        assert strategy._total_sells == 1
        assert strategy._total_buys == 0

    def test_failure_does_not_increment(self, strategy):
        intent = MagicMock()
        intent.intent_type.value = "SWAP"
        intent.from_token = "USDC"
        strategy.on_intent_executed(intent, success=False, result=None)
        assert strategy._total_buys == 0

    def test_buy_tracks_base_held_from_trade_record(self, strategy):
        """PnL backtester passes TradeRecord with actual_amount_out."""
        intent = MagicMock()
        intent.intent_type.value = "SWAP"
        intent.from_token = "USDC"
        result = MagicMock(spec=[])  # no swap_amounts attribute
        result.actual_amount_out = Decimal("0.15")
        strategy.on_intent_executed(intent, success=True, result=result)
        assert strategy._base_held == Decimal("0.15")

    def test_sell_tracks_base_held_from_trade_record(self, strategy):
        """PnL backtester passes TradeRecord with actual_amount_in."""
        strategy._base_held = Decimal("0.5")
        intent = MagicMock()
        intent.intent_type.value = "SWAP"
        intent.from_token = "WBNB"
        result = MagicMock(spec=[])
        result.actual_amount_in = Decimal("0.2")
        strategy.on_intent_executed(intent, success=True, result=result)
        assert strategy._base_held == Decimal("0.3")


class TestStatePersistence:
    def test_round_trip(self, strategy):
        strategy._tick_count = 15
        strategy._total_buys = 3
        strategy._total_sells = 2
        strategy._base_held = Decimal("1.5")

        state = strategy.get_persistent_state()
        new_strategy = _create_strategy()
        new_strategy.load_persistent_state(state)

        assert new_strategy._tick_count == 15
        assert new_strategy._total_buys == 3
        assert new_strategy._total_sells == 2
        assert new_strategy._base_held == Decimal("1.5")


class TestTeardown:
    def test_supports_teardown(self, strategy):
        assert strategy.supports_teardown() is True

    def test_teardown_with_base_held(self, strategy):
        strategy._base_held = Decimal("2.0")
        positions = strategy.get_open_positions()
        assert len(positions.positions) == 1
        assert positions.positions[0].protocol == "pancakeswap_v3"

        from almanak.framework.teardown import TeardownMode
        intents = strategy.generate_teardown_intents(mode=TeardownMode.SOFT)
        assert len(intents) == 1
        assert intents[0].intent_type.value == "SWAP"
        assert intents[0].from_token == "WBNB"
        assert intents[0].to_token == "USDC"

    def test_teardown_hard_mode_wider_slippage(self, strategy):
        strategy._base_held = Decimal("2.0")
        from almanak.framework.teardown import TeardownMode
        intents = strategy.generate_teardown_intents(mode=TeardownMode.HARD)
        assert intents[0].max_slippage == Decimal("0.03")

    def test_teardown_no_positions(self, strategy):
        positions = strategy.get_open_positions()
        assert len(positions.positions) == 0
        from almanak.framework.teardown import TeardownMode
        intents = strategy.generate_teardown_intents(mode=TeardownMode.SOFT)
        assert len(intents) == 0


class TestGetStatus:
    def test_status_fields(self, strategy):
        status = strategy.get_status()
        assert status["chain"] == "bsc"
        assert status["base_token"] == "WBNB"
        assert status["quote_token"] == "USDC"
        assert status["total_buys"] == 0
        assert status["total_sells"] == 0
