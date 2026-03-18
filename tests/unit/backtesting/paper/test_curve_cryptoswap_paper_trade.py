"""Unit tests for CurveCryptoSwapPnLStrategy paper trading compatibility.

Validates:
1. Strategy decision logic (buy/sell/hold based on RSI thresholds)
2. Correct Curve protocol and intent configuration
3. State transitions after intent execution
4. Teardown support for paper trading position unwinding
5. Edge cases: price unavailable, RSI unavailable, insufficient funds

Part of VIB-1459: Paper Trade Curve CryptoSwap Strategy on Ethereum.
"""

from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock, patch


# ---------------------------------------------------------------------------
# Helper: lightweight strategy factory (avoids full framework bootstrap)
# ---------------------------------------------------------------------------


def _make_strategy(
    trade_size_usd: str = "100",
    rsi_period: int = 14,
    rsi_oversold: str = "40",
    rsi_overbought: str = "70",
    max_slippage_bps: int = 100,
    base_token: str = "WETH",
    quote_token: str = "USDT",
    chain: str = "ethereum",
) -> Any:
    """Create a CurveCryptoSwapPnLStrategy with mocked framework wiring."""
    from strategies.demo.curve_cryptoswap_pnl.strategy import CurveCryptoSwapPnLStrategy

    with patch.object(CurveCryptoSwapPnLStrategy, "__init__", lambda self, *a, **kw: None):
        strat = CurveCryptoSwapPnLStrategy.__new__(CurveCryptoSwapPnLStrategy)

    strat.trade_size_usd = Decimal(trade_size_usd)
    strat.rsi_period = rsi_period
    strat.rsi_oversold = Decimal(rsi_oversold)
    strat.rsi_overbought = Decimal(rsi_overbought)
    strat.max_slippage_bps = max_slippage_bps
    strat.base_token = base_token
    strat.quote_token = quote_token
    strat._chain = chain
    strat._strategy_id = "test_curve_cryptoswap_pnl"
    strat._wallet_address = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
    strat._consecutive_holds = 0
    strat._has_position = False
    return strat


def _mock_market(
    eth_price: Decimal = Decimal("2500"),
    usdt_balance: Decimal = Decimal("10000"),
    weth_balance: Decimal = Decimal("5"),
    rsi_value: Decimal = Decimal("50"),
    rsi_unavailable: bool = False,
    price_unavailable: bool = False,
) -> MagicMock:
    """Create a mock MarketSnapshot with RSI support."""
    market = MagicMock()

    def price_fn(symbol: str) -> Decimal:
        if price_unavailable:
            raise ValueError("oracle unavailable")
        prices = {"WETH": eth_price, "ETH": eth_price, "USDT": Decimal("1")}
        if symbol in prices:
            return prices[symbol]
        raise ValueError(f"No price for {symbol}")

    market.price = price_fn

    def balance_fn(symbol: str) -> Any:
        bal = MagicMock()
        if symbol == "USDT":
            bal.balance = usdt_balance
            bal.balance_usd = usdt_balance
        elif symbol in ("WETH", "ETH"):
            bal.balance = weth_balance
            bal.balance_usd = weth_balance * eth_price
        else:
            raise ValueError(f"No balance for {symbol}")
        return bal

    market.balance = balance_fn

    def rsi_fn(symbol: str, period: int = 14) -> Any:
        if rsi_unavailable:
            raise ValueError("RSI unavailable")
        rsi = MagicMock()
        rsi.value = rsi_value
        return rsi

    market.rsi = rsi_fn
    return market


# ===========================================================================
# Decision Logic Tests
# ===========================================================================


class TestDecisionLogic:
    """Test buy/sell/hold RSI-based decisions."""

    def test_buy_when_rsi_oversold(self):
        """Should buy WETH when RSI <= 40."""
        strat = _make_strategy()
        market = _mock_market(rsi_value=Decimal("30"))
        intent = strat.decide(market)

        assert intent is not None
        assert intent.intent_type.value == "SWAP"
        assert intent.from_token == "USDT"
        assert intent.to_token == "WETH"
        assert intent.amount_usd == Decimal("100")
        assert intent.protocol == "curve"

    def test_sell_when_rsi_overbought(self):
        """Should sell WETH when RSI >= 70."""
        strat = _make_strategy()
        market = _mock_market(rsi_value=Decimal("80"), weth_balance=Decimal("5"))
        intent = strat.decide(market)

        assert intent is not None
        assert intent.intent_type.value == "SWAP"
        assert intent.from_token == "WETH"
        assert intent.to_token == "USDT"
        assert intent.amount_usd == Decimal("100")
        assert intent.protocol == "curve"

    def test_hold_when_rsi_neutral(self):
        """Should hold when RSI is between thresholds."""
        strat = _make_strategy()
        market = _mock_market(rsi_value=Decimal("55"))
        intent = strat.decide(market)

        assert intent is not None
        assert intent.intent_type.value == "HOLD"

    def test_hold_at_exact_oversold_boundary(self):
        """RSI exactly at oversold threshold should trigger buy."""
        strat = _make_strategy()
        market = _mock_market(rsi_value=Decimal("40"))
        intent = strat.decide(market)

        assert intent.intent_type.value == "SWAP"
        assert intent.from_token == "USDT"

    def test_hold_at_exact_overbought_boundary(self):
        """RSI exactly at overbought threshold should trigger sell."""
        strat = _make_strategy()
        market = _mock_market(rsi_value=Decimal("70"), weth_balance=Decimal("5"))
        intent = strat.decide(market)

        assert intent.intent_type.value == "SWAP"
        assert intent.from_token == "WETH"

    def test_hold_just_above_oversold(self):
        """RSI just above oversold threshold should hold."""
        strat = _make_strategy()
        market = _mock_market(rsi_value=Decimal("41"))
        intent = strat.decide(market)

        assert intent.intent_type.value == "HOLD"

    def test_hold_just_below_overbought(self):
        """RSI just below overbought threshold should hold."""
        strat = _make_strategy()
        market = _mock_market(rsi_value=Decimal("69"))
        intent = strat.decide(market)

        assert intent.intent_type.value == "HOLD"


# ===========================================================================
# Edge Cases: Insufficient Funds
# ===========================================================================


class TestInsufficientFunds:
    """Test hold behavior when funds are insufficient."""

    def test_hold_when_insufficient_usdt_for_buy(self):
        """Should hold when oversold but not enough USDT."""
        strat = _make_strategy(trade_size_usd="1000")
        market = _mock_market(rsi_value=Decimal("30"), usdt_balance=Decimal("500"))
        intent = strat.decide(market)

        assert intent.intent_type.value == "HOLD"
        assert "insufficient" in intent.reason.lower()

    def test_hold_when_insufficient_weth_for_sell(self):
        """Should hold when overbought but not enough WETH."""
        strat = _make_strategy(trade_size_usd="10000")
        market = _mock_market(
            rsi_value=Decimal("80"),
            eth_price=Decimal("2500"),
            weth_balance=Decimal("1"),  # Only $2500 worth, need $10000
        )
        intent = strat.decide(market)

        assert intent.intent_type.value == "HOLD"
        assert "insufficient" in intent.reason.lower()


# ===========================================================================
# Edge Cases: Data Unavailability
# ===========================================================================


class TestDataUnavailability:
    """Test graceful handling of missing data."""

    def test_hold_when_price_unavailable(self):
        """Should hold when price oracle fails."""
        strat = _make_strategy()
        market = _mock_market(price_unavailable=True)
        intent = strat.decide(market)

        assert intent.intent_type.value == "HOLD"
        assert "unavailable" in intent.reason.lower()

    def test_hold_when_rsi_unavailable(self):
        """Should hold when RSI data fails."""
        strat = _make_strategy()
        market = _mock_market(rsi_unavailable=True)
        intent = strat.decide(market)

        assert intent.intent_type.value == "HOLD"
        assert "unavailable" in intent.reason.lower()

    def test_hold_when_price_zero(self):
        """Should hold when price is zero."""
        strat = _make_strategy()
        market = _mock_market(eth_price=Decimal("0"))
        intent = strat.decide(market)

        assert intent.intent_type.value == "HOLD"
        assert "invalid" in intent.reason.lower()


# ===========================================================================
# Slippage Configuration
# ===========================================================================


class TestSlippageConfig:
    """Test slippage is properly configured on swap intents."""

    def test_default_slippage_100bps(self):
        """Default max_slippage should be 1% (100 bps)."""
        strat = _make_strategy(max_slippage_bps=100)
        market = _mock_market(rsi_value=Decimal("30"))
        intent = strat.decide(market)

        assert intent.max_slippage == Decimal("0.01")

    def test_custom_slippage_50bps(self):
        """Custom 50bps slippage should be 0.005."""
        strat = _make_strategy(max_slippage_bps=50)
        market = _mock_market(rsi_value=Decimal("30"))
        intent = strat.decide(market)

        assert intent.max_slippage == Decimal("0.005")


# ===========================================================================
# Consecutive Holds Counter
# ===========================================================================


class TestConsecutiveHolds:
    """Test hold counter behavior."""

    def test_consecutive_holds_increment(self):
        """Consecutive holds should increment the counter."""
        strat = _make_strategy()
        market = _mock_market(rsi_value=Decimal("55"))

        strat.decide(market)
        assert strat._consecutive_holds == 1

        strat.decide(market)
        assert strat._consecutive_holds == 2

    def test_buy_resets_consecutive_holds(self):
        """Buy should reset the consecutive holds counter."""
        strat = _make_strategy()
        strat._consecutive_holds = 5
        market = _mock_market(rsi_value=Decimal("30"))

        strat.decide(market)
        assert strat._consecutive_holds == 0

    def test_sell_resets_consecutive_holds(self):
        """Sell should reset the consecutive holds counter."""
        strat = _make_strategy()
        strat._consecutive_holds = 5
        market = _mock_market(rsi_value=Decimal("80"), weth_balance=Decimal("5"))

        strat.decide(market)
        assert strat._consecutive_holds == 0


# ===========================================================================
# Teardown Support
# ===========================================================================


class TestTeardown:
    """Test teardown methods for paper trading position unwinding."""

    def test_supports_teardown(self):
        """Strategy must support teardown for paper trading."""
        strat = _make_strategy()
        assert strat.supports_teardown() is True

    def test_get_open_positions(self):
        """Should return position info for teardown."""
        strat = _make_strategy()
        strat._has_position = True
        positions = strat.get_open_positions()
        assert positions is not None
        assert len(positions.positions) == 1

        pos = positions.positions[0]
        assert pos.protocol == "curve"
        assert pos.details["base_token"] == "WETH"
        assert pos.details["quote_token"] == "USDT"

    def test_generate_teardown_intents_soft(self):
        """Soft teardown should use normal slippage."""
        from almanak.framework.teardown import TeardownMode

        strat = _make_strategy(max_slippage_bps=100)
        strat._has_position = True
        intents = strat.generate_teardown_intents(TeardownMode.SOFT)

        assert len(intents) == 1
        intent = intents[0]
        assert intent.intent_type.value == "SWAP"
        assert intent.from_token == "WETH"
        assert intent.to_token == "USDT"
        assert intent.protocol == "curve"
        assert intent.max_slippage == Decimal("0.01")

    def test_generate_teardown_intents_hard(self):
        """Hard teardown should use 3% slippage."""
        from almanak.framework.teardown import TeardownMode

        strat = _make_strategy()
        strat._has_position = True
        intents = strat.generate_teardown_intents(TeardownMode.HARD)

        assert len(intents) == 1
        intent = intents[0]
        assert intent.max_slippage == Decimal("0.03")

    def test_teardown_sells_all(self):
        """Teardown should use amount='all' to close entire position."""
        from almanak.framework.teardown import TeardownMode

        strat = _make_strategy()
        strat._has_position = True
        intents = strat.generate_teardown_intents(TeardownMode.SOFT)

        assert intents[0].amount == "all"


# ===========================================================================
# Status / Observability
# ===========================================================================


class TestStatus:
    """Test strategy status reporting for paper trading dashboard."""

    def test_get_status_includes_config(self):
        """Status should include strategy configuration."""
        strat = _make_strategy()
        status = strat.get_status()

        assert status["strategy"] == "demo_curve_cryptoswap_pnl"
        assert status["config"]["pair"] == "WETH/USDT"
        assert status["config"]["trade_size_usd"] == "100"

    def test_get_status_includes_state(self):
        """Status should include runtime state."""
        strat = _make_strategy()
        strat._consecutive_holds = 7
        status = strat.get_status()

        assert status["state"]["consecutive_holds"] == 7


# ===========================================================================
# Custom Threshold Configuration
# ===========================================================================


class TestCustomThresholds:
    """Test non-default RSI threshold configurations."""

    def test_tight_rsi_range(self):
        """Tight RSI range (45-55) should trigger more trades."""
        strat = _make_strategy(rsi_oversold="45", rsi_overbought="55")
        market = _mock_market(rsi_value=Decimal("44"))
        intent = strat.decide(market)
        assert intent.intent_type.value == "SWAP"
        assert intent.from_token == "USDT"

    def test_wide_rsi_range(self):
        """Wide RSI range (20-80) should trigger fewer trades."""
        strat = _make_strategy(rsi_oversold="20", rsi_overbought="80")

        # RSI=30 is above oversold (20), should hold
        market = _mock_market(rsi_value=Decimal("30"))
        intent = strat.decide(market)
        assert intent.intent_type.value == "HOLD"

        # RSI=15 is below oversold (20), should buy
        market = _mock_market(rsi_value=Decimal("15"))
        intent = strat.decide(market)
        assert intent.intent_type.value == "SWAP"
        assert intent.from_token == "USDT"
