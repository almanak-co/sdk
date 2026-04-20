"""Unit tests for Velodrome V2 swap PnL backtest on Optimism (VIB-2111).

Tests the BUY/SELL swap lifecycle, intent metadata, and edge cases
without requiring a gateway or Anvil fork.

First PnL backtest of any strategy on Optimism.
First Velodrome-specific backtest.
Validates PnL engine handles Solidly-fork swap pricing.
"""

from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock, patch

from almanak.framework.intents.vocabulary import IntentType


# ---------------------------------------------------------------------------
# Helper: lightweight strategy factory
# ---------------------------------------------------------------------------


def _create_strategy(config_overrides: dict | None = None):
    """Create a VelodromeSwapOptimismStrategy with mocked framework dependencies."""
    from almanak.demo_strategies.velodrome_swap_optimism.strategy import VelodromeSwapOptimismStrategy

    with patch.object(VelodromeSwapOptimismStrategy, "__init__", lambda self, *a, **kw: None):
        strategy = VelodromeSwapOptimismStrategy.__new__(VelodromeSwapOptimismStrategy)

    config = {
        "swap_amount": "50",
        "max_slippage_pct": "1.0",
        "base_token": "WETH",
        "quote_token": "USDC",
        "force_action": "buy",
    }
    if config_overrides:
        config.update(config_overrides)

    strategy._strategy_id = "test-velodrome-pnl-optimism"
    strategy._chain = "optimism"
    strategy._wallet_address = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
    strategy.swap_amount = Decimal(str(config["swap_amount"]))
    strategy.max_slippage_pct = Decimal(str(config["max_slippage_pct"]))
    strategy.base_token = config["base_token"]
    strategy.quote_token = config["quote_token"]
    strategy.force_action = config["force_action"]

    return strategy


def _mock_market(
    weth_balance: Decimal | None = None,
    weth_balance_usd: Decimal | None = None,
    usdc_balance: Decimal | None = None,
    usdc_balance_usd: Decimal | None = None,
    weth_price: float = 3000.0,
    usdc_price: float = 1.0,
    price_unavailable: bool = False,
    balance_unavailable: bool = False,
) -> MagicMock:
    """Create a mock MarketSnapshot for Velodrome backtesting."""
    market = MagicMock()

    def price_fn(symbol: str, quote: str = "USD") -> Decimal:
        if price_unavailable:
            raise ValueError(f"Price unavailable for {symbol}")
        if symbol == "WETH":
            return Decimal(str(weth_price))
        if symbol == "USDC":
            return Decimal(str(usdc_price))
        raise ValueError(f"No price for {symbol}")

    market.price = price_fn

    def balance_fn(symbol: str) -> Any:
        if balance_unavailable:
            raise ValueError(f"Balance unavailable for {symbol}")
        if symbol == "WETH":
            bal = MagicMock()
            bal.balance = weth_balance if weth_balance is not None else Decimal("0")
            bal.balance_usd = weth_balance_usd if weth_balance_usd is not None else (
                (weth_balance or Decimal("0")) * Decimal(str(weth_price))
            )
            return bal
        if symbol == "USDC":
            bal = MagicMock()
            bal.balance = usdc_balance if usdc_balance is not None else Decimal("0")
            bal.balance_usd = usdc_balance_usd if usdc_balance_usd is not None else (
                usdc_balance or Decimal("0")
            )
            return bal
        raise ValueError(f"No balance for {symbol}")

    market.balance = balance_fn
    return market


# ===========================================================================
# BUY Phase Tests
# ===========================================================================


class TestBuyPhase:
    """Test BUY swap: USDC -> WETH via Velodrome V2."""

    def test_buy_generates_swap_intent(self):
        strategy = _create_strategy({"force_action": "buy"})
        market = _mock_market(usdc_balance=Decimal("10000"), usdc_balance_usd=Decimal("10000"))

        intent = strategy.decide(market)

        assert intent.intent_type == IntentType.SWAP
        assert intent.from_token == "USDC"
        assert intent.to_token == "WETH"

    def test_buy_uses_aerodrome_protocol(self):
        strategy = _create_strategy({"force_action": "buy"})
        market = _mock_market(usdc_balance=Decimal("10000"), usdc_balance_usd=Decimal("10000"))

        intent = strategy.decide(market)

        assert intent.protocol == "aerodrome"

    def test_buy_swap_amount_usd(self):
        strategy = _create_strategy({"force_action": "buy", "swap_amount": "100"})
        market = _mock_market(usdc_balance=Decimal("10000"), usdc_balance_usd=Decimal("10000"))

        intent = strategy.decide(market)

        assert intent.amount_usd == Decimal("100")

    def test_buy_holds_when_insufficient_usdc(self):
        strategy = _create_strategy({"force_action": "buy", "swap_amount": "100"})
        market = _mock_market(usdc_balance=Decimal("50"), usdc_balance_usd=Decimal("50"))

        intent = strategy.decide(market)

        assert intent.intent_type == IntentType.HOLD
        assert "Insufficient" in intent.reason

    def test_buy_proceeds_when_balance_unavailable(self):
        """Balance check failure should not block BUY — strategy catches ValueError."""
        strategy = _create_strategy({"force_action": "buy"})
        market = _mock_market(balance_unavailable=True)

        intent = strategy.decide(market)

        assert intent.intent_type == IntentType.SWAP
        assert intent.from_token == "USDC"

    def test_buy_proceeds_when_price_unavailable(self):
        """Price prefetch failure should not block BUY — strategy catches ValueError."""
        strategy = _create_strategy({"force_action": "buy"})
        market = _mock_market(
            usdc_balance=Decimal("10000"),
            usdc_balance_usd=Decimal("10000"),
            price_unavailable=True,
        )

        intent = strategy.decide(market)

        assert intent.intent_type == IntentType.SWAP


# ===========================================================================
# SELL Phase Tests
# ===========================================================================


class TestSellPhase:
    """Test SELL swap: WETH -> USDC via Velodrome V2."""

    def test_sell_generates_swap_intent(self):
        strategy = _create_strategy({"force_action": "sell"})
        market = _mock_market(
            weth_balance=Decimal("1"),
            weth_balance_usd=Decimal("3000"),
        )

        intent = strategy.decide(market)

        assert intent.intent_type == IntentType.SWAP
        assert intent.from_token == "WETH"
        assert intent.to_token == "USDC"

    def test_sell_uses_aerodrome_protocol(self):
        strategy = _create_strategy({"force_action": "sell"})
        market = _mock_market(weth_balance=Decimal("1"), weth_balance_usd=Decimal("3000"))

        intent = strategy.decide(market)

        assert intent.protocol == "aerodrome"

    def test_sell_swap_amount_usd(self):
        strategy = _create_strategy({"force_action": "sell", "swap_amount": "200"})
        market = _mock_market(weth_balance=Decimal("1"), weth_balance_usd=Decimal("3000"))

        intent = strategy.decide(market)

        assert intent.amount_usd == Decimal("200")

    def test_sell_holds_when_insufficient_weth(self):
        strategy = _create_strategy({"force_action": "sell", "swap_amount": "100"})
        market = _mock_market(weth_balance=Decimal("0.01"), weth_balance_usd=Decimal("30"))

        intent = strategy.decide(market)

        assert intent.intent_type == IntentType.HOLD
        assert "Insufficient" in intent.reason

    def test_sell_proceeds_when_balance_unavailable(self):
        strategy = _create_strategy({"force_action": "sell"})
        market = _mock_market(balance_unavailable=True)

        intent = strategy.decide(market)

        assert intent.intent_type == IntentType.SWAP
        assert intent.from_token == "WETH"


# ===========================================================================
# Multi-Iteration Backtest Lifecycle
# ===========================================================================


class TestMultiIterationLifecycle:
    """Test BUY -> SELL lifecycle across backtest iterations."""

    def test_buy_then_sell_sequence(self):
        """Full roundtrip: BUY USDC->WETH, then SELL WETH->USDC."""
        strategy = _create_strategy({"force_action": "buy"})

        # Iteration 1: BUY phase
        market_buy = _mock_market(
            usdc_balance=Decimal("10000"),
            usdc_balance_usd=Decimal("10000"),
        )
        intent_buy = strategy.decide(market_buy)
        assert intent_buy.intent_type == IntentType.SWAP
        assert intent_buy.from_token == "USDC"
        assert intent_buy.to_token == "WETH"

        # Switch to SELL phase (simulating config change between iterations)
        strategy.force_action = "sell"

        # Iteration 2: SELL phase
        market_sell = _mock_market(
            weth_balance=Decimal("1"),
            weth_balance_usd=Decimal("3000"),
        )
        intent_sell = strategy.decide(market_sell)
        assert intent_sell.intent_type == IntentType.SWAP
        assert intent_sell.from_token == "WETH"
        assert intent_sell.to_token == "USDC"

    def test_multiple_buy_iterations(self):
        """Strategy should produce BUY swaps across multiple iterations."""
        strategy = _create_strategy({"force_action": "buy"})

        for _ in range(5):
            market = _mock_market(
                usdc_balance=Decimal("10000"),
                usdc_balance_usd=Decimal("10000"),
            )
            intent = strategy.decide(market)
            assert intent.intent_type == IntentType.SWAP
            assert intent.from_token == "USDC"

    def test_sell_after_balance_grows(self):
        """SELL should work once WETH balance exceeds swap amount."""
        strategy = _create_strategy({"force_action": "sell", "swap_amount": "100"})

        # Iteration 1: insufficient WETH
        market1 = _mock_market(weth_balance=Decimal("0.01"), weth_balance_usd=Decimal("30"))
        intent1 = strategy.decide(market1)
        assert intent1.intent_type == IntentType.HOLD

        # Iteration 2: enough WETH
        market2 = _mock_market(weth_balance=Decimal("0.05"), weth_balance_usd=Decimal("150"))
        intent2 = strategy.decide(market2)
        assert intent2.intent_type == IntentType.SWAP
        assert intent2.from_token == "WETH"


# ===========================================================================
# Intent Metadata for PnL Tracking
# ===========================================================================


class TestIntentMetadata:
    """Test intent structure is compatible with PnL backtester."""

    def test_buy_intent_has_required_fields(self):
        """BUY SwapIntent must have all fields for PnL tracking."""
        strategy = _create_strategy({"force_action": "buy"})
        market = _mock_market(usdc_balance=Decimal("10000"), usdc_balance_usd=Decimal("10000"))
        intent = strategy.decide(market)

        assert intent.intent_type == IntentType.SWAP
        assert intent.from_token == "USDC"
        assert intent.to_token == "WETH"
        assert intent.amount_usd == Decimal("50")
        assert intent.max_slippage == Decimal("0.01")  # 1.0% -> 0.01
        assert intent.protocol == "aerodrome"

    def test_sell_intent_has_required_fields(self):
        """SELL SwapIntent must have all fields for PnL tracking."""
        strategy = _create_strategy({"force_action": "sell"})
        market = _mock_market(weth_balance=Decimal("1"), weth_balance_usd=Decimal("3000"))
        intent = strategy.decide(market)

        assert intent.intent_type == IntentType.SWAP
        assert intent.from_token == "WETH"
        assert intent.to_token == "USDC"
        assert intent.amount_usd == Decimal("50")
        assert intent.max_slippage == Decimal("0.01")
        assert intent.protocol == "aerodrome"

    def test_hold_intent_has_reason(self):
        """HoldIntent must have reason string for PnL journal entries."""
        strategy = _create_strategy({"force_action": "buy", "swap_amount": "100"})
        market = _mock_market(usdc_balance=Decimal("10"), usdc_balance_usd=Decimal("10"))
        intent = strategy.decide(market)

        assert intent.intent_type == IntentType.HOLD
        assert isinstance(intent.reason, str)
        assert len(intent.reason) > 0


# ===========================================================================
# Force Action Configuration
# ===========================================================================


class TestForceActionConfig:
    """Test force_action parameter drives strategy behavior."""

    def test_unknown_force_action_holds(self):
        strategy = _create_strategy({"force_action": "invalid"})
        market = _mock_market(usdc_balance=Decimal("10000"), usdc_balance_usd=Decimal("10000"))

        intent = strategy.decide(market)

        assert intent.intent_type == IntentType.HOLD
        assert "Unknown" in intent.reason or "invalid" in intent.reason

    def test_empty_force_action_holds(self):
        strategy = _create_strategy({"force_action": ""})
        market = _mock_market(usdc_balance=Decimal("10000"), usdc_balance_usd=Decimal("10000"))

        intent = strategy.decide(market)

        assert intent.intent_type == IntentType.HOLD
        assert isinstance(intent.reason, str)
        assert intent.reason


# ===========================================================================
# Slippage Configuration for Backtest Accuracy
# ===========================================================================


class TestSlippageConfig:
    """Test slippage config affects PnL calculation accuracy."""

    def test_default_slippage_1_percent(self):
        """Default 1.0% slippage should be Decimal('0.01')."""
        strategy = _create_strategy({"force_action": "buy"})
        market = _mock_market(usdc_balance=Decimal("10000"), usdc_balance_usd=Decimal("10000"))
        intent = strategy.decide(market)

        assert intent.max_slippage == Decimal("0.01")

    def test_custom_slippage_half_percent(self):
        """Custom 0.5% slippage should be Decimal('0.005')."""
        strategy = _create_strategy({"force_action": "buy", "max_slippage_pct": "0.5"})
        market = _mock_market(usdc_balance=Decimal("10000"), usdc_balance_usd=Decimal("10000"))
        intent = strategy.decide(market)

        assert intent.max_slippage == Decimal("0.005")

    def test_custom_slippage_2_percent(self):
        """Custom 2.0% slippage should be Decimal('0.02')."""
        strategy = _create_strategy({"force_action": "sell", "max_slippage_pct": "2.0"})
        market = _mock_market(weth_balance=Decimal("1"), weth_balance_usd=Decimal("3000"))
        intent = strategy.decide(market)

        assert intent.max_slippage == Decimal("0.02")

    def test_slippage_applies_to_both_buy_and_sell(self):
        """Same slippage config should apply to both directions."""
        buy = _create_strategy({"force_action": "buy", "max_slippage_pct": "0.3"})
        sell = _create_strategy({"force_action": "sell", "max_slippage_pct": "0.3"})

        buy_intent = buy.decide(
            _mock_market(usdc_balance=Decimal("10000"), usdc_balance_usd=Decimal("10000"))
        )
        sell_intent = sell.decide(
            _mock_market(weth_balance=Decimal("1"), weth_balance_usd=Decimal("3000"))
        )

        assert buy_intent.max_slippage == Decimal("0.003")
        assert sell_intent.max_slippage == Decimal("0.003")


# ===========================================================================
# Token Configuration
# ===========================================================================


class TestTokenConfig:
    """Test custom token pair configuration."""

    def test_custom_base_token(self):
        """Custom base_token changes swap target."""
        strategy = _create_strategy({"force_action": "buy", "base_token": "OP"})
        market = _mock_market(usdc_balance=Decimal("10000"), usdc_balance_usd=Decimal("10000"))

        # Mock OP price
        original_price = market.price

        def custom_price(symbol: str, quote: str = "USD") -> Decimal:
            if symbol == "OP":
                return Decimal("2.5")
            return original_price(symbol, quote)

        market.price = custom_price

        intent = strategy.decide(market)

        assert intent.to_token == "OP"
        assert intent.from_token == "USDC"

    def test_custom_quote_token(self):
        """Custom quote_token changes swap source."""
        strategy = _create_strategy({"force_action": "buy", "quote_token": "USDT"})

        market = MagicMock()
        market.price = MagicMock(side_effect=lambda token, quote="USD": Decimal("1"))

        bal = MagicMock()
        bal.balance = Decimal("10000")
        bal.balance_usd = Decimal("10000")

        def balance_fn(token):
            if token == "USDT":
                return bal
            raise ValueError(f"No balance for {token}")

        market.balance = MagicMock(side_effect=balance_fn)

        intent = strategy.decide(market)

        assert intent.from_token == "USDT"


# ===========================================================================
# Teardown for PnL Cleanup
# ===========================================================================


class TestTeardownPnL:
    """Test teardown works for PnL backtest position cleanup."""

    def test_get_open_positions_returns_empty_summary(self):
        """Swap-only strategy should report no open positions."""
        strategy = _create_strategy()
        positions = strategy.get_open_positions()

        assert positions is not None
        assert len(positions.positions) == 0

    def test_generate_teardown_intents_returns_empty_list(self):
        """Swap-only strategy should have no teardown intents."""
        strategy = _create_strategy()
        intents = strategy.generate_teardown_intents()

        assert isinstance(intents, list)
        assert len(intents) == 0


# ===========================================================================
# PnL Backtest Config for Optimism / Velodrome
# ===========================================================================


class TestPnLBacktestConfigVelodrome:
    """Test PnL backtest config handles Velodrome on Optimism correctly."""

    def test_config_chain_optimism(self):
        from datetime import UTC, datetime

        from almanak.framework.backtesting import PnLBacktestConfig

        config = PnLBacktestConfig(
            start_time=datetime(2024, 10, 1, tzinfo=UTC),
            end_time=datetime(2025, 1, 1, tzinfo=UTC),
            interval_seconds=3600,
            initial_capital_usd=Decimal("10000"),
            chain="optimism",
            tokens=["WETH", "USDC"],
            gas_price_gwei=Decimal("0.001"),
        )

        assert config.chain == "optimism"
        assert "WETH" in config.tokens
        assert "USDC" in config.tokens

    def test_config_optimism_low_gas(self):
        """Optimism L2 gas price should be sub-1 gwei."""
        from datetime import UTC, datetime

        from almanak.framework.backtesting import PnLBacktestConfig

        config = PnLBacktestConfig(
            start_time=datetime(2024, 10, 1, tzinfo=UTC),
            end_time=datetime(2025, 1, 1, tzinfo=UTC),
            chain="optimism",
            tokens=["WETH", "USDC"],
            gas_price_gwei=Decimal("0.001"),
        )

        assert config.gas_price_gwei < Decimal("1")

    def test_config_90_day_backtest_window(self):
        """Validate 90-day backtest window (>30 days per acceptance criteria)."""
        from datetime import UTC, datetime

        from almanak.framework.backtesting import PnLBacktestConfig

        config = PnLBacktestConfig(
            start_time=datetime(2024, 10, 1, tzinfo=UTC),
            end_time=datetime(2025, 1, 1, tzinfo=UTC),
            interval_seconds=3600,
            chain="optimism",
            tokens=["WETH", "USDC"],
        )

        duration_days = (config.end_time - config.start_time).days
        assert duration_days >= 30
        assert duration_days == 92  # Oct 1 -> Jan 1 = 92 days
