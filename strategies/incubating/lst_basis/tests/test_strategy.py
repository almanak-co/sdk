"""Tests for LST Basis Trading Strategy.

This test suite covers:
1. Configuration validation and serialization
2. Basis calculation and opportunity detection
3. State management and transitions
4. Intent creation and trade execution
5. Statistics tracking
"""

import time
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from almanak.framework.intents.vocabulary import HoldIntent, SwapIntent
from strategies.lst_basis import (
    LST_TOKEN_INFO,
    BasisDirection,
    LSTBasisConfig,
    LSTBasisOpportunity,
    LSTBasisState,
    LSTBasisStrategy,
)

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def default_config() -> LSTBasisConfig:
    """Create a default configuration for testing."""
    return LSTBasisConfig(
        strategy_id="test-lst-basis",
        chain="ethereum",
        wallet_address="0x1234567890123456789012345678901234567890",
        lst_tokens=["stETH", "rETH", "cbETH"],
        min_spread_bps=30,
        min_premium_bps=10,
        max_spread_bps=500,
        trade_premium=True,
        trade_discount=True,
        min_profit_usd=Decimal("10"),
        min_profit_bps=10,
        estimated_gas_cost_usd=Decimal("25"),
        default_trade_size_eth=Decimal("1"),
        max_slippage_bps=50,
        trade_cooldown_seconds=120,
    )


@pytest.fixture
def strategy(default_config: LSTBasisConfig) -> LSTBasisStrategy:
    """Create a strategy instance for testing."""
    return LSTBasisStrategy(config=default_config)


@pytest.fixture
def mock_market() -> MagicMock:
    """Create a mock market snapshot."""
    market = MagicMock()

    # Default prices - LST tokens at fair value
    def mock_price(token: str, quote: str = "USD") -> Decimal:
        if quote == "ETH":
            if token == "stETH":
                return Decimal("1.0")  # stETH is 1:1 rebasing
            elif token == "rETH":
                return Decimal("1.08")  # rETH accumulates value
            elif token == "cbETH":
                return Decimal("1.05")  # cbETH accumulates value
            else:
                return Decimal("1.0")
        elif quote == "USD":
            eth_price = Decimal("2500")
            if token == "ETH":
                return eth_price
            elif token == "stETH":
                return eth_price * Decimal("1.0")
            elif token == "rETH":
                return eth_price * Decimal("1.08")
            elif token == "cbETH":
                return eth_price * Decimal("1.05")
            else:
                return eth_price
        return Decimal("0")

    market.price = mock_price
    return market


# =============================================================================
# Configuration Tests
# =============================================================================


class TestLSTBasisConfig:
    """Tests for LSTBasisConfig."""

    def test_default_values(self) -> None:
        """Test default configuration values."""
        config = LSTBasisConfig()

        assert config.chain == "ethereum"
        assert config.lst_tokens == ["stETH", "rETH", "cbETH"]
        assert config.min_spread_bps == 30
        assert config.min_premium_bps == 10
        assert config.max_spread_bps == 500
        assert config.trade_premium is True
        assert config.trade_discount is True
        assert config.min_profit_usd == Decimal("10")

    def test_to_dict(self, default_config: LSTBasisConfig) -> None:
        """Test configuration serialization."""
        data = default_config.to_dict()

        assert data["strategy_id"] == "test-lst-basis"
        assert data["chain"] == "ethereum"
        assert data["lst_tokens"] == ["stETH", "rETH", "cbETH"]
        assert data["min_spread_bps"] == 30
        assert data["trade_premium"] is True

    def test_from_dict(self) -> None:
        """Test configuration deserialization."""
        data = {
            "strategy_id": "test-from-dict",
            "chain": "ethereum",
            "lst_tokens": ["stETH"],
            "min_spread_bps": 50,
            "trade_premium": False,
        }
        config = LSTBasisConfig.from_dict(data)

        assert config.strategy_id == "test-from-dict"
        assert config.lst_tokens == ["stETH"]
        assert config.min_spread_bps == 50
        assert config.trade_premium is False

    def test_calculate_spread_bps(self, default_config: LSTBasisConfig) -> None:
        """Test spread calculation."""
        # Premium case: market > fair value
        spread = default_config.calculate_spread_bps(
            market_price=Decimal("1.005"),
            fair_value=Decimal("1.0"),
        )
        assert spread == 50  # 0.5% premium

        # Discount case: market < fair value
        spread = default_config.calculate_spread_bps(
            market_price=Decimal("0.995"),
            fair_value=Decimal("1.0"),
        )
        assert spread == -50  # 0.5% discount

        # Fair value
        spread = default_config.calculate_spread_bps(
            market_price=Decimal("1.0"),
            fair_value=Decimal("1.0"),
        )
        assert spread == 0

    def test_is_premium(self, default_config: LSTBasisConfig) -> None:
        """Test premium detection."""
        assert default_config.is_premium(50) is True
        assert default_config.is_premium(10) is True
        assert default_config.is_premium(5) is False
        assert default_config.is_premium(-50) is False

    def test_is_discount(self, default_config: LSTBasisConfig) -> None:
        """Test discount detection."""
        assert default_config.is_discount(-50) is True
        assert default_config.is_discount(-10) is True
        assert default_config.is_discount(-5) is False
        assert default_config.is_discount(50) is False

    def test_is_opportunity(self, default_config: LSTBasisConfig) -> None:
        """Test opportunity detection."""
        # Valid opportunities
        assert default_config.is_opportunity(50) is True  # Premium
        assert default_config.is_opportunity(-50) is True  # Discount

        # Too small
        assert default_config.is_opportunity(20) is False
        assert default_config.is_opportunity(-20) is False

        # Too large (risky)
        assert default_config.is_opportunity(600) is False
        assert default_config.is_opportunity(-600) is False

    def test_is_opportunity_direction_disabled(self, default_config: LSTBasisConfig) -> None:
        """Test opportunity detection with direction disabled."""
        # Disable premium trading
        default_config.trade_premium = False
        assert default_config.is_opportunity(50) is False
        assert default_config.is_opportunity(-50) is True

        # Disable discount trading
        default_config.trade_premium = True
        default_config.trade_discount = False
        assert default_config.is_opportunity(50) is True
        assert default_config.is_opportunity(-50) is False

    def test_is_profitable(self, default_config: LSTBasisConfig) -> None:
        """Test profitability check."""
        # Profitable
        assert (
            default_config.is_profitable(
                gross_profit_usd=Decimal("50"),
                gross_profit_bps=20,
            )
            is True
        )

        # Below min bps
        assert (
            default_config.is_profitable(
                gross_profit_usd=Decimal("50"),
                gross_profit_bps=5,
            )
            is False
        )

        # Below min USD after gas
        assert (
            default_config.is_profitable(
                gross_profit_usd=Decimal("30"),  # $30 - $25 gas = $5 < $10 min
                gross_profit_bps=20,
            )
            is False
        )


# =============================================================================
# Strategy Initialization Tests
# =============================================================================


class TestStrategyInitialization:
    """Tests for strategy initialization."""

    def test_init_default_state(self, strategy: LSTBasisStrategy) -> None:
        """Test initial state is MONITORING."""
        assert strategy.get_state() == LSTBasisState.MONITORING

    def test_init_no_opportunity(self, strategy: LSTBasisStrategy) -> None:
        """Test no initial opportunity."""
        assert strategy.get_current_opportunity() is None

    def test_init_stats(self, strategy: LSTBasisStrategy) -> None:
        """Test initial statistics."""
        stats = strategy.get_stats()
        assert stats["state"] == "monitoring"
        assert stats["total_trades"] == 0
        assert stats["total_profit_usd"] == "0"
        assert stats["total_profit_eth"] == "0"


# =============================================================================
# State Management Tests
# =============================================================================


class TestStateManagement:
    """Tests for state management."""

    def test_paused_returns_hold(self, default_config: LSTBasisConfig, mock_market: MagicMock) -> None:
        """Test paused strategy returns hold."""
        default_config.pause_strategy = True
        strategy = LSTBasisStrategy(config=default_config)

        result = strategy.decide(mock_market)

        assert isinstance(result, HoldIntent)
        assert result.reason is not None
        assert "paused" in result.reason.lower()

    def test_cooldown_returns_hold(self, default_config: LSTBasisConfig, mock_market: MagicMock) -> None:
        """Test cooldown returns hold."""
        default_config.last_trade_timestamp = int(time.time())  # Just traded
        strategy = LSTBasisStrategy(config=default_config)

        result = strategy.decide(mock_market)

        assert isinstance(result, HoldIntent)
        assert result.reason is not None
        assert "cooldown" in result.reason.lower()

    def test_expired_cooldown_allows_trading(self, default_config: LSTBasisConfig) -> None:
        """Test expired cooldown allows trading."""
        default_config.trade_cooldown_seconds = 60
        default_config.last_trade_timestamp = int(time.time()) - 120  # 2 min ago
        strategy = LSTBasisStrategy(config=default_config)

        assert strategy._can_trade() is True
        assert strategy._cooldown_remaining() == 0


# =============================================================================
# Price and Fair Value Tests
# =============================================================================


class TestPriceCalculation:
    """Tests for price and fair value calculations."""

    def test_get_lst_price_direct(self, strategy: LSTBasisStrategy, mock_market: MagicMock) -> None:
        """Test getting LST price in ETH directly."""
        price = strategy._get_lst_price(mock_market, "stETH")
        assert price == Decimal("1.0")

        price = strategy._get_lst_price(mock_market, "rETH")
        assert price == Decimal("1.08")

    def test_get_lst_price_fallback(self, strategy: LSTBasisStrategy) -> None:
        """Test fallback price calculation via USD."""
        market = MagicMock()

        def mock_price(token: str, quote: str = "USD") -> Decimal:
            if quote == "ETH":
                raise Exception("ETH quote not available")
            if token == "stETH":
                return Decimal("2500")
            if token == "ETH":
                return Decimal("2500")
            return Decimal("0")

        market.price = mock_price

        price = strategy._get_lst_price(market, "stETH")
        assert price == Decimal("1.0")

    def test_get_fair_value_rebasing(self, strategy: LSTBasisStrategy, mock_market: MagicMock) -> None:
        """Test fair value for rebasing tokens (stETH)."""
        fair_value = strategy._get_fair_value(mock_market, "stETH")
        assert fair_value == Decimal("1.0")

    def test_get_fair_value_accumulating(self, strategy: LSTBasisStrategy, mock_market: MagicMock) -> None:
        """Test fair value for accumulating tokens (rETH, cbETH)."""
        fair_value = strategy._get_fair_value(mock_market, "rETH")
        assert fair_value == Decimal("1.08")

        fair_value = strategy._get_fair_value(mock_market, "cbETH")
        assert fair_value == Decimal("1.05")


# =============================================================================
# Opportunity Detection Tests
# =============================================================================


class TestOpportunityDetection:
    """Tests for opportunity detection."""

    def test_no_opportunity_at_fair_value(self, strategy: LSTBasisStrategy, mock_market: MagicMock) -> None:
        """Test no opportunity when LST at fair value."""
        # Mock market has LST at fair value
        result = strategy.decide(mock_market)

        assert isinstance(result, HoldIntent)
        assert result.reason is not None
        assert "no basis opportunity" in result.reason.lower()

    def test_premium_opportunity_detected(self, strategy: LSTBasisStrategy) -> None:
        """Test premium opportunity detection."""
        market = MagicMock()

        def mock_price(token: str, quote: str = "USD") -> Decimal:
            if quote == "ETH":
                if token == "stETH":
                    return Decimal("1.005")  # 0.5% premium
                return Decimal("1.0")
            if token == "ETH":
                return Decimal("2500")
            if token == "stETH":
                return Decimal("2512.5")  # 0.5% premium
            return Decimal("2500")

        market.price = mock_price

        result = strategy.decide(market)

        assert isinstance(result, SwapIntent)
        assert strategy.get_state() == LSTBasisState.COOLDOWN

        opportunity = strategy._current_opportunity
        # After trade execution, opportunity is cleared
        assert opportunity is None

    def test_discount_opportunity_detected(self, default_config: LSTBasisConfig) -> None:
        """Test discount opportunity detection."""
        strategy = LSTBasisStrategy(config=default_config)
        market = MagicMock()

        def mock_price(token: str, quote: str = "USD") -> Decimal:
            if quote == "ETH":
                if token == "stETH":
                    return Decimal("0.995")  # 0.5% discount
                return Decimal("1.0")
            if token == "ETH":
                return Decimal("2500")
            if token == "stETH":
                return Decimal("2487.5")  # 0.5% discount
            return Decimal("2500")

        market.price = mock_price

        result = strategy.decide(market)

        assert isinstance(result, SwapIntent)

    def test_check_token_opportunity_returns_opportunity(self, default_config: LSTBasisConfig) -> None:
        """Test _check_token_opportunity returns opportunity for valid spread."""
        # Use single token config to isolate test
        default_config.lst_tokens = ["stETH"]
        default_config.min_spread_bps = 30
        default_config.min_profit_usd = Decimal("5")  # Lower threshold
        default_config.estimated_gas_cost_usd = Decimal("5")  # Lower gas
        strategy = LSTBasisStrategy(config=default_config)
        market = MagicMock()

        def mock_price(token: str, quote: str = "USD") -> Decimal:
            if quote == "ETH":
                if token == "stETH":
                    return Decimal("1.005")  # 0.5% premium
                return Decimal("1.0")
            if token == "ETH":
                return Decimal("2500")
            return Decimal("2500")

        market.price = mock_price

        opportunity = strategy._check_token_opportunity(market, "stETH")

        assert opportunity is not None
        assert opportunity.lst_token == "stETH"
        assert opportunity.direction == BasisDirection.PREMIUM
        assert opportunity.spread_bps == 50


# =============================================================================
# Intent Creation Tests
# =============================================================================


class TestIntentCreation:
    """Tests for intent creation."""

    def test_premium_swap_intent(self, strategy: LSTBasisStrategy) -> None:
        """Test swap intent for premium (sell LST)."""
        opportunity = LSTBasisOpportunity(
            lst_token="stETH",
            direction=BasisDirection.PREMIUM,
            market_price=Decimal("1.005"),
            fair_value=Decimal("1.0"),
            spread_bps=50,
            trade_amount_eth=Decimal("1"),
            expected_profit_bps=46,
            expected_profit_usd=Decimal("11.5"),
            swap_protocol="curve",
            timestamp=datetime.now(UTC),
        )

        intent = strategy._create_swap_intent(opportunity)

        assert isinstance(intent, SwapIntent)
        assert intent.from_token == "stETH"
        assert intent.to_token == "ETH"
        assert intent.protocol == "curve"

    def test_discount_swap_intent(self, strategy: LSTBasisStrategy) -> None:
        """Test swap intent for discount (buy LST)."""
        opportunity = LSTBasisOpportunity(
            lst_token="rETH",
            direction=BasisDirection.DISCOUNT,
            market_price=Decimal("1.07"),
            fair_value=Decimal("1.08"),
            spread_bps=-93,
            trade_amount_eth=Decimal("1"),
            expected_profit_bps=89,
            expected_profit_usd=Decimal("22.25"),
            swap_protocol="curve",
            timestamp=datetime.now(UTC),
        )

        intent = strategy._create_swap_intent(opportunity)

        assert isinstance(intent, SwapIntent)
        assert intent.from_token == "ETH"
        assert intent.to_token == "rETH"


# =============================================================================
# Trade Recording Tests
# =============================================================================


class TestTradeRecording:
    """Tests for trade recording and statistics."""

    def test_record_trade_updates_stats(self, strategy: LSTBasisStrategy) -> None:
        """Test that recording a trade updates statistics."""
        opportunity = LSTBasisOpportunity(
            lst_token="stETH",
            direction=BasisDirection.PREMIUM,
            market_price=Decimal("1.005"),
            fair_value=Decimal("1.0"),
            spread_bps=50,
            trade_amount_eth=Decimal("1"),
            expected_profit_bps=46,
            expected_profit_usd=Decimal("50"),
            swap_protocol="curve",
            timestamp=datetime.now(UTC),
        )

        strategy._record_trade(opportunity)

        assert strategy.config.total_trades == 1
        assert strategy.config.last_trade_timestamp is not None
        assert strategy._state == LSTBasisState.COOLDOWN
        assert strategy._current_opportunity is None

    def test_record_trade_calculates_profit(self, strategy: LSTBasisStrategy) -> None:
        """Test profit calculation on trade recording."""
        opportunity = LSTBasisOpportunity(
            lst_token="stETH",
            direction=BasisDirection.PREMIUM,
            market_price=Decimal("1.005"),
            fair_value=Decimal("1.0"),
            spread_bps=50,
            trade_amount_eth=Decimal("1"),
            expected_profit_bps=46,
            expected_profit_usd=Decimal("50"),
            swap_protocol="curve",
            timestamp=datetime.now(UTC),
        )

        strategy._record_trade(opportunity)

        # Expected: $50 - $25 gas = $25
        assert strategy.config.total_profit_usd == Decimal("25")
        # ETH profit: 1 ETH * 46 bps = 0.0046 ETH
        assert strategy.config.total_profit_eth == Decimal("0.0046")


# =============================================================================
# Scan Basis Tests
# =============================================================================


class TestScanBasis:
    """Tests for manual basis scanning."""

    def test_scan_basis_returns_data(self, strategy: LSTBasisStrategy, mock_market: MagicMock) -> None:
        """Test scan_basis returns data for all configured tokens."""
        basis_data = strategy.scan_basis(mock_market)

        assert len(basis_data) == 3  # stETH, rETH, cbETH
        assert all("token" in item for item in basis_data)
        assert all("spread_bps" in item for item in basis_data)

    def test_scan_basis_includes_protocol(self, strategy: LSTBasisStrategy, mock_market: MagicMock) -> None:
        """Test scan_basis includes protocol information."""
        basis_data = strategy.scan_basis(mock_market)

        protocols = {item["token"]: item["protocol"] for item in basis_data}
        assert protocols["stETH"] == "Lido"
        assert protocols["rETH"] == "Rocket Pool"
        assert protocols["cbETH"] == "Coinbase"


# =============================================================================
# Clear State Tests
# =============================================================================


class TestClearState:
    """Tests for state clearing."""

    def test_clear_state_resets_all(self, strategy: LSTBasisStrategy) -> None:
        """Test clear_state resets all state."""
        # Set some state
        strategy.config.total_trades = 5
        strategy.config.total_profit_usd = Decimal("100")
        strategy.config.total_profit_eth = Decimal("0.1")
        strategy.config.last_trade_timestamp = int(time.time())

        strategy.clear_state()

        assert strategy.get_state() == LSTBasisState.MONITORING
        assert strategy.get_current_opportunity() is None
        assert strategy.config.total_trades == 0
        assert strategy.config.total_profit_usd == Decimal("0")
        assert strategy.config.total_profit_eth == Decimal("0")
        assert strategy.config.last_trade_timestamp is None


# =============================================================================
# LST Token Info Tests
# =============================================================================


class TestLSTTokenInfo:
    """Tests for LST token information."""

    def test_token_info_completeness(self) -> None:
        """Test all expected tokens are in LST_TOKEN_INFO."""
        expected_tokens = {"stETH", "wstETH", "rETH", "cbETH", "frxETH"}
        assert all(token in LST_TOKEN_INFO for token in expected_tokens)

    def test_token_info_fair_values(self) -> None:
        """Test fair value ratios are reasonable."""
        assert LST_TOKEN_INFO["stETH"].fair_value_ratio == Decimal("1.0")
        assert LST_TOKEN_INFO["rETH"].fair_value_ratio > Decimal("1.0")
        assert LST_TOKEN_INFO["cbETH"].fair_value_ratio > Decimal("1.0")

    def test_token_info_curve_pools(self) -> None:
        """Test Curve pool assignments."""
        assert LST_TOKEN_INFO["stETH"].curve_pool == "steth"
        assert LST_TOKEN_INFO["rETH"].curve_pool == "reth"
        assert LST_TOKEN_INFO["cbETH"].curve_pool == "cbeth"


# =============================================================================
# Edge Cases Tests
# =============================================================================


class TestEdgeCases:
    """Tests for edge cases."""

    def test_unknown_lst_token(self, default_config: LSTBasisConfig) -> None:
        """Test handling of unknown LST token."""
        default_config.lst_tokens = ["UNKNOWN_TOKEN"]
        strategy = LSTBasisStrategy(config=default_config)
        market = MagicMock()
        market.price = MagicMock(return_value=Decimal("1.0"))

        result = strategy.decide(market)

        # Should return hold since token is unknown
        assert isinstance(result, HoldIntent)

    def test_zero_fair_value(self, strategy: LSTBasisStrategy) -> None:
        """Test handling of zero fair value."""
        spread = strategy.config.calculate_spread_bps(
            market_price=Decimal("1.0"),
            fair_value=Decimal("0"),
        )
        assert spread == 0

    def test_opportunity_expiry(self, strategy: LSTBasisStrategy) -> None:
        """Test opportunity expiration."""
        # Create an old opportunity
        strategy._current_opportunity = LSTBasisOpportunity(
            lst_token="stETH",
            direction=BasisDirection.PREMIUM,
            market_price=Decimal("1.005"),
            fair_value=Decimal("1.0"),
            spread_bps=50,
            trade_amount_eth=Decimal("1"),
            expected_profit_bps=46,
            expected_profit_usd=Decimal("11.5"),
            swap_protocol="curve",
            timestamp=datetime.now(UTC) - timedelta(seconds=120),  # Old
        )
        strategy._state = LSTBasisState.OPPORTUNITY_FOUND

        # Update should clear expired opportunity
        strategy._update_state()

        assert strategy.get_state() == LSTBasisState.MONITORING
        assert strategy._current_opportunity is None


# =============================================================================
# Integration-style Tests
# =============================================================================


class TestIntegration:
    """Integration-style tests for complete flows."""

    def test_full_premium_trade_flow(self, default_config: LSTBasisConfig) -> None:
        """Test complete premium trade flow."""
        # Use single token config to isolate test
        default_config.lst_tokens = ["stETH"]
        default_config.min_profit_usd = Decimal("5")
        default_config.estimated_gas_cost_usd = Decimal("5")
        strategy = LSTBasisStrategy(config=default_config)
        market = MagicMock()

        def mock_price(token: str, quote: str = "USD") -> Decimal:
            if quote == "ETH":
                if token == "stETH":
                    return Decimal("1.005")  # 0.5% premium
                return Decimal("1.0")
            if token == "ETH":
                return Decimal("2500")
            return Decimal("2500")

        market.price = mock_price

        # First call should find opportunity and create intent
        result = strategy.decide(market)
        assert isinstance(result, SwapIntent)
        assert result.from_token == "stETH"
        assert result.to_token == "ETH"

        # State should be COOLDOWN
        assert strategy.get_state() == LSTBasisState.COOLDOWN

        # Stats should be updated
        assert strategy.config.total_trades == 1

    def test_full_discount_trade_flow(self, default_config: LSTBasisConfig) -> None:
        """Test complete discount trade flow."""
        # Use single token config to isolate test
        default_config.lst_tokens = ["stETH"]
        default_config.min_profit_usd = Decimal("5")
        default_config.estimated_gas_cost_usd = Decimal("5")
        strategy = LSTBasisStrategy(config=default_config)
        market = MagicMock()

        def mock_price(token: str, quote: str = "USD") -> Decimal:
            if quote == "ETH":
                if token == "stETH":
                    return Decimal("0.995")  # 0.5% discount
                return Decimal("1.0")
            if token == "ETH":
                return Decimal("2500")
            return Decimal("2500")

        market.price = mock_price

        result = strategy.decide(market)
        assert isinstance(result, SwapIntent)
        assert result.from_token == "ETH"
        assert result.to_token == "stETH"

    def test_cooldown_prevents_repeat_trades(self, default_config: LSTBasisConfig) -> None:
        """Test cooldown prevents repeat trades."""
        strategy = LSTBasisStrategy(config=default_config)
        market = MagicMock()

        def mock_price(token: str, quote: str = "USD") -> Decimal:
            if quote == "ETH":
                if token == "stETH":
                    return Decimal("1.005")  # Premium opportunity
                return Decimal("1.0")
            if token == "ETH":
                return Decimal("2500")
            return Decimal("2500")

        market.price = mock_price

        # First trade
        result1 = strategy.decide(market)
        assert isinstance(result1, SwapIntent)

        # Second call should be blocked by cooldown
        result2 = strategy.decide(market)
        assert isinstance(result2, HoldIntent)
        assert result2.reason is not None
        assert "cooldown" in result2.reason.lower()
