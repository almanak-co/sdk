"""Tests for Stablecoin Peg Arbitrage Strategy."""

import time
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from almanak.framework.intents.vocabulary import (
    HoldIntent,
    SwapIntent,
)
from strategies.stablecoin_peg_arb import (
    StablecoinPegArbConfig,
    StablecoinPegArbStrategy,
)
from strategies.stablecoin_peg_arb.strategy import (
    CURVE_POOL_TOKENS,
    DepegDirection,
    DepegOpportunity,
    PegArbState,
    get_pool_for_tokens,
)

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def config() -> StablecoinPegArbConfig:
    """Create a test configuration."""
    return StablecoinPegArbConfig(
        strategy_id="test-peg-arb",
        chain="ethereum",
        wallet_address="0x1234567890123456789012345678901234567890",
        stablecoins=["USDC", "USDT", "DAI", "FRAX"],
        curve_pools=["3pool", "frax_usdc"],
        depeg_threshold_bps=50,
        min_depeg_bps=10,
        max_depeg_bps=500,
        min_profit_usd=Decimal("5"),
        min_profit_bps=5,
        default_trade_size_usd=Decimal("10000"),
        estimated_gas_cost_usd=Decimal("15"),
        trade_cooldown_seconds=60,
    )


@pytest.fixture
def mock_market() -> MagicMock:
    """Create a mock market snapshot."""
    market = MagicMock()

    # Default all stablecoins at peg
    def price_side_effect(token: str, quote: str = "USD") -> Decimal:
        prices = {
            "USDC": Decimal("1.0000"),
            "USDT": Decimal("1.0000"),
            "DAI": Decimal("1.0000"),
            "FRAX": Decimal("1.0000"),
        }
        return prices.get(token, Decimal("1.0000"))

    market.price.side_effect = price_side_effect
    return market


@pytest.fixture
def strategy(config: StablecoinPegArbConfig) -> StablecoinPegArbStrategy:
    """Create a test strategy instance."""
    return StablecoinPegArbStrategy(config)


def create_depeg_opportunity(
    depegged_token: str = "USDC",
    stable_token: str = "USDT",
    direction: DepegDirection = DepegDirection.BELOW_PEG,
    current_price: Decimal = Decimal("0.9950"),
    depeg_bps: int = 50,
    trade_amount: Decimal = Decimal("10000"),
    expected_profit_bps: int = 46,
    expected_profit_usd: Decimal = Decimal("46"),
    curve_pool: str = "3pool",
) -> DepegOpportunity:
    """Helper to create a DepegOpportunity."""
    return DepegOpportunity(
        depegged_token=depegged_token,
        stable_token=stable_token,
        direction=direction,
        current_price=current_price,
        depeg_bps=depeg_bps,
        trade_amount=trade_amount,
        expected_profit_bps=expected_profit_bps,
        expected_profit_usd=expected_profit_usd,
        curve_pool=curve_pool,
        timestamp=datetime.now(UTC),
    )


# =============================================================================
# Configuration Tests
# =============================================================================


class TestStablecoinPegArbConfig:
    """Tests for StablecoinPegArbConfig."""

    def test_default_values(self) -> None:
        """Test default configuration values."""
        config = StablecoinPegArbConfig()
        assert config.chain == "ethereum"
        assert config.depeg_threshold_bps == 50
        assert config.min_profit_usd == Decimal("5")
        assert config.default_trade_size_usd == Decimal("10000")
        assert "USDC" in config.stablecoins
        assert "3pool" in config.curve_pools

    def test_to_dict(self, config: StablecoinPegArbConfig) -> None:
        """Test configuration serialization."""
        data = config.to_dict()
        assert data["strategy_id"] == "test-peg-arb"
        assert data["chain"] == "ethereum"
        assert data["depeg_threshold_bps"] == 50
        assert "USDC" in data["stablecoins"]

    def test_from_dict(self) -> None:
        """Test configuration deserialization."""
        data = {
            "strategy_id": "test-arb",
            "chain": "arbitrum",
            "wallet_address": "0xabc",
            "stablecoins": ["USDC", "USDT"],
            "depeg_threshold_bps": 75,
            "min_profit_usd": "10",
        }
        config = StablecoinPegArbConfig.from_dict(data)
        assert config.strategy_id == "test-arb"
        assert config.chain == "arbitrum"
        assert config.depeg_threshold_bps == 75
        assert config.min_profit_usd == Decimal("10")

    def test_calculate_depeg_bps_below_peg(self, config: StablecoinPegArbConfig) -> None:
        """Test depeg calculation for price below peg."""
        price = Decimal("0.9950")  # 50 bps below peg
        depeg_bps = config.calculate_depeg_bps(price)
        assert depeg_bps == 50

    def test_calculate_depeg_bps_above_peg(self, config: StablecoinPegArbConfig) -> None:
        """Test depeg calculation for price above peg."""
        price = Decimal("1.0050")  # 50 bps above peg
        depeg_bps = config.calculate_depeg_bps(price)
        assert depeg_bps == 50

    def test_calculate_depeg_bps_at_peg(self, config: StablecoinPegArbConfig) -> None:
        """Test depeg calculation for price at peg."""
        price = Decimal("1.0000")
        depeg_bps = config.calculate_depeg_bps(price)
        assert depeg_bps == 0

    def test_is_depegged_true(self, config: StablecoinPegArbConfig) -> None:
        """Test is_depegged returns true for significant depeg."""
        config.min_depeg_bps = 10
        config.max_depeg_bps = 500
        price = Decimal("0.9950")  # 50 bps depeg
        assert config.is_depegged(price) is True

    def test_is_depegged_false_too_small(self, config: StablecoinPegArbConfig) -> None:
        """Test is_depegged returns false for small depeg."""
        config.min_depeg_bps = 10
        price = Decimal("0.9995")  # 5 bps depeg (below minimum)
        assert config.is_depegged(price) is False

    def test_is_depegged_false_too_large(self, config: StablecoinPegArbConfig) -> None:
        """Test is_depegged returns false for extreme depeg."""
        config.max_depeg_bps = 500
        price = Decimal("0.9000")  # 1000 bps depeg (above maximum)
        assert config.is_depegged(price) is False

    def test_is_opportunity_true(self, config: StablecoinPegArbConfig) -> None:
        """Test is_opportunity returns true for tradeable depeg."""
        config.depeg_threshold_bps = 50
        price = Decimal("0.9940")  # 60 bps depeg (above threshold)
        assert config.is_opportunity(price) is True

    def test_is_opportunity_false(self, config: StablecoinPegArbConfig) -> None:
        """Test is_opportunity returns false for small depeg."""
        config.depeg_threshold_bps = 50
        price = Decimal("0.9960")  # 40 bps depeg (below threshold)
        assert config.is_opportunity(price) is False

    def test_is_profitable_meets_threshold(self, config: StablecoinPegArbConfig) -> None:
        """Test profitability check when meeting thresholds."""
        config.min_profit_bps = 5
        config.min_profit_usd = Decimal("5")
        config.estimated_gas_cost_usd = Decimal("15")

        # Gross 30 USD, 10 bps -> net 15 USD
        assert config.is_profitable(Decimal("30"), 10) is True

    def test_is_profitable_below_bps_threshold(self, config: StablecoinPegArbConfig) -> None:
        """Test profitability check below bps threshold."""
        config.min_profit_bps = 10
        # 5 bps is below 10 bps threshold
        assert config.is_profitable(Decimal("100"), 5) is False

    def test_is_profitable_below_usd_threshold(self, config: StablecoinPegArbConfig) -> None:
        """Test profitability check below USD threshold."""
        config.min_profit_usd = Decimal("10")
        config.estimated_gas_cost_usd = Decimal("15")
        # Gross 20 - 15 gas = 5 net (below 10 threshold)
        assert config.is_profitable(Decimal("20"), 50) is False


# =============================================================================
# Helper Function Tests
# =============================================================================


class TestHelperFunctions:
    """Tests for helper functions."""

    def test_get_pool_for_tokens_3pool(self) -> None:
        """Test pool lookup for 3pool tokens."""
        pool = get_pool_for_tokens("DAI", "USDC")
        assert pool == "3pool"

        pool = get_pool_for_tokens("USDC", "USDT")
        assert pool == "3pool"

    def test_get_pool_for_tokens_frax(self) -> None:
        """Test pool lookup for FRAX tokens."""
        pool = get_pool_for_tokens("FRAX", "USDC")
        assert pool == "frax_usdc"

    def test_get_pool_for_tokens_not_found(self) -> None:
        """Test pool lookup for unsupported pair."""
        pool = get_pool_for_tokens("WETH", "USDC")
        assert pool is None

    def test_curve_pool_tokens_content(self) -> None:
        """Test CURVE_POOL_TOKENS contains expected pools."""
        assert "3pool" in CURVE_POOL_TOKENS
        assert "DAI" in CURVE_POOL_TOKENS["3pool"]
        assert "USDC" in CURVE_POOL_TOKENS["3pool"]
        assert "USDT" in CURVE_POOL_TOKENS["3pool"]


# =============================================================================
# Strategy State Tests
# =============================================================================


class TestStrategyState:
    """Tests for strategy state management."""

    def test_initial_state(self, strategy: StablecoinPegArbStrategy) -> None:
        """Test strategy starts in monitoring state."""
        assert strategy.get_state() == PegArbState.MONITORING

    def test_get_current_opportunity_initially_none(self, strategy: StablecoinPegArbStrategy) -> None:
        """Test current opportunity is None initially."""
        assert strategy.get_current_opportunity() is None

    def test_get_stats(self, strategy: StablecoinPegArbStrategy) -> None:
        """Test stats retrieval."""
        stats = strategy.get_stats()
        assert stats["state"] == "monitoring"
        assert stats["total_trades"] == 0
        assert stats["total_profit_usd"] == "0"
        assert stats["cooldown_remaining"] == 0

    def test_clear_state(self, strategy: StablecoinPegArbStrategy) -> None:
        """Test state clearing."""
        # Set some state
        strategy._state = PegArbState.COOLDOWN
        strategy.config.total_trades = 5
        strategy.config.total_profit_usd = Decimal("100")

        # Clear
        strategy.clear_state()

        assert strategy.get_state() == PegArbState.MONITORING
        assert strategy.config.total_trades == 0
        assert strategy.config.total_profit_usd == Decimal("0")


# =============================================================================
# Cooldown Tests
# =============================================================================


class TestCooldown:
    """Tests for cooldown behavior."""

    def test_can_trade_initially(self, strategy: StablecoinPegArbStrategy) -> None:
        """Test trading is allowed initially."""
        assert strategy._can_trade() is True

    def test_can_trade_after_cooldown(self, strategy: StablecoinPegArbStrategy) -> None:
        """Test trading is allowed after cooldown."""
        strategy.config.trade_cooldown_seconds = 60
        strategy.config.last_trade_timestamp = int(time.time()) - 61
        assert strategy._can_trade() is True

    def test_cannot_trade_during_cooldown(self, strategy: StablecoinPegArbStrategy) -> None:
        """Test trading is blocked during cooldown."""
        strategy.config.trade_cooldown_seconds = 60
        strategy.config.last_trade_timestamp = int(time.time()) - 30
        assert strategy._can_trade() is False

    def test_cooldown_remaining(self, strategy: StablecoinPegArbStrategy) -> None:
        """Test cooldown remaining calculation."""
        strategy.config.trade_cooldown_seconds = 60
        strategy.config.last_trade_timestamp = int(time.time()) - 30
        remaining = strategy._cooldown_remaining()
        assert 25 <= remaining <= 35  # Allow some tolerance

    def test_cooldown_remaining_zero_when_expired(self, strategy: StablecoinPegArbStrategy) -> None:
        """Test cooldown remaining is zero when expired."""
        strategy.config.trade_cooldown_seconds = 60
        strategy.config.last_trade_timestamp = int(time.time()) - 100
        assert strategy._cooldown_remaining() == 0


# =============================================================================
# Depeg Detection Tests
# =============================================================================


class TestDepegDetection:
    """Tests for depeg detection."""

    def test_no_depeg_at_peg(
        self,
        strategy: StablecoinPegArbStrategy,
        mock_market: MagicMock,
    ) -> None:
        """Test no opportunity when all stablecoins at peg."""
        result = strategy.decide(mock_market)
        assert isinstance(result, HoldIntent)
        assert result.reason is not None
        assert "No depeg" in result.reason

    def test_detects_depeg_below_peg(
        self,
        strategy: StablecoinPegArbStrategy,
        mock_market: MagicMock,
    ) -> None:
        """Test detection of depeg below peg."""

        # USDC at 0.9940 (60 bps below peg)
        def price_side_effect(token: str, quote: str = "USD") -> Decimal:
            if token == "USDC":
                return Decimal("0.9940")
            return Decimal("1.0000")

        mock_market.price.side_effect = price_side_effect

        result = strategy.decide(mock_market)
        assert isinstance(result, SwapIntent)
        # Should buy USDC (cheap) with stable counterparty
        assert result.to_token == "USDC"
        assert result.from_token in ["USDT", "DAI", "FRAX"]

    def test_detects_depeg_above_peg(
        self,
        strategy: StablecoinPegArbStrategy,
        mock_market: MagicMock,
    ) -> None:
        """Test detection of depeg above peg."""

        # DAI at 1.0060 (60 bps above peg)
        def price_side_effect(token: str, quote: str = "USD") -> Decimal:
            if token == "DAI":
                return Decimal("1.0060")
            return Decimal("1.0000")

        mock_market.price.side_effect = price_side_effect

        result = strategy.decide(mock_market)
        assert isinstance(result, SwapIntent)
        # Should sell DAI (expensive) for stable counterparty
        assert result.from_token == "DAI"
        assert result.to_token in ["USDC", "USDT", "FRAX"]

    def test_ignores_small_depeg(
        self,
        strategy: StablecoinPegArbStrategy,
        mock_market: MagicMock,
    ) -> None:
        """Test ignoring depegs below threshold."""
        strategy.config.depeg_threshold_bps = 50

        # USDC at 0.9980 (only 20 bps, below 50 bps threshold)
        def price_side_effect(token: str, quote: str = "USD") -> Decimal:
            if token == "USDC":
                return Decimal("0.9980")
            return Decimal("1.0000")

        mock_market.price.side_effect = price_side_effect

        result = strategy.decide(mock_market)
        assert isinstance(result, HoldIntent)

    def test_ignores_extreme_depeg(
        self,
        strategy: StablecoinPegArbStrategy,
        mock_market: MagicMock,
    ) -> None:
        """Test ignoring extreme depegs (too risky)."""
        strategy.config.max_depeg_bps = 500

        # USDC at 0.9000 (1000 bps, above max threshold)
        def price_side_effect(token: str, quote: str = "USD") -> Decimal:
            if token == "USDC":
                return Decimal("0.9000")
            return Decimal("1.0000")

        mock_market.price.side_effect = price_side_effect

        result = strategy.decide(mock_market)
        assert isinstance(result, HoldIntent)


# =============================================================================
# Swap Intent Creation Tests
# =============================================================================


class TestSwapIntentCreation:
    """Tests for swap intent creation."""

    def test_create_swap_below_peg(self, strategy: StablecoinPegArbStrategy) -> None:
        """Test swap intent for below-peg depeg (buy cheap)."""
        opportunity = create_depeg_opportunity(
            depegged_token="USDC",
            stable_token="USDT",
            direction=DepegDirection.BELOW_PEG,
        )
        strategy._current_opportunity = opportunity

        intent = strategy._create_swap_intent(opportunity)

        assert isinstance(intent, SwapIntent)
        assert intent.from_token == "USDT"  # Sell stable
        assert intent.to_token == "USDC"  # Buy cheap depegged

    def test_create_swap_above_peg(self, strategy: StablecoinPegArbStrategy) -> None:
        """Test swap intent for above-peg depeg (sell expensive)."""
        opportunity = create_depeg_opportunity(
            depegged_token="DAI",
            stable_token="USDC",
            direction=DepegDirection.ABOVE_PEG,
            current_price=Decimal("1.0050"),
        )
        strategy._current_opportunity = opportunity

        intent = strategy._create_swap_intent(opportunity)

        assert isinstance(intent, SwapIntent)
        assert intent.from_token == "DAI"  # Sell expensive depegged
        assert intent.to_token == "USDC"  # Buy stable

    def test_swap_uses_curve_protocol(self, strategy: StablecoinPegArbStrategy) -> None:
        """Test swap intent uses Curve protocol."""
        opportunity = create_depeg_opportunity()
        strategy._current_opportunity = opportunity

        intent = strategy._create_swap_intent(opportunity)

        assert intent.protocol == "curve"

    def test_swap_applies_slippage(self, strategy: StablecoinPegArbStrategy) -> None:
        """Test swap intent applies max slippage."""
        strategy.config.max_slippage_bps = 30
        opportunity = create_depeg_opportunity()
        strategy._current_opportunity = opportunity

        intent = strategy._create_swap_intent(opportunity)

        assert intent.max_slippage == Decimal("0.003")  # 30 bps

    def test_swap_updates_trade_stats(self, strategy: StablecoinPegArbStrategy) -> None:
        """Test swap creation updates trade statistics."""
        opportunity = create_depeg_opportunity()
        strategy._current_opportunity = opportunity
        initial_trades = strategy.config.total_trades

        strategy._create_swap_intent(opportunity)

        assert strategy.config.total_trades == initial_trades + 1
        assert strategy.config.last_trade_timestamp is not None


# =============================================================================
# Opportunity Finding Tests
# =============================================================================


class TestOpportunityFinding:
    """Tests for opportunity finding logic."""

    def test_find_best_opportunity_single_depeg(
        self,
        strategy: StablecoinPegArbStrategy,
        mock_market: MagicMock,
    ) -> None:
        """Test finding opportunity with single depeg."""

        def price_side_effect(token: str, quote: str = "USD") -> Decimal:
            if token == "USDC":
                return Decimal("0.9940")  # 60 bps depeg
            return Decimal("1.0000")

        mock_market.price.side_effect = price_side_effect

        opportunity = strategy._find_best_opportunity(mock_market)

        assert opportunity is not None
        assert opportunity.depegged_token == "USDC"
        assert opportunity.direction == DepegDirection.BELOW_PEG
        assert opportunity.depeg_bps == 60

    def test_find_best_opportunity_multiple_depegs(
        self,
        strategy: StablecoinPegArbStrategy,
        mock_market: MagicMock,
    ) -> None:
        """Test selecting best opportunity from multiple depegs."""

        def price_side_effect(token: str, quote: str = "USD") -> Decimal:
            prices = {
                "USDC": Decimal("0.9950"),  # 50 bps depeg
                "DAI": Decimal("0.9930"),  # 70 bps depeg (better)
                "USDT": Decimal("1.0000"),  # At peg
                "FRAX": Decimal("1.0000"),  # At peg
            }
            return prices.get(token, Decimal("1.0000"))

        mock_market.price.side_effect = price_side_effect

        opportunity = strategy._find_best_opportunity(mock_market)

        assert opportunity is not None
        assert opportunity.depegged_token == "DAI"  # Best opportunity
        assert opportunity.depeg_bps == 70

    def test_find_stable_counterparty(self, strategy: StablecoinPegArbStrategy) -> None:
        """Test finding stable counterparty."""
        prices = {
            "USDC": Decimal("0.9940"),  # Depegged
            "USDT": Decimal("1.0000"),  # Stable
            "DAI": Decimal("0.9990"),  # Very slight depeg
        }

        counterparty = strategy._find_stable_counterparty(prices, "USDC")

        # Should pick USDT (exactly at peg)
        assert counterparty == "USDT"

    def test_find_stable_counterparty_none_stable(self, strategy: StablecoinPegArbStrategy) -> None:
        """Test no counterparty when all depegged."""
        strategy.config.min_depeg_bps = 10
        prices = {
            "USDC": Decimal("0.9940"),  # Depegged
            "USDT": Decimal("0.9930"),  # Also depegged
            "DAI": Decimal("0.9920"),  # Also depegged
        }

        counterparty = strategy._find_stable_counterparty(prices, "USDC")

        # No stable counterparty available
        assert counterparty is None


# =============================================================================
# Depeg Opportunity Tests
# =============================================================================


class TestDepegOpportunity:
    """Tests for DepegOpportunity dataclass."""

    def test_to_dict(self) -> None:
        """Test opportunity serialization."""
        opportunity = create_depeg_opportunity()
        data = opportunity.to_dict()

        assert data["depegged_token"] == "USDC"
        assert data["stable_token"] == "USDT"
        assert data["direction"] == "below_peg"
        assert data["depeg_bps"] == 50
        assert "timestamp" in data


# =============================================================================
# Scan Depegs Tests
# =============================================================================


class TestScanDepegs:
    """Tests for scan_depegs method."""

    def test_scan_depegs_all_at_peg(
        self,
        strategy: StablecoinPegArbStrategy,
        mock_market: MagicMock,
    ) -> None:
        """Test scanning when all at peg."""
        depegs = strategy.scan_depegs(mock_market)

        assert len(depegs) == 4  # All configured stablecoins
        for depeg in depegs:
            if "error" not in depeg:
                assert depeg["depeg_bps"] == 0
                assert depeg["is_opportunity"] is False

    def test_scan_depegs_with_depeg(
        self,
        strategy: StablecoinPegArbStrategy,
        mock_market: MagicMock,
    ) -> None:
        """Test scanning with a depeg."""

        def price_side_effect(token: str, quote: str = "USD") -> Decimal:
            if token == "DAI":
                return Decimal("0.9940")
            return Decimal("1.0000")

        mock_market.price.side_effect = price_side_effect

        depegs = strategy.scan_depegs(mock_market)

        dai_depeg = next(d for d in depegs if d.get("token") == "DAI")
        assert dai_depeg["depeg_bps"] == 60
        assert dai_depeg["direction"] == "below_peg"
        assert dai_depeg["is_opportunity"] is True


# =============================================================================
# Paused Strategy Tests
# =============================================================================


class TestPausedStrategy:
    """Tests for paused strategy behavior."""

    def test_paused_returns_hold(
        self,
        strategy: StablecoinPegArbStrategy,
        mock_market: MagicMock,
    ) -> None:
        """Test paused strategy returns hold intent."""
        strategy.config.pause_strategy = True

        result = strategy.decide(mock_market)

        assert isinstance(result, HoldIntent)
        assert result.reason is not None
        assert "paused" in result.reason.lower()


# =============================================================================
# Cooldown State Tests
# =============================================================================


class TestCooldownState:
    """Tests for cooldown state behavior."""

    def test_cooldown_returns_hold(
        self,
        strategy: StablecoinPegArbStrategy,
        mock_market: MagicMock,
    ) -> None:
        """Test cooldown state returns hold intent."""
        strategy.config.last_trade_timestamp = int(time.time())
        strategy.config.trade_cooldown_seconds = 60

        result = strategy.decide(mock_market)

        assert isinstance(result, HoldIntent)
        assert result.reason is not None
        assert "cooldown" in result.reason.lower()


# =============================================================================
# Opportunity Expiry Tests
# =============================================================================


class TestOpportunityExpiry:
    """Tests for opportunity expiry behavior."""

    def test_expired_opportunity_returns_to_monitoring(
        self,
        strategy: StablecoinPegArbStrategy,
        mock_market: MagicMock,
    ) -> None:
        """Test expired opportunity returns to monitoring state."""
        # Create an old opportunity
        old_opportunity = create_depeg_opportunity()
        old_opportunity.timestamp = datetime.now(UTC) - timedelta(seconds=120)
        strategy._current_opportunity = old_opportunity
        strategy._state = PegArbState.OPPORTUNITY_FOUND
        strategy.config.opportunity_expiry_seconds = 30

        # Update state should detect expiry
        strategy._update_state()

        assert strategy._state == PegArbState.MONITORING
        assert strategy._current_opportunity is None
