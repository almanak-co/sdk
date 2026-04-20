"""Unit tests for RateMonitor service.

Tests cover:
- Initialization and configuration
- Protocol support per chain
- Rate fetching (mocked)
- Caching behavior
- Cross-protocol comparison
- Error handling
"""

import asyncio
from decimal import Decimal

import pytest

from ..monitor import (
    PROTOCOL_CHAINS,
    RAY,
    SUPPORTED_PROTOCOLS,
    SUPPORTED_TOKENS,
    BestRateResult,
    LendingRate,
    ProtocolNotSupportedError,
    RateMonitor,
    RateSide,
    TokenNotSupportedError,
)

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def ethereum_monitor() -> RateMonitor:
    """Create a RateMonitor for Ethereum mainnet."""
    return RateMonitor(chain="ethereum")


@pytest.fixture
def arbitrum_monitor() -> RateMonitor:
    """Create a RateMonitor for Arbitrum."""
    return RateMonitor(chain="arbitrum")


@pytest.fixture
def mocked_monitor() -> RateMonitor:
    """Create a RateMonitor with mocked rates."""
    monitor = RateMonitor(chain="ethereum")
    # Set up mock rates
    monitor.set_mock_rate("aave_v3", "USDC", "supply", Decimal("4.5"))
    monitor.set_mock_rate("aave_v3", "USDC", "borrow", Decimal("6.0"))
    monitor.set_mock_rate("morpho_blue", "USDC", "supply", Decimal("5.5"))
    monitor.set_mock_rate("morpho_blue", "USDC", "borrow", Decimal("5.5"))
    monitor.set_mock_rate("compound_v3", "USDC", "supply", Decimal("5.0"))
    monitor.set_mock_rate("compound_v3", "USDC", "borrow", Decimal("6.5"))
    monitor.set_mock_rate("aave_v3", "WETH", "supply", Decimal("2.0"))
    monitor.set_mock_rate("aave_v3", "WETH", "borrow", Decimal("3.5"))
    return monitor


# =============================================================================
# Initialization Tests
# =============================================================================


class TestRateMonitorInit:
    """Tests for RateMonitor initialization."""

    def test_default_chain(self) -> None:
        """Test default chain is ethereum."""
        monitor = RateMonitor()
        assert monitor.chain == "ethereum"

    def test_custom_chain(self) -> None:
        """Test custom chain configuration."""
        monitor = RateMonitor(chain="arbitrum")
        assert monitor.chain == "arbitrum"

    def test_protocols_on_ethereum(self, ethereum_monitor: RateMonitor) -> None:
        """Test all protocols available on Ethereum."""
        protocols = ethereum_monitor.protocols
        assert "aave_v3" in protocols
        assert "morpho_blue" in protocols
        assert "compound_v3" in protocols

    def test_protocols_on_arbitrum(self, arbitrum_monitor: RateMonitor) -> None:
        """Test protocols available on Arbitrum."""
        protocols = arbitrum_monitor.protocols
        assert "aave_v3" in protocols
        assert "compound_v3" in protocols
        # Morpho Blue not on Arbitrum
        assert "morpho_blue" not in protocols

    def test_custom_protocols(self) -> None:
        """Test filtering protocols."""
        monitor = RateMonitor(chain="ethereum", protocols=["aave_v3"])
        assert monitor.protocols == ["aave_v3"]

    def test_custom_cache_ttl(self) -> None:
        """Test custom cache TTL."""
        monitor = RateMonitor(cache_ttl_seconds=60.0)
        assert monitor._cache_ttl_seconds == 60.0


# =============================================================================
# Mock Rate Tests
# =============================================================================


class TestMockRates:
    """Tests for mock rate functionality."""

    def test_set_mock_rate(self) -> None:
        """Test setting a mock rate."""
        monitor = RateMonitor(chain="ethereum")
        monitor.set_mock_rate("aave_v3", "USDC", "supply", Decimal("5.0"))

        assert "aave_v3" in monitor._mock_rates
        assert "USDC" in monitor._mock_rates["aave_v3"]
        assert monitor._mock_rates["aave_v3"]["USDC"]["supply"] == Decimal("5.0")

    def test_clear_mock_rates(self) -> None:
        """Test clearing mock rates."""
        monitor = RateMonitor(chain="ethereum")
        monitor.set_mock_rate("aave_v3", "USDC", "supply", Decimal("5.0"))
        monitor.clear_mock_rates()

        assert len(monitor._mock_rates) == 0

    @pytest.mark.asyncio
    async def test_mock_rate_fetched(self, mocked_monitor: RateMonitor) -> None:
        """Test that mock rates are returned."""
        rate = await mocked_monitor.get_lending_rate("aave_v3", "USDC", RateSide.SUPPLY)

        assert rate.protocol == "aave_v3"
        assert rate.token == "USDC"
        assert rate.side == "supply"
        assert rate.apy_percent == Decimal("4.5")


# =============================================================================
# Rate Fetching Tests
# =============================================================================


class TestRateFetching:
    """Tests for rate fetching functionality."""

    @pytest.mark.asyncio
    async def test_get_aave_rate(self, ethereum_monitor: RateMonitor) -> None:
        """Test fetching Aave V3 rate."""
        rate = await ethereum_monitor.get_lending_rate("aave_v3", "USDC", RateSide.SUPPLY)

        assert rate.protocol == "aave_v3"
        assert rate.token == "USDC"
        assert rate.side == "supply"
        assert rate.apy_percent > Decimal("0")
        assert rate.chain == "ethereum"

    @pytest.mark.asyncio
    async def test_get_morpho_rate(self, ethereum_monitor: RateMonitor) -> None:
        """Test fetching Morpho Blue rate."""
        rate = await ethereum_monitor.get_lending_rate("morpho_blue", "USDC", RateSide.SUPPLY)

        assert rate.protocol == "morpho_blue"
        assert rate.token == "USDC"
        assert rate.apy_percent > Decimal("0")

    @pytest.mark.asyncio
    async def test_get_compound_rate(self, ethereum_monitor: RateMonitor) -> None:
        """Test fetching Compound V3 rate."""
        rate = await ethereum_monitor.get_lending_rate("compound_v3", "USDC", RateSide.SUPPLY)

        assert rate.protocol == "compound_v3"
        assert rate.token == "USDC"
        assert rate.apy_percent > Decimal("0")

    @pytest.mark.asyncio
    async def test_get_borrow_rate(self, ethereum_monitor: RateMonitor) -> None:
        """Test fetching borrow rate."""
        rate = await ethereum_monitor.get_lending_rate("aave_v3", "USDC", RateSide.BORROW)

        assert rate.side == "borrow"
        # Borrow rate typically higher than supply
        supply_rate = await ethereum_monitor.get_lending_rate("aave_v3", "USDC", RateSide.SUPPLY)
        assert rate.apy_percent > supply_rate.apy_percent

    @pytest.mark.asyncio
    async def test_unsupported_protocol(self, ethereum_monitor: RateMonitor) -> None:
        """Test error for unsupported protocol."""
        with pytest.raises(ProtocolNotSupportedError):
            await ethereum_monitor.get_lending_rate("unknown_protocol", "USDC", RateSide.SUPPLY)

    @pytest.mark.asyncio
    async def test_unsupported_token(self, ethereum_monitor: RateMonitor) -> None:
        """Test error for unsupported token."""
        with pytest.raises(TokenNotSupportedError):
            await ethereum_monitor.get_lending_rate("compound_v3", "UNKNOWN_TOKEN", RateSide.SUPPLY)

    @pytest.mark.asyncio
    async def test_protocol_not_on_chain(self, arbitrum_monitor: RateMonitor) -> None:
        """Test error for protocol not available on chain."""
        with pytest.raises(ProtocolNotSupportedError):
            await arbitrum_monitor.get_lending_rate("morpho_blue", "USDC", RateSide.SUPPLY)


# =============================================================================
# Caching Tests
# =============================================================================


class TestCaching:
    """Tests for rate caching."""

    @pytest.mark.asyncio
    async def test_rate_cached(self, mocked_monitor: RateMonitor) -> None:
        """Test that rates are cached."""
        # First call
        rate1 = await mocked_monitor.get_lending_rate("aave_v3", "USDC", RateSide.SUPPLY)

        # Check cache
        cached = mocked_monitor._get_cached_rate("aave_v3", "USDC", "supply")
        assert cached is not None
        assert cached.apy_percent == rate1.apy_percent

    @pytest.mark.asyncio
    async def test_cache_hit(self, mocked_monitor: RateMonitor) -> None:
        """Test cache hit returns same rate."""
        rate1 = await mocked_monitor.get_lending_rate("aave_v3", "USDC", RateSide.SUPPLY)
        rate2 = await mocked_monitor.get_lending_rate("aave_v3", "USDC", RateSide.SUPPLY)

        # Same timestamp means cache hit
        assert rate1.timestamp == rate2.timestamp

    def test_clear_cache(self, mocked_monitor: RateMonitor) -> None:
        """Test clearing cache."""
        # Populate cache
        asyncio.run(mocked_monitor.get_lending_rate("aave_v3", "USDC", RateSide.SUPPLY))

        # Clear and verify
        mocked_monitor.clear_cache()
        assert len(mocked_monitor._cache) == 0

    def test_cache_stats(self, mocked_monitor: RateMonitor) -> None:
        """Test cache statistics."""
        # Populate cache
        asyncio.run(mocked_monitor.get_lending_rate("aave_v3", "USDC", RateSide.SUPPLY))
        asyncio.run(mocked_monitor.get_lending_rate("aave_v3", "WETH", RateSide.SUPPLY))

        stats = mocked_monitor.get_cache_stats()
        assert stats["total_entries"] == 2
        assert "aave_v3" in stats["protocols"]


# =============================================================================
# Best Rate Tests
# =============================================================================


class TestBestRate:
    """Tests for cross-protocol rate comparison."""

    @pytest.mark.asyncio
    async def test_best_supply_rate(self, mocked_monitor: RateMonitor) -> None:
        """Test finding best supply rate."""
        result = await mocked_monitor.get_best_lending_rate("USDC", RateSide.SUPPLY)

        assert result.token == "USDC"
        assert result.side == "supply"
        assert result.best_rate is not None
        # Morpho has highest supply rate (5.5%)
        assert result.best_rate.protocol == "morpho_blue"
        assert result.best_rate.apy_percent == Decimal("5.5")

    @pytest.mark.asyncio
    async def test_best_borrow_rate(self, mocked_monitor: RateMonitor) -> None:
        """Test finding best borrow rate (lowest)."""
        result = await mocked_monitor.get_best_lending_rate("USDC", RateSide.BORROW)

        assert result.best_rate is not None
        # Morpho has lowest borrow rate (5.5%)
        assert result.best_rate.protocol == "morpho_blue"
        assert result.best_rate.apy_percent == Decimal("5.5")

    @pytest.mark.asyncio
    async def test_all_rates_returned(self, mocked_monitor: RateMonitor) -> None:
        """Test that all protocol rates are returned."""
        result = await mocked_monitor.get_best_lending_rate("USDC", RateSide.SUPPLY)

        assert len(result.all_rates) == 3
        protocols = {r.protocol for r in result.all_rates}
        assert "aave_v3" in protocols
        assert "morpho_blue" in protocols
        assert "compound_v3" in protocols

    @pytest.mark.asyncio
    async def test_filter_protocols(self, mocked_monitor: RateMonitor) -> None:
        """Test filtering protocols in comparison."""
        result = await mocked_monitor.get_best_lending_rate(
            "USDC", RateSide.SUPPLY, protocols=["aave_v3", "compound_v3"]
        )

        assert len(result.all_rates) == 2
        protocols = {r.protocol for r in result.all_rates}
        assert "morpho_blue" not in protocols


# =============================================================================
# Protocol Rates Tests
# =============================================================================


class TestProtocolRates:
    """Tests for fetching all rates from a protocol."""

    @pytest.mark.asyncio
    async def test_get_protocol_rates(self, mocked_monitor: RateMonitor) -> None:
        """Test fetching all rates for a protocol."""
        rates = await mocked_monitor.get_protocol_rates("aave_v3", tokens=["USDC", "WETH"])

        assert rates.protocol == "aave_v3"
        assert rates.chain == "ethereum"
        assert "USDC" in rates.rates
        assert "WETH" in rates.rates

    @pytest.mark.asyncio
    async def test_protocol_rates_both_sides(self, mocked_monitor: RateMonitor) -> None:
        """Test that both supply and borrow rates are fetched."""
        rates = await mocked_monitor.get_protocol_rates("aave_v3", tokens=["USDC"])

        usdc_rates = rates.rates["USDC"]
        assert "supply" in usdc_rates
        assert "borrow" in usdc_rates

    @pytest.mark.asyncio
    async def test_get_rate_from_protocol_rates(self, mocked_monitor: RateMonitor) -> None:
        """Test helper to get specific rate from ProtocolRates."""
        rates = await mocked_monitor.get_protocol_rates("aave_v3", tokens=["USDC"])

        supply_rate = rates.get_rate("USDC", "supply")
        assert supply_rate is not None
        assert supply_rate.apy_percent == Decimal("4.5")

    @pytest.mark.asyncio
    async def test_unsupported_protocol_rates(self, arbitrum_monitor: RateMonitor) -> None:
        """Test error for unsupported protocol."""
        with pytest.raises(ProtocolNotSupportedError):
            await arbitrum_monitor.get_protocol_rates("morpho_blue")


# =============================================================================
# Data Class Tests
# =============================================================================


class TestLendingRate:
    """Tests for LendingRate dataclass."""

    def test_to_dict(self) -> None:
        """Test serialization to dict."""
        rate = LendingRate(
            protocol="aave_v3",
            token="USDC",
            side="supply",
            apy_ray=Decimal("42500000000000000000000000"),  # 4.25%
            apy_percent=Decimal("4.25"),
            utilization_percent=Decimal("72.5"),
            chain="ethereum",
        )

        d = rate.to_dict()
        assert d["protocol"] == "aave_v3"
        assert d["token"] == "USDC"
        assert d["apy_percent"] == 4.25
        assert d["utilization_percent"] == 72.5


class TestBestRateResult:
    """Tests for BestRateResult dataclass."""

    def test_empty_result(self) -> None:
        """Test empty result when no rates found."""
        result = BestRateResult(
            token="UNKNOWN",
            side="supply",
            best_rate=None,
            all_rates=[],
        )

        assert result.best_rate is None
        assert len(result.all_rates) == 0

    def test_to_dict(self) -> None:
        """Test serialization to dict."""
        rate = LendingRate(
            protocol="aave_v3",
            token="USDC",
            side="supply",
            apy_ray=RAY * Decimal("5") / Decimal("100"),
            apy_percent=Decimal("5.0"),
            chain="ethereum",
        )
        result = BestRateResult(
            token="USDC",
            side="supply",
            best_rate=rate,
            all_rates=[rate],
        )

        d = result.to_dict()
        assert d["token"] == "USDC"
        assert d["best_rate"] is not None
        assert len(d["all_rates"]) == 1


# =============================================================================
# Constants Tests
# =============================================================================


class TestConstants:
    """Tests for module constants."""

    def test_supported_protocols(self) -> None:
        """Test supported protocols list."""
        assert "aave_v3" in SUPPORTED_PROTOCOLS
        assert "morpho_blue" in SUPPORTED_PROTOCOLS
        assert "compound_v3" in SUPPORTED_PROTOCOLS

    def test_protocol_chains(self) -> None:
        """Test protocol availability per chain."""
        # Ethereum has all protocols
        assert len(PROTOCOL_CHAINS["ethereum"]) == 3

        # Arbitrum has fewer
        assert "morpho_blue" not in PROTOCOL_CHAINS["arbitrum"]

    def test_supported_tokens(self) -> None:
        """Test supported tokens per chain."""
        assert "USDC" in SUPPORTED_TOKENS["ethereum"]
        assert "WETH" in SUPPORTED_TOKENS["ethereum"]
        assert "ARB" in SUPPORTED_TOKENS["arbitrum"]


# =============================================================================
# Integration-like Tests
# =============================================================================


class TestRateMonitorIntegration:
    """Integration-like tests using mock rates."""

    @pytest.mark.asyncio
    async def test_rate_arbitrage_scenario(self, mocked_monitor: RateMonitor) -> None:
        """Test scenario: find rate arbitrage opportunity."""
        # Find best supply rate
        supply_result = await mocked_monitor.get_best_lending_rate("USDC", RateSide.SUPPLY)
        # Find best borrow rate
        borrow_result = await mocked_monitor.get_best_lending_rate("USDC", RateSide.BORROW)

        assert supply_result.best_rate is not None
        assert borrow_result.best_rate is not None

        # Calculate spread
        spread = supply_result.best_rate.apy_percent - borrow_result.best_rate.apy_percent
        # Morpho has 5.5% supply, 5.5% borrow = 0 spread
        assert spread == Decimal("0")

    @pytest.mark.asyncio
    async def test_cross_protocol_comparison(self, mocked_monitor: RateMonitor) -> None:
        """Test comparing rates across all protocols."""
        result = await mocked_monitor.get_best_lending_rate("USDC", RateSide.SUPPLY)

        # Sort by APY
        sorted_rates = sorted(result.all_rates, key=lambda r: r.apy_percent, reverse=True)

        # Verify order: Morpho (5.5%) > Compound (5.0%) > Aave (4.5%)
        assert sorted_rates[0].protocol == "morpho_blue"
        assert sorted_rates[1].protocol == "compound_v3"
        assert sorted_rates[2].protocol == "aave_v3"
