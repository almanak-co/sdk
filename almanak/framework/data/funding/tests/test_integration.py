"""Integration tests for the FundingRateProvider service.

These tests verify the funding rate provider with actual API calls.
They are designed to run on Anvil fork where applicable, or against
real APIs for Hyperliquid (which has its own L1).

Note: GMX V2 rates are fetched from Arbitrum, Hyperliquid from its own chain.
"""

from decimal import Decimal

import pytest

from almanak.framework.data.funding import (
    SUPPORTED_MARKETS,
    FundingRateProvider,
    Venue,
)

# =============================================================================
# Integration Test Configuration
# =============================================================================


@pytest.fixture
def provider() -> FundingRateProvider:
    """Create a FundingRateProvider instance for integration tests."""
    return FundingRateProvider()


# =============================================================================
# GMX V2 Integration Tests
# =============================================================================


class TestGMXV2Integration:
    """Integration tests for GMX V2 funding rates.

    These tests verify GMX V2 rate fetching works correctly.
    In production, these would require an Anvil fork of Arbitrum.
    """

    @pytest.mark.asyncio
    async def test_fetch_eth_rate(self, provider: FundingRateProvider) -> None:
        """Test fetching ETH-USD funding rate from GMX V2."""
        rate = await provider.get_funding_rate(Venue.GMX_V2, "ETH-USD")

        # Verify rate structure
        assert rate.venue == "gmx_v2"
        assert rate.market == "ETH-USD"
        assert rate.rate_hourly > Decimal("0")
        assert rate.rate_8h > Decimal("0")
        assert rate.rate_annualized > Decimal("0")

        # Verify 8h rate is 8x hourly
        assert rate.rate_8h == rate.rate_hourly * Decimal("8")

        # Verify annualized rate calculation
        expected_annual = rate.rate_hourly * Decimal("8760")
        assert rate.rate_annualized == expected_annual

    @pytest.mark.asyncio
    async def test_fetch_btc_rate(self, provider: FundingRateProvider) -> None:
        """Test fetching BTC-USD funding rate from GMX V2."""
        rate = await provider.get_funding_rate(Venue.GMX_V2, "BTC-USD")

        assert rate.venue == "gmx_v2"
        assert rate.market == "BTC-USD"
        assert rate.rate_hourly > Decimal("0")
        assert rate.next_funding_time is not None

    @pytest.mark.asyncio
    async def test_fetch_all_gmx_markets(self, provider: FundingRateProvider) -> None:
        """Test fetching rates for all supported GMX V2 markets."""
        rates = await provider.get_all_funding_rates(Venue.GMX_V2)

        # All markets should have rates
        assert len(rates) == len(SUPPORTED_MARKETS["gmx_v2"])

        for market, rate in rates.items():
            assert rate.venue == "gmx_v2"
            assert rate.market == market
            assert rate.rate_hourly >= Decimal("0")

    @pytest.mark.asyncio
    async def test_gmx_open_interest(self, provider: FundingRateProvider) -> None:
        """Test that GMX V2 rates include open interest data."""
        rate = await provider.get_funding_rate(Venue.GMX_V2, "ETH-USD")

        # Open interest should be present and positive
        assert rate.open_interest_long is not None
        assert rate.open_interest_short is not None
        assert rate.open_interest_long > Decimal("0")
        assert rate.open_interest_short > Decimal("0")

    @pytest.mark.asyncio
    async def test_gmx_mark_price(self, provider: FundingRateProvider) -> None:
        """Test that GMX V2 rates include mark/index prices."""
        rate = await provider.get_funding_rate(Venue.GMX_V2, "ETH-USD")

        assert rate.mark_price is not None
        assert rate.index_price is not None
        assert rate.mark_price > Decimal("0")
        assert rate.index_price > Decimal("0")


# =============================================================================
# Hyperliquid Integration Tests
# =============================================================================


class TestHyperliquidIntegration:
    """Integration tests for Hyperliquid funding rates.

    These tests verify Hyperliquid rate fetching works correctly.
    Hyperliquid has its own L1 chain.
    """

    @pytest.mark.asyncio
    async def test_fetch_eth_rate(self, provider: FundingRateProvider) -> None:
        """Test fetching ETH-USD funding rate from Hyperliquid."""
        rate = await provider.get_funding_rate(Venue.HYPERLIQUID, "ETH-USD")

        assert rate.venue == "hyperliquid"
        assert rate.market == "ETH-USD"
        assert rate.rate_hourly > Decimal("0")
        assert rate.rate_8h == rate.rate_hourly * Decimal("8")

    @pytest.mark.asyncio
    async def test_fetch_all_hyperliquid_markets(self, provider: FundingRateProvider) -> None:
        """Test fetching rates for all supported Hyperliquid markets."""
        rates = await provider.get_all_funding_rates(Venue.HYPERLIQUID)

        assert len(rates) == len(SUPPORTED_MARKETS["hyperliquid"])

        for market, rate in rates.items():
            assert rate.venue == "hyperliquid"
            assert rate.market == market

    @pytest.mark.asyncio
    async def test_hyperliquid_next_funding(self, provider: FundingRateProvider) -> None:
        """Test that Hyperliquid rates include next funding time."""
        rate = await provider.get_funding_rate(Venue.HYPERLIQUID, "ETH-USD")

        assert rate.next_funding_time is not None
        # Next funding time should be in the future (or very recent)


# =============================================================================
# Cross-Venue Integration Tests
# =============================================================================


class TestCrossVenueIntegration:
    """Integration tests for cross-venue functionality."""

    @pytest.mark.asyncio
    async def test_spread_between_venues(self, provider: FundingRateProvider) -> None:
        """Test calculating spread between GMX V2 and Hyperliquid."""
        spread = await provider.get_funding_rate_spread("ETH-USD", Venue.GMX_V2, Venue.HYPERLIQUID)

        assert spread.market == "ETH-USD"
        assert spread.venue_a == "gmx_v2"
        assert spread.venue_b == "hyperliquid"
        assert spread.rate_a is not None
        assert spread.rate_b is not None

        # Spread calculation should be correct
        expected_spread = spread.rate_a.rate_8h - spread.rate_b.rate_8h
        assert spread.spread_8h == expected_spread

    @pytest.mark.asyncio
    async def test_spread_btc(self, provider: FundingRateProvider) -> None:
        """Test calculating BTC spread between venues."""
        spread = await provider.get_funding_rate_spread("BTC-USD", Venue.GMX_V2, Venue.HYPERLIQUID)

        assert spread.market == "BTC-USD"
        assert spread.rate_a.market == "BTC-USD"
        assert spread.rate_b.market == "BTC-USD"

    @pytest.mark.asyncio
    async def test_compare_multiple_markets(self, provider: FundingRateProvider) -> None:
        """Test comparing funding rates across multiple markets."""
        markets = ["ETH-USD", "BTC-USD", "ARB-USD"]

        for market in markets:
            spread = await provider.get_funding_rate_spread(market, Venue.GMX_V2, Venue.HYPERLIQUID)
            assert spread.market == market
            # Log the spread for analysis
            print(
                f"{market}: GMX={spread.rate_a.rate_percent_8h:.4f}% "
                f"HL={spread.rate_b.rate_percent_8h:.4f}% "
                f"Spread={spread.spread_percent_8h:.4f}%"
            )


# =============================================================================
# Historical Data Integration Tests
# =============================================================================


class TestHistoricalIntegration:
    """Integration tests for historical funding rate data."""

    @pytest.mark.asyncio
    async def test_gmx_historical_24h(self, provider: FundingRateProvider) -> None:
        """Test fetching 24h historical data from GMX V2."""
        history = await provider.get_historical_funding_rates(Venue.GMX_V2, "ETH-USD", hours=24)

        assert history.venue == "gmx_v2"
        assert history.market == "ETH-USD"
        assert history.period_hours == 24
        assert len(history.rates) >= 1

        # Average should be between min and max
        assert history.min_rate_8h <= history.average_rate_8h <= history.max_rate_8h

    @pytest.mark.asyncio
    async def test_hyperliquid_historical_24h(self, provider: FundingRateProvider) -> None:
        """Test fetching 24h historical data from Hyperliquid."""
        history = await provider.get_historical_funding_rates(Venue.HYPERLIQUID, "ETH-USD", hours=24)

        assert history.venue == "hyperliquid"
        assert history.market == "ETH-USD"
        assert len(history.rates) >= 1

    @pytest.mark.asyncio
    async def test_historical_volatility(self, provider: FundingRateProvider) -> None:
        """Test historical data volatility calculation."""
        history = await provider.get_historical_funding_rates(Venue.GMX_V2, "ETH-USD", hours=24)

        # Volatility = max - min
        expected_volatility = history.max_rate_8h - history.min_rate_8h
        assert history.volatility == expected_volatility
        assert history.volatility >= Decimal("0")


# =============================================================================
# Performance Tests
# =============================================================================


class TestPerformance:
    """Performance tests for the funding rate provider."""

    @pytest.mark.asyncio
    async def test_caching_reduces_latency(self, provider: FundingRateProvider) -> None:
        """Test that caching reduces subsequent request latency."""
        import time

        # First request (uncached)
        start = time.time()
        await provider.get_funding_rate(Venue.GMX_V2, "ETH-USD")
        first_duration = time.time() - start

        # Second request (cached)
        start = time.time()
        await provider.get_funding_rate(Venue.GMX_V2, "ETH-USD")
        second_duration = time.time() - start

        # Cached request should be faster (or at least not slower)
        # Allow some margin for timing variance
        assert second_duration <= first_duration + 0.01

    @pytest.mark.asyncio
    async def test_parallel_rate_fetching(self, provider: FundingRateProvider) -> None:
        """Test that rates can be fetched in parallel."""
        import asyncio

        # Fetch multiple rates in parallel
        rates = await asyncio.gather(
            provider.get_funding_rate(Venue.GMX_V2, "ETH-USD"),
            provider.get_funding_rate(Venue.GMX_V2, "BTC-USD"),
            provider.get_funding_rate(Venue.HYPERLIQUID, "ETH-USD"),
            provider.get_funding_rate(Venue.HYPERLIQUID, "BTC-USD"),
        )

        assert len(rates) == 4
        assert rates[0].venue == "gmx_v2"
        assert rates[1].venue == "gmx_v2"
        assert rates[2].venue == "hyperliquid"
        assert rates[3].venue == "hyperliquid"


# =============================================================================
# MarketSnapshot Integration Tests
# =============================================================================


class TestMarketSnapshotIntegration:
    """Test FundingRateProvider integration with MarketSnapshot."""

    def test_funding_rate_through_snapshot(self) -> None:
        """Test accessing funding rates through MarketSnapshot."""
        from almanak.framework.data.market_snapshot import MarketSnapshot

        provider = FundingRateProvider()
        snapshot = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0x0000000000000000000000000000000000000000",
            funding_rate_provider=provider,
        )

        rate = snapshot.funding_rate("gmx_v2", "ETH-USD")

        assert rate.venue == "gmx_v2"
        assert rate.market == "ETH-USD"
        assert rate.rate_hourly > Decimal("0")

    def test_funding_rate_spread_through_snapshot(self) -> None:
        """Test accessing funding rate spread through MarketSnapshot."""
        from almanak.framework.data.market_snapshot import MarketSnapshot

        provider = FundingRateProvider()
        snapshot = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0x0000000000000000000000000000000000000000",
            funding_rate_provider=provider,
        )

        spread = snapshot.funding_rate_spread("ETH-USD", "gmx_v2", "hyperliquid")

        assert spread.market == "ETH-USD"
        assert spread.venue_a == "gmx_v2"
        assert spread.venue_b == "hyperliquid"

    def test_snapshot_without_provider_raises(self) -> None:
        """Test that accessing funding rate without provider raises ValueError."""
        from almanak.framework.data.market_snapshot import MarketSnapshot

        snapshot = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0x0000000000000000000000000000000000000000",
            # No funding_rate_provider
        )

        with pytest.raises(ValueError, match="No funding rate provider"):
            snapshot.funding_rate("gmx_v2", "ETH-USD")
