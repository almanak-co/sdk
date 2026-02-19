"""Unit tests for the FundingRateProvider service.

These tests verify the funding rate provider functionality with mocked responses.
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from almanak.framework.data.funding import (
    HOURS_PER_YEAR,
    SUPPORTED_MARKETS,
    SUPPORTED_VENUES,
    FundingRate,
    FundingRateProvider,
    FundingRateSpread,
    FundingRateUnavailableError,
    HistoricalFundingData,
    MarketNotSupportedError,
    Venue,
    VenueNotSupportedError,
)

# =============================================================================
# Test Provider Initialization
# =============================================================================


class TestProviderInitialization:
    """Test FundingRateProvider initialization."""

    def test_default_initialization(self) -> None:
        """Test provider with default parameters."""
        provider = FundingRateProvider()
        assert provider.venues == SUPPORTED_VENUES
        assert provider._cache_ttl_seconds == 60.0

    def test_custom_venues(self) -> None:
        """Test provider with custom venues."""
        provider = FundingRateProvider(venues=["gmx_v2"])
        assert provider.venues == ["gmx_v2"]

    def test_custom_cache_ttl(self) -> None:
        """Test provider with custom cache TTL."""
        provider = FundingRateProvider(cache_ttl_seconds=30.0)
        assert provider._cache_ttl_seconds == 30.0

    def test_venues_property_returns_copy(self) -> None:
        """Test that venues property returns a copy."""
        provider = FundingRateProvider()
        venues = provider.venues
        venues.append("fake_venue")
        assert "fake_venue" not in provider.venues


# =============================================================================
# Test Mock Rates
# =============================================================================


class TestMockRates:
    """Test mock rate functionality for testing."""

    @pytest.mark.asyncio
    async def test_set_mock_rate(self) -> None:
        """Test setting a mock rate."""
        provider = FundingRateProvider()
        provider.set_mock_rate("gmx_v2", "ETH-USD", Decimal("0.00001"))

        rate = await provider.get_funding_rate(Venue.GMX_V2, "ETH-USD")
        assert rate.rate_hourly == Decimal("0.00001")
        assert rate.rate_8h == Decimal("0.00008")
        assert rate.venue == "gmx_v2"
        assert rate.market == "ETH-USD"

    @pytest.mark.asyncio
    async def test_clear_mock_rates(self) -> None:
        """Test clearing mock rates."""
        provider = FundingRateProvider()
        provider.set_mock_rate("gmx_v2", "ETH-USD", Decimal("0.00001"))
        provider.clear_mock_rates()

        # Should fall back to default rates after clearing
        rate = await provider.get_funding_rate(Venue.GMX_V2, "ETH-USD")
        # Default rate is 0.000012 for ETH-USD on GMX V2
        assert rate.rate_hourly == Decimal("0.000012")

    @pytest.mark.asyncio
    async def test_multiple_mock_rates(self) -> None:
        """Test setting multiple mock rates."""
        provider = FundingRateProvider()
        provider.set_mock_rate("gmx_v2", "ETH-USD", Decimal("0.00001"))
        provider.set_mock_rate("gmx_v2", "BTC-USD", Decimal("0.00002"))
        provider.set_mock_rate("hyperliquid", "ETH-USD", Decimal("0.00003"))

        rate_gmx_eth = await provider.get_funding_rate(Venue.GMX_V2, "ETH-USD")
        rate_gmx_btc = await provider.get_funding_rate(Venue.GMX_V2, "BTC-USD")
        rate_hl_eth = await provider.get_funding_rate(Venue.HYPERLIQUID, "ETH-USD")

        assert rate_gmx_eth.rate_hourly == Decimal("0.00001")
        assert rate_gmx_btc.rate_hourly == Decimal("0.00002")
        assert rate_hl_eth.rate_hourly == Decimal("0.00003")


# =============================================================================
# Test Get Funding Rate
# =============================================================================


class TestGetFundingRate:
    """Test get_funding_rate method."""

    @pytest.mark.asyncio
    async def test_get_gmx_v2_rate(self) -> None:
        """Test getting GMX V2 funding rate."""
        provider = FundingRateProvider()
        rate = await provider.get_funding_rate(Venue.GMX_V2, "ETH-USD")

        assert rate.venue == "gmx_v2"
        assert rate.market == "ETH-USD"
        assert rate.rate_hourly > Decimal("0")
        assert rate.rate_8h == rate.rate_hourly * Decimal("8")
        assert rate.rate_annualized == rate.rate_hourly * Decimal(str(HOURS_PER_YEAR))
        assert rate.next_funding_time is not None
        assert rate.open_interest_long is not None
        assert rate.open_interest_short is not None
        assert rate.mark_price is not None

    @pytest.mark.asyncio
    async def test_get_hyperliquid_rate(self) -> None:
        """Test getting Hyperliquid funding rate."""
        provider = FundingRateProvider()
        rate = await provider.get_funding_rate(Venue.HYPERLIQUID, "ETH-USD")

        assert rate.venue == "hyperliquid"
        assert rate.market == "ETH-USD"
        assert rate.rate_hourly > Decimal("0")
        assert rate.next_funding_time is not None

    @pytest.mark.asyncio
    async def test_unsupported_venue(self) -> None:
        """Test getting rate for unsupported venue."""
        provider = FundingRateProvider()

        with pytest.raises(VenueNotSupportedError) as exc_info:
            # Type ignored since we're testing invalid input
            await provider.get_funding_rate("invalid_venue", "ETH-USD")  # type: ignore

        assert exc_info.value.venue == "invalid_venue"

    @pytest.mark.asyncio
    async def test_unsupported_market(self) -> None:
        """Test getting rate for unsupported market."""
        provider = FundingRateProvider()

        with pytest.raises(MarketNotSupportedError) as exc_info:
            await provider.get_funding_rate(Venue.GMX_V2, "INVALID-USD")

        assert exc_info.value.market == "INVALID-USD"
        assert exc_info.value.venue == "gmx_v2"

    @pytest.mark.asyncio
    async def test_all_supported_gmx_v2_markets(self) -> None:
        """Test getting rates for all supported GMX V2 markets."""
        provider = FundingRateProvider()

        for market in SUPPORTED_MARKETS["gmx_v2"]:
            rate = await provider.get_funding_rate(Venue.GMX_V2, market)
            assert rate.venue == "gmx_v2"
            assert rate.market == market
            assert rate.rate_hourly > Decimal("0")

    @pytest.mark.asyncio
    async def test_all_supported_hyperliquid_markets(self) -> None:
        """Test getting rates for all supported Hyperliquid markets."""
        provider = FundingRateProvider()

        for market in SUPPORTED_MARKETS["hyperliquid"]:
            rate = await provider.get_funding_rate(Venue.HYPERLIQUID, market)
            assert rate.venue == "hyperliquid"
            assert rate.market == market
            assert rate.rate_hourly > Decimal("0")


# =============================================================================
# Test FundingRate Data Class
# =============================================================================


class TestFundingRateDataClass:
    """Test FundingRate data class properties."""

    def test_rate_percent_8h(self) -> None:
        """Test rate_percent_8h property."""
        rate = FundingRate(
            venue="gmx_v2",
            market="ETH-USD",
            rate_hourly=Decimal("0.00001"),  # 0.001%/hour
            rate_8h=Decimal("0.00008"),  # 0.008%/8h
            rate_annualized=Decimal("0.0876"),  # 8.76%/year
        )
        assert rate.rate_percent_8h == Decimal("0.008")

    def test_rate_percent_annualized(self) -> None:
        """Test rate_percent_annualized property."""
        rate = FundingRate(
            venue="gmx_v2",
            market="ETH-USD",
            rate_hourly=Decimal("0.00001"),
            rate_8h=Decimal("0.00008"),
            rate_annualized=Decimal("0.0876"),
        )
        assert rate.rate_percent_annualized == Decimal("8.76")

    def test_is_positive(self) -> None:
        """Test is_positive property."""
        positive_rate = FundingRate(
            venue="gmx_v2",
            market="ETH-USD",
            rate_hourly=Decimal("0.00001"),
            rate_8h=Decimal("0.00008"),
            rate_annualized=Decimal("0.0876"),
        )
        assert positive_rate.is_positive is True
        assert positive_rate.is_negative is False

    def test_is_negative(self) -> None:
        """Test is_negative property."""
        negative_rate = FundingRate(
            venue="gmx_v2",
            market="ETH-USD",
            rate_hourly=Decimal("-0.00001"),
            rate_8h=Decimal("-0.00008"),
            rate_annualized=Decimal("-0.0876"),
        )
        assert negative_rate.is_positive is False
        assert negative_rate.is_negative is True

    def test_to_dict(self) -> None:
        """Test to_dict method."""
        now = datetime.now(UTC)
        next_funding = now + timedelta(hours=1)
        rate = FundingRate(
            venue="gmx_v2",
            market="ETH-USD",
            rate_hourly=Decimal("0.00001"),
            rate_8h=Decimal("0.00008"),
            rate_annualized=Decimal("0.0876"),
            timestamp=now,
            next_funding_time=next_funding,
            open_interest_long=Decimal("125000000"),
            open_interest_short=Decimal("118000000"),
            mark_price=Decimal("2500"),
            index_price=Decimal("2500"),
        )

        data = rate.to_dict()
        assert data["venue"] == "gmx_v2"
        assert data["market"] == "ETH-USD"
        assert data["rate_hourly"] == "0.00001"
        assert data["rate_8h"] == "0.00008"
        assert data["rate_annualized"] == "0.0876"
        assert data["rate_percent_8h"] == 0.008
        assert data["rate_percent_annualized"] == 8.76
        assert data["open_interest_long"] == 125000000.0
        assert data["open_interest_short"] == 118000000.0
        assert data["mark_price"] == 2500.0


# =============================================================================
# Test Funding Rate Spread
# =============================================================================


class TestFundingRateSpread:
    """Test funding rate spread functionality."""

    @pytest.mark.asyncio
    async def test_get_funding_rate_spread(self) -> None:
        """Test getting funding rate spread between venues."""
        provider = FundingRateProvider()
        provider.set_mock_rate("gmx_v2", "ETH-USD", Decimal("0.00001"))
        provider.set_mock_rate("hyperliquid", "ETH-USD", Decimal("0.00002"))

        spread = await provider.get_funding_rate_spread("ETH-USD", Venue.GMX_V2, Venue.HYPERLIQUID)

        assert spread.market == "ETH-USD"
        assert spread.venue_a == "gmx_v2"
        assert spread.venue_b == "hyperliquid"
        assert spread.rate_a.rate_hourly == Decimal("0.00001")
        assert spread.rate_b.rate_hourly == Decimal("0.00002")
        # Spread = rate_a - rate_b = 0.00001 - 0.00002 = -0.00001
        assert spread.spread_8h == Decimal("-0.00008")

    @pytest.mark.asyncio
    async def test_spread_with_default_rates(self) -> None:
        """Test spread calculation with default rates."""
        provider = FundingRateProvider()
        spread = await provider.get_funding_rate_spread("ETH-USD", Venue.GMX_V2, Venue.HYPERLIQUID)

        assert spread.market == "ETH-USD"
        assert spread.rate_a.venue == "gmx_v2"
        assert spread.rate_b.venue == "hyperliquid"
        # Default GMX V2 ETH rate is 0.000012, Hyperliquid is 0.000015
        # Spread = 0.000012 - 0.000015 = -0.000003
        expected_spread_8h = Decimal("-0.000024")  # -0.000003 * 8
        assert spread.spread_8h == expected_spread_8h

    def test_spread_is_profitable(self) -> None:
        """Test is_profitable property of spread."""
        rate_a = FundingRate(
            venue="gmx_v2",
            market="ETH-USD",
            rate_hourly=Decimal("0.0001"),
            rate_8h=Decimal("0.0008"),
            rate_annualized=Decimal("0.876"),
        )
        rate_b = FundingRate(
            venue="hyperliquid",
            market="ETH-USD",
            rate_hourly=Decimal("0.00001"),
            rate_8h=Decimal("0.00008"),
            rate_annualized=Decimal("0.0876"),
        )

        # Large positive spread (profitable)
        spread = FundingRateSpread(
            market="ETH-USD",
            venue_a="gmx_v2",
            venue_b="hyperliquid",
            rate_a=rate_a,
            rate_b=rate_b,
            spread_8h=Decimal("0.00072"),  # 0.072% > 0.01% threshold
            spread_annualized=Decimal("0.7884"),
        )
        assert spread.is_profitable is True
        assert spread.recommended_direction == "short_a_long_b"

    def test_spread_not_profitable(self) -> None:
        """Test spread below threshold is not profitable."""
        rate_a = FundingRate(
            venue="gmx_v2",
            market="ETH-USD",
            rate_hourly=Decimal("0.00001"),
            rate_8h=Decimal("0.00008"),
            rate_annualized=Decimal("0.0876"),
        )
        rate_b = FundingRate(
            venue="hyperliquid",
            market="ETH-USD",
            rate_hourly=Decimal("0.000011"),
            rate_8h=Decimal("0.000088"),
            rate_annualized=Decimal("0.09636"),
        )

        # Small spread (not profitable)
        spread = FundingRateSpread(
            market="ETH-USD",
            venue_a="gmx_v2",
            venue_b="hyperliquid",
            rate_a=rate_a,
            rate_b=rate_b,
            spread_8h=Decimal("-0.000008"),  # < 0.01% threshold
            spread_annualized=Decimal("-0.00876"),
        )
        assert spread.is_profitable is False
        assert spread.recommended_direction is None

    def test_spread_to_dict(self) -> None:
        """Test FundingRateSpread to_dict method."""
        rate_a = FundingRate(
            venue="gmx_v2",
            market="ETH-USD",
            rate_hourly=Decimal("0.00002"),
            rate_8h=Decimal("0.00016"),
            rate_annualized=Decimal("0.1752"),
        )
        rate_b = FundingRate(
            venue="hyperliquid",
            market="ETH-USD",
            rate_hourly=Decimal("0.00001"),
            rate_8h=Decimal("0.00008"),
            rate_annualized=Decimal("0.0876"),
        )

        spread = FundingRateSpread(
            market="ETH-USD",
            venue_a="gmx_v2",
            venue_b="hyperliquid",
            rate_a=rate_a,
            rate_b=rate_b,
            spread_8h=Decimal("0.00008"),
            spread_annualized=Decimal("0.0876"),
        )

        data = spread.to_dict()
        assert data["market"] == "ETH-USD"
        assert data["venue_a"] == "gmx_v2"
        assert data["venue_b"] == "hyperliquid"
        assert "rate_a" in data
        assert "rate_b" in data
        assert data["spread_8h"] == "0.00008"
        assert data["is_profitable"] is False  # 0.008% < 0.01%


# =============================================================================
# Test Historical Funding Rates
# =============================================================================


class TestHistoricalFundingRates:
    """Test historical funding rate functionality."""

    @pytest.mark.asyncio
    async def test_get_historical_gmx_v2(self) -> None:
        """Test getting historical rates from GMX V2."""
        provider = FundingRateProvider()
        history = await provider.get_historical_funding_rates(Venue.GMX_V2, "ETH-USD", hours=24)

        assert history.venue == "gmx_v2"
        assert history.market == "ETH-USD"
        assert history.period_hours == 24
        assert len(history.rates) == 24
        assert history.average_rate_8h > Decimal("0")
        assert history.max_rate_8h >= history.average_rate_8h
        assert history.min_rate_8h <= history.average_rate_8h

    @pytest.mark.asyncio
    async def test_get_historical_hyperliquid(self) -> None:
        """Test getting historical rates from Hyperliquid."""
        provider = FundingRateProvider()
        history = await provider.get_historical_funding_rates(Venue.HYPERLIQUID, "BTC-USD", hours=48)

        assert history.venue == "hyperliquid"
        assert history.market == "BTC-USD"
        assert history.period_hours == 48
        assert len(history.rates) == 48

    @pytest.mark.asyncio
    async def test_historical_hours_limit(self) -> None:
        """Test historical hours are limited to valid range."""
        provider = FundingRateProvider()

        # Request too many hours (should be capped to 168)
        history = await provider.get_historical_funding_rates(Venue.GMX_V2, "ETH-USD", hours=500)
        assert history.period_hours == 168  # Max is 168 (7 days)

        # Use different market to avoid cache hit
        # Request negative hours (should be floored to 1)
        history = await provider.get_historical_funding_rates(Venue.GMX_V2, "BTC-USD", hours=-5)
        assert history.period_hours == 1

    def test_historical_data_volatility(self) -> None:
        """Test volatility calculation."""
        from almanak.framework.data.funding.provider import HistoricalFundingRate

        rates = [
            HistoricalFundingRate(
                venue="gmx_v2",
                market="ETH-USD",
                rate_hourly=Decimal("0.00001"),
                timestamp=datetime.now(UTC),
            ),
            HistoricalFundingRate(
                venue="gmx_v2",
                market="ETH-USD",
                rate_hourly=Decimal("0.00003"),
                timestamp=datetime.now(UTC) - timedelta(hours=1),
            ),
            HistoricalFundingRate(
                venue="gmx_v2",
                market="ETH-USD",
                rate_hourly=Decimal("0.00002"),
                timestamp=datetime.now(UTC) - timedelta(hours=2),
            ),
        ]

        history = HistoricalFundingData(
            venue="gmx_v2",
            market="ETH-USD",
            rates=rates,
            period_hours=3,
        )

        # Max rate_8h = 0.00003 * 8 = 0.00024
        # Min rate_8h = 0.00001 * 8 = 0.00008
        # Volatility = 0.00024 - 0.00008 = 0.00016
        assert history.max_rate_8h == Decimal("0.00024")
        assert history.min_rate_8h == Decimal("0.00008")
        assert history.volatility == Decimal("0.00016")

    def test_empty_historical_data(self) -> None:
        """Test historical data with no rates."""
        history = HistoricalFundingData(
            venue="gmx_v2",
            market="ETH-USD",
            rates=[],
            period_hours=24,
        )

        assert history.average_rate_8h == Decimal("0")
        assert history.max_rate_8h == Decimal("0")
        assert history.min_rate_8h == Decimal("0")
        assert history.volatility == Decimal("0")


# =============================================================================
# Test Caching
# =============================================================================


class TestCaching:
    """Test caching functionality."""

    @pytest.mark.asyncio
    async def test_rate_is_cached(self) -> None:
        """Test that rates are cached."""
        provider = FundingRateProvider(cache_ttl_seconds=60.0)

        # First call
        rate1 = await provider.get_funding_rate(Venue.GMX_V2, "ETH-USD")

        # Second call should return cached value
        rate2 = await provider.get_funding_rate(Venue.GMX_V2, "ETH-USD")

        # Both should be identical (same object or same timestamp)
        assert rate1.rate_hourly == rate2.rate_hourly
        assert rate1.timestamp == rate2.timestamp

    @pytest.mark.asyncio
    async def test_clear_cache(self) -> None:
        """Test clearing the cache."""
        provider = FundingRateProvider()

        # Fetch and cache
        await provider.get_funding_rate(Venue.GMX_V2, "ETH-USD")

        stats_before = provider.get_cache_stats()
        assert stats_before["current_rates_cached"] == 1

        # Clear cache
        provider.clear_cache()

        stats_after = provider.get_cache_stats()
        assert stats_after["current_rates_cached"] == 0

    def test_cache_stats(self) -> None:
        """Test cache statistics."""
        provider = FundingRateProvider(cache_ttl_seconds=30.0)
        stats = provider.get_cache_stats()

        assert stats["current_rates_cached"] == 0
        assert stats["historical_cached"] == 0
        assert stats["ttl_seconds"] == 30.0


# =============================================================================
# Test Get All Funding Rates
# =============================================================================


class TestGetAllFundingRates:
    """Test get_all_funding_rates method."""

    @pytest.mark.asyncio
    async def test_get_all_gmx_v2_rates(self) -> None:
        """Test getting all rates from GMX V2."""
        provider = FundingRateProvider()
        rates = await provider.get_all_funding_rates(Venue.GMX_V2)

        assert len(rates) == len(SUPPORTED_MARKETS["gmx_v2"])
        for market in SUPPORTED_MARKETS["gmx_v2"]:
            assert market in rates
            assert rates[market].venue == "gmx_v2"
            assert rates[market].market == market

    @pytest.mark.asyncio
    async def test_get_all_hyperliquid_rates(self) -> None:
        """Test getting all rates from Hyperliquid."""
        provider = FundingRateProvider()
        rates = await provider.get_all_funding_rates(Venue.HYPERLIQUID)

        assert len(rates) == len(SUPPORTED_MARKETS["hyperliquid"])
        for market in SUPPORTED_MARKETS["hyperliquid"]:
            assert market in rates


# =============================================================================
# Test Error Handling
# =============================================================================


class TestErrorHandling:
    """Test error handling."""

    def test_venue_not_supported_error(self) -> None:
        """Test VenueNotSupportedError exception."""
        error = VenueNotSupportedError("invalid_venue")
        assert error.venue == "invalid_venue"
        assert "invalid_venue" in str(error)
        assert "SUPPORTED_VENUES" in str(error) or "Supported venues" in str(error)

    def test_market_not_supported_error(self) -> None:
        """Test MarketNotSupportedError exception."""
        error = MarketNotSupportedError("INVALID-USD", "gmx_v2")
        assert error.market == "INVALID-USD"
        assert error.venue == "gmx_v2"
        assert "INVALID-USD" in str(error)
        assert "gmx_v2" in str(error)

    def test_funding_rate_unavailable_error(self) -> None:
        """Test FundingRateUnavailableError exception."""
        error = FundingRateUnavailableError("gmx_v2", "ETH-USD", "API timeout")
        assert error.venue == "gmx_v2"
        assert error.market == "ETH-USD"
        assert error.reason == "API timeout"
        assert "API timeout" in str(error)


# =============================================================================
# Test Constants
# =============================================================================


class TestConstants:
    """Test module constants."""

    def test_supported_venues(self) -> None:
        """Test SUPPORTED_VENUES constant."""
        assert "gmx_v2" in SUPPORTED_VENUES
        assert "hyperliquid" in SUPPORTED_VENUES
        assert len(SUPPORTED_VENUES) == 2

    def test_supported_markets(self) -> None:
        """Test SUPPORTED_MARKETS constant."""
        assert "gmx_v2" in SUPPORTED_MARKETS
        assert "hyperliquid" in SUPPORTED_MARKETS
        assert "ETH-USD" in SUPPORTED_MARKETS["gmx_v2"]
        assert "BTC-USD" in SUPPORTED_MARKETS["gmx_v2"]
        assert "ETH-USD" in SUPPORTED_MARKETS["hyperliquid"]
        assert "BTC-USD" in SUPPORTED_MARKETS["hyperliquid"]

    def test_hours_per_year(self) -> None:
        """Test HOURS_PER_YEAR constant."""
        assert HOURS_PER_YEAR == 8760  # 365 * 24
