"""Integration tests for the FundingRateProvider service.

The tests in this module hit the live GMX V2 reader and the Hyperliquid
public API. They are kept out of the unit test tree (`tests/unit/**`)
because real network calls violate unit-test isolation; they live here
under `tests/integration/**` and are marked `@pytest.mark.integration`
so `make test-unit` skips them.

NB: `FundingRateProvider` itself currently performs direct HTTP egress
from the strategy container, which is a gateway-boundary violation
tracked in #2067. Once that lands the venue calls will go through the
gateway and these tests can move back under the unit tree against a
gateway stub.
"""

from decimal import Decimal

import pytest

from almanak.framework.data.funding import (
    HOURS_PER_YEAR,
    SUPPORTED_MARKETS,
    FundingRateProvider,
    Venue,
)

# Whole-module marker — every test below performs real network egress.
pytestmark = pytest.mark.integration


# =============================================================================
# Test get_funding_rate (live)
# =============================================================================


class TestGetFundingRate:
    """Live get_funding_rate calls."""

    @pytest.mark.asyncio
    async def test_get_hyperliquid_rate(self) -> None:
        """Get a single Hyperliquid funding rate.

        Funding can be negative (shorts pay longs in bearish regimes), so
        this asserts model invariants instead of positivity.
        """
        provider = FundingRateProvider()
        rate = await provider.get_funding_rate(Venue.HYPERLIQUID, "ETH-USD")

        assert rate.venue == "hyperliquid"
        assert rate.market == "ETH-USD"
        assert isinstance(rate.rate_hourly, Decimal)
        assert rate.rate_8h == rate.rate_hourly * Decimal("8")
        assert rate.rate_annualized == rate.rate_hourly * Decimal(str(HOURS_PER_YEAR))
        assert rate.next_funding_time is not None

    @pytest.mark.asyncio
    async def test_all_supported_gmx_v2_markets(self) -> None:
        """Get rates for every supported GMX V2 market.

        Rates can be negative depending on OI imbalance; assert model
        invariants rather than positivity.
        """
        provider = FundingRateProvider()

        for market in SUPPORTED_MARKETS["gmx_v2"]:
            rate = await provider.get_funding_rate(Venue.GMX_V2, market)
            assert rate.venue == "gmx_v2"
            assert rate.market == market
            assert isinstance(rate.rate_hourly, Decimal)
            assert rate.rate_8h == rate.rate_hourly * Decimal("8")

    @pytest.mark.asyncio
    async def test_all_supported_hyperliquid_markets(self) -> None:
        """Get rates for every supported Hyperliquid market.

        Funding can be negative; assert invariants rather than positivity.
        """
        provider = FundingRateProvider()

        for market in SUPPORTED_MARKETS["hyperliquid"]:
            rate = await provider.get_funding_rate(Venue.HYPERLIQUID, market)
            assert rate.venue == "hyperliquid"
            assert rate.market == market
            assert isinstance(rate.rate_hourly, Decimal)
            assert rate.rate_8h == rate.rate_hourly * Decimal("8")


# =============================================================================
# Test get_funding_rate_spread (live)
# =============================================================================


class TestFundingRateSpread:
    """Live spread calculations."""

    @pytest.mark.asyncio
    async def test_spread_with_default_rates(self) -> None:
        """Compute the GMX-vs-Hyperliquid spread against live data.

        Funding rates drift, so assert the model invariants tying spread
        fields to the underlying rates rather than hard-coded snapshots.
        """
        provider = FundingRateProvider()
        spread = await provider.get_funding_rate_spread("ETH-USD", Venue.GMX_V2, Venue.HYPERLIQUID)

        assert spread.market == "ETH-USD"
        assert spread.rate_a.venue == "gmx_v2"
        assert spread.rate_b.venue == "hyperliquid"
        expected_spread_hourly = spread.rate_a.rate_hourly - spread.rate_b.rate_hourly
        assert spread.spread_8h == expected_spread_hourly * Decimal("8")
        assert spread.spread_annualized == expected_spread_hourly * Decimal(str(HOURS_PER_YEAR))


# =============================================================================
# Test get_all_funding_rates (live)
# =============================================================================


class TestGetAllFundingRates:
    """Live get_all_funding_rates calls."""

    @pytest.mark.asyncio
    async def test_get_all_gmx_v2_rates(self) -> None:
        """Get every GMX V2 rate at once.

        Lives here because there is no `set_mock_rate` setup — every
        market resolution is a real RPC read against the GMX V2 reader.
        """
        provider = FundingRateProvider()
        rates = await provider.get_all_funding_rates(Venue.GMX_V2)

        assert len(rates) == len(SUPPORTED_MARKETS["gmx_v2"])
        for market in SUPPORTED_MARKETS["gmx_v2"]:
            assert market in rates
            assert rates[market].venue == "gmx_v2"
            assert rates[market].market == market

    @pytest.mark.asyncio
    async def test_get_all_hyperliquid_rates(self) -> None:
        """Get every Hyperliquid rate at once.

        Hits the live Hyperliquid public API.
        """
        provider = FundingRateProvider()
        rates = await provider.get_all_funding_rates(Venue.HYPERLIQUID)

        assert len(rates) == len(SUPPORTED_MARKETS["hyperliquid"])
        for market in SUPPORTED_MARKETS["hyperliquid"]:
            assert market in rates
