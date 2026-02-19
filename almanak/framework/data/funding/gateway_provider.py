"""Gateway-backed Funding Rate Provider.

This module provides a gateway-backed implementation of funding rate fetching
for perpetual venues (GMX V2, Hyperliquid). All API calls go through the
gateway sidecar, keeping credentials secure.

Example:
    from almanak.framework.data.funding import GatewayFundingRateProvider, Venue
    from almanak.framework.gateway_client import GatewayClient

    with GatewayClient() as gateway:
        provider = GatewayFundingRateProvider(gateway_client=gateway)

        # Get GMX V2 funding rate for ETH
        rate = await provider.get_funding_rate(Venue.GMX_V2, "ETH-USD")

        # Get funding rate spread between venues
        spread = await provider.get_funding_rate_spread(
            "ETH-USD", Venue.GMX_V2, Venue.HYPERLIQUID
        )
"""

import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from almanak.framework.gateway_client import GatewayClient

logger = logging.getLogger(__name__)


# =============================================================================
# Exceptions
# =============================================================================


class FundingRateError(Exception):
    """Raised when funding rate operations fail."""


# =============================================================================
# Constants
# =============================================================================

HOURS_PER_YEAR = 8760
# Use shorter TTL for live funding rates (they can go stale quickly)
DEFAULT_CACHE_TTL_SECONDS = 10.0


class Venue(StrEnum):
    """Supported perpetual venues."""

    GMX_V2 = "gmx_v2"
    HYPERLIQUID = "hyperliquid"


SUPPORTED_VENUES: list[str] = [v.value for v in Venue]


# =============================================================================
# Data Models
# =============================================================================


@dataclass
class FundingRate:
    """Funding rate data for a perpetual market.

    Attributes:
        venue: Venue identifier (e.g., "gmx_v2", "hyperliquid")
        market: Market symbol (e.g., "ETH-USD")
        rate_hourly: Hourly funding rate as a decimal
        rate_8h: 8-hour funding rate
        rate_annualized: Annualized funding rate
        next_funding_time: Next funding settlement time
        open_interest_long: Total long open interest in USD
        open_interest_short: Total short open interest in USD
        mark_price: Current mark price
        index_price: Current index price
        is_live_data: Whether data is from live source (vs defaults)
    """

    venue: str
    market: str
    rate_hourly: Decimal
    rate_8h: Decimal
    rate_annualized: Decimal
    next_funding_time: datetime
    open_interest_long: Decimal
    open_interest_short: Decimal
    mark_price: Decimal
    index_price: Decimal
    is_live_data: bool = True


@dataclass
class FundingRateSpread:
    """Funding rate spread between two venues.

    Attributes:
        market: Market symbol
        venue_a: First venue
        venue_b: Second venue
        spread_hourly: Absolute spread in hourly rate
        spread_annualized: Annualized spread
        rate_a: Rate from first venue
        rate_b: Rate from second venue
    """

    market: str
    venue_a: str
    venue_b: str
    spread_hourly: Decimal
    spread_annualized: Decimal
    rate_a: FundingRate
    rate_b: FundingRate


# =============================================================================
# Gateway Provider
# =============================================================================


class GatewayFundingRateProvider:
    """Gateway-backed funding rate provider.

    All funding rate requests are proxied through the gateway, which handles:
    - API credentials
    - RPC connections
    - Rate limiting
    - Caching

    Example:
        with GatewayClient() as gateway:
            provider = GatewayFundingRateProvider(gateway_client=gateway)

            # Get single rate
            rate = await provider.get_funding_rate(Venue.GMX_V2, "ETH-USD")

            # Get spread
            spread = await provider.get_funding_rate_spread(
                "ETH-USD", Venue.GMX_V2, Venue.HYPERLIQUID
            )
    """

    def __init__(
        self,
        gateway_client: "GatewayClient",
        chain: str = "arbitrum",
        cache_ttl_seconds: float = DEFAULT_CACHE_TTL_SECONDS,
    ) -> None:
        """Initialize the gateway-backed funding rate provider.

        Args:
            gateway_client: Connected GatewayClient instance
            chain: Chain for on-chain venues (default: arbitrum)
            cache_ttl_seconds: Local cache TTL in seconds
        """
        self._gateway_client = gateway_client
        self._chain = chain.lower()
        self._cache_ttl_seconds = cache_ttl_seconds

        # Local rate cache: venue -> market -> (rate, timestamp)
        self._cache: dict[str, dict[str, tuple[FundingRate, float]]] = {}

        logger.info(
            "GatewayFundingRateProvider initialized (chain=%s, cache_ttl=%s)",
            chain,
            cache_ttl_seconds,
        )

    def _get_cached_rate(self, venue: str, market: str) -> FundingRate | None:
        """Get cached rate if still valid."""
        venue_cache = self._cache.get(venue, {})
        if market in venue_cache:
            rate, timestamp = venue_cache[market]
            if time.time() - timestamp < self._cache_ttl_seconds:
                return rate
        return None

    def _set_cached_rate(self, venue: str, market: str, rate: FundingRate) -> None:
        """Cache a funding rate."""
        if venue not in self._cache:
            self._cache[venue] = {}
        self._cache[venue][market] = (rate, time.time())

    def _response_to_funding_rate(self, response) -> FundingRate:
        """Convert gateway FundingRateResponse to FundingRate dataclass."""
        return FundingRate(
            venue=response.venue,
            market=response.market,
            rate_hourly=Decimal(response.rate_hourly),
            rate_8h=Decimal(response.rate_8h),
            rate_annualized=Decimal(response.rate_annualized),
            next_funding_time=datetime.fromtimestamp(response.next_funding_time, tz=UTC),
            open_interest_long=Decimal(response.open_interest_long),
            open_interest_short=Decimal(response.open_interest_short),
            mark_price=Decimal(response.mark_price),
            index_price=Decimal(response.index_price),
            is_live_data=response.is_live_data,
        )

    async def get_funding_rate(
        self,
        venue: Venue | str,
        market: str,
    ) -> FundingRate:
        """Get funding rate for a market on a specific venue.

        Args:
            venue: Venue (e.g., Venue.GMX_V2 or "gmx_v2")
            market: Market symbol (e.g., "ETH-USD")

        Returns:
            FundingRate with current funding data

        Raises:
            ValueError: If venue is not supported or request fails
        """
        venue_str = venue.value if isinstance(venue, Venue) else venue.lower()
        market = market.upper()

        # Check cache first
        cached = self._get_cached_rate(venue_str, market)
        if cached is not None:
            return cached

        # Fetch from gateway
        from almanak.gateway.proto import gateway_pb2

        request = gateway_pb2.FundingRateRequest(
            venue=venue_str,
            market=market,
            chain=self._chain,
        )

        response = await asyncio.to_thread(
            self._gateway_client.funding_rate.GetFundingRate,
            request,
            timeout=self._gateway_client.config.timeout,
        )

        if not response.success:
            raise FundingRateError(f"Failed to get funding rate: {response.error}")

        rate = self._response_to_funding_rate(response)

        # Cache the result
        self._set_cached_rate(venue_str, market, rate)

        return rate

    async def get_funding_rate_spread(
        self,
        market: str,
        venue_a: Venue | str,
        venue_b: Venue | str,
    ) -> FundingRateSpread:
        """Get funding rate spread between two venues.

        Useful for funding rate arbitrage strategies.

        Args:
            market: Market symbol (e.g., "ETH-USD")
            venue_a: First venue
            venue_b: Second venue

        Returns:
            FundingRateSpread with spread and individual rates

        Raises:
            ValueError: If request fails
        """
        venue_a_str = venue_a.value if isinstance(venue_a, Venue) else venue_a.lower()
        venue_b_str = venue_b.value if isinstance(venue_b, Venue) else venue_b.lower()
        market = market.upper()

        from almanak.gateway.proto import gateway_pb2

        request = gateway_pb2.FundingRateSpreadRequest(
            market=market,
            venue_a=venue_a_str,
            venue_b=venue_b_str,
            chain=self._chain,
        )

        response = await asyncio.to_thread(
            self._gateway_client.funding_rate.GetFundingRateSpread,
            request,
            timeout=self._gateway_client.config.timeout,
        )

        if not response.success:
            raise FundingRateError(f"Failed to get funding rate spread: {response.error}")

        rate_a = self._response_to_funding_rate(response.venue_a_rate)
        rate_b = self._response_to_funding_rate(response.venue_b_rate)

        return FundingRateSpread(
            market=market,
            venue_a=venue_a_str,
            venue_b=venue_b_str,
            spread_hourly=Decimal(response.spread_hourly),
            spread_annualized=Decimal(response.spread_annualized),
            rate_a=rate_a,
            rate_b=rate_b,
        )

    async def get_rates_for_market(
        self,
        market: str,
        venues: list[Venue | str] | None = None,
    ) -> dict[str, FundingRate]:
        """Get funding rates for a market across multiple venues.

        Args:
            market: Market symbol (e.g., "ETH-USD")
            venues: Venues to query (default: all supported)

        Returns:
            Dictionary mapping venue -> FundingRate
        """
        if venues is None:
            venues = list(Venue)

        # Fetch all rates concurrently
        tasks = [self.get_funding_rate(venue, market) for venue in venues]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        rates: dict[str, FundingRate] = {}
        for venue, result in zip(venues, results, strict=False):
            venue_str = venue.value if isinstance(venue, Venue) else venue.lower()
            if isinstance(result, Exception):
                logger.warning("Failed to get rate for %s/%s: %s", venue_str, market, result)
            else:
                rates[venue_str] = result  # type: ignore[assignment]

        return rates

    def clear_cache(self) -> None:
        """Clear the local rate cache."""
        self._cache.clear()


__all__ = [
    "FundingRate",
    "FundingRateError",
    "FundingRateSpread",
    "GatewayFundingRateProvider",
    "SUPPORTED_VENUES",
    "Venue",
]
