"""Gateway-backed funding rate provider.

Routes all funding rate requests through the gateway sidecar's
:class:`FundingRateService` (see ``almanak/gateway/services/funding_rate_service.py``)
so the strategy container has zero network egress for funding data.

Example:
    from almanak.framework.data.funding import GatewayFundingRateProvider, Venue
    from almanak.framework.gateway_client import GatewayClient

    with GatewayClient() as gateway:
        provider = GatewayFundingRateProvider(gateway_client=gateway)

        rate = await provider.get_funding_rate(Venue.GMX_V2, "ETH-USD")
        spread = await provider.get_funding_rate_spread(
            "ETH-USD", Venue.GMX_V2, Venue.HYPERLIQUID,
        )
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING

from .models import (
    DEFAULT_CACHE_TTL_SECONDS,
    HOURS_PER_YEAR,
    SUPPORTED_MARKETS,
    SUPPORTED_VENUES,
    FundingRate,
    FundingRateSpread,
    FundingRateUnavailableError,
    MarketNotSupportedError,
    Venue,
    VenueNotSupportedError,
)

if TYPE_CHECKING:
    from almanak.framework.gateway_client import GatewayClient

logger = logging.getLogger(__name__)


def _normalize_venue(venue: Venue | str) -> str:
    """Coerce Venue/str to a lowercase string and validate."""
    venue_str = venue.value if isinstance(venue, Venue) else str(venue).lower()
    if venue_str not in SUPPORTED_VENUES:
        raise VenueNotSupportedError(venue_str)
    return venue_str


def _validate_market(venue: str, market: str) -> str:
    """Coerce market to upper-case and validate against the venue."""
    market_upper = market.upper()
    if market_upper not in SUPPORTED_MARKETS.get(venue, []):
        raise MarketNotSupportedError(market_upper, venue)
    return market_upper


class GatewayFundingRateProvider:
    """Funding rate provider that delegates to the gateway sidecar.

    The gateway owns all network egress (Hyperliquid HTTP, GMX V2 RPC),
    credential storage, SSL configuration, and rate limiting. Strategy
    code calls this provider over the in-cluster gRPC channel only.
    """

    def __init__(
        self,
        gateway_client: GatewayClient,
        chain: str = "arbitrum",
        cache_ttl_seconds: float = DEFAULT_CACHE_TTL_SECONDS,
    ) -> None:
        self._gateway_client = gateway_client
        self._chain = chain.lower()
        self._cache_ttl_seconds = cache_ttl_seconds

        # venue -> market -> (rate, monotonic_timestamp)
        self._cache: dict[str, dict[str, tuple[FundingRate, float]]] = {}

        logger.info(
            "GatewayFundingRateProvider initialized (chain=%s, cache_ttl=%ss)",
            self._chain,
            cache_ttl_seconds,
        )

    @property
    def chain(self) -> str:
        return self._chain

    def _get_cached_rate(self, venue: str, market: str) -> FundingRate | None:
        venue_cache = self._cache.get(venue, {})
        if market in venue_cache:
            rate, ts = venue_cache[market]
            if time.monotonic() - ts < self._cache_ttl_seconds:
                return rate
        return None

    def _set_cached_rate(self, venue: str, market: str, rate: FundingRate) -> None:
        self._cache.setdefault(venue, {})[market] = (rate, time.monotonic())

    def _response_to_funding_rate(self, response) -> FundingRate:
        """Convert a gRPC FundingRateResponse to :class:`FundingRate`."""
        rate_hourly = Decimal(response.rate_hourly)
        next_funding = (
            datetime.fromtimestamp(response.next_funding_time, tz=UTC) if response.next_funding_time else None
        )
        return FundingRate(
            venue=response.venue,
            market=response.market,
            rate_hourly=rate_hourly,
            rate_8h=Decimal(response.rate_8h),
            rate_annualized=Decimal(response.rate_annualized),
            next_funding_time=next_funding,
            # Proto strings default to "" when unset; treat that as missing.
            open_interest_long=Decimal(response.open_interest_long) if response.open_interest_long != "" else None,
            open_interest_short=Decimal(response.open_interest_short) if response.open_interest_short != "" else None,
            mark_price=Decimal(response.mark_price) if response.mark_price != "" else None,
            index_price=Decimal(response.index_price) if response.index_price != "" else None,
            is_live_data=response.is_live_data,
        )

    async def get_funding_rate(
        self,
        venue: Venue | str,
        market: str,
    ) -> FundingRate:
        """Get the current funding rate for ``venue``/``market``.

        Raises:
            VenueNotSupportedError: ``venue`` is not in ``SUPPORTED_VENUES``.
            MarketNotSupportedError: ``market`` is not in ``SUPPORTED_MARKETS[venue]``.
            FundingRateUnavailableError: the gateway returned an error.
        """
        venue_str = _normalize_venue(venue)
        market_str = _validate_market(venue_str, market)

        cached = self._get_cached_rate(venue_str, market_str)
        if cached is not None:
            return cached

        from almanak.gateway.proto import gateway_pb2

        request = gateway_pb2.FundingRateRequest(
            venue=venue_str,
            market=market_str,
            chain=self._chain,
        )

        try:
            response = await asyncio.to_thread(
                self._gateway_client.funding_rate.GetFundingRate,
                request,
                timeout=self._gateway_client.config.timeout,
            )
        except Exception as exc:
            raise FundingRateUnavailableError(venue_str, market_str, str(exc)) from exc

        if not response.success:
            raise FundingRateUnavailableError(venue_str, market_str, response.error or "gateway returned success=False")

        rate = self._response_to_funding_rate(response)
        self._set_cached_rate(venue_str, market_str, rate)
        return rate

    async def get_funding_rate_spread(
        self,
        market: str,
        venue_a: Venue | str,
        venue_b: Venue | str,
    ) -> FundingRateSpread:
        """Get the funding rate spread between ``venue_a`` and ``venue_b``.

        Issues a single ``GetFundingRateSpread`` RPC so the gateway can
        fetch both rates concurrently. The signed ``spread_8h`` is computed
        locally from ``venue_a_rate.rate_hourly - venue_b_rate.rate_hourly``
        because the wire ``spread_hourly`` field is absolute by historical
        convention and we need sign for ``recommended_direction``.
        """
        venue_a_str = _normalize_venue(venue_a)
        venue_b_str = _normalize_venue(venue_b)
        market_str = _validate_market(venue_a_str, market)
        _validate_market(venue_b_str, market_str)

        from almanak.gateway.proto import gateway_pb2

        request = gateway_pb2.FundingRateSpreadRequest(
            market=market_str,
            venue_a=venue_a_str,
            venue_b=venue_b_str,
            chain=self._chain,
        )

        try:
            response = await asyncio.to_thread(
                self._gateway_client.funding_rate.GetFundingRateSpread,
                request,
                timeout=self._gateway_client.config.timeout,
            )
        except Exception as exc:
            raise FundingRateUnavailableError(f"{venue_a_str}/{venue_b_str}", market_str, str(exc)) from exc

        if not response.success:
            raise FundingRateUnavailableError(
                f"{venue_a_str}/{venue_b_str}",
                market_str,
                response.error or "gateway returned success=False",
            )

        rate_a = self._response_to_funding_rate(response.venue_a_rate)
        rate_b = self._response_to_funding_rate(response.venue_b_rate)
        spread_hourly = rate_a.rate_hourly - rate_b.rate_hourly
        return FundingRateSpread(
            market=market_str,
            venue_a=venue_a_str,
            venue_b=venue_b_str,
            rate_a=rate_a,
            rate_b=rate_b,
            spread_8h=spread_hourly * Decimal("8"),
            spread_annualized=spread_hourly * Decimal(str(HOURS_PER_YEAR)),
        )

    async def get_rates_for_market(
        self,
        market: str,
        venues: list[Venue | str] | None = None,
    ) -> dict[str, FundingRate]:
        """Fetch ``market`` funding rates across multiple venues concurrently."""
        if venues is None:
            venues = list(Venue)

        tasks = [self.get_funding_rate(venue, market) for venue in venues]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        rates: dict[str, FundingRate] = {}
        for venue, result in zip(venues, results, strict=False):
            venue_str = venue.value if isinstance(venue, Venue) else str(venue).lower()
            if isinstance(result, BaseException):
                logger.warning("Failed to get rate for %s/%s: %s", venue_str, market, result)
            else:
                rates[venue_str] = result
        return rates

    def clear_cache(self) -> None:
        self._cache.clear()


__all__ = [
    "GatewayFundingRateProvider",
]
