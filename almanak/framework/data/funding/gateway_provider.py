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
import re
import time
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING

from almanak.core.chains import DEFAULT_CHAIN
from almanak.core.perp_markets import (
    PERP_MARKET_SEPARATORS,
    perp_market_funding_key,
    perp_market_pair_key,
)

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

_EVM_ADDRESS_RE = re.compile(r"^0[xX][0-9a-fA-F]{40}$")


def _normalize_venue(venue: Venue | str) -> str:
    """Coerce Venue/str to a lowercase string and validate."""
    venue_str = venue.value if isinstance(venue, Venue) else str(venue).lower()
    if venue_str not in SUPPORTED_VENUES:
        raise VenueNotSupportedError(venue_str)
    return venue_str


def _validate_market(venue: str, market: str) -> str:
    """Canonicalize a public perp identifier and validate it for the venue.

    Funding manifests use venue transport keys (``"ETH-USD"``), while public
    perp intents and GMX execution registries use ``"ETH/USD"``. Both must
    enter the funding lane through the shared canonicalization seam; otherwise
    a strategy needs two identifiers for one market (ALM-3094).
    """
    # ``perp_market_funding_key`` derives "<BASE>-USD" and DISCARDS the quote,
    # so on its own it maps ETH/EUR, ETH-WHATEVER and ETH-EUR-PERP all onto
    # ETH-USD and this validator would accept them — returning funding for a
    # market the caller never asked for, on a value strategies gate entry
    # decisions on. ``main`` raised on every one of those (it exact-matched the
    # upper-cased string); canonicalising must not turn that fail-closed into a
    # fail-open (Codex P2 + delta review, #3565).
    #
    # Round-trip instead of enumerating served quotes: if the caller supplied a
    # separator at all, the derived transport key must reproduce exactly the
    # pair they wrote. That is total over every spelling — including the
    # multi-separator ones ``perp_market_pair_key`` refuses to parse — and it
    # needs no list of quote currencies to rot.
    from almanak.connectors._strategy_base.funding_history_registry import FundingHistoryRegistry

    stripped_market = market.strip()
    if _EVM_ADDRESS_RE.fullmatch(stripped_market):
        if FundingHistoryRegistry.discovers_markets(venue):
            return stripped_market.lower()
        raise MarketNotSupportedError(stripped_market.upper(), venue)

    market_key = perp_market_funding_key(market)
    if any(separator in market for separator in PERP_MARKET_SEPARATORS):
        pair = perp_market_pair_key(market)
        if pair is None or market_key is None or pair.replace("/", "-") != market_key:
            raise MarketNotSupportedError(market.strip().upper(), venue)

    if market_key is None or (
        not FundingHistoryRegistry.discovers_markets(venue) and market_key not in SUPPORTED_MARKETS.get(venue, [])
    ):
        rejected = market.strip().upper()
        raise MarketNotSupportedError(rejected, venue)
    return market_key


class GatewayFundingRateProvider:
    """Funding rate provider that delegates to the gateway sidecar.

    The gateway owns all network egress (Hyperliquid HTTP, GMX V2 RPC),
    credential storage, SSL configuration, and rate limiting. Strategy
    code calls this provider over the in-cluster gRPC channel only.
    """

    def __init__(
        self,
        gateway_client: GatewayClient,
        chain: str = DEFAULT_CHAIN,
        cache_ttl_seconds: float = DEFAULT_CACHE_TTL_SECONDS,
    ) -> None:
        self._gateway_client = gateway_client
        self._chain = chain.lower()
        self._cache_ttl_seconds = cache_ttl_seconds

        # venue -> market -> (rate, monotonic_timestamp)
        self._cache: dict[str, dict[tuple[str, str], tuple[FundingRate, float]]] = {}

        logger.info(
            "GatewayFundingRateProvider initialized (chain=%s, cache_ttl=%ss)",
            self._chain,
            cache_ttl_seconds,
        )

    @property
    def chain(self) -> str:
        return self._chain

    def _get_cached_rate(self, venue: str, market_identity: tuple[str, str]) -> FundingRate | None:
        venue_cache = self._cache.get(venue, {})
        if market_identity in venue_cache:
            rate, ts = venue_cache[market_identity]
            if time.monotonic() - ts < self._cache_ttl_seconds:
                return rate
        return None

    def _set_cached_rate(self, venue: str, market_identity: tuple[str, str], rate: FundingRate) -> None:
        self._cache.setdefault(venue, {})[market_identity] = (rate, time.monotonic())

    def _response_to_funding_rate(self, response) -> FundingRate:
        """Convert a gRPC FundingRateResponse to :class:`FundingRate`."""
        rate_hourly = Decimal(response.rate_hourly)
        next_funding = (
            datetime.fromtimestamp(response.next_funding_time, tz=UTC) if response.next_funding_time else None
        )
        observed_at = getattr(response, "observed_at", 0)
        return FundingRate(
            venue=response.venue,
            market=response.market,
            rate_hourly=rate_hourly,
            rate_8h=Decimal(response.rate_8h),
            rate_annualized=Decimal(response.rate_annualized),
            long_rate_hourly=(
                Decimal(response.long_rate_hourly) if getattr(response, "long_rate_hourly", "") != "" else None
            ),
            short_rate_hourly=(
                Decimal(response.short_rate_hourly) if getattr(response, "short_rate_hourly", "") != "" else None
            ),
            market_address=getattr(response, "market_address", "") or None,
            timestamp=datetime.fromtimestamp(observed_at, tz=UTC) if observed_at else datetime.now(UTC),
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
        market_address: str = "",
    ) -> FundingRate:
        """Get the current funding rate for ``venue``/``market``.

        Raises:
            VenueNotSupportedError: ``venue`` is not in ``SUPPORTED_VENUES``.
            MarketNotSupportedError: ``market`` is not in ``SUPPORTED_MARKETS[venue]``.
            FundingRateUnavailableError: the gateway returned an error.
        """
        venue_str = _normalize_venue(venue)
        market_str = _validate_market(venue_str, market)
        effective_market_address = market_address.strip()
        if not effective_market_address and _EVM_ADDRESS_RE.fullmatch(market_str):
            effective_market_address = market_str

        cache_key = (market_str, effective_market_address.lower())
        cached = self._get_cached_rate(venue_str, cache_key)
        if cached is not None:
            return cached

        from almanak.gateway.proto import gateway_pb2

        request = gateway_pb2.FundingRateRequest(
            venue=venue_str,
            market=market_str,
            chain=self._chain,
            market_address=effective_market_address,
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
        response_market_address = getattr(response, "market_address", "")
        if effective_market_address and response_market_address.lower() != effective_market_address.lower():
            raise FundingRateUnavailableError(
                venue_str,
                market_str,
                "gateway did not preserve the requested market address",
            )

        rate = self._response_to_funding_rate(response)
        self._set_cached_rate(venue_str, cache_key, rate)
        return rate

    async def get_funding_rate_spread(
        self,
        market: str,
        venue_a: Venue | str,
        venue_b: Venue | str,
        market_address: str = "",
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
        effective_market_address = market_address.strip()

        from almanak.gateway.proto import gateway_pb2

        request = gateway_pb2.FundingRateSpreadRequest(
            market=market_str,
            venue_a=venue_a_str,
            venue_b=venue_b_str,
            chain=self._chain,
            market_address=effective_market_address,
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

        if effective_market_address:
            from almanak.connectors._strategy_base.funding_history_registry import FundingHistoryRegistry

            for venue, rate_response in (
                (venue_a_str, response.venue_a_rate),
                (venue_b_str, response.venue_b_rate),
            ):
                if FundingHistoryRegistry.discovers_markets(venue) and (
                    getattr(rate_response, "market_address", "").lower() != effective_market_address.lower()
                ):
                    raise FundingRateUnavailableError(
                        f"{venue_a_str}/{venue_b_str}",
                        market_str,
                        f"gateway did not preserve the requested market address for {venue}",
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
