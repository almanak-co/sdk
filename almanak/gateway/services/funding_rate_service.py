"""FundingRateService implementation - perpetual funding rate data.

This service provides funding rate data from perpetual trading venues:
- Hyperliquid: REST API for funding rates
- GMX V2: On-chain contract calls for funding rates

All external access is handled in the gateway, keeping API keys and
RPC credentials secure.
"""

import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import aiohttp
import grpc
from pydantic import BaseModel
from web3 import AsyncHTTPProvider, AsyncWeb3

from almanak.connectors._base.gateway_capabilities import (
    GatewayFundingRateCapability,
)
from almanak.connectors._gateway_registry import GATEWAY_REGISTRY
from almanak.core.perp_markets import perp_market_base, perp_market_funding_key
from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2, gateway_pb2_grpc
from almanak.gateway.utils import get_rpc_url
from almanak.gateway.utils.ssl_context import build_ssl_context

logger = logging.getLogger(__name__)


# =============================================================================
# Pydantic Models for API Response Validation
# =============================================================================


class HyperliquidAssetContext(BaseModel):
    """Asset context from Hyperliquid metaAndAssetCtxs response."""

    funding: str | None = None
    openInterest: str | None = None
    markPx: str | None = None


class HyperliquidUniverseItem(BaseModel):
    """Universe item from Hyperliquid meta response."""

    name: str


# =============================================================================
# Constants
# =============================================================================

HOURS_PER_YEAR = 8760

# Hyperliquid API endpoint
HYPERLIQUID_API_URL = "https://api.hyperliquid.xyz/info"

# GMX V2 plumbing (reader/DataStore/market addresses + the getMarketInfo
# ABI) lives on the GMX connector's gateway provider
# (``almanak.connectors.gmx_v2.gateway.provider``), sourced from the
# connector's audited address catalogue. This module previously carried
# hand-copied duplicates that drifted outside that audit.

# Default funding rates — registry-driven (VIB-4811 / Phase 3).
#
# Previously a hardcoded ``DEFAULT_RATES = {"gmx_v2": {...}, "hyperliquid":
# {...}}`` dispatch dict. Each venue's defaults now live on its own
# connector under ``almanak/connectors/<venue>/gateway/provider.py`` and
# implement ``GatewayFundingRateCapability``. ``_get_default_rate``
# below queries the registry; behavior is byte-identical.

# Default mark prices (fallback)
DEFAULT_MARK_PRICES = {
    "ETH-USD": Decimal("3000"),
    "BTC-USD": Decimal("60000"),
    "ARB-USD": Decimal("1.2"),
    "LINK-USD": Decimal("15"),
    "SOL-USD": Decimal("150"),
}


@dataclass
class FundingRateData:
    """Internal funding rate data structure."""

    venue: str
    market: str
    rate_hourly: Decimal
    open_interest_long: Decimal
    open_interest_short: Decimal
    mark_price: Decimal
    index_price: Decimal
    next_funding_time: datetime
    is_live_data: bool


# =============================================================================
# Hyperliquid response parsing helpers (pure, module-private)
# =============================================================================


def _find_hyperliquid_coin_index(universe: list, coin: str) -> int | None:
    """Locate ``coin`` within Hyperliquid's universe list, validating each entry."""
    for i, u in enumerate(universe):
        try:
            item = HyperliquidUniverseItem.model_validate(u)
        except Exception:
            logger.debug("Skipping invalid universe item at index %d", i)
            continue
        if item.name.upper() == coin:
            return i
    return None


def _parse_hyperliquid_asset_ctx(
    asset_ctxs: list,
    coin_index: int,
    market: str,
    default_rate: Decimal,
    default_mark: Decimal,
    default_oi_long: Decimal,
    default_oi_short: Decimal,
) -> tuple[Decimal, Decimal, Decimal, Decimal, bool]:
    """Extract (rate_hourly, oi_long, oi_short, mark_price, is_live) from one asset context."""
    rate_hourly = default_rate
    open_interest_long = default_oi_long
    open_interest_short = default_oi_short
    mark_price = default_mark
    is_live_data = False

    if coin_index >= len(asset_ctxs):
        return rate_hourly, open_interest_long, open_interest_short, mark_price, is_live_data

    try:
        ctx = HyperliquidAssetContext.model_validate(asset_ctxs[coin_index])
    except Exception as e:
        logger.warning("Invalid Hyperliquid asset context for %s: %s", market, e)
        ctx = HyperliquidAssetContext()

    # Funding is reported as an 8-hour rate; convert to hourly to match our schema.
    if ctx.funding:
        funding_8h = Decimal(str(ctx.funding))
        rate_hourly = funding_8h / Decimal("8")
        is_live_data = True

    if ctx.openInterest and ctx.markPx:
        oi_coins = Decimal(str(ctx.openInterest))
        mark_price = Decimal(str(ctx.markPx))
        total_oi_usd = oi_coins * mark_price
        open_interest_long = total_oi_usd * Decimal("0.52")
        open_interest_short = total_oi_usd * Decimal("0.48")

    return rate_hourly, open_interest_long, open_interest_short, mark_price, is_live_data


def _compute_hyperliquid_next_funding_time(now: datetime) -> datetime:
    """Hyperliquid settles funding every 8 hours at 00:00 / 08:00 / 16:00 UTC."""
    next_settlement_hour = ((now.hour // 8) + 1) * 8
    if next_settlement_hour >= 24:
        return (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    return now.replace(hour=next_settlement_hour, minute=0, second=0, microsecond=0)


class FundingRateServiceServicer(gateway_pb2_grpc.FundingRateServiceServicer):
    """Implements FundingRateService gRPC interface.

    Provides funding rate data from perpetual trading venues:
    - Hyperliquid: REST API
    - GMX V2: On-chain contract calls
    """

    def __init__(self, settings: GatewaySettings):
        """Initialize FundingRateService.

        Args:
            settings: Gateway settings with API keys
        """
        self.settings = settings
        self._http_session: aiohttp.ClientSession | None = None
        self._web3_cache: dict[str, AsyncWeb3] = {}

        # Venue → ``GatewayFundingRateCapability`` provider. Resolved
        # once at servicer construction so the dispatch path stays
        # O(1) (no per-request registry walk). Built from
        # ``GATEWAY_REGISTRY.capability_providers`` so adding a new
        # perp venue is purely a new connector registration — no edit
        # to this file required. (VIB-4811 / Phase 3.)
        # Venue keys are lowercased so the dispatcher (which does
        # ``request.venue.lower()`` on incoming requests) lines up
        # whatever case a connector's ``venue()`` returns. Duplicate
        # venue ids across two registered connectors are a hard error —
        # ``GATEWAY_REGISTRY.register`` only guards unique
        # ``ProtocolName``, not unique ``venue()``. (CodeRabbit +
        # Gemini code-review.)
        self._funding_rate_providers: dict[str, GatewayFundingRateCapability] = {}
        # mypy: passing a ``@runtime_checkable`` Protocol class to
        # ``capability_providers`` trips ``type-abstract``; this is
        # the intentional dispatcher contract.
        for connector in GATEWAY_REGISTRY.capability_providers(GatewayFundingRateCapability):  # type: ignore[type-abstract]
            venue = connector.venue().lower()
            existing = self._funding_rate_providers.get(venue)
            if existing is not None and existing is not connector:
                raise RuntimeError(
                    f"Duplicate funding-rate provider for venue {venue!r}: "
                    f"{type(existing).__qualname__} vs "
                    f"{type(connector).__qualname__}"
                )
            self._funding_rate_providers[venue] = connector

        logger.debug(
            "Initialized FundingRateService (venues=%s)",
            sorted(self._funding_rate_providers.keys()),
        )

    async def _get_http_session(self) -> aiohttp.ClientSession:
        """Get or create HTTP session."""
        if self._http_session is None or self._http_session.closed:
            connector = aiohttp.TCPConnector(ssl=build_ssl_context())
            self._http_session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=10.0),
                connector=connector,
            )
        return self._http_session

    async def _get_web3(self, chain: str) -> AsyncWeb3 | None:
        """Get Web3 instance for a chain."""
        if chain in self._web3_cache:
            return self._web3_cache[chain]

        try:
            network = self.settings.network
            rpc_url = get_rpc_url(chain, network=network)
            web3 = AsyncWeb3(AsyncHTTPProvider(rpc_url, request_kwargs={"ssl": build_ssl_context()}))
            self._web3_cache[chain] = web3
            return web3
        except ValueError as e:
            logger.warning("Failed to get RPC URL for %s: %s", chain, e)
            return None

    def _get_default_rate(self, venue: str, market: str) -> Decimal:
        """Get default funding rate for a market via the capability registry.

        Each perp connector publishes its own per-market default table
        through ``GatewayFundingRateCapability.default_funding_rate``;
        the gateway no longer carries a hardcoded venue dict. Returns
        the historical ``Decimal("0.00001")`` fallback for unknown
        ``(venue, market)`` pairs.
        """
        connector = self._funding_rate_providers.get(venue.lower())
        if connector is None:
            return Decimal("0.00001")
        return connector.default_funding_rate(market)

    def _get_default_mark_price(self, market: str) -> Decimal:
        """Get default mark price for a market."""
        return DEFAULT_MARK_PRICES.get(market, Decimal("1000"))

    async def _fetch_hyperliquid_rate(self, market: str) -> FundingRateData:
        """Fetch Hyperliquid funding rate from their public API."""
        # Canonicalize defensively (the RPC ingress already does): default
        # rate/mark tables are keyed by the dash form.
        market = perp_market_funding_key(market) or market
        rate_hourly = self._get_default_rate("hyperliquid", market)
        open_interest_long = Decimal("85000000")
        open_interest_short = Decimal("82000000")
        mark_price = self._get_default_mark_price(market)
        is_live_data = False

        # Map any market spelling (ETH-USD / ETH/USD / ETH) to the
        # Hyperliquid coin format (ETH) via the canonical parse.
        coin = perp_market_base(market) or market.upper()

        try:
            session = await self._get_http_session()

            async with session.post(
                HYPERLIQUID_API_URL,
                json={"type": "metaAndAssetCtxs"},
                headers={"Content-Type": "application/json"},
            ) as response:
                if response.status != 200:
                    logger.warning("Hyperliquid API returned %d for %s", response.status, market)
                else:
                    data = await response.json()
                    if isinstance(data, list) and len(data) >= 2:
                        universe = data[0].get("universe", [])
                        asset_ctxs = data[1]
                        coin_index = _find_hyperliquid_coin_index(universe, coin)
                        if coin_index is not None:
                            (
                                rate_hourly,
                                open_interest_long,
                                open_interest_short,
                                mark_price,
                                is_live_data,
                            ) = _parse_hyperliquid_asset_ctx(
                                asset_ctxs,
                                coin_index,
                                market,
                                rate_hourly,
                                mark_price,
                                open_interest_long,
                                open_interest_short,
                            )
                            logger.debug(
                                "Fetched Hyperliquid rate for %s: %s/hour (live)",
                                market,
                                rate_hourly,
                            )

        except TimeoutError:
            logger.warning("Timeout fetching Hyperliquid rate for %s", market)
        except Exception as e:
            logger.warning("Failed to fetch Hyperliquid rate for %s: %s", market, e)

        return FundingRateData(
            venue="hyperliquid",
            market=market,
            rate_hourly=rate_hourly,
            open_interest_long=open_interest_long,
            open_interest_short=open_interest_short,
            mark_price=mark_price,
            index_price=mark_price,
            next_funding_time=_compute_hyperliquid_next_funding_time(datetime.now(UTC)),
            is_live_data=is_live_data,
        )

    def _rate_data_to_response(self, data: FundingRateData) -> gateway_pb2.FundingRateResponse:
        """Convert internal rate data to proto response."""
        rate_8h = data.rate_hourly * Decimal("8")
        rate_annualized = data.rate_hourly * Decimal(str(HOURS_PER_YEAR))

        return gateway_pb2.FundingRateResponse(
            venue=data.venue,
            market=data.market,
            rate_hourly=str(data.rate_hourly),
            rate_8h=str(rate_8h),
            rate_annualized=str(rate_annualized),
            next_funding_time=int(data.next_funding_time.timestamp()),
            open_interest_long=str(data.open_interest_long),
            open_interest_short=str(data.open_interest_short),
            mark_price=str(data.mark_price),
            index_price=str(data.index_price),
            is_live_data=data.is_live_data,
            success=True,
        )

    async def GetFundingRate(
        self,
        request: gateway_pb2.FundingRateRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.FundingRateResponse:
        """Get funding rate for a market on a specific venue.

        Args:
            request: Funding rate request with venue and market
            context: gRPC context

        Returns:
            FundingRateResponse with rate data
        """
        venue = request.venue.lower()
        # Canonicalize the market spelling at gateway ingress: venue tables
        # (GMX market addresses, default-rate maps) are keyed by the dash form
        # ("ETH-USD"); the SDK's documented slash form ("ETH/USD") must map to
        # the same rows (campaign-50 s38).
        market = perp_market_funding_key(request.market) or request.market.upper()
        chain = request.chain.lower() or "arbitrum"

        start_time = time.time()

        try:
            connector = self._funding_rate_providers.get(venue)
            if connector is None:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details(f"Unknown venue: {venue}")
                return gateway_pb2.FundingRateResponse(success=False, error=f"Unknown venue: {venue}")
            rate_data = await connector.fetch_funding_rate(self, market, chain)

            latency = time.time() - start_time
            logger.debug(
                "GetFundingRate for %s/%s completed in %.2fms",
                venue,
                market,
                latency * 1000,
            )

            return self._rate_data_to_response(rate_data)

        except Exception as e:
            logger.exception("GetFundingRate failed for %s/%s", venue, market)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return gateway_pb2.FundingRateResponse(success=False, error=str(e))

    async def GetFundingRateSpread(
        self,
        request: gateway_pb2.FundingRateSpreadRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.FundingRateSpreadResponse:
        """Get funding rate spread between two venues.

        Args:
            request: Spread request with market and two venues
            context: gRPC context

        Returns:
            FundingRateSpreadResponse with spread and individual rates
        """
        market = perp_market_funding_key(request.market) or request.market.upper()
        venue_a = request.venue_a.lower()
        venue_b = request.venue_b.lower()
        chain = request.chain.lower() or "arbitrum"

        try:
            # Fetch both rates concurrently
            rate_a_future = self._fetch_rate(venue_a, market, chain)
            rate_b_future = self._fetch_rate(venue_b, market, chain)

            rate_a, rate_b = await asyncio.gather(rate_a_future, rate_b_future)

            # Wire spread is absolute; SDK callers compute signed spread
            # locally from venue_a_rate and venue_b_rate to preserve the
            # historical wire convention for any out-of-repo consumer.
            spread_hourly = abs(rate_a.rate_hourly - rate_b.rate_hourly)
            spread_annualized = spread_hourly * Decimal(str(HOURS_PER_YEAR))

            return gateway_pb2.FundingRateSpreadResponse(
                spread_hourly=str(spread_hourly),
                spread_annualized=str(spread_annualized),
                venue_a_rate=self._rate_data_to_response(rate_a),
                venue_b_rate=self._rate_data_to_response(rate_b),
                success=True,
            )

        except ValueError as e:
            # Unknown venue is a user input error
            logger.warning(
                "GetFundingRateSpread invalid argument for %s (%s vs %s): %s",
                market,
                venue_a,
                venue_b,
                e,
            )
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.FundingRateSpreadResponse(success=False, error=str(e))

        except Exception as e:
            logger.exception(
                "GetFundingRateSpread failed for %s (%s vs %s)",
                market,
                venue_a,
                venue_b,
            )
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return gateway_pb2.FundingRateSpreadResponse(success=False, error=str(e))

    async def _fetch_rate(self, venue: str, market: str, chain: str) -> FundingRateData:
        """Fetch rate for any supported venue via the capability registry."""
        # ``_funding_rate_providers`` is lower-case keyed; normalize the
        # incoming venue so spread-request callers that don't pre-lower
        # the string still resolve. (Gemini code-review.)
        connector = self._funding_rate_providers.get(venue.lower())
        if connector is None:
            logger.error("Unknown venue requested: %s", venue)
            raise ValueError("Unknown venue")
        return await connector.fetch_funding_rate(self, market, chain)

    async def close(self) -> None:
        """Close HTTP session and Web3 connections."""
        import inspect

        if self._http_session and not self._http_session.closed:
            await self._http_session.close()
            self._http_session = None

        # Close Web3 provider sessions
        for chain, web3 in self._web3_cache.items():
            try:
                if hasattr(web3.provider, "disconnect"):
                    result = web3.provider.disconnect()
                    # Handle both sync and async disconnect methods
                    if inspect.iscoroutine(result):
                        await result
            except Exception as e:
                logger.warning("Failed to disconnect Web3 provider for %s: %s", chain, e)

        self._web3_cache.clear()
        logger.info("FundingRateService closed")
