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

from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2, gateway_pb2_grpc
from almanak.gateway.utils import get_rpc_url

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

# GMX V2 contract addresses
GMX_V2_READER_ADDRESSES = {
    "arbitrum": "0x5Ca84c34a381434786738735265b9f3FD814b824",
}

GMX_V2_DATA_STORE_ADDRESSES = {
    "arbitrum": "0xFD70de6b91282D8017aA4E741e9Ae325CAb992d8",
}

# GMX V2 market addresses (Arbitrum)
GMX_V2_MARKETS = {
    "arbitrum": {
        "ETH-USD": "0x70d95587d40A2caf56bd97485aB3Eec10Bee6336",
        "BTC-USD": "0x47c031236e19d024b42f8AE6780E44A573170703",
        "ARB-USD": "0xC25cEf6061Cf5dE5eb761b50E4743c1F5D7E5407",
        "LINK-USD": "0x7f1fa204bb700853D36994DA19F830b6Ad18455C",
        "SOL-USD": "0x09400D9DB990D5ed3f35D7be61DfAEB900Af03C9",
    },
}

# GMX V2 Reader ABI (minimal for getMarketInfo)
GMX_V2_READER_ABI = [
    {
        "inputs": [
            {"name": "dataStore", "type": "address"},
            {
                "name": "marketPrices",
                "type": "tuple",
                "components": [
                    {
                        "name": "indexTokenPrice",
                        "type": "tuple",
                        "components": [
                            {"name": "min", "type": "uint256"},
                            {"name": "max", "type": "uint256"},
                        ],
                    },
                    {
                        "name": "longTokenPrice",
                        "type": "tuple",
                        "components": [
                            {"name": "min", "type": "uint256"},
                            {"name": "max", "type": "uint256"},
                        ],
                    },
                    {
                        "name": "shortTokenPrice",
                        "type": "tuple",
                        "components": [
                            {"name": "min", "type": "uint256"},
                            {"name": "max", "type": "uint256"},
                        ],
                    },
                ],
            },
            {"name": "market", "type": "address"},
        ],
        "name": "getMarketInfo",
        "outputs": [
            {
                "name": "",
                "type": "tuple",
                "components": [
                    {
                        "name": "market",
                        "type": "tuple",
                        "components": [
                            {"name": "marketToken", "type": "address"},
                            {"name": "indexToken", "type": "address"},
                            {"name": "longToken", "type": "address"},
                            {"name": "shortToken", "type": "address"},
                        ],
                    },
                    {"name": "borrowingFactorPerSecondForLongs", "type": "uint256"},
                    {"name": "borrowingFactorPerSecondForShorts", "type": "uint256"},
                    {"name": "baseFundingFactorPerSecond", "type": "int256"},
                    {"name": "longsPayShorts", "type": "bool"},
                    {"name": "nextFundingFactorPerSecond", "type": "int256"},
                ],
            },
        ],
        "stateMutability": "view",
        "type": "function",
    },
]

# Default funding rates (fallback)
DEFAULT_RATES = {
    "gmx_v2": {
        "ETH-USD": Decimal("0.000012"),
        "BTC-USD": Decimal("0.000010"),
        "ARB-USD": Decimal("0.000015"),
        "LINK-USD": Decimal("0.000008"),
        "SOL-USD": Decimal("0.000018"),
    },
    "hyperliquid": {
        "ETH-USD": Decimal("0.000015"),
        "BTC-USD": Decimal("0.000011"),
        "ARB-USD": Decimal("0.000018"),
        "LINK-USD": Decimal("0.000009"),
        "SOL-USD": Decimal("0.000022"),
    },
}

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

        logger.debug("Initialized FundingRateService")

    async def _get_http_session(self) -> aiohttp.ClientSession:
        """Get or create HTTP session."""
        if self._http_session is None or self._http_session.closed:
            self._http_session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10.0))
        return self._http_session

    async def _get_web3(self, chain: str) -> AsyncWeb3 | None:
        """Get Web3 instance for a chain."""
        if chain in self._web3_cache:
            return self._web3_cache[chain]

        try:
            network = self.settings.network
            rpc_url = get_rpc_url(chain, network=network)
            web3 = AsyncWeb3(AsyncHTTPProvider(rpc_url))
            self._web3_cache[chain] = web3
            return web3
        except ValueError as e:
            logger.warning("Failed to get RPC URL for %s: %s", chain, e)
            return None

    def _get_default_rate(self, venue: str, market: str) -> Decimal:
        """Get default funding rate for a market."""
        venue_rates = DEFAULT_RATES.get(venue.lower(), {})
        return venue_rates.get(market, Decimal("0.00001"))

    def _get_default_mark_price(self, market: str) -> Decimal:
        """Get default mark price for a market."""
        return DEFAULT_MARK_PRICES.get(market, Decimal("1000"))

    async def _fetch_hyperliquid_rate(self, market: str) -> FundingRateData:
        """Fetch Hyperliquid funding rate from their public API."""
        rate_hourly = self._get_default_rate("hyperliquid", market)
        open_interest_long = Decimal("85000000")
        open_interest_short = Decimal("82000000")
        mark_price = self._get_default_mark_price(market)
        is_live_data = False

        # Map market format (ETH-USD) to Hyperliquid format (ETH)
        coin = market.split("-")[0].upper()

        try:
            session = await self._get_http_session()

            async with session.post(
                HYPERLIQUID_API_URL,
                json={"type": "metaAndAssetCtxs"},
                headers={"Content-Type": "application/json"},
            ) as response:
                if response.status == 200:
                    data = await response.json()

                    if isinstance(data, list) and len(data) >= 2:
                        meta = data[0]
                        asset_ctxs = data[1]

                        # Find the coin index by validating universe items
                        universe = meta.get("universe", [])
                        coin_index = None
                        for i, u in enumerate(universe):
                            try:
                                item = HyperliquidUniverseItem.model_validate(u)
                                if item.name.upper() == coin:
                                    coin_index = i
                                    break
                            except Exception:
                                logger.debug("Skipping invalid universe item at index %d", i)
                                continue

                        if coin_index is not None and coin_index < len(asset_ctxs):
                            # Validate asset context with Pydantic
                            try:
                                ctx = HyperliquidAssetContext.model_validate(asset_ctxs[coin_index])
                            except Exception as e:
                                logger.warning("Invalid Hyperliquid asset context for %s: %s", market, e)
                                ctx = HyperliquidAssetContext()

                            # Extract funding rate (8-hour rate as decimal)
                            if ctx.funding:
                                funding_8h = Decimal(str(ctx.funding))
                                rate_hourly = funding_8h / Decimal("8")
                                is_live_data = True

                            # Extract open interest
                            if ctx.openInterest and ctx.markPx:
                                oi_coins = Decimal(str(ctx.openInterest))
                                mark_price = Decimal(str(ctx.markPx))
                                total_oi_usd = oi_coins * mark_price
                                open_interest_long = total_oi_usd * Decimal("0.52")
                                open_interest_short = total_oi_usd * Decimal("0.48")

                            logger.debug(
                                "Fetched Hyperliquid rate for %s: %s/hour (live)",
                                market,
                                rate_hourly,
                            )
                else:
                    logger.warning("Hyperliquid API returned %d for %s", response.status, market)

        except TimeoutError:
            logger.warning("Timeout fetching Hyperliquid rate for %s", market)
        except Exception as e:
            logger.warning("Failed to fetch Hyperliquid rate for %s: %s", market, e)

        # Calculate next funding time (8-hour windows at 00:00, 08:00, 16:00 UTC)
        now = datetime.now(UTC)
        current_hour = now.hour
        next_settlement_hour = ((current_hour // 8) + 1) * 8
        if next_settlement_hour >= 24:
            next_settlement_hour = 0
            next_funding_time = (now + timedelta(days=1)).replace(
                hour=next_settlement_hour, minute=0, second=0, microsecond=0
            )
        else:
            next_funding_time = now.replace(hour=next_settlement_hour, minute=0, second=0, microsecond=0)

        return FundingRateData(
            venue="hyperliquid",
            market=market,
            rate_hourly=rate_hourly,
            open_interest_long=open_interest_long,
            open_interest_short=open_interest_short,
            mark_price=mark_price,
            index_price=mark_price,
            next_funding_time=next_funding_time,
            is_live_data=is_live_data,
        )

    async def _fetch_gmx_v2_rate(self, market: str, chain: str) -> FundingRateData:
        """Fetch GMX V2 funding rate from on-chain contract."""
        rate_hourly = self._get_default_rate("gmx_v2", market)
        open_interest_long = Decimal("125000000")
        open_interest_short = Decimal("118000000")
        mark_price = self._get_default_mark_price(market)
        is_live_data = False

        web3 = await self._get_web3(chain)
        if web3 and chain in GMX_V2_READER_ADDRESSES:
            market_address = GMX_V2_MARKETS.get(chain, {}).get(market)
            if market_address:
                try:
                    reader_address = GMX_V2_READER_ADDRESSES[chain]
                    data_store_address = GMX_V2_DATA_STORE_ADDRESSES[chain]

                    reader = web3.eth.contract(
                        address=web3.to_checksum_address(reader_address),
                        abi=GMX_V2_READER_ABI,
                    )

                    # Use approximate prices (GMX uses 30 decimals)
                    eth_price = 3000 * 10**30
                    btc_price = 60000 * 10**30

                    if "BTC" in market:
                        price = btc_price
                    else:
                        price = eth_price

                    market_prices = (
                        (price, price),  # indexTokenPrice (min, max)
                        (price, price),  # longTokenPrice
                        (1 * 10**30, 1 * 10**30),  # shortTokenPrice (USDC = $1)
                    )

                    market_info = await asyncio.wait_for(
                        reader.functions.getMarketInfo(
                            web3.to_checksum_address(data_store_address),
                            market_prices,
                            web3.to_checksum_address(market_address),
                        ).call(),
                        timeout=10.0,
                    )

                    # Extract funding factor from market info
                    next_funding_factor_per_second = market_info[5]  # int256

                    # Convert from per-second (30 decimals) to hourly rate
                    # Preserve sign: positive = longs pay shorts, negative = shorts pay longs
                    funding_per_second = Decimal(str(next_funding_factor_per_second)) / Decimal(10**30)
                    rate_hourly = funding_per_second * Decimal("3600")

                    is_live_data = True
                    logger.debug(
                        "Fetched GMX V2 rate for %s: %s/hour (live)",
                        market,
                        rate_hourly,
                    )

                except TimeoutError:
                    logger.warning("Timeout fetching GMX V2 rate for %s", market)
                except Exception as e:
                    logger.warning("Failed to fetch GMX V2 rate for %s: %s", market, e)

        # Calculate next funding time (GMX V2 settles hourly)
        now = datetime.now(UTC)
        next_hour = (now + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)

        return FundingRateData(
            venue="gmx_v2",
            market=market,
            rate_hourly=rate_hourly,
            open_interest_long=open_interest_long,
            open_interest_short=open_interest_short,
            mark_price=mark_price,
            index_price=mark_price,
            next_funding_time=next_hour,
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
        market = request.market.upper()
        chain = request.chain.lower() or "arbitrum"

        start_time = time.time()

        try:
            if venue == "hyperliquid":
                rate_data = await self._fetch_hyperliquid_rate(market)
            elif venue == "gmx_v2":
                rate_data = await self._fetch_gmx_v2_rate(market, chain)
            else:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details(f"Unknown venue: {venue}")
                return gateway_pb2.FundingRateResponse(success=False, error=f"Unknown venue: {venue}")

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
        market = request.market.upper()
        venue_a = request.venue_a.lower()
        venue_b = request.venue_b.lower()
        chain = request.chain.lower() or "arbitrum"

        try:
            # Fetch both rates concurrently
            rate_a_future = self._fetch_rate(venue_a, market, chain)
            rate_b_future = self._fetch_rate(venue_b, market, chain)

            rate_a, rate_b = await asyncio.gather(rate_a_future, rate_b_future)

            # Calculate spread
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
        """Fetch rate for any supported venue."""
        if venue == "hyperliquid":
            return await self._fetch_hyperliquid_rate(market)
        elif venue == "gmx_v2":
            return await self._fetch_gmx_v2_rate(market, chain)
        else:
            logger.error("Unknown venue requested: %s", venue)
            raise ValueError("Unknown venue")

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
