"""Funding rate data provider for perpetual futures protocols.

This module provides a client for fetching historical funding rate data
from perpetual futures protocols. Accurate funding rates are essential
for realistic perp position P&L calculation in backtesting.

Supported Protocols:
    - GMX V2: Via GMX stats API and subgraph
    - Hyperliquid: Via public REST API

Key Features:
    - Fetches historical funding rates by protocol, market, and timestamp
    - Implements caching with 1-hour TTL
    - Handles rate limits gracefully with exponential backoff
    - Falls back to default rates when data unavailable

Example:
    from almanak.framework.backtesting.pnl.providers.funding_rates import (
        FundingRateProvider,
        FundingRateData,
    )
    from datetime import datetime, timezone

    provider = FundingRateProvider()

    # Get historical funding rate for GMX ETH-USD
    rate = await provider.get_historical_funding_rate(
        protocol="gmx",
        market="ETH-USD",
        timestamp=datetime(2024, 1, 15, 12, 0, tzinfo=timezone.utc),
    )
    print(f"Funding rate: {rate.rate} ({rate.annualized_rate_pct}% APR)")
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import aiohttp

logger = logging.getLogger(__name__)


# =============================================================================
# API Endpoints
# =============================================================================

# GMX Stats API endpoints (https://stats.gmx.io)
GMX_STATS_API: dict[str, str] = {
    "arbitrum": "https://arbitrum-api.gmxinfra.io",
    "avalanche": "https://avalanche-api.gmxinfra.io",
}

# GMX V2 Subgraph endpoints for funding rates
GMX_V2_SUBGRAPHS: dict[str, str] = {
    "arbitrum": "https://subgraph.satsuma-prod.com/3b2ced13c8d9/gmx/synthetics-arbitrum-stats/api",
    "avalanche": "https://subgraph.satsuma-prod.com/3b2ced13c8d9/gmx/synthetics-avalanche-stats/api",
}

# Hyperliquid REST API endpoint
HYPERLIQUID_API = "https://api.hyperliquid.xyz/info"

# Supported protocols
SUPPORTED_PROTOCOLS = ["gmx", "gmx_v2", "hyperliquid"]

# Default cache TTL: 1 hour for historical data
DEFAULT_CACHE_TTL_SECONDS = 3600

# Rate limit settings
DEFAULT_REQUESTS_PER_MINUTE = 30
DEFAULT_REQUEST_TIMEOUT_SECONDS = 30


# =============================================================================
# Exceptions
# =============================================================================


class FundingRateError(Exception):
    """Base exception for funding rate provider errors."""


class FundingRateNotFoundError(FundingRateError):
    """Raised when funding rate data is not found for a market."""

    def __init__(self, protocol: str, market: str, timestamp: datetime) -> None:
        self.protocol = protocol
        self.market = market
        self.timestamp = timestamp
        super().__init__(f"Funding rate not found for {protocol} {market} at {timestamp.isoformat()}")


class FundingRateRateLimitError(FundingRateError):
    """Raised when API rate limit is exceeded."""

    def __init__(self, retry_after_seconds: float | None = None) -> None:
        self.retry_after_seconds = retry_after_seconds
        msg = "Funding rate API rate limit exceeded"
        if retry_after_seconds:
            msg += f", retry after {retry_after_seconds}s"
        super().__init__(msg)


class UnsupportedProtocolError(FundingRateError):
    """Raised when protocol is not supported."""

    def __init__(self, protocol: str) -> None:
        self.protocol = protocol
        super().__init__(f"Unsupported protocol: {protocol}. Supported: {SUPPORTED_PROTOCOLS}")


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class FundingRateData:
    """Funding rate data for a market at a specific time.

    Attributes:
        protocol: The perpetual protocol (gmx, hyperliquid)
        market: The market identifier (e.g., "ETH-USD", "BTC-USD")
        timestamp: The timestamp this rate applies to
        rate: The hourly funding rate (positive = longs pay, negative = shorts pay)
        annualized_rate_pct: Annualized funding rate as percentage
        next_funding_time: When the next funding payment occurs (if available)
        open_interest_long: Long open interest in USD (if available)
        open_interest_short: Short open interest in USD (if available)
        source: Data source (api, subgraph, fallback)
    """

    protocol: str
    market: str
    timestamp: datetime
    rate: Decimal  # Hourly funding rate
    annualized_rate_pct: Decimal = Decimal("0")
    next_funding_time: datetime | None = None
    open_interest_long: Decimal | None = None
    open_interest_short: Decimal | None = None
    source: str = "api"

    def __post_init__(self) -> None:
        """Calculate annualized rate if not provided."""
        if self.annualized_rate_pct == Decimal("0") and self.rate != Decimal("0"):
            # Annualize: hourly_rate * 24 * 365 * 100 for percentage
            self.annualized_rate_pct = self.rate * Decimal("8760") * Decimal("100")

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "protocol": self.protocol,
            "market": self.market,
            "timestamp": self.timestamp.isoformat(),
            "rate": str(self.rate),
            "annualized_rate_pct": str(self.annualized_rate_pct),
            "next_funding_time": self.next_funding_time.isoformat() if self.next_funding_time else None,
            "open_interest_long": str(self.open_interest_long) if self.open_interest_long else None,
            "open_interest_short": str(self.open_interest_short) if self.open_interest_short else None,
            "source": self.source,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "FundingRateData":
        """Deserialize from dictionary."""
        return cls(
            protocol=data["protocol"],
            market=data["market"],
            timestamp=datetime.fromisoformat(data["timestamp"]),
            rate=Decimal(data["rate"]),
            annualized_rate_pct=Decimal(data.get("annualized_rate_pct", "0")),
            next_funding_time=datetime.fromisoformat(data["next_funding_time"])
            if data.get("next_funding_time")
            else None,
            open_interest_long=Decimal(data["open_interest_long"]) if data.get("open_interest_long") else None,
            open_interest_short=Decimal(data["open_interest_short"]) if data.get("open_interest_short") else None,
            source=data.get("source", "api"),
        )


@dataclass
class CachedFundingRate:
    """Cached funding rate data with expiration."""

    data: FundingRateData
    fetched_at: float
    ttl_seconds: float

    @property
    def is_expired(self) -> bool:
        """Check if the cached data has expired."""
        return time.time() - self.fetched_at > self.ttl_seconds


@dataclass
class RateLimitState:
    """Tracks rate limit state for exponential backoff."""

    last_limit_time: float | None = None
    backoff_seconds: float = 1.0
    consecutive_limits: int = 0
    requests_this_minute: int = 0
    minute_start: float = field(default_factory=time.time)

    def record_rate_limit(self) -> None:
        """Record a rate limit hit and increase backoff."""
        self.last_limit_time = time.time()
        self.consecutive_limits += 1
        # Exponential backoff: 1s, 2s, 4s, 8s, 16s, max 32s
        self.backoff_seconds = min(32.0, 2 ** (self.consecutive_limits - 1))

    def record_success(self) -> None:
        """Record successful request, reset backoff."""
        self.consecutive_limits = 0
        self.backoff_seconds = 1.0

    def get_wait_time(self) -> float:
        """Get time to wait before next request."""
        if self.last_limit_time is None:
            return 0.0
        elapsed = time.time() - self.last_limit_time
        remaining = self.backoff_seconds - elapsed
        return max(0.0, remaining)

    def record_request(self) -> None:
        """Record a request for rate limiting."""
        current_time = time.time()
        if current_time - self.minute_start >= 60:
            # Reset counter for new minute
            self.minute_start = current_time
            self.requests_this_minute = 0
        self.requests_this_minute += 1


# =============================================================================
# Market Mappings
# =============================================================================

# GMX market symbols to contract addresses/identifiers
GMX_MARKETS: dict[str, dict[str, str]] = {
    "arbitrum": {
        "ETH-USD": "ETH",
        "BTC-USD": "BTC",
        "LINK-USD": "LINK",
        "ARB-USD": "ARB",
        "SOL-USD": "SOL",
        "UNI-USD": "UNI",
        "DOGE-USD": "DOGE",
        "LTC-USD": "LTC",
        "XRP-USD": "XRP",
    },
    "avalanche": {
        "ETH-USD": "ETH",
        "BTC-USD": "BTC",
        "AVAX-USD": "AVAX",
    },
}

# Hyperliquid market symbols (they use their own format)
HYPERLIQUID_MARKETS: dict[str, str] = {
    "ETH-USD": "ETH",
    "BTC-USD": "BTC",
    "SOL-USD": "SOL",
    "ARB-USD": "ARB",
    "LINK-USD": "LINK",
    "DOGE-USD": "DOGE",
    "AVAX-USD": "AVAX",
    "MATIC-USD": "MATIC",
    "OP-USD": "OP",
    "APT-USD": "APT",
    "TIA-USD": "TIA",
    "SEI-USD": "SEI",
    "INJ-USD": "INJ",
    "ATOM-USD": "ATOM",
}

# Default funding rates per protocol (hourly)
DEFAULT_FUNDING_RATES: dict[str, Decimal] = {
    "gmx": Decimal("0.0001"),  # 0.01% per hour (~8.76% APR)
    "gmx_v2": Decimal("0.0001"),
    "hyperliquid": Decimal("0.0001"),
}


# =============================================================================
# Funding Rate Provider
# =============================================================================


class FundingRateProvider:
    """Provider for fetching historical funding rates from perp protocols.

    This provider supports fetching funding rate data from GMX and Hyperliquid.
    It implements caching with 1-hour TTL and handles rate limits gracefully.

    Attributes:
        chain: The blockchain for GMX queries (arbitrum, avalanche)
        cache_ttl_seconds: Cache TTL in seconds (default: 1 hour)
        request_timeout: HTTP request timeout in seconds
        requests_per_minute: Maximum requests per minute

    Example:
        provider = FundingRateProvider()

        # Get historical funding rate
        rate = await provider.get_historical_funding_rate(
            protocol="gmx",
            market="ETH-USD",
            timestamp=datetime(2024, 1, 15, 12, 0, tzinfo=timezone.utc),
        )

        # Get current funding rate
        rate = await provider.get_current_funding_rate(
            protocol="hyperliquid",
            market="BTC-USD",
        )
    """

    def __init__(
        self,
        chain: str = "arbitrum",
        cache_ttl_seconds: float = DEFAULT_CACHE_TTL_SECONDS,
        request_timeout: float = DEFAULT_REQUEST_TIMEOUT_SECONDS,
        requests_per_minute: int = DEFAULT_REQUESTS_PER_MINUTE,
    ) -> None:
        """Initialize the funding rate provider.

        Args:
            chain: Blockchain for GMX queries (arbitrum, avalanche)
            cache_ttl_seconds: Cache TTL in seconds (default: 3600 = 1 hour)
            request_timeout: HTTP request timeout in seconds
            requests_per_minute: Maximum requests per minute

        Raises:
            ValueError: If chain is not supported for GMX
        """
        chain_lower = chain.lower()
        if chain_lower not in GMX_STATS_API:
            raise ValueError(f"Unsupported chain for GMX: {chain}. Supported: {list(GMX_STATS_API.keys())}")

        self._chain = chain_lower
        self._cache_ttl_seconds = cache_ttl_seconds
        self._request_timeout = request_timeout
        self._requests_per_minute = requests_per_minute

        # Cache: (protocol, market, timestamp_hour) -> CachedFundingRate
        self._cache: dict[tuple[str, str, datetime], CachedFundingRate] = {}

        # Rate limit state per protocol
        self._rate_limit_states: dict[str, RateLimitState] = {
            "gmx": RateLimitState(),
            "hyperliquid": RateLimitState(),
        }

        # HTTP session (lazy initialized)
        self._session: aiohttp.ClientSession | None = None

    @property
    def chain(self) -> str:
        """Get the chain this provider queries for GMX."""
        return self._chain

    @property
    def provider_name(self) -> str:
        """Get the provider name."""
        return f"funding_rates_{self._chain}"

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create HTTP session."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self._request_timeout))
        return self._session

    async def close(self) -> None:
        """Close the HTTP session."""
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    def _normalize_timestamp(self, timestamp: datetime) -> datetime:
        """Normalize timestamp to hourly boundary for caching."""
        # Round down to the hour
        return timestamp.replace(minute=0, second=0, microsecond=0)

    def _get_cache_key(self, protocol: str, market: str, timestamp: datetime) -> tuple[str, str, datetime]:
        """Get cache key for a funding rate query."""
        return (protocol.lower(), market.upper(), self._normalize_timestamp(timestamp))

    def _get_from_cache(self, protocol: str, market: str, timestamp: datetime) -> FundingRateData | None:
        """Try to get funding rate from cache."""
        key = self._get_cache_key(protocol, market, timestamp)
        cached = self._cache.get(key)

        if cached is None:
            return None

        if cached.is_expired:
            # Remove expired entry
            del self._cache[key]
            return None

        logger.debug(f"Cache hit for {protocol} {market} at {timestamp.isoformat()}")
        return cached.data

    def _add_to_cache(self, data: FundingRateData) -> None:
        """Add funding rate to cache."""
        key = self._get_cache_key(data.protocol, data.market, data.timestamp)
        self._cache[key] = CachedFundingRate(
            data=data,
            fetched_at=time.time(),
            ttl_seconds=self._cache_ttl_seconds,
        )

    async def _wait_for_rate_limit(self, protocol: str) -> None:
        """Wait if rate limited."""
        state = self._rate_limit_states.get(protocol, RateLimitState())
        wait_time = state.get_wait_time()
        if wait_time > 0:
            logger.debug(f"Rate limited for {protocol}, waiting {wait_time:.1f}s")
            await asyncio.sleep(wait_time)

    async def get_historical_funding_rate(
        self,
        protocol: str,
        market: str,
        timestamp: datetime,
    ) -> FundingRateData:
        """Get historical funding rate for a market at a specific timestamp.

        Args:
            protocol: The perpetual protocol (gmx, gmx_v2, hyperliquid)
            market: The market identifier (e.g., "ETH-USD")
            timestamp: The timestamp to query funding rate for

        Returns:
            FundingRateData with the funding rate information

        Raises:
            UnsupportedProtocolError: If protocol is not supported
            FundingRateNotFoundError: If data is not available for the query
            FundingRateRateLimitError: If rate limit is exceeded
        """
        protocol_lower = protocol.lower()

        # Validate protocol
        if protocol_lower not in SUPPORTED_PROTOCOLS:
            raise UnsupportedProtocolError(protocol)

        # Check cache first
        cached = self._get_from_cache(protocol_lower, market, timestamp)
        if cached is not None:
            return cached

        # Wait for rate limit if needed
        await self._wait_for_rate_limit(protocol_lower)

        # Fetch from appropriate API
        try:
            if protocol_lower in ("gmx", "gmx_v2"):
                data = await self._fetch_gmx_funding_rate(market, timestamp)
            elif protocol_lower == "hyperliquid":
                data = await self._fetch_hyperliquid_funding_rate(market, timestamp)
            else:
                raise UnsupportedProtocolError(protocol)

            # Update data with correct protocol
            data = FundingRateData(
                protocol=protocol_lower,
                market=data.market,
                timestamp=data.timestamp,
                rate=data.rate,
                annualized_rate_pct=data.annualized_rate_pct,
                next_funding_time=data.next_funding_time,
                open_interest_long=data.open_interest_long,
                open_interest_short=data.open_interest_short,
                source=data.source,
            )

            # Cache the result
            self._add_to_cache(data)

            # Record success
            state = self._rate_limit_states.get(protocol_lower)
            if state:
                state.record_success()
                state.record_request()

            return data

        except FundingRateRateLimitError:
            state = self._rate_limit_states.get(protocol_lower)
            if state:
                state.record_rate_limit()
            raise

        except FundingRateNotFoundError:
            # Fall back to default rate
            logger.warning(f"Funding rate not found for {protocol} {market}, using default")
            return self._get_default_funding_rate(protocol_lower, market, timestamp)

    async def get_current_funding_rate(
        self,
        protocol: str,
        market: str,
    ) -> FundingRateData:
        """Get current funding rate for a market.

        Convenience method that queries the current timestamp.

        Args:
            protocol: The perpetual protocol
            market: The market identifier

        Returns:
            FundingRateData with the current funding rate
        """
        return await self.get_historical_funding_rate(
            protocol=protocol,
            market=market,
            timestamp=datetime.now(UTC),
        )

    async def _fetch_gmx_funding_rate(
        self,
        market: str,
        timestamp: datetime,
    ) -> FundingRateData:
        """Fetch funding rate from GMX API.

        GMX uses a borrowing fee model where the rate depends on:
        - Open interest imbalance (longs vs shorts)
        - Pool utilization

        Args:
            market: Market identifier (e.g., "ETH-USD")
            timestamp: Timestamp to query

        Returns:
            FundingRateData from GMX API

        Raises:
            FundingRateNotFoundError: If market not found
            FundingRateRateLimitError: If rate limited
        """
        # Map market to GMX symbol
        markets = GMX_MARKETS.get(self._chain, {})
        gmx_symbol = markets.get(market.upper())
        if not gmx_symbol:
            raise FundingRateNotFoundError("gmx", market, timestamp)

        session = await self._get_session()

        # Try the GMX stats API first
        api_url = GMX_STATS_API.get(self._chain, "")
        if not api_url:
            raise FundingRateNotFoundError("gmx", market, timestamp)

        try:
            # GMX V2 funding rate endpoint
            # The actual endpoint structure may vary - this is a common pattern
            url = f"{api_url}/funding-rates/{gmx_symbol}"
            params = {
                "timestamp": int(timestamp.timestamp()),
            }

            async with session.get(url, params=params) as response:
                if response.status == 429:
                    retry_after = response.headers.get("Retry-After")
                    raise FundingRateRateLimitError(float(retry_after) if retry_after else None)

                if response.status == 404:
                    raise FundingRateNotFoundError("gmx", market, timestamp)

                if response.status != 200:
                    logger.warning(f"GMX API returned {response.status}, falling back to subgraph")
                    return await self._fetch_gmx_funding_rate_subgraph(market, timestamp, gmx_symbol)

                data = await response.json()

                # Parse GMX response
                # GMX funding rates are typically expressed as hourly rates
                hourly_rate = Decimal(str(data.get("fundingRate", "0.0001")))

                return FundingRateData(
                    protocol="gmx",
                    market=market.upper(),
                    timestamp=self._normalize_timestamp(timestamp),
                    rate=hourly_rate,
                    open_interest_long=Decimal(str(data.get("longOpenInterest", 0)))
                    if data.get("longOpenInterest")
                    else None,
                    open_interest_short=Decimal(str(data.get("shortOpenInterest", 0)))
                    if data.get("shortOpenInterest")
                    else None,
                    source="gmx_api",
                )

        except aiohttp.ClientError as e:
            logger.warning(f"GMX API error: {e}, trying subgraph")
            return await self._fetch_gmx_funding_rate_subgraph(market, timestamp, gmx_symbol)

    async def _fetch_gmx_funding_rate_subgraph(
        self,
        market: str,
        timestamp: datetime,
        gmx_symbol: str,
    ) -> FundingRateData:
        """Fetch funding rate from GMX subgraph as fallback.

        Args:
            market: Market identifier
            timestamp: Timestamp to query
            gmx_symbol: GMX internal symbol

        Returns:
            FundingRateData from subgraph

        Raises:
            FundingRateNotFoundError: If data not available
        """
        subgraph_url = GMX_V2_SUBGRAPHS.get(self._chain)
        if not subgraph_url:
            raise FundingRateNotFoundError("gmx", market, timestamp)

        session = await self._get_session()

        # Query GMX subgraph for funding rate data
        # This is a simplified query - actual GMX subgraph schema may differ
        timestamp_int = int(timestamp.timestamp())
        query = """
        query FundingRate($market: String!, $timestamp: Int!) {
            fundingRates(
                where: {
                    market: $market,
                    timestamp_lte: $timestamp
                }
                orderBy: timestamp
                orderDirection: desc
                first: 1
            ) {
                market
                timestamp
                fundingRate
                longOpenInterest
                shortOpenInterest
            }
        }
        """

        try:
            async with session.post(
                subgraph_url,
                json={
                    "query": query,
                    "variables": {
                        "market": gmx_symbol,
                        "timestamp": timestamp_int,
                    },
                },
            ) as response:
                if response.status == 429:
                    raise FundingRateRateLimitError()

                if response.status != 200:
                    raise FundingRateNotFoundError("gmx", market, timestamp)

                result = await response.json()

                if "errors" in result:
                    logger.warning(f"GMX subgraph errors: {result['errors']}")
                    raise FundingRateNotFoundError("gmx", market, timestamp)

                rates = result.get("data", {}).get("fundingRates", [])
                if not rates:
                    raise FundingRateNotFoundError("gmx", market, timestamp)

                rate_data = rates[0]

                # Parse subgraph response
                # Rates are typically in wei-like format, need to convert
                raw_rate = Decimal(str(rate_data.get("fundingRate", "0")))
                # Convert from basis points or percentage to decimal rate
                hourly_rate = raw_rate / Decimal("1000000")  # Adjust as needed

                return FundingRateData(
                    protocol="gmx",
                    market=market.upper(),
                    timestamp=datetime.fromtimestamp(rate_data.get("timestamp", timestamp_int), tz=UTC),
                    rate=hourly_rate,
                    open_interest_long=Decimal(str(rate_data.get("longOpenInterest", 0)))
                    if rate_data.get("longOpenInterest")
                    else None,
                    open_interest_short=Decimal(str(rate_data.get("shortOpenInterest", 0)))
                    if rate_data.get("shortOpenInterest")
                    else None,
                    source="gmx_subgraph",
                )

        except aiohttp.ClientError as e:
            logger.warning(f"GMX subgraph error: {e}")
            raise FundingRateNotFoundError("gmx", market, timestamp) from e

    async def _fetch_hyperliquid_funding_rate(
        self,
        market: str,
        timestamp: datetime,
    ) -> FundingRateData:
        """Fetch funding rate from Hyperliquid API.

        Hyperliquid provides both current and historical funding rates
        via their public REST API.

        Args:
            market: Market identifier (e.g., "ETH-USD")
            timestamp: Timestamp to query

        Returns:
            FundingRateData from Hyperliquid API

        Raises:
            FundingRateNotFoundError: If market not found
            FundingRateRateLimitError: If rate limited
        """
        # Map market to Hyperliquid symbol
        hl_symbol = HYPERLIQUID_MARKETS.get(market.upper())
        if not hl_symbol:
            # Try direct symbol (Hyperliquid uses coin name)
            hl_symbol = market.upper().replace("-USD", "")

        session = await self._get_session()

        try:
            # Hyperliquid meta endpoint for funding rates
            # POST to /info with type "metaAndAssetCtxs"
            async with session.post(
                HYPERLIQUID_API,
                json={"type": "metaAndAssetCtxs"},
            ) as response:
                if response.status == 429:
                    retry_after = response.headers.get("Retry-After")
                    raise FundingRateRateLimitError(float(retry_after) if retry_after else None)

                if response.status != 200:
                    raise FundingRateNotFoundError("hyperliquid", market, timestamp)

                result = await response.json()

                # Parse Hyperliquid response
                # Response is [meta_info, [asset_ctxs...]]
                if not isinstance(result, list) or len(result) < 2:
                    raise FundingRateNotFoundError("hyperliquid", market, timestamp)

                asset_ctxs = result[1]
                meta = result[0]

                # Find the asset in the list
                asset_names = [u["name"] for u in meta.get("universe", [])]

                asset_index = None
                for i, name in enumerate(asset_names):
                    if name.upper() == hl_symbol.upper():
                        asset_index = i
                        break

                if asset_index is None or asset_index >= len(asset_ctxs):
                    raise FundingRateNotFoundError("hyperliquid", market, timestamp)

                asset_ctx = asset_ctxs[asset_index]

                # Parse funding rate from asset context
                # Hyperliquid returns funding as a percentage rate per 8 hours
                # Convert to hourly rate
                funding_8h = Decimal(str(asset_ctx.get("funding", "0")))
                hourly_rate = funding_8h / Decimal("8")

                # Open interest is in coin units, need mark price for USD
                mark_price = Decimal(str(asset_ctx.get("markPx", "1")))
                oi_long = None
                oi_short = None

                if "openInterest" in asset_ctx:
                    oi_total = Decimal(str(asset_ctx["openInterest"])) * mark_price
                    # Hyperliquid doesn't split long/short in this endpoint
                    # We could estimate based on funding rate direction
                    oi_long = oi_total / Decimal("2")  # Rough estimate
                    oi_short = oi_total / Decimal("2")

                return FundingRateData(
                    protocol="hyperliquid",
                    market=market.upper(),
                    timestamp=self._normalize_timestamp(timestamp),
                    rate=hourly_rate,
                    open_interest_long=oi_long,
                    open_interest_short=oi_short,
                    source="hyperliquid_api",
                )

        except aiohttp.ClientError as e:
            logger.warning(f"Hyperliquid API error: {e}")
            raise FundingRateNotFoundError("hyperliquid", market, timestamp) from e

    def _get_default_funding_rate(
        self,
        protocol: str,
        market: str,
        timestamp: datetime,
    ) -> FundingRateData:
        """Get default funding rate when API data is unavailable.

        Args:
            protocol: The protocol
            market: The market
            timestamp: The timestamp

        Returns:
            FundingRateData with default rate
        """
        default_rate = DEFAULT_FUNDING_RATES.get(protocol, Decimal("0.0001"))

        return FundingRateData(
            protocol=protocol,
            market=market.upper(),
            timestamp=self._normalize_timestamp(timestamp),
            rate=default_rate,
            source="fallback",
        )

    def get_default_rate(self, protocol: str) -> Decimal:
        """Get the default funding rate for a protocol.

        Args:
            protocol: Protocol name

        Returns:
            Default hourly funding rate
        """
        return DEFAULT_FUNDING_RATES.get(protocol.lower(), Decimal("0.0001"))

    def clear_cache(self) -> None:
        """Clear all cached funding rates."""
        self._cache.clear()

    def get_cache_stats(self) -> dict[str, Any]:
        """Get cache statistics.

        Returns:
            Dictionary with cache stats
        """
        total = len(self._cache)
        expired = sum(1 for c in self._cache.values() if c.is_expired)
        return {
            "total_entries": total,
            "expired_entries": expired,
            "valid_entries": total - expired,
        }

    def to_dict(self) -> dict[str, Any]:
        """Serialize provider config to dictionary."""
        return {
            "provider_name": self.provider_name,
            "chain": self._chain,
            "cache_ttl_seconds": self._cache_ttl_seconds,
            "request_timeout": self._request_timeout,
            "requests_per_minute": self._requests_per_minute,
            "supported_protocols": SUPPORTED_PROTOCOLS,
        }


__all__ = [
    # Main Provider
    "FundingRateProvider",
    # Data Classes
    "FundingRateData",
    "CachedFundingRate",
    "RateLimitState",
    # Exceptions
    "FundingRateError",
    "FundingRateNotFoundError",
    "FundingRateRateLimitError",
    "UnsupportedProtocolError",
    # Constants
    "SUPPORTED_PROTOCOLS",
    "DEFAULT_FUNDING_RATES",
    "GMX_MARKETS",
    "HYPERLIQUID_MARKETS",
]
