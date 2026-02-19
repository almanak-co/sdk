"""Lending APY data provider for historical interest rates.

This module provides a client for fetching historical supply and borrow APY data
from lending protocol subgraphs. Accurate historical APYs are essential for
realistic interest accrual calculations in backtesting.

Supported Protocols:
    - Aave V3: Via The Graph Aave V3 subgraphs
    - Compound V3: Via The Graph Compound V3 subgraphs

Key Features:
    - Fetches historical APY rates by protocol, market, and timestamp
    - Returns both supply APY and borrow APY
    - Implements caching with 1-hour TTL
    - Handles rate limits gracefully with exponential backoff
    - Falls back to default rates when data unavailable

Example:
    from almanak.framework.backtesting.pnl.providers.lending_apy import (
        LendingAPYProvider,
        LendingAPYData,
    )
    from datetime import datetime, timezone

    provider = LendingAPYProvider()

    # Get historical APY for Aave V3 USDC
    apy = await provider.get_historical_apy(
        protocol="aave_v3",
        market="USDC",
        timestamp=datetime(2024, 1, 15, 12, 0, tzinfo=timezone.utc),
    )
    print(f"Supply APY: {apy.supply_apy_pct}%, Borrow APY: {apy.borrow_apy_pct}%")
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

# Aave V3 Subgraph endpoints (The Graph decentralized network)
AAVE_V3_SUBGRAPHS: dict[str, str] = {
    "ethereum": "https://gateway.thegraph.com/api/subgraphs/id/Cd2gEDVeqnjBn1hSeqFMitw8Q1iiyV9FYUZkLNRcL87g",
    "arbitrum": "https://gateway.thegraph.com/api/subgraphs/id/DLuE98kEb5pQNXAcKFQGQgfSQ57Xdou4jnVbAEqMfy3B",
    "optimism": "https://gateway.thegraph.com/api/subgraphs/id/DSfLz8oQBUeU5atALgUFQKMTSYV9mZAVYp4noLSXAfvb",
    "polygon": "https://gateway.thegraph.com/api/subgraphs/id/Co2URyXjnxaw8WqxKyVHdirq9Ahhm5vcTs4dMedAq211",
    "base": "https://gateway.thegraph.com/api/subgraphs/id/GQFbb95cE6d8mV989mL5figjaGaKCQB3xqYrr1bRyXqF",
    "avalanche": "https://gateway.thegraph.com/api/subgraphs/id/EZvK18pMhwiCjxwesRLTg81fP33WnR6BnZe5Cvma3H1C",
}

# Compound V3 Subgraph endpoints
COMPOUND_V3_SUBGRAPHS: dict[str, str] = {
    "ethereum": "https://gateway.thegraph.com/api/subgraphs/id/5nwMCSHaTqG3Kd2gHznbTXEnZ9QNWsssQfbHhDqQSQFp",
    "arbitrum": "https://gateway.thegraph.com/api/subgraphs/id/6JFDsgPx6mwY8H4RQZGP7BAR7S9bLjqp7AHZifk7g71K",
    "polygon": "https://gateway.thegraph.com/api/subgraphs/id/HCvC3BPS9F4gMpQeMwj8GXMy1PuCnhqWe1M9fuFuSo3A",
    "base": "https://gateway.thegraph.com/api/subgraphs/id/4E1fjskmZzuHqrkP16kHKxyp7hVdh5aGF33YUCCLxJ7y",
}

# Supported protocols
SUPPORTED_PROTOCOLS = ["aave_v3", "compound_v3"]

# Default cache TTL: 1 hour for historical data
DEFAULT_CACHE_TTL_SECONDS = 3600

# Rate limit settings
DEFAULT_REQUESTS_PER_MINUTE = 30
DEFAULT_REQUEST_TIMEOUT_SECONDS = 30


# =============================================================================
# Exceptions
# =============================================================================


class LendingAPYError(Exception):
    """Base exception for lending APY provider errors."""


class LendingAPYNotFoundError(LendingAPYError):
    """Raised when APY data is not found for a market."""

    def __init__(self, protocol: str, market: str, timestamp: datetime) -> None:
        self.protocol = protocol
        self.market = market
        self.timestamp = timestamp
        super().__init__(f"Lending APY not found for {protocol} {market} at {timestamp.isoformat()}")


class LendingAPYRateLimitError(LendingAPYError):
    """Raised when API rate limit is exceeded."""

    def __init__(self, retry_after_seconds: float | None = None) -> None:
        self.retry_after_seconds = retry_after_seconds
        msg = "Lending APY API rate limit exceeded"
        if retry_after_seconds:
            msg += f", retry after {retry_after_seconds}s"
        super().__init__(msg)


class UnsupportedProtocolError(LendingAPYError):
    """Raised when protocol is not supported."""

    def __init__(self, protocol: str) -> None:
        self.protocol = protocol
        super().__init__(f"Unsupported protocol: {protocol}. Supported: {SUPPORTED_PROTOCOLS}")


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class LendingAPYData:
    """APY data for a lending market at a specific time.

    Attributes:
        protocol: The lending protocol (aave_v3, compound_v3)
        market: The market/asset identifier (e.g., "USDC", "WETH")
        timestamp: The timestamp this rate applies to
        supply_apy: The supply APY as a decimal (0.03 = 3%)
        borrow_apy: The borrow APY as a decimal (0.05 = 5%)
        supply_apy_pct: Supply APY as percentage (3.0 = 3%)
        borrow_apy_pct: Borrow APY as percentage (5.0 = 5%)
        utilization_rate: Market utilization (0-1, if available)
        total_supply_usd: Total supplied in USD (if available)
        total_borrow_usd: Total borrowed in USD (if available)
        source: Data source (subgraph, api, fallback)
    """

    protocol: str
    market: str
    timestamp: datetime
    supply_apy: Decimal  # APY as decimal (0.03 = 3%)
    borrow_apy: Decimal  # APY as decimal (0.05 = 5%)
    supply_apy_pct: Decimal = Decimal("0")
    borrow_apy_pct: Decimal = Decimal("0")
    utilization_rate: Decimal | None = None
    total_supply_usd: Decimal | None = None
    total_borrow_usd: Decimal | None = None
    source: str = "subgraph"

    def __post_init__(self) -> None:
        """Calculate percentage APYs if not provided."""
        if self.supply_apy_pct == Decimal("0") and self.supply_apy != Decimal("0"):
            self.supply_apy_pct = self.supply_apy * Decimal("100")
        if self.borrow_apy_pct == Decimal("0") and self.borrow_apy != Decimal("0"):
            self.borrow_apy_pct = self.borrow_apy * Decimal("100")

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "protocol": self.protocol,
            "market": self.market,
            "timestamp": self.timestamp.isoformat(),
            "supply_apy": str(self.supply_apy),
            "borrow_apy": str(self.borrow_apy),
            "supply_apy_pct": str(self.supply_apy_pct),
            "borrow_apy_pct": str(self.borrow_apy_pct),
            "utilization_rate": str(self.utilization_rate) if self.utilization_rate else None,
            "total_supply_usd": str(self.total_supply_usd) if self.total_supply_usd else None,
            "total_borrow_usd": str(self.total_borrow_usd) if self.total_borrow_usd else None,
            "source": self.source,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "LendingAPYData":
        """Deserialize from dictionary."""
        return cls(
            protocol=data["protocol"],
            market=data["market"],
            timestamp=datetime.fromisoformat(data["timestamp"]),
            supply_apy=Decimal(data["supply_apy"]),
            borrow_apy=Decimal(data["borrow_apy"]),
            supply_apy_pct=Decimal(data.get("supply_apy_pct", "0")),
            borrow_apy_pct=Decimal(data.get("borrow_apy_pct", "0")),
            utilization_rate=Decimal(data["utilization_rate"]) if data.get("utilization_rate") else None,
            total_supply_usd=Decimal(data["total_supply_usd"]) if data.get("total_supply_usd") else None,
            total_borrow_usd=Decimal(data["total_borrow_usd"]) if data.get("total_borrow_usd") else None,
            source=data.get("source", "subgraph"),
        )


@dataclass
class CachedLendingAPY:
    """Cached lending APY data with expiration."""

    data: LendingAPYData
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

# Aave V3 reserve symbols to underlying token addresses (for subgraph queries)
# The subgraph indexes by underlying token address
AAVE_V3_MARKETS: dict[str, dict[str, str]] = {
    "ethereum": {
        "USDC": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
        "USDT": "0xdac17f958d2ee523a2206206994597c13d831ec7",
        "DAI": "0x6b175474e89094c44da98b954eedeac495271d0f",
        "WETH": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
        "WBTC": "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599",
        "LINK": "0x514910771af9ca656af840dff83e8264ecf986ca",
    },
    "arbitrum": {
        "USDC": "0xaf88d065e77c8cc2239327c5edb3a432268e5831",
        "USDC.e": "0xff970a61a04b1ca14834a43f5de4533ebddb5cc8",
        "USDT": "0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9",
        "DAI": "0xda10009cbd5d07dd0cecc66161fc93d7c9000da1",
        "WETH": "0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
        "WBTC": "0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f",
        "ARB": "0x912ce59144191c1204e64559fe8253a0e49e6548",
        "LINK": "0xf97f4df75117a78c1a5a0dbb814af92458539fb4",
    },
    "polygon": {
        "USDC": "0x3c499c542cef5e3811e1192ce70d8cc03d5c3359",
        "USDC.e": "0x2791bca1f2de4661ed88a30c99a7a9449aa84174",
        "USDT": "0xc2132d05d31c914a87c6611c10748aeb04b58e8f",
        "DAI": "0x8f3cf7ad23cd3cadbd9735aff958023239c6a063",
        "WETH": "0x7ceb23fd6bc0add59e62ac25578270cff1b9f619",
        "WBTC": "0x1bfd67037b42cf73acf2047067bd4f2c47d9bfd6",
        "MATIC": "0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270",
    },
    "base": {
        "USDC": "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",
        "WETH": "0x4200000000000000000000000000000000000006",
        "cbETH": "0x2ae3f1ec7f1f5012cfeab0185bfc7aa3cf0dec22",
    },
    "optimism": {
        "USDC": "0x0b2c639c533813f4aa9d7837caf62653d097ff85",
        "USDC.e": "0x7f5c764cbc14f9669b88837ca1490cca17c31607",
        "USDT": "0x94b008aa00579c1307b0ef2c499ad98a8ce58e58",
        "DAI": "0xda10009cbd5d07dd0cecc66161fc93d7c9000da1",
        "WETH": "0x4200000000000000000000000000000000000006",
        "WBTC": "0x68f180fcce6836688e9084f035309e29bf0a2095",
        "OP": "0x4200000000000000000000000000000000000042",
    },
    "avalanche": {
        "USDC": "0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e",
        "USDT": "0x9702230a8ea53601f5cd2dc00fdbc13d4df4a8c7",
        "DAI.e": "0xd586e7f844cea2f87f50152665bcbc2c279d8d70",
        "WAVAX": "0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7",
        "WETH.e": "0x49d5c2bdffac6ce2bfdb6640f4f80f226bc10bab",
        "WBTC.e": "0x50b7545627a5162f82a992c33b87adc75187b218",
    },
}

# Compound V3 comet addresses for different chains
COMPOUND_V3_MARKETS: dict[str, dict[str, str]] = {
    "ethereum": {
        "USDC": "0xc3d688b66703497daa19211eedff47f25384cdc3",  # cUSDCv3
        "WETH": "0xa17581a9e3356d9a858b789d68b4d866e593ae94",  # cWETHv3
    },
    "arbitrum": {
        "USDC": "0xa5edbdd9646f8dff606d7448e414884c7d905dca",  # cUSDCv3
        "USDC.e": "0x9c4ec768c28520b50860ea7a15bd7213a9ff58bf",  # cUSDCv3 bridged
    },
    "polygon": {
        "USDC": "0xf25212e676d1f7f89cd72ffee66158f541246445",  # cUSDCv3
    },
    "base": {
        "USDC": "0xb125e6687d4313864e53df431d5425969c15eb2f",  # cUSDCv3
        "WETH": "0x46e6b214b524310239732d51387075e0e70970bf",  # cWETHv3
    },
}

# Default APYs per protocol (as decimal, 0.03 = 3%)
DEFAULT_SUPPLY_APYS: dict[str, Decimal] = {
    "aave_v3": Decimal("0.03"),  # 3% supply
    "compound_v3": Decimal("0.025"),  # 2.5% supply
}

DEFAULT_BORROW_APYS: dict[str, Decimal] = {
    "aave_v3": Decimal("0.05"),  # 5% borrow
    "compound_v3": Decimal("0.045"),  # 4.5% borrow
}


# =============================================================================
# Lending APY Provider
# =============================================================================


class LendingAPYProvider:
    """Provider for fetching historical lending APY from protocol subgraphs.

    This provider supports fetching APY data from Aave V3 and Compound V3
    subgraphs. It implements caching with 1-hour TTL and handles rate limits
    gracefully.

    Attributes:
        chain: The blockchain for subgraph queries (ethereum, arbitrum, etc.)
        api_key: Optional API key for The Graph Gateway (recommended for production)
        cache_ttl_seconds: Cache TTL in seconds (default: 1 hour)
        request_timeout: HTTP request timeout in seconds
        requests_per_minute: Maximum requests per minute

    Example:
        provider = LendingAPYProvider()

        # Get historical APY
        apy = await provider.get_historical_apy(
            protocol="aave_v3",
            market="USDC",
            timestamp=datetime(2024, 1, 15, 12, 0, tzinfo=timezone.utc),
        )

        # Get current APY
        apy = await provider.get_current_apy(
            protocol="compound_v3",
            market="USDC",
        )
    """

    def __init__(
        self,
        chain: str = "ethereum",
        api_key: str | None = None,
        cache_ttl_seconds: float = DEFAULT_CACHE_TTL_SECONDS,
        request_timeout: float = DEFAULT_REQUEST_TIMEOUT_SECONDS,
        requests_per_minute: int = DEFAULT_REQUESTS_PER_MINUTE,
    ) -> None:
        """Initialize the lending APY provider.

        Args:
            chain: Blockchain for subgraph queries (ethereum, arbitrum, etc.)
            api_key: API key for The Graph Gateway (required for production)
            cache_ttl_seconds: Cache TTL in seconds (default: 3600 = 1 hour)
            request_timeout: HTTP request timeout in seconds
            requests_per_minute: Maximum requests per minute

        Raises:
            ValueError: If chain is not supported
        """
        chain_lower = chain.lower()
        supported_chains = set(AAVE_V3_SUBGRAPHS.keys()) | set(COMPOUND_V3_SUBGRAPHS.keys())
        if chain_lower not in supported_chains:
            raise ValueError(f"Unsupported chain: {chain}. Supported: {sorted(supported_chains)}")

        self._chain = chain_lower
        self._api_key = api_key
        self._cache_ttl_seconds = cache_ttl_seconds
        self._request_timeout = request_timeout
        self._requests_per_minute = requests_per_minute

        # Cache: (protocol, market, timestamp_hour) -> CachedLendingAPY
        self._cache: dict[tuple[str, str, datetime], CachedLendingAPY] = {}

        # Rate limit state per protocol
        self._rate_limit_states: dict[str, RateLimitState] = {
            "aave_v3": RateLimitState(),
            "compound_v3": RateLimitState(),
        }

        # HTTP session (lazy initialized)
        self._session: aiohttp.ClientSession | None = None

    @property
    def chain(self) -> str:
        """Get the chain this provider queries."""
        return self._chain

    @property
    def provider_name(self) -> str:
        """Get the provider name."""
        return f"lending_apy_{self._chain}"

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
        """Get cache key for an APY query."""
        return (protocol.lower(), market.upper(), self._normalize_timestamp(timestamp))

    def _get_from_cache(self, protocol: str, market: str, timestamp: datetime) -> LendingAPYData | None:
        """Try to get APY from cache."""
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

    def _add_to_cache(self, data: LendingAPYData) -> None:
        """Add APY data to cache."""
        key = self._get_cache_key(data.protocol, data.market, data.timestamp)
        self._cache[key] = CachedLendingAPY(
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

    async def get_historical_apy(
        self,
        protocol: str,
        market: str,
        timestamp: datetime,
    ) -> LendingAPYData:
        """Get historical APY for a market at a specific timestamp.

        Args:
            protocol: The lending protocol (aave_v3, compound_v3)
            market: The market identifier (e.g., "USDC", "WETH")
            timestamp: The timestamp to query APY for

        Returns:
            LendingAPYData with supply and borrow APY information

        Raises:
            UnsupportedProtocolError: If protocol is not supported
            LendingAPYNotFoundError: If data is not available for the query
            LendingAPYRateLimitError: If rate limit is exceeded
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

        # Fetch from appropriate subgraph
        try:
            if protocol_lower == "aave_v3":
                data = await self._fetch_aave_v3_apy(market, timestamp)
            elif protocol_lower == "compound_v3":
                data = await self._fetch_compound_v3_apy(market, timestamp)
            else:
                raise UnsupportedProtocolError(protocol)

            # Update data with correct protocol
            data = LendingAPYData(
                protocol=protocol_lower,
                market=data.market,
                timestamp=data.timestamp,
                supply_apy=data.supply_apy,
                borrow_apy=data.borrow_apy,
                supply_apy_pct=data.supply_apy_pct,
                borrow_apy_pct=data.borrow_apy_pct,
                utilization_rate=data.utilization_rate,
                total_supply_usd=data.total_supply_usd,
                total_borrow_usd=data.total_borrow_usd,
                source=data.source,
            )

            # Cache the result
            self._add_to_cache(data)

            # Record success
            state = self._rate_limit_states.get(protocol_lower)
            if state:
                state.record_success()
                state.record_request()

            logger.info(
                "Fetched APY for %s %s: supply=%.2f%%, borrow=%.2f%% (provider: %s)",
                protocol_lower,
                market,
                float(data.supply_apy_pct),
                float(data.borrow_apy_pct),
                self.provider_name,
            )

            return data

        except LendingAPYRateLimitError:
            state = self._rate_limit_states.get(protocol_lower)
            if state:
                state.record_rate_limit()
            raise

        except LendingAPYNotFoundError:
            # Fall back to default rate
            logger.warning(f"APY not found for {protocol} {market}, using default")
            return self._get_default_apy(protocol_lower, market, timestamp)

    async def get_current_apy(
        self,
        protocol: str,
        market: str,
    ) -> LendingAPYData:
        """Get current APY for a market.

        Convenience method that queries the current timestamp.

        Args:
            protocol: The lending protocol
            market: The market identifier

        Returns:
            LendingAPYData with the current APY rates
        """
        return await self.get_historical_apy(
            protocol=protocol,
            market=market,
            timestamp=datetime.now(UTC),
        )

    async def _fetch_aave_v3_apy(
        self,
        market: str,
        timestamp: datetime,
    ) -> LendingAPYData:
        """Fetch APY from Aave V3 subgraph.

        Aave V3 stores reserve data including liquidity rate (supply APY) and
        variable borrow rate (borrow APY) expressed as ray (1e27).

        Args:
            market: Market identifier (e.g., "USDC")
            timestamp: Timestamp to query

        Returns:
            LendingAPYData from Aave V3 subgraph

        Raises:
            LendingAPYNotFoundError: If market not found
            LendingAPYRateLimitError: If rate limited
        """
        # Get underlying token address
        markets = AAVE_V3_MARKETS.get(self._chain, {})
        underlying_address = markets.get(market.upper())
        if not underlying_address:
            raise LendingAPYNotFoundError("aave_v3", market, timestamp)

        subgraph_url = AAVE_V3_SUBGRAPHS.get(self._chain)
        if not subgraph_url:
            raise LendingAPYNotFoundError("aave_v3", market, timestamp)

        session = await self._get_session()

        # Query Aave V3 subgraph for reserve data
        # The subgraph stores historical rate snapshots via ReserveParamsHistoryItem
        timestamp_int = int(timestamp.timestamp())

        query = """
        query GetReserveAPY($underlyingAsset: Bytes!, $timestamp: Int!) {
            reserveParamsHistoryItems(
                where: {
                    reserve_: { underlyingAsset: $underlyingAsset }
                    timestamp_lte: $timestamp
                }
                orderBy: timestamp
                orderDirection: desc
                first: 1
            ) {
                reserve {
                    symbol
                    underlyingAsset
                }
                timestamp
                liquidityRate
                variableBorrowRate
                utilizationRate
                totalLiquidity
                totalCurrentVariableDebt
            }
        }
        """

        headers = {"Content-Type": "application/json"}
        if self._api_key:
            headers["Authorization"] = f"Bearer {self._api_key}"

        try:
            async with session.post(
                subgraph_url,
                json={
                    "query": query,
                    "variables": {
                        "underlyingAsset": underlying_address.lower(),
                        "timestamp": timestamp_int,
                    },
                },
                headers=headers,
            ) as response:
                if response.status == 429:
                    raise LendingAPYRateLimitError()

                if response.status != 200:
                    raise LendingAPYNotFoundError("aave_v3", market, timestamp)

                result = await response.json()

                if "errors" in result:
                    logger.warning(f"Aave V3 subgraph errors: {result['errors']}")
                    raise LendingAPYNotFoundError("aave_v3", market, timestamp)

                items = result.get("data", {}).get("reserveParamsHistoryItems", [])
                if not items:
                    raise LendingAPYNotFoundError("aave_v3", market, timestamp)

                item = items[0]

                # Aave stores rates as ray (1e27 precision)
                # Convert to decimal APY (0.05 = 5%)
                ray = Decimal("1000000000000000000000000000")  # 1e27
                liquidity_rate = Decimal(str(item.get("liquidityRate", "0")))
                variable_borrow_rate = Decimal(str(item.get("variableBorrowRate", "0")))

                supply_apy = liquidity_rate / ray
                borrow_apy = variable_borrow_rate / ray

                # Utilization rate is also in ray
                utilization = None
                if item.get("utilizationRate"):
                    utilization = Decimal(str(item["utilizationRate"])) / ray

                # Total supply/borrow in USD
                total_supply = None
                total_borrow = None
                if item.get("totalLiquidity"):
                    total_supply = Decimal(str(item["totalLiquidity"]))
                if item.get("totalCurrentVariableDebt"):
                    total_borrow = Decimal(str(item["totalCurrentVariableDebt"]))

                return LendingAPYData(
                    protocol="aave_v3",
                    market=market.upper(),
                    timestamp=self._normalize_timestamp(timestamp),
                    supply_apy=supply_apy,
                    borrow_apy=borrow_apy,
                    utilization_rate=utilization,
                    total_supply_usd=total_supply,
                    total_borrow_usd=total_borrow,
                    source="aave_v3_subgraph",
                )

        except aiohttp.ClientError as e:
            logger.warning(f"Aave V3 subgraph error: {e}")
            raise LendingAPYNotFoundError("aave_v3", market, timestamp) from e

    async def _fetch_compound_v3_apy(
        self,
        market: str,
        timestamp: datetime,
    ) -> LendingAPYData:
        """Fetch APY from Compound V3 subgraph.

        Compound V3 has a different model where there's a base rate plus
        supply/borrow rates that depend on utilization.

        Args:
            market: Market identifier (e.g., "USDC")
            timestamp: Timestamp to query

        Returns:
            LendingAPYData from Compound V3 subgraph

        Raises:
            LendingAPYNotFoundError: If market not found
            LendingAPYRateLimitError: If rate limited
        """
        # Get comet (market) address
        markets = COMPOUND_V3_MARKETS.get(self._chain, {})
        comet_address = markets.get(market.upper())
        if not comet_address:
            raise LendingAPYNotFoundError("compound_v3", market, timestamp)

        subgraph_url = COMPOUND_V3_SUBGRAPHS.get(self._chain)
        if not subgraph_url:
            raise LendingAPYNotFoundError("compound_v3", market, timestamp)

        session = await self._get_session()

        # Query Compound V3 subgraph
        # Compound V3 stores market data with supply/borrow rates
        timestamp_int = int(timestamp.timestamp())

        query = """
        query GetCometAPY($cometAddress: ID!, $timestamp: Int!) {
            marketHourlySnapshots(
                where: {
                    market: $cometAddress
                    timestamp_lte: $timestamp
                }
                orderBy: timestamp
                orderDirection: desc
                first: 1
            ) {
                market {
                    id
                    name
                }
                timestamp
                supplyAPY
                borrowAPY
                utilization
                totalSupply
                totalBorrow
            }
        }
        """

        headers = {"Content-Type": "application/json"}
        if self._api_key:
            headers["Authorization"] = f"Bearer {self._api_key}"

        try:
            async with session.post(
                subgraph_url,
                json={
                    "query": query,
                    "variables": {
                        "cometAddress": comet_address.lower(),
                        "timestamp": timestamp_int,
                    },
                },
                headers=headers,
            ) as response:
                if response.status == 429:
                    raise LendingAPYRateLimitError()

                if response.status != 200:
                    raise LendingAPYNotFoundError("compound_v3", market, timestamp)

                result = await response.json()

                if "errors" in result:
                    logger.warning(f"Compound V3 subgraph errors: {result['errors']}")
                    raise LendingAPYNotFoundError("compound_v3", market, timestamp)

                snapshots = result.get("data", {}).get("marketHourlySnapshots", [])
                if not snapshots:
                    raise LendingAPYNotFoundError("compound_v3", market, timestamp)

                snapshot = snapshots[0]

                # Compound V3 stores APY as decimals (already in 0.05 = 5% format)
                supply_apy = Decimal(str(snapshot.get("supplyAPY", "0")))
                borrow_apy = Decimal(str(snapshot.get("borrowAPY", "0")))

                # Utilization as decimal (0-1)
                utilization = None
                if snapshot.get("utilization"):
                    utilization = Decimal(str(snapshot["utilization"]))

                # Total supply/borrow
                total_supply = None
                total_borrow = None
                if snapshot.get("totalSupply"):
                    total_supply = Decimal(str(snapshot["totalSupply"]))
                if snapshot.get("totalBorrow"):
                    total_borrow = Decimal(str(snapshot["totalBorrow"]))

                return LendingAPYData(
                    protocol="compound_v3",
                    market=market.upper(),
                    timestamp=self._normalize_timestamp(timestamp),
                    supply_apy=supply_apy,
                    borrow_apy=borrow_apy,
                    utilization_rate=utilization,
                    total_supply_usd=total_supply,
                    total_borrow_usd=total_borrow,
                    source="compound_v3_subgraph",
                )

        except aiohttp.ClientError as e:
            logger.warning(f"Compound V3 subgraph error: {e}")
            raise LendingAPYNotFoundError("compound_v3", market, timestamp) from e

    def _get_default_apy(
        self,
        protocol: str,
        market: str,
        timestamp: datetime,
    ) -> LendingAPYData:
        """Get default APY when subgraph data is unavailable.

        Args:
            protocol: The protocol
            market: The market
            timestamp: The timestamp

        Returns:
            LendingAPYData with default rates
        """
        supply_apy = DEFAULT_SUPPLY_APYS.get(protocol, Decimal("0.03"))
        borrow_apy = DEFAULT_BORROW_APYS.get(protocol, Decimal("0.05"))

        return LendingAPYData(
            protocol=protocol,
            market=market.upper(),
            timestamp=self._normalize_timestamp(timestamp),
            supply_apy=supply_apy,
            borrow_apy=borrow_apy,
            source="fallback",
        )

    def get_default_supply_apy(self, protocol: str) -> Decimal:
        """Get the default supply APY for a protocol.

        Args:
            protocol: Protocol name

        Returns:
            Default supply APY
        """
        return DEFAULT_SUPPLY_APYS.get(protocol.lower(), Decimal("0.03"))

    def get_default_borrow_apy(self, protocol: str) -> Decimal:
        """Get the default borrow APY for a protocol.

        Args:
            protocol: Protocol name

        Returns:
            Default borrow APY
        """
        return DEFAULT_BORROW_APYS.get(protocol.lower(), Decimal("0.05"))

    def clear_cache(self) -> None:
        """Clear all cached APY data."""
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
    "LendingAPYProvider",
    # Data Classes
    "LendingAPYData",
    "CachedLendingAPY",
    "RateLimitState",
    # Exceptions
    "LendingAPYError",
    "LendingAPYNotFoundError",
    "LendingAPYRateLimitError",
    "UnsupportedProtocolError",
    # Constants
    "SUPPORTED_PROTOCOLS",
    "DEFAULT_SUPPLY_APYS",
    "DEFAULT_BORROW_APYS",
    "AAVE_V3_MARKETS",
    "COMPOUND_V3_MARKETS",
]
