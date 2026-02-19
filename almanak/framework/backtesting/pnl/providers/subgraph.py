"""Uniswap V3 Subgraph Provider for historical pool volume data.

This module provides a client for fetching historical pool volume data
from The Graph's Uniswap V3 subgraphs. Volume data is essential for
accurate LP fee calculation in backtesting.

Key Features:
    - Fetches historical daily volume from poolDayDatas
    - Supports multiple chains via chain-specific subgraph endpoints
    - Implements caching with 1-hour TTL
    - Handles subgraph rate limits gracefully

Example:
    from almanak.framework.backtesting.pnl.providers.subgraph import (
        SubgraphVolumeProvider,
        PoolVolumeData,
    )
    from datetime import date

    provider = SubgraphVolumeProvider(chain="arbitrum")

    # Get volume for a specific pool and date
    volume = await provider.get_pool_volume(
        pool_address="0xC31E54c7a869B9FcBEcc14363CF510d1c41fa443",
        date=date(2024, 1, 15),
    )
    print(f"Volume: ${volume.volume_usd}")
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any

import aiohttp

logger = logging.getLogger(__name__)


# =============================================================================
# Subgraph Endpoints
# =============================================================================

# The Graph hosted service and decentralized network endpoints
UNISWAP_V3_SUBGRAPHS: dict[str, str] = {
    "ethereum": "https://gateway.thegraph.com/api/subgraphs/id/5zvR82QoaXYFyDEKLZ9t6v9adgnptxYpKpSbxtgVENFV",
    "arbitrum": "https://gateway.thegraph.com/api/subgraphs/id/FbCGRftH4a3yZugY7TnbYgPJVEv2LvMT6oF1fxPe9aJM",
    "base": "https://gateway.thegraph.com/api/subgraphs/id/43Hwfi3dJSoGpyas9VwNoDAv28rqtbnqUk3EYCRr3j6i",
    "optimism": "https://gateway.thegraph.com/api/subgraphs/id/Gc2DPCVq5UkBfyHjZDMbKTc7ynrjoSKxc6sHLKY9Pmjc",
    "polygon": "https://gateway.thegraph.com/api/subgraphs/id/3hCPRGf4z88VC5rsBKU5AA9FBBq5nF3jbKJG7VZCbhjm",
}

# Backup hosted service endpoints (free, no API key required, may be deprecated)
UNISWAP_V3_HOSTED_SUBGRAPHS: dict[str, str] = {
    "ethereum": "https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3",
    "arbitrum": "https://api.thegraph.com/subgraphs/name/ianlapham/uniswap-arbitrum-one",
    "optimism": "https://api.thegraph.com/subgraphs/name/ianlapham/optimism-post-regenesis",
    "polygon": "https://api.thegraph.com/subgraphs/name/ianlapham/uniswap-v3-polygon",
}

# Supported chains
SUPPORTED_CHAINS = list(UNISWAP_V3_SUBGRAPHS.keys())

# Default cache TTL: 1 hour for historical data
DEFAULT_CACHE_TTL_SECONDS = 3600

# Rate limit settings
DEFAULT_REQUESTS_PER_MINUTE = 30
DEFAULT_REQUEST_TIMEOUT_SECONDS = 30


# =============================================================================
# Exceptions
# =============================================================================


class SubgraphError(Exception):
    """Base exception for subgraph errors."""


class SubgraphRateLimitError(SubgraphError):
    """Raised when subgraph rate limit is exceeded."""

    def __init__(self, retry_after_seconds: float | None = None) -> None:
        self.retry_after_seconds = retry_after_seconds
        msg = "Subgraph rate limit exceeded"
        if retry_after_seconds:
            msg += f", retry after {retry_after_seconds}s"
        super().__init__(msg)


class SubgraphQueryError(SubgraphError):
    """Raised when subgraph query fails."""

    def __init__(self, query: str, errors: list[dict[str, Any]]) -> None:
        self.query = query
        self.errors = errors
        error_msgs = [e.get("message", str(e)) for e in errors]
        super().__init__(f"Subgraph query failed: {'; '.join(error_msgs)}")


class PoolNotFoundError(SubgraphError):
    """Raised when pool is not found in subgraph."""

    def __init__(self, pool_address: str, chain: str) -> None:
        self.pool_address = pool_address
        self.chain = chain
        super().__init__(f"Pool {pool_address} not found on {chain}")


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class PoolVolumeData:
    """Volume data for a pool on a specific date.

    Attributes:
        pool_address: The pool contract address
        date: The date for this volume data
        volume_usd: Total volume in USD for the day
        volume_token0: Volume of token0 traded
        volume_token1: Volume of token1 traded
        fees_usd: Total fees collected in USD
        tvl_usd: Total value locked at end of day
        liquidity: Pool liquidity at end of day
        token0_price: Token0 price in USD
        token1_price: Token1 price in USD
    """

    pool_address: str
    date: date
    volume_usd: Decimal
    volume_token0: Decimal = Decimal("0")
    volume_token1: Decimal = Decimal("0")
    fees_usd: Decimal = Decimal("0")
    tvl_usd: Decimal = Decimal("0")
    liquidity: int = 0
    token0_price: Decimal = Decimal("0")
    token1_price: Decimal = Decimal("0")


@dataclass
class CachedVolume:
    """Cached volume data with expiration."""

    data: PoolVolumeData
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
# Subgraph Volume Provider
# =============================================================================


class SubgraphVolumeProvider:
    """Provider for fetching historical pool volume from Uniswap V3 subgraphs.

    Attributes:
        chain: The blockchain to query (ethereum, arbitrum, base, etc.)
        api_key: Optional API key for The Graph Gateway (recommended for production)
        cache_ttl_seconds: Cache TTL in seconds (default: 1 hour)
        request_timeout: HTTP request timeout in seconds
        requests_per_minute: Maximum requests per minute

    Example:
        provider = SubgraphVolumeProvider(chain="arbitrum")

        # Get volume for a specific date
        volume = await provider.get_pool_volume(
            "0xC31E54c7a869B9FcBEcc14363CF510d1c41fa443",
            date(2024, 1, 15),
        )

        # Get volume range
        volumes = await provider.get_pool_volume_range(
            "0xC31E54c7a869B9FcBEcc14363CF510d1c41fa443",
            date(2024, 1, 1),
            date(2024, 1, 31),
        )
    """

    def __init__(
        self,
        chain: str = "arbitrum",
        api_key: str | None = None,
        cache_ttl_seconds: float = DEFAULT_CACHE_TTL_SECONDS,
        request_timeout: float = DEFAULT_REQUEST_TIMEOUT_SECONDS,
        requests_per_minute: int = DEFAULT_REQUESTS_PER_MINUTE,
        use_hosted_service: bool = False,
    ) -> None:
        """Initialize the subgraph volume provider.

        Args:
            chain: Blockchain to query (ethereum, arbitrum, base, optimism, polygon)
            api_key: API key for The Graph Gateway (required for gateway endpoints)
            cache_ttl_seconds: Cache TTL in seconds (default: 3600 = 1 hour)
            request_timeout: HTTP request timeout in seconds
            requests_per_minute: Maximum requests per minute
            use_hosted_service: Use hosted service instead of gateway (no API key required)

        Raises:
            ValueError: If chain is not supported
        """
        chain_lower = chain.lower()
        if chain_lower not in SUPPORTED_CHAINS:
            raise ValueError(f"Unsupported chain: {chain}. Supported chains: {SUPPORTED_CHAINS}")

        self._chain = chain_lower
        self._api_key = api_key
        self._cache_ttl_seconds = cache_ttl_seconds
        self._request_timeout = request_timeout
        self._requests_per_minute = requests_per_minute
        self._use_hosted_service = use_hosted_service

        # Cache: (pool_address, date) -> CachedVolume
        self._cache: dict[tuple[str, date], CachedVolume] = {}

        # Rate limit state
        self._rate_limit_state = RateLimitState()

        # HTTP session (lazy initialized)
        self._session: aiohttp.ClientSession | None = None

    @property
    def chain(self) -> str:
        """Get the chain this provider queries."""
        return self._chain

    @property
    def provider_name(self) -> str:
        """Get the provider name."""
        return f"subgraph_{self._chain}"

    @property
    def subgraph_url(self) -> str:
        """Get the subgraph URL for the current chain."""
        if self._use_hosted_service:
            return UNISWAP_V3_HOSTED_SUBGRAPHS.get(
                self._chain,
                UNISWAP_V3_SUBGRAPHS[self._chain],
            )
        return UNISWAP_V3_SUBGRAPHS[self._chain]

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create HTTP session."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self._request_timeout))
        return self._session

    async def close(self) -> None:
        """Close the HTTP session."""
        if self._session is not None and not self._session.closed:
            await self._session.close()
            self._session = None

    async def _wait_for_rate_limit(self) -> None:
        """Wait if necessary due to rate limiting."""
        wait_time = self._rate_limit_state.get_wait_time()
        if wait_time > 0:
            logger.debug(f"Rate limit backoff: waiting {wait_time:.2f}s")
            await asyncio.sleep(wait_time)

        # Check requests per minute
        if self._rate_limit_state.requests_this_minute >= self._requests_per_minute:
            # Wait until next minute
            time_until_reset = 60 - (time.time() - self._rate_limit_state.minute_start)
            if time_until_reset > 0:
                logger.debug(f"Rate limit: waiting {time_until_reset:.2f}s for minute reset")
                await asyncio.sleep(time_until_reset)

    async def _execute_query(
        self,
        query: str,
        variables: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Execute a GraphQL query against the subgraph.

        Args:
            query: GraphQL query string
            variables: Optional query variables

        Returns:
            Query response data

        Raises:
            SubgraphRateLimitError: If rate limit is exceeded
            SubgraphQueryError: If query returns errors
        """
        await self._wait_for_rate_limit()
        self._rate_limit_state.record_request()

        session = await self._get_session()
        url = self.subgraph_url

        headers = {"Content-Type": "application/json"}
        if self._api_key and not self._use_hosted_service:
            headers["Authorization"] = f"Bearer {self._api_key}"

        payload: dict[str, Any] = {"query": query}
        if variables:
            payload["variables"] = variables

        try:
            async with session.post(url, json=payload, headers=headers) as response:
                if response.status == 429:
                    # Rate limited
                    self._rate_limit_state.record_rate_limit()
                    retry_after = response.headers.get("Retry-After")
                    retry_seconds = float(retry_after) if retry_after else None
                    logger.warning(
                        f"Subgraph rate limit hit on {self._chain}, backoff: {self._rate_limit_state.backoff_seconds}s"
                    )
                    raise SubgraphRateLimitError(retry_after_seconds=retry_seconds)

                if response.status != 200:
                    error_text = await response.text()
                    logger.error(f"Subgraph request failed: status={response.status}, body={error_text[:500]}")
                    raise SubgraphQueryError(query, [{"message": f"HTTP {response.status}: {error_text}"}])

                data = await response.json()

                if "errors" in data and data["errors"]:
                    raise SubgraphQueryError(query, data["errors"])

                self._rate_limit_state.record_success()
                return data.get("data", {})

        except aiohttp.ClientError as e:
            logger.error(f"Subgraph HTTP error: {e}")
            raise SubgraphQueryError(query, [{"message": str(e)}]) from e

    async def get_pool_volume(
        self,
        pool_address: str,
        target_date: date,
    ) -> PoolVolumeData | None:
        """Get volume data for a specific pool and date.

        Args:
            pool_address: The pool contract address (lowercase hex)
            target_date: The date to get volume for

        Returns:
            PoolVolumeData if found, None otherwise
        """
        pool_address_lower = pool_address.lower()
        cache_key = (pool_address_lower, target_date)

        # Check cache
        if cache_key in self._cache:
            cached = self._cache[cache_key]
            if not cached.is_expired:
                logger.debug(f"Cache hit for pool {pool_address_lower[:10]}... on {target_date}")
                return cached.data

        # Query subgraph using poolDayDatas
        # Convert date to Unix timestamp (start of day)
        day_start_timestamp = int(datetime.combine(target_date, datetime.min.time(), tzinfo=UTC).timestamp())

        query = """
        query GetPoolDayData($poolAddress: String!, $dateTimestamp: Int!) {
            poolDayDatas(
                first: 1
                where: {
                    pool: $poolAddress
                    date: $dateTimestamp
                }
            ) {
                id
                date
                volumeUSD
                volumeToken0
                volumeToken1
                feesUSD
                tvlUSD
                liquidity
                token0Price
                token1Price
            }
        }
        """

        variables = {
            "poolAddress": pool_address_lower,
            "dateTimestamp": day_start_timestamp,
        }

        try:
            data = await self._execute_query(query, variables)
            pool_day_datas = data.get("poolDayDatas", [])

            if not pool_day_datas:
                logger.debug(f"No volume data for pool {pool_address_lower[:10]}... on {target_date}")
                return None

            day_data = pool_day_datas[0]

            volume_data = PoolVolumeData(
                pool_address=pool_address_lower,
                date=target_date,
                volume_usd=Decimal(str(day_data.get("volumeUSD", "0"))),
                volume_token0=Decimal(str(day_data.get("volumeToken0", "0"))),
                volume_token1=Decimal(str(day_data.get("volumeToken1", "0"))),
                fees_usd=Decimal(str(day_data.get("feesUSD", "0"))),
                tvl_usd=Decimal(str(day_data.get("tvlUSD", "0"))),
                liquidity=int(day_data.get("liquidity", 0)),
                token0_price=Decimal(str(day_data.get("token0Price", "0"))),
                token1_price=Decimal(str(day_data.get("token1Price", "0"))),
            )

            # Cache the result
            self._cache[cache_key] = CachedVolume(
                data=volume_data,
                fetched_at=time.time(),
                ttl_seconds=self._cache_ttl_seconds,
            )

            logger.info(
                f"Fetched volume for pool {pool_address_lower[:10]}... on {target_date}: "
                f"${volume_data.volume_usd:,.2f} (provider: {self.provider_name})"
            )

            return volume_data

        except SubgraphRateLimitError:
            # Re-raise for caller to handle retry
            raise
        except SubgraphQueryError as e:
            logger.error(f"Failed to fetch volume data: {e}")
            return None

    async def get_pool_volume_range(
        self,
        pool_address: str,
        start_date: date,
        end_date: date,
    ) -> list[PoolVolumeData]:
        """Get volume data for a date range.

        Args:
            pool_address: The pool contract address
            start_date: Start date (inclusive)
            end_date: End date (inclusive)

        Returns:
            List of PoolVolumeData for each day with data
        """
        pool_address_lower = pool_address.lower()

        # Convert dates to Unix timestamps
        start_timestamp = int(datetime.combine(start_date, datetime.min.time(), tzinfo=UTC).timestamp())
        end_timestamp = int(datetime.combine(end_date, datetime.min.time(), tzinfo=UTC).timestamp())

        # First check cache for any missing dates
        results: list[PoolVolumeData] = []
        dates_to_fetch: list[date] = []

        current = start_date
        while current <= end_date:
            cache_key = (pool_address_lower, current)
            if cache_key in self._cache and not self._cache[cache_key].is_expired:
                results.append(self._cache[cache_key].data)
            else:
                dates_to_fetch.append(current)
            current += timedelta(days=1)

        # If all dates are cached, return
        if not dates_to_fetch:
            logger.debug(f"All dates cached for pool {pool_address_lower[:10]}...")
            return sorted(results, key=lambda x: x.date)

        # Fetch missing dates in batch
        query = """
        query GetPoolDayDatas($poolAddress: String!, $startDate: Int!, $endDate: Int!) {
            poolDayDatas(
                first: 1000
                where: {
                    pool: $poolAddress
                    date_gte: $startDate
                    date_lte: $endDate
                }
                orderBy: date
                orderDirection: asc
            ) {
                id
                date
                volumeUSD
                volumeToken0
                volumeToken1
                feesUSD
                tvlUSD
                liquidity
                token0Price
                token1Price
            }
        }
        """

        variables = {
            "poolAddress": pool_address_lower,
            "startDate": start_timestamp,
            "endDate": end_timestamp,
        }

        try:
            data = await self._execute_query(query, variables)
            pool_day_datas = data.get("poolDayDatas", [])

            for day_data in pool_day_datas:
                # Convert timestamp back to date
                day_timestamp = int(day_data.get("date", 0))
                day_date = datetime.fromtimestamp(day_timestamp, tz=UTC).date()

                volume_data = PoolVolumeData(
                    pool_address=pool_address_lower,
                    date=day_date,
                    volume_usd=Decimal(str(day_data.get("volumeUSD", "0"))),
                    volume_token0=Decimal(str(day_data.get("volumeToken0", "0"))),
                    volume_token1=Decimal(str(day_data.get("volumeToken1", "0"))),
                    fees_usd=Decimal(str(day_data.get("feesUSD", "0"))),
                    tvl_usd=Decimal(str(day_data.get("tvlUSD", "0"))),
                    liquidity=int(day_data.get("liquidity", 0)),
                    token0_price=Decimal(str(day_data.get("token0Price", "0"))),
                    token1_price=Decimal(str(day_data.get("token1Price", "0"))),
                )

                # Cache the result
                cache_key = (pool_address_lower, day_date)
                self._cache[cache_key] = CachedVolume(
                    data=volume_data,
                    fetched_at=time.time(),
                    ttl_seconds=self._cache_ttl_seconds,
                )

                results.append(volume_data)

            logger.info(
                f"Fetched {len(pool_day_datas)} days of volume data for pool "
                f"{pool_address_lower[:10]}... ({start_date} to {end_date})"
            )

            return sorted(results, key=lambda x: x.date)

        except SubgraphRateLimitError:
            # Re-raise for caller to handle retry
            raise
        except SubgraphQueryError as e:
            logger.error(f"Failed to fetch volume range: {e}")
            return results  # Return whatever we have cached

    def clear_cache(self) -> None:
        """Clear the volume data cache."""
        self._cache.clear()
        logger.debug(f"Cleared subgraph volume cache for {self._chain}")

    def get_cache_stats(self) -> dict[str, Any]:
        """Get cache statistics.

        Returns:
            Dictionary with cache statistics
        """
        total = len(self._cache)
        expired = sum(1 for c in self._cache.values() if c.is_expired)
        return {
            "total_entries": total,
            "expired_entries": expired,
            "valid_entries": total - expired,
            "cache_ttl_seconds": self._cache_ttl_seconds,
        }

    async def warm_cache(
        self,
        pool_addresses: list[str],
        start_date: date,
        end_date: date,
    ) -> dict[str, int]:
        """Pre-fetch volume data for multiple pools.

        Useful for warming the cache before a backtest to minimize
        API calls during the backtest run.

        Args:
            pool_addresses: List of pool addresses to warm
            start_date: Start date for data range
            end_date: End date for data range

        Returns:
            Dictionary mapping pool address to number of days fetched
        """
        results: dict[str, int] = {}

        for pool_address in pool_addresses:
            try:
                volumes = await self.get_pool_volume_range(pool_address, start_date, end_date)
                results[pool_address] = len(volumes)
            except SubgraphRateLimitError:
                # Wait and continue with remaining pools
                wait_time = self._rate_limit_state.get_wait_time()
                if wait_time > 0:
                    await asyncio.sleep(wait_time)
                # Try this pool again
                try:
                    volumes = await self.get_pool_volume_range(pool_address, start_date, end_date)
                    results[pool_address] = len(volumes)
                except Exception as e:
                    logger.warning(f"Failed to warm cache for pool {pool_address}: {e}")
                    results[pool_address] = 0
            except Exception as e:
                logger.warning(f"Failed to warm cache for pool {pool_address}: {e}")
                results[pool_address] = 0

        total_days = sum(results.values())
        logger.info(f"Cache warmed with {total_days} days of volume data for {len(pool_addresses)} pools")

        return results
