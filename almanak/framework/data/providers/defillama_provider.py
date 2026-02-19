"""DeFi Llama Data Provider.

Provides TVL, yield, and historical price data from DeFi Llama's free public API.
Used as a fallback data source for OHLCV, pool data, and yield information.

Key Features:
    - Historical token prices via /prices/historical/{timestamp}/{coins}
    - Pool yield data via /yields/pools
    - Protocol TVL via /tvl/{protocol}
    - No API key required
    - Conservative 10 req/s self-imposed rate limit
    - Implements DataProvider protocol

DeFi Llama Coin ID Format:
    - "{chain}:{token_address}" (e.g., "arbitrum:0xaf88d065e77c8cC2239327C5EDb3A432268e5831")

Example:
    from almanak.framework.data.providers.defillama_provider import DefiLlamaProvider

    provider = DefiLlamaProvider()

    # Historical prices
    envelope = provider.fetch(
        endpoint="prices",
        token_address="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
        chain="arbitrum",
        timestamps=[1700000000, 1700003600],
    )

    # Yield data
    envelope = provider.fetch(endpoint="yields")

    # TVL
    envelope = provider.fetch(endpoint="tvl", protocol="uniswap")
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

import aiohttp

from almanak.framework.data.interfaces import DataSourceUnavailable, OHLCVCandle
from almanak.framework.data.models import (
    DataClassification,
    DataEnvelope,
    DataMeta,
)

logger = logging.getLogger(__name__)

# DeFi Llama API base URLs
_PRICES_API = "https://coins.llama.fi"
_YIELDS_API = "https://yields.llama.fi"
_TVL_API = "https://api.llama.fi"

# Chain name -> DeFi Llama chain prefix mapping
_CHAIN_TO_LLAMA: dict[str, str] = {
    "ethereum": "ethereum",
    "arbitrum": "arbitrum",
    "base": "base",
    "optimism": "optimism",
    "polygon": "polygon",
    "avalanche": "avax",
    "bsc": "bsc",
    "sonic": "sonic",
}


@dataclass
class _HealthMetrics:
    """Mutable health counters for the provider."""

    total_requests: int = 0
    successful_requests: int = 0
    cache_hits: int = 0
    errors: int = 0
    total_latency_ms: float = 0.0


class _TokenBucket:
    """Thread-safe token bucket rate limiter.

    Allows `rate` requests per `period` seconds using a token bucket algorithm.
    Tokens are refilled lazily on each call to `acquire()`.
    """

    def __init__(self, rate: int = 10, period: float = 1.0) -> None:
        self._rate = rate
        self._period = period
        self._tokens = float(rate)
        self._last_refill = time.monotonic()
        self._lock = threading.Lock()

    def acquire(self) -> bool:
        """Try to acquire a token. Returns True if allowed, False if rate limited."""
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_refill
            self._tokens = min(float(self._rate), self._tokens + elapsed * (self._rate / self._period))
            self._last_refill = now

            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return True
            return False


def to_llama_coin_id(token_address: str, chain: str) -> str:
    """Convert a token address and chain to DeFi Llama coin ID format.

    Args:
        token_address: Token contract address (e.g., "0xaf88d065...").
        chain: Chain name (e.g., "arbitrum", "ethereum").

    Returns:
        DeFi Llama coin ID (e.g., "arbitrum:0xaf88d065...").

    Raises:
        DataSourceUnavailable: If chain is not supported.
    """
    llama_chain = _CHAIN_TO_LLAMA.get(chain.lower())
    if llama_chain is None:
        raise DataSourceUnavailable(
            source="defillama",
            reason=f"Unsupported chain: {chain}. Supported: {', '.join(sorted(_CHAIN_TO_LLAMA))}",
        )
    return f"{llama_chain}:{token_address}"


@dataclass(frozen=True)
class LlamaPrice:
    """A historical price point from DeFi Llama.

    Attributes:
        price: Token price in USD.
        timestamp: UTC timestamp of the price.
        coin_id: DeFi Llama coin identifier.
        confidence: Price confidence (1.0 for DeFi Llama).
    """

    price: Decimal
    timestamp: datetime
    coin_id: str
    confidence: float = 1.0


@dataclass(frozen=True)
class LlamaYieldPool:
    """A yield pool entry from DeFi Llama yields API.

    Attributes:
        pool_id: DeFi Llama pool identifier.
        chain: Chain name.
        project: Protocol name (e.g., "uniswap-v3", "aave-v3").
        symbol: Pool symbol (e.g., "USDC-WETH").
        tvl_usd: Total value locked in USD.
        apy: Annual percentage yield.
        apy_base: Base APY (from fees/interest).
        apy_reward: Reward APY (from incentives).
        il_risk: Whether the pool has impermanent loss risk.
        exposure: Exposure type (e.g., "single", "multi").
    """

    pool_id: str
    chain: str
    project: str
    symbol: str
    tvl_usd: Decimal
    apy: float
    apy_base: float | None = None
    apy_reward: float | None = None
    il_risk: bool = False
    exposure: str | None = None


@dataclass(frozen=True)
class LlamaTvl:
    """Protocol TVL data from DeFi Llama.

    Attributes:
        protocol: Protocol name.
        tvl_usd: Current total value locked in USD.
        chain_tvls: TVL breakdown by chain.
    """

    protocol: str
    tvl_usd: Decimal
    chain_tvls: dict[str, Decimal] = field(default_factory=dict)


class DefiLlamaProvider:
    """DeFi Llama data provider for TVL, yield, and historical prices.

    Free public API with no API key required. Implements the DataProvider
    protocol with a conservative 10 req/s self-imposed rate limit.

    Supports three endpoints:
        - prices: Historical token prices
        - yields: Pool yield data across DeFi protocols
        - tvl: Protocol TVL data

    Attributes:
        name: Provider identifier ("defillama").
        data_class: INFORMATIONAL classification.
    """

    def __init__(
        self,
        cache_ttl: int = 300,
        request_timeout: float = 10.0,
        rate_limit: int = 10,
    ) -> None:
        """Initialize the DeFi Llama provider.

        Args:
            cache_ttl: Cache time-to-live in seconds. Default 300 (5 minutes).
            request_timeout: HTTP request timeout in seconds. Default 10.
            rate_limit: Maximum requests per second. Default 10.
        """
        self._cache_ttl = cache_ttl
        self._request_timeout = request_timeout
        self._rate_limiter = _TokenBucket(rate=rate_limit, period=1.0)
        self._metrics = _HealthMetrics()
        self._session: aiohttp.ClientSession | None = None
        self._cache: dict[str, tuple[Any, float]] = {}

        logger.info("Initialized DefiLlamaProvider (rate_limit=%d/s)", rate_limit)

    # -- DataProvider protocol --------------------------------------------------

    @property
    def name(self) -> str:
        """Unique provider identifier."""
        return "defillama"

    @property
    def data_class(self) -> DataClassification:
        """Classification: INFORMATIONAL (not execution-grade)."""
        return DataClassification.INFORMATIONAL

    def fetch(self, **kwargs: object) -> DataEnvelope:
        """Synchronous DataProvider entry point.

        Routes to the appropriate endpoint based on the 'endpoint' kwarg.

        Keyword Args:
            endpoint: One of "prices", "yields", "tvl" (str, default "prices").
            For prices:
                token_address: Token contract address (str).
                chain: Chain name (str, default "ethereum").
                timestamps: List of UNIX timestamps (list[int], optional).
                    If not provided, fetches current price.
            For yields:
                chain: Optional chain filter (str).
                project: Optional project filter (str).
            For tvl:
                protocol: Protocol name (str, required).

        Returns:
            DataEnvelope wrapping the result.
        """
        import asyncio

        endpoint = str(kwargs.get("endpoint", "prices"))

        start = time.monotonic()
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                import concurrent.futures

                with concurrent.futures.ThreadPoolExecutor() as pool:
                    result = pool.submit(
                        asyncio.run,
                        self._dispatch(endpoint, kwargs),
                    ).result()
            else:
                result = loop.run_until_complete(self._dispatch(endpoint, kwargs))
        except RuntimeError:
            result = asyncio.run(self._dispatch(endpoint, kwargs))

        latency_ms = int((time.monotonic() - start) * 1000)
        meta = DataMeta(
            source=self.name,
            observed_at=datetime.now(UTC),
            finality="off_chain",
            staleness_ms=0,
            latency_ms=latency_ms,
            confidence=0.85,
            cache_hit=False,
        )
        return DataEnvelope(value=result, meta=meta)

    def health(self) -> dict[str, object]:
        """Return health metrics for observability."""
        m = self._metrics
        success_rate = (m.successful_requests / m.total_requests * 100) if m.total_requests > 0 else 100.0
        avg_latency = (m.total_latency_ms / m.successful_requests) if m.successful_requests > 0 else 0.0
        return {
            "status": "healthy" if m.errors < m.total_requests * 0.5 or m.total_requests == 0 else "degraded",
            "total_requests": m.total_requests,
            "successful_requests": m.successful_requests,
            "cache_hits": m.cache_hits,
            "errors": m.errors,
            "success_rate": round(success_rate, 2),
            "average_latency_ms": round(avg_latency, 2),
        }

    # -- Endpoint dispatch -------------------------------------------------------

    async def _dispatch(self, endpoint: str, kwargs: dict[str, object]) -> Any:
        """Route to the correct async handler."""
        if endpoint == "prices":
            return await self.get_historical_prices(
                token_address=str(kwargs.get("token_address", "")),
                chain=str(kwargs.get("chain", "ethereum")),
                timestamps=kwargs.get("timestamps"),  # type: ignore[arg-type]
            )
        elif endpoint == "yields":
            return await self.get_yield_pools(
                chain=kwargs.get("chain"),  # type: ignore[arg-type]
                project=kwargs.get("project"),  # type: ignore[arg-type]
            )
        elif endpoint == "tvl":
            return await self.get_tvl(
                protocol=str(kwargs.get("protocol", "")),
            )
        else:
            raise DataSourceUnavailable(
                source="defillama",
                reason=f"Unknown endpoint: {endpoint}. Supported: prices, yields, tvl",
            )

    # -- Historical Prices -------------------------------------------------------

    async def get_historical_prices(
        self,
        token_address: str,
        chain: str = "ethereum",
        timestamps: list[int] | None = None,
    ) -> list[LlamaPrice]:
        """Fetch historical token prices from DeFi Llama.

        Uses the /prices/historical/{timestamp}/{coins} endpoint.
        If no timestamps provided, fetches current price via /prices/current/{coins}.

        Args:
            token_address: Token contract address.
            chain: Chain name (e.g., "arbitrum", "ethereum").
            timestamps: List of UNIX timestamps to fetch prices for.
                If None, fetches current price.

        Returns:
            List of LlamaPrice objects.

        Raises:
            DataSourceUnavailable: On API errors or rate limiting.
        """
        self._metrics.total_requests += 1
        coin_id = to_llama_coin_id(token_address, chain)

        # Check cache
        cache_key = f"prices:{coin_id}:{timestamps}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            self._metrics.cache_hits += 1
            self._metrics.successful_requests += 1
            return cached

        if not self._rate_limiter.acquire():
            self._metrics.errors += 1
            raise DataSourceUnavailable(
                source="defillama",
                reason="Rate limited (10 req/s)",
                retry_after=0.1,
            )

        results: list[LlamaPrice] = []
        start_time = time.monotonic()

        try:
            session = await self._get_session()

            if timestamps:
                for ts in timestamps:
                    url = f"{_PRICES_API}/prices/historical/{ts}/{coin_id}"
                    price = await self._fetch_price(session, url, coin_id, ts)
                    if price is not None:
                        results.append(price)
            else:
                url = f"{_PRICES_API}/prices/current/{coin_id}"
                price = await self._fetch_current_price(session, url, coin_id)
                if price is not None:
                    results.append(price)

            latency_ms = (time.monotonic() - start_time) * 1000

            if not results:
                self._metrics.errors += 1
                raise DataSourceUnavailable(
                    source="defillama",
                    reason=f"No price data returned for {coin_id}",
                )

            self._update_cache(cache_key, results)
            self._metrics.successful_requests += 1
            self._metrics.total_latency_ms += latency_ms

            logger.debug(
                "Fetched %d DeFi Llama prices for %s (latency: %.1fms)",
                len(results),
                coin_id,
                latency_ms,
            )

            return results

        except aiohttp.ClientError as e:
            self._metrics.errors += 1
            raise DataSourceUnavailable(
                source="defillama",
                reason=str(e),
            ) from e
        except TimeoutError:
            self._metrics.errors += 1
            raise DataSourceUnavailable(
                source="defillama",
                reason=f"Timeout after {self._request_timeout}s",
            ) from None

    async def _fetch_price(
        self,
        session: aiohttp.ClientSession,
        url: str,
        coin_id: str,
        timestamp: int,
    ) -> LlamaPrice | None:
        """Fetch a single historical price."""
        async with session.get(url) as response:
            if response.status != 200:
                logger.debug("DeFi Llama price fetch failed: HTTP %d for %s", response.status, url)
                return None

            data = await response.json()
            coins = data.get("coins", {})
            coin_data = coins.get(coin_id)
            if coin_data is None:
                return None

            return LlamaPrice(
                price=Decimal(str(coin_data["price"])),
                timestamp=datetime.fromtimestamp(coin_data.get("timestamp", timestamp), tz=UTC),
                coin_id=coin_id,
                confidence=coin_data.get("confidence", 1.0),
            )

    async def _fetch_current_price(
        self,
        session: aiohttp.ClientSession,
        url: str,
        coin_id: str,
    ) -> LlamaPrice | None:
        """Fetch the current price for a coin."""
        async with session.get(url) as response:
            if response.status != 200:
                logger.debug("DeFi Llama current price fetch failed: HTTP %d", response.status)
                return None

            data = await response.json()
            coins = data.get("coins", {})
            coin_data = coins.get(coin_id)
            if coin_data is None:
                return None

            return LlamaPrice(
                price=Decimal(str(coin_data["price"])),
                timestamp=datetime.fromtimestamp(coin_data.get("timestamp", time.time()), tz=UTC),
                coin_id=coin_id,
                confidence=coin_data.get("confidence", 1.0),
            )

    # -- Yield Pools --------------------------------------------------------------

    async def get_yield_pools(
        self,
        chain: str | None = None,
        project: str | None = None,
    ) -> list[LlamaYieldPool]:
        """Fetch yield pool data from DeFi Llama yields API.

        Uses the /pools endpoint, optionally filtered by chain and project.

        Args:
            chain: Optional chain filter (e.g., "arbitrum").
            project: Optional project filter (e.g., "uniswap-v3").

        Returns:
            List of LlamaYieldPool objects.

        Raises:
            DataSourceUnavailable: On API errors or rate limiting.
        """
        self._metrics.total_requests += 1

        cache_key = f"yields:{chain}:{project}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            self._metrics.cache_hits += 1
            self._metrics.successful_requests += 1
            return cached

        if not self._rate_limiter.acquire():
            self._metrics.errors += 1
            raise DataSourceUnavailable(
                source="defillama",
                reason="Rate limited (10 req/s)",
                retry_after=0.1,
            )

        url = f"{_YIELDS_API}/pools"
        start_time = time.monotonic()

        try:
            session = await self._get_session()
            async with session.get(url) as response:
                latency_ms = (time.monotonic() - start_time) * 1000

                if response.status != 200:
                    error_text = await response.text()
                    self._metrics.errors += 1
                    raise DataSourceUnavailable(
                        source="defillama",
                        reason=f"Yields API HTTP {response.status}: {error_text[:200]}",
                    )

                data = await response.json()
                pools = self._parse_yield_pools(data, chain=chain, project=project)

                self._update_cache(cache_key, pools)
                self._metrics.successful_requests += 1
                self._metrics.total_latency_ms += latency_ms

                logger.debug(
                    "Fetched %d DeFi Llama yield pools (chain=%s, project=%s, latency: %.1fms)",
                    len(pools),
                    chain,
                    project,
                    latency_ms,
                )

                return pools

        except aiohttp.ClientError as e:
            self._metrics.errors += 1
            raise DataSourceUnavailable(
                source="defillama",
                reason=str(e),
            ) from e
        except TimeoutError:
            self._metrics.errors += 1
            raise DataSourceUnavailable(
                source="defillama",
                reason=f"Timeout after {self._request_timeout}s",
            ) from None

    def _parse_yield_pools(
        self,
        data: dict[str, Any],
        chain: str | None = None,
        project: str | None = None,
    ) -> list[LlamaYieldPool]:
        """Parse DeFi Llama yields API response.

        Response format:
            {"status": "success", "data": [{"pool": "...", "chain": "...", ...}, ...]}
        """
        raw_pools = data.get("data", [])
        results: list[LlamaYieldPool] = []

        # Map our chain names to DeFi Llama chain names
        llama_chain = _CHAIN_TO_LLAMA.get(chain.lower()) if chain else None

        for pool in raw_pools:
            pool_chain = str(pool.get("chain", "")).lower()

            # Apply chain filter
            if llama_chain is not None and pool_chain != llama_chain.lower():
                continue

            # Apply project filter
            if project is not None and str(pool.get("project", "")).lower() != project.lower():
                continue

            try:
                results.append(
                    LlamaYieldPool(
                        pool_id=str(pool.get("pool", "")),
                        chain=pool_chain,
                        project=str(pool.get("project", "")),
                        symbol=str(pool.get("symbol", "")),
                        tvl_usd=Decimal(str(pool.get("tvlUsd", 0))),
                        apy=float(pool.get("apy", 0) or 0),
                        apy_base=pool.get("apyBase"),
                        apy_reward=pool.get("apyReward"),
                        il_risk=bool(pool.get("ilRisk", False)),
                        exposure=pool.get("exposure"),
                    )
                )
            except (ValueError, TypeError, InvalidOperation):
                logger.debug("Skipping malformed yield pool: %s", pool.get("pool"))
                continue

        return results

    # -- TVL ----------------------------------------------------------------------

    async def get_tvl(self, protocol: str) -> LlamaTvl:
        """Fetch TVL data for a protocol from DeFi Llama.

        Uses the /tvl/{protocol} endpoint for current TVL, and
        /protocol/{protocol} for chain-level breakdown.

        Args:
            protocol: Protocol slug (e.g., "uniswap", "aave-v3").

        Returns:
            LlamaTvl with current TVL and chain breakdown.

        Raises:
            DataSourceUnavailable: On API errors or rate limiting.
        """
        self._metrics.total_requests += 1

        if not protocol:
            self._metrics.errors += 1
            raise DataSourceUnavailable(
                source="defillama",
                reason="Protocol name is required for TVL endpoint",
            )

        cache_key = f"tvl:{protocol.lower()}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            self._metrics.cache_hits += 1
            self._metrics.successful_requests += 1
            return cached

        if not self._rate_limiter.acquire():
            self._metrics.errors += 1
            raise DataSourceUnavailable(
                source="defillama",
                reason="Rate limited (10 req/s)",
                retry_after=0.1,
            )

        start_time = time.monotonic()

        try:
            session = await self._get_session()

            # Fetch protocol details (includes chain breakdown)
            protocol_url = f"{_TVL_API}/protocol/{protocol}"
            async with session.get(protocol_url) as response:
                latency_ms = (time.monotonic() - start_time) * 1000

                if response.status != 200:
                    error_text = await response.text()
                    self._metrics.errors += 1
                    raise DataSourceUnavailable(
                        source="defillama",
                        reason=f"TVL API HTTP {response.status}: {error_text[:200]}",
                    )

                data = await response.json()
                result = self._parse_tvl(protocol, data)

                self._update_cache(cache_key, result)
                self._metrics.successful_requests += 1
                self._metrics.total_latency_ms += latency_ms

                logger.debug(
                    "Fetched DeFi Llama TVL for %s: $%.2f (latency: %.1fms)",
                    protocol,
                    result.tvl_usd,
                    latency_ms,
                )

                return result

        except aiohttp.ClientError as e:
            self._metrics.errors += 1
            raise DataSourceUnavailable(
                source="defillama",
                reason=str(e),
            ) from e
        except TimeoutError:
            self._metrics.errors += 1
            raise DataSourceUnavailable(
                source="defillama",
                reason=f"Timeout after {self._request_timeout}s",
            ) from None

    def _parse_tvl(self, protocol: str, data: dict[str, Any]) -> LlamaTvl:
        """Parse DeFi Llama protocol TVL response.

        Response format:
            {
                "name": "Uniswap V3",
                "currentChainTvls": {"Ethereum": 1234567890, "Arbitrum": 987654321, ...},
                ...
            }
        """
        current_tvl = Decimal(str(data.get("currentChainTvls", {}).get("total", 0) or 0))

        # If no "total", sum all chain TVLs (excluding staking/borrowed variants)
        chain_tvls_raw = data.get("currentChainTvls", {})
        chain_tvls: dict[str, Decimal] = {}

        if current_tvl == 0:
            total = Decimal(0)
            for chain_name, tvl in chain_tvls_raw.items():
                # Skip derived categories like "Ethereum-staking", "Arbitrum-borrowed"
                if "-" in chain_name:
                    continue
                chain_val = Decimal(str(tvl or 0))
                chain_tvls[chain_name.lower()] = chain_val
                total += chain_val
            current_tvl = total
        else:
            for chain_name, tvl in chain_tvls_raw.items():
                if "-" in chain_name and chain_name != "total":
                    continue
                if chain_name == "total":
                    continue
                chain_tvls[chain_name.lower()] = Decimal(str(tvl or 0))

        return LlamaTvl(
            protocol=protocol,
            tvl_usd=current_tvl,
            chain_tvls=chain_tvls,
        )

    # -- OHLCV convenience (for use as OHLCV fallback) ---------------------------

    async def get_ohlcv_from_prices(
        self,
        token_address: str,
        chain: str = "ethereum",
        timestamps: list[int] | None = None,
    ) -> list[OHLCVCandle]:
        """Build pseudo-OHLCV candles from DeFi Llama historical prices.

        DeFi Llama doesn't provide native OHLCV, so this creates single-price
        candles (open=high=low=close=price, volume=None) from historical data.
        Useful as a fallback when DEX-native OHLCV sources are unavailable.

        Args:
            token_address: Token contract address.
            chain: Chain name.
            timestamps: List of UNIX timestamps.

        Returns:
            List of OHLCVCandle with price-only data (no volume).
        """
        prices = await self.get_historical_prices(
            token_address=token_address,
            chain=chain,
            timestamps=timestamps,
        )

        candles: list[OHLCVCandle] = []
        for p in prices:
            candles.append(
                OHLCVCandle(
                    timestamp=p.timestamp,
                    open=p.price,
                    high=p.price,
                    low=p.price,
                    close=p.price,
                    volume=None,
                )
            )

        candles.sort(key=lambda c: c.timestamp)
        return candles

    # -- Internal helpers -------------------------------------------------------

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create HTTP session."""
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=self._request_timeout)
            self._session = aiohttp.ClientSession(
                timeout=timeout,
                headers={"Accept": "application/json"},
            )
        return self._session

    async def close(self) -> None:
        """Close the HTTP session."""
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    def _get_cached(self, key: str) -> Any | None:
        """Return cached value if fresh, else None."""
        entry = self._cache.get(key)
        if entry is None:
            return None
        value, cached_at = entry
        if time.monotonic() - cached_at > self._cache_ttl:
            return None
        return value

    def _update_cache(self, key: str, value: Any) -> None:
        """Store value in the in-memory cache."""
        self._cache[key] = (value, time.monotonic())

    def clear_cache(self) -> None:
        """Clear the data cache."""
        self._cache.clear()
        logger.info("Cleared DeFi Llama cache")

    async def __aenter__(self) -> DefiLlamaProvider:
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit."""
        await self.close()


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    "DefiLlamaProvider",
    "LlamaPrice",
    "LlamaYieldPool",
    "LlamaTvl",
    "to_llama_coin_id",
]
