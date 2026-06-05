"""CoinGecko Onchain OHLCV Data Provider.

Provides DEX-native OHLCV candlestick data from CoinGecko's Onchain API.
Primary data source for DeFi pairs where on-chain DEX trade data is preferred
over CEX reference prices.

Key Features:
    - DEX-native price data from actual on-chain trades
    - Supported timeframes: 1m, 5m, 15m, 1h, 4h, 1d
    - Rate limiting: 30 req/min with built-in token bucket
    - Requires a CoinGecko Pro API key for Onchain endpoints
    - Implements both OHLCVProvider and DataProvider protocols

Example:
    from almanak.gateway.data.ohlcv.geckoterminal_provider import GeckoTerminalOHLCVProvider

    provider = GeckoTerminalOHLCVProvider()
    candles = await provider.get_ohlcv("WETH", timeframe="1h", limit=100)

    # Or via DataProvider protocol:
    envelope = provider.fetch(token="WETH", timeframe="1h", limit=100)
    candles = envelope.value
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from types import MappingProxyType
from typing import Any

import aiohttp

from almanak.core.chains._helpers import vendor_chain_map
from almanak.framework.data.interfaces import (
    DataSourceUnavailable,
    OHLCVCandle,
    validate_timeframe,
)
from almanak.framework.data.models import (
    DataClassification,
    DataEnvelope,
    DataMeta,
)
from almanak.gateway.utils.rpc_provider import _get_gateway_api_key

logger = logging.getLogger(__name__)

# CoinGecko Onchain API base URLs. The Onchain DEX endpoints share the
# GeckoTerminal backend, but the requested migration is to route through
# CoinGecko's API host.
_FREE_API_BASE = "https://api.coingecko.com/api/v3/onchain"
_PRO_API_BASE = "https://pro-api.coingecko.com/api/v3/onchain"
_SOURCE = "coingecko_onchain"

# Chain name -> CoinGecko Onchain network ID mapping. CoinGecko's onchain
# network ids are the same ids previously used by GeckoTerminal.
_CHAIN_TO_NETWORK: Mapping[str, str] = MappingProxyType(vendor_chain_map("geckoterminal"))

# CoinGecko Onchain timeframe -> API parameter mapping. The endpoint uses day,
# hour, minute path segments plus an aggregate query param.
_TIMEFRAME_TO_GT: dict[str, dict[str, str]] = {
    "1m": {"aggregate": "1", "timeframe": "minute"},
    "5m": {"aggregate": "5", "timeframe": "minute"},
    "15m": {"aggregate": "15", "timeframe": "minute"},
    "1h": {"aggregate": "1", "timeframe": "hour"},
    "4h": {"aggregate": "4", "timeframe": "hour"},
    "1d": {"aggregate": "1", "timeframe": "day"},
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

    def __init__(self, rate: int = 30, period: float = 60.0) -> None:
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


class GeckoTerminalOHLCVProvider:
    """Legacy-named CoinGecko Onchain OHLCV provider for DEX-native candle data.

    Fetches OHLCV data from CoinGecko's Onchain API. This provider returns
    data based on actual DEX trades, making it the preferred source for
    DeFi-native pairs. The class name remains for compatibility.

    Implements both the OHLCVProvider and DataProvider protocols.

    Attributes:
        name: Provider identifier ("geckoterminal").
        data_class: INFORMATIONAL classification.
    """

    SUPPORTED_TIMEFRAMES: list[str] = ["1m", "5m", "15m", "1h", "4h", "1d"]

    def __init__(
        self,
        cache_ttl: int = 60,
        request_timeout: float = 10.0,
        rate_limit: int = 30,
        api_key: str | None = None,
    ) -> None:
        """Initialize the CoinGecko Onchain OHLCV provider.

        Args:
            cache_ttl: Cache time-to-live in seconds. Default 60.
            request_timeout: HTTP request timeout in seconds. Default 10.
            rate_limit: Maximum requests per minute. Default 30.
            api_key: CoinGecko Pro API key. Uses the gateway environment
                fallback when omitted.
        """
        self._cache_ttl = cache_ttl
        self._request_timeout = request_timeout
        self._rate_limiter = _TokenBucket(rate=rate_limit, period=60.0)
        self._metrics = _HealthMetrics()
        self._session: aiohttp.ClientSession | None = None
        self._cache: dict[str, tuple[list[OHLCVCandle], float]] = {}
        self._api_key = api_key if api_key is not None else _get_gateway_api_key("COINGECKO_API_KEY")

        logger.info(
            "Initialized CoinGeckoOnchainOHLCVProvider (tier=%s, rate_limit=%d/min)",
            "pro" if self._api_key else "free",
            rate_limit,
        )

    # -- DataProvider protocol --------------------------------------------------

    @property
    def name(self) -> str:
        """Unique provider identifier."""
        return "geckoterminal"

    @property
    def data_class(self) -> DataClassification:
        """Classification: INFORMATIONAL (not execution-grade)."""
        return DataClassification.INFORMATIONAL

    def fetch(self, **kwargs: object) -> DataEnvelope:
        """Synchronous DataProvider entry point.

        Wraps the async ``get_ohlcv`` call and returns a DataEnvelope.

        Keyword Args:
            token: Token symbol (str).
            quote: Quote currency (str, default "USD").
            timeframe: Candle timeframe (str, default "1h").
            limit: Number of candles (int, default 100).
            pool_address: Explicit pool address (str, optional).
            chain: Chain name (str, default "ethereum").

        Returns:
            DataEnvelope wrapping a list of OHLCVCandle.
        """
        import asyncio

        token = str(kwargs.get("token", ""))
        quote = str(kwargs.get("quote", "USD"))
        timeframe = str(kwargs.get("timeframe", "1h"))
        limit = int(kwargs.get("limit", 100))  # type: ignore[call-overload]
        pool_address = kwargs.get("pool_address")
        chain = str(kwargs.get("chain", "ethereum"))

        start = time.monotonic()
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                import concurrent.futures

                with concurrent.futures.ThreadPoolExecutor() as pool:
                    candles = pool.submit(
                        asyncio.run,
                        self.get_ohlcv(
                            token=token,
                            quote=quote,
                            timeframe=timeframe,
                            limit=limit,
                            pool_address=str(pool_address) if pool_address else None,
                            chain=chain,
                        ),
                    ).result()
            else:
                candles = loop.run_until_complete(
                    self.get_ohlcv(
                        token=token,
                        quote=quote,
                        timeframe=timeframe,
                        limit=limit,
                        pool_address=str(pool_address) if pool_address else None,
                        chain=chain,
                    )
                )
        except RuntimeError:
            candles = asyncio.run(
                self.get_ohlcv(
                    token=token,
                    quote=quote,
                    timeframe=timeframe,
                    limit=limit,
                    pool_address=str(pool_address) if pool_address else None,
                    chain=chain,
                )
            )

        latency_ms = int((time.monotonic() - start) * 1000)
        meta = DataMeta(
            source=self.name,
            observed_at=datetime.now(UTC),
            finality="off_chain",
            staleness_ms=0,
            latency_ms=latency_ms,
            confidence=0.9,
            cache_hit=False,
        )
        return DataEnvelope(value=candles, meta=meta)

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

    # -- OHLCVProvider protocol -------------------------------------------------

    @property
    def supported_timeframes(self) -> list[str]:
        """Return supported timeframes."""
        return self.SUPPORTED_TIMEFRAMES.copy()

    async def get_ohlcv(
        self,
        token: str,
        quote: str = "USD",
        timeframe: str = "1h",
        limit: int = 100,
        *,
        pool_address: str | None = None,
        chain: str = "ethereum",
        include_empty_intervals: bool = False,
    ) -> list[OHLCVCandle]:
        """Fetch OHLCV candles from CoinGecko Onchain.

        Args:
            token: Token symbol (e.g. "WETH", "ETH").
            quote: Quote currency (ignored for pool_address lookups).
            timeframe: Candle timeframe (1m, 5m, 15m, 1h, 4h, 1d).
            limit: Number of candles to fetch (max 1000).
            pool_address: Explicit pool contract address. If provided, fetched
                directly. Otherwise a search is performed.
            chain: Chain name for network resolution (default "ethereum").
            include_empty_intervals: When True, ask CoinGecko Onchain to backfill
                no-trade intervals as continuous buckets. Fills *interior* gaps
                up to the most recent trade; it does NOT advance the newest
                candle past the last trade (that trailing-edge gap is handled
                in the framework OHLCV router). Default False.

        Returns:
            List of OHLCVCandle sorted by timestamp ascending.

        Raises:
            DataSourceUnavailable: On API errors, rate limiting, or missing data.
            ValueError: If timeframe is invalid.
        """
        validate_timeframe(timeframe)
        self._metrics.total_requests += 1
        limit = min(limit, 1000)

        # Check cache
        cache_key = self._cache_key(token, chain, timeframe, limit, pool_address)
        cached = self._get_cached(cache_key)
        if cached is not None:
            self._metrics.cache_hits += 1
            self._metrics.successful_requests += 1
            return cached

        # Rate limiting
        if not self._rate_limiter.acquire():
            self._metrics.errors += 1
            raise DataSourceUnavailable(
                source=_SOURCE,
                reason="Rate limited (30 req/min)",
                retry_after=2.0,
            )

        # Resolve network
        network = _CHAIN_TO_NETWORK.get(chain.lower())
        if network is None:
            self._metrics.errors += 1
            raise DataSourceUnavailable(
                source=_SOURCE,
                reason=f"Unsupported chain: {chain}. Supported: {', '.join(sorted(_CHAIN_TO_NETWORK))}",
            )

        # Resolve timeframe params
        tf_params = _TIMEFRAME_TO_GT.get(timeframe)
        if tf_params is None:
            self._metrics.errors += 1
            raise DataSourceUnavailable(
                source=_SOURCE,
                reason=f"Unsupported timeframe: {timeframe}",
            )

        if not self._api_key:
            self._metrics.errors += 1
            raise DataSourceUnavailable(
                source=_SOURCE,
                reason=(
                    "CoinGecko Onchain API requires a valid COINGECKO_API_KEY; "
                    "set ALMANAK_GATEWAY_COINGECKO_API_KEY on the gateway"
                ),
            )

        # Build URL
        if pool_address:
            url = f"{self._api_base}/networks/{network}/pools/{pool_address}/ohlcv/{tf_params['timeframe']}"
        else:
            # Search for pool by token symbol -- use top pool from search
            url = await self._resolve_pool_ohlcv_url(token, quote, network, tf_params["timeframe"])

        params: dict[str, str | int] = {
            "aggregate": tf_params["aggregate"],
            "limit": limit,
            "currency": "usd",
        }
        if include_empty_intervals:
            params["include_empty_intervals"] = "true"

        start_time = time.monotonic()

        try:
            session = await self._get_session()
            async with session.get(url, params=params) as response:
                latency_ms = (time.monotonic() - start_time) * 1000

                if response.status == 429:
                    self._metrics.errors += 1
                    raise DataSourceUnavailable(
                        source=_SOURCE,
                        reason="Rate limited by CoinGecko Onchain API",
                        retry_after=60.0,
                    )

                if response.status != 200:
                    error_text = await response.text()
                    self._metrics.errors += 1
                    reason = f"HTTP {response.status}: {error_text[:200]}"
                    if response.status == 401:
                        reason = (
                            "CoinGecko Onchain API requires a valid COINGECKO_API_KEY; "
                            "the key may be missing, invalid, or expired; HTTP 401"
                        )
                    raise DataSourceUnavailable(
                        source=_SOURCE,
                        reason=reason,
                    )

                data = await response.json()
                candles = self._parse_ohlcv_response(data)

                if not candles:
                    self._metrics.errors += 1
                    raise DataSourceUnavailable(
                        source=_SOURCE,
                        reason=f"No OHLCV data returned for {token} on {chain}",
                    )

                # Update cache and metrics
                self._update_cache(cache_key, candles)
                self._metrics.successful_requests += 1
                self._metrics.total_latency_ms += latency_ms

                logger.debug(
                    "Fetched %d CoinGecko Onchain OHLCV candles for %s/%s (latency: %.1fms)",
                    len(candles),
                    token,
                    chain,
                    latency_ms,
                )

                return candles

        except aiohttp.ClientError as e:
            self._metrics.errors += 1
            raise DataSourceUnavailable(
                source=_SOURCE,
                reason=str(e),
            ) from e
        except TimeoutError:
            self._metrics.errors += 1
            raise DataSourceUnavailable(
                source=_SOURCE,
                reason=f"Timeout after {self._request_timeout}s",
            ) from None

    # -- Internal helpers -------------------------------------------------------

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create HTTP session."""
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=self._request_timeout)
            self._session = aiohttp.ClientSession(
                timeout=timeout,
                headers=self._headers,
            )
        return self._session

    @property
    def _api_base(self) -> str:
        return _PRO_API_BASE if self._api_key else _FREE_API_BASE

    @property
    def _headers(self) -> dict[str, str]:
        headers = {"Accept": "application/json", "User-Agent": "Almanak-Gateway/1.0"}
        if self._api_key:
            headers["x-cg-pro-api-key"] = self._api_key
        return headers

    async def close(self) -> None:
        """Close the HTTP session."""
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    async def _resolve_pool_ohlcv_url(
        self,
        token: str,
        quote: str,
        network: str,
        timeframe_key: str,
    ) -> str:
        """Search CoinGecko Onchain for a pool and return the OHLCV URL.

        Uses the search endpoint to find the top pool for the token pair.
        """
        # Try the search endpoint to find pools for this token
        search_url = f"{self._api_base}/search/pools"
        params = {"query": token, "network": network}

        try:
            session = await self._get_session()
            async with session.get(search_url, params=params) as response:
                if response.status != 200:
                    reason = f"Pool search failed for {token} on {network}: HTTP {response.status}"
                    if response.status == 401:
                        reason = (
                            "CoinGecko Onchain pool search requires a valid COINGECKO_API_KEY; "
                            "the key may be missing, invalid, or expired; HTTP 401"
                        )
                    raise DataSourceUnavailable(
                        source=_SOURCE,
                        reason=reason,
                    )

                data = await response.json()
                pools = data.get("data", [])

                if not pools:
                    raise DataSourceUnavailable(
                        source=_SOURCE,
                        reason=f"No pools found for {token} on {network}",
                    )

                # Use the first pool result
                pool_id = pools[0].get("id", "")
                # Pool ID format: "network_poolAddress"
                if "_" in pool_id:
                    pool_address = pool_id.split("_", 1)[1]
                else:
                    pool_address = pools[0].get("attributes", {}).get("address", "")

                if not pool_address:
                    raise DataSourceUnavailable(
                        source=_SOURCE,
                        reason=f"Could not resolve pool address for {token} on {network}",
                    )

                return f"{self._api_base}/networks/{network}/pools/{pool_address}/ohlcv/{timeframe_key}"

        except aiohttp.ClientError as e:
            raise DataSourceUnavailable(
                source=_SOURCE,
                reason=f"Pool search network error: {e}",
            ) from e

    def _parse_ohlcv_response(self, data: dict[str, Any]) -> list[OHLCVCandle]:
        """Parse CoinGecko Onchain OHLCV JSON response into OHLCVCandle list.

        CoinGecko Onchain response format:
            {
                "data": {
                    "attributes": {
                        "ohlcv_list": [[timestamp, open, high, low, close, volume], ...]
                    }
                }
            }

        Candles are returned in descending order (newest first) from the API,
        so we reverse to ascending.
        """
        try:
            ohlcv_list = data.get("data", {}).get("attributes", {}).get("ohlcv_list", [])
        except AttributeError:
            return []

        candles: list[OHLCVCandle] = []
        for entry in ohlcv_list:
            if len(entry) < 6:
                continue
            try:
                candles.append(
                    OHLCVCandle(
                        timestamp=datetime.fromtimestamp(entry[0], tz=UTC),
                        open=Decimal(str(entry[1])),
                        high=Decimal(str(entry[2])),
                        low=Decimal(str(entry[3])),
                        close=Decimal(str(entry[4])),
                        volume=Decimal(str(entry[5])),
                    )
                )
            except (ValueError, TypeError, IndexError):
                logger.debug("Skipping malformed OHLCV entry: %s", entry)
                continue

        # CoinGecko Onchain returns newest first; reverse to ascending
        candles.sort(key=lambda c: c.timestamp)
        return candles

    def _cache_key(
        self,
        token: str,
        chain: str,
        timeframe: str,
        limit: int,
        pool_address: str | None,
    ) -> str:
        """Generate a cache key."""
        addr = pool_address or "auto"
        return f"{token.upper()}:{chain.lower()}:{timeframe}:{limit}:{addr.lower()}"

    def _get_cached(self, key: str) -> list[OHLCVCandle] | None:
        """Return cached candles if fresh, else None."""
        entry = self._cache.get(key)
        if entry is None:
            return None
        candles, cached_at = entry
        if time.monotonic() - cached_at > self._cache_ttl:
            return None
        return candles

    def _update_cache(self, key: str, candles: list[OHLCVCandle]) -> None:
        """Store candles in the in-memory cache."""
        self._cache[key] = (candles, time.monotonic())

    def clear_cache(self) -> None:
        """Clear the OHLCV cache."""
        self._cache.clear()
        logger.info("Cleared CoinGecko Onchain OHLCV cache")

    async def __aenter__(self) -> GeckoTerminalOHLCVProvider:
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit."""
        await self.close()


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    "GeckoTerminalOHLCVProvider",
]
