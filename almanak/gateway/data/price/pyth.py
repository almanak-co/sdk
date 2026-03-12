"""Pyth Network Price Source for Solana tokens.

This module provides a price source using the Pyth Network Hermes API,
which serves real-time price feeds on Solana. Pyth offers sub-second
price updates from first-party data providers.

Key Features:
    - Low-latency prices via Hermes HTTP API (no API key required)
    - Confidence interval from Pyth's oracle network
    - Response caching with configurable TTL
    - Graceful degradation on timeout (returns stale data)

Example:
    from almanak.gateway.data.price.pyth import PythPriceSource

    source = PythPriceSource()
    result = await source.get_price("SOL", "USD")
    print(f"Price: {result.price}, Confidence: {result.confidence}")
"""

import logging
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal

import aiohttp

from almanak.framework.data.interfaces import (
    BasePriceSource,
    DataSourceUnavailable,
    PriceResult,
)

logger = logging.getLogger(__name__)

# Pyth Hermes API (free, no key required)
HERMES_BASE_URL = "https://hermes.pyth.network"

# Pyth price feed IDs (hex, without 0x prefix)
# From https://pyth.network/developers/price-feed-ids
PYTH_FEED_IDS: dict[str, str] = {
    "SOL": "ef0d8b6fda2ceba41da15d4095d1da392a0d2f8ed0c6c7bc0f4cfac8c280b56d",
    "WSOL": "ef0d8b6fda2ceba41da15d4095d1da392a0d2f8ed0c6c7bc0f4cfac8c280b56d",
    "BTC": "e62df6c8b4a85fe1a67db44dc12de5db330f7ac66b72dc658afedf0f4a415b43",
    "WBTC": "e62df6c8b4a85fe1a67db44dc12de5db330f7ac66b72dc658afedf0f4a415b43",
    "ETH": "ff61491a931112ddf1bd8147cd1b641375f79f5825126d665480874634fd0ace",
    "WETH": "ff61491a931112ddf1bd8147cd1b641375f79f5825126d665480874634fd0ace",
    "USDC": "eaa020c61cc479712813461ce153894a96a6c00b21ed0cfc2798d1f9a9e9c94a",
    "USDT": "2b89b9dc8fdf9f34709a5b106b472f0f39bb6ca9ce04b0fd7f2e971688e2e53b",
    "JUP": "0a0408d619e9380abad35060f9192039ed5042fa6f82301d0e48bb52be830996",
    "RAY": "91568baa8beb53db23eb3fb7f22c6e8bd303d103919e19733f2bb642d3e7987a",
    "ORCA": "37505261e557e251290b8c8899453064e862e3c9d0bc4b14527fee2b5a426bed",
    "BONK": "72b021217ca3fe68922a19aaf990109cb9d84e9ad004b4d2025ad6f529314419",
    "WIF": "4ca4beeca86f0d164160323817a4e42b10010a724c2217c6ee41b54e4c843b6b",
    "JTO": "b43660a5f790c69354b0729a5ef9d50d68f1df92107540210b9cccba1f947cc2",
    "PYTH": "0bbf28e9a841a1cc788f6a361b17ca072d0ea3098a1e5df1c3922d06719579ff",
    "MSOL": "c2289a6a43d2ce91c6f55caec370f4acc38a2ed477f58813334c6d03749ff2a4",
    "JITOSOL": "67be9f519b95cf24338801051f9a808eff0a578ccb388db73b7f6fe1de019ffb",
}

# Staleness thresholds
_STALE_WARN_SECONDS = 60
_STALE_ERROR_SECONDS = 300


@dataclass
class _CacheEntry:
    """Cache entry for a Pyth price result."""

    result: PriceResult
    cached_at: float  # time.time()


class PythPriceSource(BasePriceSource):
    """Pyth Network price source using the Hermes HTTP API.

    Fetches real-time price feeds from Pyth's Hermes service. No API key
    is required. Prices update every ~400ms on-chain; this source caches
    for a configurable TTL (default 15s) to avoid excessive requests.

    Example:
        source = PythPriceSource(cache_ttl=30)
        result = await source.get_price("SOL", "USD")
    """

    def __init__(
        self,
        cache_ttl: int = 15,
        request_timeout: float = 10.0,
        stale_confidence: float = 0.7,
    ) -> None:
        self._cache_ttl = cache_ttl
        self._request_timeout = request_timeout
        self._stale_confidence = stale_confidence
        self._cache: dict[str, _CacheEntry] = {}
        self._session: aiohttp.ClientSession | None = None

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create the HTTP session."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=self._request_timeout),
            )
        return self._session

    async def close(self) -> None:
        """Close the HTTP session."""
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    @property
    def source_name(self) -> str:
        return "pyth"

    @property
    def supported_tokens(self) -> list[str]:
        return list(PYTH_FEED_IDS.keys())

    @property
    def cache_ttl_seconds(self) -> int:
        return self._cache_ttl

    async def get_price(self, token: str, quote: str = "USD") -> PriceResult:
        """Fetch the current price for a token from Pyth.

        Args:
            token: Token symbol (e.g., "SOL", "ETH", "USDC")
            quote: Quote currency (only "USD" supported by Pyth)

        Returns:
            PriceResult with price and metadata

        Raises:
            DataSourceUnavailable: If Pyth is unreachable and no cache exists
        """
        token_upper = token.upper()
        cache_key = f"{token_upper}/{quote}"

        # Check cache first
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        # Resolve feed ID
        feed_id = PYTH_FEED_IDS.get(token_upper)
        if not feed_id:
            raise DataSourceUnavailable(
                source="pyth",
                reason=f"No Pyth feed for token '{token_upper}'",
            )

        # Fetch from Hermes
        try:
            result = await self._fetch_price(feed_id, token_upper)
            self._cache[cache_key] = _CacheEntry(result=result, cached_at=time.time())
            return result
        except DataSourceUnavailable:
            raise
        except Exception as e:
            # Try stale cache
            stale = self._get_stale_cached(cache_key)
            if stale is not None:
                logger.warning("Pyth fetch failed for %s, using stale cache: %s", token_upper, e)
                return stale
            raise DataSourceUnavailable(
                source="pyth",
                reason=f"Fetch failed for {token_upper}: {e}",
            ) from e

    async def _fetch_price(self, feed_id: str, token: str) -> PriceResult:
        """Fetch a single price from Pyth Hermes API.

        Args:
            feed_id: Hex feed ID (without 0x prefix)
            token: Token symbol for logging

        Returns:
            PriceResult with parsed price
        """
        session = await self._get_session()

        url = f"{HERMES_BASE_URL}/v2/updates/price/latest"
        params = {"ids[]": feed_id}

        async with session.get(url, params=params) as response:
            if response.status != 200:
                text = await response.text()
                raise DataSourceUnavailable(
                    source="pyth",
                    reason=f"HTTP {response.status}: {text[:200]}",
                )

            data = await response.json()

        # Parse the response
        parsed = data.get("parsed", [])
        if not parsed:
            raise DataSourceUnavailable(
                source="pyth",
                reason=f"No parsed data for feed {feed_id[:16]}...",
            )

        price_data = parsed[0].get("price", {})
        price_int = int(price_data.get("price", "0"))
        expo = int(price_data.get("expo", 0))
        conf_int = int(price_data.get("conf", "0"))
        publish_time = int(price_data.get("publish_time", 0))

        if price_int == 0:
            raise DataSourceUnavailable(
                source="pyth",
                reason=f"Zero price for {token}",
            )

        # Calculate actual price: price_int * 10^expo
        actual_price = Decimal(price_int) * Decimal(10) ** expo

        # Calculate confidence from Pyth's confidence interval
        confidence = self._calculate_confidence(price_int, conf_int, publish_time)

        return PriceResult(
            price=actual_price,
            source="pyth",
            timestamp=datetime.fromtimestamp(publish_time, tz=UTC),
            confidence=confidence,
            stale=False,
        )

    def _calculate_confidence(
        self,
        price_int: int,
        conf_int: int,
        publish_time: int,
    ) -> float:
        """Calculate confidence score from Pyth data.

        Factors:
        - Confidence interval relative to price (tighter = better)
        - Age of the price (fresher = better)

        Returns:
            Confidence score between 0.0 and 1.0
        """
        # Start at high confidence
        confidence = 1.0

        # Reduce based on confidence interval spread
        if price_int != 0 and conf_int != 0:
            spread = abs(conf_int) / abs(price_int)
            if spread > 0.01:  # >1% spread
                confidence = 0.8
            elif spread > 0.001:  # >0.1% spread
                confidence = 0.9
            # else: tight spread, keep 1.0

        # Reduce based on staleness
        age = time.time() - publish_time
        if age > _STALE_ERROR_SECONDS:
            confidence = 0.5
        elif age > _STALE_WARN_SECONDS:
            confidence = 0.85

        return confidence

    def _get_cached(self, key: str) -> PriceResult | None:
        """Get fresh cached result."""
        entry = self._cache.get(key)
        if entry is None:
            return None
        age = time.time() - entry.cached_at
        if age < self._cache_ttl:
            return entry.result
        return None

    def _get_stale_cached(self, key: str) -> PriceResult | None:
        """Get stale cached result with reduced confidence."""
        entry = self._cache.get(key)
        if entry is None:
            return None
        return PriceResult(
            price=entry.result.price,
            source="pyth",
            timestamp=entry.result.timestamp,
            confidence=self._stale_confidence,
            stale=True,
        )

    async def health_check(self) -> bool:
        """Check if Pyth Hermes is reachable."""
        try:
            await self.get_price("SOL", "USD")
            return True
        except Exception:
            return False


__all__ = ["PythPriceSource", "PYTH_FEED_IDS"]
