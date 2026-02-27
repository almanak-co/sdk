"""Binance OHLCV Data Provider.

This module provides an OHLCV data provider using the Binance public API,
offering true minute-level granularity for candlestick data.

Key Features:
    - True 1m, 3m, 5m, 15m, 30m granularity (not approximated like CoinGecko)
    - Up to 1000 candles per request
    - Caching with configurable TTL
    - Rate limiting support (1200 requests/minute)
    - Health metrics tracking

Example:
    from almanak.framework.data.ohlcv import BinanceOHLCVProvider

    provider = BinanceOHLCVProvider()
    candles = await provider.get_ohlcv("WETH", timeframe="5m", limit=100)

    for candle in candles[-3:]:
        print(f"{candle.timestamp}: close={candle.close}")
"""

import logging
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import aiohttp

from ..interfaces import (
    DataSourceUnavailable,
    OHLCVCandle,
    validate_timeframe,
)

logger = logging.getLogger(__name__)


# Token symbol to Binance trading pair mapping
BINANCE_SYMBOL_MAP: dict[str, str] = {
    # Major tokens
    "WETH": "ETHUSDT",
    "ETH": "ETHUSDT",
    "BTC": "BTCUSDT",
    "WBTC": "BTCUSDT",
    # Stablecoins (use USDC as proxy)
    "USDC": "USDCUSDT",
    "USDT": "USDCUSDT",  # USDT/USDT doesn't exist, use USDC
    "DAI": "DAIUSDT",
    # DeFi tokens
    "LINK": "LINKUSDT",
    "UNI": "UNIUSDT",
    "AAVE": "AAVEUSDT",
    "CRV": "CRVUSDT",
    "MKR": "MKRUSDT",
    "COMP": "COMPUSDT",
    "SNX": "SNXUSDT",
    "SUSHI": "SUSHIUSDT",
    "YFI": "YFIUSDT",
    "1INCH": "1INCHUSDT",
    # L2 tokens
    "ARB": "ARBUSDT",
    "OP": "OPUSDT",
    "MATIC": "MATICUSDT",
    "POL": "MATICUSDT",  # Polygon rebranding
    # Wrapped native tokens (chain-specific)
    "WBNB": "BNBUSDT",
    "BNB": "BNBUSDT",
    "WAVAX": "AVAXUSDT",
    "WMATIC": "MATICUSDT",
    "S": "SUSDT",
    "WS": "SUSDT",
    # Other popular tokens
    "SOL": "SOLUSDT",
    "AVAX": "AVAXUSDT",
    "DOGE": "DOGEUSDT",
    "SHIB": "SHIBUSDT",
    "LDO": "LDOUSDT",
    "APE": "APEUSDT",
    "GMX": "GMXUSDT",
    "PEPE": "PEPEUSDT",
    "WLD": "WLDUSDT",
    "STX": "STXUSDT",
    "INJ": "INJUSDT",
    "TIA": "TIAUSDT",
    "SEI": "SEIUSDT",
    "SUI": "SUIUSDT",
    "APT": "APTUSDT",
    "FET": "FETUSDT",
    "RNDR": "RNDRUSDT",
    "GRT": "GRTUSDT",
    "FIL": "FILUSDT",
}

# Binance timeframe to API interval mapping
BINANCE_INTERVAL_MAP: dict[str, str] = {
    "1m": "1m",
    "3m": "3m",
    "5m": "5m",
    "15m": "15m",
    "30m": "30m",
    "1h": "1h",
    "2h": "2h",
    "4h": "4h",
    "6h": "6h",
    "8h": "8h",
    "12h": "12h",
    "1d": "1d",
    "3d": "3d",
    "1w": "1w",
    "1M": "1M",
}


@dataclass
class BinanceOHLCVCacheEntry:
    """Cache entry for Binance OHLCV data."""

    data: list[OHLCVCandle]
    cached_at: datetime
    token: str
    timeframe: str


@dataclass
class BinanceHealthMetrics:
    """Health metrics for Binance OHLCV provider."""

    total_requests: int = 0
    successful_requests: int = 0
    cache_hits: int = 0
    errors: int = 0
    total_latency_ms: float = 0.0

    @property
    def success_rate(self) -> float:
        """Calculate success rate as percentage."""
        if self.total_requests == 0:
            return 100.0
        return (self.successful_requests / self.total_requests) * 100

    @property
    def average_latency_ms(self) -> float:
        """Calculate average latency in milliseconds."""
        if self.successful_requests == 0:
            return 0.0
        return self.total_latency_ms / self.successful_requests


class BinanceOHLCVProvider:
    """Binance OHLCV data provider implementing the OHLCVProvider protocol.

    Fetches historical OHLCV data from Binance public API for technical analysis.
    Provides true minute-level granularity unlike CoinGecko.

    Attributes:
        cache_ttl: Cache time-to-live in seconds (default 60)
        request_timeout: HTTP request timeout in seconds (default 30)

    Rate Limits:
        - 1200 requests per minute (public API)
        - Up to 1000 candles per request

    Example:
        provider = BinanceOHLCVProvider(cache_ttl=60)
        candles = await provider.get_ohlcv("WETH", timeframe="5m", limit=100)
        print(f"Got {len(candles)} 5-minute candles")
    """

    API_BASE = "https://api.binance.com/api/v3"

    # Supported timeframes - true minute-level granularity
    SUPPORTED_TIMEFRAMES: list[str] = [
        "1m",
        "3m",
        "5m",
        "15m",
        "30m",
        "1h",
        "2h",
        "4h",
        "6h",
        "8h",
        "12h",
        "1d",
        "3d",
        "1w",
        "1M",
    ]

    def __init__(
        self,
        cache_ttl: int = 60,
        request_timeout: float = 30.0,
    ) -> None:
        """Initialize the Binance OHLCV provider.

        Args:
            cache_ttl: Cache time-to-live in seconds. Default 60 (1 minute).
            request_timeout: HTTP request timeout in seconds. Default 30.
        """
        self._cache_ttl = cache_ttl
        self._request_timeout = request_timeout

        # Cache: key (token:timeframe:limit) -> BinanceOHLCVCacheEntry
        self._cache: dict[str, BinanceOHLCVCacheEntry] = {}

        # Health metrics
        self._metrics = BinanceHealthMetrics()

        # HTTP session
        self._session: aiohttp.ClientSession | None = None

        logger.info(
            "Initialized BinanceOHLCVProvider",
            extra={"cache_ttl": cache_ttl},
        )

    @property
    def supported_timeframes(self) -> list[str]:
        """Return the list of timeframes this provider supports.

        Returns:
            List of supported timeframe strings.
            All timeframes are true granularity (not approximated).
        """
        return self.SUPPORTED_TIMEFRAMES.copy()

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create HTTP session."""
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=self._request_timeout)
            self._session = aiohttp.ClientSession(timeout=timeout)
        return self._session

    async def close(self) -> None:
        """Close the HTTP session."""
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    def _get_cache_key(self, token: str, timeframe: str, limit: int) -> str:
        """Generate cache key for OHLCV data."""
        return f"{token.upper()}:{timeframe}:{limit}"

    def _get_cached(self, token: str, timeframe: str, limit: int) -> list[OHLCVCandle] | None:
        """Get cached OHLCV data if exists and not expired."""
        cache_key = self._get_cache_key(token, timeframe, limit)
        entry = self._cache.get(cache_key)
        if entry is None:
            return None

        # Check if expired
        age_seconds = (datetime.now(UTC) - entry.cached_at).total_seconds()
        if age_seconds > self._cache_ttl:
            return None

        return entry.data

    def _update_cache(self, token: str, timeframe: str, limit: int, data: list[OHLCVCandle]) -> None:
        """Update cache with OHLCV data."""
        cache_key = self._get_cache_key(token, timeframe, limit)
        self._cache[cache_key] = BinanceOHLCVCacheEntry(
            data=data,
            cached_at=datetime.now(UTC),
            token=token,
            timeframe=timeframe,
        )

    def _resolve_symbol(self, token: str) -> str | None:
        """Resolve token symbol to Binance trading pair."""
        return BINANCE_SYMBOL_MAP.get(token.upper())

    def _resolve_interval(self, timeframe: str) -> str | None:
        """Resolve timeframe to Binance API interval."""
        return BINANCE_INTERVAL_MAP.get(timeframe)

    async def get_ohlcv(
        self,
        token: str,
        quote: str = "USD",
        timeframe: str = "1h",
        limit: int = 100,
    ) -> list[OHLCVCandle]:
        """Get OHLCV data for a token from Binance.

        Fetches historical candlestick data from Binance public API.
        Provides true minute-level granularity.

        Args:
            token: Token symbol (e.g., "WETH", "ETH", "BTC")
            quote: Quote currency (ignored - Binance uses USDT pairs)
            timeframe: Candle timeframe. Supported:
                1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1M
            limit: Number of candles to fetch (max 1000)

        Returns:
            List of OHLCVCandle objects sorted by timestamp ascending.

        Raises:
            DataSourceUnavailable: If data cannot be fetched
            ValueError: If timeframe is not supported
        """
        # Validate timeframe
        validate_timeframe(timeframe)
        self._metrics.total_requests += 1
        token_upper = token.upper()

        # Cap limit at Binance maximum
        limit = min(limit, 1000)

        # Check cache
        cached = self._get_cached(token_upper, timeframe, limit)
        if cached is not None:
            self._metrics.cache_hits += 1
            self._metrics.successful_requests += 1
            logger.debug(
                "Cache hit for Binance OHLCV %s (timeframe=%s)",
                token_upper,
                timeframe,
            )
            return cached

        # Resolve symbol and interval
        symbol = self._resolve_symbol(token_upper)
        if symbol is None:
            error_msg = f"Unknown token for Binance: {token_upper}"
            self._metrics.errors += 1
            raise DataSourceUnavailable(source="binance_ohlcv", reason=error_msg)

        interval = self._resolve_interval(timeframe)
        if interval is None:
            error_msg = f"Unsupported timeframe: {timeframe}"
            self._metrics.errors += 1
            raise DataSourceUnavailable(source="binance_ohlcv", reason=error_msg)

        # Build API URL
        url = f"{self.API_BASE}/klines"
        params: dict[str, str | int] = {
            "symbol": symbol,
            "interval": interval,
            "limit": limit,
        }

        start_time = time.time()

        try:
            session = await self._get_session()
            async with session.get(url, params=params) as response:
                latency_ms = (time.time() - start_time) * 1000

                if response.status == 429:
                    self._metrics.errors += 1
                    raise DataSourceUnavailable(
                        source="binance_ohlcv",
                        reason="Rate limited by Binance",
                        retry_after=60.0,
                    )

                if response.status != 200:
                    error_text = await response.text()
                    self._metrics.errors += 1
                    raise DataSourceUnavailable(
                        source="binance_ohlcv",
                        reason=f"HTTP {response.status}: {error_text}",
                    )

                # Parse response
                # Binance klines format: [
                #   [open_time, open, high, low, close, volume, close_time,
                #    quote_volume, trades, taker_buy_base, taker_buy_quote, ignore]
                # ]
                data = await response.json()

                if not data:
                    self._metrics.errors += 1
                    raise DataSourceUnavailable(
                        source="binance_ohlcv",
                        reason=f"No OHLCV data returned for {token_upper}",
                    )

                # Convert to OHLCVCandle objects
                ohlcv_list: list[OHLCVCandle] = []
                for candle in data:
                    if len(candle) >= 6:
                        ohlcv_list.append(
                            OHLCVCandle(
                                timestamp=datetime.fromtimestamp(candle[0] / 1000, tz=UTC),
                                open=Decimal(str(candle[1])),
                                high=Decimal(str(candle[2])),
                                low=Decimal(str(candle[3])),
                                close=Decimal(str(candle[4])),
                                volume=Decimal(str(candle[5])),
                            )
                        )

                # Already sorted by timestamp from Binance

                # Update cache
                self._update_cache(token_upper, timeframe, limit, ohlcv_list)

                # Update metrics
                self._metrics.successful_requests += 1
                self._metrics.total_latency_ms += latency_ms

                logger.debug(
                    "Fetched %d Binance OHLCV candles for %s (latency: %.2fms)",
                    len(ohlcv_list),
                    token_upper,
                    latency_ms,
                )

                return ohlcv_list

        except aiohttp.ClientError as e:
            self._metrics.errors += 1
            raise DataSourceUnavailable(
                source="binance_ohlcv",
                reason=str(e),
            ) from e
        except TimeoutError:
            self._metrics.errors += 1
            raise DataSourceUnavailable(
                source="binance_ohlcv",
                reason=f"Timeout after {self._request_timeout}s",
            ) from None

    def get_health_metrics(self) -> dict[str, Any]:
        """Get health metrics for observability."""
        return {
            "total_requests": self._metrics.total_requests,
            "successful_requests": self._metrics.successful_requests,
            "cache_hits": self._metrics.cache_hits,
            "errors": self._metrics.errors,
            "success_rate": round(self._metrics.success_rate, 2),
            "average_latency_ms": round(self._metrics.average_latency_ms, 2),
        }

    def clear_cache(self) -> None:
        """Clear the OHLCV cache."""
        self._cache.clear()
        logger.info("Cleared Binance OHLCV cache")

    async def __aenter__(self) -> "BinanceOHLCVProvider":
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit."""
        await self.close()


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    "BinanceOHLCVProvider",
    "BINANCE_SYMBOL_MAP",
]
