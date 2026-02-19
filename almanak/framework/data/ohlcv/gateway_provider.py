"""Gateway-backed OHLCV provider.

This module provides an OHLCV provider that fetches data through the gateway
sidecar instead of making direct HTTP requests. This is the preferred mode
for production deployments where strategies run in isolated containers.

The provider uses the gateway's Binance integration for OHLCV data.
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, ClassVar

from ..interfaces import (
    DataSourceUnavailable,
    OHLCVCandle,
    validate_timeframe,
)

if TYPE_CHECKING:
    from almanak.framework.gateway_client import GatewayClient

logger = logging.getLogger(__name__)


# Token symbol to Binance trading pair mapping
# Assumes USDT as quote currency for most pairs
TOKEN_TO_BINANCE_SYMBOL = {
    "ETH": "ETHUSDT",
    "WETH": "ETHUSDT",
    "BTC": "BTCUSDT",
    "WBTC": "BTCUSDT",
    "ARB": "ARBUSDT",
    "OP": "OPUSDT",
    "AVAX": "AVAXUSDT",
    "WAVAX": "AVAXUSDT",
    "MATIC": "MATICUSDT",
    "WMATIC": "MATICUSDT",
    "SOL": "SOLUSDT",
    "LINK": "LINKUSDT",
    "UNI": "UNIUSDT",
    "AAVE": "AAVEUSDT",
    "CRV": "CRVUSDT",
    "LDO": "LDOUSDT",
    "MKR": "MKRUSDT",
    "SNX": "SNXUSDT",
    "COMP": "COMPUSDT",
    "SUSHI": "SUSHIUSDT",
    "YFI": "YFIUSDT",
    "BAL": "BALUSDT",
    "1INCH": "1INCHUSDT",
}

# Timeframe mapping: our timeframes to Binance intervals
TIMEFRAME_TO_BINANCE_INTERVAL = {
    "1m": "1m",
    "5m": "5m",
    "15m": "15m",
    "30m": "30m",
    "1h": "1h",
    "4h": "4h",
    "1d": "1d",
}


@dataclass
class OHLCVHealthMetrics:
    """Health metrics for OHLCV provider."""

    total_requests: int = 0
    successful_requests: int = 0
    cache_hits: int = 0
    errors: int = 0

    @property
    def success_rate(self) -> float:
        """Calculate success rate as percentage."""
        if self.total_requests == 0:
            return 100.0
        return (self.successful_requests / self.total_requests) * 100


@dataclass
class CacheEntry:
    """Cache entry for OHLCV data."""

    candles: list[OHLCVCandle]
    timestamp: float = field(default_factory=time.time)


class GatewayOHLCVProvider:
    """Gateway-backed OHLCV provider implementing the OHLCVProvider protocol.

    Fetches OHLCV data through the gateway's Binance integration, ensuring
    all external API access is mediated by the gateway sidecar.

    This provider maps token symbols to Binance trading pairs and uses
    the gateway's BinanceGetKlines RPC for data fetching.

    Includes configurable TTL caching: shorter for live data (1m, 5m timeframes)
    and longer for historical data (1h+).

    Example:
        from almanak.framework.gateway_client import GatewayClient
        from almanak.framework.data.ohlcv.gateway_provider import GatewayOHLCVProvider

        with GatewayClient() as client:
            provider = GatewayOHLCVProvider(gateway_client=client)
            candles = await provider.get_ohlcv("WETH", timeframe="1h", limit=100)
            print(f"Got {len(candles)} candles")
    """

    _SUPPORTED_TIMEFRAMES: ClassVar[list[str]] = ["1m", "5m", "15m", "30m", "1h", "4h", "1d"]
    _LIVE_TIMEFRAMES: ClassVar[set[str]] = {"1m", "5m"}

    def __init__(
        self,
        gateway_client: "GatewayClient",
        cache_ttl_live: float = 10.0,
        cache_ttl_historical: float = 60.0,
    ) -> None:
        """Initialize the gateway OHLCV provider.

        Args:
            gateway_client: Connected GatewayClient instance
            cache_ttl_live: Cache TTL in seconds for live timeframes (1m, 5m). Default: 10s
            cache_ttl_historical: Cache TTL in seconds for historical timeframes (15m+). Default: 60s
        """
        self._gateway_client = gateway_client
        self._metrics = OHLCVHealthMetrics()
        self._cache: dict[tuple[str, str, int], CacheEntry] = {}
        self._cache_ttl_live = cache_ttl_live
        self._cache_ttl_historical = cache_ttl_historical

        logger.info(
            "Initialized GatewayOHLCVProvider with cache TTLs: live=%ss, historical=%ss",
            cache_ttl_live,
            cache_ttl_historical,
        )

    @property
    def supported_timeframes(self) -> list[str]:
        """Return the list of timeframes this provider supports.

        Returns:
            List of supported timeframe strings.
        """
        return self._SUPPORTED_TIMEFRAMES.copy()

    def _resolve_binance_symbol(self, token: str) -> str | None:
        """Resolve token symbol to Binance trading pair.

        Args:
            token: Token symbol (e.g., "WETH", "ETH")

        Returns:
            Binance trading pair (e.g., "ETHUSDT") or None if not found
        """
        token_upper = token.upper()
        return TOKEN_TO_BINANCE_SYMBOL.get(token_upper)

    def _get_cache_ttl(self, timeframe: str) -> float:
        """Get the appropriate cache TTL for a timeframe.

        Args:
            timeframe: Candle timeframe

        Returns:
            Cache TTL in seconds
        """
        if timeframe in self._LIVE_TIMEFRAMES:
            return self._cache_ttl_live
        return self._cache_ttl_historical

    def _get_cached(self, cache_key: tuple[str, str, int], ttl: float) -> list[OHLCVCandle] | None:
        """Get cached data if still valid.

        Args:
            cache_key: Cache key tuple (token, timeframe, limit)
            ttl: TTL in seconds

        Returns:
            Cached candles if valid, None otherwise
        """
        entry = self._cache.get(cache_key)
        if entry is None:
            return None

        age = time.time() - entry.timestamp
        if age > ttl:
            # Cache expired
            del self._cache[cache_key]
            return None

        return entry.candles

    async def get_ohlcv(
        self,
        token: str,
        quote: str = "USD",  # noqa: ARG002 - unused, internally uses USDT pairs
        timeframe: str = "1h",
        limit: int = 100,
    ) -> list[OHLCVCandle]:
        """Get OHLCV data for a token through gateway.

        Uses the gateway's Binance integration to fetch kline data.
        Results are cached with configurable TTL based on timeframe.

        Args:
            token: Token symbol (e.g., "WETH", "ETH")
            quote: Quote currency (unused - internally uses USDT pairs)
            timeframe: Candle timeframe. Supported: "1m", "5m", "15m", "30m", "1h", "4h", "1d"
            limit: Number of candles to fetch (max 1000)

        Returns:
            List of OHLCVCandle objects sorted by timestamp ascending.

        Raises:
            DataSourceUnavailable: If data cannot be fetched
            ValueError: If timeframe is not supported
        """
        validate_timeframe(timeframe)
        self._metrics.total_requests += 1

        # Check cache first
        cache_key = (token.upper(), timeframe, limit)
        ttl = self._get_cache_ttl(timeframe)
        cached = self._get_cached(cache_key, ttl)
        if cached is not None:
            self._metrics.cache_hits += 1
            self._metrics.successful_requests += 1
            logger.debug("Cache hit for %s %s (limit=%d)", token, timeframe, limit)
            return cached

        # Resolve token to Binance symbol
        binance_symbol = self._resolve_binance_symbol(token)
        if binance_symbol is None:
            error_msg = f"Unknown token for Binance: {token}"
            self._metrics.errors += 1
            raise DataSourceUnavailable(source="gateway_ohlcv", reason=error_msg)

        # Map timeframe to Binance interval
        binance_interval = TIMEFRAME_TO_BINANCE_INTERVAL.get(timeframe)
        if binance_interval is None:
            error_msg = f"Unsupported timeframe: {timeframe}"
            self._metrics.errors += 1
            raise DataSourceUnavailable(source="gateway_ohlcv", reason=error_msg)

        try:
            from almanak.gateway.proto import gateway_pb2

            # Call gateway's Binance klines endpoint in a thread to avoid blocking
            request = gateway_pb2.BinanceKlinesRequest(
                symbol=binance_symbol,
                interval=binance_interval,
                limit=min(limit, 1000),  # Binance max is 1000
            )
            response = await asyncio.to_thread(
                self._gateway_client.integration.BinanceGetKlines,
                request,
                self._gateway_client.config.timeout,
            )

            if not response.klines:
                error_msg = f"No kline data returned for {binance_symbol}"
                self._metrics.errors += 1
                raise DataSourceUnavailable(source="gateway_ohlcv", reason=error_msg)

            # Convert Binance klines to OHLCVCandle
            candles = []
            for kline in response.klines:
                candles.append(
                    OHLCVCandle(
                        timestamp=datetime.fromtimestamp(kline.open_time / 1000, tz=UTC),
                        open=Decimal(kline.open) if kline.open else Decimal(0),
                        high=Decimal(kline.high) if kline.high else Decimal(0),
                        low=Decimal(kline.low) if kline.low else Decimal(0),
                        close=Decimal(kline.close) if kline.close else Decimal(0),
                        volume=Decimal(kline.volume) if kline.volume else None,
                    )
                )

            # Sort by timestamp (oldest first)
            candles.sort(key=lambda x: x.timestamp)

            # Cache the result
            self._cache[cache_key] = CacheEntry(candles=candles)

            self._metrics.successful_requests += 1
            logger.debug(
                "Fetched %d OHLCV candles for %s via gateway",
                len(candles),
                token,
            )

            return candles

        except DataSourceUnavailable:
            raise
        except Exception as e:
            error_msg = f"Gateway OHLCV request failed: {e}"
            self._metrics.errors += 1
            logger.exception(error_msg)
            raise DataSourceUnavailable(source="gateway_ohlcv", reason=error_msg) from e

    def get_health_metrics(self) -> dict[str, Any]:
        """Get health metrics for observability."""
        return {
            "total_requests": self._metrics.total_requests,
            "successful_requests": self._metrics.successful_requests,
            "cache_hits": self._metrics.cache_hits,
            "errors": self._metrics.errors,
            "success_rate": round(self._metrics.success_rate, 2),
            "cache_size": len(self._cache),
        }

    def clear_cache(self) -> None:
        """Clear the OHLCV cache."""
        self._cache.clear()
        logger.debug("OHLCV cache cleared")


__all__ = [
    "GatewayOHLCVProvider",
    "OHLCVHealthMetrics",
    "TOKEN_TO_BINANCE_SYMBOL",
]
