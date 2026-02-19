"""Binance integration for gateway.

Provides read-only access to Binance market data through the gateway:
- Ticker (24h price statistics)
- Klines (candlestick/OHLCV data)
- Order book depth

Note: Only market data endpoints are exposed. No trading functionality.
"""

import logging
from typing import Any

from almanak.gateway.integrations.base import BaseIntegration

logger = logging.getLogger(__name__)


class BinanceIntegration(BaseIntegration):
    """Binance market data integration.

    Provides read-only access to Binance public API endpoints.
    Rate limit: 1200 requests per minute (Binance public API limit).

    Supported endpoints:
    - get_ticker: 24h price statistics
    - get_klines: OHLCV candlestick data
    - get_order_book: Order book depth

    Example:
        integration = BinanceIntegration()
        ticker = await integration.get_ticker("BTCUSDT")
        klines = await integration.get_klines("ETHUSDT", interval="1h", limit=100)
    """

    name = "binance"
    rate_limit_requests = 1200  # Binance public API limit
    default_cache_ttl = 5  # Short TTL for market data

    # Valid kline intervals
    VALID_INTERVALS = {
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
    }

    # Valid order book limits
    VALID_DEPTH_LIMITS = {5, 10, 20, 50, 100, 500, 1000}

    def __init__(
        self,
        api_key: str | None = None,
        request_timeout: float = 30.0,
    ):
        """Initialize Binance integration.

        Args:
            api_key: Optional Binance API key (not required for public endpoints)
            request_timeout: HTTP request timeout in seconds
        """
        super().__init__(
            api_key=api_key,
            base_url="https://api.binance.com",
            request_timeout=request_timeout,
        )

    def _get_headers(self) -> dict[str, str]:
        """Get headers for Binance API requests."""
        headers = super()._get_headers()
        if self._api_key:
            headers["X-MBX-APIKEY"] = self._api_key
        return headers

    async def health_check(self) -> bool:
        """Check if Binance API is healthy.

        Returns:
            True if API is responding, False otherwise
        """
        try:
            await self._fetch("/api/v3/ping")
            return True
        except Exception as e:
            logger.warning("Binance health check failed: %s", e)
            return False

    async def get_ticker(self, symbol: str) -> dict[str, Any]:
        """Get 24h ticker price statistics.

        Args:
            symbol: Trading pair (e.g., "BTCUSDT", "ETHUSDT")

        Returns:
            Ticker data with price, volume, and change statistics

        Raises:
            IntegrationError: On API errors
        """
        symbol = symbol.upper()

        # Check cache
        cache_key = f"ticker:{symbol}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        data = await self._fetch(f"/api/v3/ticker/24hr?symbol={symbol}")

        # Update cache
        self._update_cache(cache_key, data, ttl=5)

        return data

    async def get_klines(
        self,
        symbol: str,
        interval: str = "1h",
        limit: int = 100,
        start_time: int | None = None,
        end_time: int | None = None,
    ) -> list[dict[str, Any]]:
        """Get kline/candlestick data.

        Args:
            symbol: Trading pair (e.g., "BTCUSDT")
            interval: Kline interval (1m, 5m, 15m, 1h, 4h, 1d, etc.)
            limit: Number of klines to return (max 1000)
            start_time: Start time in milliseconds (optional)
            end_time: End time in milliseconds (optional)

        Returns:
            List of klines with OHLCV data

        Raises:
            IntegrationError: On API errors
            ValueError: On invalid parameters
        """
        symbol = symbol.upper()

        # Validate interval
        if interval not in self.VALID_INTERVALS:
            raise ValueError(f"Invalid interval: {interval}. Must be one of {self.VALID_INTERVALS}")

        # Cap limit
        limit = min(limit, 1000)

        # Build query params
        params: dict[str, Any] = {
            "symbol": symbol,
            "interval": interval,
            "limit": limit,
        }
        if start_time:
            params["startTime"] = start_time
        if end_time:
            params["endTime"] = end_time

        # Check cache (only if no time params - avoid caching specific time ranges)
        cache_key = f"klines:{symbol}:{interval}:{limit}"
        if start_time is None and end_time is None:
            cached = self._get_cached(cache_key)
            if cached is not None:
                return cached

        data = await self._fetch("/api/v3/klines", params=params)

        # Transform to structured format
        klines = []
        for k in data:
            klines.append(
                {
                    "open_time": k[0],
                    "open": k[1],
                    "high": k[2],
                    "low": k[3],
                    "close": k[4],
                    "volume": k[5],
                    "close_time": k[6],
                    "quote_volume": k[7],
                    "trades": k[8],
                }
            )

        # Update cache
        if start_time is None and end_time is None:
            self._update_cache(cache_key, klines, ttl=60)  # 1 minute cache for klines

        return klines

    async def get_order_book(
        self,
        symbol: str,
        limit: int = 100,
    ) -> dict[str, Any]:
        """Get order book depth.

        Args:
            symbol: Trading pair (e.g., "BTCUSDT")
            limit: Depth limit (5, 10, 20, 50, 100, 500, 1000)

        Returns:
            Order book with bids and asks

        Raises:
            IntegrationError: On API errors
            ValueError: On invalid parameters
        """
        symbol = symbol.upper()

        # Validate limit
        if limit not in self.VALID_DEPTH_LIMITS:
            # Round to nearest valid limit
            limit = min(self.VALID_DEPTH_LIMITS, key=lambda x: abs(x - limit))

        # Check cache (short TTL for order book)
        cache_key = f"depth:{symbol}:{limit}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        data = await self._fetch(f"/api/v3/depth?symbol={symbol}&limit={limit}")

        # Transform to structured format
        result = {
            "last_update_id": data.get("lastUpdateId"),
            "bids": [{"price": b[0], "quantity": b[1]} for b in data.get("bids", [])],
            "asks": [{"price": a[0], "quantity": a[1]} for a in data.get("asks", [])],
        }

        # Update cache (very short TTL for order book)
        self._update_cache(cache_key, result, ttl=2)

        return result

    async def get_exchange_info(self, symbol: str | None = None) -> dict[str, Any]:
        """Get exchange trading rules and symbol info.

        Args:
            symbol: Optional symbol to filter (e.g., "BTCUSDT")

        Returns:
            Exchange info with trading rules

        Raises:
            IntegrationError: On API errors
        """
        cache_key = f"exchange_info:{symbol or 'all'}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        path = "/api/v3/exchangeInfo"
        if symbol:
            path += f"?symbol={symbol.upper()}"

        data = await self._fetch(path)

        # Cache for longer (exchange info rarely changes)
        self._update_cache(cache_key, data, ttl=300)

        return data
