"""RSI Calculator with Wilder's Smoothing Method.

This module provides a production-ready RSI (Relative Strength Index) calculator
using Wilder's smoothing method, with OHLCV data fetching from CoinGecko.

Key Features:
    - Standard RSI formula using Wilder's smoothing (exponential moving average)
    - Configurable period (default 14)
    - OHLCV data caching to avoid repeated API calls
    - Proper error handling for insufficient data

RSI Interpretation:
    - RSI < 30: Oversold (potential buy signal)
    - RSI > 70: Overbought (potential sell signal)
    - RSI 30-70: Neutral zone

Example:
    from almanak.framework.data.indicators.rsi import RSICalculator, CoinGeckoOHLCVProvider

    # Create provider and calculator
    provider = CoinGeckoOHLCVProvider(api_key="optional-key")
    calculator = RSICalculator(ohlcv_provider=provider)

    # Calculate RSI
    rsi = await calculator.calculate_rsi("WETH", period=14)
    print(f"WETH RSI(14): {rsi:.2f}")
"""

import logging
import os
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import aiohttp

from ..interfaces import (
    DataSourceUnavailable,
    InsufficientDataError,
    OHLCVCandle,
    OHLCVProvider,
    validate_timeframe,
)
from ..tokens import get_coingecko_id

logger = logging.getLogger(__name__)


@dataclass
class OHLCVData:
    """OHLCV candlestick data point.

    Attributes:
        timestamp: Candle open time
        open: Opening price
        high: Highest price
        low: Lowest price
        close: Closing price
        volume: Trading volume (optional, CoinGecko OHLC doesn't include volume)
    """

    timestamp: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "timestamp": self.timestamp.isoformat(),
            "open": str(self.open),
            "high": str(self.high),
            "low": str(self.low),
            "close": str(self.close),
            "volume": str(self.volume) if self.volume else None,
        }


@dataclass
class OHLCVCacheEntry:
    """Cache entry for OHLCV data."""

    data: list[OHLCVData]
    cached_at: datetime
    token: str
    timeframe: str


@dataclass
class OHLCVHealthMetrics:
    """Health metrics for OHLCV provider."""

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


class CoinGeckoOHLCVProvider:
    """CoinGecko OHLCV data provider implementing the OHLCVProvider protocol.

    Fetches historical OHLCV data from CoinGecko API for technical analysis.
    Implements the OHLCVProvider protocol from src/data/interfaces.py.

    Note on timeframe support:
        CoinGecko's OHLC API has granularity limitations based on the day range:
        - 1-2 days: Returns 30-minute candles (supports 1m, 5m, 15m approximation)
        - 3-30 days: Returns 4-hour candles (supports 1h, 4h)
        - 31+ days: Returns daily candles (supports 1d)

        For finer timeframes (1m, 5m, 15m), we request 1-2 day ranges and return
        the 30-minute candles. Strategies requiring exact minute-level candles
        should use a more granular data source.

    Attributes:
        api_key: Optional CoinGecko API key (uses pro API if provided)
        cache_ttl: Cache time-to-live in seconds (default 300 = 5 minutes)
        request_timeout: HTTP request timeout in seconds (default 30)

    Example:
        provider = CoinGeckoOHLCVProvider(api_key="optional-key")
        candles = await provider.get_ohlcv("WETH", timeframe="1h", limit=100)
        print(f"Got {len(candles)} candles")
        for candle in candles[-3:]:
            print(f"  {candle.timestamp}: close={candle.close}")
    """

    _FREE_API_BASE = "https://api.coingecko.com/api/v3"
    _PRO_API_BASE = "https://pro-api.coingecko.com/api/v3"

    # Supported timeframes per OHLCVProvider protocol
    # Note: 1m, 5m, 15m return 30-minute candles from CoinGecko (best available)
    _SUPPORTED_TIMEFRAMES: list[str] = ["1m", "5m", "15m", "1h", "4h", "1d"]

    # CoinGecko OHLC API day limits per granularity:
    # - 1-2 days: 30-minute candles
    # - 3-30 days: 4-hour candles
    # - 31-90 days: 4-hour candles (pro) or daily (free)
    # - 91+ days: daily candles
    # For 1-hour granularity, we need to use market_chart endpoint instead

    def __init__(
        self,
        api_key: str = "",
        cache_ttl: int = 300,
        request_timeout: float = 30.0,
    ) -> None:
        """Initialize the CoinGecko OHLCV provider.

        Args:
            api_key: Optional CoinGecko API key. If provided, uses pro API.
            cache_ttl: Cache time-to-live in seconds. Default 300 (5 minutes).
            request_timeout: HTTP request timeout in seconds. Default 30.
        """
        self._api_key = api_key or os.environ.get("COINGECKO_API_KEY", "")
        self._cache_ttl = cache_ttl
        self._request_timeout = request_timeout

        self._api_base = self._PRO_API_BASE if self._api_key else self._FREE_API_BASE

        # Cache: key (token:timeframe:days) -> OHLCVCacheEntry
        self._cache: dict[str, OHLCVCacheEntry] = {}

        # Health metrics
        self._metrics = OHLCVHealthMetrics()

        # HTTP session
        self._session: aiohttp.ClientSession | None = None

        logger.info(
            "Initialized CoinGeckoOHLCVProvider",
            extra={
                "api_type": "pro" if api_key else "free",
                "cache_ttl": cache_ttl,
            },
        )

    @property
    def supported_timeframes(self) -> list[str]:
        """Return the list of timeframes this provider supports.

        Returns:
            List of supported timeframe strings.
            Note: 1m, 5m, 15m return 30-minute candles (CoinGecko's finest granularity)
        """
        return self._SUPPORTED_TIMEFRAMES.copy()

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

    def _get_cache_key(self, token: str, timeframe: str, days: int) -> str:
        """Generate cache key for OHLCV data."""
        return f"{token.upper()}:{timeframe}:{days}"

    def _get_cached(self, token: str, timeframe: str, days: int) -> list[OHLCVData] | None:
        """Get cached OHLCV data if exists and not expired."""
        cache_key = self._get_cache_key(token, timeframe, days)
        entry = self._cache.get(cache_key)
        if entry is None:
            return None

        # Check if expired
        age_seconds = (datetime.now(UTC) - entry.cached_at).total_seconds()
        if age_seconds > self._cache_ttl:
            return None

        return entry.data

    def _update_cache(self, token: str, timeframe: str, days: int, data: list[OHLCVData]) -> None:
        """Update cache with OHLCV data."""
        cache_key = self._get_cache_key(token, timeframe, days)
        self._cache[cache_key] = OHLCVCacheEntry(
            data=data,
            cached_at=datetime.now(UTC),
            token=token,
            timeframe=timeframe,
        )

    def _resolve_token_id(self, token: str) -> str | None:
        """Resolve token symbol to CoinGecko ID."""
        return get_coingecko_id(token.upper())

    async def get_ohlcv(
        self,
        token: str,
        quote: str = "USD",
        timeframe: str = "1h",
        limit: int = 100,
    ) -> list[OHLCVCandle]:
        """Get OHLCV data for a token.

        Uses the CoinGecko OHLC endpoint for historical candlestick data.
        Implements the OHLCVProvider protocol.

        Args:
            token: Token symbol (e.g., "WETH", "ETH")
            quote: Quote currency (default "USD")
            timeframe: Candle timeframe. Supported: "1m", "5m", "15m", "1h", "4h", "1d"
                Note: 1m, 5m, 15m return 30-minute candles (CoinGecko's finest granularity)
            limit: Number of candles to fetch

        Returns:
            List of OHLCVCandle objects sorted by timestamp ascending.
            Volume is always None as CoinGecko OHLC API does not provide volume data.

        Raises:
            DataSourceUnavailable: If data cannot be fetched
            ValueError: If timeframe is not supported
        """
        # Validate timeframe
        validate_timeframe(timeframe)
        self._metrics.total_requests += 1
        token_upper = token.upper()

        # Calculate days needed based on timeframe and limit
        # CoinGecko OHLC API only accepts specific days values: 1, 7, 14, 30, 90, 180, 365, max
        # CoinGecko OHLC granularity:
        # - 1-2 days: ~48 30-min candles per day
        # - 3-30 days: ~6 4-hour candles per day
        # - 31+ days: 1 daily candle per day
        valid_days = [1, 7, 14, 30, 90, 180, 365]

        if timeframe in ("1m", "5m", "15m"):
            # For minute timeframes, use 1 day to get 30-min candles
            days = 1
        elif timeframe == "1h":
            # For hourly, we get 4-hour candles and need more days
            # To get ~limit hourly data points, we need limit/6 days worth of 4-hour candles
            needed_days = max(7, (limit // 6) + 2)
            # Find the smallest valid days value that covers needed_days
            days = next((d for d in valid_days if d >= needed_days), 30)
        elif timeframe == "4h":
            needed_days = max(7, (limit // 6) + 2)
            # Find the smallest valid days value that covers needed_days
            days = next((d for d in valid_days if d >= needed_days), 30)
        elif timeframe == "1d":
            needed_days = limit + 5  # Add buffer for daily
            # Find the smallest valid days value that covers needed_days
            days = next((d for d in valid_days if d >= needed_days), 90)
        else:
            days = 30  # Default fallback

        # Check cache
        cached = self._get_cached(token_upper, timeframe, days)
        if cached is not None:
            self._metrics.cache_hits += 1
            self._metrics.successful_requests += 1
            logger.debug(
                "Cache hit for OHLCV %s/%s (timeframe=%s)",
                token_upper,
                quote,
                timeframe,
            )
            # Convert OHLCVData to OHLCVCandle for protocol compatibility
            return [
                OHLCVCandle(
                    timestamp=candle.timestamp,
                    open=candle.open,
                    high=candle.high,
                    low=candle.low,
                    close=candle.close,
                    volume=candle.volume,  # None for CoinGecko
                )
                for candle in cached[-limit:]
            ]

        # Resolve token ID
        token_id = self._resolve_token_id(token_upper)
        if token_id is None:
            error_msg = f"Unknown token: {token_upper}"
            self._metrics.errors += 1
            raise DataSourceUnavailable(source="coingecko_ohlcv", reason=error_msg)

        # Build API URL for OHLC endpoint
        url = f"{self._api_base}/coins/{token_id}/ohlc"
        params: dict[str, str] = {
            "vs_currency": quote.lower(),
            "days": str(days),
        }
        if self._api_key:
            params["x_cg_pro_api_key"] = self._api_key

        start_time = time.time()

        try:
            session = await self._get_session()
            async with session.get(url, params=params) as response:
                latency_ms = (time.time() - start_time) * 1000

                if response.status == 429:
                    self._metrics.errors += 1
                    raise DataSourceUnavailable(
                        source="coingecko_ohlcv",
                        reason="Rate limited by CoinGecko",
                        retry_after=60.0,
                    )

                if response.status != 200:
                    error_text = await response.text()
                    self._metrics.errors += 1
                    raise DataSourceUnavailable(
                        source="coingecko_ohlcv",
                        reason=f"HTTP {response.status}: {error_text}",
                    )

                # Parse response: [[timestamp, open, high, low, close], ...]
                data = await response.json()

                if not data or len(data) == 0:
                    self._metrics.errors += 1
                    raise DataSourceUnavailable(
                        source="coingecko_ohlcv",
                        reason=f"No OHLC data returned for {token_upper}",
                    )

                # Convert to OHLCVData objects
                ohlcv_list: list[OHLCVData] = []
                for candle in data:
                    if len(candle) >= 5:
                        ohlcv_list.append(
                            OHLCVData(
                                timestamp=datetime.fromtimestamp(candle[0] / 1000, tz=UTC),
                                open=Decimal(str(candle[1])),
                                high=Decimal(str(candle[2])),
                                low=Decimal(str(candle[3])),
                                close=Decimal(str(candle[4])),
                            )
                        )

                # Sort by timestamp (oldest first)
                ohlcv_list.sort(key=lambda x: x.timestamp)

                # Update cache
                self._update_cache(token_upper, timeframe, days, ohlcv_list)

                # Update metrics
                self._metrics.successful_requests += 1
                self._metrics.total_latency_ms += latency_ms

                logger.debug(
                    "Fetched %d OHLCV candles for %s/%s (latency: %.2fms)",
                    len(ohlcv_list),
                    token_upper,
                    quote,
                    latency_ms,
                )

                # Convert OHLCVData to OHLCVCandle for protocol compatibility
                result = [
                    OHLCVCandle(
                        timestamp=candle.timestamp,
                        open=candle.open,
                        high=candle.high,
                        low=candle.low,
                        close=candle.close,
                        volume=candle.volume,  # None for CoinGecko
                    )
                    for candle in ohlcv_list[-limit:]
                ]
                return result

        except aiohttp.ClientError as e:
            self._metrics.errors += 1
            raise DataSourceUnavailable(
                source="coingecko_ohlcv",
                reason=str(e),
            ) from e
        except TimeoutError:
            self._metrics.errors += 1
            raise DataSourceUnavailable(
                source="coingecko_ohlcv",
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
        logger.info("Cleared OHLCV cache")

    async def __aenter__(self) -> "CoinGeckoOHLCVProvider":
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit."""
        await self.close()


class RSICalculator:
    """RSI (Relative Strength Index) Calculator using Wilder's Smoothing Method.

    Implements the standard RSI formula with Wilder's exponential moving average
    for smoothing, which is the industry-standard approach.

    RSI Formula:
        RSI = 100 - (100 / (1 + RS))
        RS = Average Gain / Average Loss

    Wilder's Smoothing:
        - First average: Simple average of first N periods
        - Subsequent: ((Previous Avg * (N-1)) + Current Value) / N

    Attributes:
        ohlcv_provider: Provider for OHLCV data (implements OHLCVProvider protocol)
        default_period: Default RSI period (default 14)

    Example:
        provider = CoinGeckoOHLCVProvider()
        calculator = RSICalculator(ohlcv_provider=provider)

        # Calculate 14-period RSI for WETH
        rsi = await calculator.calculate_rsi("WETH", period=14)
        print(f"RSI: {rsi:.2f}")

        # Interpret the signal
        if rsi < 30:
            print("Oversold - potential buy signal")
        elif rsi > 70:
            print("Overbought - potential sell signal")
    """

    def __init__(
        self,
        ohlcv_provider: OHLCVProvider,
        default_period: int = 14,
    ) -> None:
        """Initialize the RSI Calculator.

        Args:
            ohlcv_provider: Provider implementing OHLCVProvider protocol
            default_period: Default RSI calculation period (default 14)
        """
        self._ohlcv_provider = ohlcv_provider
        self._default_period = default_period

        logger.info(
            "Initialized RSICalculator with default_period=%d",
            default_period,
        )

    @staticmethod
    def calculate_rsi_from_prices(close_prices: list[Decimal], period: int = 14) -> float:
        """Calculate RSI from a list of close prices using Wilder's smoothing.

        This is a pure calculation function that can be used independently
        of the data provider for testing or alternative data sources.

        Args:
            close_prices: List of closing prices (oldest first)
            period: RSI calculation period (default 14)

        Returns:
            RSI value from 0 to 100

        Raises:
            InsufficientDataError: If not enough price data (need period + 1)
        """
        required_points = period + 1
        if len(close_prices) < required_points:
            raise InsufficientDataError(
                required=required_points,
                available=len(close_prices),
                indicator="RSI",
            )

        # Calculate price changes (gains and losses)
        gains: list[float] = []
        losses: list[float] = []

        for i in range(1, len(close_prices)):
            change = float(close_prices[i] - close_prices[i - 1])
            if change > 0:
                gains.append(change)
                losses.append(0.0)
            else:
                gains.append(0.0)
                losses.append(abs(change))

        # Calculate initial average (simple average of first 'period' values)
        avg_gain = sum(gains[:period]) / period
        avg_loss = sum(losses[:period]) / period

        # Apply Wilder's smoothing for remaining values
        for i in range(period, len(gains)):
            avg_gain = ((avg_gain * (period - 1)) + gains[i]) / period
            avg_loss = ((avg_loss * (period - 1)) + losses[i]) / period

        # Calculate RS and RSI
        if avg_loss == 0:
            # No losses means RSI is 100 (extremely overbought)
            return 100.0

        rs = avg_gain / avg_loss
        rsi = 100.0 - (100.0 / (1.0 + rs))

        return rsi

    async def calculate_rsi(self, token: str, period: int = 14, timeframe: str = "4h") -> float:
        """Calculate RSI for a token.

        Fetches OHLCV data from the configured provider and calculates
        RSI using Wilder's smoothing method.

        Args:
            token: Token symbol (e.g., "WETH", "ETH")
            period: RSI calculation period (default 14)
            timeframe: OHLCV candle timeframe (default "4h")
                Supported: "1m", "5m", "15m", "1h", "4h", "1d"
                Note: 1m/5m/15m may return 30-min candles (CoinGecko limitation)

        Returns:
            RSI value from 0 to 100

        Raises:
            InsufficientDataError: If not enough historical data
            DataSourceError: If data cannot be fetched

        Example:
            # Default 4-hour candles
            rsi = await calculator.calculate_rsi("WETH", period=14)

            # 1-hour candles for shorter-term analysis
            rsi_1h = await calculator.calculate_rsi("WETH", period=14, timeframe="1h")

            # Daily candles for longer-term analysis
            rsi_1d = await calculator.calculate_rsi("WETH", period=14, timeframe="1d")
        """
        # Need at least period + 1 data points for RSI calculation
        # Request extra for buffer
        limit = period + 20

        logger.debug(
            "Calculating RSI for %s with period=%d, timeframe=%s (fetching %d candles)",
            token,
            period,
            timeframe,
            limit,
        )

        # Fetch OHLCV data
        ohlcv_data = await self._ohlcv_provider.get_ohlcv(
            token=token,
            quote="USD",
            timeframe=timeframe,
            limit=limit,
        )

        if not ohlcv_data:
            raise InsufficientDataError(
                required=period + 1,
                available=0,
                indicator="RSI",
            )

        # Extract close prices (already sorted oldest-first)
        # OHLCVCandle objects have a .close attribute (Decimal type)
        close_prices: list[Decimal] = [candle.close for candle in ohlcv_data]

        # Calculate RSI
        rsi = self.calculate_rsi_from_prices(close_prices, period)

        logger.debug(
            "Calculated RSI for %s: %.2f (period=%d, timeframe=%s, data_points=%d)",
            token,
            rsi,
            period,
            timeframe,
            len(close_prices),
        )

        return rsi

    def get_ohlcv_provider_health(self) -> dict[str, Any]:
        """Get health metrics from the OHLCV provider if available."""
        if hasattr(self._ohlcv_provider, "get_health_metrics"):
            return self._ohlcv_provider.get_health_metrics()
        return {}


# =============================================================================
# Exports
# =============================================================================


__all__ = [
    "RSICalculator",
    "CoinGeckoOHLCVProvider",
    "OHLCVData",
    "OHLCVCacheEntry",
    "OHLCVHealthMetrics",
]
