"""OHLCV Module - Combines OHLCV provider with caching.

Provides the OHLCVModule class that wraps an OHLCVProvider with an OHLCVCache
for efficient historical candlestick data access with incremental fetching.
"""

import asyncio
import logging
from datetime import UTC, datetime, timedelta
from typing import Literal

import pandas as pd

from almanak.framework.data.cache.ohlcv_cache import OHLCVCache
from almanak.framework.data.interfaces import OHLCVCandle, OHLCVProvider, validate_timeframe

logger = logging.getLogger(__name__)

# Type alias for gap strategy
GapStrategy = Literal["nan", "ffill", "drop"]


class OHLCVModule:
    """OHLCV module that combines a provider with caching.

    This class wraps an OHLCVProvider with an OHLCVCache to provide efficient
    historical candlestick data access. It implements incremental fetching,
    only requesting candles newer than what's already cached.

    Attributes:
        provider: The underlying OHLCV data provider
        cache: The SQLite-based cache for storing candles

    Example:
        from almanak.framework.data.indicators.rsi import CoinGeckoOHLCVProvider
        from almanak.framework.data.cache.ohlcv_cache import OHLCVCache

        provider = CoinGeckoOHLCVProvider()
        cache = OHLCVCache()
        module = OHLCVModule(provider, cache)

        # Get OHLCV data as DataFrame
        df = module.get_ohlcv("ETH", timeframe="1h", limit=100)
        print(df.columns)  # timestamp, open, high, low, close, volume
    """

    def __init__(
        self,
        provider: OHLCVProvider,
        cache: OHLCVCache,
        chain: str = "ethereum",
    ) -> None:
        """Initialize the OHLCV module.

        Args:
            provider: The OHLCV data provider to fetch candles from
            cache: The SQLite cache for storing candles
            chain: Default chain identifier for cache keys
        """
        self.provider = provider
        self.cache = cache
        self.chain = chain

    def get_ohlcv(
        self,
        token: str,
        timeframe: str = "1h",
        limit: int = 100,
        quote: str = "USD",
        gap_strategy: GapStrategy = "nan",
    ) -> pd.DataFrame:
        """Get OHLCV data as a pandas DataFrame.

        Fetches candles from the cache first, then incrementally fetches
        only the newer candles from the provider. Returns a DataFrame
        with standard OHLCV columns.

        Args:
            token: Token symbol (e.g., "ETH", "WETH", "BTC")
            timeframe: Candle timeframe (1m, 5m, 15m, 1h, 4h, 1d)
            limit: Maximum number of candles to return
            quote: Quote currency (default "USD")
            gap_strategy: How to handle gaps in data:
                - 'nan': Fill gaps with NaN values (default)
                - 'ffill': Forward-fill gaps with last known values
                - 'drop': Remove gaps (returns only continuous data)

        Returns:
            pandas DataFrame with columns:
                - timestamp: datetime
                - open: float64
                - high: float64
                - low: float64
                - close: float64
                - volume: float64 (may contain NaN if unavailable)

            DataFrame.attrs includes metadata:
                - base: Token symbol
                - quote: Quote currency
                - timeframe: Candle timeframe
                - source: Provider source name
                - chain: Chain identifier
                - fetched_at: When the data was fetched

        Raises:
            ValueError: If timeframe is invalid
        """
        # Validate timeframe
        validate_timeframe(timeframe)

        # Run async fetch synchronously
        candles = asyncio.get_event_loop().run_until_complete(self._fetch_with_cache(token, quote, timeframe, limit))

        # Convert to DataFrame
        df = self._candles_to_dataframe(candles)

        # Detect and handle gaps
        if len(df) > 0:
            df = self._handle_gaps(df, token, timeframe, gap_strategy)

        # Set DataFrame attrs with metadata
        df.attrs = {
            "base": token,
            "quote": quote,
            "timeframe": timeframe,
            "source": self._get_source_name(),
            "chain": self.chain,
            "fetched_at": datetime.now(UTC).isoformat(),
        }

        return df

    async def _fetch_with_cache(
        self,
        token: str,
        quote: str,
        timeframe: str,
        limit: int,
    ) -> list[OHLCVCandle]:
        """Fetch candles with incremental caching.

        First checks the cache for existing candles. If not enough,
        fetches new candles from the provider starting after the
        latest cached candle.

        Args:
            token: Token symbol
            quote: Quote currency
            timeframe: Candle timeframe
            limit: Number of candles needed

        Returns:
            List of OHLCVCandle objects
        """
        # Check cache for latest timestamp
        latest_cached = self.cache.get_latest_timestamp(token, quote, timeframe, self.chain)

        # Calculate how far back we need data
        interval_seconds = self._timeframe_to_seconds(timeframe)
        start_time = datetime.now(UTC) - timedelta(seconds=interval_seconds * limit)

        # Get cached candles in the range we need
        cached_candles = self.cache.get_candles(token, quote, timeframe, self.chain, start=start_time)

        # If we have enough cached candles, return them
        if len(cached_candles) >= limit:
            return cached_candles[-limit:]

        # Determine how many new candles we need
        if latest_cached is not None:
            # Only fetch candles newer than what we have
            # Calculate how many candles we might be missing
            time_since_cached = (datetime.now(UTC) - latest_cached).total_seconds()
            candles_needed = max(
                int(time_since_cached / interval_seconds) + 1,
                limit - len(cached_candles),
            )
        else:
            # No cache, fetch the full limit
            candles_needed = limit

        # Fetch from provider
        new_candles = await self.provider.get_ohlcv(
            token=token,
            quote=quote,
            timeframe=timeframe,
            limit=candles_needed,
        )

        # Filter out candles we already have cached
        if latest_cached is not None:
            new_candles = [c for c in new_candles if c.timestamp > latest_cached]

        # Store new candles in cache
        if new_candles:
            self.cache.store_candles(new_candles, token, quote, timeframe, self.chain)

        # Get all candles from cache now (includes new ones)
        all_candles = self.cache.get_candles(token, quote, timeframe, self.chain, start=start_time)

        # Return the most recent 'limit' candles
        return all_candles[-limit:] if len(all_candles) > limit else all_candles

    def _candles_to_dataframe(self, candles: list[OHLCVCandle]) -> pd.DataFrame:
        """Convert a list of OHLCVCandle objects to a pandas DataFrame.

        Args:
            candles: List of OHLCVCandle objects

        Returns:
            DataFrame with columns: timestamp, open, high, low, close, volume
        """
        if not candles:
            # Return empty DataFrame with correct schema
            return pd.DataFrame(
                columns=["timestamp", "open", "high", "low", "close", "volume"],
            ).astype(
                {
                    "timestamp": "datetime64[ns]",
                    "open": "float64",
                    "high": "float64",
                    "low": "float64",
                    "close": "float64",
                    "volume": "float64",
                }
            )

        data = {
            "timestamp": [c.timestamp for c in candles],
            "open": [float(c.open) for c in candles],
            "high": [float(c.high) for c in candles],
            "low": [float(c.low) for c in candles],
            "close": [float(c.close) for c in candles],
            "volume": [float(c.volume) if c.volume is not None else float("nan") for c in candles],
        }

        df = pd.DataFrame(data)

        # Ensure correct dtypes
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df["open"] = df["open"].astype("float64")
        df["high"] = df["high"].astype("float64")
        df["low"] = df["low"].astype("float64")
        df["close"] = df["close"].astype("float64")
        df["volume"] = df["volume"].astype("float64")

        return df

    def _timeframe_to_seconds(self, timeframe: str) -> int:
        """Convert a timeframe string to seconds.

        Args:
            timeframe: Timeframe string (1m, 5m, 15m, 1h, 4h, 1d)

        Returns:
            Number of seconds in the timeframe
        """
        mapping = {
            "1m": 60,
            "5m": 300,
            "15m": 900,
            "1h": 3600,
            "4h": 14400,
            "1d": 86400,
        }
        return mapping.get(timeframe, 3600)  # Default to 1h

    def _handle_gaps(
        self,
        df: pd.DataFrame,
        token: str,
        timeframe: str,
        gap_strategy: GapStrategy,
    ) -> pd.DataFrame:
        """Detect and handle gaps in OHLCV data.

        Detects gaps by checking timestamp continuity based on the timeframe.
        Logs INFO when gaps are detected, WARNING for gaps > 24 hours.
        Applies the specified gap strategy to handle missing data.

        Args:
            df: DataFrame with timestamp column
            token: Token symbol for logging
            timeframe: Candle timeframe for gap detection
            gap_strategy: How to handle gaps ('nan', 'ffill', 'drop')

        Returns:
            DataFrame with gaps handled according to the strategy
        """
        if len(df) < 2:
            return df

        # Calculate expected interval
        interval_seconds = self._timeframe_to_seconds(timeframe)
        expected_interval = timedelta(seconds=interval_seconds)

        # Detect gaps by checking timestamp differences
        df = df.sort_values("timestamp").reset_index(drop=True)
        timestamps = df["timestamp"]
        time_diffs = timestamps.diff()

        # Find gap indices (where time diff exceeds expected interval + small tolerance)
        tolerance = timedelta(seconds=interval_seconds * 0.1)  # 10% tolerance
        gap_mask = time_diffs > (expected_interval + tolerance)
        gap_indices = df.index[gap_mask].tolist()

        if not gap_indices:
            return df  # No gaps detected

        # Calculate gap statistics
        total_missing = 0
        max_gap_hours = 0.0

        for idx in gap_indices:
            if idx > 0:
                actual_gap = time_diffs.iloc[idx]
                if pd.notna(actual_gap):
                    gap_seconds = actual_gap.total_seconds()
                    missing_candles = int(gap_seconds / interval_seconds) - 1
                    total_missing += missing_candles
                    gap_hours = gap_seconds / 3600
                    max_gap_hours = max(max_gap_hours, gap_hours)

        # Log gap detection
        if max_gap_hours > 24:
            logger.warning(
                "OHLCV gap detected: %s %s missing %d candles (max gap: %.1f hours)",
                token,
                timeframe,
                total_missing,
                max_gap_hours,
            )
        else:
            logger.info(
                "OHLCV gap detected: %s %s missing %d candles",
                token,
                timeframe,
                total_missing,
            )

        # Apply gap strategy
        if gap_strategy == "drop":
            # Return only the longest continuous segment
            return self._get_longest_continuous_segment(df, gap_indices)
        elif gap_strategy == "ffill":
            # Fill gaps with forward-filled values
            return self._fill_gaps_ffill(df, timeframe)
        else:  # gap_strategy == "nan"
            # Fill gaps with NaN values
            return self._fill_gaps_nan(df, timeframe)

    def _get_longest_continuous_segment(
        self,
        df: pd.DataFrame,
        gap_indices: list[int],
    ) -> pd.DataFrame:
        """Get the longest continuous segment of data without gaps.

        Args:
            df: DataFrame sorted by timestamp
            gap_indices: Indices where gaps start

        Returns:
            DataFrame containing only the longest continuous segment
        """
        if not gap_indices:
            return df

        # Segment boundaries
        boundaries = [0] + gap_indices + [len(df)]
        longest_segment = df.iloc[0:0]  # Empty DataFrame with same structure

        for i in range(len(boundaries) - 1):
            start = boundaries[i]
            end = boundaries[i + 1]
            segment = df.iloc[start:end]
            if len(segment) > len(longest_segment):
                longest_segment = segment

        return longest_segment.reset_index(drop=True)

    def _fill_gaps_nan(self, df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
        """Fill gaps with NaN values for missing timestamps.

        Args:
            df: DataFrame sorted by timestamp
            timeframe: Candle timeframe

        Returns:
            DataFrame with NaN rows for missing timestamps
        """
        if len(df) < 2:
            return df

        interval_seconds = self._timeframe_to_seconds(timeframe)
        timedelta(seconds=interval_seconds)

        # Create a complete timestamp range
        start_ts = df["timestamp"].min()
        end_ts = df["timestamp"].max()

        # Generate expected timestamps
        expected_timestamps = pd.date_range(start=start_ts, end=end_ts, freq=f"{interval_seconds}s")

        # Reindex to include missing timestamps (fills with NaN)
        df = df.set_index("timestamp")
        df = df.reindex(expected_timestamps)
        df = df.reset_index()
        df = df.rename(columns={"index": "timestamp"})

        return df

    def _fill_gaps_ffill(self, df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
        """Fill gaps with forward-filled values.

        Args:
            df: DataFrame sorted by timestamp
            timeframe: Candle timeframe

        Returns:
            DataFrame with forward-filled values for missing timestamps
        """
        # First fill with NaN, then forward-fill
        df = self._fill_gaps_nan(df, timeframe)
        df = df.ffill()
        return df

    def _get_source_name(self) -> str:
        """Get the source name from the provider.

        Returns:
            Source name string, or "unknown" if not available
        """
        # Try to get source_name from provider
        if hasattr(self.provider, "source_name"):
            return self.provider.source_name
        return "unknown"
