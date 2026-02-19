"""Data Cache with SQLite backend.

Provides general-purpose persistent caching for OHLCV and other data
types using SQLite as the default backend.

Features:
    - OHLCV data storage with token/timestamp/interval composite key
    - TTL-based cache invalidation
    - Batch operations for efficient bulk writes
    - Cache warming for pre-loading data ranges
    - Generic key-value storage for other data types
"""

import json
import logging
import sqlite3
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class CacheKey:
    """Cache key for OHLCV data.

    Uniquely identifies cached data by token, timestamp, and interval.

    Attributes:
        token: Token symbol (e.g., "ETH", "WETH", "BTC")
        timestamp: Timestamp of the data point
        interval: Time interval/timeframe (e.g., "1m", "5m", "1h", "1d")
    """

    token: str
    timestamp: datetime
    interval: str

    def __hash__(self) -> int:
        """Make CacheKey hashable for use in dicts."""
        return hash((self.token, self.timestamp.isoformat(), self.interval))

    def __eq__(self, other: object) -> bool:
        """Check equality with another CacheKey."""
        if not isinstance(other, CacheKey):
            return False
        return self.token == other.token and self.timestamp == other.timestamp and self.interval == other.interval


@dataclass
class OHLCVData:
    """OHLCV data for caching.

    Represents a single OHLCV candlestick with all values as Decimal.

    Attributes:
        open: Opening price
        high: Highest price during the period
        low: Lowest price during the period
        close: Closing price
        volume: Trading volume (optional)
    """

    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal | None = None

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return {
            "open": str(self.open),
            "high": str(self.high),
            "low": str(self.low),
            "close": str(self.close),
            "volume": str(self.volume) if self.volume is not None else None,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "OHLCVData":
        """Create OHLCVData from dictionary."""
        return cls(
            open=Decimal(data["open"]),
            high=Decimal(data["high"]),
            low=Decimal(data["low"]),
            close=Decimal(data["close"]),
            volume=Decimal(data["volume"]) if data.get("volume") is not None else None,
        )


@dataclass
class CacheStats:
    """Statistics about cache usage.

    Attributes:
        hits: Number of cache hits
        misses: Number of cache misses
        expired: Number of expired entries encountered
        total_entries: Total number of entries in cache
    """

    hits: int = 0
    misses: int = 0
    expired: int = 0
    total_entries: int = 0

    def hit_rate(self) -> float:
        """Calculate cache hit rate."""
        total = self.hits + self.misses
        return self.hits / total if total > 0 else 0.0

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "hits": self.hits,
            "misses": self.misses,
            "expired": self.expired,
            "total_entries": self.total_entries,
            "hit_rate": self.hit_rate(),
        }


class DataCache:
    """General-purpose data cache with SQLite backend.

    Provides persistent caching for OHLCV data with token, timestamp,
    and interval as the composite key. Supports both get/set operations
    and batch operations.

    The cache uses SQLite for reliability and atomic operations.

    Features:
        - TTL-based invalidation: Entries older than TTL are considered expired
        - Cache warming: Pre-fetch data for a date range using a provider
        - Hit rate tracking: Monitor cache performance

    Attributes:
        db_path: Path to the SQLite database file
        ttl_seconds: Time-to-live in seconds (0 = no expiration)

    Example:
        # Create cache with 1-hour TTL
        cache = DataCache("/path/to/cache.db", ttl_seconds=3600)

        # Store data using set()
        key = CacheKey(token="ETH", timestamp=datetime(2024, 1, 1), interval="1h")
        data = OHLCVData(open=Decimal("3000"), high=Decimal("3100"),
                         low=Decimal("2950"), close=Decimal("3050"))
        cache.set(key, data)

        # Retrieve data using get() - returns None if expired
        result = cache.get(key)
        if result is not None:
            print(f"Close price: {result.close}")

        # Pre-warm cache for a date range
        await cache.warm_cache(
            tokens=["ETH", "BTC"],
            start_date=datetime(2024, 1, 1),
            end_date=datetime(2024, 1, 31),
            provider=my_data_provider,
            interval="1h",
        )
    """

    def __init__(
        self,
        db_path: str | None = None,
        ttl_seconds: int = 0,
    ) -> None:
        """Initialize the data cache.

        Args:
            db_path: Path to the SQLite database file. If None, uses
                     a default path in ~/.almanak/cache/data_cache.db
                     Use ":memory:" for an in-memory database (useful for testing).
            ttl_seconds: Time-to-live in seconds. Entries older than this are
                        considered expired and will not be returned. Set to 0
                        to disable TTL (entries never expire). Default: 0.
        """
        self._ttl_seconds = ttl_seconds
        self._stats = CacheStats()
        self._is_memory = db_path == ":memory:"
        self._memory_conn: sqlite3.Connection | None = None

        if db_path is None:
            cache_dir = Path.home() / ".almanak" / "cache"
            cache_dir.mkdir(parents=True, exist_ok=True)
            self.db_path = str(cache_dir / "data_cache.db")
        elif self._is_memory:
            self.db_path = ":memory:"
            # For in-memory, create persistent connection immediately
            self._memory_conn = sqlite3.connect(":memory:")
        else:
            self.db_path = db_path
            # Ensure parent directory exists
            Path(db_path).parent.mkdir(parents=True, exist_ok=True)

        self._init_db()

    @contextmanager
    def _connection(self) -> Generator[sqlite3.Connection, None, None]:
        """Context manager for database connections.

        For in-memory databases, uses the persistent connection.
        For file-based databases, creates and closes a new connection.

        Yields:
            SQLite connection object
        """
        if self._is_memory:
            if self._memory_conn is None:
                self._memory_conn = sqlite3.connect(":memory:")
            yield self._memory_conn
        else:
            conn = sqlite3.connect(self.db_path)
            try:
                yield conn
            finally:
                conn.close()

    def _init_db(self) -> None:
        """Initialize the database schema."""
        with self._connection() as conn:
            # OHLCV data table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS ohlcv_data (
                    token TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    interval TEXT NOT NULL,
                    open TEXT NOT NULL,
                    high TEXT NOT NULL,
                    low TEXT NOT NULL,
                    close TEXT NOT NULL,
                    volume TEXT,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (token, timestamp, interval)
                )
            """)
            # Index for efficient queries
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_ohlcv_lookup
                ON ohlcv_data (token, interval, timestamp)
            """)
            # Generic key-value cache for other data types
            conn.execute("""
                CREATE TABLE IF NOT EXISTS kv_cache (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
            """)
            conn.commit()

    @property
    def ttl_seconds(self) -> int:
        """Get the cache TTL in seconds."""
        return self._ttl_seconds

    @ttl_seconds.setter
    def ttl_seconds(self, value: int) -> None:
        """Set the cache TTL in seconds."""
        self._ttl_seconds = value

    @property
    def stats(self) -> CacheStats:
        """Get cache statistics."""
        self._stats.total_entries = self.count()
        return self._stats

    def reset_stats(self) -> None:
        """Reset cache statistics."""
        self._stats = CacheStats()

    def _is_expired(self, created_at_str: str) -> bool:
        """Check if an entry is expired based on TTL.

        Args:
            created_at_str: ISO format timestamp when entry was created

        Returns:
            True if entry is expired, False otherwise
        """
        if self._ttl_seconds <= 0:
            return False

        created_at = datetime.fromisoformat(created_at_str)
        age = datetime.utcnow() - created_at
        return age.total_seconds() > self._ttl_seconds

    def get(self, key: CacheKey) -> OHLCVData | None:
        """Retrieve OHLCV data from the cache.

        Returns None if the entry is not found or if TTL has expired.

        Args:
            key: CacheKey with token, timestamp, and interval

        Returns:
            OHLCVData if found and not expired, None otherwise
        """
        query = """
            SELECT open, high, low, close, volume, created_at
            FROM ohlcv_data
            WHERE token = ? AND timestamp = ? AND interval = ?
        """
        with self._connection() as conn:
            cursor = conn.execute(query, (key.token, key.timestamp.isoformat(), key.interval))
            row = cursor.fetchone()
            if row is None:
                self._stats.misses += 1
                return None

            open_str, high_str, low_str, close_str, volume_str, created_at_str = row

            # Check TTL
            if self._is_expired(created_at_str):
                self._stats.expired += 1
                self._stats.misses += 1
                logger.debug(
                    "Cache entry expired for %s at %s (TTL: %ds)",
                    key.token,
                    key.timestamp,
                    self._ttl_seconds,
                )
                return None

            self._stats.hits += 1
            return OHLCVData(
                open=Decimal(open_str),
                high=Decimal(high_str),
                low=Decimal(low_str),
                close=Decimal(close_str),
                volume=Decimal(volume_str) if volume_str else None,
            )

    def set(self, key: CacheKey, data: OHLCVData) -> None:
        """Store OHLCV data in the cache.

        Uses INSERT OR REPLACE to update existing entries with the
        same key (token, timestamp, interval).

        Args:
            key: CacheKey with token, timestamp, and interval
            data: OHLCVData to cache
        """
        with self._connection() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO ohlcv_data
                (token, timestamp, interval, open, high, low, close, volume, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    key.token,
                    key.timestamp.isoformat(),
                    key.interval,
                    str(data.open),
                    str(data.high),
                    str(data.low),
                    str(data.close),
                    str(data.volume) if data.volume is not None else None,
                    datetime.utcnow().isoformat(),
                ),
            )
            conn.commit()

    def get_range(
        self,
        token: str,
        interval: str,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> list[tuple[datetime, OHLCVData]]:
        """Retrieve a range of OHLCV data from the cache.

        Args:
            token: Token symbol (e.g., "ETH", "WETH")
            interval: Time interval (e.g., "1h", "1d")
            start: Optional start time filter (inclusive)
            end: Optional end time filter (inclusive)

        Returns:
            List of (timestamp, OHLCVData) tuples sorted by timestamp
        """
        query = """
            SELECT timestamp, open, high, low, close, volume
            FROM ohlcv_data
            WHERE token = ? AND interval = ?
        """
        params: list[str] = [token, interval]

        if start is not None:
            query += " AND timestamp >= ?"
            params.append(start.isoformat())
        if end is not None:
            query += " AND timestamp <= ?"
            params.append(end.isoformat())

        query += " ORDER BY timestamp ASC"

        results: list[tuple[datetime, OHLCVData]] = []
        with self._connection() as conn:
            cursor = conn.execute(query, params)
            for row in cursor.fetchall():
                timestamp_str, open_str, high_str, low_str, close_str, volume_str = row
                results.append(
                    (
                        datetime.fromisoformat(timestamp_str),
                        OHLCVData(
                            open=Decimal(open_str),
                            high=Decimal(high_str),
                            low=Decimal(low_str),
                            close=Decimal(close_str),
                            volume=Decimal(volume_str) if volume_str else None,
                        ),
                    )
                )
        return results

    def set_batch(self, items: list[tuple[CacheKey, OHLCVData]]) -> int:
        """Store multiple OHLCV data entries in a single transaction.

        Args:
            items: List of (CacheKey, OHLCVData) tuples

        Returns:
            Number of entries stored
        """
        if not items:
            return 0

        now = datetime.utcnow().isoformat()
        with self._connection() as conn:
            conn.executemany(
                """
                INSERT OR REPLACE INTO ohlcv_data
                (token, timestamp, interval, open, high, low, close, volume, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        key.token,
                        key.timestamp.isoformat(),
                        key.interval,
                        str(data.open),
                        str(data.high),
                        str(data.low),
                        str(data.close),
                        str(data.volume) if data.volume is not None else None,
                        now,
                    )
                    for key, data in items
                ],
            )
            conn.commit()
        return len(items)

    def delete(self, key: CacheKey) -> bool:
        """Delete a cached entry.

        Args:
            key: CacheKey identifying the entry to delete

        Returns:
            True if entry was deleted, False if not found
        """
        with self._connection() as conn:
            cursor = conn.execute(
                """
                DELETE FROM ohlcv_data
                WHERE token = ? AND timestamp = ? AND interval = ?
                """,
                (key.token, key.timestamp.isoformat(), key.interval),
            )
            conn.commit()
            return cursor.rowcount > 0

    def clear(
        self,
        token: str | None = None,
        interval: str | None = None,
    ) -> int:
        """Clear cached data.

        Can clear all data or filter by token and/or interval.

        Args:
            token: Optional token filter
            interval: Optional interval filter

        Returns:
            Number of rows deleted
        """
        query = "DELETE FROM ohlcv_data WHERE 1=1"
        params: list[str] = []

        if token is not None:
            query += " AND token = ?"
            params.append(token)
        if interval is not None:
            query += " AND interval = ?"
            params.append(interval)

        with self._connection() as conn:
            cursor = conn.execute(query, params)
            conn.commit()
            return cursor.rowcount

    def count(
        self,
        token: str | None = None,
        interval: str | None = None,
    ) -> int:
        """Count cached entries.

        Args:
            token: Optional token filter
            interval: Optional interval filter

        Returns:
            Number of cached entries matching the filters
        """
        query = "SELECT COUNT(*) FROM ohlcv_data WHERE 1=1"
        params: list[str] = []

        if token is not None:
            query += " AND token = ?"
            params.append(token)
        if interval is not None:
            query += " AND interval = ?"
            params.append(interval)

        with self._connection() as conn:
            cursor = conn.execute(query, params)
            row = cursor.fetchone()
            return row[0] if row else 0

    def contains(self, key: CacheKey, check_ttl: bool = True) -> bool:
        """Check if a key exists in the cache.

        Args:
            key: CacheKey to check
            check_ttl: If True, also checks if entry has expired (default: True)

        Returns:
            True if key exists (and not expired if check_ttl), False otherwise
        """
        query = """
            SELECT created_at FROM ohlcv_data
            WHERE token = ? AND timestamp = ? AND interval = ?
            LIMIT 1
        """
        with self._connection() as conn:
            cursor = conn.execute(query, (key.token, key.timestamp.isoformat(), key.interval))
            row = cursor.fetchone()
            if row is None:
                return False

            if check_ttl and self._is_expired(row[0]):
                return False

            return True

    # Key-value methods for generic caching

    def get_kv(self, key: str) -> str | None:
        """Retrieve a value from the key-value cache.

        Args:
            key: String key

        Returns:
            Cached value as string, or None if not found
        """
        with self._connection() as conn:
            cursor = conn.execute("SELECT value FROM kv_cache WHERE key = ?", (key,))
            row = cursor.fetchone()
            return row[0] if row else None

    def set_kv(self, key: str, value: str) -> None:
        """Store a key-value pair in the cache.

        Args:
            key: String key
            value: String value
        """
        with self._connection() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO kv_cache (key, value, created_at)
                VALUES (?, ?, ?)
                """,
                (key, value, datetime.utcnow().isoformat()),
            )
            conn.commit()

    def get_json(self, key: str) -> dict | list | None:
        """Retrieve a JSON value from the key-value cache.

        Args:
            key: String key

        Returns:
            Deserialized JSON value, or None if not found
        """
        value = self.get_kv(key)
        if value is None:
            return None
        return json.loads(value)

    def set_json(self, key: str, value: dict | list) -> None:
        """Store a JSON value in the key-value cache.

        Args:
            key: String key
            value: Dictionary or list to store as JSON
        """
        self.set_kv(key, json.dumps(value))

    # TTL and cache invalidation methods

    def invalidate_expired(self) -> int:
        """Remove all expired entries from the cache.

        This method scans the cache and deletes entries older than TTL.
        Only has effect when TTL is set (> 0).

        Returns:
            Number of entries deleted
        """
        if self._ttl_seconds <= 0:
            return 0

        cutoff = datetime.utcnow() - timedelta(seconds=self._ttl_seconds)
        cutoff_str = cutoff.isoformat()

        with self._connection() as conn:
            # Delete expired OHLCV entries
            cursor = conn.execute(
                "DELETE FROM ohlcv_data WHERE created_at < ?",
                (cutoff_str,),
            )
            ohlcv_deleted = cursor.rowcount

            # Delete expired KV entries
            cursor = conn.execute(
                "DELETE FROM kv_cache WHERE created_at < ?",
                (cutoff_str,),
            )
            kv_deleted = cursor.rowcount

            conn.commit()

        total_deleted = ohlcv_deleted + kv_deleted
        if total_deleted > 0:
            logger.info(
                "Invalidated %d expired entries (TTL: %ds)",
                total_deleted,
                self._ttl_seconds,
            )
        return total_deleted

    def get_expired_count(self) -> int:
        """Count expired entries in the cache.

        Returns:
            Number of expired entries
        """
        if self._ttl_seconds <= 0:
            return 0

        cutoff = datetime.utcnow() - timedelta(seconds=self._ttl_seconds)
        cutoff_str = cutoff.isoformat()

        with self._connection() as conn:
            cursor = conn.execute(
                "SELECT COUNT(*) FROM ohlcv_data WHERE created_at < ?",
                (cutoff_str,),
            )
            ohlcv_expired = cursor.fetchone()[0] or 0

            cursor = conn.execute(
                "SELECT COUNT(*) FROM kv_cache WHERE created_at < ?",
                (cutoff_str,),
            )
            kv_expired = cursor.fetchone()[0] or 0

        return ohlcv_expired + kv_expired

    # Cache warming methods

    async def warm_cache(
        self,
        tokens: list[str],
        start_date: datetime,
        end_date: datetime,
        provider: Any,
        interval: str = "1h",
        interval_seconds: int = 3600,
    ) -> int:
        """Pre-fetch OHLCV data from a provider and store in cache.

        This method fetches data for the specified tokens and date range
        from the given data provider and stores it in the cache. This is
        useful for pre-loading data before a backtest to avoid latency
        during execution.

        Args:
            tokens: List of token symbols to pre-fetch (e.g., ["ETH", "BTC"])
            start_date: Start of the date range (inclusive)
            end_date: End of the date range (inclusive)
            provider: Data provider implementing get_ohlcv(token, start, end, interval_seconds)
            interval: Interval string for cache key (e.g., "1h", "1d")
            interval_seconds: Interval in seconds for provider API (default: 3600 = 1 hour)

        Returns:
            Number of data points cached

        Example:
            from almanak.framework.backtesting.pnl.providers import CoinGeckoDataProvider

            provider = CoinGeckoDataProvider(api_key="...")
            cache = DataCache(ttl_seconds=86400)  # 24-hour TTL

            # Pre-warm cache for a month of hourly data
            count = await cache.warm_cache(
                tokens=["ETH", "BTC", "ARB"],
                start_date=datetime(2024, 1, 1),
                end_date=datetime(2024, 1, 31),
                provider=provider,
                interval="1h",
            )
            print(f"Cached {count} data points")
        """
        total_cached = 0

        for token in tokens:
            try:
                logger.info(
                    "Warming cache for %s from %s to %s (%s interval)",
                    token,
                    start_date,
                    end_date,
                    interval,
                )

                # Fetch OHLCV data from provider
                ohlcv_data = await provider.get_ohlcv(token, start_date, end_date, interval_seconds)

                if not ohlcv_data:
                    logger.warning(
                        "No OHLCV data returned for %s from provider",
                        token,
                    )
                    continue

                # Convert to cache format and batch insert
                items: list[tuple[CacheKey, OHLCVData]] = []
                for ohlcv in ohlcv_data:
                    key = CacheKey(
                        token=token.upper(),
                        timestamp=ohlcv.timestamp,
                        interval=interval,
                    )
                    data = OHLCVData(
                        open=ohlcv.open,
                        high=ohlcv.high,
                        low=ohlcv.low,
                        close=ohlcv.close,
                        volume=ohlcv.volume if hasattr(ohlcv, "volume") else None,
                    )
                    items.append((key, data))

                cached_count = self.set_batch(items)
                total_cached += cached_count

                logger.info(
                    "Cached %d data points for %s",
                    cached_count,
                    token,
                )

            except Exception as e:
                logger.error(
                    "Failed to warm cache for %s: %s",
                    token,
                    e,
                )

        logger.info(
            "Cache warming complete: %d total data points cached for %d tokens",
            total_cached,
            len(tokens),
        )
        return total_cached

    def warm_cache_sync(
        self,
        tokens: list[str],
        start_date: datetime,
        end_date: datetime,
        data: dict[str, list[tuple[datetime, OHLCVData]]],
        interval: str = "1h",
    ) -> int:
        """Synchronous cache warming with pre-loaded data.

        This is a synchronous alternative to warm_cache() for cases where
        the data is already available or the provider only supports sync.

        Args:
            tokens: List of token symbols to cache
            start_date: Start of the date range (for logging)
            end_date: End of the date range (for logging)
            data: Pre-loaded data as dict mapping token -> list of (timestamp, OHLCVData)
            interval: Interval string for cache key (e.g., "1h", "1d")

        Returns:
            Number of data points cached

        Example:
            # Pre-load data
            data = {
                "ETH": [(datetime(2024, 1, 1, 0), eth_ohlcv_0), ...],
                "BTC": [(datetime(2024, 1, 1, 0), btc_ohlcv_0), ...],
            }

            # Warm cache
            count = cache.warm_cache_sync(
                tokens=["ETH", "BTC"],
                start_date=datetime(2024, 1, 1),
                end_date=datetime(2024, 1, 31),
                data=data,
            )
        """
        total_cached = 0

        for token in tokens:
            token_upper = token.upper()
            if token_upper not in data and token not in data:
                logger.warning("No data provided for token %s", token)
                continue

            token_data = data.get(token_upper) or data.get(token, [])

            logger.info(
                "Warming cache for %s from %s to %s (%d points)",
                token,
                start_date,
                end_date,
                len(token_data),
            )

            items: list[tuple[CacheKey, OHLCVData]] = []
            for timestamp, ohlcv in token_data:
                key = CacheKey(
                    token=token_upper,
                    timestamp=timestamp,
                    interval=interval,
                )
                items.append((key, ohlcv))

            cached_count = self.set_batch(items)
            total_cached += cached_count

        logger.info(
            "Cache warming complete: %d total data points cached",
            total_cached,
        )
        return total_cached

    def close(self) -> None:
        """Close the cache and release resources.

        For file-based caches, this is a no-op since connections are
        opened/closed per operation. For in-memory caches, closes the
        persistent connection.
        """
        if self._is_memory and self._memory_conn is not None:
            self._memory_conn.close()
            self._memory_conn = None
