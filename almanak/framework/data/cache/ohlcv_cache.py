"""OHLCV SQLite Cache.

Provides persistent caching for OHLCV candlestick data using SQLite.
This cache helps avoid rate limiting from external APIs by storing
historical candle data locally.
"""

import sqlite3
from datetime import datetime
from decimal import Decimal
from pathlib import Path

from almanak.framework.data.interfaces import OHLCVCandle


class OHLCVCache:
    """SQLite-based cache for OHLCV candlestick data.

    Stores OHLCV candles in a local SQLite database to avoid repeated
    API calls for historical data. The cache key includes token, quote,
    timeframe, and chain for proper data isolation.

    Attributes:
        db_path: Path to the SQLite database file

    Example:
        cache = OHLCVCache("/path/to/cache.db")

        # Store candles
        candles = [OHLCVCandle(...), ...]
        cache.store_candles(candles, "ETH", "USD", "1h", "ethereum")

        # Retrieve candles
        cached = cache.get_candles(
            "ETH", "USD", "1h", "ethereum",
            start=datetime(2024, 1, 1),
            end=datetime(2024, 1, 31)
        )
    """

    def __init__(self, db_path: str | None = None) -> None:
        """Initialize the OHLCV cache.

        Args:
            db_path: Path to the SQLite database file. If None, uses
                     a default path in the user's home directory.
        """
        if db_path is None:
            cache_dir = Path.home() / ".almanak" / "cache"
            try:
                cache_dir.mkdir(parents=True, exist_ok=True)
                self.db_path = str(cache_dir / "ohlcv_cache.db")
            except OSError:
                fallback_dir = Path("/tmp/.almanak/cache")
                try:
                    fallback_dir.mkdir(parents=True, exist_ok=True)
                except OSError:
                    pass
                self.db_path = str(fallback_dir / "ohlcv_cache.db")
        else:
            self.db_path = db_path
            # Ensure parent directory exists
            Path(db_path).parent.mkdir(parents=True, exist_ok=True)

        self._init_db()

    def _init_db(self) -> None:
        """Initialize the database schema."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS ohlcv_candles (
                    token TEXT NOT NULL,
                    quote TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    chain TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    open TEXT NOT NULL,
                    high TEXT NOT NULL,
                    low TEXT NOT NULL,
                    close TEXT NOT NULL,
                    volume TEXT,
                    PRIMARY KEY (token, quote, timeframe, chain, timestamp)
                )
            """)
            # Index for efficient time-range queries
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_ohlcv_lookup
                ON ohlcv_candles (token, quote, timeframe, chain, timestamp)
            """)
            conn.commit()

    def get_candles(
        self,
        token: str,
        quote: str,
        timeframe: str,
        chain: str,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> list[OHLCVCandle]:
        """Retrieve cached OHLCV candles.

        Args:
            token: Token symbol (e.g., "ETH", "WETH")
            quote: Quote currency (e.g., "USD")
            timeframe: Candle timeframe (e.g., "1h", "1d")
            chain: Chain identifier (e.g., "ethereum", "arbitrum")
            start: Optional start time filter (inclusive)
            end: Optional end time filter (inclusive)

        Returns:
            List of OHLCVCandle objects sorted by timestamp ascending
        """
        query = """
            SELECT timestamp, open, high, low, close, volume
            FROM ohlcv_candles
            WHERE token = ? AND quote = ? AND timeframe = ? AND chain = ?
        """
        params: list[str] = [token, quote, timeframe, chain]

        if start is not None:
            query += " AND timestamp >= ?"
            params.append(start.isoformat())
        if end is not None:
            query += " AND timestamp <= ?"
            params.append(end.isoformat())

        query += " ORDER BY timestamp ASC"

        candles: list[OHLCVCandle] = []
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(query, params)
            for row in cursor.fetchall():
                timestamp_str, open_str, high_str, low_str, close_str, volume_str = row
                candles.append(
                    OHLCVCandle(
                        timestamp=datetime.fromisoformat(timestamp_str),
                        open=Decimal(open_str),
                        high=Decimal(high_str),
                        low=Decimal(low_str),
                        close=Decimal(close_str),
                        volume=Decimal(volume_str) if volume_str else None,
                    )
                )
        return candles

    def store_candles(
        self,
        candles: list[OHLCVCandle],
        token: str,
        quote: str,
        timeframe: str,
        chain: str,
    ) -> int:
        """Store OHLCV candles in the cache.

        Uses INSERT OR REPLACE to update existing candles with the same
        primary key (token, quote, timeframe, chain, timestamp).

        Args:
            candles: List of OHLCVCandle objects to store
            token: Token symbol (e.g., "ETH", "WETH")
            quote: Quote currency (e.g., "USD")
            timeframe: Candle timeframe (e.g., "1h", "1d")
            chain: Chain identifier (e.g., "ethereum", "arbitrum")

        Returns:
            Number of candles stored/updated
        """
        if not candles:
            return 0

        with sqlite3.connect(self.db_path) as conn:
            conn.executemany(
                """
                INSERT OR REPLACE INTO ohlcv_candles
                (token, quote, timeframe, chain, timestamp, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        token,
                        quote,
                        timeframe,
                        chain,
                        candle.timestamp.isoformat(),
                        str(candle.open),
                        str(candle.high),
                        str(candle.low),
                        str(candle.close),
                        str(candle.volume) if candle.volume is not None else None,
                    )
                    for candle in candles
                ],
            )
            conn.commit()
        return len(candles)

    def get_latest_timestamp(
        self,
        token: str,
        quote: str,
        timeframe: str,
        chain: str,
    ) -> datetime | None:
        """Get the timestamp of the most recent cached candle.

        This is useful for incremental fetching - only request candles
        newer than the latest cached one.

        Args:
            token: Token symbol (e.g., "ETH", "WETH")
            quote: Quote currency (e.g., "USD")
            timeframe: Candle timeframe (e.g., "1h", "1d")
            chain: Chain identifier (e.g., "ethereum", "arbitrum")

        Returns:
            The timestamp of the most recent candle, or None if no
            candles are cached for this combination.
        """
        query = """
            SELECT MAX(timestamp)
            FROM ohlcv_candles
            WHERE token = ? AND quote = ? AND timeframe = ? AND chain = ?
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(query, (token, quote, timeframe, chain))
            row = cursor.fetchone()
            if row and row[0]:
                return datetime.fromisoformat(row[0])
        return None

    def clear(
        self,
        token: str | None = None,
        quote: str | None = None,
        timeframe: str | None = None,
        chain: str | None = None,
    ) -> int:
        """Clear cached candles.

        Can clear all candles or filter by any combination of parameters.

        Args:
            token: Optional token filter
            quote: Optional quote filter
            timeframe: Optional timeframe filter
            chain: Optional chain filter

        Returns:
            Number of rows deleted
        """
        query = "DELETE FROM ohlcv_candles WHERE 1=1"
        params: list[str] = []

        if token is not None:
            query += " AND token = ?"
            params.append(token)
        if quote is not None:
            query += " AND quote = ?"
            params.append(quote)
        if timeframe is not None:
            query += " AND timeframe = ?"
            params.append(timeframe)
        if chain is not None:
            query += " AND chain = ?"
            params.append(chain)

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(query, params)
            conn.commit()
            return cursor.rowcount

    def count(
        self,
        token: str | None = None,
        quote: str | None = None,
        timeframe: str | None = None,
        chain: str | None = None,
    ) -> int:
        """Count cached candles.

        Args:
            token: Optional token filter
            quote: Optional quote filter
            timeframe: Optional timeframe filter
            chain: Optional chain filter

        Returns:
            Number of cached candles matching the filters
        """
        query = "SELECT COUNT(*) FROM ohlcv_candles WHERE 1=1"
        params: list[str] = []

        if token is not None:
            query += " AND token = ?"
            params.append(token)
        if quote is not None:
            query += " AND quote = ?"
            params.append(quote)
        if timeframe is not None:
            query += " AND timeframe = ?"
            params.append(timeframe)
        if chain is not None:
            query += " AND chain = ?"
            params.append(chain)

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(query, params)
            row = cursor.fetchone()
            return row[0] if row else 0
