"""Gas Price Provider for historical and current gas prices.

This module provides gas price data for backtesting and cost estimation.
It supports multiple data sources including Etherscan API for historical
gas data and direct RPC queries for current prices.

Key Features:
    - Fetches historical gas prices from Etherscan Gas Tracker API
    - Supports multiple chains (Ethereum, Arbitrum, Base, etc.)
    - Provides both base fee and priority fee data
    - Implements caching to minimize API calls
    - Handles interpolation for missing timestamps

Example:
    from almanak.framework.backtesting.pnl.providers.gas import (
        EtherscanGasPriceProvider,
        GasPrice,
    )
    from datetime import datetime

    provider = EtherscanGasPriceProvider(api_key="your-etherscan-key")

    # Get current gas price
    gas = await provider.get_gas_price(chain="ethereum")
    print(f"Base fee: {gas.base_fee_gwei} gwei, Priority: {gas.priority_fee_gwei} gwei")

    # Get historical gas price
    historical_gas = await provider.get_gas_price(
        timestamp=datetime(2024, 6, 15, 12, 0),
        chain="ethereum",
    )
"""

import logging
import os
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

import aiohttp

from ..types import DataConfidence

if TYPE_CHECKING:
    from almanak.framework.backtesting.config import BacktestDataConfig

logger = logging.getLogger(__name__)


# =============================================================================
# Gas Price Data Models
# =============================================================================


@dataclass
class GasPrice:
    """Gas price data at a specific point in time.

    Represents both the base fee and priority fee (tip) for EIP-1559
    compatible chains. For non-EIP-1559 chains, only gas_price is used.

    Attributes:
        timestamp: When this gas price was recorded
        chain: Chain identifier (e.g., "ethereum", "arbitrum")
        base_fee_gwei: EIP-1559 base fee in gwei (None for legacy chains)
        priority_fee_gwei: EIP-1559 priority fee (tip) in gwei
        gas_price_gwei: Legacy gas price in gwei (base + priority for EIP-1559)
        source: Data source identifier (e.g., "etherscan", "rpc", "archive_rpc")
        confidence: Data confidence level (HIGH for real API/RPC data,
            MEDIUM for estimates, LOW for fallback values)
    """

    timestamp: datetime
    chain: str
    base_fee_gwei: Decimal | None = None
    priority_fee_gwei: Decimal | None = None
    gas_price_gwei: Decimal | None = None
    source: str = "unknown"
    confidence: DataConfidence = DataConfidence.MEDIUM

    def __post_init__(self) -> None:
        """Ensure at least one gas price value is set."""
        if self.base_fee_gwei is None and self.priority_fee_gwei is None and self.gas_price_gwei is None:
            raise ValueError("At least one of base_fee_gwei, priority_fee_gwei, or gas_price_gwei must be set")

    @property
    def effective_gas_price_gwei(self) -> Decimal:
        """Get the effective gas price (base + priority or legacy price).

        Returns:
            Effective gas price in gwei for transaction cost estimation.
        """
        if self.gas_price_gwei is not None:
            return self.gas_price_gwei
        base = self.base_fee_gwei or Decimal("0")
        priority = self.priority_fee_gwei or Decimal("0")
        return base + priority

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "timestamp": self.timestamp.isoformat(),
            "chain": self.chain,
            "base_fee_gwei": str(self.base_fee_gwei) if self.base_fee_gwei is not None else None,
            "priority_fee_gwei": str(self.priority_fee_gwei) if self.priority_fee_gwei is not None else None,
            "gas_price_gwei": str(self.gas_price_gwei) if self.gas_price_gwei is not None else None,
            "source": self.source,
            "confidence": self.confidence.value,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "GasPrice":
        """Deserialize from dictionary."""
        confidence_str = data.get("confidence", "medium")
        confidence = DataConfidence(confidence_str) if confidence_str else DataConfidence.MEDIUM
        return cls(
            timestamp=datetime.fromisoformat(data["timestamp"]),
            chain=data["chain"],
            base_fee_gwei=Decimal(data["base_fee_gwei"]) if data.get("base_fee_gwei") else None,
            priority_fee_gwei=Decimal(data["priority_fee_gwei"]) if data.get("priority_fee_gwei") else None,
            gas_price_gwei=Decimal(data["gas_price_gwei"]) if data.get("gas_price_gwei") else None,
            source=data.get("source", "unknown"),
            confidence=confidence,
        )


@dataclass
class GasPriceCache:
    """Cache for gas price data with TTL support.

    Provides in-memory caching for gas prices with configurable TTL.
    Supports both point queries and range queries.

    Attributes:
        data: Dictionary mapping (chain, timestamp_rounded) to GasPrice
        ttl_seconds: Time-to-live for cached entries (default 60 seconds)
    """

    data: dict[tuple[str, str], GasPrice] = field(default_factory=dict)
    ttl_seconds: int = 60
    _fetch_times: dict[tuple[str, str], datetime] = field(default_factory=dict)

    def _make_key(self, chain: str, timestamp: datetime) -> tuple[str, str]:
        """Create cache key from chain and timestamp (rounded to minute)."""
        rounded = timestamp.replace(second=0, microsecond=0)
        return (chain.lower(), rounded.isoformat())

    def get(self, chain: str, timestamp: datetime) -> GasPrice | None:
        """Get cached gas price if available and not expired.

        Args:
            chain: Chain identifier
            timestamp: Target timestamp

        Returns:
            GasPrice if found and not expired, None otherwise
        """
        key = self._make_key(chain, timestamp)
        if key not in self.data:
            return None

        fetch_time = self._fetch_times.get(key)
        if fetch_time is not None:
            age = (datetime.now(UTC) - fetch_time).total_seconds()
            if age > self.ttl_seconds:
                # Expired, remove from cache
                del self.data[key]
                del self._fetch_times[key]
                return None

        return self.data[key]

    def set(self, gas_price: GasPrice) -> None:
        """Cache a gas price entry.

        Args:
            gas_price: GasPrice to cache
        """
        key = self._make_key(gas_price.chain, gas_price.timestamp)
        self.data[key] = gas_price
        self._fetch_times[key] = datetime.now(UTC)

    def set_batch(self, gas_prices: list[GasPrice]) -> int:
        """Cache multiple gas price entries.

        Args:
            gas_prices: List of GasPrice objects to cache

        Returns:
            Number of entries cached
        """
        now = datetime.now(UTC)
        for gp in gas_prices:
            key = self._make_key(gp.chain, gp.timestamp)
            self.data[key] = gp
            self._fetch_times[key] = now
        return len(gas_prices)

    def clear(self, chain: str | None = None) -> int:
        """Clear cached entries.

        Args:
            chain: If provided, only clear entries for this chain

        Returns:
            Number of entries cleared
        """
        if chain is None:
            count = len(self.data)
            self.data.clear()
            self._fetch_times.clear()
            return count

        chain_lower = chain.lower()
        keys_to_remove = [k for k in self.data if k[0] == chain_lower]
        for key in keys_to_remove:
            del self.data[key]
            self._fetch_times.pop(key, None)
        return len(keys_to_remove)

    def get_nearest(
        self,
        chain: str,
        timestamp: datetime,
        max_delta_seconds: int = 300,
    ) -> GasPrice | None:
        """Get the nearest cached gas price within a time window.

        Args:
            chain: Chain identifier
            timestamp: Target timestamp
            max_delta_seconds: Maximum time difference to accept (default 5 min)

        Returns:
            Nearest GasPrice within the window, or None if not found
        """
        chain_lower = chain.lower()
        best_match: GasPrice | None = None
        best_delta: float = float("inf")

        for (cached_chain, _), gas_price in self.data.items():
            if cached_chain != chain_lower:
                continue

            delta = abs((gas_price.timestamp - timestamp).total_seconds())
            if delta < best_delta and delta <= max_delta_seconds:
                best_delta = delta
                best_match = gas_price

        return best_match


# =============================================================================
# Persistent Gas Price Cache with SQLite Backend
# =============================================================================


class GasPriceDataCache:
    """Persistent gas price cache with SQLite backend and interpolation support.

    Extends the basic in-memory GasPriceCache with:
    - SQLite-backed persistent storage via DataCache
    - Linear interpolation for missing timestamps
    - Cache statistics tracking
    - Batch operations for efficient bulk writes

    The cache stores gas prices by chain and timestamp, and can interpolate
    missing values between known data points for more accurate historical
    backtesting.

    Attributes:
        db_path: Path to the SQLite database file
        ttl_seconds: Time-to-live in seconds (0 = no expiration)

    Example:
        cache = GasPriceDataCache(ttl_seconds=86400)  # 24-hour TTL

        # Store gas price
        cache.set(gas_price)

        # Retrieve gas price (uses interpolation if exact timestamp not found)
        gas = cache.get("ethereum", timestamp)

        # Get interpolated price between two known points
        gas = cache.get_interpolated("ethereum", timestamp)
    """

    def __init__(
        self,
        db_path: str | None = None,
        ttl_seconds: int = 86400,  # Default: 24 hours for gas prices
    ) -> None:
        """Initialize the persistent gas price cache.

        Args:
            db_path: Path to the SQLite database file. If None, uses
                     ~/.almanak/cache/gas_price_cache.db.
                     Use ":memory:" for an in-memory database (useful for testing).
            ttl_seconds: Time-to-live in seconds. Set to 0 to disable TTL.
                        Default: 86400 (24 hours).
        """
        from almanak.framework.data.cache.data_cache import DataCache

        self._ttl_seconds = ttl_seconds
        self._is_memory = db_path == ":memory:"

        # Determine database path
        if db_path is None:
            from pathlib import Path

            cache_dir = Path.home() / ".almanak" / "cache"
            cache_dir.mkdir(parents=True, exist_ok=True)
            self._db_path = str(cache_dir / "gas_price_cache.db")
        else:
            self._db_path = db_path

        # Initialize the underlying DataCache for KV storage
        self._data_cache = DataCache(db_path=self._db_path, ttl_seconds=0)  # We handle TTL ourselves

        # Initialize gas price table
        self._init_gas_table()

        # Statistics
        self._hits = 0
        self._misses = 0
        self._interpolations = 0

        logger.info(
            "Initialized GasPriceDataCache",
            extra={
                "db_path": self._db_path,
                "ttl_seconds": ttl_seconds,
            },
        )

    def _init_gas_table(self) -> None:
        """Initialize the gas price table in SQLite."""
        with self._connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS gas_prices (
                    chain TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    base_fee_gwei TEXT,
                    priority_fee_gwei TEXT,
                    gas_price_gwei TEXT,
                    source TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (chain, timestamp)
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_gas_chain_timestamp
                ON gas_prices (chain, timestamp)
            """)
            conn.commit()

    def _connection(self):
        """Get a database connection."""
        import sqlite3
        from contextlib import contextmanager

        @contextmanager
        def _get_conn():
            if self._is_memory:
                # Use DataCache's connection for in-memory
                with self._data_cache._connection() as conn:
                    # Ensure gas table exists in this connection
                    conn.execute("""
                        CREATE TABLE IF NOT EXISTS gas_prices (
                            chain TEXT NOT NULL,
                            timestamp TEXT NOT NULL,
                            base_fee_gwei TEXT,
                            priority_fee_gwei TEXT,
                            gas_price_gwei TEXT,
                            source TEXT NOT NULL,
                            created_at TEXT NOT NULL,
                            PRIMARY KEY (chain, timestamp)
                        )
                    """)
                    yield conn
            else:
                conn = sqlite3.connect(self._db_path)
                try:
                    yield conn
                finally:
                    conn.close()

        return _get_conn()

    def _is_expired(self, created_at_str: str) -> bool:
        """Check if an entry is expired based on TTL."""
        if self._ttl_seconds <= 0:
            return False
        created_at = datetime.fromisoformat(created_at_str)
        age = (datetime.now(UTC) - created_at).total_seconds()
        return age > self._ttl_seconds

    def get(self, chain: str, timestamp: datetime) -> GasPrice | None:
        """Retrieve gas price from the cache.

        Returns None if the entry is not found or if TTL has expired.

        Args:
            chain: Chain identifier (e.g., "ethereum", "arbitrum")
            timestamp: Target timestamp

        Returns:
            GasPrice if found and not expired, None otherwise
        """
        chain_lower = chain.lower()

        # Normalize timestamp to UTC
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=UTC)

        # Round to minute for lookup
        rounded = timestamp.replace(second=0, microsecond=0)

        with self._connection() as conn:
            cursor = conn.execute(
                """
                SELECT base_fee_gwei, priority_fee_gwei, gas_price_gwei, source, created_at
                FROM gas_prices
                WHERE chain = ? AND timestamp = ?
                """,
                (chain_lower, rounded.isoformat()),
            )
            row = cursor.fetchone()

            if row is None:
                self._misses += 1
                return None

            base_fee_str, priority_fee_str, gas_price_str, source, created_at_str = row

            # Check TTL
            if self._is_expired(created_at_str):
                self._misses += 1
                return None

            self._hits += 1
            return GasPrice(
                timestamp=rounded,
                chain=chain_lower,
                base_fee_gwei=Decimal(base_fee_str) if base_fee_str else None,
                priority_fee_gwei=Decimal(priority_fee_str) if priority_fee_str else None,
                gas_price_gwei=Decimal(gas_price_str) if gas_price_str else None,
                source=source,
            )

    def set(self, gas_price: GasPrice) -> None:
        """Store a gas price in the cache.

        Args:
            gas_price: GasPrice to store
        """
        chain_lower = gas_price.chain.lower()
        rounded = gas_price.timestamp.replace(second=0, microsecond=0)

        with self._connection() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO gas_prices
                (chain, timestamp, base_fee_gwei, priority_fee_gwei, gas_price_gwei, source, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    chain_lower,
                    rounded.isoformat(),
                    str(gas_price.base_fee_gwei) if gas_price.base_fee_gwei is not None else None,
                    str(gas_price.priority_fee_gwei) if gas_price.priority_fee_gwei is not None else None,
                    str(gas_price.gas_price_gwei) if gas_price.gas_price_gwei is not None else None,
                    gas_price.source,
                    datetime.now(UTC).isoformat(),
                ),
            )
            conn.commit()

    def set_batch(self, gas_prices: list[GasPrice]) -> int:
        """Store multiple gas prices in a single transaction.

        Args:
            gas_prices: List of GasPrice objects to store

        Returns:
            Number of entries stored
        """
        if not gas_prices:
            return 0

        now = datetime.now(UTC).isoformat()
        with self._connection() as conn:
            conn.executemany(
                """
                INSERT OR REPLACE INTO gas_prices
                (chain, timestamp, base_fee_gwei, priority_fee_gwei, gas_price_gwei, source, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        gp.chain.lower(),
                        gp.timestamp.replace(second=0, microsecond=0).isoformat(),
                        str(gp.base_fee_gwei) if gp.base_fee_gwei is not None else None,
                        str(gp.priority_fee_gwei) if gp.priority_fee_gwei is not None else None,
                        str(gp.gas_price_gwei) if gp.gas_price_gwei is not None else None,
                        gp.source,
                        now,
                    )
                    for gp in gas_prices
                ],
            )
            conn.commit()
        return len(gas_prices)

    def get_interpolated(
        self,
        chain: str,
        timestamp: datetime,
        max_delta_seconds: int = 3600,
    ) -> GasPrice | None:
        """Get gas price with interpolation for missing timestamps.

        If an exact match isn't found, looks for the nearest data points
        before and after the timestamp and performs linear interpolation.

        Args:
            chain: Chain identifier
            timestamp: Target timestamp
            max_delta_seconds: Maximum time difference for interpolation (default 1 hour)

        Returns:
            GasPrice (exact or interpolated), or None if interpolation not possible
        """
        # First try exact match
        exact = self.get(chain, timestamp)
        if exact is not None:
            return exact

        chain_lower = chain.lower()
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=UTC)

        timestamp_str = timestamp.isoformat()
        min_timestamp = (timestamp - timedelta(seconds=max_delta_seconds)).isoformat()
        max_timestamp = (timestamp + timedelta(seconds=max_delta_seconds)).isoformat()

        with self._connection() as conn:
            # Get nearest point before
            cursor = conn.execute(
                """
                SELECT timestamp, base_fee_gwei, priority_fee_gwei, gas_price_gwei, source
                FROM gas_prices
                WHERE chain = ? AND timestamp < ? AND timestamp >= ?
                ORDER BY timestamp DESC
                LIMIT 1
                """,
                (chain_lower, timestamp_str, min_timestamp),
            )
            before_row = cursor.fetchone()

            # Get nearest point after
            cursor = conn.execute(
                """
                SELECT timestamp, base_fee_gwei, priority_fee_gwei, gas_price_gwei, source
                FROM gas_prices
                WHERE chain = ? AND timestamp > ? AND timestamp <= ?
                ORDER BY timestamp ASC
                LIMIT 1
                """,
                (chain_lower, timestamp_str, max_timestamp),
            )
            after_row = cursor.fetchone()

        # If we have both points, interpolate
        if before_row is not None and after_row is not None:
            return self._interpolate(chain_lower, timestamp, before_row, after_row)

        # If only one point available, use it (nearest neighbor)
        if before_row is not None:
            ts_str, base_fee_str, priority_fee_str, gas_price_str, source = before_row
            self._interpolations += 1
            return GasPrice(
                timestamp=timestamp,
                chain=chain_lower,
                base_fee_gwei=Decimal(base_fee_str) if base_fee_str else None,
                priority_fee_gwei=Decimal(priority_fee_str) if priority_fee_str else None,
                gas_price_gwei=Decimal(gas_price_str) if gas_price_str else None,
                source=f"{source} (nearest)",
            )

        if after_row is not None:
            ts_str, base_fee_str, priority_fee_str, gas_price_str, source = after_row
            self._interpolations += 1
            return GasPrice(
                timestamp=timestamp,
                chain=chain_lower,
                base_fee_gwei=Decimal(base_fee_str) if base_fee_str else None,
                priority_fee_gwei=Decimal(priority_fee_str) if priority_fee_str else None,
                gas_price_gwei=Decimal(gas_price_str) if gas_price_str else None,
                source=f"{source} (nearest)",
            )

        self._misses += 1
        return None

    def _interpolate(
        self,
        chain: str,
        target_ts: datetime,
        before_row: tuple,
        after_row: tuple,
    ) -> GasPrice:
        """Perform linear interpolation between two gas price points.

        Args:
            chain: Chain identifier
            target_ts: Target timestamp
            before_row: (timestamp, base_fee, priority_fee, gas_price, source) before target
            after_row: (timestamp, base_fee, priority_fee, gas_price, source) after target

        Returns:
            Interpolated GasPrice
        """
        ts1_str, base1_str, priority1_str, gas1_str, source1 = before_row
        ts2_str, base2_str, priority2_str, gas2_str, source2 = after_row

        ts1 = datetime.fromisoformat(ts1_str)
        ts2 = datetime.fromisoformat(ts2_str)

        # Ensure timestamps have timezone
        if ts1.tzinfo is None:
            ts1 = ts1.replace(tzinfo=UTC)
        if ts2.tzinfo is None:
            ts2 = ts2.replace(tzinfo=UTC)

        # Calculate interpolation factor (0.0 = at ts1, 1.0 = at ts2)
        total_seconds = (ts2 - ts1).total_seconds()
        if total_seconds == 0:
            factor = Decimal("0.5")
        else:
            factor = Decimal(str((target_ts - ts1).total_seconds() / total_seconds))

        # Interpolate each field
        def interpolate_field(val1_str: str | None, val2_str: str | None) -> Decimal | None:
            if val1_str is None and val2_str is None:
                return None
            if val1_str is None:
                return Decimal(val2_str) if val2_str else None
            if val2_str is None:
                return Decimal(val1_str)
            val1 = Decimal(val1_str)
            val2 = Decimal(val2_str)
            return val1 + (val2 - val1) * factor

        base_fee = interpolate_field(base1_str, base2_str)
        priority_fee = interpolate_field(priority1_str, priority2_str)
        gas_price = interpolate_field(gas1_str, gas2_str)

        self._interpolations += 1

        return GasPrice(
            timestamp=target_ts,
            chain=chain,
            base_fee_gwei=base_fee,
            priority_fee_gwei=priority_fee,
            gas_price_gwei=gas_price,
            source=f"{source1} (interpolated)",
        )

    def get_range(
        self,
        chain: str,
        start: datetime,
        end: datetime,
    ) -> list[GasPrice]:
        """Retrieve gas prices for a time range.

        Args:
            chain: Chain identifier
            start: Start of time range (inclusive)
            end: End of time range (inclusive)

        Returns:
            List of GasPrice objects sorted by timestamp
        """
        chain_lower = chain.lower()

        if start.tzinfo is None:
            start = start.replace(tzinfo=UTC)
        if end.tzinfo is None:
            end = end.replace(tzinfo=UTC)

        with self._connection() as conn:
            cursor = conn.execute(
                """
                SELECT timestamp, base_fee_gwei, priority_fee_gwei, gas_price_gwei, source
                FROM gas_prices
                WHERE chain = ? AND timestamp >= ? AND timestamp <= ?
                ORDER BY timestamp ASC
                """,
                (chain_lower, start.isoformat(), end.isoformat()),
            )

            results: list[GasPrice] = []
            for row in cursor.fetchall():
                ts_str, base_fee_str, priority_fee_str, gas_price_str, source = row
                results.append(
                    GasPrice(
                        timestamp=datetime.fromisoformat(ts_str),
                        chain=chain_lower,
                        base_fee_gwei=Decimal(base_fee_str) if base_fee_str else None,
                        priority_fee_gwei=Decimal(priority_fee_str) if priority_fee_str else None,
                        gas_price_gwei=Decimal(gas_price_str) if gas_price_str else None,
                        source=source,
                    )
                )

            return results

    def clear(self, chain: str | None = None) -> int:
        """Clear cached gas prices.

        Args:
            chain: If provided, only clear entries for this chain

        Returns:
            Number of entries deleted
        """
        with self._connection() as conn:
            if chain is None:
                cursor = conn.execute("DELETE FROM gas_prices")
            else:
                cursor = conn.execute(
                    "DELETE FROM gas_prices WHERE chain = ?",
                    (chain.lower(),),
                )
            conn.commit()
            return cursor.rowcount

    def count(self, chain: str | None = None) -> int:
        """Count cached gas prices.

        Args:
            chain: If provided, only count entries for this chain

        Returns:
            Number of cached entries
        """
        with self._connection() as conn:
            if chain is None:
                cursor = conn.execute("SELECT COUNT(*) FROM gas_prices")
            else:
                cursor = conn.execute(
                    "SELECT COUNT(*) FROM gas_prices WHERE chain = ?",
                    (chain.lower(),),
                )
            row = cursor.fetchone()
            return row[0] if row else 0

    @property
    def stats(self) -> dict[str, Any]:
        """Get cache statistics.

        Returns:
            Dictionary with hits, misses, interpolations, hit_rate, and total_entries
        """
        total = self._hits + self._misses
        return {
            "hits": self._hits,
            "misses": self._misses,
            "interpolations": self._interpolations,
            "hit_rate": self._hits / total if total > 0 else 0.0,
            "total_entries": self.count(),
        }

    def reset_stats(self) -> None:
        """Reset cache statistics."""
        self._hits = 0
        self._misses = 0
        self._interpolations = 0

    def close(self) -> None:
        """Close the cache and release resources."""
        self._data_cache.close()


# =============================================================================
# Gas Price Provider Protocol
# =============================================================================


@runtime_checkable
class GasPriceProvider(Protocol):
    """Protocol defining the interface for gas price providers.

    Gas price providers fetch historical and current gas price data
    for cost estimation in backtesting and live trading.

    Implementations should handle:
        - Fetching current gas prices from RPC or API
        - Fetching historical gas prices when available
        - Caching to minimize API calls
        - Graceful fallback when data is unavailable
    """

    async def get_gas_price(
        self,
        timestamp: datetime | None = None,
        chain: str = "ethereum",
    ) -> GasPrice:
        """Get gas price at a specific timestamp.

        Args:
            timestamp: Target timestamp (None for current price)
            chain: Chain identifier (default: "ethereum")

        Returns:
            GasPrice with base fee, priority fee, and/or legacy gas price

        Raises:
            ValueError: If gas price data is not available
        """
        ...

    async def get_gas_prices_range(
        self,
        start: datetime,
        end: datetime,
        chain: str = "ethereum",
        interval_seconds: int = 3600,
    ) -> list[GasPrice]:
        """Get gas prices for a time range.

        Args:
            start: Start of time range
            end: End of time range
            chain: Chain identifier
            interval_seconds: Interval between data points

        Returns:
            List of GasPrice objects for the range

        Raises:
            ValueError: If data is not available for the range
        """
        ...

    @property
    def provider_name(self) -> str:
        """Return the unique name of this provider."""
        ...

    @property
    def supported_chains(self) -> list[str]:
        """Return list of supported chain identifiers."""
        ...


# =============================================================================
# Etherscan Gas Price Provider
# =============================================================================

# Etherscan API endpoints by chain
ETHERSCAN_API_URLS: dict[str, str] = {
    "ethereum": "https://api.etherscan.io/api",
    "arbitrum": "https://api.arbiscan.io/api",
    "optimism": "https://api-optimistic.etherscan.io/api",
    "base": "https://api.basescan.org/api",
    "polygon": "https://api.polygonscan.com/api",
    "bsc": "https://api.bscscan.com/api",
    "avalanche": "https://api.snowtrace.io/api",
}

# API key environment variable names by chain
ETHERSCAN_API_KEY_ENV_VARS: dict[str, str] = {
    "ethereum": "ETHERSCAN_API_KEY",
    "arbitrum": "ARBISCAN_API_KEY",
    "optimism": "OPTIMISTIC_ETHERSCAN_API_KEY",
    "base": "BASESCAN_API_KEY",
    "polygon": "POLYGONSCAN_API_KEY",
    "bsc": "BSCSCAN_API_KEY",
    "avalanche": "SNOWTRACE_API_KEY",
}

# Typical gas prices by chain (gwei) for fallback estimation
DEFAULT_GAS_PRICES: dict[str, dict[str, Decimal]] = {
    "ethereum": {
        "base_fee": Decimal("20"),
        "priority_fee": Decimal("2"),
    },
    "arbitrum": {
        "base_fee": Decimal("0.1"),
        "priority_fee": Decimal("0"),
    },
    "optimism": {
        "base_fee": Decimal("0.001"),
        "priority_fee": Decimal("0.001"),
    },
    "base": {
        "base_fee": Decimal("0.001"),
        "priority_fee": Decimal("0.001"),
    },
    "polygon": {
        "base_fee": Decimal("30"),
        "priority_fee": Decimal("30"),
    },
    "bsc": {
        "base_fee": Decimal("3"),
        "priority_fee": Decimal("0"),
    },
    "avalanche": {
        "base_fee": Decimal("25"),
        "priority_fee": Decimal("1"),
    },
}


# Archive RPC URL environment variable pattern (same as ChainlinkDataProvider)
ARCHIVE_RPC_URL_ENV_PATTERN = "ARCHIVE_RPC_URL_{chain}"

# Chains that support archive RPC queries
ARCHIVE_RPC_CHAINS = ["ethereum", "arbitrum", "base", "optimism", "polygon", "avalanche"]


class EtherscanGasPriceProvider:
    """Gas price provider using Etherscan-compatible APIs and archive RPC.

    Fetches gas price data from Etherscan and compatible block explorer
    APIs. Supports multiple chains and provides both current and historical
    gas prices.

    Data sources (in priority order for historical queries):
    1. Archive RPC - Direct query to archive node for historical block base fee
    2. Etherscan API - Gas oracle for current prices
    3. Estimation - Time-based estimation with known patterns
    4. Fallback - Config-specified fallback value (gas_fallback_gwei)

    Supports two caching modes:
    - In-memory cache (default): Fast but non-persistent
    - Persistent cache: SQLite-backed with interpolation support

    Attributes:
        api_keys: Dictionary mapping chain to API key
        request_timeout: HTTP request timeout in seconds
        min_request_interval: Minimum time between API requests

    Example:
        # Basic usage with in-memory cache
        provider = EtherscanGasPriceProvider(
            api_keys={"ethereum": "your-key"},
        )

        # With BacktestDataConfig for fallback values
        from almanak.framework.backtesting.config import BacktestDataConfig
        config = BacktestDataConfig(gas_fallback_gwei=Decimal("25"))
        provider = EtherscanGasPriceProvider(data_config=config)

        # With persistent cache for historical backtesting
        persistent_cache = GasPriceDataCache(ttl_seconds=86400)
        provider = EtherscanGasPriceProvider(
            api_keys={"ethereum": "your-key"},
            persistent_cache=persistent_cache,
        )

        # Get current gas price
        gas = await provider.get_gas_price(chain="ethereum")
        print(f"Gas price: {gas.effective_gas_price_gwei} gwei")

        # Get historical gas price (uses archive RPC if available)
        gas = await provider.get_gas_price(
            timestamp=datetime(2024, 6, 1, 12, 0),
            chain="ethereum",
        )
    """

    _SUPPORTED_CHAINS = list(ETHERSCAN_API_URLS.keys())

    def __init__(
        self,
        api_keys: dict[str, str] | None = None,
        request_timeout: float = 30.0,
        min_request_interval: float = 0.25,
        cache_ttl_seconds: int = 60,
        persistent_cache: "GasPriceDataCache | None" = None,
        use_interpolation: bool = True,
        data_config: "BacktestDataConfig | None" = None,
        archive_rpc_urls: dict[str, str] | None = None,
    ) -> None:
        """Initialize the Etherscan gas price provider.

        Args:
            api_keys: Dictionary mapping chain identifier to API key.
                     If not provided, attempts to read from environment variables.
            request_timeout: HTTP request timeout in seconds (default 30).
            min_request_interval: Minimum interval between API requests (default 0.25s).
            cache_ttl_seconds: TTL for in-memory cached gas prices (default 60s).
            persistent_cache: Optional GasPriceDataCache for persistent SQLite storage.
                            When provided, gas prices are stored in SQLite and
                            interpolation is available for missing timestamps.
            use_interpolation: When True and persistent_cache is provided, uses
                             interpolation for missing historical timestamps.
                             Default: True.
            data_config: Optional BacktestDataConfig for fallback gas price.
                        When historical gas price is unavailable, uses
                        data_config.gas_fallback_gwei.
            archive_rpc_urls: Optional dictionary mapping chain to archive RPC URL.
                            If not provided, attempts to read from environment
                            variables (ARCHIVE_RPC_URL_ETHEREUM, etc.).
        """
        self._api_keys = api_keys or {}
        self._request_timeout = request_timeout
        self._min_request_interval = min_request_interval
        self._last_request_time: float = 0.0
        self._session: aiohttp.ClientSession | None = None
        self._cache = GasPriceCache(ttl_seconds=cache_ttl_seconds)
        self._persistent_cache = persistent_cache
        self._use_interpolation = use_interpolation
        self._data_config = data_config
        self._archive_rpc_urls: dict[str, str] = archive_rpc_urls or {}

        # Load API keys from environment if not provided
        for chain, env_var in ETHERSCAN_API_KEY_ENV_VARS.items():
            if chain not in self._api_keys:
                key = os.environ.get(env_var, "")
                if key:
                    self._api_keys[chain] = key

        # Load archive RPC URLs from environment if not provided
        for chain in ARCHIVE_RPC_CHAINS:
            if chain not in self._archive_rpc_urls:
                env_var = ARCHIVE_RPC_URL_ENV_PATTERN.format(chain=chain.upper())
                url = os.environ.get(env_var, "")
                if url:
                    self._archive_rpc_urls[chain] = url

        logger.info(
            "Initialized EtherscanGasPriceProvider",
            extra={
                "chains_with_keys": list(self._api_keys.keys()),
                "chains_with_archive_rpc": list(self._archive_rpc_urls.keys()),
                "request_timeout": request_timeout,
                "cache_ttl_seconds": cache_ttl_seconds,
                "persistent_cache": persistent_cache is not None,
                "use_interpolation": use_interpolation,
                "data_config": data_config is not None,
            },
        )

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

    async def _wait_for_rate_limit(self) -> None:
        """Wait if needed to respect rate limits."""
        elapsed = time.time() - self._last_request_time
        if elapsed < self._min_request_interval:
            import asyncio

            await asyncio.sleep(self._min_request_interval - elapsed)

    async def _make_request(
        self,
        chain: str,
        action: str,
        extra_params: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Make an API request to Etherscan.

        Args:
            chain: Chain identifier
            action: API action (e.g., "gasoracle")
            extra_params: Additional query parameters

        Returns:
            JSON response as dictionary

        Raises:
            ValueError: If the API returns an error
        """
        await self._wait_for_rate_limit()

        chain_lower = chain.lower()
        if chain_lower not in ETHERSCAN_API_URLS:
            raise ValueError(f"Unsupported chain: {chain}")

        base_url = ETHERSCAN_API_URLS[chain_lower]
        api_key = self._api_keys.get(chain_lower, "")

        params: dict[str, str] = {
            "module": "gastracker",
            "action": action,
        }
        if api_key:
            params["apikey"] = api_key
        if extra_params:
            params.update(extra_params)

        self._last_request_time = time.time()
        session = await self._get_session()

        try:
            async with session.get(base_url, params=params) as response:
                if response.status != 200:
                    text = await response.text()
                    raise ValueError(f"Etherscan API error {response.status}: {text}")

                result: dict[str, Any] = await response.json()

                if result.get("status") != "1":
                    message = result.get("message", "Unknown error")
                    if "Max rate limit reached" in message:
                        raise ValueError(f"Rate limited by Etherscan: {message}")
                    raise ValueError(f"Etherscan API error: {message}")

                return result

        except TimeoutError as e:
            raise ValueError(f"Request timed out after {self._request_timeout}s") from e
        except aiohttp.ClientError as e:
            raise ValueError(f"Network error: {e!s}") from e

    async def _get_current_gas_price(self, chain: str) -> GasPrice:
        """Get current gas price from Etherscan gas oracle.

        Args:
            chain: Chain identifier

        Returns:
            Current GasPrice

        Raises:
            ValueError: If API request fails
        """
        result = await self._make_request(chain, "gasoracle")
        data = result.get("result", {})

        # Etherscan returns prices in gwei
        # ProposeGasPrice is the standard gas price
        # SafeGasPrice is a lower, slower price
        # FastGasPrice is a higher, faster price
        # suggestBaseFee is the current base fee (EIP-1559)

        base_fee_str = data.get("suggestBaseFee", "")
        propose_gas = data.get("ProposeGasPrice", "")
        # SafeGasPrice and FastGasPrice are available but we use ProposeGasPrice
        # as the standard gas price for cost estimation

        # Try to extract base fee (EIP-1559)
        base_fee: Decimal | None = None
        if base_fee_str:
            try:
                base_fee = Decimal(base_fee_str)
            except (ValueError, TypeError):
                pass

        # Use ProposeGasPrice as the standard gas price
        gas_price: Decimal | None = None
        if propose_gas:
            try:
                gas_price = Decimal(propose_gas)
            except (ValueError, TypeError):
                pass

        # Estimate priority fee if we have both base fee and gas price
        priority_fee: Decimal | None = None
        if base_fee is not None and gas_price is not None and gas_price > base_fee:
            priority_fee = gas_price - base_fee
        elif base_fee is not None:
            # Default priority fee
            priority_fee = Decimal("2")

        now = datetime.now(UTC)

        return GasPrice(
            timestamp=now,
            chain=chain.lower(),
            base_fee_gwei=base_fee,
            priority_fee_gwei=priority_fee,
            gas_price_gwei=gas_price,
            source="etherscan",
            confidence=DataConfidence.HIGH,  # Real API data
        )

    async def _get_historical_gas_price_from_archive(
        self,
        timestamp: datetime,
        chain: str,
    ) -> GasPrice | None:
        """Get historical gas price from archive RPC node.

        Queries the archive RPC node for the block at the given timestamp
        and extracts the base fee. This provides accurate historical gas
        price data for EIP-1559 chains.

        Args:
            timestamp: Target timestamp
            chain: Chain identifier

        Returns:
            GasPrice with HIGH confidence if successful, None if unavailable
        """
        chain_lower = chain.lower()
        rpc_url = self._archive_rpc_urls.get(chain_lower)
        if not rpc_url:
            logger.debug(f"No archive RPC URL configured for {chain}")
            return None

        try:
            session = await self._get_session()

            # First, get the block number for the timestamp
            # eth_getBlockByNumber with "finalized" doesn't work for historical queries
            # We need to find the block closest to our target timestamp
            # For simplicity, we'll query the block at a specific number

            # Get current block number to estimate historical block
            current_block_payload = {
                "jsonrpc": "2.0",
                "method": "eth_blockNumber",
                "params": [],
                "id": 1,
            }

            async with session.post(rpc_url, json=current_block_payload) as response:
                if response.status != 200:
                    logger.warning(f"Archive RPC error: HTTP {response.status}")
                    return None
                result = await response.json()
                if "error" in result:
                    logger.warning(f"Archive RPC error: {result['error']}")
                    return None
                current_block = int(result["result"], 16)

            # Estimate historical block number
            # Approximate: 12 seconds per block on Ethereum, ~0.25s on L2s
            now = datetime.now(UTC)
            seconds_ago = (now - timestamp).total_seconds()

            # Chain-specific block times (seconds)
            block_times = {
                "ethereum": 12.0,
                "arbitrum": 0.25,
                "optimism": 2.0,
                "base": 2.0,
                "polygon": 2.0,
                "avalanche": 2.0,
            }
            block_time = block_times.get(chain_lower, 12.0)
            blocks_ago = int(seconds_ago / block_time)
            target_block = max(1, current_block - blocks_ago)

            # Get the block data
            block_payload = {
                "jsonrpc": "2.0",
                "method": "eth_getBlockByNumber",
                "params": [hex(target_block), False],
                "id": 2,
            }

            async with session.post(rpc_url, json=block_payload) as response:
                if response.status != 200:
                    return None
                result = await response.json()
                if "error" in result or result.get("result") is None:
                    return None

                block_data = result["result"]

            # Extract base fee (EIP-1559)
            base_fee_hex = block_data.get("baseFeePerGas")
            if not base_fee_hex:
                # Pre-EIP-1559 block, use gasPrice heuristic
                logger.debug(f"Block {target_block} has no baseFeePerGas (pre-EIP-1559)")
                return None

            # Convert from wei to gwei
            base_fee_wei = int(base_fee_hex, 16)
            base_fee_gwei = Decimal(base_fee_wei) / Decimal("1000000000")

            # Default priority fee based on chain
            defaults = DEFAULT_GAS_PRICES.get(chain_lower, DEFAULT_GAS_PRICES["ethereum"])
            priority_fee = defaults["priority_fee"]

            gas_price_gwei = base_fee_gwei + priority_fee

            logger.debug(f"Archive RPC: {chain} block {target_block} base_fee={base_fee_gwei} gwei")

            return GasPrice(
                timestamp=timestamp,
                chain=chain_lower,
                base_fee_gwei=base_fee_gwei,
                priority_fee_gwei=priority_fee,
                gas_price_gwei=gas_price_gwei,
                source="archive_rpc",
                confidence=DataConfidence.HIGH,  # Real on-chain data
            )

        except (TimeoutError, aiohttp.ClientError, KeyError, ValueError) as e:
            logger.warning(f"Archive RPC query failed for {chain}: {e}")
            return None

    def _estimate_historical_gas_price(
        self,
        timestamp: datetime,
        chain: str,
    ) -> GasPrice:
        """Estimate historical gas price based on known patterns.

        This is a fallback when precise historical data is unavailable.
        Uses default gas prices adjusted by a simple time-based factor.

        Args:
            timestamp: Target timestamp
            chain: Chain identifier

        Returns:
            Estimated GasPrice
        """
        chain_lower = chain.lower()
        defaults = DEFAULT_GAS_PRICES.get(chain_lower, DEFAULT_GAS_PRICES["ethereum"])

        base_fee = defaults["base_fee"]
        priority_fee = defaults["priority_fee"]

        # Apply a simple time-of-day adjustment (gas tends to be higher during business hours)
        hour = timestamp.hour
        day_of_week = timestamp.weekday()

        # Business hours (9-17 UTC) on weekdays tend to have higher gas
        if day_of_week < 5 and 9 <= hour <= 17:
            multiplier = Decimal("1.3")
        elif day_of_week < 5:
            multiplier = Decimal("1.0")
        else:
            # Weekend tends to be lower
            multiplier = Decimal("0.8")

        adjusted_base_fee = base_fee * multiplier
        gas_price = adjusted_base_fee + priority_fee

        logger.debug(
            "Estimated historical gas price for %s at %s: %s gwei",
            chain,
            timestamp,
            gas_price,
        )

        return GasPrice(
            timestamp=timestamp,
            chain=chain_lower,
            base_fee_gwei=adjusted_base_fee,
            priority_fee_gwei=priority_fee,
            gas_price_gwei=gas_price,
            source="etherscan_estimated",
            confidence=DataConfidence.MEDIUM,  # Estimated based on patterns
        )

    def _get_fallback_gas_price(
        self,
        timestamp: datetime,
        chain: str,
    ) -> GasPrice:
        """Get fallback gas price from BacktestDataConfig.

        Uses the gas_fallback_gwei value from data_config when all other
        sources are unavailable.

        Args:
            timestamp: Target timestamp
            chain: Chain identifier

        Returns:
            GasPrice with LOW confidence (fallback value)
        """
        chain_lower = chain.lower()

        # Use config fallback if available, otherwise use default
        if self._data_config is not None:
            gas_price = self._data_config.gas_fallback_gwei
        else:
            defaults = DEFAULT_GAS_PRICES.get(chain_lower, DEFAULT_GAS_PRICES["ethereum"])
            gas_price = defaults["base_fee"] + defaults["priority_fee"]

        logger.warning(
            "Using fallback gas price for %s at %s: %s gwei",
            chain,
            timestamp,
            gas_price,
        )

        return GasPrice(
            timestamp=timestamp,
            chain=chain_lower,
            base_fee_gwei=None,
            priority_fee_gwei=None,
            gas_price_gwei=gas_price,
            source="config_fallback",
            confidence=DataConfidence.LOW,  # Fallback value
        )

    async def get_gas_price(
        self,
        timestamp: datetime | None = None,
        chain: str = "ethereum",
    ) -> GasPrice:
        """Get gas price at a specific timestamp.

        For current prices, queries the Etherscan gas oracle API.
        For historical prices (in priority order):
        1. Check cache (in-memory, then persistent)
        2. Query archive RPC for historical block base fee (HIGH confidence)
        3. Use time-based estimation (MEDIUM confidence)
        4. Use config fallback value (LOW confidence)

        Args:
            timestamp: Target timestamp (None for current price)
            chain: Chain identifier (default: "ethereum")

        Returns:
            GasPrice with base fee, priority fee, and/or legacy gas price,
            including confidence level based on data source

        Raises:
            ValueError: If chain is not supported
        """
        chain_lower = chain.lower()
        if chain_lower not in ETHERSCAN_API_URLS:
            raise ValueError(f"Unsupported chain: {chain}. Supported: {', '.join(self._SUPPORTED_CHAINS)}")

        # Normalize timestamp
        now = datetime.now(UTC)
        if timestamp is None:
            timestamp = now
        elif timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=UTC)

        # Check in-memory cache first (fast path)
        cached = self._cache.get(chain_lower, timestamp)
        if cached is not None:
            logger.debug(f"In-memory cache hit for {chain} gas price at {timestamp}")
            return cached

        # Check persistent cache if available
        if self._persistent_cache is not None:
            if self._use_interpolation:
                # Use interpolation for historical timestamps
                cached = self._persistent_cache.get_interpolated(chain_lower, timestamp)
            else:
                cached = self._persistent_cache.get(chain_lower, timestamp)

            if cached is not None:
                logger.debug(f"Persistent cache hit for {chain} gas price at {timestamp}")
                # Also store in in-memory cache for faster subsequent access
                self._cache.set(cached)
                return cached

        # Determine if this is a current or historical request
        time_diff = abs((now - timestamp).total_seconds())
        is_current_request = time_diff < 300  # Within 5 minutes

        if is_current_request:
            try:
                gas_price = await self._get_current_gas_price(chain_lower)
                self._cache.set(gas_price)
                # Also store in persistent cache
                if self._persistent_cache is not None:
                    self._persistent_cache.set(gas_price)
                return gas_price
            except ValueError as e:
                logger.warning(f"Failed to get current gas price: {e}")
                # Fall back to estimation
                gas_price = self._estimate_historical_gas_price(timestamp, chain_lower)
                self._cache.set(gas_price)
                return gas_price
        else:
            # Historical request - try multiple sources in priority order
            historical_gas_price: GasPrice | None = None

            # 1. Try archive RPC (HIGH confidence - real on-chain data)
            if chain_lower in self._archive_rpc_urls:
                historical_gas_price = await self._get_historical_gas_price_from_archive(timestamp, chain_lower)
                if historical_gas_price is not None:
                    logger.debug(f"Got historical gas price from archive RPC for {chain} at {timestamp}")
                    self._cache.set(historical_gas_price)
                    if self._persistent_cache is not None:
                        self._persistent_cache.set(historical_gas_price)
                    return historical_gas_price

            # 2. Use time-based estimation (MEDIUM confidence)
            gas_price = self._estimate_historical_gas_price(timestamp, chain_lower)
            self._cache.set(gas_price)
            # Also store in persistent cache for future interpolation
            if self._persistent_cache is not None:
                self._persistent_cache.set(gas_price)
            return gas_price

    async def get_gas_prices_range(
        self,
        start: datetime,
        end: datetime,
        chain: str = "ethereum",
        interval_seconds: int = 3600,
    ) -> list[GasPrice]:
        """Get gas prices for a time range.

        When persistent cache is available, first checks for cached data
        and uses interpolation for missing points. Otherwise generates
        estimated gas prices at regular intervals.

        Args:
            start: Start of time range
            end: End of time range
            chain: Chain identifier (default: "ethereum")
            interval_seconds: Interval between data points (default: 3600 = 1 hour)

        Returns:
            List of GasPrice objects for the range
        """
        chain_lower = chain.lower()
        if chain_lower not in ETHERSCAN_API_URLS:
            raise ValueError(f"Unsupported chain: {chain}")

        # Normalize timestamps
        if start.tzinfo is None:
            start = start.replace(tzinfo=UTC)
        if end.tzinfo is None:
            end = end.replace(tzinfo=UTC)

        # If persistent cache available, try to get range from it first
        if self._persistent_cache is not None:
            cached_range = self._persistent_cache.get_range(chain_lower, start, end)
            if cached_range:
                logger.info(f"Retrieved {len(cached_range)} gas prices from persistent cache for {chain}")
                return cached_range

        gas_prices: list[GasPrice] = []
        prices_to_persist: list[GasPrice] = []
        current = start
        interval = timedelta(seconds=interval_seconds)

        while current <= end:
            # Check in-memory cache first
            cached = self._cache.get_nearest(chain_lower, current, max_delta_seconds=interval_seconds // 2)
            if cached is not None:
                gas_prices.append(cached)
            else:
                # Check persistent cache with interpolation
                if self._persistent_cache is not None and self._use_interpolation:
                    cached = self._persistent_cache.get_interpolated(
                        chain_lower, current, max_delta_seconds=interval_seconds
                    )
                    if cached is not None:
                        gas_prices.append(cached)
                        self._cache.set(cached)
                        current += interval
                        continue

                # Generate estimate
                gas_price = self._estimate_historical_gas_price(current, chain_lower)
                gas_prices.append(gas_price)
                self._cache.set(gas_price)
                prices_to_persist.append(gas_price)

            current += interval

        # Batch store new prices in persistent cache
        if self._persistent_cache is not None and prices_to_persist:
            self._persistent_cache.set_batch(prices_to_persist)

        logger.info(f"Generated {len(gas_prices)} gas price estimates for {chain} from {start} to {end}")
        return gas_prices

    def set_historical_gas_prices(
        self,
        gas_prices: list[GasPrice],
    ) -> int:
        """Pre-load historical gas price data into the cache.

        This allows loading gas prices from external sources (e.g., CSV,
        database) for more accurate historical backtesting.

        When persistent cache is available, stores in both in-memory
        and persistent caches for optimal performance.

        Args:
            gas_prices: List of GasPrice objects to cache

        Returns:
            Number of entries cached
        """
        # Store in in-memory cache
        count = self._cache.set_batch(gas_prices)

        # Also store in persistent cache
        if self._persistent_cache is not None:
            self._persistent_cache.set_batch(gas_prices)

        return count

    def clear_cache(self, chain: str | None = None) -> int:
        """Clear the gas price cache.

        Clears both in-memory and persistent caches when available.

        Args:
            chain: If provided, only clear entries for this chain

        Returns:
            Number of entries cleared from in-memory cache
        """
        count = self._cache.clear(chain)

        # Also clear persistent cache
        if self._persistent_cache is not None:
            self._persistent_cache.clear(chain)

        return count

    @property
    def provider_name(self) -> str:
        """Return the unique name of this provider."""
        return "etherscan"

    @property
    def supported_chains(self) -> list[str]:
        """Return list of supported chain identifiers."""
        return self._SUPPORTED_CHAINS.copy()

    async def __aenter__(self) -> "EtherscanGasPriceProvider":
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit."""
        await self.close()


# =============================================================================
# Exports
# =============================================================================


__all__ = [
    "GasPrice",
    "GasPriceCache",
    "GasPriceDataCache",
    "GasPriceProvider",
    "EtherscanGasPriceProvider",
    "ETHERSCAN_API_URLS",
    "ETHERSCAN_API_KEY_ENV_VARS",
    "DEFAULT_GAS_PRICES",
    "ARCHIVE_RPC_URL_ENV_PATTERN",
    "ARCHIVE_RPC_CHAINS",
]
