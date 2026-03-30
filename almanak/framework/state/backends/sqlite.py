"""SQLite state storage backend.

Provides production-quality SQLite persistence for local development
and lightweight deployments. Implements the same interface as PostgresStore
for consistent behavior across backends.

Features:
- Single row per agent (matches PostgreSQL model)
- CAS (Compare-And-Swap) via optimistic locking with version field
- WAL mode for better concurrent read performance
- Timeline events storage for execution audit trail
- Checksum integrity verification

Important: Each strategy uses exactly one gateway and vice versa.
No two strategies share a gateway.

Usage:
    config = SQLiteConfig(db_path="./state.db")
    store = SQLiteStore(config)
    await store.initialize()

    # Save state
    state = StateData(strategy_id="strat-1", version=1, state={"key": "value"})
    await store.save(state)

    # CAS update
    await store.save(state, expected_version=1)

    # Save timeline event
    await store.save_event(event)
"""

import asyncio
import hashlib
import json
import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from ..state_manager import StateConflictError, StateData, StateTier

if TYPE_CHECKING:
    from almanak.framework.execution.clob_handler import ClobFill, ClobOrderState, ClobOrderStatus
    from almanak.framework.portfolio import PortfolioMetrics, PortfolioSnapshot

logger = logging.getLogger(__name__)


# =============================================================================
# EXCEPTIONS
# =============================================================================


class SQLiteBackendError(Exception):
    """Base exception for SQLite backend errors."""

    pass


class DatabaseInitializationError(SQLiteBackendError):
    """Raised when database initialization fails."""

    pass


class EventNotFoundError(SQLiteBackendError):
    """Raised when a timeline event is not found."""

    def __init__(self, event_id: int, message: str | None = None) -> None:
        self.event_id = event_id
        super().__init__(message or f"Event not found: {event_id}")


# =============================================================================
# DATA CLASSES
# =============================================================================


@dataclass
class SQLiteConfig:
    """SQLite connection configuration.

    Attributes:
        db_path: Path to SQLite database file. Use ":memory:" for in-memory DB.
        timeout: Connection timeout in seconds.
        isolation_level: Transaction isolation level (None for autocommit).
        check_same_thread: Whether to check same thread (False for async use).
        wal_mode: Enable WAL mode for better concurrent read performance.
        journal_mode: Journal mode when WAL is disabled.
        busy_timeout: How long to wait when database is locked (ms).
        cache_size: SQLite cache size in pages (negative for KB).
    """

    db_path: str = "./almanak_state.db"
    timeout: float = 30.0
    isolation_level: Literal["DEFERRED", "EXCLUSIVE", "IMMEDIATE"] | None = None
    check_same_thread: bool = False
    wal_mode: bool = True
    journal_mode: str = "DELETE"
    busy_timeout: int = 5000
    cache_size: int = -2000  # 2MB cache

    def __post_init__(self) -> None:
        """Validate configuration."""
        if self.timeout <= 0:
            raise ValueError("timeout must be positive")
        if self.busy_timeout < 0:
            raise ValueError("busy_timeout must be non-negative")


@dataclass
class TimelineEvent:
    """Timeline event for strategy execution audit trail.

    Attributes:
        id: Auto-generated event ID (None for new events).
        strategy_id: Strategy that generated the event.
        event_type: Type of event (e.g., "EXECUTION_SUCCESS", "TX_CONFIRMED").
        event_data: JSON-serializable event payload.
        correlation_id: ID to correlate related events.
        created_at: When the event occurred.
    """

    strategy_id: str
    event_type: str
    event_data: dict[str, Any] = field(default_factory=dict)
    correlation_id: str | None = None
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    id: int | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "id": self.id,
            "strategy_id": self.strategy_id,
            "event_type": self.event_type,
            "event_data": self.event_data,
            "correlation_id": self.correlation_id,
            "created_at": self.created_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TimelineEvent":
        """Create TimelineEvent from dictionary."""
        created_at = data.get("created_at")
        if isinstance(created_at, str):
            created_at = datetime.fromisoformat(created_at)
        elif created_at is None:
            created_at = datetime.now(UTC)

        return cls(
            id=data.get("id"),
            strategy_id=data["strategy_id"],
            event_type=data["event_type"],
            event_data=data.get("event_data", {}),
            correlation_id=data.get("correlation_id"),
            created_at=created_at,
        )

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "TimelineEvent":
        """Create TimelineEvent from SQLite row."""
        event_data = row["event_data"]
        if isinstance(event_data, str):
            event_data = json.loads(event_data)

        created_at = row["created_at"]
        if isinstance(created_at, str):
            created_at = datetime.fromisoformat(created_at)

        return cls(
            id=row["id"],
            strategy_id=row["strategy_id"],
            event_type=row["event_type"],
            event_data=event_data,
            correlation_id=row["correlation_id"],
            created_at=created_at,
        )


# =============================================================================
# SQL SCHEMA
# =============================================================================

SCHEMA_SQL = """
-- Strategy state table for local SQLite mode.
-- Deployed PostgreSQL uses agent_id; local SQLite keeps strategy_id and
-- relies on resolve_agent_id() to make the logical identifier match.
CREATE TABLE IF NOT EXISTS v2_strategy_state (
    strategy_id TEXT PRIMARY KEY,
    version INTEGER NOT NULL DEFAULT 1,
    state_data TEXT NOT NULL,  -- JSON string
    schema_version INTEGER NOT NULL DEFAULT 1,
    checksum TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

-- Timeline events table (matches PostgreSQL schema)
CREATE TABLE IF NOT EXISTS v2_timeline_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    strategy_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    event_data TEXT NOT NULL,  -- JSON string
    correlation_id TEXT,
    created_at TEXT NOT NULL
);

-- Index for strategy event queries
CREATE INDEX IF NOT EXISTS idx_v2_timeline_events_strategy_id
ON v2_timeline_events (strategy_id);

-- Index for correlation ID queries
CREATE INDEX IF NOT EXISTS idx_v2_timeline_events_correlation_id
ON v2_timeline_events (correlation_id);

-- Index for event type queries
CREATE INDEX IF NOT EXISTS idx_v2_timeline_events_event_type
ON v2_timeline_events (event_type);

-- Index for time-ordered queries
CREATE INDEX IF NOT EXISTS idx_v2_timeline_events_created_at
ON v2_timeline_events (created_at DESC);

-- CLOB orders table for Polymarket order tracking
CREATE TABLE IF NOT EXISTS v2_clob_orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id TEXT NOT NULL UNIQUE,
    market_id TEXT NOT NULL,
    token_id TEXT NOT NULL,
    side TEXT NOT NULL,  -- BUY or SELL
    status TEXT NOT NULL,  -- pending, submitted, live, matched, cancelled, etc.
    price TEXT NOT NULL,  -- Decimal as string
    size TEXT NOT NULL,  -- Decimal as string
    filled_size TEXT NOT NULL DEFAULT '0',
    average_fill_price TEXT,  -- Decimal as string, nullable
    fills TEXT NOT NULL DEFAULT '[]',  -- JSON array of fills
    order_type TEXT NOT NULL DEFAULT 'GTC',
    intent_id TEXT,  -- Associated intent ID for tracing
    error TEXT,  -- Error message if failed
    metadata TEXT NOT NULL DEFAULT '{}',  -- Additional JSON metadata
    submitted_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

-- Index for order_id lookups
CREATE INDEX IF NOT EXISTS idx_v2_clob_orders_order_id
ON v2_clob_orders (order_id);

-- Index for market_id queries (open orders by market)
CREATE INDEX IF NOT EXISTS idx_v2_clob_orders_market_id
ON v2_clob_orders (market_id);

-- Index for status queries (finding open orders)
CREATE INDEX IF NOT EXISTS idx_v2_clob_orders_status
ON v2_clob_orders (status);

-- Index for intent_id queries (tracing orders to intents)
CREATE INDEX IF NOT EXISTS idx_v2_clob_orders_intent_id
ON v2_clob_orders (intent_id);

-- Composite index for open orders by market
CREATE INDEX IF NOT EXISTS idx_v2_clob_orders_market_status
ON v2_clob_orders (market_id, status);

-- Portfolio snapshots table for value tracking and PnL charts
CREATE TABLE IF NOT EXISTS v2_portfolio_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    strategy_id TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    iteration_number INTEGER DEFAULT 0,
    total_value_usd TEXT NOT NULL,  -- Decimal as string
    available_cash_usd TEXT NOT NULL,  -- Decimal as string
    value_confidence TEXT DEFAULT 'HIGH',  -- HIGH, ESTIMATED, STALE, UNAVAILABLE
    positions_json TEXT NOT NULL,  -- JSON array of positions
    chain TEXT,
    created_at TEXT NOT NULL
);

-- Index for strategy + time queries (dashboard charts)
CREATE INDEX IF NOT EXISTS idx_v2_portfolio_snapshots_strategy_time
ON v2_portfolio_snapshots (strategy_id, timestamp DESC);

-- Index for cleanup queries
CREATE INDEX IF NOT EXISTS idx_v2_portfolio_snapshots_created_at
ON v2_portfolio_snapshots (created_at);

-- Unique constraint to prevent duplicate timestamps per strategy
CREATE UNIQUE INDEX IF NOT EXISTS idx_v2_portfolio_snapshots_unique
ON v2_portfolio_snapshots (strategy_id, timestamp);

-- Portfolio metrics table for PnL baseline tracking
-- Stores values that survive strategy restarts
CREATE TABLE IF NOT EXISTS v2_portfolio_metrics (
    strategy_id TEXT PRIMARY KEY,
    initial_value_usd TEXT NOT NULL,  -- Decimal as string, set on first run
    initial_timestamp TEXT NOT NULL,
    deposits_usd TEXT DEFAULT '0',
    withdrawals_usd TEXT DEFAULT '0',
    gas_spent_usd TEXT DEFAULT '0',
    updated_at TEXT NOT NULL
);
"""


# =============================================================================
# SQLITE STORE
# =============================================================================


class SQLiteStore:
    """SQLite state storage backend.

    Provides <5ms access for local state storage with full CAS support.
    Single row per agent -- schema matches PostgreSQL for easy migration.

    Thread Safety:
        Uses check_same_thread=False to allow async access from different threads.
        SQLite's WAL mode provides safe concurrent reads with serialized writes.

    Example:
        >>> config = SQLiteConfig(db_path="./state.db")
        >>> store = SQLiteStore(config)
        >>> await store.initialize()
        >>>
        >>> # Save new state
        >>> state = StateData(strategy_id="strat-1", version=1, state={"key": "value"})
        >>> await store.save(state)
        >>>
        >>> # CAS update
        >>> state.state["key"] = "new_value"
        >>> state.version = 2
        >>> await store.save(state, expected_version=1)
    """

    def __init__(self, config: SQLiteConfig | None = None) -> None:
        """Initialize SQLite store.

        Args:
            config: SQLite configuration. Uses defaults if not provided.
        """
        self._config = config or SQLiteConfig()
        self._conn: sqlite3.Connection | None = None
        self._initialized = False
        self._lock = asyncio.Lock()

    @property
    def is_initialized(self) -> bool:
        """Check if store is initialized."""
        return self._initialized

    @property
    def db_path(self) -> str:
        """Get database file path."""
        return self._config.db_path

    async def initialize(self) -> None:
        """Initialize database connection and create schema.

        Creates the database file if it doesn't exist.
        Enables WAL mode for better concurrent performance.

        Raises:
            DatabaseInitializationError: If initialization fails.
        """
        if self._initialized:
            return

        async with self._lock:
            if self._initialized:
                return

            try:
                await self._connect()
                await self._create_schema()
                self._initialized = True
                logger.info(f"SQLite store initialized: {self._config.db_path} (WAL: {self._config.wal_mode})")
            except Exception as e:
                logger.error(f"Failed to initialize SQLite store: {e}")
                raise DatabaseInitializationError(f"Failed to initialize database: {e}") from e

    async def _connect(self) -> None:
        """Create database connection."""

        def _sync_connect() -> sqlite3.Connection:
            # Create parent directory if needed
            if self._config.db_path != ":memory:":
                path = Path(self._config.db_path)
                path.parent.mkdir(parents=True, exist_ok=True)

            conn = sqlite3.connect(
                self._config.db_path,
                timeout=self._config.timeout,
                isolation_level=self._config.isolation_level,
                check_same_thread=self._config.check_same_thread,
            )

            # Enable row factory for dict-like access
            conn.row_factory = sqlite3.Row

            # Configure connection
            conn.execute(f"PRAGMA busy_timeout = {self._config.busy_timeout}")
            conn.execute(f"PRAGMA cache_size = {self._config.cache_size}")

            # Enable WAL mode for better concurrent reads
            if self._config.wal_mode:
                conn.execute("PRAGMA journal_mode = WAL")
                conn.execute("PRAGMA synchronous = NORMAL")
                # Auto-checkpoint WAL every 100 pages to prevent unbounded growth
                # when multiple processes write concurrently
                conn.execute("PRAGMA wal_autocheckpoint = 100")
            else:
                conn.execute(f"PRAGMA journal_mode = {self._config.journal_mode}")

            # Enable foreign keys
            conn.execute("PRAGMA foreign_keys = ON")

            return conn

        loop = asyncio.get_event_loop()
        self._conn = await loop.run_in_executor(None, _sync_connect)

    async def _create_schema(self) -> None:
        """Create database tables and indexes."""
        if self._conn is None:
            raise DatabaseInitializationError("Connection not established")

        def _sync_create_schema() -> None:
            self._conn.executescript(SCHEMA_SQL)  # type: ignore[union-attr]
            self._conn.commit()  # type: ignore[union-attr]

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, _sync_create_schema)

    async def close(self) -> None:
        """Close database connection."""
        if self._conn:

            def _sync_close() -> None:
                if self._conn:
                    self._conn.close()

            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, _sync_close)
            self._conn = None
            self._initialized = False
            logger.info("SQLite store closed")

    # -------------------------------------------------------------------------
    # State Operations
    # -------------------------------------------------------------------------

    async def get(self, strategy_id: str) -> StateData | None:
        """Get state for a strategy (single row per agent).

        Args:
            strategy_id: Strategy identifier.

        Returns:
            StateData if found, None otherwise.
        """
        if not self._initialized:
            await self.initialize()

        def _sync_get() -> StateData | None:
            cursor = self._conn.execute(  # type: ignore[union-attr]
                """
                SELECT strategy_id, version, state_data, schema_version,
                       checksum, created_at
                FROM v2_strategy_state
                WHERE strategy_id = ?
                """,
                (strategy_id,),
            )
            row = cursor.fetchone()

            if row is None:
                return None

            state_data = row["state_data"]
            if isinstance(state_data, str):
                state_data = json.loads(state_data)

            created_at = row["created_at"]
            if isinstance(created_at, str):
                created_at = datetime.fromisoformat(created_at)

            return StateData(
                strategy_id=row["strategy_id"],
                version=row["version"],
                state=state_data,
                schema_version=row["schema_version"],
                checksum=row["checksum"] or "",
                created_at=created_at,
                loaded_from=StateTier.WARM,  # SQLite acts as WARM tier
            )

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _sync_get)

    async def save(self, state: StateData, expected_version: int | None = None) -> bool:
        """Save state with optional CAS semantics (single row per agent).

        Args:
            state: State data to save.
            expected_version: Expected current version for CAS update.
                If None, upserts without version check.
                If provided, updates only if current version matches.

        Returns:
            True if save succeeded.

        Raises:
            StateConflictError: If expected_version doesn't match current version.
        """
        if not self._initialized:
            await self.initialize()

        # Calculate checksum
        state_json = json.dumps(state.state, sort_keys=True, default=str)
        checksum = hashlib.sha256(state_json.encode()).hexdigest()
        now = datetime.now(UTC).isoformat()

        def _sync_save() -> bool:
            if expected_version is not None:
                # CAS update -- version must match
                cursor = self._conn.execute(  # type: ignore[union-attr]
                    """
                    UPDATE v2_strategy_state
                    SET version = version + 1,
                        state_data = ?,
                        schema_version = ?,
                        checksum = ?,
                        updated_at = ?
                    WHERE strategy_id = ? AND version = ?
                    """,
                    (state_json, state.schema_version, checksum, now, state.strategy_id, expected_version),
                )
                if cursor.rowcount == 0:
                    # Check actual version for error message
                    cursor2 = self._conn.execute(  # type: ignore[union-attr]
                        "SELECT version FROM v2_strategy_state WHERE strategy_id = ?",
                        (state.strategy_id,),
                    )
                    row = cursor2.fetchone()
                    raise StateConflictError(
                        strategy_id=state.strategy_id,
                        expected_version=expected_version,
                        actual_version=row["version"] if row else 0,
                    )
                self._conn.commit()  # type: ignore[union-attr]
                return True
            else:
                # UPSERT: insert or update with version increment
                self._conn.execute(  # type: ignore[union-attr]
                    """
                    INSERT INTO v2_strategy_state
                    (strategy_id, version, state_data, schema_version, checksum,
                     created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (strategy_id)
                    DO UPDATE SET
                        version = v2_strategy_state.version + 1,
                        state_data = excluded.state_data,
                        schema_version = excluded.schema_version,
                        checksum = excluded.checksum,
                        updated_at = excluded.updated_at
                    """,
                    (
                        state.strategy_id,
                        state.version,
                        state_json,
                        state.schema_version,
                        checksum,
                        now,
                        now,
                    ),
                )
                self._conn.commit()  # type: ignore[union-attr]
                return True

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _sync_save)

    async def delete(self, strategy_id: str) -> bool:
        """Delete state row for a strategy.

        Args:
            strategy_id: Strategy identifier.

        Returns:
            True if state was deleted, False if not found.
        """
        if not self._initialized:
            await self.initialize()

        def _sync_delete() -> bool:
            cursor = self._conn.execute(  # type: ignore[union-attr]
                "DELETE FROM v2_strategy_state WHERE strategy_id = ?",
                (strategy_id,),
            )
            self._conn.commit()  # type: ignore[union-attr]
            return cursor.rowcount > 0

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _sync_delete)

    async def get_all_strategy_ids(self) -> list[str]:
        """Get all strategy IDs.

        Returns:
            List of strategy IDs.
        """
        if not self._initialized:
            await self.initialize()

        def _sync_get_ids() -> list[str]:
            cursor = self._conn.execute(  # type: ignore[union-attr]
                "SELECT strategy_id FROM v2_strategy_state ORDER BY strategy_id"
            )
            return [row["strategy_id"] for row in cursor.fetchall()]

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _sync_get_ids)

    # -------------------------------------------------------------------------
    # Timeline Event Operations
    # -------------------------------------------------------------------------

    async def save_event(self, event: TimelineEvent) -> int:
        """Save a timeline event.

        Args:
            event: Timeline event to save.

        Returns:
            ID of the saved event.
        """
        if not self._initialized:
            await self.initialize()

        event_json = json.dumps(event.event_data, sort_keys=True, default=str)
        created_at = event.created_at.isoformat()

        def _sync_save_event() -> int:
            cursor = self._conn.execute(  # type: ignore[union-attr]
                """
                INSERT INTO v2_timeline_events
                (strategy_id, event_type, event_data, correlation_id, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    event.strategy_id,
                    event.event_type,
                    event_json,
                    event.correlation_id,
                    created_at,
                ),
            )
            self._conn.commit()  # type: ignore[union-attr]
            return cursor.lastrowid or 0

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _sync_save_event)

    async def get_events(
        self,
        strategy_id: str,
        event_type: str | None = None,
        correlation_id: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[TimelineEvent]:
        """Get timeline events for a strategy.

        Args:
            strategy_id: Strategy identifier.
            event_type: Optional filter by event type.
            correlation_id: Optional filter by correlation ID.
            limit: Maximum number of events to return.
            offset: Number of events to skip for pagination.

        Returns:
            List of TimelineEvent, newest first.
        """
        if not self._initialized:
            await self.initialize()

        def _sync_get_events() -> list[TimelineEvent]:
            query = """
                SELECT id, strategy_id, event_type, event_data,
                       correlation_id, created_at
                FROM v2_timeline_events
                WHERE strategy_id = ?
            """
            params: list[Any] = [strategy_id]

            if event_type:
                query += " AND event_type = ?"
                params.append(event_type)

            if correlation_id:
                query += " AND correlation_id = ?"
                params.append(correlation_id)

            query += " ORDER BY created_at DESC LIMIT ? OFFSET ?"
            params.extend([limit, offset])

            cursor = self._conn.execute(query, params)  # type: ignore[union-attr]

            return [TimelineEvent.from_row(row) for row in cursor.fetchall()]

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _sync_get_events)

    async def get_event(self, event_id: int) -> TimelineEvent | None:
        """Get a specific timeline event by ID.

        Args:
            event_id: Event ID.

        Returns:
            TimelineEvent if found, None otherwise.
        """
        if not self._initialized:
            await self.initialize()

        def _sync_get_event() -> TimelineEvent | None:
            cursor = self._conn.execute(  # type: ignore[union-attr]
                """
                SELECT id, strategy_id, event_type, event_data,
                       correlation_id, created_at
                FROM v2_timeline_events
                WHERE id = ?
                """,
                (event_id,),
            )
            row = cursor.fetchone()
            return TimelineEvent.from_row(row) if row else None

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _sync_get_event)

    async def get_events_by_correlation_id(self, correlation_id: str) -> list[TimelineEvent]:
        """Get all events with a specific correlation ID.

        Args:
            correlation_id: Correlation ID to search for.

        Returns:
            List of TimelineEvent, newest first.
        """
        if not self._initialized:
            await self.initialize()

        def _sync_get_events() -> list[TimelineEvent]:
            cursor = self._conn.execute(  # type: ignore[union-attr]
                """
                SELECT id, strategy_id, event_type, event_data,
                       correlation_id, created_at
                FROM v2_timeline_events
                WHERE correlation_id = ?
                ORDER BY created_at DESC
                """,
                (correlation_id,),
            )
            return [TimelineEvent.from_row(row) for row in cursor.fetchall()]

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _sync_get_events)

    async def count_events(
        self,
        strategy_id: str | None = None,
        event_type: str | None = None,
    ) -> int:
        """Count timeline events.

        Args:
            strategy_id: Optional filter by strategy.
            event_type: Optional filter by event type.

        Returns:
            Number of matching events.
        """
        if not self._initialized:
            await self.initialize()

        def _sync_count() -> int:
            query = "SELECT COUNT(*) as count FROM v2_timeline_events WHERE 1=1"
            params: list[Any] = []

            if strategy_id:
                query += " AND strategy_id = ?"
                params.append(strategy_id)

            if event_type:
                query += " AND event_type = ?"
                params.append(event_type)

            cursor = self._conn.execute(query, params)  # type: ignore[union-attr]
            row = cursor.fetchone()
            return row["count"] if row else 0

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _sync_count)

    async def delete_events(
        self,
        strategy_id: str,
        before: datetime | None = None,
    ) -> int:
        """Delete timeline events for a strategy.

        Args:
            strategy_id: Strategy identifier.
            before: Optional datetime to delete events before (for retention).

        Returns:
            Number of events deleted.
        """
        if not self._initialized:
            await self.initialize()

        def _sync_delete() -> int:
            if before:
                cursor = self._conn.execute(  # type: ignore[union-attr]
                    """
                    DELETE FROM v2_timeline_events
                    WHERE strategy_id = ? AND created_at < ?
                    """,
                    (strategy_id, before.isoformat()),
                )
            else:
                cursor = self._conn.execute(  # type: ignore[union-attr]
                    """
                    DELETE FROM v2_timeline_events
                    WHERE strategy_id = ?
                    """,
                    (strategy_id,),
                )
            self._conn.commit()  # type: ignore[union-attr]
            return cursor.rowcount

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _sync_delete)

    # -------------------------------------------------------------------------
    # Maintenance Operations
    # -------------------------------------------------------------------------

    async def vacuum(self) -> None:
        """Reclaim disk space by running VACUUM.

        Should be run periodically after many deletes.
        """
        if not self._initialized:
            await self.initialize()

        def _sync_vacuum() -> None:
            self._conn.execute("VACUUM")  # type: ignore[union-attr]

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, _sync_vacuum)
        logger.info("SQLite VACUUM completed")

    async def checkpoint(self) -> None:
        """Checkpoint WAL file to main database.

        Forces WAL file contents to be written to the main database file.
        """
        if not self._initialized or not self._config.wal_mode:
            return

        def _sync_checkpoint() -> None:
            self._conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")  # type: ignore[union-attr]

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, _sync_checkpoint)
        logger.debug("SQLite WAL checkpoint completed")

    async def get_stats(self) -> dict[str, Any]:
        """Get database statistics.

        Returns:
            Dictionary with database statistics.
        """
        if not self._initialized:
            await self.initialize()

        def _sync_get_stats() -> dict[str, Any]:
            stats: dict[str, Any] = {
                "db_path": self._config.db_path,
                "wal_mode": self._config.wal_mode,
            }

            # Count states (single row per agent)
            cursor = self._conn.execute(  # type: ignore[union-attr]
                "SELECT COUNT(*) as count FROM v2_strategy_state"
            )
            row = cursor.fetchone()
            stats["active_states"] = row["count"] if row else 0

            # Count timeline events
            cursor = self._conn.execute(  # type: ignore[union-attr]
                "SELECT COUNT(*) as count FROM v2_timeline_events"
            )
            row = cursor.fetchone()
            stats["total_events"] = row["count"] if row else 0

            # Get page count and page size
            cursor = self._conn.execute("PRAGMA page_count")  # type: ignore[union-attr]
            row = cursor.fetchone()
            page_count = row[0] if row else 0

            cursor = self._conn.execute("PRAGMA page_size")  # type: ignore[union-attr]
            row = cursor.fetchone()
            page_size = row[0] if row else 0

            stats["page_count"] = page_count
            stats["page_size"] = page_size
            stats["db_size_bytes"] = page_count * page_size

            return stats

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _sync_get_stats)

    # -------------------------------------------------------------------------
    # CLOB Order Operations
    # -------------------------------------------------------------------------

    async def save_clob_order(self, order: "ClobOrderState") -> bool:
        """Save or update a CLOB order state.

        If order_id exists, updates the existing record.
        Otherwise, inserts a new record.

        Args:
            order: ClobOrderState to persist.

        Returns:
            True if save succeeded.
        """
        if not self._initialized:
            await self.initialize()

        fills_json = json.dumps([f.to_dict() for f in order.fills], default=str)
        metadata_json = json.dumps(order.metadata, default=str)
        now = datetime.now(UTC).isoformat()

        def _sync_save() -> bool:
            # Check if order already exists
            cursor = self._conn.execute(  # type: ignore[union-attr]
                "SELECT id FROM v2_clob_orders WHERE order_id = ?",
                (order.order_id,),
            )
            existing = cursor.fetchone()

            if existing:
                # Update existing order
                self._conn.execute(  # type: ignore[union-attr]
                    """
                    UPDATE v2_clob_orders
                    SET market_id = ?, token_id = ?, side = ?, status = ?,
                        price = ?, size = ?, filled_size = ?, average_fill_price = ?,
                        fills = ?, order_type = ?, intent_id = ?, error = ?,
                        metadata = ?, updated_at = ?
                    WHERE order_id = ?
                    """,
                    (
                        order.market_id,
                        order.token_id,
                        order.side,
                        order.status.value,
                        str(order.price),
                        str(order.size),
                        str(order.filled_size),
                        str(order.average_fill_price) if order.average_fill_price else None,
                        fills_json,
                        order.order_type,
                        order.intent_id,
                        order.error,
                        metadata_json,
                        now,
                        order.order_id,
                    ),
                )
            else:
                # Insert new order
                self._conn.execute(  # type: ignore[union-attr]
                    """
                    INSERT INTO v2_clob_orders
                    (order_id, market_id, token_id, side, status, price, size,
                     filled_size, average_fill_price, fills, order_type, intent_id,
                     error, metadata, submitted_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        order.order_id,
                        order.market_id,
                        order.token_id,
                        order.side,
                        order.status.value,
                        str(order.price),
                        str(order.size),
                        str(order.filled_size),
                        str(order.average_fill_price) if order.average_fill_price else None,
                        fills_json,
                        order.order_type,
                        order.intent_id,
                        order.error,
                        metadata_json,
                        order.submitted_at.isoformat(),
                        now,
                    ),
                )

            self._conn.commit()  # type: ignore[union-attr]
            return True

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _sync_save)

    async def get_clob_order(self, order_id: str) -> "ClobOrderState | None":
        """Get a CLOB order by order_id.

        Args:
            order_id: Order identifier.

        Returns:
            ClobOrderState if found, None otherwise.
        """
        if not self._initialized:
            await self.initialize()

        def _sync_get() -> "ClobOrderState | None":
            cursor = self._conn.execute(  # type: ignore[union-attr]
                """
                SELECT order_id, market_id, token_id, side, status, price, size,
                       filled_size, average_fill_price, fills, order_type, intent_id,
                       error, metadata, submitted_at, updated_at
                FROM v2_clob_orders
                WHERE order_id = ?
                """,
                (order_id,),
            )
            row = cursor.fetchone()

            if row is None:
                return None

            return self._row_to_clob_order(row)

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _sync_get)

    async def get_open_v2_clob_orders(self, market_id: str | None = None) -> list["ClobOrderState"]:
        """Get all open CLOB orders, optionally filtered by market.

        Open orders are those with status: pending, submitted, live, partially_filled.

        Args:
            market_id: Optional market ID to filter by.

        Returns:
            List of open ClobOrderState, newest first.
        """
        if not self._initialized:
            await self.initialize()

        open_statuses = ("pending", "submitted", "live", "partially_filled")

        def _sync_get() -> list["ClobOrderState"]:
            if market_id:
                placeholders = ",".join("?" * len(open_statuses))
                cursor = self._conn.execute(  # type: ignore[union-attr]
                    f"""
                    SELECT order_id, market_id, token_id, side, status, price, size,
                           filled_size, average_fill_price, fills, order_type, intent_id,
                           error, metadata, submitted_at, updated_at
                    FROM v2_clob_orders
                    WHERE market_id = ? AND status IN ({placeholders})
                    ORDER BY submitted_at DESC
                    """,
                    (market_id, *open_statuses),
                )
            else:
                placeholders = ",".join("?" * len(open_statuses))
                cursor = self._conn.execute(  # type: ignore[union-attr]
                    f"""
                    SELECT order_id, market_id, token_id, side, status, price, size,
                           filled_size, average_fill_price, fills, order_type, intent_id,
                           error, metadata, submitted_at, updated_at
                    FROM v2_clob_orders
                    WHERE status IN ({placeholders})
                    ORDER BY submitted_at DESC
                    """,
                    open_statuses,
                )

            return [self._row_to_clob_order(row) for row in cursor.fetchall()]

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _sync_get)

    async def update_clob_order_status(
        self,
        order_id: str,
        status: "ClobOrderStatus",
        fills: list["ClobFill"] | None = None,
        filled_size: str | None = None,
        average_fill_price: str | None = None,
        error: str | None = None,
    ) -> bool:
        """Update the status and fill information of a CLOB order.

        Args:
            order_id: Order identifier.
            status: New order status.
            fills: Updated list of fills (replaces existing).
            filled_size: Updated filled size.
            average_fill_price: Updated average fill price.
            error: Error message if order failed.

        Returns:
            True if order was found and updated.
        """
        if not self._initialized:
            await self.initialize()

        now = datetime.now(UTC).isoformat()

        def _sync_update() -> bool:
            # Build dynamic update query
            updates = ["status = ?", "updated_at = ?"]
            params: list[Any] = [status.value, now]

            if fills is not None:
                fills_json = json.dumps([f.to_dict() for f in fills], default=str)
                updates.append("fills = ?")
                params.append(fills_json)

            if filled_size is not None:
                updates.append("filled_size = ?")
                params.append(filled_size)

            if average_fill_price is not None:
                updates.append("average_fill_price = ?")
                params.append(average_fill_price)

            if error is not None:
                updates.append("error = ?")
                params.append(error)

            params.append(order_id)

            query = f"UPDATE v2_clob_orders SET {', '.join(updates)} WHERE order_id = ?"
            cursor = self._conn.execute(query, params)  # type: ignore[union-attr]
            self._conn.commit()  # type: ignore[union-attr]
            return cursor.rowcount > 0

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _sync_update)

    async def delete_clob_order(self, order_id: str) -> bool:
        """Delete a CLOB order from storage.

        Args:
            order_id: Order identifier.

        Returns:
            True if order was found and deleted.
        """
        if not self._initialized:
            await self.initialize()

        def _sync_delete() -> bool:
            cursor = self._conn.execute(  # type: ignore[union-attr]
                "DELETE FROM v2_clob_orders WHERE order_id = ?",
                (order_id,),
            )
            self._conn.commit()  # type: ignore[union-attr]
            return cursor.rowcount > 0

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _sync_delete)

    async def get_v2_clob_orders_by_intent(self, intent_id: str) -> list["ClobOrderState"]:
        """Get all CLOB orders associated with an intent.

        Args:
            intent_id: Intent identifier.

        Returns:
            List of ClobOrderState, newest first.
        """
        if not self._initialized:
            await self.initialize()

        def _sync_get() -> list["ClobOrderState"]:
            cursor = self._conn.execute(  # type: ignore[union-attr]
                """
                SELECT order_id, market_id, token_id, side, status, price, size,
                       filled_size, average_fill_price, fills, order_type, intent_id,
                       error, metadata, submitted_at, updated_at
                FROM v2_clob_orders
                WHERE intent_id = ?
                ORDER BY submitted_at DESC
                """,
                (intent_id,),
            )
            return [self._row_to_clob_order(row) for row in cursor.fetchall()]

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _sync_get)

    def _row_to_clob_order(self, row: sqlite3.Row) -> "ClobOrderState":
        """Convert a SQLite row to ClobOrderState.

        Args:
            row: SQLite row from v2_clob_orders table.

        Returns:
            ClobOrderState instance.
        """
        # Import here to avoid circular imports
        from decimal import Decimal

        from almanak.framework.execution.clob_handler import (
            ClobFill,
            ClobOrderState,
            ClobOrderStatus,
        )

        # Parse fills JSON
        fills_data = row["fills"]
        if isinstance(fills_data, str):
            fills_data = json.loads(fills_data)

        fills = [
            ClobFill(
                fill_id=f["fill_id"],
                price=Decimal(f["price"]),
                size=Decimal(f["size"]),
                fee=Decimal(f["fee"]),
                timestamp=datetime.fromisoformat(f["timestamp"]),
                counterparty=f.get("counterparty"),
            )
            for f in fills_data
        ]

        # Parse metadata JSON
        metadata = row["metadata"]
        if isinstance(metadata, str):
            metadata = json.loads(metadata)

        # Parse timestamps
        submitted_at = row["submitted_at"]
        if isinstance(submitted_at, str):
            submitted_at = datetime.fromisoformat(submitted_at)

        updated_at = row["updated_at"]
        if isinstance(updated_at, str):
            updated_at = datetime.fromisoformat(updated_at)

        return ClobOrderState(
            order_id=row["order_id"],
            market_id=row["market_id"],
            token_id=row["token_id"],
            side=row["side"],
            status=ClobOrderStatus(row["status"]),
            price=Decimal(row["price"]),
            size=Decimal(row["size"]),
            filled_size=Decimal(row["filled_size"]),
            average_fill_price=Decimal(row["average_fill_price"]) if row["average_fill_price"] else None,
            fills=fills,
            order_type=row["order_type"],
            intent_id=row["intent_id"],
            submitted_at=submitted_at,
            updated_at=updated_at,
            error=row["error"],
            metadata=metadata,
        )

    # =========================================================================
    # Portfolio Snapshot Methods
    # =========================================================================

    async def save_portfolio_snapshot(self, snapshot: "PortfolioSnapshot") -> int:
        """Save a portfolio snapshot for value tracking.

        Args:
            snapshot: PortfolioSnapshot to persist.

        Returns:
            ID of the inserted row.

        Note:
            Uses INSERT OR REPLACE to handle unique constraint on (strategy_id, timestamp).
        """

        if not self._initialized:
            await self.initialize()

        def _sync_save() -> int:
            now = datetime.now(UTC).isoformat()

            cursor = self._conn.execute(  # type: ignore[union-attr]
                """
                INSERT OR REPLACE INTO v2_portfolio_snapshots (
                    strategy_id, timestamp, iteration_number, total_value_usd,
                    available_cash_usd, value_confidence, positions_json, chain, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    snapshot.strategy_id,
                    snapshot.timestamp.isoformat(),
                    snapshot.iteration_number,
                    str(snapshot.total_value_usd),
                    str(snapshot.available_cash_usd),
                    snapshot.value_confidence.value,
                    json.dumps(snapshot.to_dict()["positions"]),
                    snapshot.chain,
                    now,
                ),
            )
            self._conn.commit()  # type: ignore[union-attr]
            return cursor.lastrowid or 0

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _sync_save)

    async def get_latest_snapshot(self, strategy_id: str) -> "PortfolioSnapshot | None":
        """Get the most recent portfolio snapshot for a strategy.

        Args:
            strategy_id: Strategy identifier.

        Returns:
            Most recent PortfolioSnapshot or None if not found.
        """

        if not self._initialized:
            await self.initialize()

        def _sync_get() -> "PortfolioSnapshot | None":
            cursor = self._conn.execute(  # type: ignore[union-attr]
                """
                SELECT strategy_id, timestamp, iteration_number, total_value_usd,
                       available_cash_usd, value_confidence, positions_json, chain
                FROM v2_portfolio_snapshots
                WHERE strategy_id = ?
                ORDER BY timestamp DESC
                LIMIT 1
                """,
                (strategy_id,),
            )
            row = cursor.fetchone()
            if not row:
                return None
            return self._row_to_portfolio_snapshot(row)

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _sync_get)

    async def get_snapshots_since(
        self,
        strategy_id: str,
        since: datetime,
        limit: int = 168,
    ) -> list["PortfolioSnapshot"]:
        """Get portfolio snapshots since a timestamp.

        Used for building PnL charts in the dashboard.

        Args:
            strategy_id: Strategy identifier.
            since: Start timestamp (inclusive).
            limit: Maximum number of snapshots to return (default 168 = 7 days hourly).

        Returns:
            List of PortfolioSnapshots ordered by timestamp ascending.
        """

        if not self._initialized:
            await self.initialize()

        def _sync_get() -> list["PortfolioSnapshot"]:
            cursor = self._conn.execute(  # type: ignore[union-attr]
                """
                SELECT strategy_id, timestamp, iteration_number, total_value_usd,
                       available_cash_usd, value_confidence, positions_json, chain
                FROM v2_portfolio_snapshots
                WHERE strategy_id = ? AND timestamp >= ?
                ORDER BY timestamp ASC
                LIMIT ?
                """,
                (strategy_id, since.isoformat(), limit),
            )
            return [self._row_to_portfolio_snapshot(row) for row in cursor.fetchall()]

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _sync_get)

    async def get_snapshot_at(
        self,
        strategy_id: str,
        timestamp: datetime,
    ) -> "PortfolioSnapshot | None":
        """Get the portfolio snapshot closest to a timestamp.

        Used for calculating PnL at specific points in time (e.g., 24h ago).

        Args:
            strategy_id: Strategy identifier.
            timestamp: Target timestamp.

        Returns:
            PortfolioSnapshot closest to timestamp or None if not found.
        """

        if not self._initialized:
            await self.initialize()

        def _sync_get() -> "PortfolioSnapshot | None":
            # Get closest snapshot at or before the timestamp
            cursor = self._conn.execute(  # type: ignore[union-attr]
                """
                SELECT strategy_id, timestamp, iteration_number, total_value_usd,
                       available_cash_usd, value_confidence, positions_json, chain
                FROM v2_portfolio_snapshots
                WHERE strategy_id = ? AND timestamp <= ?
                ORDER BY timestamp DESC
                LIMIT 1
                """,
                (strategy_id, timestamp.isoformat()),
            )
            row = cursor.fetchone()
            if not row:
                return None
            return self._row_to_portfolio_snapshot(row)

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _sync_get)

    def _row_to_portfolio_snapshot(self, row: sqlite3.Row) -> "PortfolioSnapshot":
        """Convert a SQLite row to PortfolioSnapshot.

        Args:
            row: SQLite row from v2_portfolio_snapshots table.

        Returns:
            PortfolioSnapshot instance.
        """
        from decimal import Decimal

        from almanak.framework.portfolio.models import (
            PortfolioSnapshot,
            PositionValue,
            ValueConfidence,
        )
        from almanak.framework.teardown.models import PositionType

        # Parse positions JSON
        positions_data = row["positions_json"]
        if isinstance(positions_data, str):
            positions_data = json.loads(positions_data)

        positions = []
        for p in positions_data:
            positions.append(
                PositionValue(
                    position_type=PositionType(p["position_type"]),
                    protocol=p["protocol"],
                    chain=p["chain"],
                    value_usd=Decimal(p["value_usd"]),
                    label=p["label"],
                    tokens=p.get("tokens", []),
                    details=p.get("details", {}),
                )
            )

        # Parse timestamp
        timestamp = row["timestamp"]
        if isinstance(timestamp, str):
            timestamp = datetime.fromisoformat(timestamp)

        return PortfolioSnapshot(
            timestamp=timestamp,
            strategy_id=row["strategy_id"],
            total_value_usd=Decimal(row["total_value_usd"]),
            available_cash_usd=Decimal(row["available_cash_usd"]),
            value_confidence=ValueConfidence(row["value_confidence"]),
            positions=positions,
            wallet_balances=[],  # Not stored in snapshots table, rebuild from positions
            chain=row["chain"] or "",
            iteration_number=row["iteration_number"] or 0,
        )

    # =========================================================================
    # Portfolio Metrics Methods (PnL Baseline)
    # =========================================================================

    async def save_v2_portfolio_metrics(self, metrics: "PortfolioMetrics") -> bool:
        """Save or update portfolio metrics for a strategy.

        Args:
            metrics: PortfolioMetrics to persist.

        Returns:
            True if successful.
        """

        if not self._initialized:
            await self.initialize()

        def _sync_save() -> bool:
            now = datetime.now(UTC).isoformat()

            self._conn.execute(  # type: ignore[union-attr]
                """
                INSERT OR REPLACE INTO v2_portfolio_metrics (
                    strategy_id, initial_value_usd, initial_timestamp,
                    deposits_usd, withdrawals_usd, gas_spent_usd, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    metrics.strategy_id,
                    str(metrics.initial_value_usd),
                    metrics.timestamp.isoformat(),
                    str(metrics.deposits_usd),
                    str(metrics.withdrawals_usd),
                    str(metrics.gas_spent_usd),
                    now,
                ),
            )
            self._conn.commit()  # type: ignore[union-attr]
            return True

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _sync_save)

    async def get_v2_portfolio_metrics(self, strategy_id: str) -> "PortfolioMetrics | None":
        """Get portfolio metrics for a strategy.

        Args:
            strategy_id: Strategy identifier.

        Returns:
            PortfolioMetrics or None if not found.
        """
        from decimal import Decimal

        from almanak.framework.portfolio.models import PortfolioMetrics

        if not self._initialized:
            await self.initialize()

        def _sync_get() -> "PortfolioMetrics | None":
            cursor = self._conn.execute(  # type: ignore[union-attr]
                """
                SELECT strategy_id, initial_value_usd, initial_timestamp,
                       deposits_usd, withdrawals_usd, gas_spent_usd, updated_at
                FROM v2_portfolio_metrics
                WHERE strategy_id = ?
                """,
                (strategy_id,),
            )
            row = cursor.fetchone()
            if not row:
                return None

            # Parse timestamp
            initial_timestamp = row["initial_timestamp"]
            if isinstance(initial_timestamp, str):
                initial_timestamp = datetime.fromisoformat(initial_timestamp)

            updated_at = row["updated_at"]
            if isinstance(updated_at, str):
                updated_at = datetime.fromisoformat(updated_at)

            return PortfolioMetrics(
                strategy_id=row["strategy_id"],
                timestamp=updated_at,
                total_value_usd=Decimal("0"),  # Not stored, get from latest snapshot
                initial_value_usd=Decimal(row["initial_value_usd"]),
                deposits_usd=Decimal(row["deposits_usd"]),
                withdrawals_usd=Decimal(row["withdrawals_usd"]),
                gas_spent_usd=Decimal(row["gas_spent_usd"]),
            )

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _sync_get)

    async def cleanup_old_snapshots(self, days: int = 7) -> int:
        """Delete portfolio snapshots older than specified days.

        Args:
            days: Number of days to retain snapshots.

        Returns:
            Number of deleted rows.
        """
        if not self._initialized:
            await self.initialize()

        def _sync_cleanup() -> int:
            cutoff = (datetime.now(UTC) - timedelta(days=days)).isoformat()

            cursor = self._conn.execute(  # type: ignore[union-attr]
                """
                DELETE FROM v2_portfolio_snapshots
                WHERE created_at < ?
                """,
                (cutoff,),
            )
            self._conn.commit()  # type: ignore[union-attr]
            return cursor.rowcount

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _sync_cleanup)
