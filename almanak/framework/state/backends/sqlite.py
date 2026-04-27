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
import threading
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from ..state_manager import StateConflictError, StateData, StateTier

if TYPE_CHECKING:
    from almanak.framework.execution.clob_handler import ClobFill, ClobOrderState, ClobOrderStatus
    from almanak.framework.observability.ledger import LedgerEntry
    from almanak.framework.observability.position_events import PositionEvent
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
CREATE TABLE IF NOT EXISTS strategy_state (
    strategy_id TEXT PRIMARY KEY,
    version INTEGER NOT NULL DEFAULT 1,
    state_data TEXT NOT NULL,  -- JSON string
    schema_version INTEGER NOT NULL DEFAULT 1,
    checksum TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

-- Timeline events table (matches PostgreSQL schema)
CREATE TABLE IF NOT EXISTS timeline_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    strategy_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    event_data TEXT NOT NULL,  -- JSON string
    correlation_id TEXT,
    cycle_id TEXT DEFAULT '',
    phase TEXT DEFAULT '',
    created_at TEXT NOT NULL
);

-- Index for strategy event queries
CREATE INDEX IF NOT EXISTS idx_timeline_events_strategy_id
ON timeline_events (strategy_id);

-- Index for correlation ID queries
CREATE INDEX IF NOT EXISTS idx_timeline_events_correlation_id
ON timeline_events (correlation_id);

-- Index for event type queries
CREATE INDEX IF NOT EXISTS idx_timeline_events_event_type
ON timeline_events (event_type);

-- Index for time-ordered queries
CREATE INDEX IF NOT EXISTS idx_timeline_events_created_at
ON timeline_events (created_at DESC);

-- CLOB orders table for Polymarket order tracking
CREATE TABLE IF NOT EXISTS clob_orders (
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
CREATE INDEX IF NOT EXISTS idx_clob_orders_order_id
ON clob_orders (order_id);

-- Index for market_id queries (open orders by market)
CREATE INDEX IF NOT EXISTS idx_clob_orders_market_id
ON clob_orders (market_id);

-- Index for status queries (finding open orders)
CREATE INDEX IF NOT EXISTS idx_clob_orders_status
ON clob_orders (status);

-- Index for intent_id queries (tracing orders to intents)
CREATE INDEX IF NOT EXISTS idx_clob_orders_intent_id
ON clob_orders (intent_id);

-- Composite index for open orders by market
CREATE INDEX IF NOT EXISTS idx_clob_orders_market_status
ON clob_orders (market_id, status);

-- Portfolio snapshots table for value tracking and PnL charts
CREATE TABLE IF NOT EXISTS portfolio_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    strategy_id TEXT NOT NULL,
    deployment_id TEXT DEFAULT '',  -- Phase 4: canonical identity key (VIB-2835)
    cycle_id TEXT DEFAULT '',  -- Phase 4: correlation to iteration (VIB-2835)
    execution_mode TEXT DEFAULT '',  -- Phase 4: live, paper, dry_run (VIB-2837)
    timestamp TEXT NOT NULL,
    iteration_number INTEGER DEFAULT 0,
    total_value_usd TEXT NOT NULL,  -- Decimal as string
    available_cash_usd TEXT NOT NULL,  -- Decimal as string
    value_confidence TEXT DEFAULT 'HIGH',  -- HIGH, ESTIMATED, STALE, UNAVAILABLE
    positions_json TEXT NOT NULL,  -- JSON array of positions
    token_prices_json TEXT DEFAULT '{}',  -- {chain:address: {price_usd, symbol, decimals}}
    wallet_balances_json TEXT DEFAULT '[]',  -- JSON array of TokenBalance dicts
    chain TEXT,
    created_at TEXT NOT NULL
);

-- Index for strategy + time queries (dashboard charts)
CREATE INDEX IF NOT EXISTS idx_portfolio_snapshots_strategy_time
ON portfolio_snapshots (strategy_id, timestamp DESC);

-- Index for cleanup queries
CREATE INDEX IF NOT EXISTS idx_portfolio_snapshots_created_at
ON portfolio_snapshots (created_at);

-- Unique constraint to prevent duplicate timestamps per strategy
CREATE UNIQUE INDEX IF NOT EXISTS idx_portfolio_snapshots_unique
ON portfolio_snapshots (strategy_id, timestamp);

-- Portfolio metrics table for PnL baseline tracking
-- Stores values that survive strategy restarts
CREATE TABLE IF NOT EXISTS portfolio_metrics (
    strategy_id TEXT PRIMARY KEY,
    initial_value_usd TEXT NOT NULL,  -- Decimal as string, set on first run
    initial_timestamp TEXT NOT NULL,
    deposits_usd TEXT DEFAULT '0',
    withdrawals_usd TEXT DEFAULT '0',
    gas_spent_usd TEXT DEFAULT '0',
    total_value_usd TEXT DEFAULT '0',  -- Current portfolio value (VIB-2765)
    positions_json TEXT DEFAULT '[]',  -- Snapshot of position state (VIB-2765)
    cycle_id TEXT,  -- Correlation to portfolio_snapshots (VIB-2765)
    deployment_id TEXT DEFAULT '',  -- Phase 4: canonical identity key (VIB-2835)
    execution_mode TEXT DEFAULT '',  -- Phase 4: live, paper, dry_run (VIB-2837)
    is_complete BOOLEAN DEFAULT 1,  -- Phase 4: all records for this cycle committed (VIB-2839)
    updated_at TEXT NOT NULL
);

-- Transaction ledger -- structured trade records (VIB-2402)
CREATE TABLE IF NOT EXISTS transaction_ledger (
    id TEXT PRIMARY KEY,
    cycle_id TEXT NOT NULL,
    strategy_id TEXT NOT NULL,
    deployment_id TEXT DEFAULT '',  -- Phase 4: canonical identity key (VIB-2835)
    execution_mode TEXT DEFAULT '',  -- Phase 4: live, paper, dry_run (VIB-2837)
    timestamp TEXT NOT NULL,
    intent_type TEXT NOT NULL,
    token_in TEXT,
    amount_in TEXT,
    token_out TEXT,
    amount_out TEXT,
    effective_price TEXT,
    slippage_bps REAL,
    gas_used INTEGER,
    gas_usd TEXT,
    tx_hash TEXT,
    chain TEXT,
    protocol TEXT,
    success BOOLEAN NOT NULL DEFAULT 1,
    error TEXT,
    extracted_data_json TEXT DEFAULT '',
    price_inputs_json TEXT DEFAULT '',   -- token prices at execution time (VIB-3480)
    pre_state_json TEXT DEFAULT '',      -- on-chain state before execution (VIB-3480)
    post_state_json TEXT DEFAULT ''      -- on-chain state after execution (VIB-3480)
);

-- Index for strategy + time queries
CREATE INDEX IF NOT EXISTS idx_transaction_ledger_strategy_time
ON transaction_ledger (strategy_id, timestamp DESC);

-- Index for cycle correlation
CREATE INDEX IF NOT EXISTS idx_transaction_ledger_cycle_id
ON transaction_ledger (cycle_id);

-- Index for intent type filtering
CREATE INDEX IF NOT EXISTS idx_transaction_ledger_intent_type
ON transaction_ledger (strategy_id, intent_type);

-- Position lifecycle events (Phase 2, VIB-2774)
-- Tracks OPEN -> SNAPSHOT* -> CLOSE for immutable-ID positions (LP, perps).
CREATE TABLE IF NOT EXISTS position_events (
    id TEXT PRIMARY KEY,
    deployment_id TEXT NOT NULL,
    cycle_id TEXT DEFAULT '',  -- Phase 4: correlation to iteration (VIB-2835)
    execution_mode TEXT DEFAULT '',  -- Phase 4: live, paper, dry_run (VIB-2837)
    position_id TEXT NOT NULL,
    position_type TEXT NOT NULL,  -- LP, PERP
    event_type TEXT NOT NULL,  -- OPEN, CLOSE, COLLECT_FEES, SNAPSHOT
    timestamp TEXT NOT NULL,
    protocol TEXT,
    chain TEXT,
    token0 TEXT,
    token1 TEXT,
    amount0 TEXT,
    amount1 TEXT,
    value_usd TEXT,
    tick_lower INTEGER,
    tick_upper INTEGER,
    liquidity TEXT,
    in_range BOOLEAN,
    fees_token0 TEXT,
    fees_token1 TEXT,
    leverage TEXT,
    entry_price TEXT,
    mark_price TEXT,
    unrealized_pnl TEXT,
    is_long BOOLEAN,
    tx_hash TEXT,
    gas_usd TEXT,
    ledger_entry_id TEXT,
    protocol_fees_usd TEXT DEFAULT '',  -- VIB-3205: ProtocolFees.total_usd on triggering tx
    attribution_json TEXT DEFAULT '{}',
    attribution_version INTEGER DEFAULT 0
);

-- Index for position lifecycle queries
CREATE INDEX IF NOT EXISTS idx_position_events_lifecycle
ON position_events (deployment_id, position_id, timestamp);

-- Index for event type filtering
CREATE INDEX IF NOT EXISTS idx_position_events_type
ON position_events (deployment_id, event_type);

-- Index for position_id lookups
CREATE INDEX IF NOT EXISTS idx_position_events_position
ON position_events (position_id, timestamp);

-- Typed accounting events — unified store for LendingAccountingEvent,
-- PendleAccountingEvent, and future position types (VIB-3417).
-- Local SQLite only; hosted Postgres requires metrics-database migration (IMPL-3).
CREATE TABLE IF NOT EXISTS accounting_events (
    id TEXT PRIMARY KEY,
    deployment_id TEXT NOT NULL,
    strategy_id TEXT NOT NULL,
    cycle_id TEXT NOT NULL,
    execution_mode TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    chain TEXT NOT NULL,
    protocol TEXT NOT NULL,
    wallet_address TEXT NOT NULL,
    event_type TEXT NOT NULL,
    position_key TEXT NOT NULL,
    ledger_entry_id TEXT,
    tx_hash TEXT,
    confidence TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    schema_version INTEGER NOT NULL DEFAULT 1
);

CREATE INDEX IF NOT EXISTS idx_ae_deployment_ts
ON accounting_events (deployment_id, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_ae_position
ON accounting_events (deployment_id, position_key, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_ae_event_type
ON accounting_events (deployment_id, event_type, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_ae_cycle
ON accounting_events (cycle_id);

CREATE INDEX IF NOT EXISTS idx_ae_ledger
ON accounting_events (ledger_entry_id);

-- Durable accounting outbox (VIB-3480).
-- Written synchronously on the execution hot path; drained asynchronously by
-- AccountingProcessor (VIB-3467).  Crash-safe: items remain pending until the
-- processor confirms successful write to accounting_events.
CREATE TABLE IF NOT EXISTS accounting_outbox (
    id TEXT PRIMARY KEY,                     -- UUID, generated at write time
    deployment_id TEXT NOT NULL,
    strategy_id TEXT NOT NULL,
    cycle_id TEXT NOT NULL,
    ledger_entry_id TEXT NOT NULL,           -- FK to transaction_ledger.id
    intent_type TEXT NOT NULL,               -- e.g. "SUPPLY", "LP_OPEN"
    status TEXT NOT NULL DEFAULT 'pending',  -- pending | processing | processed | failed
    attempts INTEGER NOT NULL DEFAULT 0,
    error TEXT DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

-- Partial index for drain queries — only unprocessed rows are ever scanned.
CREATE INDEX IF NOT EXISTS idx_outbox_drain
ON accounting_outbox (deployment_id, status, created_at ASC)
WHERE status != 'processed';
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
        self._db_lock = threading.Lock()

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
                # synchronous=FULL guarantees each commit is fsync'd to disk
                # before returning. This is required by the VIB-3156 durability
                # invariant: save_state() either durably persists or raises.
                # Without FULL, WAL can lose the last commit on crash, producing
                # recovery states where the chain nonce is ahead of the cached
                # state we reload.
                conn.execute("PRAGMA synchronous = FULL")
                # Auto-checkpoint WAL every 100 pages to prevent unbounded growth
                # when multiple processes write concurrently
                conn.execute("PRAGMA wal_autocheckpoint = 100")
            else:
                conn.execute(f"PRAGMA journal_mode = {self._config.journal_mode}")
                # Match WAL's strict durability for non-WAL journal modes too.
                conn.execute("PRAGMA synchronous = FULL")

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
            self._run_migrations()
            self._conn.commit()  # type: ignore[union-attr]

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, _sync_create_schema)

    def _run_migrations(self) -> None:
        """Run schema migrations for existing databases.

        Adds columns that may be missing from databases created before
        the accounting PRD changes. Each migration is idempotent.
        """
        conn = self._conn
        if conn is None:
            return

        def _add_column_if_missing(table: str, column: str, col_type: str) -> None:
            """Add a column to a table if it doesn't already exist."""
            cursor = conn.execute(f"PRAGMA table_info({table})")
            existing = {row["name"] for row in cursor.fetchall()}
            if column not in existing:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")
                logger.info(f"Migration: added {table}.{column}")

        # Phase 1a: total_value_usd, positions_json, cycle_id on portfolio_metrics (VIB-2765)
        _add_column_if_missing("portfolio_metrics", "total_value_usd", "TEXT DEFAULT '0'")
        _add_column_if_missing("portfolio_metrics", "positions_json", "TEXT DEFAULT '[]'")
        _add_column_if_missing("portfolio_metrics", "cycle_id", "TEXT")

        # Phase 1b: extracted_data_json on transaction_ledger
        _add_column_if_missing("transaction_ledger", "extracted_data_json", "TEXT DEFAULT ''")

        # VIB-3480: price and state capture columns for audit-grade replay
        _add_column_if_missing("transaction_ledger", "price_inputs_json", "TEXT DEFAULT ''")
        _add_column_if_missing("transaction_ledger", "pre_state_json", "TEXT DEFAULT ''")
        _add_column_if_missing("transaction_ledger", "post_state_json", "TEXT DEFAULT ''")

        # Phase 1c: token_prices_json and wallet_balances_json on portfolio_snapshots
        _add_column_if_missing("portfolio_snapshots", "token_prices_json", "TEXT DEFAULT '{}'")
        _add_column_if_missing("portfolio_snapshots", "wallet_balances_json", "TEXT DEFAULT '[]'")

        # Phase 4: deployment_id, cycle_id, execution_mode across all tables (VIB-2835, VIB-2837)
        _add_column_if_missing("portfolio_snapshots", "deployment_id", "TEXT DEFAULT ''")
        _add_column_if_missing("portfolio_snapshots", "cycle_id", "TEXT DEFAULT ''")
        _add_column_if_missing("portfolio_snapshots", "execution_mode", "TEXT DEFAULT ''")
        _add_column_if_missing("portfolio_metrics", "deployment_id", "TEXT DEFAULT ''")
        _add_column_if_missing("portfolio_metrics", "execution_mode", "TEXT DEFAULT ''")
        _add_column_if_missing("portfolio_metrics", "is_complete", "BOOLEAN DEFAULT 1")
        _add_column_if_missing("transaction_ledger", "deployment_id", "TEXT DEFAULT ''")
        _add_column_if_missing("transaction_ledger", "execution_mode", "TEXT DEFAULT ''")
        _add_column_if_missing("position_events", "cycle_id", "TEXT DEFAULT ''")
        _add_column_if_missing("position_events", "execution_mode", "TEXT DEFAULT ''")

        # VIB-3205: protocol_fees_usd captured from ProtocolFees.total_usd at
        # event time so attribution can attribute real fee PnL (not the v1
        # placeholder of 0). Empty string remains the "unknown" sentinel.
        _add_column_if_missing("position_events", "protocol_fees_usd", "TEXT DEFAULT ''")

    async def backfill_deployment_id(self, old_strategy_id: str, new_deployment_id: str) -> int:
        """Migrate data from a bare strategy name to the canonical deployment_id.

        Rewrites ``strategy_id`` in all accounting tables so that data written
        under the old bare name is accessible under the new deployment_id.

        Idempotent: rows already using ``new_deployment_id`` are unaffected.
        Skipped if ``old_strategy_id == new_deployment_id`` or if the old ID
        doesn't match any existing rows.

        Args:
            old_strategy_id: The bare strategy name (e.g. "AaveYieldStrategy").
            new_deployment_id: The new deployment_id (e.g. "AaveYieldStrategy:a1b2c3d4e5f6").

        Returns:
            Total number of rows migrated across all tables.
        """
        if old_strategy_id == new_deployment_id:
            return 0
        if not self._initialized:
            await self.initialize()

        def _sync_backfill() -> int:
            conn = self._conn
            assert conn is not None
            total = 0

            tables = [
                "strategy_state",
                "portfolio_snapshots",
                "portfolio_metrics",
                "transaction_ledger",
                "timeline_events",
                "accounting_outbox",
            ]

            with self._db_lock:
                conn.execute("BEGIN IMMEDIATE")
                try:
                    for table in tables:
                        # Check table exists (some may not be present in older DBs)
                        exists = conn.execute(
                            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                            (table,),
                        ).fetchone()
                        if not exists:
                            continue

                        # Only migrate if the old ID has data and the new one doesn't
                        old_count = conn.execute(
                            f"SELECT COUNT(*) FROM {table} WHERE strategy_id = ?",
                            (old_strategy_id,),
                        ).fetchone()[0]

                        if old_count == 0:
                            continue

                        # Skip if target already has rows (avoids PK/unique-index collisions)
                        new_count = conn.execute(
                            f"SELECT COUNT(*) FROM {table} WHERE strategy_id = ?",
                            (new_deployment_id,),
                        ).fetchone()[0]
                        if new_count > 0:
                            continue

                        cursor = conn.execute(
                            f"UPDATE {table} SET strategy_id = ? WHERE strategy_id = ?",
                            (new_deployment_id, old_strategy_id),
                        )
                        total += cursor.rowcount

                    # position_events, accounting_events, and accounting_outbox use deployment_id
                    for dep_id_table in ("position_events", "accounting_events", "accounting_outbox"):
                        tbl_exists = conn.execute(
                            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                            (dep_id_table,),
                        ).fetchone()
                        if not tbl_exists:
                            continue
                        old_rows = conn.execute(
                            f"SELECT COUNT(*) FROM {dep_id_table} WHERE deployment_id = ?",
                            (old_strategy_id,),
                        ).fetchone()[0]
                        if old_rows > 0:
                            cursor = conn.execute(
                                f"UPDATE {dep_id_table} SET deployment_id = ? WHERE deployment_id = ?",
                                (new_deployment_id, old_strategy_id),
                            )
                            total += cursor.rowcount

                    conn.execute("COMMIT")
                    if total > 0:
                        logger.info(
                            "Backfilled %d rows from '%s' to '%s'",
                            total,
                            old_strategy_id,
                            new_deployment_id,
                        )
                except Exception:
                    conn.execute("ROLLBACK")
                    raise

            return total

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _sync_backfill)

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
                FROM strategy_state
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

        Durability (VIB-3156):
            The write is performed inside a single SQLite transaction
            (``BEGIN IMMEDIATE`` ... ``COMMIT``) that spans both the
            version-CAS check and the data write. With
            ``synchronous = FULL`` (set in ``_connect``) the commit is
            fsync'd before returning, so on success the caller has the
            durability guarantee: a crash after this function returns
            will either see the full new row or the prior row -- never a
            torn state with version bumped but stale checksum. The
            checksum-vs-state_data consistency is verified BEFORE the
            transaction commits, so an invalid serialization never
            reaches disk at the real row.

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

        # Calculate checksum from state_data. The stored checksum must be a
        # function of state_json alone so that a post-crash reader can
        # re-derive it; that is the check gating recovery.
        state_json = json.dumps(state.state, sort_keys=True, default=str)
        checksum = hashlib.sha256(state_json.encode()).hexdigest()
        # Verify: re-hash state_json and confirm it equals the checksum we are
        # about to commit. This is the pre-commit equivalent of "verify before
        # rename" for file-atomic writes -- it catches any checksum drift
        # (e.g. non-deterministic serialization) BEFORE a version bump lands.
        if hashlib.sha256(state_json.encode()).hexdigest() != checksum:
            raise SQLiteBackendError(
                f"Pre-commit checksum verification failed for {state.strategy_id}; refusing to write torn state"
            )
        now = datetime.now(UTC).isoformat()

        def _sync_save() -> bool:
            conn = self._conn
            assert conn is not None

            # Serialize concurrent writers and open an immediate write
            # transaction so the version read and write are atomic.
            with self._db_lock:
                conn.execute("BEGIN IMMEDIATE")
                try:
                    if expected_version is not None:
                        # CAS update -- version must match
                        cursor = conn.execute(
                            """
                            UPDATE strategy_state
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
                            # Read actual version while still inside the
                            # transaction so the error is consistent with what
                            # the CAS saw.
                            row = conn.execute(
                                "SELECT version FROM strategy_state WHERE strategy_id = ?",
                                (state.strategy_id,),
                            ).fetchone()
                            conn.execute("ROLLBACK")
                            raise StateConflictError(
                                strategy_id=state.strategy_id,
                                expected_version=expected_version,
                                actual_version=row["version"] if row else 0,
                            )
                    else:
                        # UPSERT: insert or update with version increment
                        conn.execute(
                            """
                            INSERT INTO strategy_state
                            (strategy_id, version, state_data, schema_version, checksum,
                             created_at, updated_at)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                            ON CONFLICT (strategy_id)
                            DO UPDATE SET
                                version = strategy_state.version + 1,
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
                    conn.execute("COMMIT")
                except StateConflictError:
                    # Already rolled back above.
                    raise
                except Exception:
                    conn.execute("ROLLBACK")
                    raise
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
                "DELETE FROM strategy_state WHERE strategy_id = ?",
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
                "SELECT strategy_id FROM strategy_state ORDER BY strategy_id"
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
                INSERT INTO timeline_events
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
                FROM timeline_events
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
                FROM timeline_events
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
                FROM timeline_events
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
            query = "SELECT COUNT(*) as count FROM timeline_events WHERE 1=1"
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
                    DELETE FROM timeline_events
                    WHERE strategy_id = ? AND created_at < ?
                    """,
                    (strategy_id, before.isoformat()),
                )
            else:
                cursor = self._conn.execute(  # type: ignore[union-attr]
                    """
                    DELETE FROM timeline_events
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
                "SELECT COUNT(*) as count FROM strategy_state"
            )
            row = cursor.fetchone()
            stats["active_states"] = row["count"] if row else 0

            # Count timeline events
            cursor = self._conn.execute(  # type: ignore[union-attr]
                "SELECT COUNT(*) as count FROM timeline_events"
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
                "SELECT id FROM clob_orders WHERE order_id = ?",
                (order.order_id,),
            )
            existing = cursor.fetchone()

            if existing:
                # Update existing order
                self._conn.execute(  # type: ignore[union-attr]
                    """
                    UPDATE clob_orders
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
                    INSERT INTO clob_orders
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
                FROM clob_orders
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

    async def get_open_clob_orders(self, market_id: str | None = None) -> list["ClobOrderState"]:
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
                    FROM clob_orders
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
                    FROM clob_orders
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

            query = f"UPDATE clob_orders SET {', '.join(updates)} WHERE order_id = ?"
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
                "DELETE FROM clob_orders WHERE order_id = ?",
                (order_id,),
            )
            self._conn.commit()  # type: ignore[union-attr]
            return cursor.rowcount > 0

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _sync_delete)

    async def get_clob_orders_by_intent(self, intent_id: str) -> list["ClobOrderState"]:
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
                FROM clob_orders
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
            row: SQLite row from clob_orders table.

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
                INSERT OR REPLACE INTO portfolio_snapshots (
                    strategy_id, timestamp, iteration_number, total_value_usd,
                    available_cash_usd, value_confidence, positions_json,
                    token_prices_json, wallet_balances_json,
                    chain, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    snapshot.strategy_id,
                    snapshot.timestamp.isoformat(),
                    snapshot.iteration_number,
                    str(snapshot.total_value_usd),
                    str(snapshot.available_cash_usd),
                    snapshot.value_confidence.value,
                    json.dumps(snapshot.to_positions_payload()),
                    json.dumps(snapshot.token_prices) if snapshot.token_prices else "{}",
                    json.dumps(
                        [
                            {
                                "symbol": b.symbol,
                                "balance": str(b.balance),
                                "value_usd": str(b.value_usd),
                                "address": b.address,
                                "price_usd": str(b.price_usd) if b.price_usd is not None else None,
                            }
                            for b in snapshot.wallet_balances
                        ]
                    )
                    if snapshot.wallet_balances
                    else "[]",
                    snapshot.chain,
                    now,
                ),
            )
            self._conn.commit()  # type: ignore[union-attr]
            return cursor.lastrowid or 0

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _sync_save)

    async def save_snapshot_and_metrics(
        self,
        snapshot: "PortfolioSnapshot",
        metrics: "PortfolioMetrics",
    ) -> int:
        """Atomically save a portfolio snapshot and its associated metrics.

        Wraps both writes in a single SQLite transaction so that a snapshot
        exists if-and-only-if its metrics row also exists.  This prevents
        the dashboard from ever showing ``$0`` when data actually exists.

        Args:
            snapshot: PortfolioSnapshot to persist.
            metrics: PortfolioMetrics to persist (same cycle).

        Returns:
            Row ID of the inserted snapshot.
        """
        if not self._initialized:
            await self.initialize()

        def _sync_save_atomic() -> int:
            now = datetime.now(UTC).isoformat()
            conn = self._conn
            assert conn is not None

            # Acquire _db_lock to serialize concurrent callers, then BEGIN
            # IMMEDIATE for the SQLite write lock.
            with self._db_lock:
                conn.execute("BEGIN IMMEDIATE")
                try:
                    # Extract Phase 4 fields from metrics (single source of truth)
                    deployment_id = getattr(metrics, "deployment_id", "") or ""
                    cycle_id = getattr(metrics, "cycle_id", "") or ""
                    execution_mode = getattr(metrics, "execution_mode", "") or ""

                    # 1. Save snapshot
                    cursor = conn.execute(
                        """
                        INSERT OR REPLACE INTO portfolio_snapshots (
                            strategy_id, deployment_id, cycle_id, execution_mode,
                            timestamp, iteration_number, total_value_usd,
                            available_cash_usd, value_confidence, positions_json,
                            token_prices_json, wallet_balances_json,
                            chain, created_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            snapshot.strategy_id,
                            deployment_id,
                            cycle_id,
                            execution_mode,
                            snapshot.timestamp.isoformat(),
                            snapshot.iteration_number,
                            str(snapshot.total_value_usd),
                            str(snapshot.available_cash_usd),
                            snapshot.value_confidence.value,
                            json.dumps(snapshot.to_positions_payload()),
                            json.dumps(snapshot.token_prices) if snapshot.token_prices else "{}",
                            json.dumps(
                                [
                                    {
                                        "symbol": b.symbol,
                                        "balance": str(b.balance),
                                        "value_usd": str(b.value_usd),
                                        "address": b.address,
                                        "price_usd": str(b.price_usd) if b.price_usd is not None else None,
                                    }
                                    for b in snapshot.wallet_balances
                                ]
                            )
                            if snapshot.wallet_balances
                            else "[]",
                            snapshot.chain,
                            now,
                        ),
                    )
                    snapshot_id = cursor.lastrowid or 0

                    # 2. Save metrics in the same transaction
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO portfolio_metrics (
                            strategy_id, initial_value_usd, initial_timestamp,
                            deposits_usd, withdrawals_usd, gas_spent_usd,
                            total_value_usd, positions_json, cycle_id,
                            deployment_id, execution_mode, is_complete, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            metrics.strategy_id,
                            str(metrics.initial_value_usd),
                            metrics.timestamp.isoformat(),
                            str(metrics.deposits_usd),
                            str(metrics.withdrawals_usd),
                            str(metrics.gas_spent_usd),
                            str(metrics.total_value_usd),
                            getattr(metrics, "positions_json", "[]"),
                            cycle_id,
                            deployment_id,
                            execution_mode,
                            getattr(metrics, "is_complete", True),
                            now,
                        ),
                    )

                    conn.execute("COMMIT")
                    return snapshot_id
                except Exception:
                    conn.execute("ROLLBACK")
                    raise

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _sync_save_atomic)

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
                       available_cash_usd, value_confidence, positions_json,
                       token_prices_json, wallet_balances_json, chain
                FROM portfolio_snapshots
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
                       available_cash_usd, value_confidence, positions_json,
                       token_prices_json, wallet_balances_json, chain
                FROM portfolio_snapshots
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
                       available_cash_usd, value_confidence, positions_json,
                       token_prices_json, wallet_balances_json, chain
                FROM portfolio_snapshots
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
            row: SQLite row from portfolio_snapshots table.

        Returns:
            PortfolioSnapshot instance.
        """
        from almanak.framework.portfolio.models import PortfolioSnapshot

        # Parse timestamp
        timestamp = row["timestamp"]
        if isinstance(timestamp, str):
            timestamp = datetime.fromisoformat(timestamp)
        positions_payload = row["positions_json"]
        if isinstance(positions_payload, str):
            positions_payload = json.loads(positions_payload)
        positions, snapshot_metadata = PortfolioSnapshot.unpack_positions_payload(positions_payload)

        # Deserialize token_prices_json (new column, may not exist in old DBs)
        token_prices: dict[str, dict] = {}
        try:
            tp_raw = row["token_prices_json"]
            if tp_raw and isinstance(tp_raw, str):
                token_prices = json.loads(tp_raw)
        except (KeyError, json.JSONDecodeError):
            pass

        # Deserialize wallet_balances_json (new column, may not exist in old DBs)
        wallet_balances_raw: list[dict] = []
        try:
            wb_raw = row["wallet_balances_json"]
            if wb_raw and isinstance(wb_raw, str):
                wallet_balances_raw = json.loads(wb_raw)
        except (KeyError, json.JSONDecodeError):
            pass

        return PortfolioSnapshot.from_dict(
            {
                "timestamp": timestamp.isoformat(),
                "strategy_id": row["strategy_id"],
                "total_value_usd": str(row["total_value_usd"]),
                "available_cash_usd": str(row["available_cash_usd"]),
                "value_confidence": row["value_confidence"],
                "positions": positions,
                "wallet_balances": wallet_balances_raw,
                "token_prices": token_prices,
                "chain": row["chain"] or "",
                "iteration_number": row["iteration_number"] or 0,
                "snapshot_metadata": snapshot_metadata,
            }
        )

    # =========================================================================
    # Portfolio Metrics Methods (PnL Baseline)
    # =========================================================================

    async def save_portfolio_metrics(self, metrics: "PortfolioMetrics") -> bool:
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
                INSERT OR REPLACE INTO portfolio_metrics (
                    strategy_id, initial_value_usd, initial_timestamp,
                    deposits_usd, withdrawals_usd, gas_spent_usd,
                    total_value_usd, positions_json, cycle_id,
                    deployment_id, execution_mode, is_complete, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    metrics.strategy_id,
                    str(metrics.initial_value_usd),
                    metrics.timestamp.isoformat(),
                    str(metrics.deposits_usd),
                    str(metrics.withdrawals_usd),
                    str(metrics.gas_spent_usd),
                    str(metrics.total_value_usd),
                    getattr(metrics, "positions_json", "[]"),
                    getattr(metrics, "cycle_id", None),
                    getattr(metrics, "deployment_id", "") or "",
                    getattr(metrics, "execution_mode", "") or "",
                    getattr(metrics, "is_complete", True),
                    now,
                ),
            )
            self._conn.commit()  # type: ignore[union-attr]
            return True

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _sync_save)

    async def get_portfolio_metrics(self, strategy_id: str) -> "PortfolioMetrics | None":
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
                       deposits_usd, withdrawals_usd, gas_spent_usd,
                       total_value_usd, positions_json, cycle_id,
                       deployment_id, execution_mode, is_complete, updated_at
                FROM portfolio_metrics
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

            # Read Phase 4 fields safely (may not exist in old DBs)
            deployment_id = ""
            execution_mode = ""
            is_complete = True
            try:
                deployment_id = row["deployment_id"] or ""
            except (KeyError, IndexError):
                pass
            try:
                execution_mode = row["execution_mode"] or ""
            except (KeyError, IndexError):
                pass
            try:
                is_complete = bool(row["is_complete"]) if row["is_complete"] is not None else True
            except (KeyError, IndexError):
                pass

            return PortfolioMetrics(
                strategy_id=row["strategy_id"],
                timestamp=updated_at,
                total_value_usd=Decimal(row["total_value_usd"] or "0"),
                initial_value_usd=Decimal(row["initial_value_usd"]),
                deposits_usd=Decimal(row["deposits_usd"]),
                withdrawals_usd=Decimal(row["withdrawals_usd"]),
                gas_spent_usd=Decimal(row["gas_spent_usd"]),
                positions_json=row["positions_json"] or "[]",
                cycle_id=row["cycle_id"],
                deployment_id=deployment_id,
                execution_mode=execution_mode,
                is_complete=is_complete,
            )

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _sync_get)

    # =========================================================================
    # Position Events Methods (Phase 2, VIB-2774)
    # =========================================================================

    async def save_position_event(self, event: "PositionEvent") -> bool:
        """Save a position lifecycle event.

        Args:
            event: PositionEvent to persist.

        Returns:
            True if successful.
        """

        if not self._initialized:
            await self.initialize()

        def _sync_save() -> bool:
            with self._db_lock:
                self._conn.execute(  # type: ignore[union-attr]
                    """
                    INSERT OR IGNORE INTO position_events (
                        id, deployment_id, cycle_id, execution_mode,
                        position_id, position_type, event_type,
                        timestamp, protocol, chain,
                        token0, token1, amount0, amount1, value_usd,
                        tick_lower, tick_upper, liquidity, in_range,
                        fees_token0, fees_token1,
                        leverage, entry_price, mark_price, unrealized_pnl, is_long,
                        tx_hash, gas_usd, ledger_entry_id,
                        protocol_fees_usd,
                        attribution_json, attribution_version
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        event.id,
                        event.deployment_id,
                        getattr(event, "cycle_id", "") or "",
                        getattr(event, "execution_mode", "") or "",
                        event.position_id,
                        event.position_type,
                        event.event_type,
                        event.timestamp.isoformat(),
                        event.protocol,
                        event.chain,
                        event.token0,
                        event.token1,
                        event.amount0,
                        event.amount1,
                        event.value_usd,
                        event.tick_lower,
                        event.tick_upper,
                        event.liquidity,
                        event.in_range,
                        event.fees_token0,
                        event.fees_token1,
                        event.leverage,
                        event.entry_price,
                        event.mark_price,
                        event.unrealized_pnl,
                        event.is_long,
                        event.tx_hash,
                        event.gas_usd,
                        event.ledger_entry_id,
                        # VIB-3205: preserve measured-zero vs unknown.
                        # ``getattr(..., "") or ""`` collapses a measured Decimal("0") to
                        # the empty string because Decimal(0) is falsy, which would make
                        # it indistinguishable from "parser did not emit protocol_fees"
                        # at read time. Normalize only the None / missing-attr cases.
                        ("" if getattr(event, "protocol_fees_usd", None) is None else str(event.protocol_fees_usd)),
                        event.attribution_json,
                        event.attribution_version,
                    ),
                )
                self._conn.commit()  # type: ignore[union-attr]
            return True

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _sync_save)

    async def update_position_attribution(self, event_id: str, attribution_json: str, attribution_version: int) -> bool:
        """Update only the attribution fields of a position event.

        Unlike save_position_event (INSERT OR REPLACE), this preserves all
        other stored fields (timestamp, token0/token1, ticks, liquidity, etc.).
        """
        if not self._initialized:
            await self.initialize()

        def _sync_update() -> bool:
            with self._db_lock:
                cursor = self._conn.execute(  # type: ignore[union-attr]
                    """
                    UPDATE position_events
                    SET attribution_json = ?, attribution_version = ?
                    WHERE id = ?
                    """,
                    (attribution_json, attribution_version, event_id),
                )
                self._conn.commit()  # type: ignore[union-attr]
            return cursor.rowcount > 0

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _sync_update)

    async def get_position_events(
        self,
        deployment_id: str,
        position_id: str | None = None,
        event_type: str | None = None,
        limit: int = 100,
    ) -> list[dict]:
        """Query position lifecycle events.

        Args:
            deployment_id: Strategy deployment identifier.
            position_id: Optional filter by position_id.
            event_type: Optional filter by event type (OPEN, CLOSE, etc.).
            limit: Maximum number of events to return.

        Returns:
            List of event dicts ordered by timestamp descending.
        """
        if not self._initialized:
            await self.initialize()

        def _sync_get() -> list[dict]:
            query = "SELECT * FROM position_events WHERE deployment_id = ?"
            params: list = [deployment_id]

            if position_id is not None:
                query += " AND position_id = ?"
                params.append(position_id)

            if event_type is not None:
                query += " AND event_type = ?"
                params.append(event_type)

            query += " ORDER BY timestamp DESC LIMIT ?"
            params.append(limit)

            cursor = self._conn.execute(query, params)  # type: ignore[union-attr]
            return [dict(row) for row in cursor.fetchall()]

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _sync_get)

    async def get_position_history(
        self,
        deployment_id: str,
        position_id: str,
    ) -> list[dict]:
        """Get full lifecycle for a single position.

        Returns events ordered chronologically (OPEN -> SNAPSHOT* -> CLOSE).

        Args:
            deployment_id: Strategy deployment identifier.
            position_id: The position to query.

        Returns:
            List of event dicts in chronological order.
        """
        if not self._initialized:
            await self.initialize()

        def _sync_get() -> list[dict]:
            cursor = self._conn.execute(  # type: ignore[union-attr]
                """
                SELECT * FROM position_events
                WHERE deployment_id = ? AND position_id = ?
                ORDER BY timestamp ASC
                """,
                (deployment_id, position_id),
            )
            return [dict(row) for row in cursor.fetchall()]

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
                DELETE FROM portfolio_snapshots
                WHERE created_at < ?
                """,
                (cutoff,),
            )
            self._conn.commit()  # type: ignore[union-attr]
            return cursor.rowcount

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _sync_cleanup)

    # =========================================================================
    # Transaction Ledger (VIB-2402)
    # =========================================================================

    async def save_ledger_entry(self, entry: "LedgerEntry") -> None:
        """Persist a transaction ledger entry.

        Args:
            entry: LedgerEntry to save.
        """

        if not self._initialized:
            await self.initialize()

        def _sync_save() -> None:
            with self._db_lock:
                self._conn.execute(  # type: ignore[union-attr]
                    """
                    INSERT OR REPLACE INTO transaction_ledger
                    (id, cycle_id, strategy_id, deployment_id, execution_mode,
                     timestamp, intent_type,
                     token_in, amount_in, token_out, amount_out,
                     effective_price, slippage_bps, gas_used, gas_usd,
                     tx_hash, chain, protocol, success, error,
                     extracted_data_json, price_inputs_json, pre_state_json, post_state_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        entry.id,
                        entry.cycle_id,
                        entry.strategy_id,
                        getattr(entry, "deployment_id", "") or "",
                        getattr(entry, "execution_mode", "") or "",
                        entry.timestamp.isoformat(),
                        entry.intent_type,
                        entry.token_in,
                        entry.amount_in,
                        entry.token_out,
                        entry.amount_out,
                        entry.effective_price,
                        entry.slippage_bps,
                        entry.gas_used,
                        entry.gas_usd,
                        entry.tx_hash,
                        entry.chain,
                        entry.protocol,
                        entry.success,
                        entry.error,
                        entry.extracted_data_json,
                        entry.price_inputs_json,
                        entry.pre_state_json,
                        entry.post_state_json,
                    ),
                )
                self._conn.commit()  # type: ignore[union-attr]

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, _sync_save)

    async def get_ledger_entries(
        self,
        strategy_id: str,
        since: datetime | None = None,
        intent_type: str | None = None,
        limit: int = 100,
    ) -> list["LedgerEntry"]:
        """Query transaction ledger entries.

        Args:
            strategy_id: Strategy to query.
            since: Only entries after this timestamp.
            intent_type: Filter by intent type.
            limit: Maximum entries to return.

        Returns:
            List of LedgerEntry objects, newest first.
        """
        from almanak.framework.observability.ledger import LedgerEntry

        if not self._initialized:
            await self.initialize()

        def _sync_get() -> list[LedgerEntry]:
            conditions = ["strategy_id = ?"]
            params: list[Any] = [strategy_id]

            if since is not None:
                conditions.append("timestamp > ?")
                params.append(since.isoformat())
            if intent_type is not None:
                conditions.append("intent_type = ?")
                params.append(intent_type)

            where = " AND ".join(conditions)
            params.append(limit)

            with self._db_lock:
                cursor = self._conn.execute(  # type: ignore[union-attr]
                    f"""
                    SELECT * FROM transaction_ledger
                    WHERE {where}
                    ORDER BY timestamp DESC
                    LIMIT ?
                    """,
                    params,
                )
                rows = cursor.fetchall()

            entries = []
            row_keys = None
            for row in rows:
                if row_keys is None:
                    row_keys = row.keys()
                entries.append(
                    LedgerEntry(
                        id=row["id"],
                        cycle_id=row["cycle_id"],
                        strategy_id=row["strategy_id"],
                        deployment_id=row["deployment_id"] if "deployment_id" in row_keys else "",
                        execution_mode=row["execution_mode"] if "execution_mode" in row_keys else "",
                        timestamp=datetime.fromisoformat(row["timestamp"]),
                        intent_type=row["intent_type"],
                        token_in=row["token_in"] or "",
                        amount_in=row["amount_in"] or "",
                        token_out=row["token_out"] or "",
                        amount_out=row["amount_out"] or "",
                        effective_price=row["effective_price"] or "",
                        slippage_bps=row["slippage_bps"],
                        gas_used=row["gas_used"] or 0,
                        gas_usd=row["gas_usd"] or "",
                        tx_hash=row["tx_hash"] or "",
                        chain=row["chain"] or "",
                        protocol=row["protocol"] or "",
                        success=bool(row["success"]),
                        error=row["error"] or "",
                        extracted_data_json=row["extracted_data_json"] if "extracted_data_json" in row_keys else "",
                        price_inputs_json=row["price_inputs_json"] if "price_inputs_json" in row_keys else "",
                        pre_state_json=row["pre_state_json"] if "pre_state_json" in row_keys else "",
                        post_state_json=row["post_state_json"] if "post_state_json" in row_keys else "",
                    )
                )
            return entries

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _sync_get)

    # -------------------------------------------------------------------------
    # Typed accounting events (VIB-3417)
    # -------------------------------------------------------------------------

    async def save_accounting_event(self, event: Any) -> bool:
        """Persist a typed accounting event (LendingAccountingEvent, PendleAccountingEvent, etc.)."""
        if not self._initialized:
            await self.initialize()

        identity = event.identity

        def _sync_save() -> bool:
            with self._db_lock:
                self._conn.execute(  # type: ignore[union-attr]
                    """
                    INSERT OR REPLACE INTO accounting_events
                    (id, deployment_id, strategy_id, cycle_id, execution_mode,
                     timestamp, chain, protocol, wallet_address, event_type, position_key,
                     ledger_entry_id, tx_hash, confidence, payload_json, schema_version)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        identity.id,
                        identity.deployment_id,
                        identity.strategy_id,
                        identity.cycle_id,
                        identity.execution_mode,
                        identity.timestamp.isoformat(),
                        identity.chain,
                        identity.protocol,
                        identity.wallet_address,
                        str(getattr(event, "event_type", "UNKNOWN")),
                        getattr(event, "position_key", ""),
                        identity.ledger_entry_id,
                        identity.tx_hash,
                        str(event.confidence),
                        event.to_payload_json(),
                        event.schema_version,
                    ),
                )
                self._conn.commit()  # type: ignore[union-attr]
            return True

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _sync_save)

    async def get_accounting_events(
        self,
        deployment_id: str,
        event_type: str | None = None,
        position_key: str | None = None,
        limit: int = 500,
    ) -> list[dict]:
        """Query typed accounting events as raw dicts (caller deserializes payload_json)."""
        if not self._initialized:
            await self.initialize()

        def _sync_get() -> list[dict]:
            params: list[Any] = [deployment_id]
            where = ["deployment_id = ?"]
            if event_type:
                where.append("event_type = ?")
                params.append(event_type)
            if position_key:
                where.append("position_key = ?")
                params.append(position_key)
            params.append(limit)
            sql = f"""
                SELECT * FROM accounting_events
                WHERE {" AND ".join(where)}
                ORDER BY timestamp DESC
                LIMIT ?
            """
            with self._db_lock:
                cursor = self._conn.execute(sql, params)  # type: ignore[union-attr]
                rows = cursor.fetchall()
            return [dict(row) for row in rows]

        loop2 = asyncio.get_event_loop()
        return await loop2.run_in_executor(None, _sync_get)

    def get_accounting_events_sync(
        self,
        deployment_id: str,
        position_key: str | None = None,
    ) -> list[dict]:
        """Synchronous accounting event query for use from non-async callers.

        Bypasses the async executor wrapper so PortfolioValuer (synchronous)
        can enrich PositionValue objects without spawning a new event loop.
        Returns [] when the store is not yet initialized.

        No LIMIT is applied: cost_basis computation requires the full event
        history from the opening event forward. Truncating early events would
        produce an incorrect (context-free) cost basis.
        """
        if not self._initialized or not self._conn:
            return []
        params: list[Any] = [deployment_id]
        where = ["deployment_id = ?"]
        if position_key is not None:
            where.append("position_key = ?")
            params.append(position_key)
        sql = f"""
            SELECT * FROM accounting_events
            WHERE {" AND ".join(where)}
            ORDER BY timestamp ASC
        """
        with self._db_lock:
            cursor = self._conn.execute(sql, params)
            rows = cursor.fetchall()
        return [dict(row) for row in rows]

    def get_position_events_sync(
        self,
        deployment_id: str,
        position_id: str | None = None,
        position_type: str | None = None,
        event_type: str | None = None,
    ) -> list[dict]:
        """Synchronous position event query for use from non-async callers.

        Bypasses the async executor wrapper so PortfolioValuer (synchronous)
        can enrich PositionValue objects with cost_basis_usd at snapshot time.
        Returns [] when the store is not yet initialized.

        Ordered by timestamp ASC so that the first row is always the earliest
        OPEN event (reliable cost-basis anchor regardless of pagination).

        Args:
            deployment_id: Strategy deployment identifier.
            position_id: Optional filter by position_id.
            position_type: Optional filter by position_type (LP, PERP).
            event_type: Optional filter by event_type (OPEN, CLOSE, etc.).
        """
        if not self._initialized or not self._conn:
            return []
        params: list[Any] = [deployment_id]
        where = ["deployment_id = ?"]
        if position_id is not None:
            where.append("position_id = ?")
            params.append(position_id)
        if position_type is not None:
            where.append("position_type = ?")
            params.append(position_type)
        if event_type is not None:
            where.append("event_type = ?")
            params.append(event_type)
        sql = f"""
            SELECT * FROM position_events
            WHERE {" AND ".join(where)}
            ORDER BY timestamp ASC
        """
        with self._db_lock:
            cursor = self._conn.execute(sql, params)
            rows = cursor.fetchall()
        return [dict(row) for row in rows]

    async def get_accounting_history(
        self,
        deployment_id: str,
        position_key: str,
    ) -> list[dict]:
        """Full chronological history for a position_key."""
        if not self._initialized:
            await self.initialize()

        def _sync_get_hist() -> list[dict]:
            with self._db_lock:
                cursor = self._conn.execute(  # type: ignore[union-attr]
                    """
                    SELECT * FROM accounting_events
                    WHERE deployment_id = ? AND position_key = ?
                    ORDER BY timestamp ASC
                    """,
                    (deployment_id, position_key),
                )
                return [dict(row) for row in cursor.fetchall()]

        loop3 = asyncio.get_event_loop()
        return await loop3.run_in_executor(None, _sync_get_hist)
