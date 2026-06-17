"""SQLite state storage backend.

Provides production-quality SQLite persistence for local development
and lightweight deployments. Implements the same interface as PostgresStore
for consistent behavior across backends.

Features:
- Single row per agent (matches PostgreSQL model)
- CAS (Compare-And-Swap) via optimistic locking with version field
- WAL mode for better concurrent read performance
- Checksum integrity verification

Important: Each strategy uses exactly one gateway and vice versa.
No two strategies share a gateway.

VIB-4044 / PR5: SDK-side `timeline_events` table and its CRUD methods are
hard-deleted. Production timeline_events lives gateway-side in
`almanak/gateway/timeline/store.py`. The 3-demo empirical inspection in PR1
confirmed zero rows ever land in the SDK-side table on real runs — it was
test-only dead code.

Usage:
    config = SQLiteConfig(db_path="./state.db")
    store = SQLiteStore(config)
    await store.initialize()

    # Save state
    state = StateData(deployment_id="strat-1", version=1, state={"key": "value"})
    await store.save(state)

    # CAS update
    await store.save(state, expected_version=1)
"""

import asyncio
import hashlib
import json
import logging
import sqlite3
import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from ..state_manager import StateConflictError, StateData, StateTier


def _default_sqlite_db_path() -> str:
    """Resolve the canonical local DB path (VIB-3761).

    Defers the import to dataclass-construction time so importing this
    module does not require the framework deployment helper to be
    importable at module load (matters during hosted-mode boot before
    settings are realized).
    """
    from almanak.framework.local_paths import LocalPathError, local_db_path

    try:
        return str(local_db_path())
    except LocalPathError:
        return ":hosted-mode-no-sqlite-path:"


if TYPE_CHECKING:
    from almanak.framework.accounting.commit import HandleMapping, RegistryRow
    from almanak.framework.execution.clob_handler import ClobFill, ClobOrderState, ClobOrderStatus
    from almanak.framework.observability.ledger import LedgerEntry, LedgerQuantStats
    from almanak.framework.observability.position_events import PositionEvent
    from almanak.framework.portfolio import PortfolioMetrics, PortfolioSnapshot

logger = logging.getLogger(__name__)


def _canonical_deployment_id(obj: Any) -> str:
    """Return the canonical deployment identity carried by a persistence model."""
    return str(obj.deployment_id)


def _require_deployment_id(value: str | None, *, operation: str) -> str:
    deployment_id = (value or "").strip()
    if not deployment_id:
        raise ValueError(f"{operation}: deployment_id is required")
    return deployment_id


def _extract_position_reference_column(payload_json: str) -> str | None:
    """Extract the ``position_reference`` JSON sub-document from an augmented payload.

    Helper for the SQLite + gateway state-manager INSERT paths. Returns:

    * ``None`` if the augment chokepoint did not emit the key (non-OPEN/CLOSE
      event_kind, unknown event_type fallback, or malformed JSON in non-live
      mode — same path that returns the original payload unchanged).
    * The JSON-serialized sub-document (``json.dumps(d['position_reference'],
      sort_keys=True)``) otherwise — byte-identical between two equal
      references so deduplication / parity tests can compare strings.

    Per `CLAUDE.md` "Empty ≠ zero": a missing key returns ``None``, NOT an
    empty string. The DB column stays NULL, signalling "no position pointer
    for this row" — distinct from "the position pointer is the empty dict".
    """
    import json as _json

    try:
        d = _json.loads(payload_json)
    except (json.JSONDecodeError, TypeError):
        return None
    if not isinstance(d, dict):
        return None
    pr = d.get("position_reference")
    if pr is None:
        return None
    return _json.dumps(pr, sort_keys=True)


def _backfill_position_reference_legacy(conn: sqlite3.Connection) -> None:
    """Backfill ``accounting_events.position_reference`` for legacy OPEN/CLOSE rows.

    Called once at migration time when the column is freshly added. For every
    pre-existing row whose ``event_type`` resolves through the canonical
    taxonomy to an OPEN or CLOSE event_kind, we stamp:

    .. code-block:: json

        {
          "source": "legacy",
          "primitive": "...",
          "accounting_category": "...",
          "physical_identity_hash": null,
          "semantic_grouping_key": null,
          "registry_handle": null,
          "grouping_policy_version": null,
          "matching_policy_version": null
        }

    Rows whose ``event_type`` is unknown or whose event_kind is not OPEN/CLOSE
    stay NULL — same contract as the runtime augment chokepoint, so a single
    auditor cannot tell migrated rows from natively-written rows on the bridge
    column.

    Per `CLAUDE.md` "Database schema ownership": this function only mutates
    the LOCAL SQLite DB. Hosted Postgres backfill is deferred to T19 / VIB-4205.
    """
    # Lazy import — keeps the migration function callable from a fresh boot
    # before the accounting package is fully imported, and avoids a circular
    # import via ``almanak.framework.accounting.writer`` re-importing this
    # module's exceptions at top-level.
    import json as _json

    from almanak.framework.accounting.position_reference import (
        build_legacy_position_reference,
    )
    from almanak.framework.primitives.taxonomy import (
        UnknownIntentTypeError,
        record_for,
    )

    # Stream the read cursor instead of `.fetchall()` — `accounting_events`
    # can accumulate tens of thousands of rows on long-running strategies, and
    # this is one-shot boot-time code where streaming is strictly better.
    # Flush UPDATEs in batches via `executemany` so we don't hold the full
    # rendered list in memory either.
    _BATCH_SIZE = 1000
    try:
        cursor = conn.execute("SELECT id, event_type FROM accounting_events WHERE position_reference IS NULL")
    except sqlite3.OperationalError:
        # Table missing the column or absent entirely — nothing to backfill.
        return

    batch: list[tuple[str, str]] = []
    total = 0

    def _flush(batch: list[tuple[str, str]]) -> int:
        if not batch:
            return 0
        conn.executemany(
            "UPDATE accounting_events SET position_reference = ? WHERE id = ?",
            batch,
        )
        return len(batch)

    for row in cursor:
        event_type = row["event_type"] if hasattr(row, "keys") else row[1]
        row_id = row["id"] if hasattr(row, "keys") else row[0]
        if not isinstance(event_type, str) or not event_type:
            continue
        try:
            record = record_for(event_type)
        except UnknownIntentTypeError:
            continue
        try:
            ref = build_legacy_position_reference(record)
        except ValueError:
            # Non-OPEN/CLOSE event_kind — column stays NULL.
            continue
        batch.append((_json.dumps(ref.to_dict(), sort_keys=True), row_id))
        if len(batch) >= _BATCH_SIZE:
            total += _flush(batch)
            batch = []

    total += _flush(batch)

    if total == 0:
        logger.info("Migration: position_reference backfill — no OPEN/CLOSE rows to populate")
        return

    logger.info(
        "Migration: position_reference backfill — populated %d legacy OPEN/CLOSE rows",
        total,
    )


def _backfill_pendle_registry_category(conn: sqlite3.Connection) -> None:
    """Migrate legacy ``pendle_lp`` / ``pendle_pt`` ``position_registry`` categories (VIB-4931).

    Pendle's accounting_category de-leaked to the generic ``lp`` / ``swap`` when the
    protocol-named ``PENDLE_LP`` / ``PENDLE_PT`` enum members were removed. Any pre-existing
    registry rows under the removed categories are migrated to the generic value so an
    open-before/close-after round-trip still matches the
    ``(deployment_id, accounting_category, …)`` uniqueness index. The Pendle partition is
    empty by construction (only UniV3-LP protocols write ``position_registry``), so this is a
    0-row safety net; the cheap guard SELECT keeps it overhead-free on every boot and
    idempotent on re-run.

    Collision-safe: both registry unique indexes (``ix_registry_handle``,
    ``ix_registry_auto_mode``) are scoped by ``accounting_category``, so a legacy
    ``pendle_lp`` / ``pendle_pt`` row that shares an identity with an existing generic
    ``lp`` / ``swap`` row would clash on relabel. ``UPDATE OR IGNORE`` skips only the
    offending row (preserving the existing generic row — no data loss) instead of raising
    ``IntegrityError`` and stranding the strategy at boot; any skipped row is surfaced at
    ERROR for an operator to reconcile.
    """
    if not conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='position_registry'").fetchone():
        return
    has_legacy_pendle = conn.execute(
        "SELECT 1 FROM position_registry WHERE accounting_category IN ('pendle_lp', 'pendle_pt') LIMIT 1"
    ).fetchone()
    if has_legacy_pendle is None:
        return
    conn.execute("BEGIN IMMEDIATE")
    try:
        conn.execute(
            "UPDATE OR IGNORE position_registry SET accounting_category = 'lp' WHERE accounting_category = 'pendle_lp'"
        )
        conn.execute(
            "UPDATE OR IGNORE position_registry SET accounting_category = 'swap' WHERE accounting_category = 'pendle_pt'"
        )
        leftover = conn.execute(
            "SELECT COUNT(*) FROM position_registry WHERE accounting_category IN ('pendle_lp', 'pendle_pt')"
        ).fetchone()[0]
        conn.execute("COMMIT")
        if leftover:
            # A skipped row means a Pendle position collides with a generic lp/swap
            # identity on a category-scoped unique index — an anomaly, since the Pendle
            # partition is empty by construction. Boot is not stranded; surface it loudly
            # so an operator can reconcile the duplicate identity.
            logger.error(
                "Migration: VIB-4931 — %d legacy pendle_lp/pendle_pt position_registry row(s) "
                "could not be relabeled (would collide with an existing lp/swap identity on a "
                "category-scoped unique index); left unchanged. Investigate the duplicate identity.",
                leftover,
            )
        else:
            logger.info(
                "Migration: VIB-4931 — migrated legacy pendle_lp/pendle_pt position_registry categories to lp/swap"
            )
    except Exception:
        conn.execute("ROLLBACK")
        raise


def _convert_dual_identity_tables_to_deployment_id(conn: sqlite3.Connection) -> None:  # noqa: C901
    """Collapse the remaining dual-identity tables to a single ``deployment_id``.

    VIB-4722 — blueprint 29 §3: every deployment-scoped table carries exactly
    one identity column, ``deployment_id``, identical in meaning on both
    backends (the hosted-Postgres side already keys these on ``deployment_id``
    per VIB-4721).

    Six tables historically declared BOTH a live identity column AND a dead
    one added under the abandoned "VIB-2835 Phase 4" migration:

    * ``portfolio_snapshots`` / ``portfolio_metrics`` / ``transaction_ledger``
      / ``position_state_snapshots``: live ``strategy_id``, dead
      ``deployment_id`` — drop the dead ``deployment_id``, then rename
      ``strategy_id`` into its place.
    * ``accounting_events`` / ``accounting_outbox``: live ``deployment_id``,
      dead ``strategy_id`` — drop the dead ``strategy_id`` only.

    Each conversion inspects ``PRAGMA table_info`` first so an
    already-converted DB, where only ``deployment_id`` remains, is a no-op.
    When both legacy and canonical columns exist, the dead column is dropped
    before the live column is renamed to avoid a name collision.
    """

    def _table_exists(table: str) -> bool:
        return (
            conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (table,),
            ).fetchone()
            is not None
        )

    def _drop_column_if_present(table: str, column: str) -> None:
        try:
            conn.execute(f"ALTER TABLE {table} DROP COLUMN {column}")
            logger.info("Migration: dropped dead %s.%s (VIB-4722)", table, column)
        except sqlite3.OperationalError:
            # Column already absent (fresh / converted DB) — no-op.
            pass

    def _rename_column_if_present(table: str, old: str, new: str) -> None:
        try:
            conn.execute(f"ALTER TABLE {table} RENAME COLUMN {old} TO {new}")
            logger.info("Migration: renamed %s.%s → %s (VIB-4722)", table, old, new)
        except sqlite3.OperationalError:
            # Source column already absent (already converted) — no-op.
            pass

    def _columns(table: str) -> set[str]:
        return {row["name"] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}

    def _convert_strategy_key_table(table: str, *, drop_index: str | None = None) -> None:
        if not _table_exists(table):
            return
        cols = _columns(table)
        if "strategy_id" not in cols:
            return
        if "deployment_id" in cols:
            if drop_index:
                conn.execute(f"DROP INDEX IF EXISTS {drop_index}")
            _drop_column_if_present(table, "deployment_id")
        _rename_column_if_present(table, "strategy_id", "deployment_id")

    # Tables whose dead column is `deployment_id` and whose live identity
    # column `strategy_id` must be renamed into its place.
    for table in ("portfolio_snapshots", "portfolio_metrics", "transaction_ledger"):
        _convert_strategy_key_table(table)

    # position_state_snapshots: same shape, but the dead `deployment_id`
    # column carries `idx_pss_position` — SQLite refuses DROP COLUMN on an
    # indexed column, so drop the index first. SCHEMA_SQL's
    # CREATE INDEX IF NOT EXISTS recreates it on the renamed canonical
    # column (this helper runs before executescript()).
    _convert_strategy_key_table("position_state_snapshots", drop_index="idx_pss_position")

    # accounting_events / accounting_outbox: modern dual-identity DBs already
    # use `deployment_id` as the live key and carry `strategy_id` only as a
    # dead column. Very old DBs may have only `strategy_id`; rename those
    # instead of dropping the only identity column.
    for table in ("accounting_events", "accounting_outbox"):
        if _table_exists(table):
            cols = _columns(table)
            if "deployment_id" in cols:
                _drop_column_if_present(table, "strategy_id")
            else:
                _rename_column_if_present(table, "strategy_id", "deployment_id")


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

    db_path: str = field(default_factory=lambda: _default_sqlite_db_path())
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


# NOTE (VIB-4044 / PR5): The SDK-side `TimelineEvent` dataclass is removed.
# Gateway-side timeline events live in `almanak/gateway/timeline/store.py:TimelineEvent`.


# =============================================================================
# SQL SCHEMA
# =============================================================================

SCHEMA_SQL = """
-- Strategy state table for local SQLite mode.
-- deployment_id is the single canonical identity column (blueprint 29 §3),
-- identical in meaning on both backends. The Python StateData.deployment_id
-- field still feeds it (the textual field rename is VIB-4726).
CREATE TABLE IF NOT EXISTS strategy_state (
    deployment_id TEXT PRIMARY KEY,
    version INTEGER NOT NULL DEFAULT 1,
    state_data TEXT NOT NULL,  -- JSON string
    schema_version INTEGER NOT NULL DEFAULT 1,
    checksum TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

-- VIB-4044 / PR5: SDK-side `timeline_events` table is removed.
-- Gateway-side timeline_events lives in ~/.config/almanak/gateway.db (local)
-- or hosted Postgres (deployed). See almanak/gateway/timeline/store.py.

-- CLOB orders table for Polymarket order tracking.
-- deployment_id is the single identity column (blueprint 29 §3): every
-- deployment-scoped row carries the canonical deployment_id.
CREATE TABLE IF NOT EXISTS clob_orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    deployment_id TEXT NOT NULL,
    order_id TEXT NOT NULL,
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
-- NOTE: deployment_id indexes are created in _run_migrations() AFTER the
-- deployment_id column is added, so an upgraded DB (table predates the
-- column) does not trip "no such column" while executing this script.

-- Portfolio snapshots table for value tracking and PnL charts
CREATE TABLE IF NOT EXISTS portfolio_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    deployment_id TEXT NOT NULL,  -- canonical identity column (blueprint 29 §3)
    cycle_id TEXT DEFAULT '',  -- Phase 4: correlation to iteration (VIB-2835)
    execution_mode TEXT DEFAULT '',  -- Phase 4: live, paper, dry_run (VIB-2837)
    timestamp TEXT NOT NULL,
    iteration_number INTEGER DEFAULT 0,
    total_value_usd TEXT NOT NULL,  -- Decimal as string; strategy-scoped (VIB-3614)
    available_cash_usd TEXT NOT NULL,  -- Decimal as string
    deployed_capital_usd TEXT DEFAULT '0',  -- sum of cost_basis_usd for open positions (VIB-3614)
    wallet_total_value_usd TEXT DEFAULT '0',  -- wallet + non-overlapping positions; TOKEN-class wallet pseudo-positions excluded by symbol/address overlap (VIB-3614 / VIB-4909)
    value_confidence TEXT DEFAULT 'HIGH',  -- HIGH, ESTIMATED, STALE, UNAVAILABLE
    positions_json TEXT NOT NULL,  -- JSON array of positions
    token_prices_json TEXT DEFAULT '{}',  -- {chain:address: {price_usd, symbol, decimals}}
    wallet_balances_json TEXT DEFAULT '[]',  -- JSON array of TokenBalance dicts
    chain TEXT,
    created_at TEXT NOT NULL
);

-- Index for deployment + time queries (dashboard charts)
CREATE INDEX IF NOT EXISTS idx_portfolio_snapshots_strategy_time
ON portfolio_snapshots (deployment_id, timestamp DESC);

-- Index for cleanup queries
CREATE INDEX IF NOT EXISTS idx_portfolio_snapshots_created_at
ON portfolio_snapshots (created_at);

-- Unique constraint to prevent duplicate timestamps per deployment
CREATE UNIQUE INDEX IF NOT EXISTS idx_portfolio_snapshots_unique
ON portfolio_snapshots (deployment_id, timestamp);

-- Portfolio metrics table for PnL baseline tracking
-- Stores values that survive strategy restarts
CREATE TABLE IF NOT EXISTS portfolio_metrics (
    deployment_id TEXT PRIMARY KEY,  -- canonical identity column (blueprint 29 §3)
    initial_value_usd TEXT NOT NULL,  -- Decimal as string, set on first run
    initial_timestamp TEXT NOT NULL,
    deposits_usd TEXT DEFAULT '0',
    withdrawals_usd TEXT DEFAULT '0',
    gas_spent_usd TEXT DEFAULT '0',
    total_value_usd TEXT DEFAULT '0',  -- Current portfolio value (VIB-2765)
    positions_json TEXT DEFAULT '[]',  -- Snapshot of position state (VIB-2765)
    cycle_id TEXT,  -- Correlation to portfolio_snapshots (VIB-2765)
    execution_mode TEXT DEFAULT '',  -- Phase 4: live, paper, dry_run (VIB-2837)
    is_complete BOOLEAN DEFAULT 1,  -- Phase 4: all records for this cycle committed (VIB-2839)
    updated_at TEXT NOT NULL
);

-- Transaction ledger -- structured trade records (VIB-2402)
CREATE TABLE IF NOT EXISTS transaction_ledger (
    id TEXT PRIMARY KEY,
    cycle_id TEXT NOT NULL,
    deployment_id TEXT NOT NULL,  -- canonical identity column (blueprint 29 §3)
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

-- Index for deployment + time queries
CREATE INDEX IF NOT EXISTS idx_transaction_ledger_strategy_time
ON transaction_ledger (deployment_id, timestamp DESC);

-- Index for cycle correlation
CREATE INDEX IF NOT EXISTS idx_transaction_ledger_cycle_id
ON transaction_ledger (cycle_id);

-- Index for intent type filtering
CREATE INDEX IF NOT EXISTS idx_transaction_ledger_intent_type
ON transaction_ledger (deployment_id, intent_type);

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
    deployment_id TEXT NOT NULL,  -- canonical identity column (blueprint 29 §3)
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
    schema_version INTEGER NOT NULL DEFAULT 1,
    -- VIB-4196 / T10: position_reference JSON pointer for OPEN/CLOSE rows.
    -- NULL for non-OPEN/CLOSE rows (ADJUST, COLLECT, TRANSFER, NONE) and for
    -- the augment-fallback path (unknown event_type in paper mode). The
    -- accounting writer (`augment_accounting_payload`) is the only code
    -- permitted to construct the JSON; persisted here as a denormalized
    -- query-convenience copy of the payload's `position_reference` key.
    position_reference TEXT
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
    deployment_id TEXT NOT NULL,             -- canonical identity column (blueprint 29 §3)
    cycle_id TEXT NOT NULL,
    ledger_entry_id TEXT NOT NULL,           -- FK to transaction_ledger.id
    intent_type TEXT NOT NULL,               -- e.g. "SUPPLY", "LP_OPEN"
    wallet_address TEXT NOT NULL DEFAULT '', -- runner wallet; needed for position_key derivation
    position_key TEXT NOT NULL DEFAULT '',   -- pre-computed by runner to ensure derivation parity
    market_id TEXT NOT NULL DEFAULT '',      -- e.g. Morpho Blue market ID; absent for Aave
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

-- Per-iteration position-state snapshots (AttemptNo17 §3 D4 / Track C / VIB-3891).
-- Continuously-accrued fields the event-driven Layer-1/3/5 writers cannot
-- emit on their own — HF trajectory (L2), in-range fraction (LP2),
-- supply/borrow APR (L5), funding accrual (P2), liquidation buffer (L3/P4).
-- One row per open position per portfolio_snapshots row; gap-free time
-- series gives the cell evaluators a curve, not just an integral.
--
-- Hosted Postgres has the equivalent table behind VIB-3871 (Infra). This
-- DDL is the local SQLite half — the materializer in
-- almanak.framework.accounting.position_state already short-circuits in
-- hosted mode (returns None) until the metrics-database PR ships.
CREATE TABLE IF NOT EXISTS position_state_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_id INTEGER NOT NULL,             -- FK → portfolio_snapshots.id
    deployment_id TEXT NOT NULL,              -- canonical identity column (blueprint 29 §3)
    cycle_id TEXT NOT NULL DEFAULT '',
    captured_at TEXT NOT NULL,                -- ISO-8601 UTC
    position_id TEXT NOT NULL,
    position_type TEXT NOT NULL,              -- LP | LENDING | PERP

    -- LP fields ------------------------------------------------------
    current_tick INTEGER,
    in_range INTEGER,                         -- SQLite has no native bool
    liquidity TEXT,                           -- string-decimal for precision
    sqrt_price_x96 TEXT,

    -- Lending fields -------------------------------------------------
    supply_balance TEXT,
    borrow_balance TEXT,
    health_factor TEXT,
    supply_apy_pct TEXT,
    borrow_apy_pct TEXT,
    interest_accrued_since_last TEXT,

    -- Perp fields (postponed — VIB-3872) -----------------------------
    mark_price TEXT,
    unrealized_pnl TEXT,
    funding_accrued_since_last TEXT,
    liquidation_price TEXT,
    margin_utilisation_pct TEXT,

    -- Reconciliation + provenance -----------------------------------
    delta_vs_protocol_pct TEXT,               -- G14 dust check input
    value_confidence TEXT NOT NULL DEFAULT 'ESTIMATED',
    schema_version INTEGER NOT NULL DEFAULT 1,
    formula_version INTEGER NOT NULL DEFAULT 1,
    matching_policy_version INTEGER NOT NULL DEFAULT 1,
    -- ON DELETE CASCADE so cleanup_old_snapshots() and any save_portfolio_snapshot
    -- INSERT-OR-REPLACE on the parent doesn't trip an FK error once Track C
    -- rows have been written — SQLite REPLACE is delete-then-insert, which
    -- without CASCADE would block on the child's existence (CodeRabbit
    -- finding, 2026-05-02).
    FOREIGN KEY (snapshot_id) REFERENCES portfolio_snapshots(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_pss_snapshot
ON position_state_snapshots (snapshot_id);

CREATE INDEX IF NOT EXISTS idx_pss_strategy_time
ON position_state_snapshots (deployment_id, captured_at DESC);

CREATE INDEX IF NOT EXISTS idx_pss_position
ON position_state_snapshots (deployment_id, position_id, captured_at DESC);

CREATE INDEX IF NOT EXISTS idx_pss_cycle
ON position_state_snapshots (cycle_id);

-- Position registry (VIB-4190 / T05 of multi-position-tracking epic VIB-4185).
--
-- Authoritative table for "is this position open?". Columns ratified by
-- docs/internal/prds/multi-position-tracking.md §Registry Data Shape.
-- Transactional surface specified in docs/internal/blueprints/28-position-registry.md.
--
-- T05 lands the schema only. No production code reads or writes this table
-- yet; T11 (VIB-4197) introduces the atomic save_ledger_and_registry primitive
-- and T12+ flips primitives to registry mode one at a time. The
-- schema_contract entry is intentionally deferred to T11 (no writers means
-- the contract guard would gate boot against a column shape no production
-- path depends on).
--
-- The dual-key model is load-bearing:
--   physical_identity_hash   = durable PK from receipt facts only.
--   semantic_grouping_key    = auto-mode uniqueness predicate (partial unique
--                              index, bypassed when handle is supplied).
-- primitive uses Primitive enum value; accounting_category uses
-- AccountingCategory enum value (sourced via record_for(intent_type)).
CREATE TABLE IF NOT EXISTS position_registry (
    deployment_id            TEXT NOT NULL,
    chain                    TEXT NOT NULL,
    primitive                TEXT NOT NULL,
    accounting_category      TEXT NOT NULL,
    physical_identity_hash   TEXT NOT NULL,
    semantic_grouping_key    TEXT NOT NULL,
    grouping_policy_version  TEXT NOT NULL,
    handle                   TEXT,
    -- The CHECK pins `status` to the three canonical values ratified by PRD
    -- §Registry Data Shape (and blueprint 28 §4.3 status-priority table).
    -- Without it, a case-variant typo (`OPEN`, `Open`, …) would bypass the
    -- partial unique index `ix_registry_auto_mode` (which guards rows where
    -- `status = 'open'`) and admit duplicate semantic groups silently.
    status                   TEXT NOT NULL
                              CHECK (status IN ('open', 'closed', 'reorg_invalidated')),
    -- PRD §Registry Data Shape declares this as `JSON NOT NULL`. SQLite has
    -- no native JSON type — a `JSON` declaration falls under NUMERIC affinity
    -- and would coerce JSON-valued strings unexpectedly. The realization here
    -- uses `TEXT NOT NULL`, matching the existing JSON-storing columns in
    -- this file (clob_orders.fills/metadata, transaction_ledger.*_json,
    -- accounting_events.payload_json, accounting_outbox.error,
    -- position_state_snapshots.attribution_json). T11 (VIB-4197) writers
    -- serialize via json.dumps; a CHECK(json_valid(payload)) constraint may
    -- be added in T11 with the writer code (see PR #2197 audit notes).
    payload                  TEXT NOT NULL,
    opened_at_block          INTEGER,
    opened_tx                TEXT,
    closed_at_block          INTEGER,
    closed_tx                TEXT,
    last_reconciled_at_block INTEGER,
    matching_policy_version  INTEGER NOT NULL,
    PRIMARY KEY (deployment_id, chain, primitive, physical_identity_hash)
);

-- Handle uniqueness within a deployment+accounting_category when set.
-- (UniV3 LP and Pendle LP share Primitive='lp' but have distinct
-- AccountingCategory values, so a single deployment can carry one "leg_a"
-- handle on a UniV3 NFT and another "leg_a" handle on a Pendle LP without
-- collision.)
CREATE UNIQUE INDEX IF NOT EXISTS ix_registry_handle
    ON position_registry (deployment_id, accounting_category, handle)
    WHERE handle IS NOT NULL;

-- Auto-mode collision guard: reject duplicate semantic groups within
-- deployment+chain+accounting_category when no handle is supplied AND the
-- existing row is still open. Closed rows do not block reopening; handles
-- bypass the guard (per §Strategy author UX).
CREATE UNIQUE INDEX IF NOT EXISTS ix_registry_auto_mode
    ON position_registry (deployment_id, chain, accounting_category, semantic_grouping_key)
    WHERE status = 'open' AND handle IS NULL;

-- VIB-4278: lookup-by-tx indexes for the augment chokepoint
-- (``SQLiteStore._build_registry_lookup_for_event``). The chokepoint runs
-- inside the ``_db_lock`` critical section on every OPEN/CLOSE accounting
-- write; without these, the SELECT degrades to a sequential scan over all
-- ``position_registry`` rows for the same ``(deployment_id, chain,
-- primitive)`` and serializes accounting writes against history size.
--
-- The chokepoint queries case-insensitively (``LOWER(opened_tx) = ?``) so
-- a runner-side mixed-case tx_hash still hits the row written by backfill.
-- Indexes are therefore built on ``LOWER(...)`` to remain usable by the
-- predicate. Partial-index WHERE eliminates NULL rows from the index —
-- ``opened_tx`` / ``closed_tx`` are sparse (most rows have one set, not
-- both).
CREATE INDEX IF NOT EXISTS ix_registry_opened_tx_lookup
    ON position_registry (deployment_id, chain, primitive, LOWER(opened_tx))
    WHERE opened_tx IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_registry_closed_tx_lookup
    ON position_registry (deployment_id, chain, primitive, LOWER(closed_tx))
    WHERE closed_tx IS NOT NULL;

-- Per-(deployment_id, primitive, cutover_key) cutover-progress tracking
-- (VIB-4197 / T11 of multi-position-tracking epic VIB-4185).
--
-- Per docs/internal/migration-cutover-position-registry.md §2.1. T11
-- lands the schema and the schema_contract entry. The boot-time guard
-- (`StrategyRunner._enforce_or_run_cutover`) and the BackfillReader base
-- class are runner-side cutover infrastructure with no production caller
-- until T12+; they ship in a follow-up ticket per the T11 split decision.
--
-- The `notes` column is a structured JSON audit + control field. The
-- cutover spec defines three well-known top-level keys:
--   "rollback":            bool   -- operator-set (env or CLI) rollback flag.
--   "audit":               array  -- append-only operator log.
--   "last_rollback_state": bool   -- runner-set; rollback transition tracker.
-- The CHECK on json_valid(notes) is a structural backstop: SQLite would
-- otherwise admit '' or any byte string and the runner's json.loads would
-- crash on every read.
CREATE TABLE IF NOT EXISTS migration_state (
    deployment_id                       TEXT NOT NULL,
    primitive                           TEXT NOT NULL,
    cutover_key                         TEXT NOT NULL,
    position_registry_backfill_complete INTEGER NOT NULL DEFAULT 0
                                        CHECK (position_registry_backfill_complete IN (0, 1)),
    backfill_started_at                 TEXT,    -- ISO-8601 UTC
    backfill_completed_at               TEXT,    -- ISO-8601 UTC
    backfill_source_table               TEXT NOT NULL DEFAULT 'position_events',
    backfill_reader_version             INTEGER NOT NULL DEFAULT 1,
    rows_synthesized                    INTEGER NOT NULL DEFAULT 0,
    rows_skipped_already_present        INTEGER NOT NULL DEFAULT 0,
    notes                               TEXT NOT NULL DEFAULT '{}'
                                        CHECK (json_valid(notes)),
    created_at                          TEXT NOT NULL,
    updated_at                          TEXT NOT NULL,
    PRIMARY KEY (deployment_id, primitive, cutover_key)
);
"""


# =============================================================================
# Custom SQL helpers (VIB-5059 Phase 1)
# =============================================================================


def _register_quant_sql_functions(conn: sqlite3.Connection) -> None:
    """Register the exact-decimal SQL helpers the quant aggregates use.

    The dashboard quant-stats query (:meth:`SQLiteStore.get_ledger_quant_stats`)
    must compute the life-to-date ``gas_usd`` total SQL-side so it transfers
    O(1) rows — but ``SUM(CAST(gas_usd AS REAL))`` would route the Decimal-as-
    TEXT column through IEEE-754 double, violating the lossless-precision
    invariant the accounting stack maintains (the same pr-auditor finding that
    shaped :meth:`SQLiteStore.sum_ledger_gas_usd`). A custom aggregate keeps
    the summation exact Decimal arithmetic while staying inside the SQL
    statement. Parse semantics are ``lenient_ledger_decimal`` — NULL / empty /
    unparsable / non-finite text contribute zero and never raise (one garbage
    row must not be able to fail the whole aggregate and zero every tile).
    """
    from almanak.framework.observability.ledger import lenient_ledger_decimal

    class _LenientDecimalSum:
        """Aggregate ``almanak_decimal_sum(col)`` → exact-Decimal sum as TEXT."""

        def __init__(self) -> None:
            self._total = Decimal("0")

        def step(self, value: Any) -> None:
            self._total += lenient_ledger_decimal(value)

        def finalize(self) -> str:
            return str(self._total)

    def _lenient_decimal_positive(value: Any) -> int:
        """Scalar ``almanak_decimal_positive(col)`` → 1 iff finite-parse > 0."""
        return 1 if lenient_ledger_decimal(value) > 0 else 0

    # typeshed's _AggregateProtocol pins ``finalize() -> int``; sqlite3
    # accepts any SQLite-storable value (we return the exact Decimal sum as
    # TEXT) — typeshed false positive.
    conn.create_aggregate("almanak_decimal_sum", 1, _LenientDecimalSum)  # type: ignore[arg-type]
    conn.create_function(
        "almanak_decimal_positive",
        1,
        _lenient_decimal_positive,
        deterministic=True,
    )


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
        >>> state = StateData(deployment_id="strat-1", version=1, state={"key": "value"})
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

            _register_quant_sql_functions(conn)

            return conn

        loop = asyncio.get_event_loop()
        self._conn = await loop.run_in_executor(None, _sync_connect)

    async def _create_schema(self) -> None:
        """Create database tables and indexes."""
        if self._conn is None:
            raise DatabaseInitializationError("Connection not established")

        def _sync_create_schema() -> None:
            # VIB-4722: collapse the dual-identity tables to a single
            # canonical `deployment_id` column BEFORE executescript() —
            # SCHEMA_SQL creates indexes (idx_transaction_ledger_*,
            # idx_portfolio_snapshots_*, idx_pss_position) on the
            # deployment_id column, which would fail on a pre-rename DB
            # whose tables still carry the old `deployment_id` identity
            # column. Same ordering rationale as the gateway lifecycle
            # store's legacy identity → deployment_id rename.
            _convert_dual_identity_tables_to_deployment_id(self._conn)  # type: ignore[arg-type]
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

        def _add_column_if_missing(table: str, column: str, col_type: str) -> bool:
            """Add a column to a table if it doesn't already exist.

            Returns True if the column was newly added, False if it already existed.
            """
            cursor = conn.execute(f"PRAGMA table_info({table})")
            existing = {row["name"] for row in cursor.fetchall()}
            if column not in existing:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")
                logger.info(f"Migration: added {table}.{column}")
                return True
            return False

        def _drop_table_if_exists(table: str, reason: str) -> None:
            """Drop a deprecated table from upgraded databases.

            Idempotent — DROP TABLE IF EXISTS is a no-op on fresh databases
            (the SCHEMA_SQL above never created it).
            """
            cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
            if cursor.fetchone() is not None:
                conn.execute(f"DROP TABLE IF EXISTS {table}")
                logger.info(f"Migration: dropped legacy {table} table ({reason})")

        # VIB-4044 / PR5: SDK-side `timeline_events` is removed. Existing
        # local databases carry the table from earlier SDK versions; drop it
        # here so an upgraded user converges to the new schema. Production
        # timeline_events flow goes through the gateway-side store
        # (almanak/gateway/timeline/store.py); the SDK-side table received
        # zero writes (verified by the 3-demo PR1 inspection in
        # docs/internal/TimelineScope-E2E-Findings.md), so dropping it on
        # upgrade is safe — no user data lives in it.
        _drop_table_if_exists("timeline_events", "moved to gateway-side store")

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

        # VIB-3614: strategy-scoped valuation columns — wrapped in an explicit
        # transaction so the ALTER TABLE and backfill UPDATE are atomic.
        # isolation_level=None means autocommit; without BEGIN/COMMIT a crash
        # between ALTER and UPDATE leaves legacy rows permanently at '0'.
        conn.execute("BEGIN IMMEDIATE")
        try:
            _add_column_if_missing("portfolio_snapshots", "deployed_capital_usd", "TEXT DEFAULT '0'")
            if _add_column_if_missing("portfolio_snapshots", "wallet_total_value_usd", "TEXT DEFAULT '0'"):
                # Backfill legacy rows: pre-VIB-3614 total_value_usd was the full wallet
                # value, so it's the best available proxy for wallet_total_value_usd.
                conn.execute(
                    """
                    UPDATE portfolio_snapshots
                    SET wallet_total_value_usd = total_value_usd
                    WHERE wallet_total_value_usd IS NULL OR wallet_total_value_usd = '0'
                    """
                )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

        # VIB-4722: strategy_state's identity column is renamed
        # strategy_id → deployment_id (blueprint 29 §3 — one identity column,
        # one name, both backends). Idempotent: the rename runs only when the
        # legacy column is still present. SCHEMA_SQL's CREATE TABLE IF NOT
        # EXISTS is a no-op on an upgraded DB, so this migration is the path
        # that converges the column name.
        if conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='strategy_state'").fetchone():
            ss_cols = {row["name"] for row in conn.execute("PRAGMA table_info(strategy_state)").fetchall()}
            if "strategy_id" in ss_cols and "deployment_id" not in ss_cols:
                conn.execute("ALTER TABLE strategy_state RENAME COLUMN strategy_id TO deployment_id")
                logger.info("Migration: renamed strategy_state.strategy_id → deployment_id")

        # VIB-4722: clob_orders gains the canonical deployment_id identity
        # column (blueprint 29 §3 — it previously had no identity column).
        # The indexes are created here, after the column add, so an upgraded
        # DB whose clob_orders predates the column does not trip "no such
        # column" while SCHEMA_SQL runs (CREATE INDEX in SCHEMA_SQL would
        # execute before this migration on an existing table).
        if conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='clob_orders'").fetchone():
            _add_column_if_missing("clob_orders", "deployment_id", "TEXT NOT NULL DEFAULT ''")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_clob_orders_deployment ON clob_orders (deployment_id)")
            conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_clob_orders_deployment_order "
                "ON clob_orders (deployment_id, order_id)"
            )

        # Phase 4: cycle_id, execution_mode across all tables (VIB-2835, VIB-2837).
        # The legacy `deployment_id TEXT DEFAULT ''` adds for portfolio_snapshots /
        # portfolio_metrics / transaction_ledger were removed in VIB-4722 — that
        # column was the abandoned dead identity column; the canonical
        # deployment_id is now the renamed live key, converged by
        # _convert_dual_identity_tables_to_deployment_id (run in
        # _create_schema, before executescript()).
        _add_column_if_missing("portfolio_snapshots", "cycle_id", "TEXT DEFAULT ''")
        _add_column_if_missing("portfolio_snapshots", "execution_mode", "TEXT DEFAULT ''")
        _add_column_if_missing("portfolio_metrics", "execution_mode", "TEXT DEFAULT ''")
        _add_column_if_missing("portfolio_metrics", "is_complete", "BOOLEAN DEFAULT 1")
        _add_column_if_missing("transaction_ledger", "execution_mode", "TEXT DEFAULT ''")
        _add_column_if_missing("position_events", "cycle_id", "TEXT DEFAULT ''")
        _add_column_if_missing("position_events", "execution_mode", "TEXT DEFAULT ''")

        # VIB-3205: protocol_fees_usd captured from ProtocolFees.total_usd at
        # event time so attribution can attribute real fee PnL (not the v1
        # placeholder of 0). Empty string remains the "unknown" sentinel.
        _add_column_if_missing("position_events", "protocol_fees_usd", "TEXT DEFAULT ''")

        # VIB-3467: wallet_address, position_key, market_id on accounting_outbox.
        # Guard: pre-VIB-3480 databases don't have this table yet — skip the ALTER
        # when the table is absent (the CREATE TABLE IF NOT EXISTS in the DDL block
        # will create it with the correct schema for new and old-schema databases).
        if conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='accounting_outbox'").fetchone():
            _add_column_if_missing("accounting_outbox", "wallet_address", "TEXT NOT NULL DEFAULT ''")
            _add_column_if_missing("accounting_outbox", "position_key", "TEXT NOT NULL DEFAULT ''")
            _add_column_if_missing("accounting_outbox", "market_id", "TEXT NOT NULL DEFAULT ''")

        # VIB-4196 / T10: position_reference JSON pointer on accounting_events.
        # Forward-compat shape between accounting_events and the (T11-shipped)
        # position_registry; cutover tickets (T12 / T16 / T23 / T28) flip per
        # primitive without rebasing the accounting schema. Backfill OPEN/CLOSE
        # rows with `source="legacy"` derived from the canonical taxonomy
        # `record_for(event_type)` so historical rows are byte-comparable to
        # rows written under T10's writer chokepoint. Non-OPEN/CLOSE rows
        # (event_kind in {ADJUST, COLLECT, TRANSFER, NONE}) and rows whose
        # event_type has no taxonomy row stay NULL — same contract as the
        # writer's runtime path.
        #
        # Atomicity contract: ALTER TABLE + backfill UPDATE wrapped in
        # BEGIN IMMEDIATE / COMMIT — same pattern as the VIB-3614 migration
        # at line ~909 above. Without this, a crash between the column-add
        # and the backfill leaves rows permanently NULL because the next boot
        # sees the column already present and skips the backfill branch.
        if conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='accounting_events'").fetchone():
            conn.execute("BEGIN IMMEDIATE")
            try:
                if _add_column_if_missing("accounting_events", "position_reference", "TEXT"):
                    _backfill_position_reference_legacy(conn)
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise

        # VIB-4931: migrate any legacy pendle_lp/pendle_pt position_registry categories
        # to the generic lp/swap (see the function for the empty-partition rationale).
        _backfill_pendle_registry_category(conn)

    # -------------------------------------------------------------------------
    # State Operations
    # -------------------------------------------------------------------------

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

    async def get(self, deployment_id: str) -> StateData | None:
        """Get state for a strategy (single row per agent).

        Args:
            deployment_id: Deployment identifier.

        Returns:
            StateData if found, None otherwise.
        """
        if not self._initialized:
            await self.initialize()

        def _sync_get() -> StateData | None:
            cursor = self._conn.execute(  # type: ignore[union-attr]
                """
                SELECT deployment_id, version, state_data, schema_version,
                       checksum, created_at
                FROM strategy_state
                WHERE deployment_id = ?
                """,
                (deployment_id,),
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
                deployment_id=row["deployment_id"],
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
                f"Pre-commit checksum verification failed for {state.deployment_id}; refusing to write torn state"
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
                            WHERE deployment_id = ? AND version = ?
                            """,
                            (state_json, state.schema_version, checksum, now, state.deployment_id, expected_version),
                        )
                        if cursor.rowcount == 0:
                            # Read actual version while still inside the
                            # transaction so the error is consistent with what
                            # the CAS saw.
                            row = conn.execute(
                                "SELECT version FROM strategy_state WHERE deployment_id = ?",
                                (state.deployment_id,),
                            ).fetchone()
                            conn.execute("ROLLBACK")
                            raise StateConflictError(
                                deployment_id=state.deployment_id,
                                expected_version=expected_version,
                                actual_version=row["version"] if row else 0,
                            )
                    else:
                        # UPSERT: insert or update with version increment
                        conn.execute(
                            """
                            INSERT INTO strategy_state
                            (deployment_id, version, state_data, schema_version, checksum,
                             created_at, updated_at)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                            ON CONFLICT (deployment_id)
                            DO UPDATE SET
                                version = strategy_state.version + 1,
                                state_data = excluded.state_data,
                                schema_version = excluded.schema_version,
                                checksum = excluded.checksum,
                                updated_at = excluded.updated_at
                            """,
                            (
                                state.deployment_id,
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

    async def delete(self, deployment_id: str) -> bool:
        """Delete state row for a strategy.

        Args:
            deployment_id: Deployment identifier.

        Returns:
            True if state was deleted, False if not found.
        """
        if not self._initialized:
            await self.initialize()

        def _sync_delete() -> bool:
            cursor = self._conn.execute(  # type: ignore[union-attr]
                "DELETE FROM strategy_state WHERE deployment_id = ?",
                (deployment_id,),
            )
            self._conn.commit()  # type: ignore[union-attr]
            return cursor.rowcount > 0

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _sync_delete)

    async def get_all_deployment_ids(self) -> list[str]:
        """Get all deployment IDs.

        Returns:
            List of deployment IDs.
        """
        if not self._initialized:
            await self.initialize()

        def _sync_get_ids() -> list[str]:
            cursor = self._conn.execute(  # type: ignore[union-attr]
                "SELECT deployment_id FROM strategy_state ORDER BY deployment_id"
            )
            return [row["deployment_id"] for row in cursor.fetchall()]

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _sync_get_ids)

    # -------------------------------------------------------------------------
    # Timeline Event Operations
    # -------------------------------------------------------------------------
    #
    # VIB-4044 / PR5: removed. The SDK-side `timeline_events` table never
    # received writes in production runs (verified empirically across 3 demos
    # in PR1's TimelineScope-E2E-Findings.md). Gateway-side timeline events
    # are owned by `almanak.gateway.timeline.store.TimelineStore`.

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

            # VIB-4044 / PR5: timeline_events removed; total_events kept
            # in the stats payload for back-compat but is now always 0.
            stats["total_events"] = 0

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

        Durability (VIB-3181):
            The exists-check + INSERT/UPDATE pair runs inside a single
            ``BEGIN IMMEDIATE`` ... ``COMMIT`` transaction under
            ``_db_lock``. Without this, a concurrent writer racing
            between the SELECT and the INSERT could insert a row with
            the same ``order_id``, producing either a UNIQUE constraint
            violation or duplicate writes against the same logical
            order. The transaction also makes the write atomic with
            respect to readers and crash-durable on ``synchronous=FULL``.
        """
        # clob_orders is a deployment-scoped table (blueprint 29 §3): refuse
        # to persist a row under a blank identity. The caller must stamp the
        # order's deployment_id before it reaches persistence. (The CLOB
        # execution path does not yet wire this call — the guard makes the
        # latent empty-id footgun fail loudly if/when that wiring lands.)
        order_deployment_id = _require_deployment_id(order.deployment_id, operation="save_clob_order")
        if not self._initialized:
            await self.initialize()

        fills_json = json.dumps([f.to_dict() for f in order.fills], default=str)
        metadata_json = json.dumps(order.metadata, default=str)
        now = datetime.now(UTC).isoformat()

        def _sync_save() -> bool:
            conn = self._conn
            assert conn is not None

            with self._db_lock:
                conn.execute("BEGIN IMMEDIATE")
                try:
                    cursor = conn.execute(
                        """
                        SELECT id FROM clob_orders
                        WHERE order_id = ?
                          AND deployment_id = ?
                        LIMIT 1
                        """,
                        (order.order_id, order_deployment_id),
                    )
                    existing = cursor.fetchone()

                    if existing:
                        conn.execute(
                            """
                            UPDATE clob_orders
                            SET deployment_id = ?, market_id = ?, token_id = ?, side = ?, status = ?,
                                price = ?, size = ?, filled_size = ?, average_fill_price = ?,
                                fills = ?, order_type = ?, intent_id = ?, error = ?,
                                metadata = ?, updated_at = ?
                            WHERE id = ?
                            """,
                            (
                                order_deployment_id,
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
                                existing["id"],
                            ),
                        )
                    else:
                        conn.execute(
                            """
                            INSERT INTO clob_orders
                            (deployment_id, order_id, market_id, token_id, side, status,
                             price, size, filled_size, average_fill_price, fills,
                             order_type, intent_id, error, metadata, submitted_at,
                             updated_at)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                order_deployment_id,
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

                    conn.execute("COMMIT")
                    return True
                except Exception:
                    # Wrap ROLLBACK so a failure here (e.g. BEGIN IMMEDIATE
                    # itself failed and there is no active transaction)
                    # cannot mask the original exception (gemini-code-assist).
                    try:
                        conn.execute("ROLLBACK")
                    except Exception:
                        pass
                    raise

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _sync_save)

    async def get_clob_order(self, order_id: str, *, deployment_id: str) -> "ClobOrderState | None":
        """Get a CLOB order by order_id.

        Args:
            order_id: Order identifier.

        Returns:
            ClobOrderState if found, None otherwise.
        """
        deployment_id = _require_deployment_id(deployment_id, operation="get_clob_order")
        if not self._initialized:
            await self.initialize()

        def _sync_get() -> "ClobOrderState | None":
            cursor = self._conn.execute(  # type: ignore[union-attr]
                """
                SELECT deployment_id, order_id, market_id, token_id, side, status, price, size,
                       filled_size, average_fill_price, fills, order_type, intent_id,
                       error, metadata, submitted_at, updated_at
                FROM clob_orders
                WHERE order_id = ? AND deployment_id = ?
                ORDER BY submitted_at DESC
                LIMIT 1
                """,
                (order_id, deployment_id),
            )
            row = cursor.fetchone()

            if row is None:
                return None

            return self._row_to_clob_order(row)

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _sync_get)

    async def get_open_clob_orders(
        self,
        market_id: str | None = None,
        *,
        deployment_id: str,
    ) -> list["ClobOrderState"]:
        """Get all open CLOB orders, optionally filtered by market.

        Open orders are those with status: pending, submitted, live, partially_filled.

        Args:
            market_id: Optional market ID to filter by.

        Returns:
            List of open ClobOrderState, newest first.
        """
        deployment_id = _require_deployment_id(deployment_id, operation="get_open_clob_orders")
        if not self._initialized:
            await self.initialize()

        open_statuses = ("pending", "submitted", "live", "partially_filled")

        def _sync_get() -> list["ClobOrderState"]:
            placeholders = ",".join("?" * len(open_statuses))
            where = [f"status IN ({placeholders})", "deployment_id = ?"]
            params: list[Any] = [*open_statuses, deployment_id]
            if market_id:
                where.append("market_id = ?")
                params.append(market_id)
            cursor = self._conn.execute(  # type: ignore[union-attr]
                f"""
                SELECT deployment_id, order_id, market_id, token_id, side, status, price, size,
                       filled_size, average_fill_price, fills, order_type, intent_id,
                       error, metadata, submitted_at, updated_at
                FROM clob_orders
                WHERE {" AND ".join(where)}
                ORDER BY submitted_at DESC
                """,  # noqa: S608
                params,
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
        *,
        deployment_id: str,
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
        deployment_id = _require_deployment_id(deployment_id, operation="update_clob_order_status")
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

            params.extend([order_id, deployment_id])
            where = "order_id = ? AND deployment_id = ?"

            query = f"UPDATE clob_orders SET {', '.join(updates)} WHERE {where}"
            cursor = self._conn.execute(query, params)  # type: ignore[union-attr]
            self._conn.commit()  # type: ignore[union-attr]
            return cursor.rowcount > 0

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _sync_update)

    async def delete_clob_order(self, order_id: str, *, deployment_id: str) -> bool:
        """Delete a CLOB order from storage.

        Args:
            order_id: Order identifier.

        Returns:
            True if order was found and deleted.
        """
        deployment_id = _require_deployment_id(deployment_id, operation="delete_clob_order")
        if not self._initialized:
            await self.initialize()

        def _sync_delete() -> bool:
            cursor = self._conn.execute(  # type: ignore[union-attr]
                "DELETE FROM clob_orders WHERE order_id = ? AND deployment_id = ?",
                (order_id, deployment_id),
            )
            self._conn.commit()  # type: ignore[union-attr]
            return cursor.rowcount > 0

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _sync_delete)

    async def get_clob_orders_by_intent(
        self,
        intent_id: str,
        *,
        deployment_id: str,
    ) -> list["ClobOrderState"]:
        """Get all CLOB orders associated with an intent.

        Args:
            intent_id: Intent identifier.

        Returns:
            List of ClobOrderState, newest first.
        """
        deployment_id = _require_deployment_id(deployment_id, operation="get_clob_orders_by_intent")
        if not self._initialized:
            await self.initialize()

        def _sync_get() -> list["ClobOrderState"]:
            cursor = self._conn.execute(  # type: ignore[union-attr]
                """
                SELECT deployment_id, order_id, market_id, token_id, side, status, price, size,
                       filled_size, average_fill_price, fills, order_type, intent_id,
                       error, metadata, submitted_at, updated_at
                FROM clob_orders
                WHERE intent_id = ? AND deployment_id = ?
                ORDER BY submitted_at DESC
                """,
                (intent_id, deployment_id),
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
            deployment_id=row["deployment_id"] or "",
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

        Durability (VIB-3181):
            Wrapped in an explicit ``BEGIN IMMEDIATE`` ... ``COMMIT``
            transaction under ``_db_lock`` so the write is serialized
            with concurrent writers (CAS state, snapshot+metrics,
            clob orders) and torn writes never become visible to a
            later reader. With ``synchronous = FULL`` the commit is
            fsync'd before returning.

        Note:
            Uses INSERT OR REPLACE to handle unique constraint on (deployment_id, timestamp).
        """

        if not self._initialized:
            await self.initialize()

        def _sync_save() -> int:
            conn = self._conn
            assert conn is not None
            now = datetime.now(UTC).isoformat()

            with self._db_lock:
                conn.execute("BEGIN IMMEDIATE")
                try:
                    # Use INSERT...ON CONFLICT DO UPDATE rather than
                    # INSERT OR REPLACE: the latter deletes the old row
                    # and reinserts, which silently resets phase-4
                    # identity columns (deployment_id, cycle_id,
                    # execution_mode) — written by save_snapshot_and_metrics
                    # — to their TEXT '' defaults. The DO UPDATE clause
                    # only overwrites the columns this method actually
                    # provides, preserving phase-4 metadata on conflict
                    # (CodeRabbit review).
                    # VIB-4096 (3.5) — Phase 4 identity columns are now read
                    # off the snapshot itself (real fields after VIB-4092 /
                    # 3.1). The runner stamps them at capture time
                    # (VIB-4099 / 3.8); this writer carries them onto the
                    # row. ON CONFLICT preserves any existing non-empty
                    # identity (asymmetric: writing "" over a real id MUST
                    # keep the real id; writing a real id over "" MUST take
                    # the real id) so a late re-save by a less-stamped
                    # caller cannot blank out a previously-stamped row.
                    cursor = conn.execute(
                        """
                        INSERT INTO portfolio_snapshots (
                            deployment_id, timestamp, iteration_number, total_value_usd,
                            available_cash_usd, deployed_capital_usd, wallet_total_value_usd,
                            value_confidence, positions_json,
                            token_prices_json, wallet_balances_json,
                            chain, created_at,
                            cycle_id, execution_mode
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(deployment_id, timestamp) DO UPDATE SET
                            iteration_number = excluded.iteration_number,
                            total_value_usd = excluded.total_value_usd,
                            available_cash_usd = excluded.available_cash_usd,
                            deployed_capital_usd = excluded.deployed_capital_usd,
                            wallet_total_value_usd = excluded.wallet_total_value_usd,
                            value_confidence = excluded.value_confidence,
                            positions_json = excluded.positions_json,
                            token_prices_json = excluded.token_prices_json,
                            wallet_balances_json = excluded.wallet_balances_json,
                            chain = excluded.chain,
                            created_at = excluded.created_at,
                            cycle_id = CASE
                                WHEN portfolio_snapshots.cycle_id = ''
                                THEN excluded.cycle_id
                                ELSE portfolio_snapshots.cycle_id
                            END,
                            execution_mode = CASE
                                WHEN portfolio_snapshots.execution_mode = ''
                                THEN excluded.execution_mode
                                ELSE portfolio_snapshots.execution_mode
                            END
                        RETURNING id
                        """,
                        (
                            _canonical_deployment_id(snapshot),
                            snapshot.timestamp.isoformat(),
                            snapshot.iteration_number,
                            str(snapshot.total_value_usd),
                            str(snapshot.available_cash_usd),
                            str(snapshot.deployed_capital_usd),
                            str(snapshot.wallet_total_value_usd),
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
                            snapshot.cycle_id or "",
                            snapshot.execution_mode or "",
                        ),
                    )
                    # `RETURNING id` makes the row id unambiguous on both
                    # the INSERT and the ON CONFLICT DO UPDATE path
                    # (cursor.lastrowid is not updated when a conflict
                    # took the UPDATE branch).
                    returned = cursor.fetchone()
                    row_id = returned[0] if returned else (cursor.lastrowid or 0)
                    conn.execute("COMMIT")
                    return row_id
                except Exception:
                    # Wrap ROLLBACK so a failure here (e.g. BEGIN IMMEDIATE
                    # itself failed and there is no active transaction)
                    # cannot mask the original exception (gemini-code-assist).
                    try:
                        conn.execute("ROLLBACK")
                    except Exception:
                        pass
                    raise

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
                    cycle_id = getattr(snapshot, "cycle_id", "") or getattr(metrics, "cycle_id", "") or ""
                    execution_mode = (
                        getattr(snapshot, "execution_mode", "") or getattr(metrics, "execution_mode", "") or ""
                    )
                    deployment_id = snapshot.deployment_id or metrics.deployment_id

                    # 1. Save snapshot via INSERT ... ON CONFLICT DO UPDATE
                    # mirroring save_portfolio_snapshot (CodeRabbit). The
                    # legacy INSERT OR REPLACE deleted-and-reinserted on
                    # conflict, which (a) blanked any previously-stamped
                    # identity to TEXT '' on a less-stamped retry and
                    # (b) cascade-deleted any position_state_snapshots
                    # tied to the original snapshot id. The asymmetric
                    # CASE preserves an existing non-empty identity:
                    # writing "" over a real id MUST keep the real id;
                    # writing a real id over "" MUST take the real id.
                    # RETURNING id makes the row id unambiguous on both
                    # the INSERT and the DO UPDATE branch (lastrowid is
                    # not updated when ON CONFLICT takes the UPDATE path).
                    cursor = conn.execute(
                        """
                        INSERT INTO portfolio_snapshots (
                            deployment_id, cycle_id, execution_mode,
                            timestamp, iteration_number, total_value_usd,
                            available_cash_usd, deployed_capital_usd, wallet_total_value_usd,
                            value_confidence, positions_json,
                            token_prices_json, wallet_balances_json,
                            chain, created_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(deployment_id, timestamp) DO UPDATE SET
                            iteration_number = excluded.iteration_number,
                            total_value_usd = excluded.total_value_usd,
                            available_cash_usd = excluded.available_cash_usd,
                            deployed_capital_usd = excluded.deployed_capital_usd,
                            wallet_total_value_usd = excluded.wallet_total_value_usd,
                            value_confidence = excluded.value_confidence,
                            positions_json = excluded.positions_json,
                            token_prices_json = excluded.token_prices_json,
                            wallet_balances_json = excluded.wallet_balances_json,
                            chain = excluded.chain,
                            created_at = excluded.created_at,
                            cycle_id = CASE
                                WHEN portfolio_snapshots.cycle_id = ''
                                THEN excluded.cycle_id
                                ELSE portfolio_snapshots.cycle_id
                            END,
                            execution_mode = CASE
                                WHEN portfolio_snapshots.execution_mode = ''
                                THEN excluded.execution_mode
                                ELSE portfolio_snapshots.execution_mode
                            END
                        RETURNING id
                        """,
                        (
                            deployment_id,
                            cycle_id,
                            execution_mode,
                            snapshot.timestamp.isoformat(),
                            snapshot.iteration_number,
                            str(snapshot.total_value_usd),
                            str(snapshot.available_cash_usd),
                            str(snapshot.deployed_capital_usd),
                            str(snapshot.wallet_total_value_usd),
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
                    returned = cursor.fetchone()
                    snapshot_id = returned[0] if returned else (cursor.lastrowid or 0)

                    # 2. Save metrics in the same transaction
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO portfolio_metrics (
                            deployment_id, initial_value_usd, initial_timestamp,
                            deposits_usd, withdrawals_usd, gas_spent_usd,
                            total_value_usd, positions_json, cycle_id,
                            execution_mode, is_complete, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            _canonical_deployment_id(metrics),
                            str(metrics.initial_value_usd),
                            metrics.timestamp.isoformat(),
                            str(metrics.deposits_usd),
                            str(metrics.withdrawals_usd),
                            str(metrics.gas_spent_usd),
                            str(metrics.total_value_usd),
                            getattr(metrics, "positions_json", "[]"),
                            cycle_id,
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

    async def get_latest_snapshot(self, deployment_id: str) -> "PortfolioSnapshot | None":
        """Get the most recent portfolio snapshot for a strategy.

        Args:
            deployment_id: Deployment identifier.

        Returns:
            Most recent PortfolioSnapshot or None if not found.
        """

        if not self._initialized:
            await self.initialize()

        def _sync_get() -> "PortfolioSnapshot | None":
            cursor = self._conn.execute(  # type: ignore[union-attr]
                """
                SELECT timestamp, iteration_number, total_value_usd,
                       available_cash_usd, deployed_capital_usd, wallet_total_value_usd,
                       value_confidence, positions_json,
                       token_prices_json, wallet_balances_json, chain,
                       deployment_id, cycle_id, execution_mode
                FROM portfolio_snapshots
                WHERE deployment_id = ?
                ORDER BY timestamp DESC
                LIMIT 1
                """,
                (deployment_id,),
            )
            row = cursor.fetchone()
            if not row:
                return None
            return self._row_to_portfolio_snapshot(row)

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _sync_get)

    async def get_first_snapshot(self, deployment_id: str) -> "PortfolioSnapshot | None":
        """Get the earliest persisted portfolio snapshot for a strategy."""
        if not self._initialized:
            await self.initialize()

        def _sync_get() -> "PortfolioSnapshot | None":
            cursor = self._conn.execute(  # type: ignore[union-attr]
                """
                SELECT timestamp, iteration_number, total_value_usd,
                       available_cash_usd, deployed_capital_usd, wallet_total_value_usd,
                       value_confidence, positions_json,
                       token_prices_json, wallet_balances_json, chain,
                       deployment_id, cycle_id, execution_mode
                FROM portfolio_snapshots
                WHERE deployment_id = ?
                ORDER BY timestamp ASC, id ASC
                LIMIT 1
                """,
                (deployment_id,),
            )
            row = cursor.fetchone()
            if not row:
                return None
            return self._row_to_portfolio_snapshot(row)

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _sync_get)

    async def get_recent_snapshots(
        self,
        deployment_id: str,
        limit: int = 2,
    ) -> list["PortfolioSnapshot"]:
        """Get the N most-recent portfolio snapshots ordered **oldest-first**.

        The oldest-first ordering matches what the F4 / VIB-4907 SWAP-class
        fallback detector expects (it compares the last two entries as
        ``pre`` / ``post``), and is also the natural ordering for any
        consumer that wants to walk the window forward in time.

        Use this when you need a fixed-size window of the latest snapshots
        without computing a ``since`` timestamp upfront — for unbounded /
        chart use cases ``get_snapshots_since`` remains the right call.

        Args:
            deployment_id: Deployment identifier.
            limit: Maximum number of snapshots to return.  ``1`` is
                equivalent to ``get_latest_snapshot`` wrapped in a list.

        Returns:
            Up to ``limit`` snapshots, oldest-first.  Empty list when the
            deployment has no snapshots; never raises.
        """
        if limit <= 0:
            return []
        if not self._initialized:
            await self.initialize()

        def _sync_get() -> list["PortfolioSnapshot"]:
            cursor = self._conn.execute(  # type: ignore[union-attr]
                """
                SELECT timestamp, iteration_number, total_value_usd,
                       available_cash_usd, deployed_capital_usd, wallet_total_value_usd,
                       value_confidence, positions_json,
                       token_prices_json, wallet_balances_json, chain,
                       deployment_id, cycle_id, execution_mode
                FROM portfolio_snapshots
                WHERE deployment_id = ?
                ORDER BY timestamp DESC
                LIMIT ?
                """,
                (deployment_id, limit),
            )
            # SELECT DESC then reverse, so the caller gets oldest-first.
            return list(reversed([self._row_to_portfolio_snapshot(row) for row in cursor.fetchall()]))

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _sync_get)

    async def get_snapshots_since(
        self,
        deployment_id: str,
        since: datetime,
        limit: int = 168,
    ) -> list["PortfolioSnapshot"]:
        """Get portfolio snapshots since a timestamp.

        Used for building PnL charts in the dashboard.

        Args:
            deployment_id: Deployment identifier.
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
                SELECT timestamp, iteration_number, total_value_usd,
                       available_cash_usd, deployed_capital_usd, wallet_total_value_usd,
                       value_confidence, positions_json,
                       token_prices_json, wallet_balances_json, chain,
                       deployment_id, cycle_id, execution_mode
                FROM portfolio_snapshots
                WHERE deployment_id = ? AND timestamp >= ?
                ORDER BY timestamp ASC
                LIMIT ?
                """,
                (deployment_id, since.isoformat(), limit),
            )
            return [self._row_to_portfolio_snapshot(row) for row in cursor.fetchall()]

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _sync_get)

    async def get_snapshots_in_window(
        self,
        deployment_id: str,
        from_ts: datetime | None,
        to_ts: datetime | None,
        *,
        scan_cap: int = 200_000,
    ) -> tuple[list[tuple[datetime, str | None, str | None, str | None]], bool]:
        """Projected NAV samples inside a time window, for windowed charts (VIB-5059 P2).

        Returns ``(rows, truncated)`` where ``rows`` is ``(timestamp,
        total_value_usd_text, value_confidence_text, positions_json_text)``
        oldest-first. The chart-relevant columns plus ``positions_json`` (VIB-5170,
        for per-row BORROW debt netting) are projected; the other JSON blobs
        (``token_prices_json`` / ``wallet_balances_json``) are still excluded as
        they dominate transfer size and the NAV line does not need them.

        ``total_value_usd`` is returned as its **raw stored text** (not parsed to
        ``Decimal``) so the caller owns the Empty≠Zero decision: ``""`` / ``None``
        is an unmeasured sample, never a measured ``$0``.

        Window bounds are open when ``None`` (``from_ts=None`` → from inception,
        ``to_ts=None`` → until now). ``scan_cap`` bounds the fetch: the newest
        ``scan_cap`` in-window rows are returned (so the chart's right edge / current
        value is always present) and ``truncated`` is ``True`` when the window held
        more — surfaced loudly by the caller rather than presented as the whole
        window.
        """
        if scan_cap <= 0:
            raise ValueError(f"scan_cap must be positive, got {scan_cap}")
        if not self._initialized:
            await self.initialize()

        def _sync_get() -> tuple[list[tuple[datetime, str | None, str | None, str | None]], bool]:
            clauses = ["deployment_id = ?"]
            params: list[object] = [deployment_id]
            if from_ts is not None:
                clauses.append("timestamp >= ?")
                params.append(from_ts.isoformat())
            if to_ts is not None:
                clauses.append("timestamp <= ?")
                params.append(to_ts.isoformat())
            params.append(scan_cap + 1)
            cursor = self._conn.execute(  # type: ignore[union-attr]
                f"""
                SELECT timestamp, total_value_usd, value_confidence, positions_json
                FROM portfolio_snapshots
                WHERE {" AND ".join(clauses)}
                ORDER BY timestamp DESC, id DESC
                LIMIT ?
                """,
                tuple(params),
            )
            fetched = cursor.fetchall()
            truncated = len(fetched) > scan_cap
            if truncated:
                # Keep the newest scan_cap rows; the DESC order already places
                # them first, so the surplus (oldest) tail is dropped.
                fetched = fetched[:scan_cap]

            rows: list[tuple[datetime, str | None, str | None, str | None]] = []
            for row in reversed(fetched):  # DESC fetch -> emit oldest-first
                ts = row["timestamp"]
                if isinstance(ts, str):
                    ts = datetime.fromisoformat(ts)
                rows.append((ts, row["total_value_usd"], row["value_confidence"], row["positions_json"]))
            return rows, truncated

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _sync_get)

    async def get_nav_series(
        self,
        deployment_id: str,
        *,
        since: tuple[datetime, int] | None = None,
        scan_cap: int = 200_000,
    ) -> tuple[list[tuple[datetime, str | None, str | None, int, str | None]], bool]:
        """NAV-component series for lifetime drawdown / high-watermark (VIB-5118/5134).

        Returns ``(rows, truncated)`` where ``rows`` is ``(timestamp,
        total_value_usd_text, available_cash_usd_text, id, positions_json_text)``
        oldest-first (``positions_json`` is the VIB-5170 debt-netting input). Unlike
        :meth:`get_recent_snapshots` (newest 168 rows — a ~14h window at the
        5-min cadence), the default (``since=None``) projects the **whole**
        history so a lifetime peak or drawdown older than the recent window is no
        longer silently understated. Only the two NAV columns are projected (the
        JSON blobs dominate transfer size and the wallet-NAV line ``total + cash``,
        VIB-3884, does not need them) plus the row ``id``, which is the cursor
        tiebreaker for incremental folds.

        Both NAV columns are returned as their **raw stored text** (not parsed to
        ``Decimal``) so the caller owns the Empty≠Zero decision: ``""`` / ``None``
        is an unmeasured sample, never a measured ``$0``.

        **Two fetch modes** (VIB-5134 — the incremental "fetch since cursor"):

        - ``since=None`` (full scan): the **newest** ``scan_cap`` rows are kept
          (so the right edge / current value is always present for the
          current-drawdown term), emitted oldest-first; ``truncated`` is ``True``
          when older history was dropped — surfaced loudly rather than presented
          as lifetime.
        - ``since=(last_ts, last_id)`` (incremental): only rows strictly newer than
          the cursor — ``timestamp > last_ts OR (timestamp = last_ts AND id >
          last_id)`` — are returned, **oldest-first**, so folding them advances a
          running-peak checkpoint identically to a full recompute. Here truncation
          keeps the **oldest** ``scan_cap`` rows after the cursor (contiguous, no
          gap), so the caller advances its cursor and catches up on the next call;
          ``truncated`` means more new rows remain, not that history was lost.

        VIB-5059 Phase 3's persisted incremental fold is the unbounded-history fix.
        """
        if scan_cap <= 0:
            raise ValueError(f"scan_cap must be positive, got {scan_cap}")
        if not self._initialized:
            await self.initialize()

        def _sync_get() -> tuple[list[tuple[datetime, str | None, str | None, int, str | None]], bool]:
            params: list[object] = [deployment_id]
            where = "deployment_id = ?"
            if since is not None:
                since_ts, since_id = since
                # (timestamp, id) composite cursor: rows strictly after it, so
                # identical-timestamp / replay rows neither skip nor double-fold.
                # The TEXT timestamp column compares against .isoformat() exactly
                # as the existing ORDER BY's lexicographic ISO-8601 ordering does
                # (same precedent as get_snapshot_at / get_snapshots_in_window).
                # Snapshots are ALWAYS written via ``snapshot.timestamp.isoformat()``
                # (see save_portfolio_snapshot / save_snapshot_and_metrics), so the
                # stored text is itself an isoformat() product and the
                # parse-then-reserialize round-trip on the cursor is byte-idempotent
                # — the boundary row is matched exactly, never skipped/double-folded.
                where += " AND (timestamp > ? OR (timestamp = ? AND id > ?))"
                params.extend([since_ts.isoformat(), since_ts.isoformat(), since_id])
            params.append(scan_cap + 1)
            # Full scan keeps the newest cap (DESC) then reverses to oldest-first;
            # the incremental scan must keep the OLDEST cap after the cursor so the
            # fold has no gap, so it orders ASC (already oldest-first).
            order = "ASC, id ASC" if since is not None else "DESC, id DESC"
            # VIB-5134: serialize the read under _db_lock. Writers hold _db_lock
            # across BEGIN IMMEDIATE … COMMIT on this single shared connection, and
            # WAL gives snapshot isolation BETWEEN connections, not within one — so
            # an unsynchronized read could observe a writer's uncommitted rows. For
            # the incremental (since) cursor that is a correctness bug, not just a
            # transient one: advancing the (timestamp, id) cursor past a row that
            # later rolls back would permanently skip it. Taking the same lock the
            # writers take makes the read see only committed state (CodeRabbit).
            with self._db_lock:
                cursor = self._conn.execute(  # type: ignore[union-attr]
                    f"""
                    SELECT timestamp, total_value_usd, available_cash_usd, id, positions_json
                    FROM portfolio_snapshots
                    WHERE {where}
                    ORDER BY timestamp {order}
                    LIMIT ?
                    """,
                    tuple(params),
                )
                fetched = cursor.fetchall()
            truncated = len(fetched) > scan_cap
            if truncated:
                # Keep the first scan_cap rows: for the full scan (DESC) these are
                # the newest; for the incremental scan (ASC) these are the oldest
                # after the cursor (contiguous paging). The surplus tail is dropped.
                fetched = fetched[:scan_cap]
            # Incremental (ASC) is already oldest-first; full (DESC) is reversed.
            ordered = fetched if since is not None else list(reversed(fetched))

            rows: list[tuple[datetime, str | None, str | None, int, str | None]] = []
            for row in ordered:
                ts = row["timestamp"]
                if isinstance(ts, str):
                    ts = datetime.fromisoformat(ts)
                # VIB-5170: positions_json rides along (raw text, 5th element) so
                # the lifetime-drawdown fold can debt-net the BORROW leg per row
                # (the dashboard layer owns the parse + Empty≠Zero). Without it the
                # lifetime drawdown — preferred over the recent window on the main
                # PnL surface — overstates drawdown for a leverage loop whose NAV
                # phantom-spikes at open and collapses at teardown.
                rows.append((ts, row["total_value_usd"], row["available_cash_usd"], row["id"], row["positions_json"]))
            return rows, truncated

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _sync_get)

    async def get_snapshot_at(
        self,
        deployment_id: str,
        timestamp: datetime,
    ) -> "PortfolioSnapshot | None":
        """Get the portfolio snapshot closest to a timestamp.

        Used for calculating PnL at specific points in time (e.g., 24h ago).

        Args:
            deployment_id: Deployment identifier.
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
                SELECT timestamp, iteration_number, total_value_usd,
                       available_cash_usd, deployed_capital_usd, wallet_total_value_usd,
                       value_confidence, positions_json,
                       token_prices_json, wallet_balances_json, chain,
                       deployment_id, cycle_id, execution_mode
                FROM portfolio_snapshots
                WHERE deployment_id = ? AND timestamp <= ?
                ORDER BY timestamp DESC
                LIMIT 1
                """,
                (deployment_id, timestamp.isoformat()),
            )
            row = cursor.fetchone()
            if not row:
                return None
            return self._row_to_portfolio_snapshot(row)

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _sync_get)

    @staticmethod
    def _safe_row_str(row: sqlite3.Row, column: str, default: str = "") -> str:
        """Read a string column defensively — returns ``default`` if the column
        is missing (older DB schema) or stored as ``NULL``."""
        try:
            return str(row[column] or default)
        except (KeyError, IndexError):
            return default

    @staticmethod
    def _safe_row_json(row: sqlite3.Row, column: str, default: Any) -> Any:
        """Decode a JSON-text column defensively.

        Three failure modes, two signal levels:

        * Missing column (legacy DB schema) → silent. Expected for older
          DBs that pre-date the column being added; the pattern across
          this file is migrate-then-validate, so an absent column self-
          heals on next bootstrap.
        * NULL / empty value → silent. No data is a valid state.
        * Invalid JSON → ``logger.warning`` with the column name. Corrupt
          payload is *not* expected and indicates either a writer bug
          or row-level data corruption; an operator should see it.
        """
        try:
            raw = row[column]
        except (KeyError, IndexError):
            return default
        if raw and isinstance(raw, str):
            try:
                return json.loads(raw)
            except json.JSONDecodeError as exc:
                logger.warning(
                    "Corrupt JSON in portfolio_snapshots.%s — returning default; error: %s",
                    column,
                    exc,
                )
                return default
        return default

    def _row_to_portfolio_snapshot(self, row: sqlite3.Row) -> "PortfolioSnapshot":
        """Convert a SQLite row to PortfolioSnapshot.

        Args:
            row: SQLite row from portfolio_snapshots table.

        Returns:
            PortfolioSnapshot instance.
        """
        from almanak.framework.portfolio.models import PortfolioSnapshot

        timestamp = row["timestamp"]
        if isinstance(timestamp, str):
            timestamp = datetime.fromisoformat(timestamp)
        positions_payload = row["positions_json"]
        if isinstance(positions_payload, str):
            positions_payload = json.loads(positions_payload)
        positions, snapshot_metadata = PortfolioSnapshot.unpack_positions_payload(positions_payload)

        return PortfolioSnapshot.from_dict(
            {
                "timestamp": timestamp.isoformat(),
                "total_value_usd": str(row["total_value_usd"]),
                "available_cash_usd": str(row["available_cash_usd"]),
                "deployed_capital_usd": self._safe_row_str(row, "deployed_capital_usd", "0"),
                "wallet_total_value_usd": self._safe_row_str(row, "wallet_total_value_usd", "0"),
                "value_confidence": row["value_confidence"],
                "positions": positions,
                "wallet_balances": self._safe_row_json(row, "wallet_balances_json", []),
                "token_prices": self._safe_row_json(row, "token_prices_json", {}),
                "chain": row["chain"] or "",
                "iteration_number": row["iteration_number"] or 0,
                "snapshot_metadata": snapshot_metadata,
                "deployment_id": self._safe_row_str(row, "deployment_id", ""),
                "cycle_id": self._safe_row_str(row, "cycle_id", ""),
                "execution_mode": self._safe_row_str(row, "execution_mode", ""),
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
                    deployment_id, initial_value_usd, initial_timestamp,
                    deposits_usd, withdrawals_usd, gas_spent_usd,
                    total_value_usd, positions_json, cycle_id,
                    execution_mode, is_complete, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    _canonical_deployment_id(metrics),
                    str(metrics.initial_value_usd),
                    metrics.timestamp.isoformat(),
                    str(metrics.deposits_usd),
                    str(metrics.withdrawals_usd),
                    str(metrics.gas_spent_usd),
                    str(metrics.total_value_usd),
                    getattr(metrics, "positions_json", "[]"),
                    getattr(metrics, "cycle_id", None),
                    getattr(metrics, "execution_mode", "") or "",
                    getattr(metrics, "is_complete", True),
                    now,
                ),
            )
            self._conn.commit()  # type: ignore[union-attr]
            return True

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _sync_save)

    async def get_portfolio_metrics(self, deployment_id: str) -> "PortfolioMetrics | None":
        """Get portfolio metrics for a strategy.

        Args:
            deployment_id: Deployment identifier.

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
                SELECT initial_value_usd, initial_timestamp,
                       deposits_usd, withdrawals_usd, gas_spent_usd,
                       total_value_usd, positions_json, cycle_id,
                       deployment_id, execution_mode, is_complete, updated_at
                FROM portfolio_metrics
                WHERE deployment_id = ?
                """,
                (deployment_id,),
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
            row_deployment_id = ""
            execution_mode = ""
            is_complete = True
            try:
                row_deployment_id = row["deployment_id"] or ""
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
                timestamp=updated_at,
                total_value_usd=Decimal(row["total_value_usd"] or "0"),
                initial_value_usd=Decimal(row["initial_value_usd"]),
                deposits_usd=Decimal(row["deposits_usd"]),
                withdrawals_usd=Decimal(row["withdrawals_usd"]),
                gas_spent_usd=Decimal(row["gas_spent_usd"]),
                positions_json=row["positions_json"] or "[]",
                cycle_id=row["cycle_id"],
                deployment_id=row_deployment_id,
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

    async def update_position_attribution(
        self,
        event_id: str,
        attribution_json: str,
        attribution_version: int,
        deployment_id: str = "",
    ) -> bool:
        """Update only the attribution fields of a position event.

        Unlike save_position_event (INSERT OR REPLACE), this preserves all
        other stored fields (timestamp, token0/token1, ticks, liquidity, etc.).

        ``deployment_id`` is accepted but currently unused at the WHERE clause
        because ``id`` is a UUID — globally unique by construction, so a
        ``WHERE id = ?`` filter is data-layer-safe in the single-tenant SQLite
        backend. The kwarg exists so the GSM client can forward the caller's
        deployment_id to the gateway proto request as defense-in-depth wire
        scope; future hosted PostgresStore implementations may add it to the
        WHERE clause for multi-tenant scoping.
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
    # Position-state snapshots (Track C / AttemptNo17 §3 D4 / VIB-3891)
    # =========================================================================

    async def save_position_state_snapshots(
        self,
        snapshot_id: int,
        rows: list,
    ) -> int:
        """Bulk-insert ``position_state_snapshots`` rows tied to a parent
        ``portfolio_snapshots.id``.

        Each row in ``rows`` must be a
        :class:`almanak.framework.accounting.position_state.PositionStateRow`.
        The caller is responsible for materializing those rows from open
        positions — this method only persists them. We accept ``Any`` in
        the type signature to keep the import surface from circular-stamping
        the accounting module from the state backend at module-load time
        (state backend is imported before the accounting module).

        Returns the number of rows written. ``0`` is a valid return value
        when the snapshot had no open positions; that's a measured zero,
        not a failure. The rows are persisted in the same write-lock window
        as a single transaction so the time series cannot have a gap that
        looks like "no positions" but is actually a partial-write crash.
        """
        if not rows:
            return 0
        if not self._initialized:
            await self.initialize()

        # Materialize the value tuples eagerly while the caller still owns
        # the dataclass references — avoids holding the GIL inside the
        # executor longer than the actual sqlite write.
        captured_rows: list[tuple] = []
        for r in rows:
            captured_rows.append(
                (
                    snapshot_id,
                    r.deployment_id,
                    r.cycle_id,
                    r.timestamp.isoformat() if r.timestamp else "",
                    r.position_id,
                    r.position_type,
                    r.current_tick,
                    # SQLite has no native bool — store 0/1, but preserve
                    # the None case (unmeasured ≠ False).
                    None if r.in_range is None else int(bool(r.in_range)),
                    None if r.liquidity is None else str(r.liquidity),
                    None if r.sqrt_price_x96 is None else str(r.sqrt_price_x96),
                    None if r.supply_balance is None else str(r.supply_balance),
                    None if r.borrow_balance is None else str(r.borrow_balance),
                    None if r.health_factor is None else str(r.health_factor),
                    None if r.supply_apy_pct is None else str(r.supply_apy_pct),
                    None if r.borrow_apy_pct is None else str(r.borrow_apy_pct),
                    None if r.interest_accrued_since_last is None else str(r.interest_accrued_since_last),
                    None if r.mark_price is None else str(r.mark_price),
                    None if r.unrealized_pnl is None else str(r.unrealized_pnl),
                    None if r.funding_accrued_since_last is None else str(r.funding_accrued_since_last),
                    None if r.liquidation_price is None else str(r.liquidation_price),
                    None if r.margin_utilisation_pct is None else str(r.margin_utilisation_pct),
                    None if r.delta_vs_protocol_pct is None else str(r.delta_vs_protocol_pct),
                    r.value_confidence,
                    r.schema_version,
                    r.formula_version,
                    r.matching_policy_version,
                )
            )

        def _sync_save() -> int:
            with self._db_lock:
                self._conn.executemany(  # type: ignore[union-attr]
                    """
                    INSERT INTO position_state_snapshots (
                        snapshot_id, deployment_id, cycle_id,
                        captured_at, position_id, position_type,
                        current_tick, in_range, liquidity, sqrt_price_x96,
                        supply_balance, borrow_balance, health_factor,
                        supply_apy_pct, borrow_apy_pct,
                        interest_accrued_since_last,
                        mark_price, unrealized_pnl,
                        funding_accrued_since_last, liquidation_price,
                        margin_utilisation_pct,
                        delta_vs_protocol_pct, value_confidence,
                        schema_version, formula_version, matching_policy_version
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    captured_rows,
                )
                self._conn.commit()  # type: ignore[union-attr]
            return len(captured_rows)

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _sync_save)

    async def get_position_state_snapshots(
        self,
        snapshot_id: int | None = None,
        deployment_id: str | None = None,
        position_id: str | None = None,
        limit: int = 1000,
    ) -> list[dict]:
        """Read back ``position_state_snapshots`` rows, filtered as needed.

        Used by the Accountant Test cell evaluators (G14, G15, LP2, LP6,
        L2/L3/L5) and by debug/inspection tools. Filters are AND'd; pass
        ``None`` to skip a dimension.
        """
        if not self._initialized:
            await self.initialize()

        def _sync_get() -> list[dict]:
            where: list[str] = []
            params: list = []
            if snapshot_id is not None:
                where.append("snapshot_id = ?")
                params.append(snapshot_id)
            if deployment_id is not None:
                # SQL column is the canonical `deployment_id` (blueprint 29
                # §3); the method parameter is still named `deployment_id`
                # (VIB-4726).
                where.append("deployment_id = ?")
                params.append(deployment_id)
            if position_id is not None:
                where.append("position_id = ?")
                params.append(position_id)
            sql = "SELECT * FROM position_state_snapshots"
            if where:
                sql += " WHERE " + " AND ".join(where)
            sql += " ORDER BY captured_at DESC LIMIT ?"
            params.append(limit)
            cursor = self._conn.execute(sql, params)  # type: ignore[union-attr]
            return [dict(row) for row in cursor.fetchall()]

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _sync_get)

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
                    (id, cycle_id, deployment_id, execution_mode,
                     timestamp, intent_type,
                     token_in, amount_in, token_out, amount_out,
                     effective_price, slippage_bps, gas_used, gas_usd,
                     tx_hash, chain, protocol, success, error,
                     extracted_data_json, price_inputs_json, pre_state_json, post_state_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        entry.id,
                        entry.cycle_id,
                        _canonical_deployment_id(entry),
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

    # =========================================================================
    # Auto-mode collision preflight (VIB-4614)
    # =========================================================================

    async def find_open_auto_mode_registry_row(
        self,
        *,
        deployment_id: str,
        chain: str,
        accounting_category: str,
        semantic_grouping_key: str,
    ) -> dict[str, str] | None:
        """Return the open auto-mode ``position_registry`` row that would
        collide with a handle-less open for this semantic group, or ``None``.

        This is the single-source predicate behind both the pre-execution
        LP registry-collision preflight (VIB-4614 — reject BEFORE minting an
        orphan NFT) and the post-mint commit-path collision classifier in
        :meth:`save_ledger_and_registry_atomic`. Both MUST mirror the partial
        unique index ``ix_registry_auto_mode`` predicate exactly:

            WHERE status = 'open' AND handle IS NULL

        (defined in ``SCHEMA_SQL`` on
        ``(deployment_id, chain, accounting_category, semantic_grouping_key)``).
        The ``status = 'open' AND handle IS NULL`` clauses are inlined into
        the SQL — NOT bound as parameters — because they MUST stay byte-for-byte
        identical to the index's ``WHERE`` clause; a parameterised variant could
        drift from the index predicate during a future edit and silently stop
        matching.

        Returns a dict with ``physical_identity_hash`` and ``opened_tx`` (the
        winning row's identity, so the caller can raise an actionable
        :class:`RegistryAutoCollisionError`), or ``None`` when the group is
        free (no orphan risk; the open may proceed).
        """
        # Init guard — mirror the sibling registry read
        # ``get_position_registry_open_rows``: ensure the store is initialized
        # so ``self._conn`` is established before the worker thread touches it.
        if not self._initialized:
            await self.initialize()

        def _sync_find() -> dict[str, str] | None:
            # Controlled failure (matches ``get_position_registry_open_rows``'s
            # ``if not self._conn: return []``): an unestablished connection
            # means there is no registry to consult, so there is no collision —
            # return ``None`` (allow the open) rather than dereferencing None.
            # Fail-open is safe here: the commit-path unique-index INSERT is the
            # authoritative backstop.
            if self._conn is None:
                return None
            with self._db_lock:
                cursor = self._conn.execute(
                    """
                    SELECT physical_identity_hash, opened_tx
                    FROM position_registry
                    WHERE deployment_id = ?
                      AND chain = ?
                      AND accounting_category = ?
                      AND semantic_grouping_key = ?
                      AND status = 'open'
                      AND handle IS NULL
                    LIMIT 1
                    """,
                    (
                        deployment_id,
                        chain,
                        accounting_category,
                        semantic_grouping_key,
                        # status='open' AND handle IS NULL are inlined above
                        # because they MUST exactly mirror the partial unique
                        # index ix_registry_auto_mode WHERE clause (SCHEMA_SQL).
                    ),
                )
                row = cursor.fetchone()
            if row is None:
                return None
            return {
                "physical_identity_hash": row["physical_identity_hash"],
                "opened_tx": row["opened_tx"] or "",
            }

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _sync_find)

    # =========================================================================
    # Atomic ledger + registry + handle commit (VIB-4197 / T11)
    # =========================================================================

    async def save_ledger_and_registry_atomic(
        self,
        entry: "LedgerEntry",
        registry: "RegistryRow",
        handle: "HandleMapping | None",
        mode: str = "commit",
    ) -> None:
        """Single-transaction commit of ledger + position_registry + handle.

        Per blueprint 28 §4.1 (local-mode contract). All three writes execute
        inside a single ``BEGIN IMMEDIATE`` ... ``COMMIT``; failure of any
        write rolls the entire transaction back so neither row lands on disk.

        Mode contract (T24 / VIB-4210 / VIB-4221 ADR §8.1 — ratified Option (c)):

        - ``mode='commit'`` (default; backward-compatible): the original
          three-write contract. Writes ``transaction_ledger`` row, then
          UPSERTs ``position_registry``, then runs the handle-backfill UPDATE.
        - ``mode='registry_reconciliation'``: control-plane reconciliation
          path invoked by ``PositionService.Reconcile`` when ``apply=true``.
          SKIPS the ledger write (step 1) but still runs the registry UPSERT
          + handle backfill atomically inside the same SQLite transaction.
          The ``entry`` argument is still required so the function signature
          stays uniform — it just isn't written. Callers under this mode
          MUST pass a registry row whose ``payload`` includes
          ``source='reconciliation_discovery'`` so downstream readers can
          distinguish chain-derived rows from intent-derived rows
          (ADR §4 algorithm sketch step 6).

        Why this design: the ratified single-registry-writer rule (VIB-3862
        chokepoint lesson + ADR §8.1 Option (c)) keeps the ON CONFLICT clause,
        the monotone-priority guard, and the ``RegistryAutoCollisionError``
        classifier in ONE place. A parallel ``save_registry_only`` writer
        would create drift risk. The ``mode`` parameter localizes the
        "skip ledger" branch to one ``if`` inside the existing transaction.

        Idempotency:
        - The ledger row uses ``INSERT OR REPLACE`` keyed on ``id`` (UUID
          generated by the runner) — re-firing the same intent on a lost
          response leaves a single row.
        - The registry row uses ``INSERT … ON CONFLICT (deployment_id, chain,
          primitive, physical_identity_hash) DO UPDATE`` with a strict
          monotone-priority guard on ``status`` (open=0, closed=1,
          reorg_invalidated=1; strict ``>`` so equal-priority terminal-state
          retries silently no-op the status field while still refreshing
          payload / closed_tx / closed_at_block / last_reconciled_at_block).
          See blueprint 28 §4.3 for the priority table.
        - The handle slot uses ``COALESCE(existing, EXCLUDED)`` so a retry
          that supplies a handle for a previously-unhandled row sets it on
          first conflict and a retry that omits the handle does not clear it.

        The handle is a column on ``position_registry`` (not a separate
        ``position_handles`` table — blueprint 28 §4.2). The ``handle``
        argument here is the typed shape; its value lands in the same
        ``handle`` column populated by the registry row's own ``handle``
        attribute. We honor an explicit ``handle.handle`` value over the
        registry row's ``handle`` only when the registry row left it as
        ``None`` (the Author API guidance is "set it on the registry row";
        the standalone ``HandleMapping`` is the legacy path and stays for
        forward-compat with the Postgres RPC shape).

        Failure contract: any ``sqlite3.Error`` (IntegrityError on CHECK
        violation, OperationalError on disk full, etc.) propagates with
        the connection rolled back. The caller (StateManager) wraps the
        exception in :class:`AccountingPersistenceError` so the runner's
        existing fail-closed pipeline (VIB-3157 / VIB-3762) handles it.
        """
        if not self._initialized:
            await self.initialize()

        # Resolve canonical string forms. The RegistryRow accessors validate
        # against the Primitive / AccountingCategory enums and raise on a typo
        # — better to fail at the value-resolution site than to land an
        # un-typed string in the DB.
        primitive_str = registry.primitive_value()
        category_str = registry.accounting_category_value()
        payload_json = registry.payload_json()
        # Effective handle: prefer registry.handle; fall back to the
        # standalone HandleMapping when present. Validation of the
        # (deployment_id, accounting_category) alignment happened in
        # commit.py:_validate_inputs.
        effective_handle = (
            registry.handle if registry.handle is not None else (handle.handle if handle is not None else None)
        )
        # Status priority for the monotone guard.
        # open=0; closed=1; reorg_invalidated=1.
        new_status_priority = 0 if registry.status == "open" else 1

        # T24 / VIB-4210: validate mode at the boundary so a typo lands
        # as a typed ValueError, not a silent fall-through to the default
        # branch. Only two values are valid; anything else is a bug.
        if mode not in ("commit", "registry_reconciliation"):
            raise ValueError(
                f"save_ledger_and_registry_atomic: invalid mode={mode!r}; "
                "expected 'commit' (default, write ledger+registry+handle) "
                "or 'registry_reconciliation' (skip ledger, write registry+handle only)."
            )
        _skip_ledger = mode == "registry_reconciliation"

        def _sync_atomic_commit() -> None:
            with self._db_lock:
                conn = self._conn
                assert conn is not None  # _initialized=True implies _conn set
                # Use IMMEDIATE so we acquire a RESERVED lock immediately.
                # The default DEFERRED would let two writers race up to the
                # first WRITE statement, which is the exact pattern we want
                # to avoid for the atomic primitive — we MUST hold the lock
                # for the entire ledger+registry+handle write.
                conn.execute("BEGIN IMMEDIATE")
                try:
                    # 1) Ledger row.
                    #
                    # T24 / VIB-4210: in mode='registry_reconciliation' the
                    # ledger insert is SKIPPED entirely. ADR §2.3 #1+#2
                    # forbids Reconcile from writing transaction_ledger —
                    # ledger is the immutable intent history, reconciliation
                    # is a recovery path that discovers chain-only positions
                    # (no corresponding intent ever existed). Synthesising
                    # a fake ledger row would pollute the audit trail and
                    # defeat the whole point of having a separate registry
                    # surface. The skip is localised to ONE branch inside
                    # the existing transaction (ADR §8.1 Option (c)) so
                    # there is still a single registry writer path.
                    if not _skip_ledger:
                        conn.execute(
                            """
                            INSERT OR REPLACE INTO transaction_ledger
                            (id, cycle_id, deployment_id, execution_mode,
                             timestamp, intent_type,
                             token_in, amount_in, token_out, amount_out,
                             effective_price, slippage_bps, gas_used, gas_usd,
                             tx_hash, chain, protocol, success, error,
                             extracted_data_json, price_inputs_json, pre_state_json, post_state_json)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                entry.id,
                                entry.cycle_id,
                                _canonical_deployment_id(entry),
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
                    # 2a) Handle reuse after close (AccountingStrats.md D3 /
                    # VIB-5051): a strategy that closes a handled position and
                    # later reopens the SAME logical slot (e.g. an LP
                    # rebalance) mints a NEW physical position, so the upsert's
                    # physical-identity conflict key does not fire and the
                    # plain INSERT would trip ``ix_registry_handle`` against
                    # the old TERMINAL row still holding the handle. Release
                    # the handle from terminal rows first — inside this same
                    # transaction — so the handle always points at the CURRENT
                    # physical position of the slot. A handle held by a row
                    # that is still OPEN is NOT released: that collision is a
                    # genuine strategy bug and must keep failing loud below.
                    # The physical-identity guard keeps an idempotent retry of
                    # the same row from clearing its own handle.
                    if effective_handle is not None:
                        conn.execute(
                            """
                            UPDATE position_registry
                            SET handle = NULL
                            WHERE deployment_id = ?
                              AND accounting_category = ?
                              AND handle = ?
                              AND status IN ('closed', 'reorg_invalidated')
                              AND physical_identity_hash != ?
                            """,
                            (
                                registry.deployment_id,
                                category_str,
                                effective_handle,
                                registry.physical_identity_hash,
                            ),
                        )
                    # 2) Registry row + handle column atomically.
                    #
                    # The ON CONFLICT clause's WHERE predicate enforces:
                    #   a) Strict monotone status: status_priority of EXCLUDED
                    #      must be > current row's. Equal terminal states
                    #      (closed vs reorg_invalidated, both priority 1)
                    #      do NOT overwrite each other.
                    #   b) Idempotent retries: same-status retries pass the
                    #      guard with strict > false, so the conflict
                    #      clause's UPDATE doesn't run — but the row stays
                    #      (DO NOTHING semantically for status).
                    #
                    # The CASE expression on `status` materializes the
                    # priority inline because SQLite has no shorthand for
                    # "lookup column priority via mapping." Mapping kept
                    # in lock-step with blueprint 28 §4.3.
                    conn.execute(
                        """
                        INSERT INTO position_registry
                        (deployment_id, chain, primitive, accounting_category,
                         physical_identity_hash, semantic_grouping_key, grouping_policy_version,
                         handle, status, payload,
                         opened_at_block, opened_tx, closed_at_block, closed_tx,
                         last_reconciled_at_block, matching_policy_version)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT (deployment_id, chain, primitive, physical_identity_hash)
                        DO UPDATE SET
                            status = excluded.status,
                            payload = excluded.payload,
                            handle = COALESCE(position_registry.handle, excluded.handle),
                            closed_at_block = COALESCE(excluded.closed_at_block, position_registry.closed_at_block),
                            closed_tx = COALESCE(excluded.closed_tx, position_registry.closed_tx),
                            last_reconciled_at_block = COALESCE(
                                excluded.last_reconciled_at_block, position_registry.last_reconciled_at_block
                            ),
                            grouping_policy_version = excluded.grouping_policy_version,
                            matching_policy_version = excluded.matching_policy_version,
                            semantic_grouping_key = excluded.semantic_grouping_key,
                            accounting_category = excluded.accounting_category
                        WHERE
                            (CASE excluded.status
                                WHEN 'open' THEN 0
                                WHEN 'closed' THEN 1
                                WHEN 'reorg_invalidated' THEN 1
                                ELSE -1
                            END)
                            >
                            (CASE position_registry.status
                                WHEN 'open' THEN 0
                                WHEN 'closed' THEN 1
                                WHEN 'reorg_invalidated' THEN 1
                                ELSE -1
                            END)
                        """,
                        (
                            registry.deployment_id,
                            registry.chain,
                            primitive_str,
                            category_str,
                            registry.physical_identity_hash,
                            registry.semantic_grouping_key,
                            registry.grouping_policy_version,
                            effective_handle,
                            registry.status,
                            payload_json,
                            registry.opened_at_block,
                            registry.opened_tx,
                            registry.closed_at_block,
                            registry.closed_tx,
                            registry.last_reconciled_at_block,
                            registry.matching_policy_version,
                        ),
                    )
                    # NOTE on `new_status_priority`: it's computed pre-tx for
                    # logging / debugging; the actual priority comparison
                    # happens inline in the SQL above so the decision is
                    # atomic with the row's existing status. We deliberately
                    # do NOT pre-fetch the existing status and decide in
                    # Python — that would race with concurrent writers and
                    # re-introduce the very atomicity gap this primitive
                    # closes.
                    _ = new_status_priority

                    # Same-status retry handle backfill (CodeRabbit PR #2207
                    # finding). The priority-gated WHERE clause above skips
                    # the entire DO UPDATE when status doesn't strictly
                    # increase, so a row landed with handle=NULL stays NULL
                    # forever even if a later same-status writer knows the
                    # handle. Run a separate, idempotent UPDATE that backfills
                    # ONLY when the existing handle is NULL — preserves the
                    # priority-rejected-retry contract for every other column
                    # (status, payload, anchors) since those are gated on
                    # status-priority increase, not on handle presence.
                    if effective_handle is not None:
                        conn.execute(
                            """
                            UPDATE position_registry
                            SET handle = ?
                            WHERE deployment_id = ?
                              AND chain = ?
                              AND primitive = ?
                              AND physical_identity_hash = ?
                              AND handle IS NULL
                            """,
                            (
                                effective_handle,
                                registry.deployment_id,
                                registry.chain,
                                primitive_str,
                                registry.physical_identity_hash,
                            ),
                        )
                    conn.commit()
                except sqlite3.IntegrityError as ie:
                    # Roll back the transaction FIRST so the DB is in a
                    # clean state regardless of how we classify the error.
                    # The rollback must happen before the row-fetch SELECT
                    # below so the SELECT cannot accidentally see rows in
                    # an uncommitted-pending state on this connection.
                    conn.rollback()

                    # Distinguish the auto-mode-collision case
                    # (ix_registry_auto_mode partial unique index) from
                    # other IntegrityErrors (CHECK violations, handle
                    # uniqueness via ix_registry_handle, NOT NULL, …).
                    #
                    # Detection contract (VIB-4200 / UAT card §D3.F1, F8,
                    # F10):
                    #   - The classifier is two-layered:
                    #     (1) The IntegrityError MESSAGE PREFIX
                    #         distinguishes the constraint TYPE: SQLite
                    #         emits "UNIQUE constraint failed: ..." for
                    #         unique-index violations and "CHECK constraint
                    #         failed: ..." for CHECK violations. The
                    #         constraint-type prefix IS reliable across
                    #         SQLite versions even when the constraint
                    #         NAME is not (UAT D3.F8). A CHECK violation
                    #         is NEVER a collision regardless of whether
                    #         a same-group row exists (UAT D3.F1
                    #         over-broad-classifier guard).
                    #     (2) Among UNIQUE-constraint violations, the
                    #         row-existence check on the partial-index
                    #         predicate distinguishes
                    #         ``ix_registry_auto_mode`` (auto-mode
                    #         collision) from ``ix_registry_handle``
                    #         (duplicate handle, UAT D3.F10) and from the
                    #         primary-key conflict (which the upstream
                    #         ON CONFLICT clause already handles, so it
                    #         shouldn't reach here).
                    #   - Pre-INSERT SELECT-then-INSERT is forbidden (UAT
                    #     D3.F7.b) because it races under concurrent
                    #     writers; the check happens here, post-INSERT,
                    #     post-rollback, in a fresh read.
                    # Detect UNIQUE-constraint violation in two layers
                    # (gemini-code-assist medium finding, PR #2222 review):
                    # the ``sqlite3`` module exposes the extended errorcode
                    # on ``IntegrityError.sqlite_errorcode`` (Python 3.11+,
                    # and this repo requires 3.12+) as the authoritative,
                    # locale- and SQLite-version-independent signal. The
                    # canonical constant is
                    # ``sqlite3.SQLITE_CONSTRAINT_UNIQUE`` (extended code
                    # 2067). The string-prefix fallback covers the
                    # (theoretical) case where ``sqlite_errorcode`` is
                    # missing because the underlying SQLite library was
                    # built without extended-errorcode support — falling
                    # back to ``False`` in that case would silently
                    # downgrade every UNIQUE violation to
                    # ``AccountingPersistenceError``, which is SAFE (errs
                    # on the generic-error side, NEVER silently swallows)
                    # but loses the typed-collision signal.
                    sqlite_errorcode = getattr(ie, "sqlite_errorcode", None)
                    if sqlite_errorcode is not None:
                        is_unique_violation = sqlite_errorcode == sqlite3.SQLITE_CONSTRAINT_UNIQUE
                    else:
                        is_unique_violation = "unique constraint failed" in str(ie).lower()
                    if not is_unique_violation:
                        # CHECK / NOT NULL / FOREIGN KEY etc. — never a
                        # collision. Re-raise so the caller wraps as
                        # AccountingPersistenceError.
                        raise

                    # CodeRabbit MAJOR finding (PR #2228 review): The
                    # partial unique index ``ix_registry_auto_mode`` is
                    # defined ``WHERE status = 'open' AND handle IS NULL``
                    # — it CANNOT fire on an INSERT whose row carries a
                    # handle. If the incoming row has a handle, the only
                    # plausible UNIQUE-constraint source is
                    # ``ix_registry_handle`` (duplicate handle, UAT D3.F10)
                    # — and the row-existence check below could otherwise
                    # mis-classify it as an auto-mode collision when an
                    # unrelated handle-less open row happens to occupy the
                    # same semantic group. Short-circuit here to preserve
                    # the AccountingPersistenceError surface for that case.
                    if effective_handle is not None:
                        raise

                    # UNIQUE-constraint violation. Run the row-existence
                    # check on the auto-mode partial-index predicate.
                    cursor = conn.execute(
                        """
                        SELECT physical_identity_hash, opened_tx
                        FROM position_registry
                        WHERE deployment_id = ?
                          AND chain = ?
                          AND accounting_category = ?
                          AND semantic_grouping_key = ?
                          AND status = 'open'
                          AND handle IS NULL
                        LIMIT 1
                        """,
                        (
                            registry.deployment_id,
                            registry.chain,
                            category_str,
                            registry.semantic_grouping_key,
                            # status='open' AND handle IS NULL are inlined
                            # because they MUST exactly mirror the partial
                            # unique index's WHERE clause (sqlite.py:677).
                        ),
                    )
                    existing = cursor.fetchone()

                    if existing is not None:
                        # The partial unique index group is occupied by an
                        # open handle-less row, and the IntegrityError is a
                        # UNIQUE-constraint violation — therefore the
                        # offending index IS ix_registry_auto_mode.
                        # Distinguish from the idempotent-retry path: if
                        # the incoming row's PIH equals the existing row's
                        # PIH, the upstream ON CONFLICT clause should have
                        # handled it (so we shouldn't be here), but guard
                        # defensively.
                        existing_pih, existing_tx = existing
                        new_pih = registry.physical_identity_hash
                        if existing_pih != new_pih:
                            # Auto-mode collision confirmed.
                            from ..registry_errors import (  # local import: keep state.exceptions module lean
                                RegistryAutoCollisionError,
                            )

                            raise RegistryAutoCollisionError(
                                semantic_grouping_key=registry.semantic_grouping_key,
                                existing_physical_identity_hash=existing_pih,
                                opened_tx=existing_tx or "",
                                accounting_category=category_str,
                            ) from ie

                    # UNIQUE violation but not the auto-mode collision —
                    # most likely ix_registry_handle (duplicate handle,
                    # UAT D3.F10). Re-raise so the caller wraps as
                    # AccountingPersistenceError.
                    raise
                except Exception:
                    # Roll back to leave the DB unchanged. Re-raise so the
                    # caller (StateManager) wraps in AccountingPersistenceError.
                    conn.rollback()
                    raise

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, _sync_atomic_commit)

    async def get_ledger_entries(
        self,
        deployment_id: str,
        since: datetime | None = None,
        intent_type: str | None = None,
        limit: int = 100,
        before: datetime | None = None,
    ) -> list["LedgerEntry"]:
        """Query transaction ledger entries.

        Args:
            deployment_id: Strategy to query.
            since: Only entries after this timestamp.
            intent_type: Filter by intent type.
            limit: Maximum entries to return.
            before: Only entries strictly older than this timestamp.

        Returns:
            List of LedgerEntry objects, newest first.
        """
        from almanak.framework.observability.ledger import LedgerEntry

        if not self._initialized:
            await self.initialize()

        def _sync_get() -> list[LedgerEntry]:
            # SQL column is the canonical `deployment_id` (blueprint 29 §3);
            # the method parameter is still named `deployment_id` (VIB-4726).
            conditions = ["deployment_id = ?"]
            params: list[Any] = [deployment_id]

            if since is not None:
                conditions.append("timestamp > ?")
                params.append(since.isoformat())
            if before is not None:
                conditions.append("timestamp < ?")
                params.append(before.isoformat())
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
                        deployment_id=row["deployment_id"],
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

    async def sum_ledger_gas_usd(
        self,
        deployment_id: str,
    ) -> Decimal:
        """Σ transaction_ledger.gas_usd for a deployment (VIB-4225 ACC-02).

        NULL / empty-string `gas_usd` rows coalesce to 0 inside the SUM (the
        empty-string case is the parser-didn't-emit signal — must not silently
        drop the row). Returns ``Decimal("0")`` on no rows.

        VIB-4722 collapsed the table to a single ``deployment_id`` SQL column.
        Reads filter that column directly; there is no legacy identity fallback.
        """
        if not self._initialized:
            await self.initialize()

        def _sync_sum() -> Decimal:
            # pr-auditor finding #1: SUM(CAST(... AS REAL)) routes Decimal-as-
            # TEXT through IEEE-754 double before re-wrapping in Decimal,
            # silently violating the lossless-precision invariant the rest
            # of the accounting stack maintains. Read raw rows and sum in
            # Python with Decimal — preserves measurement semantics, keeps
            # NULL/empty-as-zero coalescing (F5 pin), and stays well under
            # the F6 perf budget (10k rows in <100ms).
            with self._db_lock:
                cursor = self._conn.execute(  # type: ignore[union-attr]
                    """
                    SELECT gas_usd
                    FROM transaction_ledger
                    WHERE deployment_id = ?
                    """,
                    (deployment_id,),
                )
                rows = cursor.fetchall()
            total = Decimal("0")
            for row in rows:
                raw = row["gas_usd"]
                if raw is None or raw == "":
                    # F5 pin: NULL / empty-string rows coalesce to zero
                    # (parser-didn't-emit, not silent-drop signal). The row
                    # is counted as measured-but-zero; the row itself is
                    # preserved.
                    continue
                total += Decimal(str(raw))
            return total

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _sync_sum)

    async def get_ledger_quant_stats(self, deployment_id: str) -> "LedgerQuantStats":
        """SQL-side ledger aggregates for the dashboard quant tiles (VIB-5059).

        One aggregate statement, O(1) rows transferred, NO JSON-blob columns
        selected. Field semantics mirror the legacy per-row Python loops in
        ``quant_aggregations`` (counts of non-empty columns; exact-Decimal
        ``gas_usd`` sum via the ``almanak_decimal_sum`` custom aggregate —
        see :func:`_register_quant_sql_functions`).

        Zero-row semantics: counts → 0, sum → ``Decimal("0")``,
        ``first_action_wallet_value_usd`` stays ``None`` (the anchor is
        computed by the caller from :meth:`get_ledger_anchor_candidates`,
        never here).
        """
        from almanak.framework.observability.ledger import LedgerQuantStats

        if not self._initialized:
            await self.initialize()

        def _sync_get() -> LedgerQuantStats:
            with self._db_lock:
                cursor = self._conn.execute(  # type: ignore[union-attr]
                    """
                    SELECT
                        COUNT(*) AS total,
                        COUNT(CASE WHEN tx_hash IS NOT NULL AND tx_hash != ''
                                   THEN 1 END) AS with_tx_hash,
                        COUNT(CASE WHEN cycle_id IS NOT NULL AND cycle_id != ''
                                   THEN 1 END) AS with_cycle_id,
                        COUNT(CASE WHEN price_inputs_json IS NOT NULL
                                    AND price_inputs_json != ''
                                   THEN 1 END) AS with_price_inputs,
                        COUNT(CASE WHEN pre_state_json IS NOT NULL
                                    AND pre_state_json != ''
                                    AND post_state_json IS NOT NULL
                                    AND post_state_json != ''
                                   THEN 1 END) AS with_pre_post_state,
                        COUNT(CASE WHEN almanak_decimal_positive(gas_usd) = 1
                                   THEN 1 END) AS with_positive_gas_usd,
                        almanak_decimal_sum(gas_usd) AS gas_usd_sum
                    FROM transaction_ledger
                    WHERE deployment_id = ?
                    """,
                    (deployment_id,),
                )
                row = cursor.fetchone()
            if row is None:
                return LedgerQuantStats()
            return LedgerQuantStats(
                total=row["total"] or 0,
                with_tx_hash=row["with_tx_hash"] or 0,
                with_cycle_id=row["with_cycle_id"] or 0,
                with_price_inputs=row["with_price_inputs"] or 0,
                with_pre_post_state=row["with_pre_post_state"] or 0,
                with_positive_gas_usd=row["with_positive_gas_usd"] or 0,
                gas_usd_sum=Decimal(str(row["gas_usd_sum"] or "0")),
            )

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _sync_get)

    async def get_ledger_anchor_candidates(
        self,
        deployment_id: str,
        limit: int = 64,
        offset: int = 0,
    ) -> list["LedgerEntry"]:
        """Oldest-first ledger rows that can anchor "Deployed" (VIB-5059).

        Returns the ``limit`` earliest rows (after ``offset``) that carry BOTH
        ``pre_state_json`` and ``price_inputs_json`` — the only rows the
        VIB-3914 first-action wallet anchor can be computed from. This is the
        ONE quant-load statement allowed to read those JSON columns, and it is
        always LIMIT-bounded: the caller walks batches until one row yields a
        usable anchor (normally the very first batch).

        Rows are projected to ``id`` plus the three columns the anchor walk
        reads; the returned ``LedgerEntry`` objects carry the persisted row
        identity and defaults everywhere else. Identical-timestamp rows are
        returned lower-id first (``id ASC``) — an intentional tiebreak: the
        legacy in-memory stable sort over a DESC fetch inspected the
        higher-id row first; lower id (truly written first) is the more
        correct "first action".
        """
        from almanak.framework.observability.ledger import LedgerEntry

        if limit <= 0:
            return []
        if not self._initialized:
            await self.initialize()

        def _sync_get() -> list[LedgerEntry]:
            with self._db_lock:
                cursor = self._conn.execute(  # type: ignore[union-attr]
                    """
                    SELECT id, timestamp, pre_state_json, price_inputs_json
                    FROM transaction_ledger
                    WHERE deployment_id = ?
                      AND pre_state_json IS NOT NULL AND pre_state_json != ''
                      AND price_inputs_json IS NOT NULL AND price_inputs_json != ''
                    ORDER BY timestamp ASC, id ASC
                    LIMIT ? OFFSET ?
                    """,
                    (deployment_id, limit, offset),
                )
                rows = cursor.fetchall()
            return [
                LedgerEntry(
                    id=str(row["id"]),
                    deployment_id=deployment_id,
                    timestamp=datetime.fromisoformat(row["timestamp"]),
                    pre_state_json=row["pre_state_json"] or "",
                    price_inputs_json=row["price_inputs_json"] or "",
                )
                for row in rows
            ]

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _sync_get)

    # -------------------------------------------------------------------------
    # Typed accounting events (VIB-3417)
    # -------------------------------------------------------------------------

    async def save_accounting_event(self, event: Any) -> bool:
        """Persist a typed accounting event (LendingAccountingEvent, PendleAccountingEvent, etc.).

        G13 stamp + L1/L4 alias projection (Accounting-AttemptNo17 §A4) —
        scoped to the SQLite/local persistence path: every accounting write
        that lands in the local SDK SQLite backend is augmented here, so the
        local stamp + alias-projection is per-backend rather than per-caller.
        The hosted (gateway) path performs the same augmentation in
        ``GatewayStateManager.save_accounting_event`` before the gRPC hop —
        the projection is therefore *single-point per backend*, not single-
        point across the whole writer surface (the SQLite backend cannot
        observe writes that go straight through the gateway, and vice versa).
        """
        if not self._initialized:
            await self.initialize()

        identity = event.identity
        # Augment payload with version stamps + lending aliases at the
        # last possible moment, regardless of who called us. Mode-aware
        # error contract (VIB-3863): live raises AccountingPersistenceError
        # on a malformed payload so the runner halts; paper/dry-run logs
        # ERROR and pass-throughs so the loop keeps moving.
        from ...accounting.writer import augment_accounting_payload

        is_live = getattr(identity, "execution_mode", "") == "live"
        # VIB-4278: build a registry_lookup callable bound to this event's
        # identity context so the augment chokepoint can stamp
        # `source="registry"` on the position_reference shape when the
        # position_registry has a matching row. The lookup runs inside the
        # same _db_lock-held connection used for the INSERT below — see the
        # design note in `_build_registry_lookup_for_event`.
        raw_event_payload = event.to_payload_json()

        def _sync_save() -> bool:
            with self._db_lock:
                registry_lookup = self._build_registry_lookup_for_event(
                    deployment_id=identity.deployment_id,
                    chain=identity.chain,
                    tx_hash=identity.tx_hash,
                )
                payload_json = augment_accounting_payload(
                    raw_event_payload,
                    is_live=is_live,
                    registry_lookup=registry_lookup,
                )
                # VIB-4196 / T10: extract `position_reference` (when present)
                # out of the augmented payload and persist it to the
                # dedicated column. The column is a denormalized
                # query-convenience copy — payload_json remains the canonical
                # source. The augment chokepoint only emits the key for
                # OPEN/CLOSE rows with a known event_type; non-OPEN/CLOSE
                # rows + unknown-event-type fallback rows leave it NULL.
                position_reference = _extract_position_reference_column(payload_json)

                self._conn.execute(  # type: ignore[union-attr]
                    """
                    INSERT OR REPLACE INTO accounting_events
                    (id, deployment_id, cycle_id, execution_mode,
                     timestamp, chain, protocol, wallet_address, event_type, position_key,
                     ledger_entry_id, tx_hash, confidence, payload_json, schema_version,
                     position_reference)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        identity.id,
                        identity.deployment_id,
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
                        payload_json,
                        event.schema_version,
                        position_reference,
                    ),
                )
                self._conn.commit()  # type: ignore[union-attr]
            return True

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _sync_save)

    def _build_registry_lookup_for_event(
        self,
        *,
        deployment_id: str,
        chain: str,
        tx_hash: str,
    ) -> "Callable[[str, str, str], dict | None] | None":
        """Build a ``RegistryLookup`` callable for an accounting event (VIB-4278).

        The augment chokepoint calls the returned callable with
        ``(primitive, event_kind, accounting_category)`` and expects a
        ``position_registry`` row dict back when a row matches the event by:

        * ``deployment_id`` equals the event's deployment_id, AND
        * ``primitive`` equals the canonical ``Primitive`` value resolved by
          the chokepoint, AND
        * ``accounting_category`` equals the canonical ``AccountingCategory``
          value resolved by the chokepoint (multiple categories can share a
          primitive — e.g. UniV3 ``"lp"`` and Pendle ``"pendle_lp"`` both
          have ``Primitive="lp"`` — so the join key needs the category to
          disambiguate batched transactions), AND
        * for OPEN events: ``opened_tx`` equals the event's ``tx_hash``, OR
        * for CLOSE events: ``closed_tx`` equals the event's ``tx_hash``.

        The lookup is read-only — it never mutates ``position_registry``. It
        returns ``None`` when no row matches (registry-mode is opt-in per
        blueprint 28 §5; legacy primitives don't write to the registry and
        their accounting events keep ``source="legacy"``).

        Returns ``None`` (i.e. "do not pass a lookup callable") when the
        event's tx_hash is empty / falsy — without a tx_hash there is nothing
        to match on, and we should fall through to the legacy reference.

        The returned callable assumes the caller already holds ``_db_lock``
        (it does — both callers — the chokepoint and the INSERT — run inside
        the ``with self._db_lock:`` block in :meth:`save_accounting_event`).
        """
        if not tx_hash:
            # No tx_hash → no match possible. Legacy path keeps emitting
            # ``source="legacy"`` and the event lands cleanly.
            return None

        # Normalize the event's tx_hash to lowercase once. The registry
        # stores ``opened_tx`` / ``closed_tx`` as supplied by the runner /
        # backfill (mixed-case is possible on EVM chains — checksum-cased
        # addresses, mixed-case hashes from some RPCs), but accounting
        # event ``tx_hash`` can arrive in either case. A case-sensitive
        # ``= ?`` comparison silently misses the row and stamps
        # ``source="legacy"`` — see CodeRabbit review on PR #2236. We
        # normalise on BOTH sides: lowercase the bind param here and wrap
        # the column in ``LOWER(...)`` in the WHERE predicate. The
        # expression indexes ``ix_registry_opened_tx_lookup`` /
        # ``ix_registry_closed_tx_lookup`` defined alongside the table
        # are built on ``LOWER(opened_tx)`` / ``LOWER(closed_tx)``, so
        # the WHERE remains indexable.
        normalized_tx_hash = tx_hash.lower()
        conn = self._conn
        if conn is None:
            return None

        # Columns selected from position_registry. Pulled out so the
        # SELECT list stays in lock-step with what callers expect when
        # they read keys off the returned dict
        # (``build_registry_position_reference`` in
        # ``accounting/position_reference.py``).
        _REGISTRY_LOOKUP_COLS = (
            "physical_identity_hash",
            "semantic_grouping_key",
            "grouping_policy_version",
            "handle",
            "matching_policy_version",
            "status",
            "accounting_category",
        )

        def _lookup(primitive: str, event_kind: str, accounting_category: str) -> dict | None:
            # OPEN events match opened_tx; CLOSE events match closed_tx.
            # Filter by chain too so a tx_hash collision across forks
            # (rare but possible with Anvil snapshots) cannot return the
            # wrong row.
            if event_kind == "open":
                tx_col = "opened_tx"
            elif event_kind == "close":
                tx_col = "closed_tx"
            else:
                # _resolve_position_reference only calls us for OPEN/CLOSE;
                # any other kind is a chokepoint bug. Fall through to
                # legacy rather than raise (registry-mode is opt-in).
                return None

            # The ``{tx_col} IS NOT NULL`` predicate is technically
            # redundant (``LOWER(NULL) = ?`` is false), but it lets
            # SQLite's planner pick the partial index
            # ``ix_registry_opened_tx_lookup`` /
            # ``ix_registry_closed_tx_lookup`` (declared
            # ``WHERE opened_tx IS NOT NULL``). Without the predicate the
            # planner falls back to a table scan once the table is
            # ANALYZE'd — verified locally on SQLite 3.49.
            #
            # ``accounting_category = ?`` was added in PR #2236 round 2
            # (CodeRabbit). Multiple AccountingCategory values can share
            # the same Primitive — UniV3 ``"lp"`` and Pendle
            # ``"pendle_lp"`` both have ``Primitive="lp"`` — so a tx
            # that touches positions in different categories would
            # otherwise return multiple rows here, and stamping the
            # wrong category's ``physical_identity_hash`` / ``handle``
            # onto an accounting event loses the L5_22 join key
            # silently.
            sql = (
                f"SELECT {', '.join(_REGISTRY_LOOKUP_COLS)} "
                f"FROM position_registry "
                f"WHERE deployment_id = ? AND chain = ? AND primitive = ? "
                f"AND accounting_category = ? "
                f"AND {tx_col} IS NOT NULL "
                f"AND LOWER({tx_col}) = ? "
                f"ORDER BY physical_identity_hash ASC"
            )
            cursor = conn.execute(
                sql,
                (
                    deployment_id,
                    chain,
                    primitive,
                    accounting_category,
                    normalized_tx_hash,
                ),
            )
            rows = cursor.fetchall()
            if not rows:
                return None
            if len(rows) > 1:
                # Ambiguity safeguard (CodeRabbit PR #2236 round 2): even
                # with ``accounting_category`` in the join key, a single
                # tx that opens multiple positions in the same
                # (primitive, category) — e.g. a batched lp_open that
                # mints two UniV3 NFTs in one transaction — would still
                # land multiple rows here. Picking the first row by
                # ``physical_identity_hash ASC`` would stamp ONE
                # position's identity onto BOTH accounting events,
                # silently losing the L5_22 join key. Return None
                # instead: the augment chokepoint falls through to the
                # legacy reference (null identity fields), which
                # preserves the "Empty ≠ Zero" contract — better to
                # admit "unmeasured" than to stamp a wrong hash.
                #
                # The durable fix is to thread ``registry_handle`` (when
                # the strategy supplies one) into the lookup key so each
                # leg of a multi-position tx joins to its own row; that
                # is the follow-up tracked separately.
                logger.warning(
                    "registry_lookup: %d rows matched for "
                    "(deployment_id=%s, chain=%s, primitive=%s, "
                    "accounting_category=%s, %s=%s); falling back to "
                    "legacy until the lookup is disambiguated by "
                    "registry_handle (multi-position-in-one-tx).",
                    len(rows),
                    deployment_id,
                    chain,
                    primitive,
                    accounting_category,
                    tx_col,
                    normalized_tx_hash,
                )
                return None
            return dict(rows[0])

        return _lookup

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
            ORDER BY timestamp ASC, rowid ASC
        """
        # rowid tiebreak: FIFO lot replay (VIB-5057) assumes BUY precedes
        # SELL; two events sharing an identical ISO timestamp must replay in
        # insertion order, not scan order.
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

    # -------------------------------------------------------------------------
    # Accounting outbox (VIB-3467) — drained by AccountingProcessor
    # -------------------------------------------------------------------------

    async def save_outbox_entry(
        self,
        outbox_id: str,
        deployment_id: str,
        cycle_id: str,
        ledger_entry_id: str,
        intent_type: str,
        wallet_address: str,
        position_key: str,
        market_id: str,
        created_at: str,
    ) -> None:
        """Write one row to accounting_outbox.  Called from the execution hot path via write_outbox_entry."""
        if not self._initialized:
            await self.initialize()
        # Capture all args for the inner closure. VIB-4722 collapsed
        # accounting_outbox to a single canonical `deployment_id` column
        # (the dead `deployment_id` column was dropped), so the `deployment_id`
        # method parameter (kept for the signature — VIB-4726) is no longer
        # captured.
        _outbox_id, _dep_id, _cycle_id = outbox_id, deployment_id, cycle_id
        _led_id, _intent, _wallet, _pos, _mkt = ledger_entry_id, intent_type, wallet_address, position_key, market_id
        _created = created_at

        def _sync() -> None:
            if not self._conn:
                return
            with self._db_lock:
                self._conn.execute(
                    """
                    INSERT OR IGNORE INTO accounting_outbox
                    (id, deployment_id, cycle_id, ledger_entry_id,
                     intent_type, wallet_address, position_key, market_id,
                     status, attempts, error, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', 0, '', ?, ?)
                    """,
                    (
                        _outbox_id,
                        _dep_id,
                        _cycle_id,
                        _led_id,
                        _intent,
                        _wallet,
                        _pos,
                        _mkt,
                        _created,
                        _created,
                    ),
                )
                self._conn.commit()

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, _sync)

    async def get_outbox_by_ledger_id(self, ledger_entry_id: str) -> dict | None:
        """Return the outbox row for the given ledger_entry_id, or None."""
        if not self._initialized:
            await self.initialize()

        def _sync() -> dict | None:
            if not self._conn:
                return None
            with self._db_lock:
                cursor = self._conn.execute(
                    "SELECT * FROM accounting_outbox WHERE ledger_entry_id = ? LIMIT 1",
                    (ledger_entry_id,),
                )
                row = cursor.fetchone()
            return dict(row) if row else None

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _sync)

    async def get_outbox_pending(self, deployment_id: str, max_retries: int = 3) -> list[dict]:
        """Return pending/failed (and any stuck 'processing') outbox rows eligible for drain.

        'processing' rows are included so that entries that were in-flight when the runner
        crashed are retried on restart rather than being permanently orphaned.
        """
        if not self._initialized:
            await self.initialize()

        def _sync() -> list[dict]:
            if not self._conn:
                return []
            with self._db_lock:
                cursor = self._conn.execute(
                    """
                    SELECT * FROM accounting_outbox
                    WHERE deployment_id = ?
                      AND status IN ('pending', 'failed', 'processing')
                      AND attempts < ?
                    ORDER BY created_at ASC
                    """,
                    (deployment_id, max_retries),
                )
                rows = cursor.fetchall()
            return [dict(r) for r in rows]

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _sync)

    async def update_outbox_entry(
        self,
        outbox_id: str,
        status: str,
        error: str = "",
        attempts: int | None = None,
    ) -> None:
        """Update the status (and optionally attempts) of an outbox row."""
        if not self._initialized:
            await self.initialize()

        def _sync() -> None:
            if not self._conn:
                return
            now = datetime.now(UTC).isoformat()
            with self._db_lock:
                if attempts is not None:
                    self._conn.execute(
                        "UPDATE accounting_outbox SET status=?, error=?, attempts=?, updated_at=? WHERE id=?",
                        (status, error, attempts, now, outbox_id),
                    )
                else:
                    self._conn.execute(
                        "UPDATE accounting_outbox SET status=?, error=?, updated_at=? WHERE id=?",
                        (status, error, now, outbox_id),
                    )
                self._conn.commit()

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, _sync)

    async def has_accounting_events_for_ledger(self, ledger_entry_id: str) -> bool:
        """Return True if accounting_events already has a row for ledger_entry_id."""
        if not self._initialized:
            await self.initialize()

        def _sync() -> bool:
            if not self._conn:
                return False
            with self._db_lock:
                cursor = self._conn.execute(
                    "SELECT COUNT(*) FROM accounting_events WHERE ledger_entry_id = ?",
                    (ledger_entry_id,),
                )
                count = cursor.fetchone()[0]
            return count > 0

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _sync)

    async def get_ledger_entry_by_id(self, ledger_entry_id: str) -> dict | None:
        """Return the full transaction_ledger row for the given id, or None."""
        if not self._initialized:
            await self.initialize()

        def _sync() -> dict | None:
            if not self._conn:
                return None
            with self._db_lock:
                cursor = self._conn.execute(
                    "SELECT * FROM transaction_ledger WHERE id = ? LIMIT 1",
                    (ledger_entry_id,),
                )
                row = cursor.fetchone()
            return dict(row) if row else None

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _sync)

    # =========================================================================
    # Position-registry CRUD + migration_state CRUD — VIB-4198 / T12
    # =========================================================================
    #
    # Per blueprint 28 §4.1 / cutover spec §2.1 / §3 — these accessors are
    # the SDK-owned local-SQLite implementation of the position_registry +
    # migration_state surfaces. Hosted Postgres equivalents land in T19
    # (VIB-4205) via the metrics-database repo.
    #
    # The atomic primitive `save_ledger_and_registry_atomic` (T11, above)
    # is the production write path for runtime LP_OPEN / LP_CLOSE. The
    # backfill READ path uses `insert_position_registry_row_if_absent`
    # (`INSERT … ON CONFLICT DO NOTHING`) so the backfill is idempotent
    # under restart per cutover spec §3.4.

    async def get_position_registry_open_rows(
        self,
        deployment_id: str,
        *,
        chain: str | None = None,
        primitive: str | None = None,
        accounting_category: str | None = None,
    ) -> list[dict]:
        """Return open ``position_registry`` rows for a deployment.

        The runtime "is this open?" answer surface for cutover-flipped
        primitives. The runner / teardown call this after cutover instead
        of consulting LPPositionTracker / position_events.

        Filters: ``status='open'`` always; chain / primitive /
        accounting_category narrow the set when supplied. The payload
        column is returned as the parsed dict (caller does not need to
        re-deserialize).
        """
        if not self._initialized:
            await self.initialize()

        def _sync() -> list[dict]:
            if not self._conn:
                return []
            sql = "SELECT * FROM position_registry WHERE deployment_id = ? AND status = 'open'"
            params: list[Any] = [deployment_id]
            if chain is not None:
                sql += " AND chain = ?"
                params.append(chain)
            if primitive is not None:
                sql += " AND primitive = ?"
                params.append(primitive)
            if accounting_category is not None:
                sql += " AND accounting_category = ?"
                params.append(accounting_category)
            sql += " ORDER BY opened_at_block ASC, opened_tx ASC"
            with self._db_lock:
                cursor = self._conn.execute(sql, params)
                rows = cursor.fetchall()
            out: list[dict] = []
            for row in rows:
                d = dict(row)
                payload_raw = d.get("payload") or "{}"
                try:
                    parsed = json.loads(payload_raw)
                except (TypeError, ValueError) as exc:
                    # A corrupt payload row stays opaque rather than tripping
                    # the iterator, but we surface a WARNING + structured
                    # diagnostic field so corruption is visible to operators
                    # rather than silently degrading to {}.
                    logger.warning(
                        "position_registry.payload JSON decode failed for "
                        "deployment_id=%s chain=%s primitive=%s "
                        "physical_identity_hash=%s: %s",
                        d.get("deployment_id"),
                        d.get("chain"),
                        d.get("primitive"),
                        d.get("physical_identity_hash"),
                        exc,
                    )
                    d["payload_raw"] = payload_raw
                    d["payload_decode_error"] = str(exc)
                    d["payload"] = {}
                else:
                    # Audit m5 (CodeRabbit): the accessor's contract is
                    # "parsed dict". ``json.loads`` accepts arrays /
                    # strings / numbers too — those would slip through
                    # the decode-error guard above and break callers
                    # that do ``payload.get(...)``. Normalize non-dict
                    # JSON to ``{}`` and surface a diagnostic field so
                    # malformed-by-shape rows are observable.
                    if isinstance(parsed, dict):
                        d["payload"] = parsed
                    else:
                        logger.warning(
                            "position_registry.payload is not a JSON "
                            "object (got %s) for deployment_id=%s chain=%s "
                            "primitive=%s physical_identity_hash=%s — "
                            "coercing to {}.",
                            type(parsed).__name__,
                            d.get("deployment_id"),
                            d.get("chain"),
                            d.get("primitive"),
                            d.get("physical_identity_hash"),
                        )
                        d["payload_raw"] = payload_raw
                        d["payload_shape_error"] = f"expected JSON object, got {type(parsed).__name__}"
                        d["payload"] = {}
                out.append(d)
            return out

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _sync)

    async def insert_position_registry_row_if_absent(self, *, row: Any) -> bool:
        """Backfill insert: add a registry row only if absent.

        Used by :class:`almanak.framework.migration.BackfillReader` per
        cutover spec §3.4. Uses ``INSERT … ON CONFLICT DO NOTHING`` keyed
        on ``(deployment_id, chain, primitive, physical_identity_hash)``
        so a SIGKILL-and-restart of the backfill leaves the existing row
        untouched. Runtime status flips on CLOSE go through the live
        atomic primitive (`save_ledger_and_registry_atomic`), NOT this
        path — the backfill is observation, not mutation.

        Args:
            row: A ``RegistryRow`` (see :mod:`almanak.framework.accounting.commit`).

        Returns:
            ``True`` if a new row was inserted; ``False`` if the row
            already existed.
        """
        if not self._initialized:
            await self.initialize()

        primitive_str = row.primitive_value()
        category_str = row.accounting_category_value()
        payload_json = row.payload_json()

        def _sync() -> bool:
            if not self._conn:
                return False
            with self._db_lock:
                cursor = self._conn.execute(
                    """
                    INSERT INTO position_registry
                    (deployment_id, chain, primitive, accounting_category,
                     physical_identity_hash, semantic_grouping_key, grouping_policy_version,
                     handle, status, payload,
                     opened_at_block, opened_tx, closed_at_block, closed_tx,
                     last_reconciled_at_block, matching_policy_version)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (deployment_id, chain, primitive, physical_identity_hash)
                    DO NOTHING
                    """,
                    (
                        row.deployment_id,
                        row.chain,
                        primitive_str,
                        category_str,
                        row.physical_identity_hash,
                        row.semantic_grouping_key,
                        row.grouping_policy_version,
                        row.handle,
                        row.status,
                        payload_json,
                        row.opened_at_block,
                        row.opened_tx,
                        row.closed_at_block,
                        row.closed_tx,
                        row.last_reconciled_at_block,
                        row.matching_policy_version,
                    ),
                )
                self._conn.commit()
                return cursor.rowcount > 0

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _sync)

    async def upsert_migration_state(
        self,
        *,
        deployment_id: str,
        primitive: str,
        cutover_key: str,
    ) -> None:
        """Idempotent insert of a ``migration_state`` row at the deploy-time
        baseline (``complete=0``).

        Cutover spec §2.1 says the cutover ticket creates the row at
        deploy time. T12 (this PR) creates it lazily on the runner's
        first start when the registry-mode dispatch for UniV3 LP is
        active. This is functionally equivalent to "deploy time" for
        local SDK; hosted (T19) does the same lazy create on first
        gateway startup.
        """
        if not self._initialized:
            await self.initialize()

        now = datetime.now(UTC).isoformat()

        def _sync() -> None:
            if not self._conn:
                return
            with self._db_lock:
                self._conn.execute(
                    """
                    INSERT INTO migration_state
                    (deployment_id, primitive, cutover_key,
                     position_registry_backfill_complete, backfill_source_table,
                     backfill_reader_version, rows_synthesized,
                     rows_skipped_already_present, notes, created_at, updated_at)
                    VALUES (?, ?, ?, 0, 'position_events', 1, 0, 0, '{}', ?, ?)
                    ON CONFLICT (deployment_id, primitive, cutover_key)
                    DO NOTHING
                    """,
                    (deployment_id, primitive, cutover_key, now, now),
                )
                self._conn.commit()

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, _sync)

    async def get_migration_state(
        self,
        *,
        deployment_id: str,
        primitive: str,
        cutover_key: str,
    ) -> Any | None:
        """Return the migration_state row as a parsed dataclass, or None.

        Returns a :class:`almanak.framework.migration.MigrationStateRow`
        with ``notes`` parsed as ``dict``. ``None`` indicates the row has
        not been created yet — the caller (boot guard) raises
        :class:`RegistryCutoverNotDeployedError` per cutover spec §2.2.
        """
        if not self._initialized:
            await self.initialize()

        def _sync() -> dict | None:
            if not self._conn:
                return None
            with self._db_lock:
                cursor = self._conn.execute(
                    """
                    SELECT * FROM migration_state
                    WHERE deployment_id = ? AND primitive = ? AND cutover_key = ?
                    """,
                    (deployment_id, primitive, cutover_key),
                )
                row = cursor.fetchone()
            return dict(row) if row else None

        loop = asyncio.get_event_loop()
        raw = await loop.run_in_executor(None, _sync)
        if raw is None:
            return None
        from almanak.framework.migration.backfill import MigrationStateRow

        try:
            notes = json.loads(raw.get("notes") or "{}")
        except (TypeError, ValueError):
            notes = {}
        # Defensive: ``notes`` is contractually an object on the
        # migration_state JSON column, but ``json.loads`` accepts arrays /
        # strings / numbers too. ``MigrationStateRow.notes`` is typed
        # ``dict[str, Any]``, so coerce non-dict shapes back to {} rather
        # than letting a malformed row break downstream consumers.
        if not isinstance(notes, dict):
            notes = {}
        return MigrationStateRow(
            deployment_id=raw["deployment_id"],
            primitive=raw["primitive"],
            cutover_key=raw["cutover_key"],
            position_registry_backfill_complete=bool(raw.get("position_registry_backfill_complete", 0)),
            backfill_started_at=raw.get("backfill_started_at"),
            backfill_completed_at=raw.get("backfill_completed_at"),
            backfill_source_table=raw.get("backfill_source_table") or "position_events",
            backfill_reader_version=int(raw.get("backfill_reader_version") or 1),
            rows_synthesized=int(raw.get("rows_synthesized") or 0),
            rows_skipped_already_present=int(raw.get("rows_skipped_already_present") or 0),
            notes=notes,
            created_at=raw.get("created_at") or "",
            updated_at=raw.get("updated_at") or "",
        )

    async def update_migration_state(
        self,
        *,
        deployment_id: str,
        primitive: str,
        cutover_key: str,
        backfill_started_at: str | None = None,
        rows_synthesized: int | None = None,
        rows_skipped_already_present: int | None = None,
    ) -> None:
        """Partial update — only the supplied columns are written.

        ``mark_backfill_complete`` is the dedicated terminal flip; this
        method handles in-flight progress writes (start time, batch
        counter checkpoints).
        """
        if not self._initialized:
            await self.initialize()

        sets: list[str] = []
        params: list[Any] = []
        if backfill_started_at is not None:
            sets.append("backfill_started_at = ?")
            params.append(backfill_started_at)
        if rows_synthesized is not None:
            sets.append("rows_synthesized = ?")
            params.append(rows_synthesized)
        if rows_skipped_already_present is not None:
            sets.append("rows_skipped_already_present = ?")
            params.append(rows_skipped_already_present)
        if not sets:
            return
        sets.append("updated_at = ?")
        params.append(datetime.now(UTC).isoformat())
        params.extend([deployment_id, primitive, cutover_key])

        def _sync() -> None:
            if not self._conn:
                return
            with self._db_lock:
                self._conn.execute(
                    f"""
                    UPDATE migration_state
                    SET {", ".join(sets)}
                    WHERE deployment_id = ? AND primitive = ? AND cutover_key = ?
                    """,
                    params,
                )
                self._conn.commit()

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, _sync)

    async def mark_backfill_complete(
        self,
        *,
        deployment_id: str,
        primitive: str,
        cutover_key: str,
        rows_synthesized: int,
        rows_skipped_already_present: int,
        backfill_completed_at: str,
    ) -> None:
        """Terminal flip — set ``complete=1`` + final counters + completed_at.

        Single statement so the flip is atomic at the SQLite-page level.
        Per cutover spec §3.3 step 4 — only invoked AFTER the read cursor
        is exhausted; a failure mid-loop leaves the flag at 0 and the
        next start re-runs the (idempotent) loop.
        """
        if not self._initialized:
            await self.initialize()

        now = datetime.now(UTC).isoformat()

        def _sync() -> None:
            if not self._conn:
                return
            with self._db_lock:
                self._conn.execute(
                    """
                    UPDATE migration_state
                    SET position_registry_backfill_complete = 1,
                        rows_synthesized = ?,
                        rows_skipped_already_present = ?,
                        backfill_completed_at = ?,
                        updated_at = ?
                    WHERE deployment_id = ? AND primitive = ? AND cutover_key = ?
                    """,
                    (
                        rows_synthesized,
                        rows_skipped_already_present,
                        backfill_completed_at,
                        now,
                        deployment_id,
                        primitive,
                        cutover_key,
                    ),
                )
                self._conn.commit()

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, _sync)

    async def get_position_events_filtered(
        self,
        *,
        deployment_id: str,
        position_types: frozenset[str],
    ) -> list[dict]:
        """Read all ``position_events`` rows for a deployment matching one
        of the legacy ``position_type`` values in ``position_types``.

        Used by the backfill — it streams all events for the deployment
        and groups by ``position_id`` in Python (memory-bound by the
        largest single position's history, typically <100 rows). Returns
        the rows in ``(position_id ASC, timestamp ASC, id ASC)`` order
        so the downstream group-by-position_id is stable even though
        the fold is order-independent (cutover spec §3.5). Audit P3
        (CodeRabbit): the ``id ASC`` tiebreaker makes the read order
        fully deterministic when two rows share a timestamp — without
        it, ``fold_position_events_for_univ3`` could pick a different
        first OPEN / last CLOSE across restarts on the same dataset.
        """
        if not self._initialized:
            await self.initialize()
        if not position_types:
            return []
        placeholders = ",".join("?" for _ in position_types)
        params: list[Any] = [deployment_id, *position_types]

        def _sync() -> list[dict]:
            if not self._conn:
                return []
            with self._db_lock:
                cursor = self._conn.execute(
                    f"""
                    SELECT * FROM position_events
                    WHERE deployment_id = ?
                      AND position_type IN ({placeholders})
                    ORDER BY position_id ASC, timestamp ASC, id ASC
                    """,
                    params,
                )
                return [dict(r) for r in cursor.fetchall()]

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _sync)
