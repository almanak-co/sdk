"""Regression tests for the 7-point reconstructability contract (VIB-2839).

Phase 4 accounting changes added deployment_id, cycle_id, execution_mode,
and is_complete across the persistence layer.  These tests verify that
every field round-trips through real SQLite storage and that migrations
upgrade old schemas.

The 7-point contract:
  1. Positions existed       -> positions_json is non-empty
  2. Total value was known   -> total_value_usd is non-zero
  3. Delta is computable     -> consecutive snapshots differ
  4. What tx caused it       -> ledger entry with matching cycle_id
  5. Costs were recorded     -> gas_usd and slippage_bps populated
  6. Record is complete?     -> is_complete flag
  7. Identity and mode       -> deployment_id + execution_mode present
"""

import json
import sqlite3
from datetime import UTC, datetime
from decimal import Decimal

import pytest
import pytest_asyncio

from almanak.framework.observability.ledger import LedgerEntry
from almanak.framework.observability.position_events import PositionEvent
from almanak.framework.portfolio.models import (
    PortfolioMetrics,
    PortfolioSnapshot,
    PositionValue,
    ValueConfidence,
)
from almanak.framework.state.backends.sqlite import SQLiteConfig, SQLiteStore
from almanak.framework.teardown.models import PositionType


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def store():
    """Create an in-memory SQLite store with full schema."""
    s = SQLiteStore(SQLiteConfig(db_path=":memory:"))
    await s.initialize()
    return s


# Shared test constants
STRATEGY_ID = "TestStrat"
DEPLOYMENT_ID = "TestStrat:a1b2c3d4"
CYCLE_ID = "cycle-001"
EXECUTION_MODE = "paper"
NOW = datetime(2026, 4, 14, 12, 0, 0, tzinfo=UTC)


# ---------------------------------------------------------------------------
# 1. Schema has required columns
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_schema_has_required_columns(store: SQLiteStore):
    """All accounting tables must have Phase 4 columns after initialization."""
    conn = store._conn
    assert conn is not None

    expected_columns = {
        "portfolio_snapshots": {"deployment_id", "cycle_id", "execution_mode"},
        "portfolio_metrics": {"deployment_id", "cycle_id", "execution_mode", "is_complete"},
        "transaction_ledger": {"deployment_id", "execution_mode"},
        "position_events": {"cycle_id", "execution_mode"},
    }

    for table, required in expected_columns.items():
        cursor = conn.execute(f"PRAGMA table_info({table})")
        actual = {row["name"] for row in cursor.fetchall()}
        missing = required - actual
        assert not missing, f"{table} is missing columns: {missing}"


# ---------------------------------------------------------------------------
# 2. Snapshot + metrics atomic co-write
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_snapshot_and_metrics_atomic_cowrite(store: SQLiteStore):
    """save_snapshot_and_metrics persists both rows sharing deployment_id / cycle_id."""
    snapshot = PortfolioSnapshot(
        timestamp=NOW,
        strategy_id=STRATEGY_ID,
        total_value_usd=Decimal("10000"),
        available_cash_usd=Decimal("2000"),
        value_confidence=ValueConfidence.HIGH,
        positions=[
            PositionValue(
                position_type=PositionType.TOKEN,
                protocol="aave_v3",
                chain="arbitrum",
                value_usd=Decimal("8000"),
                label="AAVE WETH Supply",
                tokens=["WETH"],
            ),
        ],
        chain="arbitrum",
        iteration_number=1,
    )
    metrics = PortfolioMetrics(
        strategy_id=STRATEGY_ID,
        timestamp=NOW,
        total_value_usd=Decimal("10000"),
        initial_value_usd=Decimal("9500"),
        deposits_usd=Decimal("0"),
        withdrawals_usd=Decimal("0"),
        gas_spent_usd=Decimal("1.50"),
        positions_json=json.dumps([{"label": "AAVE WETH Supply"}]),
        cycle_id=CYCLE_ID,
        deployment_id=DEPLOYMENT_ID,
        execution_mode=EXECUTION_MODE,
        is_complete=True,
    )

    row_id = await store.save_snapshot_and_metrics(snapshot, metrics)
    assert row_id > 0

    # Verify snapshot row has Phase 4 fields
    conn = store._conn
    assert conn is not None
    cursor = conn.execute(
        "SELECT deployment_id, cycle_id, execution_mode FROM portfolio_snapshots WHERE strategy_id = ?",
        (STRATEGY_ID,),
    )
    snap_row = cursor.fetchone()
    assert snap_row is not None
    assert snap_row["deployment_id"] == DEPLOYMENT_ID
    assert snap_row["cycle_id"] == CYCLE_ID
    assert snap_row["execution_mode"] == EXECUTION_MODE

    # Verify metrics row has the same Phase 4 fields
    cursor = conn.execute(
        "SELECT deployment_id, cycle_id, execution_mode, is_complete FROM portfolio_metrics WHERE strategy_id = ?",
        (STRATEGY_ID,),
    )
    met_row = cursor.fetchone()
    assert met_row is not None
    assert met_row["deployment_id"] == DEPLOYMENT_ID
    assert met_row["cycle_id"] == CYCLE_ID
    assert met_row["execution_mode"] == EXECUTION_MODE
    assert met_row["is_complete"] == 1  # SQLite stores booleans as integers


# ---------------------------------------------------------------------------
# 3. LedgerEntry new fields round-trip
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ledger_entry_persists_new_fields(store: SQLiteStore):
    """deployment_id and execution_mode on LedgerEntry survive save -> read."""
    entry = LedgerEntry(
        id="ledger-rt-1",
        cycle_id=CYCLE_ID,
        strategy_id=STRATEGY_ID,
        deployment_id=DEPLOYMENT_ID,
        execution_mode=EXECUTION_MODE,
        timestamp=NOW,
        intent_type="SWAP",
        token_in="USDC",
        amount_in="1000",
        token_out="ETH",
        amount_out="0.5",
        effective_price="2000",
        slippage_bps=5.0,
        gas_used=150_000,
        gas_usd="0.50",
        tx_hash="0xabc123",
        chain="arbitrum",
        protocol="uniswap_v3",
        success=True,
    )

    await store.save_ledger_entry(entry)
    entries = await store.get_ledger_entries(STRATEGY_ID)

    assert len(entries) == 1
    loaded = entries[0]
    assert loaded.deployment_id == DEPLOYMENT_ID
    assert loaded.execution_mode == EXECUTION_MODE
    assert loaded.cycle_id == CYCLE_ID
    assert loaded.gas_usd == "0.50"
    assert loaded.slippage_bps == 5.0


# ---------------------------------------------------------------------------
# 4. PositionEvent new fields round-trip
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_position_event_persists_new_fields(store: SQLiteStore):
    """cycle_id and execution_mode on PositionEvent survive save -> read."""
    event = PositionEvent(
        id="pe-rt-1",
        deployment_id=DEPLOYMENT_ID,
        cycle_id=CYCLE_ID,
        execution_mode=EXECUTION_MODE,
        position_id="nft-12345",
        position_type="LP",
        event_type="OPEN",
        timestamp=NOW,
        protocol="uniswap_v3",
        chain="arbitrum",
        token0="WETH",
        token1="USDC",
        amount0="1.0",
        amount1="2000",
        value_usd="4000",
        tick_lower=-887220,
        tick_upper=887220,
        liquidity="123456789",
        tx_hash="0xdef456",
        gas_usd="0.30",
    )

    ok = await store.save_position_event(event)
    assert ok is True

    events = await store.get_position_events(DEPLOYMENT_ID)
    assert len(events) == 1
    loaded = events[0]
    assert loaded["cycle_id"] == CYCLE_ID
    assert loaded["execution_mode"] == EXECUTION_MODE
    assert loaded["deployment_id"] == DEPLOYMENT_ID
    assert loaded["position_id"] == "nft-12345"


# ---------------------------------------------------------------------------
# 5. PositionValue economic state round-trip (serialize -> deserialize)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_position_value_economic_state_roundtrip():
    """Phase 4 PositionValue fields survive to_dict -> from_dict via PortfolioSnapshot."""
    snapshot = PortfolioSnapshot(
        timestamp=NOW,
        strategy_id=STRATEGY_ID,
        total_value_usd=Decimal("15000"),
        available_cash_usd=Decimal("3000"),
        value_confidence=ValueConfidence.HIGH,
        positions=[
            PositionValue(
                position_type=PositionType.TOKEN,
                protocol="aave_v3",
                chain="arbitrum",
                value_usd=Decimal("12000"),
                label="AAVE WETH Supply",
                tokens=["WETH"],
                cost_basis_usd=Decimal("10000"),
                unrealized_pnl_usd=Decimal("2000"),
                realized_pnl_usd=Decimal("500"),
                entry_timestamp="2026-04-01T00:00:00+00:00",
                last_update_timestamp="2026-04-14T12:00:00+00:00",
                ledger_entry_id="ledger-abc-123",
            ),
        ],
        chain="arbitrum",
    )

    # Serialize and deserialize
    data = snapshot.to_dict()
    restored = PortfolioSnapshot.from_dict(data)

    assert len(restored.positions) == 1
    pos = restored.positions[0]
    assert pos.cost_basis_usd == Decimal("10000")
    assert pos.unrealized_pnl_usd == Decimal("2000")
    assert pos.realized_pnl_usd == Decimal("500")
    assert pos.entry_timestamp == "2026-04-01T00:00:00+00:00"
    assert pos.last_update_timestamp == "2026-04-14T12:00:00+00:00"
    assert pos.ledger_entry_id == "ledger-abc-123"


# ---------------------------------------------------------------------------
# 6. Metrics execution_mode + is_complete round-trip through SQLite
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_metrics_execution_mode_roundtrip(store: SQLiteStore):
    """PortfolioMetrics with execution_mode='paper' and is_complete=True survives save -> read."""
    snapshot = PortfolioSnapshot(
        timestamp=NOW,
        strategy_id=STRATEGY_ID,
        total_value_usd=Decimal("5000"),
        available_cash_usd=Decimal("1000"),
        chain="arbitrum",
    )
    metrics = PortfolioMetrics(
        strategy_id=STRATEGY_ID,
        timestamp=NOW,
        total_value_usd=Decimal("5000"),
        initial_value_usd=Decimal("4800"),
        cycle_id=CYCLE_ID,
        deployment_id=DEPLOYMENT_ID,
        execution_mode="paper",
        is_complete=True,
    )

    await store.save_snapshot_and_metrics(snapshot, metrics)

    loaded = await store.get_portfolio_metrics(STRATEGY_ID)
    assert loaded is not None
    assert loaded.execution_mode == "paper"
    assert loaded.is_complete is True
    assert loaded.deployment_id == DEPLOYMENT_ID
    assert loaded.cycle_id == CYCLE_ID


# ---------------------------------------------------------------------------
# 7. Migration adds new columns to old schema
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_migration_adds_new_columns():
    """Databases created with old schema gain Phase 4 columns after migration."""
    # Create a database with the pre-Phase-4 schema (no Phase 4 columns)
    OLD_SCHEMA = """
    CREATE TABLE IF NOT EXISTS strategy_state (
        strategy_id TEXT PRIMARY KEY,
        version INTEGER NOT NULL DEFAULT 1,
        state_data TEXT NOT NULL,
        schema_version INTEGER NOT NULL DEFAULT 1,
        checksum TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS timeline_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        strategy_id TEXT NOT NULL,
        event_type TEXT NOT NULL,
        event_data TEXT NOT NULL,
        correlation_id TEXT,
        cycle_id TEXT DEFAULT '',
        phase TEXT DEFAULT '',
        created_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS portfolio_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        strategy_id TEXT NOT NULL,
        timestamp TEXT NOT NULL,
        iteration_number INTEGER DEFAULT 0,
        total_value_usd TEXT NOT NULL,
        available_cash_usd TEXT NOT NULL,
        value_confidence TEXT DEFAULT 'HIGH',
        positions_json TEXT NOT NULL,
        chain TEXT,
        created_at TEXT NOT NULL
    );
    CREATE UNIQUE INDEX IF NOT EXISTS idx_portfolio_snapshots_unique
    ON portfolio_snapshots (strategy_id, timestamp);
    CREATE TABLE IF NOT EXISTS portfolio_metrics (
        strategy_id TEXT PRIMARY KEY,
        initial_value_usd TEXT NOT NULL,
        initial_timestamp TEXT NOT NULL,
        deposits_usd TEXT DEFAULT '0',
        withdrawals_usd TEXT DEFAULT '0',
        gas_spent_usd TEXT DEFAULT '0',
        updated_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS transaction_ledger (
        id TEXT PRIMARY KEY,
        cycle_id TEXT NOT NULL,
        strategy_id TEXT NOT NULL,
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
        error TEXT
    );
    CREATE TABLE IF NOT EXISTS position_events (
        id TEXT PRIMARY KEY,
        deployment_id TEXT NOT NULL,
        position_id TEXT NOT NULL,
        position_type TEXT NOT NULL,
        event_type TEXT NOT NULL,
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
        attribution_json TEXT DEFAULT '{}',
        attribution_version INTEGER DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS clob_orders (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        order_id TEXT NOT NULL UNIQUE,
        market_id TEXT NOT NULL,
        token_id TEXT NOT NULL,
        side TEXT NOT NULL,
        status TEXT NOT NULL,
        price TEXT NOT NULL,
        size TEXT NOT NULL,
        filled_size TEXT NOT NULL DEFAULT '0',
        average_fill_price TEXT,
        fills TEXT NOT NULL DEFAULT '[]',
        order_type TEXT NOT NULL DEFAULT 'GTC',
        intent_id TEXT,
        error TEXT,
        metadata TEXT NOT NULL DEFAULT '{}',
        submitted_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    );
    """

    # Build the old DB manually, then let SQLiteStore run migrations on top
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(OLD_SCHEMA)
    conn.commit()

    # Confirm Phase 4 columns are missing before migration
    def _column_names(table: str) -> set[str]:
        cursor = conn.execute(f"PRAGMA table_info({table})")
        return {row[1] for row in cursor.fetchall()}

    assert "deployment_id" not in _column_names("portfolio_snapshots")
    assert "execution_mode" not in _column_names("portfolio_metrics")
    assert "cycle_id" not in _column_names("portfolio_metrics")
    assert "cycle_id" not in _column_names("position_events")
    assert "deployment_id" not in _column_names("transaction_ledger")

    # Inject the connection into a SQLiteStore and run migrations
    store = SQLiteStore(SQLiteConfig(db_path=":memory:"))
    store._conn = conn
    store._run_migrations()

    # Verify Phase 4 columns exist after migration
    snap_cols = _column_names("portfolio_snapshots")
    assert "deployment_id" in snap_cols
    assert "cycle_id" in snap_cols
    assert "execution_mode" in snap_cols

    met_cols = _column_names("portfolio_metrics")
    assert "deployment_id" in met_cols
    assert "cycle_id" in met_cols
    assert "execution_mode" in met_cols
    assert "is_complete" in met_cols

    ledger_cols = _column_names("transaction_ledger")
    assert "deployment_id" in ledger_cols
    assert "execution_mode" in ledger_cols

    pe_cols = _column_names("position_events")
    assert "cycle_id" in pe_cols
    assert "execution_mode" in pe_cols

    conn.close()
