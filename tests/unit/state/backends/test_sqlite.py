"""Tests for SQLite state backend.

Tests cover:
- CRUD operations for strategy state
- CAS (Compare-And-Swap) conflict detection
- Concurrent access patterns
- Database maintenance operations

VIB-4044 / PR5: timeline_events table and its CRUD methods are removed;
the corresponding TestTimelineEvents and TestTimelineEventDataclass
classes have been deleted. Gateway-side timeline events are tested in
``tests/gateway/test_timeline_store.py``.
"""

import os
import sqlite3
import tempfile
from datetime import UTC, datetime

import pytest
import pytest_asyncio

from almanak.framework.state.backends.sqlite import (
    SQLiteConfig,
    SQLiteStore,
    _convert_dual_identity_tables_to_deployment_id,
)
from almanak.framework.state.state_manager import StateConflictError, StateData

# Mark all tests in this module as async
pytestmark = pytest.mark.asyncio


# =============================================================================
# FIXTURES
# =============================================================================


@pytest.fixture
def temp_db_path():
    """Create a temporary database file path."""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    yield path
    # Cleanup
    for ext in ["", "-wal", "-shm"]:
        try:
            os.unlink(path + ext)
        except FileNotFoundError:
            pass


@pytest.fixture
def memory_config():
    """Create in-memory database config."""
    return SQLiteConfig(db_path=":memory:")


@pytest.fixture
def file_config(temp_db_path):
    """Create file-based database config."""
    return SQLiteConfig(db_path=temp_db_path)


@pytest_asyncio.fixture
async def memory_store(memory_config):
    """Create and initialize in-memory store."""
    store = SQLiteStore(memory_config)
    await store.initialize()
    yield store
    await store.close()


@pytest_asyncio.fixture
async def file_store(file_config):
    """Create and initialize file-based store."""
    store = SQLiteStore(file_config)
    await store.initialize()
    yield store
    await store.close()


@pytest.fixture
def sample_state():
    """Create a sample state for testing."""
    return StateData(
        deployment_id="test-strategy-1",
        version=1,
        state={"key": "value", "nested": {"a": 1, "b": 2}},
        schema_version=1,
    )


# =============================================================================
# CONFIG TESTS
# =============================================================================


class TestSQLiteConfig:
    """Tests for SQLiteConfig."""

    def test_default_config(self):
        """Test default configuration values.

        VIB-3761: db_path now resolves through ``local_paths.local_db_path``;
        the cwd-relative ``./almanak_state.db`` legacy default was the proximate
        cause of April 29's silent accounting failure and is removed.
        """
        from almanak.framework.local_paths import local_db_path

        config = SQLiteConfig()
        assert config.db_path == str(local_db_path())
        # Critical regression guard: the cwd-relative default must not return.
        assert config.db_path != "./almanak_state.db"
        assert config.timeout == 30.0
        assert config.wal_mode is True
        assert config.busy_timeout == 5000
        assert config.cache_size == -2000

    def test_custom_config(self):
        """Test custom configuration values."""
        config = SQLiteConfig(
            db_path="/custom/path.db",
            timeout=60.0,
            wal_mode=False,
            busy_timeout=10000,
        )
        assert config.db_path == "/custom/path.db"
        assert config.timeout == 60.0
        assert config.wal_mode is False
        assert config.busy_timeout == 10000

    def test_invalid_timeout(self):
        """Test validation rejects invalid timeout."""
        with pytest.raises(ValueError, match="timeout must be positive"):
            SQLiteConfig(timeout=0)

        with pytest.raises(ValueError, match="timeout must be positive"):
            SQLiteConfig(timeout=-1)

    def test_invalid_busy_timeout(self):
        """Test validation rejects invalid busy_timeout."""
        with pytest.raises(ValueError, match="busy_timeout must be non-negative"):
            SQLiteConfig(busy_timeout=-1)


# =============================================================================
# INITIALIZATION TESTS
# =============================================================================


class TestSQLiteStoreInit:
    """Tests for SQLiteStore initialization."""

    async def test_dual_identity_conversion_preserves_already_converted_plain_deployment_id(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        try:
            conn.execute(
                """
                CREATE TABLE portfolio_snapshots (
                    id INTEGER PRIMARY KEY,
                    deployment_id TEXT,
                    total_value_usd TEXT
                )
                """
            )
            conn.execute(
                "INSERT INTO portfolio_snapshots (deployment_id, total_value_usd) VALUES (?, ?)",
                ("deploy-1", "123.45"),
            )

            _convert_dual_identity_tables_to_deployment_id(conn)

            columns = {row["name"] for row in conn.execute("PRAGMA table_info(portfolio_snapshots)").fetchall()}
            row = conn.execute("SELECT deployment_id, total_value_usd FROM portfolio_snapshots").fetchone()

            assert "deployment_id" in columns
            assert "strategy_id" not in columns
            assert row["deployment_id"] == "deploy-1"
            assert row["total_value_usd"] == "123.45"
        finally:
            conn.close()

    async def test_initialize_migrates_legacy_clob_orders_before_deployment_index(self, temp_db_path):
        conn = sqlite3.connect(temp_db_path)
        try:
            conn.executescript(
                """
                CREATE TABLE clob_orders (
                    order_id TEXT NOT NULL,
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
            )
            conn.commit()
        finally:
            conn.close()

        store = SQLiteStore(SQLiteConfig(db_path=temp_db_path))
        await store.initialize()
        try:
            columns = {
                row["name"]
                for row in store._conn.execute("PRAGMA table_info(clob_orders)").fetchall()  # type: ignore[union-attr]
            }
            indexes = {
                row["name"]
                for row in store._conn.execute("PRAGMA index_list(clob_orders)").fetchall()  # type: ignore[union-attr]
            }

            assert "deployment_id" in columns
            assert "idx_clob_orders_deployment_order" in indexes
        finally:
            await store.close()

    async def test_initialize_memory_db(self, memory_config):
        """Test initialization with in-memory database."""
        store = SQLiteStore(memory_config)
        assert not store.is_initialized

        await store.initialize()
        assert store.is_initialized
        assert store.db_path == ":memory:"

        await store.close()
        assert not store.is_initialized

    async def test_initialize_file_db(self, file_config, temp_db_path):
        """Test initialization with file database."""
        store = SQLiteStore(file_config)
        await store.initialize()

        assert store.is_initialized
        assert os.path.exists(temp_db_path)

        await store.close()

    async def test_initialize_creates_parent_dirs(self, temp_db_path):
        """Test initialization creates parent directories."""
        nested_path = os.path.join(os.path.dirname(temp_db_path), "nested", "dir", "db.sqlite")
        config = SQLiteConfig(db_path=nested_path)
        store = SQLiteStore(config)

        await store.initialize()
        assert os.path.exists(nested_path)

        await store.close()
        # Cleanup
        os.unlink(nested_path)
        os.rmdir(os.path.dirname(nested_path))
        os.rmdir(os.path.dirname(os.path.dirname(nested_path)))

    async def test_double_initialize(self, memory_store):
        """Test double initialization is idempotent."""
        assert memory_store.is_initialized
        await memory_store.initialize()  # Should not raise
        assert memory_store.is_initialized


# =============================================================================
# STATE CRUD TESTS
# =============================================================================


class TestStateOperations:
    """Tests for state CRUD operations."""

    async def test_save_and_get_state(self, memory_store, sample_state):
        """Test saving and retrieving state."""
        await memory_store.save(sample_state)

        loaded = await memory_store.get(sample_state.deployment_id)
        assert loaded is not None
        assert loaded.deployment_id == sample_state.deployment_id
        assert loaded.version == sample_state.version
        assert loaded.state == sample_state.state
        assert loaded.schema_version == sample_state.schema_version
        assert loaded.checksum != ""

    async def test_get_nonexistent_state(self, memory_store):
        """Test getting nonexistent state returns None."""
        loaded = await memory_store.get("nonexistent-strategy")
        assert loaded is None

    async def test_update_state(self, memory_store, sample_state):
        """Test updating existing state."""
        await memory_store.save(sample_state)

        # Update state
        sample_state.state["key"] = "new_value"
        sample_state.version = 2
        await memory_store.save(sample_state)

        loaded = await memory_store.get(sample_state.deployment_id)
        assert loaded is not None
        assert loaded.version == 2
        assert loaded.state["key"] == "new_value"

    async def test_delete_state(self, memory_store, sample_state):
        """Test deleting state."""
        await memory_store.save(sample_state)

        deleted = await memory_store.delete(sample_state.deployment_id)
        assert deleted is True

        loaded = await memory_store.get(sample_state.deployment_id)
        assert loaded is None

    async def test_delete_nonexistent_state(self, memory_store):
        """Test deleting nonexistent state returns False."""
        deleted = await memory_store.delete("nonexistent-strategy")
        assert deleted is False

    async def test_multiple_strategies(self, memory_store):
        """Test managing multiple strategies."""
        states = [StateData(deployment_id=f"strategy-{i}", version=1, state={"index": i}) for i in range(5)]

        for state in states:
            await memory_store.save(state)

        # Verify all saved
        for i in range(5):
            loaded = await memory_store.get(f"strategy-{i}")
            assert loaded is not None
            assert loaded.state["index"] == i

    async def test_get_all_deployment_ids(self, memory_store):
        """Test getting all deployment IDs."""
        for i in range(3):
            state = StateData(deployment_id=f"strat-{i:03d}", version=1, state={"index": i})
            await memory_store.save(state)

        ids = await memory_store.get_all_deployment_ids()
        assert len(ids) == 3
        assert ids == ["strat-000", "strat-001", "strat-002"]  # Sorted


# =============================================================================
# CAS (COMPARE-AND-SWAP) TESTS
# =============================================================================


class TestCASOperations:
    """Tests for CAS (optimistic locking) operations."""

    async def test_cas_update_success(self, memory_store, sample_state):
        """Test successful CAS update with correct version."""
        await memory_store.save(sample_state)

        # Update with correct expected version
        sample_state.state["key"] = "updated"
        sample_state.version = 2
        result = await memory_store.save(sample_state, expected_version=1)
        assert result is True

        loaded = await memory_store.get(sample_state.deployment_id)
        assert loaded is not None
        assert loaded.version == 2
        assert loaded.state["key"] == "updated"

    async def test_cas_update_conflict(self, memory_store, sample_state):
        """Test CAS update fails with wrong version."""
        await memory_store.save(sample_state)

        # Update with wrong expected version
        sample_state.state["key"] = "updated"
        sample_state.version = 2

        with pytest.raises(StateConflictError) as exc_info:
            await memory_store.save(sample_state, expected_version=99)

        assert exc_info.value.deployment_id == sample_state.deployment_id
        assert exc_info.value.expected_version == 99
        assert exc_info.value.actual_version == 1

    async def test_cas_sequential_updates(self, memory_store, sample_state):
        """Test sequential CAS updates maintain version chain."""
        await memory_store.save(sample_state)

        for i in range(1, 5):
            sample_state.state["iteration"] = i
            sample_state.version = i + 1
            await memory_store.save(sample_state, expected_version=i)

        loaded = await memory_store.get(sample_state.deployment_id)
        assert loaded is not None
        assert loaded.version == 5
        assert loaded.state["iteration"] == 4

    async def test_concurrent_update_conflict(self, memory_store, sample_state):
        """Test concurrent updates cause conflict."""
        await memory_store.save(sample_state)

        # Simulate two concurrent readers
        reader1 = await memory_store.get(sample_state.deployment_id)
        reader2 = await memory_store.get(sample_state.deployment_id)

        assert reader1 is not None
        assert reader2 is not None

        # First update succeeds
        reader1.state["writer"] = "reader1"
        reader1.version = 2
        await memory_store.save(reader1, expected_version=1)

        # Second update fails due to stale version
        reader2.state["writer"] = "reader2"
        reader2.version = 2

        with pytest.raises(StateConflictError):
            await memory_store.save(reader2, expected_version=1)


class TestMaintenance:
    """Tests for database maintenance operations."""

    async def test_get_stats(self, memory_store, sample_state):
        """Test getting database statistics."""
        await memory_store.save(sample_state)

        stats = await memory_store.get_stats()
        assert stats["db_path"] == ":memory:"
        assert stats["wal_mode"] is True
        assert stats["active_states"] == 1
        # VIB-4044 / PR5: timeline_events table removed; total_events is
        # always 0 to preserve the stats payload shape.
        assert stats["total_events"] == 0

    async def test_vacuum(self, memory_store, sample_state):
        """Test VACUUM operation."""
        await memory_store.save(sample_state)
        await memory_store.delete(sample_state.deployment_id)

        # Should not raise
        await memory_store.vacuum()

    async def test_checkpoint(self, file_store, sample_state):
        """Test WAL checkpoint operation."""
        await file_store.save(sample_state)

        # Should not raise
        await file_store.checkpoint()


# =============================================================================
# FILE PERSISTENCE TESTS
# =============================================================================


class TestFilePersistence:
    """Tests for file-based database persistence."""

    async def test_data_persists_after_close(self, file_config, sample_state):
        """Test data persists after closing and reopening."""
        # First store
        store1 = SQLiteStore(file_config)
        await store1.initialize()
        await store1.save(sample_state)
        await store1.close()

        # Second store
        store2 = SQLiteStore(file_config)
        await store2.initialize()
        loaded = await store2.get(sample_state.deployment_id)
        await store2.close()

        assert loaded is not None
        assert loaded.state == sample_state.state

    async def test_wal_mode_creates_wal_file(self, temp_db_path):
        """Test WAL mode creates -wal file."""
        config = SQLiteConfig(db_path=temp_db_path, wal_mode=True)
        store = SQLiteStore(config)
        await store.initialize()

        # Write something to trigger WAL
        state = StateData(deployment_id="test", version=1, state={"key": "value"})
        await store.save(state)

        # WAL file should exist (or be empty after checkpoint)
        await store.close()

    async def test_legacy_timeline_events_table_dropped_on_upgrade(self, temp_db_path):
        """VIB-4044 / PR5 (CodeRabbit review): existing local SDK databases
        carry the deprecated `timeline_events` table from earlier SDK versions.
        Dropping the DDL from `SCHEMA_SQL` only affects fresh databases, so
        upgraded users would carry the table forever. The migration in
        `_run_migrations` must drop it on upgrade.
        """
        import sqlite3

        # Set up a database that pre-dates PR5 by manually creating the
        # legacy table with the old shape.
        with sqlite3.connect(str(temp_db_path)) as legacy_conn:
            legacy_conn.execute(
                """
                CREATE TABLE timeline_events (
                    id INTEGER PRIMARY KEY,
                    deployment_id TEXT,
                    timestamp TEXT,
                    event_type TEXT,
                    description TEXT
                )
                """
            )
            legacy_conn.execute(
                "INSERT INTO timeline_events (deployment_id, timestamp, event_type, description) VALUES (?, ?, ?, ?)",
                ("legacy_strategy", "2026-01-01T00:00:00Z", "TRADE", "old data"),
            )
            legacy_conn.commit()

        # Open the store — initialize() runs SCHEMA_SQL + migrations.
        config = SQLiteConfig(db_path=temp_db_path)
        store = SQLiteStore(config)
        await store.initialize()
        await store.close()

        # Confirm the migration dropped the table.
        with sqlite3.connect(str(temp_db_path)) as conn:
            cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='timeline_events'")
            assert cursor.fetchone() is None, "legacy timeline_events table must be dropped on upgrade"

    async def test_legacy_timeline_events_drop_is_idempotent(self, temp_db_path):
        """The migration must be a no-op on fresh databases (PR5)."""
        import sqlite3

        # Fresh database: no `timeline_events` table exists at all.
        config = SQLiteConfig(db_path=temp_db_path)
        store = SQLiteStore(config)
        await store.initialize()  # First run — should not raise.
        await store.close()

        # Second initialize — exercising the same migration path again.
        store2 = SQLiteStore(config)
        await store2.initialize()
        await store2.close()

        with sqlite3.connect(str(temp_db_path)) as conn:
            cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='timeline_events'")
            assert cursor.fetchone() is None


# =============================================================================
# EDGE CASES
# =============================================================================


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    async def test_empty_state(self, memory_store):
        """Test saving state with empty state dict."""
        state = StateData(deployment_id="empty", version=1, state={})
        await memory_store.save(state)

        loaded = await memory_store.get("empty")
        assert loaded is not None
        assert loaded.state == {}

    async def test_large_state(self, memory_store):
        """Test saving state with large data."""
        large_data = {"key_" + str(i): "value_" * 100 for i in range(100)}
        state = StateData(deployment_id="large", version=1, state=large_data)
        await memory_store.save(state)

        loaded = await memory_store.get("large")
        assert loaded is not None
        assert loaded.state == large_data

    async def test_special_characters_in_state(self, memory_store):
        """Test state with special characters."""
        state = StateData(
            deployment_id="special",
            version=1,
            state={
                "unicode": "Hello\n\t\r",
                "emoji": "Test",
                "quotes": 'He said "hello"',
                "backslash": "path\\to\\file",
            },
        )
        await memory_store.save(state)

        loaded = await memory_store.get("special")
        assert loaded is not None
        assert loaded.state == state.state

    async def test_nested_state(self, memory_store):
        """Test state with deep nesting."""
        nested = {"level1": {"level2": {"level3": {"level4": {"value": 42}}}}}
        state = StateData(deployment_id="nested", version=1, state=nested)
        await memory_store.save(state)

        loaded = await memory_store.get("nested")
        assert loaded is not None
        assert loaded.state["level1"]["level2"]["level3"]["level4"]["value"] == 42

    async def test_checksum_verification(self, memory_store, sample_state):
        """Test checksum is calculated and stored."""
        await memory_store.save(sample_state)

        loaded = await memory_store.get(sample_state.deployment_id)
        assert loaded is not None
        assert loaded.checksum != ""
        assert loaded.verify_checksum()

    async def test_auto_initialize_on_operation(self, memory_config):
        """Test store auto-initializes on first operation."""
        store = SQLiteStore(memory_config)
        assert not store.is_initialized

        # Operation should trigger initialization
        result = await store.get("nonexistent")
        assert store.is_initialized
        assert result is None

        await store.close()


# =============================================================================
# CLOB ORDER TESTS
# =============================================================================


class TestClobOrderOperations:
    """Tests for CLOB order state persistence."""

    @pytest.fixture
    def sample_clob_order(self):
        """Create a sample CLOB order for testing."""
        from decimal import Decimal

        from almanak.framework.execution.clob_handler import (
            ClobOrderState,
            ClobOrderStatus,
        )

        return ClobOrderState(
            order_id="order-123",
            market_id="market-456",
            token_id="token-789",
            side="BUY",
            status=ClobOrderStatus.LIVE,
            price=Decimal("0.55"),
            size=Decimal("100"),
            filled_size=Decimal("0"),
            order_type="GTC",
            intent_id="intent-abc",
            metadata={"source": "test"},
            deployment_id="deployment:test-clob",
        )

    @pytest.fixture
    def sample_clob_order_with_fills(self):
        """Create a sample CLOB order with fills for testing."""
        from decimal import Decimal

        from almanak.framework.execution.clob_handler import (
            ClobFill,
            ClobOrderState,
            ClobOrderStatus,
        )

        return ClobOrderState(
            order_id="order-filled",
            market_id="market-456",
            token_id="token-789",
            side="SELL",
            status=ClobOrderStatus.PARTIALLY_FILLED,
            price=Decimal("0.65"),
            size=Decimal("100"),
            filled_size=Decimal("50"),
            average_fill_price=Decimal("0.66"),
            fills=[
                ClobFill(
                    fill_id="fill-1",
                    price=Decimal("0.66"),
                    size=Decimal("50"),
                    fee=Decimal("0.01"),
                    timestamp=datetime.now(UTC),
                    counterparty="0x1234",
                ),
            ],
            order_type="GTC",
            intent_id="intent-def",
            deployment_id="deployment:test-clob",
        )

    async def test_save_and_get_clob_order(self, memory_store, sample_clob_order):
        """Test saving and retrieving a CLOB order."""
        await memory_store.save_clob_order(sample_clob_order)

        loaded = await memory_store.get_clob_order(
            sample_clob_order.order_id,
            deployment_id=sample_clob_order.deployment_id,
        )
        assert loaded is not None
        assert loaded.order_id == sample_clob_order.order_id
        assert loaded.market_id == sample_clob_order.market_id
        assert loaded.token_id == sample_clob_order.token_id
        assert loaded.side == sample_clob_order.side
        assert loaded.status == sample_clob_order.status
        assert loaded.price == sample_clob_order.price
        assert loaded.size == sample_clob_order.size
        assert loaded.filled_size == sample_clob_order.filled_size
        assert loaded.order_type == sample_clob_order.order_type
        assert loaded.intent_id == sample_clob_order.intent_id
        assert loaded.metadata == sample_clob_order.metadata
        assert loaded.deployment_id == sample_clob_order.deployment_id
        scoped = await memory_store.get_clob_order(
            sample_clob_order.order_id,
            deployment_id=sample_clob_order.deployment_id,
        )
        assert scoped is not None
        assert scoped.order_id == sample_clob_order.order_id
        assert await memory_store.get_clob_order(sample_clob_order.order_id, deployment_id="deployment:other") is None

    async def test_save_clob_order_with_fills(self, memory_store, sample_clob_order_with_fills):
        """Test saving order with fills preserves fill data."""
        await memory_store.save_clob_order(sample_clob_order_with_fills)

        loaded = await memory_store.get_clob_order(
            sample_clob_order_with_fills.order_id,
            deployment_id=sample_clob_order_with_fills.deployment_id,
        )
        assert loaded is not None
        assert loaded.filled_size == sample_clob_order_with_fills.filled_size
        assert loaded.average_fill_price == sample_clob_order_with_fills.average_fill_price
        assert len(loaded.fills) == 1
        assert loaded.fills[0].fill_id == "fill-1"
        assert loaded.fills[0].price == sample_clob_order_with_fills.fills[0].price
        assert loaded.fills[0].size == sample_clob_order_with_fills.fills[0].size
        assert loaded.fills[0].fee == sample_clob_order_with_fills.fills[0].fee
        assert loaded.fills[0].counterparty == "0x1234"

    async def test_get_nonexistent_clob_order(self, memory_store):
        """Test getting nonexistent order returns None."""
        loaded = await memory_store.get_clob_order("nonexistent-order", deployment_id="deployment:test-clob")
        assert loaded is None

    async def test_update_clob_order(self, memory_store, sample_clob_order):
        """Test updating an existing CLOB order."""
        from decimal import Decimal

        from almanak.framework.execution.clob_handler import ClobOrderStatus

        await memory_store.save_clob_order(sample_clob_order)

        # Update the order
        sample_clob_order.status = ClobOrderStatus.PARTIALLY_FILLED
        sample_clob_order.filled_size = Decimal("25")
        await memory_store.save_clob_order(sample_clob_order)

        loaded = await memory_store.get_clob_order(
            sample_clob_order.order_id,
            deployment_id=sample_clob_order.deployment_id,
        )
        assert loaded is not None
        assert loaded.status == ClobOrderStatus.PARTIALLY_FILLED
        assert loaded.filled_size == Decimal("25")
        assert loaded.deployment_id == sample_clob_order.deployment_id

    async def test_save_clob_order_does_not_repair_legacy_blank_deployment_id(self, memory_store, sample_clob_order):
        """Saving a stamped order does not match or rewrite a legacy blank row."""
        memory_store._conn.execute(  # type: ignore[union-attr]
            """
            INSERT INTO clob_orders
            (deployment_id, order_id, market_id, token_id, side, status,
             price, size, filled_size, order_type, fills, metadata,
             submitted_at, updated_at)
            VALUES ('', ?, 'old-market', 'old-token', 'BUY', 'live',
                    '0.10', '1', '0', 'GTC', '[]', '{}', ?, ?)
            """,
            (
                sample_clob_order.order_id,
                sample_clob_order.submitted_at.isoformat(),
                sample_clob_order.updated_at.isoformat(),
            ),
        )
        memory_store._conn.commit()  # type: ignore[union-attr]

        await memory_store.save_clob_order(sample_clob_order)

        loaded = await memory_store.get_clob_order(
            sample_clob_order.order_id,
            deployment_id=sample_clob_order.deployment_id,
        )
        assert loaded is not None
        assert loaded.deployment_id == sample_clob_order.deployment_id
        assert loaded.market_id == sample_clob_order.market_id
        legacy = memory_store._conn.execute(  # type: ignore[union-attr]
            "SELECT COUNT(*) AS n FROM clob_orders WHERE order_id = ? AND deployment_id = ''",
            (sample_clob_order.order_id,),
        ).fetchone()
        assert legacy["n"] == 1

    async def test_update_clob_order_status(self, memory_store, sample_clob_order):
        """Test updating order status via update_clob_order_status."""
        from decimal import Decimal

        from almanak.framework.execution.clob_handler import ClobFill, ClobOrderStatus

        await memory_store.save_clob_order(sample_clob_order)

        # Update status with fills
        new_fill = ClobFill(
            fill_id="fill-new",
            price=Decimal("0.55"),
            size=Decimal("50"),
            fee=Decimal("0.005"),
            timestamp=datetime.now(UTC),
        )

        updated = await memory_store.update_clob_order_status(
            order_id=sample_clob_order.order_id,
            status=ClobOrderStatus.PARTIALLY_FILLED,
            fills=[new_fill],
            filled_size="50",
            average_fill_price="0.55",
            deployment_id=sample_clob_order.deployment_id,
        )
        assert updated is True

        loaded = await memory_store.get_clob_order(
            sample_clob_order.order_id,
            deployment_id=sample_clob_order.deployment_id,
        )
        assert loaded is not None
        assert loaded.status == ClobOrderStatus.PARTIALLY_FILLED
        assert loaded.filled_size == Decimal("50")
        assert loaded.average_fill_price == Decimal("0.55")
        assert len(loaded.fills) == 1
        assert loaded.fills[0].fill_id == "fill-new"

    async def test_update_nonexistent_order_status(self, memory_store):
        """Test updating nonexistent order returns False."""
        from almanak.framework.execution.clob_handler import ClobOrderStatus

        updated = await memory_store.update_clob_order_status(
            order_id="nonexistent",
            status=ClobOrderStatus.CANCELLED,
            deployment_id="deployment:test-clob",
        )
        assert updated is False

    async def test_get_open_clob_orders(self, memory_store):
        """Test getting open orders."""
        from decimal import Decimal

        from almanak.framework.execution.clob_handler import ClobOrderState, ClobOrderStatus

        # Create orders with different statuses
        orders = [
            ClobOrderState(
                deployment_id="deployment:test-clob",
                order_id="order-live",
                market_id="market-1",
                token_id="token-1",
                side="BUY",
                status=ClobOrderStatus.LIVE,
                price=Decimal("0.50"),
                size=Decimal("100"),
            ),
            ClobOrderState(
                deployment_id="deployment:test-clob",
                order_id="order-partial",
                market_id="market-1",
                token_id="token-1",
                side="SELL",
                status=ClobOrderStatus.PARTIALLY_FILLED,
                price=Decimal("0.60"),
                size=Decimal("100"),
                filled_size=Decimal("50"),
            ),
            ClobOrderState(
                deployment_id="deployment:test-clob",
                order_id="order-matched",
                market_id="market-1",
                token_id="token-1",
                side="BUY",
                status=ClobOrderStatus.MATCHED,
                price=Decimal("0.55"),
                size=Decimal("100"),
                filled_size=Decimal("100"),
            ),
            ClobOrderState(
                deployment_id="deployment:test-clob",
                order_id="order-cancelled",
                market_id="market-2",
                token_id="token-2",
                side="SELL",
                status=ClobOrderStatus.CANCELLED,
                price=Decimal("0.70"),
                size=Decimal("100"),
            ),
        ]

        for order in orders:
            await memory_store.save_clob_order(order)

        # Get all open orders
        open_orders = await memory_store.get_open_clob_orders(deployment_id="deployment:test-clob")
        assert len(open_orders) == 2
        open_ids = {o.order_id for o in open_orders}
        assert "order-live" in open_ids
        assert "order-partial" in open_ids
        assert "order-matched" not in open_ids
        assert "order-cancelled" not in open_ids

    async def test_get_open_clob_orders_by_market(self, memory_store):
        """Test getting open orders filtered by market."""
        from decimal import Decimal

        from almanak.framework.execution.clob_handler import ClobOrderState, ClobOrderStatus

        orders = [
            ClobOrderState(
                deployment_id="deployment:test-clob",
                order_id="order-m1-1",
                market_id="market-1",
                token_id="token-1",
                side="BUY",
                status=ClobOrderStatus.LIVE,
                price=Decimal("0.50"),
                size=Decimal("100"),
            ),
            ClobOrderState(
                deployment_id="deployment:test-clob",
                order_id="order-m1-2",
                market_id="market-1",
                token_id="token-1",
                side="SELL",
                status=ClobOrderStatus.LIVE,
                price=Decimal("0.60"),
                size=Decimal("100"),
            ),
            ClobOrderState(
                deployment_id="deployment:test-clob",
                order_id="order-m2-1",
                market_id="market-2",
                token_id="token-2",
                side="BUY",
                status=ClobOrderStatus.LIVE,
                price=Decimal("0.55"),
                size=Decimal("100"),
            ),
        ]

        for order in orders:
            await memory_store.save_clob_order(order)

        # Get open orders for market-1 only
        m1_orders = await memory_store.get_open_clob_orders(
            market_id="market-1",
            deployment_id="deployment:test-clob",
        )
        assert len(m1_orders) == 2
        for order in m1_orders:
            assert order.market_id == "market-1"

        # Get open orders for market-2 only
        m2_orders = await memory_store.get_open_clob_orders(
            market_id="market-2",
            deployment_id="deployment:test-clob",
        )
        assert len(m2_orders) == 1
        assert m2_orders[0].market_id == "market-2"

    async def test_delete_clob_order(self, memory_store, sample_clob_order):
        """Test deleting a CLOB order."""
        await memory_store.save_clob_order(sample_clob_order)

        deleted = await memory_store.delete_clob_order(
            sample_clob_order.order_id,
            deployment_id=sample_clob_order.deployment_id,
        )
        assert deleted is True

        loaded = await memory_store.get_clob_order(
            sample_clob_order.order_id,
            deployment_id=sample_clob_order.deployment_id,
        )
        assert loaded is None

    async def test_delete_nonexistent_clob_order(self, memory_store):
        """Test deleting nonexistent order returns False."""
        deleted = await memory_store.delete_clob_order("nonexistent", deployment_id="deployment:test-clob")
        assert deleted is False

    async def test_get_clob_orders_by_intent(self, memory_store):
        """Test getting orders by intent ID."""
        from decimal import Decimal

        from almanak.framework.execution.clob_handler import ClobOrderState, ClobOrderStatus

        orders = [
            ClobOrderState(
                deployment_id="deployment:test-clob",
                order_id="order-1",
                market_id="market-1",
                token_id="token-1",
                side="BUY",
                status=ClobOrderStatus.LIVE,
                price=Decimal("0.50"),
                size=Decimal("100"),
                intent_id="intent-abc",
            ),
            ClobOrderState(
                deployment_id="deployment:test-clob",
                order_id="order-2",
                market_id="market-1",
                token_id="token-1",
                side="SELL",
                status=ClobOrderStatus.MATCHED,
                price=Decimal("0.60"),
                size=Decimal("100"),
                intent_id="intent-abc",
            ),
            ClobOrderState(
                deployment_id="deployment:test-clob",
                order_id="order-3",
                market_id="market-2",
                token_id="token-2",
                side="BUY",
                status=ClobOrderStatus.LIVE,
                price=Decimal("0.55"),
                size=Decimal("100"),
                intent_id="intent-xyz",
            ),
        ]

        for order in orders:
            await memory_store.save_clob_order(order)

        # Get orders for intent-abc
        abc_orders = await memory_store.get_clob_orders_by_intent(
            "intent-abc",
            deployment_id="deployment:test-clob",
        )
        assert len(abc_orders) == 2
        for order in abc_orders:
            assert order.intent_id == "intent-abc"

    async def test_clob_order_error_field(self, memory_store, sample_clob_order):
        """Test order error field is persisted."""
        from almanak.framework.execution.clob_handler import ClobOrderStatus

        sample_clob_order.status = ClobOrderStatus.FAILED
        sample_clob_order.error = "Insufficient balance"
        await memory_store.save_clob_order(sample_clob_order)

        loaded = await memory_store.get_clob_order(
            sample_clob_order.order_id,
            deployment_id=sample_clob_order.deployment_id,
        )
        assert loaded is not None
        assert loaded.status == ClobOrderStatus.FAILED
        assert loaded.error == "Insufficient balance"

    async def test_clob_order_status_update_with_error(self, memory_store, sample_clob_order):
        """Test updating order status with error message."""
        from almanak.framework.execution.clob_handler import ClobOrderStatus

        await memory_store.save_clob_order(sample_clob_order)

        updated = await memory_store.update_clob_order_status(
            order_id=sample_clob_order.order_id,
            status=ClobOrderStatus.FAILED,
            error="API rate limit exceeded",
            deployment_id=sample_clob_order.deployment_id,
        )
        assert updated is True
        assert (
            await memory_store.update_clob_order_status(
                order_id=sample_clob_order.order_id,
                status=ClobOrderStatus.LIVE,
                deployment_id="deployment:other",
            )
            is False
        )

        loaded = await memory_store.get_clob_order(
            sample_clob_order.order_id,
            deployment_id=sample_clob_order.deployment_id,
        )
        assert loaded is not None
        assert loaded.status == ClobOrderStatus.FAILED
        assert loaded.error == "API rate limit exceeded"
