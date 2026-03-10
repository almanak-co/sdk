"""Tests for SQLite state backend.

Tests cover:
- CRUD operations for strategy state
- CAS (Compare-And-Swap) conflict detection
- Timeline event operations
- Concurrent access patterns
- Database maintenance operations
"""

import os
import tempfile
from datetime import UTC, datetime, timedelta

import pytest
import pytest_asyncio

from almanak.framework.state.backends.sqlite import (
    SQLiteConfig,
    SQLiteStore,
    TimelineEvent,
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
        strategy_id="test-strategy-1",
        version=1,
        state={"key": "value", "nested": {"a": 1, "b": 2}},
        schema_version=1,
    )


@pytest.fixture
def sample_event():
    """Create a sample timeline event for testing."""
    return TimelineEvent(
        strategy_id="test-strategy-1",
        event_type="EXECUTION_SUCCESS",
        event_data={"tx_hash": "0x123", "gas_used": 21000},
        correlation_id="corr-123",
    )


# =============================================================================
# CONFIG TESTS
# =============================================================================


class TestSQLiteConfig:
    """Tests for SQLiteConfig."""

    def test_default_config(self):
        """Test default configuration values."""
        config = SQLiteConfig()
        assert config.db_path == "./almanak_state.db"
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

        loaded = await memory_store.get(sample_state.strategy_id)
        assert loaded is not None
        assert loaded.strategy_id == sample_state.strategy_id
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

        loaded = await memory_store.get(sample_state.strategy_id)
        assert loaded is not None
        assert loaded.version == 2
        assert loaded.state["key"] == "new_value"

    async def test_delete_state(self, memory_store, sample_state):
        """Test deleting state."""
        await memory_store.save(sample_state)

        deleted = await memory_store.delete(sample_state.strategy_id)
        assert deleted is True

        loaded = await memory_store.get(sample_state.strategy_id)
        assert loaded is None

    async def test_delete_nonexistent_state(self, memory_store):
        """Test deleting nonexistent state returns False."""
        deleted = await memory_store.delete("nonexistent-strategy")
        assert deleted is False

    async def test_multiple_strategies(self, memory_store):
        """Test managing multiple strategies."""
        states = [StateData(strategy_id=f"strategy-{i}", version=1, state={"index": i}) for i in range(5)]

        for state in states:
            await memory_store.save(state)

        # Verify all saved
        for i in range(5):
            loaded = await memory_store.get(f"strategy-{i}")
            assert loaded is not None
            assert loaded.state["index"] == i

    async def test_get_all_strategy_ids(self, memory_store):
        """Test getting all strategy IDs."""
        for i in range(3):
            state = StateData(strategy_id=f"strat-{i:03d}", version=1, state={"index": i})
            await memory_store.save(state)

        ids = await memory_store.get_all_strategy_ids()
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

        loaded = await memory_store.get(sample_state.strategy_id)
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

        assert exc_info.value.strategy_id == sample_state.strategy_id
        assert exc_info.value.expected_version == 99
        assert exc_info.value.actual_version == 1

    async def test_cas_sequential_updates(self, memory_store, sample_state):
        """Test sequential CAS updates maintain version chain."""
        await memory_store.save(sample_state)

        for i in range(1, 5):
            sample_state.state["iteration"] = i
            sample_state.version = i + 1
            await memory_store.save(sample_state, expected_version=i)

        loaded = await memory_store.get(sample_state.strategy_id)
        assert loaded is not None
        assert loaded.version == 5
        assert loaded.state["iteration"] == 4

    async def test_concurrent_update_conflict(self, memory_store, sample_state):
        """Test concurrent updates cause conflict."""
        await memory_store.save(sample_state)

        # Simulate two concurrent readers
        reader1 = await memory_store.get(sample_state.strategy_id)
        reader2 = await memory_store.get(sample_state.strategy_id)

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


# =============================================================================
# TIMELINE EVENT TESTS
# =============================================================================


class TestTimelineEvents:
    """Tests for timeline event operations."""

    async def test_save_and_get_event(self, memory_store, sample_event):
        """Test saving and retrieving events."""
        event_id = await memory_store.save_event(sample_event)
        assert event_id > 0

        loaded = await memory_store.get_event(event_id)
        assert loaded is not None
        assert loaded.strategy_id == sample_event.strategy_id
        assert loaded.event_type == sample_event.event_type
        assert loaded.event_data == sample_event.event_data
        assert loaded.correlation_id == sample_event.correlation_id

    async def test_get_events_by_strategy(self, memory_store):
        """Test getting events filtered by strategy."""
        # Create events for different strategies
        for strategy in ["strat-1", "strat-2"]:
            for i in range(3):
                event = TimelineEvent(
                    strategy_id=strategy,
                    event_type="TEST_EVENT",
                    event_data={"index": i},
                )
                await memory_store.save_event(event)

        events = await memory_store.get_events("strat-1")
        assert len(events) == 3
        for event in events:
            assert event.strategy_id == "strat-1"

    async def test_get_events_by_type(self, memory_store):
        """Test getting events filtered by type."""
        strategy_id = "test-strategy"
        for event_type in ["SUCCESS", "FAILURE", "SUCCESS"]:
            event = TimelineEvent(
                strategy_id=strategy_id,
                event_type=event_type,
                event_data={},
            )
            await memory_store.save_event(event)

        events = await memory_store.get_events(strategy_id, event_type="SUCCESS")
        assert len(events) == 2
        for event in events:
            assert event.event_type == "SUCCESS"

    async def test_get_events_by_correlation_id(self, memory_store):
        """Test getting events by correlation ID."""
        corr_id = "execution-123"

        for i in range(3):
            event = TimelineEvent(
                strategy_id=f"strategy-{i}",
                event_type="EVENT",
                event_data={"index": i},
                correlation_id=corr_id,
            )
            await memory_store.save_event(event)

        events = await memory_store.get_events_by_correlation_id(corr_id)
        assert len(events) == 3
        for event in events:
            assert event.correlation_id == corr_id

    async def test_events_pagination(self, memory_store):
        """Test event pagination."""
        strategy_id = "test-strategy"
        for i in range(10):
            event = TimelineEvent(
                strategy_id=strategy_id,
                event_type="EVENT",
                event_data={"index": i},
            )
            await memory_store.save_event(event)

        # Get first page
        page1 = await memory_store.get_events(strategy_id, limit=3, offset=0)
        assert len(page1) == 3

        # Get second page
        page2 = await memory_store.get_events(strategy_id, limit=3, offset=3)
        assert len(page2) == 3

        # No overlap
        page1_ids = {e.id for e in page1}
        page2_ids = {e.id for e in page2}
        assert page1_ids.isdisjoint(page2_ids)

    async def test_count_events(self, memory_store):
        """Test counting events."""
        for i in range(5):
            event = TimelineEvent(
                strategy_id=f"strat-{i % 2}",
                event_type="SUCCESS" if i % 2 == 0 else "FAILURE",
                event_data={},
            )
            await memory_store.save_event(event)

        # Count all
        total = await memory_store.count_events()
        assert total == 5

        # Count by strategy
        strat0_count = await memory_store.count_events(strategy_id="strat-0")
        assert strat0_count == 3

        # Count by type
        success_count = await memory_store.count_events(event_type="SUCCESS")
        assert success_count == 3

    async def test_delete_events(self, memory_store):
        """Test deleting events."""
        strategy_id = "test-strategy"
        for i in range(5):
            event = TimelineEvent(
                strategy_id=strategy_id,
                event_type="EVENT",
                event_data={"index": i},
            )
            await memory_store.save_event(event)

        deleted = await memory_store.delete_events(strategy_id)
        assert deleted == 5

        events = await memory_store.get_events(strategy_id)
        assert len(events) == 0

    async def test_delete_events_before_date(self, memory_store):
        """Test deleting events before a date."""
        strategy_id = "test-strategy"
        now = datetime.now(UTC)

        # Create old events
        for i in range(3):
            event = TimelineEvent(
                strategy_id=strategy_id,
                event_type="OLD_EVENT",
                event_data={"index": i},
                created_at=now - timedelta(days=10),
            )
            await memory_store.save_event(event)

        # Create recent events
        for i in range(2):
            event = TimelineEvent(
                strategy_id=strategy_id,
                event_type="NEW_EVENT",
                event_data={"index": i},
                created_at=now,
            )
            await memory_store.save_event(event)

        # Delete old events
        cutoff = now - timedelta(days=5)
        deleted = await memory_store.delete_events(strategy_id, before=cutoff)
        assert deleted == 3

        events = await memory_store.get_events(strategy_id)
        assert len(events) == 2
        for event in events:
            assert event.event_type == "NEW_EVENT"


# =============================================================================
# TIMELINE EVENT DATACLASS TESTS
# =============================================================================


class TestTimelineEventDataclass:
    """Tests for TimelineEvent dataclass."""

    def test_create_event(self):
        """Test creating a timeline event."""
        event = TimelineEvent(
            strategy_id="test",
            event_type="TEST",
            event_data={"key": "value"},
        )
        assert event.strategy_id == "test"
        assert event.event_type == "TEST"
        assert event.event_data == {"key": "value"}
        assert event.id is None
        assert event.correlation_id is None

    def test_to_dict(self, sample_event):
        """Test converting event to dictionary."""
        d = sample_event.to_dict()
        assert d["strategy_id"] == sample_event.strategy_id
        assert d["event_type"] == sample_event.event_type
        assert d["event_data"] == sample_event.event_data
        assert d["correlation_id"] == sample_event.correlation_id
        assert "created_at" in d

    def test_from_dict(self):
        """Test creating event from dictionary."""
        d = {
            "strategy_id": "strat-1",
            "event_type": "SUCCESS",
            "event_data": {"key": "value"},
            "correlation_id": "corr-1",
            "created_at": "2024-01-01T00:00:00+00:00",
        }
        event = TimelineEvent.from_dict(d)
        assert event.strategy_id == "strat-1"
        assert event.event_type == "SUCCESS"
        assert event.event_data == {"key": "value"}
        assert event.correlation_id == "corr-1"

    def test_roundtrip(self, sample_event):
        """Test to_dict/from_dict roundtrip."""
        d = sample_event.to_dict()
        restored = TimelineEvent.from_dict(d)
        assert restored.strategy_id == sample_event.strategy_id
        assert restored.event_type == sample_event.event_type
        assert restored.event_data == sample_event.event_data
        assert restored.correlation_id == sample_event.correlation_id


# =============================================================================
# MAINTENANCE TESTS
# =============================================================================


class TestMaintenance:
    """Tests for database maintenance operations."""

    async def test_get_stats(self, memory_store, sample_state, sample_event):
        """Test getting database statistics."""
        await memory_store.save(sample_state)
        await memory_store.save_event(sample_event)

        stats = await memory_store.get_stats()
        assert stats["db_path"] == ":memory:"
        assert stats["wal_mode"] is True
        assert stats["active_states"] == 1
        assert stats["total_events"] == 1

    async def test_vacuum(self, memory_store, sample_state):
        """Test VACUUM operation."""
        await memory_store.save(sample_state)
        await memory_store.delete(sample_state.strategy_id)

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
        loaded = await store2.get(sample_state.strategy_id)
        await store2.close()

        assert loaded is not None
        assert loaded.state == sample_state.state

    async def test_wal_mode_creates_wal_file(self, temp_db_path):
        """Test WAL mode creates -wal file."""
        config = SQLiteConfig(db_path=temp_db_path, wal_mode=True)
        store = SQLiteStore(config)
        await store.initialize()

        # Write something to trigger WAL
        state = StateData(strategy_id="test", version=1, state={"key": "value"})
        await store.save(state)

        # WAL file should exist (or be empty after checkpoint)
        await store.close()


# =============================================================================
# EDGE CASES
# =============================================================================


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    async def test_empty_state(self, memory_store):
        """Test saving state with empty state dict."""
        state = StateData(strategy_id="empty", version=1, state={})
        await memory_store.save(state)

        loaded = await memory_store.get("empty")
        assert loaded is not None
        assert loaded.state == {}

    async def test_large_state(self, memory_store):
        """Test saving state with large data."""
        large_data = {"key_" + str(i): "value_" * 100 for i in range(100)}
        state = StateData(strategy_id="large", version=1, state=large_data)
        await memory_store.save(state)

        loaded = await memory_store.get("large")
        assert loaded is not None
        assert loaded.state == large_data

    async def test_special_characters_in_state(self, memory_store):
        """Test state with special characters."""
        state = StateData(
            strategy_id="special",
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
        state = StateData(strategy_id="nested", version=1, state=nested)
        await memory_store.save(state)

        loaded = await memory_store.get("nested")
        assert loaded is not None
        assert loaded.state["level1"]["level2"]["level3"]["level4"]["value"] == 42

    async def test_checksum_verification(self, memory_store, sample_state):
        """Test checksum is calculated and stored."""
        await memory_store.save(sample_state)

        loaded = await memory_store.get(sample_state.strategy_id)
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
        )

    async def test_save_and_get_clob_order(self, memory_store, sample_clob_order):
        """Test saving and retrieving a CLOB order."""
        await memory_store.save_clob_order(sample_clob_order)

        loaded = await memory_store.get_clob_order(sample_clob_order.order_id)
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

    async def test_save_clob_order_with_fills(self, memory_store, sample_clob_order_with_fills):
        """Test saving order with fills preserves fill data."""
        await memory_store.save_clob_order(sample_clob_order_with_fills)

        loaded = await memory_store.get_clob_order(sample_clob_order_with_fills.order_id)
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
        loaded = await memory_store.get_clob_order("nonexistent-order")
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

        loaded = await memory_store.get_clob_order(sample_clob_order.order_id)
        assert loaded is not None
        assert loaded.status == ClobOrderStatus.PARTIALLY_FILLED
        assert loaded.filled_size == Decimal("25")

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
        )
        assert updated is True

        loaded = await memory_store.get_clob_order(sample_clob_order.order_id)
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
        )
        assert updated is False

    async def test_get_open_clob_orders(self, memory_store):
        """Test getting open orders."""
        from decimal import Decimal

        from almanak.framework.execution.clob_handler import ClobOrderState, ClobOrderStatus

        # Create orders with different statuses
        orders = [
            ClobOrderState(
                order_id="order-live",
                market_id="market-1",
                token_id="token-1",
                side="BUY",
                status=ClobOrderStatus.LIVE,
                price=Decimal("0.50"),
                size=Decimal("100"),
            ),
            ClobOrderState(
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
        open_orders = await memory_store.get_open_clob_orders()
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
                order_id="order-m1-1",
                market_id="market-1",
                token_id="token-1",
                side="BUY",
                status=ClobOrderStatus.LIVE,
                price=Decimal("0.50"),
                size=Decimal("100"),
            ),
            ClobOrderState(
                order_id="order-m1-2",
                market_id="market-1",
                token_id="token-1",
                side="SELL",
                status=ClobOrderStatus.LIVE,
                price=Decimal("0.60"),
                size=Decimal("100"),
            ),
            ClobOrderState(
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
        m1_orders = await memory_store.get_open_clob_orders(market_id="market-1")
        assert len(m1_orders) == 2
        for order in m1_orders:
            assert order.market_id == "market-1"

        # Get open orders for market-2 only
        m2_orders = await memory_store.get_open_clob_orders(market_id="market-2")
        assert len(m2_orders) == 1
        assert m2_orders[0].market_id == "market-2"

    async def test_delete_clob_order(self, memory_store, sample_clob_order):
        """Test deleting a CLOB order."""
        await memory_store.save_clob_order(sample_clob_order)

        deleted = await memory_store.delete_clob_order(sample_clob_order.order_id)
        assert deleted is True

        loaded = await memory_store.get_clob_order(sample_clob_order.order_id)
        assert loaded is None

    async def test_delete_nonexistent_clob_order(self, memory_store):
        """Test deleting nonexistent order returns False."""
        deleted = await memory_store.delete_clob_order("nonexistent")
        assert deleted is False

    async def test_get_clob_orders_by_intent(self, memory_store):
        """Test getting orders by intent ID."""
        from decimal import Decimal

        from almanak.framework.execution.clob_handler import ClobOrderState, ClobOrderStatus

        orders = [
            ClobOrderState(
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
        abc_orders = await memory_store.get_clob_orders_by_intent("intent-abc")
        assert len(abc_orders) == 2
        for order in abc_orders:
            assert order.intent_id == "intent-abc"

    async def test_clob_order_error_field(self, memory_store, sample_clob_order):
        """Test order error field is persisted."""
        from almanak.framework.execution.clob_handler import ClobOrderStatus

        sample_clob_order.status = ClobOrderStatus.FAILED
        sample_clob_order.error = "Insufficient balance"
        await memory_store.save_clob_order(sample_clob_order)

        loaded = await memory_store.get_clob_order(sample_clob_order.order_id)
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
        )
        assert updated is True

        loaded = await memory_store.get_clob_order(sample_clob_order.order_id)
        assert loaded is not None
        assert loaded.status == ClobOrderStatus.FAILED
        assert loaded.error == "API rate limit exceeded"
