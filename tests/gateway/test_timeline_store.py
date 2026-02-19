"""Tests for the gateway TimelineStore.

Tests cover:
- TimelineStore initialization (in-memory and SQLite)
- Adding and retrieving events
- Filtering by event type and timestamp
- Recent events across all strategies
- Clearing events
- Persistence and reload from SQLite
- Thread safety with concurrent operations
"""

import tempfile
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest

from almanak.gateway.timeline.store import (
    TimelineEvent,
    TimelineStore,
    get_timeline_store,
    reset_timeline_store,
)


class TestTimelineEvent:
    """Tests for TimelineEvent dataclass."""

    def test_create_event(self):
        """Test creating a timeline event."""
        event = TimelineEvent(
            event_id="test-123",
            strategy_id="my-strategy",
            timestamp=datetime.now(UTC),
            event_type="TRADE",
            description="Swapped 100 USDC for ETH",
            tx_hash="0x123abc",
            chain="arbitrum",
            details={"amount": "100", "token": "USDC"},
        )

        assert event.event_id == "test-123"
        assert event.strategy_id == "my-strategy"
        assert event.event_type == "TRADE"
        assert event.tx_hash == "0x123abc"
        assert event.chain == "arbitrum"
        assert event.details["amount"] == "100"

    def test_to_dict(self):
        """Test converting event to dictionary."""
        timestamp = datetime.now(UTC)
        event = TimelineEvent(
            event_id="test-123",
            strategy_id="my-strategy",
            timestamp=timestamp,
            event_type="TRADE",
            description="Test trade",
        )

        data = event.to_dict()
        assert data["event_id"] == "test-123"
        assert data["strategy_id"] == "my-strategy"
        assert data["timestamp"] == timestamp.isoformat()
        assert data["event_type"] == "TRADE"
        assert data["description"] == "Test trade"
        assert data["tx_hash"] is None
        assert data["chain"] is None
        assert data["details"] == {}

    def test_from_dict(self):
        """Test creating event from dictionary."""
        data = {
            "event_id": "test-456",
            "strategy_id": "other-strategy",
            "timestamp": "2024-01-15T10:30:00+00:00",
            "event_type": "ERROR",
            "description": "Transaction failed",
            "tx_hash": "0xabc",
            "chain": "base",
            "details": {"error": "Out of gas"},
        }

        event = TimelineEvent.from_dict(data)
        assert event.event_id == "test-456"
        assert event.strategy_id == "other-strategy"
        assert event.event_type == "ERROR"
        assert event.tx_hash == "0xabc"
        assert event.details["error"] == "Out of gas"


class TestTimelineStoreInMemory:
    """Tests for in-memory TimelineStore."""

    def test_initialize_in_memory(self):
        """Test initializing in-memory store."""
        store = TimelineStore(db_path=None)
        store.initialize()

        # Should start empty
        assert store.get_strategy_ids() == []

    def test_add_and_get_event(self):
        """Test adding and retrieving events."""
        store = TimelineStore(db_path=None)
        store.initialize()

        event = TimelineEvent(
            event_id=str(uuid4()),
            strategy_id="test-strategy",
            timestamp=datetime.now(UTC),
            event_type="TRADE",
            description="Test event",
        )

        store.add_event(event)

        events = store.get_events("test-strategy")
        assert len(events) == 1
        assert events[0].event_id == event.event_id
        assert events[0].event_type == "TRADE"

    def test_events_sorted_by_timestamp_descending(self):
        """Test that events are sorted by timestamp (newest first)."""
        store = TimelineStore(db_path=None)
        store.initialize()

        now = datetime.now(UTC)

        # Add events in random order
        event1 = TimelineEvent(
            event_id="1",
            strategy_id="test",
            timestamp=now - timedelta(hours=2),
            event_type="TRADE",
            description="Oldest",
        )
        event2 = TimelineEvent(
            event_id="2",
            strategy_id="test",
            timestamp=now,
            event_type="TRADE",
            description="Newest",
        )
        event3 = TimelineEvent(
            event_id="3",
            strategy_id="test",
            timestamp=now - timedelta(hours=1),
            event_type="TRADE",
            description="Middle",
        )

        store.add_event(event1)
        store.add_event(event2)
        store.add_event(event3)

        events = store.get_events("test")
        assert len(events) == 3
        assert events[0].event_id == "2"  # Newest first
        assert events[1].event_id == "3"  # Middle
        assert events[2].event_id == "1"  # Oldest last

    def test_filter_by_event_type(self):
        """Test filtering events by type."""
        store = TimelineStore(db_path=None)
        store.initialize()

        store.add_event(
            TimelineEvent(
                event_id="1",
                strategy_id="test",
                timestamp=datetime.now(UTC),
                event_type="TRADE",
                description="Trade 1",
            )
        )
        store.add_event(
            TimelineEvent(
                event_id="2",
                strategy_id="test",
                timestamp=datetime.now(UTC),
                event_type="ERROR",
                description="Error 1",
            )
        )
        store.add_event(
            TimelineEvent(
                event_id="3",
                strategy_id="test",
                timestamp=datetime.now(UTC),
                event_type="TRADE",
                description="Trade 2",
            )
        )

        trades = store.get_events("test", event_type="TRADE")
        assert len(trades) == 2
        assert all(e.event_type == "TRADE" for e in trades)

        errors = store.get_events("test", event_type="ERROR")
        assert len(errors) == 1
        assert errors[0].event_type == "ERROR"

    def test_filter_by_since_timestamp(self):
        """Test filtering events by timestamp."""
        store = TimelineStore(db_path=None)
        store.initialize()

        now = datetime.now(UTC)

        store.add_event(
            TimelineEvent(
                event_id="1",
                strategy_id="test",
                timestamp=now - timedelta(hours=3),
                event_type="TRADE",
                description="Old event",
            )
        )
        store.add_event(
            TimelineEvent(
                event_id="2",
                strategy_id="test",
                timestamp=now - timedelta(hours=1),
                event_type="TRADE",
                description="Recent event",
            )
        )
        store.add_event(
            TimelineEvent(
                event_id="3",
                strategy_id="test",
                timestamp=now,
                event_type="TRADE",
                description="New event",
            )
        )

        # Get events from last 2 hours
        since = now - timedelta(hours=2)
        events = store.get_events("test", since=since)
        assert len(events) == 2
        assert all(e.timestamp > since for e in events)

    def test_limit_events(self):
        """Test limiting number of events returned."""
        store = TimelineStore(db_path=None)
        store.initialize()

        # Add 10 events
        for i in range(10):
            store.add_event(
                TimelineEvent(
                    event_id=str(i),
                    strategy_id="test",
                    timestamp=datetime.now(UTC) - timedelta(minutes=i),
                    event_type="TRADE",
                    description=f"Event {i}",
                )
            )

        events = store.get_events("test", limit=5)
        assert len(events) == 5

    def test_get_recent_events_across_strategies(self):
        """Test getting recent events across all strategies."""
        store = TimelineStore(db_path=None)
        store.initialize()

        now = datetime.now(UTC)

        # Add events for multiple strategies
        store.add_event(
            TimelineEvent(
                event_id="1",
                strategy_id="strategy-a",
                timestamp=now - timedelta(minutes=10),
                event_type="TRADE",
                description="Event A",
            )
        )
        store.add_event(
            TimelineEvent(
                event_id="2",
                strategy_id="strategy-b",
                timestamp=now - timedelta(minutes=5),
                event_type="TRADE",
                description="Event B",
            )
        )
        store.add_event(
            TimelineEvent(
                event_id="3",
                strategy_id="strategy-a",
                timestamp=now,
                event_type="TRADE",
                description="Event A2",
            )
        )

        events = store.get_recent_events(limit=10)
        assert len(events) == 3
        # Should be sorted by timestamp (newest first)
        assert events[0].event_id == "3"
        assert events[1].event_id == "2"
        assert events[2].event_id == "1"

    def test_get_strategy_ids(self):
        """Test getting list of strategy IDs."""
        store = TimelineStore(db_path=None)
        store.initialize()

        store.add_event(
            TimelineEvent(
                event_id="1",
                strategy_id="strategy-a",
                timestamp=datetime.now(UTC),
                event_type="TRADE",
                description="Event A",
            )
        )
        store.add_event(
            TimelineEvent(
                event_id="2",
                strategy_id="strategy-b",
                timestamp=datetime.now(UTC),
                event_type="TRADE",
                description="Event B",
            )
        )

        ids = store.get_strategy_ids()
        assert len(ids) == 2
        assert "strategy-a" in ids
        assert "strategy-b" in ids

    def test_clear_single_strategy(self):
        """Test clearing events for a single strategy."""
        store = TimelineStore(db_path=None)
        store.initialize()

        store.add_event(
            TimelineEvent(
                event_id="1",
                strategy_id="strategy-a",
                timestamp=datetime.now(UTC),
                event_type="TRADE",
                description="Event A",
            )
        )
        store.add_event(
            TimelineEvent(
                event_id="2",
                strategy_id="strategy-b",
                timestamp=datetime.now(UTC),
                event_type="TRADE",
                description="Event B",
            )
        )

        store.clear_events("strategy-a")

        assert store.get_events("strategy-a") == []
        assert len(store.get_events("strategy-b")) == 1

    def test_clear_all_events(self):
        """Test clearing all events."""
        store = TimelineStore(db_path=None)
        store.initialize()

        store.add_event(
            TimelineEvent(
                event_id="1",
                strategy_id="strategy-a",
                timestamp=datetime.now(UTC),
                event_type="TRADE",
                description="Event A",
            )
        )
        store.add_event(
            TimelineEvent(
                event_id="2",
                strategy_id="strategy-b",
                timestamp=datetime.now(UTC),
                event_type="TRADE",
                description="Event B",
            )
        )

        store.clear_events()

        assert store.get_strategy_ids() == []


class TestTimelineStoreSQLite:
    """Tests for SQLite-backed TimelineStore."""

    def test_initialize_sqlite(self):
        """Test initializing SQLite-backed store."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "timeline.db"
            store = TimelineStore(db_path=db_path)
            store.initialize()

            assert db_path.exists()

    def test_persistence(self):
        """Test that events persist across store instances."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "timeline.db"

            # First store - add events
            store1 = TimelineStore(db_path=db_path)
            store1.initialize()

            event = TimelineEvent(
                event_id="persist-test",
                strategy_id="test-strategy",
                timestamp=datetime.now(UTC),
                event_type="TRADE",
                description="Persistent event",
                tx_hash="0xabc",
                chain="arbitrum",
                details={"key": "value"},
            )
            store1.add_event(event)
            store1.close()

            # Second store - should load events
            store2 = TimelineStore(db_path=db_path)
            store2.initialize()

            events = store2.get_events("test-strategy")
            assert len(events) == 1
            assert events[0].event_id == "persist-test"
            assert events[0].tx_hash == "0xabc"
            assert events[0].details["key"] == "value"

    def test_clear_events_sqlite(self):
        """Test clearing events from SQLite store."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "timeline.db"
            store = TimelineStore(db_path=db_path)
            store.initialize()

            store.add_event(
                TimelineEvent(
                    event_id="1",
                    strategy_id="test",
                    timestamp=datetime.now(UTC),
                    event_type="TRADE",
                    description="Event 1",
                )
            )

            store.clear_events("test")
            store.close()

            # Reopen and verify cleared
            store2 = TimelineStore(db_path=db_path)
            store2.initialize()
            assert store2.get_events("test") == []


class TestTimelineStoreThreadSafety:
    """Tests for thread safety of TimelineStore."""

    def test_concurrent_writes(self):
        """Test concurrent event additions."""
        store = TimelineStore(db_path=None)
        store.initialize()

        errors = []
        events_added = []

        def add_events(thread_id: int):
            try:
                for i in range(100):
                    event = TimelineEvent(
                        event_id=f"thread-{thread_id}-event-{i}",
                        strategy_id="test",
                        timestamp=datetime.now(UTC),
                        event_type="TRADE",
                        description=f"Event from thread {thread_id}",
                    )
                    store.add_event(event)
                    events_added.append(event.event_id)
            except Exception as e:
                errors.append(e)

        # Create and start threads
        threads = [threading.Thread(target=add_events, args=(i,)) for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Check no errors
        assert errors == [], f"Errors during concurrent writes: {errors}"

        # Check all events were added
        all_events = store.get_events("test", limit=1000)
        assert len(all_events) == 500  # 5 threads x 100 events

    def test_concurrent_reads_and_writes(self):
        """Test concurrent reads and writes."""
        store = TimelineStore(db_path=None)
        store.initialize()

        # Pre-populate with some events
        for i in range(50):
            store.add_event(
                TimelineEvent(
                    event_id=f"initial-{i}",
                    strategy_id="test",
                    timestamp=datetime.now(UTC),
                    event_type="TRADE",
                    description=f"Initial event {i}",
                )
            )

        errors = []
        read_counts = []

        def writer(thread_id: int):
            try:
                for i in range(50):
                    store.add_event(
                        TimelineEvent(
                            event_id=f"writer-{thread_id}-{i}",
                            strategy_id="test",
                            timestamp=datetime.now(UTC),
                            event_type="TRADE",
                            description=f"Event from writer {thread_id}",
                        )
                    )
            except Exception as e:
                errors.append(e)

        def reader(thread_id: int):
            try:
                for _ in range(50):
                    events = store.get_events("test", limit=100)
                    read_counts.append(len(events))
            except Exception as e:
                errors.append(e)

        # Start writers and readers
        threads = []
        for i in range(3):
            threads.append(threading.Thread(target=writer, args=(i,)))
            threads.append(threading.Thread(target=reader, args=(i,)))

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == [], f"Errors during concurrent operations: {errors}"
        # All reads should have gotten some events
        assert all(count > 0 for count in read_counts)


class TestTimelineStoreSingleton:
    """Tests for singleton accessor functions."""

    def test_get_timeline_store_singleton(self):
        """Test that get_timeline_store returns singleton."""
        reset_timeline_store()

        store1 = get_timeline_store()
        store2 = get_timeline_store()

        assert store1 is store2

    def test_reset_timeline_store(self):
        """Test resetting the singleton."""
        reset_timeline_store()

        store1 = get_timeline_store()
        store1.add_event(
            TimelineEvent(
                event_id="test",
                strategy_id="test",
                timestamp=datetime.now(UTC),
                event_type="TRADE",
                description="Test",
            )
        )

        reset_timeline_store()

        store2 = get_timeline_store()
        # New store should be empty (in-memory)
        assert store2.get_events("test") == []

    def teardown_method(self):
        """Reset singleton after each test."""
        reset_timeline_store()
