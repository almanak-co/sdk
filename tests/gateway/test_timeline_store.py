"""Tests for the gateway TimelineStore.

Tests cover:
- TimelineStore initialization (in-memory and SQLite)
- Adding and retrieving events
- Filtering by event type and timestamp
- Recent events across all strategies
- Clearing events
- Persistence and reload from SQLite
- Thread safety with concurrent operations
- Deployed-mode identity pass-through
"""

import logging
import tempfile
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch
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
            deployment_id="my-strategy",
            timestamp=datetime.now(UTC),
            event_type="TRADE",
            description="Swapped 100 USDC for ETH",
            tx_hash="0x123abc",
            chain="arbitrum",
            details={"amount": "100", "token": "USDC"},
        )

        assert event.event_id == "test-123"
        assert event.deployment_id == "my-strategy"
        assert event.event_type == "TRADE"
        assert event.tx_hash == "0x123abc"
        assert event.chain == "arbitrum"
        assert event.details["amount"] == "100"

    def test_to_dict(self):
        """Test converting event to dictionary."""
        timestamp = datetime.now(UTC)
        event = TimelineEvent(
            event_id="test-123",
            deployment_id="my-strategy",
            timestamp=timestamp,
            event_type="TRADE",
            description="Test trade",
        )

        data = event.to_dict()
        assert data["event_id"] == "test-123"
        assert data["deployment_id"] == "my-strategy"
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
            "deployment_id": "other-strategy",
            "timestamp": "2024-01-15T10:30:00+00:00",
            "event_type": "ERROR",
            "description": "Transaction failed",
            "tx_hash": "0xabc",
            "chain": "base",
            "details": {"error": "Out of gas"},
        }

        event = TimelineEvent.from_dict(data)
        assert event.event_id == "test-456"
        assert event.deployment_id == "other-strategy"
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
        assert store.get_deployment_ids() == []

    def test_add_and_get_event(self):
        """Test adding and retrieving events."""
        store = TimelineStore(db_path=None)
        store.initialize()

        event = TimelineEvent(
            event_id=str(uuid4()),
            deployment_id="test-strategy",
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
            deployment_id="test",
            timestamp=now - timedelta(hours=2),
            event_type="TRADE",
            description="Oldest",
        )
        event2 = TimelineEvent(
            event_id="2",
            deployment_id="test",
            timestamp=now,
            event_type="TRADE",
            description="Newest",
        )
        event3 = TimelineEvent(
            event_id="3",
            deployment_id="test",
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
                deployment_id="test",
                timestamp=datetime.now(UTC),
                event_type="TRADE",
                description="Trade 1",
            )
        )
        store.add_event(
            TimelineEvent(
                event_id="2",
                deployment_id="test",
                timestamp=datetime.now(UTC),
                event_type="ERROR",
                description="Error 1",
            )
        )
        store.add_event(
            TimelineEvent(
                event_id="3",
                deployment_id="test",
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
                deployment_id="test",
                timestamp=now - timedelta(hours=3),
                event_type="TRADE",
                description="Old event",
            )
        )
        store.add_event(
            TimelineEvent(
                event_id="2",
                deployment_id="test",
                timestamp=now - timedelta(hours=1),
                event_type="TRADE",
                description="Recent event",
            )
        )
        store.add_event(
            TimelineEvent(
                event_id="3",
                deployment_id="test",
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
                    deployment_id="test",
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
                deployment_id="strategy-a",
                timestamp=now - timedelta(minutes=10),
                event_type="TRADE",
                description="Event A",
            )
        )
        store.add_event(
            TimelineEvent(
                event_id="2",
                deployment_id="strategy-b",
                timestamp=now - timedelta(minutes=5),
                event_type="TRADE",
                description="Event B",
            )
        )
        store.add_event(
            TimelineEvent(
                event_id="3",
                deployment_id="strategy-a",
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

    def test_get_deployment_ids(self):
        """Test getting list of deployment IDs."""
        store = TimelineStore(db_path=None)
        store.initialize()

        store.add_event(
            TimelineEvent(
                event_id="1",
                deployment_id="strategy-a",
                timestamp=datetime.now(UTC),
                event_type="TRADE",
                description="Event A",
            )
        )
        store.add_event(
            TimelineEvent(
                event_id="2",
                deployment_id="strategy-b",
                timestamp=datetime.now(UTC),
                event_type="TRADE",
                description="Event B",
            )
        )

        ids = store.get_deployment_ids()
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
                deployment_id="strategy-a",
                timestamp=datetime.now(UTC),
                event_type="TRADE",
                description="Event A",
            )
        )
        store.add_event(
            TimelineEvent(
                event_id="2",
                deployment_id="strategy-b",
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
                deployment_id="strategy-a",
                timestamp=datetime.now(UTC),
                event_type="TRADE",
                description="Event A",
            )
        )
        store.add_event(
            TimelineEvent(
                event_id="2",
                deployment_id="strategy-b",
                timestamp=datetime.now(UTC),
                event_type="TRADE",
                description="Event B",
            )
        )

        store.clear_events()

        assert store.get_deployment_ids() == []


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
                deployment_id="test-strategy",
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
                    deployment_id="test",
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
                        deployment_id="test",
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
                    deployment_id="test",
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
                            deployment_id="test",
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


class TestTimelineStoreIdentityKeying:
    """Tests that the TimelineStore keys events on the canonical identity.

    Per blueprint 29 the TimelineStore performs NO identity translation on
    either backend: ``event.deployment_id`` (already the canonical
    ``deployment_id``) is the cache/DB key as-is. VIB-4722 removed the old
    ``_resolve_deployment_id`` hosted-env rewrite path.
    """

    def test_resolve_deployment_id_method_is_removed(self):
        """The identity-translation helper no longer exists (blueprint 29)."""
        assert not hasattr(TimelineStore, "_resolve_deployment_id")

    def test_inmemory_store_keys_on_deployment_id(self):
        """In-memory store caches under the event's deployment_id verbatim."""
        store = TimelineStore(db_path=None)  # No database_url → in-memory
        store.initialize()

        event = TimelineEvent(
            event_id="test-1",
            deployment_id="deployment:abc123def456",
            timestamp=datetime.now(UTC),
            event_type="TRADE",
            description="Test",
        )
        store.add_event(event)

        assert "deployment:abc123def456" in store.get_deployment_ids()
        assert len(store.get_events("deployment:abc123def456")) == 1

    def test_sqlite_store_keys_on_deployment_id(self):
        """SQLite store caches under the event's deployment_id verbatim."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "timeline.db"
            store = TimelineStore(db_path=db_path)
            store.initialize()

            event = TimelineEvent(
                event_id="test-1",
                deployment_id="deployment:abc123def456",
                timestamp=datetime.now(UTC),
                event_type="TRADE",
                description="Test",
            )
            store.add_event(event)

            assert "deployment:abc123def456" in store.get_deployment_ids()
            assert len(store.get_events("deployment:abc123def456")) == 1

    def test_postgres_backend_keys_on_deployment_id_no_translation(self):
        """PostgreSQL backend keys the cache on deployment_id with NO rewrite.

        We cannot connect to PostgreSQL in unit tests, so the PG persist
        call is mocked. The point of the test is the cache key: blueprint 29
        forbids any identity translation, so the cache must be keyed by the
        event's deployment_id exactly — the same as the local backends.
        """
        store = TimelineStore(database_url="postgres://fake:5432/test")
        store._initialized = True
        store._pg_pool = True  # Truthy sentinel — we'll mock the persist call

        event = TimelineEvent(
            event_id="test-1",
            deployment_id="platform-agent-uuid-123",
            timestamp=datetime.now(UTC),
            event_type="TRADE",
            description="Test",
        )

        with patch.object(store, "_persist_event_postgres"):
            store.add_event(event)

        # Cache is keyed by the event's deployment_id — no translation.
        assert "platform-agent-uuid-123" in store.get_deployment_ids()
        events = store.get_events("platform-agent-uuid-123")
        assert len(events) == 1
        assert events[0].event_id == "test-1"

    def test_postgres_clear_events_keys_on_deployment_id(self):
        """clear_events on the PG backend uses the deployment_id verbatim."""
        store = TimelineStore(database_url="postgres://fake:5432/test")
        store._initialized = True
        store._pg_pool = True

        event = TimelineEvent(
            event_id="test-1",
            deployment_id="platform-agent-uuid-123",
            timestamp=datetime.now(UTC),
            event_type="TRADE",
            description="Test",
        )

        with patch.object(store, "_persist_event_postgres"):
            store.add_event(event)

        with patch.object(store, "_clear_events_postgres") as mock_clear:
            store.clear_events("platform-agent-uuid-123")
            mock_clear.assert_called_once_with("platform-agent-uuid-123")

        assert store.get_events("platform-agent-uuid-123") == []


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
                deployment_id="test",
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


class _FakePgPool:
    """Capture the SQL and args _async_load_events sends to Postgres."""

    def __init__(self):
        self.queries: list[tuple[str, tuple]] = []

    def acquire(self):
        pool = self

        class _Ctx:
            async def __aenter__(self):
                class _Conn:
                    async def fetch(self, sql, *args):
                        pool.queries.append((sql, args))
                        return []

                return _Conn()

            async def __aexit__(self, *exc):
                return False

        return _Ctx()


class TestTimelineStoreStartupLoadBounds:
    """The Postgres startup load must be deployment-scoped and row-capped.

    August 2026 Cloud NAT incident: the unscoped, unlimited SELECT loaded the
    platform-wide timeline_events table (2.5M rows) into every gateway
    sidecar at boot, OOM-crashlooping 55 dashboards which re-downloaded the
    table every 5 minutes (~7 TiB/day of NAT traffic).
    """

    def _run_load(self, store: TimelineStore) -> tuple[str, tuple]:
        import asyncio

        pool = _FakePgPool()
        store._pg_pool = pool  # type: ignore[assignment]
        asyncio.run(store._async_load_events())
        assert len(pool.queries) == 1
        return pool.queries[0]

    def test_scoped_load_filters_by_deployment_and_caps_rows(self):
        store = TimelineStore(
            database_url="postgres://fake:5432/test",
            scope_deployment_id="dep-abc",
            startup_load_limit=123,
        )
        sql, args = self._run_load(store)
        assert "WHERE deployment_id = $1" in sql
        assert "LIMIT $2" in sql
        assert args == ("dep-abc", 123)

    def test_unscoped_load_still_caps_rows(self):
        store = TimelineStore(
            database_url="postgres://fake:5432/test",
            startup_load_limit=456,
        )
        sql, args = self._run_load(store)
        assert "WHERE" not in sql
        assert "LIMIT $1" in sql
        assert args == (456,)


class TestLoadFromPostgres:
    """Branch coverage for the boot-cache hydration wrapper.

    ``_load_from_postgres`` owns three behaviors the CRAP gate requires
    pinned: cache keying by deployment_id, the truncation warning when the
    startup cap is hit, and swallow-and-log on load failure (a broken
    metrics DB must not stop the gateway from booting).
    """

    def _event(self, event_id: str, deployment_id: str) -> TimelineEvent:
        return TimelineEvent(
            event_id=event_id,
            deployment_id=deployment_id,
            timestamp=datetime.now(UTC),
            event_type="TRADE",
            description="Test",
        )

    def _store(self, **kwargs) -> TimelineStore:
        store = TimelineStore(database_url="postgres://fake:5432/test", **kwargs)
        # Mark initialized so cache reads don't trigger a real PG connect
        # (same sentinel style as TestTimelineStoreIdentityKeying).
        store._initialized = True
        return store

    def test_load_populates_cache_keyed_by_deployment_id(self):
        store = self._store()
        events = [self._event("e1", "dep-a"), self._event("e2", "dep-b"), self._event("e3", "dep-a")]
        with patch.object(store, "_pg_submit", return_value=events):
            store._load_from_postgres()
        assert sorted(store.get_deployment_ids()) == ["dep-a", "dep-b"]
        assert {e.event_id for e in store.get_events("dep-a")} == {"e1", "e3"}

    def test_truncation_warning_when_cap_hit(self, caplog):
        store = self._store(startup_load_limit=2)
        events = [self._event("e1", "dep-a"), self._event("e2", "dep-a")]
        with (
            patch.object(store, "_pg_submit", return_value=events),
            caplog.at_level(logging.WARNING, logger="almanak.gateway.timeline.store"),
        ):
            store._load_from_postgres()
        assert any("cap" in r.getMessage() for r in caplog.records)

    def test_no_truncation_warning_below_cap(self, caplog):
        store = self._store(startup_load_limit=10)
        with (
            patch.object(store, "_pg_submit", return_value=[self._event("e1", "dep-a")]),
            caplog.at_level(logging.WARNING, logger="almanak.gateway.timeline.store"),
        ):
            store._load_from_postgres()
        assert not any("cap" in r.getMessage() for r in caplog.records)

    def test_empty_load_logs_nothing_and_caches_nothing(self, caplog):
        store = self._store()
        with (
            patch.object(store, "_pg_submit", return_value=[]),
            caplog.at_level(logging.INFO, logger="almanak.gateway.timeline.store"),
        ):
            store._load_from_postgres()
        assert store.get_deployment_ids() == []
        assert not any("Loaded" in r.getMessage() for r in caplog.records)

    def test_load_failure_is_swallowed(self):
        store = self._store()
        with patch.object(store, "_pg_submit", side_effect=RuntimeError("db down")):
            store._load_from_postgres()  # must not raise — gateway still boots
        assert store.get_deployment_ids() == []


class TestTruncatedHistoryFallback:
    """PR 3560 review (P1): capping the boot cache must not truncate reads.

    When the startup load hit the row cap, pages that may extend past the
    cached window fall back to a scoped, filtered PostgreSQL read; pages
    fully inside the cached window keep being served from memory.
    """

    def _event(self, event_id: str, ts: datetime) -> TimelineEvent:
        return TimelineEvent(
            event_id=event_id,
            deployment_id="dep-a",
            timestamp=ts,
            event_type="TRADE",
            description="Test",
        )

    def _truncated_store(self, floor: datetime, cached: list[TimelineEvent]) -> TimelineStore:
        store = TimelineStore(database_url="postgres://fake:5432/test")
        store._initialized = True
        store._pg_history_truncated = True
        store._pg_cache_floor = floor
        for e in cached:
            store._cache[e.deployment_id].append(e)
        return store

    def test_cursor_below_floor_falls_back_to_postgres(self):
        now = datetime.now(UTC)
        floor = now - timedelta(hours=1)
        store = self._truncated_store(floor, [self._event("cached", now)])
        db_events = [self._event("from-db", now - timedelta(hours=5))]
        with patch.object(store, "_pg_submit", return_value=db_events) as submit:
            page = store.get_events("dep-a", limit=50, before=floor)
        submit.assert_called_once()
        # PR 3560 review: the fallback must fail well inside the 30s gRPC
        # client deadline so degradation happens instead of DEADLINE_EXCEEDED.
        # The 5.0 contract is pinned — changing the constant is a deliberate
        # decision that must touch this test.
        assert submit.call_args.kwargs["timeout"] == TimelineStore.HISTORY_FALLBACK_TIMEOUT_SECONDS
        assert TimelineStore.HISTORY_FALLBACK_TIMEOUT_SECONDS == 5.0
        assert [e.event_id for e in page] == ["from-db"]

    def test_full_cache_page_does_not_touch_postgres(self):
        now = datetime.now(UTC)
        floor = now - timedelta(hours=1)
        store = self._truncated_store(floor, [self._event(f"e{i}", now - timedelta(minutes=i)) for i in range(5)])
        with patch.object(store, "_pg_submit") as submit:
            page = store.get_events("dep-a", limit=5)
        submit.assert_not_called()
        assert len(page) == 5

    def test_window_inside_cache_does_not_touch_postgres(self):
        now = datetime.now(UTC)
        floor = now - timedelta(hours=1)
        store = self._truncated_store(floor, [self._event("cached", now)])
        with patch.object(store, "_pg_submit") as submit:
            page = store.get_events("dep-a", limit=50, since=floor)
        submit.assert_not_called()
        assert [e.event_id for e in page] == ["cached"]

    def test_untruncated_store_never_falls_back(self):
        store = TimelineStore(database_url="postgres://fake:5432/test")
        store._initialized = True
        with patch.object(store, "_pg_submit") as submit:
            page = store.get_events("dep-a", limit=50)
        submit.assert_not_called()
        assert page == []

    def test_postgres_failure_degrades_to_cached_page(self):
        now = datetime.now(UTC)
        floor = now - timedelta(hours=1)
        store = self._truncated_store(floor, [self._event("cached", now)])
        with patch.object(store, "_pg_submit", side_effect=RuntimeError("db down")):
            page = store.get_events("dep-a", limit=50)
        assert [e.event_id for e in page] == ["cached"]

    def test_fetch_query_shape_all_filters(self):
        import asyncio

        store = TimelineStore(database_url="postgres://fake:5432/test")
        pool = _FakePgPool()
        store._pg_pool = pool
        now = datetime.now(UTC)
        asyncio.run(store._async_fetch_events("dep-a", 25, "TRADE", now - timedelta(days=1), now))
        sql, args = pool.queries[0]
        assert "WHERE deployment_id = $1 AND event_type = $2 AND timestamp > $3 AND timestamp < $4" in sql
        assert "LIMIT $5" in sql
        assert args == ("dep-a", "TRADE", now - timedelta(days=1), now, 25)

    def test_fetch_query_shape_no_filters(self):
        import asyncio

        store = TimelineStore(database_url="postgres://fake:5432/test")
        pool = _FakePgPool()
        store._pg_pool = pool
        asyncio.run(store._async_fetch_events("dep-a", 50, None, None, None))
        sql, args = pool.queries[0]
        assert "WHERE deployment_id = $1" in sql
        # No other predicate: a second one would be joined with " AND ".
        assert " AND " not in sql
        assert "LIMIT $2" in sql
        assert args == ("dep-a", 50)

    def test_constructor_clamps_non_positive_limit(self, caplog):
        with caplog.at_level(logging.WARNING, logger="almanak.gateway.timeline.store"):
            store = TimelineStore(database_url="postgres://fake:5432/test", startup_load_limit=0)
        assert store._startup_load_limit == 10000
        assert any("must be > 0" in r.getMessage() for r in caplog.records)


class TestRowToEvent:
    """Row mapping branches: details_json variants and the related-ledger gate."""

    def _row(self, **overrides):
        row = {
            "event_id": "e1",
            "deployment_id": "dep-a",
            "timestamp": datetime.now(UTC),
            "event_type": "TRADE",
            "description": "Test",
            "tx_hash": None,
            "chain": None,
            "details_json": None,
            "cycle_id": "",
            "phase": "",
        }
        row.update(overrides)
        return row

    def _store(self, supports_related: bool = False) -> TimelineStore:
        store = TimelineStore(database_url="postgres://fake:5432/test")
        store._pg_supports_related_ledger = supports_related
        return store

    def test_details_json_string(self):
        event = self._store()._row_to_event(self._row(details_json='{"amount": "100"}'))
        assert event.details == {"amount": "100"}

    def test_details_json_invalid_string_ignored(self):
        event = self._store()._row_to_event(self._row(details_json="not json"))
        assert event.details == {}

    def test_details_json_dict_passthrough(self):
        event = self._store()._row_to_event(self._row(details_json={"k": "v"}))
        assert event.details == {"k": "v"}

    def test_details_json_none(self):
        event = self._store()._row_to_event(self._row())
        assert event.details == {}

    def test_empty_description_normalized(self):
        event = self._store()._row_to_event(self._row(description=None))
        assert event.description == ""

    def test_related_ledger_gated(self):
        row = self._row(related_ledger_entry_id="ledger-7")
        assert self._store(supports_related=True)._row_to_event(row).related_ledger_entry_id == "ledger-7"
        assert self._store(supports_related=False)._row_to_event(row).related_ledger_entry_id == ""


class TestPgSubmitTimeout:
    """_pg_submit must honor its per-call timeout and cancel the abandoned
    future so a slow query cannot keep loading the DB behind the caller."""

    def _store_with_loop(self):
        import asyncio as _asyncio

        store = TimelineStore(database_url="postgres://fake:5432/test")
        loop = _asyncio.new_event_loop()
        thread = threading.Thread(target=loop.run_forever, daemon=True)
        thread.start()
        store._pg_loop = loop
        return store, loop

    def test_timeout_raises_against_real_loop(self):
        import asyncio as _asyncio

        store, loop = self._store_with_loop()
        try:
            with pytest.raises(TimeoutError):
                store._pg_submit(_asyncio.sleep(30), timeout=0.05)
        finally:
            loop.call_soon_threadsafe(loop.stop)

    def test_timeout_cancels_the_abandoned_future(self):
        """Removing future.cancel() from _pg_submit must fail this test —
        an abandoned query would otherwise keep loading the DB."""
        store = TimelineStore(database_url="postgres://fake:5432/test")
        store._pg_loop = MagicMock()
        fake_future = MagicMock()
        fake_future.result.side_effect = TimeoutError
        with (
            patch(
                "almanak.gateway.timeline.store.asyncio.run_coroutine_threadsafe",
                return_value=fake_future,
            ),
            pytest.raises(TimeoutError),
        ):
            store._pg_submit(MagicMock(), timeout=0.05)
        fake_future.result.assert_called_once_with(timeout=0.05)
        fake_future.cancel.assert_called_once()

    def test_result_returned_within_timeout(self):
        async def _quick():
            return "ok"

        store, loop = self._store_with_loop()
        try:
            assert store._pg_submit(_quick(), timeout=5) == "ok"
        finally:
            loop.call_soon_threadsafe(loop.stop)
