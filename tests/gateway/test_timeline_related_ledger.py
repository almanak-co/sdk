"""Round-trip tests for `timeline_events.related_ledger_entry_id` (VIB-4041).

The typed correlation column is the formal way for UX consumers to navigate
from a timeline event back to its source-of-truth row in `transaction_ledger`,
without putting money-shaped data in `details_json`.

Round-trip surface validated here:

  Producer   →  add_event(TimelineEvent(related_ledger_entry_id=...))
  Storage    →  SQLite INSERT / SELECT preserves the field
  Reload     →  fresh TimelineStore re-reads it from disk
  Wire       →  RecordTimelineEventRequest carries the field
  Wire       →  TimelineEventInfo returns the field
  Dashboard  →  GatewayDashboardClient._convert_timeline_event preserves it

If any of these break, a UX consumer that wants to render "this card refers
to ledger row X" silently loses the link and falls back to scraping
`details_json` — which is exactly what PRD-TimelineEvents §6.1 forbids.
"""

from __future__ import annotations

import sqlite3
import tempfile
from datetime import UTC, datetime
from pathlib import Path

import pytest

from almanak.gateway.proto import gateway_pb2
from almanak.gateway.timeline.store import TimelineEvent, TimelineStore


@pytest.fixture
def sqlite_store() -> tuple[TimelineStore, Path]:
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "timeline.db"
        store = TimelineStore(db_path=db_path)
        store.initialize()
        try:
            yield store, db_path
        finally:
            store.close()


def _make_event(related_ledger_entry_id: str = "") -> TimelineEvent:
    return TimelineEvent(
        event_id="evt-1",
        strategy_id="strat-1",
        timestamp=datetime.now(UTC),
        event_type="POSITION_OPENED",
        description="LP_OPEN landed on-chain",
        tx_hash="0xabc",
        chain="arbitrum",
        details={},
        cycle_id="cyc-1",
        phase="EXECUTE",
        related_ledger_entry_id=related_ledger_entry_id,
    )


class TestDataclass:
    def test_default_is_empty_string(self) -> None:
        evt = TimelineEvent(
            event_id="e",
            strategy_id="s",
            timestamp=datetime.now(UTC),
            event_type="STATE_CHANGE",
            description="d",
        )
        assert evt.related_ledger_entry_id == ""

    def test_to_dict_omits_when_empty(self) -> None:
        evt = _make_event(related_ledger_entry_id="")
        d = evt.to_dict()
        assert "related_ledger_entry_id" not in d

    def test_to_dict_includes_when_set(self) -> None:
        evt = _make_event(related_ledger_entry_id="ledger-42")
        d = evt.to_dict()
        assert d["related_ledger_entry_id"] == "ledger-42"

    def test_from_dict_round_trip(self) -> None:
        original = _make_event(related_ledger_entry_id="ledger-99")
        rebuilt = TimelineEvent.from_dict(original.to_dict() | {"strategy_id": original.strategy_id})
        assert rebuilt.related_ledger_entry_id == "ledger-99"


class TestSQLiteRoundTrip:
    def test_insert_select_preserves_field(self, sqlite_store) -> None:
        store, _ = sqlite_store
        evt = _make_event(related_ledger_entry_id="ledger-7")
        store.add_event(evt)

        events = store.get_events("strat-1", limit=10)
        assert len(events) == 1
        assert events[0].related_ledger_entry_id == "ledger-7"

    def test_field_persists_across_reload(self, sqlite_store) -> None:
        store, db_path = sqlite_store
        evt = _make_event(related_ledger_entry_id="ledger-7")
        store.add_event(evt)
        store.close()

        fresh = TimelineStore(db_path=db_path)
        fresh.initialize()
        try:
            events = fresh.get_events("strat-1", limit=10)
            assert len(events) == 1
            assert events[0].related_ledger_entry_id == "ledger-7"
        finally:
            fresh.close()

    def test_ddl_creates_column_and_index(self, sqlite_store) -> None:
        _, db_path = sqlite_store
        with sqlite3.connect(str(db_path)) as conn:
            cols = {row[1] for row in conn.execute("PRAGMA table_info(timeline_events)")}
            assert "related_ledger_entry_id" in cols

            idx_names = {row[1] for row in conn.execute("PRAGMA index_list(timeline_events)")}
            assert "idx_timeline_related_ledger" in idx_names

    def test_legacy_db_without_column_is_migrated(self) -> None:
        """A pre-VIB-4041 SQLite file must gain the column on initialize()."""
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "legacy.db"
            with sqlite3.connect(str(db_path)) as conn:
                conn.execute(
                    """
                    CREATE TABLE timeline_events (
                        event_id TEXT PRIMARY KEY,
                        strategy_id TEXT NOT NULL,
                        timestamp TEXT NOT NULL,
                        event_type TEXT NOT NULL,
                        description TEXT,
                        tx_hash TEXT,
                        chain TEXT,
                        details_json TEXT,
                        cycle_id TEXT DEFAULT '',
                        phase TEXT DEFAULT ''
                    )
                    """
                )
                conn.commit()

            store = TimelineStore(db_path=db_path)
            try:
                store.initialize()
                with sqlite3.connect(str(db_path)) as conn:
                    cols = {row[1] for row in conn.execute("PRAGMA table_info(timeline_events)")}
                    assert "related_ledger_entry_id" in cols
            finally:
                store.close()


class TestProtoSurface:
    def test_record_request_carries_field(self) -> None:
        req = gateway_pb2.RecordTimelineEventRequest(
            strategy_id="s",
            event_type="POSITION_OPENED",
            description="d",
            related_ledger_entry_id="ledger-7",
        )
        assert req.related_ledger_entry_id == "ledger-7"

    def test_event_info_carries_field(self) -> None:
        info = gateway_pb2.TimelineEventInfo(
            event_type="POSITION_OPENED",
            description="d",
            related_ledger_entry_id="ledger-7",
            cycle_id="cyc-1",
            phase="EXECUTE",
        )
        assert info.related_ledger_entry_id == "ledger-7"
        assert info.cycle_id == "cyc-1"
        assert info.phase == "EXECUTE"

    def test_event_info_default_is_empty_string(self) -> None:
        info = gateway_pb2.TimelineEventInfo(event_type="STATE_CHANGE", description="d")
        assert info.related_ledger_entry_id == ""


class TestDashboardClientConversion:
    def test_convert_preserves_field(self) -> None:
        from almanak.framework.dashboard.gateway_client import GatewayDashboardClient

        proto = gateway_pb2.TimelineEventInfo(
            timestamp=int(datetime.now(UTC).timestamp()),
            event_type="POSITION_OPENED",
            description="d",
            tx_hash="0xabc",
            chain="arbitrum",
            details_json="",
            cycle_id="cyc-1",
            phase="EXECUTE",
            related_ledger_entry_id="ledger-7",
        )

        client = GatewayDashboardClient.__new__(GatewayDashboardClient)
        evt = client._convert_timeline_event(proto)

        assert evt.cycle_id == "cyc-1"
        assert evt.phase == "EXECUTE"
        assert evt.related_ledger_entry_id == "ledger-7"


class TestPostgresCapabilityGate:
    """CodeRabbit on PR #2117: ``_pg_supports_related_ledger`` decides whether
    deployed gateways read/write the correlation column at all.

    The hosted Postgres branch was previously validated only via integration
    runs and boot-log inspection. A regression here would silently drop
    ``related_ledger_entry_id`` in production while every SQLite-backed unit
    test stayed green. These tests stub the introspection result and pin both
    branches (column present / column absent) of the read and write paths.
    """

    @pytest.mark.asyncio
    async def test_detect_returns_true_when_column_present(self) -> None:
        from unittest.mock import AsyncMock, MagicMock

        from almanak.gateway.timeline.store import TimelineStore

        store = TimelineStore.__new__(TimelineStore)
        # information_schema query returns a non-None row → column present.
        conn = MagicMock()
        conn.fetchval = AsyncMock(return_value=1)
        pool = MagicMock()
        pool.acquire = MagicMock(return_value=_AsyncCtx(conn))
        store._pg_pool = pool

        assert await store._async_detect_related_ledger_column() is True

    @pytest.mark.asyncio
    async def test_detect_returns_false_when_column_absent(self) -> None:
        from unittest.mock import AsyncMock, MagicMock

        from almanak.gateway.timeline.store import TimelineStore

        store = TimelineStore.__new__(TimelineStore)
        # information_schema query returns None → column absent.
        conn = MagicMock()
        conn.fetchval = AsyncMock(return_value=None)
        pool = MagicMock()
        pool.acquire = MagicMock(return_value=_AsyncCtx(conn))
        store._pg_pool = pool

        assert await store._async_detect_related_ledger_column() is False

    @pytest.mark.asyncio
    async def test_detect_propagates_infrastructure_failure(self) -> None:
        """CodeRabbit on PR #2117 (round 5): the previous behaviour was to
        catch every exception and silently return False. That conflated two
        very different signals — "column legitimately absent" (clean None
        from information_schema) and "infrastructure broken" (connection
        lost, permission denied, timeout, asyncpg internal error). Silent
        degradation in the latter case would survive every SQLite-backed
        unit test while disabling correlation writes in a live deployment,
        which is exactly the failure mode this PR is designed to prevent.

        New contract: clean information_schema lookup with no row → False
        (graceful, pre-VIB-4051 host). Anything else propagates and fails
        gateway boot loudly.
        """
        from unittest.mock import AsyncMock, MagicMock

        from almanak.gateway.timeline.store import TimelineStore

        store = TimelineStore.__new__(TimelineStore)
        conn = MagicMock()
        conn.fetchval = AsyncMock(side_effect=RuntimeError("introspection blew up"))
        pool = MagicMock()
        pool.acquire = MagicMock(return_value=_AsyncCtx(conn))
        store._pg_pool = pool

        with pytest.raises(RuntimeError, match="introspection blew up"):
            await store._async_detect_related_ledger_column()

    @pytest.mark.asyncio
    async def test_load_query_includes_column_only_when_supported(self) -> None:
        """The SELECT projection MUST include ``related_ledger_entry_id``
        only when the capability gate says the column exists, and MUST omit
        it otherwise."""
        from unittest.mock import AsyncMock, MagicMock

        from almanak.gateway.timeline.store import TimelineStore

        # When gate is True, the SELECT projection must include the column.
        store = TimelineStore.__new__(TimelineStore)
        store._pg_supports_related_ledger = True
        conn = MagicMock()
        conn.fetch = AsyncMock(return_value=[])
        pool = MagicMock()
        pool.acquire = MagicMock(return_value=_AsyncCtx(conn))
        store._pg_pool = pool
        await store._async_load_events()
        sql_when_present = conn.fetch.await_args.args[0]
        assert "related_ledger_entry_id" in sql_when_present, (
            "When the capability gate is True, the SELECT projection must include the typed correlation column."
        )

        # When gate is False, the SELECT must NOT reference the column —
        # otherwise the query would fail on a hosted Postgres without the
        # migration.
        store._pg_supports_related_ledger = False
        conn.fetch.reset_mock()
        await store._async_load_events()
        sql_when_absent = conn.fetch.await_args.args[0]
        assert "related_ledger_entry_id" not in sql_when_absent, (
            "When the capability gate is False, the SELECT must omit the "
            "column entirely (the column doesn't exist on hosted Postgres "
            "pre-VIB-4051)."
        )

    @pytest.mark.asyncio
    async def test_persist_query_handles_both_branches(self) -> None:
        """The INSERT path has two SQL variants — one with the column, one
        without. Both must be exercised by tests so a regression in either
        is caught at CI."""
        from unittest.mock import AsyncMock, MagicMock

        from almanak.gateway.timeline.store import TimelineStore

        event = TimelineEvent(
            event_id="evt-pg",
            strategy_id="strat-pg",
            timestamp=datetime.now(UTC),
            event_type="STATE_CHANGE",
            description="d",
            related_ledger_entry_id="lg-9",
        )

        # Branch A: column present — INSERT must include the column +
        # bind the event's value.
        store = TimelineStore.__new__(TimelineStore)
        store._pg_supports_related_ledger = True
        conn = MagicMock()
        conn.execute = AsyncMock(return_value=None)
        pool = MagicMock()
        pool.acquire = MagicMock(return_value=_AsyncCtx(conn))
        store._pg_pool = pool
        await store._async_persist_event(event, "strat-pg")
        assert conn.execute.await_args is not None
        sql_a = conn.execute.await_args.args[0]
        bound_a = conn.execute.await_args.args[1:]
        assert "related_ledger_entry_id" in sql_a
        assert "lg-9" in bound_a, "Branch A must bind the event's related_ledger_entry_id value."

        # Branch B: column absent — INSERT must omit the column AND the
        # event's value must NOT be bound (otherwise asyncpg would raise on
        # a parameter-count mismatch in production).
        store._pg_supports_related_ledger = False
        conn.execute.reset_mock()
        await store._async_persist_event(event, "strat-pg")
        sql_b = conn.execute.await_args.args[0]
        bound_b = conn.execute.await_args.args[1:]
        assert "related_ledger_entry_id" not in sql_b
        assert "lg-9" not in bound_b, (
            "Branch B must NOT bind the related_ledger_entry_id value when "
            "the column is absent — otherwise asyncpg raises on parameter "
            "count mismatch."
        )


class _AsyncCtx:
    """Minimal async context manager that yields a fixed value for ``async with``."""

    def __init__(self, value: object) -> None:
        self._value = value

    async def __aenter__(self) -> object:
        return self._value

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        return None
