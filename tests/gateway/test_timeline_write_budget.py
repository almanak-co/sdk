"""Execution telemetry bounds database waits without changing default store behavior."""

import asyncio
import sqlite3
import threading
import time
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from almanak.framework.execution.events import ExecutionEventType
from almanak.framework.execution.orchestrator import ExecutionContext, ExecutionOrchestrator
from almanak.gateway.timeline import store as timeline


def event():
    return timeline.TimelineEvent("id", "deployment", datetime.now(UTC), "CUSTOM", "observed")


def test_bounded_write_never_initializes_database(monkeypatch):
    store = timeline.TimelineStore(database_url="postgresql://unused")
    initialize = MagicMock()
    monkeypatch.setattr(store, "initialize", initialize)
    with pytest.raises(RuntimeError, match="initialized"):
        store.add_event(event(), timeout=0.1)
    initialize.assert_not_called()
    monkeypatch.setattr(timeline, "_timeline_store", store)
    with pytest.raises(RuntimeError, match="initialized"):
        timeline.get_initialized_timeline_store()


def test_lock_wait_is_bounded_and_does_not_append():
    store = timeline.TimelineStore()
    store.initialize()
    acquired, release = threading.Event(), threading.Event()

    def hold():
        with store._lock:
            acquired.set()
            release.wait(5)

    thread = threading.Thread(target=hold)
    thread.start()
    assert acquired.wait(2)
    try:
        started = time.monotonic()
        with pytest.raises(TimeoutError, match="lock budget"):
            store.add_event(event(), timeout=0.05)
        assert time.monotonic() - started < 1
        assert not store._cache
    finally:
        release.set()
        thread.join(2)


def test_sqlite_busy_wait_is_bounded_and_cache_semantics_are_preserved(tmp_path):
    path = tmp_path / "timeline.db"
    store = timeline.TimelineStore(path)
    store.initialize()
    with sqlite3.connect(path) as blocker:
        blocker.execute("BEGIN EXCLUSIVE")
        started = time.monotonic()
        with pytest.raises(sqlite3.OperationalError, match="locked"):
            store.add_event(event(), timeout=0.05)
        assert time.monotonic() - started < 1
        assert len(store._cache["deployment"]) == 1
    with sqlite3.connect(path) as reader:
        assert reader.execute("SELECT COUNT(*) FROM timeline_events").fetchone()[0] == 0
    store.add_event(event(), timeout=0.2)
    with sqlite3.connect(path) as reader:
        assert reader.execute("SELECT COUNT(*) FROM timeline_events").fetchone()[0] == 1


def test_lock_and_database_share_one_budget(monkeypatch):
    store = timeline.TimelineStore(database_url="postgresql://unused")
    store._initialized = True
    clock = iter([10.0, 10.1, 11.5])
    monkeypatch.setattr(timeline, "time", SimpleNamespace(monotonic=lambda: next(clock)))
    persist = MagicMock()
    monkeypatch.setattr(store, "_persist_event_postgres", persist)
    item = event()
    store.add_event(item, timeout=2)
    persist.assert_called_once_with(item, "deployment", timeout=0.5)


def test_postgres_timeout_cancels_future_and_retains_nonfatal_cache(monkeypatch, caplog):
    store = timeline.TimelineStore(database_url="postgresql://unused")
    store._initialized = True
    store._pg_loop = MagicMock()
    future = MagicMock()
    future.result.side_effect = TimeoutError("budget")

    def submit(coro, loop):
        coro.close()
        return future

    monkeypatch.setattr(asyncio, "run_coroutine_threadsafe", submit)
    store.add_event(event(), timeout=0.05)
    future.cancel.assert_called_once_with()
    assert 0 < future.result.call_args.kwargs["timeout"] <= 0.05
    assert len(store._cache["deployment"]) == 1
    assert "Failed to persist timeline event" in caplog.text


@pytest.mark.parametrize("backend", ["sqlite", "postgres"])
def test_default_store_call_preserves_unbudgeted_override(backend, monkeypatch, tmp_path):
    store = timeline.TimelineStore(tmp_path / "timeline.db" if backend == "sqlite" else None)
    store.initialize()
    if backend == "postgres":
        store._database_url = "postgresql://unused"
    persist = MagicMock()
    monkeypatch.setattr(store, f"_persist_event_{backend}", persist)
    item = event()
    store.add_event(item)
    persist.assert_called_once_with(*((item, "deployment") if backend == "postgres" else (item,)))


def test_failed_sink_is_nonfatal_sanitized_and_callback_order_is_preserved(caplog):
    order = []

    def fail(item):
        order.append("sink")
        raise RuntimeError("private-database-credential")

    runner = ExecutionOrchestrator(
        signer=MagicMock(),
        submitter=MagicMock(),
        simulator=None,
        chain="bsc",
        timeline_sink=fail,
        event_callback=lambda *args: order.append("callback"),
    )
    runner._emit_event(ExecutionEventType.SIMULATING, ExecutionContext(deployment_id="deployment", chain="bsc"))
    assert order == ["sink", "callback"]
    assert "non-fatal): RuntimeError deployment=deployment event=CUSTOM" in caplog.text
    assert "private-database-credential" not in caplog.text


def test_sink_cancellation_is_preserved():
    runner = ExecutionOrchestrator(signer=MagicMock(), submitter=MagicMock(), simulator=None, chain="bsc")
    runner._timeline_sink = MagicMock(side_effect=asyncio.CancelledError())
    with pytest.raises(asyncio.CancelledError):
        runner._emit_event(ExecutionEventType.SIMULATING, ExecutionContext(deployment_id="deployment", chain="bsc"))


def test_native_empty_identity_refuses_write_but_preserves_callback(monkeypatch):
    from almanak.gateway.services.execution_service import ExecutionServiceServicer

    accessor = MagicMock()
    monkeypatch.setattr(timeline, "get_initialized_timeline_store", accessor)
    callback = MagicMock()
    runner = ExecutionOrchestrator(
        signer=MagicMock(),
        submitter=MagicMock(),
        simulator=None,
        chain="bsc",
        timeline_sink=ExecutionServiceServicer._persist_execution_timeline_event,
        event_callback=callback,
    )
    runner._emit_event(ExecutionEventType.SIMULATING, ExecutionContext(deployment_id="", chain="bsc"))
    accessor.assert_not_called()
    callback.assert_called_once()


def test_native_ingestion_preserves_fields_and_precision_and_detaches_details(tmp_path, monkeypatch):
    from almanak.framework.api.timeline import TimelineEvent, TimelineEventType
    from almanak.gateway.services.execution_service import ExecutionServiceServicer

    store = timeline.TimelineStore(tmp_path / "events.db")
    store.initialize()
    monkeypatch.setattr(timeline, "_timeline_store", store)
    observed = TimelineEvent(
        timestamp=datetime(2026, 1, 2, 3, 4, 5, 678, tzinfo=UTC),
        event_type=TimelineEventType.TRANSACTION_CONFIRMED,
        description="canonical receipt",
        deployment_id="authorized-deployment",
        chain="bsc",
        tx_hash="0x123",
        details={"amount": "0", "nested": {"status": "confirmed"}},
        cycle_id="original-cycle",
        phase="execute",
        related_ledger_entry_id="ledger",
    )
    ExecutionServiceServicer._persist_execution_timeline_event(observed)
    observed.details["nested"]["status"] = "changed"
    saved = store._cache["authorized-deployment"][0]
    assert saved.timestamp == observed.timestamp
    assert saved.details == {"amount": "0", "nested": {"status": "confirmed"}}
    assert (saved.event_type, saved.description, saved.chain, saved.tx_hash) == (
        "TRANSACTION_CONFIRMED",
        "canonical receipt",
        "bsc",
        "0x123",
    )
    assert (saved.cycle_id, saved.phase, saved.related_ledger_entry_id) == ("original-cycle", "execute", "ledger")
    reopened = timeline.TimelineStore(tmp_path / "events.db")
    reopened.initialize()
    assert reopened._cache["authorized-deployment"][0].to_dict() == saved.to_dict()


def test_sqlite_refreshes_insert_and_commit_budgets_and_closes(tmp_path, monkeypatch):
    store = timeline.TimelineStore(tmp_path / "timeline.db")
    store.initialize()
    observed = []

    class ObservedConnection(sqlite3.Connection):
        def execute(self, sql, *args):
            if sql.lstrip().startswith("INSERT"):
                observed.append(("insert", super().execute("PRAGMA busy_timeout").fetchone()[0]))
            return super().execute(sql, *args)

        def commit(self):
            observed.append(("commit", super().execute("PRAGMA busy_timeout").fetchone()[0]))
            return super().commit()

    conn = sqlite3.connect(store._db_path, factory=ObservedConnection)
    monkeypatch.setattr(timeline, "sqlite3", SimpleNamespace(connect=lambda *args, **kwargs: conn))
    monkeypatch.setattr(timeline, "_remaining_wait", MagicMock(side_effect=[0.8, 0.4, 0.2]))
    store._persist_event_sqlite(event(), deadline=10)
    assert observed == [("insert", 400), ("commit", 200)]
    with pytest.raises(sqlite3.ProgrammingError, match="closed"):
        conn.execute("SELECT 1")
    with sqlite3.connect(store._db_path) as reader:
        assert reader.execute("SELECT COUNT(*) FROM timeline_events").fetchone()[0] == 1


def test_sqlite_expired_commit_budget_rolls_back_and_closes(tmp_path, monkeypatch):
    store = timeline.TimelineStore(tmp_path / "timeline.db")
    store.initialize()
    conn = sqlite3.connect(store._db_path)
    monkeypatch.setattr(timeline, "sqlite3", SimpleNamespace(connect=lambda *args, **kwargs: conn))
    monkeypatch.setattr(timeline, "_remaining_wait", MagicMock(side_effect=[1, 1, TimeoutError("budget exhausted")]))
    with pytest.raises(TimeoutError, match="budget exhausted"):
        store._persist_event_sqlite(event(), deadline=10)
    with pytest.raises(sqlite3.ProgrammingError, match="closed"):
        conn.execute("SELECT 1")
    with sqlite3.connect(store._db_path) as reader:
        assert reader.execute("SELECT COUNT(*) FROM timeline_events").fetchone()[0] == 0
