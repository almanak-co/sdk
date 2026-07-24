"""Branch coverage for ExecutionSessionStore recovery/cleanup queries.

Targets ``get_incomplete_sessions`` (terminal filtering, oldest-first
ordering, malformed-file tolerance, storage-scan failure) and
``cleanup_old_sessions`` (age threshold, keep_incomplete guard, delete
failure accounting). File-based storage under pytest's tmp_path — no
network, no shared state.
"""

import json
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pytest

from almanak.framework.execution.session import ExecutionSession
from almanak.framework.execution.session_store import ExecutionSessionStore


@pytest.fixture
def store(tmp_path):
    return ExecutionSessionStore(storage_path=str(tmp_path / "sessions"))


def _session(session_id: str, *, completed: bool = False, age: timedelta = timedelta(0)) -> ExecutionSession:
    now = datetime.now(UTC)
    session = ExecutionSession(
        session_id=session_id,
        deployment_id="deployment:abc123",
        intent_id=f"intent_{session_id}",
        completed=completed,
        success=completed,
        created_at=now - age,
        updated_at=now - age,
    )
    return session


def _write_raw(store: ExecutionSessionStore, session: ExecutionSession) -> None:
    """Write a session file directly, preserving its timestamps.

    ``store.save`` calls ``session.touch()`` which resets ``updated_at`` to
    now — useless for age-based cleanup tests, so aged sessions are written
    verbatim.
    """
    path = store._session_file_path(session.session_id)
    path.write_text(json.dumps(session.to_dict(), indent=2))


class TestGetIncompleteSessions:
    def test_filters_terminal_sessions_and_sorts_oldest_first(self, store):
        _write_raw(store, _session("newer_open", age=timedelta(hours=1)))
        _write_raw(store, _session("older_open", age=timedelta(hours=5)))
        _write_raw(store, _session("done", completed=True))

        incomplete = store.get_incomplete_sessions()

        assert [s.session_id for s in incomplete] == ["older_open", "newer_open"]

    def test_empty_store_returns_empty_list(self, store):
        assert store.get_incomplete_sessions() == []

    def test_malformed_json_file_is_skipped(self, store):
        _write_raw(store, _session("good"))
        (store.storage_path / "corrupt.json").write_text("{not valid json")

        incomplete = store.get_incomplete_sessions()

        assert [s.session_id for s in incomplete] == ["good"]

    def test_valid_json_with_bad_schema_is_skipped(self, store):
        _write_raw(store, _session("good"))
        # Decodes as JSON but ExecutionSession.from_dict raises (missing keys).
        (store.storage_path / "alien.json").write_text('{"foo": "bar"}')

        incomplete = store.get_incomplete_sessions()

        assert [s.session_id for s in incomplete] == ["good"]

    def test_storage_scan_failure_returns_empty_list(self, store, monkeypatch):
        broken_path = MagicMock()
        broken_path.glob.side_effect = OSError("permission denied")
        monkeypatch.setattr(store, "_storage_path", broken_path)

        assert store.get_incomplete_sessions() == []


class TestCleanupOldSessions:
    def test_deletes_only_old_completed_sessions(self, store):
        _write_raw(store, _session("old_done", completed=True, age=timedelta(days=8)))
        _write_raw(store, _session("fresh_done", completed=True, age=timedelta(hours=1)))
        _write_raw(store, _session("old_open", completed=False, age=timedelta(days=8)))

        deleted = store.cleanup_old_sessions()

        assert deleted == 1
        remaining = {s.session_id for s in store.get_all_sessions()}
        assert remaining == {"fresh_done", "old_open"}

    def test_keep_incomplete_false_also_deletes_old_incomplete(self, store):
        _write_raw(store, _session("old_done", completed=True, age=timedelta(days=8)))
        _write_raw(store, _session("old_open", completed=False, age=timedelta(days=8)))
        _write_raw(store, _session("fresh_open", completed=False, age=timedelta(hours=1)))

        deleted = store.cleanup_old_sessions(keep_incomplete=False)

        assert deleted == 2
        remaining = {s.session_id for s in store.get_all_sessions()}
        assert remaining == {"fresh_open"}

    def test_custom_max_age_tightens_the_window(self, store):
        _write_raw(store, _session("hour_old_done", completed=True, age=timedelta(hours=1)))

        assert store.cleanup_old_sessions(max_age_seconds=60) == 1
        assert store.get_all_sessions() == []

    def test_nothing_old_deletes_nothing(self, store):
        _write_raw(store, _session("fresh_done", completed=True, age=timedelta(hours=1)))

        assert store.cleanup_old_sessions() == 0
        assert len(store.get_all_sessions()) == 1

    def test_failed_delete_is_not_counted(self, store, monkeypatch):
        _write_raw(store, _session("old_done", completed=True, age=timedelta(days=8)))
        monkeypatch.setattr(store, "delete", lambda session_id: False)

        assert store.cleanup_old_sessions() == 0
