"""Tests for SQLiteLifecycleStore.

Tests cover:
- State CRUD: write/read, error state, upsert, not found, heartbeat count,
  heartbeat timestamp, state transitions
- Command CRUD: write/read, ack, no pending, multiple returns latest,
  ack leaves other commands, command fields
- Persistence: persistence across instances, idempotent initialize,
  commands persist across restart
- Thread safety: concurrent heartbeats, concurrent state writes
- Factory: create sqlite store, create postgres without plugin raises,
  singleton returns same instance, reset clears singleton
"""

import threading
from pathlib import Path
from unittest.mock import patch

import pytest

from almanak.gateway.lifecycle import (
    create_lifecycle_store,
    get_lifecycle_store,
    reset_lifecycle_store,
)
from almanak.gateway.lifecycle.sqlite_store import SQLiteLifecycleStore


@pytest.fixture
def store(tmp_path):
    db_path = tmp_path / "test_lifecycle.db"
    s = SQLiteLifecycleStore(db_path=db_path)
    s.initialize()
    yield s
    s.close()


@pytest.fixture(autouse=True)
def _reset_singleton():
    """Ensure singleton is reset before and after each test."""
    reset_lifecycle_store()
    yield
    reset_lifecycle_store()


class TestSQLiteLifecycleStoreState:
    """Tests for state CRUD operations."""

    def test_write_and_read_state(self, store):
        store.write_state("agent-1", "RUNNING")
        state = store.read_state("agent-1")
        assert state is not None
        assert state.agent_id == "agent-1"
        assert state.state == "RUNNING"
        assert state.error_message is None

    def test_write_state_with_error(self, store):
        store.write_state("agent-1", "ERROR", error_message="Something broke")
        state = store.read_state("agent-1")
        assert state.state == "ERROR"
        assert state.error_message == "Something broke"

    def test_write_state_upsert(self, store):
        store.write_state("agent-1", "INITIALIZING")
        store.write_state("agent-1", "RUNNING")
        state = store.read_state("agent-1")
        assert state.state == "RUNNING"

    def test_read_state_not_found(self, store):
        assert store.read_state("nonexistent") is None

    def test_heartbeat_increments_count(self, store):
        store.write_state("agent-1", "RUNNING")
        state_before = store.read_state("agent-1")
        store.heartbeat("agent-1")
        state_after = store.read_state("agent-1")
        assert state_after.iteration_count == state_before.iteration_count + 1

    def test_heartbeat_updates_timestamp(self, store):
        import time

        store.write_state("agent-1", "RUNNING")
        state_before = store.read_state("agent-1")
        time.sleep(0.01)  # Ensure measurable time passes
        store.heartbeat("agent-1")
        state_after = store.read_state("agent-1")
        assert state_after.last_heartbeat_at > state_before.last_heartbeat_at

    def test_state_transitions(self, store):
        """Test full lifecycle state machine."""
        transitions = ["INITIALIZING", "RUNNING", "PAUSED", "RUNNING", "STOPPING", "TERMINATED"]
        for state_name in transitions:
            store.write_state("agent-1", state_name)
            state = store.read_state("agent-1")
            assert state.state == state_name


class TestSQLiteLifecycleStoreCommands:
    """Tests for command CRUD operations."""

    def test_write_and_read_command(self, store):
        store.write_command("agent-1", "PAUSE", "operator@example.com")
        cmd = store.read_pending_command("agent-1")
        assert cmd is not None
        assert cmd.command == "PAUSE"
        assert cmd.issued_by == "operator@example.com"

    def test_ack_command(self, store):
        store.write_command("agent-1", "STOP", "admin")
        cmd = store.read_pending_command("agent-1")
        assert cmd is not None
        store.ack_command(cmd.id)
        # After ack, no pending command
        assert store.read_pending_command("agent-1") is None

    def test_no_pending_command(self, store):
        assert store.read_pending_command("agent-1") is None

    def test_multiple_commands_returns_latest(self, store):
        store.write_command("agent-1", "PAUSE", "admin")
        store.write_command("agent-1", "RESUME", "admin")
        cmd = store.read_pending_command("agent-1")
        assert cmd.command == "RESUME"  # Most recent (highest id)

    def test_ack_leaves_other_commands(self, store):
        """Acking one command doesn't affect others."""
        store.write_command("agent-1", "PAUSE", "admin")
        store.write_command("agent-2", "STOP", "admin")

        cmd1 = store.read_pending_command("agent-1")
        store.ack_command(cmd1.id)

        # agent-2's command should still be pending
        cmd2 = store.read_pending_command("agent-2")
        assert cmd2 is not None
        assert cmd2.command == "STOP"

    def test_command_fields(self, store):
        """Verify all fields are correctly stored and retrieved."""
        store.write_command("agent-1", "RESUME", "dashboard-user@test.com")
        cmd = store.read_pending_command("agent-1")
        assert cmd.agent_id == "agent-1"
        assert cmd.command == "RESUME"
        assert cmd.issued_by == "dashboard-user@test.com"
        assert cmd.issued_at is not None
        assert cmd.processed_at is None
        assert cmd.id > 0


class TestSQLiteLifecycleStorePersistence:
    """Tests for data persistence across store instances."""

    def test_persistence_across_instances(self, tmp_path):
        """Data survives close and re-open."""
        db_path = tmp_path / "persist.db"
        store1 = SQLiteLifecycleStore(db_path=db_path)
        store1.initialize()
        store1.write_state("agent-1", "RUNNING")
        store1.write_command("agent-1", "PAUSE", "admin")
        store1.close()

        store2 = SQLiteLifecycleStore(db_path=db_path)
        store2.initialize()
        state = store2.read_state("agent-1")
        assert state is not None
        assert state.state == "RUNNING"

        cmd = store2.read_pending_command("agent-1")
        assert cmd is not None
        assert cmd.command == "PAUSE"
        store2.close()

    def test_idempotent_initialize(self, store):
        """Calling initialize twice should not error."""
        store.initialize()
        store.write_state("agent-1", "RUNNING")
        assert store.read_state("agent-1").state == "RUNNING"

    def test_commands_persist_across_restart(self, tmp_path):
        """Commands survive store restart."""
        db_path = tmp_path / "restart.db"
        store1 = SQLiteLifecycleStore(db_path=db_path)
        store1.initialize()
        store1.write_command("agent-1", "STOP", "operator")
        store1.close()

        store2 = SQLiteLifecycleStore(db_path=db_path)
        store2.initialize()
        cmd = store2.read_pending_command("agent-1")
        assert cmd is not None
        assert cmd.command == "STOP"
        store2.close()


class TestSQLiteLifecycleStoreThreadSafety:
    """Tests for concurrent access safety."""

    def test_concurrent_heartbeats(self, store):
        """Multiple threads sending heartbeats concurrently."""
        store.write_state("agent-1", "RUNNING")

        errors = []

        def heartbeat_worker(n_beats):
            try:
                for _ in range(n_beats):
                    store.heartbeat("agent-1")
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=heartbeat_worker, args=(100,)) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        state = store.read_state("agent-1")
        assert state.iteration_count == 500  # 5 threads x 100 beats

    def test_concurrent_state_writes(self, store):
        """Multiple threads writing state concurrently."""
        errors = []

        def state_writer(agent_id, states):
            try:
                for s in states:
                    store.write_state(agent_id, s)
            except Exception as e:
                errors.append(e)

        threads = [
            threading.Thread(target=state_writer, args=(f"agent-{i}", ["RUNNING", "PAUSED", "RUNNING"]))
            for i in range(5)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        # All agents should have final state "RUNNING" (last write wins)
        for i in range(5):
            state = store.read_state(f"agent-{i}")
            assert state is not None
            assert state.state == "RUNNING"


class TestLifecycleFactory:
    """Tests for factory function and singleton accessor."""

    def test_create_sqlite_store(self, tmp_path):
        store = create_lifecycle_store(sqlite_path=str(tmp_path / "test.db"))
        assert isinstance(store, SQLiteLifecycleStore)

    def test_create_postgres_store_without_plugin_raises(self):
        with patch("importlib.metadata.entry_points", return_value=[]):
            with pytest.raises(RuntimeError, match="plugin is installed"):
                create_lifecycle_store(database_url="postgresql://localhost/test")

    def test_singleton_returns_same_instance(self, tmp_path):
        store1 = get_lifecycle_store(sqlite_path=str(tmp_path / "singleton.db"))
        store2 = get_lifecycle_store()
        assert store1 is store2

    def test_reset_clears_singleton(self, tmp_path):
        store1 = get_lifecycle_store(sqlite_path=str(tmp_path / "reset1.db"))
        reset_lifecycle_store()
        store2 = get_lifecycle_store(sqlite_path=str(tmp_path / "reset2.db"))
        assert store1 is not store2
