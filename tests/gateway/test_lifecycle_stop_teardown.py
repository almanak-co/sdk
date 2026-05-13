"""Tests for lifecycle store state transitions (store-level, no runner).

Tests cover:
- STOP command state machine: RUNNING -> STOPPING -> TERMINATED
- STOP -> TEARING_DOWN -> TERMINATED via the hosted teardown bridge
- ERROR state with error messages
- Heartbeat during lifecycle transitions

VIB-4281: PAUSE / RESUME / PAUSED are no longer part of the lifecycle vocabulary.
"""

import pytest

from almanak.gateway.lifecycle.sqlite_store import SQLiteLifecycleStore


@pytest.fixture
def store(tmp_path):
    db_path = tmp_path / "test_lifecycle.db"
    s = SQLiteLifecycleStore(db_path=db_path)
    s.initialize()
    yield s
    s.close()


class TestStopCommandLifecycle:
    """Test STOP command state transitions via the SQLite store directly."""

    def test_stop_command_state_machine(self, store):
        """STOP command follows RUNNING -> STOPPING -> TERMINATED."""
        agent_id = "test-agent-1"

        # Agent starts RUNNING
        store.write_state(agent_id, "RUNNING")
        assert store.read_state(agent_id).state == "RUNNING"

        # Operator issues STOP command
        store.write_command(agent_id, "STOP", "operator")
        cmd = store.read_pending_command(agent_id)
        assert cmd is not None
        assert cmd.command == "STOP"

        # Agent reads and acks command
        store.ack_command(cmd.id)
        assert store.read_pending_command(agent_id) is None

        # Agent transitions to STOPPING
        store.write_state(agent_id, "STOPPING")
        assert store.read_state(agent_id).state == "STOPPING"

        # Agent finishes and transitions to TERMINATED
        store.write_state(agent_id, "TERMINATED")
        assert store.read_state(agent_id).state == "TERMINATED"

    def test_stop_teardown_terminated_flow(self, store):
        """STOP routes through STOPPING -> TEARING_DOWN -> TERMINATED."""
        agent_id = "test-agent-2"

        store.write_state(agent_id, "RUNNING")

        # STOP
        store.write_command(agent_id, "STOP", "operator")
        cmd = store.read_pending_command(agent_id)
        store.ack_command(cmd.id)
        store.write_state(agent_id, "STOPPING")
        # Hosted teardown bridge writes TEARING_DOWN once unwind starts.
        store.write_state(agent_id, "TEARING_DOWN")
        store.write_state(agent_id, "TERMINATED")
        assert store.read_state(agent_id).state == "TERMINATED"

    def test_error_state_with_message(self, store):
        """ERROR state includes error message."""
        agent_id = "test-agent-4"
        store.write_state(agent_id, "RUNNING")
        store.write_state(agent_id, "ERROR", error_message="Too many consecutive errors")

        state = store.read_state(agent_id)
        assert state.state == "ERROR"
        assert state.error_message == "Too many consecutive errors"

    def test_heartbeat_during_lifecycle(self, store):
        """Heartbeats continue to work during lifecycle transitions."""
        agent_id = "test-agent-5"
        store.write_state(agent_id, "RUNNING")

        # Send some heartbeats
        for _ in range(5):
            store.heartbeat(agent_id)

        state = store.read_state(agent_id)
        assert state.iteration_count == 5

        # Heartbeat after state change still works
        store.write_state(agent_id, "STOPPING")
        store.heartbeat(agent_id)
        # Note: heartbeat doesn't change state, only timestamp and count
        state = store.read_state(agent_id)
        assert state.state == "STOPPING"
