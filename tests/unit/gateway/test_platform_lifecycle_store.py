"""Hosted PostgreSQL lifecycle adapter typing contract (ALM-3080)."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.core.lifecycle import (
    LifecycleCommand,
    LifecycleState,
    LifecycleStateSource,
    LifecycleValueError,
)

_PLATFORM_PLUGINS_DIR = str(Path(__file__).resolve().parents[3] / "platform-plugins")
if _PLATFORM_PLUGINS_DIR not in sys.path:
    sys.path.insert(0, _PLATFORM_PLUGINS_DIR)

from almanak_platform.lifecycle_store import PostgresLifecycleStore  # noqa: E402


class _AcquireContext:
    def __init__(self, connection: AsyncMock) -> None:
        self._connection = connection

    async def __aenter__(self) -> AsyncMock:
        return self._connection

    async def __aexit__(self, *_args: object) -> None:
        return None


def _store_with_connection() -> tuple[PostgresLifecycleStore, AsyncMock]:
    store = PostgresLifecycleStore("postgresql://localhost/metrics")
    connection = AsyncMock()
    pool = MagicMock()
    pool.acquire.return_value = _AcquireContext(connection)
    store._pool = pool
    return store, connection


def test_public_writers_reject_untyped_and_historical_values_before_database_io() -> None:
    store = PostgresLifecycleStore("postgresql://localhost/metrics")
    store._submit = MagicMock()

    with pytest.raises(LifecycleValueError, match="untyped lifecycle state"):
        store.write_state("agent-1", "RUNNING")  # type: ignore[arg-type]
    with pytest.raises(LifecycleValueError, match="retired lifecycle state"):
        store.write_state("agent-1", LifecycleState.PAUSED)
    with pytest.raises(LifecycleValueError, match="platform-owned lifecycle state"):
        store.write_state("agent-1", LifecycleState.V2_PREPARING)
    with pytest.raises(LifecycleValueError, match="untyped lifecycle command"):
        store.write_command("agent-1", "STOP", "operator")  # type: ignore[arg-type]
    with pytest.raises(LifecycleValueError, match="retired lifecycle command"):
        store.write_command("agent-1", LifecycleCommand.RESUME, "operator")

    store._submit.assert_not_called()


@pytest.mark.asyncio
async def test_state_write_serializes_typed_values_without_schema_mutation() -> None:
    store, connection = _store_with_connection()

    await store._write_state("agent-1", LifecycleState.TEARING_DOWN, None, "2.16.0")

    query, deployment_id, state, error, source, version = connection.execute.await_args.args
    assert "ALTER TABLE" not in query
    assert deployment_id == "agent-1"
    assert state == "TEARING_DOWN"
    assert error is None
    assert source == "gateway"
    assert version == "2.16.0"


@pytest.mark.asyncio
async def test_stop_command_write_serializes_typed_value() -> None:
    """The PostgreSQL boundary persists the typed STOP command as wire text."""
    store, connection = _store_with_connection()

    await store._write_command("agent-1", LifecycleCommand.STOP, "operator")

    query, deployment_id, command, issued_by = connection.execute.await_args.args
    assert "INSERT INTO agent_command" in query
    assert deployment_id == "agent-1"
    assert command == "STOP"
    assert issued_by == "operator"


@pytest.mark.asyncio
async def test_platform_owned_state_and_source_rows_decode_to_typed_values() -> None:
    store, connection = _store_with_connection()
    connection.fetchrow.return_value = {
        "deployment_id": "agent-1",
        "state": "V2_DEPLOYING",
        "state_changed_at": MagicMock(),
        "last_heartbeat_at": None,
        "error_message": None,
        "iteration_count": 4,
        "source": "platform",
        "running_almanak_version": None,
    }

    state = await store._read_state("agent-1")

    assert state is not None
    assert state.state is LifecycleState.V2_DEPLOYING
    assert state.source is LifecycleStateSource.PLATFORM


@pytest.mark.asyncio
async def test_historical_command_row_decodes_without_becoming_enqueueable() -> None:
    store, connection = _store_with_connection()
    connection.fetchrow.return_value = {
        "id": 9,
        "deployment_id": "agent-1",
        "command": "RESUME",
        "issued_at": MagicMock(),
        "issued_by": "legacy-operator",
    }

    command = await store._read_pending_command("agent-1")

    assert command is not None
    assert command.command is LifecycleCommand.RESUME
    assert not command.command.is_enqueueable
