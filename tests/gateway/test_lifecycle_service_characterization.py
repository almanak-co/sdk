"""Characterization tests for ``almanak.gateway.services.lifecycle_service``.

LifecycleService is a thin gRPC pass-through to ``LifecycleStore`` (Protocol).
All business logic lives in the store; the servicer just maps gRPC ↔ Python
and enforces a small set of input validations.

Phase 5 prep: this file exists primarily to **demonstrate the canonical
service-test pattern** for the rest of Phase 5 (the gateway/services/ cluster).
The pattern:

  1. ``service``  fixture wires the servicer with a mock backend
                  (``MagicMock(spec=LifecycleStore)``).
  2. ``context``  fixture is a fake ``grpc.aio.ServicerContext`` from
                  ``tests.gateway.grpc_harness.make_grpc_context``.
  3. Each test constructs the request proto inline.
  4. Assertions use ``assert_grpc_error`` for the error path and direct field
     reads for the happy path.

Six RPCs × ~3 branches each = 19 tests covering all input-validation guards,
happy paths, and the ``except Exception:`` blocks that map backend failures to
appropriate gRPC status codes.

Brings ``lifecycle_service.py`` from 17% → ~95% on the unit-scope.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock

import grpc
import pytest

from almanak.gateway.lifecycle import AgentCommand, AgentState, LifecycleStore
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.lifecycle_service import LifecycleServiceServicer
from tests.gateway.grpc_harness import (
    assert_grpc_error,
    assert_set_code_not_called,
    make_grpc_context,
)

# ──────────────────────────────────────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────────────────────────────────────


@pytest.fixture
def store() -> MagicMock:
    """Mock LifecycleStore. Tests configure return values / side effects per case."""
    return MagicMock(spec=LifecycleStore)


@pytest.fixture
def service(store: MagicMock) -> LifecycleServiceServicer:
    """Servicer wired with the mock store."""
    return LifecycleServiceServicer(store=store)


@pytest.fixture
def context() -> MagicMock:
    """Fake ``grpc.aio.ServicerContext``."""
    return make_grpc_context()


# ──────────────────────────────────────────────────────────────────────────────
# WriteState
# ──────────────────────────────────────────────────────────────────────────────


class TestWriteState:
    @pytest.mark.parametrize("deployment_id", ["", "   "])
    @pytest.mark.asyncio
    async def test_empty_or_whitespace_deployment_id_returns_invalid_argument(
        self, service, store, context, deployment_id
    ):
        request = gateway_pb2.WriteAgentStateRequest(deployment_id=deployment_id, state="RUNNING")
        response = await service.WriteState(request, context)
        assert_grpc_error(
            context, response,
            expected_status=grpc.StatusCode.INVALID_ARGUMENT,
            error_substring="deployment_id",
        )
        store.write_state.assert_not_called()

    @pytest.mark.asyncio
    async def test_invalid_state_returns_invalid_argument(self, service, store, context):
        request = gateway_pb2.WriteAgentStateRequest(deployment_id="agt-1", state="NOT_A_STATE")
        response = await service.WriteState(request, context)
        assert_grpc_error(
            context, response,
            expected_status=grpc.StatusCode.INVALID_ARGUMENT,
            error_substring="invalid state",
        )
        store.write_state.assert_not_called()

    @pytest.mark.asyncio
    async def test_happy_path_returns_success_and_calls_store(self, service, store, context):
        request = gateway_pb2.WriteAgentStateRequest(deployment_id="agt-1", state="RUNNING")
        response = await service.WriteState(request, context)
        assert response.success is True
        assert_set_code_not_called(context)
        store.write_state.assert_called_once_with(
            deployment_id="agt-1",
            state="RUNNING",
            error_message=None,
            running_almanak_version=None,
        )

    @pytest.mark.asyncio
    async def test_error_message_passed_through_to_store(self, service, store, context):
        request = gateway_pb2.WriteAgentStateRequest(
            deployment_id="agt-1", state="ERROR", error_message="rpc timeout",
        )
        response = await service.WriteState(request, context)
        assert response.success is True
        store.write_state.assert_called_once_with(
            deployment_id="agt-1",
            state="ERROR",
            error_message="rpc timeout",
            running_almanak_version=None,
        )

    @pytest.mark.asyncio
    async def test_backend_exception_returns_internal_server_error(self, service, store, context):
        store.write_state.side_effect = RuntimeError("db connection lost")
        request = gateway_pb2.WriteAgentStateRequest(deployment_id="agt-1", state="RUNNING")
        response = await service.WriteState(request, context)
        assert response.success is False
        assert "internal server error" in response.error.lower()
        # WriteState's except branch does NOT set a gRPC status code (intentional —
        # the failure is logged and returned in the proto, not propagated as a transport error).
        assert_set_code_not_called(context)


# ──────────────────────────────────────────────────────────────────────────────
# ReadState
# ──────────────────────────────────────────────────────────────────────────────


class TestReadState:
    @pytest.mark.asyncio
    async def test_empty_deployment_id_returns_invalid_argument(self, service, store, context):
        request = gateway_pb2.ReadAgentStateRequest(deployment_id="")
        response = await service.ReadState(request, context)
        assert response.found is False
        context.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)
        context.set_details.assert_called_once_with("deployment_id must be non-empty")
        store.read_state.assert_not_called()

    @pytest.mark.asyncio
    async def test_unknown_agent_returns_found_false(self, service, store, context):
        store.read_state.return_value = None
        request = gateway_pb2.ReadAgentStateRequest(deployment_id="unknown")
        response = await service.ReadState(request, context)
        assert response.found is False
        assert_set_code_not_called(context)

    @pytest.mark.asyncio
    async def test_happy_path_returns_full_state(self, service, store, context):
        store.read_state.return_value = AgentState(
            deployment_id="agt-1",
            state="RUNNING",
            state_changed_at=datetime(2026, 5, 4, 12, 0, tzinfo=UTC),
            last_heartbeat_at=datetime(2026, 5, 4, 12, 1, tzinfo=UTC),
            error_message=None,
            iteration_count=42,
        )
        request = gateway_pb2.ReadAgentStateRequest(deployment_id="agt-1")
        response = await service.ReadState(request, context)
        assert response.found is True
        assert response.deployment_id == "agt-1"
        assert response.state == "RUNNING"
        assert response.state_changed_at == "2026-05-04T12:00:00+00:00"
        assert response.last_heartbeat_at == "2026-05-04T12:01:00+00:00"
        assert response.error_message == ""  # None → empty string
        assert response.iteration_count == 42
        assert_set_code_not_called(context)

    @pytest.mark.asyncio
    async def test_state_with_no_heartbeat_renders_empty_string(self, service, store, context):
        store.read_state.return_value = AgentState(
            deployment_id="agt-1",
            state="INITIALIZING",
            state_changed_at=datetime(2026, 5, 4, 12, 0, tzinfo=UTC),
            last_heartbeat_at=None,
            error_message="bootstrap failed",
            iteration_count=0,
        )
        request = gateway_pb2.ReadAgentStateRequest(deployment_id="agt-1")
        response = await service.ReadState(request, context)
        assert response.found is True
        assert response.last_heartbeat_at == ""
        assert response.error_message == "bootstrap failed"
        assert_set_code_not_called(context)

    @pytest.mark.asyncio
    async def test_backend_exception_returns_internal(self, service, store, context):
        store.read_state.side_effect = RuntimeError("db down")
        request = gateway_pb2.ReadAgentStateRequest(deployment_id="agt-1")
        response = await service.ReadState(request, context)
        assert response.found is False
        context.set_code.assert_called_once_with(grpc.StatusCode.INTERNAL)
        context.set_details.assert_called_once_with("failed to read agent state")


# ──────────────────────────────────────────────────────────────────────────────
# Heartbeat
# ──────────────────────────────────────────────────────────────────────────────


class TestHeartbeat:
    @pytest.mark.asyncio
    async def test_empty_deployment_id_returns_invalid_argument(self, service, store, context):
        request = gateway_pb2.HeartbeatRequest(deployment_id="")
        response = await service.Heartbeat(request, context)
        assert_grpc_error(
            context, response,
            expected_status=grpc.StatusCode.INVALID_ARGUMENT,
            error_substring="deployment_id",
        )
        store.heartbeat.assert_not_called()

    @pytest.mark.asyncio
    async def test_happy_path(self, service, store, context):
        request = gateway_pb2.HeartbeatRequest(deployment_id="agt-1")
        response = await service.Heartbeat(request, context)
        assert response.success is True
        store.heartbeat.assert_called_once_with("agt-1")
        assert_set_code_not_called(context)

    @pytest.mark.asyncio
    async def test_backend_exception_returns_internal(self, service, store, context):
        store.heartbeat.side_effect = RuntimeError("db down")
        request = gateway_pb2.HeartbeatRequest(deployment_id="agt-1")
        response = await service.Heartbeat(request, context)
        assert response.success is False
        context.set_code.assert_called_once_with(grpc.StatusCode.INTERNAL)
        # Heartbeat's exception branch sets the gRPC code but intentionally
        # omits set_details (the failure is also returned in the response.error
        # field). Pin that deviation from the ReadState/ReadCommand pattern.
        context.set_details.assert_not_called()


# ──────────────────────────────────────────────────────────────────────────────
# ReadCommand
# ──────────────────────────────────────────────────────────────────────────────


class TestReadCommand:
    @pytest.mark.asyncio
    async def test_empty_deployment_id_returns_invalid_argument(self, service, store, context):
        request = gateway_pb2.ReadAgentCommandRequest(deployment_id="")
        response = await service.ReadCommand(request, context)
        assert response.found is False
        context.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)
        context.set_details.assert_called_once_with("deployment_id must be non-empty")
        store.read_pending_command.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_pending_command_returns_found_false(self, service, store, context):
        store.read_pending_command.return_value = None
        request = gateway_pb2.ReadAgentCommandRequest(deployment_id="agt-1")
        response = await service.ReadCommand(request, context)
        assert response.found is False
        assert_set_code_not_called(context)

    @pytest.mark.asyncio
    async def test_happy_path_returns_command_fields(self, service, store, context):
        store.read_pending_command.return_value = AgentCommand(
            id=42,
            deployment_id="agt-1",
            command="STOP",
            issued_at=datetime(2026, 5, 4, 12, 0, tzinfo=UTC),
            issued_by="operator@team",
        )
        request = gateway_pb2.ReadAgentCommandRequest(deployment_id="agt-1")
        response = await service.ReadCommand(request, context)
        assert response.found is True
        assert response.command_id == 42
        assert response.deployment_id == "agt-1"
        assert response.command == "STOP"
        assert response.issued_at == "2026-05-04T12:00:00+00:00"
        assert response.issued_by == "operator@team"
        store.read_pending_command.assert_called_once_with("agt-1")
        assert_set_code_not_called(context)

    @pytest.mark.asyncio
    async def test_backend_exception_returns_internal(self, service, store, context):
        store.read_pending_command.side_effect = RuntimeError("db down")
        request = gateway_pb2.ReadAgentCommandRequest(deployment_id="agt-1")
        response = await service.ReadCommand(request, context)
        assert response.found is False
        context.set_code.assert_called_once_with(grpc.StatusCode.INTERNAL)
        context.set_details.assert_called_once_with("failed to read agent command")


# ──────────────────────────────────────────────────────────────────────────────
# AckCommand
# ──────────────────────────────────────────────────────────────────────────────


class TestAckCommand:
    @pytest.mark.asyncio
    async def test_zero_command_id_returns_invalid_argument(self, service, store, context):
        # command_id is an int — falsy 0 trips the guard.
        request = gateway_pb2.AckAgentCommandRequest(command_id=0)
        response = await service.AckCommand(request, context)
        assert_grpc_error(
            context, response,
            expected_status=grpc.StatusCode.INVALID_ARGUMENT,
            error_substring="command_id",
        )
        store.ack_command.assert_not_called()

    @pytest.mark.asyncio
    async def test_happy_path(self, service, store, context):
        request = gateway_pb2.AckAgentCommandRequest(command_id=42)
        response = await service.AckCommand(request, context)
        assert response.success is True
        store.ack_command.assert_called_once_with(42)
        assert_set_code_not_called(context)

    @pytest.mark.asyncio
    async def test_backend_exception_returns_internal(self, service, store, context):
        store.ack_command.side_effect = RuntimeError("db down")
        request = gateway_pb2.AckAgentCommandRequest(command_id=42)
        response = await service.AckCommand(request, context)
        assert response.success is False
        context.set_code.assert_called_once_with(grpc.StatusCode.INTERNAL)
        # AckCommand's exception branch sets the gRPC code but intentionally
        # omits set_details (mirrors Heartbeat — failure is also returned in
        # response.error). Pin that deviation from the Read* pattern.
        context.set_details.assert_not_called()


# ──────────────────────────────────────────────────────────────────────────────
# WriteCommand
# ──────────────────────────────────────────────────────────────────────────────


class TestWriteCommand:
    @pytest.mark.asyncio
    async def test_empty_deployment_id_returns_invalid_argument(self, service, store, context):
        request = gateway_pb2.WriteAgentCommandRequest(
            deployment_id="", command="STOP", issued_by="op",
        )
        response = await service.WriteCommand(request, context)
        assert_grpc_error(
            context, response,
            expected_status=grpc.StatusCode.INVALID_ARGUMENT,
            error_substring="deployment_id",
        )
        store.write_command.assert_not_called()

    @pytest.mark.asyncio
    async def test_invalid_command_returns_invalid_argument(self, service, store, context):
        request = gateway_pb2.WriteAgentCommandRequest(
            deployment_id="agt-1", command="LAUNCH_NUKES", issued_by="op",
        )
        response = await service.WriteCommand(request, context)
        assert_grpc_error(
            context, response,
            expected_status=grpc.StatusCode.INVALID_ARGUMENT,
            error_substring="invalid command",
        )
        store.write_command.assert_not_called()

    @pytest.mark.parametrize("command", ["STOP"])
    @pytest.mark.asyncio
    async def test_each_valid_command_accepted(self, service, store, context, command):
        request = gateway_pb2.WriteAgentCommandRequest(
            deployment_id="agt-1", command=command, issued_by="op@team",
        )
        response = await service.WriteCommand(request, context)
        assert response.success is True
        store.write_command.assert_called_once_with(
            deployment_id="agt-1", command=command, issued_by="op@team",
        )
        assert_set_code_not_called(context)

    @pytest.mark.parametrize("command", ["PAUSE", "RESUME"])
    @pytest.mark.asyncio
    async def test_retired_commands_rejected(self, service, store, context, command):
        """VIB-4281: PAUSE / RESUME no longer accepted."""
        request = gateway_pb2.WriteAgentCommandRequest(
            deployment_id="agt-1", command=command, issued_by="op@team",
        )
        response = await service.WriteCommand(request, context)
        assert_grpc_error(
            context, response,
            expected_status=grpc.StatusCode.INVALID_ARGUMENT,
            error_substring="invalid command",
        )
        store.write_command.assert_not_called()

    @pytest.mark.asyncio
    async def test_backend_exception_returns_internal_error(self, service, store, context):
        store.write_command.side_effect = RuntimeError("db down")
        request = gateway_pb2.WriteAgentCommandRequest(
            deployment_id="agt-1", command="STOP", issued_by="op",
        )
        response = await service.WriteCommand(request, context)
        assert response.success is False
        assert "internal server error" in response.error.lower()
        # Like WriteState, this branch does NOT set a gRPC status code.
        assert_set_code_not_called(context)
