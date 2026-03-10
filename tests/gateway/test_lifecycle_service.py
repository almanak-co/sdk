"""Tests for the gateway LifecycleServiceServicer.

Tests cover:
- WriteState success and error handling
- ReadState found and not found
- Heartbeat success
- ReadCommand found and not found
- AckCommand success
- WriteCommand success
- Input validation (INVALID_ARGUMENT for malformed inputs)
- Backend failures (INTERNAL with sanitized error payloads)
"""

from unittest.mock import MagicMock

import grpc
import pytest

from almanak.gateway.lifecycle.sqlite_store import SQLiteLifecycleStore
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.lifecycle_service import LifecycleServiceServicer


@pytest.fixture
def store(tmp_path):
    db_path = tmp_path / "test_lifecycle.db"
    s = SQLiteLifecycleStore(db_path=db_path)
    s.initialize()
    yield s
    s.close()


@pytest.fixture
def mock_context():
    """Create mock gRPC context."""
    context = MagicMock(spec=grpc.aio.ServicerContext)
    return context


@pytest.fixture
def service(store):
    """Create LifecycleServiceServicer with real SQLite store."""
    return LifecycleServiceServicer(store=store)


class TestLifecycleServiceWriteState:
    @pytest.mark.asyncio
    async def test_write_state_success(self, service, store, mock_context):
        request = gateway_pb2.WriteAgentStateRequest(
            agent_id="agent-1",
            state="RUNNING",
        )
        response = await service.WriteState(request, mock_context)
        assert response.success is True

        # Verify state was written
        state = store.read_state("agent-1")
        assert state.state == "RUNNING"

    @pytest.mark.asyncio
    async def test_write_state_with_error_message(self, service, store, mock_context):
        request = gateway_pb2.WriteAgentStateRequest(
            agent_id="agent-1",
            state="ERROR",
            error_message="Connection timeout",
        )
        response = await service.WriteState(request, mock_context)
        assert response.success is True

        state = store.read_state("agent-1")
        assert state.state == "ERROR"
        assert state.error_message == "Connection timeout"

    @pytest.mark.asyncio
    async def test_write_state_error_handling(self, mock_context):
        """WriteState handles store errors gracefully."""
        broken_store = MagicMock()
        broken_store.write_state.side_effect = RuntimeError("DB locked")
        service = LifecycleServiceServicer(store=broken_store)

        request = gateway_pb2.WriteAgentStateRequest(
            agent_id="agent-1",
            state="RUNNING",
        )
        response = await service.WriteState(request, mock_context)
        assert response.success is False
        assert response.error  # generic error, no backend details exposed


class TestLifecycleServiceReadState:
    @pytest.mark.asyncio
    async def test_read_state_found(self, service, store, mock_context):
        store.write_state("agent-1", "RUNNING")

        request = gateway_pb2.ReadAgentStateRequest(agent_id="agent-1")
        response = await service.ReadState(request, mock_context)
        assert response.found is True
        assert response.agent_id == "agent-1"
        assert response.state == "RUNNING"
        assert response.iteration_count == 0

    @pytest.mark.asyncio
    async def test_read_state_not_found(self, service, mock_context):
        request = gateway_pb2.ReadAgentStateRequest(agent_id="nonexistent")
        response = await service.ReadState(request, mock_context)
        assert response.found is False


class TestLifecycleServiceHeartbeat:
    @pytest.mark.asyncio
    async def test_heartbeat_success(self, service, store, mock_context):
        store.write_state("agent-1", "RUNNING")

        request = gateway_pb2.HeartbeatRequest(agent_id="agent-1")
        response = await service.Heartbeat(request, mock_context)
        assert response.success is True

        # Verify iteration count incremented
        state = store.read_state("agent-1")
        assert state.iteration_count == 1


    @pytest.mark.asyncio
    async def test_heartbeat_error_handling(self, mock_context):
        """Heartbeat handles store errors gracefully."""
        broken_store = MagicMock()
        broken_store.heartbeat.side_effect = RuntimeError("DB locked")
        service = LifecycleServiceServicer(store=broken_store)

        request = gateway_pb2.HeartbeatRequest(agent_id="agent-1")
        response = await service.Heartbeat(request, mock_context)
        assert response.success is False
        assert response.error


class TestLifecycleServiceReadCommand:
    @pytest.mark.asyncio
    async def test_read_command_found(self, service, store, mock_context):
        store.write_command("agent-1", "PAUSE", "operator@test.com")

        request = gateway_pb2.ReadAgentCommandRequest(agent_id="agent-1")
        response = await service.ReadCommand(request, mock_context)
        assert response.found is True
        assert response.command == "PAUSE"
        assert response.issued_by == "operator@test.com"
        assert response.command_id > 0

    @pytest.mark.asyncio
    async def test_read_command_not_found(self, service, mock_context):
        request = gateway_pb2.ReadAgentCommandRequest(agent_id="agent-1")
        response = await service.ReadCommand(request, mock_context)
        assert response.found is False


class TestLifecycleServiceAckCommand:
    @pytest.mark.asyncio
    async def test_ack_command_success(self, service, store, mock_context):
        store.write_command("agent-1", "STOP", "admin")
        cmd = store.read_pending_command("agent-1")

        request = gateway_pb2.AckAgentCommandRequest(command_id=cmd.id)
        response = await service.AckCommand(request, mock_context)
        assert response.success is True

        # Verify command is no longer pending
        assert store.read_pending_command("agent-1") is None


class TestLifecycleServiceWriteCommand:
    @pytest.mark.asyncio
    async def test_write_command_success(self, service, store, mock_context):
        request = gateway_pb2.WriteAgentCommandRequest(
            agent_id="agent-1",
            command="PAUSE",
            issued_by="dashboard-user",
        )
        response = await service.WriteCommand(request, mock_context)
        assert response.success is True

        # Verify command was written
        cmd = store.read_pending_command("agent-1")
        assert cmd is not None
        assert cmd.command == "PAUSE"
        assert cmd.issued_by == "dashboard-user"


# ---- Input validation (INVALID_ARGUMENT) ----


class TestLifecycleServiceInputValidation:
    """Tests that malformed inputs return INVALID_ARGUMENT."""

    @pytest.mark.asyncio
    async def test_write_state_empty_agent_id(self, service, mock_context):
        request = gateway_pb2.WriteAgentStateRequest(agent_id="", state="RUNNING")
        response = await service.WriteState(request, mock_context)
        assert response.success is False
        mock_context.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)

    @pytest.mark.asyncio
    async def test_write_state_whitespace_agent_id(self, service, mock_context):
        request = gateway_pb2.WriteAgentStateRequest(agent_id="   ", state="RUNNING")
        response = await service.WriteState(request, mock_context)
        assert response.success is False
        mock_context.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)

    @pytest.mark.asyncio
    async def test_write_state_invalid_state(self, service, mock_context):
        request = gateway_pb2.WriteAgentStateRequest(agent_id="agent-1", state="BOGUS")
        response = await service.WriteState(request, mock_context)
        assert response.success is False
        mock_context.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)

    @pytest.mark.asyncio
    async def test_write_state_accepts_all_valid_states(self, service, store, mock_context):
        """All documented valid states are accepted without INVALID_ARGUMENT."""
        for state in ("INITIALIZING", "RUNNING", "PAUSED", "STOPPING", "TERMINATED", "ERROR"):
            request = gateway_pb2.WriteAgentStateRequest(agent_id="agent-v", state=state)
            response = await service.WriteState(request, mock_context)
            assert response.success is True, f"State {state} should be accepted"

    @pytest.mark.asyncio
    async def test_read_state_empty_agent_id(self, service, mock_context):
        request = gateway_pb2.ReadAgentStateRequest(agent_id="")
        await service.ReadState(request, mock_context)
        mock_context.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)

    @pytest.mark.asyncio
    async def test_heartbeat_empty_agent_id(self, service, mock_context):
        request = gateway_pb2.HeartbeatRequest(agent_id="")
        response = await service.Heartbeat(request, mock_context)
        assert response.success is False
        mock_context.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)

    @pytest.mark.asyncio
    async def test_read_command_empty_agent_id(self, service, mock_context):
        request = gateway_pb2.ReadAgentCommandRequest(agent_id="")
        await service.ReadCommand(request, mock_context)
        mock_context.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)

    @pytest.mark.asyncio
    async def test_ack_command_empty_command_id(self, service, mock_context):
        request = gateway_pb2.AckAgentCommandRequest(command_id=0)
        response = await service.AckCommand(request, mock_context)
        assert response.success is False
        mock_context.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)

    @pytest.mark.asyncio
    async def test_write_command_empty_agent_id(self, service, mock_context):
        request = gateway_pb2.WriteAgentCommandRequest(agent_id="", command="STOP", issued_by="admin")
        response = await service.WriteCommand(request, mock_context)
        assert response.success is False
        mock_context.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)

    @pytest.mark.asyncio
    async def test_write_command_invalid_command(self, service, mock_context):
        request = gateway_pb2.WriteAgentCommandRequest(agent_id="agent-1", command="EXPLODE", issued_by="admin")
        response = await service.WriteCommand(request, mock_context)
        assert response.success is False
        mock_context.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)


# ---- Backend failures (INTERNAL with sanitized errors) ----


class TestLifecycleServiceBackendFailures:
    """Tests that backend store failures return INTERNAL with no sensitive details."""

    @pytest.mark.asyncio
    async def test_read_state_store_failure(self, mock_context):
        broken_store = MagicMock()
        broken_store.read_state.side_effect = RuntimeError("connection refused to 10.0.0.5:5432")
        service = LifecycleServiceServicer(store=broken_store)

        request = gateway_pb2.ReadAgentStateRequest(agent_id="agent-1")
        response = await service.ReadState(request, mock_context)
        assert response.found is False
        mock_context.set_code.assert_called_with(grpc.StatusCode.INTERNAL)
        # Verify error message is generic (no IP/host/stack trace)
        details = mock_context.set_details.call_args[0][0]
        assert "10.0.0.5" not in details
        assert "connection refused" not in details

    @pytest.mark.asyncio
    async def test_read_command_store_failure(self, mock_context):
        broken_store = MagicMock()
        broken_store.read_pending_command.side_effect = RuntimeError("SSL handshake failed")
        service = LifecycleServiceServicer(store=broken_store)

        request = gateway_pb2.ReadAgentCommandRequest(agent_id="agent-1")
        response = await service.ReadCommand(request, mock_context)
        assert response.found is False
        mock_context.set_code.assert_called_with(grpc.StatusCode.INTERNAL)
        details = mock_context.set_details.call_args[0][0]
        assert "SSL" not in details

    @pytest.mark.asyncio
    async def test_write_state_error_has_generic_payload(self, mock_context):
        """WriteState error response contains only a generic message."""
        broken_store = MagicMock()
        broken_store.write_state.side_effect = RuntimeError("password authentication failed for user 'admin'")
        service = LifecycleServiceServicer(store=broken_store)

        request = gateway_pb2.WriteAgentStateRequest(agent_id="agent-1", state="RUNNING")
        response = await service.WriteState(request, mock_context)
        assert response.success is False
        assert response.error == "internal server error"
        assert "password" not in response.error
        assert "admin" not in response.error

    @pytest.mark.asyncio
    async def test_heartbeat_error_has_generic_payload(self, mock_context):
        """Heartbeat error response contains only a generic message."""
        broken_store = MagicMock()
        broken_store.heartbeat.side_effect = RuntimeError("disk full /var/lib/pg")
        service = LifecycleServiceServicer(store=broken_store)

        request = gateway_pb2.HeartbeatRequest(agent_id="agent-1")
        response = await service.Heartbeat(request, mock_context)
        assert response.success is False
        assert response.error == "internal server error"
        assert "disk" not in response.error
