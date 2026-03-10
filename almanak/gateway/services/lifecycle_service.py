"""LifecycleService gRPC servicer.

Thin pass-through to the LifecycleStore. All business logic
lives in the store; the servicer just maps gRPC <-> Python.
"""

import asyncio
import logging

import grpc

from almanak.gateway.lifecycle import LifecycleStore, get_lifecycle_store
from almanak.gateway.proto import gateway_pb2, gateway_pb2_grpc

logger = logging.getLogger(__name__)

_VALID_STATES = {"INITIALIZING", "RUNNING", "PAUSED", "STOPPING", "TERMINATED", "ERROR"}
_VALID_COMMANDS = {"STOP", "PAUSE", "RESUME"}


class LifecycleServiceServicer(gateway_pb2_grpc.LifecycleServiceServicer):
    """Implements LifecycleService gRPC interface.

    Thin pass-through to the LifecycleStore. All business logic
    lives in the store; the servicer just maps gRPC <-> Python.
    """

    def __init__(self, store: LifecycleStore | None = None):
        self._store = store or get_lifecycle_store()

    async def WriteState(self, request, context):
        if not request.agent_id or not request.agent_id.strip():
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("agent_id must be non-empty")
            return gateway_pb2.WriteAgentStateResponse(success=False, error="agent_id must be non-empty")
        if request.state not in _VALID_STATES:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(f"invalid state: {request.state}")
            return gateway_pb2.WriteAgentStateResponse(success=False, error=f"invalid state: {request.state}")
        try:
            await asyncio.to_thread(
                self._store.write_state,
                agent_id=request.agent_id,
                state=request.state,
                error_message=request.error_message or None,
            )
            return gateway_pb2.WriteAgentStateResponse(success=True)
        except Exception:
            logger.exception("WriteState failed for agent %s", request.agent_id)
            return gateway_pb2.WriteAgentStateResponse(success=False, error="internal server error")

    async def ReadState(self, request, context):
        if not request.agent_id or not request.agent_id.strip():
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("agent_id must be non-empty")
            return gateway_pb2.ReadAgentStateResponse(found=False)
        try:
            state = await asyncio.to_thread(self._store.read_state, request.agent_id)
            if state is None:
                return gateway_pb2.ReadAgentStateResponse(found=False)
            return gateway_pb2.ReadAgentStateResponse(
                found=True,
                agent_id=state.agent_id,
                state=state.state,
                state_changed_at=state.state_changed_at.isoformat(),
                last_heartbeat_at=state.last_heartbeat_at.isoformat() if state.last_heartbeat_at else "",
                error_message=state.error_message or "",
                iteration_count=state.iteration_count,
            )
        except Exception:
            logger.exception("ReadState failed for agent %s", request.agent_id)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("failed to read agent state")
            return gateway_pb2.ReadAgentStateResponse(found=False)

    async def Heartbeat(self, request, context):
        if not request.agent_id or not request.agent_id.strip():
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("agent_id must be non-empty")
            return gateway_pb2.HeartbeatResponse(success=False, error="agent_id must be non-empty")
        try:
            await asyncio.to_thread(self._store.heartbeat, request.agent_id)
            return gateway_pb2.HeartbeatResponse(success=True)
        except Exception:
            logger.exception("Heartbeat failed for agent %s", request.agent_id)
            context.set_code(grpc.StatusCode.INTERNAL)
            return gateway_pb2.HeartbeatResponse(success=False, error="internal server error")

    async def ReadCommand(self, request, context):
        if not request.agent_id or not request.agent_id.strip():
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("agent_id must be non-empty")
            return gateway_pb2.ReadAgentCommandResponse(found=False)
        try:
            cmd = await asyncio.to_thread(self._store.read_pending_command, request.agent_id)
            if cmd is None:
                return gateway_pb2.ReadAgentCommandResponse(found=False)
            return gateway_pb2.ReadAgentCommandResponse(
                found=True,
                command_id=cmd.id,
                agent_id=cmd.agent_id,
                command=cmd.command,
                issued_at=cmd.issued_at.isoformat(),
                issued_by=cmd.issued_by,
            )
        except Exception:
            logger.exception("ReadCommand failed for agent %s", request.agent_id)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("failed to read agent command")
            return gateway_pb2.ReadAgentCommandResponse(found=False)

    async def AckCommand(self, request, context):
        if not request.command_id:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("command_id must be non-empty")
            return gateway_pb2.AckAgentCommandResponse(success=False, error="command_id must be non-empty")
        try:
            await asyncio.to_thread(self._store.ack_command, request.command_id)
            return gateway_pb2.AckAgentCommandResponse(success=True)
        except Exception:
            logger.exception("AckCommand failed for command %s", request.command_id)
            context.set_code(grpc.StatusCode.INTERNAL)
            return gateway_pb2.AckAgentCommandResponse(success=False, error="internal server error")

    async def WriteCommand(self, request, context):
        if not request.agent_id or not request.agent_id.strip():
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("agent_id must be non-empty")
            return gateway_pb2.WriteAgentCommandResponse(success=False, error="agent_id must be non-empty")
        if request.command not in _VALID_COMMANDS:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(f"invalid command: {request.command}")
            return gateway_pb2.WriteAgentCommandResponse(success=False, error=f"invalid command: {request.command}")
        try:
            await asyncio.to_thread(
                self._store.write_command,
                agent_id=request.agent_id,
                command=request.command,
                issued_by=request.issued_by,
            )
            return gateway_pb2.WriteAgentCommandResponse(success=True)
        except Exception:
            logger.exception("WriteCommand failed for agent %s", request.agent_id)
            return gateway_pb2.WriteAgentCommandResponse(success=False, error="internal server error")
