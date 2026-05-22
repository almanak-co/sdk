"""TeardownService gRPC servicer.

Hosted strategy containers route teardown state through this service so the
Postgres DSN stays inside the gateway process.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Callable
from typing import Any

import grpc

from almanak.framework.teardown.models import TeardownPhase
from almanak.framework.teardown.serialization import (
    teardown_request_from_json,
    teardown_request_to_json,
    teardown_state_from_json,
    teardown_state_to_json,
)
from almanak.framework.teardown.state_manager import TeardownStateAdapterProtocol, TeardownStateManagerProtocol
from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2, gateway_pb2_grpc

logger = logging.getLogger(__name__)

_INPUT_ERROR_TYPES = (json.JSONDecodeError, ValueError, TypeError, KeyError)


class TeardownServiceServicer(gateway_pb2_grpc.TeardownServiceServicer):
    """Gateway-hosted teardown state service."""

    def __init__(
        self,
        manager: TeardownStateManagerProtocol | None = None,
        adapter: TeardownStateAdapterProtocol | None = None,
        settings: GatewaySettings | None = None,
    ):
        self._manager = manager
        self._adapter = adapter
        self._settings = settings or GatewaySettings()

    def _get_manager(self) -> TeardownStateManagerProtocol:
        if self._manager is None:
            from almanak.framework.teardown import create_teardown_state_manager

            self._manager = create_teardown_state_manager(database_url=self._settings.database_url or None)
        return self._manager

    def _get_adapter(self) -> TeardownStateAdapterProtocol:
        if self._adapter is None:
            from almanak.framework.teardown import create_teardown_state_adapter

            self._adapter = create_teardown_state_adapter(database_url=self._settings.database_url or None)
        return self._adapter

    @staticmethod
    def _validate_non_empty(value: str, field: str, context: grpc.aio.ServicerContext) -> bool:
        if value and value.strip():
            return True
        context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
        context.set_details(f"{field} must be non-empty")
        return False

    @staticmethod
    def _invalid_argument(context: grpc.aio.ServicerContext, details: str, response: Any) -> Any:
        context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
        context.set_details(details)
        return response

    @staticmethod
    def _request_response(request: Any | None) -> Any:
        if request is None:
            return gateway_pb2.GetTeardownRequestResponse(found=False)
        return gateway_pb2.GetTeardownRequestResponse(found=True, request_json=teardown_request_to_json(request))

    @staticmethod
    def _mutation_response(request: Any | None) -> Any:
        if request is None:
            return gateway_pb2.TeardownRequestMutationResponse(success=True, found=False)
        return gateway_pb2.TeardownRequestMutationResponse(
            success=True,
            found=True,
            request_json=teardown_request_to_json(request),
        )

    async def CreateTeardownRequest(self, request, context):
        if not request.request_json:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("request_json must be non-empty")
            return gateway_pb2.CreateTeardownRequestResponse(success=False, error="request_json must be non-empty")
        try:
            teardown_request = teardown_request_from_json(request.request_json)
            await asyncio.to_thread(self._get_manager().create_request, teardown_request)
            return gateway_pb2.CreateTeardownRequestResponse(success=True)
        except _INPUT_ERROR_TYPES as exc:
            logger.debug("CreateTeardownRequest rejected malformed request_json: %s", exc)
            return self._invalid_argument(
                context,
                "request_json is invalid",
                gateway_pb2.CreateTeardownRequestResponse(success=False, error="request_json is invalid"),
            )
        except Exception:
            logger.exception("CreateTeardownRequest failed")
            context.set_code(grpc.StatusCode.INTERNAL)
            return gateway_pb2.CreateTeardownRequestResponse(success=False, error="internal server error")

    async def GetTeardownRequest(self, request, context):
        if not self._validate_non_empty(request.deployment_id, "deployment_id", context):
            return gateway_pb2.GetTeardownRequestResponse(found=False, error="deployment_id must be non-empty")
        try:
            result = await asyncio.to_thread(self._get_manager().get_request, request.deployment_id)
            return self._request_response(result)
        except Exception:
            logger.exception("GetTeardownRequest failed for %s", request.deployment_id)
            context.set_code(grpc.StatusCode.INTERNAL)
            return gateway_pb2.GetTeardownRequestResponse(found=False, error="internal server error")

    async def GetActiveTeardownRequest(self, request, context):
        if not self._validate_non_empty(request.deployment_id, "deployment_id", context):
            return gateway_pb2.GetTeardownRequestResponse(found=False, error="deployment_id must be non-empty")
        try:
            result = await asyncio.to_thread(self._get_manager().get_active_request, request.deployment_id)
            return self._request_response(result)
        except Exception:
            logger.exception("GetActiveTeardownRequest failed for %s", request.deployment_id)
            context.set_code(grpc.StatusCode.INTERNAL)
            return gateway_pb2.GetTeardownRequestResponse(found=False, error="internal server error")

    async def GetPendingTeardownRequests(self, request, context):
        return await self._list_requests(context, lambda manager: manager.get_pending_requests())

    async def GetAllActiveTeardownRequests(self, request, context):
        return await self._list_requests(context, lambda manager: manager.get_all_active_requests())

    async def GetAllTeardownRequests(self, request, context):
        return await self._list_requests(context, lambda manager: manager.get_all_requests())

    async def _list_requests(self, context, fn: Callable[[TeardownStateManagerProtocol], list[Any]]):
        try:
            requests = await asyncio.to_thread(fn, self._get_manager())
            return gateway_pb2.ListTeardownRequestsResponse(
                success=True,
                requests_json=[teardown_request_to_json(r) for r in requests],
            )
        except Exception:
            logger.exception("List teardown requests failed")
            context.set_code(grpc.StatusCode.INTERNAL)
            return gateway_pb2.ListTeardownRequestsResponse(success=False, error="internal server error")

    async def UpdateTeardownRequest(self, request, context):
        if not request.request_json:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("request_json must be non-empty")
            return gateway_pb2.TeardownRequestMutationResponse(success=False, error="request_json must be non-empty")
        try:
            teardown_request = teardown_request_from_json(request.request_json)
            await asyncio.to_thread(self._get_manager().update_request, teardown_request)
            return self._mutation_response(teardown_request)
        except _INPUT_ERROR_TYPES as exc:
            logger.debug("UpdateTeardownRequest rejected malformed request_json: %s", exc)
            return self._invalid_argument(
                context,
                "request_json is invalid",
                gateway_pb2.TeardownRequestMutationResponse(success=False, error="request_json is invalid"),
            )
        except Exception:
            logger.exception("UpdateTeardownRequest failed")
            context.set_code(grpc.StatusCode.INTERNAL)
            return gateway_pb2.TeardownRequestMutationResponse(success=False, error="internal server error")

    async def AcknowledgeTeardownRequest(self, request, context):
        return await self._strategy_mutation(
            context,
            request.deployment_id,
            lambda manager, deployment_id: manager.acknowledge_request(deployment_id),
        )

    async def MarkTeardownStarted(self, request, context):
        return await self._strategy_mutation(
            context,
            request.deployment_id,
            lambda manager, deployment_id: manager.mark_started(deployment_id, total_positions=request.total_positions),
        )

    async def UpdateTeardownProgress(self, request, context):
        def _update(manager: TeardownStateManagerProtocol, deployment_id: str) -> Any | None:
            phase = TeardownPhase(request.current_phase) if request.current_phase else None
            return manager.update_progress(
                deployment_id,
                positions_closed=request.positions_closed,
                positions_failed=request.positions_failed,
                current_phase=phase,
            )

        return await self._strategy_mutation(
            context,
            request.deployment_id,
            _update,
            invalid_error="current_phase is invalid",
        )

    async def MarkTeardownCompleted(self, request, context):
        def _complete(manager: TeardownStateManagerProtocol, deployment_id: str) -> Any | None:
            result = json.loads(request.result_json) if request.result_json else None
            if result is not None and not isinstance(result, dict):
                raise ValueError("result_json must be a JSON object")
            return manager.mark_completed(deployment_id, result=result)

        return await self._strategy_mutation(
            context,
            request.deployment_id,
            _complete,
            invalid_error="result_json is invalid",
        )

    async def MarkTeardownFailed(self, request, context):
        if not request.error_message:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("error_message must be non-empty")
            return gateway_pb2.TeardownRequestMutationResponse(success=False, error="error_message must be non-empty")
        # VIB-4542 (audit PR #2343): use proto3 ``optional`` field presence to
        # distinguish "caller supplied a count" from "caller omitted the field"
        # — the prior ``-1`` sentinel was unsafe because proto3 scalar defaults
        # are 0 (not -1), so a legacy client sending only deployment_id +
        # error_message would have arrived as positions_closed=0 (overwrite)
        # instead of "preserve". HasField returns False for absent optional
        # scalars on both unset and not-yet-set messages.
        closed = request.positions_closed if request.HasField("positions_closed") else None
        failed = request.positions_failed if request.HasField("positions_failed") else None
        return await self._strategy_mutation(
            context,
            request.deployment_id,
            lambda manager, deployment_id: manager.mark_failed(
                deployment_id,
                error=request.error_message,
                positions_closed=closed,
                positions_failed=failed,
            ),
        )

    async def RequestTeardownCancel(self, request, context):
        return await self._bool_mutation(
            context,
            request.deployment_id,
            lambda manager, deployment_id: manager.request_cancel(deployment_id),
        )

    async def MarkTeardownCancelled(self, request, context):
        return await self._strategy_mutation(
            context,
            request.deployment_id,
            lambda manager, deployment_id: manager.mark_cancelled(deployment_id),
        )

    async def DeleteTeardownRequest(self, request, context):
        return await self._bool_mutation(
            context, request.deployment_id, lambda manager, deployment_id: manager.delete_request(deployment_id)
        )

    async def _strategy_mutation(
        self,
        context,
        deployment_id: str,
        fn: Callable[[TeardownStateManagerProtocol, str], Any | None],
        invalid_error: str = "teardown request input is invalid",
    ):
        if not self._validate_non_empty(deployment_id, "deployment_id", context):
            return gateway_pb2.TeardownRequestMutationResponse(success=False, error="deployment_id must be non-empty")
        try:
            result = await asyncio.to_thread(fn, self._get_manager(), deployment_id)
            return self._mutation_response(result)
        except _INPUT_ERROR_TYPES as exc:
            logger.debug("Teardown request mutation rejected malformed input for %s: %s", deployment_id, exc)
            return self._invalid_argument(
                context,
                invalid_error,
                gateway_pb2.TeardownRequestMutationResponse(success=False, error=invalid_error),
            )
        except Exception:
            logger.exception("Teardown request mutation failed for %s", deployment_id)
            context.set_code(grpc.StatusCode.INTERNAL)
            return gateway_pb2.TeardownRequestMutationResponse(success=False, error="internal server error")

    async def _bool_mutation(
        self,
        context,
        deployment_id: str,
        fn: Callable[[TeardownStateManagerProtocol, str], bool],
    ):
        if not self._validate_non_empty(deployment_id, "deployment_id", context):
            return gateway_pb2.BoolMutationResponse(success=False, error="deployment_id must be non-empty")
        try:
            result = await asyncio.to_thread(fn, self._get_manager(), deployment_id)
            return gateway_pb2.BoolMutationResponse(success=True, value=bool(result))
        except Exception:
            logger.exception("Teardown bool mutation failed for %s", deployment_id)
            context.set_code(grpc.StatusCode.INTERNAL)
            return gateway_pb2.BoolMutationResponse(success=False, error="internal server error")

    async def SaveTeardownState(self, request, context):
        if not request.state_json:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("state_json must be non-empty")
            return gateway_pb2.SaveTeardownStateResponse(success=False, error="state_json must be non-empty")
        try:
            state = teardown_state_from_json(request.state_json)
            await self._get_adapter().save_teardown_state(state)
            return gateway_pb2.SaveTeardownStateResponse(success=True)
        except _INPUT_ERROR_TYPES as exc:
            logger.debug("SaveTeardownState rejected malformed state_json: %s", exc)
            return self._invalid_argument(
                context,
                "state_json is invalid",
                gateway_pb2.SaveTeardownStateResponse(success=False, error="state_json is invalid"),
            )
        except Exception:
            logger.exception("SaveTeardownState failed")
            context.set_code(grpc.StatusCode.INTERNAL)
            return gateway_pb2.SaveTeardownStateResponse(success=False, error="internal server error")

    async def LoadTeardownState(self, request, context):
        if not self._validate_non_empty(request.deployment_id, "deployment_id", context):
            return gateway_pb2.LoadTeardownStateResponse(found=False, error="deployment_id must be non-empty")
        try:
            state = await self._get_adapter().get_teardown_state(request.deployment_id)
            if state is None:
                return gateway_pb2.LoadTeardownStateResponse(found=False)
            return gateway_pb2.LoadTeardownStateResponse(found=True, state_json=teardown_state_to_json(state))
        except Exception:
            logger.exception("LoadTeardownState failed for %s", request.deployment_id)
            context.set_code(grpc.StatusCode.INTERNAL)
            return gateway_pb2.LoadTeardownStateResponse(found=False, error="internal server error")

    async def DeleteTeardownState(self, request, context):
        if not self._validate_non_empty(request.teardown_id, "teardown_id", context):
            return gateway_pb2.DeleteTeardownStateResponse(success=False, error="teardown_id must be non-empty")
        try:
            await self._get_adapter().delete_teardown_state(request.teardown_id)
            return gateway_pb2.DeleteTeardownStateResponse(success=True)
        except Exception:
            logger.exception("DeleteTeardownState failed for %s", request.teardown_id)
            context.set_code(grpc.StatusCode.INTERNAL)
            return gateway_pb2.DeleteTeardownStateResponse(success=False, error="internal server error")

    async def CreateApprovalRequest(self, request, context):
        if not all(
            (request.teardown_id, request.deployment_id, request.level, request.request_json, request.expires_at)
        ):
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("approval request fields must be non-empty")
            return gateway_pb2.CreateApprovalRequestResponse(
                success=False, error="approval request fields must be non-empty"
            )
        try:
            await asyncio.to_thread(
                self._get_adapter().create_approval_request,
                request.teardown_id,
                request.deployment_id,
                request.level,
                request.request_json,
                request.expires_at,
            )
            return gateway_pb2.CreateApprovalRequestResponse(success=True)
        except Exception:
            logger.exception("CreateApprovalRequest failed for %s", request.teardown_id)
            context.set_code(grpc.StatusCode.INTERNAL)
            return gateway_pb2.CreateApprovalRequestResponse(success=False, error="internal server error")

    async def GetApprovalResponse(self, request, context):
        if not all((request.teardown_id, request.level)):
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("teardown_id and level must be non-empty")
            return gateway_pb2.GetApprovalResponseResponse(found=False, error="teardown_id and level must be non-empty")
        try:
            response = await asyncio.to_thread(
                self._get_adapter().get_approval_response,
                request.teardown_id,
                request.level,
            )
            return gateway_pb2.GetApprovalResponseResponse(
                found=response is not None,
                response_json=response or "",
            )
        except Exception:
            logger.exception("GetApprovalResponse failed for %s", request.teardown_id)
            context.set_code(grpc.StatusCode.INTERNAL)
            return gateway_pb2.GetApprovalResponseResponse(found=False, error="internal server error")

    async def WriteApprovalResponse(self, request, context):
        if not all((request.teardown_id, request.level, request.response_json)):
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("teardown_id, level, and response_json must be non-empty")
            return gateway_pb2.BoolMutationResponse(success=False, error="approval response fields must be non-empty")
        try:
            value = await asyncio.to_thread(
                self._get_adapter().write_approval_response,
                request.teardown_id,
                request.level,
                request.response_json,
            )
            return gateway_pb2.BoolMutationResponse(success=True, value=bool(value))
        except Exception:
            logger.exception("WriteApprovalResponse failed for %s", request.teardown_id)
            context.set_code(grpc.StatusCode.INTERNAL)
            return gateway_pb2.BoolMutationResponse(success=False, error="internal server error")

    async def GetLatestPendingApproval(self, request, context):
        if not self._validate_non_empty(request.deployment_id, "deployment_id", context):
            return gateway_pb2.GetLatestPendingApprovalResponse(found=False, error="deployment_id must be non-empty")
        try:
            approval = await asyncio.to_thread(self._get_adapter().get_latest_pending_approval, request.deployment_id)
            if approval is None:
                return gateway_pb2.GetLatestPendingApprovalResponse(found=False)
            return gateway_pb2.GetLatestPendingApprovalResponse(
                found=True,
                approval_json=json.dumps(approval, sort_keys=True),
            )
        except Exception:
            logger.exception("GetLatestPendingApproval failed for %s", request.deployment_id)
            context.set_code(grpc.StatusCode.INTERNAL)
            return gateway_pb2.GetLatestPendingApprovalResponse(found=False, error="internal server error")

    async def WriteApprovalResponseByStrategy(self, request, context):
        if not all((request.deployment_id, request.response_json)):
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("deployment_id and response_json must be non-empty")
            return gateway_pb2.BoolMutationResponse(
                success=False, error="deployment_id and response_json must be non-empty"
            )
        try:
            value = await asyncio.to_thread(
                self._get_adapter().write_approval_response_by_strategy,
                request.deployment_id,
                request.response_json,
            )
            return gateway_pb2.BoolMutationResponse(success=True, value=bool(value))
        except Exception:
            logger.exception("WriteApprovalResponseByStrategy failed for %s", request.deployment_id)
            context.set_code(grpc.StatusCode.INTERNAL)
            return gateway_pb2.BoolMutationResponse(success=False, error="internal server error")
