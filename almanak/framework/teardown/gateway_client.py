"""Gateway-backed teardown state clients for hosted strategy runtimes."""

from __future__ import annotations

import json
from typing import Any

from almanak.framework.gateway_client import GatewayClient
from almanak.framework.teardown.models import EscalationLevel, TeardownPhase, TeardownRequest, TeardownState
from almanak.framework.teardown.serialization import (
    teardown_request_from_json,
    teardown_request_to_json,
    teardown_state_from_json,
    teardown_state_to_json,
)
from almanak.gateway.proto import gateway_pb2


def _timeout(client: GatewayClient) -> float:
    return client.config.timeout


def _require_success(response: Any, operation: str) -> None:
    if not response.success:
        raise RuntimeError(response.error or f"{operation} failed")


def _request_from_response(response: Any, operation: str) -> TeardownRequest | None:
    if response.error:
        raise RuntimeError(response.error or f"{operation} failed")
    if not response.found:
        return None
    return teardown_request_from_json(response.request_json)


def _mutation_from_response(response: Any, operation: str) -> TeardownRequest | None:
    _require_success(response, operation)
    if not response.found:
        return None
    return teardown_request_from_json(response.request_json)


def _level_value(level: EscalationLevel | str) -> str:
    return level.value if isinstance(level, EscalationLevel) else str(level)


class GatewayTeardownStateManager:
    """Teardown request store that routes hosted runtime calls through gRPC."""

    def __init__(self, gateway_client: GatewayClient):
        if gateway_client is None or not getattr(gateway_client, "is_connected", False):
            raise RuntimeError("Hosted teardown state requires a connected gateway client")
        self._client = gateway_client

    def create_request(self, request: TeardownRequest) -> None:
        response = self._client.teardown.CreateTeardownRequest(
            gateway_pb2.CreateTeardownRequestRequest(request_json=teardown_request_to_json(request)),
            timeout=_timeout(self._client),
        )
        _require_success(response, "CreateTeardownRequest")

    def get_request(self, strategy_id: str) -> TeardownRequest | None:
        response = self._client.teardown.GetTeardownRequest(
            gateway_pb2.GetTeardownRequestRequest(strategy_id=strategy_id),
            timeout=_timeout(self._client),
        )
        return _request_from_response(response, "GetTeardownRequest")

    def get_active_request(self, strategy_id: str) -> TeardownRequest | None:
        response = self._client.teardown.GetActiveTeardownRequest(
            gateway_pb2.GetActiveTeardownRequestRequest(strategy_id=strategy_id),
            timeout=_timeout(self._client),
        )
        return _request_from_response(response, "GetActiveTeardownRequest")

    def get_pending_requests(self) -> list[TeardownRequest]:
        response = self._client.teardown.GetPendingTeardownRequests(
            gateway_pb2.Empty(),
            timeout=_timeout(self._client),
        )
        _require_success(response, "GetPendingTeardownRequests")
        return [teardown_request_from_json(raw) for raw in response.requests_json]

    def get_all_active_requests(self) -> list[TeardownRequest]:
        response = self._client.teardown.GetAllActiveTeardownRequests(
            gateway_pb2.Empty(),
            timeout=_timeout(self._client),
        )
        _require_success(response, "GetAllActiveTeardownRequests")
        return [teardown_request_from_json(raw) for raw in response.requests_json]

    def get_all_requests(self) -> list[TeardownRequest]:
        response = self._client.teardown.GetAllTeardownRequests(
            gateway_pb2.Empty(),
            timeout=_timeout(self._client),
        )
        _require_success(response, "GetAllTeardownRequests")
        return [teardown_request_from_json(raw) for raw in response.requests_json]

    def update_request(self, request: TeardownRequest) -> None:
        response = self._client.teardown.UpdateTeardownRequest(
            gateway_pb2.UpdateTeardownRequestRequest(request_json=teardown_request_to_json(request)),
            timeout=_timeout(self._client),
        )
        _require_success(response, "UpdateTeardownRequest")

    def acknowledge_request(self, strategy_id: str) -> TeardownRequest | None:
        response = self._client.teardown.AcknowledgeTeardownRequest(
            gateway_pb2.AckTeardownRequestRequest(strategy_id=strategy_id),
            timeout=_timeout(self._client),
        )
        return _mutation_from_response(response, "AcknowledgeTeardownRequest")

    def mark_started(self, strategy_id: str, total_positions: int = 0) -> TeardownRequest | None:
        response = self._client.teardown.MarkTeardownStarted(
            gateway_pb2.MarkTeardownStartedRequest(strategy_id=strategy_id, total_positions=total_positions),
            timeout=_timeout(self._client),
        )
        return _mutation_from_response(response, "MarkTeardownStarted")

    def update_progress(
        self,
        strategy_id: str,
        positions_closed: int,
        positions_failed: int = 0,
        current_phase: TeardownPhase | None = None,
    ) -> TeardownRequest | None:
        response = self._client.teardown.UpdateTeardownProgress(
            gateway_pb2.UpdateTeardownProgressRequest(
                strategy_id=strategy_id,
                positions_closed=positions_closed,
                positions_failed=positions_failed,
                current_phase=current_phase.value if current_phase is not None else "",
            ),
            timeout=_timeout(self._client),
        )
        return _mutation_from_response(response, "UpdateTeardownProgress")

    def mark_completed(self, strategy_id: str, result: dict | None = None) -> TeardownRequest | None:
        response = self._client.teardown.MarkTeardownCompleted(
            gateway_pb2.MarkTeardownCompletedRequest(
                strategy_id=strategy_id,
                result_json=json.dumps(result, sort_keys=True) if result else "",
            ),
            timeout=_timeout(self._client),
        )
        return _mutation_from_response(response, "MarkTeardownCompleted")

    def mark_failed(self, strategy_id: str, error: str) -> TeardownRequest | None:
        response = self._client.teardown.MarkTeardownFailed(
            gateway_pb2.MarkTeardownFailedRequest(strategy_id=strategy_id, error_message=error),
            timeout=_timeout(self._client),
        )
        return _mutation_from_response(response, "MarkTeardownFailed")

    def request_cancel(self, strategy_id: str) -> bool:
        response = self._client.teardown.RequestTeardownCancel(
            gateway_pb2.RequestTeardownCancelRequest(strategy_id=strategy_id),
            timeout=_timeout(self._client),
        )
        _require_success(response, "RequestTeardownCancel")
        return bool(response.value)

    def mark_cancelled(self, strategy_id: str) -> TeardownRequest | None:
        response = self._client.teardown.MarkTeardownCancelled(
            gateway_pb2.MarkTeardownCancelledRequest(strategy_id=strategy_id),
            timeout=_timeout(self._client),
        )
        return _mutation_from_response(response, "MarkTeardownCancelled")

    def delete_request(self, strategy_id: str) -> bool:
        response = self._client.teardown.DeleteTeardownRequest(
            gateway_pb2.DeleteTeardownRequestRequest(strategy_id=strategy_id),
            timeout=_timeout(self._client),
        )
        _require_success(response, "DeleteTeardownRequest")
        return bool(response.value)


class GatewayTeardownStateAdapter:
    """Teardown execution-state and approval adapter over gateway gRPC."""

    def __init__(self, gateway_client: GatewayClient):
        if gateway_client is None or not getattr(gateway_client, "is_connected", False):
            raise RuntimeError("Hosted teardown execution state requires a connected gateway client")
        self._client = gateway_client

    async def save_teardown_state(self, state: TeardownState) -> None:
        response = self._client.teardown.SaveTeardownState(
            gateway_pb2.SaveTeardownStateRequest(state_json=teardown_state_to_json(state)),
            timeout=_timeout(self._client),
        )
        _require_success(response, "SaveTeardownState")

    async def get_teardown_state(self, strategy_id: str) -> TeardownState | None:
        response = self._client.teardown.LoadTeardownState(
            gateway_pb2.LoadTeardownStateRequest(strategy_id=strategy_id),
            timeout=_timeout(self._client),
        )
        if response.error:
            raise RuntimeError(response.error or "LoadTeardownState failed")
        if not response.found:
            return None
        return teardown_state_from_json(response.state_json)

    async def delete_teardown_state(self, teardown_id: str) -> None:
        response = self._client.teardown.DeleteTeardownState(
            gateway_pb2.DeleteTeardownStateRequest(teardown_id=teardown_id),
            timeout=_timeout(self._client),
        )
        _require_success(response, "DeleteTeardownState")

    def create_approval_request(
        self,
        teardown_id: str,
        strategy_id: str,
        level: EscalationLevel | str,
        request_json: str,
        expires_at: str,
    ) -> None:
        response = self._client.teardown.CreateApprovalRequest(
            gateway_pb2.CreateApprovalRequestRequest(
                teardown_id=teardown_id,
                strategy_id=strategy_id,
                level=_level_value(level),
                request_json=request_json,
                expires_at=expires_at,
            ),
            timeout=_timeout(self._client),
        )
        _require_success(response, "CreateApprovalRequest")

    def get_approval_response(self, teardown_id: str, level: EscalationLevel | str) -> str | None:
        response = self._client.teardown.GetApprovalResponse(
            gateway_pb2.GetApprovalResponseRequest(teardown_id=teardown_id, level=_level_value(level)),
            timeout=_timeout(self._client),
        )
        if response.error:
            raise RuntimeError(response.error or "GetApprovalResponse failed")
        if not response.found:
            return None
        return response.response_json

    def write_approval_response(
        self,
        teardown_id: str,
        level: EscalationLevel | str,
        response_json: str,
    ) -> bool:
        response = self._client.teardown.WriteApprovalResponse(
            gateway_pb2.WriteApprovalResponseRequest(
                teardown_id=teardown_id,
                level=_level_value(level),
                response_json=response_json,
            ),
            timeout=_timeout(self._client),
        )
        _require_success(response, "WriteApprovalResponse")
        return bool(response.value)

    def get_latest_pending_approval(self, strategy_id: str) -> dict[str, Any] | None:
        response = self._client.teardown.GetLatestPendingApproval(
            gateway_pb2.GetLatestPendingApprovalRequest(strategy_id=strategy_id),
            timeout=_timeout(self._client),
        )
        if response.error:
            raise RuntimeError(response.error or "GetLatestPendingApproval failed")
        if not response.found:
            return None
        return json.loads(response.approval_json)

    def write_approval_response_by_strategy(self, strategy_id: str, response_json: str) -> bool:
        response = self._client.teardown.WriteApprovalResponseByStrategy(
            gateway_pb2.WriteApprovalResponseByStrategyRequest(strategy_id=strategy_id, response_json=response_json),
            timeout=_timeout(self._client),
        )
        _require_success(response, "WriteApprovalResponseByStrategy")
        return bool(response.value)


__all__ = [
    "GatewayTeardownStateAdapter",
    "GatewayTeardownStateManager",
]
