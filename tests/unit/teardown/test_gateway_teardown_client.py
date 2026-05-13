from __future__ import annotations

import json
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from almanak.framework.teardown.gateway_client import GatewayTeardownStateAdapter, GatewayTeardownStateManager
from almanak.framework.teardown.models import (
    EscalationLevel,
    TeardownMode,
    TeardownRequest,
    TeardownState,
    TeardownStatus,
)
from almanak.framework.teardown.serialization import (
    teardown_request_to_json,
    teardown_state_from_json,
    teardown_state_to_json,
)
from almanak.gateway.proto import gateway_pb2


class _FakeTeardownStub:
    def __init__(self) -> None:
        self.calls: list[tuple[str, object, float | None]] = []
        self.request = TeardownRequest(strategy_id="agent-1", mode=TeardownMode.HARD)
        now = datetime.now(UTC)
        self.state = TeardownState(
            teardown_id="td-1",
            strategy_id="agent-1",
            mode=TeardownMode.SOFT,
            status=TeardownStatus.EXECUTING,
            total_intents=2,
            completed_intents=1,
            current_intent_index=1,
            started_at=now,
            updated_at=now,
        )

    def _record(self, name: str, request: object, timeout: float | None = None) -> None:
        self.calls.append((name, request, timeout))

    def CreateTeardownRequest(self, request, timeout=None):
        self._record("CreateTeardownRequest", request, timeout)
        return gateway_pb2.CreateTeardownRequestResponse(success=True)

    def GetActiveTeardownRequest(self, request, timeout=None):
        self._record("GetActiveTeardownRequest", request, timeout)
        return gateway_pb2.GetTeardownRequestResponse(
            found=True,
            request_json=teardown_request_to_json(self.request),
        )

    def MarkTeardownCompleted(self, request, timeout=None):
        self._record("MarkTeardownCompleted", request, timeout)
        completed = TeardownRequest(
            strategy_id=request.strategy_id,
            mode=TeardownMode.HARD,
            status=TeardownStatus.COMPLETED,
        )
        return gateway_pb2.TeardownRequestMutationResponse(
            success=True,
            found=True,
            request_json=teardown_request_to_json(completed),
        )

    def RequestTeardownCancel(self, request, timeout=None):
        self._record("RequestTeardownCancel", request, timeout)
        return gateway_pb2.BoolMutationResponse(success=True, value=True)

    def SaveTeardownState(self, request, timeout=None):
        self._record("SaveTeardownState", request, timeout)
        return gateway_pb2.SaveTeardownStateResponse(success=True)

    def LoadTeardownState(self, request, timeout=None):
        self._record("LoadTeardownState", request, timeout)
        return gateway_pb2.LoadTeardownStateResponse(found=True, state_json=teardown_state_to_json(self.state))

    def GetLatestPendingApproval(self, request, timeout=None):
        self._record("GetLatestPendingApproval", request, timeout)
        return gateway_pb2.GetLatestPendingApprovalResponse(
            found=True,
            approval_json=json.dumps({"teardown_id": "td-1", "level": "level_3"}),
        )

    def WriteApprovalResponse(self, request, timeout=None):
        self._record("WriteApprovalResponse", request, timeout)
        return gateway_pb2.BoolMutationResponse(success=True, value=True)


def _client(stub: _FakeTeardownStub) -> SimpleNamespace:
    return SimpleNamespace(teardown=stub, config=SimpleNamespace(timeout=12.0), is_connected=True)


def test_gateway_manager_serializes_request_and_reads_mode() -> None:
    stub = _FakeTeardownStub()
    manager = GatewayTeardownStateManager(_client(stub))  # type: ignore[arg-type]

    manager.create_request(TeardownRequest(strategy_id="agent-1", mode=TeardownMode.SOFT))
    request = manager.get_active_request("agent-1")
    completed = manager.mark_completed("agent-1", result={"intents": 2})
    cancelled = manager.request_cancel("agent-1")

    assert request is not None
    assert request.mode == TeardownMode.HARD
    assert completed is not None
    assert completed.status == TeardownStatus.COMPLETED
    assert cancelled is True
    assert [call[0] for call in stub.calls] == [
        "CreateTeardownRequest",
        "GetActiveTeardownRequest",
        "MarkTeardownCompleted",
        "RequestTeardownCancel",
    ]
    assert stub.calls[2][1].result_json == '{"intents": 2}'


@pytest.mark.asyncio
async def test_gateway_adapter_round_trips_state_and_approvals() -> None:
    stub = _FakeTeardownStub()
    adapter = GatewayTeardownStateAdapter(_client(stub))  # type: ignore[arg-type]

    await adapter.save_teardown_state(stub.state)
    loaded = await adapter.get_teardown_state("agent-1")
    approval = adapter.get_latest_pending_approval("agent-1")
    wrote = adapter.write_approval_response("td-1", EscalationLevel.LEVEL_3, '{"approved": true}')

    assert loaded is not None
    assert loaded.teardown_id == "td-1"
    assert approval == {"teardown_id": "td-1", "level": "level_3"}
    assert wrote is True
    assert [call[0] for call in stub.calls] == [
        "SaveTeardownState",
        "LoadTeardownState",
        "GetLatestPendingApproval",
        "WriteApprovalResponse",
    ]


def test_gateway_teardown_clients_require_connected_gateway_client() -> None:
    disconnected = SimpleNamespace(
        teardown=_FakeTeardownStub(),
        config=SimpleNamespace(timeout=12.0),
        is_connected=False,
    )

    with pytest.raises(RuntimeError, match="requires a connected gateway client"):
        GatewayTeardownStateManager(disconnected)  # type: ignore[arg-type]

    with pytest.raises(RuntimeError, match="requires a connected gateway client"):
        GatewayTeardownStateAdapter(disconnected)  # type: ignore[arg-type]


def test_teardown_state_deserialization_rejects_invalid_payload_field_types() -> None:
    raw = teardown_state_to_json(_FakeTeardownStub().state)
    payload = json.loads(raw)

    payload["intent_results"] = {"not": "a-list"}
    with pytest.raises(ValueError, match="intent_results must be a list"):
        teardown_state_from_json(json.dumps(payload))

    payload = json.loads(raw)
    payload["pending_intents_json"] = ["not", "a-string"]
    with pytest.raises(ValueError, match="pending_intents_json must be a string"):
        teardown_state_from_json(json.dumps(payload))

    payload = json.loads(raw)
    payload["config_json"] = {"not": "a-string"}
    with pytest.raises(ValueError, match="config_json must be a string"):
        teardown_state_from_json(json.dumps(payload))
