from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import grpc
import pytest

from almanak.framework.teardown import create_teardown_state_adapter, create_teardown_state_manager
from almanak.framework.teardown.models import TeardownMode, TeardownRequest, TeardownState, TeardownStatus
from almanak.framework.teardown.serialization import teardown_request_to_json, teardown_state_to_json
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.teardown_service import TeardownServiceServicer


class _Context:
    def __init__(self) -> None:
        self.code: grpc.StatusCode | None = None
        self.details: str | None = None

    def set_code(self, code: grpc.StatusCode) -> None:
        self.code = code

    def set_details(self, details: str) -> None:
        self.details = details


@pytest.fixture
def servicer(tmp_path: Path) -> TeardownServiceServicer:
    db_path = tmp_path / "state.db"
    return TeardownServiceServicer(
        manager=create_teardown_state_manager(sqlite_path=db_path),
        adapter=create_teardown_state_adapter(sqlite_path=db_path),
    )


@pytest.mark.asyncio
async def test_teardown_service_request_lifecycle(servicer: TeardownServiceServicer) -> None:
    ctx = _Context()
    request = TeardownRequest(deployment_id="agent-1", mode=TeardownMode.HARD)

    created = await servicer.CreateTeardownRequest(
        gateway_pb2.CreateTeardownRequestRequest(request_json=teardown_request_to_json(request)),
        ctx,
    )
    active = await servicer.GetActiveTeardownRequest(gateway_pb2.GetActiveTeardownRequestRequest(deployment_id="agent-1"), ctx)
    started = await servicer.MarkTeardownStarted(
        gateway_pb2.MarkTeardownStartedRequest(deployment_id="agent-1", total_positions=3),
        ctx,
    )
    completed = await servicer.MarkTeardownCompleted(
        gateway_pb2.MarkTeardownCompletedRequest(deployment_id="agent-1", result_json='{"intents": 3}'),
        ctx,
    )

    assert created.success is True
    assert active.found is True
    assert started.found is True
    assert completed.found is True
    assert ctx.code is None


@pytest.mark.asyncio
async def test_teardown_service_state_and_approval_lifecycle(servicer: TeardownServiceServicer) -> None:
    ctx = _Context()
    now = datetime.now(UTC)
    state = TeardownState(
        teardown_id="td-1",
        deployment_id="agent-1",
        mode=TeardownMode.SOFT,
        status=TeardownStatus.EXECUTING,
        total_intents=1,
        completed_intents=0,
        current_intent_index=0,
        started_at=now,
        updated_at=now,
    )

    saved = await servicer.SaveTeardownState(
        gateway_pb2.SaveTeardownStateRequest(state_json=teardown_state_to_json(state)),
        ctx,
    )
    loaded = await servicer.LoadTeardownState(gateway_pb2.LoadTeardownStateRequest(deployment_id="agent-1"), ctx)
    created = await servicer.CreateApprovalRequest(
        gateway_pb2.CreateApprovalRequestRequest(
            teardown_id="td-1",
            deployment_id="agent-1",
            level="level_3",
            request_json='{"why": "slippage"}',
            expires_at=(now + timedelta(minutes=5)).isoformat(),
        ),
        ctx,
    )
    wrote = await servicer.WriteApprovalResponseByStrategy(
        gateway_pb2.WriteApprovalResponseByStrategyRequest(
            deployment_id="agent-1",
            response_json='{"approved": true}',
        ),
        ctx,
    )
    response = await servicer.GetApprovalResponse(
        gateway_pb2.GetApprovalResponseRequest(teardown_id="td-1", level="level_3"),
        ctx,
    )

    assert saved.success is True
    assert loaded.found is True
    assert created.success is True
    assert wrote.success is True
    assert response.found is True
    assert ctx.code is None


@pytest.mark.asyncio
async def test_teardown_service_returns_invalid_argument_for_malformed_payloads(
    servicer: TeardownServiceServicer,
) -> None:
    create_ctx = _Context()
    created = await servicer.CreateTeardownRequest(
        gateway_pb2.CreateTeardownRequestRequest(request_json="{not-json"),
        create_ctx,
    )

    update_ctx = _Context()
    updated = await servicer.UpdateTeardownRequest(
        gateway_pb2.UpdateTeardownRequestRequest(request_json="{not-json"),
        update_ctx,
    )

    progress_ctx = _Context()
    progress = await servicer.UpdateTeardownProgress(
        gateway_pb2.UpdateTeardownProgressRequest(deployment_id="agent-1", current_phase="bogus"),
        progress_ctx,
    )

    completed_ctx = _Context()
    completed = await servicer.MarkTeardownCompleted(
        gateway_pb2.MarkTeardownCompletedRequest(deployment_id="agent-1", result_json="{not-json"),
        completed_ctx,
    )

    state_ctx = _Context()
    saved = await servicer.SaveTeardownState(
        gateway_pb2.SaveTeardownStateRequest(state_json="{not-json"),
        state_ctx,
    )

    assert created.success is False
    assert created.error == "request_json is invalid"
    assert create_ctx.code == grpc.StatusCode.INVALID_ARGUMENT
    assert updated.success is False
    assert updated.error == "request_json is invalid"
    assert update_ctx.code == grpc.StatusCode.INVALID_ARGUMENT
    assert progress.success is False
    assert progress.error == "current_phase is invalid"
    assert progress_ctx.code == grpc.StatusCode.INVALID_ARGUMENT
    assert completed.success is False
    assert completed.error == "result_json is invalid"
    assert completed_ctx.code == grpc.StatusCode.INVALID_ARGUMENT
    assert saved.success is False
    assert saved.error == "state_json is invalid"
    assert state_ctx.code == grpc.StatusCode.INVALID_ARGUMENT
