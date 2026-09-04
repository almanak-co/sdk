"""Focused public-contract tests for DashboardService.GetTimeline."""

from __future__ import annotations

import json
import os
import time
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import grpc
import pytest

from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.dashboard_service import DashboardServiceServicer
from almanak.gateway.timeline.store import TimelineEvent

_STORE_PATCH = "almanak.gateway.services.dashboard_service.get_timeline_store"


@pytest.fixture
def service(tmp_path) -> DashboardServiceServicer:
    servicer = DashboardServiceServicer.__new__(DashboardServiceServicer)
    servicer._initialized = True
    servicer._strategies_root = tmp_path / "strategies"
    servicer._state_manager = None
    return servicer


@pytest.fixture
def context() -> MagicMock:
    return MagicMock(spec=grpc.aio.ServicerContext)


def _stored_event(
    *,
    deployment_id: str = "test-deployment",
    description: str = "stored",
    timestamp: datetime | None = None,
) -> TimelineEvent:
    return TimelineEvent(
        event_id=description,
        deployment_id=deployment_id,
        timestamp=timestamp or datetime(2026, 1, 2, tzinfo=UTC),
        event_type="TRADE",
        description=description,
        tx_hash="0xabc",
        details={"source": "store"},
        chain="arbitrum",
        cycle_id="cycle-1",
        phase="EXECUTE",
        related_ledger_entry_id="ledger-1",
    )


def _set_state(service: DashboardServiceServicer, state: object) -> AsyncMock:
    manager = MagicMock()
    manager.load_state = AsyncMock(return_value=SimpleNamespace(state=state))
    service._state_manager = manager
    return manager.load_state


@pytest.mark.asyncio
async def test_invalid_deployment_id_returns_invalid_argument_without_reads(service, context) -> None:
    with patch(_STORE_PATCH) as store_getter:
        response = await service.GetTimeline(gateway_pb2.GetTimelineRequest(deployment_id=""), context)

    assert list(response.events) == []
    assert response.has_more is False
    context.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)
    assert "deployment_id" in context.set_details.call_args.args[0]
    store_getter.assert_not_called()


@pytest.mark.asyncio
async def test_out_of_range_since_timestamp_returns_invalid_argument_without_reads(service, context) -> None:
    with patch(_STORE_PATCH) as store_getter:
        response = await service.GetTimeline(
            gateway_pb2.GetTimelineRequest(
                deployment_id="test-deployment",
                since_timestamp=(1 << 63) - 1,
            ),
            context,
        )

    assert list(response.events) == []
    context.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)
    assert "since_timestamp" in context.set_details.call_args.args[0]
    store_getter.assert_not_called()


@pytest.mark.parametrize(
    ("request_fields", "expected_limit", "expected_type", "expected_since"),
    [
        ({"limit": 0}, 50, None, None),
        ({"limit": -1}, 50, None, None),
        (
            {"limit": 7, "event_type_filter": "TRADE", "since_timestamp": 1_767_225_600},
            7,
            "TRADE",
            datetime(2026, 1, 1, tzinfo=UTC),
        ),
    ],
)
@pytest.mark.asyncio
async def test_request_options_are_normalized_and_forwarded(
    service,
    context,
    request_fields,
    expected_limit,
    expected_type,
    expected_since,
) -> None:
    store = MagicMock()
    store.get_events.return_value = []

    with patch(_STORE_PATCH, return_value=store):
        response = await service.GetTimeline(
            gateway_pb2.GetTimelineRequest(deployment_id="test-deployment", **request_fields),
            context,
        )

    assert list(response.events) == []
    store.get_events.assert_called_once_with(
        deployment_id="test-deployment",
        limit=expected_limit,
        event_type=expected_type,
        since=expected_since,
    )


@pytest.mark.asyncio
async def test_mixed_sources_preserve_fields_timestamps_order_and_full_page_contract(service, context) -> None:
    stored = _stored_event(timestamp=datetime(2026, 1, 2, 12, 0, 0, 900_000, tzinfo=UTC))
    stored_tie = _stored_event(description="stored-tie", timestamp=stored.timestamp)
    store = MagicMock()
    store.get_events.return_value = [stored, stored_tie]
    _set_state(
        service,
        {
            "execution_history": [
                {
                    "timestamp": "2026-01-03T00:00:00+00:00",
                    "event_type": "EXECUTION",
                    "description": "state",
                    "tx_hash": "0xdef",
                    "details": {"source": "state"},
                    "chain": "base",
                    "cycle_id": "ignored-cycle",
                    "phase": "ignored-phase",
                    "related_ledger_entry_id": "ignored-ledger",
                }
            ]
        },
    )

    with patch(_STORE_PATCH, return_value=store):
        response = await service.GetTimeline(
            gateway_pb2.GetTimelineRequest(deployment_id="test-deployment", limit=3),
            context,
        )

    assert [event.description for event in response.events] == ["state", "stored", "stored-tie"]
    assert [event.timestamp for event in response.events] == [
        int(datetime(2026, 1, 3, tzinfo=UTC).timestamp()),
        int(stored.timestamp.timestamp()),
        int(stored_tie.timestamp.timestamp()),
    ]
    stored_info = response.events[1]
    assert stored_info.tx_hash == "0xabc"
    assert json.loads(stored_info.details_json) == {"source": "store"}
    assert stored_info.chain == "arbitrum"
    assert stored_info.cycle_id == "cycle-1"
    assert stored_info.phase == "EXECUTE"
    assert stored_info.related_ledger_entry_id == "ledger-1"
    state_info = response.events[0]
    assert (state_info.cycle_id, state_info.phase, state_info.related_ledger_entry_id) == ("", "", "")
    assert response.has_more is True


@pytest.mark.skipif(not hasattr(time, "tzset"), reason="requires POSIX timezone control")
@pytest.mark.asyncio
async def test_naive_fallback_timestamp_is_interpreted_as_utc(service, context) -> None:
    original_tz = os.environ.get("TZ")
    os.environ["TZ"] = "GMT+8"
    time.tzset()
    try:
        store = MagicMock()
        store.get_events.return_value = []
        _set_state(
            service,
            {
                "execution_history": [
                    {
                        "timestamp": "2026-01-03T00:00:00",
                        "event_type": "EXECUTION",
                        "description": "naive-state",
                    }
                ]
            },
        )

        with patch(_STORE_PATCH, return_value=store):
            response = await service.GetTimeline(
                gateway_pb2.GetTimelineRequest(deployment_id="test-deployment"),
                context,
            )
    finally:
        if original_tz is None:
            os.environ.pop("TZ", None)
        else:
            os.environ["TZ"] = original_tz
        time.tzset()

    assert response.events[0].timestamp == int(datetime(2026, 1, 3, tzinfo=UTC).timestamp())


@pytest.mark.asyncio
async def test_hosted_read_uses_exact_deployment_id_across_sources(service, context) -> None:
    deployment_id = "platform-agent-uuid-123"
    other_id = "platform-agent-uuid-456"
    (service._strategies_root.parent / ".dashboard_events.json").write_text(
        json.dumps(
            {
                deployment_id: [
                    {
                        "timestamp": "2026-01-02T00:00:00+00:00",
                        "event_type": "TRADE",
                        "description": "requested-cache",
                    }
                ],
                other_id: [
                    {
                        "timestamp": "2026-01-04T00:00:00+00:00",
                        "event_type": "TRADE",
                        "description": "other-cache",
                    }
                ],
            }
        )
    )
    load_state = _set_state(
        service,
        {
            "execution_history": [
                {
                    "timestamp": "2026-01-03T00:00:00+00:00",
                    "event_type": "EXECUTION",
                    "description": "requested-state",
                }
            ]
        },
    )
    store = MagicMock()
    store.get_events.return_value = [_stored_event(deployment_id=other_id, description="other-store")]

    with patch(_STORE_PATCH, return_value=store):
        response = await service.GetTimeline(
            gateway_pb2.GetTimelineRequest(deployment_id=deployment_id, limit=10),
            context,
        )

    assert [event.description for event in response.events] == ["requested-state", "requested-cache"]
    store.get_events.assert_called_once_with(
        deployment_id=deployment_id,
        limit=10,
        event_type=None,
        since=None,
    )
    load_state.assert_awaited_once_with(deployment_id)


@pytest.mark.parametrize("source", ["store", "cache", "state"])
@pytest.mark.asyncio
async def test_malformed_record_does_not_hide_later_valid_record(service, context, source) -> None:
    store = MagicMock()
    store.get_events.return_value = []
    valid_description = f"valid-{source}"

    if source == "store":
        malformed = SimpleNamespace(deployment_id="test-deployment", timestamp="not-a-datetime")
        store.get_events.return_value = [malformed, _stored_event(description=valid_description)]
    elif source == "cache":
        (service._strategies_root.parent / ".dashboard_events.json").write_text(
            json.dumps(
                {
                    "test-deployment": [
                        None,
                        {"timestamp": "not-a-timestamp", "description": "malformed"},
                        {
                            "timestamp": "2026-01-02T00:00:00+00:00",
                            "event_type": "TRADE",
                            "description": valid_description,
                        },
                    ]
                }
            )
        )
    else:
        _set_state(
            service,
            {
                "execution_history": [
                    None,
                    {"timestamp": "not-a-timestamp", "description": "malformed"},
                    {
                        "timestamp": "2026-01-02T00:00:00+00:00",
                        "event_type": "EXECUTION",
                        "description": valid_description,
                    },
                ]
            },
        )

    with patch(_STORE_PATCH, return_value=store):
        response = await service.GetTimeline(
            gateway_pb2.GetTimelineRequest(deployment_id="test-deployment", limit=10),
            context,
        )

    assert [event.description for event in response.events] == [valid_description]


@pytest.mark.parametrize("source", ["cache", "state"])
@pytest.mark.asyncio
async def test_non_list_fallback_history_is_ignored(service, context, source) -> None:
    store = MagicMock()
    store.get_events.return_value = []

    if source == "cache":
        (service._strategies_root.parent / ".dashboard_events.json").write_text(
            json.dumps({"test-deployment": {"unexpected": "mapping"}})
        )
    else:
        _set_state(service, {"execution_history": {"unexpected": "mapping"}})

    with patch(_STORE_PATCH, return_value=store):
        response = await service.GetTimeline(
            gateway_pb2.GetTimelineRequest(deployment_id="test-deployment"),
            context,
        )

    assert list(response.events) == []


@pytest.mark.parametrize(
    ("source", "expected_description"),
    [("cache", "matching-earlier"), ("state", "matching")],
)
@pytest.mark.asyncio
async def test_filter_and_since_apply_before_fallback_source_limit(
    service,
    context,
    source,
    expected_description,
) -> None:
    records = [
        {
            "timestamp": "2026-01-04T00:00:00+00:00",
            "event_type": "ERROR",
            "description": "wrong-type",
        },
        {
            "event_type": "TRADE",
            "description": "unknown-time",
        },
        {
            "timestamp": "2025-12-31T00:00:00+00:00",
            "event_type": "TRADE",
            "description": "too-old",
        },
        {
            "timestamp": "2026-01-02T00:00:00+00:00",
            "event_type": "TRADE",
            "description": "matching-earlier",
        },
        {
            "timestamp": "2026-01-03T00:00:00+00:00",
            "event_type": "TRADE",
            "description": "matching",
        },
    ]
    store = MagicMock()
    store.get_events.return_value = []
    if source == "cache":
        (service._strategies_root.parent / ".dashboard_events.json").write_text(
            json.dumps({"test-deployment": records})
        )
    else:
        _set_state(service, {"execution_history": records})

    with patch(_STORE_PATCH, return_value=store):
        response = await service.GetTimeline(
            gateway_pb2.GetTimelineRequest(
                deployment_id="test-deployment",
                limit=1,
                event_type_filter="TRADE",
                since_timestamp=int(datetime(2026, 1, 1, tzinfo=UTC).timestamp()),
            ),
            context,
        )

    assert [event.description for event in response.events] == [expected_description]


@pytest.mark.asyncio
async def test_source_failures_degrade_through_cache_to_state(service, context) -> None:
    (service._strategies_root.parent / ".dashboard_events.json").write_text("not-json")
    _set_state(
        service,
        {
            "execution_history": [
                {
                    "event_type": "EXECUTION",
                    "description": "state-fallback",
                }
            ]
        },
    )
    store = MagicMock()
    store.get_events.side_effect = RuntimeError("store unavailable")

    with patch(_STORE_PATCH, return_value=store):
        response = await service.GetTimeline(
            gateway_pb2.GetTimelineRequest(deployment_id="test-deployment", limit=10),
            context,
        )

    assert [event.description for event in response.events] == ["state-fallback"]
    assert response.events[0].timestamp == 0
    assert response.events[0].details_json == "{}"
    assert response.has_more is False
