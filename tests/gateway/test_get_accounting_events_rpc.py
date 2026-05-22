"""Tests for GetAccountingEvents gRPC endpoint (VIB-3514).

Covers:
- Missing deployment_id → INVALID_ARGUMENT
- Warm backend missing get_accounting_events_sync → returns empty events
- Successful delegation → events returned, SQLite dict → AccountingEvent conversion
- Backend exception → fail-quiet (empty events, no status code set)
- GatewayStateManager.get_accounting_events_sync round-trip shape
"""

from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock

import grpc
import pytest

from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.state_service import StateServiceServicer

_DEPLOYMENT_ID = "deploy-abc123"
_POSITION_KEY = "lp:aerodrome:base:0xwallet:0xpool"


@pytest.fixture
def settings() -> GatewaySettings:
    return GatewaySettings(db_path=":memory:")


def _make_servicer(warm_rows: list[dict] | None = None, raise_exc: Exception | None = None) -> StateServiceServicer:
    servicer = StateServiceServicer(GatewaySettings(db_path=":memory:"))
    servicer._initialized = True

    state_manager = MagicMock()
    warm = MagicMock()

    if raise_exc is not None:
        warm.get_accounting_events_sync = MagicMock(side_effect=raise_exc)
    elif warm_rows is not None:
        warm.get_accounting_events_sync = MagicMock(return_value=warm_rows)
    else:
        # No get_accounting_events_sync attribute
        del warm.get_accounting_events_sync

    state_manager.warm_backend = warm
    servicer._state_manager = state_manager
    return servicer


def _make_context() -> MagicMock:
    ctx = MagicMock(spec=grpc.aio.ServicerContext)
    ctx.set_code = MagicMock()
    ctx.set_details = MagicMock()
    return ctx


@pytest.mark.asyncio
async def test_get_accounting_events_missing_deployment_id() -> None:
    servicer = _make_servicer(warm_rows=[])
    ctx = _make_context()

    req = gateway_pb2.GetAccountingEventsRequest(deployment_id="", position_key="")
    resp = await servicer.GetAccountingEvents(req, ctx)

    ctx.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)
    assert "deployment_id" in ctx.set_details.call_args.args[0]
    assert "required" in ctx.set_details.call_args.args[0]
    assert len(resp.events) == 0


@pytest.mark.asyncio
async def test_get_accounting_events_backend_missing_method() -> None:
    servicer = _make_servicer(warm_rows=None)  # warm has no get_accounting_events_sync
    ctx = _make_context()

    req = gateway_pb2.GetAccountingEventsRequest(deployment_id=_DEPLOYMENT_ID)
    resp = await servicer.GetAccountingEvents(req, ctx)

    ctx.set_code.assert_not_called()
    assert len(resp.events) == 0


@pytest.mark.asyncio
async def test_get_accounting_events_empty_result() -> None:
    servicer = _make_servicer(warm_rows=[])
    ctx = _make_context()

    req = gateway_pb2.GetAccountingEventsRequest(deployment_id=_DEPLOYMENT_ID)
    resp = await servicer.GetAccountingEvents(req, ctx)

    ctx.set_code.assert_not_called()
    assert len(resp.events) == 0


@pytest.mark.asyncio
async def test_get_accounting_events_rows_converted() -> None:
    now_iso = "2026-01-01T00:00:00+00:00"
    rows = [
        {
            "id": "a1b2c3d4-0000-0000-0000-000000000001",
            "deployment_id": _DEPLOYMENT_ID,
            "cycle_id": "cycle-1",
            "execution_mode": "paper",
            "timestamp": now_iso,
            "chain": "base",
            "protocol": "aerodrome",
            "wallet_address": "0xwallet",
            "event_type": "LP_OPEN",
            "position_key": _POSITION_KEY,
            "ledger_entry_id": "led-1",
            "tx_hash": "0xtxhash",
            "confidence": "ESTIMATED",
            "payload_json": '{"event_type":"LP_OPEN","position_key":"pk1"}',
            "schema_version": 1,
        }
    ]
    servicer = _make_servicer(warm_rows=rows)
    ctx = _make_context()

    req = gateway_pb2.GetAccountingEventsRequest(deployment_id=_DEPLOYMENT_ID)
    resp = await servicer.GetAccountingEvents(req, ctx)

    ctx.set_code.assert_not_called()
    assert len(resp.events) == 1
    row = resp.events[0]
    assert row.id == "a1b2c3d4-0000-0000-0000-000000000001"
    assert row.deployment_id == _DEPLOYMENT_ID
    assert row.event_type == "LP_OPEN"
    assert row.position_key == _POSITION_KEY
    # timestamp is serialised as int64 epoch seconds in main's proto
    assert isinstance(row.timestamp, int)
    assert row.payload_json == b'{"event_type":"LP_OPEN","position_key":"pk1"}'
    assert row.schema_version == 1


@pytest.mark.asyncio
async def test_get_accounting_events_position_key_filter_passed_through() -> None:
    servicer = _make_servicer(warm_rows=[])
    ctx = _make_context()

    req = gateway_pb2.GetAccountingEventsRequest(deployment_id=_DEPLOYMENT_ID, position_key=_POSITION_KEY)
    await servicer.GetAccountingEvents(req, ctx)

    warm = servicer._state_manager.warm_backend  # type: ignore[union-attr]
    call_kwargs = warm.get_accounting_events_sync.call_args
    assert call_kwargs is not None
    assert call_kwargs.kwargs.get("position_key") == _POSITION_KEY or _POSITION_KEY in call_kwargs.args


@pytest.mark.asyncio
async def test_get_accounting_events_backend_exception() -> None:
    servicer = _make_servicer(raise_exc=RuntimeError("db is gone"))
    ctx = _make_context()

    req = gateway_pb2.GetAccountingEventsRequest(deployment_id=_DEPLOYMENT_ID)
    resp = await servicer.GetAccountingEvents(req, ctx)

    # Main's service is fail-quiet on SQLite exceptions — logs warning and returns empty.
    ctx.set_code.assert_not_called()
    assert len(resp.events) == 0


# ---------------------------------------------------------------------------
# GatewayStateManager.get_accounting_events_sync unit tests
# ---------------------------------------------------------------------------


def _make_gsm(events: list[dict] | None = None, raise_exc: Exception | None = None):
    from almanak.framework.state.gateway_state_manager import GatewayStateManager

    if raise_exc is not None:
        mock_client = MagicMock()
        mock_client.state.GetAccountingEvents = MagicMock(side_effect=raise_exc)
    else:
        # Build AccountingEvent proto objects (main's proto type, epoch int timestamp)
        rows = []
        for row in (events or []):
            payload = row.get("payload_json", "")
            payload_bytes = payload.encode("utf-8") if isinstance(payload, str) else payload
            ts_iso = row.get("timestamp", "")
            try:
                epoch = int(datetime.fromisoformat(ts_iso).timestamp()) if ts_iso else 0
            except (ValueError, TypeError):
                epoch = 0
            rows.append(
                gateway_pb2.AccountingEvent(
                    id=row.get("id", ""),
                    deployment_id=row.get("deployment_id", ""),
                    cycle_id=row.get("cycle_id", ""),
                    execution_mode=row.get("execution_mode", ""),
                    timestamp=epoch,
                    chain=row.get("chain", ""),
                    protocol=row.get("protocol", ""),
                    wallet_address=row.get("wallet_address", ""),
                    event_type=row.get("event_type", ""),
                    position_key=row.get("position_key", ""),
                    ledger_entry_id=row.get("ledger_entry_id", ""),
                    tx_hash=row.get("tx_hash", ""),
                    confidence=row.get("confidence", ""),
                    payload_json=payload_bytes,
                    schema_version=int(row.get("schema_version", 1)),
                )
            )
        mock_response = MagicMock()
        mock_response.events = rows
        mock_client = MagicMock()
        mock_client.state.GetAccountingEvents = MagicMock(return_value=mock_response)

    return GatewayStateManager(client=mock_client)


def test_gsm_get_accounting_events_sync_empty() -> None:
    gsm = _make_gsm(events=[])
    result = gsm.get_accounting_events_sync("deploy-1")
    assert result == []


def test_gsm_get_accounting_events_sync_returns_dicts() -> None:
    rows = [
        {
            "id": "row-id-1",
            "deployment_id": "dep-1",
            "cycle_id": "cycle-1",
            "execution_mode": "paper",
            "timestamp": "2026-01-01T00:00:00+00:00",
            "chain": "base",
            "protocol": "aerodrome",
            "wallet_address": "0xwallet",
            "event_type": "LP_OPEN",
            "position_key": "lp:aerodrome:base:0xwallet:0xpool",
            "ledger_entry_id": "",
            "tx_hash": "0xtx",
            "confidence": "ESTIMATED",
            "payload_json": '{"event_type":"LP_OPEN"}',
            "schema_version": 1,
        }
    ]
    gsm = _make_gsm(events=rows)
    result = gsm.get_accounting_events_sync("dep-1")

    assert len(result) == 1
    row = result[0]
    assert row["event_type"] == "LP_OPEN"
    assert row["deployment_id"] == "dep-1"
    # _proto_event_to_dict converts epoch → ISO string for downstream consumers
    assert "2026-01-01" in row["timestamp"]
    assert row["payload_json"] == '{"event_type":"LP_OPEN"}'
    assert row["schema_version"] == 1


def test_gsm_get_accounting_events_sync_exception_returns_empty() -> None:
    gsm = _make_gsm(raise_exc=RuntimeError("rpc gone"))
    result = gsm.get_accounting_events_sync("dep-1")
    assert result == []


def test_gsm_get_accounting_events_sync_empty_deployment_id_returns_empty() -> None:
    gsm = _make_gsm(events=[])
    result = gsm.get_accounting_events_sync("")
    assert result == []
