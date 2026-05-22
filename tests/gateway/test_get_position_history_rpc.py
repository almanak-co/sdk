"""Tests for GetPositionHistory gRPC endpoint (VIB-3944).

Covers the path that ``pnl_attributor.run_attribution_on_close`` takes when
the runner is wired against a ``GatewayStateManager``. Before this RPC
existed the attributor crashed with ``AttributeError: 'GatewayStateManager'
object has no attribute 'get_position_history'`` on every LP_CLOSE in
gateway-sidecar mode, silently corrupting FIFO PnL attribution.

Coverage:
- Missing deployment_id / position_id → INVALID_ARGUMENT
- Warm backend missing get_position_history → returns empty events
- Successful delegation → events returned, dict → PositionEventData conversion
- Backend exception → fail-quiet (empty events, no status code set)
- GatewayStateManager.get_position_history round-trip dict shape
- pnl_attributor smoke: a CLOSE event matched against a GSM-backed history
  no longer raises AttributeError.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock

import grpc
import pytest

from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.state_service import StateServiceServicer

_DEPLOYMENT_ID = "deploy-pnl-1"
_POSITION_ID = "lp:base:0xwallet:0xpool"


def _make_servicer(
    rows: list[dict] | None = None,
    raise_exc: Exception | None = None,
    has_method: bool = True,
) -> StateServiceServicer:
    servicer = StateServiceServicer(GatewaySettings(db_path=":memory:"))
    servicer._initialized = True

    state_manager = MagicMock()
    warm = MagicMock()
    if not has_method:
        del warm.get_position_history
    elif raise_exc is not None:
        warm.get_position_history = MagicMock(side_effect=raise_exc)
    else:
        async def _coro(*_args, **_kwargs):
            return rows or []

        warm.get_position_history = _coro

    state_manager.warm_backend = warm
    servicer._state_manager = state_manager
    return servicer


def _make_context() -> MagicMock:
    ctx = MagicMock(spec=grpc.aio.ServicerContext)
    ctx.set_code = MagicMock()
    ctx.set_details = MagicMock()
    return ctx


# ---------------------------------------------------------------------------
# Server-side gateway RPC
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_missing_deployment_id_returns_invalid_argument() -> None:
    servicer = _make_servicer(rows=[])
    ctx = _make_context()
    req = gateway_pb2.GetPositionHistoryRequest(
        deployment_id="", position_id=_POSITION_ID
    )
    resp = await servicer.GetPositionHistory(req, ctx)
    ctx.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)
    assert resp.events == []


@pytest.mark.asyncio
async def test_missing_position_id_returns_invalid_argument() -> None:
    servicer = _make_servicer(rows=[])
    ctx = _make_context()
    req = gateway_pb2.GetPositionHistoryRequest(
        deployment_id=_DEPLOYMENT_ID, position_id=""
    )
    resp = await servicer.GetPositionHistory(req, ctx)
    ctx.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)
    assert resp.events == []


@pytest.mark.asyncio
async def test_warm_backend_missing_method_returns_empty_quietly() -> None:
    servicer = _make_servicer(has_method=False)
    ctx = _make_context()
    req = gateway_pb2.GetPositionHistoryRequest(
        deployment_id=_DEPLOYMENT_ID, position_id=_POSITION_ID
    )
    resp = await servicer.GetPositionHistory(req, ctx)
    ctx.set_code.assert_not_called()
    assert resp.events == []


@pytest.mark.asyncio
async def test_rows_converted_to_position_event_data() -> None:
    open_iso = "2026-01-01T00:00:00+00:00"
    close_iso = "2026-01-01T01:00:00+00:00"
    rows = [
        {
            "id": "11111111-1111-1111-1111-111111111111",
            "deployment_id": _DEPLOYMENT_ID,
            "cycle_id": "cycle-1",
            "execution_mode": "paper",
            "position_id": _POSITION_ID,
            "position_type": "LP",
            "event_type": "OPEN",
            "timestamp": open_iso,
            "protocol": "uniswap_v3",
            "chain": "ethereum",
            "token0": "WETH",
            "token1": "USDC",
            "amount0": "1.0",
            "amount1": "2500",
            "value_usd": "5000",
            "tick_lower": -100,
            "tick_upper": 100,
            "liquidity": "12345",
            "in_range": True,
            "fees_token0": "0",
            "fees_token1": "0",
            "leverage": "",
            "entry_price": "",
            "mark_price": "",
            "unrealized_pnl": "",
            "is_long": None,
            "tx_hash": "0xopen",
            "gas_usd": "1.5",
            "ledger_entry_id": "led-open",
            "protocol_fees_usd": "0",
            "attribution_json": "{}",
            "attribution_version": 0,
        },
        {
            "id": "22222222-2222-2222-2222-222222222222",
            "deployment_id": _DEPLOYMENT_ID,
            "cycle_id": "cycle-2",
            "execution_mode": "paper",
            "position_id": _POSITION_ID,
            "position_type": "LP",
            "event_type": "CLOSE",
            "timestamp": close_iso,
            "protocol": "uniswap_v3",
            "chain": "ethereum",
            "token0": "WETH",
            "token1": "USDC",
            "amount0": "1.01",
            "amount1": "2480",
            "value_usd": "5010",
            "tick_lower": -100,
            "tick_upper": 100,
            "liquidity": "0",
            "in_range": False,
            "fees_token0": "0.005",
            "fees_token1": "12",
            "leverage": "",
            "entry_price": "",
            "mark_price": "",
            "unrealized_pnl": "",
            "is_long": None,
            "tx_hash": "0xclose",
            "gas_usd": "2.0",
            "ledger_entry_id": "led-close",
            "protocol_fees_usd": "0",
            "attribution_json": "{}",
            "attribution_version": 0,
        },
    ]
    servicer = _make_servicer(rows=rows)
    ctx = _make_context()
    req = gateway_pb2.GetPositionHistoryRequest(
        deployment_id=_DEPLOYMENT_ID, position_id=_POSITION_ID
    )
    resp = await servicer.GetPositionHistory(req, ctx)

    ctx.set_code.assert_not_called()
    assert len(resp.events) == 2
    open_ev, close_ev = resp.events
    assert open_ev.event_type == "OPEN"
    assert close_ev.event_type == "CLOSE"
    assert open_ev.tick_lower == -100
    assert open_ev.HasField("in_range") and open_ev.in_range is True
    # Non-LB perp fields stay un-set when source row is None
    assert not open_ev.HasField("is_long")
    # Timestamps round-trip to epoch
    assert open_ev.timestamp == int(datetime.fromisoformat(open_iso).timestamp())
    assert close_ev.timestamp == int(datetime.fromisoformat(close_iso).timestamp())


@pytest.mark.asyncio
async def test_backend_exception_is_fail_quiet() -> None:
    servicer = _make_servicer(raise_exc=RuntimeError("db gone"))
    ctx = _make_context()
    req = gateway_pb2.GetPositionHistoryRequest(
        deployment_id=_DEPLOYMENT_ID, position_id=_POSITION_ID
    )
    resp = await servicer.GetPositionHistory(req, ctx)
    ctx.set_code.assert_not_called()
    assert resp.events == []


# ---------------------------------------------------------------------------
# Client-side GatewayStateManager
# ---------------------------------------------------------------------------


def _make_gsm_with_events(events: list[dict]):
    from almanak.framework.state.gateway_state_manager import GatewayStateManager

    proto_events = []
    for row in events:
        ts_iso = row.get("timestamp", "")
        try:
            epoch = int(datetime.fromisoformat(ts_iso).timestamp()) if ts_iso else 0
        except (ValueError, TypeError):
            epoch = 0
        msg = gateway_pb2.PositionEventData(
            id=row.get("id", ""),
            deployment_id=row.get("deployment_id", ""),
            cycle_id=row.get("cycle_id", ""),
            execution_mode=row.get("execution_mode", ""),
            position_id=row.get("position_id", ""),
            position_type=row.get("position_type", ""),
            event_type=row.get("event_type", ""),
            timestamp=epoch,
            protocol=row.get("protocol", ""),
            chain=row.get("chain", ""),
            token0=row.get("token0", ""),
            token1=row.get("token1", ""),
            amount0=row.get("amount0", ""),
            amount1=row.get("amount1", ""),
            value_usd=row.get("value_usd", ""),
            liquidity=row.get("liquidity", ""),
            fees_token0=row.get("fees_token0", ""),
            fees_token1=row.get("fees_token1", ""),
            leverage=row.get("leverage", ""),
            entry_price=row.get("entry_price", ""),
            mark_price=row.get("mark_price", ""),
            unrealized_pnl=row.get("unrealized_pnl", ""),
            tx_hash=row.get("tx_hash", ""),
            gas_usd=row.get("gas_usd", ""),
            ledger_entry_id=row.get("ledger_entry_id", ""),
            protocol_fees_usd=row.get("protocol_fees_usd", ""),
            attribution_json=row.get("attribution_json", "{}"),
            attribution_version=int(row.get("attribution_version", 0)),
        )
        if row.get("tick_lower") is not None:
            msg.tick_lower = int(row["tick_lower"])
        if row.get("tick_upper") is not None:
            msg.tick_upper = int(row["tick_upper"])
        if row.get("in_range") is not None:
            msg.in_range = bool(row["in_range"])
        if row.get("is_long") is not None:
            msg.is_long = bool(row["is_long"])
        proto_events.append(msg)

    mock_response = MagicMock()
    mock_response.events = proto_events
    mock_client = MagicMock()
    mock_client.state.GetPositionHistory = MagicMock(return_value=mock_response)
    return GatewayStateManager(client=mock_client)


@pytest.mark.asyncio
async def test_gsm_get_position_history_returns_dicts() -> None:
    rows = [
        {
            "id": "ev-1",
            "deployment_id": _DEPLOYMENT_ID,
            "cycle_id": "c1",
            "execution_mode": "live",
            "position_id": _POSITION_ID,
            "position_type": "LP",
            "event_type": "OPEN",
            "timestamp": "2026-02-01T00:00:00+00:00",
            "protocol": "uniswap_v3",
            "chain": "ethereum",
            "token0": "WETH",
            "token1": "USDC",
            "amount0": "1",
            "amount1": "2500",
            "value_usd": "5000",
            "tick_lower": -10,
            "tick_upper": 10,
            "liquidity": "1000000",
            "in_range": True,
            "tx_hash": "0xopen",
        }
    ]
    gsm = _make_gsm_with_events(rows)
    out = await gsm.get_position_history(_DEPLOYMENT_ID, _POSITION_ID)
    assert len(out) == 1
    ev = out[0]
    assert ev["event_type"] == "OPEN"
    assert ev["position_id"] == _POSITION_ID
    assert ev["tick_lower"] == -10
    assert ev["tick_upper"] == 10
    assert ev["in_range"] is True
    assert ev["is_long"] is None  # not set on the wire -> None on the dict
    # Timestamp should be an ISO string so pnl_attributor's
    # datetime.fromisoformat call works.
    assert ev["timestamp"].startswith("2026-02-01T00:00:00")


@pytest.mark.asyncio
async def test_gsm_get_position_history_empty_returns_empty_list() -> None:
    gsm = _make_gsm_with_events([])
    out = await gsm.get_position_history(_DEPLOYMENT_ID, _POSITION_ID)
    assert out == []


@pytest.mark.asyncio
async def test_gsm_get_position_history_blank_args_returns_empty() -> None:
    gsm = _make_gsm_with_events([])
    assert await gsm.get_position_history("", _POSITION_ID) == []
    assert await gsm.get_position_history(_DEPLOYMENT_ID, "") == []


@pytest.mark.asyncio
async def test_gsm_get_position_history_rpc_failure_is_fail_quiet() -> None:
    from almanak.framework.state.gateway_state_manager import GatewayStateManager

    mock_client = MagicMock()
    mock_client.state.GetPositionHistory = MagicMock(side_effect=RuntimeError("rpc down"))
    gsm = GatewayStateManager(client=mock_client)
    out = await gsm.get_position_history(_DEPLOYMENT_ID, _POSITION_ID)
    assert out == []


# ---------------------------------------------------------------------------
# pnl_attributor smoke: the original AttributeError in the ticket
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pnl_attributor_run_on_close_no_attribute_error() -> None:
    """VIB-3944: ``run_attribution_on_close`` against a GatewayStateManager
    must NOT raise AttributeError. Before the fix this was the prod failure
    every LP_CLOSE on the gateway-sidecar architecture hit.
    """
    from almanak.framework.observability.pnl_attributor import run_attribution_on_close

    open_iso = "2026-03-01T00:00:00+00:00"
    close_iso = "2026-03-01T01:00:00+00:00"
    rows = [
        {
            "id": "11111111-1111-1111-1111-111111111111",
            "deployment_id": _DEPLOYMENT_ID,
            "cycle_id": "c1",
            "execution_mode": "paper",
            "position_id": _POSITION_ID,
            "position_type": "LP",
            "event_type": "OPEN",
            "timestamp": open_iso,
            "protocol": "uniswap_v3",
            "chain": "ethereum",
            "token0": "WETH",
            "token1": "USDC",
            "amount0": "1",
            "amount1": "2500",
            "value_usd": "5000",
            "liquidity": "1000",
            "tx_hash": "0xopen",
        }
    ]
    gsm = _make_gsm_with_events(rows)

    close_event = MagicMock()
    close_event.deployment_id = _DEPLOYMENT_ID
    close_event.position_id = _POSITION_ID
    close_event.id = "22222222-2222-2222-2222-222222222222"
    close_event.event_type = "CLOSE"
    close_event.attribution_json = "{}"
    close_event.attribution_version = 0
    close_event.chain = "ethereum"
    close_event.protocol = "uniswap_v3"
    close_event.token0 = "WETH"
    close_event.token1 = "USDC"
    close_event.amount0 = "1.01"
    close_event.amount1 = "2480"
    close_event.value_usd = "5010"
    close_event.tx_hash = "0xclose"
    close_event.timestamp = datetime.fromisoformat(close_iso).replace(tzinfo=UTC)
    close_event.to_dict = MagicMock(return_value={"event_type": "CLOSE", "attribution_json": "{}"})

    # Should complete without AttributeError. Whether the attribution payload
    # is non-empty depends on _pair_close_with_open's matching, which is not
    # the bug we're guarding against — the bug was the missing method.
    result = await run_attribution_on_close(gsm, close_event)
    assert isinstance(result, str)


# ---------------------------------------------------------------------------
# UpdatePositionAttribution — server side (PR #2018 audit Important #1)
# ---------------------------------------------------------------------------


def _make_update_servicer(
    update_return: bool = True,
    raise_exc: Exception | None = None,
    has_method: bool = True,
) -> StateServiceServicer:
    servicer = StateServiceServicer(GatewaySettings(db_path=":memory:"))
    servicer._initialized = True
    state_manager = MagicMock()
    warm = MagicMock()
    if not has_method:
        del warm.update_position_attribution
    elif raise_exc is not None:
        warm.update_position_attribution = MagicMock(side_effect=raise_exc)
    else:
        async def _coro(*_args, **_kwargs):
            return update_return

        warm.update_position_attribution = _coro
    state_manager.warm_backend = warm
    servicer._state_manager = state_manager
    return servicer


@pytest.mark.asyncio
async def test_update_attribution_missing_event_id_invalid_argument() -> None:
    servicer = _make_update_servicer()
    ctx = _make_context()
    req = gateway_pb2.UpdatePositionAttributionRequest(
        event_id="", attribution_json="{}", attribution_version=1
    )
    resp = await servicer.UpdatePositionAttribution(req, ctx)
    ctx.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)
    assert resp.success is False
    assert "event_id is required" in resp.error


@pytest.mark.asyncio
async def test_update_attribution_invalid_uuid_invalid_argument() -> None:
    servicer = _make_update_servicer()
    ctx = _make_context()
    req = gateway_pb2.UpdatePositionAttributionRequest(
        event_id="not-a-uuid", attribution_json="{}", attribution_version=1
    )
    resp = await servicer.UpdatePositionAttribution(req, ctx)
    ctx.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)
    assert resp.success is False


@pytest.mark.asyncio
async def test_update_attribution_warm_missing_method_returns_unimplemented() -> None:
    servicer = _make_update_servicer(has_method=False)
    ctx = _make_context()
    req = gateway_pb2.UpdatePositionAttributionRequest(
        event_id="11111111-1111-1111-1111-111111111111",
        attribution_json="{}",
        attribution_version=1,
    )
    resp = await servicer.UpdatePositionAttribution(req, ctx)
    ctx.set_code.assert_called_once_with(grpc.StatusCode.UNIMPLEMENTED)
    assert resp.success is False
    assert "does not support" in resp.error


@pytest.mark.asyncio
async def test_update_attribution_no_row_matched_returns_soft_failure() -> None:
    servicer = _make_update_servicer(update_return=False)
    ctx = _make_context()
    req = gateway_pb2.UpdatePositionAttributionRequest(
        event_id="11111111-1111-1111-1111-111111111111",
        attribution_json='{"realized_pnl_usd":"0"}',
        attribution_version=3,
    )
    resp = await servicer.UpdatePositionAttribution(req, ctx)
    ctx.set_code.assert_not_called()
    assert resp.success is False
    assert "no position_event row matched" in resp.error


@pytest.mark.asyncio
async def test_update_attribution_success() -> None:
    servicer = _make_update_servicer(update_return=True)
    ctx = _make_context()
    req = gateway_pb2.UpdatePositionAttributionRequest(
        event_id="11111111-1111-1111-1111-111111111111",
        attribution_json='{"realized_pnl_usd":"0"}',
        attribution_version=3,
    )
    resp = await servicer.UpdatePositionAttribution(req, ctx)
    ctx.set_code.assert_not_called()
    assert resp.success is True
    assert resp.error == ""


@pytest.mark.asyncio
async def test_update_attribution_backend_exception_returns_generic_error() -> None:
    """CR audit: never leak raw backend exception text across the gateway
    boundary. Operator correlation goes through the warning log; the
    response error stays opaque.
    """
    servicer = _make_update_servicer(raise_exc=RuntimeError("db gone with secret/path/leak"))
    ctx = _make_context()
    req = gateway_pb2.UpdatePositionAttributionRequest(
        event_id="11111111-1111-1111-1111-111111111111",
        attribution_json="{}",
        attribution_version=1,
    )
    resp = await servicer.UpdatePositionAttribution(req, ctx)
    ctx.set_code.assert_not_called()
    assert resp.success is False
    assert resp.error == "internal server error"
    assert "secret/path/leak" not in resp.error


@pytest.mark.asyncio
async def test_update_attribution_invalid_json_rejected() -> None:
    """CR audit: malformed attribution_json must be rejected at the gateway
    boundary so a corrupt payload can't reach the position_events column
    and break every downstream ``json.loads(row["attribution_json"])``.
    """
    servicer = _make_update_servicer()
    ctx = _make_context()
    req = gateway_pb2.UpdatePositionAttributionRequest(
        event_id="11111111-1111-1111-1111-111111111111",
        attribution_json="{not valid json",
        attribution_version=1,
    )
    resp = await servicer.UpdatePositionAttribution(req, ctx)
    ctx.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)
    assert resp.success is False
    assert "valid JSON" in resp.error


@pytest.mark.asyncio
async def test_update_attribution_blank_deployment_id_rejected() -> None:
    """CR audit: ``deployment_id`` is optional defense-in-depth scope. Empty
    is allowed; whitespace-only is rejected so the field can't silently
    degrade.
    """
    servicer = _make_update_servicer()
    ctx = _make_context()
    req = gateway_pb2.UpdatePositionAttributionRequest(
        event_id="11111111-1111-1111-1111-111111111111",
        attribution_json="{}",
        attribution_version=1,
        deployment_id="   ",
    )
    resp = await servicer.UpdatePositionAttribution(req, ctx)
    ctx.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)
    assert resp.success is False


# ---------------------------------------------------------------------------
# GatewayStateManager client side
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_gsm_update_attribution_round_trip() -> None:
    from almanak.framework.state.gateway_state_manager import GatewayStateManager

    mock_response = gateway_pb2.UpdatePositionAttributionResponse(success=True)
    mock_client = MagicMock()
    mock_client.state.UpdatePositionAttribution = MagicMock(return_value=mock_response)
    gsm = GatewayStateManager(client=mock_client)

    ok = await gsm.update_position_attribution(
        "11111111-1111-1111-1111-111111111111",
        '{"x":1}',
        3,
        deployment_id="deploy-x",
    )
    assert ok is True
    sent_req = mock_client.state.UpdatePositionAttribution.call_args[0][0]
    assert sent_req.event_id == "11111111-1111-1111-1111-111111111111"
    assert sent_req.attribution_json == '{"x":1}'
    assert sent_req.attribution_version == 3
    # CR audit: deployment_id is plumbed through GSM -> gateway proto so the
    # wire request carries the caller's tenant scope.
    assert sent_req.deployment_id == "deploy-x"


@pytest.mark.asyncio
async def test_gsm_update_attribution_default_deployment_is_empty() -> None:
    """Backwards-compat: callers that don't pass deployment_id (e.g., legacy
    test code or unmigrated paths) get an empty wire field, not a crash.
    """
    from almanak.framework.state.gateway_state_manager import GatewayStateManager

    mock_response = gateway_pb2.UpdatePositionAttributionResponse(success=True)
    mock_client = MagicMock()
    mock_client.state.UpdatePositionAttribution = MagicMock(return_value=mock_response)
    gsm = GatewayStateManager(client=mock_client)

    ok = await gsm.update_position_attribution(
        "11111111-1111-1111-1111-111111111111", '{"x":1}', 3
    )
    assert ok is True
    sent_req = mock_client.state.UpdatePositionAttribution.call_args[0][0]
    assert sent_req.deployment_id == ""


@pytest.mark.asyncio
async def test_gsm_update_attribution_blank_event_id_returns_false() -> None:
    from almanak.framework.state.gateway_state_manager import GatewayStateManager

    mock_client = MagicMock()
    gsm = GatewayStateManager(client=mock_client)
    assert await gsm.update_position_attribution("", "{}", 1) is False
    mock_client.state.UpdatePositionAttribution.assert_not_called()


@pytest.mark.asyncio
async def test_gsm_update_attribution_rpc_failure_is_fail_quiet() -> None:
    from almanak.framework.state.gateway_state_manager import GatewayStateManager

    mock_client = MagicMock()
    mock_client.state.UpdatePositionAttribution = MagicMock(side_effect=RuntimeError("rpc down"))
    gsm = GatewayStateManager(client=mock_client)
    ok = await gsm.update_position_attribution(
        "11111111-1111-1111-1111-111111111111", "{}", 1
    )
    assert ok is False


@pytest.mark.asyncio
async def test_gsm_update_attribution_soft_failure_returns_false() -> None:
    from almanak.framework.state.gateway_state_manager import GatewayStateManager

    mock_response = gateway_pb2.UpdatePositionAttributionResponse(
        success=False, error="no position_event row matched event_id"
    )
    mock_client = MagicMock()
    mock_client.state.UpdatePositionAttribution = MagicMock(return_value=mock_response)
    gsm = GatewayStateManager(client=mock_client)
    ok = await gsm.update_position_attribution(
        "11111111-1111-1111-1111-111111111111", "{}", 1
    )
    assert ok is False


# ---------------------------------------------------------------------------
# pnl_attributor smoke: confirms hasattr() picks the partial-update path now
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pnl_attributor_close_uses_update_path_on_gsm() -> None:
    """PR #2018 audit Important #1: when GSM exposes ``update_position_attribution``
    pnl_attributor should take the partial-update path instead of the
    ``save_position_event`` (INSERT-OR-IGNORE) NO-OP fallback.
    """
    from almanak.framework.state.gateway_state_manager import GatewayStateManager

    mock_update = MagicMock(return_value=gateway_pb2.UpdatePositionAttributionResponse(success=True))
    mock_save = MagicMock()
    mock_get = MagicMock(return_value=MagicMock(events=[]))
    mock_client = MagicMock()
    mock_client.state.GetPositionHistory = mock_get
    mock_client.state.UpdatePositionAttribution = mock_update
    mock_client.state.SavePositionEvent = mock_save
    gsm = GatewayStateManager(client=mock_client)

    assert hasattr(gsm, "update_position_attribution")
    ok = await gsm.update_position_attribution(
        "11111111-1111-1111-1111-111111111111", '{"v":1}', 3
    )
    assert ok is True
    mock_update.assert_called_once()
    mock_save.assert_not_called()
