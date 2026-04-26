"""Tests for SaveAccountingEvent and SavePositionEvent gRPC endpoints (VIB-3449).

Covers:
- strategy_id / id validation (missing, non-UUID)
- timestamp validation (zero/negative)
- event_type validation (unknown types → INVALID_ARGUMENT)
- position_type / event_type enum validation for SavePositionEvent
- payload_json validation (missing, invalid UTF-8) for SaveAccountingEvent
- Unsupported warm backend → UNIMPLEMENTED
- Successful delegation → success=True
- Backend exception → INTERNAL
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock

import grpc
import pytest

from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.state_service import StateServiceServicer

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_VALID_UUID = "550e8400-e29b-41d4-a716-446655440000"
_VALID_TS = 1_712_000_000
_VALID_STRATEGY = "test-strategy"

_LENDING_PAYLOAD = json.dumps(
    {
        "event_type": "SUPPLY",
        "position_key": "aave-usdc",
        "market_id": "0xmarketid",
        "asset": "USDC",
        "collateral_value_before_usd": "1000",
        "collateral_value_after_usd": "1100",
        "debt_value_before_usd": None,
        "debt_value_after_usd": None,
        "net_equity_before_usd": "1000",
        "net_equity_after_usd": "1100",
        "health_factor_before": None,
        "health_factor_after": None,
        "liquidation_threshold": None,
        "lltv": None,
        "supply_apr_bps": 50,
        "borrow_apr_bps": None,
        "principal_delta_usd": "100",
        "interest_delta_usd": "0",
        "gas_usd": "1.50",
        "confidence": "HIGH",
        "unavailable_reason": "",
        "schema_version": 1,
    }
).encode("utf-8")


@pytest.fixture
def service() -> StateServiceServicer:
    svc = StateServiceServicer(GatewaySettings())
    svc._initialized = True
    svc._snapshot_pool_initialized = True
    svc._snapshot_pool = None
    svc._ensure_initialized = AsyncMock()
    svc._ensure_snapshot_pool = AsyncMock()
    return svc


@pytest.fixture
def ctx() -> MagicMock:
    c = MagicMock(spec=grpc.aio.ServicerContext)
    c.set_code = MagicMock()
    c.set_details = MagicMock()
    return c


def _accounting_request(**overrides) -> gateway_pb2.SaveAccountingEventRequest:
    defaults = dict(
        id=_VALID_UUID,
        strategy_id=_VALID_STRATEGY,
        deployment_id="deploy-1",
        cycle_id="cycle-1",
        execution_mode="paper",
        timestamp=_VALID_TS,
        chain="arbitrum",
        protocol="aave_v3",
        wallet_address="0xwallet",
        tx_hash="0xtx",
        ledger_entry_id=_VALID_UUID,
        event_type="SUPPLY",
        position_key="aave-usdc",
        confidence="HIGH",
        payload_json=_LENDING_PAYLOAD,
        schema_version=1,
    )
    defaults.update(overrides)
    return gateway_pb2.SaveAccountingEventRequest(**defaults)


def _position_request(**overrides) -> gateway_pb2.SavePositionEventRequest:
    defaults = dict(
        id=_VALID_UUID,
        deployment_id="deploy-1",
        cycle_id="cycle-1",
        execution_mode="paper",
        position_id="pos-1",
        position_type="LP",
        event_type="OPEN",
        timestamp=_VALID_TS,
        protocol="uniswap_v3",
        chain="arbitrum",
        token0="USDC",
        token1="WETH",
        amount0="100",
        amount1="0.05",
        value_usd="200",
        liquidity="0",
        fees_token0="0",
        fees_token1="0",
        leverage="1",
        entry_price="2000",
        mark_price="2000",
        unrealized_pnl="0",
        tx_hash="0xtx",
        gas_usd="1.50",
        ledger_entry_id=_VALID_UUID,
        protocol_fees_usd="0",
        attribution_json="{}",
        attribution_version=1,
    )
    defaults.update(overrides)
    return gateway_pb2.SavePositionEventRequest(**defaults)


# ---------------------------------------------------------------------------
# SaveAccountingEvent — validation tests
# ---------------------------------------------------------------------------


class TestSaveAccountingEventValidation:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("strategy_id", ["", "   "])
    async def test_missing_strategy_id(self, service, ctx, strategy_id):
        req = _accounting_request(strategy_id=strategy_id)
        resp = await service.SaveAccountingEvent(req, ctx)
        assert resp.success is False
        ctx.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)

    @pytest.mark.asyncio
    async def test_missing_event_id(self, service, ctx):
        req = _accounting_request(id="")
        resp = await service.SaveAccountingEvent(req, ctx)
        assert resp.success is False
        assert "id is required" in resp.error
        ctx.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)

    @pytest.mark.asyncio
    async def test_invalid_uuid_event_id(self, service, ctx):
        req = _accounting_request(id="not-a-uuid")
        resp = await service.SaveAccountingEvent(req, ctx)
        assert resp.success is False
        assert "valid UUID" in resp.error
        ctx.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("ts", [0, -1])
    async def test_non_positive_timestamp(self, service, ctx, ts):
        req = _accounting_request(timestamp=ts)
        resp = await service.SaveAccountingEvent(req, ctx)
        assert resp.success is False
        assert "timestamp must be positive" in resp.error
        ctx.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)

    @pytest.mark.asyncio
    async def test_unknown_event_type(self, service, ctx):
        req = _accounting_request(event_type="NOT_REAL_EVENT")
        resp = await service.SaveAccountingEvent(req, ctx)
        assert resp.success is False
        assert "unknown event_type" in resp.error
        ctx.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)

    @pytest.mark.asyncio
    async def test_missing_payload_json(self, service, ctx):
        req = _accounting_request(payload_json=b"")
        resp = await service.SaveAccountingEvent(req, ctx)
        assert resp.success is False
        assert "payload_json is required" in resp.error
        ctx.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)

    @pytest.mark.asyncio
    async def test_invalid_utf8_payload(self, service, ctx):
        req = _accounting_request(payload_json=b"\xff\xfe")
        resp = await service.SaveAccountingEvent(req, ctx)
        assert resp.success is False
        assert "valid UTF-8" in resp.error
        ctx.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)

    @pytest.mark.asyncio
    async def test_invalid_json_payload(self, service, ctx):
        """Non-JSON UTF-8 payload → INVALID_ARGUMENT with JSON error message."""
        req = _accounting_request(payload_json=b"not valid json {")
        resp = await service.SaveAccountingEvent(req, ctx)
        assert resp.success is False
        assert "valid JSON" in resp.error
        ctx.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)

    @pytest.mark.asyncio
    async def test_timestamp_out_of_range(self, service, ctx):
        """Timestamps too large for datetime.fromtimestamp() → INVALID_ARGUMENT."""
        service._state_manager = MagicMock()
        service._state_manager.warm_backend = AsyncMock()
        service._state_manager.warm_backend.save_accounting_event = AsyncMock(return_value=True)
        req = _accounting_request(timestamp=9_999_999_999_999)
        resp = await service.SaveAccountingEvent(req, ctx)
        assert resp.success is False
        assert "timestamp out of range" in resp.error
        ctx.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)

    @pytest.mark.asyncio
    async def test_malformed_payload_schema_returns_invalid_argument(self, service, ctx):
        """Payload that passes JSON but fails schema deserialization → INVALID_ARGUMENT."""
        service._state_manager = MagicMock()
        service._state_manager.warm_backend = AsyncMock()
        service._state_manager.warm_backend.save_accounting_event = AsyncMock(return_value=True)
        # Valid JSON but missing required fields — from_payload_json will raise KeyError/ValueError
        req = _accounting_request(payload_json=json.dumps({"event_type": "SUPPLY"}).encode())
        resp = await service.SaveAccountingEvent(req, ctx)
        assert resp.success is False
        # Should be INVALID_ARGUMENT (deserialization failure), not INTERNAL
        ctx.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)

    @pytest.mark.asyncio
    async def test_unsupported_warm_backend(self, service, ctx):
        """Backend without save_accounting_event → UNIMPLEMENTED."""
        service._state_manager = MagicMock()
        service._state_manager.warm_backend = MagicMock(spec=[])  # no save_accounting_event
        req = _accounting_request()
        resp = await service.SaveAccountingEvent(req, ctx)
        assert resp.success is False
        ctx.set_code.assert_called_with(grpc.StatusCode.UNIMPLEMENTED)

    @pytest.mark.asyncio
    async def test_successful_delegation(self, service, ctx):
        """Valid request with backend returning True → success=True."""
        warm = AsyncMock()
        warm.save_accounting_event = AsyncMock(return_value=True)
        service._state_manager = MagicMock()
        service._state_manager.warm_backend = warm
        req = _accounting_request()
        resp = await service.SaveAccountingEvent(req, ctx)
        assert resp.success is True
        ctx.set_code.assert_not_called()
        warm.save_accounting_event.assert_called_once()

    @pytest.mark.asyncio
    async def test_backend_returns_false(self, service, ctx):
        """Backend returning False → success=False but no gRPC error code."""
        warm = AsyncMock()
        warm.save_accounting_event = AsyncMock(return_value=False)
        service._state_manager = MagicMock()
        service._state_manager.warm_backend = warm
        req = _accounting_request()
        resp = await service.SaveAccountingEvent(req, ctx)
        assert resp.success is False
        ctx.set_code.assert_not_called()

    @pytest.mark.asyncio
    async def test_backend_exception_returns_internal(self, service, ctx):
        """Backend raising an exception → INTERNAL, success=False."""
        warm = AsyncMock()
        warm.save_accounting_event = AsyncMock(side_effect=RuntimeError("db exploded"))
        service._state_manager = MagicMock()
        service._state_manager.warm_backend = warm
        req = _accounting_request()
        resp = await service.SaveAccountingEvent(req, ctx)
        assert resp.success is False
        assert "internal server error" in resp.error
        ctx.set_code.assert_called_with(grpc.StatusCode.INTERNAL)

    @pytest.mark.asyncio
    async def test_pendle_event_type_dispatched(self, service, ctx):
        """PT_BUY routes to PendleAccountingEvent (not LendingAccountingEvent)."""
        from almanak.framework.accounting.models import PendleAccountingEvent

        captured = []

        async def _capture(ev):
            captured.append(ev)
            return True

        warm = AsyncMock()
        warm.save_accounting_event = _capture
        service._state_manager = MagicMock()
        service._state_manager.warm_backend = warm

        pendle_payload = json.dumps(
            {
                "event_type": "PT_BUY",
                "position_key": "pendle-pt",
                "market_id": "0xmarket",
                "pt_token": "PT-stETH",
                "maturity_timestamp": None,
                "pt_amount": "1.0",
                "sy_amount": "0.95",
                "pt_price": "0.95",
                "implied_apr_bps": 500,
                "days_to_maturity": 30,
                "realized_yield_usd": None,
                "basis_lot_id": None,
                "confidence": "HIGH",
                "unavailable_reason": "",
                "schema_version": 1,
            }
        ).encode("utf-8")

        req = _accounting_request(event_type="PT_BUY", payload_json=pendle_payload)
        resp = await service.SaveAccountingEvent(req, ctx)
        assert resp.success is True
        assert len(captured) == 1
        assert isinstance(captured[0], PendleAccountingEvent)


# ---------------------------------------------------------------------------
# SavePositionEvent — validation tests
# ---------------------------------------------------------------------------


class TestSavePositionEventValidation:
    @pytest.mark.asyncio
    async def test_missing_event_id(self, service, ctx):
        req = _position_request(id="")
        resp = await service.SavePositionEvent(req, ctx)
        assert resp.success is False
        assert "id is required" in resp.error
        ctx.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)

    @pytest.mark.asyncio
    async def test_invalid_uuid_event_id(self, service, ctx):
        req = _position_request(id="not-a-uuid")
        resp = await service.SavePositionEvent(req, ctx)
        assert resp.success is False
        assert "valid UUID" in resp.error
        ctx.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("ts", [0, -1])
    async def test_non_positive_timestamp(self, service, ctx, ts):
        req = _position_request(timestamp=ts)
        resp = await service.SavePositionEvent(req, ctx)
        assert resp.success is False
        assert "timestamp must be positive" in resp.error
        ctx.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)

    @pytest.mark.asyncio
    async def test_unknown_position_type(self, service, ctx):
        req = _position_request(position_type="STAKING")
        resp = await service.SavePositionEvent(req, ctx)
        assert resp.success is False
        assert "unknown position_type" in resp.error
        ctx.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)

    @pytest.mark.asyncio
    async def test_unknown_event_type(self, service, ctx):
        req = _position_request(event_type="LIQUIDATED")
        resp = await service.SavePositionEvent(req, ctx)
        assert resp.success is False
        assert "unknown event_type" in resp.error
        ctx.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)

    @pytest.mark.asyncio
    async def test_timestamp_out_of_range(self, service, ctx):
        """Timestamps too large for datetime.fromtimestamp() → INVALID_ARGUMENT."""
        service._state_manager = MagicMock()
        service._state_manager.warm_backend = AsyncMock()
        service._state_manager.warm_backend.save_position_event = AsyncMock(return_value=True)
        req = _position_request(timestamp=9_999_999_999_999)
        resp = await service.SavePositionEvent(req, ctx)
        assert resp.success is False
        assert "timestamp out of range" in resp.error
        ctx.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)

    @pytest.mark.asyncio
    async def test_unsupported_warm_backend(self, service, ctx):
        service._state_manager = MagicMock()
        service._state_manager.warm_backend = MagicMock(spec=[])  # no save_position_event
        req = _position_request()
        resp = await service.SavePositionEvent(req, ctx)
        assert resp.success is False
        ctx.set_code.assert_called_with(grpc.StatusCode.UNIMPLEMENTED)

    @pytest.mark.asyncio
    async def test_successful_delegation(self, service, ctx):
        warm = AsyncMock()
        warm.save_position_event = AsyncMock(return_value=True)
        service._state_manager = MagicMock()
        service._state_manager.warm_backend = warm
        req = _position_request()
        resp = await service.SavePositionEvent(req, ctx)
        assert resp.success is True
        ctx.set_code.assert_not_called()
        warm.save_position_event.assert_called_once()

    @pytest.mark.asyncio
    async def test_backend_returns_false(self, service, ctx):
        warm = AsyncMock()
        warm.save_position_event = AsyncMock(return_value=False)
        service._state_manager = MagicMock()
        service._state_manager.warm_backend = warm
        req = _position_request()
        resp = await service.SavePositionEvent(req, ctx)
        assert resp.success is False
        ctx.set_code.assert_not_called()

    @pytest.mark.asyncio
    async def test_backend_exception_returns_internal(self, service, ctx):
        warm = AsyncMock()
        warm.save_position_event = AsyncMock(side_effect=RuntimeError("db exploded"))
        service._state_manager = MagicMock()
        service._state_manager.warm_backend = warm
        req = _position_request()
        resp = await service.SavePositionEvent(req, ctx)
        assert resp.success is False
        assert "internal server error" in resp.error
        ctx.set_code.assert_called_with(grpc.StatusCode.INTERNAL)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("event_type", ["OPEN", "CLOSE", "COLLECT_FEES", "SNAPSHOT"])
    async def test_all_valid_event_types_accepted(self, service, ctx, event_type):
        """All PositionEventType values are accepted without INVALID_ARGUMENT."""
        warm = AsyncMock()
        warm.save_position_event = AsyncMock(return_value=True)
        service._state_manager = MagicMock()
        service._state_manager.warm_backend = warm
        req = _position_request(event_type=event_type)
        resp = await service.SavePositionEvent(req, ctx)
        assert resp.success is True

    @pytest.mark.asyncio
    @pytest.mark.parametrize("position_type", ["LP", "PERP"])
    async def test_all_valid_position_types_accepted(self, service, ctx, position_type):
        """Both PositionType values are accepted."""
        warm = AsyncMock()
        warm.save_position_event = AsyncMock(return_value=True)
        service._state_manager = MagicMock()
        service._state_manager.warm_backend = warm
        req = _position_request(position_type=position_type)
        resp = await service.SavePositionEvent(req, ctx)
        assert resp.success is True
