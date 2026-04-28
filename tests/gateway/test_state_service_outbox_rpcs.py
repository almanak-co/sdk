"""Unit tests for the 6 new StateService accounting outbox RPCs (VIB-3652).

Covers:
  - SaveOutboxEntry: validation, SQLite happy path, error handling
  - GetOutboxEntry: found and not-found
  - GetOutboxPending: empty list and populated
  - UpdateOutboxEntry: success, failure
  - HasAccountingEventsForLedger: true and false
  - GetLedgerEntry: found (with timestamp conversion) and not-found

All tests run against the SQLite path (_snapshot_pool = None).
PG path requires an external database and is exercised by integration tests.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import grpc
import pytest

from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.state_service import StateServiceServicer

_LEDGER_ID = "ledger-abc-123"
_DEPLOYMENT_ID = "deploy-xyz"
_STRATEGY_ID = "my_strategy:abc123"


@pytest.fixture
def settings():
    return GatewaySettings()


@pytest.fixture
def mock_context():
    ctx = MagicMock(spec=grpc.aio.ServicerContext)
    ctx.set_code = MagicMock()
    ctx.set_details = MagicMock()
    return ctx


@pytest.fixture
def state_service(settings):
    svc = StateServiceServicer(settings)
    svc._snapshot_pool = None  # force SQLite path
    svc._initialized = True
    return svc


def _make_warm(svc, **overrides):
    """Attach a mock warm_backend to the service."""
    warm = AsyncMock()
    warm.save_outbox_entry = AsyncMock(return_value=None)
    warm.get_outbox_by_ledger_id = AsyncMock(return_value=None)
    warm.get_outbox_pending = AsyncMock(return_value=[])
    warm.update_outbox_entry = AsyncMock(return_value=None)
    warm.has_accounting_events_for_ledger = AsyncMock(return_value=False)
    warm.get_ledger_entry_by_id = AsyncMock(return_value=None)
    for k, v in overrides.items():
        setattr(warm, k, v)

    sm = AsyncMock()
    sm.warm_backend = warm
    svc._state_manager = sm
    return warm


# =============================================================================
# SaveOutboxEntry
# =============================================================================


class TestSaveOutboxEntry:
    @pytest.mark.asyncio
    async def test_rejects_missing_ledger_entry_id(self, state_service, mock_context):
        req = gateway_pb2.SaveOutboxEntryRequest(
            ledger_entry_id="",
            deployment_id=_DEPLOYMENT_ID,
            strategy_id=_STRATEGY_ID,
        )
        resp = await state_service.SaveOutboxEntry(req, mock_context)
        assert not resp.success
        assert "ledger_entry_id" in resp.error

    @pytest.mark.asyncio
    async def test_rejects_missing_deployment_id(self, state_service, mock_context):
        req = gateway_pb2.SaveOutboxEntryRequest(
            ledger_entry_id=_LEDGER_ID,
            deployment_id="",
            strategy_id=_STRATEGY_ID,
        )
        resp = await state_service.SaveOutboxEntry(req, mock_context)
        assert not resp.success
        assert "deployment_id" in resp.error

    @pytest.mark.asyncio
    async def test_rejects_invalid_strategy_id(self, state_service, mock_context):
        req = gateway_pb2.SaveOutboxEntryRequest(
            ledger_entry_id=_LEDGER_ID,
            deployment_id=_DEPLOYMENT_ID,
            strategy_id="has spaces!",
        )
        resp = await state_service.SaveOutboxEntry(req, mock_context)
        assert not resp.success

    @pytest.mark.asyncio
    async def test_sqlite_happy_path(self, state_service, mock_context):
        warm = _make_warm(state_service)
        req = gateway_pb2.SaveOutboxEntryRequest(
            outbox_id="outbox-uuid",
            ledger_entry_id=_LEDGER_ID,
            deployment_id=_DEPLOYMENT_ID,
            strategy_id=_STRATEGY_ID,
            intent_type="SWAP",
            wallet_address="0xdeadbeef",
            position_key="swap:arbitrum:0xdeadbeef",
            market_id="eth-usdc",
        )
        resp = await state_service.SaveOutboxEntry(req, mock_context)
        assert resp.success
        # warm backend received all attribution fields
        call_kwargs = warm.save_outbox_entry.call_args.kwargs
        assert call_kwargs["ledger_entry_id"] == _LEDGER_ID
        assert call_kwargs["wallet_address"] == "0xdeadbeef"
        assert call_kwargs["position_key"] == "swap:arbitrum:0xdeadbeef"
        assert call_kwargs["market_id"] == "eth-usdc"

    @pytest.mark.asyncio
    async def test_sqlite_error_returns_failure(self, state_service, mock_context):
        warm = _make_warm(state_service)
        warm.save_outbox_entry.side_effect = RuntimeError("disk full")
        req = gateway_pb2.SaveOutboxEntryRequest(
            ledger_entry_id=_LEDGER_ID,
            deployment_id=_DEPLOYMENT_ID,
            strategy_id=_STRATEGY_ID,
        )
        resp = await state_service.SaveOutboxEntry(req, mock_context)
        assert not resp.success

    @pytest.mark.asyncio
    async def test_missing_warm_backend_returns_failure(self, state_service, mock_context):
        sm = AsyncMock()
        sm.warm_backend = None
        state_service._state_manager = sm
        req = gateway_pb2.SaveOutboxEntryRequest(
            ledger_entry_id=_LEDGER_ID,
            deployment_id=_DEPLOYMENT_ID,
            strategy_id=_STRATEGY_ID,
        )
        resp = await state_service.SaveOutboxEntry(req, mock_context)
        assert not resp.success


# =============================================================================
# GetOutboxEntry
# =============================================================================


class TestGetOutboxEntry:
    @pytest.mark.asyncio
    async def test_not_found_returns_found_false(self, state_service, mock_context):
        _make_warm(state_service)  # warm.get_outbox_by_ledger_id returns None by default
        req = gateway_pb2.GetOutboxEntryRequest(ledger_entry_id=_LEDGER_ID)
        resp = await state_service.GetOutboxEntry(req, mock_context)
        assert not resp.found

    @pytest.mark.asyncio
    async def test_found_returns_entry(self, state_service, mock_context):
        warm = _make_warm(state_service)
        warm.get_outbox_by_ledger_id = AsyncMock(
            return_value={
                "id": _LEDGER_ID,
                "deployment_id": _DEPLOYMENT_ID,
                "strategy_id": _STRATEGY_ID,
                "cycle_id": "cycle-1",
                "ledger_entry_id": _LEDGER_ID,
                "intent_type": "SWAP",
                "wallet_address": "0xdeadbeef",
                "position_key": "swap:arb:0xdeadbeef",
                "market_id": "eth-usdc",
                "status": "pending",
                "attempts": 0,
                "error": "",
                "created_at": "2026-01-01T00:00:00+00:00",
                "updated_at": "2026-01-01T00:00:00+00:00",
            }
        )
        req = gateway_pb2.GetOutboxEntryRequest(ledger_entry_id=_LEDGER_ID)
        resp = await state_service.GetOutboxEntry(req, mock_context)
        assert resp.found
        assert resp.entry.ledger_entry_id == _LEDGER_ID
        assert resp.entry.wallet_address == "0xdeadbeef"
        assert resp.entry.intent_type == "SWAP"


# =============================================================================
# GetOutboxPending
# =============================================================================


class TestGetOutboxPending:
    @pytest.mark.asyncio
    async def test_missing_deployment_id_returns_empty(self, state_service, mock_context):
        _make_warm(state_service)
        req = gateway_pb2.GetOutboxPendingRequest(deployment_id="", max_retries=3)
        resp = await state_service.GetOutboxPending(req, mock_context)
        assert len(resp.entries) == 0

    @pytest.mark.asyncio
    async def test_empty_returns_no_entries(self, state_service, mock_context):
        _make_warm(state_service)
        req = gateway_pb2.GetOutboxPendingRequest(deployment_id=_DEPLOYMENT_ID, max_retries=3)
        resp = await state_service.GetOutboxPending(req, mock_context)
        assert len(resp.entries) == 0

    @pytest.mark.asyncio
    async def test_returns_pending_entries(self, state_service, mock_context):
        warm = _make_warm(state_service)
        warm.get_outbox_pending = AsyncMock(
            return_value=[
                {
                    "id": _LEDGER_ID,
                    "deployment_id": _DEPLOYMENT_ID,
                    "strategy_id": _STRATEGY_ID,
                    "cycle_id": "cycle-1",
                    "ledger_entry_id": _LEDGER_ID,
                    "intent_type": "LP_OPEN",
                    "wallet_address": "0xfoo",
                    "position_key": "lp:arb:0xpool",
                    "market_id": "weth-usdc",
                    "status": "pending",
                    "attempts": 0,
                    "error": "",
                    "created_at": "2026-01-01T00:00:00+00:00",
                    "updated_at": "2026-01-01T00:00:00+00:00",
                }
            ]
        )
        req = gateway_pb2.GetOutboxPendingRequest(deployment_id=_DEPLOYMENT_ID, max_retries=3)
        resp = await state_service.GetOutboxPending(req, mock_context)
        assert len(resp.entries) == 1
        assert resp.entries[0].position_key == "lp:arb:0xpool"


# =============================================================================
# UpdateOutboxEntry
# =============================================================================


class TestUpdateOutboxEntry:
    @pytest.mark.asyncio
    async def test_rejects_missing_outbox_id(self, state_service, mock_context):
        _make_warm(state_service)
        req = gateway_pb2.UpdateOutboxEntryRequest(outbox_id="", status="processed")
        resp = await state_service.UpdateOutboxEntry(req, mock_context)
        assert not resp.success
        assert "outbox_id" in resp.error

    @pytest.mark.asyncio
    async def test_rejects_invalid_status(self, state_service, mock_context):
        _make_warm(state_service)
        req = gateway_pb2.UpdateOutboxEntryRequest(outbox_id="outbox-uuid", status="unknown")
        resp = await state_service.UpdateOutboxEntry(req, mock_context)
        assert not resp.success

    @pytest.mark.asyncio
    async def test_success(self, state_service, mock_context):
        _make_warm(state_service)
        req = gateway_pb2.UpdateOutboxEntryRequest(
            outbox_id="outbox-uuid",
            status="processed",
            error="",
        )
        resp = await state_service.UpdateOutboxEntry(req, mock_context)
        assert resp.success

    @pytest.mark.asyncio
    async def test_warm_error_returns_failure(self, state_service, mock_context):
        warm = _make_warm(state_service)
        warm.update_outbox_entry = AsyncMock(side_effect=RuntimeError("db error"))
        req = gateway_pb2.UpdateOutboxEntryRequest(
            outbox_id="outbox-uuid",
            status="failed",
            error="some error",
        )
        resp = await state_service.UpdateOutboxEntry(req, mock_context)
        assert not resp.success


# =============================================================================
# HasAccountingEventsForLedger
# =============================================================================


class TestHasAccountingEventsForLedger:
    @pytest.mark.asyncio
    async def test_returns_false_when_no_events(self, state_service, mock_context):
        _make_warm(state_service)
        req = gateway_pb2.HasAccountingEventsForLedgerRequest(ledger_entry_id=_LEDGER_ID)
        resp = await state_service.HasAccountingEventsForLedger(req, mock_context)
        assert not resp.has_events

    @pytest.mark.asyncio
    async def test_returns_true_when_events_exist(self, state_service, mock_context):
        warm = _make_warm(state_service)
        warm.has_accounting_events_for_ledger = AsyncMock(return_value=True)
        req = gateway_pb2.HasAccountingEventsForLedgerRequest(ledger_entry_id=_LEDGER_ID)
        resp = await state_service.HasAccountingEventsForLedger(req, mock_context)
        assert resp.has_events


# =============================================================================
# GetLedgerEntry
# =============================================================================


class TestGetLedgerEntry:
    @pytest.mark.asyncio
    async def test_not_found(self, state_service, mock_context):
        _make_warm(state_service)
        req = gateway_pb2.GetLedgerEntryRequest(ledger_entry_id=_LEDGER_ID)
        resp = await state_service.GetLedgerEntry(req, mock_context)
        assert not resp.found

    @pytest.mark.asyncio
    async def test_found_with_full_fields(self, state_service, mock_context):
        warm = _make_warm(state_service)
        ts_dt = datetime(2026, 1, 1, tzinfo=UTC)
        ts_iso = ts_dt.isoformat()
        ts_epoch = int(ts_dt.timestamp())
        warm.get_ledger_entry_by_id = AsyncMock(
            return_value={
                "id": _LEDGER_ID,
                "cycle_id": "cycle-1",
                "strategy_id": _STRATEGY_ID,
                "deployment_id": _DEPLOYMENT_ID,
                "execution_mode": "live",
                "timestamp": ts_iso,  # SQLite warm backend returns ISO string
                "intent_type": "SWAP",
                "token_in": "WETH",
                "amount_in": "0.1",
                "token_out": "USDC",
                "amount_out": "300.0",
                "effective_price": "3000.0",
                "gas_used": "200000",
                "gas_usd": "0.5",
                "tx_hash": "0xabc",
                "chain": "arbitrum",
                "protocol": "uniswap_v3",
                "success": True,
                "error": "",
                "extracted_data_json": "{}",
                "price_inputs_json": "{}",
                "pre_state_json": "{}",
                "post_state_json": "{}",
            }
        )
        req = gateway_pb2.GetLedgerEntryRequest(ledger_entry_id=_LEDGER_ID)
        resp = await state_service.GetLedgerEntry(req, mock_context)
        assert resp.found
        assert resp.entry.id == _LEDGER_ID
        assert resp.entry.intent_type == "SWAP"
        assert resp.entry.chain == "arbitrum"
        # Service converts ISO string → Unix epoch int for the proto field
        assert resp.entry.timestamp == ts_epoch
