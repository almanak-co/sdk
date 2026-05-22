"""Tests for the 6 accounting outbox + GetLedgerEntry gRPC endpoints.

Covers input validation, SQLite-delegate happy paths, and error paths for:
  SaveOutboxEntry, GetOutboxEntry, GetOutboxPending, UpdateOutboxEntry,
  HasAccountingEventsForLedger, GetLedgerEntry.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import grpc
import pytest

from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.state_service import StateServiceServicer

_LEDGER_UUID = "deadbeef-dead-beef-dead-beefdeadbeef"
_OUTBOX_UUID = "cafebabe-cafe-babe-cafe-babecafebabe"
_DEPLOY_ID = "deploy-test"
_DEPLOYMENT_ID = "test-strategy"


@pytest.fixture
def settings() -> GatewaySettings:
    return GatewaySettings()


@pytest.fixture
def mock_context() -> MagicMock:
    ctx = MagicMock(spec=grpc.aio.ServicerContext)
    ctx.set_code = MagicMock()
    ctx.set_details = MagicMock()
    return ctx


@pytest.fixture
def state_service(settings: GatewaySettings) -> StateServiceServicer:
    svc = StateServiceServicer(settings)
    svc._initialized = True
    svc._snapshot_pool_initialized = True
    svc._snapshot_pool = None  # force SQLite delegate path
    return svc


# ---------------------------------------------------------------------------
# SaveOutboxEntry
# ---------------------------------------------------------------------------


class TestSaveOutboxEntryValidation:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("ledger_entry_id", ["", "   "])
    async def test_missing_ledger_entry_id(self, state_service, mock_context, ledger_entry_id):
        req = gateway_pb2.SaveOutboxEntryRequest(
            ledger_entry_id=ledger_entry_id,
            deployment_id=_DEPLOYMENT_ID,
        )
        resp = await state_service.SaveOutboxEntry(req, mock_context)
        assert resp.success is False
        assert "ledger_entry_id is required" in resp.error
        mock_context.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("deployment_id", ["", "  ", "   "])
    async def test_missing_deployment_id(self, state_service, mock_context, deployment_id):
        req = gateway_pb2.SaveOutboxEntryRequest(
            ledger_entry_id=_LEDGER_UUID,
            deployment_id=deployment_id,
        )
        resp = await state_service.SaveOutboxEntry(req, mock_context)
        assert resp.success is False
        assert "deployment_id" in resp.error
        assert "required" in resp.error
        mock_context.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)


class TestSaveOutboxEntrySqlite:
    @pytest.mark.asyncio
    async def test_delegates_to_warm_backend(self, state_service, mock_context):
        warm = MagicMock()
        warm.save_outbox_entry = AsyncMock()
        state_service._state_manager = MagicMock()
        state_service._state_manager.warm_backend = warm

        req = gateway_pb2.SaveOutboxEntryRequest(
            outbox_id=_OUTBOX_UUID,
            ledger_entry_id=_LEDGER_UUID,
            deployment_id=_DEPLOYMENT_ID,
            intent_type="SWAP",
        )
        resp = await state_service.SaveOutboxEntry(req, mock_context)

        assert resp.success is True
        warm.save_outbox_entry.assert_awaited_once()
        call_kwargs = warm.save_outbox_entry.await_args.kwargs
        assert call_kwargs["ledger_entry_id"] == _LEDGER_UUID
        assert call_kwargs["deployment_id"] == _DEPLOYMENT_ID
        assert call_kwargs["deployment_id"] != ""

    @pytest.mark.asyncio
    async def test_backend_error_returns_failure(self, state_service, mock_context):
        warm = MagicMock()
        warm.save_outbox_entry = AsyncMock(side_effect=RuntimeError("db locked"))
        state_service._state_manager = MagicMock()
        state_service._state_manager.warm_backend = warm

        req = gateway_pb2.SaveOutboxEntryRequest(
            ledger_entry_id=_LEDGER_UUID,
            deployment_id=_DEPLOYMENT_ID,
        )
        resp = await state_service.SaveOutboxEntry(req, mock_context)
        assert resp.success is False
        assert "internal server error" in resp.error

    @pytest.mark.asyncio
    async def test_no_warm_backend_returns_failure(self, state_service, mock_context):
        state_service._state_manager = MagicMock()
        state_service._state_manager.warm_backend = None

        req = gateway_pb2.SaveOutboxEntryRequest(
            ledger_entry_id=_LEDGER_UUID,
            deployment_id=_DEPLOYMENT_ID,
        )
        resp = await state_service.SaveOutboxEntry(req, mock_context)
        assert resp.success is False


# ---------------------------------------------------------------------------
# GetOutboxEntry
# ---------------------------------------------------------------------------


class TestGetOutboxEntryValidation:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("ledger_entry_id", ["", "  "])
    async def test_missing_ledger_entry_id(self, state_service, mock_context, ledger_entry_id):
        req = gateway_pb2.GetOutboxEntryRequest(ledger_entry_id=ledger_entry_id)
        resp = await state_service.GetOutboxEntry(req, mock_context)
        assert resp.found is False
        mock_context.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)


class TestGetOutboxEntrySqlite:
    @pytest.mark.asyncio
    async def test_found(self, state_service, mock_context):
        warm = MagicMock()
        warm.get_outbox_by_ledger_id = AsyncMock(
            return_value={
                "id": _OUTBOX_UUID,
                "ledger_entry_id": _LEDGER_UUID,
                "deployment_id": _DEPLOYMENT_ID,
                "cycle_id": "cycle-1",
                "intent_type": "SWAP",
                "wallet_address": "0xabc",
                "position_key": "pos-1",
                "market_id": "mkt-1",
                "status": "pending",
                "attempts": 0,
                "error": "",
                "created_at": "2026-01-01T00:00:00+00:00",
                "updated_at": "2026-01-01T00:00:00+00:00",
            }
        )
        state_service._state_manager = MagicMock()
        state_service._state_manager.warm_backend = warm

        req = gateway_pb2.GetOutboxEntryRequest(ledger_entry_id=_LEDGER_UUID)
        resp = await state_service.GetOutboxEntry(req, mock_context)

        assert resp.found is True
        assert resp.entry.ledger_entry_id == _LEDGER_UUID
        assert resp.entry.status == "pending"
        assert resp.entry.attempts == 0

    @pytest.mark.asyncio
    async def test_not_found(self, state_service, mock_context):
        warm = MagicMock()
        warm.get_outbox_by_ledger_id = AsyncMock(return_value=None)
        state_service._state_manager = MagicMock()
        state_service._state_manager.warm_backend = warm

        req = gateway_pb2.GetOutboxEntryRequest(ledger_entry_id=_LEDGER_UUID)
        resp = await state_service.GetOutboxEntry(req, mock_context)
        assert resp.found is False


# ---------------------------------------------------------------------------
# GetOutboxPending
# ---------------------------------------------------------------------------


class TestGetOutboxPendingValidation:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("deployment_id", ["", "  "])
    async def test_missing_deployment_id(self, state_service, mock_context, deployment_id):
        req = gateway_pb2.GetOutboxPendingRequest(deployment_id=deployment_id)
        resp = await state_service.GetOutboxPending(req, mock_context)
        assert len(resp.entries) == 0
        mock_context.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)


class TestGetOutboxPendingSqlite:
    @pytest.mark.asyncio
    async def test_returns_pending_rows(self, state_service, mock_context):
        row = {
            "id": _OUTBOX_UUID,
            "ledger_entry_id": _LEDGER_UUID,
            "deployment_id": _DEPLOYMENT_ID,
            "cycle_id": "c1",
            "intent_type": "LP_OPEN",
            "wallet_address": "0xabc",
            "position_key": "pos-1",
            "market_id": "mkt",
            "status": "pending",
            "attempts": 0,
            "error": "",
            "created_at": "2026-01-01T00:00:00+00:00",
            "updated_at": "2026-01-01T00:00:00+00:00",
        }
        warm = MagicMock()
        warm.get_outbox_pending = AsyncMock(return_value=[row])
        state_service._state_manager = MagicMock()
        state_service._state_manager.warm_backend = warm

        req = gateway_pb2.GetOutboxPendingRequest(deployment_id=_DEPLOY_ID, max_retries=3)
        resp = await state_service.GetOutboxPending(req, mock_context)

        assert len(resp.entries) == 1
        assert resp.entries[0].intent_type == "LP_OPEN"
        warm.get_outbox_pending.assert_awaited_once_with(_DEPLOY_ID, max_retries=3)

    @pytest.mark.asyncio
    async def test_max_retries_zero_uses_server_default(self, state_service, mock_context):
        warm = MagicMock()
        warm.get_outbox_pending = AsyncMock(return_value=[])
        state_service._state_manager = MagicMock()
        state_service._state_manager.warm_backend = warm

        req = gateway_pb2.GetOutboxPendingRequest(deployment_id=_DEPLOY_ID, max_retries=0)
        await state_service.GetOutboxPending(req, mock_context)

        _, kwargs = warm.get_outbox_pending.await_args
        assert kwargs["max_retries"] == 3  # server default

    @pytest.mark.asyncio
    async def test_empty_on_backend_error(self, state_service, mock_context):
        warm = MagicMock()
        warm.get_outbox_pending = AsyncMock(side_effect=RuntimeError("db error"))
        state_service._state_manager = MagicMock()
        state_service._state_manager.warm_backend = warm

        req = gateway_pb2.GetOutboxPendingRequest(deployment_id=_DEPLOY_ID)
        resp = await state_service.GetOutboxPending(req, mock_context)
        assert len(resp.entries) == 0


# ---------------------------------------------------------------------------
# UpdateOutboxEntry
# ---------------------------------------------------------------------------


class TestUpdateOutboxEntryValidation:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("outbox_id", ["", "  "])
    async def test_missing_outbox_id(self, state_service, mock_context, outbox_id):
        req = gateway_pb2.UpdateOutboxEntryRequest(outbox_id=outbox_id, status="processed")
        resp = await state_service.UpdateOutboxEntry(req, mock_context)
        assert resp.success is False
        assert "outbox_id is required" in resp.error
        mock_context.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("status", ["", "done", "PROCESSED", "unknown"])
    async def test_invalid_status(self, state_service, mock_context, status):
        req = gateway_pb2.UpdateOutboxEntryRequest(outbox_id=_OUTBOX_UUID, status=status)
        resp = await state_service.UpdateOutboxEntry(req, mock_context)
        assert resp.success is False
        assert "invalid status" in resp.error
        mock_context.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)


class TestUpdateOutboxEntrySqlite:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("status", ["pending", "processing", "processed", "failed"])
    async def test_valid_status_transitions(self, state_service, mock_context, status):
        warm = MagicMock()
        warm.update_outbox_entry = AsyncMock()
        state_service._state_manager = MagicMock()
        state_service._state_manager.warm_backend = warm

        req = gateway_pb2.UpdateOutboxEntryRequest(outbox_id=_OUTBOX_UUID, status=status)
        resp = await state_service.UpdateOutboxEntry(req, mock_context)
        assert resp.success is True
        warm.update_outbox_entry.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_attempts_optional_field_passed_through(self, state_service, mock_context):
        warm = MagicMock()
        warm.update_outbox_entry = AsyncMock()
        state_service._state_manager = MagicMock()
        state_service._state_manager.warm_backend = warm

        req = gateway_pb2.UpdateOutboxEntryRequest(outbox_id=_OUTBOX_UUID, status="processing")
        req.attempts = 2
        resp = await state_service.UpdateOutboxEntry(req, mock_context)

        assert resp.success is True
        _, kwargs = warm.update_outbox_entry.await_args
        assert kwargs.get("attempts") == 2

    @pytest.mark.asyncio
    async def test_backend_error_returns_failure(self, state_service, mock_context):
        warm = MagicMock()
        warm.update_outbox_entry = AsyncMock(side_effect=RuntimeError("write failed"))
        state_service._state_manager = MagicMock()
        state_service._state_manager.warm_backend = warm

        req = gateway_pb2.UpdateOutboxEntryRequest(outbox_id=_OUTBOX_UUID, status="processed")
        resp = await state_service.UpdateOutboxEntry(req, mock_context)
        assert resp.success is False
        assert "internal server error" in resp.error


# ---------------------------------------------------------------------------
# HasAccountingEventsForLedger
# ---------------------------------------------------------------------------


class TestHasAccountingEventsForLedgerSqlite:
    @pytest.mark.asyncio
    async def test_returns_true_when_event_exists(self, state_service, mock_context):
        warm = MagicMock()
        warm.has_accounting_events_for_ledger = AsyncMock(return_value=True)
        state_service._state_manager = MagicMock()
        state_service._state_manager.warm_backend = warm

        req = gateway_pb2.HasAccountingEventsForLedgerRequest(ledger_entry_id=_LEDGER_UUID)
        resp = await state_service.HasAccountingEventsForLedger(req, mock_context)
        assert resp.has_events is True

    @pytest.mark.asyncio
    async def test_returns_false_when_no_event(self, state_service, mock_context):
        warm = MagicMock()
        warm.has_accounting_events_for_ledger = AsyncMock(return_value=False)
        state_service._state_manager = MagicMock()
        state_service._state_manager.warm_backend = warm

        req = gateway_pb2.HasAccountingEventsForLedgerRequest(ledger_entry_id=_LEDGER_UUID)
        resp = await state_service.HasAccountingEventsForLedger(req, mock_context)
        assert resp.has_events is False

    @pytest.mark.asyncio
    async def test_empty_ledger_entry_id_returns_invalid_argument(self, state_service, mock_context):
        req = gateway_pb2.HasAccountingEventsForLedgerRequest(ledger_entry_id="")
        resp = await state_service.HasAccountingEventsForLedger(req, mock_context)
        assert resp.has_events is False
        mock_context.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)

    @pytest.mark.asyncio
    async def test_backend_error_sets_grpc_internal_status(self, state_service, mock_context):
        """DB failures must surface as gRPC INTERNAL, not as has_events=False.

        The client (GatewayStateManager.has_accounting_events_for_ledger) raises on
        any gRPC exception rather than returning False, to avoid conflating "no row"
        with "lookup failed" and risk re-processing already-written ledger entries.
        """
        warm = MagicMock()
        warm.has_accounting_events_for_ledger = AsyncMock(side_effect=RuntimeError("db error"))
        state_service._state_manager = MagicMock()
        state_service._state_manager.warm_backend = warm

        req = gateway_pb2.HasAccountingEventsForLedgerRequest(ledger_entry_id=_LEDGER_UUID)
        await state_service.HasAccountingEventsForLedger(req, mock_context)

        # The response content is irrelevant; what matters is that INTERNAL is set
        # so grpc propagates it as an exception to the client.
        mock_context.set_code.assert_called_with(grpc.StatusCode.INTERNAL)


# ---------------------------------------------------------------------------
# GetLedgerEntry
# ---------------------------------------------------------------------------


class TestGetLedgerEntryValidation:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("ledger_entry_id", ["", "  "])
    async def test_missing_ledger_entry_id(self, state_service, mock_context, ledger_entry_id):
        req = gateway_pb2.GetLedgerEntryRequest(ledger_entry_id=ledger_entry_id)
        resp = await state_service.GetLedgerEntry(req, mock_context)
        assert resp.found is False
        mock_context.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)


class TestGetLedgerEntrySqlite:
    @pytest.mark.asyncio
    async def test_found(self, state_service, mock_context):
        row = {
            "id": _LEDGER_UUID,
            "cycle_id": "cycle-1",
            "deployment_id": _DEPLOY_ID,
            "execution_mode": "live",
            "timestamp": "2026-01-01T12:00:00+00:00",
            "intent_type": "SWAP",
            "token_in": "USDC",
            "amount_in": "100",
            "token_out": "ETH",
            "amount_out": "0.05",
            "effective_price": "2000",
            "gas_used": 21000,
            "gas_usd": "1.50",
            "tx_hash": "0xabc123",
            "chain": "arbitrum",
            "protocol": "uniswap_v3",
            "success": True,
            "error": "",
            "extracted_data_json": "",
            "price_inputs_json": "",
            "pre_state_json": "",
            "post_state_json": "",
        }
        warm = MagicMock()
        warm.get_ledger_entry_by_id = AsyncMock(return_value=row)
        state_service._state_manager = MagicMock()
        state_service._state_manager.warm_backend = warm

        req = gateway_pb2.GetLedgerEntryRequest(ledger_entry_id=_LEDGER_UUID)
        resp = await state_service.GetLedgerEntry(req, mock_context)

        assert resp.found is True
        assert resp.entry.id == _LEDGER_UUID
        assert resp.entry.chain == "arbitrum"
        assert resp.entry.intent_type == "SWAP"
        assert resp.entry.success is True
        # Regression guard: handler must convert the ISO string from the row to a
        # positive epoch int (proto field is int64).  The GatewayStateManager then
        # converts it back to ISO so category handlers see a parseable string.
        # A zero here would indicate the ISO→epoch conversion silently fell back.
        assert resp.entry.timestamp > 0

    @pytest.mark.asyncio
    async def test_not_found(self, state_service, mock_context):
        warm = MagicMock()
        warm.get_ledger_entry_by_id = AsyncMock(return_value=None)
        state_service._state_manager = MagicMock()
        state_service._state_manager.warm_backend = warm

        req = gateway_pb2.GetLedgerEntryRequest(ledger_entry_id=_LEDGER_UUID)
        resp = await state_service.GetLedgerEntry(req, mock_context)
        assert resp.found is False

    @pytest.mark.asyncio
    async def test_backend_error_sets_grpc_internal_status(self, state_service, mock_context):
        """DB failures must surface as gRPC INTERNAL, not as found=False.

        Returning found=False on a DB error conflates "missing ledger row" with
        "lookup failed" and can send drain_one down the wrong branch during
        transient outages.
        """
        warm = MagicMock()
        warm.get_ledger_entry_by_id = AsyncMock(side_effect=RuntimeError("db down"))
        state_service._state_manager = MagicMock()
        state_service._state_manager.warm_backend = warm

        req = gateway_pb2.GetLedgerEntryRequest(ledger_entry_id=_LEDGER_UUID)
        resp = await state_service.GetLedgerEntry(req, mock_context)
        assert resp.found is False
        mock_context.set_code.assert_called_with(grpc.StatusCode.INTERNAL)
