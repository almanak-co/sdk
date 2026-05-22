"""Unit tests for the 6 new StateService accounting outbox RPCs (VIB-3652).

Covers:
  - SaveOutboxEntry: validation, SQLite happy path, error handling
  - GetOutboxEntry: found and not-found
  - GetOutboxPending: empty list and populated
  - UpdateOutboxEntry: success, failure
  - HasAccountingEventsForLedger: true and false
  - GetLedgerEntry: found (with timestamp conversion) and not-found

The SQLite path (``_snapshot_pool = None``) is the default. The PG path
fixture (``state_service_pg``) is used by ``TestPostgresOutboxRoundTrip``
to pin the SaveOutboxEntry / GetOutboxEntry / GetOutboxPending PG SQL
shape (column names + per-position attribution fields, VIB-3658).
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import grpc
import pytest

from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.state_service import StateServiceServicer

_LEDGER_ID = "ledger-abc-123"
_DEPLOYMENT_ID = "deploy-xyz"
_DEPLOYMENT_ID = "my_strategy:abc123"


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
        )
        resp = await state_service.SaveOutboxEntry(req, mock_context)
        assert not resp.success
        assert "ledger_entry_id" in resp.error

    @pytest.mark.asyncio
    async def test_rejects_missing_deployment_id(self, state_service, mock_context):
        req = gateway_pb2.SaveOutboxEntryRequest(
            ledger_entry_id=_LEDGER_ID,
            deployment_id="",
        )
        resp = await state_service.SaveOutboxEntry(req, mock_context)
        assert not resp.success
        assert "deployment_id" in resp.error

    @pytest.mark.asyncio
    async def test_rejects_invalid_deployment_id(self, state_service, mock_context):
        req = gateway_pb2.SaveOutboxEntryRequest(
            ledger_entry_id=_LEDGER_ID,
            deployment_id="has spaces!",
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


# =============================================================================
# PostgreSQL path — accounting_outbox SQL contract (VIB-3658)
# =============================================================================
#
# Pins the column contract between PG, SQLite, and the wire:
#   - VIB-4721/4722: ``accounting_outbox`` has a single identity column,
#     ``deployment_id`` (the legacy ``deployment_id`` column was DROPPED). The PG
#     SQL writes/reads/filters ``deployment_id`` directly with no identity
#     translation (blueprint 29 §4-5).
#   - VIB-3658 adds cycle_id / wallet_address / position_key / market_id to
#     the PG schema; SaveOutboxEntry must persist them and GetOutboxEntry /
#     GetOutboxPending must read them back into the proto.


@pytest.fixture
def state_service_pg(settings):
    """StateService configured for the PG path with mocked snapshot pool.

    ``_snapshot_pool`` is a truthy MagicMock so the RPC takes the PG branch;
    ``_ensure_snapshot_pool`` is a no-op so the test owns the pool state;
    ``_snapshot_execute / _snapshot_fetchrow / _snapshot_fetch`` are
    AsyncMocks the test reads call args from / sets return values on.
    """
    svc = StateServiceServicer(settings)
    svc._snapshot_pool_initialized = True
    svc._snapshot_pool = MagicMock()
    svc._ensure_snapshot_pool = AsyncMock()
    svc._snapshot_execute = AsyncMock(return_value="INSERT 0 1")
    svc._snapshot_fetchrow = AsyncMock(return_value=None)
    svc._snapshot_fetch = AsyncMock(return_value=[])
    return svc


def _outbox_pg_row(
    *,
    deployment_id: str = _DEPLOYMENT_ID,
    cycle_id: str = "cycle-1",
    wallet_address: str = "0xdeadbeef",
    position_key: str = "uniswap_v3:arbitrum:0xdeadbeef:eth-usdc",
    market_id: str = "eth-usdc",
):
    """A dict shaped like an asyncpg.Record for the post-VIB-4721 schema."""
    return {
        "ledger_entry_id": _LEDGER_ID,
        "deployment_id": deployment_id,
        "intent_type": "SWAP",
        "cycle_id": cycle_id,
        "wallet_address": wallet_address,
        "position_key": position_key,
        "market_id": market_id,
        "status": "pending",
        "retry_count": 0,
        "last_error": None,
        "created_at": datetime(2026, 4, 29, 12, 0, 0, tzinfo=UTC),
        "processed_at": None,
    }


class TestPostgresOutboxRoundTrip:
    @pytest.mark.asyncio
    async def test_save_outbox_pg_insert_uses_deployment_id_and_position_columns(
        self, state_service_pg, mock_context
    ):
        """PG INSERT must reference ``deployment_id`` (the single identity
        column, VIB-4721/4722) and carry all four per-position columns
        added in VIB-3658."""
        req = gateway_pb2.SaveOutboxEntryRequest(
            ledger_entry_id=_LEDGER_ID,
            deployment_id=_DEPLOYMENT_ID,
            cycle_id="cycle-1",
            intent_type="SWAP",
            wallet_address="0xdeadbeef",
            position_key="uniswap_v3:arbitrum:0xdeadbeef:eth-usdc",
            market_id="eth-usdc",
        )

        resp = await state_service_pg.SaveOutboxEntry(req, mock_context)

        assert resp.success
        state_service_pg._snapshot_execute.assert_awaited_once()
        sql, *args = state_service_pg._snapshot_execute.call_args.args
        # Column contract: deployment_id only, no legacy identity columns.
        column_section = sql.split("VALUES")[0]
        assert "deployment_id" in column_section
        assert "agent_id" not in column_section
        # Per-position columns must be in the INSERT.
        for col in ("cycle_id", "wallet_address", "position_key", "market_id"):
            assert col in column_section, f"missing column {col} in INSERT"
        # Argument order matches (ledger_entry_id, deployment_id,
        # intent_type, cycle_id, wallet_address, position_key, market_id).
        assert args[0] == _LEDGER_ID
        assert args[1] == _DEPLOYMENT_ID
        assert args[2] == "SWAP"
        assert args[3] == "cycle-1"
        assert args[4] == "0xdeadbeef"
        assert args[5] == "uniswap_v3:arbitrum:0xdeadbeef:eth-usdc"
        assert args[6] == "eth-usdc"

    @pytest.mark.asyncio
    async def test_save_outbox_pg_uses_empty_strings_when_fields_missing(
        self, state_service_pg, mock_context
    ):
        """Optional per-position fields default to '' so the schema's
        NOT NULL DEFAULT '' is satisfied even when the caller omits them."""
        req = gateway_pb2.SaveOutboxEntryRequest(
            ledger_entry_id=_LEDGER_ID,
            deployment_id=_DEPLOYMENT_ID,
            intent_type="SWAP",
            # cycle_id / wallet_address / position_key / market_id all unset
        )

        resp = await state_service_pg.SaveOutboxEntry(req, mock_context)

        assert resp.success
        args = state_service_pg._snapshot_execute.call_args.args[1:]
        # Last four positional args correspond to cycle/wallet/position/market
        # (VIB-4721/4722: no legacy identity column, so they shift down to 3:7).
        assert args[3:7] == ("", "", "", "")

    @pytest.mark.asyncio
    async def test_get_outbox_pg_select_includes_position_columns(
        self, state_service_pg, mock_context
    ):
        """GetOutboxEntry PG SELECT must request deployment_id + 4 new
        columns, and the proto round-trips all of them via
        _pg_outbox_row_to_proto."""
        state_service_pg._snapshot_fetchrow.return_value = _outbox_pg_row()

        resp = await state_service_pg.GetOutboxEntry(
            gateway_pb2.GetOutboxEntryRequest(ledger_entry_id=_LEDGER_ID),
            mock_context,
        )

        assert resp.found
        # SELECT-list contract.
        sql = state_service_pg._snapshot_fetchrow.call_args.args[0]
        select_section = sql.split("FROM")[0]
        assert "deployment_id" in select_section
        assert "agent_id" not in select_section
        for col in ("cycle_id", "wallet_address", "position_key", "market_id"):
            assert col in select_section, f"missing column {col} in SELECT"
        # Round-trip: every per-position field survives PG → proto.
        assert resp.entry.ledger_entry_id == _LEDGER_ID
        assert resp.entry.cycle_id == "cycle-1"
        assert resp.entry.wallet_address == "0xdeadbeef"
        assert resp.entry.position_key == "uniswap_v3:arbitrum:0xdeadbeef:eth-usdc"
        assert resp.entry.market_id == "eth-usdc"
        # PG deployment_id backs both wire identity fields (one identity).
        assert resp.entry.deployment_id == _DEPLOYMENT_ID

    @pytest.mark.asyncio
    async def test_get_outbox_pending_pg_select_includes_position_columns(
        self, state_service_pg, mock_context
    ):
        """GetOutboxPending PG SELECT must request the same fields and
        round-trip all of them across multiple rows."""
        rows = [
            _outbox_pg_row(),
            _outbox_pg_row(
                cycle_id="cycle-2",
                wallet_address="0xcafebabe",
                position_key="aave_v3:base:0xcafebabe:USDC",
                market_id="aave-usdc",
            ),
        ]
        # asyncpg.Record-like rows are dict-like; the helpers support .get().
        state_service_pg._snapshot_fetch.return_value = rows

        resp = await state_service_pg.GetOutboxPending(
            gateway_pb2.GetOutboxPendingRequest(deployment_id=_DEPLOYMENT_ID, max_retries=3),
            mock_context,
        )

        assert len(resp.entries) == 2
        sql = state_service_pg._snapshot_fetch.call_args.args[0]
        select_section = sql.split("FROM")[0]
        assert "deployment_id" in select_section
        assert "agent_id" not in select_section
        for col in ("cycle_id", "wallet_address", "position_key", "market_id"):
            assert col in select_section, f"missing column {col} in SELECT"
        assert resp.entries[0].cycle_id == "cycle-1"
        assert resp.entries[1].cycle_id == "cycle-2"
        assert resp.entries[1].wallet_address == "0xcafebabe"
        assert resp.entries[1].position_key == "aave_v3:base:0xcafebabe:USDC"
        assert resp.entries[1].market_id == "aave-usdc"
