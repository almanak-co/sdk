"""Unit tests for the cutover storage RPCs.

History:
  - VIB-4208 / T22 shipped the SQLite half of these RPCs and the
    request-validation / handler-orchestration scaffolding.
  - VIB-4205 / T19 ships the hosted Postgres half by wiring each RPC's
    ``_snapshot_pool is not None`` branch to the asyncpg helpers
    (``_snapshot_execute`` / ``_snapshot_fetchrow`` / ``_snapshot_fetch``)
    and — for ``SaveLedgerAndRegistry`` — a single ``conn.transaction()``
    for atomic ledger + registry + handle commit.

Covers:
  - UpsertMigrationState: validation, SQLite happy path, idempotency,
    Postgres path now wired (no more UNIMPLEMENTED).
  - GetMigrationState: found/not_found on both backends.
  - UpdateMigrationState: partial-update plumbing through to SQLite kwargs;
    Postgres path now wired.
  - MarkBackfillComplete: terminal flip, validation.
  - GetPositionEventsFiltered: empty / populated, error propagation.
  - GetPositionRegistryOpenRows: empty / populated, payload roundtrip.
  - SaveLedgerAndRegistry: happy path, RegistryAutoCollisionError discrimination,
    AccountingPersistenceError. Postgres path now wired (atomic primitive).
  - GatewayStateManager client-adapter round-trips through a real (in-process)
    StateServiceServicer + SQLite WARM backend — proves the proto + handler +
    adapter triangle is wired correctly.
  - Hard-coding guard (D3.F7): non-LP (primitive, cutover_key) tuple round-trips
    faithfully and a sibling lookup against (lp, lp) returns found=false.

Detailed PG-branch SQL-shape and error-classification tests live in
``test_state_service_postgres_registry_rpcs.py`` (T19).
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import grpc
import pytest
import pytest_asyncio

from almanak.framework.accounting.commit import RegistryRow
from almanak.framework.migration.backfill import MigrationStateRow
from almanak.framework.observability.ledger import LedgerEntry
from almanak.framework.state.backends.sqlite import SQLiteConfig, SQLiteStore
from almanak.framework.state.gateway_state_manager import GatewayStateManager
from almanak.framework.state.registry_errors import RegistryAutoCollisionError
from almanak.framework.state.state_manager import StateManager, StateManagerConfig
from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.state_service import StateServiceServicer

_DEPLOYMENT_ID = "TestStrategy:vib4208"
_PRIMITIVE = "lp"
_CUTOVER_KEY = "lp"


@pytest.fixture
def settings() -> GatewaySettings:
    return GatewaySettings()


@pytest.fixture
def mock_context():
    ctx = MagicMock(spec=grpc.aio.ServicerContext)
    ctx.set_code = MagicMock()
    ctx.set_details = MagicMock()
    return ctx


@pytest.fixture
def state_service(settings: GatewaySettings) -> StateServiceServicer:
    svc = StateServiceServicer(settings)
    svc._snapshot_pool = None  # force SQLite path
    svc._initialized = True
    return svc


def _make_warm(svc: StateServiceServicer, **overrides: Any) -> AsyncMock:
    warm = AsyncMock()
    warm.upsert_migration_state = AsyncMock(return_value=None)
    warm.get_migration_state = AsyncMock(return_value=None)
    warm.update_migration_state = AsyncMock(return_value=None)
    warm.mark_backfill_complete = AsyncMock(return_value=None)
    warm.get_position_events_filtered = AsyncMock(return_value=[])
    warm.get_position_registry_open_rows = AsyncMock(return_value=[])
    for k, v in overrides.items():
        setattr(warm, k, v)
    sm = AsyncMock()
    sm.warm_backend = warm
    svc._state_manager = sm
    return warm


@pytest_asyncio.fixture
async def real_sqlite_servicer(tmp_path):
    """A real StateServiceServicer wired to a real SQLite WARM backend.

    Used by the round-trip tests below to prove the full proto +
    handler + adapter triangle works.
    """
    settings = GatewaySettings()
    svc = StateServiceServicer(settings)
    sm = StateManager(
        StateManagerConfig(),
        warm_backend=SQLiteStore(SQLiteConfig(db_path=str(tmp_path / "vib4208.db"))),
    )
    await sm.initialize()
    svc._state_manager = sm
    svc._initialized = True
    svc._snapshot_pool = None
    yield svc
    await sm.close()


class _DirectServiceClient:
    """Minimal gRPC-client stand-in that dispatches to a servicer's coroutines.

    Lets us exercise the GatewayStateManager adapter end-to-end without
    spinning up a real gRPC channel. The methods we expose mirror the
    ``almanak.gateway.proto.gateway_pb2_grpc.StateServiceStub`` surface
    — which is SYNC in production (``grpc.insecure_channel`` based, see
    ``framework/gateway_client.py``). We therefore drive the async
    servicer coroutines to completion synchronously, using a dedicated
    event loop in a worker thread (matches how blocking gRPC stubs
    behave against an asyncio server).
    """

    def __init__(self, svc: StateServiceServicer) -> None:
        self._svc = svc
        self._ctx = MagicMock(spec=grpc.aio.ServicerContext)
        self._ctx.set_code = MagicMock()
        self._ctx.set_details = MagicMock()

    def _run_sync(self, coro: Any) -> Any:
        """Drive an async servicer coroutine to completion synchronously.

        Mirrors production: the GatewayClient uses ``grpc.insecure_channel``
        so the stub methods are SYNC blocking calls. The corresponding
        server-side handler is ``async def`` because the gateway server
        uses ``grpc.aio.server``. To bridge those two worlds in-test, we
        spin up a private event loop in a worker thread and ``run_until_complete``.
        """
        import threading

        container: dict[str, Any] = {}

        def _worker() -> None:
            import asyncio

            loop = asyncio.new_event_loop()
            try:
                container["result"] = loop.run_until_complete(coro)
            except Exception as exc:  # noqa: BLE001
                container["error"] = exc
            finally:
                loop.close()

        t = threading.Thread(target=_worker)
        t.start()
        t.join()
        if "error" in container:
            raise container["error"]
        return container["result"]

    def UpsertMigrationState(self, req, timeout=None):  # noqa: N802, ARG002
        return self._run_sync(self._svc.UpsertMigrationState(req, self._ctx))

    def GetMigrationState(self, req, timeout=None):  # noqa: N802, ARG002
        return self._run_sync(self._svc.GetMigrationState(req, self._ctx))

    def UpdateMigrationState(self, req, timeout=None):  # noqa: N802, ARG002
        return self._run_sync(self._svc.UpdateMigrationState(req, self._ctx))

    def MarkBackfillComplete(self, req, timeout=None):  # noqa: N802, ARG002
        return self._run_sync(self._svc.MarkBackfillComplete(req, self._ctx))

    def GetPositionEventsFiltered(self, req, timeout=None):  # noqa: N802, ARG002
        return self._run_sync(self._svc.GetPositionEventsFiltered(req, self._ctx))

    def GetPositionRegistryOpenRows(self, req, timeout=None):  # noqa: N802, ARG002
        return self._run_sync(self._svc.GetPositionRegistryOpenRows(req, self._ctx))

    def SaveLedgerAndRegistry(self, req, timeout=None):  # noqa: N802, ARG002
        return self._run_sync(self._svc.SaveLedgerAndRegistry(req, self._ctx))


@pytest_asyncio.fixture
async def gsm_client(real_sqlite_servicer) -> GatewayStateManager:
    """A GatewayStateManager wired to the in-process real-SQLite servicer."""
    fake_gateway_client = MagicMock()
    fake_gateway_client.state = _DirectServiceClient(real_sqlite_servicer)
    return GatewayStateManager(fake_gateway_client)


# =============================================================================
# UpsertMigrationState — D1.S3 / D3.F1 / D3.F5
# =============================================================================


class TestUpsertMigrationState:
    @pytest.mark.asyncio
    async def test_rejects_missing_deployment_id(self, state_service, mock_context):
        req = gateway_pb2.UpsertMigrationStateRequest(
            deployment_id="", primitive=_PRIMITIVE, cutover_key=_CUTOVER_KEY
        )
        resp = await state_service.UpsertMigrationState(req, mock_context)
        assert not resp.success
        assert "deployment_id" in resp.error

    @pytest.mark.asyncio
    async def test_rejects_missing_primitive(self, state_service, mock_context):
        req = gateway_pb2.UpsertMigrationStateRequest(
            deployment_id=_DEPLOYMENT_ID, primitive="", cutover_key=_CUTOVER_KEY
        )
        resp = await state_service.UpsertMigrationState(req, mock_context)
        assert not resp.success
        assert "primitive" in resp.error

    @pytest.mark.asyncio
    async def test_rejects_missing_cutover_key(self, state_service, mock_context):
        req = gateway_pb2.UpsertMigrationStateRequest(
            deployment_id=_DEPLOYMENT_ID, primitive=_PRIMITIVE, cutover_key=""
        )
        resp = await state_service.UpsertMigrationState(req, mock_context)
        assert not resp.success
        assert "cutover_key" in resp.error

    @pytest.mark.asyncio
    async def test_sqlite_happy_path(self, state_service, mock_context):
        warm = _make_warm(state_service)
        req = gateway_pb2.UpsertMigrationStateRequest(
            deployment_id=_DEPLOYMENT_ID, primitive=_PRIMITIVE, cutover_key=_CUTOVER_KEY
        )
        resp = await state_service.UpsertMigrationState(req, mock_context)
        assert resp.success
        warm.upsert_migration_state.assert_awaited_once_with(
            deployment_id=_DEPLOYMENT_ID, primitive=_PRIMITIVE, cutover_key=_CUTOVER_KEY
        )

    @pytest.mark.asyncio
    async def test_postgres_now_wired(self, state_service, mock_context):
        """T19 (VIB-4205): the Postgres branch is now implemented.

        The handler calls ``_snapshot_execute`` with an ``INSERT … ON
        CONFLICT DO NOTHING`` keyed on the composite triple. Detailed
        SQL-shape assertions live in
        ``test_state_service_postgres_registry_rpcs.py``; this guard only
        proves the UNIMPLEMENTED stub is gone.
        """
        state_service._snapshot_pool = object()  # truthy → Postgres path
        state_service._snapshot_execute = AsyncMock(return_value="INSERT 0 1")
        req = gateway_pb2.UpsertMigrationStateRequest(
            deployment_id=_DEPLOYMENT_ID, primitive=_PRIMITIVE, cutover_key=_CUTOVER_KEY
        )
        resp = await state_service.UpsertMigrationState(req, mock_context)
        assert resp.success
        mock_context.set_code.assert_not_called()
        state_service._snapshot_execute.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_upsert_idempotent_via_real_sqlite(self, gsm_client):
        """D3.F5 — idempotency: re-Upsert MUST NOT mutate the existing row."""
        await gsm_client.upsert_migration_state(
            deployment_id=_DEPLOYMENT_ID, primitive=_PRIMITIVE, cutover_key=_CUTOVER_KEY
        )
        state1 = await gsm_client.get_migration_state(
            deployment_id=_DEPLOYMENT_ID, primitive=_PRIMITIVE, cutover_key=_CUTOVER_KEY
        )
        assert state1 is not None
        # Re-Upsert should be a no-op (ON CONFLICT DO NOTHING).
        await gsm_client.upsert_migration_state(
            deployment_id=_DEPLOYMENT_ID, primitive=_PRIMITIVE, cutover_key=_CUTOVER_KEY
        )
        state2 = await gsm_client.get_migration_state(
            deployment_id=_DEPLOYMENT_ID, primitive=_PRIMITIVE, cutover_key=_CUTOVER_KEY
        )
        assert state2 is not None
        # Every contractual field is unchanged across the two calls.
        assert state1.position_registry_backfill_complete == state2.position_registry_backfill_complete
        assert state1.rows_synthesized == state2.rows_synthesized
        assert state1.rows_skipped_already_present == state2.rows_skipped_already_present
        assert state1.created_at == state2.created_at
        # backfill_started_at / completed_at are NULL on a fresh upsert and
        # MUST stay NULL across the no-op.
        assert state1.backfill_started_at == state2.backfill_started_at
        assert state1.backfill_completed_at == state2.backfill_completed_at


# =============================================================================
# GetMigrationState — D1.S4 / D1.S5
# =============================================================================


class TestGetMigrationState:
    @pytest.mark.asyncio
    async def test_get_not_found_returns_found_false(self, state_service, mock_context):
        _make_warm(state_service)
        req = gateway_pb2.GetMigrationStateRequest(
            deployment_id=_DEPLOYMENT_ID, primitive=_PRIMITIVE, cutover_key=_CUTOVER_KEY
        )
        resp = await state_service.GetMigrationState(req, mock_context)
        assert not resp.found

    @pytest.mark.asyncio
    async def test_get_found_returns_full_row(self, state_service, mock_context):
        warm = _make_warm(state_service)
        warm.get_migration_state = AsyncMock(
            return_value=MigrationStateRow(
                deployment_id=_DEPLOYMENT_ID,
                primitive=_PRIMITIVE,
                cutover_key=_CUTOVER_KEY,
                position_registry_backfill_complete=True,
                backfill_started_at="2026-05-11T00:00:00+00:00",
                backfill_completed_at="2026-05-11T00:00:05+00:00",
                backfill_source_table="position_events",
                backfill_reader_version=1,
                rows_synthesized=0,
                rows_skipped_already_present=0,
                notes={"audit": []},
                created_at="2026-05-11T00:00:00+00:00",
                updated_at="2026-05-11T00:00:05+00:00",
            )
        )
        req = gateway_pb2.GetMigrationStateRequest(
            deployment_id=_DEPLOYMENT_ID, primitive=_PRIMITIVE, cutover_key=_CUTOVER_KEY
        )
        resp = await state_service.GetMigrationState(req, mock_context)
        assert resp.found
        assert resp.data.deployment_id == _DEPLOYMENT_ID
        assert resp.data.primitive == _PRIMITIVE
        assert resp.data.cutover_key == _CUTOVER_KEY
        assert resp.data.position_registry_backfill_complete is True
        assert resp.data.backfill_source_table == "position_events"
        assert json.loads(resp.data.notes.decode("utf-8")) == {"audit": []}

    @pytest.mark.asyncio
    async def test_get_postgres_now_wired_not_found(self, state_service, mock_context):
        """T19 (VIB-4205): Postgres branch wired; fetchrow → None → found=False."""
        state_service._snapshot_pool = object()
        state_service._snapshot_fetchrow = AsyncMock(return_value=None)
        req = gateway_pb2.GetMigrationStateRequest(
            deployment_id=_DEPLOYMENT_ID, primitive=_PRIMITIVE, cutover_key=_CUTOVER_KEY
        )
        resp = await state_service.GetMigrationState(req, mock_context)
        assert not resp.found
        # UNIMPLEMENTED gone — no error code, no error_class.
        mock_context.set_code.assert_not_called()
        assert not resp.error
        state_service._snapshot_fetchrow.assert_awaited_once()


# =============================================================================
# Client-adapter round-trip (D1.S6 / D1.S7) — proves the proto + handler + adapter triangle
# =============================================================================


class TestClientAdapterRoundTrip:
    @pytest.mark.asyncio
    async def test_client_adapter_upsert_and_get(self, gsm_client):
        """D1.S6 + D1.S7 — round-trip through real SQLite via GatewayStateManager."""
        await gsm_client.upsert_migration_state(
            deployment_id=_DEPLOYMENT_ID, primitive=_PRIMITIVE, cutover_key=_CUTOVER_KEY
        )
        state = await gsm_client.get_migration_state(
            deployment_id=_DEPLOYMENT_ID, primitive=_PRIMITIVE, cutover_key=_CUTOVER_KEY
        )
        assert state is not None
        assert isinstance(state, MigrationStateRow)
        assert state.deployment_id == _DEPLOYMENT_ID
        assert state.primitive == _PRIMITIVE
        assert state.cutover_key == _CUTOVER_KEY
        # Fresh seed defaults: complete=0, backfill_source_table='position_events'
        assert state.position_registry_backfill_complete is False
        assert state.backfill_source_table == "position_events"
        assert state.notes == {}

    @pytest.mark.asyncio
    async def test_client_adapter_get_not_found_returns_none(self, gsm_client):
        state = await gsm_client.get_migration_state(
            deployment_id="DoesNotExist:xyz", primitive=_PRIMITIVE, cutover_key=_CUTOVER_KEY
        )
        assert state is None

    @pytest.mark.asyncio
    async def test_grpc_error_propagates(self, gsm_client):
        """D3.F3 — gRPC errors that are NOT UNIMPLEMENTED propagate loud."""

        # Patch the underlying client surface to raise a non-UNIMPLEMENTED RpcError.
        class _BoomRpcError(grpc.RpcError):
            def code(self):  # noqa: ARG002
                return grpc.StatusCode.UNAVAILABLE

            def details(self):  # noqa: ARG002
                return "channel down"

        original = gsm_client._client.state.UpsertMigrationState

        def _raise(_req, timeout=None):  # noqa: ARG001
            raise _BoomRpcError()

        gsm_client._client.state.UpsertMigrationState = _raise  # type: ignore[assignment]
        try:
            with pytest.raises(grpc.RpcError):
                await gsm_client.upsert_migration_state(
                    deployment_id=_DEPLOYMENT_ID, primitive=_PRIMITIVE, cutover_key=_CUTOVER_KEY
                )
        finally:
            gsm_client._client.state.UpsertMigrationState = original  # type: ignore[assignment]


# =============================================================================
# D3.F7 — Non-LP tuple round-trip (prove primitive/cutover_key not hard-coded server-side)
# =============================================================================


class TestNonHardcodedTuple:
    @pytest.mark.asyncio
    async def test_non_lp_round_trip(self, gsm_client):
        """A non-LP (primitive, cutover_key) tuple round-trips faithfully.

        A buggy server that hard-coded primitive='lp' would either:
        (1) write the row at (lp, lp) — sibling lookup at (perp, gmx-v2) returns None.
        (2) fail to retrieve the row at (perp, gmx-v2) — Get returns None.
        The two-sided assertion below catches both.
        """
        non_lp_deployment = "PerpStrategy:vib4208"
        await gsm_client.upsert_migration_state(
            deployment_id=non_lp_deployment, primitive="perp", cutover_key="gmx-v2"
        )
        # Retrieve under the SAME tuple we wrote — must succeed.
        row = await gsm_client.get_migration_state(
            deployment_id=non_lp_deployment, primitive="perp", cutover_key="gmx-v2"
        )
        assert row is not None, "Get under (perp, gmx-v2) returned None — server may be hard-coded"
        assert row.primitive == "perp"
        assert row.cutover_key == "gmx-v2"

        # Sibling lookup under (lp, lp) MUST return None — proving the
        # server did NOT silently key on a hard-coded LP slot.
        sibling = await gsm_client.get_migration_state(
            deployment_id=non_lp_deployment, primitive="lp", cutover_key="lp"
        )
        assert sibling is None, "Server appears hard-coded — (lp, lp) row exists for a (perp, gmx-v2) Upsert"


# =============================================================================
# UpdateMigrationState / MarkBackfillComplete — partial-update + terminal flip
# =============================================================================


class TestUpdateMigrationState:
    @pytest.mark.asyncio
    async def test_partial_update_threads_to_warm(self, state_service, mock_context):
        warm = _make_warm(state_service)
        req = gateway_pb2.UpdateMigrationStateRequest(
            deployment_id=_DEPLOYMENT_ID,
            primitive=_PRIMITIVE,
            cutover_key=_CUTOVER_KEY,
            backfill_started_at="2026-05-11T01:00:00+00:00",
            rows_synthesized=42,
        )
        resp = await state_service.UpdateMigrationState(req, mock_context)
        assert resp.success
        kwargs = warm.update_migration_state.call_args.kwargs
        assert kwargs["backfill_started_at"] == "2026-05-11T01:00:00+00:00"
        assert kwargs["rows_synthesized"] == 42
        assert kwargs["rows_skipped_already_present"] is None

    @pytest.mark.asyncio
    async def test_postgres_now_wired_empty_request_is_noop(
        self, state_service, mock_context
    ):
        """T19 (VIB-4205): empty Update on Postgres is a no-op (mirrors SQLite)."""
        state_service._snapshot_pool = object()
        state_service._snapshot_execute = AsyncMock(return_value="UPDATE 0")
        # No counters / timestamp supplied — no UPDATE should be issued.
        req = gateway_pb2.UpdateMigrationStateRequest(
            deployment_id=_DEPLOYMENT_ID, primitive=_PRIMITIVE, cutover_key=_CUTOVER_KEY
        )
        resp = await state_service.UpdateMigrationState(req, mock_context)
        assert resp.success
        mock_context.set_code.assert_not_called()
        # Empty request path returns without calling _snapshot_execute.
        state_service._snapshot_execute.assert_not_called()


class TestMarkBackfillComplete:
    @pytest.mark.asyncio
    async def test_threads_to_warm(self, state_service, mock_context):
        warm = _make_warm(state_service)
        req = gateway_pb2.MarkBackfillCompleteRequest(
            deployment_id=_DEPLOYMENT_ID,
            primitive=_PRIMITIVE,
            cutover_key=_CUTOVER_KEY,
            rows_synthesized=3,
            rows_skipped_already_present=1,
            backfill_completed_at="2026-05-11T02:00:00+00:00",
        )
        resp = await state_service.MarkBackfillComplete(req, mock_context)
        assert resp.success
        kwargs = warm.mark_backfill_complete.call_args.kwargs
        assert kwargs["rows_synthesized"] == 3
        assert kwargs["rows_skipped_already_present"] == 1
        assert kwargs["backfill_completed_at"] == "2026-05-11T02:00:00+00:00"

    @pytest.mark.asyncio
    async def test_rejects_missing_completed_at(self, state_service, mock_context):
        _make_warm(state_service)
        req = gateway_pb2.MarkBackfillCompleteRequest(
            deployment_id=_DEPLOYMENT_ID,
            primitive=_PRIMITIVE,
            cutover_key=_CUTOVER_KEY,
            backfill_completed_at="",
        )
        resp = await state_service.MarkBackfillComplete(req, mock_context)
        assert not resp.success


# =============================================================================
# GetPositionEventsFiltered — empty + populated
# =============================================================================


class TestGetPositionEventsFiltered:
    @pytest.mark.asyncio
    async def test_empty_returns_empty(self, state_service, mock_context):
        _make_warm(state_service)  # default returns []
        req = gateway_pb2.GetPositionEventsFilteredRequest(
            deployment_id=_DEPLOYMENT_ID, position_types=["LP"]
        )
        resp = await state_service.GetPositionEventsFiltered(req, mock_context)
        assert len(resp.events) == 0

    @pytest.mark.asyncio
    async def test_populated_returns_rows(self, state_service, mock_context):
        warm = _make_warm(state_service)
        warm.get_position_events_filtered = AsyncMock(
            return_value=[
                {
                    "id": "evt-1",
                    "deployment_id": _DEPLOYMENT_ID,
                    "position_id": "pos-1",
                    "position_type": "LP",
                    "event_type": "OPEN",
                    "timestamp": "2026-05-11T00:00:00+00:00",
                    "protocol": "uniswap_v3",
                    "chain": "arbitrum",
                    "token0": "USDC",
                    "token1": "WETH",
                    "amount0": "100",
                    "amount1": "0.1",
                    "value_usd": "100",
                    "liquidity": "1000",
                    "fees_token0": "0",
                    "fees_token1": "0",
                    "leverage": "",
                    "entry_price": "",
                    "mark_price": "",
                    "unrealized_pnl": "",
                    "tx_hash": "0xabc",
                    "gas_usd": "0.5",
                    "ledger_entry_id": "ledger-1",
                    "protocol_fees_usd": "",
                    "attribution_json": "{}",
                    "attribution_version": 0,
                }
            ]
        )
        req = gateway_pb2.GetPositionEventsFilteredRequest(
            deployment_id=_DEPLOYMENT_ID, position_types=["LP"]
        )
        resp = await state_service.GetPositionEventsFiltered(req, mock_context)
        assert len(resp.events) == 1
        ev = resp.events[0]
        assert ev.id == "evt-1"
        assert ev.position_type == "LP"
        assert ev.protocol == "uniswap_v3"

    @pytest.mark.asyncio
    async def test_postgres_now_wired_empty_position_types_returns_empty(
        self, state_service, mock_context
    ):
        """T19 (VIB-4205): empty position_types returns empty list without hitting DB."""
        state_service._snapshot_pool = object()
        state_service._snapshot_fetch = AsyncMock(return_value=[])
        req = gateway_pb2.GetPositionEventsFilteredRequest(deployment_id=_DEPLOYMENT_ID)
        resp = await state_service.GetPositionEventsFiltered(req, mock_context)
        assert not resp.error
        assert len(resp.events) == 0
        mock_context.set_code.assert_not_called()
        # Fast-path: no DB call.
        state_service._snapshot_fetch.assert_not_called()


# =============================================================================
# GetPositionRegistryOpenRows
# =============================================================================


class TestGetPositionRegistryOpenRows:
    @pytest.mark.asyncio
    async def test_empty_returns_empty(self, state_service, mock_context):
        _make_warm(state_service)
        req = gateway_pb2.GetPositionRegistryOpenRowsRequest(deployment_id=_DEPLOYMENT_ID)
        resp = await state_service.GetPositionRegistryOpenRows(req, mock_context)
        assert len(resp.rows) == 0

    @pytest.mark.asyncio
    async def test_populated_returns_parsed_payload(self, state_service, mock_context):
        warm = _make_warm(state_service)
        warm.get_position_registry_open_rows = AsyncMock(
            return_value=[
                {
                    "deployment_id": _DEPLOYMENT_ID,
                    "chain": "arbitrum",
                    "primitive": "lp",
                    "accounting_category": "lp",
                    "physical_identity_hash": "0xabc",
                    "semantic_grouping_key": "arbitrum:0xpool",
                    "grouping_policy_version": "univ3_lp@v1",
                    "handle": None,
                    "status": "open",
                    "payload": {"token_id": 5482307, "pool_address": "0xpool"},
                    "opened_at_block": 12345,
                    "opened_tx": "0xtxopen",
                    "closed_at_block": None,
                    "closed_tx": None,
                    "last_reconciled_at_block": None,
                    "matching_policy_version": 1,
                }
            ]
        )
        req = gateway_pb2.GetPositionRegistryOpenRowsRequest(
            deployment_id=_DEPLOYMENT_ID, primitive="lp"
        )
        resp = await state_service.GetPositionRegistryOpenRows(req, mock_context)
        assert len(resp.rows) == 1
        row = resp.rows[0]
        assert row.physical_identity_hash == "0xabc"
        assert row.status == "open"
        parsed = json.loads(row.payload.decode("utf-8"))
        assert parsed == {"token_id": 5482307, "pool_address": "0xpool"}


# =============================================================================
# SaveLedgerAndRegistry — happy path, collision class, generic failure
# =============================================================================


def _make_ledger() -> LedgerEntry:
    # Use a real UUID — SaveLedgerAndRegistry validates id format (mirrors
    # SaveLedgerEntry; CodeRabbit PR #2230 flagged the bypass).
    return LedgerEntry(
        id="11111111-1111-1111-1111-111111111111",
        cycle_id="cyc-1",
        strategy_id="TestStrategy:vib4208",
        deployment_id=_DEPLOYMENT_ID,
        execution_mode="paper",
        timestamp=datetime(2026, 5, 11, tzinfo=UTC),
        intent_type="LP_OPEN",
        token_in="USDC",
        amount_in="100",
        token_out="LP",
        amount_out="1",
        effective_price="100",
        slippage_bps=None,
        gas_used=200000,
        gas_usd="0.5",
        tx_hash="0xtx",
        chain="arbitrum",
        protocol="uniswap_v3",
        success=True,
        error="",
        extracted_data_json="{}",
        price_inputs_json="{}",
        pre_state_json="",
        post_state_json="",
    )


def _make_registry_row(*, status: str = "open") -> RegistryRow:
    return RegistryRow(
        deployment_id=_DEPLOYMENT_ID,
        chain="arbitrum",
        primitive="lp",
        accounting_category="lp",
        physical_identity_hash="0xpih1",
        semantic_grouping_key="arbitrum:0xpool",
        grouping_policy_version="univ3_lp@v1",
        status=status,  # type: ignore[arg-type]
        payload={"token_id": 5482307, "pool_address": "0xpool"},
        matching_policy_version=1,
        opened_at_block=12345,
        opened_tx="0xopen",
    )


class TestSaveLedgerAndRegistry:
    @pytest.mark.asyncio
    async def test_happy_path_via_real_sqlite(self, gsm_client):
        """Round-trip a registry-mode write through the proto + adapter."""
        ledger = _make_ledger()
        registry = _make_registry_row()
        await gsm_client.save_ledger_and_registry(ledger=ledger, registry=registry, handle=None)
        # Confirm the row landed via the read side.
        rows = await gsm_client.get_position_registry_open_rows(
            _DEPLOYMENT_ID, primitive="lp"
        )
        assert len(rows) == 1
        assert rows[0]["physical_identity_hash"] == "0xpih1"
        assert rows[0]["status"] == "open"
        assert rows[0]["payload"]["token_id"] == 5482307

    @pytest.mark.asyncio
    async def test_collision_class_propagates(self, state_service, mock_context):
        """RegistryAutoCollisionError surfaces with the right error_class."""
        from almanak.framework.accounting.commit import RegistryRow as _RR
        from almanak.framework.observability.ledger import LedgerEntry as _LE

        sm = MagicMock()
        sm.save_ledger_and_registry = AsyncMock(
            side_effect=RegistryAutoCollisionError(
                semantic_grouping_key="arbitrum:0xpool",
                existing_physical_identity_hash="0xexisting",
                opened_tx="0xwinner",
                accounting_category="lp",
            )
        )
        state_service._state_manager = sm

        req = gateway_pb2.SaveLedgerAndRegistryRequest(
            id="22222222-2222-2222-2222-222222222222",
            cycle_id="cyc-2",
            strategy_id="s",
            deployment_id=_DEPLOYMENT_ID,
            execution_mode="paper",
            timestamp=int(datetime(2026, 5, 11, tzinfo=UTC).timestamp()),
            intent_type="LP_OPEN",
            token_in="USDC",
            token_out="LP",
            chain="arbitrum",
            protocol="uniswap_v3",
            success=True,
            registry_chain="arbitrum",
            registry_primitive="lp",
            registry_accounting_category="lp",
            registry_physical_identity_hash="0xpih",
            registry_semantic_grouping_key="arbitrum:pool",
            registry_grouping_policy_version="univ3_lp@v1",
            registry_status="open",
            registry_payload_json=b"{}",
            registry_matching_policy_version=1,
        )
        resp = await state_service.SaveLedgerAndRegistry(req, mock_context)
        assert not resp.success
        assert resp.error_class == "RegistryAutoCollisionError"

    @pytest.mark.asyncio
    async def test_postgres_now_wired(self, state_service, mock_context):
        """T19 (VIB-4205): Postgres SaveLedgerAndRegistry is now wired.

        Detailed SQL-shape, atomicity, and error-classification tests live
        in ``test_state_service_postgres_registry_rpcs.py``; this guard
        only proves the UNIMPLEMENTED stub is gone by patching the
        internal PG branch helper with an AsyncMock so the handler can
        reach the success path without a real asyncpg pool.
        """
        state_service._snapshot_pool = object()
        # Replace the private PG branch with an AsyncMock that returns a
        # success response — this short-circuits the asyncpg work while
        # still proving the dispatcher routes to the new T19 branch.
        success_resp = gateway_pb2.SaveLedgerAndRegistryResponse(success=True)
        state_service._save_ledger_and_registry_pg = AsyncMock(return_value=success_resp)

        req = gateway_pb2.SaveLedgerAndRegistryRequest(
            id="33333333-3333-3333-3333-333333333333",
            cycle_id="cyc-3",
            strategy_id="TestStrategy:vib4208",
            deployment_id=_DEPLOYMENT_ID,
            execution_mode="paper",
            timestamp=int(datetime(2026, 5, 11, tzinfo=UTC).timestamp()),
            intent_type="LP_OPEN",
            chain="arbitrum",
            protocol="uniswap_v3",
            registry_chain="arbitrum",
            registry_primitive="lp",
            registry_accounting_category="lp",
            registry_physical_identity_hash="0xpih",
            registry_semantic_grouping_key="arbitrum:pool",
            registry_grouping_policy_version="univ3_lp@v1",
            registry_status="open",
            registry_payload_json=b"{}",
            registry_matching_policy_version=1,
        )
        resp = await state_service.SaveLedgerAndRegistry(req, mock_context)
        assert resp.success
        mock_context.set_code.assert_not_called()
        state_service._save_ledger_and_registry_pg.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_client_adapter_no_longer_translates_unimplemented(self, gsm_client):
        """T19 (VIB-4205): the Postgres branch is wired, so the adapter no
        longer raises ``CutoverStorageNotSupported``.

        We can't easily exercise the real asyncpg pool from a unit test, so
        we patch the in-process servicer's PG branch helper to return a
        canned success response. This proves the client adapter no longer
        sees gRPC UNIMPLEMENTED for the Postgres path.
        """
        svc = gsm_client._client.state._svc  # type: ignore[attr-defined]
        svc._snapshot_pool = object()
        svc._save_ledger_and_registry_pg = AsyncMock(
            return_value=gateway_pb2.SaveLedgerAndRegistryResponse(success=True)
        )
        try:
            # Should NOT raise CutoverStorageNotSupported any more.
            await gsm_client.save_ledger_and_registry(
                ledger=_make_ledger(),
                registry=_make_registry_row(),
                handle=None,
            )
        finally:
            svc._snapshot_pool = None
            # Restore the bound method by deletion (the AsyncMock attr override
            # was set on the instance and will be cleared on next servicer init).
            try:
                del svc._save_ledger_and_registry_pg
            except AttributeError:
                pass
