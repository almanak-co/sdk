"""Tests for the GetAccountingEvents gRPC endpoint (VIB-3503 Part 2c).

Two readers depend on this RPC in deployed mode:
- Runner startup ``_run_loop_helpers`` reconstructs the lending FIFO
  basis store so REPAY / PT_REDEEM realized-PnL is correct after restart.
- ``PortfolioValuer`` per-snapshot prefetch enriches lending and vault
  positions with cost_basis_usd / unrealized_pnl_usd / realized_pnl_usd.

PG branch tests pin the SELECT column order, ORDER BY ASC ordering,
filter pushdowns, and JSONB → wire-bytes round-trip. SQLite branch tests
exercise the warm-backend delegate with Python-side filter parity for
fields the SQLite primitive doesn't support.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import grpc
import pytest

from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.state_service import StateServiceServicer

_VALID_STRATEGY = "test-strategy"
_DEPLOYMENT = "deploy-1"


@pytest.fixture
def ctx() -> MagicMock:
    c = MagicMock(spec=grpc.aio.ServicerContext)
    c.set_code = MagicMock()
    c.set_details = MagicMock()
    return c


@pytest.fixture
def pg_service() -> StateServiceServicer:
    """Service with snapshot pool wired (truthy) so the PG branch runs."""
    svc = StateServiceServicer(GatewaySettings())
    svc._initialized = True
    svc._snapshot_pool_initialized = True
    svc._snapshot_pool = MagicMock()
    svc._snapshot_fetch = AsyncMock(return_value=[])
    svc._ensure_initialized = AsyncMock()
    svc._ensure_snapshot_pool = AsyncMock()
    return svc


@pytest.fixture
def sqlite_service() -> StateServiceServicer:
    """Service with snapshot pool None so the SQLite delegate path runs."""
    svc = StateServiceServicer(GatewaySettings())
    svc._initialized = True
    svc._snapshot_pool_initialized = True
    svc._snapshot_pool = None
    svc._ensure_initialized = AsyncMock()
    svc._ensure_snapshot_pool = AsyncMock()
    return svc


def _request(**overrides) -> gateway_pb2.GetAccountingEventsRequest:
    defaults = dict(
        strategy_id=_VALID_STRATEGY,
        deployment_id=_DEPLOYMENT,
        position_key="",
        event_type="",
        since_timestamp=0,
        limit=0,
    )
    defaults.update(overrides)
    return gateway_pb2.GetAccountingEventsRequest(**defaults)


def _pg_row(**overrides) -> dict:
    """Mock asyncpg.Record-like dict matching the SELECT column aliases."""
    base = {
        "id": "11111111-1111-1111-1111-111111111111",
        "deployment_id": _DEPLOYMENT,
        "agent_id": _VALID_STRATEGY,
        "cycle_id": "cycle-1",
        "execution_mode": "live",
        "ts_epoch": 1_712_000_000,
        "chain": "arbitrum",
        "protocol": "aave_v3",
        "wallet_address": "0xwallet",
        "event_type": "SUPPLY",
        "position_key": "aave-usdc",
        "ledger_entry_id": "22222222-2222-2222-2222-222222222222",
        "tx_hash": "0xabc",
        "confidence": "HIGH",
        "payload_text": '{"foo":"bar"}',
        "schema_version": 1,
    }
    base.update(overrides)
    return base


def _sqlite_row(**overrides) -> dict:
    """Mock SQLite row dict matching SQLiteStore.get_accounting_events_sync return shape."""
    base = {
        "id": "11111111-1111-1111-1111-111111111111",
        "deployment_id": _DEPLOYMENT,
        "strategy_id": _VALID_STRATEGY,
        "cycle_id": "cycle-1",
        "execution_mode": "live",
        "timestamp": "2024-04-01T00:00:00+00:00",
        "chain": "arbitrum",
        "protocol": "aave_v3",
        "wallet_address": "0xwallet",
        "event_type": "SUPPLY",
        "position_key": "aave-usdc",
        "ledger_entry_id": "22222222-2222-2222-2222-222222222222",
        "tx_hash": "0xabc",
        "confidence": "HIGH",
        "payload_json": '{"foo":"bar"}',
        "schema_version": 1,
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


class TestGetAccountingEventsValidation:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("strategy_id", ["", "   ", "bad/id"])
    async def test_invalid_strategy_id(self, pg_service, ctx, strategy_id):
        response = await pg_service.GetAccountingEvents(_request(strategy_id=strategy_id), ctx)
        assert list(response.events) == []
        ctx.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)

    @pytest.mark.asyncio
    async def test_missing_deployment_id(self, pg_service, ctx):
        response = await pg_service.GetAccountingEvents(_request(deployment_id=""), ctx)
        assert list(response.events) == []
        ctx.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("deployment_id", ["   ", "\t", "\n  "])
    async def test_whitespace_only_deployment_id_rejected(self, pg_service, ctx, deployment_id):
        """Whitespace-only deployment_id is functionally missing -- reject with
        INVALID_ARGUMENT before the WHERE-clause filter would silently match
        nothing on the PG side or pass through to SQLite as a junk filter.
        """
        response = await pg_service.GetAccountingEvents(_request(deployment_id=deployment_id), ctx)
        assert list(response.events) == []
        ctx.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)
        # The fetch must NOT have been issued -- validation runs first.
        pg_service._snapshot_fetch.assert_not_called()

    @pytest.mark.asyncio
    async def test_negative_limit_rejected(self, pg_service, ctx):
        """limit < 0 has no defined meaning. PG would silently return empty;
        SQLite list slicing would slice from the end. Reject at the boundary
        so both backends agree.
        """
        response = await pg_service.GetAccountingEvents(_request(limit=-1), ctx)
        assert list(response.events) == []
        ctx.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)
        pg_service._snapshot_fetch.assert_not_called()

    @pytest.mark.asyncio
    async def test_negative_since_timestamp_rejected(self, pg_service, ctx):
        """since_timestamp < 0 has no defined meaning. Reject at the boundary."""
        response = await pg_service.GetAccountingEvents(_request(since_timestamp=-1), ctx)
        assert list(response.events) == []
        ctx.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)
        pg_service._snapshot_fetch.assert_not_called()

    @pytest.mark.asyncio
    async def test_limit_zero_still_accepted_as_unbounded(self, pg_service, ctx):
        """limit=0 is the documented sentinel for "no limit" (FIFO replay
        needs full history). Verify the validation guard for negatives does
        NOT regress this contract.
        """
        response = await pg_service.GetAccountingEvents(_request(limit=0), ctx)
        # No error, fetch was issued.
        assert list(response.events) == []
        pg_service._snapshot_fetch.assert_called_once()


# ---------------------------------------------------------------------------
# Postgres branch
# ---------------------------------------------------------------------------


class TestGetAccountingEventsPostgresBranch:
    """VIB-3503 Part 2c: hosted-mode reads from PG."""

    @pytest.mark.asyncio
    async def test_pg_happy_path(self, pg_service, ctx):
        """3 rows from mock fetch → 3 proto events with all fields round-tripped."""
        rows = [
            _pg_row(id="aaa", ts_epoch=100),
            _pg_row(id="bbb", ts_epoch=200),
            _pg_row(id="ccc", ts_epoch=300),
        ]
        pg_service._snapshot_fetch = AsyncMock(return_value=rows)

        response = await pg_service.GetAccountingEvents(_request(), ctx)

        assert len(response.events) == 3
        assert response.events[0].id == "aaa"
        assert response.events[0].timestamp == 100
        assert response.events[0].strategy_id == _VALID_STRATEGY  # agent_id → strategy_id on wire
        assert response.events[0].deployment_id == _DEPLOYMENT
        assert response.events[0].payload_json == b'{"foo":"bar"}'
        assert response.events[0].schema_version == 1

    @pytest.mark.asyncio
    async def test_pg_empty_result(self, pg_service, ctx):
        """No rows → empty events list, no error."""
        pg_service._snapshot_fetch = AsyncMock(return_value=[])
        response = await pg_service.GetAccountingEvents(_request(), ctx)
        assert list(response.events) == []
        ctx.set_code.assert_not_called()

    @pytest.mark.asyncio
    async def test_pg_position_key_filter_passed_through(self, pg_service, ctx):
        """position_key forwarded to SQL parameter slot 3."""
        await pg_service.GetAccountingEvents(_request(position_key="key-xyz"), ctx)
        args = pg_service._snapshot_fetch.call_args.args
        # args[0] = SQL, then strategy_id, deployment_id, position_key, event_type, since_ts, limit
        assert args[3] == "key-xyz"

    @pytest.mark.asyncio
    async def test_pg_event_type_filter_passed_through(self, pg_service, ctx):
        await pg_service.GetAccountingEvents(_request(event_type="REPAY"), ctx)
        args = pg_service._snapshot_fetch.call_args.args
        assert args[4] == "REPAY"

    @pytest.mark.asyncio
    async def test_pg_since_timestamp_passed_through(self, pg_service, ctx):
        await pg_service.GetAccountingEvents(_request(since_timestamp=999), ctx)
        args = pg_service._snapshot_fetch.call_args.args
        assert args[5] == 999

    @pytest.mark.asyncio
    async def test_pg_limit_zero_means_unbounded(self, pg_service, ctx):
        """limit=0 binds as 0; the SQL uses NULLIF($6, 0) to mean LIMIT NULL (unbounded)."""
        await pg_service.GetAccountingEvents(_request(limit=0), ctx)
        args = pg_service._snapshot_fetch.call_args.args
        assert args[6] == 0
        sql = args[0]
        assert "NULLIF($6, 0)" in sql

    @pytest.mark.asyncio
    async def test_pg_order_by_timestamp_asc(self, pg_service, ctx):
        """ORDER BY timestamp ASC is load-bearing for FIFO basis-store reconstruction."""
        await pg_service.GetAccountingEvents(_request(), ctx)
        sql = pg_service._snapshot_fetch.call_args.args[0]
        normalized = re.sub(r"\s+", " ", sql)
        assert "ORDER BY timestamp ASC" in normalized

    @pytest.mark.asyncio
    async def test_pg_payload_round_trip_preserves_bytes(self, pg_service, ctx):
        """payload_json::text from PG → wire bytes — exact byte equality preserved."""
        original = '{"USDC":{"price":"1.000001"},"_t":42}'
        pg_service._snapshot_fetch = AsyncMock(return_value=[_pg_row(payload_text=original)])

        response = await pg_service.GetAccountingEvents(_request(), ctx)

        assert response.events[0].payload_json == original.encode("utf-8")

    @pytest.mark.asyncio
    async def test_pg_resolves_agent_id_for_where_clause(self, pg_service, ctx, monkeypatch):
        """AGENT_ID env → SQL parameter slot 1 receives platform agent id."""
        monkeypatch.setenv("AGENT_ID", "platform-agent-uuid")
        await pg_service.GetAccountingEvents(_request(), ctx)
        args = pg_service._snapshot_fetch.call_args.args
        assert args[1] == "platform-agent-uuid"

    @pytest.mark.asyncio
    async def test_pg_exception_returns_empty_list_fail_quiet(self, pg_service, ctx):
        """Read-side fail-quiet: PG error → empty list, no INTERNAL status raised."""
        pg_service._snapshot_fetch = AsyncMock(side_effect=RuntimeError("pg down"))

        response = await pg_service.GetAccountingEvents(_request(), ctx)

        assert list(response.events) == []
        # Crucially: no INTERNAL status, no error response — fail-quiet.
        for call in ctx.set_code.call_args_list:
            assert call.args[0] != grpc.StatusCode.INTERNAL


# ---------------------------------------------------------------------------
# SQLite branch
# ---------------------------------------------------------------------------


class TestGetAccountingEventsSqliteBranch:
    """Local-mode delegates to the warm backend's get_accounting_events_sync."""

    @pytest.mark.asyncio
    async def test_sqlite_warm_missing_method_returns_empty(self, sqlite_service, ctx):
        """No warm backend or backend missing the method → empty list, no error."""
        sqlite_service._state_manager = MagicMock()
        sqlite_service._state_manager.warm_backend = None

        response = await sqlite_service.GetAccountingEvents(_request(), ctx)

        assert list(response.events) == []

    @pytest.mark.asyncio
    async def test_sqlite_warm_present_returns_events(self, sqlite_service, ctx):
        """Warm backend's sync primitive is called and rows are converted to proto."""
        warm = MagicMock()
        warm.get_accounting_events_sync = MagicMock(return_value=[_sqlite_row(id="aaa")])
        sqlite_service._state_manager = MagicMock()
        sqlite_service._state_manager.warm_backend = warm

        response = await sqlite_service.GetAccountingEvents(_request(position_key="aave-usdc"), ctx)

        assert len(response.events) == 1
        assert response.events[0].id == "aaa"
        assert response.events[0].strategy_id == _VALID_STRATEGY
        warm.get_accounting_events_sync.assert_called_once_with(
            deployment_id=_DEPLOYMENT,
            position_key="aave-usdc",
        )

    @pytest.mark.asyncio
    async def test_sqlite_python_side_filters_match_pg_semantics(self, sqlite_service, ctx):
        """event_type / since_timestamp / limit applied in Python so SQLite parity
        matches the PG branch even though the SQLite primitive only accepts
        deployment_id + position_key.
        """
        rows = [
            _sqlite_row(id="a", event_type="SUPPLY", timestamp="2024-01-01T00:00:00+00:00"),
            _sqlite_row(id="b", event_type="BORROW", timestamp="2024-02-01T00:00:00+00:00"),
            _sqlite_row(id="c", event_type="REPAY", timestamp="2024-03-01T00:00:00+00:00"),
            _sqlite_row(id="d", event_type="REPAY", timestamp="2024-04-01T00:00:00+00:00"),
        ]
        warm = MagicMock()
        warm.get_accounting_events_sync = MagicMock(return_value=rows)
        sqlite_service._state_manager = MagicMock()
        sqlite_service._state_manager.warm_backend = warm

        feb_epoch = int(datetime(2024, 2, 1, tzinfo=UTC).timestamp())
        response = await sqlite_service.GetAccountingEvents(
            _request(event_type="REPAY", since_timestamp=feb_epoch, limit=10),
            ctx,
        )

        # event_type=REPAY filters to c, d. since_timestamp keeps both. limit=10 doesn't trim.
        ids = [e.id for e in response.events]
        assert ids == ["c", "d"]


# ---------------------------------------------------------------------------
# End-to-end "restart simulation" round-trip
# ---------------------------------------------------------------------------


class TestSaveAndGetAccountingEventsRoundTrip:
    """VIB-3503 Part 3 verification: prove the contract holds across the wire
    boundary -- write 3 SaveAccountingEvent calls, fresh ``StateServiceServicer``
    (simulating gateway restart) reads them back via GetAccountingEvents,
    LendingFIFOBasisStore.reconstruct_from_events rebuilds open lots correctly.

    This is the smallest test that catches the regression scenario the
    ticket flags: "PnL resets to zero after every crash" if events written
    pre-restart can't be read post-restart.
    """

    @pytest.mark.asyncio
    async def test_round_trip_with_fifo_replay(self, ctx) -> None:
        # ----- Stage 1: writer service captures 3 INSERT calls -----
        import json as _json
        from almanak.framework.accounting.ids import make_accounting_event_id

        writer = StateServiceServicer(GatewaySettings())
        writer._initialized = True
        writer._snapshot_pool_initialized = True
        writer._snapshot_pool = MagicMock()
        captured_inserts: list[tuple] = []

        async def _capture(*args, **kwargs):
            captured_inserts.append(args)
            return "INSERT 0 1"

        writer._snapshot_execute = AsyncMock(side_effect=_capture)
        writer._ensure_initialized = AsyncMock()
        writer._ensure_snapshot_pool = AsyncMock()

        deployment_id = "deploy-restart"
        position_key = "lending:arbitrum:aave_v3:0xwallet:dai"

        # 1 BORROW followed by 2 REPAYs that partially close the lot.
        # FIFO replay should leave a positive remaining principal after
        # the two repays (BORROW 1000 - REPAY 200 - REPAY 300 = 500).
        events = [
            (
                "BORROW",
                _json.dumps({"asset": "DAI", "amount_token": "1000"}),
                1_712_000_000,
            ),
            (
                "REPAY",
                _json.dumps({"asset": "DAI", "amount_token": "200"}),
                1_712_000_100,
            ),
            (
                "REPAY",
                _json.dumps({"asset": "DAI", "amount_token": "300"}),
                1_712_000_200,
            ),
        ]

        for evt_type, payload, ts in events:
            event_id = make_accounting_event_id(
                deployment_id=deployment_id,
                cycle_id=f"cyc-{ts}",
                intent_type=evt_type,
                tx_hash=f"0x{ts:x}",
                position_key=position_key,
            )
            req = gateway_pb2.SaveAccountingEventRequest(
                id=event_id,
                deployment_id=deployment_id,
                strategy_id="strat-1",
                cycle_id=f"cyc-{ts}",
                execution_mode="live",
                timestamp=ts,
                chain="arbitrum",
                protocol="aave_v3",
                wallet_address="0xwallet",
                tx_hash=f"0x{ts:x}",
                ledger_entry_id="11111111-1111-1111-1111-111111111111",
                event_type=evt_type,
                position_key=position_key,
                confidence="HIGH",
                payload_json=payload.encode("utf-8"),
                schema_version=1,
            )
            response = await writer.SaveAccountingEvent(req, ctx)
            assert response.success is True

        assert len(captured_inserts) == 3

        # ----- Stage 2: fresh service simulates gateway restart, reads events back -----
        reader = StateServiceServicer(GatewaySettings())
        reader._initialized = True
        reader._snapshot_pool_initialized = True
        reader._snapshot_pool = MagicMock()
        reader._ensure_initialized = AsyncMock()
        reader._ensure_snapshot_pool = AsyncMock()

        # Build the rows that asyncpg would return -- one per captured INSERT --
        # in timestamp ASC order (ORDER BY timestamp ASC is load-bearing for FIFO).
        from datetime import UTC, datetime as _dt

        def _row_from_insert(insert_args: tuple) -> dict:
            # Positional args after SQL match the INSERT column list:
            # event_id, deployment_id, agent_id, cycle_id, execution_mode,
            # ts(datetime), chain, protocol, wallet_address, event_type,
            # position_key, ledger_entry_id, tx_hash, confidence,
            # payload_str, schema_version.
            ts_dt: _dt = insert_args[6]
            return {
                "id": insert_args[1],
                "deployment_id": insert_args[2],
                "agent_id": insert_args[3],
                "cycle_id": insert_args[4],
                "execution_mode": insert_args[5],
                "ts_epoch": int(ts_dt.replace(tzinfo=UTC).timestamp()),
                "chain": insert_args[7],
                "protocol": insert_args[8],
                "wallet_address": insert_args[9],
                "event_type": insert_args[10],
                "position_key": insert_args[11],
                "ledger_entry_id": insert_args[12],
                "tx_hash": insert_args[13],
                "confidence": insert_args[14],
                "payload_text": insert_args[15],
                "schema_version": insert_args[16],
            }

        pg_rows = sorted([_row_from_insert(a) for a in captured_inserts], key=lambda r: r["ts_epoch"])
        reader._snapshot_fetch = AsyncMock(return_value=pg_rows)

        # Read with the same agent/strategy id that wrote the rows. Sending a
        # different strategy_id here would still pass because _snapshot_fetch
        # is mocked to return rows unconditionally, but it would mask
        # regressions in the load-bearing `agent_id = $1` WHERE clause.
        get_req = gateway_pb2.GetAccountingEventsRequest(
            strategy_id="strat-1",
            deployment_id=deployment_id,
        )
        get_resp = await reader.GetAccountingEvents(get_req, ctx)

        assert len(get_resp.events) == 3
        # Confirm the WHERE clause was bound with the resolved agent_id from
        # the request, so a future regression that drops agent_id filtering
        # would surface here even with the mocked fetch.
        fetch_args = reader._snapshot_fetch.call_args.args
        assert fetch_args[1] == "strat-1", "agent_id WHERE clause must be bound from the request"
        # ASC order is required by FIFO replay so BORROW arrives before REPAYs.
        assert [e.event_type for e in get_resp.events] == ["BORROW", "REPAY", "REPAY"]
        assert get_resp.events[0].timestamp < get_resp.events[1].timestamp < get_resp.events[2].timestamp

        # ----- Stage 3: rebuild FIFO basis store from the fetched events -----
        from almanak.framework.accounting.basis import FIFOBasisStore
        from almanak.framework.state.gateway_state_manager import _proto_event_to_dict

        store = FIFOBasisStore()
        replayed = store.reconstruct_from_events([_proto_event_to_dict(e) for e in get_resp.events])

        # 1 BORROW + 2 REPAY events should all replay successfully.
        assert replayed == 3
        # The lot store key for lending uses (deployment_id, position_key, token.lower());
        # 500 DAI remains open after BORROW(1000) − REPAY(200) − REPAY(300).
        lot_key = f"{deployment_id}:{position_key}:dai"
        remaining_lots = store._lots.get(lot_key, [])
        remaining_principal = sum(
            (lot.get("remaining", lot.get("principal", 0)) for lot in remaining_lots),
            Decimal("0") if False else 0,  # int OK; positive integer expected
        )
        from decimal import Decimal as _D
        assert _D(str(remaining_principal)) == _D("500"), (
            f"FIFO replay: expected $500 DAI remaining after BORROW 1000 − REPAY 200 − REPAY 300, "
            f"got {remaining_principal}"
        )
