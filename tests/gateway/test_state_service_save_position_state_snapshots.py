"""Tests for the SavePositionStateSnapshots gRPC endpoint (VIB-3891 / VIB-4541).

Covers the gateway's server-side handler responsible for routing Track-C
position-state rows from the runner through the warm backend's
``save_position_state_snapshots`` method. Mirrors the
``SavePositionEvent`` test shape (boundary validation, warm-backend
capability gate, delegation paths, exception mapping).

Why these tests exist: pre-VIB-4541 the runner's
``_persist_position_state_snapshots`` (runner_state.py:435) saw
``hasattr(state_manager, "save_position_state_snapshots") == False`` on
``GatewayStateManager`` and returned 0 silently, so every SDK run wrote
zero ``position_state_snapshots`` rows and accountant cells G14 / G15 /
L2 / L3 / L5 had nothing to score against. The RPC + handler close that
gap end-to-end for the local SQLite warm backend; a future
metrics-database migration unblocks the hosted Postgres backend (PRD
T-DRAFT-25).
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import grpc
import pytest

from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.state_service import StateServiceServicer

_VALID_STRATEGY = "test-strategy"
_VALID_DEPLOYMENT = "deploy-1"
_VALID_CYCLE = "cycle-1"
_VALID_CAPTURED_AT = "2026-05-17T12:00:00+00:00"
_VALID_POSITION_ID = "pos-1"


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


def _row(**overrides) -> gateway_pb2.PositionStateSnapshotRow:
    defaults = dict(
        strategy_id=_VALID_STRATEGY,
        deployment_id=_VALID_DEPLOYMENT,
        cycle_id=_VALID_CYCLE,
        captured_at=_VALID_CAPTURED_AT,
        position_id=_VALID_POSITION_ID,
        position_type="LENDING",
        value_confidence="HIGH",
        schema_version=1,
        formula_version=1,
        matching_policy_version=1,
    )
    defaults.update(overrides)
    return gateway_pb2.PositionStateSnapshotRow(**defaults)


def _request(*rows, snapshot_id: int = 42) -> gateway_pb2.SavePositionStateSnapshotsRequest:
    return gateway_pb2.SavePositionStateSnapshotsRequest(
        snapshot_id=snapshot_id,
        rows=list(rows) if rows else [_row()],
    )


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


class TestValidation:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("sid", [0, -1])
    async def test_non_positive_snapshot_id_rejected(self, service, ctx, sid):
        req = _request(snapshot_id=sid)
        resp = await service.SavePositionStateSnapshots(req, ctx)
        assert resp.success is False
        assert "snapshot_id" in resp.error
        ctx.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)

    @pytest.mark.asyncio
    async def test_empty_rows_is_success_with_zero_rows_written(self, service, ctx):
        """Empty rows is a measured zero per the Accountant Test contract —
        success=True keeps the runner-side semantics aligned with the local
        SQLite path (sqlite.py returns 0 for empty input)."""
        req = gateway_pb2.SavePositionStateSnapshotsRequest(snapshot_id=1, rows=[])
        resp = await service.SavePositionStateSnapshots(req, ctx)
        assert resp.success is True
        assert resp.rows_written == 0
        ctx.set_code.assert_not_called()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("blank", ["", "  ", "\t"])
    async def test_blank_per_row_strategy_id_rejected(self, service, ctx, blank):
        req = _request(_row(strategy_id=blank))
        resp = await service.SavePositionStateSnapshots(req, ctx)
        assert resp.success is False
        assert "strategy_id" in resp.error
        ctx.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("blank", ["", "  ", "\t"])
    async def test_blank_per_row_deployment_id_rejected(self, service, ctx, blank):
        req = _request(_row(deployment_id=blank))
        resp = await service.SavePositionStateSnapshots(req, ctx)
        assert resp.success is False
        assert "deployment_id" in resp.error
        ctx.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)

    @pytest.mark.asyncio
    async def test_unknown_position_type_rejected(self, service, ctx):
        req = _request(_row(position_type="STAKING"))
        resp = await service.SavePositionStateSnapshots(req, ctx)
        assert resp.success is False
        assert "position_type" in resp.error
        ctx.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("blank", ["", "  ", "\t"])
    async def test_missing_position_id_rejected(self, service, ctx, blank):
        # Whitespace-only position_id must reject — locks the CodeRabbit P3
        # finding that ``if not proto_row.position_id`` accepts "   " as
        # truthy. Identifier hygiene now strips before checking.
        req = _request(_row(position_id=blank))
        resp = await service.SavePositionStateSnapshots(req, ctx)
        assert resp.success is False
        assert "position_id" in resp.error
        ctx.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)

    @pytest.mark.asyncio
    async def test_malformed_captured_at_rejected(self, service, ctx):
        """An ISO-8601 parse failure must surface as INVALID_ARGUMENT rather
        than getting silently rewritten to None — the captured_at column is
        the only timestamp the cell scorers can correlate against."""
        service._state_manager = MagicMock()
        warm = AsyncMock()
        warm.save_position_state_snapshots = AsyncMock(return_value=1)
        service._state_manager.warm_backend = warm
        req = _request(_row(captured_at="not-a-date"))
        resp = await service.SavePositionStateSnapshots(req, ctx)
        assert resp.success is False
        assert "captured_at" in resp.error
        ctx.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)


# ---------------------------------------------------------------------------
# Capability gate + delegation
# ---------------------------------------------------------------------------


class TestDelegation:
    @pytest.mark.asyncio
    async def test_warm_backend_missing_method_returns_unimplemented(self, service, ctx):
        """Hosted Postgres pre-metrics-database migration shape — UNIMPLEMENTED
        is the correct degradation per PRD T-DRAFT-25 (the client translates
        it to a silent 0 so the cell stays XFAIL rather than the runner
        halting in live mode)."""
        service._state_manager = MagicMock()
        service._state_manager.warm_backend = MagicMock(spec=[])  # no method
        resp = await service.SavePositionStateSnapshots(_request(), ctx)
        assert resp.success is False
        ctx.set_code.assert_called_with(grpc.StatusCode.UNIMPLEMENTED)

    @pytest.mark.asyncio
    async def test_warm_backend_none_returns_unimplemented(self, service, ctx):
        service._state_manager = MagicMock()
        service._state_manager.warm_backend = None
        resp = await service.SavePositionStateSnapshots(_request(), ctx)
        assert resp.success is False
        ctx.set_code.assert_called_with(grpc.StatusCode.UNIMPLEMENTED)

    @pytest.mark.asyncio
    async def test_successful_delegation_returns_rows_written(self, service, ctx):
        warm = AsyncMock()
        warm.save_position_state_snapshots = AsyncMock(return_value=2)
        service._state_manager = MagicMock()
        service._state_manager.warm_backend = warm
        req = _request(_row(), _row(position_id="pos-2"))
        resp = await service.SavePositionStateSnapshots(req, ctx)
        assert resp.success is True
        assert resp.rows_written == 2
        ctx.set_code.assert_not_called()
        warm.save_position_state_snapshots.assert_awaited_once()
        kwargs = warm.save_position_state_snapshots.call_args.kwargs
        assert kwargs["snapshot_id"] == 42
        assert len(kwargs["rows"]) == 2

    @pytest.mark.asyncio
    async def test_backend_exception_returns_internal(self, service, ctx):
        warm = AsyncMock()
        warm.save_position_state_snapshots = AsyncMock(side_effect=RuntimeError("db down"))
        service._state_manager = MagicMock()
        service._state_manager.warm_backend = warm
        resp = await service.SavePositionStateSnapshots(_request(), ctx)
        assert resp.success is False
        assert "internal server error" in resp.error
        ctx.set_code.assert_called_with(grpc.StatusCode.INTERNAL)

    @pytest.mark.asyncio
    async def test_malformed_decimal_rejected_as_invalid_argument(self, service, ctx):
        """A present-but-non-numeric optional Decimal field MUST surface as
        INVALID_ARGUMENT — not INTERNAL via the catch-all (CodeRabbit P1,
        2026-05-17, "Gateway is the security boundary"). Pre-fix the bare
        ``Decimal(raw)`` would raise InvalidOperation and the outer
        ``except Exception`` would mask the malformed input as an internal
        error, hiding caller bugs."""
        warm = AsyncMock()
        warm.save_position_state_snapshots = AsyncMock(return_value=1)
        service._state_manager = MagicMock()
        service._state_manager.warm_backend = warm
        bad = _row()
        bad.supply_balance = "not-a-decimal"
        resp = await service.SavePositionStateSnapshots(_request(bad), ctx)
        assert resp.success is False
        assert "supply_balance" in resp.error
        assert "Decimal" in resp.error
        ctx.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)
        warm.save_position_state_snapshots.assert_not_called()

    @pytest.mark.asyncio
    async def test_malformed_int_rejected_as_invalid_argument(self, service, ctx):
        """Sister test for the int helper — malformed ``liquidity`` /
        ``sqrt_price_x96`` / ``current_tick`` strings must reject at the
        boundary rather than leaking InvalidOperation/ValueError → INTERNAL."""
        warm = AsyncMock()
        warm.save_position_state_snapshots = AsyncMock(return_value=1)
        service._state_manager = MagicMock()
        service._state_manager.warm_backend = warm
        bad = _row(position_type="LP")
        bad.liquidity = "definitely-not-an-int"
        resp = await service.SavePositionStateSnapshots(_request(bad), ctx)
        assert resp.success is False
        assert "liquidity" in resp.error
        ctx.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)
        warm.save_position_state_snapshots.assert_not_called()


# ---------------------------------------------------------------------------
# Empty != Zero — wire-shape contract
# ---------------------------------------------------------------------------


class TestEmptyNotZero:
    @pytest.mark.asyncio
    async def test_unmeasured_optional_fields_arrive_as_none_at_warm_backend(self, service, ctx):
        """Per CLAUDE.md §Accounting "Empty != Zero": fields not set on the
        wire (HasField==False) must reach the warm backend as ``None``, NOT
        as empty strings or ``Decimal("0")``. A regression here would let
        the materialiser conflate "unmeasured" with "measured zero" and
        silently corrupt the cell scores it backs."""
        captured: dict = {}

        async def _capture(*, snapshot_id, rows):
            captured["snapshot_id"] = snapshot_id
            captured["rows"] = rows
            return len(rows)

        warm = AsyncMock()
        warm.save_position_state_snapshots = AsyncMock(side_effect=_capture)
        service._state_manager = MagicMock()
        service._state_manager.warm_backend = warm

        row = _row(position_type="LENDING")
        # Set ONLY supply_balance — every other optional field is absent on
        # the wire. The server must translate absence → None on the typed
        # PositionStateRow, not "" / "0".
        row.supply_balance = "1234.5"
        resp = await service.SavePositionStateSnapshots(_request(row), ctx)
        assert resp.success is True
        assert len(captured["rows"]) == 1
        warm_row = captured["rows"][0]
        # The handler converts the wire string to typed Decimal before
        # building PositionStateRow (state_service.py:_opt_decimal). The
        # numeric value AND the type must both survive the boundary.
        assert warm_row.supply_balance == Decimal("1234.5")
        assert isinstance(warm_row.supply_balance, Decimal)
        # Every OTHER optional field must be None on the warm side — exhaustive
        # list so a regression on any of the 15 absentees trips CI (Claude
        # pr-auditor P3 follow-up). The client-side mirror test
        # ``test_none_fields_are_unset_on_wire`` covers the wire half; this
        # asserts the server-side deserialiser doesn't materialise empty
        # strings or Decimal("0") for HasField()==False fields.
        for fname in (
            "borrow_balance",
            "health_factor",
            "supply_apy_pct",
            "borrow_apy_pct",
            "interest_accrued_since_last",
            "mark_price",
            "unrealized_pnl",
            "funding_accrued_since_last",
            "liquidation_price",
            "margin_utilisation_pct",
            "delta_vs_protocol_pct",
            "current_tick",
            "in_range",
            "liquidity",
            "sqrt_price_x96",
        ):
            assert getattr(warm_row, fname) is None, f"{fname} leaked non-None on warm side"

    @pytest.mark.asyncio
    async def test_measured_zero_is_preserved(self, service, ctx):
        """A field explicitly set to "0" on the wire (HasField==True) must
        arrive at the warm backend as the literal ``"0"``, NOT None. This
        is the inverse of the unmeasured-fields test and locks both sides
        of the Empty!=Zero invariant."""
        captured: dict = {}

        async def _capture(*, snapshot_id, rows):
            captured["rows"] = rows
            return len(rows)

        warm = AsyncMock()
        warm.save_position_state_snapshots = AsyncMock(side_effect=_capture)
        service._state_manager = MagicMock()
        service._state_manager.warm_backend = warm

        row = _row(position_type="LENDING")
        row.supply_balance = "0"
        row.borrow_balance = "0"
        resp = await service.SavePositionStateSnapshots(_request(row), ctx)
        assert resp.success is True
        warm_row = captured["rows"][0]
        # Decimal("0") arrives as a measured zero — distinguishable from None
        # by both value AND type. Any regression that conflates Decimal("0")
        # with None (or strips type to bare int/float) breaks the cell scorers
        # that gate the runner's accounting halt path.
        assert warm_row.supply_balance == Decimal("0")
        assert warm_row.borrow_balance == Decimal("0")
        assert warm_row.health_factor is None  # unmeasured stays None
