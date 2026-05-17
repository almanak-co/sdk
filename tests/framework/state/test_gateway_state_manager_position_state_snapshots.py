"""Unit tests for ``GatewayStateManager.save_position_state_snapshots`` (VIB-4541).

These tests lock the client-side contract of the Track-C wiring half:

1. Closes the capability gate at runner_state.py:480 — ``hasattr(state_manager,
   "save_position_state_snapshots")`` is now ``True`` on the production
   state manager. Without this method the wiring quietly returns 0 and
   no ``position_state_snapshots`` rows ever land.
2. ``Empty != Zero`` serialisation (CLAUDE.md §Accounting) — a ``None``
   ``PositionStateRow`` field must NOT set the proto field; a ``Decimal("0")``
   field MUST set it to ``"0"``. The wire-shape test asserts the proto
   request the gateway sees so the contract is locked at the boundary.
3. Hosted-Postgres degradation — gRPC ``UNIMPLEMENTED`` from the server
   (pre-metrics-database migration, PRD T-DRAFT-25) maps to a silent
   ``return 0`` so the runner's live-mode handler at runner_state.py:565
   does NOT escalate it to ``AccountingPersistenceError``. Other
   ``RpcError`` codes (UNAVAILABLE, INTERNAL, etc.) propagate untouched
   so the runner CAN halt on a real backend regression.
4. ``response.success == False`` is logged + returns 0 (matches the
   ``SavePositionEvent`` pattern at line 904 of gateway_state_manager.py).
5. Empty rows short-circuits without an RPC call.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock

import grpc
import pytest

from almanak.framework.accounting.position_state import PositionStateRow
from almanak.framework.state.gateway_state_manager import GatewayStateManager
from almanak.gateway.proto import gateway_pb2


def _run(coro):
    return asyncio.run(coro)


def _client_with_state_stub() -> tuple[MagicMock, MagicMock]:
    """Build a fake ``GatewayClient`` whose ``.state`` attribute is a stub
    over the gRPC State service. Returns ``(client, state_stub)`` so the
    caller can configure ``state_stub.SavePositionStateSnapshots``."""
    client = MagicMock()
    state = MagicMock()
    client.state = state
    return client, state


def _gsm() -> tuple[GatewayStateManager, MagicMock, MagicMock]:
    client, state = _client_with_state_stub()
    gsm = GatewayStateManager(client, timeout=1.0)
    return gsm, client, state


def _row(**overrides) -> PositionStateRow:
    defaults = dict(
        snapshot_id=None,
        strategy_id="strat-1",
        deployment_id="deploy-1",
        cycle_id="cycle-1",
        timestamp=datetime(2026, 5, 17, 12, 0, tzinfo=UTC),
        position_id="pos-1",
        position_type="LENDING",
        value_confidence="HIGH",
        schema_version=1,
        formula_version=1,
        matching_policy_version=1,
    )
    defaults.update(overrides)
    return PositionStateRow(**defaults)


# ---------------------------------------------------------------------------
# Capability gate
# ---------------------------------------------------------------------------


def test_gsm_exposes_save_position_state_snapshots():
    """The runner's deployment-time capability gate at runner_state.py:480
    checks ``hasattr(state_manager, "save_position_state_snapshots")``.
    A regression here would re-open the silent-zero behaviour that VIB-4541
    closed."""
    gsm, _, _ = _gsm()
    assert hasattr(gsm, "save_position_state_snapshots")
    assert callable(gsm.save_position_state_snapshots)


# ---------------------------------------------------------------------------
# Happy path + short-circuits
# ---------------------------------------------------------------------------


def test_empty_rows_returns_zero_without_rpc():
    """Empty rows must skip the RPC entirely — measured zero, no wire
    traffic. Mirrors sqlite.py:2708's short-circuit so the two paths are
    observationally identical."""
    gsm, _, state = _gsm()
    result = _run(gsm.save_position_state_snapshots(42, []))
    assert result == 0
    state.SavePositionStateSnapshots.assert_not_called()


def test_successful_rpc_returns_rows_written():
    gsm, _, state = _gsm()
    state.SavePositionStateSnapshots.return_value = gateway_pb2.SavePositionStateSnapshotsResponse(
        success=True, rows_written=2
    )
    rows = [_row(), _row(position_id="pos-2")]
    result = _run(gsm.save_position_state_snapshots(42, rows))
    assert result == 2
    state.SavePositionStateSnapshots.assert_called_once()
    req, _kwargs = state.SavePositionStateSnapshots.call_args.args, state.SavePositionStateSnapshots.call_args.kwargs
    assert req[0].snapshot_id == 42
    assert len(req[0].rows) == 2


def test_response_success_false_returns_zero():
    """``success=False`` is a backend-side failure — the client logs and
    returns 0 (matches SavePositionEvent at line 904 of gateway_state_manager.py).
    A regression would conflate it with a measured zero and inflate the
    cell coverage."""
    gsm, _, state = _gsm()
    state.SavePositionStateSnapshots.return_value = gateway_pb2.SavePositionStateSnapshotsResponse(
        success=False, error="some validation rejected the request"
    )
    result = _run(gsm.save_position_state_snapshots(42, [_row()]))
    assert result == 0


# ---------------------------------------------------------------------------
# Hosted-Postgres degradation
# ---------------------------------------------------------------------------


class _FakeRpcError(grpc.RpcError):
    def __init__(self, code: grpc.StatusCode) -> None:
        super().__init__()
        self._code = code

    def code(self) -> grpc.StatusCode:
        return self._code


def test_unimplemented_maps_to_silent_zero():
    """The hosted PG warm backend doesn't expose
    ``save_position_state_snapshots`` until the metrics-database migration
    lands (PRD T-DRAFT-25). The server returns UNIMPLEMENTED; the client
    MUST translate to ``return 0`` so the runner's live-mode handler at
    runner_state.py:565 does NOT escalate it to
    ``AccountingPersistenceError`` and halt the strategy."""
    gsm, _, state = _gsm()
    state.SavePositionStateSnapshots.side_effect = _FakeRpcError(grpc.StatusCode.UNIMPLEMENTED)
    result = _run(gsm.save_position_state_snapshots(42, [_row()]))
    assert result == 0


@pytest.mark.parametrize(
    "code",
    [
        grpc.StatusCode.UNAVAILABLE,
        grpc.StatusCode.INTERNAL,
        grpc.StatusCode.DEADLINE_EXCEEDED,
    ],
)
def test_other_rpc_errors_propagate(code):
    """Non-UNIMPLEMENTED gRPC errors are real backend regressions — the
    runner's live-mode handler MUST see them so it can flip to
    ACCOUNTING_FAILED rather than masking a halt-worthy fault as 'no
    rows to write'."""
    gsm, _, state = _gsm()
    state.SavePositionStateSnapshots.side_effect = _FakeRpcError(code)
    with pytest.raises(grpc.RpcError):
        _run(gsm.save_position_state_snapshots(42, [_row()]))


# ---------------------------------------------------------------------------
# Empty != Zero — wire shape
# ---------------------------------------------------------------------------


def test_none_fields_are_unset_on_wire():
    """A ``None`` PositionStateRow field MUST stay ``HasField()==False`` on
    the proto, otherwise the server will see ``Decimal("0")`` for an
    unmeasured value and the materialiser's cell scores will silently
    corrupt."""
    gsm, _, state = _gsm()
    state.SavePositionStateSnapshots.return_value = gateway_pb2.SavePositionStateSnapshotsResponse(
        success=True, rows_written=1
    )
    # LENDING row — supply_balance only; every other lending field None.
    row = _row(supply_balance=Decimal("100.5"))
    _run(gsm.save_position_state_snapshots(42, [row]))
    sent_req = state.SavePositionStateSnapshots.call_args.args[0]
    sent_row = sent_req.rows[0]
    assert sent_row.HasField("supply_balance")
    assert sent_row.supply_balance == "100.5"
    # Every other optional field must NOT be set on the wire.
    for fname in (
        "current_tick",
        "in_range",
        "liquidity",
        "sqrt_price_x96",
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
    ):
        assert not sent_row.HasField(fname), f"unmeasured field {fname!r} leaked onto wire"


def test_measured_zero_is_set_on_wire():
    """A ``Decimal("0")`` is a MEASURED ZERO and MUST hit the wire with
    ``HasField()==True``. The inverse of the None test — both halves of
    the Empty!=Zero invariant locked at the boundary."""
    gsm, _, state = _gsm()
    state.SavePositionStateSnapshots.return_value = gateway_pb2.SavePositionStateSnapshotsResponse(
        success=True, rows_written=1
    )
    row = _row(supply_balance=Decimal("0"), borrow_balance=Decimal("0"))
    _run(gsm.save_position_state_snapshots(42, [row]))
    sent_row = state.SavePositionStateSnapshots.call_args.args[0].rows[0]
    assert sent_row.HasField("supply_balance") and sent_row.supply_balance == "0"
    assert sent_row.HasField("borrow_balance") and sent_row.borrow_balance == "0"
    assert not sent_row.HasField("health_factor")  # unmeasured stays absent


def test_optional_int_and_bool_serialise_correctly():
    """``current_tick`` (int) and ``in_range`` (bool) are LP-only fields
    serialised as proto optional int64 / bool. Numeric 0 and False are
    valid measured values — they MUST set the field, NOT be conflated
    with unmeasured."""
    gsm, _, state = _gsm()
    state.SavePositionStateSnapshots.return_value = gateway_pb2.SavePositionStateSnapshotsResponse(
        success=True, rows_written=1
    )
    row = _row(position_type="LP", current_tick=0, in_range=False)
    _run(gsm.save_position_state_snapshots(42, [row]))
    sent_row = state.SavePositionStateSnapshots.call_args.args[0].rows[0]
    assert sent_row.HasField("current_tick") and sent_row.current_tick == 0
    assert sent_row.HasField("in_range") and sent_row.in_range is False


def test_captured_at_preserves_iso_8601_subsecond_precision():
    """The SQLite captured_at column is TEXT ISO-8601 (sqlite.py:560). The
    int64 epoch-seconds wire shape used by SavePositionEvent would discard
    sub-second precision — for Track-C row ordering at sub-second-interval
    iterations that matters. A regression that switches to int64 would
    show up here."""
    gsm, _, state = _gsm()
    state.SavePositionStateSnapshots.return_value = gateway_pb2.SavePositionStateSnapshotsResponse(
        success=True, rows_written=1
    )
    ts = datetime(2026, 5, 17, 12, 34, 56, 123456, tzinfo=UTC)
    row = _row(timestamp=ts)
    _run(gsm.save_position_state_snapshots(42, [row]))
    sent_row = state.SavePositionStateSnapshots.call_args.args[0].rows[0]
    assert sent_row.captured_at == ts.isoformat()
    assert ".123456" in sent_row.captured_at, "sub-second precision must survive the wire"
