"""VIB-5416 — StateService.GetLedgerEntriesMeasured + GatewayStateManager client.

Mirrors the GetAccountingEvents measured-read contract (VIB-5185): the teardown
swap-back clamp's NO_ACCOUNTING ledger lane MUST distinguish a MEASURED-empty read
(AVAILABLE) from an UNMEASURED one (ABSENT / ERRORED / UNSPECIFIED) so it fails in
the safe under-sweep direction.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import grpc
import pytest

from almanak.framework.observability.ledger import LedgerEntry
from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.state_service import StateServiceServicer

_DEPLOYMENT_ID = "deploy-abc123"


def _make_servicer(entries=None, raise_exc=None, has_method=True) -> StateServiceServicer:
    servicer = StateServiceServicer(GatewaySettings(db_path=":memory:"))
    servicer._initialized = True
    # The handler reads the WARM BACKEND directly (not the StateManager facade,
    # which collapses absent/errored into []) so ABSENT/ERRORED are distinguishable.
    state_manager = MagicMock()
    warm = MagicMock()
    if not has_method:
        del warm.get_ledger_entries
    elif raise_exc is not None:
        warm.get_ledger_entries = AsyncMock(side_effect=raise_exc)
    else:
        warm.get_ledger_entries = AsyncMock(return_value=entries or [])
    state_manager.warm_backend = warm
    servicer._state_manager = state_manager
    return servicer


def _make_context() -> MagicMock:
    ctx = MagicMock(spec=grpc.aio.ServicerContext)
    ctx.set_code = MagicMock()
    ctx.set_details = MagicMock()
    return ctx


def _stake_entry() -> LedgerEntry:
    return LedgerEntry(
        id="led-1",
        deployment_id=_DEPLOYMENT_ID,
        timestamp=datetime(2026, 6, 25, tzinfo=UTC),
        intent_type="STAKE",
        token_in="ETH",
        amount_in="1.0",
        token_out="wstETH",
        amount_out="0.88",
        chain="ethereum",
        success=True,
    )


@pytest.mark.asyncio
async def test_missing_deployment_id_is_errored():
    servicer = _make_servicer()
    ctx = _make_context()
    req = gateway_pb2.GetLedgerEntriesMeasuredRequest(deployment_id="")
    resp = await servicer.GetLedgerEntriesMeasured(req, ctx)
    ctx.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)
    assert resp.backend_status == gateway_pb2.ACCOUNTING_BACKEND_STATUS_ERRORED


@pytest.mark.asyncio
async def test_available_on_success_with_rows():
    servicer = _make_servicer(entries=[_stake_entry()])
    ctx = _make_context()
    req = gateway_pb2.GetLedgerEntriesMeasuredRequest(deployment_id=_DEPLOYMENT_ID)
    resp = await servicer.GetLedgerEntriesMeasured(req, ctx)
    ctx.set_code.assert_not_called()
    assert resp.backend_status == gateway_pb2.ACCOUNTING_BACKEND_STATUS_AVAILABLE
    assert len(resp.entries) == 1
    e = resp.entries[0]
    assert e.intent_type == "STAKE"
    assert e.token_out == "wstETH"
    assert e.amount_out == "0.88"
    assert isinstance(e.timestamp, int) and e.timestamp > 0


@pytest.mark.asyncio
async def test_available_measured_empty_is_real_zero():
    servicer = _make_servicer(entries=[])
    ctx = _make_context()
    req = gateway_pb2.GetLedgerEntriesMeasuredRequest(deployment_id=_DEPLOYMENT_ID)
    resp = await servicer.GetLedgerEntriesMeasured(req, ctx)
    assert resp.backend_status == gateway_pb2.ACCOUNTING_BACKEND_STATUS_AVAILABLE
    assert len(resp.entries) == 0


@pytest.mark.asyncio
async def test_absent_when_backend_cannot_serve_ledger():
    servicer = _make_servicer(has_method=False)
    ctx = _make_context()
    req = gateway_pb2.GetLedgerEntriesMeasuredRequest(deployment_id=_DEPLOYMENT_ID)
    resp = await servicer.GetLedgerEntriesMeasured(req, ctx)
    assert resp.backend_status == gateway_pb2.ACCOUNTING_BACKEND_STATUS_ABSENT
    assert len(resp.entries) == 0


@pytest.mark.asyncio
async def test_errored_on_backend_exception():
    servicer = _make_servicer(raise_exc=RuntimeError("relation does not exist"))
    ctx = _make_context()
    req = gateway_pb2.GetLedgerEntriesMeasuredRequest(deployment_id=_DEPLOYMENT_ID)
    resp = await servicer.GetLedgerEntriesMeasured(req, ctx)
    assert resp.backend_status == gateway_pb2.ACCOUNTING_BACKEND_STATUS_ERRORED
    assert len(resp.entries) == 0


@pytest.mark.asyncio
async def test_limit_zero_means_full_history_not_zero_rows():
    servicer = _make_servicer(entries=[_stake_entry()])
    ctx = _make_context()
    req = gateway_pb2.GetLedgerEntriesMeasuredRequest(deployment_id=_DEPLOYMENT_ID, limit=0)
    await servicer.GetLedgerEntriesMeasured(req, ctx)
    call = servicer._state_manager.warm_backend.get_ledger_entries.call_args
    # limit=0 must NOT be forwarded as a literal 0 (SQLite LIMIT 0 = zero rows).
    assert call.kwargs.get("limit", 0) > 0


@pytest.mark.asyncio
async def test_entries_returned_in_deterministic_chronological_order():
    # VIB-5416: the backend reads ORDER BY timestamp DESC, but the clamp needs
    # chronological order so a same-block NO_ACCOUNTING disposal never replays
    # before its acquisition. The handler must re-sort ascending by (timestamp, id).
    def _entry(_id, dt):
        return LedgerEntry(
            id=_id, deployment_id=_DEPLOYMENT_ID, timestamp=dt, intent_type="STAKE",
            token_in="ETH", amount_in="1.0", token_out="wstETH", amount_out="0.1", chain="ethereum", success=True,
        )

    # Backend returns newest-first (DESC) + a same-timestamp pair out of id order.
    t0 = datetime(2026, 6, 25, 0, 0, 0, tzinfo=UTC)
    t1 = datetime(2026, 6, 25, 0, 0, 1, tzinfo=UTC)
    backend_order = [_entry("b", t1), _entry("z", t0), _entry("a", t0)]
    servicer = _make_servicer(entries=backend_order)
    ctx = _make_context()
    req = gateway_pb2.GetLedgerEntriesMeasuredRequest(deployment_id=_DEPLOYMENT_ID)
    resp = await servicer.GetLedgerEntriesMeasured(req, ctx)
    # Ascending by (timestamp, id): t0/a, t0/z, t1/b
    assert [e.id for e in resp.entries] == ["a", "z", "b"]


@pytest.mark.asyncio
async def test_out_of_range_since_timestamp_is_invalid_argument():
    # A huge epoch overflows datetime.fromtimestamp — must be rejected at the
    # boundary, not escape as an unhandled OverflowError/OSError.
    servicer = _make_servicer(entries=[])
    ctx = _make_context()
    req = gateway_pb2.GetLedgerEntriesMeasuredRequest(deployment_id=_DEPLOYMENT_ID, since_timestamp=10**18)
    resp = await servicer.GetLedgerEntriesMeasured(req, ctx)
    ctx.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)
    assert resp.backend_status == gateway_pb2.ACCOUNTING_BACKEND_STATUS_ERRORED


# ---------------------------------------------------------------------------
# GatewayStateManager.read_ledger_entries_measured (client)
# ---------------------------------------------------------------------------


def _make_gsm(status, entries=None, raise_exc=None):
    from almanak.framework.state.gateway_state_manager import GatewayStateManager

    mock_client = MagicMock()
    if raise_exc is not None:
        mock_client.state.GetLedgerEntriesMeasured = MagicMock(side_effect=raise_exc)
    else:
        resp = gateway_pb2.GetLedgerEntriesMeasuredResponse(entries=entries or [], backend_status=status)
        mock_client.state.GetLedgerEntriesMeasured = MagicMock(return_value=resp)
    return GatewayStateManager(client=mock_client)


def test_client_available_with_rows_is_measured():
    entry = gateway_pb2.LedgerEntryInfo(
        id="led-1",
        deployment_id=_DEPLOYMENT_ID,
        intent_type="STAKE",
        token_in="ETH",
        amount_in="1.0",
        token_out="wstETH",
        amount_out="0.88",
        chain="ethereum",
        success=True,
    )
    gsm = _make_gsm(gateway_pb2.ACCOUNTING_BACKEND_STATUS_AVAILABLE, entries=[entry])
    rows, measured = gsm.read_ledger_entries_measured(_DEPLOYMENT_ID)
    assert measured is True
    assert rows[0]["intent_type"] == "STAKE" and rows[0]["token_out"] == "wstETH"


def test_client_projection_carries_tx_hash():
    """VIB-5866: ``tx_hash`` must survive the wire→dict projection.

    The projection was written for the teardown clamp, which does not read
    ``tx_hash``, so the field was dropped while the servicer had always
    populated it. The capital-flow producer keys its own-transaction
    exclusion on exactly this field: with it missing, a gateway-managed run
    scans its own trades as unclassified external flows and self-poisons the
    era (found live in real-fork proof run 3, invisible to every fake that
    served attribute-style rows).
    """
    entry = gateway_pb2.LedgerEntryInfo(
        id="led-1",
        deployment_id=_DEPLOYMENT_ID,
        intent_type="SUPPLY",
        chain="arbitrum",
        tx_hash="0x160a765a",
        success=True,
    )
    gsm = _make_gsm(gateway_pb2.ACCOUNTING_BACKEND_STATUS_AVAILABLE, entries=[entry])
    rows, measured = gsm.read_ledger_entries_measured(_DEPLOYMENT_ID)
    assert measured is True
    assert "tx_hash" in rows[0], "projection must not drop tx_hash"
    assert rows[0]["tx_hash"] == "0x160a765a"


def test_client_absent_is_unmeasured():
    gsm = _make_gsm(gateway_pb2.ACCOUNTING_BACKEND_STATUS_ABSENT)
    rows, measured = gsm.read_ledger_entries_measured(_DEPLOYMENT_ID)
    assert rows == [] and measured is False


def test_client_errored_is_unmeasured():
    gsm = _make_gsm(gateway_pb2.ACCOUNTING_BACKEND_STATUS_ERRORED)
    _rows, measured = gsm.read_ledger_entries_measured(_DEPLOYMENT_ID)
    assert measured is False


def test_client_old_gateway_unspecified_is_unmeasured():
    gsm = _make_gsm(gateway_pb2.ACCOUNTING_BACKEND_STATUS_UNSPECIFIED)
    _rows, measured = gsm.read_ledger_entries_measured(_DEPLOYMENT_ID)
    assert measured is False


def test_client_rpc_exception_is_unmeasured():
    gsm = _make_gsm(None, raise_exc=RuntimeError("rpc gone"))
    rows, measured = gsm.read_ledger_entries_measured(_DEPLOYMENT_ID)
    assert rows == [] and measured is False


def test_client_empty_deployment_id_is_unmeasured():
    gsm = _make_gsm(gateway_pb2.ACCOUNTING_BACKEND_STATUS_AVAILABLE)
    rows, measured = gsm.read_ledger_entries_measured("")
    assert rows == [] and measured is False
