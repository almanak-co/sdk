"""Teardown closure of an Aster Pro position is measured at the venue, never assumed."""

from __future__ import annotations

from types import SimpleNamespace

import grpc
import pytest

from almanak.connectors._strategy_base.venue_account_read_base import VENUE_ACCOUNT_VALUATION_SOURCE
from almanak.connectors.aster_perps.proto import aster_perps_pb2
from almanak.connectors.aster_perps.teardown_post_condition import aster_perps_teardown_post_condition
from almanak.framework.teardown.models import PositionInfo, PositionType

WALLET = "0x" + "ab" * 20


class _Stub:
    def __init__(self, positions: list | Exception) -> None:
        self.positions = positions

    def GetPositions(self, request, timeout):  # noqa: N802 — gRPC method name
        if isinstance(self.positions, Exception):
            raise self.positions
        return aster_perps_pb2.AsterPositionsResponse(success=True, positions=self.positions)


class _Unavailable(grpc.RpcError):
    def details(self) -> str:
        return "unavailable"


def _gateway(positions: list | Exception) -> SimpleNamespace:
    stub = _Stub(positions)
    return SimpleNamespace(connector_stub=lambda name: stub)


def _position(**details: str) -> PositionInfo:
    return PositionInfo(
        position_type=PositionType.PERP,
        position_id="aster:SOL/USD",
        chain="bsc",
        protocol="aster_perps",
        value_usd=0,
        details={"market": "SOL/USD", **details},
    )


def _held(amount: str) -> aster_perps_pb2.AsterPosition:
    return aster_perps_pb2.AsterPosition(symbol="SOLUSDT", position_amt=amount, unrealized_pnl="0")


def test_a_flat_venue_position_is_a_measured_close() -> None:
    result = aster_perps_teardown_post_condition(_position(), WALLET, _gateway([]))
    assert result.closed and not result.unmeasured and not result.not_applicable


def test_a_residual_at_the_venue_is_a_measured_residual() -> None:
    result = aster_perps_teardown_post_condition(_position(), WALLET, _gateway([_held("-0.02")]))
    assert not result.closed and not result.unmeasured
    assert result.residual == {"symbol": "SOLUSDT", "position_amt": "-0.02"}


def test_an_unreadable_venue_is_unmeasured_never_closed() -> None:
    result = aster_perps_teardown_post_condition(_position(), WALLET, _gateway(_Unavailable()))
    assert result.unmeasured and not result.closed


def test_the_venue_account_cash_row_is_out_of_scope() -> None:
    result = aster_perps_teardown_post_condition(_position(valuation_source="venue_account"), WALLET, _gateway([]))
    assert result.not_applicable


async def _plan_a(position: PositionInfo, gateway: object) -> object:
    from almanak.framework.teardown.plan_a_reconciliation import _reconcile_one

    verdict, _ = await _reconcile_one(
        position=position, gateway_client=gateway, market=None, network="mainnet", wallet_address=WALLET
    )
    return verdict


@pytest.mark.asyncio
async def test_teardown_reconciliation_measures_an_aster_position_open_closed_or_unread() -> None:
    """Without this, a teardown that closed an Aster perp is downgraded to UNVERIFIED (Phase 4 D2.M4)."""
    from almanak.framework.teardown.plan_a_reconciliation import ReconciliationVerdict

    assert await _plan_a(_position(), _gateway([_held("-0.02")])) is ReconciliationVerdict.CONFIRMED_OPEN
    assert await _plan_a(_position(), _gateway([])) is ReconciliationVerdict.DIVERGED_CLOSED
    assert await _plan_a(_position(), _gateway(_Unavailable())) is ReconciliationVerdict.UNVERIFIABLE


@pytest.mark.asyncio
async def test_teardown_reconciliation_never_reads_the_venue_cash_row_as_a_closed_position() -> None:
    from almanak.framework.teardown.plan_a_reconciliation import ReconciliationVerdict

    cash = _position(valuation_source=VENUE_ACCOUNT_VALUATION_SOURCE)
    assert await _plan_a(cash, _gateway([])) is ReconciliationVerdict.NOT_APPLICABLE
