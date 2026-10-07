"""Aster Pro account equity enters NAV as one venue-account row."""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch

import grpc
import pytest

from almanak.connectors._strategy_base.venue_account_read_base import (
    VENUE_ACCOUNT_VALUATION_SOURCE,
    VenueAccountRead,
)
from almanak.connectors._strategy_base.venue_account_read_registry import VenueAccountReadRegistry
from almanak.connectors.aster_perps.proto import aster_perps_pb2
from almanak.connectors.aster_perps.venue_account_read import read_aster_account
from almanak.framework.teardown.models import PositionInfo, PositionType
from almanak.framework.valuation.portfolio_valuer import PortfolioValuer
from almanak.framework.valuation.position_discovery import DiscoveryConfig, PositionDiscoveryService

WALLET = "0x" + "ab" * 20


class _Stub:
    def __init__(self, balances: list, positions: list, *, fail: bool = False, pending: list | None = None) -> None:
        self._balances, self._positions, self._fail, self._pending = balances, positions, fail, pending or []

    def GetBalances(self, request, timeout):  # noqa: N802 — gRPC method name
        if self._fail:
            raise _RpcError()
        return aster_perps_pb2.AsterBalancesResponse(
            success=True, balances=self._balances, pending_transfers=self._pending
        )

    def GetPositions(self, request, timeout):  # noqa: N802 — gRPC method name
        return aster_perps_pb2.AsterPositionsResponse(success=True, positions=self._positions)


class _RpcError(grpc.RpcError):
    def details(self) -> str:
        return "unavailable"


def _gateway(stub: _Stub) -> SimpleNamespace:
    return SimpleNamespace(connector_stub=lambda name: stub)


def _usdt(balance: str, upnl: str) -> aster_perps_pb2.AsterBalance:
    return aster_perps_pb2.AsterBalance(asset="USDT", balance=balance, available_balance=balance, cross_unrealized_pnl=upnl)


def test_equity_is_cash_plus_cross_unrealized_pnl() -> None:
    position = aster_perps_pb2.AsterPosition(
        symbol="ETHUSDT", position_amt="0.002", entry_price="2720", mark_price="2730", unrealized_pnl="0.02",
        leverage="5",
    )
    read = read_aster_account(
        gateway_client=_gateway(_Stub([_usdt("4.6", "0.02")], [position])), chain="bsc", wallet_address=WALLET
    )
    assert read.ok
    assert (read.cash_usd, read.unrealized_pnl_usd, read.equity_usd) == (
        Decimal("4.6"), Decimal("0.02"), Decimal("4.62"),
    )
    assert read.positions[0].market == "ETHUSDT" and read.positions[0].is_long


def test_empty_account_is_a_measured_empty() -> None:
    read = read_aster_account(gateway_client=_gateway(_Stub([], [])), chain="bsc", wallet_address=WALLET)
    assert read.ok and read.is_empty


def test_unpriced_asset_leaves_equity_unmeasured() -> None:
    bnb = aster_perps_pb2.AsterBalance(asset="BNB", balance="0.003", cross_unrealized_pnl="0")
    read = read_aster_account(gateway_client=_gateway(_Stub([_usdt("1", "0"), bnb], [])), chain="bsc", wallet_address=WALLET)
    assert not read.ok and read.equity_usd is None


def test_gateway_failure_is_unmeasured_not_empty() -> None:
    read = read_aster_account(gateway_client=_gateway(_Stub([], [], fail=True)), chain="bsc", wallet_address=WALLET)
    assert not read.ok and read.equity_usd is None


def _pending(kind: str, amount: str, fee: str = "") -> aster_perps_pb2.AsterPendingTransfer:
    return aster_perps_pb2.AsterPendingTransfer(type=kind, asset="USDT", amount=amount, fee=fee)


def test_uncredited_deposit_counts_toward_equity() -> None:
    read = read_aster_account(
        gateway_client=_gateway(_Stub([], [], pending=[_pending("DEPOSIT", "2.5")])), chain="bsc", wallet_address=WALLET
    )
    assert read.ok and read.equity_usd == Decimal("2.5") and not read.is_empty
    assert read.details["in_flight_usd"] == "2.5"


def test_unpaid_withdrawal_counts_net_of_the_venue_fee() -> None:
    read = read_aster_account(
        gateway_client=_gateway(_Stub([], [], pending=[_pending("WITHDRAW", "2.4940393", "0.11")])),
        chain="bsc",
        wallet_address=WALLET,
    )
    assert read.ok and read.equity_usd == Decimal("2.3840393")


def test_unpaid_withdrawal_without_a_fee_is_unmeasured() -> None:
    read = read_aster_account(
        gateway_client=_gateway(_Stub([], [], pending=[_pending("WITHDRAW", "2.49")])), chain="bsc", wallet_address=WALLET
    )
    assert not read.ok and read.equity_usd is None


def test_registry_scopes_reads_to_declared_protocols_on_the_account_chain() -> None:
    assert VenueAccountReadRegistry.protocols_to_read(["uniswap_v3", "aster_perps"], "bsc") == ["aster_perps"]
    assert VenueAccountReadRegistry.protocols_to_read(["aster_perps"], "arbitrum") == []


def _discover(read: VenueAccountRead):
    service = PositionDiscoveryService(gateway_client=object())
    config = DiscoveryConfig(chain="bsc", wallet_address=WALLET, protocols=["aster_perps"])
    with patch.object(VenueAccountReadRegistry, "read", return_value=read):
        return service.discover(config)


def test_discovery_emits_one_account_row_and_is_authoritative() -> None:
    result = _discover(VenueAccountRead(ok=True, equity_usd=Decimal("4.62"), cash_usd=Decimal("4.6"),
                                        unrealized_pnl_usd=Decimal("0.02")))
    assert "aster_perps" in result.perp_protocols_ok
    [row] = result.positions
    assert row.position_type == PositionType.PERP and row.value_usd == Decimal("4.62")
    assert row.details["valuation_source"] == VENUE_ACCOUNT_VALUATION_SOURCE


def test_discovery_of_an_empty_account_adds_no_row_but_drops_stubs() -> None:
    result = _discover(VenueAccountRead(ok=True, equity_usd=Decimal(0), cash_usd=Decimal(0),
                                        unrealized_pnl_usd=Decimal(0)))
    assert result.positions == [] and "aster_perps" in result.perp_protocols_ok


def test_failed_discovery_is_an_error_not_an_empty_account() -> None:
    result = _discover(VenueAccountRead(ok=False, error="boom"))
    assert "aster_perps" not in result.perp_protocols_ok
    assert any("aster_perps" in e for e in result.errors)
    [row] = result.positions
    assert row.details["venue_account_unread"] is True and row.details["unavailable_reason"] == "boom"


def test_an_unread_venue_account_is_unpriced_so_the_snapshot_is_unavailable() -> None:
    row = _discover(VenueAccountRead(ok=False, error="boom")).positions[0]
    valuer = PortfolioValuer.__new__(PortfolioValuer)
    valuer._perps_reader = SimpleNamespace(read_positions=lambda *a, **k: SimpleNamespace(positions=()))
    value, _details, repriced = valuer._reprice_perp_enriched(row, "bsc", market=None)
    assert (value, repriced) == (Decimal("0"), False)


def _row(protocol: str, source: str = VENUE_ACCOUNT_VALUATION_SOURCE) -> PositionInfo:
    return PositionInfo(
        position_type=PositionType.PERP, position_id=f"{protocol}:account", chain="bsc", protocol=protocol,
        value_usd=Decimal("4.62"), details={"valuation_source": source},
    )


def test_valuer_takes_the_discovered_equity_as_measured() -> None:
    assert PortfolioValuer._venue_account_value(_row("aster_perps")) == (Decimal("4.62"), {}, True)


def test_valuer_ignores_the_marker_for_a_protocol_without_a_venue_account_read() -> None:
    assert PortfolioValuer._venue_account_value(_row("gmx_v2")) is None
    assert PortfolioValuer._venue_account_value(_row("aster_perps", source="on_chain")) is None


def test_malformed_vault_deposit_log_raises_instead_of_decoding_a_fake_amount() -> None:
    from almanak.connectors.aster_perps.addresses import ASTER_PRO
    from almanak.connectors.aster_perps.vault_events import DEPOSIT_TOPIC, decode_deposits

    vault = ASTER_PRO["bsc"]["vault"]
    log = {
        "address": vault,
        "topics": [DEPOSIT_TOPIC, "0x" + "00" * 12 + "ab" * 20, "0x" + "00" * 12 + "cd" * 20],
        "data": "0x" + "00" * 31,
    }
    with pytest.raises(Exception):  # noqa: B017 — any decode error; the contract is "never a silent amount"
        decode_deposits({"logs": [log]}, vault=vault)


def test_isolated_position_pnl_counts_toward_equity() -> None:
    """The balance's cross PnL omits isolated positions; equity sums every position."""
    position = aster_perps_pb2.AsterPosition(
        symbol="SOLUSDT", position_amt="-0.05", entry_price="120", mark_price="119", unrealized_pnl="0.05", leverage="5"
    )
    read = read_aster_account(
        gateway_client=_gateway(_Stub([_usdt("4.6", "0")], [position])), chain="bsc", wallet_address=WALLET
    )
    assert read.ok and read.equity_usd == Decimal("4.65")


def test_position_without_unrealized_pnl_leaves_equity_unmeasured() -> None:
    position = aster_perps_pb2.AsterPosition(symbol="ETHUSDT", position_amt="0.002", unrealized_pnl="")
    read = read_aster_account(
        gateway_client=_gateway(_Stub([_usdt("4.6", "0")], [position])), chain="bsc", wallet_address=WALLET
    )
    assert not read.ok and read.equity_usd is None


def test_an_unmeasured_account_component_stays_none_not_the_string_none() -> None:
    [row] = _discover(VenueAccountRead(ok=True, equity_usd=Decimal("4.62"), cash_usd=None, unrealized_pnl_usd=None)).positions
    assert row.details["cash_usd"] is None and row.details["unrealized_pnl_usd"] is None
