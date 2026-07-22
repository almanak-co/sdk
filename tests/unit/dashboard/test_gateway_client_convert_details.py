"""Unit coverage for ``GatewayDashboardClient._convert_details``.

This is the proto -> dataclass boundary for the strategy-detail view. It is a
money-DISPLAY path: blank proto numeric strings must convert to the correct
Empty != Zero shape (``Decimal("0")`` for the always-present balance/LP fields,
but ``None`` for the strategy-reported PT inventory mark/PnL so the UI renders
"—" rather than "$0"). VIB-5317 added the ``strategy_positions`` block here;
these tests lock the whole converter, with explicit emphasis on the
measured-vs-unmeasured distinction.
"""

from __future__ import annotations

from decimal import Decimal

from almanak.framework.dashboard.gateway_client import GatewayDashboardClient
from almanak.gateway.proto import gateway_pb2


def _client() -> GatewayDashboardClient:
    # No gateway connection needed: _convert_details is a pure proto->dataclass
    # transform that never touches the channel.
    return GatewayDashboardClient(gateway_client=None)


def _full_details_proto() -> gateway_pb2.StrategyDetails:
    proto = gateway_pb2.StrategyDetails()
    proto.summary.deployment_id = "deployment:abc123"
    proto.summary.name = "pendle_pt"
    proto.summary.status = "running"

    # Wallet balances (always-present numeric fields -> Decimal, blank -> 0).
    tb_measured = proto.position.token_balances.add()
    tb_measured.symbol = "wstETH"
    tb_measured.balance = "1.5"
    tb_measured.value_usd = "4500.25"
    tb_blank = proto.position.token_balances.add()
    tb_blank.symbol = "USDC"
    tb_blank.balance = ""  # blank -> Decimal("0") (balances are always measured)
    tb_blank.value_usd = ""

    # LP position with a mix of set / blank numeric fields.
    lp = proto.position.lp_positions.add()
    lp.pool = "0xpool"
    lp.token0 = "WETH"
    lp.token1 = "USDC"
    lp.liquidity_usd = "1000.50"
    lp.range_lower = ""  # blank -> Decimal("0")
    lp.range_upper = "2000"
    lp.current_price = "1500"
    lp.in_range = True

    proto.position.total_lp_value_usd = "1000.50"
    proto.position.health_factor = "2.5"
    proto.position.leverage = "1.8"

    # Strategy-reported PT inventory: one measured, one UNMEASURED (blank).
    pt_measured = proto.position.strategy_positions.add()
    pt_measured.position_type = "pt_inventory"
    pt_measured.position_id = "PT-wstETH-25JUN2026"
    pt_measured.chain = "arbitrum"
    pt_measured.protocol = "pt"
    pt_measured.value_usd = "28.38"
    pt_measured.unrealized_pnl_usd = "1.12"
    pt_measured.details["source"] = "pt_inventory_lots"

    pt_unmeasured = proto.position.strategy_positions.add()
    pt_unmeasured.position_type = "pt_inventory"
    pt_unmeasured.position_id = "PT-sUSDe-30SEP2026"
    pt_unmeasured.chain = "arbitrum"
    pt_unmeasured.protocol = "pendle"
    pt_unmeasured.value_usd = ""  # UNMEASURED -> None (renders "—"), never $0
    pt_unmeasured.unrealized_pnl_usd = ""
    pt_unmeasured.details["source"] = "pt_inventory_lots"
    pt_unmeasured.details["mark_unmeasured"] = "true"

    # Timeline + pnl_history (the latter filters out zero-timestamp entries).
    ev = proto.timeline.add()
    ev.timestamp = 1_700_000_000
    ev.event_type = "TRADE"
    ev.description = "PT BUY"
    valid_pnl = proto.pnl_history.add()
    valid_pnl.timestamp = 1_700_000_000
    valid_pnl.value_usd = "100.0"
    valid_pnl.pnl_usd = "5.0"
    dropped_pnl = proto.pnl_history.add()
    dropped_pnl.timestamp = 0  # filtered out (no valid timestamp)
    dropped_pnl.value_usd = "999"
    return proto


def test_convert_details_full_proto_roundtrip():
    details = _client()._convert_details(_full_details_proto())

    # Summary delegated through _convert_summary.
    assert details.summary.deployment_id == "deployment:abc123"
    assert details.summary.name == "pendle_pt"

    # Token balances: blank -> Decimal("0") (always-measured fields).
    assert details.position.token_balances[0].balance == Decimal("1.5")
    assert details.position.token_balances[0].value_usd == Decimal("4500.25")
    assert details.position.token_balances[1].balance == Decimal("0")
    assert details.position.token_balances[1].value_usd == Decimal("0")

    # LP position decimals, blank -> Decimal("0").
    lp = details.position.lp_positions[0]
    assert lp.liquidity_usd == Decimal("1000.50")
    assert lp.range_lower == Decimal("0")
    assert lp.range_upper == Decimal("2000")
    assert lp.current_price == Decimal("1500")
    assert lp.in_range is True

    assert details.position.total_lp_value_usd == Decimal("1000.50")
    assert details.position.health_factor == Decimal("2.5")
    assert details.position.leverage == Decimal("1.8")

    # Timeline + pnl_history (zero-timestamp entry dropped).
    assert len(details.timeline) == 1
    assert details.timeline[0].event_type == "TRADE"
    assert len(details.pnl_history) == 1
    assert details.pnl_history[0]["value_usd"] == Decimal("100.0")
    assert details.pnl_history[0]["pnl_usd"] == Decimal("5.0")


def test_convert_details_pt_inventory_empty_not_zero():
    """The crux of VIB-5317: a blank PT mark/PnL is UNMEASURED (None), never $0,
    while a measured mark converts to Decimal. Keyed on the value being blank,
    independent of protocol name (``pt`` vs ``pendle``)."""
    details = _client()._convert_details(_full_details_proto())
    positions = details.position.strategy_positions
    assert len(positions) == 2

    measured = positions[0]
    assert measured.protocol == "pt"
    assert measured.value_usd == Decimal("28.38")
    assert measured.unrealized_pnl_usd == Decimal("1.12")
    assert measured.details["source"] == "pt_inventory_lots"

    unmeasured = positions[1]
    assert unmeasured.protocol == "pendle"
    # Empty != Zero: blank proto string -> None, NOT Decimal("0").
    assert unmeasured.value_usd is None
    assert unmeasured.unrealized_pnl_usd is None
    assert unmeasured.details["mark_unmeasured"] == "true"


def test_convert_details_empty_position_is_safe():
    """An empty position sub-message converts to empty lists, no crash."""
    proto = gateway_pb2.StrategyDetails()
    proto.summary.deployment_id = "deployment:empty"
    details = _client()._convert_details(proto)
    assert details.position.token_balances == []
    assert details.position.lp_positions == []
    assert details.position.strategy_positions == []
    assert details.timeline == []
    assert details.pnl_history == []


def test_convert_details_empty_pnl_and_value_are_none_not_zero():
    """VIB-5942 CodeRabbit #1: an empty ``value_usd`` / ``pnl_usd`` on the wire is
    UNMEASURED → None in the converted dict, never Decimal("0"). A measured "0"
    stays Decimal("0")."""
    proto = gateway_pb2.StrategyDetails()
    unmeasured = proto.pnl_history.add()
    unmeasured.timestamp = 1_700_000_000
    unmeasured.value_usd = ""  # unmeasured NAV
    unmeasured.pnl_usd = ""  # unmeasured pnl
    measured_zero = proto.pnl_history.add()
    measured_zero.timestamp = 1_700_000_100
    measured_zero.value_usd = "0"  # MEASURED zero
    measured_zero.pnl_usd = "0"

    details = _client()._convert_details(proto)
    assert len(details.pnl_history) == 2
    assert details.pnl_history[0]["value_usd"] is None
    assert details.pnl_history[0]["pnl_usd"] is None
    assert details.pnl_history[1]["value_usd"] == Decimal("0")  # measured zero preserved
    assert details.pnl_history[1]["pnl_usd"] == Decimal("0")
