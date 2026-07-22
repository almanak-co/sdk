"""VIB-5942 — unit coverage for the perp-story conversion boundary + Empty≠Zero /
finite-only decimal parsing (CodeRabbit follow-ups #3, #5).

Lives under tests/unit/ (the rest of the perp-story suite is under tests/framework/)
so the pure proto→dataclass conversion + the finite-only `_safe_optional_decimal`
guard have focused, framework-free coverage.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from almanak.framework.dashboard.gateway_client import (
    PerpPositionInfo,
    _convert_perp_position,
    _safe_optional_decimal,
)
from almanak.gateway.proto import gateway_pb2


# --------------------------------------------------------------------------- #
# _safe_optional_decimal — Empty≠Zero + finite-only (CodeRabbit #3)
# --------------------------------------------------------------------------- #


def test_safe_optional_decimal_empty_is_none():
    assert _safe_optional_decimal("") is None


def test_safe_optional_decimal_measured_zero_is_decimal_zero():
    # Empty≠Zero the OTHER way: a measured "0" is Decimal("0"), NOT None.
    assert _safe_optional_decimal("0") == Decimal("0")


def test_safe_optional_decimal_measured_value():
    assert _safe_optional_decimal("1905.98") == Decimal("1905.98")


@pytest.mark.parametrize("bad", ["NaN", "nan", "Infinity", "-Infinity", "inf", "-inf", "N/A", "not-a-number"])
def test_safe_optional_decimal_non_finite_or_malformed_is_none(bad):
    """VIB-5942 CodeRabbit #3: Decimal('NaN')/Decimal('Infinity') PARSE but are
    non-finite — they must read UNMEASURED (None), never reach money math / a chart
    as a garbage magnitude."""
    assert _safe_optional_decimal(bad) is None


# --------------------------------------------------------------------------- #
# _convert_perp_position — proto → PerpPositionInfo (CodeRabbit #5)
# --------------------------------------------------------------------------- #


def test_perp_position_info_defaults_are_unmeasured():
    p = PerpPositionInfo()
    assert p.market == "" and p.direction == "" and p.protocol == "" and p.chain == ""
    assert p.is_long is None
    assert p.entry_price_usd is None and p.mark_price_usd is None and p.leverage is None
    assert p.notional_usd is None and p.collateral_usd is None and p.unrealized_pnl_usd is None


def test_convert_empty_prices_and_leverage_are_none():
    proto = gateway_pb2.PerpPositionSummary(market="ETH/USD", entry_price_usd="", leverage="", notional_usd="")
    info = _convert_perp_position(proto)
    assert info.market == "ETH/USD"
    assert info.entry_price_usd is None and info.leverage is None and info.notional_usd is None
    assert info.is_long is None and info.direction == ""


def test_convert_measured_fields_round_trip():
    proto = gateway_pb2.PerpPositionSummary(
        market="ETH/USD",
        is_long=True,
        entry_price_usd="1905.98",
        mark_price_usd="1906.18",
        leverage="2.0",
        notional_usd="20",
        collateral_usd="9.99",
        unrealized_pnl_usd="0.002",
        protocol="gmx_v2",
        chain="avalanche",
    )
    info = _convert_perp_position(proto)
    assert info.is_long is True and info.direction == "LONG"
    assert info.entry_price_usd == Decimal("1905.98")
    assert info.mark_price_usd == Decimal("1906.18")
    assert info.leverage == Decimal("2.0")
    assert info.notional_usd == Decimal("20")
    assert info.collateral_usd == Decimal("9.99")
    assert info.unrealized_pnl_usd == Decimal("0.002")
    assert info.protocol == "gmx_v2" and info.chain == "avalanche"


def test_convert_short_measured_false_is_not_none():
    """proto3 optional bool: is_long=False (SHORT) is a MEASURED False, not None."""
    info = _convert_perp_position(gateway_pb2.PerpPositionSummary(market="ETH/USD", is_long=False))
    assert info.is_long is False and info.direction == "SHORT"


def test_convert_direction_from_side_string_when_is_long_absent():
    info = _convert_perp_position(gateway_pb2.PerpPositionSummary(market="ETH/USD", direction="SHORT"))
    assert info.is_long is None and info.direction == "SHORT"


@pytest.mark.parametrize("bad", ["NaN", "Infinity", "-Infinity"])
def test_convert_non_finite_prices_are_unmeasured(bad):
    """A corrupt non-finite price on the wire converts to None (— unmeasured),
    never a garbage Decimal that would render/scale a chart absurdly."""
    proto = gateway_pb2.PerpPositionSummary(market="ETH/USD", entry_price_usd=bad, mark_price_usd=bad, leverage=bad)
    info = _convert_perp_position(proto)
    assert info.entry_price_usd is None and info.mark_price_usd is None and info.leverage is None
