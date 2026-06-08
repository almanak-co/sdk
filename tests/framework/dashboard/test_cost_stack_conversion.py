"""VIB-4984: presence-aware proto→dataclass conversion for CostStackInfo.

The new ``inventory_unrealized_usd`` proto field must map Empty≠Zero: an empty
proto string => ``None`` (unmeasured), NOT ``Decimal("0")``.
"""

from __future__ import annotations

from decimal import Decimal

from almanak.framework.dashboard.gateway_client import _convert_cost_stack, _safe_optional_decimal
from almanak.gateway.proto import gateway_pb2


def test_safe_optional_decimal_empty_is_none() -> None:
    assert _safe_optional_decimal("") is None


def test_safe_optional_decimal_unparseable_is_none() -> None:
    assert _safe_optional_decimal("not-a-number") is None


def test_safe_optional_decimal_parses_value() -> None:
    assert _safe_optional_decimal("-0.0038") == Decimal("-0.0038")


def test_convert_cost_stack_inventory_empty_is_none() -> None:
    proto = gateway_pb2.CostStackInfo(
        cost_gas_usd="0.05",
        realized_pnl_usd="1.0",
        inventory_unrealized_usd="",  # unmeasured
    )
    cs = _convert_cost_stack(proto)
    assert cs.inventory_unrealized_usd is None
    # Other fields still collapse "" → Decimal("0") via _safe_decimal.
    assert cs.cost_gas_usd == Decimal("0.05")
    assert cs.realized_pnl_usd == Decimal("1.0")


def test_convert_cost_stack_inventory_value_round_trips() -> None:
    proto = gateway_pb2.CostStackInfo(inventory_unrealized_usd="-0.0038")
    cs = _convert_cost_stack(proto)
    assert cs.inventory_unrealized_usd == Decimal("-0.0038")
