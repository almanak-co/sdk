"""Unit coverage for typed connector-native perp history targets."""

from __future__ import annotations

from typing import Any

import pytest

from almanak.framework.backtesting.pnl.perp_targets import PerpPriceHistoryTarget

MARKET_ADDRESS = "0x09400d9db990d5ed3f35d7be61dfaeb900af03c9"


def test_target_normalizes_protocol_and_routes_prices_address_first() -> None:
    target = PerpPriceHistoryTarget(
        protocol="  GMX-V2  ",
        market="  SOL/USD  ",
        market_address=f"  {MARKET_ADDRESS}  ",
    )

    assert target.protocol == "gmx_v2"
    assert target.market == "SOL/USD"
    assert target.market_address == MARKET_ADDRESS
    assert target.price_market == MARKET_ADDRESS


def test_target_routes_prices_by_market_without_address() -> None:
    target = PerpPriceHistoryTarget(protocol="gmx_v2", market="SOL/USD")

    assert target.price_market == "SOL/USD"


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"protocol": "", "market": "SOL/USD"}, "protocol"),
        ({"protocol": "   ", "market": "SOL/USD"}, "protocol"),
        ({"protocol": None, "market": "SOL/USD"}, "protocol"),
        ({"protocol": "gmx_v2", "market": ""}, "market"),
        ({"protocol": "gmx_v2", "market": "   "}, "market"),
        ({"protocol": "gmx_v2", "market": None}, "market"),
        ({"protocol": "gmx_v2", "market": "SOL/USD", "market_address": ""}, "market_address"),
        ({"protocol": "gmx_v2", "market": "SOL/USD", "market_address": "0x1234"}, "market_address"),
        ({"protocol": "gmx_v2", "market": "SOL/USD", "market_address": 123}, "market_address"),
    ],
)
def test_target_rejects_invalid_values(kwargs: dict[str, Any], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        PerpPriceHistoryTarget(**kwargs)
