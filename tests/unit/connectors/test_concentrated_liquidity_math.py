"""Tests for shared concentrated-liquidity math primitives."""

from decimal import Decimal

import pytest

from almanak.connectors._strategy_base.concentrated_liquidity_math import (
    MAX_TICK,
    MIN_TICK,
    V3_TICK_SPACING,
    get_max_tick,
    get_min_tick,
    get_nearest_tick,
    price_to_tick,
    tick_to_price,
)


def test_price_to_tick_uses_decimal_math_for_asymmetric_decimals():
    tick = price_to_tick(Decimal("2000"), decimals0=6, decimals1=18)
    price_back = tick_to_price(tick, decimals0=6, decimals1=18)

    relative_error = abs(price_back - Decimal("2000")) / Decimal("2000")
    assert relative_error < Decimal("0.0001")


def test_price_to_tick_raises_for_non_positive_prices_by_default():
    with pytest.raises(ValueError, match="Price must be positive"):
        price_to_tick(Decimal("0"))

    with pytest.raises(ValueError, match="Price must be positive"):
        price_to_tick(Decimal("-1"))


def test_price_to_tick_can_preserve_legacy_connector_non_positive_behavior():
    assert price_to_tick(Decimal("0"), non_positive="min_tick") == MIN_TICK
    assert price_to_tick(Decimal("-1"), non_positive="min_tick") == MIN_TICK


def test_tick_bounds_are_aligned_to_spacing():
    min_tick = get_min_tick(3000)
    max_tick = get_max_tick(3000)

    assert min_tick >= MIN_TICK
    assert max_tick <= MAX_TICK
    assert min_tick % V3_TICK_SPACING[3000] == 0
    assert max_tick % V3_TICK_SPACING[3000] == 0


def test_get_nearest_tick_snaps_to_fee_spacing():
    assert get_nearest_tick(62, 3000) == 60
    assert get_nearest_tick(91, 3000) == 120
