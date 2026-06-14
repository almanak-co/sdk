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
    sqrt_price_x96_to_tick,
    tick_to_price,
    tick_to_sqrt_price_x96,
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


class TestSqrtPriceToTick:
    """Regression coverage for sqrt_price_x96_to_tick (VIB-5113).

    The inverse double-floored against the floored forward (``int(...)``) and
    landed one tick low for negative ticks (e.g. -1 -> -2). Existing tests
    encoded the bug with an ``abs(...) <= 1`` tolerance and only exercised
    positive ticks. The canonical shared helper here is what uniswap_v3 and
    sushiswap_v3 delegate to, so it is the right place to pin the contract.
    """

    @pytest.mark.parametrize(
        "tick",
        [MIN_TICK, -887271, -100000, -10000, -100, -2, -1, 0, 1, 2, 100, 10000, 100000, 887271, MAX_TICK],
    )
    def test_round_trip_is_exact(self, tick):
        assert sqrt_price_x96_to_tick(tick_to_sqrt_price_x96(tick)) == tick

    def test_round_trip_exhaustive_sample(self):
        for tick in range(MIN_TICK, MAX_TICK + 1, 1009):
            assert sqrt_price_x96_to_tick(tick_to_sqrt_price_x96(tick)) == tick, tick

    def test_get_tick_at_sqrt_ratio_invariant(self):
        """sqrtPrice(t) <= x < sqrtPrice(t+1) for the returned tick."""
        for tick in (-50000, -1, 0, 1, 777, 50000):
            x = tick_to_sqrt_price_x96(tick) + 12345
            t = sqrt_price_x96_to_tick(x)
            assert tick_to_sqrt_price_x96(t) <= x < tick_to_sqrt_price_x96(t + 1)

    def test_non_positive_raises(self):
        """Existing contract preserved: shared helper raises on non-positive input."""
        with pytest.raises(ValueError):
            sqrt_price_x96_to_tick(0)
        with pytest.raises(ValueError):
            sqrt_price_x96_to_tick(-1)

    def test_out_of_domain_clamps_to_max_tick(self):
        """An out-of-domain price clamps to MAX_TICK without spinning the loop."""
        assert sqrt_price_x96_to_tick(2**200) == MAX_TICK


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
