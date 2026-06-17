"""Unit tests for the shared Solana CLMM math foundation module.

Covers the maths shared by the Raydium CLMM and Orca Whirlpool connectors:
price<->tick roundtrips, tick-spacing alignment, Q64.64 sqrt-price conversions,
liquidity in all three price ranges, forward/inverse amount consistency, the
venue-specific tick-array sizing, and the typed error surface.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from almanak.connectors._strategy_base.solana_clmm_math import (
    MAX_SQRT_PRICE_X64,
    MAX_TICK,
    MIN_SQRT_PRICE_X64,
    MIN_TICK,
    Q64,
    SolanaCLMMTickError,
    align_tick_to_spacing,
    get_amounts_from_liquidity,
    get_liquidity_from_amounts,
    price_to_tick,
    sqrt_price_x64_to_tick,
    tick_array_start_index,
    tick_to_price,
    tick_to_sqrt_price_x64,
)


class TestPriceTickRoundtrip:
    @pytest.mark.parametrize("decimals_a,decimals_b", [(9, 6), (6, 9), (9, 9), (6, 6)])
    def test_price_to_tick_to_price_roundtrips_within_one_bps(self, decimals_a: int, decimals_b: int) -> None:
        price = Decimal("150")  # token_b per token_a
        tick = price_to_tick(price, decimals_a, decimals_b)
        recovered = tick_to_price(tick, decimals_a, decimals_b)
        # 1.0001^1 == 1 bps granularity, so the recovered price is within one tick.
        assert abs(recovered - price) / price < Decimal("0.0001")

    def test_price_to_tick_rejects_non_positive(self) -> None:
        with pytest.raises(SolanaCLMMTickError, match="positive"):
            price_to_tick(Decimal("0"), 9, 6)
        with pytest.raises(SolanaCLMMTickError, match="positive"):
            price_to_tick(Decimal("-1"), 9, 6)


class TestAlignTickToSpacing:
    @pytest.mark.parametrize("spacing", [1, 10, 60, 120])
    def test_aligned_is_multiple_of_spacing(self, spacing: int) -> None:
        for tick in (-12345, -1, 0, 1, 9999, 250_000):
            aligned = align_tick_to_spacing(tick, spacing)
            assert aligned % spacing == 0
            assert MIN_TICK <= aligned <= MAX_TICK

    def test_round_up_vs_down(self) -> None:
        assert align_tick_to_spacing(155, 60, round_up=False) == 120
        assert align_tick_to_spacing(155, 60, round_up=True) == 180

    def test_rejects_non_positive_spacing(self) -> None:
        with pytest.raises(SolanaCLMMTickError, match="tick_spacing"):
            align_tick_to_spacing(100, 0)

    @pytest.mark.parametrize("spacing", [10, 60, 120])
    def test_clamped_extremes_stay_spacing_aligned(self, spacing: int) -> None:
        # MIN_TICK / MAX_TICK are not multiples of these spacings, so clamping at the
        # extremes must land on a spacing-aligned bound -- never on the raw constant.
        for tick in (MIN_TICK - 5000, MIN_TICK, MAX_TICK, MAX_TICK + 5000):
            for round_up in (False, True):
                aligned = align_tick_to_spacing(tick, spacing, round_up=round_up)
                assert aligned % spacing == 0
                assert MIN_TICK <= aligned <= MAX_TICK

    def test_max_tick_not_overshot_when_unaligned(self) -> None:
        # spacing=60 does not divide MAX_TICK (443636 % 60 == 56). Rounding up at the
        # ceiling must clamp to the largest in-bounds multiple, not exceed MAX_TICK.
        spacing = 60
        assert MAX_TICK % spacing != 0  # guard the premise
        aligned = align_tick_to_spacing(MAX_TICK, spacing, round_up=True)
        assert aligned == (MAX_TICK // spacing) * spacing
        assert aligned <= MAX_TICK
        assert aligned % spacing == 0

    def test_min_tick_not_undershot_when_unaligned(self) -> None:
        # Symmetric to the MAX_TICK case: rounding down at the floor stays in bounds.
        spacing = 60
        assert MIN_TICK % spacing != 0  # guard the premise
        aligned = align_tick_to_spacing(MIN_TICK, spacing, round_up=False)
        assert aligned == -((-MIN_TICK) // spacing) * spacing
        assert aligned >= MIN_TICK
        assert aligned % spacing == 0


class TestSqrtPriceConversion:
    def test_tick_zero_is_q64_scale(self) -> None:
        # sqrt(1.0001^0) * 2^64 == 2^64
        assert tick_to_sqrt_price_x64(0) == Q64

    def test_sqrt_price_clamped_to_bounds(self) -> None:
        assert tick_to_sqrt_price_x64(MIN_TICK) >= MIN_SQRT_PRICE_X64
        assert tick_to_sqrt_price_x64(MAX_TICK) <= MAX_SQRT_PRICE_X64

    def test_roundtrip_tick_sqrt_tick(self) -> None:
        for tick in (-100_000, -1000, 0, 1000, 100_000):
            sqrt_x64 = tick_to_sqrt_price_x64(tick)
            recovered = sqrt_price_x64_to_tick(sqrt_x64)
            assert abs(recovered - tick) <= 1


class TestLiquidityMath:
    # A symmetric range around the current price (Q64.64).
    lower = tick_to_sqrt_price_x64(-1000)
    cur = tick_to_sqrt_price_x64(0)
    upper = tick_to_sqrt_price_x64(1000)

    def test_below_range_uses_token_a_only(self) -> None:
        liq = get_liquidity_from_amounts(self.lower - 1, self.lower, self.upper, 1_000_000, 1_000_000)
        amt_a, amt_b = get_amounts_from_liquidity(self.lower - 1, self.lower, self.upper, liq)
        assert amt_a > 0
        assert amt_b == 0

    def test_above_range_uses_token_b_only(self) -> None:
        liq = get_liquidity_from_amounts(self.upper + 1, self.lower, self.upper, 1_000_000, 1_000_000)
        amt_a, amt_b = get_amounts_from_liquidity(self.upper + 1, self.lower, self.upper, liq)
        assert amt_a == 0
        assert amt_b > 0

    def test_in_range_uses_both_tokens(self) -> None:
        liq = get_liquidity_from_amounts(self.cur, self.lower, self.upper, 1_000_000, 1_000_000)
        amt_a, amt_b = get_amounts_from_liquidity(self.cur, self.lower, self.upper, liq)
        assert amt_a > 0
        assert amt_b > 0

    def test_inverse_consistency_in_range(self) -> None:
        # Forward then inverse should not exceed the inputs (floor division only loses).
        liq = get_liquidity_from_amounts(self.cur, self.lower, self.upper, 1_000_000, 2_000_000)
        amt_a, amt_b = get_amounts_from_liquidity(self.cur, self.lower, self.upper, liq)
        assert amt_a <= 1_000_000
        assert amt_b <= 2_000_000

    def test_rejects_inverted_range(self) -> None:
        with pytest.raises(SolanaCLMMTickError, match="Lower sqrt price"):
            get_liquidity_from_amounts(self.cur, self.upper, self.lower, 1, 1)

    def test_amounts_rejects_inverted_range(self) -> None:
        # Inverse mirrors the forward guard: an inverted range would otherwise yield
        # negative amounts from the negative width in the below/above-range branches.
        with pytest.raises(SolanaCLMMTickError, match="Lower sqrt price"):
            get_amounts_from_liquidity(self.cur, self.upper, self.lower, 1_000_000)

    def test_amounts_rejects_zero_width_range(self) -> None:
        # lower == upper is degenerate (division by a zero width); reject it.
        with pytest.raises(SolanaCLMMTickError, match="Lower sqrt price"):
            get_amounts_from_liquidity(self.cur, self.lower, self.lower, 1_000_000)

    def test_amounts_rejects_non_positive_sqrt_prices(self) -> None:
        # A non-positive sqrt price divides by zero in the in-range / below-range
        # denominators; each position is guarded.
        for bad_args in (
            (0, self.lower, self.upper),  # current
            (self.cur, 0, self.upper),  # lower
            (self.cur, self.lower, 0),  # upper
            (self.cur, self.lower, -5),  # negative upper
            (-1, self.lower, self.upper),  # negative current
        ):
            with pytest.raises(SolanaCLMMTickError, match="positive"):
                get_amounts_from_liquidity(*bad_args, 1_000_000)


class TestTickArrayStartIndex:
    def test_raydium_default_60(self) -> None:
        # array_size = 60 * spacing; tick 0 starts at 0.
        assert tick_array_start_index(0, 10) == 0
        assert tick_array_start_index(599, 10) == 0
        assert tick_array_start_index(600, 10) == 600

    def test_orca_88_differs_from_raydium(self) -> None:
        # 88-tick arrays produce a different start than the 60 default.
        assert tick_array_start_index(700, 10, ticks_per_array=88) == 0
        assert tick_array_start_index(880, 10, ticks_per_array=88) == 880
        assert tick_array_start_index(700, 10) != tick_array_start_index(700, 10, ticks_per_array=88)

    def test_negative_tick(self) -> None:
        assert tick_array_start_index(-1, 10) == -600

    def test_rejects_non_positive_inputs(self) -> None:
        with pytest.raises(SolanaCLMMTickError, match="tick_spacing"):
            tick_array_start_index(0, 0)
        with pytest.raises(SolanaCLMMTickError, match="ticks_per_array"):
            tick_array_start_index(0, 10, ticks_per_array=0)
