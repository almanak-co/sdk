"""Unit tests for lp_math.py — Uniswap V3 sqrtPriceX96 amount recomputation."""

from __future__ import annotations

import math

import pytest

from almanak.framework.intents.lp_math import (
    MAX_TICK,
    MIN_TICK,
    _amount0_for_liquidity,
    _amount1_for_liquidity,
    _liquidity_for_amount0,
    _liquidity_for_amount1,
    recompute_lp_amounts,
    tick_to_sqrt_ratio_x96,
)

_Q96 = 2**96


class TestTickToSqrtRatioX96:
    def test_tick_zero_exact(self):
        # tick=0 -> sqrt(1.0001^0) = 1.0, result must be exactly Q96
        assert tick_to_sqrt_ratio_x96(0) == _Q96

    def test_tick_positive_exact(self):
        # Precomputed with Python Decimal(50-digit precision)
        assert tick_to_sqrt_ratio_x96(60) == 79466191966197645195421774832

    def test_tick_negative_exact(self):
        # Precomputed with Python Decimal(50-digit precision)
        assert tick_to_sqrt_ratio_x96(-60) == 78990846045029531151608375685

    def test_tick_large_exact(self):
        # Precomputed with Python Decimal(50-digit precision)
        assert tick_to_sqrt_ratio_x96(10000) == 130621891405341611593710811005

    def test_tick_near_max(self):
        result = tick_to_sqrt_ratio_x96(MAX_TICK)
        assert result > 0
        assert isinstance(result, int)

    def test_tick_near_min(self):
        result = tick_to_sqrt_ratio_x96(MIN_TICK)
        assert result > 0
        assert isinstance(result, int)

    def test_tick_out_of_range_raises(self):
        with pytest.raises(ValueError, match="out of Uniswap V3 range"):
            tick_to_sqrt_ratio_x96(MAX_TICK + 1)
        with pytest.raises(ValueError, match="out of Uniswap V3 range"):
            tick_to_sqrt_ratio_x96(MIN_TICK - 1)

    def test_lower_tick_lt_upper(self):
        lo = tick_to_sqrt_ratio_x96(-100)
        hi = tick_to_sqrt_ratio_x96(100)
        assert lo < hi

    def test_negative_ticks_realistic_usdc_weth(self):
        # USDC/WETH pools typically have large negative ticks
        lo = tick_to_sqrt_ratio_x96(-207240)
        hi = tick_to_sqrt_ratio_x96(-193380)
        assert 0 < lo < hi


class TestLiquidityMath:
    """Test the private liquidity math primitives."""

    def test_liquidity_for_amount0_zero_amount(self):
        assert _liquidity_for_amount0(1_000, 2_000, 0) == 0

    def test_liquidity_for_amount1_zero_amount(self):
        assert _liquidity_for_amount1(1_000, 2_000, 0) == 0

    def test_liquidity_for_amount0_equal_sqrt(self):
        # degenerate range
        assert _liquidity_for_amount0(1_000, 1_000, 1_000) == 0

    def test_liquidity_for_amount1_equal_sqrt(self):
        assert _liquidity_for_amount1(1_000, 1_000, 1_000) == 0

    def test_amount0_for_liquidity_zero_sqrt(self):
        assert _amount0_for_liquidity(0, 1_000, 500) == 0

    def test_amount1_for_liquidity_basic(self):
        # L * (sqrtB - sqrtA) / Q96
        liq = 1_000_000
        sqrt_a = _Q96
        sqrt_b = 2 * _Q96
        result = _amount1_for_liquidity(sqrt_a, sqrt_b, liq)
        expected = liq * (sqrt_b - sqrt_a) // _Q96
        assert result == expected

    def test_liquidity_roundtrip_amount0(self):
        # Compute liquidity from amount0, then recover amount0 -- should be <= original
        sqrt_a = tick_to_sqrt_ratio_x96(-1000)
        sqrt_b = tick_to_sqrt_ratio_x96(-500)
        amount0 = 10**18  # 1 ETH worth
        liq = _liquidity_for_amount0(sqrt_a, sqrt_b, amount0)
        recovered = _amount0_for_liquidity(sqrt_a, sqrt_b, liq)
        assert 0 < recovered <= amount0

    def test_liquidity_roundtrip_amount1(self):
        sqrt_a = tick_to_sqrt_ratio_x96(500)
        sqrt_b = tick_to_sqrt_ratio_x96(1000)
        amount1 = 10**6  # 1 USDC
        liq = _liquidity_for_amount1(sqrt_a, sqrt_b, amount1)
        recovered = _amount1_for_liquidity(sqrt_a, sqrt_b, liq)
        assert 0 < recovered <= amount1


class TestRecomputeLPAmounts:
    """Integration tests for recompute_lp_amounts."""

    def _make_sqrt_price(self, tick: int) -> int:
        return tick_to_sqrt_ratio_x96(tick)

    def test_price_below_range_uses_only_amount0(self):
        # Pool price < lower tick: position is 100% token0
        sqrt_price = self._make_sqrt_price(-2000)  # below range
        a0, a1 = recompute_lp_amounts(
            sqrt_price,
            tick_lower=-1000,
            tick_upper=1000,
            amount0_desired=10**18,
            amount1_desired=0,
        )
        assert a0 > 0
        assert a1 == 0

    def test_price_above_range_uses_only_amount1(self):
        # Pool price > upper tick: position is 100% token1
        sqrt_price = self._make_sqrt_price(2000)  # above range
        a0, a1 = recompute_lp_amounts(
            sqrt_price,
            tick_lower=-1000,
            tick_upper=1000,
            amount0_desired=0,
            amount1_desired=10**6,
        )
        assert a0 == 0
        assert a1 > 0

    def test_in_range_mixed_position(self):
        # Price in range: both tokens used
        sqrt_price = self._make_sqrt_price(0)  # in range [-1000, 1000]
        a0, a1 = recompute_lp_amounts(
            sqrt_price,
            tick_lower=-1000,
            tick_upper=1000,
            amount0_desired=10**18,
            amount1_desired=3000 * 10**6,
        )
        assert a0 > 0
        assert a1 > 0
        assert a0 <= 10**18
        assert a1 <= 3000 * 10**6

    def test_degenerate_range_returns_original(self):
        sqrt_price = self._make_sqrt_price(0)
        a0, a1 = recompute_lp_amounts(
            sqrt_price,
            tick_lower=100,
            tick_upper=100,  # degenerate: same tick
            amount0_desired=10**18,
            amount1_desired=10**6,
        )
        assert a0 == 10**18
        assert a1 == 10**6

    def test_zero_liquidity_returns_original(self):
        # Both amounts 0 -> liquidity=0 -> returns original
        sqrt_price = self._make_sqrt_price(0)
        a0, a1 = recompute_lp_amounts(
            sqrt_price,
            tick_lower=-1000,
            tick_upper=1000,
            amount0_desired=0,
            amount1_desired=0,
        )
        assert a0 == 0
        assert a1 == 0

    def test_wrong_side_token_above_range_returns_zero(self):
        # Price above range: pool expects only token1, but user supplied only token0.
        # Liquidity is zero -> must return (0, 0) so compiler fails fast.
        sqrt_price = self._make_sqrt_price(2000)  # above range [-1000, 1000]
        a0, a1 = recompute_lp_amounts(
            sqrt_price,
            tick_lower=-1000,
            tick_upper=1000,
            amount0_desired=10**18,
            amount1_desired=0,
        )
        assert a0 == 0
        assert a1 == 0

    def test_wrong_side_token_below_range_returns_zero(self):
        # Price below range: pool expects only token0, but user supplied only token1.
        sqrt_price = self._make_sqrt_price(-2000)  # below range [-1000, 1000]
        a0, a1 = recompute_lp_amounts(
            sqrt_price,
            tick_lower=-1000,
            tick_upper=1000,
            amount0_desired=0,
            amount1_desired=10**6,
        )
        assert a0 == 0
        assert a1 == 0

    def test_result_lte_desired(self):
        # Corrected amounts must never exceed desired amounts
        sqrt_price = self._make_sqrt_price(100)
        a0, a1 = recompute_lp_amounts(
            sqrt_price,
            tick_lower=-500,
            tick_upper=500,
            amount0_desired=5 * 10**18,
            amount1_desired=10_000 * 10**6,
        )
        assert a0 <= 5 * 10**18
        assert a1 <= 10_000 * 10**6

    def test_tick_order_does_not_matter(self):
        # Swapped tick_lower/tick_upper gives same result
        sqrt_price = self._make_sqrt_price(0)
        a0a, a1a = recompute_lp_amounts(sqrt_price, -1000, 1000, 10**18, 3000 * 10**6)
        a0b, a1b = recompute_lp_amounts(sqrt_price, 1000, -1000, 10**18, 3000 * 10**6)
        assert a0a == a0b
        assert a1a == a1b

    def test_realistic_wbtc_weth_position(self):
        # Simulate WBTC/WETH pool at ratio ~15 (both 18-decimal here for simplicity)
        # Price = 15, tick = log(15) / log(1.0001) ~ 27081
        current_tick = int(math.log(15) / math.log(1.0001))
        sqrt_price = self._make_sqrt_price(current_tick)

        # Range: 12-18 (covers price=15, in-range)
        tick_lower = int(math.log(12) / math.log(1.0001))
        tick_upper = int(math.log(18) / math.log(1.0001))

        amount0_desired = 1 * 10**18  # 1 WBTC (18 dec)
        amount1_desired = 15 * 10**18  # 15 WETH (18 dec)

        a0, a1 = recompute_lp_amounts(sqrt_price, tick_lower, tick_upper, amount0_desired, amount1_desired)
        # Both tokens should be used (price is in range), neither exceeds desired
        assert a0 > 0
        assert a1 > 0
        assert a0 <= amount0_desired
        assert a1 <= amount1_desired

    def test_out_of_bounds_tick_returns_original(self):
        # Ticks outside Uniswap V3 range should return original amounts
        sqrt_price = self._make_sqrt_price(0)
        a0, a1 = recompute_lp_amounts(
            sqrt_price,
            tick_lower=MIN_TICK - 1,
            tick_upper=MAX_TICK + 1,
            amount0_desired=10**18,
            amount1_desired=10**6,
        )
        assert a0 == 10**18
        assert a1 == 10**6

    def test_negative_tick_range_usdc_weth(self):
        # Realistic USDC/WETH pool with large negative ticks
        current_tick = -200000
        sqrt_price = self._make_sqrt_price(current_tick)
        a0, a1 = recompute_lp_amounts(
            sqrt_price,
            tick_lower=-207240,
            tick_upper=-193380,
            amount0_desired=1000 * 10**6,  # 1000 USDC (6 dec)
            amount1_desired=10**18,  # 1 WETH (18 dec)
            current_tick=current_tick,
        )
        # Price is in range, both tokens used
        assert a0 > 0
        assert a1 > 0
        assert a0 <= 1000 * 10**6
        assert a1 <= 10**18
