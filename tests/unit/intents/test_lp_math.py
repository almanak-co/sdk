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
    liquidity_for_amounts_at_sqrt_price,
    range_midpoint_sqrt_price_x96,
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


# =============================================================================
# VIB-3823: getLiquidityForAmounts pre-flight helpers
# =============================================================================


class TestRangeMidpointSqrtPriceX96:
    """Geometric midpoint helper for the no-slot0 fallback."""

    def test_midpoint_inside_range(self):
        sqrt_a = tick_to_sqrt_ratio_x96(-1000)
        sqrt_b = tick_to_sqrt_ratio_x96(1000)
        mid = range_midpoint_sqrt_price_x96(-1000, 1000)
        assert sqrt_a < mid < sqrt_b

    def test_midpoint_symmetric_around_tick0(self):
        # Symmetric range around tick 0 should put midpoint near 2**96
        mid = range_midpoint_sqrt_price_x96(-1000, 1000)
        # Tolerate isqrt floor rounding
        assert abs(mid - 2**96) <= 1

    def test_midpoint_handles_swapped_inputs(self):
        forward = range_midpoint_sqrt_price_x96(-500, 500)
        reversed_ = range_midpoint_sqrt_price_x96(500, -500)
        assert forward == reversed_

    def test_midpoint_degenerate_returns_zero(self):
        # Same lower/upper -> degenerate -> 0 (caller short-circuits)
        assert range_midpoint_sqrt_price_x96(100, 100) == 0


class TestLiquidityForAmountsAtSqrtPrice:
    """Pre-flight liquidity helper used by the LP_OPEN compile-time check."""

    def test_in_range_both_legs_positive(self):
        sqrt_p = tick_to_sqrt_ratio_x96(0)
        liq = liquidity_for_amounts_at_sqrt_price(
            sqrt_p, -1000, 1000, 10**18, 10**18
        )
        assert liq > 0

    def test_below_range_with_only_token0(self):
        # Pool is below the position range — needs only token0 to mint
        sqrt_p = tick_to_sqrt_ratio_x96(-2000)
        liq = liquidity_for_amounts_at_sqrt_price(
            sqrt_p, -1000, 1000, 10**18, 0
        )
        assert liq > 0

    def test_below_range_no_token0_returns_zero(self):
        # Pool below range and only token1 supplied — cannot mint
        sqrt_p = tick_to_sqrt_ratio_x96(-2000)
        assert (
            liquidity_for_amounts_at_sqrt_price(sqrt_p, -1000, 1000, 0, 10**18)
            == 0
        )

    def test_above_range_no_token1_returns_zero(self):
        # Pool above range and only token0 supplied — cannot mint
        sqrt_p = tick_to_sqrt_ratio_x96(2000)
        assert (
            liquidity_for_amounts_at_sqrt_price(sqrt_p, -1000, 1000, 10**18, 0)
            == 0
        )

    def test_in_range_zero_amount0_returns_zero(self):
        # In-range needs both legs; missing one rounds liquidity to 0
        sqrt_p = tick_to_sqrt_ratio_x96(0)
        assert (
            liquidity_for_amounts_at_sqrt_price(sqrt_p, -1000, 1000, 0, 10**18)
            == 0
        )

    def test_in_range_zero_amount1_returns_zero(self):
        sqrt_p = tick_to_sqrt_ratio_x96(0)
        assert (
            liquidity_for_amounts_at_sqrt_price(sqrt_p, -1000, 1000, 10**18, 0)
            == 0
        )

    def test_degenerate_range_returns_zero(self):
        sqrt_p = tick_to_sqrt_ratio_x96(0)
        assert (
            liquidity_for_amounts_at_sqrt_price(sqrt_p, 100, 100, 10**18, 10**18)
            == 0
        )

    def test_out_of_bounds_sqrt_returns_zero(self):
        # sqrtPriceX96 below MIN_SQRT_RATIO classifies as a sentinel; returns 0
        assert liquidity_for_amounts_at_sqrt_price(0, -1000, 1000, 10**18, 10**18) == 0

    def test_steth_weth_tight_range_one_wei_returns_zero(self):
        # Near-1:1 peg with very tight range and token-1 missing.
        # This is the canonical VIB-3823 stETH/WETH M0 reproduction.
        sqrt_p = tick_to_sqrt_ratio_x96(0)
        # 1 wei of token0 alone in a tight in-range slot rounds liquidity to 0
        assert liquidity_for_amounts_at_sqrt_price(sqrt_p, -10, 10, 1, 0) == 0

    def test_steth_weth_tight_range_paired_one_wei_positive(self):
        # When both legs supply at least 1 wei, the in-range branch
        # uses min(L0, L1); for tick spacing 10 that still rounds to a
        # small but POSITIVE liquidity (verified against Solidity in
        # the reference implementation).
        sqrt_p = tick_to_sqrt_ratio_x96(0)
        assert liquidity_for_amounts_at_sqrt_price(sqrt_p, -10, 10, 1, 1) > 0


class TestLpOpenZeroLiquidityError:
    """Typed error for VIB-3823 strategy-side catch."""

    def test_error_message_prefix_stable(self):
        from almanak.framework.intents import LpOpenZeroLiquidityError

        err = LpOpenZeroLiquidityError(
            amount0_desired=1,
            amount1_desired=0,
            tick_lower=-10,
            tick_upper=10,
            reason="test",
        )
        # The string-match contract: strategies use this prefix to catch
        # the error from CompilationResult.error.
        assert str(err).startswith(LpOpenZeroLiquidityError.ERROR_PREFIX)

    def test_attributes_round_trip(self):
        from almanak.framework.intents import LpOpenZeroLiquidityError

        err = LpOpenZeroLiquidityError(
            amount0_desired=42,
            amount1_desired=0,
            tick_lower=-60,
            tick_upper=60,
            reason="why",
        )
        assert err.amount0_desired == 42
        assert err.amount1_desired == 0
        assert err.tick_lower == -60
        assert err.tick_upper == 60
        assert err.reason == "why"
        assert isinstance(err, ValueError)
