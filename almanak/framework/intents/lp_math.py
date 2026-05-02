"""Uniswap V3 liquidity math helpers for LP amount recomputation.

These functions implement the core Uniswap V3 math from:
  - LiquidityAmounts.sol: getLiquidityForAmounts / getAmountsForLiquidity
  - SqrtPriceMath.sol: getAmount0Delta / getAmount1Delta

Primary use case: recompute LP amount0/amount1 using the pool's on-chain
sqrtPriceX96 so that the amounts match the pool's expected ratio, preventing
the "Price slippage check" revert that occurs when oracle price != pool price.
"""

from __future__ import annotations

import logging
from decimal import Decimal, localcontext

logger = logging.getLogger(__name__)

_Q96 = 2**96

# Uniswap V3 tick bounds (from TickMath.sol)
MIN_TICK = -887272
MAX_TICK = 887272

# sqrtPriceX96 bounds (from TickMath.sol)
MIN_SQRT_RATIO = 4295128739
MAX_SQRT_RATIO = 1461446703485210103287273052203988822378723970342


def tick_to_sqrt_ratio_x96(tick: int) -> int:
    """Convert a Uniswap V3 tick to sqrtRatioX96.

    sqrtRatioX96 = sqrt(1.0001^tick) * 2^96

    Uses Python Decimal with 50-digit precision for exact computation,
    avoiding IEEE 754 float precision loss at extreme ticks.
    """
    if tick < MIN_TICK or tick > MAX_TICK:
        raise ValueError(f"Tick {tick} out of Uniswap V3 range [{MIN_TICK}, {MAX_TICK}]")

    with localcontext() as ctx:
        ctx.prec = 50
        base = Decimal("1.0001")
        sqrt_val = base ** Decimal(tick) if tick >= 0 else Decimal(1) / base ** Decimal(-tick)
        sqrt_val = sqrt_val.sqrt()
        return int(sqrt_val * Decimal(_Q96))


def recompute_lp_amounts(
    sqrt_price_x96: int,
    tick_lower: int,
    tick_upper: int,
    amount0_desired: int,
    amount1_desired: int,
    current_tick: int | None = None,
) -> tuple[int, int]:
    """Recompute LP deposit amounts to match the pool's on-chain sqrtPriceX96.

    When a strategy computes amount0 and amount1 from an oracle price that
    diverges from the pool's actual price, Uniswap V3's NonfungiblePositionManager
    reverts with "Price slippage check" because the actual amounts taken by the
    pool (based on sqrtPriceX96) fall below the minimum amounts.

    This function applies the same math as Uniswap V3's getLiquidityForAmounts +
    getAmountsForLiquidity to compute amounts that exactly match the pool's ratio:
      1. Compute maximum liquidity achievable from (amount0_desired, amount1_desired).
      2. Back-compute the exact amounts the pool will use for that liquidity.

    One token is fully consumed (the "limiting factor"); the other may be less
    than desired. The returned amounts satisfy:
      actual0 <= amount0_desired and actual1 <= amount1_desired

    Args:
        sqrt_price_x96: Pool's current sqrtPriceX96 from slot0().
        tick_lower: Lower tick of the LP position range.
        tick_upper: Upper tick of the LP position range.
        amount0_desired: Desired amount of token0 in wei (from oracle price).
        amount1_desired: Desired amount of token1 in wei (from oracle price).
        current_tick: Pool's current tick from slot0(). Accepted for API
            compatibility but not used for branch classification — sqrtPriceX96
            comparisons are authoritative per Uniswap V3 LiquidityAmounts.sol.

    Returns:
        (amount0_corrected, amount1_corrected) in wei, matching pool's ratio.
        Returns the original (amount0_desired, amount1_desired) if computation
        fails (e.g., zero liquidity, degenerate range).
    """
    # Normalize tick order
    tick_lo = min(tick_lower, tick_upper)
    tick_hi = max(tick_lower, tick_upper)

    # Validate tick bounds
    if tick_lo < MIN_TICK or tick_hi > MAX_TICK:
        logger.warning("Tick range [%d, %d] out of bounds — skipping amount recomputation", tick_lower, tick_upper)
        return amount0_desired, amount1_desired

    sqrt_a = tick_to_sqrt_ratio_x96(tick_lo)
    sqrt_b = tick_to_sqrt_ratio_x96(tick_hi)

    if sqrt_a == sqrt_b or sqrt_b == 0:
        logger.warning("Degenerate tick range [%d, %d] — skipping amount recomputation", tick_lower, tick_upper)
        return amount0_desired, amount1_desired

    # Validate sqrtPriceX96 is in sane range
    if sqrt_price_x96 < MIN_SQRT_RATIO or sqrt_price_x96 > MAX_SQRT_RATIO:
        logger.warning(
            "sqrtPriceX96 %d out of valid range [%d, %d] — skipping recomputation",
            sqrt_price_x96,
            MIN_SQRT_RATIO,
            MAX_SQRT_RATIO,
        )
        return amount0_desired, amount1_desired

    # Classify using sqrtPriceX96 comparisons (matching Uniswap V3 LiquidityAmounts.sol).
    # slot0.tick can lag behind the true tick at exact boundary crossings, so sqrt-price
    # comparison is the authoritative classification method.
    price_below_range = sqrt_price_x96 <= sqrt_a
    price_above_range = sqrt_price_x96 >= sqrt_b

    # Clamp pool price to the position range for in-range math
    sqrt_p = max(sqrt_a, min(sqrt_b, sqrt_price_x96))

    # getLiquidityForAmounts
    try:
        if price_below_range:
            # Price is below range: position holds only token0
            liquidity = _liquidity_for_amount0(sqrt_a, sqrt_b, amount0_desired)
        elif price_above_range:
            # Price is above range: position holds only token1
            liquidity = _liquidity_for_amount1(sqrt_a, sqrt_b, amount1_desired)
        else:
            # Price is in range: mixed position — use the more restrictive token
            l0 = _liquidity_for_amount0(sqrt_p, sqrt_b, amount0_desired)
            l1 = _liquidity_for_amount1(sqrt_a, sqrt_p, amount1_desired)
            liquidity = min(l0, l1)
    except ZeroDivisionError:
        logger.warning("ZeroDivisionError in getLiquidityForAmounts — skipping recomputation")
        return amount0_desired, amount1_desired

    if liquidity <= 0:
        # Zero liquidity means the supplied token(s) cannot mint in this range at the
        # current price (e.g., price above range but only token0 supplied). Return (0, 0)
        # so the compiler's "both corrected amounts are zero" guard can fail fast rather
        # than forwarding the original (unmintable) amounts to the pool.
        return 0, 0

    # getAmountsForLiquidity — clamp to desired amounts to prevent exceeding approvals
    try:
        if price_below_range:
            a0 = min(_amount0_for_liquidity(sqrt_a, sqrt_b, liquidity), amount0_desired)
            return a0, 0
        elif price_above_range:
            a1 = min(_amount1_for_liquidity(sqrt_a, sqrt_b, liquidity), amount1_desired)
            return 0, a1
        else:
            a0 = min(_amount0_for_liquidity(sqrt_p, sqrt_b, liquidity), amount0_desired)
            a1 = min(_amount1_for_liquidity(sqrt_a, sqrt_p, liquidity), amount1_desired)
            return a0, a1
    except ZeroDivisionError:
        logger.warning("ZeroDivisionError in getAmountsForLiquidity — skipping recomputation")
        return amount0_desired, amount1_desired


# ---------------------------------------------------------------------------
# Private math primitives (matching Uniswap V3 SqrtPriceMath.sol)
# Uses full-precision integer arithmetic to avoid rounding loss from
# intermediate floor divisions.
# ---------------------------------------------------------------------------


def liquidity_for_amounts_at_sqrt_price(
    sqrt_price_x96: int,
    tick_lower: int,
    tick_upper: int,
    amount0: int,
    amount1: int,
) -> int:
    """Compute the liquidity Uniswap V3 ``mint()`` will use for the given amounts.

    Mirrors Solidity's ``LiquidityAmounts.getLiquidityForAmounts``: classifies
    the pool's current ``sqrtPriceX96`` against the position range and returns
    the maximum liquidity the position can hold without exceeding either
    ``amount0`` or ``amount1``. Returns ``0`` when the math floors to zero.

    Used by the LP_OPEN compile-time pre-flight (VIB-3823) to short-circuit
    before submitting a mint() that would revert with ``M0`` ("liquidity
    must be greater than zero"). Tight ranges on near-1:1 pairs and very
    small amounts both round to zero liquidity even when the input
    amounts are non-zero.

    Args:
        sqrt_price_x96: The reference ``sqrtPriceX96`` to classify the
            position against. When the live pool slot0 is unavailable,
            callers should pass the geometric range midpoint
            (``isqrt(sqrt_a * sqrt_b)``) so the in-range branch exercises
            the most permissive math.
        tick_lower: Lower tick of the LP range (spacing-aligned).
        tick_upper: Upper tick of the LP range (spacing-aligned).
        amount0: ``amount0_desired`` in wei.
        amount1: ``amount1_desired`` in wei.

    Returns:
        Maximum liquidity attainable from the supplied amounts, or 0 when
        the result floors to zero / inputs are degenerate.
    """
    tick_lo = min(tick_lower, tick_upper)
    tick_hi = max(tick_lower, tick_upper)
    if tick_lo < MIN_TICK or tick_hi > MAX_TICK or tick_lo == tick_hi:
        return 0

    sqrt_a = tick_to_sqrt_ratio_x96(tick_lo)
    sqrt_b = tick_to_sqrt_ratio_x96(tick_hi)
    if sqrt_a >= sqrt_b:
        return 0
    if sqrt_price_x96 < MIN_SQRT_RATIO or sqrt_price_x96 > MAX_SQRT_RATIO:
        return 0

    try:
        if sqrt_price_x96 <= sqrt_a:
            # Below range — liquidity comes entirely from token0
            return _liquidity_for_amount0(sqrt_a, sqrt_b, amount0)
        if sqrt_price_x96 >= sqrt_b:
            # Above range — liquidity comes entirely from token1
            return _liquidity_for_amount1(sqrt_a, sqrt_b, amount1)
        # In range — both legs participate; minimum bounds the position
        sqrt_p = sqrt_price_x96
        l0 = _liquidity_for_amount0(sqrt_p, sqrt_b, amount0)
        l1 = _liquidity_for_amount1(sqrt_a, sqrt_p, amount1)
        return min(l0, l1)
    except ZeroDivisionError:
        return 0


def range_midpoint_sqrt_price_x96(tick_lower: int, tick_upper: int) -> int:
    """Geometric midpoint sqrtPriceX96 for a position range.

    When the live pool slot0 is unavailable, this is the assumption the
    LP_OPEN pre-flight uses for "would-this-mint-zero?" classification.
    Geometric midpoint matches the convention used by Uniswap V3 quote
    helpers and stays in-range for any non-degenerate ``[tick_lower,
    tick_upper]`` pair, so the in-range branch of
    :func:`liquidity_for_amounts_at_sqrt_price` is exercised (the most
    permissive — both legs contribute).
    """
    from math import isqrt

    sqrt_a = tick_to_sqrt_ratio_x96(min(tick_lower, tick_upper))
    sqrt_b = tick_to_sqrt_ratio_x96(max(tick_lower, tick_upper))
    if sqrt_a == 0 or sqrt_b == 0 or sqrt_a == sqrt_b:
        return 0
    return isqrt(sqrt_a * sqrt_b)


def _liquidity_for_amount0(sqrt_a: int, sqrt_b: int, amount0: int) -> int:
    """Compute liquidity achievable from amount0 in range [sqrt_a, sqrt_b].

    From SqrtPriceMath: L = amount0 * sqrtA * sqrtB / (Q96 * (sqrtB - sqrtA))
    Full-precision: single floor division at the end.
    """
    if amount0 == 0 or sqrt_b == sqrt_a:
        return 0
    return amount0 * sqrt_a * sqrt_b // (_Q96 * (sqrt_b - sqrt_a))


def _liquidity_for_amount1(sqrt_a: int, sqrt_b: int, amount1: int) -> int:
    """Compute liquidity achievable from amount1 in range [sqrt_a, sqrt_b].

    From SqrtPriceMath: L = amount1 * Q96 / (sqrtB - sqrtA)
    """
    if amount1 == 0 or sqrt_b == sqrt_a:
        return 0
    return amount1 * _Q96 // (sqrt_b - sqrt_a)


def _amount0_for_liquidity(sqrt_a: int, sqrt_b: int, liquidity: int) -> int:
    """Compute amount0 for liquidity L in range [sqrt_a, sqrt_b].

    From SqrtPriceMath: amount0 = L * Q96 * (sqrtB - sqrtA) / (sqrtB * sqrtA)
    Full-precision: single floor division at the end.
    """
    if sqrt_a == 0 or sqrt_b == 0:
        return 0
    return liquidity * _Q96 * (sqrt_b - sqrt_a) // (sqrt_b * sqrt_a)


def _amount1_for_liquidity(sqrt_a: int, sqrt_b: int, liquidity: int) -> int:
    """Compute amount1 for liquidity L in range [sqrt_a, sqrt_b].

    From SqrtPriceMath: amount1 = L * (sqrtB - sqrtA) / Q96
    """
    return liquidity * (sqrt_b - sqrt_a) // _Q96
