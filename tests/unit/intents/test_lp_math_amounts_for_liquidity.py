"""``amounts_for_liquidity`` mirrors ``LiquidityAmounts.getAmountsForLiquidity`` for a burn."""

from __future__ import annotations

import pytest

from almanak.framework.intents.lp_math import (
    amounts_for_liquidity,
    liquidity_for_amounts_at_sqrt_price,
    tick_to_sqrt_ratio_x96,
)

Q96 = 2**96
TICK_LOWER, TICK_UPPER = -5000, 5000
L = 10**18


def test_in_range_split_round_trips_through_the_mint_liquidity_formula() -> None:
    sqrt_p = tick_to_sqrt_ratio_x96(120)
    amount0, amount1 = amounts_for_liquidity(sqrt_p, TICK_LOWER, TICK_UPPER, L)

    assert amount0 > 0 and amount1 > 0
    recovered = liquidity_for_amounts_at_sqrt_price(sqrt_p, TICK_LOWER, TICK_UPPER, amount0, amount1)
    # Both legs round down, so the recovered liquidity never overshoots; the gap is wei-scale rounding.
    assert 0 <= L - recovered <= 10


@pytest.mark.parametrize("tick", [TICK_LOWER - 1, TICK_LOWER - 3000])
def test_below_range_holds_only_token0(tick: int) -> None:
    amount0, amount1 = amounts_for_liquidity(tick_to_sqrt_ratio_x96(tick), TICK_LOWER, TICK_UPPER, L)
    assert amount0 > 0 and amount1 == 0


@pytest.mark.parametrize("tick", [TICK_UPPER + 1, TICK_UPPER + 3000])
def test_above_range_holds_only_token1(tick: int) -> None:
    amount0, amount1 = amounts_for_liquidity(tick_to_sqrt_ratio_x96(tick), TICK_LOWER, TICK_UPPER, L)
    assert amount0 == 0 and amount1 > 0


def test_range_bounds_are_classified_like_the_contract() -> None:
    """``<= sqrtA`` is all token0 and ``>= sqrtB`` is all token1, on the boundary itself."""
    at_lower = amounts_for_liquidity(tick_to_sqrt_ratio_x96(TICK_LOWER), TICK_LOWER, TICK_UPPER, L)
    at_upper = amounts_for_liquidity(tick_to_sqrt_ratio_x96(TICK_UPPER), TICK_LOWER, TICK_UPPER, L)
    assert at_lower[1] == 0 and at_lower[0] > 0
    assert at_upper[0] == 0 and at_upper[1] > 0
    assert at_lower[0] == amounts_for_liquidity(tick_to_sqrt_ratio_x96(TICK_LOWER - 100), TICK_LOWER, TICK_UPPER, L)[0]


def test_legs_are_monotone_in_price() -> None:
    ticks = range(TICK_LOWER - 200, TICK_UPPER + 201, 400)
    series = [amounts_for_liquidity(tick_to_sqrt_ratio_x96(t), TICK_LOWER, TICK_UPPER, L) for t in ticks]
    amount0s = [a0 for a0, _ in series]
    amount1s = [a1 for _, a1 in series]
    assert amount0s == sorted(amount0s, reverse=True)
    assert amount1s == sorted(amount1s)


def test_both_legs_round_down() -> None:
    sqrt_p = tick_to_sqrt_ratio_x96(7)
    sqrt_a, sqrt_b = tick_to_sqrt_ratio_x96(TICK_LOWER), tick_to_sqrt_ratio_x96(TICK_UPPER)
    amount0, amount1 = amounts_for_liquidity(sqrt_p, TICK_LOWER, TICK_UPPER, L)
    assert amount0 * sqrt_p * sqrt_b <= L * Q96 * (sqrt_b - sqrt_p) < (amount0 + 1) * sqrt_p * sqrt_b
    assert amount1 * Q96 <= L * (sqrt_p - sqrt_a) < (amount1 + 1) * Q96


def test_zero_liquidity_is_zero_on_both_legs() -> None:
    assert amounts_for_liquidity(Q96, TICK_LOWER, TICK_UPPER, 0) == (0, 0)


@pytest.mark.parametrize(
    ("sqrt_p", "tick_lower", "tick_upper", "liquidity"),
    [
        (Q96, 10, 10, L),
        (Q96, 10, -10, L),
        (0, TICK_LOWER, TICK_UPPER, L),
        (Q96, TICK_LOWER, TICK_UPPER, -1),
        (Q96, -900000, TICK_UPPER, L),
    ],
    ids=("empty-range", "inverted-range", "zero-price", "negative-liquidity", "tick-out-of-bounds"),
)
def test_unmodellable_inputs_raise_instead_of_guessing(
    sqrt_p: int, tick_lower: int, tick_upper: int, liquidity: int
) -> None:
    with pytest.raises(ValueError):
        amounts_for_liquidity(sqrt_p, tick_lower, tick_upper, liquidity)
