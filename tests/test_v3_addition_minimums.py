"""V3 addition floors hold at every price the liquidity price gate admits."""

import random
from math import isqrt

import pytest

from almanak.connectors.concentrated_math import (
    addition_minimums,
    amounts,
    liquidity_for,
    sqrt_at_tick,
)

# Robinhood SPCX/WETH 0.05% mint 0x13b10df9…f995e: planned near tick 27420, mined at
# 27418 after third-party swaps, and reverted on the position manager's slippage check.
LOWER, UPPER = sqrt_at_tick(27220), sqrt_at_tick(27620)
WETH, SPCX = 10**15, 15505160619593558
PLANNED = sqrt_at_tick(27420)
MINED = 312057555214697929445502953778
PULLED = (10**15, 15281747373274148)


def pulled(maximum0, maximum1, price, lower, upper):
    """What the position manager takes: liquidity from the maxima, amounts rounded up."""
    liquidity = liquidity_for(maximum0, maximum1, price, lower, upper)
    return liquidity, amounts(liquidity, price, lower, upper, up=True)


def test_mined_robinhood_mint_meets_window_floors_that_observed_price_floors_refused():
    assert pulled(WETH, SPCX, MINED, LOWER, UPPER)[1] == PULLED
    planned = liquidity_for(WETH, SPCX, PLANNED, LOWER, UPPER)
    observed_floor = amounts(planned, PLANNED, LOWER, UPPER, up=True)[1] * 9900 // 10000
    assert PULLED[1] < observed_floor

    minimum0, minimum1, minimum_liquidity = addition_minimums(
        WETH, SPCX, PLANNED, LOWER, UPPER, 100
    )
    assert minimum0 <= PULLED[0] and minimum1 <= PULLED[1]
    assert minimum_liquidity <= liquidity_for(WETH, SPCX, MINED, LOWER, UPPER)


def window(price, tolerance_bps):
    """Prices just inside v3_pool_validation.price_movement for `price` and tolerance."""
    old = price * price
    inside = []
    for share in (-10000, -5000, -100, 0, 100, 5000, 10000):
        candidate = isqrt(old + old * tolerance_bps * share // 10**8)
        for nearby in (candidate - 1, candidate, candidate + 1, candidate + 2):
            if abs(nearby * nearby - old) * 10000 <= old * tolerance_bps:
                inside.append(nearby)
    return inside


@pytest.mark.parametrize("seed", range(200))
def test_floors_hold_across_the_admitted_price_window(seed):
    rng = random.Random(seed)
    spacing = rng.choice((1, 10, 60, 200))
    centre = rng.randrange(-200000, 200000) // spacing * spacing
    half = rng.randrange(1, 1000) * spacing
    lower, upper = sqrt_at_tick(centre - half), sqrt_at_tick(centre + half)
    tick = centre + rng.randrange(-2 * half, 2 * half + 1)
    price = sqrt_at_tick(max(-887272, min(887272, tick)))
    tolerance = rng.choice((0, 1, 10, 50, 100, 500, 2000))
    maximum0, maximum1 = rng.randrange(1, 10**24), rng.randrange(1, 10**24)

    minimum0, minimum1, minimum_liquidity = addition_minimums(
        maximum0, maximum1, price, lower, upper, tolerance
    )
    assert minimum_liquidity <= liquidity_for(maximum0, maximum1, price, lower, upper)
    for execution in window(price, tolerance):
        liquidity, (amount0, amount1) = pulled(maximum0, maximum1, execution, lower, upper)
        assert liquidity >= minimum_liquidity
        assert amount0 >= minimum0 and amount1 >= minimum1


def test_out_of_range_floors_take_only_the_held_token():
    below = sqrt_at_tick(27000)
    minimum0, minimum1, _ = addition_minimums(WETH, SPCX, below, LOWER, UPPER, 100)
    assert minimum1 == 0 and 0 < minimum0 <= WETH
    above = sqrt_at_tick(27900)
    minimum0, minimum1, _ = addition_minimums(WETH, SPCX, above, LOWER, UPPER, 100)
    assert minimum0 == 0 and 0 < minimum1 <= SPCX


def test_tolerance_must_be_basis_points():
    with pytest.raises(ValueError, match="basis points"):
        addition_minimums(WETH, SPCX, PLANNED, LOWER, UPPER, 10000)
