"""Bounded tick traversal shared only across independently verified V3/V4 pool math.

The caller supplies its protocol's fee and integer step. External market history
stays immutable; the result describes one hypothetical swap against this sample.
"""

from dataclasses import dataclass

from almanak.connectors.concentrated_math import (
    amount_delta,
    divide,
    next_price,
    next_tick,
    sqrt_at_tick,
    tick_at_price,
)
from almanak.connectors.dex_residual import QuoteUnfillable


@dataclass
class SwapCalculation:
    output: int
    sqrt_price_x96: int
    tick: int
    liquidity: int
    fee_paid: int = 0
    crossed_ticks: tuple[int, ...] = ()
    steps: int = 0


COVERAGE_REFUSAL = "swap requires liquidity outside prepared tick coverage"


def coverage_refusal(sample, amount, remaining, zero_for_one):
    """The refusal of a swap the prepared ticks cannot fill, with the coverage and the cause.

    The text is a function of the sample and the input alone, so a replayed observation
    that records it as an unmeasured entry quote reproduces it.
    """
    bound = "lower" if zero_for_one else "upper"
    return (
        f"{COVERAGE_REFUSAL} [{sample.lower}, {sample.upper}]: the price moved from tick "
        f"{sample.pool.tick} to the {bound} bound with {remaining} of {amount} input base "
        "units unfilled; widen the prepared tick coverage or reduce the swap"
    )


def cross(result, next_value, initialized, ticks, zero_for_one):
    if initialized:
        if next_value not in ticks:
            raise ValueError("swap requires unprepared initialized tick liquidity")
        change = ticks[next_value]
        result.liquidity += -change if zero_for_one else change
        if not 0 <= result.liquidity < 2**128:
            raise ValueError("tick crossing produces invalid uint128 liquidity")
        result.crossed_ticks += (next_value,)
    result.tick = next_value - 1 if zero_for_one else next_value


def exact_input(sample, amount, zero_for_one, fee, step):
    if type(amount) is not int or not 0 < amount < 2**127:
        raise ValueError("economic swap requires a positive supported int128 input")
    pool = sample.pool
    words = {w.index: w.bitmap for w in sample.words}
    ticks = {t.index: t.net for t in sample.ticks}
    limit = (
        max(sqrt_at_tick(sample.lower), sqrt_at_tick(-887272) + 1)
        if zero_for_one
        else min(sqrt_at_tick(sample.upper), sqrt_at_tick(887272) - 1)
    )
    result = SwapCalculation(0, pool.sqrt_price_x96, pool.tick, pool.liquidity)
    remaining = amount
    while remaining:
        if result.sqrt_price_x96 == limit:
            # The prepared ticks cannot fill the input: a market fact about this sample,
            # which an entry observation records as unmeasured instead of failing.
            raise QuoteUnfillable(coverage_refusal(sample, amount, remaining, zero_for_one))
        remaining = advance(
            result,
            remaining,
            limit,
            fee,
            pool.source.tick_spacing,
            words,
            ticks,
            zero_for_one,
            step,
        )
    # The loop consumed the whole input. A zero output is a measured result (the
    # input rounds away in fees and price math), not missing liquidity, which the
    # coverage limit above refuses; callers decide what a zero output permits.
    if result.output >= 2**127:
        raise ValueError("economic quote output exceeds the supported int128 range")
    return result


def advance(result, remaining, limit, fee, spacing, words, ticks, direction, step):
    tick, initialized = next_tick(words, result.tick, spacing, direction)
    boundary = sqrt_at_tick(tick)
    target = max(boundary, limit) if direction else min(boundary, limit)
    price, used, output, paid = step(
        result.sqrt_price_x96, target, result.liquidity, remaining, fee
    )
    result.sqrt_price_x96 = price
    result.output += output
    result.fee_paid += paid
    result.steps += 1
    if price == boundary:
        cross(result, tick, initialized, ticks, direction)
    else:
        result.tick = tick_at_price(price)
    return remaining - used - paid


def v3_step(price, target, liquidity, remaining, fee):
    if not 0 <= fee < 1_000_000 or not 0 <= remaining < 2**255:
        raise ValueError("V3 exact-input step requires valid fee and nonnegative int256 input")
    direction = price >= target
    usable = remaining * (1_000_000 - fee) // 1_000_000
    required = amount_delta(price, target, liquidity, direction, up=True)
    if usable >= required:
        updated = target
    else:
        updated = next_price(price, liquidity, usable, direction)
        required = amount_delta(price, updated, liquidity, direction, up=True)
    output = amount_delta(price, updated, liquidity, not direction, up=False)
    paid = (
        divide(required * fee, 1_000_000 - fee, True) if updated == target else remaining - required
    )
    return updated, required, output, paid
