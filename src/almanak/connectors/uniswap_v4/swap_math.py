"""Exact-input integer steps from V4 SwapMath, SqrtPriceMath and TickBitmap.

Semantics follow v4-core 46c6834698c48bc4a463a86d8420f4eb1d7f3b75 (MIT).
The retained contract comparisons establish applicability to deployed pools.
"""

from almanak.connectors.concentrated_math import (
    amount_delta as amount_delta,
)
from almanak.connectors.concentrated_math import (
    divide,
)
from almanak.connectors.concentrated_math import (
    next_price as next_price,
)
from almanak.connectors.concentrated_math import (
    next_tick as next_tick,
)
from almanak.connectors.concentrated_math import (
    tick_at_price as tick_at_price,
)

PIPS = 1_000_000


def swap_fee(lp_fee, protocol_fee, zero_for_one):
    if not 0 <= protocol_fee < 2**24 or not 0 <= lp_fee <= PIPS:
        raise ValueError("invalid pool fee encoding")
    forward, reverse = protocol_fee & 4095, protocol_fee >> 12
    if max(forward, reverse) > 1000:
        raise ValueError("protocol fee exceeds the V4 directional bound")
    protocol = forward if zero_for_one else reverse
    return lp_fee + protocol - lp_fee * protocol // PIPS


def swap_step(price, target, liquidity, remaining, fee):
    direction = price >= target
    usable = remaining * (PIPS - fee) // PIPS
    required = amount_delta(price, target, liquidity, direction, up=True)
    if usable >= required:
        updated = target
        paid = required if fee == PIPS else divide(required * fee, PIPS - fee, True)
    else:
        required = usable
        updated = next_price(price, liquidity, usable, direction)
        paid = remaining - usable
    output = amount_delta(price, updated, liquidity, not direction, up=False)
    return updated, required, output, paid


def exact_input(sample, amount, zero_for_one):
    """A reviewed hook's sampled terms set the LP fee override or reduce the output."""
    from almanak.connectors.concentrated_swap import exact_input as calculate

    pool, terms = sample.pool, sample.hook
    lp_fee = pool.lp_fee if terms is None else terms.lp_fee(zero_for_one, pool.lp_fee)
    result = calculate(
        sample, amount, zero_for_one, swap_fee(lp_fee, pool.protocol_fee, zero_for_one), swap_step
    )
    if terms is not None:
        result.output -= terms.take(result.output)
    return result
