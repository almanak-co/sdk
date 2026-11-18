"""Position impact and execution prices follow the verified GMX Reader rounding."""

from almanak.connectors.gmx_v2.economic_position_types import (
    PositionExecutionPrice,
    RealizedImpact,
)
from almanak.connectors.gmx_v2.economic_precision import apply_factor, ceil_div
from almanak.connectors.gmx_v2.economic_spot_math import impact_curve
from almanak.connectors.gmx_v2.economic_spot_types import Impact


def signed_div(value, divisor):
    if divisor <= 0:
        raise ValueError("GMX signed division requires a positive divisor")
    return (1 if value >= 0 else -1) * (abs(value) // divisor)


def position_imbalance(before, after, parameters):
    initial, following = abs(before[0] - before[1]), abs(after[0] - after[1])
    improved = following < initial
    positive_factor = min(parameters.positive_factor, parameters.negative_factor)
    positive_exponent = min(parameters.positive_exponent, parameters.negative_exponent)
    if (before[0] <= before[1]) == (after[0] <= after[1]):
        factor = positive_factor if improved else parameters.negative_factor
        exponent = positive_exponent if improved else parameters.negative_exponent
        magnitude = abs(
            impact_curve(initial, factor, exponent) - impact_curve(following, factor, exponent)
        )
        impact = magnitude if improved else -magnitude
    else:
        impact = impact_curve(initial, positive_factor, positive_exponent) - impact_curve(
            following, parameters.negative_factor, parameters.negative_exponent
        )
    return Impact(usd_e30=impact, balance_improved=improved)


def next_interest(before, delta, is_long):
    # The Reader saturates rounded single-token interest; execution admission is separate.
    long, short = before
    return (max(0, long + delta), short) if is_long else (long, max(0, short + delta))


def virtual_interest(inventory, delta):
    offset = max(0, -delta)
    return max(0, -inventory) + offset, max(0, inventory) + offset


def position_impact(state, usd_delta, token_delta, is_long):
    multiplier = state.index_price.midpoint if state.use_open_interest_in_tokens else 1
    delta = token_delta * multiplier if state.use_open_interest_in_tokens else usd_delta
    before = state.long_open_interest * multiplier, state.short_open_interest * multiplier
    actual = position_imbalance(before, next_interest(before, delta, is_long), state.impact)
    if actual.usd_e30 >= 0 or state.virtual_inventory is None:
        return actual
    virtual = virtual_interest(state.virtual_inventory * multiplier, delta)
    alternative = position_imbalance(virtual, next_interest(virtual, delta, is_long), state.impact)
    return alternative if alternative.usd_e30 < actual.usd_e30 else actual


def capped_impact(state, usd_delta, token_delta, is_long):
    impact = position_impact(state, usd_delta, token_delta, is_long)
    ceiling = apply_factor(abs(usd_delta), state.impact.maximum_positive_factor)
    return min(impact.usd_e30, ceiling), impact.balance_improved


def increase_price(state, size_delta_usd, is_long):
    if size_delta_usd < 0:
        raise ValueError("GMX position increase size must not be negative")
    price = state.index_price.maximum if is_long else state.index_price.minimum
    if size_delta_usd == 0:
        return PositionExecutionPrice(
            price_impact_usd_e30=0,
            price_impact_amount=0,
            base_size_delta_tokens=0,
            execution_price=price,
            balance_improved=False,
        )
    base = size_delta_usd // price if is_long else ceil_div(size_delta_usd, price)
    impact, improved = capped_impact(state, size_delta_usd, base, is_long)
    amount = (
        impact // state.index_price.maximum
        if impact > 0
        else -ceil_div(-impact, state.index_price.minimum)
    )
    adjusted = base + amount if is_long else base - amount
    if adjusted <= 0:
        raise ValueError("GMX price impact leaves no positive position token size")
    return PositionExecutionPrice(
        price_impact_usd_e30=impact,
        price_impact_amount=amount,
        base_size_delta_tokens=base,
        execution_price=size_delta_usd // adjusted,
        balance_improved=improved,
    )


def decrease_tokens(position_usd, position_tokens, delta_usd, is_long):
    if not 0 <= delta_usd <= position_usd or position_usd <= 0 or position_tokens <= 0:
        raise ValueError("GMX decrease requires a positive position and bounded size")
    product = position_tokens * delta_usd
    return ceil_div(product, position_usd) if is_long else product // position_usd


def decrease_price(state, position_usd, position_tokens, delta_usd, is_long):
    tokens = decrease_tokens(position_usd, position_tokens, delta_usd, is_long)
    price = state.index_price.minimum if is_long else state.index_price.maximum
    if delta_usd == 0:
        return PositionExecutionPrice(
            price_impact_usd_e30=0,
            price_impact_amount=0,
            base_size_delta_tokens=0,
            execution_price=price,
            balance_improved=False,
        )
    impact, improved = capped_impact(state, -delta_usd, -tokens, is_long)
    adjusted = impact if is_long else -impact
    if adjusted < -delta_usd:
        raise ValueError("GMX price impact exceeds position decrease size")
    execution = price + signed_div(signed_div(position_usd * adjusted, position_tokens), delta_usd)
    if execution < 0:
        raise ValueError("GMX decrease execution price is negative")
    return PositionExecutionPrice(
        price_impact_usd_e30=impact,
        price_impact_amount=0,
        base_size_delta_tokens=tokens,
        execution_price=execution,
        balance_improved=improved,
    )


def pending_impact_usd(price, position_usd, pending_amount, delta_usd):
    if not 0 <= delta_usd <= position_usd or position_usd <= 0:
        raise ValueError("GMX pending impact requires a bounded position decrease")
    product = abs(pending_amount) * delta_usd
    amount = -ceil_div(product, position_usd) if pending_amount < 0 else product // position_usd
    return amount * (price.minimum if amount > 0 else price.maximum)


def positive_impact_capacity(state):
    price = state.pricing.index_price
    maximum = min(
        apply_factor(state.pool_usd_e30, state.maximum_lendable_factor),
        state.maximum_lendable_usd_e30,
    )
    available = max(0, maximum - state.lent_impact_amount * price.maximum)
    return state.impact_pool_amount * price.minimum + available


def realized_impact(state, position_usd, pending_amount, delta_usd, price_impact_usd):
    pending = pending_impact_usd(state.pricing.index_price, position_usd, pending_amount, delta_usd)
    total = pending + price_impact_usd
    floor = -apply_factor(delta_usd, state.maximum_negative_factor)
    difference = max(0, floor - total)
    total = max(total, floor)
    total = min(total, apply_factor(delta_usd, state.pricing.impact.maximum_positive_factor))
    total = min(total, positive_impact_capacity(state))
    return RealizedImpact(
        proportional_pending_usd_e30=pending,
        total_usd_e30=total,
        claimable_difference_usd_e30=difference,
    )
