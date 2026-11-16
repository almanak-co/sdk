"""Reconstruct GM valuation from raw accounting inputs using the deployed integer order."""

from almanak.connectors.gmx_v2.economic_position_math import signed_div
from almanak.connectors.gmx_v2.economic_precision import FLOAT, apply_exponent, apply_factor
from almanak.connectors.gmx_v2.economic_valuation_types import PoolValue


def balance_interest(state, side):
    if state.use_open_interest_tokens_for_balance:
        return side.open_interest_tokens * state.index_price.midpoint
    return side.open_interest_usd_e30


def kink_borrowing_rate(side, reserved_usd, pool_usd):
    capacity = apply_factor(pool_usd, side.reserve_factor)
    if not capacity:
        raise ValueError("GM borrowing utilization has no reserve capacity")
    usage = reserved_usd * FLOAT // capacity
    rate = apply_factor(usage, side.base_factor)
    if usage > side.optimal_usage_factor and FLOAT > side.optimal_usage_factor:
        additional = max(0, side.above_optimal_factor - side.base_factor)
        rate += (
            additional * (usage - side.optimal_usage_factor) // (FLOAT - side.optimal_usage_factor)
        )
    return rate


def borrowing_rate(state, is_long):
    side = state.long if is_long else state.short
    opposite = state.short if is_long else state.long
    currency = state.spot.long if is_long else state.spot.short
    reserved = (
        side.open_interest_tokens * state.index_price.maximum
        if is_long
        else side.open_interest_usd_e30
    )
    if not reserved:
        return 0
    if state.skip_borrowing_for_smaller_side and balance_interest(state, side) < balance_interest(
        state, opposite
    ):
        return 0
    pool_usd = currency.pool_amount * currency.price.minimum
    if not pool_usd:
        raise ValueError("GM borrowing rate requires positive pool inventory")
    if side.optimal_usage_factor:
        return kink_borrowing_rate(side, reserved, pool_usd)
    ratio = apply_exponent(reserved, side.exponent) * FLOAT // pool_usd
    return apply_factor(ratio, side.factor)


def next_borrowing_factor(state, is_long):
    side = state.long if is_long else state.short
    duration = state.spot.block.timestamp - side.updated_at if side.updated_at else 0
    return side.cumulative_factor + duration * borrowing_rate(state, is_long)


def pending_borrowing_fees(state, is_long):
    side = state.long if is_long else state.short
    total = apply_factor(side.open_interest_usd_e30, next_borrowing_factor(state, is_long))
    if total < side.total_borrowing_usd_e30:
        raise ValueError("GM aggregate borrowing exceeds the current cumulative entitlement")
    return total - side.total_borrowing_usd_e30


def next_impact_pool(state):
    impact = state.impact
    if not impact.pool_amount or not impact.rate or impact.pool_amount <= impact.minimum_amount:
        return impact.pool_amount
    duration = state.spot.block.timestamp - impact.distributed_at if impact.distributed_at else 0
    distributed = min(
        impact.pool_amount - impact.minimum_amount, apply_factor(duration, impact.rate)
    )
    return impact.pool_amount - distributed


def side_pnl(state, is_long, maximize, pool_usd):
    side = state.long if is_long else state.short
    if not side.open_interest_usd_e30 or not side.open_interest_tokens:
        return 0
    # Pool valuation maximization minimizes traders' PnL, and vice versa.
    price = state.index_price.minimum if is_long == maximize else state.index_price.maximum
    worth = side.open_interest_tokens * price
    pnl = worth - side.open_interest_usd_e30 if is_long else side.open_interest_usd_e30 - worth
    return min(pnl, apply_factor(pool_usd, side.maximum_pnl_factor)) if pnl >= 0 else pnl


def pool_value(state, *, maximize):
    sides = (state.spot.long, state.spot.short)
    usd = tuple(
        side.pool_amount * (side.price.maximum if maximize else side.price.minimum)
        for side in sides
    )
    fees = pending_borrowing_fees(state, True) + pending_borrowing_fees(state, False)
    pool_factor = FLOAT - state.borrowing_fee_receiver_factor
    pnl = tuple(
        side_pnl(state, is_long, maximize, amount)
        for is_long, amount in zip((True, False), usd, strict=True)
    )
    impact = next_impact_pool(state)
    impact_usd = impact * (state.index_price.minimum if maximize else state.index_price.maximum)
    lent_usd = state.impact.lent_amount * (
        state.index_price.maximum if maximize else state.index_price.minimum
    )
    return PoolValue(
        pool_usd_e30=sum(usd) + apply_factor(fees, pool_factor) - sum(pnl) - impact_usd + lent_usd,
        long_pnl_usd_e30=pnl[0],
        short_pnl_usd_e30=pnl[1],
        net_pnl_usd_e30=sum(pnl),
        long_amount=sides[0].pool_amount,
        short_amount=sides[1].pool_amount,
        long_usd_e30=usd[0],
        short_usd_e30=usd[1],
        borrowing_fees_usd_e30=fees,
        borrowing_pool_factor=pool_factor,
        impact_pool_amount=impact,
        lent_impact_amount=state.impact.lent_amount,
    )


def market_token_price(state, value):
    if not state.share_supply:
        return FLOAT
    return signed_div(10**18 * value.pool_usd_e30, state.share_supply)
