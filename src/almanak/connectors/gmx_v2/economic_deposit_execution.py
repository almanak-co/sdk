"""GM deposit execution updates valuation between legs and caps each impact pool once."""

from almanak.connectors.gmx_v2.economic_liquidity import (
    LiquidityState,
    shares_for_usd,
    signed_proportion,
)
from almanak.connectors.gmx_v2.economic_spot_math import fees_for, impact_amount, spot_impact
from almanak.connectors.gmx_v2.economic_spot_types import Fees, SwapConstraint
from almanak.connectors.gmx_v2.economic_valuation import (
    next_borrowing_factor,
    next_impact_pool,
    pool_value,
)
from almanak.connectors.gmx_v2.economic_valuation_types import ValuationState
from almanak.models import Model, UInt


class DepositLeg(Model):
    is_long: bool
    amount: UInt
    minted: UInt
    fees: Fees
    price_impact_usd_e30: int
    input_impact_amount: int
    output_impact_amount: UInt
    after: ValuationState


class DepositExecution(Model):
    shares: UInt
    price_impact_usd_e30: int
    legs: tuple[DepositLeg, ...]
    after: ValuationState


def deposit_leg(state, is_long, amount, impact_usd, improved):
    value = pool_value(state, maximize=True).pool_usd_e30
    if value < 0 or (state.share_supply and not value):
        raise SwapConstraint("GM deposit execution has invalid pool valuation")
    quote = LiquidityState(
        action="DEPOSIT", spot=state.spot, pool_value_usd_e30=value, share_supply=state.share_supply
    )
    incoming = state.spot.long if is_long else state.spot.short
    outgoing = state.spot.short if is_long else state.spot.long
    fees = fees_for(amount, state.spot.fees, improved)
    positive = (
        impact_amount(outgoing, impact_usd)[0] if impact_usd > 0 and state.share_supply else 0
    )
    negative = impact_amount(incoming, impact_usd)[0] if impact_usd < 0 else 0
    net = fees.after_fees + negative
    if net < 0:
        raise SwapConstraint("GM deposit execution impact exceeds its incoming amount")
    # ExecuteDepositUtils has no Reader fallback to the input impact pool for positive impact.
    minted = shares_for_usd(quote, net * incoming.price.minimum)
    if positive:
        minted += shares_for_usd(quote, positive * outgoing.price.maximum)
    incoming = incoming.model_copy(
        update={
            "pool_amount": incoming.pool_amount + net + fees.pool_amount,
            "impact_pool_amount": incoming.impact_pool_amount - negative,
        }
    )
    outgoing = outgoing.model_copy(
        update={
            "pool_amount": outgoing.pool_amount + positive,
            "impact_pool_amount": outgoing.impact_pool_amount - positive,
        }
    )
    spot = state.spot.model_copy(
        update={
            "long": incoming if is_long else outgoing,
            "short": outgoing if is_long else incoming,
        }
    )
    after = state.model_copy(update={"spot": spot, "share_supply": state.share_supply + minted})
    return DepositLeg(
        is_long=is_long,
        amount=amount,
        minted=minted,
        fees=fees,
        price_impact_usd_e30=impact_usd,
        input_impact_amount=negative,
        output_impact_amount=positive,
        after=after,
    )


def execute_deposit(state, long_amount, short_amount):
    if state.spot.pricing_type != "DEPOSIT":
        raise ValueError("GM deposit execution requires deposit-specific accounting")
    if min(long_amount, short_amount) < 0 or long_amount + short_amount <= 0:
        raise ValueError("GM deposit execution requires nonnegative amounts and a positive total")
    usd = (
        long_amount * state.spot.long.price.midpoint,
        short_amount * state.spot.short.price.midpoint,
    )
    impact = spot_impact(state.spot, usd)
    current, legs = settled_accounting(state), []
    for is_long, amount, value in zip((True, False), (long_amount, short_amount), usd, strict=True):
        if amount:
            leg = deposit_leg(
                current,
                is_long,
                amount,
                signed_proportion(impact.usd_e30, value, sum(usd)),
                impact.balance_improved,
            )
            legs.append(leg)
            current = leg.after
    return DepositExecution(
        shares=sum(leg.minted for leg in legs),
        price_impact_usd_e30=impact.usd_e30,
        legs=tuple(legs),
        after=current,
    )


def settled_accounting(state):
    # The executor updates cumulative borrowing and distributes impact before either leg.
    # Later pool changes must not recompute elapsed borrowing against their new utilization.
    sides = {
        name: getattr(state, name).model_copy(
            update={
                "cumulative_factor": next_borrowing_factor(state, is_long),
                "updated_at": state.spot.block.timestamp,
            }
        )
        for name, is_long in (("long", True), ("short", False))
    }
    return state.model_copy(
        update={
            **sides,
            "impact": state.impact.model_copy(
                update={
                    "pool_amount": next_impact_pool(state),
                    "distributed_at": state.spot.block.timestamp,
                }
            ),
        }
    )
