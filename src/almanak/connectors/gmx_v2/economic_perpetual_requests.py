"""Prepared position requests escrow collateral and fee, then settle in deployed order.

Settlement reproduces the executor sequence for market increases and decreases:
funding and borrowing accrue first, pricing impact selects the fee policy, fees
are taken from collateral, then reserve, leverage, minimum-size and liquidation
bounds validate the resulting position. Decreases consume profit and collateral
in the deployed cost order and can suppress a withdrawal or expand to a full
close. Every amount is an exact native integer; USD values carry the e30 suffix.
"""

from bisect import bisect_left
from dataclasses import dataclass

from almanak.backtest.types import UnixTime
from almanak.connectors.gmx_v2.abi import input_amount
from almanak.connectors.gmx_v2.economic_accounting import (
    allowances_after_acceptance,
    cumulative_spending,
    inventory_after,
)
from almanak.connectors.gmx_v2.economic_data import accounting_market
from almanak.connectors.gmx_v2.economic_decrease_adjustment import (
    DecreaseAdjustmentInput,
    adjust_decrease,
)
from almanak.connectors.gmx_v2.economic_funding import accumulated_funding
from almanak.connectors.gmx_v2.economic_parameters import wallet
from almanak.connectors.gmx_v2.economic_perpetual_risk import (
    next_collateral_factor,
    validate_increase_reserves,
)
from almanak.connectors.gmx_v2.economic_perpetual_state import (
    PerpetualEscrow,
    SimulatedPosition,
)
from almanak.connectors.gmx_v2.economic_pnl import position_pnl
from almanak.connectors.gmx_v2.economic_position_collateral import (
    DecreaseCollateralInput,
    decrease_collateral,
)
from almanak.connectors.gmx_v2.economic_position_fees import (
    FeeAccumulators,
    PositionFeeInput,
    position_fees,
)
from almanak.connectors.gmx_v2.economic_position_math import (
    decrease_price,
    increase_price,
    position_impact,
    realized_impact,
)
from almanak.connectors.gmx_v2.economic_position_risk import PositionRiskInput, position_risk
from almanak.connectors.gmx_v2.economic_precision import apply_factor, ceil_div
from almanak.connectors.gmx_v2.economic_solvency import validate_token_transfer
from almanak.connectors.gmx_v2.economic_spot_types import SwapConstraint
from almanak.connectors.gmx_v2.economic_valuation import next_borrowing_factor
from almanak.connectors.gmx_v2.intents import (
    ClosePosition,
    DecreasePosition,
    IncreasePosition,
)
from almanak.connectors.gmx_v2.models import Allocation
from almanak.connectors.gmx_v2.planning import economic_order
from almanak.effects import AssetDelta
from almanak.models import AssetAmount

MARKET_INCREASE = 2


@dataclass(frozen=True)
class Settlement:
    position: SimulatedPosition | None
    outputs: tuple[AssetAmount, ...]
    # Funding claims and deferred impact collateral in (long asset, short asset) order.
    claims: tuple[int, int]
    deferred: tuple[int, int]
    execution_price: int
    unpaid_usd_e30: int
    fees_collected: bool


def controls(escrow, risk):
    return risk.increases if escrow.request.order_type == MARKET_INCREASE else risk.decreases


def is_long(target):
    return target.side == "long"


def settlement_prices(opportunity, target):
    """Collateral and profit-token prices; profit settles in the position side's token."""
    spot = opportunity.spot
    collateral = (
        spot.long.price if target.collateral_asset == target.market.long_asset else spot.short.price
    )
    profit = spot.long.price if is_long(target) else spot.short.price
    return collateral, profit


def admit(config, payload, pending, risk):
    intent = pending.intent
    if (
        not isinstance(intent, (IncreasePosition, DecreasePosition, ClosePosition))
        or intent.target != payload.target
        or payload.escrow is not None
    ):
        raise ValueError("GMX modeled admission requires a new exact-target position action")
    if (
        risk.market != accounting_market(payload.target.market)
        or risk.block.timestamp > pending.eligible_at
    ):
        raise ValueError("GMX modeled admission requires its eligible canonical market state")
    allocation = config.allocation(payload.target.allocation)
    if not isinstance(allocation, Allocation):
        raise ValueError("GMX modeled position admission requires a position allocation")
    request = economic_order(allocation, intent, payload)
    selected = risk.increases if request.order_type == MARKET_INCREASE else risk.decreases
    if risk.market_disabled or selected.creation_disabled:
        raise ValueError("GMX modeled position request creation is historically disabled")
    inputs = (AssetAmount(asset=request.execution_fee.asset, amount=input_amount(request)),)
    spent = cumulative_spending(payload, inputs)
    # Cumulative increase notional is reported, never limited.
    notional = payload.spent_notional_usd_e30
    if request.order_type == MARKET_INCREASE:
        notional += request.size_delta_usd_e30
    movements = tuple(
        AssetDelta(key=wallet(payload.target, value.asset), amount=-value.amount)
        for value in inputs
    )
    after = payload.model_copy(
        update={
            "owned": inventory_after(payload.owned, movements),
            "spent": spent,
            "spent_notional_usd_e30": notional,
            "allowances": allowances_after_acceptance(payload, inputs),
            "escrow": PerpetualEscrow(
                identity=pending.identity,
                accepted_at=pending.eligible_at,
                intent=intent,
                request=request,
            ),
        }
    )
    return after, movements


def validate_position(allocation, payload, time):
    if not isinstance(allocation, Allocation):
        raise ValueError("GMX modeled position state requires its configured position allocation")
    escrow = payload.escrow
    if escrow is None:
        return
    request = escrow.request
    acceptable = (
        allocation.increase_acceptable_price
        if request.order_type == MARKET_INCREASE
        else allocation.decrease_acceptable_price
    )
    if (
        request.execution_fee != allocation.execution_fee
        or request.acceptable_price != acceptable
        or escrow.accepted_at > time
    ):
        raise ValueError("GMX modeled escrow changed its frozen fee, price bound or admission time")


def zeroed(values):
    return tuple(value.model_copy(update={"amount": 0}) for value in values)


def cleanup(payload, risk):
    """Claim funding into cash once authority is revoked.

    Deferred impact collateral remains an obligation until its deployed claim
    window and factor have a verified historical model.
    """
    owned = payload.owned
    if any(value.amount for value in payload.allowances):
        raise ValueError("GMX modeled claims require the spending authority to be revoked first")
    if any(value.amount for value in owned.deferred_collateral):
        raise ValueError("GMX deferred impact collateral claim window and factor are not modeled")
    claims = tuple(value for value in owned.claimable_funding if value.amount)
    if not claims:
        raise ValueError("GMX modeled position authority and claims are already closed")
    if risk.funding_claim_disabled:
        raise ValueError("GMX modeled funding claims are historically disabled")
    movements = tuple(
        AssetDelta(key=wallet(payload.target, value.asset), amount=value.amount) for value in claims
    )
    cleared = owned.model_copy(update={"claimable_funding": zeroed(owned.claimable_funding)})
    after = payload.model_copy(update={"owned": inventory_after(cleared, movements)})
    return after, movements, "claimed"


def stored_accumulators(position):
    if position is None:
        return FeeAccumulators(
            borrowing_factor=0,
            funding_payment_per_size=0,
            funding_long_claim_per_size=0,
            funding_short_claim_per_size=0,
        )
    return FeeAccumulators(
        borrowing_factor=position.borrowing_factor,
        funding_payment_per_size=position.funding_fee_amount_per_size,
        funding_long_claim_per_size=position.long_claimable_funding_amount_per_size,
        funding_short_claim_per_size=position.short_claimable_funding_amount_per_size,
    )


def latest_accumulators(opportunity, target):
    # Execution updates funding and borrowing state before pricing the position.
    indexes = accumulated_funding(opportunity.funding, opportunity.funding_indexes)
    long = is_long(target)
    payments = indexes.long_payments if long else indexes.short_payments
    claims = indexes.long_claims if long else indexes.short_claims
    collateral_long = target.collateral_asset == target.market.long_asset
    return FeeAccumulators(
        borrowing_factor=next_borrowing_factor(opportunity.accounting, long),
        funding_payment_per_size=payments.long if collateral_long else payments.short,
        funding_long_claim_per_size=claims.long,
        funding_short_claim_per_size=claims.short,
    )


def accumulator_fields(latest):
    return {
        "borrowing_factor": latest.borrowing_factor,
        "funding_fee_amount_per_size": latest.funding_payment_per_size,
        "long_claimable_funding_amount_per_size": latest.funding_long_claim_per_size,
        "short_claimable_funding_amount_per_size": latest.funding_short_claim_per_size,
    }


def fees_for(opportunity, target, position, size_delta, balance_improved, *, liquidation):
    collateral_price, _ = settlement_prices(opportunity, target)
    return position_fees(
        PositionFeeInput(
            collateral_price=collateral_price,
            size_usd_e30=0 if position is None else position.size_usd_e30,
            size_delta_usd_e30=size_delta,
            position=stored_accumulators(position),
            latest=latest_accumulators(opportunity, target),
            policy=opportunity.fees.select(balance_improved),
            liquidation=liquidation,
        )
    )


def pricing_after(pricing, usd_delta, token_delta, long):
    delta = token_delta if pricing.use_open_interest_in_tokens else usd_delta
    field = "long_open_interest" if long else "short_open_interest"
    return pricing.model_copy(update={field: max(0, getattr(pricing, field) + delta)})


def pnl_after(state, usd_delta, token_delta, long):
    field = "long" if long else "short"
    side = getattr(state, field)
    changed = side.model_copy(
        update={
            "open_interest_usd_e30": max(0, side.open_interest_usd_e30 + usd_delta),
            "open_interest_tokens": max(0, side.open_interest_tokens + token_delta),
        }
    )
    return state.model_copy(update={field: changed})


def evaluate_risk(opportunity, target, position, usd_delta, token_delta, *, factor, minimum):
    """Liquidation guard for ``position`` after the market absorbed the given interest change."""
    long = is_long(target)
    collateral_price, _ = settlement_prices(opportunity, target)
    pricing = pricing_after(opportunity.pricing, usd_delta, token_delta, long)
    size, tokens = position.size_usd_e30, position.index_amount.amount
    capped = position_pnl(
        pnl_after(opportunity.pnl, usd_delta, token_delta, long), size, tokens, size, long
    )
    close = position_impact(pricing, -size, -tokens, long)
    fees = fees_for(opportunity, target, position, size, close.balance_improved, liquidation=False)
    risk = opportunity.risk
    return position_risk(
        PositionRiskInput(
            size_usd_e30=size,
            collateral_amount=position.collateral.amount,
            collateral_price=collateral_price,
            index_price=pricing.index_price,
            capped_pnl_usd_e30=capped.pnl_usd_e30,
            close_impact_usd_e30=close.usd_e30,
            pending_impact_amount=position.pending_impact_amount,
            maximum_positive_impact_factor=pricing.impact.maximum_positive_factor,
            maximum_liquidation_impact_factor=risk.maximum_liquidation_impact_factor,
            close_cost_amount=fees.total,
            minimum_collateral_usd_e30=risk.minimum_collateral_usd_e30,
            minimum_collateral_factor=factor,
            validate_minimum_collateral=minimum,
        )
    )


def validate_acceptable(execution_price, acceptable, long, increase, size_delta):
    if not size_delta:
        return
    # Longs accept increases at or below and decreases at or above the bound; shorts mirror it.
    within = execution_price <= acceptable if long == increase else execution_price >= acceptable
    if not within:
        raise SwapConstraint("GMX modeled execution price is outside the admitted acceptable price")


def empty_position(identity, target):
    return SimulatedPosition(
        identity=identity,
        size_usd_e30=0,
        index_amount=AssetAmount(asset=target.market.index_asset, amount=0),
        collateral=AssetAmount(asset=target.collateral_asset, amount=0),
        pending_impact_amount=0,
        borrowing_factor=0,
        funding_fee_amount_per_size=0,
        long_claimable_funding_amount_per_size=0,
        short_claimable_funding_amount_per_size=0,
        increased_at=UnixTime(0),
        decreased_at=UnixTime(0),
    )


def settle_increase(payload, escrow, opportunity):
    target, request, risk = payload.target, escrow.request, opportunity.risk
    long, position = is_long(target), payload.owned.position
    delta = request.size_delta_usd_e30
    price = increase_price(opportunity.pricing, delta, long)
    validate_acceptable(price.execution_price, request.acceptable_price, long, True, delta)
    fees = fees_for(opportunity, target, position, delta, price.balance_improved, liquidation=False)
    held = position or empty_position(escrow.identity, target)
    collateral = held.collateral.amount + request.collateral_delta.amount - fees.total
    if collateral < 0:
        raise SwapConstraint("GMX modeled increase cannot pay position fees from its collateral")
    tokens = held.index_amount.amount + price.base_size_delta_tokens
    after = held.model_copy(
        update={
            "size_usd_e30": held.size_usd_e30 + delta,
            "index_amount": held.index_amount.model_copy(update={"amount": tokens}),
            "collateral": held.collateral.model_copy(update={"amount": collateral}),
            "pending_impact_amount": held.pending_impact_amount + price.price_impact_amount,
            "increased_at": opportunity.block.timestamp,
            **accumulator_fields(latest_accumulators(opportunity, target)),
        }
    )
    if not after.size_usd_e30 or not tokens:
        raise SwapConstraint("GMX modeled increase leaves an empty position")
    factor = validate_increase_reserves(
        risk, opportunity.accounting, delta, price.base_size_delta_tokens, long
    )
    collateral_price, _ = settlement_prices(opportunity, target)
    if collateral * collateral_price.minimum < apply_factor(after.size_usd_e30, factor):
        raise SwapConstraint("GMX modeled increase leaves insufficient collateral for its leverage")
    if after.size_usd_e30 < risk.minimum_position_usd_e30:
        raise SwapConstraint("GMX modeled increase is below the minimum position size")
    evaluated = evaluate_risk(
        opportunity,
        target,
        after,
        delta,
        price.base_size_delta_tokens,
        factor=risk.minimum_collateral_factor,
        minimum=True,
    )
    if evaluated.liquidatable:
        raise SwapConstraint(f"GMX modeled increase would be liquidatable: {evaluated.reason}")
    return Settlement(
        position=after,
        outputs=(),
        claims=(fees.long_funding_claim, fees.short_funding_claim),
        deferred=(0, 0),
        execution_price=price.execution_price,
        unpaid_usd_e30=0,
        fees_collected=True,
    )


def adjusted_decrease(position, request, opportunity, target):
    long, risk = is_long(target), opportunity.risk
    collateral_price, _ = settlement_prices(opportunity, target)
    size, tokens = position.size_usd_e30, position.index_amount.amount
    estimated = position_pnl(opportunity.pnl, size, tokens, size, long).pnl_usd_e30
    requested = min(request.size_delta_usd_e30, size)
    result = adjust_decrease(
        DecreaseAdjustmentInput(
            position_usd_e30=size,
            collateral_amount=position.collateral.amount,
            collateral_minimum_price=collateral_price.minimum,
            full_pnl_usd_e30=estimated,
            requested_size_usd_e30=request.size_delta_usd_e30,
            requested_withdrawal=request.collateral_delta.amount,
            next_minimum_collateral_factor=next_collateral_factor(
                risk, opportunity.accounting, -requested, long
            ),
            minimum_collateral_usd_e30=risk.minimum_collateral_usd_e30,
            minimum_position_usd_e30=risk.minimum_position_usd_e30,
        )
    )
    return result.size_usd_e30, result.withdrawal


def proportional_pending(pending, delta, size):
    # Negative pending impact rounds its realized magnitude up, positive rounds down.
    product = abs(pending) * delta
    return -ceil_div(product, size) if pending < 0 else product // size


def remaining_position(position, opportunity, target, delta, delta_tokens, collateral):
    pending = position.pending_impact_amount
    after = position.model_copy(
        update={
            "size_usd_e30": position.size_usd_e30 - delta,
            "index_amount": position.index_amount.model_copy(
                update={"amount": position.index_amount.amount - delta_tokens}
            ),
            "collateral": position.collateral.model_copy(update={"amount": collateral}),
            "pending_impact_amount": pending
            - proportional_pending(pending, delta, position.size_usd_e30),
            "decreased_at": opportunity.block.timestamp,
            **accumulator_fields(latest_accumulators(opportunity, target)),
        }
    )
    if not after.size_usd_e30 or not after.index_amount.amount:
        raise SwapConstraint("GMX modeled decrease leaves an empty position")
    return after


def settlement_outputs(target, opportunity, output, secondary):
    market, solvency = target.market, opportunity.solvency
    outputs = []
    for asset, token, amount in zip(
        (market.long_asset, market.short_asset),
        (solvency.long, solvency.short),
        by_market_asset(target, output, secondary),
        strict=True,
    ):
        if amount:
            validate_token_transfer(token, -amount)
            outputs.append(AssetAmount(asset=asset, amount=amount))
    return tuple(outputs)


def by_market_asset(target, collateral_amount, secondary_amount):
    profit_asset = target.market.long_asset if is_long(target) else target.market.short_asset
    amounts = {target.collateral_asset: collateral_amount}
    amounts[profit_asset] = amounts.get(profit_asset, 0) + secondary_amount
    return amounts.get(target.market.long_asset, 0), amounts.get(target.market.short_asset, 0)


def settle_decrease(payload, escrow, opportunity, *, liquidation=False):
    target, risk = payload.target, opportunity.risk
    long, position = is_long(target), payload.owned.position
    if position is None:
        raise SwapConstraint("GMX modeled decrease has no retained position")
    collateral_price, profit_price = settlement_prices(opportunity, target)
    size, tokens = position.size_usd_e30, position.index_amount.amount
    delta, withdrawal = size, 0
    if not liquidation:
        delta, withdrawal = adjusted_decrease(position, escrow.request, opportunity, target)
    price = decrease_price(opportunity.pricing, size, tokens, delta, long)
    if not liquidation:
        validate_acceptable(
            price.execution_price, escrow.request.acceptable_price, long, False, delta
        )
    pnl = position_pnl(opportunity.pnl, size, tokens, delta, long)
    impact = realized_impact(
        opportunity.impact, size, position.pending_impact_amount, delta, price.price_impact_usd_e30
    )
    fees = fees_for(
        opportunity, target, position, delta, price.balance_improved, liquidation=liquidation
    )
    paid = decrease_collateral(
        DecreaseCollateralInput(
            collateral_price=collateral_price,
            pnl_price=profit_price,
            collateral_amount=position.collateral.amount,
            base_pnl_usd_e30=pnl.pnl_usd_e30,
            impact_usd_e30=impact.total_usd_e30,
            deferred_impact_usd_e30=impact.claimable_difference_usd_e30,
            fees=fees,
            withdrawal=withdrawal,
            insolvent_close_allowed=liquidation,
            holding_address_available=risk.holding_address_available,
        )
    )
    output, remaining, after = paid.balances.output, paid.balances.remaining, None
    if delta == size:
        # A full close returns every remaining collateral unit together with the output.
        output, remaining = output + remaining, 0
    else:
        after = remaining_position(
            position, opportunity, target, delta, pnl.size_delta_tokens, remaining
        )
        evaluated = evaluate_risk(
            opportunity,
            target,
            after,
            -delta,
            -pnl.size_delta_tokens,
            factor=risk.minimum_collateral_factor,
            minimum=False,
        )
        if evaluated.liquidatable:
            raise SwapConstraint(
                f"GMX modeled decrease leaves a liquidatable position: {evaluated.reason}"
            )
    return Settlement(
        position=after,
        outputs=settlement_outputs(target, opportunity, output, paid.balances.secondary),
        claims=(fees.long_funding_claim, fees.short_funding_claim),
        deferred=by_market_asset(target, paid.deferred_collateral, paid.deferred_secondary),
        execution_price=price.execution_price,
        unpaid_usd_e30=sum(value.unpaid_usd_e30 for value in paid.payments),
        fees_collected=paid.fees_collected,
    )


def added(values, amounts):
    return tuple(
        value.model_copy(update={"amount": value.amount + amount})
        for value, amount in zip(values, amounts, strict=True)
    )


def apply_settlement(payload, settlement):
    owned = payload.owned
    changed = owned.model_copy(
        update={
            "position": settlement.position,
            "claimable_funding": added(owned.claimable_funding, settlement.claims),
            "deferred_collateral": added(owned.deferred_collateral, settlement.deferred),
        }
    )
    return payload.model_copy(update={"owned": changed})


def settle(payload, opportunity):
    """Apply the retained request to ownership; the caller releases escrow and outputs."""
    escrow = payload.escrow
    if not isinstance(escrow, PerpetualEscrow):
        raise ValueError("GMX modeled position settlement requires its retained position escrow")
    if escrow.request.order_type == MARKET_INCREASE:
        settlement = settle_increase(payload, escrow, opportunity)
    else:
        settlement = settle_decrease(payload, escrow, opportunity)
    return apply_settlement(payload, settlement), settlement.outputs


def liquidation_risk(payload, opportunity):
    return evaluate_risk(
        opportunity,
        payload.target,
        payload.owned.position,
        0,
        0,
        factor=opportunity.risk.liquidation_collateral_factor,
        minimum=True,
    )


def liquidation_opportunity(payload, record, time):
    """The first publication at ``time`` whose verified prices make the position liquidatable.

    Only publications strictly after the position's last change are considered, so
    the settlement that created the current position never liquidates it.
    """
    position = payload.owned.position
    if position is None or time <= max(position.increased_at, position.decreased_at):
        return None
    for opportunity in record.payload.opportunities:
        if opportunity.block.timestamp != time or opportunity.risk.liquidation_disabled:
            continue
        evaluated = liquidation_risk(payload, opportunity)
        if evaluated.liquidatable:
            return opportunity, evaluated.reason
    return None


def next_liquidation(timeline, payload, start, until):
    # Rows are ordered by availability, which never precedes the block time, so the
    # first row available at ``start`` bounds every publication at or after it.
    for index in range(bisect_left(timeline.times, start), len(timeline.rows)):
        row = timeline.rows[index]
        time = row.reference.timestamp
        if time > until:
            return None
        if time >= start and liquidation_opportunity(payload, row, time) is not None:
            return time
    return None


def liquidate(payload, opportunity):
    """Accounting-only keeper liquidation: costs in deployed order, remainder to the fund."""
    settlement = settle_decrease(payload, None, opportunity, liquidation=True)
    movements = tuple(
        AssetDelta(key=wallet(payload.target, value.asset), amount=value.amount)
        for value in settlement.outputs
    )
    after = apply_settlement(payload, settlement)
    after = after.model_copy(update={"owned": inventory_after(after.owned, movements)})
    return after, movements, settlement
