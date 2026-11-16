"""Admission escrows fund inputs once; later eligible publications settle or cancel them."""

from almanak.connectors.gmx_v2.economic_accounting import (
    allowances_after_acceptance,
    cumulative_spending,
    inventory_after,
)
from almanak.connectors.gmx_v2.economic_data import accounting_market
from almanak.connectors.gmx_v2.economic_parameters import wallet
from almanak.connectors.gmx_v2.economic_solvency import validate_swap_solvency
from almanak.connectors.gmx_v2.economic_spot_math import quote_swap
from almanak.connectors.gmx_v2.economic_spot_types import SwapConstraint
from almanak.connectors.gmx_v2.economic_state import Escrow
from almanak.connectors.gmx_v2.economic_swap_risk import validate_swap_risk
from almanak.connectors.gmx_v2.exchange_operations import CreateSwap
from almanak.connectors.gmx_v2.exchange_planning import validate_deadline
from almanak.dex_intents import SwapExactInput
from almanak.effects import AssetDelta
from almanak.gateway.evm import native_asset
from almanak.models import AssetAmount


def admit(config, payload, pending, risk):
    from almanak.connectors.gmx_v2.economic_liquidity_requests import admit as admit_liquidity
    from almanak.connectors.gmx_v2.economic_liquidity_risk import LiquidityRiskState
    from almanak.connectors.gmx_v2.economic_perpetual_requests import admit as admit_perpetual
    from almanak.connectors.gmx_v2.economic_perpetual_risk import PerpetualRiskState

    if isinstance(risk, PerpetualRiskState):
        return admit_perpetual(config, payload, pending, risk)
    if isinstance(risk, LiquidityRiskState):
        return admit_liquidity(config, payload, pending, risk)
    intent = pending.intent
    validate_swap_admission(payload, pending, risk, intent)
    allocation = config.allocation(payload.target.allocation)
    validate_deadline(intent.deadline, risk.block.timestamp, allocation.deadline_seconds)
    if pending.eligible_at >= intent.deadline:
        raise ValueError("GMX modeled swap missed its explicit admission deadline")
    return admit_swap(payload, pending, intent, allocation)


def validate_swap_admission(payload, pending, risk, intent):
    validate_swap_identity(payload, intent)
    validate_swap_risk_state(payload, pending, risk)
    if risk.creation_disabled or risk.market_disabled:
        raise ValueError("GMX modeled swap admission is disabled at the historical time")


def validate_swap_identity(payload, intent):
    if (
        not isinstance(intent, SwapExactInput)
        or intent.target != payload.target
        or payload.escrow is not None
    ):
        raise ValueError("GMX modeled admission requires a new exact-target swap")


def validate_swap_risk_state(payload, pending, risk):
    if (
        risk.market != accounting_market(payload.target.market)
        or risk.block.timestamp > pending.eligible_at
    ):
        raise ValueError("GMX modeled admission requires its eligible canonical market state")


def admit_swap(payload, pending, intent, allocation):
    request = CreateSwap(
        target=intent.target,
        amount=intent.amount,
        minimum_output=intent.minimum_output,
        execution_fee=allocation.execution_fee,
        admission_deadline=intent.deadline,
    )
    spent = cumulative_spending(payload, request.inputs)
    movements = tuple(
        AssetDelta(key=wallet(payload.target, value.asset), amount=-value.amount)
        for value in request.inputs
    )
    after = payload.model_copy(
        update={
            "owned": inventory_after(payload.owned, movements),
            "spent": spent,
            "allowances": allowances_after_acceptance(payload, request.inputs),
            "escrow": Escrow(
                intent=intent,
                identity=pending.identity,
                accepted_at=pending.eligible_at,
                inputs=(intent.amount,),
                execution_fee=allocation.execution_fee,
            ),
        }
    )
    return after, movements


def keeper_payment(settings, target):
    return next(
        value.payment
        for value in settings.keeper.payments
        if value.chain == target.fund.chain.chain_id
    )


def eligible_opportunity(escrow, opportunity, settings, time):
    published, risk = opportunity.publication, opportunity.risk
    if published.block.timestamp != time or request_controls(escrow, risk).execution_disabled:
        return False
    if time < escrow.accepted_at + settings.keeper.minimum_latency_seconds:
        return False
    if published.minimum_timestamp < escrow.accepted_at:
        return False
    if published.maximum_timestamp > escrow.accepted_at + risk.request_expiration_seconds:
        return False
    maximum_age = min(risk.maximum_oracle_age_seconds, settings.data.maximum_report_age_seconds)
    return time - published.minimum_timestamp <= maximum_age


def fill_amount(escrow, opportunity):
    intent = escrow.intent
    if not isinstance(intent, SwapExactInput):
        raise ValueError("GMX swap model cannot settle another intent kind")
    calculation = quote_swap(opportunity.spot, intent.amount.asset, intent.amount.amount)
    index = next(
        price
        for price in opportunity.publication.prices
        if price.asset == intent.target.market.index_asset
    )
    validate_swap_risk(opportunity.spot, opportunity.risk, index, calculation)
    validate_swap_solvency(opportunity.spot, opportunity.solvency, calculation)
    if calculation.amount_out < intent.minimum_output.amount:
        raise SwapConstraint("GMX modeled swap output is below its admitted minimum")
    return AssetAmount(asset=calculation.asset_out, amount=calculation.amount_out)


def release(payload, settings, outputs, *, owner_cancellation=False):
    escrow = payload.escrow
    if escrow is None:
        raise ValueError("GMX modeled release requires retained request escrow")
    native = native_asset(payload.target.fund.chain.chain_id)
    keeper = (
        AssetAmount(asset=native, amount=0)
        if owner_cancellation
        else keeper_payment(settings, payload.target)
    )
    if keeper.amount > escrow.execution_fee.amount:
        raise ValueError("GMX modeled keeper cost exceeds its retained execution fee")
    refund = AssetAmount(asset=native, amount=escrow.execution_fee.amount - keeper.amount)
    movements = tuple(
        AssetDelta(key=wallet(payload.target, value.asset), amount=value.amount)
        for value in (*outputs, refund)
    )
    after = payload.model_copy(
        update={"owned": inventory_after(payload.owned, movements), "escrow": None}
    )
    return after, movements, keeper, refund


def cancellation_eligible(escrow, risk, time):
    # Request expiry is current DataStore configuration, not a frozen admission parameter.
    return (
        not request_controls(escrow, risk).cancellation_disabled
        and time >= escrow.accepted_at + risk.request_expiration_seconds
    )


def request_controls(escrow, risk):
    from almanak.connectors.gmx_v2.economic_liquidity_requests import controls
    from almanak.connectors.gmx_v2.economic_perpetual_requests import (
        controls as position_controls,
    )
    from almanak.connectors.gmx_v2.economic_perpetual_state import PerpetualEscrow
    from almanak.connectors.gmx_v2.economic_state import LiquidityEscrow

    if isinstance(escrow, PerpetualEscrow):
        return position_controls(escrow, risk)
    return controls(escrow, risk) if isinstance(escrow, LiquidityEscrow) else risk
