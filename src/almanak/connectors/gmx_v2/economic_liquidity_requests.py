"""Prepared GM requests escrow exact funds and shares, then settle against eligible publications."""

from almanak.connectors.gmx_v2.economic_accounting import (
    allowances_after_acceptance,
    cumulative_spending,
    inventory_after,
)
from almanak.connectors.gmx_v2.economic_data import accounting_market
from almanak.connectors.gmx_v2.economic_deposit_execution import execute_deposit
from almanak.connectors.gmx_v2.economic_liquidity import LiquidityState, quote_withdrawal
from almanak.connectors.gmx_v2.economic_liquidity_risk import (
    validate_deposit,
    validate_withdrawal,
)
from almanak.connectors.gmx_v2.economic_parameters import wallet
from almanak.connectors.gmx_v2.economic_solvency import validate_token_transfer
from almanak.connectors.gmx_v2.economic_spot_types import SwapConstraint
from almanak.connectors.gmx_v2.economic_state import LiquidityEscrow
from almanak.connectors.gmx_v2.economic_valuation import pool_value
from almanak.connectors.gmx_v2.exchange_planning import validate_deadline
from almanak.connectors.gmx_v2.liquidity_operations import CreateDeposit, CreateWithdrawal
from almanak.connectors.gmx_v2.liquidity_planning import request_for
from almanak.dex_intents import DecreaseLiquidity, LiquidityBudget, LiquidityMinimum
from almanak.effects import AssetDelta
from almanak.models import AssetAmount


def controls(escrow, risk):
    return risk.deposits if isinstance(escrow.request, CreateDeposit) else risk.withdrawals


def admit(config, payload, pending, risk):
    allocation = config.allocation(payload.target.allocation)
    intent = pending.intent
    if (
        intent.target != payload.target
        or payload.escrow is not None
        or risk.block.timestamp > pending.eligible_at
        or risk.market != accounting_market(payload.target.market)
    ):
        raise ValueError(
            "GM modeled admission requires a new exact-target request at its eligible time"
        )
    request = request_for(config, intent, payload)
    control = risk.deposits if isinstance(request, CreateDeposit) else risk.withdrawals
    if risk.market_disabled or control.creation_disabled:
        raise ValueError("GM modeled request creation is historically disabled")
    validate_deadline(intent.deadline, risk.block.timestamp, allocation.deadline_seconds)
    if pending.eligible_at >= intent.deadline:
        raise ValueError("GM modeled request missed its explicit admission deadline")
    spent = cumulative_spending(payload, request.inputs)
    movements = tuple(
        AssetDelta(key=wallet(payload.target, value.asset), amount=-value.amount)
        for value in request.inputs
    )
    principal = (
        request.amounts.root if isinstance(request, CreateDeposit) else (request.market_tokens,)
    )
    return payload.model_copy(
        update={
            "owned": inventory_after(payload.owned, movements),
            "spent": spent,
            "allowances": allowances_after_acceptance(payload, request.inputs),
            "escrow": LiquidityEscrow(
                intent=intent,
                identity=pending.identity,
                accepted_at=pending.eligible_at,
                inputs=principal,
                execution_fee=request.execution_fee,
                request=request,
            ),
        }
    ), movements


def validate_escrow(allocation, escrow, time):
    request, intent = escrow.request, escrow.intent
    if (
        request.execution_fee != allocation.execution_fee
        or request.admission_deadline != intent.deadline
        or escrow.accepted_at > time
    ):
        raise ValueError("GM modeled escrow changed its frozen fee, deadline or admission time")
    if isinstance(request, CreateDeposit):
        validate_deposit_escrow(allocation, request, intent)
    else:
        validate_withdrawal_escrow(request, intent)


def validate_deposit_escrow(allocation, request, intent):
    if (
        not isinstance(intent, LiquidityBudget)
        or request.amounts.root != (intent.maximum0, intent.maximum1)
        or request.minimum_market_tokens != allocation.minimum_market_tokens
    ):
        raise ValueError(
            "GM modeled deposit differs from its admitted exact inputs and share minimum"
        )


def validate_withdrawal_escrow(request, intent):
    if not isinstance(intent, LiquidityMinimum) or request.minimum_outputs.root != (
        intent.minimum0,
        intent.minimum1,
    ):
        raise ValueError("GM modeled withdrawal differs from its admitted exact output minimums")
    elif isinstance(intent, DecreaseLiquidity) and request.market_tokens.amount != intent.liquidity:
        raise ValueError("GM modeled partial withdrawal changed its admitted share amount")


def deposit_outputs(escrow, opportunity):
    request, state = escrow.request, opportunity.deposit
    result = execute_deposit(state, *(value.amount for value in request.amounts.root))
    validate_deposit(state, opportunity.risk, result)
    if result.shares < request.minimum_market_tokens.amount:
        raise SwapConstraint("GM modeled deposit shares are below the admitted minimum")
    for token, amount in zip(
        (opportunity.solvency.long, opportunity.solvency.short), request.amounts.root, strict=True
    ):
        validate_token_transfer(token, amount.amount)
    return (AssetAmount(asset=escrow.intent.target.market.share_asset, amount=result.shares),)


def withdrawal_outputs(escrow, opportunity):
    request, state = escrow.request, opportunity.withdrawal
    quote = withdrawal_quote(state)
    result = quote_withdrawal(quote, request.market_tokens.amount)
    validate_withdrawal(state, opportunity.risk, result)
    amounts = (result.long_amount, result.short_amount)
    validate_output_minimums(amounts, request.minimum_outputs.root)
    for token, amount in zip(
        (opportunity.solvency.long, opportunity.solvency.short), amounts, strict=True
    ):
        validate_token_transfer(token, -amount)
    return tuple(
        AssetAmount(asset=asset, amount=amount)
        for asset, amount in zip(escrow.intent.target.assets, amounts, strict=True)
    )


def withdrawal_quote(state):
    value = pool_value(state, maximize=False).pool_usd_e30
    if value <= 0:
        raise SwapConstraint("GM modeled withdrawal requires positive pool value")
    return LiquidityState(
        action="WITHDRAWAL",
        spot=state.spot,
        pool_value_usd_e30=value,
        share_supply=state.share_supply,
    )


def validate_output_minimums(amounts, minima):
    if any(amount < minimum.amount for amount, minimum in zip(amounts, minima, strict=True)):
        raise SwapConstraint("GM modeled withdrawal output is below the admitted minimum")


def fill_outputs(escrow, opportunity):
    if isinstance(escrow.request, CreateDeposit):
        return deposit_outputs(escrow, opportunity)
    if isinstance(escrow.request, CreateWithdrawal):
        return withdrawal_outputs(escrow, opportunity)
    raise ValueError("GM modeled settlement requires its exact retained request family")
