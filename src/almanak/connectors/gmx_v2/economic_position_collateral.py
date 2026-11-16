"""Decrease costs consume profit, collateral and secondary profit in the deployed order."""

from typing import Literal

from almanak.connectors.gmx_v2.economic_position_fees import PositionFees
from almanak.connectors.gmx_v2.economic_precision import ceil_div
from almanak.connectors.gmx_v2.economic_spot_types import Price, SwapConstraint
from almanak.models import Model, UInt

Stage = Literal["funding", "pnl", "fees", "impact", "diff"]


class CollateralBalances(Model):
    remaining: UInt
    output: UInt
    secondary: UInt


class CostPayment(Model):
    stage: Stage
    collateral_amount: UInt
    secondary_amount: UInt
    unpaid_usd_e30: UInt


class DecreaseCollateralInput(Model):
    collateral_price: Price
    pnl_price: Price
    collateral_amount: UInt
    base_pnl_usd_e30: int
    impact_usd_e30: int
    deferred_impact_usd_e30: UInt
    fees: PositionFees
    withdrawal: UInt
    insolvent_close_allowed: bool
    holding_address_available: bool


class DecreaseCollateralResult(Model):
    balances: CollateralBalances
    payments: tuple[CostPayment, ...]
    deferred_collateral: UInt
    deferred_secondary: UInt
    fees_collected: bool
    insolvent_at: Stage | None


def pay_for_cost(balances, collateral_price, secondary_price, cost_usd, stage):
    if cost_usd < 0:
        raise ValueError("GMX cost payment requires a nonnegative USD-e30 amount")
    required = ceil_div(cost_usd, collateral_price.minimum)
    output = min(required, balances.output)
    collateral = min(required - output, balances.remaining)
    required -= output + collateral
    secondary_cost = required * collateral_price.minimum // secondary_price.minimum
    secondary = min(secondary_cost, balances.secondary)
    return CollateralBalances(
        remaining=balances.remaining - collateral,
        output=balances.output - output,
        secondary=balances.secondary - secondary,
    ), CostPayment(
        stage=stage,
        collateral_amount=output + collateral,
        secondary_amount=secondary,
        unpaid_usd_e30=(secondary_cost - secondary) * secondary_price.minimum,
    )


def cost_sequence(state):
    return (
        ("funding", state.fees.funding_amount * state.collateral_price.minimum),
        ("pnl", max(0, -state.base_pnl_usd_e30)),
        ("fees", state.fees.total_excluding_funding * state.collateral_price.minimum),
        ("impact", max(0, -state.impact_usd_e30)),
        ("diff", state.deferred_impact_usd_e30),
    )


def initial_balances(state):
    # PnL and impact payouts round separately; combining USD amounts would change dust.
    payout = max(0, state.base_pnl_usd_e30) // state.pnl_price.maximum
    payout += max(0, state.impact_usd_e30) // state.pnl_price.maximum
    same_asset = state.collateral_price.asset == state.pnl_price.asset
    return CollateralBalances(
        remaining=state.collateral_amount,
        output=payout if same_asset else 0,
        secondary=0 if same_asset else payout,
    )


def decrease_collateral(state):
    balances, payments, insolvent = initial_balances(state), [], None
    fees_collected = True
    for stage, cost in cost_sequence(state):
        balances, payment = pay_for_cost(
            balances, state.collateral_price, state.pnl_price, cost, stage
        )
        payments.append(payment)
        if stage == "funding" and payment.secondary_amount and not state.holding_address_available:
            raise SwapConstraint("GMX secondary funding payment requires a holding address")
        if stage == "fees" and (payment.secondary_amount or payment.unpaid_usd_e30):
            fees_collected = False
        if payment.unpaid_usd_e30:
            if not state.insolvent_close_allowed:
                raise SwapConstraint(f"GMX position has insufficient funds for {stage}")
            insolvent, fees_collected = stage, False
            break
    deferred = next((value for value in payments if value.stage == "diff"), None)
    withdrawal = (
        0
        if insolvent
        else min(
            balances.remaining,
            max(
                0,
                state.withdrawal - state.deferred_impact_usd_e30 // state.collateral_price.minimum,
            ),
        )
    )
    balances = balances.model_copy(
        update={
            "remaining": balances.remaining - withdrawal,
            "output": balances.output + withdrawal,
        }
    )
    return DecreaseCollateralResult(
        balances=balances,
        payments=tuple(payments),
        deferred_collateral=0 if deferred is None else deferred.collateral_amount,
        deferred_secondary=0 if deferred is None else deferred.secondary_amount,
        fees_collected=fees_collected,
        insolvent_at=insolvent,
    )
