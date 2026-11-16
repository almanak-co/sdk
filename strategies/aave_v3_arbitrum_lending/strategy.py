"""Exercise lending, partial exits, collateral control and durable cleanup."""

from typing import cast

from almanak.api import (
    PositiveDecimal,
    StrategyState,
    TypedParameters,
    declared,
    parameter_base_units,
)
from almanak.connectors.aave_v3.models import Parameters


class StrategyParams(TypedParameters):
    # Borrowed per action, in the loan asset's units.
    borrow_quantity: PositiveDecimal


class State(StrategyState):
    phase: int = 0
    exiting: bool = False


def state(context) -> State:
    return cast(State, context.memory)


def capabilities(config):
    return declared(config)


def validate_config(config: Parameters) -> None:
    if {a.name for a in config.positions} != {"collateral", "loan"}:
        raise ValueError("this reference requires collateral and loan allocations")
    collateral, loan = config.allocation("collateral"), config.allocation("loan")
    if (collateral.account, collateral.reserve.pool, collateral.chain) != (
        loan.account,
        loan.reserve.pool,
        loan.chain,
    ):
        raise ValueError("this reference borrows against collateral in the same fund and Pool")
    validate_collateral(collateral)
    validate_loan(loan)
    borrow_amount(config, "loan")  # refuses a borrow_quantity finer than the loan asset


def validate_collateral(collateral):
    if not collateral.collateral or collateral.capital < 10 or collateral.borrow:
        raise ValueError("collateral requires starting capital and no borrowing")


def validate_loan(loan):
    # loan.capital is optional: a loan-asset reserve the strategy never supplies or
    # spends, so the full repay can pay the interest accrued since the borrow.
    if not loan.borrow:
        raise ValueError("loan requires borrowing")


def borrow_amount(config, name: str) -> int:
    """`borrow_quantity` in base units of the named allocation's reserve asset."""
    asset = config.allocation(name).reserve.asset
    decimals = config.asset_metadata[asset].decimals
    return parameter_base_units(config.parameters, "borrow_quantity", decimals)


def close(context):
    loan = context.observation("loan")
    if loan is None:
        return None
    if loan.position.debt_scaled or loan.allowance:
        return context.actions.repay("loan")
    return close_collateral(context)


def close_collateral(context):
    collateral = context.observation("collateral")
    if collateral is not None and any(
        (
            collateral.position.supplied_scaled,
            collateral.position.collateral_enabled,
            collateral.allowance,
        )
    ):
        return context.actions.withdraw("collateral")
    return None


def action(context):
    if state(context).phase < 4:
        return entry_action(context)
    return exit_action(context)


def entry_action(context):
    config, actions = context.config, context.actions
    phase = state(context).phase
    if phase == 0:
        return actions.supply("collateral", config.allocation("collateral").capital)
    if phase == 1:
        return actions.set_collateral("collateral", False)
    if phase == 2:
        return actions.set_collateral("collateral", True)
    if phase == 3:
        return actions.borrow("loan", borrow_amount(config, "loan"))
    return None


def exit_action(context):
    config, actions = context.config, context.actions
    phase = state(context).phase
    if phase == 4:
        return actions.repay_amount("loan", borrow_amount(config, "loan") // 2)
    if phase == 5:
        return actions.withdraw_amount("collateral", config.allocation("collateral").capital // 10)
    if phase == 6:
        return actions.repay("loan")
    if phase == 7:
        return actions.withdraw("collateral")
    return None


def decide(context):
    memory = state(context)
    if needs_emergency_exit(context):
        intent = close(context)
        return context.decision(
            intent=intent, memory=memory.updated(exiting=True), reason=unwind_reason(memory, intent)
        )
    if None in (context.observation("collateral"), context.observation("loan")):
        return context.decision(reason="Waiting for lending position data")
    if memory.phase < 4 or context.teardown:
        intent = action(context)
        return context.decision(intent=intent, reason=unwind_reason(memory, intent))
    return context.decision(reason="Supplied and borrowed; holding until the strategy stops")


def unwind_reason(memory, intent):
    if intent is None:
        return "Nothing left to unwind"
    return "Unwinding after a failed step or an outside event" if memory.exiting else None


def needs_emergency_exit(context):
    memory = state(context)
    return memory.exiting or (context.teardown and memory.phase < 4)


def on_outcome(context):
    memory = state(context)
    if context.result.status == "failed":
        return memory.updated(exiting=True)
    if context.result.status != "completed" or memory.exiting:
        return memory
    return memory.updated(phase=memory.phase + 1)


def on_event(context):
    return state(context).updated(exiting=True)
