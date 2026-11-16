"""Earn loan interest independently while borrowing against another fund's collateral."""

from typing import cast

from almanak.api import (
    PositiveDecimal,
    StrategyState,
    TypedParameters,
    declared,
    parameter_base_units,
)
from almanak.connectors.morpho_blue.models import Parameters
from almanak.connectors.morpho_blue.observations import owned


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
    if {a.name for a in config.positions} != {"earn", "collateral", "loan"}:
        raise ValueError("this reference requires earn, collateral and loan allocations")
    earn, collateral, loan = (config.allocation(name) for name in ("earn", "collateral", "loan"))
    if earn.account == loan.account or collateral.account != loan.account:
        raise ValueError(
            "earning and borrowing use separate funds; collateral belongs to the borrower"
        )
    if not earn.reserve.is_loan or not loan.reserve.is_loan or collateral.reserve.is_loan:
        raise ValueError("earning and debt use loan assets; collateral uses another market asset")
    if len({a.reserve.market for a in (earn, collateral, loan)}) != 1:
        raise ValueError("this reference uses one explicitly configured Morpho market")
    # loan.capital is optional: a loan-asset reserve the strategy never supplies or
    # spends, so the full repay can pay the interest accrued since the borrow.
    if earn.borrow or not loan.borrow or earn.capital < 10 or collateral.capital < 10:
        raise ValueError("reference branches require separate supply capital and borrowing")
    borrow_amount(config, "loan")  # refuses a borrow_quantity finer than the loan asset


def borrow_amount(config, name: str) -> int:
    """`borrow_quantity` in base units of the named allocation's reserve asset."""
    asset = config.allocation(name).reserve.asset
    decimals = config.asset_metadata[asset].decimals
    return parameter_base_units(config.parameters, "borrow_quantity", decimals)


def close(context):
    loan = context.observation("loan")
    if loan is not None and (loan.account.position.borrow_shares > 0 or loan.allowance):
        return context.actions.repay("loan")
    for name in ("collateral", "earn"):
        if name == "collateral" and loan is None:
            continue
        observation = context.observation(name)
        if observation is not None and (not owned(observation).empty or observation.allowance):
            return context.actions.withdraw(name)
    return None


def action(context):
    config, actions = context.config, context.actions
    phase = state(context).phase
    if phase == 0:
        return actions.supply("earn", config.allocation("earn").capital)
    if phase == 1:
        return actions.supply("collateral", config.allocation("collateral").capital)
    if phase == 2:
        return actions.borrow("loan", borrow_amount(config, "loan"))
    return exit_action(context)


def exit_action(context):
    config, actions = context.config, context.actions
    phase = state(context).phase
    if phase == 3:
        return actions.repay_amount("loan", borrow_amount(config, "loan") // 2)
    if phase == 4:
        return actions.withdraw_amount("collateral", config.allocation("collateral").capital // 10)
    if phase == 5:
        return actions.withdraw_amount("earn", config.allocation("earn").capital // 10)
    if phase == 6:
        return actions.repay("loan")
    if phase == 7:
        return actions.withdraw("collateral")
    if phase == 8:
        return actions.withdraw("earn")
    return None


def decide(context):
    memory = state(context)
    if memory.exiting or (context.teardown and memory.phase < 3):
        intent = close(context)
        return context.decision(
            intent=intent, memory=memory.updated(exiting=True), reason=unwind_reason(memory, intent)
        )
    if any(context.observation(name) is None for name in ("earn", "collateral", "loan")):
        if context.teardown:
            intent = close(context)
            return context.decision(
                intent=intent,
                memory=memory.updated(exiting=True),
                reason=unwind_reason(memory, intent),
            )
        return context.decision(reason="Waiting for lending position data")
    if memory.phase < 3 or context.teardown:
        intent = action(context)
        return context.decision(intent=intent, reason=unwind_reason(memory, intent))
    return context.decision(
        reason="Earning, supplied and borrowed; holding until the strategy stops"
    )


def unwind_reason(memory, intent):
    if intent is None:
        return "Nothing left to unwind"
    return "Unwinding after a failed step or an outside event" if memory.exiting else None


def on_outcome(context):
    memory = state(context)
    if context.result.status == "failed":
        return memory.updated(exiting=True)
    if context.result.status != "completed" or memory.exiting:
        return memory
    return memory.updated(phase=memory.phase + 1)


def on_event(context):
    return state(context).updated(exiting=True)
