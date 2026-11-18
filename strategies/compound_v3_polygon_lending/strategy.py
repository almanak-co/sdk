"""Earn base interest independently while borrowing against another fund's collateral."""

from typing import cast

from almanak import dashboard as ui
from almanak.api import (
    PositiveDecimal,
    StrategyState,
    TypedParameters,
    declared,
    parameter_base_units,
)
from almanak.connectors.compound_v3.models import Parameters
from almanak.connectors.compound_v3.observations import owned


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
    validate_funds(earn, collateral, loan)
    validate_assets(earn, collateral, loan)
    validate_limits(earn, collateral, loan)
    borrow_amount(config, "loan")  # refuses a borrow_quantity finer than the loan asset


def validate_funds(earn, collateral, loan):
    if earn.account == loan.account or collateral.account != loan.account:
        raise ValueError(
            "earning and borrowing use separate funds; collateral belongs to the borrower"
        )


def validate_assets(earn, collateral, loan):
    if not earn.reserve.is_base or not loan.reserve.is_base or collateral.reserve.is_base:
        raise ValueError("earning and debt use base assets; collateral uses another market asset")
    validate_market(earn, collateral, loan)


def validate_market(earn, collateral, loan):
    if len({a.reserve.market for a in (earn, collateral, loan)}) != 1:
        raise ValueError("this reference uses one explicitly configured Comet market")


def validate_limits(earn, collateral, loan):
    if earn.borrow or loan.capital or not loan.borrow:
        raise ValueError("reference branches require separate supply and borrowing allocations")
    validate_minimum_limits(earn, collateral, loan)


def validate_minimum_limits(earn, collateral, loan):
    if earn.capital < 10 or collateral.capital < 10:
        raise ValueError("reference branches require nonzero starting capital to supply")


def borrow_amount(config, name: str) -> int:
    """`borrow_quantity` in base units of the named allocation's reserve asset."""
    asset = config.allocation(name).reserve.asset
    decimals = config.asset_metadata[asset].decimals
    return parameter_base_units(config.parameters, "borrow_quantity", decimals)


def close_loan(context, loan):
    if loan is None:
        return None
    if loan.account.principal < 0 or loan.allowance:
        return context.actions.repay("loan")
    if loan.account.principal > 0:
        return context.actions.withdraw("loan")
    return None


def close(context):
    loan = context.observation("loan")
    intent = close_loan(context, loan)
    if intent is not None:
        return intent
    names = ("earn",) if loan is None else ("collateral", "earn")
    return close_supply(context, names)


def close_supply(context, names):
    for name in names:
        if supply_obligation(context.observation(name)):
            return context.actions.withdraw(name)
    return None


def supply_obligation(observation):
    return observation is not None and (not owned(observation).empty or observation.allowance)


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
    return full_exit_action(context)


def full_exit_action(context):
    actions = context.actions
    phase = state(context).phase
    if phase == 6:
        return actions.repay("loan")
    if phase == 7:
        return actions.withdraw("collateral")
    if phase == 8:
        return actions.withdraw("earn")
    return None


def exit_decision(context):
    return context.decision(intent=close(context), memory=state(context).updated(exiting=True))


def missing_observations(context):
    return any(context.observation(name) is None for name in ("earn", "collateral", "loan"))


def decide(context):
    memory = state(context)
    if memory.exiting or (context.teardown and memory.phase < 3):
        return exit_decision(context)
    return observed_decision(context)


def observed_decision(context):
    if missing_observations(context):
        return exit_decision(context) if context.teardown else context.decision()
    if state(context).phase < 3 or context.teardown:
        return context.decision(intent=action(context))
    return context.decision()


def on_outcome(context):
    memory = state(context)
    if context.result.status == "failed":
        return memory.updated(exiting=True)
    if context.result.status != "completed" or memory.exiting:
        return memory
    return memory.updated(phase=memory.phase + 1)


def on_event(context):
    return state(context).updated(exiting=True)


def dashboard(context: ui.Context) -> ui.Dashboard:
    return ui.Dashboard(
        title="Compound V3 lending",
        description="Separate earning and borrowing funds, with signed base principal "
        "and collateral ownership.",
        blocks=(
            ui.Table(
                id="positions", title="Comet positions", data=context.datasets["lending_positions"]
            ),
            ui.Table(id="gas", title="Fund execution costs", data=context.datasets["fund_costs"]),
            ui.Text(
                id="policy",
                text="The earning fund supplies base assets. The borrower holds proceeds "
                "against its own collateral until teardown. Cleanup partially repays and releases "
                "collateral and earning supply, then repays remaining debt and withdraws both "
                "positions. Values come from retained observations; fiat PnL is unavailable.",
            ),
        ),
    )
