"""Borrow and trade independently in each explicitly configured fund and chain."""

from typing import cast

from pydantic import Field

from almanak import dashboard as ui
from almanak.api import (
    PositiveDecimal,
    StrategyState,
    TypedParameters,
    declared,
    parameter_base_units,
)
from almanak.connectors.aave_v3.models import Allocation as LendingAllocation
from almanak.connectors.uniswap_v3.models import Allocation as ExchangeAllocation


class StrategyParams(TypedParameters):
    # Borrowed per action, in the loan asset's units.
    borrow_quantity: PositiveDecimal


class State(StrategyState):
    phases: dict[str, int] = Field(default_factory=dict)
    traded: dict[str, bool] = Field(default_factory=dict)
    exiting: dict[str, bool] = Field(default_factory=dict)


def state(context) -> State:
    return cast(State, context.memory)


def capabilities(config):
    return declared(config)


def accounts(config):
    return tuple(dict.fromkeys(a.account for a in config.positions))


def group(config, account):
    lending = account_allocations(config, account, LendingAllocation)
    trading = account_allocations(config, account, ExchangeAllocation)
    if (len(lending), len(trading)) != (2, 1):
        raise ValueError("each fund requires two lending allocations and one trading allocation")
    collateral = unique_lending_role(lending, "capital")
    loan = unique_lending_role(lending, "borrow")
    if collateral == loan:
        raise ValueError("each fund requires distinct collateral and borrowed assets")
    return collateral, loan, trading[0]


def account_allocations(config, account, kind):
    return tuple(a for a in config.positions if a.account == account and isinstance(a, kind))


def unique_lending_role(allocations, limit):
    candidates = tuple(a for a in allocations if getattr(a, limit) > 0)
    if len(candidates) != 1:
        raise ValueError("each fund requires one collateral allocation and one loan allocation")
    return candidates[0]


def borrow_amount(config, name: str) -> int:
    """`borrow_quantity` in base units of the named allocation's reserve asset."""
    asset = config.allocation(name).reserve.asset
    decimals = config.asset_metadata[asset].decimals
    return parameter_base_units(config.parameters, "borrow_quantity", decimals)


def validate_config(config):
    if len({a.chain for a in config.positions}) < 2:
        raise ValueError("this reference requires at least two explicit chains")
    for account in accounts(config):
        collateral, loan, trade = group(config, account)
        validate_market_pair(config, collateral, loan, trade)
        validate_author_cash(config, collateral)


def validate_market_pair(config, collateral, loan, trade):
    if len({a.chain for a in (collateral, loan, trade)}) != 1:
        raise ValueError("each fund's lending and trading must use its explicit chain")
    if collateral.reserve.pool != loan.reserve.pool:
        raise ValueError("collateral and debt must share the configured Pool")
    validate_lending(collateral, loan)
    validate_trade(config, collateral, loan, trade)


def validate_lending(collateral, loan):
    if not collateral.collateral or collateral.borrow or loan.capital or not loan.borrow:
        raise ValueError("collateral requires supply authority and the loan requires borrowing")


def validate_trade(config, collateral, loan, trade):
    if trade.range is not None:
        raise ValueError("this reference trades without opening liquidity")
    if trade.capital.asset != loan.reserve.asset or trade.capital.amount >= borrow_amount(
        config, loan.name
    ):
        raise ValueError("trade capital must consume only part of borrowed proceeds")
    if {trade.pool.currency0, trade.pool.currency1} != {
        collateral.reserve.asset,
        loan.reserve.asset,
    }:
        raise ValueError("trading must exchange this fund's collateral and borrowed assets")


def validate_author_cash(config, collateral):
    amounts = [
        b.amount
        for b in config.inventory
        if (b.key.account, b.key.asset) == (collateral.account, collateral.reserve.asset)
    ]
    if amounts != [collateral.capital]:
        raise ValueError("each fund supplies all its initial collateral-asset cash before trading")


def entry(context, account, collateral, loan, trade):
    phase = state(context).phases.get(account, 0)
    if phase == 0:
        return context.actions.supply(collateral.name, collateral.capital)
    if phase == 1:
        return context.actions.borrow(loan.name, borrow_amount(context.config, loan.name))
    if phase == 2:
        return context.actions.swap(trade.name)
    return None


def close(context, account, collateral, loan, trade):
    if state(context).traded.get(account, False):
        intent = context.actions.exit(trade.name)
        if intent is not None:
            return intent
    observed = context.require_observation(trade.name)
    if observed.needs_cleanup:
        return context.actions.revoke(trade.name)
    observed = context.require_observation(loan.name)
    if observed.position.debt_scaled or observed.allowance:
        return context.actions.repay(loan.name)
    return close_collateral(context, collateral)


def close_collateral(context, collateral):
    observed = context.require_observation(collateral.name)
    if (
        observed.position.supplied_scaled
        or observed.position.collateral_enabled
        or observed.allowance
    ):
        return context.actions.withdraw(collateral.name)
    return None


def decide(context):
    available = {o.allocation for o in context.observations}
    memory = state(context)
    for account in accounts(context.config):
        allocations = group(context.config, account)
        if not {a.name for a in allocations}.issubset(available):
            continue
        if context.teardown or memory.exiting.get(account, False):
            intent = close(context, account, *allocations)
            memory = memory.updated(exiting={**memory.exiting, account: True})
        else:
            intent = entry(context, account, *allocations)
        if intent is not None:
            return context.decision(intent, memory=memory)
    return context.decision(memory=memory)


def on_outcome(context):
    memory = state(context)
    result = context.result
    account = result.intent.target.account
    if result.status == "failed":
        return memory.updated(exiting={**memory.exiting, account: True})
    if result.status != "completed":
        return memory
    if result.intent.kind == "exit_exchange":
        return memory.updated(traded={**memory.traded, account: False})
    if memory.exiting.get(account, False):
        return memory
    phase = memory.phases.get(account, 0)
    return memory.updated(
        phases={**memory.phases, account: phase + 1},
        traded={**memory.traded, account: memory.traded.get(account, False) or phase == 2},
    )


def on_event(context):
    event = context.event
    affected = {effect.collateral.account for effect in event.liquidations}
    unavailable = {gap.key.chain for gap in event.unavailable}
    affected.update(a.account for a in context.config.positions if a.chain in unavailable)
    memory = state(context)
    return memory.updated(exiting={**memory.exiting, **dict.fromkeys(affected, True)})


def dashboard(context: ui.Context) -> ui.Dashboard:
    return ui.Dashboard(
        title="Independent borrowing and trading",
        description="Each named fund supplies, borrows, trades and exits on its own chain.",
        blocks=(
            ui.Table(
                id="inventory", title="Shared fund cash", data=context.datasets["fund_inventory"]
            ),
            ui.Table(id="gas", title="Submitter costs", data=context.datasets["payer_costs"]),
            ui.Text(
                id="policy",
                text="Healthy-chain work can continue while another chain is unavailable. "
                "Each fund reverses its trade before repaying debt and releasing collateral. "
                "Cash and obligations remain scoped to the fund and chain.",
            ),
        ),
    )
