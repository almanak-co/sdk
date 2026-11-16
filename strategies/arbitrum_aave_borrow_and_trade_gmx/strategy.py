"""Supply collateral, borrow, trade through GMX, then return, repay and withdraw."""

from typing import cast

from almanak.api import (
    AssetAmount,
    PositiveDecimal,
    StrategyState,
    TypedParameters,
    declared,
    parameter_base_units,
)


class StrategyParams(TypedParameters):
    # Borrowed per action, in the loan asset's units.
    borrow_quantity: PositiveDecimal


class State(StrategyState):
    phase: int = 0
    trade_output: AssetAmount | None = None
    exiting: bool = False


def state(context) -> State:
    return cast(State, context.memory)


def capabilities(config):
    return declared(config)


def borrow_amount(config, name: str) -> int:
    """`borrow_quantity` in base units of the named allocation's reserve asset."""
    asset = config.allocation(name).reserve.asset
    decimals = config.asset_metadata[asset].decimals
    return parameter_base_units(config.parameters, "borrow_quantity", decimals)


def validate_config(config):
    if {value.name for value in config.positions} != {"collateral", "loan", "trade"}:
        raise ValueError("the reference requires collateral, loan and trade allocations")
    collateral, loan, trade = (config.allocation(name) for name in ("collateral", "loan", "trade"))
    validate_scope(config, collateral, loan)
    validate_lending(collateral, loan)
    validate_trade(config, collateral, loan, trade)


def validate_scope(config, collateral, loan):
    validate_single_account(config)
    if collateral.reserve.pool != loan.reserve.pool:
        raise ValueError("the reference requires one lending Pool and one execution chain")
    if len({value.chain for value in config.positions}) != 1:
        raise ValueError("the reference requires one lending Pool and one execution chain")


def validate_single_account(config):
    if len({value.account for value in config.positions}) != 1:
        raise ValueError("lending and trading must share one named fund")


def validate_lending(collateral, loan):
    if not collateral.collateral or collateral.borrow or loan.capital or not loan.borrow:
        raise ValueError("collateral supplies and enables collateral; the loan only borrows")


def validate_trade(config, collateral, loan, trade):
    if (
        trade.entry_amount.asset != loan.reserve.asset
        or not 0 < trade.entry_amount.amount < borrow_amount(config, loan.name)
    ):
        raise ValueError("GMX entry must spend only part of the borrowed asset")
    if {trade.market.long_asset, trade.market.short_asset} != {
        collateral.reserve.asset,
        loan.reserve.asset,
    }:
        raise ValueError("GMX must exchange the collateral and borrowed assets")


def trade_action(context, amount, minimum):
    return context.actions.component("trade").swap("trade", amount=amount, minimum_output=minimum)


def entry(context):
    phase = state(context).phase
    if phase == 0:
        return context.actions.supply("collateral", context.config.allocation("collateral").capital)
    if phase == 1:
        return context.actions.borrow("loan", borrow_amount(context.config, "loan"))
    if phase == 2:
        trade = context.config.allocation("trade")
        return trade_action(context, trade.entry_amount, trade.entry_minimum_output)
    return None


def lending_obligation(context, name):
    observation = context.require_observation(name)
    position = observation.position
    return bool(
        position.supplied_scaled
        or position.debt_scaled
        or position.collateral_enabled
        or observation.allowance
    )


def close(context):
    memory = state(context)
    if memory.trade_output is not None:
        return trade_action(
            context, memory.trade_output, context.config.allocation("trade").exit_minimum_output
        )
    if context.require_observation("trade").needs_cleanup:
        return context.actions.revoke("trade")
    if lending_obligation(context, "loan"):
        return context.actions.repay("loan")
    if lending_obligation(context, "collateral"):
        return context.actions.withdraw("collateral")
    return None


def decide(context):
    memory = state(context)
    if any(context.observation(name) is None for name in ("collateral", "loan", "trade")):
        return context.decision()
    if context.teardown or memory.exiting:
        return context.decision(close(context), memory=memory.updated(exiting=True))
    return context.decision(entry(context))


def on_outcome(context):
    result, memory = context.result, state(context)
    if result.status == "failed":
        return memory.updated(exiting=True)
    if result.status != "completed":
        return memory
    if trade_outcome(result):
        return retain_trade_output(result, memory)
    return memory if memory.exiting else memory.updated(phase=memory.phase + 1)


def trade_outcome(result):
    return result.intent.target.allocation == "trade" and result.intent.kind == "swap_exact_input"


def retain_trade_output(result, memory):
    if memory.exiting:
        return memory.updated(trade_output=None)
    asset = result.intent.minimum_output.asset
    amount = measured_output(result, asset)
    if amount <= 0:
        raise ValueError("settled GMX entry requires a measured positive fund output")
    return memory.updated(phase=3, trade_output=AssetAmount(asset=asset, amount=amount))


def measured_output(result, asset):
    amount = sum(
        value.amount
        for value in result.assets
        if value.key.asset == asset
        and value.key.account == result.intent.target.account
        and value.key.chain == result.intent.target.fund.chain
    )
    return amount


def on_event(context):
    return state(context).updated(exiting=True)
