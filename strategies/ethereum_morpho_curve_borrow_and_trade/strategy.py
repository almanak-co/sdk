"""Borrow against WETH, trade part of the proceeds, then reverse and repay."""

from typing import cast

from almanak import dashboard as ui
from almanak.api import (
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
    traded: bool = False
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
    if {a.name for a in config.positions} != {"collateral", "loan", "trade"}:
        raise ValueError("this reference requires collateral, loan and trade allocations")
    collateral = config.allocation("collateral")
    loan = config.allocation("loan")
    trade = config.allocation("trade")
    if len({a.account for a in config.positions}) != 1:
        raise ValueError("borrowing and trading must use the same named fund")
    validate_lending_pair(collateral, loan)
    validate_trade_pair(config, collateral, loan, trade)
    validate_initial_cash(config, collateral)


def validate_lending_pair(collateral, loan):
    if collateral.reserve.market != loan.reserve.market:
        raise ValueError("collateral and debt must use the same explicit lending market")
    if collateral.borrow or loan.capital or not loan.borrow:
        raise ValueError("collateral requires supply authority and the loan requires borrowing")


def validate_trade_pair(config, collateral, loan, trade):
    validate_trade_scope(collateral, loan, trade)
    capital = funding_capital(trade)
    if trade.funding_asset != loan.reserve.asset or not 0 < capital < borrow_amount(
        config, loan.name
    ):
        raise ValueError("trade capital must consume only part of the borrowed asset")


def funding_capital(trade):
    return next(value.amount for value in trade.capital.root if value.asset == trade.funding_asset)


def validate_trade_scope(collateral, loan, trade):
    if trade.chain != loan.chain or trade.liquidity_enabled:
        raise ValueError("this reference trades on the lending chain without opening liquidity")
    if collateral.reserve.asset in trade.pool.coins:
        raise ValueError("this reference retains collateral separately from both traded coins")


def validate_initial_cash(config, collateral):
    amounts = [
        balance.amount
        for balance in config.inventory
        if (balance.key.account, balance.key.asset)
        == (collateral.account, collateral.reserve.asset)
    ]
    if amounts != [collateral.capital]:
        raise ValueError("this reference supplies all initial collateral-asset cash before trading")


def entry(context):
    phase = state(context).phase
    if phase == 0:
        return context.actions.supply("collateral", context.config.allocation("collateral").capital)
    if phase == 1:
        return context.actions.borrow("loan", borrow_amount(context.config, "loan"))
    if phase == 2:
        return context.actions.swap("trade")
    return None


def close_exchange(context):
    if state(context).traded:
        intent = context.actions.exit("trade")
        if intent is not None:
            return intent
    if context.require_observation("trade").needs_cleanup:
        return context.actions.revoke("trade")
    return None


def lending_obligation(context, name):
    observation = context.require_observation(name)
    owned = next(p for p in context.owned if p.target.allocation == name)
    return not owned.empty or observation.allowance


def close(context):
    exchange = close_exchange(context)
    if exchange is not None:
        return exchange
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
    memory = state(context)
    result = context.result
    if result.status == "failed":
        return memory.updated(exiting=True)
    if result.status != "completed":
        return memory
    return completed_outcome(memory, result.intent.kind)


def completed_outcome(memory, kind):
    if kind == "exit_exchange":
        return memory.updated(traded=False)
    if memory.exiting:
        return memory
    return memory.updated(phase=memory.phase + 1, traded=memory.traded or memory.phase == 2)


def on_event(context):
    return state(context).updated(exiting=True)


def dashboard(context: ui.Context) -> ui.Dashboard:
    return ui.Dashboard(
        title="Morpho borrowing and Curve trading",
        description="One fund supplies collateral, borrows and trades part of its proceeds.",
        blocks=(
            ui.Table(
                id="inventory", title="Shared fund cash", data=context.datasets["fund_inventory"]
            ),
            ui.Table(id="gas", title="Submitter costs", data=context.datasets["payer_costs"]),
            ui.Text(
                id="policy",
                text="Teardown reverses the trade before repaying debt and releasing collateral. "
                "Fund cash is counted once across protocols. These retained quantities do not "
                "include a common-currency portfolio valuation.",
            ),
        ),
    )
