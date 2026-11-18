"""Borrow against WAVAX, trade part of the proceeds, then reverse and repay."""

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


def required_allocations(config):
    if {a.name for a in config.positions} != {"collateral", "loan", "trade"}:
        raise ValueError("this reference requires collateral, loan and trade allocations")
    return tuple(config.allocation(n) for n in ("collateral", "loan", "trade"))


def validate_common_fund(positions):
    if len({a.account for a in positions}) != 1:
        raise ValueError("borrowing and trading must use the same named fund")


def borrow_amount(config, name: str) -> int:
    """`borrow_quantity` in base units of the named allocation's reserve asset."""
    asset = config.allocation(name).reserve.asset
    decimals = config.asset_metadata[asset].decimals
    return parameter_base_units(config.parameters, "borrow_quantity", decimals)


def validate_config(config):
    collateral, loan, trade = required_allocations(config)
    validate_common_fund(config.positions)
    validate_lending_pair(collateral, loan)
    validate_trade_pair(config, collateral, loan, trade)
    validate_initial_cash(config, collateral)


def validate_lending_pair(collateral, loan):
    if (collateral.chain, collateral.reserve.pool) != (loan.chain, loan.reserve.pool):
        raise ValueError("collateral and debt must use the same chain and Pool")
    if not collateral.collateral or collateral.borrow or loan.capital or not loan.borrow:
        raise ValueError("collateral requires supply authority and the loan requires borrowing")


def validate_trade_pair(config, collateral, loan, trade):
    validate_trade_scope(collateral, loan, trade)
    validate_trade_capital(config, collateral, loan, trade)
    validate_trade_assets(collateral, loan, trade)


def validate_trade_scope(collateral, loan, trade):
    if trade.chain != loan.chain or trade.range is not None:
        raise ValueError("this reference trades on the lending chain without opening liquidity")


def validate_trade_capital(config, collateral, loan, trade):
    if trade.capital.asset != loan.reserve.asset or trade.capital.amount >= borrow_amount(
        config, loan.name
    ):
        raise ValueError("trade capital must consume only part of the borrowed asset")


def validate_trade_assets(collateral, loan, trade):
    if {trade.pool.currency0, trade.pool.currency1} != {
        collateral.reserve.asset,
        loan.reserve.asset,
    }:
        raise ValueError("the configured trade must exchange the collateral and borrowed assets")


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
    return None


def repayment_required(loan):
    return loan.position.debt_scaled or loan.allowance


def close_collateral(context):
    collateral = context.require_observation("collateral")
    if (
        collateral.position.supplied_scaled
        or collateral.position.collateral_enabled
        or collateral.allowance
    ):
        return context.actions.withdraw("collateral")
    return None


def cleanup(context):
    trade = context.require_observation("trade")
    if trade.needs_cleanup:
        return context.actions.revoke("trade")
    loan = context.require_observation("loan")
    if repayment_required(loan):
        return context.actions.repay("loan")
    return close_collateral(context)


def close(context):
    exchange = close_exchange(context)
    if exchange is not None:
        return exchange
    return cleanup(context)


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
        title="Borrow and trade",
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
