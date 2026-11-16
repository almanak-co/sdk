"""Borrow against WETH and keep the health factor inside a band around a target."""

from decimal import Decimal
from typing import cast

from pydantic import Field

from almanak import dashboard as ui
from almanak.api import StrategyState, TypedParameters, declared

BPS = 10_000
WAD = 10**18


class StrategyParams(TypedParameters):
    repay_health_factor: Decimal = Field(gt=1)
    target_health_factor: Decimal = Field(gt=1)
    borrow_health_factor: Decimal = Field(gt=1)


class State(StrategyState):
    # Latched by a failed action or an external event such as a liquidation.
    unwinding: bool = False
    # Why the unwind holds with debt open; empty otherwise. The dashboard shows it.
    shortfall: str = ""


def state(context) -> State:
    return cast(State, context.memory)


def capabilities(config):
    return declared(config)


def wad(value: Decimal) -> int:
    return int(value * WAD)


def validate_config(config):
    if {a.name for a in config.positions} != {"collateral", "loan"}:
        raise ValueError("this strategy requires collateral and loan allocations")
    collateral, loan = config.allocation("collateral"), config.allocation("loan")
    if (collateral.account, collateral.reserve.pool, collateral.chain) != (
        loan.account,
        loan.reserve.pool,
        loan.chain,
    ):
        raise ValueError("this strategy borrows against collateral in the same fund and Pool")
    if not collateral.collateral or collateral.borrow or not collateral.capital:
        raise ValueError("collateral requires starting capital and no borrowing")
    # loan.capital is optional: a loan-asset reserve the strategy never supplies or
    # spends, so the full repay can pay the interest accrued since the borrow.
    if not loan.borrow or loan.collateral:
        raise ValueError("loan requires borrowing and is not collateral")
    params = config.parameters
    floor = Decimal(loan.minimum_health_factor) / WAD
    # The floor refuses borrows and withdrawals that end below it. The repay trigger
    # sits above it so the strategy de-risks before the gateway would block it, and
    # the hysteresis between the triggers keeps interest drift from churning.
    if not (
        floor
        < params.repay_health_factor
        < params.target_health_factor
        < params.borrow_health_factor
    ):
        raise ValueError(
            "health factors must order minimum_health_factor < repay_health_factor "
            "< target_health_factor < borrow_health_factor"
        )


def debt_change(loan, target) -> int:
    """Loan base units that move the health factor to `target`: + borrow, - repay."""
    risk = loan.position.risk
    reserve = loan.position.reserve
    unit, price = 10**reserve.asset.decimals, reserve.oracle.price
    wanted = risk.total_collateral_base * risk.liquidation_threshold_bps * WAD // (BPS * target)
    return (wanted - risk.total_debt_base) * unit // price


def manage(context):
    collateral, loan = context.observation("collateral"), context.observation("loan")
    actions, config = context.actions, context.config
    if not collateral.position.supplied_assets and not loan.position.debt_assets:
        return actions.supply("collateral", config.allocation("collateral").capital)
    risk = loan.position.risk
    if risk is None or loan.position.reserve.oracle is None:
        return None
    params = config.parameters
    target = wad(params.target_health_factor)
    if risk.health_factor_wad > wad(params.borrow_health_factor):  # no debt reads as the maximum
        amount = debt_change(loan, target)
        return actions.borrow("loan", amount) if amount > 0 else None
    if risk.health_factor_wad < wad(params.repay_health_factor):
        # A repay only raises the health factor, so the floor never blocks it.
        amount = min(-debt_change(loan, target), loan.position.debt_assets)
        return actions.repay_amount("loan", amount) if amount > 0 else None
    return None


def close(context):
    collateral, loan = context.observation("collateral"), context.observation("loan")
    actions, debt = context.actions, loan.position.debt_assets
    if debt:
        # Interest is not prefunded: the full repay needs the debt and the interest
        # accrued until execution in the fund. Short of that, repay what the fund holds.
        if loan.wallet_balance >= debt:
            return actions.repay("loan")
        return actions.repay_amount("loan", loan.wallet_balance) if loan.wallet_balance else None
    if loan.position.debt_scaled or loan.allowance:
        return actions.repay("loan")
    position = collateral.position
    if position.supplied_scaled or position.collateral_enabled or collateral.allowance:
        return actions.withdraw("collateral")
    return None


def stalled(loan, owed) -> str:
    """Why the unwind holds with debt open (empty when it does not), so it is never silent."""
    if not owed:
        return ""
    amount = Decimal(owed).scaleb(-loan.position.reserve.asset.decimals).normalize()
    return (
        f"Unwind stuck: {amount:f} of the borrowed asset is still owed and the fund can't "
        "repay it. Send that much to the fund to finish repaying."
    )


def decide(context):
    memory = state(context)
    if None in (context.observation("collateral"), context.observation("loan")):
        return context.decision(reason="Waiting for lending position data")
    if context.teardown or memory.unwinding:
        intent = close(context)
        loan = context.observation("loan")
        owed = loan.position.debt_assets if intent is None else 0
        shortfall = stalled(loan, owed)
        if intent is None:
            reason = shortfall or "Repaid and withdrawn; nothing left to do"
        elif memory.unwinding:
            reason = "Unwinding after a failed step or an outside event"
        else:
            reason = None
        return context.decision(
            intent, memory=memory.updated(unwinding=True, shortfall=shortfall), reason=reason
        )
    intent = manage(context)
    return context.decision(intent, reason=health_reason(context, intent))


def health_reason(context, intent):
    """Where the health factor sits against the borrow and repay levels."""
    collateral, loan = context.observation("collateral"), context.observation("loan")
    if not collateral.position.supplied_assets and not loan.position.debt_assets:
        return None  # the opening supply explains itself
    risk = loan.position.risk
    if risk is None or loan.position.reserve.oracle is None:
        return "Waiting for health factor data"
    params = context.config.parameters
    health = Decimal(risk.health_factor_wad) / WAD
    borrow, repay = f"{params.borrow_health_factor:g}", f"{params.repay_health_factor:g}"
    target = f"{params.target_health_factor:g}"
    if risk.health_factor_wad > wad(params.borrow_health_factor):
        if not loan.position.debt_assets:
            return f"No debt yet; borrowing to health {target}" if intent else "No debt to manage"
        if intent:
            return f"Health {health:.2f}, above {borrow}; borrowing more to bring it to {target}"
        return f"Health {health:.2f}, above {borrow}, but the extra borrow rounds to zero"
    if risk.health_factor_wad < wad(params.repay_health_factor):
        if intent:
            return f"Health fell to {health:.2f}, below {repay}; repaying to {target}"
        return f"Health {health:.2f}, below {repay}, but there is nothing to repay"
    return f"Health {health:.2f}, inside the {repay} to {borrow} band"


def on_outcome(context):
    if context.result.status == "failed":
        return state(context).updated(unwinding=True)
    return state(context)


def on_event(context):
    return state(context).updated(unwinding=True)


def dashboard(context: ui.Context) -> ui.Dashboard:
    return ui.Dashboard(
        title="Aave V3 health-factor-managed borrow",
        description="Supplied WETH backs a USDC borrow kept inside a health-factor band.",
        blocks=(
            ui.Table(
                id="positions", title="Aave positions", data=context.datasets["lending_positions"]
            ),
            ui.Table(id="gas", title="Fund execution costs", data=context.datasets["fund_costs"]),
            ui.Text(
                id="policy",
                text="Below repay_health_factor the strategy repays to the target; above "
                "borrow_health_factor it borrows to the target. Teardown repays in full and "
                "withdraws.",
            ),
            ui.Text(
                id="shortfall",
                text=str(context.memory.values.get("shortfall") or "No repay shortfall."),
            ),
        ),
    )
