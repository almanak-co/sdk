"""Short WETH: borrow it against USDC and sell it; buy back and repay at a stop or target.

The stop and take-profit compare the Pool oracle's WETH price with the price when the
short was sold. A health-factor trigger above the gateway floor unwinds too.
"""

from decimal import Decimal
from typing import cast

from pydantic import Field

from almanak import dashboard as ui
from almanak.api import StrategyState, TypedParameters, UnitFraction, declared
from almanak.connectors.aave_v4.models import Position as AaveV4Position

BPS = 10_000
WAD = 10**18
NAMES = ("collateral", "loan", "trade")


class StrategyParams(TypedParameters):
    borrow_fraction: UnitFraction
    stop_loss_bps: int = Field(gt=0, le=BPS)
    take_profit_bps: int = Field(gt=0, le=BPS)
    unwind_health_factor: Decimal = Field(gt=1)
    swap_headroom_bps: int = Field(ge=0, le=1000)

    @property
    def unwind_trigger(self) -> int:
        """`unwind_health_factor` in wad, like the allocations' minimum_health_factor."""
        return int(self.unwind_health_factor * WAD)


class State(StrategyState):
    # The loan asset's oracle price when the first slice of the short was sold: the
    # stop's reference, so later slices sold higher never move the stop up.
    entry_price: int | None = None
    # Latched by the stop, the take-profit, a health-factor breach, a failed action or
    # an external event, so a closed short is never reopened.
    unwinding: bool = False


def state(context) -> State:
    return cast(State, context.memory)


def capabilities(config):
    return declared(config)


def floor(config) -> int:
    return config.allocation("loan").minimum_health_factor


def validate_config(config):
    if any(isinstance(a, AaveV4Position) for a in config.positions):
        raise ValueError("this short needs Aave V3's partial withdraw and repay")
    if {a.name for a in config.positions} != set(NAMES):
        raise ValueError("this strategy requires collateral, loan and trade allocations")
    collateral, loan, trade = (config.allocation(name) for name in NAMES)
    if len({a.account for a in config.positions}) != 1:
        raise ValueError("lending and trading must use the same named fund")
    if (collateral.chain, collateral.reserve.pool) != (loan.chain, loan.reserve.pool):
        raise ValueError("collateral and debt must use the same chain and Pool")
    if not collateral.collateral or collateral.borrow or loan.collateral or not loan.borrow:
        raise ValueError("collateral supplies as collateral; only the loan allocation borrows")
    if loan.capital:
        # No loan-asset reserve: the stop withdraws collateral for the shortfall plus
        # swap_headroom_bps, interest included, and entry would sell a reserve anyway.
        raise ValueError("the loan allocation starts empty; it only borrows and repays")
    if trade.chain != loan.chain or trade.range is not None or trade.band is not None:
        raise ValueError("the trade swaps on the lending chain without opening liquidity")
    if {trade.pool.currency0, trade.pool.currency1} != {
        collateral.reserve.asset,
        loan.reserve.asset,
    }:
        raise ValueError("the trade pool must exchange the collateral and loan assets")
    # The entry swap sells the trade's funding asset: the borrowed asset is what the short
    # sells, and the exit swap (the only swap teardown admits) buys it back to repay.
    if trade.capital.asset != loan.reserve.asset:
        raise ValueError("trade capital must be the loan asset")
    # The floor refuses every withdrawal that would end below it, the unwind's own
    # included, so the trigger must sit strictly above it.
    if config.parameters.unwind_trigger <= floor(config):
        raise ValueError("unwind_health_factor must be above minimum_health_factor")


def owned(trade, asset) -> int:
    """Fund inventory of `asset` the strategy owns (borrowed proceeds included).

    Never the raw wallet balance: spending beyond owned inventory is refused with
    `inventory_unavailable`.
    """
    for value in (trade.owned.inventory0, trade.owned.inventory1):
        if value.asset == asset:
            return value.amount
    return 0


def observed(context):
    values = tuple(context.observation(name) for name in NAMES)
    if any(value is None for value in values):
        return None
    collateral, loan, _ = values
    if (
        loan.position.risk is None
        or collateral.position.reserve.oracle is None
        or loan.position.reserve.oracle is None
    ):
        return None
    return values


def ceil_div(numerator, denominator):
    return -(-numerator // denominator)


def borrow_size(context, loan) -> int:
    """`borrow_fraction` of the collateral value, or 0 when that ends below the trigger."""
    params = context.config.parameters
    risk = loan.position.risk
    reserve = loan.position.reserve
    unit, price = 10**reserve.asset.decimals, reserve.oracle.price
    amount = int(risk.total_collateral_base * params.borrow_fraction) * unit // price
    debt = risk.total_debt_base + ceil_div(amount * price, unit)
    # Checked against the unwind trigger, not only the gateway floor: a borrow that lands
    # between the two would start an unwind at the next decision.
    limit = risk.total_collateral_base * risk.liquidation_threshold_bps * WAD
    return amount if amount and debt * BPS * params.unwind_trigger <= limit else 0


def withdraw_size(context, collateral, loan, bought, due) -> int:
    """USDC collateral that, spent on the buy-back with the USDC already held (quoted at
    `due` WETH), covers the debt the WETH already `bought` does not; never below the floor.

    The buy-back is grossed up by swap_headroom_bps twice, so it passes the discounted
    check before it and still covers the pool fee, price impact and interest.
    """
    risk = loan.position.risk
    if not risk.liquidation_threshold_bps:
        return 0
    reserve = collateral.position.reserve
    unit, price = 10**reserve.asset.decimals, reserve.oracle.price
    loan_reserve = loan.position.reserve

    def base(amount):
        return amount * loan_reserve.oracle.price // 10**loan_reserve.asset.decimals

    headroom = context.config.parameters.swap_headroom_bps
    sale = ceil_div(max(0, risk.total_debt_base - base(bought)) * (BPS + headroom), BPS - headroom)
    shortfall = max(0, sale - base(due))
    # One basis point above the floor absorbs interest accrued before execution.
    target = floor(context.config) * (BPS + 1) // BPS
    keep = ceil_div(risk.total_debt_base * BPS * target, risk.liquidation_threshold_bps * WAD)
    safe = (risk.total_collateral_base - keep) * unit // price
    needed = ceil_div(shortfall * unit, price)
    return max(0, min(safe, needed, collateral.position.supplied_assets))


def enter(context, loan, trade):
    actions, config = context.actions, context.config
    if not loan.position.debt_assets:
        # Before the borrow, every collateral-asset unit the fund owns is collateral.
        held = owned(trade, config.allocation("collateral").reserve.asset)
        if held:
            return actions.supply("collateral", held)
        amount = borrow_size(context, loan)
        return actions.borrow("loan", amount) if amount else None
    # One swap sells min(owned WETH, trade capital x entry_swap_bps): capital is a slice
    # size, so the borrowed WETH is sold without prefunding. The proceeds stay in the
    # fund, unsupplied, to buy the WETH back.
    if owned(trade, config.allocation("loan").reserve.asset) and trade.entry_quote is not None:
        return actions.swap("trade")
    return None


def stopped(context, loan, memory) -> bool:
    """The stop or the take-profit, against the oracle price the short was sold at."""
    if memory.entry_price is None or not loan.position.debt_assets:
        return False
    params = context.config.parameters
    move = (loan.position.reserve.oracle.price - memory.entry_price) * BPS
    # A short loses when the borrowed asset's price rises.
    return (
        move >= params.stop_loss_bps * memory.entry_price
        or -move >= params.take_profit_bps * memory.entry_price
    )


def unwind(context, collateral, loan, trade):
    actions, config = context.actions, context.config
    bought = owned(trade, config.allocation("loan").reserve.asset)
    proceeds = owned(trade, config.allocation("collateral").reserve.asset)
    debt = loan.position.debt_assets
    if debt:
        # The full-repay form also clears interest accrued until execution, and may spend
        # loan asset sent to the fund beyond what the strategy owns, so it checks the
        # wallet. Partial repays and swaps stay within owned inventory.
        if loan.wallet_balance >= debt:
            return actions.repay("loan")
        quote = trade.exit_quote if proceeds else None
        # The owned WETH plus what the buy-back returns, less headroom over the whole
        # purchase: the exit swap spends every USDC unit the fund owns, the withdrawn
        # collateral included, so one swap carries both.
        due = quote.amount_out.amount if quote is not None else 0
        covered = bought + due * (BPS - config.parameters.swap_headroom_bps) // BPS
        if covered < debt:
            # The proceeds buy back less than the debt (a loss, fees, interest): release
            # the collateral that covers the rest before the buy-back, so a single swap
            # and the repay follow. Still short after the swap, this withdraws again.
            amount = withdraw_size(context, collateral, loan, bought, due)
            if amount:
                return actions.withdraw_amount("collateral", amount)
        # Buy the WETH back with every unit of USDC the fund holds.
        if quote is not None:
            return actions.swap("trade", exit=True)
        # Nothing safe to withdraw: repay what the fund owns to lift the health factor.
        return actions.repay_amount("loan", bought) if bought else None
    position = collateral.position
    if position.supplied_scaled or position.collateral_enabled or collateral.allowance:
        return actions.withdraw("collateral")
    if loan.position.debt_scaled or loan.allowance:
        return actions.repay("loan")
    # Selling WETH again adds exposure, which teardown refuses: teardown leaves leftover
    # WETH (a profitable short buys back more than it owes) as it is. A stop, target,
    # trigger or failure unwind sells it back to USDC.
    if bought and not context.teardown and trade.entry_quote is not None:
        return actions.swap("trade")
    if trade.needs_cleanup:
        return actions.revoke("trade")
    return None


def decide(context):
    memory = state(context)
    values = observed(context)
    if values is None:
        return context.decision(reason="Waiting for the lending and trade positions to be read")
    collateral, loan, trade = values
    health = loan.position.risk.health_factor_wad  # no debt reads as the maximum
    breached = health < context.config.parameters.unwind_trigger
    if context.teardown or memory.unwinding or breached or stopped(context, loan, memory):
        intent = unwind(context, collateral, loan, trade)
        reason = unwinding(context, memory, intent, loan, breached)
        return context.decision(intent, memory=memory.updated(unwinding=True), reason=reason)
    intent = enter(context, loan, trade)
    if intent is not None and loan.position.debt_assets and memory.entry_price is None:
        price = loan.position.reserve.oracle.price
        return context.decision(intent, memory=memory.updated(entry_price=price))
    reason = None if intent is not None else holding(context, loan, memory)
    return context.decision(intent, reason=reason)


def level(context) -> str:
    return format(context.config.parameters.unwind_health_factor.normalize(), "f")


def price_move(loan, memory) -> float:
    """The borrowed asset's price change since the sale, in percent."""
    return (loan.position.reserve.oracle.price / memory.entry_price - 1) * 100


def unwinding(context, memory, intent, loan, breached):
    if intent is None:
        if loan.position.debt_assets:
            return "Debt still open and nothing left to repay it with; send the borrowed asset"
        return "Everything is closed; nothing left to do"
    if context.teardown or memory.unwinding:
        return None
    if breached:
        health = loan.position.risk.health_factor_wad / WAD
        return f"Health {health:.2f} fell below the {level(context)} unwind level, buying back"
    move = price_move(loan, memory)
    if move > 0:
        return f"Stop: price {move:.1f}% above the sale price, buying back"
    return f"Target: price {-move:.1f}% below the sale price, buying back"


def holding(context, loan, memory):
    if not loan.position.debt_assets:
        return f"Not borrowing: the loan would take health near the {level(context)} unwind level"
    if memory.entry_price is None:
        return "Waiting for a swap quote to sell the borrowed asset"
    params = context.config.parameters
    move = price_move(loan, memory)
    return (
        f"Short open, price {move:+.1f}% since the sale; stop at "
        f"+{params.stop_loss_bps / 100:g}%, target at -{params.take_profit_bps / 100:g}%"
    )


def on_outcome(context):
    if context.result.status == "failed":
        return state(context).updated(unwinding=True)
    return state(context)


def on_event(context):
    return state(context).updated(unwinding=True)


def dashboard(context: ui.Context) -> ui.Dashboard:
    return ui.Dashboard(
        title="Aave V3 lending short",
        description="One fund supplies USDC, borrows WETH and sells it.",
        blocks=(
            ui.Table(
                id="inventory", title="Shared fund cash", data=context.datasets["fund_inventory"]
            ),
            ui.Table(id="gas", title="Submitter costs", data=context.datasets["payer_costs"]),
            ui.Text(
                id="policy",
                text="The stop and take-profit compare the oracle WETH price with the price "
                "at the sale. The unwind buys WETH back with the proceeds, covers any "
                "shortfall from collateral and repays.",
            ),
        ),
    )
