"""Borrow USDC against WETH and provide it as Uniswap V3 liquidity; unwind on a trigger."""

from decimal import Decimal
from typing import cast

from pydantic import Field

from almanak.api import StrategyState, TypedParameters, UnitFraction, declared

BPS = 10_000
WAD = 10**18
NAMES = ("collateral", "loan", "liquidity")


class StrategyParams(TypedParameters):
    borrow_fraction: UnitFraction
    unwind_health_factor: Decimal = Field(gt=1)
    swap_headroom_bps: int = Field(ge=0, le=1000)
    # Withdraw and sell collateral when the position returns less than the debt. Needs
    # the collateral to be one of the pool's assets.
    cover_shortfall: bool

    @property
    def unwind_trigger(self) -> int:
        """`unwind_health_factor` in wad, like the allocations' minimum_health_factor."""
        return int(self.unwind_health_factor * WAD)


class State(StrategyState):
    # Latched by a health-factor breach, a failed action or an external event, so a
    # closed position is never reopened when the health factor recovers.
    unwinding: bool = False
    # Why the unwind holds with debt open (the fund short of the debt and, with
    # cover_shortfall, no collateral to withdraw above the floor); empty otherwise.
    # The dashboard shows it.
    shortfall: str = ""


def state(context) -> State:
    return cast(State, context.memory)


def capabilities(config):
    return declared(config)


def floor(config) -> int:
    return config.allocation("loan").minimum_health_factor


def validate_config(config):
    if {a.name for a in config.positions} != set(NAMES):
        raise ValueError("this strategy requires collateral, loan and liquidity allocations")
    collateral, loan, liquidity = (config.allocation(name) for name in NAMES)
    if len({a.account for a in config.positions}) != 1:
        raise ValueError("lending and liquidity must use the same named fund")
    if (collateral.chain, collateral.reserve.pool) != (loan.chain, loan.reserve.pool):
        raise ValueError("collateral and debt must use the same chain and Pool")
    if not collateral.collateral or collateral.borrow or loan.collateral or not loan.borrow:
        raise ValueError("collateral supplies as collateral; only the loan allocation borrows")
    if loan.capital:
        raise ValueError("the loan allocation starts empty; it only borrows and repays")
    if liquidity.chain != loan.chain or liquidity.band is None:
        raise ValueError("the liquidity allocation is a band on the lending chain")
    if loan.reserve.asset not in (liquidity.pool.currency0, liquidity.pool.currency1):
        raise ValueError("the liquidity pool must include the loan asset")
    # Covering a shortfall withdraws collateral and sells it through this pool, so only
    # then must the pool pair the collateral and loan assets. Without it, collateral such
    # as wstETH works with a WETH/USDC pool; a fund has one Uniswap V3 allocation, so
    # there is no second pool for the conversion.
    if config.parameters.cover_shortfall and pair(config) != collateral.reserve.asset:
        raise ValueError(
            "cover_shortfall sells withdrawn collateral through the liquidity pool, so the "
            "pool must pair the collateral and loan assets"
        )
    # Funded in the loan asset, the position's exit converts back to the loan asset,
    # which is what the repay needs. Funded in collateral, it would exit the wrong way.
    if liquidity.capital.asset != loan.reserve.asset:
        raise ValueError("liquidity capital must be the loan asset")
    # The floor refuses every withdrawal that would end below it, the unwind's own
    # included, so the trigger must sit strictly above it.
    if config.parameters.unwind_trigger <= floor(config):
        raise ValueError("unwind_health_factor must be above minimum_health_factor")


def pair(config):
    """The pool's other asset, which the position holds beside the loan asset."""
    pool, loan = config.allocation("liquidity").pool, config.allocation("loan").reserve.asset
    return pool.currency1 if pool.currency0 == loan else pool.currency0


def pooled(config) -> bool:
    """Whether the collateral asset is the pool's other asset (e.g. WETH in WETH/USDC)."""
    return pair(config) == config.allocation("collateral").reserve.asset


def owned(liquidity, asset) -> int:
    """Fund inventory of `asset` the strategy owns (borrowed proceeds included).

    Never the raw wallet balance: spending beyond owned inventory is refused with
    `inventory_unavailable`.
    """
    for value in (liquidity.owned.inventory0, liquidity.owned.inventory1):
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


def withdraw_size(context, collateral, loan, cash, due) -> int:
    """Collateral that, sold with the inventory already held (quoted at `due` USDC),
    covers the debt the owned cash does not; never below the floor.

    The sale is grossed up by swap_headroom_bps twice, so it passes the discounted check
    before the sale and still covers the pool fee, price impact and interest.
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
    sale = ceil_div(max(0, risk.total_debt_base - base(cash)) * (BPS + headroom), BPS - headroom)
    shortfall = max(0, sale - base(due))
    # One basis point above the floor absorbs interest accrued before execution.
    target = floor(context.config) * (BPS + 1) // BPS
    keep = ceil_div(risk.total_debt_base * BPS * target, risk.liquidation_threshold_bps * WAD)
    safe = (risk.total_collateral_base - keep) * unit // price
    needed = ceil_div(shortfall * unit, price)
    return max(0, min(safe, needed, collateral.position.supplied_assets))


def enter(context, collateral, loan, liquidity):
    actions, config = context.actions, context.config
    if liquidity.owned.position is not None:
        return None
    cash = owned(liquidity, config.allocation("loan").reserve.asset)
    held = owned(liquidity, pair(config))
    if not loan.position.debt_assets:
        if pooled(config):
            # Before the borrow, every collateral-asset unit the fund owns is collateral.
            if held:
                return actions.supply("collateral", held)
        elif not collateral.position.supplied_assets:
            # Collateral outside the pool: supply its capital; the pool asset the fund
            # owns (and any loan-asset reserve) stays for the position.
            return actions.supply("collateral", config.allocation("collateral").capital)
        amount = borrow_size(context, loan)
        return actions.borrow("loan", amount) if amount else None
    # After the borrow the fund owns USDC (and any pool asset or reserve of its own): a
    # rebalance swap leaves the ratio the centred band consumes when it owns USDC only,
    # and the open then takes both assets.
    if cash and not held:
        if liquidity.rebalance_quote is None:
            return None
        return actions.component("liquidity").swap("liquidity", rebalance=True)
    if cash or held:
        return actions.open_liquidity("liquidity")
    return None


def unwind(context, collateral, loan, liquidity):
    actions, config = context.actions, context.config
    if liquidity.owned.position is not None:
        return actions.close_liquidity("liquidity")
    cash = owned(liquidity, config.allocation("loan").reserve.asset)
    held = owned(liquidity, pair(config))
    debt = loan.position.debt_assets
    if debt:
        # The full-repay form also clears interest accrued until execution, and may spend
        # loan asset sent to the fund beyond what the strategy owns, so it checks the
        # wallet. Partial repays and swaps stay within owned inventory.
        if loan.wallet_balance >= debt:
            return actions.repay("loan")
        params = config.parameters
        quote = liquidity.exit_quote if held else None
        if params.cover_shortfall:
            # The owned USDC plus what selling the owned WETH returns, less headroom over
            # the whole sale: the exit swap sells every WETH unit the fund owns, the
            # withdrawn collateral included, so one swap carries both.
            proceeds = quote.amount_out.amount if quote is not None else 0
            covered = cash + proceeds * (BPS - params.swap_headroom_bps) // BPS
            if covered < debt:
                # The position returns less than the debt (fees, price moves): release
                # the collateral that covers the rest before the sale, so a single swap
                # and the repay follow. Still short after the swap, this withdraws again.
                amount = withdraw_size(context, collateral, loan, cash, proceeds)
                if amount:
                    return actions.withdraw_amount("collateral", amount)
        if quote is not None:
            return actions.swap("liquidity", exit=True)
        # Repay what the fund owns; the rest waits for loan asset sent to the fund.
        return actions.repay_amount("loan", cash) if cash else None
    position = collateral.position
    if position.supplied_scaled or position.collateral_enabled or collateral.allowance:
        return actions.withdraw("collateral")
    if loan.position.debt_scaled or loan.allowance:
        return actions.repay("loan")
    # Buying collateral adds exposure, which teardown refuses: teardown leaves leftover
    # USDC as it is. A trigger or failure unwind converts it back when the collateral is
    # the pool asset; otherwise it stays as USDC too.
    if cash and not context.teardown and pooled(config) and liquidity.entry_quote is not None:
        return actions.swap("liquidity")
    if liquidity.needs_cleanup:
        return actions.revoke("liquidity")
    return None


def stalled(context, loan, owed) -> str:
    """Why the unwind holds with debt open (empty when it does not), so it is never silent."""
    if not owed:
        return ""
    cover = context.config.parameters.cover_shortfall
    advice = "" if cover else ", or set cover_shortfall: true to repay it from collateral"
    amount = Decimal(owed).scaleb(-loan.position.reserve.asset.decimals).normalize()
    return (
        f"Unwind stuck: {amount:f} of the borrowed asset is still owed and the fund can't "
        f"repay it. Send that much to the fund to finish repaying{advice}."
    )


def decide(context):
    memory = state(context)
    values = observed(context)
    if values is None:
        return context.decision(reason="Waiting for the lending and liquidity positions to be read")
    collateral, loan, liquidity = values
    health = loan.position.risk.health_factor_wad  # no debt reads as the maximum
    if context.teardown or memory.unwinding or health < context.config.parameters.unwind_trigger:
        intent = unwind(context, collateral, loan, liquidity)
        owed = loan.position.debt_assets if intent is None else 0
        shortfall = stalled(context, loan, owed)
        reason = shortfall or unwinding(context, memory, intent, loan)
        memory = memory.updated(unwinding=True, shortfall=shortfall)
        return context.decision(intent, memory=memory, reason=reason)
    intent = enter(context, collateral, loan, liquidity)
    reason = None if intent is not None else holding(context, loan, liquidity)
    return context.decision(intent, reason=reason)


def level(context) -> str:
    return format(context.config.parameters.unwind_health_factor.normalize(), "f")


def health_text(loan) -> str:
    if not loan.position.debt_assets:
        return "No debt"
    return f"Health {loan.position.risk.health_factor_wad / WAD:.2f}"


def unwinding(context, memory, intent, loan):
    if intent is None:
        return "Everything is closed; nothing left to do"
    if context.teardown or memory.unwinding:
        return None
    return f"{health_text(loan)} fell below the {level(context)} unwind level, unwinding"


def holding(context, loan, liquidity):
    if liquidity.owned.position is not None:
        return f"Liquidity open. {health_text(loan)}, above the {level(context)} unwind level"
    if not loan.position.debt_assets:
        return f"Not borrowing: the loan would take health near the {level(context)} unwind level"
    return "Waiting for a swap quote to balance the borrowed cash for liquidity"


def on_outcome(context):
    if context.result.status == "failed":
        return state(context).updated(unwinding=True)
    return state(context)


def on_event(context):
    return state(context).updated(unwinding=True)
