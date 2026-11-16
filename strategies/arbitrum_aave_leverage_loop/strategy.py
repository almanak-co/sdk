"""Leverage loop: borrow, buy collateral, resupply; unwind on a health-factor trigger."""

from decimal import Decimal
from typing import cast

from pydantic import Field

from almanak.api import StrategyState, TypedParameters, UnitFraction, declared

BPS = 10_000
WAD = 10**18
NAMES = ("collateral", "loan", "trade")


class StrategyParams(TypedParameters):
    loops: int = Field(ge=1, le=20)
    borrow_fraction: UnitFraction
    unwind_health_factor: Decimal = Field(gt=1)
    swap_headroom_bps: int = Field(ge=0, le=1000)

    @property
    def unwind_trigger(self) -> int:
        """`unwind_health_factor` in wad, like the allocations' minimum_health_factor."""
        return int(self.unwind_health_factor * WAD)


class State(StrategyState):
    # Completed borrows. Every other step is derived from what the fund owns.
    rounds: int = 0
    # Latched by a health-factor breach, a failed action or an external event, so a
    # partly unwound position is never levered up again when the health factor recovers.
    unwinding: bool = False
    # Why the unwind-trigger check refused the next round (empty: none). The loop then
    # holds with fewer rounds than `loops`; the dashboard shows the reason.
    capped_reason: str = ""
    # Why the unwind holds with debt open (the health factor at or below the floor, so
    # no collateral can be withdrawn); empty otherwise. The dashboard shows it.
    shortfall: str = ""


def state(context) -> State:
    return cast(State, context.memory)


def capabilities(config):
    return declared(config)


def floor(config) -> int:
    return config.allocation("loan").minimum_health_factor


def validate_config(config):
    if {a.name for a in config.positions} != set(NAMES):
        raise ValueError("this loop requires collateral, loan and trade allocations")
    collateral, loan, trade = (config.allocation(name) for name in NAMES)
    if len({a.account for a in config.positions}) != 1:
        raise ValueError("lending and trading must use the same named fund")
    if (collateral.chain, collateral.reserve.pool) != (loan.chain, loan.reserve.pool):
        raise ValueError("collateral and debt must use the same chain and Pool")
    if not collateral.collateral or collateral.borrow or loan.collateral or not loan.borrow:
        raise ValueError("collateral supplies as collateral; only the loan allocation borrows")
    if loan.capital:
        # No loan-asset reserve: the unwind sells collateral for the debt plus
        # swap_headroom_bps, interest included, and entry would swap a reserve anyway.
        raise ValueError("the loan allocation starts empty; it only borrows and repays")
    if trade.chain != loan.chain or trade.range is not None or trade.band is not None:
        raise ValueError("the trade swaps on the lending chain without opening liquidity")
    if {trade.pool.currency0, trade.pool.currency1} != {
        collateral.reserve.asset,
        loan.reserve.asset,
    }:
        raise ValueError("the trade pool must exchange the collateral and loan assets")
    # The entry swap sells the trade's funding asset, so it must be the loan asset: entry
    # buys collateral, and the exit swap (the only swap teardown admits) sells it to repay.
    if trade.capital.asset != loan.reserve.asset:
        raise ValueError("trade capital must be the loan asset")
    # The floor refuses every withdrawal that would end below it, the unwind's own
    # included. A trigger at or below the floor would start the unwind with nothing
    # safe to withdraw, so it must sit strictly above.
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


def borrow_size(context, loan) -> tuple[int, int]:
    """(`borrow_fraction` of the collateral value, or 0 when that ends below the trigger;
    the health factor that borrow would end at, in wad)."""
    params = context.config.parameters
    risk = loan.position.risk
    reserve = loan.position.reserve
    unit, price = 10**reserve.asset.decimals, reserve.oracle.price
    amount = int(risk.total_collateral_base * params.borrow_fraction) * unit // price
    debt = risk.total_debt_base + ceil_div(amount * price, unit)
    # Checked against the unwind trigger, not only the gateway floor: a borrow that lands
    # between the two would start an unwind at the next decision. The round is refused and
    # the loop holds where it is; `capped` records why.
    limit = risk.total_collateral_base * risk.liquidation_threshold_bps * WAD
    projected = limit // (debt * BPS) if debt else 2**256 - 1
    return (amount if amount and projected >= params.unwind_trigger else 0), projected


def capped(context, loan, memory, projected) -> State:
    """Memory naming the refused round, so a loop that stops early is never silent."""
    if memory.capped_reason:
        return memory
    params = context.config.parameters
    level = format(ratio(params.unwind_trigger).normalize(), "f")
    reason = (
        f"Stopped after {memory.rounds} of {params.loops} rounds: another borrow would bring "
        f"health to {ratio(projected)}, below the {level} unwind level. Lower the borrow "
        "fraction or the number of rounds."
    )
    return memory.updated(capped_reason=reason)


def ratio(wad) -> Decimal:
    return (Decimal(wad) / WAD).quantize(Decimal("0.01"))


def stalled(context, loan, owed, health) -> str:
    """Why the unwind holds with debt open (empty when it does not), so it is never silent."""
    if not owed:
        return ""
    amount = Decimal(owed).scaleb(-loan.position.reserve.asset.decimals).normalize()
    return (
        f"Unwind stuck: {amount:f} of the borrowed asset is still owed and health "
        f"{ratio(health)} frees no collateral. Send that much to the fund to finish repaying."
    )


def withdraw_size(context, collateral, loan) -> int:
    """Collateral to release for one unwind cycle, never below the floor."""
    risk = loan.position.risk
    if not risk.liquidation_threshold_bps:
        return 0
    reserve = collateral.position.reserve
    unit, price = 10**reserve.asset.decimals, reserve.oracle.price
    # One basis point above the floor absorbs interest accrued before execution.
    target = floor(context.config) * (BPS + 1) // BPS
    keep = ceil_div(risk.total_debt_base * BPS * target, risk.liquidation_threshold_bps * WAD)
    safe = (risk.total_collateral_base - keep) * unit // price
    # Release only what repays the remaining debt plus the headroom, which covers the pool
    # fee, price impact, oracle-to-pool difference and interest until the repay. Selling
    # more would leave loan asset that teardown cannot buy back.
    headroom = context.config.parameters.swap_headroom_bps
    needed = ceil_div(risk.total_debt_base * (BPS + headroom) * unit, BPS * price)
    return max(0, min(safe, needed, collateral.position.supplied_assets))


def enter(context, loan, trade, memory):
    """(the entry intent or None, memory)."""
    actions, config = context.actions, context.config
    cash = owned(trade, config.allocation("loan").reserve.asset)
    # One swap takes min(owned loan asset, trade capital x entry_swap_bps): capital is a
    # slice size, so borrowed proceeds are swapped without prefunding.
    if cash and trade.entry_quote is not None:
        return actions.swap("trade"), memory
    held = owned(trade, config.allocation("collateral").reserve.asset)
    if held:
        return actions.supply("collateral", held), memory
    if cash:
        return None, memory  # loan asset the pool cannot quote yet: wait, don't borrow more
    if memory.rounds < config.parameters.loops:
        amount, projected = borrow_size(context, loan)
        if amount:
            return actions.borrow("loan", amount), memory
        return None, capped(context, loan, memory, projected)
    return None, memory


def unwind(context, collateral, loan, trade):
    actions, config = context.actions, context.config
    cash = owned(trade, config.allocation("loan").reserve.asset)
    debt = loan.position.debt_assets
    if debt:
        # The full-repay form also clears interest accrued until execution, and may spend
        # loan asset sent to the fund beyond what the strategy owns, so it checks the
        # wallet. Partial repays and swaps stay within owned inventory.
        if loan.wallet_balance >= debt:
            return actions.repay("loan")
        if cash:
            return actions.repay_amount("loan", cash)
        held = owned(trade, config.allocation("collateral").reserve.asset)
        if held and trade.exit_quote is not None:
            return actions.swap("trade", exit=True)
        amount = withdraw_size(context, collateral, loan)
        return actions.withdraw_amount("collateral", amount) if amount else None
    position = collateral.position
    if position.supplied_scaled or position.collateral_enabled or collateral.allowance:
        return actions.withdraw("collateral")
    if loan.position.debt_scaled or loan.allowance:
        return actions.repay("loan")
    # Buying collateral adds exposure, which teardown refuses: teardown leaves leftover
    # loan asset as it is. A trigger or failure unwind converts it back.
    if cash and not context.teardown and trade.entry_quote is not None:
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
    if context.teardown or memory.unwinding or health < context.config.parameters.unwind_trigger:
        intent = unwind(context, collateral, loan, trade)
        owed = loan.position.debt_assets if intent is None else 0
        shortfall = stalled(context, loan, owed, health)
        reason = shortfall or unwinding(context, memory, intent, health)
        memory = memory.updated(unwinding=True, shortfall=shortfall)
        return context.decision(intent, memory=memory, reason=reason)
    intent, memory = enter(context, loan, trade, memory)
    debt = health if loan.position.debt_assets else None
    reason = None if intent is not None else holding(context, memory, debt)
    return context.decision(intent, memory=memory, reason=reason)


def unwinding(context, memory, intent, health):
    if intent is None:
        return "Everything is closed; nothing left to do"
    if context.teardown or memory.unwinding:
        return None
    level = format(ratio(context.config.parameters.unwind_trigger).normalize(), "f")
    return f"Health {ratio(health)} fell below the {level} unwind level, unwinding"


def holding(context, memory, health):
    if memory.capped_reason:
        return memory.capped_reason
    level = format(ratio(context.config.parameters.unwind_trigger).normalize(), "f")
    if memory.rounds < context.config.parameters.loops:
        return "Waiting for a swap quote to turn the borrowed cash into collateral"
    status = "No debt" if health is None else f"Health {ratio(health)}"
    return f"All loop rounds done. {status}, above the {level} unwind level"


def on_outcome(context):
    memory = state(context)
    result = context.result
    if result.status == "failed":
        return memory.updated(unwinding=True)
    if result.status == "completed" and result.intent.kind == "borrow":
        return memory.updated(rounds=memory.rounds + 1, capped_reason="")
    return memory


def on_event(context):
    return state(context).updated(unwinding=True)
