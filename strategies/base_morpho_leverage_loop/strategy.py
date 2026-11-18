"""Leverage loop: borrow, buy collateral, resupply; unwind on a health-factor trigger."""

from decimal import Decimal
from typing import cast

from pydantic import Field

from almanak import dashboard as ui
from almanak.api import StrategyState, TypedParameters, UnitFraction, declared
from almanak.connectors.morpho_blue.math import ORACLE_SCALE
from almanak.connectors.morpho_blue.risk import borrowed_assets, health_factor

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
    if collateral.reserve.market != loan.reserve.market:
        raise ValueError("collateral and debt must use the same explicit Morpho market")
    if collateral.reserve.is_loan or not loan.reserve.is_loan:
        raise ValueError("collateral uses the market's collateral leg and the loan its loan leg")
    # No loan-asset reserve: the unwind sells collateral for the debt plus
    # swap_headroom_bps, interest included, and entry would swap a reserve anyway.
    if collateral.borrow or not loan.borrow or loan.capital:
        raise ValueError("only the loan allocation borrows, and it starts empty")
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
    if values[1].account.market.price is None:
        return None
    return values


def ceil_div(numerator, denominator):
    return -(-numerator // denominator)


def debt(loan) -> int:
    """Debt in loan base units, with interest accrued to the observed block."""
    return borrowed_assets(loan.account) if loan.account.position.borrow_shares else 0


def borrow_size(context, loan) -> tuple[int, int]:
    """(`borrow_fraction` of the collateral value, or 0 when that ends below the trigger;
    the health factor that borrow would end at, in wad)."""
    params = context.config.parameters
    market = loan.account.market
    value = loan.account.position.collateral * market.price.price // ORACLE_SCALE
    amount = int(value * params.borrow_fraction)
    # Checked against the unwind trigger, not only the gateway floor: a borrow that lands
    # between the two would start an unwind at the next decision. The round is refused and
    # the loop holds where it is; `capped` records why.
    owed = debt(loan) + amount
    projected = value * market.source.lltv // owed if owed else 2**256 - 1
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


def stalled(context, owed, health) -> str:
    """Why the unwind holds with debt open (empty when it does not), so it is never silent."""
    if not owed:
        return ""
    return (
        f"Unwind stuck: debt is still owed and health {ratio(health)} frees no collateral. "
        "Send the borrowed asset to the fund to finish repaying."
    )


def withdraw_size(context, loan) -> int:
    """Collateral to release for one unwind cycle, never below the floor."""
    market = loan.account.market
    price = market.price.price
    owed = debt(loan)
    # One basis point above the floor absorbs interest accrued before execution.
    target = floor(context.config) * (BPS + 1) // BPS
    keep = ceil_div(owed * target * ORACLE_SCALE, market.source.lltv * price)
    # Release only what repays the remaining debt plus the headroom, which covers the pool
    # fee, price impact, oracle-to-pool difference and interest until the repay. Selling
    # more would leave loan asset that teardown cannot buy back.
    headroom = context.config.parameters.swap_headroom_bps
    needed = ceil_div(owed * (BPS + headroom) * ORACLE_SCALE, BPS * price)
    return max(0, min(loan.account.position.collateral - keep, needed))


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
    owed = debt(loan)
    if owed:
        # The full-repay form also clears interest accrued until execution, and may spend
        # loan asset sent to the fund beyond what the strategy owns, so it checks the
        # wallet. Partial repays and swaps stay within owned inventory.
        if loan.wallet_balance >= owed:
            return actions.repay("loan")
        if cash:
            return actions.repay_amount("loan", cash)
        held = owned(trade, config.allocation("collateral").reserve.asset)
        if held and trade.exit_quote is not None:
            return actions.swap("trade", exit=True)
        amount = withdraw_size(context, loan)
        return actions.withdraw_amount("collateral", amount) if amount else None
    if collateral.account.position.collateral or collateral.allowance:
        return actions.withdraw("collateral")
    if loan.account.position.borrow_shares or loan.allowance:
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
    health = health_factor(loan.account)  # None without debt
    breached = health is not None and health < context.config.parameters.unwind_trigger
    if context.teardown or memory.unwinding or breached:
        intent = unwind(context, collateral, loan, trade)
        shortfall = stalled(context, debt(loan) if intent is None else 0, health)
        reason = shortfall or unwinding(context, memory, intent, health)
        memory = memory.updated(unwinding=True, shortfall=shortfall)
        return context.decision(intent, memory=memory, reason=reason)
    intent, memory = enter(context, loan, trade, memory)
    reason = None if intent is not None else holding(context, memory, health)
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


def dashboard(context: ui.Context) -> ui.Dashboard:
    return ui.Dashboard(
        title="Morpho Blue leverage loop",
        description="One fund supplies collateral, borrows, buys more collateral and resupplies.",
        blocks=(
            ui.Table(
                id="inventory", title="Shared fund cash", data=context.datasets["fund_inventory"]
            ),
            ui.Table(id="gas", title="Submitter costs", data=context.datasets["payer_costs"]),
            ui.Text(
                id="policy",
                text="Each round borrows a share of the collateral value, swaps it into "
                "collateral and supplies it. The unwind repeats withdraw, swap and repay "
                "until the debt is zero, then withdraws the rest.",
            ),
            ui.Text(id="rounds", text=rounds_text(context.memory.values)),
            ui.Text(
                id="shortfall",
                text=str(context.memory.values.get("shortfall") or "No repay shortfall."),
            ),
        ),
    )


def rounds_text(values) -> str:
    reason = values.get("capped_reason")
    if reason:
        return str(reason)
    return f"Completed borrow rounds: {values.get('rounds', 0)}. No round has been refused."
