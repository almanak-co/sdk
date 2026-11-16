"""Banded V3 liquidity that recentres when the pool tick leaves the held range."""

from typing import cast

from pydantic import Field

from almanak.api import Memory, StrategyState, declared
from almanak.connectors.aerodrome_slipstream.api import Decision, DecisionInput, OutcomeInput


class State(StrategyState):
    started_at: dict[str, int] = Field(default_factory=dict)
    rebalanced: dict[str, bool] = Field(default_factory=dict)
    recentred: dict[str, int] = Field(default_factory=dict)


def state(context) -> State:
    return cast(State, context.memory)


def capabilities(config):
    return declared(config)


def validate_config(config):
    if any(a.band is None for a in config.allocations):
        raise ValueError("recentring LP allocations require a tick band")


def decide(context: DecisionInput) -> Decision:
    holds = []
    for item in context.observations:
        observed = context.require_observation(item.allocation)
        allocation = context.config.allocation(observed.allocation)
        intent, reason = decide_allocation(context, allocation, observed)
        if intent is not None:
            return context.decision(intent=intent, reason=reason)
        holds.append(reason)
    return context.decision(reason="; ".join(dict.fromkeys(holds)) or None)


def decide_allocation(context, allocation, observed):
    """The allocation's intent, or None, with why it holds or recentres."""
    started = state(context).started_at.get(allocation.name)
    if exit_due(context, allocation, observed, started):
        intent = exit_allocation(context, allocation, observed)
        return intent, None if intent is not None else "Liquidity closed; nothing left to do"
    position = observed.owned.position
    if position is not None:
        if position.range.lower <= observed.pool.tick < position.range.upper:
            return None, "Price inside the range, nothing to rebalance"
        return context.actions.close_liquidity(allocation.name), "Price left the range, recentring"
    intent = enter_liquidity(context, allocation, observed)
    waiting = "Price is past the band; waiting for it to return"
    return intent, None if intent is not None else waiting


def exit_due(context, allocation, observed, started):
    due = (
        started is not None
        and allocation.hold_seconds is not None
        and int(observed.effective_at.timestamp()) >= started + allocation.hold_seconds
    )
    return context.teardown or due


def exit_allocation(context, allocation, observed):
    if observed.owned.position is not None:
        return context.actions.close_liquidity(allocation.name)
    return context.actions.exit(allocation.name)


def enter_liquidity(context, allocation, observed):
    if not centred_range_holds_tick(observed):
        return None
    # One rebalance swap per cycle: the quote is recomputed after every swap and
    # rounding can leave a residual, so the retained flag ends the loop.
    if not state(context).rebalanced.get(allocation.name) and observed.rebalance_quote is not None:
        return context.actions.swap(allocation.name, rebalance=True)
    return context.actions.open_liquidity(allocation.name)


def centred_range_holds_tick(observed):
    # Beyond the band envelope the centred range is clamped inside it and excludes the
    # tick: a position opened there is out of range at once and would close again, so
    # wait until the price is back inside.
    tick = observed.pool.tick
    selected = observed.target.band.centered(tick, observed.target.pool.tick_spacing)
    return selected.lower <= tick < selected.upper


def on_outcome(context: OutcomeInput) -> Memory:
    result = context.result
    name = result.intent.target.allocation
    current = state(context)
    kind = result.intent.kind
    if result.status == "failed" and kind == "open_liquidity" and current.rebalanced.get(name):
        # The price can move between the rebalance swap and the mint; clearing the flag
        # re-swaps to the current ratio instead of retrying a mint that cannot fill.
        return current.updated(rebalanced={**current.rebalanced, name: False})
    if result.status != "completed":
        return context.memory
    if kind == "swap_exact_input":
        return current.updated(rebalanced={**current.rebalanced, name: True})
    if kind == "close_liquidity":
        return current.updated(rebalanced={**current.rebalanced, name: False})
    if kind == "open_liquidity":
        started = {name: int(result.effective_at.timestamp()), **current.started_at}
        cycles = current.recentred.get(name, 0) + int(name in current.started_at)
        return current.updated(started_at=started, recentred={**current.recentred, name: cycles})
    return context.memory
