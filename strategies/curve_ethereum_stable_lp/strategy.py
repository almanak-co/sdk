"""Curve StableSwap NG fungible LP supply, increase, partial withdrawal and exit."""

from typing import cast

from pydantic import Field

from almanak.api import Memory, StrategyState, declared
from almanak.connectors.curve.api import Decision, DecisionInput, OutcomeInput


class State(StrategyState):
    minted_at: dict[str, int] = Field(default_factory=dict)
    completed: dict[str, tuple[str, ...]] = Field(default_factory=dict)


def state(context) -> State:
    return cast(State, context.memory)


def capabilities(config):
    return declared(config)


def validate_config(config):
    if any(not a.liquidity_enabled for a in config.allocations):
        raise ValueError("LP allocations require explicit liquidity authority")


def decide(context: DecisionInput) -> Decision:
    for item in context.observations:
        observed = context.require_observation(item.allocation)
        allocation = context.config.allocation(observed.allocation)
        intent = decide_allocation(context, allocation, observed)
        if intent is not None:
            return context.decision(intent=intent)
    return context.decision()


def decide_allocation(context, allocation, observed):
    started = state(context).minted_at.get(allocation.name)
    if exit_due(context, allocation, observed, started):
        return exit_allocation(context, allocation, observed)
    if observed.owned.position is not None:
        return manage_liquidity(context, allocation, observed)
    if started is None:
        if not observed.owned.entry_complete:
            return context.actions.swap(allocation.name)
        return context.actions.open_liquidity(allocation.name, fraction_bps=5000)
    return None


def exit_allocation(context, allocation, observed):
    if observed.owned.position is not None:
        return context.actions.close_liquidity(allocation.name)
    return context.actions.exit(allocation.name)


def exit_due(context, allocation, observed, started):
    due = (
        started is not None
        and allocation.hold_seconds is not None
        and int(observed.effective_at.timestamp()) >= started + allocation.hold_seconds
    )
    return context.teardown or due


def manage_liquidity(context, allocation, observed):
    completed = state(context).completed.get(allocation.name, ())
    if "increase_liquidity" not in completed:
        return context.actions.increase_liquidity(allocation.name)
    if "decrease_liquidity" not in completed:
        liquidity = observed.owned.position.shares.amount // 2
        if liquidity == 0:
            raise ValueError("partial exit requires at least two owned liquidity units")
        return context.actions.decrease_liquidity(allocation.name, liquidity)
    return None


def on_outcome(context: OutcomeInput) -> Memory:
    result = context.result
    if result.status != "completed":
        return context.memory
    name = result.intent.target.allocation
    completed = state(context).completed.get(name, ())
    minted = dict(state(context).minted_at)
    if result.intent.kind == "open_liquidity":
        minted[name] = int(result.effective_at.timestamp())
    return state(context).updated(
        minted_at=minted,
        completed={**state(context).completed, name: (*completed, result.intent.kind)},
    )
