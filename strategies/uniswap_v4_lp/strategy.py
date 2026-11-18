"""USDC-funded fixed-range liquidity with one collection and complete exit."""

from typing import cast

from pydantic import Field

from almanak.api import Memory, StrategyState, declared
from almanak.connectors.uniswap_v4.api import (
    Decision,
    DecisionInput,
    OutcomeInput,
)


class State(StrategyState):
    minted_at: dict[str, int] = Field(default_factory=dict)
    collected_at: dict[str, int] = Field(default_factory=dict)


def state(context) -> State:
    return cast(State, context.memory)


def capabilities(config):
    return declared(config)


def validate_config(config):
    if any(a.range is None for a in config.allocations):
        raise ValueError("LP allocations require explicit fixed ranges")


def decide(context: DecisionInput) -> Decision:
    holds = []
    for item in context.observations:
        observed = context.require_observation(item.allocation)
        allocation = context.config.allocation(observed.allocation)
        intent = decide_allocation(context, allocation, observed)
        if intent is not None:
            return context.decision(intent=intent)
        holds.append(hold_reason(context, allocation, observed))
    return context.decision(reason="; ".join(dict.fromkeys(holds)) or None)


def decide_allocation(context, allocation, observed):
    started = state(context).minted_at.get(allocation.name)
    if exit_due(context, allocation, observed, started):
        return exit_allocation(context, allocation, observed)
    if observed.owned.position is not None:
        return collect_liquidity(context, allocation, observed, started)
    if started is None:
        return enter_liquidity(context, allocation, observed)
    return None


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


def collect_liquidity(context, allocation, observed, started):
    collected = state(context).collected_at.get(allocation.name)
    if (
        collected is None
        and allocation.collect_seconds is not None
        and int(observed.effective_at.timestamp()) >= started + allocation.collect_seconds
    ):
        return context.actions.collect(allocation.name)
    return None


def enter_liquidity(context, allocation, observed):
    if not observed.owned.entry_complete and observed.entry_quote is not None:
        return context.actions.swap(allocation.name)
    return context.actions.open_liquidity(allocation.name)


def hold_reason(context, allocation, observed):
    started = state(context).minted_at.get(allocation.name)
    if exit_due(context, allocation, observed, started) or observed.owned.position is None:
        return "Liquidity closed; nothing left to do"
    if (
        state(context).collected_at.get(allocation.name) is None
        and allocation.collect_seconds is not None
    ):
        return "Liquidity in place; collecting fees when the collect time comes"
    if allocation.hold_seconds is None:
        return "Liquidity in place; holding until the strategy stops"
    return "Liquidity in place; closing when the hold time ends"


def on_outcome(context: OutcomeInput) -> Memory:
    result = context.result
    if not (result.status == "completed"):
        return context.memory
    key = {"open_liquidity": "minted_at", "collect_liquidity_fees": "collected_at"}.get(
        result.intent.kind
    )
    if key is None:
        return context.memory
    timestamps = {
        **getattr(state(context), key),
        result.intent.target.allocation: int(result.effective_at.timestamp()),
    }
    return state(context).updated(**{key: timestamps})
