"""Configured-asset V4 round trip with retained strategy state."""

from typing import cast

from pydantic import Field

from almanak.api import Memory, StrategyState, declared
from almanak.connectors.uniswap_v4.api import Decision, DecisionInput, OutcomeInput


class State(StrategyState):
    bought_at: dict[str, int] = Field(default_factory=dict)


def state(context) -> State:
    return cast(State, context.memory)


def capabilities(config):
    return declared(config)


def validate_config(config):
    if any(a.range is not None for a in config.allocations):
        raise ValueError("trading allocations must not request liquidity permissions")


def decide(context: DecisionInput) -> Decision:
    for item in context.observations:
        observed = context.require_observation(item.allocation)
        allocation = context.config.allocation(observed.allocation)
        intent = decide_allocation(context, allocation, observed)
        if intent is not None:
            return context.decision(intent=intent)
    return context.decision()


def decide_allocation(context, allocation, observed):
    started = state(context).bought_at.get(allocation.name)
    due = (
        started is not None
        and allocation.hold_seconds is not None
        and int(observed.effective_at.timestamp()) >= started + allocation.hold_seconds
    )
    if context.teardown or due:
        return context.actions.exit(allocation.name)
    if not observed.owned.entry_complete:
        return context.actions.swap(allocation.name)
    return None


def on_outcome(context: OutcomeInput) -> Memory:
    result = context.result
    if not (result.status == "completed" and result.intent.kind == "swap_exact_input"):
        return context.memory
    if result.intent.amount.asset != result.intent.target.funding_currency:
        return context.memory
    started = {
        **state(context).bought_at,
        result.intent.target.allocation: int(result.effective_at.timestamp()),
    }
    return state(context).updated(bought_at=started)
