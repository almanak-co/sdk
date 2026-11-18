"""Request a GMX market swap, hold its settled proceeds, return and revoke authority."""

from typing import cast

from pydantic import Field

from almanak.api import Memory, StrategyState, declared
from almanak.connectors.gmx_v2.api import (
    Decision,
    DecisionInput,
    LiquidationResult,
    OutcomeInput,
)


class State(StrategyState):
    held_at: dict[str, int] = Field(default_factory=dict)
    failed: tuple[str, ...] = ()


def state(context) -> State:
    return cast(State, context.memory)


def capabilities(config):
    return declared(config)


def decide(context: DecisionInput) -> Decision:
    holds = []
    for item in context.observations:
        observed = context.require_observation(item.allocation)
        allocation = context.config.allocation(item.allocation)
        intent = allocation_intent(context, allocation, observed)
        if intent is not None:
            return context.decision(intent=intent)
        holds.append(hold_reason(context, allocation, observed))
    return context.decision(reason="; ".join(dict.fromkeys(holds)) or None)


def allocation_intent(context, allocation, observed):
    if exit_due(context, allocation, observed):
        return context.actions.exit(allocation.name)
    if state(context).held_at.get(allocation.name) is None:
        return context.actions.swap(
            allocation.name,
            amount=allocation.entry_amount,
            minimum_output=allocation.entry_minimum_output,
        )
    return None


def exit_due(context, allocation, observed):
    memory = state(context)
    held_at = memory.held_at.get(allocation.name)
    elapsed = (
        held_at is not None
        and allocation.hold_seconds is not None
        and observed.block.timestamp >= held_at + allocation.hold_seconds
    )
    return context.teardown or allocation.name in memory.failed or elapsed


def hold_reason(context, allocation, observed):
    if exit_due(context, allocation, observed):
        return "Swapped back; nothing left to do"
    if allocation.hold_seconds is None:
        return "Swapped; holding until the strategy stops"
    return "Swapped; swapping back when the hold time ends"


def on_outcome(context: OutcomeInput) -> Memory:
    result, memory = context.result, state(context)
    if isinstance(result, LiquidationResult):
        return memory
    if not result.complete:
        return memory
    name = result.intent.target.allocation
    if not result.success:
        return memory.updated(failed=(*memory.failed, name))
    if name not in memory.held_at:
        return memory.updated(
            held_at={**memory.held_at, name: int(result.effective_at.timestamp())}
        )
    return memory
