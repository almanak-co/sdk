"""Open, increase, partially decrease, hold and close one configured GMX position."""

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
    completed: dict[str, int] = Field(default_factory=dict)
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
    name = allocation.name
    if exit_due(context, allocation, observed):
        return context.actions.exit(name)
    completed = state(context).completed.get(name, 0)
    if completed < 2:
        return context.actions.increase(
            name,
            collateral_base_units=allocation.entry_collateral.amount,
            size_delta_usd_e30=allocation.entry_size_usd_e30,
        )
    if completed == 2:
        return context.actions.decrease(
            name, collateral_base_units=0, size_delta_usd_e30=allocation.entry_size_usd_e30
        )
    return None


def exit_due(context, allocation, observed):
    memory = state(context)
    held_at = memory.held_at.get(allocation.name)
    elapsed = (
        held_at is not None
        and allocation.hold_seconds is not None
        and int(observed.effective_at.timestamp()) >= held_at + allocation.hold_seconds
    )
    return context.teardown or allocation.name in memory.failed or elapsed


def hold_reason(context, allocation, observed):
    if exit_due(context, allocation, observed):
        return "Position closed; nothing left to do"
    if allocation.hold_seconds is None:
        return "All position steps done; holding until the strategy stops"
    return "All position steps done; closing when the hold time ends"


def on_outcome(context: OutcomeInput) -> Memory:
    result = context.result
    if isinstance(result, LiquidationResult):
        return state(context).updated(
            failed=tuple(dict.fromkeys((*state(context).failed, result.target.allocation)))
        )
    if not result.complete:
        return context.memory
    name = result.intent.target.allocation
    memory = state(context)
    if not result.success:
        return memory.updated(failed=(*memory.failed, name))
    completed = memory.completed.get(name, 0) + 1
    held_at = dict(memory.held_at)
    if completed == 3:
        held_at[name] = int(result.effective_at.timestamp())
    return memory.updated(completed={**memory.completed, name: completed}, held_at=held_at)
