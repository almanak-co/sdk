"""Open and increase GM liquidity, partially withdraw, hold, close and revoke."""

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
    settled: dict[str, int] = Field(default_factory=dict)
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
    memory = state(context)
    name = allocation.name
    if context.teardown or name in memory.failed:
        return context.actions.exit(name)
    stage = memory.settled.get(name, 0)
    if stage < 2:
        amounts = allocation.entry_amounts if stage == 0 else allocation.additional_amounts
        action = (
            context.actions.open_liquidity if stage == 0 else context.actions.increase_liquidity
        )
        return action(name, maximum0=amounts.root[0], maximum1=amounts.root[1])
    if stage == 2:
        return context.actions.decrease_liquidity(
            name,
            shares=allocation.partial_withdrawal,
            minimum0=allocation.withdrawal_minimums.root[0],
            minimum1=allocation.withdrawal_minimums.root[1],
        )
    return None


def hold_reason(context, allocation, observed):
    if context.teardown or allocation.name in state(context).failed:
        return "Liquidity closed; nothing left to do"
    return "Liquidity in place; holding until the strategy stops"


def on_outcome(context: OutcomeInput) -> Memory:
    result, memory = context.result, state(context)
    if isinstance(result, LiquidationResult):
        return memory
    if not result.complete or result.intent.kind == "revoke_exchange":
        return memory
    name = result.intent.target.allocation
    if not result.success:
        return memory.updated(failed=(*memory.failed, name))
    stage = memory.settled.get(name, 0) + 1
    held = memory.held_at
    if stage == 3:
        held = {**held, name: int(result.effective_at.timestamp())}
    return memory.updated(settled={**memory.settled, name: stage}, held_at=held)
