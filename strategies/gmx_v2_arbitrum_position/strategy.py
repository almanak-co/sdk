"""One bounded GMX request, keeper settlement, hold and full close."""

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
    opened_at: dict[str, int] = Field(default_factory=dict)
    finished: tuple[str, ...] = ()


def state(context) -> State:
    return cast(State, context.memory)


def capabilities(config):
    return declared(config)


def decide(context: DecisionInput) -> Decision:
    holds = []
    for item in context.observations:
        observed = context.require_observation(item.allocation)
        allocation = context.config.allocation(item.allocation)
        intent = decide_allocation(context, allocation, observed)
        if intent is not None:
            return context.decision(intent=intent)
        holds.append(hold_reason(context, allocation, observed))
    return context.decision(reason="; ".join(dict.fromkeys(holds)) or None)


def decide_allocation(context, allocation, observed):
    opened = state(context).opened_at.get(allocation.name)
    if exit_due(context, allocation, observed, opened):
        return context.actions.exit(allocation.name)
    if opened is None and observed.owned.position is None:
        return context.actions.increase(
            allocation.name,
            collateral_base_units=allocation.entry_collateral.amount,
            size_delta_usd_e30=allocation.entry_size_usd_e30,
        )
    return None


def hold_reason(context, allocation, observed):
    opened = state(context).opened_at.get(allocation.name)
    if exit_due(context, allocation, observed, opened):
        return "Position closed; nothing left to do"
    if allocation.hold_seconds is None:
        return "Position open; holding until the strategy stops"
    return "Position open; closing when the hold time ends"


def on_outcome(context: OutcomeInput) -> Memory:
    result = context.result
    if isinstance(result, LiquidationResult):
        return state(context).updated(
            finished=tuple(dict.fromkeys((*state(context).finished, result.target.allocation)))
        )
    if not result.complete:
        return context.memory
    name = result.intent.target.allocation
    if result.success and result.intent.kind == "increase_position":
        return state(context).updated(
            opened_at={
                **state(context).opened_at,
                name: int(result.effective_at.timestamp()),
            }
        )
    if name not in state(context).finished:
        return state(context).updated(finished=(*state(context).finished, name))
    return context.memory


def exit_due(context, allocation, observed, opened):
    due = (
        opened is not None
        and allocation.hold_seconds is not None
        and int(observed.effective_at.timestamp()) >= opened + allocation.hold_seconds
    )
    return context.teardown or due or allocation.name in state(context).finished
