"""Deposit USDC to HyperCore, set cross leverage, hold one BTC perp, close and withdraw.

The phase 3 reference lifecycle as a strategy (blueprint 30, sections 12 and 13): the
first deposit pays HyperCore's activation fee, leverage is set while flat, one `Ioc`
order opens the position, a reduce-only `Ioc` order closes it when the hold ends or at
teardown, and every USDC left on HyperCore returns to the fund on HyperEVM. A position
the venue liquidates ends the hold. The same file runs in backtests and on forks.
"""

from decimal import Decimal
from typing import Literal, cast

from pydantic import Field

from almanak.api import Memory, StrategyState, TypedParameters, declared
from almanak.connectors.hyperliquid.api import (
    Decision,
    DecisionInput,
    EffectsResult,
    OutcomeInput,
)


class StrategyParams(TypedParameters):
    size: str = Field(pattern=r"^(0|[1-9][0-9]*)\.[0-9]+$")  # coins, such as '0.002'
    side: Literal["long", "short"]


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
        intent = decide_allocation(context, observed)
        if intent is not None:
            return context.decision(intent=intent)
        holds.append(hold_reason(context, observed))
    return context.decision(reason="; ".join(dict.fromkeys(holds)) or None)


def decide_allocation(context, observed):
    name, account = observed.allocation, observed.owned
    allocation = context.config.allocation(name)
    if done(context, observed):
        if account.holding is not None:
            return context.actions.close(name)
        if observed.withdrawable > 0:
            return context.actions.withdraw(name)
        return None
    if not account.activated:
        return context.actions.deposit(name)
    if account.leverage != allocation.leverage:
        return context.actions.set_leverage(name)
    if account.holding is None:
        knobs = context.config.parameters
        return context.actions.open(name, side=knobs.side, size=Decimal(knobs.size))
    return None


def done(context, observed):
    name = observed.allocation
    allocation = context.config.allocation(name)
    opened = state(context).opened_at.get(name)
    held = (
        opened is not None
        and allocation.hold_seconds is not None
        and int(observed.effective_at.timestamp()) >= opened + allocation.hold_seconds
    )
    # A position the venue closed (liquidated) ends the hold as well.
    closed = opened is not None and observed.owned.holding is None
    return context.teardown or held or closed or name in state(context).finished


def hold_reason(context, observed):
    if done(context, observed):
        return "Position closed and USDC withdrawn; nothing left to do"
    return f"Holding {observed.owned.holding.szi} {observed.target.market.coin}"


def on_outcome(context: OutcomeInput) -> Memory:
    result = context.result
    # A fork run reports fills, funding and liquidations as effects; the next observation
    # shows what they changed, as a backtest's does. Only the intent's completed step
    # opens or closes the position: an agent renewal or a leverage change before the order
    # is an intermediate one, and an `Ioc` order that matched nothing did neither.
    if isinstance(result, EffectsResult) or not (result.success and result.complete):
        return context.memory
    if result.applied is False:
        return context.memory
    name = result.intent.target.allocation
    if result.intent.kind == "increase_position":
        return state(context).updated(
            opened_at={**state(context).opened_at, name: int(result.effective_at.timestamp())}
        )
    if result.intent.kind == "close_position" and name not in state(context).finished:
        return state(context).updated(finished=(*state(context).finished, name))
    return context.memory
