"""Buy WETH with USDC when its hourly RSI crosses below oversold; sell above overbought.

The SDK delivers the hourly WETH price in USDC of the traded pool, read from the
pool's own state at each hour; TA-Lib computes the indicator from those closes. Each
exit runs until it has no action left (dust a quote cannot sell may stay), then
memory resets and the next oversold cross buys again; a failed buy is retried until
it completes.
"""

import math
from typing import cast

import numpy as np
import talib
from pydantic import Field, field_validator

from almanak.api import (
    MAX_WINDOW,
    AssetType,
    DataDependencies,
    Memory,
    PoolPrice,
    PriceBinding,
    StrategyState,
    TypedParameters,
    declared,
)
from almanak.connectors.uniswap_v3.api import Decision, DecisionInput, OutcomeInput

WETH = AssetType("eip155:8453/erc20:0x4200000000000000000000000000000000000006")


class StrategyParams(TypedParameters):
    period: int = Field(ge=2)  # RSI lookback, in hourly closes
    overbought: float = Field(ge=0, le=100)  # RSI level an upward cross of which sells
    oversold: float = Field(ge=0, le=100)  # RSI level a downward cross of which buys
    # Hourly closes per decision; RSI needs several periods.
    window: int = Field(ge=3, le=MAX_WINDOW)

    @field_validator("oversold")
    @classmethod
    def below_overbought(cls, oversold, info):
        if oversold >= info.data.get("overbought", float("inf")):
            raise ValueError("must be below overbought")
        return oversold

    @field_validator("window")
    @classmethod
    def exceeds_period(cls, window, info):
        if window <= info.data.get("period", 0):
            raise ValueError("must exceed the RSI period")
        return window


class State(StrategyState):
    last_rsi: float | None = None
    bought_at: int | None = None
    buying: bool = False  # a buy signalled and not yet completed
    selling: bool = False  # an exit was signalled and still has an action to take


def state(context) -> State:
    return cast(State, context.memory)


def capabilities(config):
    return declared(config)


def validate_config(config):
    if any(a.range is not None for a in config.allocations):
        raise ValueError("trading allocations must not request liquidity permissions")


def data_dependencies(config):
    target = config.target("position")
    return DataDependencies(
        prices=(
            PriceBinding(
                purpose="strategy",
                chain=target.fund.chain,
                asset=WETH,
                window=config.parameters.window,
                source=PoolPrice(
                    kind="pool_state",
                    connector="uniswap_v3",
                    pool=target.pool.pool,  # the traded pool; WETH priced in USDC
                    timeframe="1h",
                    max_age_seconds=7200,
                    availability_lag_seconds=300,
                ),
            ),
        )
    )


def decide(context: DecisionInput) -> Decision:
    observed = context.require_observation("position")
    allocation = context.config.allocation("position")
    current = state(context)
    expired = (
        current.bought_at is not None
        and allocation.hold_seconds is not None
        and int(observed.effective_at.timestamp()) >= current.bought_at + allocation.hold_seconds
    )
    if context.teardown or current.selling or expired:
        intent = context.actions.exit("position")
        if intent is not None or context.teardown:
            selling = current.updated(selling=True)
            reason = "Hold time is up, selling" if expired and not current.selling else None
            return context.decision(intent=intent, memory=selling, reason=reason)
        # The exit has completed: the cycle is closed and the next oversold cross buys.
        current = current.updated(selling=False, bought_at=None)
    holding = observed.owned.entry_complete
    if current.buying and not holding:
        intent = context.actions.swap("position")  # retry the signal
        return context.decision(intent=intent, reason="Retrying the buy")
    series = context.price(WETH)
    if not series.complete:
        # a short or stale window decides nothing
        return context.decision(reason="Waiting for a full WETH price history")
    knobs = context.config.parameters
    rsi = float(talib.RSI(np.array([float(v) for v in series.values]), knobs.period)[-1])
    if not math.isfinite(rsi):
        # an undefined indicator decides nothing
        return context.decision(reason="RSI not available yet")
    previous = current.last_rsi
    memory = current.updated(last_rsi=rsi)
    if previous is not None and not holding and previous >= knobs.oversold > rsi:
        buying = memory.updated(buying=True)
        reason = f"RSI {rsi:.1f} crossed below {knobs.oversold:g}, buying"
        return context.decision(
            intent=context.actions.swap("position"), memory=buying, reason=reason
        )
    if previous is not None and holding and previous <= knobs.overbought < rsi:
        selling = memory.updated(selling=True)
        reason = f"RSI {rsi:.1f} crossed above {knobs.overbought:g}, selling"
        return context.decision(
            intent=context.actions.exit("position"), memory=selling, reason=reason
        )
    level = f"selling above {knobs.overbought:g}" if holding else f"buying below {knobs.oversold:g}"
    return context.decision(memory=memory, reason=f"RSI {rsi:.0f}; {level}")


def on_outcome(context: OutcomeInput) -> Memory:
    result = context.result
    if not (result.status == "completed" and result.intent.kind == "swap_exact_input"):
        return context.memory
    if result.intent.amount.asset != result.intent.target.funding_currency:
        return context.memory
    return state(context).updated(bought_at=int(result.effective_at.timestamp()), buying=False)
