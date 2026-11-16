"""Provide WETH/USDC liquidity while RSI says the market is ranging; leave when it trends.

The SDK delivers the hourly WETH closes of the pool the strategy provides liquidity
in, read from its on-chain state at each hour; TA-Lib computes RSI from them. A
concentrated position earns fees while the price oscillates inside it and loses to
holding when the price trends away, so the strategy opens a banded position while
RSI is inside `ranging` and closes it, returning to USDC, once RSI leaves `trending`.
Both are levels, not crossings, so a decision reaches the same answer from the same
series. Each exit runs until it has no action left (dust a quote cannot sell may
stay), then memory resets and the next ranging reading opens again; an entry that has
started finishes regardless of the next reading.
"""

import math
from typing import Annotated, cast

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
Level = Annotated[float, Field(ge=0, le=100)]


class StrategyParams(TypedParameters):
    period: int = Field(ge=2)  # RSI lookback, in hourly closes
    ranging: tuple[Level, Level]  # RSI band in which the position opens
    trending: tuple[Level, Level]  # RSI outside this band closes the position
    # Hourly closes per decision; RSI needs several periods.
    window: int = Field(ge=3, le=MAX_WINDOW)

    @field_validator("trending")
    @classmethod
    def contains_ranging(cls, trending, info):
        # The exit band contains the entry band, so a reading that opens never also closes.
        low, high = info.data.get("ranging", trending)
        if not trending[0] <= low < high <= trending[1]:
            raise ValueError("must contain ranging, each lower level first")
        return trending

    @field_validator("window")
    @classmethod
    def exceeds_period(cls, window, info):
        if window <= info.data.get("period", 0):
            raise ValueError("must exceed the RSI period")
        return window


class State(StrategyState):
    entering: bool = False  # an entry started and its position is not open yet
    rebalanced: bool = False  # the entry's funding swap completed
    opened_at: int | None = None
    closing: bool = False  # an exit was signalled and still has an action to take


def state(context) -> State:
    return cast(State, context.memory)


def capabilities(config):
    return declared(config)


def validate_config(config):
    if any(a.band is None for a in config.allocations):
        raise ValueError("the RSI LP reference requires a tick band")


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
                    pool=target.pool.pool,  # the pool it provides liquidity in
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
        current.opened_at is not None
        and allocation.hold_seconds is not None
        and int(observed.effective_at.timestamp()) >= current.opened_at + allocation.hold_seconds
    )
    if context.teardown or current.closing or expired:
        intent = closing_intent(context, observed)
        if intent is not None or context.teardown:
            reason = "Hold time is up, closing" if expired and not current.closing else None
            return context.decision(
                intent=intent, memory=current.updated(closing=True), reason=reason
            )
        # The exit has completed: the cycle is closed and the next ranging reading opens.
        current = State()
    if current.entering and observed.owned.position is None:
        return context.decision(intent=enter(context, current, observed))  # finish it
    knobs = context.config.parameters
    if current.opened_at is None and observed.owned.position is None:
        rsi = latest_rsi(context.price(WETH), knobs.period)
        low, high = knobs.ranging
        if rsi is not None and low <= rsi <= high and centred_range_holds_tick(observed):
            entering = current.updated(entering=True)
            return context.decision(intent=enter(context, current, observed), memory=entering)
        return context.decision(reason=waiting_to_open(rsi, low, high))
    if observed.owned.position is not None:
        rsi = latest_rsi(context.price(WETH), knobs.period)
        low, high = knobs.trending
        if rsi is None:
            return context.decision(reason="Liquidity open; waiting for a full WETH price history")
        if not low <= rsi <= high:
            closing = current.updated(closing=True)
            reason = f"RSI {rsi:.1f} left {low:g} to {high:g}, closing"
            intent = closing_intent(context, observed)
            return context.decision(intent=intent, memory=closing, reason=reason)
        return context.decision(reason=f"RSI {rsi:.0f}; closing if it leaves {low:g} to {high:g}")
    return context.decision()


def waiting_to_open(rsi, low, high):
    if rsi is None:
        return "Waiting for a full WETH price history"
    if low <= rsi <= high:
        return "Price is past the band; waiting for it to return"
    return f"RSI {rsi:.0f}; opening when it is between {low:g} and {high:g}"


def latest_rsi(series, period):
    """RSI of the latest close, or None for a short, stale or undefined window."""
    if not series.complete:
        return None
    rsi = float(talib.RSI(np.array([float(v) for v in series.values]), period)[-1])
    return rsi if math.isfinite(rsi) else None


def enter(context, current, observed):
    if not centred_range_holds_tick(observed):
        return None
    # One funding swap per entry: its quote is recomputed after every swap and rounding
    # can leave a residual, so the retained flag ends the loop.
    if not current.rebalanced and observed.rebalance_quote is not None:
        return context.actions.swap("position", rebalance=True)
    return context.actions.open_liquidity("position")


def centred_range_holds_tick(observed):
    # Beyond the band envelope the centred range is clamped inside it and excludes the
    # tick: a position opened there is out of range at once, so wait for the price.
    tick = observed.pool.tick
    selected = observed.target.band.centered(tick, observed.target.pool.tick_spacing)
    return selected.lower <= tick < selected.upper


def closing_intent(context, observed):
    if observed.owned.position is not None:
        return context.actions.close_liquidity("position")
    return context.actions.exit("position")


def on_outcome(context: OutcomeInput) -> Memory:
    result = context.result
    current = state(context)
    if result.status == "failed" and result.intent.kind == "open_liquidity" and current.rebalanced:
        # The price can move between the funding swap and the mint; clearing the flag
        # re-swaps to the current ratio instead of retrying a mint that cannot fill.
        return current.updated(rebalanced=False)
    if result.status != "completed":
        return context.memory
    if result.intent.kind == "swap_exact_input" and current.entering:
        return current.updated(rebalanced=True)
    if result.intent.kind == "open_liquidity":
        opened = int(result.effective_at.timestamp())
        return current.updated(entering=False, opened_at=opened)
    return context.memory
