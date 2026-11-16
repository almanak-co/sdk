"""Buy WETH above its channel midline in calm markets; exit on an ATR trailing stop.

The SDK delivers hourly OHLCV candles of the traded pool (`PoolSwapCandles`), built
from its Swap events; TA-Lib computes ATR from their highs, lows and closes. The
strategy enters while the latest close sits above the midline of the previous
`channel` candles' range and ATR is below `max_atr` of price, and exits once the close
falls `stop_atr` ATR below the highest close since entry, or when the hold ends. The
window spans the channel and the whole hold, so that highest close is always in it.
Entry and exit are levels, not crossings, so a decision reaches the same answer from
the same series. Each exit runs until it has no action left (dust a quote cannot sell
may stay), then memory resets and the next entry signal buys again; a failed buy is
retried until it completes.
"""

import math
from decimal import Decimal
from typing import cast

import numpy as np
import talib
from pydantic import Field, field_validator

from almanak.api import (
    MAX_WINDOW,
    AssetType,
    DataDependencies,
    Memory,
    PoolSwapCandles,
    PriceBinding,
    StrategyState,
    TypedParameters,
    declared,
)
from almanak.connectors.uniswap_v3.api import Decision, DecisionInput, OutcomeInput

WETH = AssetType("eip155:8453/erc20:0x4200000000000000000000000000000000000006")


class StrategyParams(TypedParameters):
    # Candles whose high-low range sets the midline, excluding the latest.
    channel: int = Field(ge=1)
    # ATR lookback, in hourly candles.
    atr_period: int = Field(ge=1)
    # Entries only while ATR is below this fraction of the close.
    max_atr: float = Field(gt=0)
    # Trailing stop distance below the highest close since entry, in ATR.
    stop_atr: float = Field(gt=0)
    # Hourly candles per decision: the channel and the whole hold, so the highest close
    # since entry is always inside the window the stop reads.
    window: int = Field(ge=2, le=MAX_WINDOW)

    @field_validator("window")
    @classmethod
    def exceeds_lookbacks(cls, window, info):
        if window <= max(info.data.get("channel", 0), info.data.get("atr_period", 0)):
            raise ValueError("must exceed the channel and ATR period")
        return window

    @property
    def hold_limit(self) -> int:
        """Seconds a position may be held: the window less the channel, in hours."""
        return (self.window - self.channel) * 3600


class State(StrategyState):
    bought_at: int | None = None
    buying: bool = False  # a buy signalled and not yet completed
    selling: bool = False  # an exit was signalled and still has an action to take


def state(context) -> State:
    return cast(State, context.memory)


def capabilities(config):
    return declared(config)


def validate_config(config):
    if any(a.range is not None or a.band is not None for a in config.allocations):
        raise ValueError("trading allocations must not request liquidity permissions")
    if any(a.hold_seconds is None for a in config.allocations):
        raise ValueError(
            "this strategy needs hold_seconds: its trailing stop reads the candle window"
        )
    if any(a.hold_seconds > config.parameters.hold_limit for a in config.allocations):
        raise ValueError("the hold must end before its entry leaves the candle window")


def data_dependencies(config):
    target = config.target("position")
    return DataDependencies(
        prices=(
            PriceBinding(
                purpose="strategy",
                chain=target.fund.chain,
                asset=WETH,
                window=config.parameters.window,
                source=PoolSwapCandles(
                    kind="pool_swaps",
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
            reason = "Hold time is up, selling" if expired and not current.selling else None
            return context.decision(
                intent=intent, memory=current.updated(selling=True), reason=reason
            )
        # The exit has completed: the cycle is closed and the next entry signal buys.
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
    levels = indicator_levels(series, knobs)
    if levels is None:
        # an undefined indicator decides nothing
        return context.decision(reason="Indicators not available yet")
    close, midline, atr = levels
    if holding:
        stop = highest_close_since(series, current.bought_at) - knobs.stop_atr * atr
        if close < stop:
            return exit_cycle(
                context, current, f"Price {close:,.2f} fell below the {stop:,.2f} stop, selling"
            )
        return context.decision(reason=f"Price {close:,.2f}; trailing stop at {stop:,.2f}")
    if current.bought_at is None and close > midline and atr < knobs.max_atr * close:
        buying = current.updated(buying=True)
        reason = f"Price {close:,.2f} broke above {midline:,.2f} in a calm market, buying"
        return context.decision(
            intent=context.actions.swap("position"), memory=buying, reason=reason
        )
    if current.bought_at is not None:
        return context.decision()
    if close <= midline:
        return context.decision(reason=f"Price {close:,.2f}; buying above {midline:,.2f}")
    swing = f"{atr / close:.1%} of price, above {knobs.max_atr:.1%}"
    return context.decision(reason=f"Price swings too wide ({swing}); waiting to buy")


def exit_cycle(context, current, reason):
    selling = current.updated(selling=True)
    return context.decision(intent=context.actions.exit("position"), memory=selling, reason=reason)


def indicator_levels(series, knobs):
    """The latest close, the previous channel's midline and ATR, or None if undefined."""
    highs, lows, closes = (
        np.array([float(v) for v in values])
        for values in (series.highs, series.lows, series.values)
    )
    atr = float(talib.ATR(highs, lows, closes, knobs.atr_period)[-1])
    close = float(closes[-1])
    channel = knobs.channel
    midline = (highs[-channel - 1 : -1].max() + lows[-channel - 1 : -1].min()) / 2
    if not all(math.isfinite(value) for value in (atr, close, midline)):
        return None
    return close, float(midline), atr


def highest_close_since(series, bought_at):
    """The highest close at or after the entry's candle, derived from the series.

    Derived rather than remembered, so holding writes no memory between decisions.
    """
    entry_candle = 0 if bought_at is None else bought_at - bought_at % 3600
    closes = [p.value for p in series.points if p.source_time >= entry_candle]
    return float(max(closes, default=Decimal(series.values[-1])))


def on_outcome(context: OutcomeInput) -> Memory:
    result = context.result
    if not (result.status == "completed" and result.intent.kind == "swap_exact_input"):
        return context.memory
    if result.intent.amount.asset != result.intent.target.funding_currency:
        return context.memory
    return state(context).updated(bought_at=int(result.effective_at.timestamp()), buying=False)
