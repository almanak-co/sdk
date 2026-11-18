"""Borrow USDC against WETH collateral only while WETH trades above its moving average.

The strategy supplies WETH collateral on Aave V3 and holds a USDC loan while the
latest hourly WETH close is above its simple moving average by `borrow_above`,
repaying it once the close falls below it by `repay_below`. The gap between the two
levels keeps a price hovering at the average from borrowing and repaying every hour. A
falling collateral price is what brings a loan toward liquidation, so the trend
filter takes the loan off before a downtrend does. The closes come from the Base
WETH/USDC Uniswap V3 pool's on-chain state at each hour: a lending strategy may read
any supported pool on its chain. Both rules are levels, not crossings, so a decision
reaches the same answer from the same series. The loan is taken and repaid each time
the trend crosses the levels; teardown repays and withdraws.
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
    PoolPrice,
    PositiveDecimal,
    PriceBinding,
    StrategyState,
    TypedParameters,
    declared,
    parameter_base_units,
)
from almanak.connectors.aave_v3.models import Parameters

WETH = AssetType("eip155:8453/erc20:0x4200000000000000000000000000000000000006")
USDC = AssetType("eip155:8453/erc20:0x833589fcd6edb6e08f4c7c32d4f71b54bda02913")
PRICE_POOL = "0xd0b53d9277642d899df5c87a3966a349a798f224"  # Uniswap V3 WETH/USDC 0.05%


class StrategyParams(TypedParameters):
    period: int = Field(ge=2)  # SMA lookback, in hourly closes
    window: int = Field(ge=2, le=MAX_WINDOW)  # hourly closes per decision
    # Borrow while the close exceeds the average by this; repay once it is below it by
    # repay_below. The gap between the two keeps a price at the average from churning.
    borrow_above: float = Field(gt=1)
    repay_below: float = Field(gt=0, lt=1)
    # Borrowed per action, in the loan asset's units.
    borrow_quantity: PositiveDecimal

    @field_validator("window")
    @classmethod
    def covers_period(cls, window, info):
        if window < info.data.get("period", 0):
            raise ValueError("must cover at least one period")
        return window


class State(StrategyState):
    supplied: bool = False
    borrowed: bool = False


def state(context) -> State:
    return cast(State, context.memory)


def capabilities(config):
    return declared(config)


def validate_config(config: Parameters) -> None:
    if {a.name for a in config.positions} != {"collateral", "loan"}:
        raise ValueError("this reference requires collateral and loan allocations")
    collateral, loan = config.allocation("collateral"), config.allocation("loan")
    require_same_fund(collateral, loan)
    require_collateral(collateral)
    require_loan(loan)
    borrow_amount(config)  # refuses a borrow_quantity finer than the loan asset


def require_same_fund(collateral, loan):
    if (collateral.account, collateral.reserve.pool, collateral.chain) != (
        loan.account,
        loan.reserve.pool,
        loan.chain,
    ):
        raise ValueError("this reference borrows against collateral in the same fund and Pool")


def require_collateral(collateral):
    # The trend filter reads the WETH series, so the collateral it protects is WETH.
    if collateral.reserve.asset != WETH:
        raise ValueError("the SMA filter prices WETH, so the collateral must be WETH")
    if not collateral.collateral or collateral.borrow or not collateral.capital:
        raise ValueError("collateral requires starting capital and no borrowing")


def require_loan(loan):
    if loan.reserve.asset != USDC:
        raise ValueError("this reference borrows USDC")
    # loan.capital is optional: a loan-asset reserve the strategy never supplies or
    # spends, so the full repay can pay the interest accrued since the borrow.
    if not loan.borrow:
        raise ValueError("loan requires borrowing")


def data_dependencies(config):
    return DataDependencies(
        prices=(
            PriceBinding(
                purpose="strategy",
                chain=config.allocation("collateral").chain,
                asset=WETH,
                window=config.parameters.window,
                source=PoolPrice(
                    kind="pool_state",
                    connector="uniswap_v3",
                    pool=PRICE_POOL,
                    timeframe="1h",
                    max_age_seconds=7200,
                    availability_lag_seconds=300,
                ),
            ),
        )
    )


def borrow_amount(config) -> int:
    """`borrow_quantity` in base units of the loan allocation's reserve asset."""
    asset = config.allocation("loan").reserve.asset
    decimals = config.asset_metadata[asset].decimals
    return parameter_base_units(config.parameters, "borrow_quantity", decimals)


def decide(context):
    current = state(context)
    if None in (context.observation("collateral"), context.observation("loan")):
        return context.decision(reason="Waiting for lending position data")
    if context.teardown:
        intent = close(context)
        return context.decision(
            intent=intent, reason=None if intent else "Repaid and withdrawn; nothing left to do"
        )
    if not current.supplied:
        capital = context.config.allocation("collateral").capital
        return context.decision(intent=context.actions.supply("collateral", capital))
    knobs = context.config.parameters
    ratio = trend_ratio(context.price(WETH), knobs.period)
    if ratio is None:
        # A short, stale or undefined window decides nothing.
        return context.decision(reason="Waiting for a complete WETH price history")
    trend = trend_text(ratio, knobs.period)
    above, below = f"{(knobs.borrow_above - 1) * 100:.3g}%", f"{(1 - knobs.repay_below) * 100:.3g}%"
    if not current.borrowed and ratio > knobs.borrow_above:
        return context.decision(
            intent=context.actions.borrow("loan", borrow_amount(context.config)),
            reason=f"{trend}; borrowing above {above}",
        )
    if current.borrowed and ratio < knobs.repay_below:
        return context.decision(
            intent=context.actions.repay("loan"), reason=f"{trend}; repaying below {below}"
        )
    if current.borrowed:
        return context.decision(reason=f"{trend}; keeping the loan until {below} below")
    return context.decision(reason=f"{trend}; borrowing starts {above} above")


def trend_text(ratio, period):
    side = "above" if ratio >= 1 else "below"
    return f"WETH {abs(ratio - 1) * 100:.1f}% {side} its {period}-hour average"


def trend_ratio(series, period):
    """The latest close over its moving average, or None."""
    if not series.complete:
        return None
    closes = np.array([float(v) for v in series.values])
    average = float(talib.SMA(closes, period)[-1])
    ratio = float(closes[-1]) / average if average else math.nan
    return ratio if math.isfinite(ratio) else None


def close(context):
    """Teardown: repay any debt or allowance, then withdraw the collateral."""
    loan = context.observation("loan")
    if loan.position.debt_scaled or loan.allowance:
        return context.actions.repay("loan")
    collateral = context.observation("collateral")
    if any(
        (
            collateral.position.supplied_scaled,
            collateral.position.collateral_enabled,
            collateral.allowance,
        )
    ):
        return context.actions.withdraw("collateral")
    return None


def on_outcome(context):
    current = state(context)
    result = context.result
    if result.status != "completed":
        return current
    kind = result.intent.kind
    if kind == "supply":
        return current.updated(supplied=True)
    if kind == "borrow":
        return current.updated(borrowed=True)
    if kind == "repay":
        return current.updated(borrowed=False)
    return current
