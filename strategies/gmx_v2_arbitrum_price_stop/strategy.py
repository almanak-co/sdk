"""Open one GMX V2 position and close it at a price stop or take-profit.

The entry price is the position's own (its USD size over its index-token size), so a
restart needs no memory of it. The current price is the latest one-minute close of the
Arbitrum WETH/USDC Uniswap V3 pool, read from the pool's on-chain state.
"""

from decimal import Decimal
from typing import cast

from pydantic import Field

from almanak.api import (
    AssetType,
    DataDependencies,
    Memory,
    PoolPrice,
    PriceBinding,
    StrategyState,
    TypedParameters,
    declared,
)
from almanak.connectors.gmx_v2.api import Decision, DecisionInput, LiquidationResult, OutcomeInput

WETH = AssetType("eip155:42161/erc20:0x82af49447d8a07e3bd95bd0d56f35241523fbab1")
PRICE_POOL = "0xc6962004f452be9203591991d15f6b388e09e8d0"  # Uniswap V3 WETH/USDC 0.05%
BPS = 10_000
USD_E30 = 10**30


class StrategyParams(TypedParameters):
    stop_loss_bps: int = Field(gt=0, le=BPS)
    take_profit_bps: int = Field(gt=0, le=BPS)


class State(StrategyState):
    opened: bool = False  # one position per run: never reopened after it closes
    unwinding: bool = False


def state(context) -> State:
    return cast(State, context.memory)


def capabilities(config):
    return declared(config)


def validate_config(config):
    if {a.name for a in config.allocations} != {"position"}:
        raise ValueError("this strategy requires one position allocation")
    # The stop reads the WETH price, so the position must be on a WETH-indexed market.
    position = config.allocation("position")
    if position.market.index_asset != WETH:
        raise ValueError("the price stop reads WETH, so the market must index WETH")
    # A long increases below a ceiling and decreases above a floor; a short mirrors both.
    if (position.increase_acceptable_price > position.decrease_acceptable_price) != (
        position.side == "long"
    ):
        raise ValueError(
            "increase_acceptable_price and decrease_acceptable_price don't suit side "
            f"{position.side}: a long's increase price is above its decrease price, a "
            "short's below; swap them when changing side"
        )


def data_dependencies(config):
    return DataDependencies(
        prices=(
            PriceBinding(
                purpose="strategy",
                chain=config.target("position").fund.chain,
                asset=WETH,  # priced in the pool's other coin, USDC
                window=2,  # only the latest close is read
                source=PoolPrice(
                    kind="pool_state",
                    connector="uniswap_v3",
                    pool=PRICE_POOL,
                    timeframe="1m",
                    max_age_seconds=180,
                    availability_lag_seconds=15,
                ),
            ),
        )
    )


def entry_price(config, position) -> Decimal:
    """USD per WETH the position was opened at."""
    decimals = config.asset_metadata[position.index_amount.asset].decimals
    tokens = Decimal(position.index_amount.amount).scaleb(-decimals)
    return Decimal(position.size_usd_e30) / USD_E30 / tokens


def price_change(context, position) -> Decimal | None:
    """The latest close's change from the entry price, in BPS, or None on a stale window."""
    series = context.price(WETH)
    if not series.complete or not position.index_amount.amount:
        return None
    close = Decimal(str(series.values[-1]))
    return (close / entry_price(context.config, position) - 1) * BPS


def stopped(context, position) -> str | None:
    """Why the latest close ends the position (past the stop or the take-profit), or None."""
    change = price_change(context, position)
    if change is None:
        return None  # a stale window decides nothing
    side = -1 if context.config.allocation("position").side == "short" else 1
    params = context.config.parameters
    if side * change <= -params.stop_loss_bps:
        return f"Stop: price {change / 100:+.1f}% from entry, closing"
    if side * change >= params.take_profit_bps:
        return f"Target: price {change / 100:+.1f}% from entry, closing"
    return None


def holding(context, position) -> str:
    change = price_change(context, position)
    if change is None:
        return "Position open; waiting for a fresh WETH price"
    side = -1 if context.config.allocation("position").side == "short" else 1
    params = context.config.parameters
    stop, target = -side * params.stop_loss_bps / 100, side * params.take_profit_bps / 100
    return f"Price {change / 100:+.1f}% from entry; stop at {stop:+g}%, target at {target:+g}%"


def decide(context: DecisionInput) -> Decision:
    memory = state(context)
    position = context.require_observation("position").owned.position
    if context.teardown or memory.unwinding:
        return context.decision(intent=context.actions.exit("position"))
    if position is None:
        if memory.opened:
            return context.decision(reason="Position closed; nothing left to do")
        allocation = context.config.allocation("position")
        return context.decision(
            intent=context.actions.increase(
                "position",
                collateral_base_units=allocation.entry_collateral.amount,
                size_delta_usd_e30=allocation.entry_size_usd_e30,
            )
        )
    reason = stopped(context, position)
    if reason is not None:
        unwinding = memory.updated(unwinding=True)
        intent = context.actions.exit("position")
        return context.decision(intent=intent, memory=unwinding, reason=reason)
    return context.decision(reason=holding(context, position))


def on_outcome(context: OutcomeInput) -> Memory:
    result = context.result
    memory = state(context)
    if isinstance(result, LiquidationResult):
        return memory.updated(opened=True, unwinding=True)
    if not result.complete:
        return memory
    if not result.success:
        return memory.updated(unwinding=True)  # a failed request closes what is open
    if result.intent.kind == "increase_position":
        return memory.updated(opened=True)
    return memory
