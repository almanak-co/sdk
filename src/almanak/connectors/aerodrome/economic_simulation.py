"""Integer fills and fungible shares against fixed external pool history."""

from almanak.connectors.aerodrome import math
from almanak.connectors.aerodrome.ownership import at_indices
from almanak.connectors.aerodrome.positions import Position, Quote
from almanak.connectors.dex_economic_inventory import moved, swap_fill, within_inventory
from almanak.connectors.dex_residual import require_liquidity
from almanak.dex_intents import (
    CloseLiquidity,
    CollectLiquidityFees,
    DecreaseLiquidity,
    IncreaseLiquidity,
    OpenLiquidity,
    RevokeExchange,
    SwapExactInput,
)
from almanak.models import AssetAmount


def quote_at(record, target, amount, time):
    pool = record.payload.pool
    if pool.source != target.pool or amount.asset not in target.assets:
        raise ValueError("modeled quote requires its configured pool and input asset")
    if record.available_at > time or record.source_time > time:
        raise ValueError("modeled quote cannot consume a future sample")
    if pool.paused:
        raise ValueError("prepared pool swaps are paused")
    direction = amount.asset == target.assets[0]
    result = math.quote(
        pool.source.stable,
        10**pool.currency0.decimals,
        10**pool.currency1.decimals,
        pool.reserve0,
        pool.reserve1,
        amount.amount,
        pool.fee_bps,
        direction,
    )
    return Quote(
        source=target,
        block=record.reference,
        amount_in=amount,
        amount_out=AssetAmount(asset=target.assets[1 if direction else 0], amount=result),
        calculation="prepared_integer_pool",
        inputs=record.evidence,
    )


def measured_quote_at(record, target, amount, time):
    """A modeled observation quote; a zero output needs both reserves to be positive."""
    quote = quote_at(record, target, amount, time)
    pool = record.payload.pool
    require_liquidity(quote, (pool.reserve0, pool.reserve1))
    return quote


def added(config, intent, payload, record):
    old = payload.owned.position
    if isinstance(intent, OpenLiquidity) == (old is not None):
        raise ValueError("liquidity addition must retain hypothetical shares and claims")
    within_inventory(payload, (intent.maximum0, intent.maximum1))
    pool = record.payload.pool
    amount0, amount1, shares = math.addition(
        pool.reserve0,
        pool.reserve1,
        pool.total_supply,
        intent.maximum0.amount,
        intent.maximum1.amount,
    )
    current = at_indices(old, pool)
    updated = Position(
        shares=AssetAmount(
            asset=intent.target.pool.shares_asset,
            amount=math.add(shares, old.shares.amount if old else 0),
        ),
        fees0=current.fees0 if current else AssetAmount(asset=intent.target.assets[0], amount=0),
        fees1=current.fees1 if current else AssetAmount(asset=intent.target.assets[1], amount=0),
        index0=pool.index0,
        index1=pool.index1,
    )
    deltas = -amount0, -amount1
    owned = moved(payload.owned, deltas, position=updated, entry_complete=True)
    result = payload.model_copy(
        update={
            "owned": owned,
            "deposit0": payload.deposit0 + intent.maximum0.amount,
            "deposit1": payload.deposit1 + intent.maximum1.amount,
        }
    )
    return result, deltas, (0, 0), shares, "mint" if old is None else "increase", None


def principal(pool, shares):
    if not shares:
        return 0, 0
    # The model retains external supply separately from hypothetical shares.
    # Applying the external unit price does not burn the historical pool's LP.
    values = tuple(
        math.div(math.mul(shares, balance), pool.total_supply)
        for balance in (pool.balance0, pool.balance1)
    )
    if not min(values):
        raise ValueError("modeled burn cannot return positive principal in both currencies")
    return values


def removal_quantities(intent, position, pool):
    if isinstance(intent, CollectLiquidityFees):
        return 0, (0, 0)
    shares = intent.liquidity if isinstance(intent, DecreaseLiquidity) else position.shares.amount
    if shares > position.shares.amount or (
        isinstance(intent, DecreaseLiquidity) and not 0 < shares < position.shares.amount
    ):
        raise ValueError("partial withdrawal must retain positive owned shares")
    amounts = principal(pool, shares)
    if any(
        value < minimum.amount
        for value, minimum in zip(amounts, (intent.minimum0, intent.minimum1), strict=True)
    ):
        raise ValueError("modeled liquidity removal violates its principal minima")
    return shares, amounts


def removed(intent, payload, record):
    old = payload.owned.position
    if old is None:
        raise ValueError("liquidity exit requires hypothetical shares or fee claims")
    current = at_indices(old, record.payload.pool)
    shares, principal_amounts = removal_quantities(intent, current, record.payload.pool)
    collect = isinstance(intent, (CloseLiquidity, CollectLiquidityFees))
    fees = (current.fees0.amount, current.fees1.amount) if collect else (0, 0)
    deltas = tuple(math.add(value, fee) for value, fee in zip(principal_amounts, fees, strict=True))
    updated = Position(
        shares=AssetAmount(asset=current.shares.asset, amount=current.shares.amount - shares),
        fees0=AssetAmount(asset=current.fees0.asset, amount=current.fees0.amount - fees[0]),
        fees1=AssetAmount(asset=current.fees1.asset, amount=current.fees1.amount - fees[1]),
        index0=current.index0,
        index1=current.index1,
    )
    owned = moved(payload.owned, deltas, position=None if updated.empty else updated)
    result = payload.model_copy(
        update={"owned": owned, "fees0": payload.fees0 + fees[0], "fees1": payload.fees1 + fees[1]}
    )
    kind = (
        "close"
        if isinstance(intent, CloseLiquidity)
        else "decrease"
        if isinstance(intent, DecreaseLiquidity)
        else "collect"
    )
    return result, deltas, fees, -shares, kind, None


def simulate(config, intent, payload, record, time, identity):
    if intent.target != payload.target or record.payload.pool.source != payload.target.pool:
        raise ValueError("economic execution requires its retained target and pool")
    if record.available_at > time or record.source_time > time:
        raise ValueError("economic execution cannot use a future sample")
    if isinstance(intent, SwapExactInput):
        return swap_fill(config, intent, payload, record, time, quote_at)
    if isinstance(intent, (OpenLiquidity, IncreaseLiquidity)):
        return added(config, intent, payload, record)
    if isinstance(intent, (CloseLiquidity, DecreaseLiquidity, CollectLiquidityFees)):
        return removed(intent, payload, record)
    if isinstance(intent, RevokeExchange):
        return payload, (0, 0), (0, 0), 0, "revoke", None
    raise ValueError("unsupported Aerodrome economic intent")
