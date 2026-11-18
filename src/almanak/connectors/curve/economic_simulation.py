"""Integer actions on an explicit virtual LP overlay of fixed external history."""

from almanak.asset_amounts import AssetAmounts
from almanak.connectors.curve import economic_pool
from almanak.connectors.curve import stable_math as math
from almanak.connectors.curve.economic_pool import virtual_pool as virtual_pool
from almanak.connectors.curve.positions import Position, Quote
from almanak.connectors.dex_residual import require_liquidity
from almanak.dex_intents import (
    ClosePoolLiquidity,
    DecreasePoolLiquidity,
    IncreasePoolLiquidity,
    OpenPoolLiquidity,
    RevokeExchange,
    SwapExactInput,
)
from almanak.models import AssetAmount


def quote_at(record, owned, amount, output_asset, time):
    target = owned.target
    if record.payload.pool.source != target.pool:
        raise ValueError("modeled Curve quote requires its configured pool")
    require_time(record, time)
    if amount.asset == output_asset:
        raise ValueError("modeled Curve swap requires different coins")
    result = economic_pool.swap(
        virtual_pool(record.payload.pool, owned, time),
        target.assets.index(amount.asset),
        target.assets.index(output_asset),
        amount.amount,
    )
    return Quote(
        source=target,
        block=record.reference,
        amount_in=amount,
        amount_out=AssetAmount(asset=output_asset, amount=result),
        calculation="prepared_integer_pool",
        inputs=record.evidence,
    )


def measured_quote_at(record, owned, amount, output_asset, time):
    """A modeled observation quote; a zero output needs both coin balances to be positive."""
    quote = quote_at(record, owned, amount, output_asset, time)
    balances, index = record.payload.pool.balances, owned.target.assets.index
    require_liquidity(quote, (balances[index(amount.asset)], balances[index(output_asset)]))
    return quote


def require_time(record, time):
    if record.available_at > time or record.source_time > time:
        raise ValueError("modeled Curve action cannot consume a future sample")


def spending(payload, amounts):
    """Cumulative inputs after this action, for reports; only inventory bounds an input."""
    spent = {value.asset: value.amount for value in payload.spent.root}
    for amount in amounts:
        if amount.amount > payload.owned.amount(amount.asset):
            raise ValueError("economic Curve action exceeds attributed inventory")
        spent[amount.asset] += amount.amount
    return AssetAmounts(
        tuple(AssetAmount(asset=asset, amount=spent[asset]) for asset in payload.target.assets)
    )


def moved(owned, deltas, **updates):
    inventory = AssetAmounts(
        tuple(
            AssetAmount(asset=value.asset, amount=value.amount + delta)
            for value, delta in zip(owned.inventory.root, deltas, strict=True)
        )
    )
    return type(owned)(**{**owned.model_dump(), "inventory": inventory, **updates})


def swap_fill(config, intent, payload, record, time):
    spent = spending(payload, (intent.amount,))
    quote = quote_at(record, payload.owned, intent.amount, intent.minimum_output.asset, time)
    if quote.amount_out.amount < intent.minimum_output.amount:
        raise ValueError("modeled Curve swap violates its retained minimum")
    deltas = tuple(
        -intent.amount.amount
        if asset == intent.amount.asset
        else quote.amount_out.amount
        if asset == quote.amount_out.asset
        else 0
        for asset in intent.target.assets
    )
    owned = moved(
        payload.owned,
        deltas,
        entry_complete=payload.owned.entry_complete
        or intent.amount.asset == intent.target.funding_asset,
    )
    return (
        payload.model_copy(update={"owned": owned, "spent": spent}),
        deltas,
        (),
        0,
        "swap",
        quote,
    )


def added(config, intent, payload, record, time):
    old = payload.owned.position
    if isinstance(intent, OpenPoolLiquidity) == (old is not None):
        raise ValueError("Curve liquidity addition must preserve hypothetical share ownership")
    spent = spending(payload, intent.maximums.root)
    amounts = tuple(value.amount for value in intent.maximums.root)
    minted = economic_pool.minted(virtual_pool(record.payload.pool, payload.owned, time), amounts)
    position = Position(
        shares=AssetAmount(
            asset=intent.target.pool.shares_asset,
            amount=math.uint((old.shares.amount if old else 0) + minted),
        )
    )
    deltas = tuple(-value for value in amounts)
    owned = moved(payload.owned, deltas, position=position, entry_complete=True)
    updated = payload.model_copy(update={"owned": owned, "spent": spent})
    return updated, deltas, (), minted, "mint" if old is None else "increase", None


def removal_amounts(target, state, shares, output_asset):
    index = None if output_asset is None else target.assets.index(output_asset)
    return economic_pool.removed(state, shares, index)


def minimums(observed, shares, slippage, output_asset):
    state = virtual_pool(observed.pool, observed.owned, int(observed.effective_at.timestamp()))
    outputs = removal_amounts(observed.target, state, shares, output_asset)
    return AssetAmounts(
        tuple(
            AssetAmount(asset=asset, amount=value * (10000 - slippage) // 10000)
            for asset, value in zip(observed.target.assets, outputs, strict=True)
        )
    )


def removed(intent, payload, record, time):
    old = payload.owned.position
    if old is None:
        raise ValueError("Curve liquidity removal requires hypothetical LP shares")
    shares = intent.liquidity if isinstance(intent, DecreasePoolLiquidity) else old.shares.amount
    if shares > old.shares.amount or (
        isinstance(intent, DecreasePoolLiquidity) and not 0 < shares < old.shares.amount
    ):
        raise ValueError("partial Curve removal must retain positive hypothetical LP shares")
    outputs = removal_amounts(
        intent.target,
        virtual_pool(record.payload.pool, payload.owned, time),
        shares,
        intent.output_asset,
    )
    if any(
        amount < minimum.amount
        for amount, minimum in zip(outputs, intent.minimums.root, strict=True)
    ):
        raise ValueError("modeled Curve removal violates its retained minima")
    position = (
        Position(shares=AssetAmount(asset=old.shares.asset, amount=old.shares.amount - shares))
        if shares < old.shares.amount
        else None
    )
    owned = moved(payload.owned, outputs, position=position)
    kind = "close" if isinstance(intent, ClosePoolLiquidity) else "decrease"
    return payload.model_copy(update={"owned": owned}), outputs, (), -shares, kind, None


def simulate(config, intent, payload, record, time, identity):
    if intent.target != payload.target or record.payload.pool.source != payload.target.pool:
        raise ValueError("economic Curve execution requires its retained target and pool")
    require_time(record, time)
    if isinstance(intent, SwapExactInput):
        return swap_fill(config, intent, payload, record, time)
    if isinstance(intent, (OpenPoolLiquidity, IncreasePoolLiquidity)):
        return added(config, intent, payload, record, time)
    if isinstance(intent, (ClosePoolLiquidity, DecreasePoolLiquidity)):
        return removed(intent, payload, record, time)
    if isinstance(intent, RevokeExchange):
        return payload, (0,) * len(payload.target.assets), (), 0, "revoke", None
    raise ValueError("unsupported Curve economic intent")
