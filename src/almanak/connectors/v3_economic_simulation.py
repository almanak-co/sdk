"""Economic V3 effects preserve exogenous history and hypothetical NFT identity."""

from almanak.connectors.band_fee_growth import Bounds
from almanak.connectors.concentrated_math import amounts, liquidity_for, sqrt_at_tick
from almanak.connectors.dex_economic_inventory import moved as moved
from almanak.connectors.dex_economic_inventory import swap_fill as swap_fill
from almanak.connectors.dex_economic_inventory import within_inventory as within_inventory
from almanak.connectors.v3_fee_growth import Q128, accrued, sampled_inside
from almanak.connectors.v3_pool_planning import concrete_range
from almanak.connectors.v3_pool_positions import SimulatedPosition
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


def quote_at(record, target, amount, time, *, quote_type, exact_input):
    if record.payload.pool.source != target.pool or amount.asset not in target.assets:
        raise ValueError("modeled quote requires its configured source and input asset")
    if record.available_at > time or record.source_time > time:
        raise ValueError("modeled quote cannot consume a future sample")
    direction = amount.asset == target.assets[0]
    result = exact_input(record.payload, amount.amount, direction)
    return quote_type(
        source=target,
        block=record.reference,
        amount_in=amount,
        amount_out=AssetAmount(asset=target.assets[1 if direction else 0], amount=result.output),
        sqrt_price_after=result.sqrt_price_x96,
        initialized_ticks_crossed=len(result.crossed_ticks),
        calculation="prepared_integer_pool",
        inputs=record.evidence,
    )


def pending_fees(position, growth):
    if not isinstance(position, SimulatedPosition):
        raise ValueError("fee accounting requires retained hypothetical liquidity")
    return owed_after(
        position.liquidity, (position.owed0, position.owed1), position.fee_growth, growth
    )


def pending_bound(position, bounds):
    """Fees the upper growth bound would owe; equal to the credited fees for a fixed range."""
    if position.bound_growth is None or position.bound_owed is None:
        return pending_fees(position, bounds.lower)
    return owed_after(position.liquidity, position.bound_owed, position.bound_growth, bounds.upper)


def owed_after(liquidity, owed, previous, current):
    return tuple(
        (value + accrued(liquidity, old, new)) % Q128
        for value, old, new in zip(owed, previous, current, strict=True)
    )


def fee_bounds(record, target, selected, growth):
    """Exact fixed-range growth, or the bracketed band growth for the concrete range."""
    if target.band is None:
        exact = sampled_inside(record.payload, target)
        return Bounds(lower=exact, upper=exact)
    if growth is None:
        raise ValueError("banded fee growth requires prepared interval evidence")
    return growth(record, target, selected)


def liquidity_capacity(sample, target, selected, added):
    max_per_tick = (2**128 - 1) // (2 * (887272 // target.pool.tick_spacing) + 1)
    gross = {tick.index: tick.gross for tick in sample.ticks}
    for boundary in (selected.lower, selected.upper):
        if gross.get(boundary, 0) + added > max_per_tick:
            raise ValueError("hypothetical liquidity exceeds the V3 per-tick capacity")


def requested_range(intent, position):
    requested = intent.range if isinstance(intent, OpenLiquidity) else None
    return concrete_range(intent.target, position=position, requested=requested)


def checkpoint(target, position, bounds, liquidity, identity, selected):
    """The retained position after an addition settles its pending fees into `owed`."""
    owed = pending_fees(position, bounds.lower) if position else (0, 0)
    band = {}
    if target.band is not None:
        band = dict(
            range=selected,
            bound_growth=bounds.upper,
            bound_owed=pending_bound(position, bounds) if position else (0, 0),
        )
    return SimulatedPosition(
        identity=position.identity if position else identity,
        liquidity=liquidity,
        owed0=owed[0],
        owed1=owed[1],
        fee_growth=bounds.lower,
        **band,
    )


def added(config, intent, payload, record, identity, growth):
    position = payload.owned.position
    if isinstance(intent, OpenLiquidity) == (position is not None):
        raise ValueError("liquidity addition must retain hypothetical position ownership")
    within_inventory(payload, (intent.maximum0, intent.maximum1))
    selected = requested_range(intent, position)
    bounds = fee_bounds(record, intent.target, selected, growth)
    price = record.payload.pool.sqrt_price_x96
    edges = sqrt_at_tick(selected.lower), sqrt_at_tick(selected.upper)
    liquidity = liquidity_for(intent.maximum0.amount, intent.maximum1.amount, price, *edges)
    total = liquidity + (position.liquidity if position else 0)
    if not 0 < liquidity < 2**128 or total >= 2**128:
        raise ValueError("liquidity addition requires positive supported uint128 capacity")
    liquidity_capacity(record.payload, intent.target, selected, total)
    deposits = amounts(liquidity, price, *edges, up=True)
    if any(
        value > cap.amount
        for value, cap in zip(deposits, (intent.maximum0, intent.maximum1), strict=True)
    ):
        raise ValueError("integer liquidity deposits exceed requested maxima")
    updated = checkpoint(intent.target, position, bounds, total, identity, selected)
    deltas = tuple(-value for value in deposits)
    owned = moved(payload.owned, deltas, position=updated, entry_complete=True)
    result = payload.model_copy(
        update={
            "owned": owned,
            "deposit0": payload.deposit0 + intent.maximum0.amount,
            "deposit1": payload.deposit1 + intent.maximum1.amount,
        }
    )
    return result, deltas, (0, 0), liquidity, "mint" if position is None else "increase", None


def removal_quantities(intent, position, sample):
    if isinstance(intent, CollectLiquidityFees):
        return 0, (0, 0)
    liquidity = intent.liquidity if isinstance(intent, DecreaseLiquidity) else position.liquidity
    if (
        not 0 < liquidity <= position.liquidity
        or isinstance(intent, DecreaseLiquidity)
        and liquidity == position.liquidity
    ):
        raise ValueError("partial removal must preserve positive owned liquidity")
    selected = concrete_range(intent.target, position=position)
    edges = sqrt_at_tick(selected.lower), sqrt_at_tick(selected.upper)
    principal = amounts(liquidity, sample.pool.sqrt_price_x96, *edges)
    if any(
        p < m.amount for p, m in zip(principal, (intent.minimum0, intent.minimum1), strict=True)
    ):
        raise ValueError("modeled liquidity removal violates its principal minima")
    return liquidity, principal


def settled(intent, position, liquidity, bounds):
    """The retained position after removal: closed, or zero owed from the new checkpoint."""
    if isinstance(intent, CloseLiquidity):
        return None
    update = {
        "liquidity": position.liquidity - liquidity,
        "owed0": 0,
        "owed1": 0,
        "fee_growth": bounds.lower,
    }
    if intent.target.band is not None:
        update |= {"bound_growth": bounds.upper, "bound_owed": (0, 0)}
    return position.model_copy(update=update)


def removed(intent, payload, record, growth):
    position = payload.owned.position
    if not isinstance(position, SimulatedPosition):
        raise ValueError("fee accounting requires retained hypothetical liquidity")
    bounds = fee_bounds(
        record, intent.target, concrete_range(intent.target, position=position), growth
    )
    fees = pending_fees(position, bounds.lower)
    unattributed = tuple(
        most - least for most, least in zip(pending_bound(position, bounds), fees, strict=True)
    )
    liquidity, principal = removal_quantities(intent, position, record.payload)
    # Hypothetical settlement assumes full access to modeled principal. It does
    # not claim to reproduce NFPM aggregate-position collection rounding.
    deltas = tuple(p + f for p, f in zip(principal, fees, strict=True))
    if any(value >= Q128 for value in deltas):
        raise ValueError("modeled collection exceeds supported uint128 settlement")
    owned = moved(payload.owned, deltas, position=settled(intent, position, liquidity, bounds))
    result = payload.model_copy(
        update={
            "owned": owned,
            "fees0": payload.fees0 + fees[0],
            "fees1": payload.fees1 + fees[1],
            "unattributed0": payload.unattributed0 + unattributed[0],
            "unattributed1": payload.unattributed1 + unattributed[1],
        }
    )
    kind = (
        "close"
        if isinstance(intent, CloseLiquidity)
        else "decrease"
        if isinstance(intent, DecreaseLiquidity)
        else "collect"
    )
    return result, deltas, fees, -liquidity, kind, None


def simulate(config, intent, payload, record, time, identity, quote, growth=None):
    """`growth(record, target, range)` supplies bracketed band growth; fixed ranges omit it."""
    if intent.target != payload.target or record.payload.pool.source != payload.target.pool:
        raise ValueError("economic execution requires its retained target and pool")
    if record.available_at > time or record.source_time > time:
        raise ValueError("economic execution cannot use a future sample")
    if isinstance(intent, SwapExactInput):
        return swap_fill(config, intent, payload, record, time, quote)
    if isinstance(intent, (OpenLiquidity, IncreaseLiquidity)):
        return added(config, intent, payload, record, identity, growth)
    if isinstance(intent, (CloseLiquidity, DecreaseLiquidity, CollectLiquidityFees)):
        return removed(intent, payload, record, growth)
    if isinstance(intent, RevokeExchange):
        return payload, (0, 0), (0, 0), 0, "revoke", None
    raise ValueError("unsupported V3 economic intent")
