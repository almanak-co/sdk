"""Typed economic helpers use retained observations without external calls."""

from almanak.connectors.concentrated_math import amounts
from almanak.connectors.v3_pool_planning import sqrt_bounds
from almanak.dex_intents import TickBounds
from almanak.models import AssetAmount


def swap(allocation, observed, quote, *, intent_type):
    if quote is None or quote.block != observed.block or quote.source != observed.target:
        raise ValueError("swap requires its exact-size source quote at the decision block")
    minimum = max(1, quote.amount_out.amount * (10000 - allocation.slippage_bps) // 10000)
    return intent_type(
        target=observed.target,
        amount=quote.amount_in,
        minimum_output=AssetAmount(asset=quote.amount_out.asset, amount=minimum),
        deadline=int(observed.effective_at.timestamp()) + allocation.deadline_seconds,
    )


def open_liquidity(allocation, observed, *, fraction_bps=10000, intent_type):
    return intent_type(
        **addition(allocation, observed, fraction_bps), range=planned_range(observed)
    )


def planned_range(observed):
    """A banded allocation opens its position centred on the observed tick; fixed ones do not."""
    band = observed.target.band
    if band is None:
        return None
    selected = band.centered(observed.pool.tick, observed.target.pool.tick_spacing)
    return TickBounds(lower=selected.lower, upper=selected.upper)


def increase_liquidity(allocation, observed, *, fraction_bps=10000, intent_type):
    if observed.owned.position is None:
        raise ValueError("liquidity increase requires an owned position")
    return intent_type(**addition(allocation, observed, fraction_bps))


def addition(allocation, observed, fraction_bps):
    if type(fraction_bps) is not int or not 0 < fraction_bps <= 10000:
        raise ValueError("liquidity fraction requires an integer in (0, 10000] basis points")
    maxima = tuple(
        AssetAmount(asset=a.asset, amount=a.amount * fraction_bps // 10000)
        for a in (observed.owned.inventory0, observed.owned.inventory1)
    )
    return dict(
        target=observed.target,
        maximum0=maxima[0],
        maximum1=maxima[1],
        deadline=int(observed.effective_at.timestamp()) + allocation.deadline_seconds,
    )


def close_liquidity(allocation, observed, *, intent_type):
    if observed.owned.position is None:
        raise ValueError("liquidity closure requires an owned position")
    return intent_type(**removal(allocation, observed, observed.owned.position.liquidity))


def decrease_liquidity(allocation, observed, liquidity, *, intent_type):
    if observed.owned.position is None or not 0 < liquidity < observed.owned.position.liquidity:
        raise ValueError("partial liquidity removal must retain positive owned liquidity")
    return intent_type(liquidity=liquidity, **removal(allocation, observed, liquidity))


def removal(allocation, observed, liquidity):
    principal = amounts(
        liquidity,
        observed.pool.sqrt_price_x96,
        *sqrt_bounds(observed.target, position=observed.owned.position),
    )
    minima = tuple(
        AssetAmount(asset=asset, amount=amount * (10000 - allocation.slippage_bps) // 10000)
        for asset, amount in zip(observed.target.assets, principal, strict=True)
    )
    return dict(
        target=observed.target,
        minimum0=minima[0],
        minimum1=minima[1],
        deadline=int(observed.effective_at.timestamp()) + allocation.deadline_seconds,
    )
