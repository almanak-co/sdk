"""Typed intent constructors consume observations without external calls."""

from almanak.connectors.uniswap_v4.math import amounts
from almanak.connectors.uniswap_v4.models import Target
from almanak.connectors.uniswap_v4.reader import centered_range, sqrt_bounds
from almanak.dex_intents import CloseLiquidity, OpenLiquidity, SwapExactInput, TickBounds
from almanak.models import AssetAmount


def swap(allocation, observed, quote):
    if quote is None or quote.block != observed.block:
        raise ValueError("swap requires its explicit exact-size quote at the decision block")
    minimum = max(1, quote.output.amount * (10000 - allocation.slippage_bps) // 10000)
    return SwapExactInput[Target](
        target=observed.target,
        amount=quote.amount,
        minimum_output=AssetAmount(asset=quote.output.asset, amount=minimum),
        deadline=int(observed.effective_at.timestamp()) + allocation.deadline_seconds,
    )


def open_liquidity(allocation, observed):
    return OpenLiquidity[Target](
        target=observed.target,
        maximum0=observed.owned.inventory0,
        maximum1=observed.owned.inventory1,
        deadline=int(observed.effective_at.timestamp()) + allocation.deadline_seconds,
        range=planned_range(observed),
    )


def planned_range(observed):
    """A banded allocation opens its position centred on the observed tick; fixed ones do not."""
    if observed.target.band is None:
        return None
    selected = centered_range(observed.target, observed.pool.tick)
    return TickBounds(lower=selected.lower, upper=selected.upper)


def close_liquidity(allocation, observed):
    lower, upper = sqrt_bounds(observed.target, position=observed.owned.position)
    principal = amounts(
        observed.owned.position.liquidity, observed.pool.sqrt_price_x96, lower, upper
    )
    minima = tuple(
        AssetAmount(asset=asset, amount=amount * (10000 - allocation.slippage_bps) // 10000)
        for asset, amount in zip(observed.target.assets, principal, strict=True)
    )
    return CloseLiquidity[Target](
        target=observed.target,
        minimum0=minima[0],
        minimum1=minima[1],
        deadline=int(observed.effective_at.timestamp()) + allocation.deadline_seconds,
    )
