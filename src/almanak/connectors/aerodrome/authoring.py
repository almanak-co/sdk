"""Economic helpers consume retained pool balances, shares and explicit quotes."""

from almanak.connectors.aerodrome import math
from almanak.connectors.aerodrome.models import Target
from almanak.connectors.aerodrome.planning import minimum
from almanak.connectors.v3_pool_authoring import addition
from almanak.connectors.v3_pool_authoring import swap as quoted_swap
from almanak.dex_intents import (
    CloseLiquidity,
    DecreaseLiquidity,
    IncreaseLiquidity,
    OpenLiquidity,
    SwapExactInput,
)


def swap(allocation, observed, quote):
    return quoted_swap(allocation, observed, quote, intent_type=SwapExactInput[Target])


def open_liquidity(allocation, observed, *, fraction_bps=10000):
    if observed.owned.position is not None:
        raise ValueError("liquidity opening cannot adopt existing shares or fee claims")
    return OpenLiquidity[Target](**addition(allocation, observed, fraction_bps))


def increase_liquidity(allocation, observed, *, fraction_bps=10000):
    if observed.owned.position is None:
        raise ValueError("liquidity increase requires an owned pool position")
    return IncreaseLiquidity[Target](**addition(allocation, observed, fraction_bps))


def removal(allocation, observed, shares):
    pool = observed.pool
    if observed.simulated:
        from almanak.connectors.aerodrome.economic_simulation import principal

        amounts = principal(pool, shares)
    else:
        amounts = math.removal(pool.balance0, pool.balance1, pool.total_supply, shares)
    minima = tuple(
        minimum(asset, value, allocation.slippage_bps)
        for asset, value in zip(observed.target.assets, amounts, strict=True)
    )
    return dict(
        target=observed.target,
        minimum0=minima[0],
        minimum1=minima[1],
        deadline=int(observed.effective_at.timestamp()) + allocation.deadline_seconds,
    )


def close_liquidity(allocation, observed):
    if observed.owned.position is None:
        raise ValueError("liquidity closure requires owned shares")
    return CloseLiquidity[Target](
        **removal(allocation, observed, observed.owned.position.shares.amount)
    )


def decrease_liquidity(allocation, observed, liquidity):
    if observed.owned.position is None or not 0 < liquidity < observed.owned.position.shares.amount:
        raise ValueError("partial removal must retain positive owned shares")
    return DecreaseLiquidity[Target](
        liquidity=liquidity, **removal(allocation, observed, liquidity)
    )
