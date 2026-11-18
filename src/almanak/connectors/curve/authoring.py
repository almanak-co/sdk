"""Economic helpers consume retained pool balances, shares and explicit quotes."""

from almanak.asset_amounts import AssetAmounts
from almanak.connectors.curve.models import Target
from almanak.connectors.curve.planning import minimums
from almanak.connectors.v3_pool_authoring import swap as quoted_swap
from almanak.dex_intents import (
    ClosePoolLiquidity,
    DecreasePoolLiquidity,
    IncreasePoolLiquidity,
    OpenPoolLiquidity,
    SwapExactInput,
)
from almanak.models import AssetAmount


def swap(allocation, observed, quote):
    return quoted_swap(allocation, observed, quote, intent_type=SwapExactInput[Target])


def open_liquidity(allocation, observed, *, fraction_bps=10000):
    if observed.owned.position is not None:
        raise ValueError("liquidity opening cannot adopt existing shares or fee claims")
    return OpenPoolLiquidity[Target](**addition(allocation, observed, fraction_bps))


def increase_liquidity(allocation, observed, *, fraction_bps=10000):
    if observed.owned.position is None:
        raise ValueError("liquidity increase requires an owned pool position")
    return IncreasePoolLiquidity[Target](**addition(allocation, observed, fraction_bps))


def addition(allocation, observed, fraction_bps):
    if type(fraction_bps) is not int or not 0 < fraction_bps <= 10000:
        raise ValueError("liquidity fraction requires an integer in (0, 10000] basis points")
    maximums = AssetAmounts(
        tuple(
            AssetAmount(asset=value.asset, amount=value.amount * fraction_bps // 10000)
            for value in observed.owned.inventory.root
        )
    )
    return dict(
        target=observed.target,
        maximums=maximums,
        deadline=int(observed.effective_at.timestamp()) + allocation.deadline_seconds,
    )


def removal(allocation, observed, shares):
    output = observed.target.single_coin_exit
    if observed.simulated:
        from almanak.connectors.curve.economic_simulation import minimums as modeled_minimums

        minima = modeled_minimums(observed, shares, allocation.slippage_bps, output)
    else:
        minima = minimums(observed.target, observed.pool, shares, allocation.slippage_bps, output)
    return dict(
        target=observed.target,
        minimums=minima,
        output_asset=output,
        deadline=int(observed.effective_at.timestamp()) + allocation.deadline_seconds,
    )


def close_liquidity(allocation, observed):
    if observed.owned.position is None:
        raise ValueError("liquidity closure requires owned shares")
    return ClosePoolLiquidity[Target](
        **removal(allocation, observed, observed.owned.position.shares.amount)
    )


def decrease_liquidity(allocation, observed, liquidity):
    if observed.owned.position is None or not 0 < liquidity < observed.owned.position.shares.amount:
        raise ValueError("partial removal must retain positive owned shares")
    return DecreasePoolLiquidity[Target](
        liquidity=liquidity, **removal(allocation, observed, liquidity)
    )
