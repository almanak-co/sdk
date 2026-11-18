"""Concrete typed author intents use shared retained-observation calculations."""

from almanak.connectors import v3_pool_authoring as family
from almanak.connectors.uniswap_v3.models import Target
from almanak.connectors.v3_pool_authoring import addition as addition
from almanak.connectors.v3_pool_authoring import removal as removal
from almanak.dex_intents import (
    CloseLiquidity,
    DecreaseLiquidity,
    IncreaseLiquidity,
    OpenLiquidity,
    SwapExactInput,
)


def swap(allocation, observed, quote):
    return family.swap(allocation, observed, quote, intent_type=SwapExactInput[Target])


def open_liquidity(allocation, observed, *, fraction_bps=10000):
    return family.open_liquidity(
        allocation, observed, fraction_bps=fraction_bps, intent_type=OpenLiquidity[Target]
    )


def increase_liquidity(allocation, observed, *, fraction_bps=10000):
    return family.increase_liquidity(
        allocation, observed, fraction_bps=fraction_bps, intent_type=IncreaseLiquidity[Target]
    )


def close_liquidity(allocation, observed):
    return family.close_liquidity(allocation, observed, intent_type=CloseLiquidity[Target])


def decrease_liquidity(allocation, observed, liquidity):
    return family.decrease_liquidity(
        allocation, observed, liquidity, intent_type=DecreaseLiquidity[Target]
    )
