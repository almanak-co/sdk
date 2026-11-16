"""Exact target and shape admission shared by the verified V3 families.

Authority is scope only (blueprint 27): intents are not compared with any author amount.
Inventory, quotes, slippage and deadlines are gateway checks against observations.
"""

from almanak.dex_intents import (
    CloseLiquidity,
    CollectLiquidityFees,
    DecreaseLiquidity,
    ExitExchange,
    IncreaseLiquidity,
    OpenLiquidity,
    RevokeExchange,
    SwapExactInput,
)


def admit(capabilities, intent):
    matches = [value for value in capabilities.requirements if value.target == intent.target]
    if len(matches) != 1:
        raise ValueError("V3 intent has no exact retained target capability")
    if isinstance(intent, (SwapExactInput, RevokeExchange, ExitExchange)):
        return
    admit_liquidity(intent)


def admit_liquidity(intent):
    if not intent.target.liquidity_enabled:
        raise ValueError("liquidity action requires a configured range or band capability")
    if isinstance(intent, OpenLiquidity):
        admit_selected_range(intent)
    elif not isinstance(
        intent, (IncreaseLiquidity, DecreaseLiquidity, CloseLiquidity, CollectLiquidityFees)
    ):
        raise ValueError("unsupported V3 economic action")


def admit_selected_range(intent):
    target = intent.target
    if target.band is None:
        if intent.range is not None:
            raise ValueError("fixed-range liquidity cannot select another tick range")
        return
    if intent.range is None:
        raise ValueError("banded liquidity requires an explicitly selected tick range")
    target.band.validate_range(intent.range, target.pool.tick_spacing)


def admit_centered(intent, tick):
    """An unbounded band opens only the range centred on the retained decision tick."""
    target = intent.target
    if target.band is not None and intent.range is not None:
        target.band.validate_centered(intent.range, tick, target.pool.tick_spacing)
