"""Bracketed band fee accounting around the shared V4 economic simulation.

The simulation credits growth from `observed.fee_growth`. For a band target that value
is the lower bracket for the concrete range the action touches; this module supplies it
and afterwards retains the upper bracket checkpoint and the collected excess it admits.
"""

from almanak.connectors.uniswap_v4.math import accrued
from almanak.dex_intents import CloseLiquidity, CollectLiquidityFees, OpenLiquidity


def acted_range(intent, owned):
    if isinstance(intent, OpenLiquidity):
        return intent.range
    if isinstance(intent, (CollectLiquidityFees, CloseLiquidity)) and owned.position is not None:
        return owned.position.range
    return None


def band_inputs(growth, record, intent, observed):
    """The observation with the lower bracket for the acted range, and both brackets."""
    selected = acted_range(intent, observed.owned) if intent.target.band is not None else None
    if selected is None:
        return observed, None
    bounds = growth(record, intent.target, selected)
    return observed.model_copy(update={"fee_growth": bounds.lower}), bounds


def band_settled(payload, before, bounds, intent, fees):
    """Retain the upper checkpoint and add the collected excess over the credited fees."""
    if bounds is None:
        return payload
    if isinstance(intent, OpenLiquidity):
        return checkpointed(payload, bounds)
    held = before.owned.position
    upper = tuple(
        accrued(held.liquidity, old, new)
        for old, new in zip(held.bound_growth, bounds.upper, strict=True)
    )
    settled = payload.model_copy(
        update={
            "unattributed0": payload.unattributed0 + upper[0] - fees[0],
            "unattributed1": payload.unattributed1 + upper[1] - fees[1],
        }
    )
    return checkpointed(settled, bounds) if isinstance(intent, CollectLiquidityFees) else settled


def checkpointed(payload, bounds):
    position = payload.owned.position.model_copy(update={"bound_growth": bounds.upper})
    return payload.model_copy(
        update={"owned": payload.owned.model_copy(update={"position": position})}
    )


def pending_bound(position, bounds):
    """Uncollected fees the upper bracket admits for a retained band position."""
    return tuple(
        accrued(position.liquidity, old, new)
        for old, new in zip(position.bound_growth, bounds.upper, strict=True)
    )
