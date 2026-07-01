"""Shared concentrated-liquidity price-band -> tick-range seam (VIB-5556).

Every Uniswap-V3-style concentrated-liquidity connector turns a human price
band into an on-chain tick range with the same four steps: orientation invert
(when the pool reports ``token0``/``token1`` swapped versus the user's stated
pair), decimals-correct price->tick, tick-spacing alignment, and the
straddle invariant. Those steps used to be hand-rolled per connector, which is
how the ALM-2901 decimals/orientation class of bug could be written more than
once. This module is the single place that composition lives, so the math can
only be written -- and tested -- once.

It deliberately takes plain prices, decimals, spacing and ``current_tick`` (no
dependency on ``LPOpenIntent`` / ``RangeSpec``) so connectors can adopt it
independently of the intent-vocabulary work (VIB-5555).

Design reference: ``docs/internal/unified-lp-range-ux-design.md`` (the shared
seam). The arithmetic core is :func:`concentrated_liquidity_math.price_to_tick`.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from almanak.connectors._strategy_base.concentrated_liquidity_math import price_to_tick

__all__ = ["PriceBandToTicksError", "TickRange", "price_band_to_ticks"]


class PriceBandToTicksError(ValueError):
    """Raised when a price band cannot be turned into a valid tick range.

    A ``ValueError`` subclass so callers that already treat bad numeric input
    as ``ValueError`` keep working, while callers that want to distinguish a
    seam-level rejection (collapse / straddle / non-invertible band) from an
    arbitrary error can catch this specific type.
    """


@dataclass(frozen=True, slots=True)
class TickRange:
    """A spacing-aligned, non-collapsed tick band ready to mint."""

    tick_lower: int
    tick_upper: int


def _as_decimal(value: Decimal | float | int | str) -> Decimal:
    """Coerce numeric input to ``Decimal`` without binary-float noise."""
    return value if isinstance(value, Decimal) else Decimal(str(value))


def price_band_to_ticks(
    *,
    range_lower: Decimal | float | int | str,
    range_upper: Decimal | float | int | str,
    token0_decimals: int,
    token1_decimals: int,
    tokens_swapped: bool,
    tick_spacing: int,
    current_tick: int | None = None,
    require_straddle: bool = True,
    allow_out_of_range: bool = False,
) -> TickRange:
    """Compose a human price band into a spacing-aligned tick range.

    The four steps run in order:

    1. **Orientation invert** -- when ``tokens_swapped`` the user's pair is the
       reciprocal of the pool's ``token0``/``token1`` order, so the band is
       inverted (``lower' = 1 / upper``, ``upper' = 1 / lower``).
    2. **Decimals-correct price->tick** -- delegated to the shared
       :func:`concentrated_liquidity_math.price_to_tick`. Decimals are mandatory
       (a USDC/WETH pair is a ~276k-tick shift, ALM-2891) and it fail-closes
       (raises) on a non-positive price rather than silently snapping to
       ``MIN_TICK``.
    3. **Tick-spacing alignment** -- both bounds are floored to the spacing
       boundary (matching the uniswap_v3 reference connector), and a band that
       collapsed into a single bucket (``tick_lower >= tick_upper``) is rejected.
    4. **Straddle invariant** -- only enforced when a live ``current_tick`` is
       supplied: the aligned band must satisfy
       ``tick_lower <= current_tick < tick_upper`` unless the caller opts into a
       one-sided (out-of-range) open via ``allow_out_of_range``.

    Args:
        range_lower: Lower price bound (token1 per token0), in the user's pair
            orientation. Must be positive.
        range_upper: Upper price bound, same orientation. Must be positive and
            greater than ``range_lower``.
        token0_decimals: Decimals of the pool's ``token0`` (required).
        token1_decimals: Decimals of the pool's ``token1`` (required).
        tokens_swapped: ``True`` when the pool's token order is the reciprocal
            of the user's stated pair (so the band must be inverted).
        tick_spacing: Pool tick spacing for the fee tier (must be positive).
        current_tick: Live pool tick, when known. ``None`` skips the straddle
            invariant (compute-time callers that read slot0 later pass ``None``).
        require_straddle: Enforce the straddle invariant when ``current_tick``
            is supplied. Defaults to ``True``.
        allow_out_of_range: Permit a one-sided open (band entirely above/below
            the current tick) even when ``current_tick`` is supplied.

    Returns:
        A :class:`TickRange` with both bounds aligned to ``tick_spacing``.

    Raises:
        PriceBandToTicksError: ``tick_spacing`` non-positive, a non-invertible
            (non-positive) band when ``tokens_swapped``, a band that collapsed
            after spacing alignment, or a straddle-invariant violation.
        ValueError: A non-positive price reaching the price->tick core.
    """
    if tick_spacing <= 0:
        raise PriceBandToTicksError(f"tick_spacing must be positive, got {tick_spacing}")

    lower = _as_decimal(range_lower)
    upper = _as_decimal(range_upper)

    # Step 1: orientation invert.
    if tokens_swapped:
        if lower <= 0 or upper <= 0:
            raise PriceBandToTicksError(
                f"price band must be positive to invert for swapped token order, got [{range_lower}, {range_upper}]"
            )
        lower, upper = Decimal(1) / upper, Decimal(1) / lower

    # Step 2: decimals-correct price -> tick (fail-closed on non-positive).
    tick_lower = price_to_tick(lower, decimals0=token0_decimals, decimals1=token1_decimals)
    tick_upper = price_to_tick(upper, decimals0=token0_decimals, decimals1=token1_decimals)

    # Step 3: tick-spacing alignment + collapse rejection.
    tick_lower = (tick_lower // tick_spacing) * tick_spacing
    tick_upper = (tick_upper // tick_spacing) * tick_spacing
    if tick_lower >= tick_upper:
        raise PriceBandToTicksError(
            "price band collapsed to a single tick after applying tick spacing; "
            "widen the range so the lower and upper ticks differ"
        )

    # Step 4: straddle invariant (only when a live current tick is known).
    if current_tick is not None and require_straddle and not allow_out_of_range:
        if not (tick_lower <= current_tick < tick_upper):
            raise PriceBandToTicksError(
                "price band does not straddle the current tick: need "
                f"tick_lower ({tick_lower}) <= current_tick ({current_tick}) "
                f"< tick_upper ({tick_upper})"
            )

    return TickRange(tick_lower=tick_lower, tick_upper=tick_upper)
