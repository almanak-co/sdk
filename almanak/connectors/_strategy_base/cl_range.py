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

from almanak.connectors._strategy_base.concentrated_liquidity_math import (
    price_to_tick,
    require_tick_spacing,
)

__all__ = [
    "LpToleranceOutOfRangeError",
    "PriceBandToTicksError",
    "TickRange",
    "price_band_to_ticks",
    "require_lp_tolerance_fits_range",
]


class PriceBandToTicksError(ValueError):
    """Raised when a price band cannot be turned into a valid tick range.

    A ``ValueError`` subclass so callers that already treat bad numeric input
    as ``ValueError`` keep working, while callers that want to distinguish a
    seam-level rejection (collapse / straddle / non-invertible band) from an
    arbitrary error can catch this specific type.
    """


class LpToleranceOutOfRangeError(PriceBandToTicksError):
    """Raised when an LP price-band tolerance cannot fit inside its own range.

    Distinct from a generic band failure so a caller can tell "your tolerance and
    your range are incompatible" (actionable: change one of two numbers) from
    "this band is unusable" (collapse / straddle / non-invertible).
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


_TICK_BASE_LN = Decimal("1.0001").ln()


def _pool_tick_spacing(pool: str, *, pool_encodes_tick_spacing: bool) -> int:
    """Resolve a pool string's tick spacing OFFLINE, with no chain read.

    The third component is a tick spacing for spacing-addressed pools
    (``aerodrome_slipstream``: ``WETH/USDC/200``) and a FEE TIER for the V3
    family (``WETH/USDC/3000``), where spacing comes from
    :data:`concentrated_liquidity_math.V3_TICK_SPACING`. Reading a fee tier as a
    spacing is not a small error -- 3000 would be read as a 3000-tick spacing
    rather than 60 -- so the caller states which encoding its protocol uses
    rather than the shape being guessed here.
    """
    parts = pool.split("/")
    if len(parts) < 3:
        raise LpToleranceOutOfRangeError(
            f"cannot resolve tick spacing from pool {pool!r}: expected "
            f"TOKEN0/TOKEN1/<fee-tier-or-tick-spacing>. Without the spacing the "
            f"protective-minimum check below cannot run, and it fails closed "
            f"rather than assuming a spacing."
        )
    try:
        third = int(parts[2])
    except ValueError as exc:
        raise LpToleranceOutOfRangeError(
            f"pool {pool!r} third component {parts[2]!r} is not an integer fee tier or tick spacing"
        ) from exc
    if pool_encodes_tick_spacing:
        if third <= 0:
            raise LpToleranceOutOfRangeError(f"pool {pool!r} declares a non-positive tick spacing {third}")
        return third
    try:
        return require_tick_spacing(third)
    except ValueError as exc:
        # `require_tick_spacing` fails closed on an unknown tier (it never
        # substitutes a default), which is the behaviour we want -- but it raises a
        # bare ValueError. Re-raise as the documented type so a caller catching
        # LpToleranceOutOfRangeError sees every refusal this function can produce.
        raise LpToleranceOutOfRangeError(
            f"pool {pool!r} declares fee tier {third}, which has no known tick "
            f"spacing ({exc}). Refusing rather than assuming a spacing, because a "
            f"wrong spacing silently changes whether the tolerance fits."
        ) from exc


def require_lp_tolerance_fits_range(
    *,
    max_slippage: Decimal | float | int | str | None,
    range_half_width_frac: Decimal | float | int | str,
    pool: str,
    pool_encodes_tick_spacing: bool,
) -> None:
    """Refuse an LP price-band tolerance the position's own range cannot floor.

    Since VIB-6269 a CL mint's ``max_slippage`` is a PRICE band: the compiler
    emits its image in ``amount0Min``/``amount1Min``. That instrument is only
    protective while the band stays strictly inside the realised range. When the
    band reaches a range bound, the leg on that side is worth zero at the band
    edge and ships with **no minimum at all**; when it reaches both bounds the
    construction degenerates and ``compute_lp_slippage_mins`` falls back to a
    flat haircut tight enough to revert an ordinary mint.

    **Spacing is the part a requested-width comparison misses.**
    :func:`price_band_to_ticks` FLOORS both bounds to the pool's tick spacing, so
    the realised bound sits up to ``tick_spacing - 1`` ticks below the requested
    one. Flooring moves ``tick_upper`` TOWARD spot, which is why the leg that
    loses its floor is ``amount0Min``. Comparing the tolerance against the
    *requested* half-width therefore admits configurations that still ship an
    unfloored leg -- measured on ``WETH/USDC/3000`` (spacing 60) at a ±1% range,
    and on ``WETH/USDC/200`` (spacing 200) at ±2%.

    Working in ticks rather than prices is what makes the check exact: spacing is
    a tick quantity, and a fixed tick budget is a different price width at every
    range. The predicate is

    .. code-block:: text

        tolerance_ticks + (tick_spacing - 1)  <  half_width_ticks

    where ``x_ticks = ln(1 + x) / ln(1.0001)``. The ``tick_spacing - 1`` term is
    the worst case over spot's unknown position inside its spacing bucket, which
    is the right posture for a boot-time guard that cannot read the live tick.

    ``range_half_width_frac`` is a FRACTION of spot (``0.05`` = ±5%),
    deliberately not the ``range_width_pct`` config key: that key means a percent
    half-width in the scaffold and a fractional TOTAL width in the demos, and a
    shared helper must not inherit that ambiguity. Callers convert.

    Refuses rather than clamps. VIB-6217 established the rule for this class:
    silently repairing an out-of-range tolerance hides the misconfiguration that
    produced it, which is maximum harm with no signal.

    Args:
        max_slippage: Declared tolerance as a fraction (``0.005`` = 0.5%).
            ``None`` means no tolerance was declared, so the price-band
            instrument is never selected and there is nothing to check.
        range_half_width_frac: Half-width of the intended symmetric range as a
            fraction of spot. Must be finite and in ``(0, 1)`` so its lower
            price remains positive.
        pool: Pool string, ``TOKEN0/TOKEN1/<fee-tier-or-tick-spacing>``.
        pool_encodes_tick_spacing: ``True`` when the third component is a tick
            spacing (Slipstream), ``False`` when it is a V3 fee tier.

    Raises:
        LpToleranceOutOfRangeError: the tolerance or half-width is malformed,
            non-finite, or outside ``(0, 1)``, the spacing cannot be resolved,
            or the band does not fit inside the realised range.
    """
    if max_slippage is None:
        return

    try:
        tolerance = _as_decimal(max_slippage)
    except (ArithmeticError, TypeError, ValueError) as exc:
        raise LpToleranceOutOfRangeError(
            f"max_slippage must be a numeric fraction in (0, 1), got {max_slippage!r}. "
            "Set max_slippage to a positive fraction below 1, such as 0.005."
        ) from exc
    if not tolerance.is_finite() or not (Decimal(0) < tolerance < Decimal(1)):
        raise LpToleranceOutOfRangeError(
            f"max_slippage must be in (0, 1) as a fraction, got {tolerance}. "
            f"A tolerance of 0 floors every mint at its exact desired amounts; "
            f"1 or more sizes both minimums at zero and accepts any outcome. "
            f"Set max_slippage to a positive fraction below 1, such as 0.005."
        )

    try:
        half_width = _as_decimal(range_half_width_frac)
    except (ArithmeticError, TypeError, ValueError) as exc:
        raise LpToleranceOutOfRangeError(
            f"range half-width must be a numeric fraction in (0, 1), got {range_half_width_frac!r}. "
            "Set range_width_pct between 0 and 100."
        ) from exc
    if not half_width.is_finite() or not (Decimal(0) < half_width < Decimal(1)):
        raise LpToleranceOutOfRangeError(
            f"range half-width must be in (0, 1), got {half_width}. A zero, "
            "negative, or 100%-plus half-width cannot form a positive symmetric "
            "price range. Set range_width_pct between 0 and 100."
        )

    spacing = _pool_tick_spacing(pool, pool_encodes_tick_spacing=pool_encodes_tick_spacing)
    half_width_ticks = (Decimal(1) + half_width).ln() / _TICK_BASE_LN
    tolerance_ticks = (Decimal(1) + tolerance).ln() / _TICK_BASE_LN
    headroom_ticks = half_width_ticks - Decimal(spacing - 1)

    if tolerance_ticks >= headroom_ticks:
        raise LpToleranceOutOfRangeError(
            f"LP tolerance {tolerance} does not fit inside the range it is meant to "
            f"protect: it spans {tolerance_ticks.quantize(Decimal('1'))} ticks, but the "
            f"range half-width is only {half_width_ticks.quantize(Decimal('1'))} ticks and "
            f"pool {pool!r} floors bounds to a {spacing}-tick spacing, leaving "
            f"{headroom_ticks.quantize(Decimal('1'))} usable ticks. At or beyond that the "
            f"price band reaches a range bound and that leg is submitted with NO minimum. "
            f"Widen the range, tighten max_slippage, or choose a pool with finer spacing."
        )
