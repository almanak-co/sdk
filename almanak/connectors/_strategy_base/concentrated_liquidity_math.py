"""Shared concentrated-liquidity math primitives.

These helpers are protocol-clean building blocks for Uniswap V3-style
concentrated-liquidity connectors. Concrete connector SDKs keep their public
exception contracts, but delegate the shared arithmetic here so framework code
does not import a concrete connector for tick conversion.
"""

from __future__ import annotations

import math
from decimal import Decimal, localcontext
from typing import Literal

Q96 = 2**96
Q128 = 2**128
MAX_UINT128 = 2**128 - 1
MAX_UINT256 = 2**256 - 1

MIN_TICK = -887272
MAX_TICK = 887272

V3_TICK_SPACING: dict[int, int] = {
    100: 1,
    500: 10,
    2500: 50,
    3000: 60,
    10000: 200,
}


def _as_decimal(value: Decimal | float | int | str) -> Decimal:
    """Convert numeric inputs to Decimal without preserving binary float noise."""
    return value if isinstance(value, Decimal) else Decimal(str(value))


def tick_to_sqrt_price_x96(tick: int) -> int:
    """Convert a CL tick to a Q64.96 sqrt price."""
    if tick < MIN_TICK or tick > MAX_TICK:
        raise ValueError(f"tick must be between {MIN_TICK} and {MAX_TICK}, got {tick}")
    sqrt_ratio = math.pow(1.0001, tick / 2)
    return int(sqrt_ratio * Q96)


def sqrt_price_x96_to_tick(sqrt_price_x96: int) -> int:
    """Convert a Q64.96 sqrt price to a CL tick.

    Returns the greatest tick ``t`` such that
    ``tick_to_sqrt_price_x96(t) <= sqrt_price_x96`` — Uniswap
    ``TickMath.getTickAtSqrtRatio`` semantics — pinned to this module's own
    forward so the round-trip is exact.

    The bare ``floor(log(...))`` estimate double-floors against the floored
    forward (``int(...)``) and lands one tick low for negative ticks (e.g.
    tick ``-1`` round-tripped to ``-2``); a bounded correction step pins it
    back to the invariant. The correction walks at most ~1 tick since the
    log estimate is already within one of the answer.
    """
    if sqrt_price_x96 <= 0:
        raise ValueError("sqrt_price_x96 must be positive")

    ratio = sqrt_price_x96 / Q96
    if ratio <= 0:
        raise ValueError("Invalid sqrt price ratio")

    candidate = math.floor(math.log(ratio, math.sqrt(1.0001)))
    # Clamp to the valid domain first so an out-of-range price (e.g. 2**200)
    # cannot spin the correction loop against the forward function.
    candidate = max(MIN_TICK, min(MAX_TICK, candidate))
    while candidate < MAX_TICK and tick_to_sqrt_price_x96(candidate + 1) <= sqrt_price_x96:
        candidate += 1
    while candidate > MIN_TICK and tick_to_sqrt_price_x96(candidate) > sqrt_price_x96:
        candidate -= 1
    return candidate


def tick_to_price(tick: int, decimals0: int = 18, decimals1: int = 18) -> Decimal:
    """Convert a CL tick to a human-readable token1/token0 price."""
    raw_price = Decimal("1.0001") ** tick
    decimal_adjustment = Decimal(10) ** (decimals0 - decimals1)
    return raw_price * decimal_adjustment


def price_to_tick(
    price: Decimal | float | int | str,
    decimals0: int = 18,
    decimals1: int = 18,
    *,
    non_positive: Literal["raise", "min_tick"] = "raise",
) -> int:
    """Convert a human-readable token1/token0 price to a CL tick.

    Uses Decimal logarithms to avoid float boundary drift for decimal-asymmetric
    token pairs. ``non_positive="min_tick"`` preserves legacy connector SDK
    behavior; public framework helpers should use the default fail-closed mode.
    """
    price_dec = _as_decimal(price)
    if price_dec <= 0:
        if non_positive == "min_tick":
            return MIN_TICK
        raise ValueError("Price must be positive")

    with localcontext() as ctx:
        ctx.prec = 50
        decimal_diff = decimals0 - decimals1
        if decimal_diff >= 0:
            adjusted_price = price_dec / (Decimal(10) ** decimal_diff)
        else:
            adjusted_price = price_dec * (Decimal(10) ** (-decimal_diff))

        if adjusted_price <= 0:
            if non_positive == "min_tick":
                return MIN_TICK
            raise ValueError("Price must be positive")

        ratio = adjusted_price.ln() / Decimal("1.0001").ln()
        tick = math.floor(ratio)

    return max(MIN_TICK, min(MAX_TICK, tick))


def sqrt_price_x96_to_price(sqrt_price_x96: int) -> Decimal:
    """Convert a Q64.96 sqrt price to a Decimal price."""
    if sqrt_price_x96 <= 0:
        return Decimal("0")
    ratio = Decimal(str(sqrt_price_x96)) / Decimal(str(Q96))
    return ratio * ratio


def price_to_sqrt_price_x96(price: Decimal | float | int | str) -> int:
    """Convert a Decimal price to Q64.96 sqrt price."""
    price_dec = _as_decimal(price)
    if price_dec <= 0:
        return 0
    with localcontext() as ctx:
        ctx.prec = 50
        sqrt_price = price_dec.sqrt()
        return int(sqrt_price * Q96)


with localcontext() as _ctx:
    _ctx.prec = 50
    _LN_1_0001 = Decimal("1.0001").ln()


def tick_to_sqrt_price_decimal(tick: int) -> Decimal:
    """Convert a CL tick to sqrt(raw price) as a high-precision Decimal.

    Decimal counterpart of :func:`tick_to_sqrt_price_x96` for valuation code
    that works in Decimal planes (ALM-2948): sqrt(1.0001^tick) computed as
    exp(tick/2 * ln(1.0001)) under 50-digit precision.
    """
    if tick < MIN_TICK or tick > MAX_TICK:
        raise ValueError(f"tick must be between {MIN_TICK} and {MAX_TICK}, got {tick}")
    with localcontext() as ctx:
        ctx.prec = 50
        return (Decimal(tick) / 2 * _LN_1_0001).exp()


def sqrt_price_decimal(price: Decimal | float | int | str) -> Decimal:
    """sqrt() of a positive price as a high-precision Decimal."""
    price_dec = _as_decimal(price)
    if price_dec <= 0:
        raise ValueError("Price must be positive")
    with localcontext() as ctx:
        ctx.prec = 50
        return price_dec.sqrt()


def position_token_amounts(
    liquidity: Decimal,
    sqrt_price: Decimal,
    sqrt_price_lower: Decimal,
    sqrt_price_upper: Decimal,
) -> tuple[Decimal, Decimal]:
    """V3 position composition (amount0, amount1) for liquidity L.

    The canonical three-case formula, plane-agnostic: pass raw-plane sqrt
    prices for on-chain composition or human-plane sqrt ratios for
    valuation — the caller owns plane consistency (ALM-2948: the live IL
    bug was mixing the two).
    """
    if liquidity < 0:
        raise ValueError("liquidity must be non-negative")
    if sqrt_price_lower > sqrt_price_upper:
        sqrt_price_lower, sqrt_price_upper = sqrt_price_upper, sqrt_price_lower
    if sqrt_price_lower <= 0:
        raise ValueError("sqrt_price_lower must be positive")
    with localcontext() as ctx:
        ctx.prec = 50
        if sqrt_price <= sqrt_price_lower:
            amount0 = liquidity * (Decimal(1) / sqrt_price_lower - Decimal(1) / sqrt_price_upper)
            amount1 = Decimal(0)
        elif sqrt_price >= sqrt_price_upper:
            amount0 = Decimal(0)
            amount1 = liquidity * (sqrt_price_upper - sqrt_price_lower)
        else:
            amount0 = liquidity * (Decimal(1) / sqrt_price - Decimal(1) / sqrt_price_upper)
            amount1 = liquidity * (sqrt_price - sqrt_price_lower)
        return max(Decimal(0), amount0), max(Decimal(0), amount1)


def concentrated_il(
    entry_price: Decimal,
    current_price: Decimal,
    price_lower: Decimal,
    price_upper: Decimal,
) -> tuple[Decimal, tuple[Decimal, Decimal], tuple[Decimal, Decimal]]:
    """True V3 impermanent loss for a concentrated position (ALM-2948).

    All four prices live on ONE plane (token1 per token0 — human or raw,
    caller's choice, but consistently). Composition is computed at entry and
    now for a unit of liquidity; IL is scale-free.

    Returns ``(il_ratio, entry_amounts, current_amounts)`` where ``il_ratio``
    is negative on loss (``pool_value / hold_value - 1``) and the amount
    tuples are per unit liquidity.
    """
    if price_lower > price_upper:
        price_lower, price_upper = price_upper, price_lower
    sqrt_lower = sqrt_price_decimal(price_lower)
    sqrt_upper = sqrt_price_decimal(price_upper)
    if sqrt_lower == sqrt_upper:
        raise ValueError("Degenerate range: price_lower == price_upper")
    entry_amount0, entry_amount1 = position_token_amounts(
        Decimal(1), sqrt_price_decimal(entry_price), sqrt_lower, sqrt_upper
    )
    now_amount0, now_amount1 = position_token_amounts(
        Decimal(1), sqrt_price_decimal(current_price), sqrt_lower, sqrt_upper
    )
    with localcontext() as ctx:
        ctx.prec = 50
        current = _as_decimal(current_price)
        hold_value = entry_amount0 * current + entry_amount1
        pool_value = now_amount0 * current + now_amount1
        if hold_value <= 0:
            return Decimal(0), (entry_amount0, entry_amount1), (now_amount0, now_amount1)
        return pool_value / hold_value - 1, (entry_amount0, entry_amount1), (now_amount0, now_amount1)


def require_tick_spacing(fee_tier: int, tick_spacing: dict[int, int] | None = None) -> int:
    """Return tick spacing for a fee tier, raising for unsupported tiers."""
    spacing_map = tick_spacing or V3_TICK_SPACING
    try:
        return spacing_map[fee_tier]
    except KeyError as exc:
        raise ValueError(f"Invalid fee tier: {fee_tier}. Valid tiers: {sorted(spacing_map)}") from exc


def tick_spacing_or_default(fee_tier: int, default: int = 60) -> int:
    """Return common V3 tick spacing, defaulting for protocol-specific tiers."""
    return V3_TICK_SPACING.get(fee_tier, default)


def get_nearest_tick(tick: int, fee_tier: int, tick_spacing: dict[int, int] | None = None) -> int:
    """Snap a tick to the nearest valid boundary for ``fee_tier``."""
    spacing = require_tick_spacing(fee_tier, tick_spacing)
    rounded = round(tick / spacing) * spacing
    return max(get_min_tick(fee_tier, tick_spacing), min(get_max_tick(fee_tier, tick_spacing), rounded))


def get_min_tick(fee_tier: int, tick_spacing: dict[int, int] | None = None) -> int:
    """Return the minimum tick aligned to ``fee_tier`` spacing."""
    spacing = require_tick_spacing(fee_tier, tick_spacing)
    return -(-MIN_TICK // spacing) * spacing


def get_max_tick(fee_tier: int, tick_spacing: dict[int, int] | None = None) -> int:
    """Return the maximum tick aligned to ``fee_tier`` spacing."""
    spacing = require_tick_spacing(fee_tier, tick_spacing)
    return (MAX_TICK // spacing) * spacing


__all__ = [
    "MAX_TICK",
    "MAX_UINT128",
    "MAX_UINT256",
    "MIN_TICK",
    "Q96",
    "Q128",
    "V3_TICK_SPACING",
    "get_max_tick",
    "get_min_tick",
    "get_nearest_tick",
    "concentrated_il",
    "position_token_amounts",
    "price_to_sqrt_price_x96",
    "price_to_tick",
    "require_tick_spacing",
    "sqrt_price_decimal",
    "sqrt_price_x96_to_price",
    "sqrt_price_x96_to_tick",
    "tick_spacing_or_default",
    "tick_to_price",
    "tick_to_sqrt_price_decimal",
    "tick_to_sqrt_price_x96",
]
