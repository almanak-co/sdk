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
    """Convert a Q64.96 sqrt price to the nearest lower CL tick."""
    if sqrt_price_x96 <= 0:
        raise ValueError("sqrt_price_x96 must be positive")

    ratio = sqrt_price_x96 / Q96
    if ratio <= 0:
        raise ValueError("Invalid sqrt price ratio")

    tick = math.floor(math.log(ratio, math.sqrt(1.0001)))
    return max(MIN_TICK, min(MAX_TICK, tick))


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
    "price_to_sqrt_price_x96",
    "price_to_tick",
    "require_tick_spacing",
    "sqrt_price_x96_to_price",
    "sqrt_price_x96_to_tick",
    "tick_spacing_or_default",
    "tick_to_price",
    "tick_to_sqrt_price_x96",
]
