"""Raydium CLMM tick and liquidity math.

Provides tick ↔ price conversion, sqrt-price calculations, and
liquidity amount computation following the concentrated liquidity
math used by Raydium (same as Uniswap V3 / Orca Whirlpool).

All sqrt prices use Q64.64 fixed-point representation internally.

Reference: https://github.com/raydium-io/raydium-clmm
"""

from __future__ import annotations

import math
from decimal import Decimal

from .constants import MAX_SQRT_PRICE_X64, MAX_TICK, MIN_SQRT_PRICE_X64, MIN_TICK, Q64
from .exceptions import RaydiumTickError


def price_to_tick(price: Decimal, decimals_a: int, decimals_b: int) -> int:
    """Convert a human-readable price to a tick index.

    The price is in terms of token_b per token_a (e.g., 150.0 USDC/SOL).

    The tick formula accounts for the decimal difference between tokens:
        adjusted_price = price * 10^(decimals_b - decimals_a)
        tick = floor(log(adjusted_price) / log(1.0001))

    Args:
        price: Human-readable price (token_b per token_a).
        decimals_a: Decimals of token A (the base token).
        decimals_b: Decimals of token B (the quote token).

    Returns:
        Tick index (integer).

    Raises:
        RaydiumTickError: If the price produces a tick outside valid range.
    """
    if price <= 0:
        raise RaydiumTickError(f"Price must be positive, got {price}")

    # Adjust for decimal difference (convert human price to raw price)
    decimal_adjustment = Decimal(10) ** (decimals_b - decimals_a)
    adjusted_price = float(price * decimal_adjustment)

    tick = math.floor(math.log(adjusted_price) / math.log(1.0001))

    if tick < MIN_TICK or tick > MAX_TICK:
        raise RaydiumTickError(f"Price {price} produces tick {tick} outside valid range [{MIN_TICK}, {MAX_TICK}]")

    return tick


def tick_to_price(tick: int, decimals_a: int, decimals_b: int) -> Decimal:
    """Convert a tick index to a human-readable price.

    Args:
        tick: Tick index.
        decimals_a: Decimals of token A.
        decimals_b: Decimals of token B.

    Returns:
        Human-readable price (token_b per token_a).
    """
    raw_price = Decimal(str(1.0001**tick))
    decimal_adjustment = Decimal(10) ** (decimals_a - decimals_b)
    return raw_price * decimal_adjustment


def align_tick_to_spacing(tick: int, tick_spacing: int, round_up: bool = False) -> int:
    """Align a tick to the nearest valid tick for the pool's tick spacing.

    Args:
        tick: Raw tick index.
        tick_spacing: Pool's tick spacing (e.g., 1, 10, 60, 120).
        round_up: If True, round toward positive infinity; else toward negative infinity.

    Returns:
        Aligned tick index.
    """
    if tick_spacing <= 0:
        raise RaydiumTickError(f"tick_spacing must be positive, got {tick_spacing}")

    if tick >= 0:
        aligned = (tick // tick_spacing) * tick_spacing
        if round_up and tick % tick_spacing != 0:
            aligned += tick_spacing
    else:
        # For negative ticks, floor division works differently
        aligned = -(-tick // tick_spacing) * tick_spacing
        if not round_up and tick % tick_spacing != 0:
            aligned -= tick_spacing

    # Clamp to valid range
    return max(MIN_TICK, min(MAX_TICK, aligned))


def tick_to_sqrt_price_x64(tick: int) -> int:
    """Convert a tick index to a Q64.64 sqrt price.

    sqrt_price = sqrt(1.0001^tick) * 2^64

    Args:
        tick: Tick index.

    Returns:
        Q64.64 fixed-point sqrt price.
    """
    sqrt_price = math.sqrt(1.0001**tick)
    result = int(sqrt_price * Q64)
    return max(MIN_SQRT_PRICE_X64, min(MAX_SQRT_PRICE_X64, result))


def sqrt_price_x64_to_tick(sqrt_price_x64: int) -> int:
    """Convert a Q64.64 sqrt price back to a tick index.

    Args:
        sqrt_price_x64: Q64.64 fixed-point sqrt price.

    Returns:
        Tick index.
    """
    sqrt_price = sqrt_price_x64 / Q64
    price = sqrt_price**2
    if price <= 0:
        return MIN_TICK
    tick = math.floor(math.log(price) / math.log(1.0001))
    return max(MIN_TICK, min(MAX_TICK, tick))


def get_liquidity_from_amounts(
    sqrt_price_x64: int,
    sqrt_price_lower_x64: int,
    sqrt_price_upper_x64: int,
    amount_a: int,
    amount_b: int,
) -> int:
    """Calculate liquidity from token amounts and price range.

    Uses the concentrated liquidity formula:
    - If current price < lower: only token A is needed
      L = amount_a * sqrt_lower * sqrt_upper / (sqrt_upper - sqrt_lower)
    - If current price > upper: only token B is needed
      L = amount_b / (sqrt_upper - sqrt_lower)
    - If in range: take the minimum of both
      L = min(La, Lb)

    All sqrt prices are Q64.64 fixed-point.

    Args:
        sqrt_price_x64: Current sqrt price (Q64.64).
        sqrt_price_lower_x64: Lower bound sqrt price (Q64.64).
        sqrt_price_upper_x64: Upper bound sqrt price (Q64.64).
        amount_a: Amount of token A in smallest units.
        amount_b: Amount of token B in smallest units.

    Returns:
        Liquidity amount (u128).
    """
    if sqrt_price_lower_x64 >= sqrt_price_upper_x64:
        raise RaydiumTickError("Lower sqrt price must be less than upper sqrt price")

    if sqrt_price_x64 <= sqrt_price_lower_x64:
        # Below range: only token A
        numerator = amount_a * sqrt_price_lower_x64 * sqrt_price_upper_x64
        denominator = (sqrt_price_upper_x64 - sqrt_price_lower_x64) * Q64
        return numerator // denominator if denominator > 0 else 0

    if sqrt_price_x64 >= sqrt_price_upper_x64:
        # Above range: only token B
        numerator = amount_b * Q64
        denominator = sqrt_price_upper_x64 - sqrt_price_lower_x64
        return numerator // denominator if denominator > 0 else 0

    # In range: min of both calculations
    la_num = amount_a * sqrt_price_x64 * sqrt_price_upper_x64
    la_den = (sqrt_price_upper_x64 - sqrt_price_x64) * Q64
    la = la_num // la_den if la_den > 0 else 0

    lb_num = amount_b * Q64
    lb_den = sqrt_price_x64 - sqrt_price_lower_x64
    lb = lb_num // lb_den if lb_den > 0 else 0

    return min(la, lb)


def get_amounts_from_liquidity(
    sqrt_price_x64: int,
    sqrt_price_lower_x64: int,
    sqrt_price_upper_x64: int,
    liquidity: int,
) -> tuple[int, int]:
    """Calculate token amounts from liquidity and price range.

    Inverse of get_liquidity_from_amounts.

    Args:
        sqrt_price_x64: Current sqrt price (Q64.64).
        sqrt_price_lower_x64: Lower bound sqrt price (Q64.64).
        sqrt_price_upper_x64: Upper bound sqrt price (Q64.64).
        liquidity: Liquidity amount (u128).

    Returns:
        Tuple of (amount_a, amount_b) in smallest units.
    """
    if sqrt_price_x64 <= sqrt_price_lower_x64:
        # Below range: only token A
        amount_a = (
            liquidity
            * (sqrt_price_upper_x64 - sqrt_price_lower_x64)
            * Q64
            // (sqrt_price_lower_x64 * sqrt_price_upper_x64)
        )
        return (amount_a, 0)

    if sqrt_price_x64 >= sqrt_price_upper_x64:
        # Above range: only token B
        amount_b = liquidity * (sqrt_price_upper_x64 - sqrt_price_lower_x64) // Q64
        return (0, amount_b)

    # In range: both tokens
    amount_a = liquidity * (sqrt_price_upper_x64 - sqrt_price_x64) * Q64 // (sqrt_price_x64 * sqrt_price_upper_x64)
    amount_b = liquidity * (sqrt_price_x64 - sqrt_price_lower_x64) // Q64

    return (amount_a, amount_b)


def tick_array_start_index(tick: int, tick_spacing: int) -> int:
    """Compute the start index of the tick array containing the given tick.

    Each tick array holds 60 ticks. The start index is the first tick
    in the array that contains the given tick.

    Args:
        tick: Tick index.
        tick_spacing: Pool's tick spacing.

    Returns:
        Tick array start index.
    """
    ticks_per_array = 60
    array_size = ticks_per_array * tick_spacing

    if tick >= 0:
        return (tick // array_size) * array_size
    else:
        return -(((-tick - 1) // array_size + 1) * array_size)
