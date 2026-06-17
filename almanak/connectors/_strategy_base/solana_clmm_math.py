"""Shared Solana concentrated-liquidity (CLMM) tick and liquidity math.

Tick <-> price conversion, sqrt-price calculations, and liquidity amount
computation for Solana CLMM pools. The maths is identical across Solana CLMM
venues (Raydium CLMM, Orca Whirlpool) -- same Q64.64 fixed-point sqrt prices,
same 60-tick tick arrays -- so it lives in the connector foundation rather than
in any single connector. Connectors import it from here; deleting one Solana
CLMM connector must not strand the shared maths in another.

Foundation rule: this module imports only the stdlib -- no concrete connector,
no framework. Mirrors the EVM-side ``concentrated_liquidity_math.py``.

All sqrt prices use Q64.64 fixed-point representation internally.

References:
  - https://github.com/raydium-io/raydium-clmm
  - https://orca-so.gitbook.io/orca-developer-portal/whirlpools
"""

from __future__ import annotations

import math
from decimal import Decimal

# Minimum and maximum tick indices (shared across Solana CLMM venues).
MIN_TICK = -443636
MAX_TICK = 443636

# Q64.64 fixed-point scale.
Q64 = 1 << 64

# Min/max sqrt prices (Q64.64).
MIN_SQRT_PRICE_X64 = 4295048016
MAX_SQRT_PRICE_X64 = 79226673521066979257578248091


class SolanaCLMMTickError(Exception):
    """Error with Solana CLMM tick / liquidity calculations."""


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
        SolanaCLMMTickError: If the price produces a tick outside valid range.
    """
    if price <= 0:
        raise SolanaCLMMTickError(f"Price must be positive, got {price}")

    # Adjust for decimal difference (convert human price to raw price)
    decimal_adjustment = Decimal(10) ** (decimals_b - decimals_a)
    adjusted_price = float(price * decimal_adjustment)

    tick = math.floor(math.log(adjusted_price) / math.log(1.0001))

    if tick < MIN_TICK or tick > MAX_TICK:
        raise SolanaCLMMTickError(f"Price {price} produces tick {tick} outside valid range [{MIN_TICK}, {MAX_TICK}]")

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
        raise SolanaCLMMTickError(f"tick_spacing must be positive, got {tick_spacing}")

    if tick >= 0:
        aligned = (tick // tick_spacing) * tick_spacing
        if round_up and tick % tick_spacing != 0:
            aligned += tick_spacing
    else:
        # For negative ticks, floor division works differently
        aligned = -(-tick // tick_spacing) * tick_spacing
        if not round_up and tick % tick_spacing != 0:
            aligned -= tick_spacing

    # Clamp to valid range. MIN_TICK / MAX_TICK are not guaranteed to be multiples
    # of tick_spacing, so clamping to them directly could return an unaligned tick at
    # the extremes. Clamp to the nearest in-bounds spacing-aligned bounds instead:
    # the smallest aligned tick >= MIN_TICK and the largest aligned tick <= MAX_TICK.
    min_aligned = -((-MIN_TICK) // tick_spacing) * tick_spacing
    max_aligned = (MAX_TICK // tick_spacing) * tick_spacing
    return max(min_aligned, min(max_aligned, aligned))


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
        raise SolanaCLMMTickError("Lower sqrt price must be less than upper sqrt price")

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

    Raises:
        SolanaCLMMTickError: If any sqrt price is non-positive, or the range is
            inverted / zero-width (lower >= upper).
    """
    # Validate inputs before any division. An inverted or zero-width range yields
    # negative amounts (e.g. a negative width in the below/above-range branches),
    # and a non-positive sqrt price divides by zero in the in-range / below-range
    # denominators. Mirror get_liquidity_from_amounts, which guards lower < upper.
    if sqrt_price_x64 <= 0 or sqrt_price_lower_x64 <= 0 or sqrt_price_upper_x64 <= 0:
        raise SolanaCLMMTickError("Sqrt prices must be positive")
    if sqrt_price_lower_x64 >= sqrt_price_upper_x64:
        raise SolanaCLMMTickError("Lower sqrt price must be less than upper sqrt price")

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


def tick_array_start_index(tick: int, tick_spacing: int, *, ticks_per_array: int = 60) -> int:
    """Compute the start index of the tick array containing the given tick.

    The start index is the first tick in the array that contains the given
    tick. ``ticks_per_array`` is venue-specific: Raydium CLMM arrays hold 60
    ticks (the default), Orca Whirlpools hold 88 — pass the venue's size so a
    shared caller derives the correct tick-array start.

    Args:
        tick: Tick index.
        tick_spacing: Pool's tick spacing.
        ticks_per_array: Ticks per array for the venue (Raydium 60, Orca 88).

    Returns:
        Tick array start index.
    """
    if tick_spacing <= 0:
        raise SolanaCLMMTickError(f"tick_spacing must be positive, got {tick_spacing}")
    if ticks_per_array <= 0:
        raise SolanaCLMMTickError(f"ticks_per_array must be positive, got {ticks_per_array}")
    array_size = ticks_per_array * tick_spacing

    if tick >= 0:
        return (tick // array_size) * array_size
    else:
        return -(((-tick - 1) // array_size + 1) * array_size)
