"""Meteora DLMM bin math.

Provides bin_id <-> price conversion and bin array index computation
for the Meteora DLMM (Dynamic Liquidity Market Maker) program.

Key difference from Uniswap V3 / Raydium CLMM:
- DLMM uses discrete price bins instead of continuous ticks
- price = (1 + bin_step/10000)^(bin_id - BIN_ID_OFFSET)
- Each bin array holds BIN_ARRAY_SIZE (70) bins

Reference: https://docs.meteora.ag/dlmm/
"""

from __future__ import annotations

import math
from decimal import Decimal

from solders.pubkey import Pubkey

from .constants import BIN_ARRAY_SEED, BIN_ARRAY_SIZE, BIN_ID_OFFSET


def bin_id_to_price(bin_id: int, bin_step: int, decimals_x: int = 0, decimals_y: int = 0) -> Decimal:
    """Convert a bin ID to a human-readable price.

    price_raw = (1 + bin_step/10000)^(bin_id - BIN_ID_OFFSET)
    price     = price_raw * 10^(decimals_x - decimals_y)

    Args:
        bin_id: The bin identifier.
        bin_step: Pool's bin step in basis points.
        decimals_x: Token X decimals (for human-readable price adjustment).
        decimals_y: Token Y decimals (for human-readable price adjustment).

    Returns:
        Human-readable price of token X in terms of token Y.
    """
    exponent = bin_id - BIN_ID_OFFSET
    base = 1 + bin_step / 10000
    raw_price = Decimal(str(base**exponent))

    if decimals_x != 0 or decimals_y != 0:
        decimal_adjustment = Decimal(10) ** (decimals_x - decimals_y)
        return raw_price * decimal_adjustment

    return raw_price


def price_to_bin_id(price: Decimal, bin_step: int, decimals_x: int = 0, decimals_y: int = 0) -> int:
    """Convert a human-readable price to a bin ID.

    Inverse of bin_id_to_price using logarithms.

    Args:
        price: Human-readable price (token Y per token X).
        bin_step: Pool's bin step in basis points.
        decimals_x: Token X decimals.
        decimals_y: Token Y decimals.

    Returns:
        Nearest bin ID.

    Raises:
        ValueError: If price is not positive.
    """
    if price <= 0:
        raise ValueError(f"Price must be positive, got {price}")

    # Undo decimal adjustment to get raw price
    if decimals_x != 0 or decimals_y != 0:
        decimal_adjustment = Decimal(10) ** (decimals_x - decimals_y)
        raw_price = float(price / decimal_adjustment)
    else:
        raw_price = float(price)

    base = 1 + bin_step / 10000
    exponent = math.log(raw_price) / math.log(base)
    bin_id = round(exponent) + BIN_ID_OFFSET

    return bin_id


def get_bin_array_index(bin_id: int) -> int:
    """Compute which bin array contains the given bin ID.

    Bin arrays are indexed from 0, covering BIN_ARRAY_SIZE bins each.
    The mapping accounts for the BIN_ID_OFFSET.

    Args:
        bin_id: Bin identifier.

    Returns:
        Bin array index (can be negative for bins below offset).
    """
    if bin_id >= 0:
        return bin_id // BIN_ARRAY_SIZE
    else:
        return -(-bin_id - 1) // BIN_ARRAY_SIZE - 1


def get_bin_array_lower_upper_bin_id(bin_array_index: int) -> tuple[int, int]:
    """Get the lower and upper bin IDs covered by a bin array.

    Args:
        bin_array_index: The bin array index.

    Returns:
        Tuple of (lower_bin_id, upper_bin_id) inclusive.
    """
    lower = bin_array_index * BIN_ARRAY_SIZE
    upper = lower + BIN_ARRAY_SIZE - 1
    return lower, upper


def get_bin_array_pda(program_id: Pubkey, lb_pair: Pubkey, bin_array_index: int) -> Pubkey:
    """Derive the PDA for a bin array account.

    seeds: [BIN_ARRAY_SEED, lb_pair, bin_array_index (i64 LE)]

    Args:
        program_id: Meteora DLMM program ID.
        lb_pair: Pool (lb_pair) address.
        bin_array_index: Bin array index (signed).

    Returns:
        Bin array PDA address.
    """
    import struct

    index_bytes = struct.pack("<q", bin_array_index)  # i64 little-endian
    pda, _bump = Pubkey.find_program_address(
        [BIN_ARRAY_SEED, bytes(lb_pair), index_bytes],
        program_id,
    )
    return pda
