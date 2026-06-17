"""Raydium CLMM tick and liquidity math.

The implementation moved to the connector foundation -- the maths is identical
across Solana CLMM venues (Raydium CLMM, Orca Whirlpool) -- and now lives in
``almanak.connectors._strategy_base.solana_clmm_math``. This module re-exports
it so ``almanak.connectors.raydium.math`` stays a stable import surface for
Raydium's own SDK and tests.

All sqrt prices use Q64.64 fixed-point representation internally.

Reference: https://github.com/raydium-io/raydium-clmm
"""

from __future__ import annotations

from almanak.connectors._strategy_base.solana_clmm_math import (
    MAX_SQRT_PRICE_X64,
    MAX_TICK,
    MIN_SQRT_PRICE_X64,
    MIN_TICK,
    Q64,
    SolanaCLMMTickError,
    align_tick_to_spacing,
    get_amounts_from_liquidity,
    get_liquidity_from_amounts,
    price_to_tick,
    sqrt_price_x64_to_tick,
    tick_array_start_index,
    tick_to_price,
    tick_to_sqrt_price_x64,
)

__all__ = [
    "MAX_SQRT_PRICE_X64",
    "MAX_TICK",
    "MIN_SQRT_PRICE_X64",
    "MIN_TICK",
    "Q64",
    "SolanaCLMMTickError",
    "align_tick_to_spacing",
    "get_amounts_from_liquidity",
    "get_liquidity_from_amounts",
    "price_to_tick",
    "sqrt_price_x64_to_tick",
    "tick_array_start_index",
    "tick_to_price",
    "tick_to_sqrt_price_x64",
]
