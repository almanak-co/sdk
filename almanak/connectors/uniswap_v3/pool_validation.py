"""Uniswap V3-family pool validation -- moved to the connector foundation.

The validator and slot0() reader are shared by the whole V3 factory family
(uniswap_v3 + sushiswap_v3 / pancakeswap_v3 / aerodrome slipstream / agni), so
they now live in
``almanak.connectors._strategy_base.v3_pool_validation``. This module re-exports
them so ``almanak.connectors.uniswap_v3.pool_validation`` stays a stable import
surface for the Uniswap V3 compiler.
"""

from __future__ import annotations

from almanak.connectors._strategy_base.v3_pool_validation import (
    V3_GET_POOL_SELECTOR,
    fetch_v3_pool_sqrt_price_x96,
    validate_v3_pool,
)

__all__ = [
    "V3_GET_POOL_SELECTOR",
    "fetch_v3_pool_sqrt_price_x96",
    "validate_v3_pool",
]
