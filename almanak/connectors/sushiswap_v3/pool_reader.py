"""Connector-owned pool reader spec for SushiSwap V3.

SushiSwap V3 is a standard Uniswap-V3 fork: fee-tier-keyed pools and the
canonical ``getPool(address,address,uint24)`` factory selector. No static
known-pools table is shipped — resolution falls back to ``factory.getPool``
across the standard fee tiers (the framework reader sweeps them).
"""

from __future__ import annotations

from almanak.connectors._strategy_base.pool_reader import PoolReaderSpec
from almanak.connectors._strategy_base.v3_pool_abi import V3_GET_POOL_SELECTOR

from .addresses import SUSHISWAP_V3

# Chains where SushiSwap V3 publishes a CL factory address.
_FACTORY_CHAINS = ("ethereum", "arbitrum", "base", "optimism", "polygon", "avalanche", "bsc")

POOL_READER_SPEC = PoolReaderSpec(
    protocol="sushiswap_v3",
    factory_addresses={chain: SUSHISWAP_V3[chain]["factory"] for chain in _FACTORY_CHAINS},
    get_pool_selector=V3_GET_POOL_SELECTOR,
)

__all__ = ["POOL_READER_SPEC"]
