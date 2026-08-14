"""Connector-owned pool reader spec for SushiSwap V3.

SushiSwap V3 is a standard Uniswap-V3 fork: fee-tier-keyed pools and the
canonical ``getPool(address,address,uint24)`` factory selector. No static
known-pools table is shipped — resolution falls back to ``factory.getPool``
across the standard fee tiers (the framework reader sweeps them).
"""

from __future__ import annotations

from almanak.connectors._connector import ImportRef
from almanak.connectors._strategy_base.pool_data import (
    PoolDataFacet,
    PoolDataSource,
    PoolDataSpec,
    PoolReferenceKind,
)
from almanak.connectors._strategy_base.pool_reader import PoolReaderSpec
from almanak.connectors._strategy_base.v3_pool_abi import V3_GET_POOL_SELECTOR

from .addresses import SUSHISWAP_V3

POOL_READER_SPEC = PoolReaderSpec(
    protocol="sushiswap_v3",
    factory_addresses={
        chain: deployment["factory"] for chain, deployment in SUSHISWAP_V3.items() if "factory" in deployment
    },
    reader=ImportRef(
        module="almanak.framework.data.pools.reader",
        attribute="SushiSwapV3PoolReader",
    ),
    get_pool_selector=V3_GET_POOL_SELECTOR,
)

POOL_DATA_SPEC = PoolDataSpec(
    protocol="sushiswap_v3",
    reference_kind=PoolReferenceKind.EVM_CONTRACT,
    bindings={
        PoolDataFacet.METADATA: PoolDataSource.GATEWAY_POOL_STATE,
        PoolDataFacet.BALANCES: PoolDataSource.GATEWAY_POOL_STATE,
        PoolDataFacet.SPOT_PRICE: PoolDataSource.LIVE_PRICE_READER,
        PoolDataFacet.LIQUIDITY: PoolDataSource.LIVE_PRICE_READER,
        PoolDataFacet.HISTORICAL_STATE: PoolDataSource.GATEWAY_POOL_STATE,
        PoolDataFacet.TWAP: PoolDataSource.GATEWAY_TWAP,
        PoolDataFacet.TICK_LIQUIDITY: PoolDataSource.LIVE_PRICE_READER,
    },
    unsupported={},
    price_reader=POOL_READER_SPEC,
)

__all__ = ["POOL_DATA_SPEC", "POOL_READER_SPEC"]
