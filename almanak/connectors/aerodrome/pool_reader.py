"""Connector-owned pool reader spec for Aerodrome Slipstream."""

from __future__ import annotations

from almanak.connectors._connector import ImportRef
from almanak.connectors._strategy_base.pool_data import (
    PoolDataFacet,
    PoolDataSource,
    PoolDataSpec,
    PoolReferenceKind,
)
from almanak.connectors._strategy_base.pool_reader import PoolDiscriminatorKind, PoolReaderSpec

from .addresses import AERODROME, SLIPSTREAM_LP_DEPLOYMENTS

# Cached (token0, token1, tickSpacing) -> pool hints. Each key below is currently
# owned by exactly one reviewed factory generation (the legacy one). The shared
# reader still probes every reviewed generation before returning a hint because
# another factory can create the same key later.
_KNOWN_POOLS: dict[str, dict[tuple[str, str, int], str]] = {
    "base": {
        (
            "0x4200000000000000000000000000000000000006",
            "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",
            100,
        ): "0xb2cc224c1c9feE385f8ad6a55b4d94E92359DC59",
        (
            "0x4200000000000000000000000000000000000006",
            "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",
            200,
        ): "0x148bC43946a902258916E580B0e6D92AAa74746F",
    },
}

SLIPSTREAM_POOL_READER_SPEC = PoolReaderSpec(
    protocol="aerodrome_slipstream",
    factory_addresses={},
    factory_generations={
        chain: tuple(deployment.factory for deployment in deployments)
        for chain, deployments in SLIPSTREAM_LP_DEPLOYMENTS.items()
    },
    reader=ImportRef(
        module="almanak.framework.data.pools.reader",
        attribute="AerodromePoolReader",
    ),
    known_pools=_KNOWN_POOLS,
    get_pool_selector="0x28af8d0b",
    # Slipstream keys pools by TICK SPACING, not Uniswap fee tier. Snapshot of
    # the Base CL factory's ``tickSpacings()`` (governance-extensible — keep
    # in sync if it grows).
    candidate_pool_keys=(1, 10, 50, 100, 200, 2000),
    discriminator_kind=PoolDiscriminatorKind.TICK_SPACING,
    identity_probe=ImportRef(
        module="almanak.connectors._strategy_base.pool_identity_base",
        attribute="identify_clamm_pool",
    ),
)

CLASSIC_POOL_READER_SPEC = PoolReaderSpec(
    protocol="aerodrome",
    factory_addresses={chain: addrs["factory"] for chain, addrs in AERODROME.items() if "factory" in addrs},
    reader=ImportRef(
        module="almanak.connectors.aerodrome.solidly_reader",
        attribute="SolidlyPoolReader",
    ),
    reader_kind="solidly_reserves",
    known_pools={},
    # Solidly getPool(address,address,bool): the stable flag is the pool key.
    get_pool_selector="0x79bc57d5",
    candidate_pool_keys=(0, 1),
    discriminator_kind=PoolDiscriminatorKind.STABLE_FLAG,
    pair_resolver=ImportRef(
        module="almanak.connectors.aerodrome.pair_resolver",
        attribute="resolve_pair_payload",
    ),
    identity_probe=ImportRef(
        module="almanak.connectors.aerodrome.pool_identity",
        attribute="identify_pool_payload",
    ),
)

AERODROME_CLASSIC_POOL_DATA_SPEC = PoolDataSpec(
    protocol="aerodrome",
    reference_kind=PoolReferenceKind.EVM_CONTRACT,
    bindings={
        PoolDataFacet.SPOT_PRICE: PoolDataSource.LIVE_PRICE_READER,
        PoolDataFacet.LIQUIDITY: PoolDataSource.LIVE_PRICE_READER,
    },
    unsupported={
        PoolDataFacet.METADATA: "Solidly generic N-asset metadata is not wired to the pool-data interface.",
        PoolDataFacet.BALANCES: "Solidly generic pool balances are not wired to the pool-data interface.",
        PoolDataFacet.HISTORICAL_STATE: "Solidly archive state transport is not implemented.",
        PoolDataFacet.TWAP: "Solidly native TWAP observations are not wired.",
        PoolDataFacet.TICK_LIQUIDITY: "Solidly pools have no concentrated-liquidity tick model.",
    },
    price_reader=CLASSIC_POOL_READER_SPEC,
)

SLIPSTREAM_POOL_DATA_SPEC = PoolDataSpec(
    protocol="aerodrome_slipstream",
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
    price_reader=SLIPSTREAM_POOL_READER_SPEC,
)

POOL_DATA_SPECS = (AERODROME_CLASSIC_POOL_DATA_SPEC, SLIPSTREAM_POOL_DATA_SPEC)

# Deprecated import alias retained for external code that imported the live
# Slipstream reader before the two Aerodrome pool families were separated.
POOL_READER_SPEC = SLIPSTREAM_POOL_READER_SPEC

__all__ = [
    "AERODROME_CLASSIC_POOL_DATA_SPEC",
    "CLASSIC_POOL_READER_SPEC",
    "POOL_DATA_SPECS",
    "POOL_READER_SPEC",
    "SLIPSTREAM_POOL_DATA_SPEC",
    "SLIPSTREAM_POOL_READER_SPEC",
]
