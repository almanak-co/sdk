"""Connector-owned pool reader spec for Aerodrome Slipstream."""

from __future__ import annotations

from almanak.connectors._connector import ImportRef
from almanak.connectors._strategy_base.pool_data import (
    PoolDataFacet,
    PoolDataSource,
    PoolDataSpec,
    PoolReferenceKind,
    unsupported_pool_data_spec,
)
from almanak.connectors._strategy_base.pool_reader import PoolDiscriminatorKind, PoolReaderSpec

from .addresses import AERODROME

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
        ): "0x6cDcb1C4A4D1C3C6d054b27AC5B77e89eAFb971d",
    },
}

SLIPSTREAM_POOL_READER_SPEC = PoolReaderSpec(
    protocol="aerodrome_slipstream",
    factory_addresses={chain: addrs["cl_factory"] for chain, addrs in AERODROME.items() if "cl_factory" in addrs},
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
)

AERODROME_CLASSIC_POOL_DATA_SPEC = unsupported_pool_data_spec(
    protocol="aerodrome",
    reference_kind=PoolReferenceKind.EVM_CONTRACT,
    reason=(
        "Aerodrome Classic is a Solidly pool family; its generic N-asset metadata, "
        "state, and price adapters have not been wired to the pool-data interface."
    ),
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
    "POOL_DATA_SPECS",
    "POOL_READER_SPEC",
    "SLIPSTREAM_POOL_DATA_SPEC",
    "SLIPSTREAM_POOL_READER_SPEC",
]
