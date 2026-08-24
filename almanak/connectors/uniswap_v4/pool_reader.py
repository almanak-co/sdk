"""Connector-owned pool reader spec for Uniswap V4.

V4 has no per-pool contracts — pool state lives in the PoolManager singleton
and is read through the StateView periphery, keyed by ``bytes32 PoolId``.
The spec therefore binds ``UniswapV4PoolReader`` directly.  The framework
dispatches through the family-neutral reader interface and does not infer an
implementation from a V4 family string.

``factory_addresses`` carries the per-chain **StateView** address — the
contract every read goes through and the honest chain gate (V4 is readable
exactly where StateView is deployed). There is no ``factory.getPool``:
resolution derives the PoolId offline from the pair + fee tier (canonical
tick spacing, no hooks) via the connector's own PoolKey hashing, then
verifies initialization on-chain. ``known_pools`` is empty — V4 "pools" are
synthetic ids, not curated contract addresses.

``candidate_pool_keys`` are the canonical v3 fee tiers: vanilla V4 pools are
fee-tier-keyed just like v3 (each tier maps to its default tick spacing).
Hooked pools and nonstandard spacings are out of scope for pair resolution —
they need an explicit PoolKey (documented follow-up).
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
from almanak.connectors.uniswap_v4.addresses import UNISWAP_V4

POOL_READER_SPEC = PoolReaderSpec(
    protocol="uniswap_v4",
    reader_kind="uniswap_v4_stateview",
    reader=ImportRef(
        module="almanak.framework.data.pools.reader",
        attribute="UniswapV4PoolReader",
    ),
    factory_addresses={chain: addrs["state_view"] for chain, addrs in UNISWAP_V4.items() if "state_view" in addrs},
    candidate_pool_keys=(100, 500, 3000, 10000),
    pair_resolver=ImportRef(
        module="almanak.connectors.uniswap_v4.pair_resolver",
        attribute="resolve_pair_payload",
    ),
    identity_probe=ImportRef(
        module="almanak.connectors.uniswap_v4.pool_identity",
        attribute="identify_pool_payload",
    ),
)

_UNSUPPORTED = {
    PoolDataFacet.METADATA: "V4 metadata has no generic public adapter bound to the PoolId contract.",
    PoolDataFacet.BALANCES: "V4 PoolIds have no per-pool contract balances; assets are held by PoolManager.",
    PoolDataFacet.HISTORICAL_STATE: "V4 StateView archive transport is not implemented.",
    PoolDataFacet.TWAP: "V4 TWAP availability is hook/oracle dependent and cannot be assumed from a PoolId.",
    PoolDataFacet.TICK_LIQUIDITY: "V4 PoolIds require a StateView tick adapter and cannot use the V3 contract walker.",
}

POOL_DATA_SPEC = PoolDataSpec(
    protocol="uniswap_v4",
    reference_kind=PoolReferenceKind.EVM_POOL_ID,
    bindings={
        PoolDataFacet.SPOT_PRICE: PoolDataSource.LIVE_PRICE_READER,
        PoolDataFacet.LIQUIDITY: PoolDataSource.LIVE_PRICE_READER,
    },
    unsupported=_UNSUPPORTED,
    price_reader=POOL_READER_SPEC,
)

__all__ = ["POOL_DATA_SPEC", "POOL_READER_SPEC"]
