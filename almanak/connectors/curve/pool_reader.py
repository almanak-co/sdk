"""Connector-owned pool reader spec for Curve.

Curve pools are NOT fee-tier-keyed and do not speak the v3 slot0() ABI.  This
spec therefore binds ``CurvePoolReader`` directly; framework dispatch depends
on the family-neutral reader interface, not a family-name switch.

Pool-address reads remain supported, but pair-to-pool resolution is deliberately
empty. Curve's MetaRegistry lookup is live and cannot be represented by the
framework's static ``known_pools`` table. Callers that know an admitted exact
pool may read it directly; pair-only callers receive an honest miss.

Curve DOES have a pairwise ``factory.getPool``-style resolver — the
MetaRegistry's ``find_pool_for_coins`` (used by the compile lane's dynamic
pair resolution, ``pair_resolver.py`` / VIB-5716) — but it is a registry
contract resolved live via the AddressProvider, not a static factory
address, so it does not fit the ``factory_addresses`` slot; wiring
MetaRegistry-backed pair resolution into this reader lane is a documented
follow-up (VIB-5716 scope note). Until then both ``factory_addresses`` and
``known_pools`` are empty. ``candidate_pool_keys=(0,)``: with
no fee-tier discriminator, best-pool resolution is a single total lookup,
and sweeps can never multi-count one pool under several tiers.

Wrapped-lending pools remain outside the exact-pool resolver's supported set.
"""

from __future__ import annotations

from almanak.connectors._connector import ImportRef
from almanak.connectors._strategy_base.curve_pool_abi import CURVE_POOL_KEY
from almanak.connectors._strategy_base.pool_data import (
    PoolDataFacet,
    PoolDataSource,
    PoolDataSpec,
    PoolReferenceKind,
)
from almanak.connectors._strategy_base.pool_reader import PoolDiscriminatorKind, PoolReaderSpec

POOL_READER_SPEC = PoolReaderSpec(
    protocol="curve",
    reader_kind="curve_pool",
    reader=ImportRef(
        module="almanak.framework.data.pools.reader",
        attribute="CurvePoolReader",
    ),
    # Curve's pairwise resolver (MetaRegistry find_pool_for_coins) is a live
    # registry lookup, not a static factory address — so this slot stays empty
    # and pair-only reader resolution fails closed (see module docstring).
    factory_addresses={},
    known_pools={},
    # Single total sweep: Curve has no fee-tier discriminator.
    candidate_pool_keys=(CURVE_POOL_KEY,),
    discriminator_kind=PoolDiscriminatorKind.NONE,
)

_UNSUPPORTED = {
    PoolDataFacet.METADATA: "Curve metadata is consumed internally by the live reader but has no generic adapter.",
    PoolDataFacet.BALANCES: "Curve's live reader does not expose arbitrary N-asset pool balances.",
    PoolDataFacet.HISTORICAL_STATE: "Curve archive state has not been migrated to the generic N-asset transport.",
    PoolDataFacet.TWAP: "Curve pools do not expose one uniform native TWAP primitive.",
    PoolDataFacet.TICK_LIQUIDITY: "Curve StableSwap has no concentrated-liquidity tick model.",
}

POOL_DATA_SPEC = PoolDataSpec(
    protocol="curve",
    reference_kind=PoolReferenceKind.EVM_CONTRACT,
    bindings={
        PoolDataFacet.SPOT_PRICE: PoolDataSource.LIVE_PRICE_READER,
        PoolDataFacet.LIQUIDITY: PoolDataSource.LIVE_PRICE_READER,
    },
    unsupported=_UNSUPPORTED,
    price_reader=POOL_READER_SPEC,
)

__all__ = ["CURVE_POOL_KEY", "POOL_DATA_SPEC", "POOL_READER_SPEC"]
