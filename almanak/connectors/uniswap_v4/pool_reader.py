"""Connector-owned pool reader spec for Uniswap V4.

V4 has no per-pool contracts — pool state lives in the PoolManager singleton
and is read through the StateView periphery, keyed by ``bytes32 PoolId``.
The spec therefore declares ``reader_kind="uniswap_v4_stateview"`` and the
framework dispatches it onto ``UniswapV4PoolReader``.

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

from almanak.connectors._strategy_base.pool_reader import PoolReaderSpec
from almanak.connectors.uniswap_v4.addresses import UNISWAP_V4

POOL_READER_SPEC = PoolReaderSpec(
    protocol="uniswap_v4",
    reader_kind="uniswap_v4_stateview",
    factory_addresses={chain: addrs["state_view"] for chain, addrs in UNISWAP_V4.items() if "state_view" in addrs},
    candidate_pool_keys=(100, 500, 3000, 10000),
)

__all__ = ["POOL_READER_SPEC"]
