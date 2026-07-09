"""Connector-owned pool reader spec for Curve.

Curve pools are NOT fee-tier-keyed and do not speak the v3 slot0() ABI, so
this spec declares ``reader_kind="curve_pool"`` — the framework dispatches it
onto ``CurvePoolReader`` (get_dy/coins-based) instead of the slot0 family.

Pool resolution is CURATED-ONLY: the pair table below is derived from the
adapter's hand-verified ``CURVE_POOLS`` registry (single source of truth —
never a parallel literal). Each curated pool contributes exactly ONE pair key,
its leading ``(coins[0], coins[1])`` pair, because ``read_pool_price`` prices
the pool's first two native coins; mapping a deeper pair (e.g. WBTC/WETH in
tricrypto) onto a leading-pair read would return a price for the WRONG pair.
Pairs at coin index >= 2 therefore resolve to ``None`` (an honest miss, never
a wrong price); a pair-aware (i, j) read API is a documented follow-up.

There is no pairwise ``factory.getPool``-style resolver for Curve, so
``factory_addresses`` is empty and chain support is gated by curated-pool
presence — exactly the chains where resolution can actually succeed.
``candidate_pool_keys=(0,)``: with no fee-tier discriminator, best-pool
resolution is a single total lookup, and sweeps can never multi-count one
pool under several tiers.

Note on the Polygon aave-type 3pool: its curated ``coin_addresses`` are the
UNDERLYING tokens (DAI/USDC.e/...), while the pool's native ``coins(i)`` are
the 1:1-rebasing aTokens. The pair key uses the curated underlying addresses;
the reader prices the pool's native coins live. aTokens mirror their
underlying's decimals and peg 1:1, so the quoted rate is the underlying rate.
"""

from __future__ import annotations

import logging

from almanak.connectors._strategy_base.curve_pool_abi import CURVE_POOL_KEY
from almanak.connectors._strategy_base.pool_reader import PoolReaderSpec
from almanak.connectors.curve.adapter import CURVE_POOLS

logger = logging.getLogger(__name__)


def _build_known_pools() -> dict[str, dict[tuple[str, str, int], str]]:
    """Derive the (leading-pair -> pool) table from the curated registry."""
    known: dict[str, dict[tuple[str, str, int], str]] = {}
    for chain, pools in CURVE_POOLS.items():
        chain_map: dict[tuple[str, str, int], str] = {}
        for name, pool in pools.items():
            coin_addresses = pool["coin_addresses"]
            if len(coin_addresses) < 2:
                # Deliberately LOUD at import: a curated pool with fewer than
                # two coins is corrupt curation data, and skipping it silently
                # would make its pair unreadable with no signal. CI catches
                # this on any import of the connector.
                raise ValueError(f"curated Curve pool {name!r} on {chain} declares fewer than 2 coin_addresses")
            addr_a, addr_b = (a.lower() for a in coin_addresses[:2])
            key = (min(addr_a, addr_b), max(addr_a, addr_b), CURVE_POOL_KEY)
            if key in chain_map:
                # Two curated pools sharing a leading pair would make the
                # mapping ambiguous; keep the first (canonical ordering in
                # CURVE_POOLS) and surface the conflict for curation.
                logger.warning(
                    "Curve curated pools share leading pair %s on %s; keeping %s, ignoring %s",
                    key[:2],
                    chain,
                    chain_map[key],
                    name,
                )
                continue
            chain_map[key] = pool["address"]
        known[chain] = chain_map
    return known


POOL_READER_SPEC = PoolReaderSpec(
    protocol="curve",
    reader_kind="curve_pool",
    # No pairwise pool factory exists for Curve — resolution is curated-only,
    # and chain gating comes from curated-pool presence in ``known_pools``.
    factory_addresses={},
    known_pools=_build_known_pools(),
    # Single total sweep: Curve has no fee-tier discriminator.
    candidate_pool_keys=(CURVE_POOL_KEY,),
)

__all__ = ["CURVE_POOL_KEY", "POOL_READER_SPEC"]
