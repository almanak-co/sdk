"""Aerodrome / Velodrome Slipstream LP-classification constants.

VIB-4864 (W2-followup): Slipstream is Aerodrome's (Base) / Velodrome's
(Optimism) concentrated-liquidity AMM — a Uniswap V3 fork at the LP layer
that uses the same NFT-position-manager-keyed grouping policy
(``univ3_lp@v1``), with its own NonfungiblePositionManager (``cl_nft``) at
a different address than canonical UniV3 on the same chain. Both slugs
ride the Aerodrome connector (Velodrome Slipstream reuses the Aerodrome
parser / address tables), so both are declared here.

The migration backfill keys a compile-time
``protocol in _UNIV3_LP_PROTOCOLS`` branch on this set; the union of every
UniV3-shape connector's contribution is derived in
``almanak/framework/intents/compiler_constants.py`` (mirroring the
VIB-4872 ``AAVE_V2_FORK_PROTOCOLS`` pattern).
"""

from __future__ import annotations

UNIV3_LP_GROUPING_PROTOCOLS: frozenset[str] = frozenset({"aerodrome_slipstream", "velodrome_slipstream"})


__all__ = ["UNIV3_LP_GROUPING_PROTOCOLS"]
