"""SushiSwap V3 LP-classification constants.

VIB-4864 (W2-followup): SushiSwap V3 is a Uniswap V3 fork at the LP layer
— it uses the same NFT-position-manager-keyed concentrated-liquidity
grouping policy (``univ3_lp@v1``). The migration backfill keys a
compile-time ``protocol in _UNIV3_LP_PROTOCOLS`` branch on this set; the
union of every UniV3-shape connector's contribution is derived in
``almanak/framework/intents/compiler_constants.py`` (mirroring the
VIB-4872 ``AAVE_V2_FORK_PROTOCOLS`` pattern).
"""

from __future__ import annotations

UNIV3_LP_GROUPING_PROTOCOLS: frozenset[str] = frozenset({"sushiswap_v3"})


__all__ = ["UNIV3_LP_GROUPING_PROTOCOLS"]
