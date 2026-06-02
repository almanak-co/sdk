"""Uniswap V3 LP-classification constants.

VIB-4864 (W2-followup): the migration backfill keys a compile-time
``protocol in _UNIV3_LP_PROTOCOLS`` branch on the set of protocol slugs
that implement the Uniswap-V3-shape LP grouping policy (``univ3_lp@v1`` —
NFT-position-manager-keyed concentrated liquidity). Per-connector
membership now lives next to the connector it describes (mirroring the
VIB-4872 ``AAVE_V3_FAMILY_PROTOCOLS`` pattern);
the union is derived in
``almanak/framework/intents/compiler_constants.py``.

The shape is intentionally trivial — a frozenset of protocol slugs — to
match the legacy lookup site (single ``in`` membership test).
"""

from __future__ import annotations

UNIV3_LP_GROUPING_PROTOCOLS: frozenset[str] = frozenset({"uniswap_v3"})


__all__ = ["UNIV3_LP_GROUPING_PROTOCOLS"]
