"""Uniswap V3 protocol-family membership.

VIB-4928 (PR-3b): declares this connector's contribution to the
``UNIV3_LP_GROUPING`` family (the framework's ``UNIV3_LP_GROUPING_PROTOCOLS``
set — NFT-position-manager-keyed concentrated-liquidity grouping, policy
``univ3_lp@v1``). The migration backfill keys its compile-time
``protocol in _UNIV3_LP_PROTOCOLS`` branch on the registry-derived union; no
framework module imports this connector directly.

Strategy-side only — gateway code does not consult this module.
"""

from __future__ import annotations

from almanak.connectors._strategy_base.protocol_family_registry import (
    ProtocolFamily,
    ProtocolFamilySpec,
)

PROTOCOL_FAMILY = ProtocolFamilySpec(families={ProtocolFamily.UNIV3_LP_GROUPING: frozenset({"uniswap_v3"})})


__all__ = ["PROTOCOL_FAMILY"]
