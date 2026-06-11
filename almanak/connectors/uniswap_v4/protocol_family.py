"""Uniswap V4 protocol-family membership (VIB-4583).

Declares this connector's contribution to the ``UNIV4_LP_GROUPING`` family
(the framework's ``UNIV4_LP_GROUPING_PROTOCOLS`` set — singleton-PoolManager
concentrated-liquidity grouping keyed by ``chain:pool_id``, policy
``univ4_lp@v1``). The migration backfill and the runner's registry-mode
dispatch key their ``protocol in _UNIV4_LP_PROTOCOLS`` branch on the
registry-derived union; no framework module imports this connector directly.

Kept distinct from ``UNIV3_LP_GROUPING`` so the V3 and V4 grouping-policy
versions evolve independently (a V4 grouping-rule change must never silently
re-baseline V3 rows).

Strategy-side only — gateway code does not consult this module.
"""

from __future__ import annotations

from almanak.connectors._strategy_base.protocol_family_registry import (
    ProtocolFamily,
    ProtocolFamilySpec,
)

PROTOCOL_FAMILY = ProtocolFamilySpec(families={ProtocolFamily.UNIV4_LP_GROUPING: frozenset({"uniswap_v4"})})


__all__ = ["PROTOCOL_FAMILY"]
