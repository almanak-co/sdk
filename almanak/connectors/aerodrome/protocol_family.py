"""Aerodrome / Velodrome Slipstream protocol-family membership.

VIB-4928 (PR-3b): Slipstream is Aerodrome's (Base) / Velodrome's (Optimism)
concentrated-liquidity AMM — a Uniswap V3 fork at the LP layer using the same
NFT-position-manager-keyed grouping policy (``univ3_lp@v1``), with its own
NonfungiblePositionManager (``cl_nft``). Both slugs ride the Aerodrome connector
(Velodrome Slipstream reuses the Aerodrome parser / address tables), so both are
declared here as contributions to the ``UNIV3_LP_GROUPING`` family (the
framework's ``UNIV3_LP_GROUPING_PROTOCOLS`` set). The union is derived from the
registry; no framework module imports this connector directly.

Strategy-side only — gateway code does not consult this module.
"""

from __future__ import annotations

from almanak.connectors._strategy_base.protocol_family_registry import (
    ProtocolFamily,
    ProtocolFamilySpec,
)

PROTOCOL_FAMILY = ProtocolFamilySpec(
    families={
        ProtocolFamily.UNIV3_LP_GROUPING: frozenset({"aerodrome_slipstream", "velodrome_slipstream"}),
        # Plan 027 Step 3: aerodrome_slipstream uses tick spacing as the fee-tier
        # field (not bps), so the ax CLI renders ``tick_spacing=<N>`` instead of
        # ``<N/10000:.2f>%``. Only aerodrome_slipstream is in this family --
        # strict parity with the old ``if protocol == "aerodrome_slipstream"`` guard
        # (velodrome_slipstream was NOT covered by the old literal).
        ProtocolFamily.TICK_SPACING_FEE_DISPLAY: frozenset({"aerodrome_slipstream"}),
    }
)


__all__ = ["PROTOCOL_FAMILY"]
