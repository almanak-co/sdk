"""SushiSwap V3 protocol-family membership.

VIB-4928 (PR-3b): SushiSwap V3 is a Uniswap V3 fork at the LP layer — it uses
the same NFT-position-manager-keyed concentrated-liquidity grouping policy
(``univ3_lp@v1``). Declares this connector's contribution to the
``UNIV3_LP_GROUPING`` family (the framework's ``UNIV3_LP_GROUPING_PROTOCOLS``
set); the union is derived from the registry, no framework module imports this
connector directly.

Strategy-side only — gateway code does not consult this module.
"""

from __future__ import annotations

from almanak.connectors._strategy_base.protocol_family_registry import (
    ProtocolFamily,
    ProtocolFamilySpec,
)

PROTOCOL_FAMILY = ProtocolFamilySpec(families={ProtocolFamily.UNIV3_LP_GROUPING: frozenset({"sushiswap_v3"})})


__all__ = ["PROTOCOL_FAMILY"]
