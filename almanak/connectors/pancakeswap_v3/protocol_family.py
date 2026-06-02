"""PancakeSwap V3 protocol-family membership.

VIB-4928 (PR-3b): PancakeSwap V3 is a Uniswap V3 fork at the LP layer — same
NFT-position-manager-keyed concentrated-liquidity grouping policy
(``univ3_lp@v1``), but ships its own NonfungiblePositionManager at a different
address than canonical UniV3 on the same chain. Declares this connector's
contribution to the ``UNIV3_LP_GROUPING`` family (the framework's
``UNIV3_LP_GROUPING_PROTOCOLS`` set); the union is derived from the registry,
no framework module imports this connector directly.

Strategy-side only — gateway code does not consult this module.
"""

from __future__ import annotations

from almanak.connectors._strategy_base.protocol_family_registry import (
    ProtocolFamily,
    ProtocolFamilySpec,
)

PROTOCOL_FAMILY = ProtocolFamilySpec(families={ProtocolFamily.UNIV3_LP_GROUPING: frozenset({"pancakeswap_v3"})})


__all__ = ["PROTOCOL_FAMILY"]
