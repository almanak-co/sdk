"""Strategy-side receipt-parser connector for Fluid DEX LP (VIB-5032).

Leaf-owned provider for the ``fluid_dex_lp`` (SmartLending fungible-LP) surface;
the parser implementation lives in the shared ``_fluid_core`` foundation.
"""

from __future__ import annotations

from typing import ClassVar

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.receipt_parser_registry import (
    ReceiptParserCapability,
    ReceiptParserConnector,
)


class FluidDexLpReceiptParserConnector(ReceiptParserConnector, ReceiptParserCapability):
    """DEX LP (SmartLending) receipts (VIB-5032) — declared on the
    ``fluid_dex_lp`` manifest so LP_OPEN/LP_CLOSE receipts compiled under
    ``protocol="fluid_dex_lp"`` route to the fungible-LP parser (wrapper share
    mint/burn + token-leg Transfers), never the DEX/fToken or vault parser."""

    protocol: ClassVar[ProtocolName] = ProtocolName("fluid_dex_lp")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LP

    def receipt_parser_keys(self) -> frozenset[str]:
        return frozenset({"fluid_dex_lp"})

    def receipt_parser_class(self, key: str) -> type:
        from almanak.connectors._fluid_core.dex_lp_receipt_parser import FluidDexLpReceiptParser

        return FluidDexLpReceiptParser


__all__ = ["FluidDexLpReceiptParserConnector"]
