"""Strategy-side receipt-parser connector for Fluid vault (VIB-5031).

Leaf-owned provider for the ``fluid_vault`` (NFT-CDP borrow) surface; the parser
implementation lives in the shared ``_fluid_core`` foundation.
"""

from __future__ import annotations

from typing import ClassVar

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.receipt_parser_registry import (
    ReceiptParserCapability,
    ReceiptParserConnector,
)


class FluidVaultReceiptParserConnector(ReceiptParserConnector, ReceiptParserCapability):
    """Vault NFT-CDP receipts (VIB-5031) — declared on the ``fluid_vault``
    manifest so the Result Enrichment contract (blueprints 02/19) routes
    SUPPLY/BORROW/REPAY/WITHDRAW/DELEVERAGE receipts compiled under
    ``protocol="fluid_vault"`` to the vault parser (factory-gated nftId +
    signed LogOperate deltas), never the DEX/fToken parser."""

    protocol: ClassVar[ProtocolName] = ProtocolName("fluid_vault")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LENDING

    def receipt_parser_keys(self) -> frozenset[str]:
        # No aliases: nothing aliases to fluid_vault (fluid_lending stays
        # on the fToken surface — ADR r2 Q0).
        return frozenset({"fluid_vault"})

    def receipt_parser_class(self, key: str) -> type:
        from almanak.connectors._fluid_core.receipt_parser import FluidVaultReceiptParser

        return FluidVaultReceiptParser


__all__ = ["FluidVaultReceiptParserConnector"]
