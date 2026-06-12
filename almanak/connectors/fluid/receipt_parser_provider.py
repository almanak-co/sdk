"""Strategy-side receipt-parser connector for Fluid (VIB-4854 / W2)."""

from __future__ import annotations

from typing import ClassVar

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.receipt_parser_registry import (
    ReceiptParserCapability,
    ReceiptParserConnector,
)


class FluidReceiptParserConnector(ReceiptParserConnector, ReceiptParserCapability):
    protocol: ClassVar[ProtocolName] = ProtocolName("fluid")
    # Fluid is an LP/DEX protocol: ``FluidReceiptParser`` extracts NFT
    # position IDs, LP open/close amounts (``LogOperate``), and swap
    # amounts. The earlier ``LENDING`` value was wrong — flagged by
    # CodeRabbit on PR 2457 and corrected here. The receipt-parser
    # ``kind`` is read by the dashboard classifier only and does not
    # affect routing.
    kind: ClassVar[ProtocolKind] = ProtocolKind.LP

    def receipt_parser_keys(self) -> frozenset[str]:
        # "fluid_lending" is the platform-spec alias (VIB-5030) — same
        # parser; mirrors the "morpho" -> morpho_blue alias precedent so a
        # receipt enriched under the alias never misses its parser.
        return frozenset({"fluid", "fluid_lending"})

    def receipt_parser_class(self, key: str) -> type:
        from almanak.connectors.fluid.receipt_parser import FluidReceiptParser

        return FluidReceiptParser


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
        from almanak.connectors.fluid.receipt_parser import FluidVaultReceiptParser

        return FluidVaultReceiptParser


__all__ = ["FluidReceiptParserConnector", "FluidVaultReceiptParserConnector"]
