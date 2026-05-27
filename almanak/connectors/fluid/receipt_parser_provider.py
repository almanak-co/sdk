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
        return frozenset({"fluid"})

    def receipt_parser_class(self, key: str) -> type:
        from almanak.connectors.fluid.receipt_parser import FluidReceiptParser

        return FluidReceiptParser


__all__ = ["FluidReceiptParserConnector"]
