"""Strategy-side receipt-parser connector for Hyperliquid (CoreWriter)."""

from __future__ import annotations

from typing import ClassVar

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.receipt_parser_registry import (
    ReceiptParserCapability,
    ReceiptParserConnector,
)


class HyperliquidReceiptParserConnector(ReceiptParserConnector, ReceiptParserCapability):
    protocol: ClassVar[ProtocolName] = ProtocolName("hyperliquid")
    kind: ClassVar[ProtocolKind] = ProtocolKind.PERP

    def receipt_parser_keys(self) -> frozenset[str]:
        return frozenset({"hyperliquid"})

    def receipt_parser_class(self, key: str) -> type:
        from almanak.connectors.hyperliquid.receipt_parser import HyperliquidReceiptParser

        return HyperliquidReceiptParser


__all__ = ["HyperliquidReceiptParserConnector"]
