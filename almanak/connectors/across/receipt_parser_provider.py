"""Strategy-side receipt-parser connector for Across (VIB-4854 / W2)."""

from __future__ import annotations

from typing import ClassVar

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.receipt_parser_registry import (
    ReceiptParserCapability,
    ReceiptParserConnector,
)


class AcrossReceiptParserConnector(ReceiptParserConnector, ReceiptParserCapability):
    protocol: ClassVar[ProtocolName] = ProtocolName("across")
    kind: ClassVar[ProtocolKind] = ProtocolKind.BRIDGE

    def receipt_parser_keys(self) -> frozenset[str]:
        return frozenset({"across"})

    def receipt_parser_class(self, key: str) -> type:
        from almanak.connectors.across.receipt_parser import (
            AcrossReceiptParser,
        )

        return AcrossReceiptParser


__all__ = ["AcrossReceiptParserConnector"]
