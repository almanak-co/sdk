"""Strategy-side receipt-parser connector for Compound V3 (VIB-4854 / W2)."""

from __future__ import annotations

from typing import ClassVar

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.receipt_parser_registry import (
    ReceiptParserCapability,
    ReceiptParserConnector,
)


class CompoundV3ReceiptParserConnector(ReceiptParserConnector, ReceiptParserCapability):
    protocol: ClassVar[ProtocolName] = ProtocolName("compound_v3")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LENDING

    def receipt_parser_keys(self) -> frozenset[str]:
        return frozenset({"compound_v3"})

    def receipt_parser_class(self, key: str) -> type:
        from almanak.connectors.compound_v3.receipt_parser import (
            CompoundV3ReceiptParser,
        )

        return CompoundV3ReceiptParser


__all__ = ["CompoundV3ReceiptParserConnector"]
