"""Strategy-side receipt-parser connector for Benqi (VIB-4854 / W2)."""

from __future__ import annotations

from typing import ClassVar

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.receipt_parser_registry import (
    ReceiptParserCapability,
    ReceiptParserConnector,
)


class BenqiReceiptParserConnector(ReceiptParserConnector, ReceiptParserCapability):
    protocol: ClassVar[ProtocolName] = ProtocolName("benqi")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LENDING

    def receipt_parser_keys(self) -> frozenset[str]:
        return frozenset({"benqi"})

    def receipt_parser_class(self, key: str) -> type:
        from almanak.connectors.benqi.receipt_parser import BenqiReceiptParser

        return BenqiReceiptParser


__all__ = ["BenqiReceiptParserConnector"]
