"""Strategy-side receipt-parser connector for Gimo (VIB-4854 / W2)."""

from __future__ import annotations

from typing import ClassVar

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.receipt_parser_registry import (
    ReceiptParserCapability,
    ReceiptParserConnector,
)


class GimoReceiptParserConnector(ReceiptParserConnector, ReceiptParserCapability):
    protocol: ClassVar[ProtocolName] = ProtocolName("gimo")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LENDING

    def receipt_parser_keys(self) -> frozenset[str]:
        return frozenset({"gimo"})

    def receipt_parser_class(self, key: str) -> type:
        from almanak.connectors.gimo.receipt_parser import GimoReceiptParser

        return GimoReceiptParser


__all__ = ["GimoReceiptParserConnector"]
