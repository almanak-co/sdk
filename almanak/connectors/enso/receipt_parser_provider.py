"""Strategy-side receipt-parser connector for Enso (VIB-4854 / W2)."""

from __future__ import annotations

from typing import ClassVar

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.receipt_parser_registry import (
    ReceiptParserCapability,
    ReceiptParserConnector,
)


class EnsoReceiptParserConnector(ReceiptParserConnector, ReceiptParserCapability):
    protocol: ClassVar[ProtocolName] = ProtocolName("enso")
    kind: ClassVar[ProtocolKind] = ProtocolKind.SWAP

    def receipt_parser_keys(self) -> frozenset[str]:
        return frozenset({"enso"})

    def receipt_parser_class(self, key: str) -> type:
        from almanak.connectors.enso.receipt_parser import EnsoReceiptParser

        return EnsoReceiptParser


__all__ = ["EnsoReceiptParserConnector"]
