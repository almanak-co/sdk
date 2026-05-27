"""Strategy-side receipt-parser connector for Lido (VIB-4854 / W2)."""

from __future__ import annotations

from typing import ClassVar

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.receipt_parser_registry import (
    ReceiptParserCapability,
    ReceiptParserConnector,
)


class LidoReceiptParserConnector(ReceiptParserConnector, ReceiptParserCapability):
    protocol: ClassVar[ProtocolName] = ProtocolName("lido")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LENDING

    def receipt_parser_keys(self) -> frozenset[str]:
        return frozenset({"lido"})

    def receipt_parser_class(self, key: str) -> type:
        from almanak.connectors.lido.receipt_parser import LidoReceiptParser

        return LidoReceiptParser


__all__ = ["LidoReceiptParserConnector"]
