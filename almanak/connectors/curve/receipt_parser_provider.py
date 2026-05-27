"""Strategy-side receipt-parser connector for Curve (VIB-4854 / W2)."""

from __future__ import annotations

from typing import ClassVar

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.receipt_parser_registry import (
    ReceiptParserCapability,
    ReceiptParserConnector,
)


class CurveReceiptParserConnector(ReceiptParserConnector, ReceiptParserCapability):
    protocol: ClassVar[ProtocolName] = ProtocolName("curve")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LP

    def receipt_parser_keys(self) -> frozenset[str]:
        return frozenset({"curve"})

    def receipt_parser_class(self, key: str) -> type:
        from almanak.connectors.curve.receipt_parser import CurveReceiptParser

        return CurveReceiptParser


__all__ = ["CurveReceiptParserConnector"]
