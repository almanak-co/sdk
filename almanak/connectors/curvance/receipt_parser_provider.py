"""Strategy-side receipt-parser connector for Curvance (VIB-4854 / W2)."""

from __future__ import annotations

from typing import ClassVar

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.receipt_parser_registry import (
    ReceiptParserCapability,
    ReceiptParserConnector,
)


class CurvanceReceiptParserConnector(ReceiptParserConnector, ReceiptParserCapability):
    protocol: ClassVar[ProtocolName] = ProtocolName("curvance")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LENDING

    def receipt_parser_keys(self) -> frozenset[str]:
        return frozenset({"curvance"})

    def receipt_parser_class(self, key: str) -> type:
        from almanak.connectors.curvance.receipt_parser import (
            CurvanceReceiptParser,
        )

        return CurvanceReceiptParser


__all__ = ["CurvanceReceiptParserConnector"]
