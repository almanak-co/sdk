"""Strategy-side receipt-parser connector for Pendle (VIB-4854 / W2)."""

from __future__ import annotations

from typing import ClassVar

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.receipt_parser_registry import (
    ReceiptParserCapability,
    ReceiptParserConnector,
)


class PendleReceiptParserConnector(ReceiptParserConnector, ReceiptParserCapability):
    protocol: ClassVar[ProtocolName] = ProtocolName("pendle")
    kind: ClassVar[ProtocolKind] = ProtocolKind.YIELD_TRADING

    def receipt_parser_keys(self) -> frozenset[str]:
        return frozenset({"pendle"})

    def receipt_parser_class(self, key: str) -> type:
        from almanak.connectors.pendle.receipt_parser import (
            PendleReceiptParser,
        )

        return PendleReceiptParser


__all__ = ["PendleReceiptParserConnector"]
