"""Strategy-side receipt-parser connector for Jupiter (VIB-4854 / W2)."""

from __future__ import annotations

from typing import ClassVar

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.receipt_parser_registry import (
    ReceiptParserCapability,
    ReceiptParserConnector,
)


class JupiterReceiptParserConnector(ReceiptParserConnector, ReceiptParserCapability):
    protocol: ClassVar[ProtocolName] = ProtocolName("jupiter")
    kind: ClassVar[ProtocolKind] = ProtocolKind.SWAP

    def receipt_parser_keys(self) -> frozenset[str]:
        return frozenset({"jupiter"})

    def receipt_parser_class(self, key: str) -> type:
        from almanak.connectors.jupiter.receipt_parser import (
            JupiterReceiptParser,
        )

        return JupiterReceiptParser


__all__ = ["JupiterReceiptParserConnector"]
