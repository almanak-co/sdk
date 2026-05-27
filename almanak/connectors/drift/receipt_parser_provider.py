"""Strategy-side receipt-parser connector for Drift (VIB-4854 / W2)."""

from __future__ import annotations

from typing import ClassVar

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.receipt_parser_registry import (
    ReceiptParserCapability,
    ReceiptParserConnector,
)


class DriftReceiptParserConnector(ReceiptParserConnector, ReceiptParserCapability):
    protocol: ClassVar[ProtocolName] = ProtocolName("drift")
    kind: ClassVar[ProtocolKind] = ProtocolKind.PERP

    def receipt_parser_keys(self) -> frozenset[str]:
        return frozenset({"drift"})

    def receipt_parser_class(self, key: str) -> type:
        from almanak.connectors.drift.receipt_parser import DriftReceiptParser

        return DriftReceiptParser


__all__ = ["DriftReceiptParserConnector"]
