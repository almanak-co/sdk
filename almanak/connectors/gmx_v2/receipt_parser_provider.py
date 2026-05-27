"""Strategy-side receipt-parser connector for GMX V2 (VIB-4854 / W2)."""

from __future__ import annotations

from typing import ClassVar

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.receipt_parser_registry import (
    ReceiptParserCapability,
    ReceiptParserConnector,
)


class GmxV2ReceiptParserConnector(ReceiptParserConnector, ReceiptParserCapability):
    protocol: ClassVar[ProtocolName] = ProtocolName("gmx_v2")
    kind: ClassVar[ProtocolKind] = ProtocolKind.PERP

    def receipt_parser_keys(self) -> frozenset[str]:
        return frozenset({"gmx_v2"})

    def receipt_parser_class(self, key: str) -> type:
        from almanak.connectors.gmx_v2.receipt_parser import GMXv2ReceiptParser

        return GMXv2ReceiptParser


__all__ = ["GmxV2ReceiptParserConnector"]
