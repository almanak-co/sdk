"""Strategy-side receipt-parser connector for LiFi (VIB-4854 / W2)."""

from __future__ import annotations

from typing import ClassVar

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.receipt_parser_registry import (
    ReceiptParserCapability,
    ReceiptParserConnector,
)


class LiFiReceiptParserConnector(ReceiptParserConnector, ReceiptParserCapability):
    protocol: ClassVar[ProtocolName] = ProtocolName("lifi")
    kind: ClassVar[ProtocolKind] = ProtocolKind.BRIDGE

    def receipt_parser_keys(self) -> frozenset[str]:
        return frozenset({"lifi"})

    def receipt_parser_class(self, key: str) -> type:
        from almanak.connectors.lifi.receipt_parser import LiFiReceiptParser

        return LiFiReceiptParser


__all__ = ["LiFiReceiptParserConnector"]
