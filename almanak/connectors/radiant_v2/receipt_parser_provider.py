"""Strategy-side receipt-parser connector for Radiant V2 (VIB-4854 / W2)."""

from __future__ import annotations

from typing import ClassVar

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.receipt_parser_registry import (
    ReceiptParserCapability,
    ReceiptParserConnector,
)


class RadiantV2ReceiptParserConnector(ReceiptParserConnector, ReceiptParserCapability):
    protocol: ClassVar[ProtocolName] = ProtocolName("radiant_v2")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LENDING

    def receipt_parser_keys(self) -> frozenset[str]:
        return frozenset({"radiant_v2"})

    def receipt_parser_class(self, key: str) -> type:
        from almanak.connectors.radiant_v2.receipt_parser import (
            RadiantV2ReceiptParser,
        )

        return RadiantV2ReceiptParser


__all__ = ["RadiantV2ReceiptParserConnector"]
