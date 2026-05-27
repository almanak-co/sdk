"""Strategy-side receipt-parser connector for Stargate (VIB-4854 / W2)."""

from __future__ import annotations

from typing import ClassVar

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.receipt_parser_registry import (
    ReceiptParserCapability,
    ReceiptParserConnector,
)


class StargateReceiptParserConnector(ReceiptParserConnector, ReceiptParserCapability):
    protocol: ClassVar[ProtocolName] = ProtocolName("stargate")
    kind: ClassVar[ProtocolKind] = ProtocolKind.BRIDGE

    def receipt_parser_keys(self) -> frozenset[str]:
        return frozenset({"stargate"})

    def receipt_parser_class(self, key: str) -> type:
        from almanak.connectors.stargate.receipt_parser import (
            StargateReceiptParser,
        )

        return StargateReceiptParser


__all__ = ["StargateReceiptParserConnector"]
