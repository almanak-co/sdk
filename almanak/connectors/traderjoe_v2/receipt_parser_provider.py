"""Strategy-side receipt-parser connector for Trader Joe V2 (VIB-4854 / W2)."""

from __future__ import annotations

from typing import ClassVar

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.receipt_parser_registry import (
    ReceiptParserCapability,
    ReceiptParserConnector,
)


class TraderJoeV2ReceiptParserConnector(ReceiptParserConnector, ReceiptParserCapability):
    protocol: ClassVar[ProtocolName] = ProtocolName("traderjoe_v2")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LP

    def receipt_parser_keys(self) -> frozenset[str]:
        return frozenset({"traderjoe_v2"})

    def receipt_parser_class(self, key: str) -> type:
        from almanak.connectors.traderjoe_v2.receipt_parser import (
            TraderJoeV2ReceiptParser,
        )

        return TraderJoeV2ReceiptParser


__all__ = ["TraderJoeV2ReceiptParserConnector"]
