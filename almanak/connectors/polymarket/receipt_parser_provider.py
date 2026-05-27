"""Strategy-side receipt-parser connector for Polymarket (VIB-4854 / W2)."""

from __future__ import annotations

from typing import ClassVar

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.receipt_parser_registry import (
    ReceiptParserCapability,
    ReceiptParserConnector,
)


class PolymarketReceiptParserConnector(ReceiptParserConnector, ReceiptParserCapability):
    protocol: ClassVar[ProtocolName] = ProtocolName("polymarket")
    kind: ClassVar[ProtocolKind] = ProtocolKind.PREDICTION_MARKET

    def receipt_parser_keys(self) -> frozenset[str]:
        return frozenset({"polymarket"})

    def receipt_parser_class(self, key: str) -> type:
        from almanak.connectors.polymarket.receipt_parser import (
            PolymarketReceiptParser,
        )

        return PolymarketReceiptParser


__all__ = ["PolymarketReceiptParserConnector"]
