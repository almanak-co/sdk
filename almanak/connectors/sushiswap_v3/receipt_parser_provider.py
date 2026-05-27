"""Strategy-side receipt-parser connector for SushiSwap V3 (VIB-4854 / W2)."""

from __future__ import annotations

from typing import ClassVar

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.receipt_parser_registry import (
    ReceiptParserCapability,
    ReceiptParserConnector,
)


class SushiSwapV3ReceiptParserConnector(ReceiptParserConnector, ReceiptParserCapability):
    protocol: ClassVar[ProtocolName] = ProtocolName("sushiswap_v3")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LP

    def receipt_parser_keys(self) -> frozenset[str]:
        return frozenset({"sushiswap_v3"})

    def receipt_parser_class(self, key: str) -> type:
        from almanak.connectors.sushiswap_v3.receipt_parser import (
            SushiSwapV3ReceiptParser,
        )

        return SushiSwapV3ReceiptParser


__all__ = ["SushiSwapV3ReceiptParserConnector"]
