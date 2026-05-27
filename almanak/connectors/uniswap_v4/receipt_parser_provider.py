"""Strategy-side receipt-parser connector for Uniswap V4 (VIB-4854 / W2)."""

from __future__ import annotations

from typing import ClassVar

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.receipt_parser_registry import (
    ReceiptParserCapability,
    ReceiptParserConnector,
)


class UniswapV4ReceiptParserConnector(ReceiptParserConnector, ReceiptParserCapability):
    protocol: ClassVar[ProtocolName] = ProtocolName("uniswap_v4")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LP

    def receipt_parser_keys(self) -> frozenset[str]:
        return frozenset({"uniswap_v4"})

    def receipt_parser_class(self, key: str) -> type:
        from almanak.connectors.uniswap_v4.receipt_parser import (
            UniswapV4ReceiptParser,
        )

        return UniswapV4ReceiptParser


__all__ = ["UniswapV4ReceiptParserConnector"]
