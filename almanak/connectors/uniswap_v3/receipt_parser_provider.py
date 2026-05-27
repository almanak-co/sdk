"""Strategy-side receipt-parser connector for Uniswap V3 (VIB-4854 / W2).

Publishes the connector's receipt parser to
``STRATEGY_RECEIPT_PARSER_REGISTRY`` via
``ReceiptParserCapability``. Two protocol keys ride this connector:
``uniswap_v3`` (canonical) and ``agni_finance`` (Mantle fork that
uses identical bytecode; resolved here so the registry routes both
to ``UniswapV3ReceiptParser``).
"""

from __future__ import annotations

from typing import ClassVar

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.receipt_parser_registry import (
    ReceiptParserCapability,
    ReceiptParserConnector,
)


class UniswapV3ReceiptParserConnector(ReceiptParserConnector, ReceiptParserCapability):
    """Receipt-parser connector for Uniswap V3 (+ ``agni_finance`` alias)."""

    protocol: ClassVar[ProtocolName] = ProtocolName("uniswap_v3")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LP

    def receipt_parser_keys(self) -> frozenset[str]:
        return frozenset({"uniswap_v3", "agni_finance"})

    def receipt_parser_class(self, key: str) -> type:
        from almanak.connectors.uniswap_v3.receipt_parser import (
            UniswapV3ReceiptParser,
        )

        return UniswapV3ReceiptParser


__all__ = ["UniswapV3ReceiptParserConnector"]
