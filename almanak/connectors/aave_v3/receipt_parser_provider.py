"""Strategy-side receipt-parser connector for Aave V3 (VIB-4854 / W2)."""

from __future__ import annotations

from typing import ClassVar

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.receipt_parser_registry import (
    ReceiptParserCapability,
    ReceiptParserConnector,
)


class AaveV3ReceiptParserConnector(ReceiptParserConnector, ReceiptParserCapability):
    protocol: ClassVar[ProtocolName] = ProtocolName("aave_v3")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LENDING

    def receipt_parser_keys(self) -> frozenset[str]:
        return frozenset({"aave_v3"})

    def receipt_parser_class(self, key: str) -> type:
        from almanak.connectors.aave_v3.receipt_parser import (
            AaveV3ReceiptParser,
        )

        return AaveV3ReceiptParser


__all__ = ["AaveV3ReceiptParserConnector"]
