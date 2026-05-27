"""Strategy-side receipt-parser connector for Spark (VIB-4854 / W2)."""

from __future__ import annotations

from typing import ClassVar

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.receipt_parser_registry import (
    ReceiptParserCapability,
    ReceiptParserConnector,
)


class SparkReceiptParserConnector(ReceiptParserConnector, ReceiptParserCapability):
    protocol: ClassVar[ProtocolName] = ProtocolName("spark")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LENDING

    def receipt_parser_keys(self) -> frozenset[str]:
        return frozenset({"spark"})

    def receipt_parser_class(self, key: str) -> type:
        from almanak.connectors.spark.receipt_parser import SparkReceiptParser

        return SparkReceiptParser


__all__ = ["SparkReceiptParserConnector"]
