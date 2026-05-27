"""Strategy-side receipt-parser connector for Ethena (VIB-4854 / W2)."""

from __future__ import annotations

from typing import ClassVar

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.receipt_parser_registry import (
    ReceiptParserCapability,
    ReceiptParserConnector,
)


class EthenaReceiptParserConnector(ReceiptParserConnector, ReceiptParserCapability):
    protocol: ClassVar[ProtocolName] = ProtocolName("ethena")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LENDING

    def receipt_parser_keys(self) -> frozenset[str]:
        return frozenset({"ethena"})

    def receipt_parser_class(self, key: str) -> type:
        from almanak.connectors.ethena.receipt_parser import (
            EthenaReceiptParser,
        )

        return EthenaReceiptParser


__all__ = ["EthenaReceiptParserConnector"]
