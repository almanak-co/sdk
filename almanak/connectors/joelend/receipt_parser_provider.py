"""Strategy-side receipt-parser connector for JoeLend (VIB-4854 / W2).

JoeLend is dormant (VIB-3960) — the protocol wound down on-chain — but
the parser is kept so historical receipts remain decodable. The
provider preserves the existing registry key so older intent logs and
position rows continue to parse.
"""

from __future__ import annotations

from typing import ClassVar

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.receipt_parser_registry import (
    ReceiptParserCapability,
    ReceiptParserConnector,
)


class JoeLendReceiptParserConnector(ReceiptParserConnector, ReceiptParserCapability):
    protocol: ClassVar[ProtocolName] = ProtocolName("joelend")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LENDING

    def receipt_parser_keys(self) -> frozenset[str]:
        return frozenset({"joelend"})

    def receipt_parser_class(self, key: str) -> type:
        from almanak.connectors.joelend.receipt_parser import (
            JoeLendReceiptParser,
        )

        return JoeLendReceiptParser


__all__ = ["JoeLendReceiptParserConnector"]
