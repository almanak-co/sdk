"""Strategy-side receipt-parser connector for Kamino (VIB-4854 / W2).

Publishes both ``kamino`` and ``kamino_klend`` (alias) keys so callers
using either resolve to the same parser class.
"""

from __future__ import annotations

from typing import ClassVar

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.receipt_parser_registry import (
    ReceiptParserCapability,
    ReceiptParserConnector,
)


class KaminoReceiptParserConnector(ReceiptParserConnector, ReceiptParserCapability):
    protocol: ClassVar[ProtocolName] = ProtocolName("kamino")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LENDING

    def receipt_parser_keys(self) -> frozenset[str]:
        return frozenset({"kamino", "kamino_klend"})

    def receipt_parser_class(self, key: str) -> type:
        from almanak.connectors.kamino.receipt_parser import (
            KaminoReceiptParser,
        )

        return KaminoReceiptParser


__all__ = ["KaminoReceiptParserConnector"]
