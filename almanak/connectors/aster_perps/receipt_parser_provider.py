"""Strategy-side receipt-parser connector for Aster Perps (VIB-4854 / W2)."""

from __future__ import annotations

from typing import ClassVar

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.receipt_parser_registry import (
    ReceiptParserCapability,
    ReceiptParserConnector,
)


class AsterPerpsReceiptParserConnector(ReceiptParserConnector, ReceiptParserCapability):
    protocol: ClassVar[ProtocolName] = ProtocolName("aster_perps")
    kind: ClassVar[ProtocolKind] = ProtocolKind.PERP

    def receipt_parser_keys(self) -> frozenset[str]:
        return frozenset({"aster_perps"})

    def receipt_parser_class(self, key: str) -> type:
        from almanak.connectors.aster_perps.receipt_parser import (
            AsterPerpsReceiptParser,
        )

        return AsterPerpsReceiptParser


__all__ = ["AsterPerpsReceiptParserConnector"]
