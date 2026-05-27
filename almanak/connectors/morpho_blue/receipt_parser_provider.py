"""Strategy-side receipt-parser connector for Morpho Blue (VIB-4854 / W2).

Publishes both ``morpho_blue`` (canonical) and ``morpho`` (alias) keys
so callers using either resolve to the same parser class.
"""

from __future__ import annotations

from typing import ClassVar

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.receipt_parser_registry import (
    ReceiptParserCapability,
    ReceiptParserConnector,
)


class MorphoBlueReceiptParserConnector(ReceiptParserConnector, ReceiptParserCapability):
    protocol: ClassVar[ProtocolName] = ProtocolName("morpho_blue")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LENDING

    def receipt_parser_keys(self) -> frozenset[str]:
        return frozenset({"morpho_blue", "morpho"})

    def receipt_parser_class(self, key: str) -> type:
        from almanak.connectors.morpho_blue.receipt_parser import (
            MorphoBlueReceiptParser,
        )

        return MorphoBlueReceiptParser


__all__ = ["MorphoBlueReceiptParserConnector"]
