"""Strategy-side receipt-parser connector for Meteora (VIB-4854 / W2).

Publishes both ``meteora_dlmm`` (canonical) and ``meteora`` (alias)
keys so callers using either resolve to the same parser class.
"""

from __future__ import annotations

from typing import ClassVar

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.receipt_parser_registry import (
    ReceiptParserCapability,
    ReceiptParserConnector,
)


class MeteoraReceiptParserConnector(ReceiptParserConnector, ReceiptParserCapability):
    protocol: ClassVar[ProtocolName] = ProtocolName("meteora")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LP

    def receipt_parser_keys(self) -> frozenset[str]:
        return frozenset({"meteora_dlmm", "meteora"})

    def receipt_parser_class(self, key: str) -> type:
        from almanak.connectors.meteora.receipt_parser import (
            MeteoraReceiptParser,
        )

        return MeteoraReceiptParser


__all__ = ["MeteoraReceiptParserConnector"]
