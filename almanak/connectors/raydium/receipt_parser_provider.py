"""Strategy-side receipt-parser connector for Raydium (VIB-4854 / W2).

Publishes both ``raydium_clmm`` (canonical) and ``raydium`` (alias)
keys so callers using either resolve to the same parser class.
"""

from __future__ import annotations

from typing import ClassVar

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.receipt_parser_registry import (
    ReceiptParserCapability,
    ReceiptParserConnector,
)


class RaydiumReceiptParserConnector(ReceiptParserConnector, ReceiptParserCapability):
    protocol: ClassVar[ProtocolName] = ProtocolName("raydium")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LP

    def receipt_parser_keys(self) -> frozenset[str]:
        return frozenset({"raydium_clmm", "raydium"})

    def receipt_parser_class(self, key: str) -> type:
        from almanak.connectors.raydium.receipt_parser import (
            RaydiumReceiptParser,
        )

        return RaydiumReceiptParser


__all__ = ["RaydiumReceiptParserConnector"]
