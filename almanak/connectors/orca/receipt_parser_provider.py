"""Strategy-side receipt-parser connector for Orca Whirlpools (VIB-4854 / W2).

Publishes both ``orca_whirlpools`` (canonical) and ``orca`` (alias)
keys so callers using either resolve to the same parser class.
"""

from __future__ import annotations

from typing import ClassVar

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.receipt_parser_registry import (
    ReceiptParserCapability,
    ReceiptParserConnector,
)


class OrcaReceiptParserConnector(ReceiptParserConnector, ReceiptParserCapability):
    protocol: ClassVar[ProtocolName] = ProtocolName("orca")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LP

    def receipt_parser_keys(self) -> frozenset[str]:
        return frozenset({"orca_whirlpools", "orca"})

    def receipt_parser_class(self, key: str) -> type:
        from almanak.connectors.orca.receipt_parser import OrcaReceiptParser

        return OrcaReceiptParser


__all__ = ["OrcaReceiptParserConnector"]
