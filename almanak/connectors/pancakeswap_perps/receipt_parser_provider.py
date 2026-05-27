"""Strategy-side receipt-parser connector for PancakeSwap Perps shim (VIB-4854 / W2).

PancakeSwap Perps is a legacy alias for Aster Perps — the perp venue
re-branded after the on-chain canonical name changed. The shim module
``almanak/connectors/pancakeswap_perps/receipt_parser.py`` re-exports
the Aster parser class under the legacy ``PancakeSwapPerpsReceiptParser``
name; both ``aster_perps`` and ``pancakeswap_perps`` callers resolve
to the same underlying parser.

The shim keeps a separate connector (and a separate registry key) so
the legacy intent payloads and historical position rows that record
``protocol="pancakeswap_perps"`` continue to dispatch — and so the
``test_receipt_parser_registry_completeness`` guard ("every
receipt_parser.py file has a registered class") still holds.
"""

from __future__ import annotations

from typing import ClassVar

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.receipt_parser_registry import (
    ReceiptParserCapability,
    ReceiptParserConnector,
)


class PancakeSwapPerpsReceiptParserConnector(ReceiptParserConnector, ReceiptParserCapability):
    protocol: ClassVar[ProtocolName] = ProtocolName("pancakeswap_perps")
    kind: ClassVar[ProtocolKind] = ProtocolKind.PERP

    def receipt_parser_keys(self) -> frozenset[str]:
        return frozenset({"pancakeswap_perps"})

    def receipt_parser_class(self, key: str) -> type:
        from almanak.connectors.pancakeswap_perps.receipt_parser import (
            PancakeSwapPerpsReceiptParser,
        )

        return PancakeSwapPerpsReceiptParser


__all__ = ["PancakeSwapPerpsReceiptParserConnector"]
