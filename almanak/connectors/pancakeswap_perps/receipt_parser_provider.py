"""Strategy-side receipt-parser connector for PancakeSwap Perps.

``almanak/connectors/pancakeswap_perps/receipt_parser.py`` re-exports the
legacy Aster Diamond parser under the ``PancakeSwapPerpsReceiptParser`` name.
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
