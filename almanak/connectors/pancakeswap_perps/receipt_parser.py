"""Backwards-compatibility submodule re-exporting the shared Aster receipt parser.

Exists so the ReceiptParserRegistry dynamic import path
``almanak.connectors.pancakeswap_perps.receipt_parser`` keeps resolving after
the VIB-3044 extraction. Re-exports from the shared
``_aster_perps_core.receipt_parser`` foundation (not the sibling ``aster_perps``
leaf). New code should import from ``almanak.connectors.aster_perps.receipt_parser``.
"""

from almanak.connectors._aster_perps_core.receipt_parser import (
    AsterPerpsReceiptParser,
    CloseTradeReceivedEvent,
    CloseTradeSuccessfulEvent,
    MarketPendingTradeEvent,
    OpenMarketTradeEvent,
    ParsedReceipt,
    PendingTradeRefundEvent,
)

# Legacy class alias — the ReceiptParserRegistry imports this name by string.
PancakeSwapPerpsReceiptParser = AsterPerpsReceiptParser

__all__ = [
    "CloseTradeReceivedEvent",
    "CloseTradeSuccessfulEvent",
    "MarketPendingTradeEvent",
    "OpenMarketTradeEvent",
    "PancakeSwapPerpsReceiptParser",
    "ParsedReceipt",
    "PendingTradeRefundEvent",
]
