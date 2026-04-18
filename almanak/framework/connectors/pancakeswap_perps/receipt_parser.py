"""Backwards-compatibility submodule re-exporting aster_perps.receipt_parser.

Exists so the ReceiptParserRegistry dynamic import path
``almanak.framework.connectors.pancakeswap_perps.receipt_parser`` keeps
resolving after the VIB-3044 extraction. New code should import from
``almanak.framework.connectors.aster_perps.receipt_parser``.
"""

from almanak.framework.connectors.aster_perps.receipt_parser import (
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
