"""PancakeSwap Perps connector (ApolloX Diamond on BSC).

PancakeSwap Perps is not a standalone protocol — PCS is broker id 2 on the
underlying ApolloX (ASX) perpetual trading platform, which lives as a Diamond
proxy (EIP-2535) at `0x1b6f2d3844c6ae7d56ceb3c3643b9060ba28feb0` on BSC.

v1 scope:
    - BSC only
    - Market orders only
    - Crypto markets (BTC/USD, ETH/USD, BNB/USD)
    - No SL/TP, no limit orders
    - Native BNB margin (openMarketTradeBNB) or ERC20 margin (openMarketTrade)

See the design doc at
`docs/internal/discussions/pancakeswap-perps-integration-20260415.md` for the
full rationale and deferred-scope items (RWA, Arbitrum, SL/TP, etc.).

Example usage — strategy-author facing::

    # Inside an IntentStrategy.decide()
    return Intent.perp_open(
        market="BTC/USD",
        collateral_token="BNB",
        collateral_amount=Decimal("0.1"),
        size_usd=Decimal("300"),
        is_long=True,
        max_slippage=Decimal("0.01"),
        protocol="pancakeswap_perps",
        leverage=Decimal("3"),
    )

    # The compiler resolves mark_price, converts to on-wire qty/limit-price,
    # and emits a single openMarketTradeBNB call. The receipt parser
    # extracts the tradeHash (== position_id) from the MarketPendingTrade event.
"""

from .adapter import (
    GAS_CLOSE_TRADE,
    GAS_OPEN_MARKET_TRADE,
    GAS_OPEN_MARKET_TRADE_BNB,
    PancakeSwapPerpsAdapter,
    PancakeSwapPerpsConfig,
    PancakeSwapPerpsTx,
    PerpOpenOrderResult,
    build_close_transaction,
    build_open_transaction,
)
from .receipt_parser import (
    CloseTradeReceivedEvent,
    CloseTradeSuccessfulEvent,
    MarketPendingTradeEvent,
    OpenMarketTradeEvent,
    PancakeSwapPerpsReceiptParser,
    ParsedReceipt,
    PendingTradeRefundEvent,
)
from .sdk import (
    EVENT_CLOSE_TRADE_RECEIVED,
    EVENT_CLOSE_TRADE_SUCCESSFUL,
    EVENT_MARKET_PENDING_TRADE,
    EVENT_OPEN_MARKET_TRADE,
    EVENT_PENDING_TRADE_REFUND,
    NATIVE_BNB_ADDRESS,
    PCS_BROKER_ID,
    PRICE_DECIMALS,
    QTY_DECIMALS,
    SELECTOR_CLOSE_TRADE,
    SELECTOR_OPEN_MARKET_TRADE,
    SELECTOR_OPEN_MARKET_TRADE_BNB,
    OpenTradeStruct,
    encode_close_trade_calldata,
    encode_get_pending_trade_calldata,
    encode_get_position_by_hash_calldata,
    encode_open_market_trade_calldata,
    get_margin_token_address,
    get_pair_base,
    get_router_address,
    slippage_to_limit_price,
    usd_size_to_qty,
)

__all__ = [
    # Event topics
    "EVENT_CLOSE_TRADE_RECEIVED",
    "EVENT_CLOSE_TRADE_SUCCESSFUL",
    "EVENT_MARKET_PENDING_TRADE",
    "EVENT_OPEN_MARKET_TRADE",
    "EVENT_PENDING_TRADE_REFUND",
    # Gas budgets
    "GAS_CLOSE_TRADE",
    "GAS_OPEN_MARKET_TRADE",
    "GAS_OPEN_MARKET_TRADE_BNB",
    # Sentinels / constants
    "NATIVE_BNB_ADDRESS",
    "PCS_BROKER_ID",
    "PRICE_DECIMALS",
    "QTY_DECIMALS",
    # Selectors
    "SELECTOR_CLOSE_TRADE",
    "SELECTOR_OPEN_MARKET_TRADE",
    "SELECTOR_OPEN_MARKET_TRADE_BNB",
    # Parser
    "CloseTradeReceivedEvent",
    "CloseTradeSuccessfulEvent",
    "MarketPendingTradeEvent",
    "OpenMarketTradeEvent",
    "PancakeSwapPerpsReceiptParser",
    "ParsedReceipt",
    "PendingTradeRefundEvent",
    # Adapter
    "OpenTradeStruct",
    "PancakeSwapPerpsAdapter",
    "PancakeSwapPerpsConfig",
    "PancakeSwapPerpsTx",
    "PerpOpenOrderResult",
    # Convenience
    "build_close_transaction",
    "build_open_transaction",
    "encode_close_trade_calldata",
    "encode_get_pending_trade_calldata",
    "encode_get_position_by_hash_calldata",
    "encode_open_market_trade_calldata",
    "get_margin_token_address",
    "get_pair_base",
    "get_router_address",
    "slippage_to_limit_price",
    "usd_size_to_qty",
]
