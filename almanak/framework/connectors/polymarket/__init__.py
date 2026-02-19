"""Polymarket prediction market connector.

This module provides integration with Polymarket's hybrid CLOB + on-chain
architecture for prediction market trading.

Architecture Overview:
    - CLOB (Central Limit Order Book): Off-chain order matching via API
    - CTF (Conditional Token Framework): On-chain ERC-1155 outcome tokens
    - Market Making: Specialized utilities for automated market making
    - Signals: Integration with news, social sentiment, and prediction models

Export Organization:
    This connector exports 103 items across 14 categories due to Polymarket's
    unique hybrid architecture. See notes/tech-debt/polymarket-export-list.md
    for detailed analysis and justification.

    Major categories:
    - Clients & SDK: Core client classes (ClobClient, CtfSDK, PolymarketSDK)
    - Adapter & Results: Adapter interface and transaction results
    - Constants: Contract addresses, EIP-712 domains, CTF SDK constants
    - Configuration: Credentials and config management
    - Enums: Order types, statuses, signature types
    - Market Data: Market info, order books, prices
    - Orders: Order parameters and state transitions
    - Positions & Trades: Position tracking and trade history
    - Filters: Query filters for markets, orders, trades
    - Historical Data: Price history and historical trades
    - Receipt Parser: Event parsing for on-chain transactions
    - Market Making: Quote generation and risk management
    - Signals: Prediction signal providers and aggregation
    - Exceptions: Comprehensive error hierarchy

Example:
    from almanak.framework.connectors.polymarket import (
        ClobClient,
        PolymarketConfig,
        MarketFilters,
    )

    # Initialize client
    config = PolymarketConfig.from_env()
    client = ClobClient(config)

    # Fetch markets
    markets = client.get_markets(MarketFilters(active=True, limit=10))
    for market in markets:
        print(f"{market.question}: YES={market.yes_price}, NO={market.no_price}")
"""

from .adapter import (
    POLYMARKET_GAS_ESTIMATES,
    OrderResult,
    PolymarketAdapter,
    RedeemResult,
)
from .clob_client import ClobClient, TokenBucketRateLimiter
from .ctf_sdk import (
    BINARY_PARTITION,
    GAS_ESTIMATES,
    INDEX_SET_NO,
    INDEX_SET_YES,
    MAX_UINT256,
    ZERO_BYTES32,
    AllowanceStatus,
    CtfSDK,
    ResolutionStatus,
    TransactionData,
)
from .exceptions import (
    PolymarketAPIError,
    PolymarketAuthenticationError,
    PolymarketCredentialsError,
    PolymarketError,
    PolymarketInsufficientBalanceError,
    PolymarketInvalidPriceError,
    PolymarketMarketClosedError,
    PolymarketMarketError,
    PolymarketMarketNotFoundError,
    PolymarketMarketNotResolvedError,
    PolymarketMinimumOrderError,
    PolymarketOrderError,
    PolymarketOrderNotFoundError,
    PolymarketRateLimitError,
    PolymarketRedemptionError,
    PolymarketSignatureError,
)
from .market_making import (
    MAX_PRICE as MM_MAX_PRICE,
)
from .market_making import (
    MIN_PRICE as MM_MIN_PRICE,
)
from .market_making import (
    Quote,
    RiskParameters,
    calculate_inventory_skew,
    calculate_optimal_spread,
    generate_quote_ladder,
    should_requote,
)
from .models import (
    # Constants
    CLOB_AUTH_DOMAIN,
    CLOB_AUTH_MESSAGE,
    CLOB_AUTH_TYPES,
    CLOB_BASE_URL,
    CONDITIONAL_TOKENS,
    CTF_EXCHANGE,
    CTF_EXCHANGE_DOMAIN,
    DATA_API_BASE_URL,
    GAMMA_BASE_URL,
    NEG_RISK_ADAPTER,
    NEG_RISK_EXCHANGE,
    ORDER_TYPES,
    POLYGON_CHAIN_ID,
    USDC_POLYGON,
    # Credentials & Config
    ApiCredentials,
    # Positions & Trades
    BalanceAllowance,
    # Market Data
    GammaMarket,
    # Historical Data
    HistoricalPrice,
    HistoricalTrade,
    # Orders
    LimitOrderParams,
    # Filters
    MarketFilters,
    MarketOrderParams,
    OpenOrder,
    OrderBook,
    OrderFilters,
    OrderResponse,
    # Enums
    OrderSide,
    OrderStatus,
    OrderType,
    PolymarketConfig,
    Position,
    PriceHistory,
    PriceHistoryInterval,
    PriceLevel,
    SignatureType,
    SignedOrder,
    TokenPrice,
    Trade,
    TradeFilters,
    TradeStatus,
    UnsignedOrder,
)
from .receipt_parser import (
    PAYOUT_REDEMPTION_TOPIC,
    TRANSFER_BATCH_TOPIC,
    TRANSFER_SINGLE_TOPIC,
    CtfEvent,
    CtfParseResult,
    Erc20TransferData,
    PayoutRedemptionData,
    PolymarketEventType,
    PolymarketReceiptParser,
    RedemptionResult,
    TradeResult,
    TransferBatchData,
    TransferSingleData,
)
from .sdk import PolymarketSDK
from .signals import (
    ModelPredictionProvider,
    NewsAPISignalProvider,
    PredictionSignal,
    SignalDirection,
    SignalResult,
    SocialSentimentProvider,
    aggregate_signals,
    combine_with_market_price,
)

# Export List Organization:
#
# This connector exports 103 items organized into 14 logical categories.
# The large export count is justified by Polymarket's unique hybrid architecture:
#
# 1. Off-chain CLOB (Central Limit Order Book) requires extensive order types,
#    filters, and API models for order management
# 2. On-chain CTF (Conditional Token Framework) requires ERC-1155 event parsing,
#    transaction building, and settlement logic
# 3. Market Making functionality is unique to prediction markets and requires
#    specialized risk parameters and quote generation
# 4. Signal Integration (news, social sentiment, models) is specific to prediction
#    market strategies
#
# Alternative sub-package organization was considered but rejected to maintain:
# - Simple import paths (from polymarket import X vs from polymarket.clob import X)
# - Backward compatibility
# - Single source of truth for exports
#
# See notes/tech-debt/polymarket-export-list.md for detailed analysis.
__all__ = [
    # Clients & SDK
    "ClobClient",
    "CtfSDK",
    "PolymarketSDK",
    # Rate Limiting
    "TokenBucketRateLimiter",
    # Adapter
    "PolymarketAdapter",
    "OrderResult",
    "RedeemResult",
    "POLYMARKET_GAS_ESTIMATES",
    # CTF SDK Types
    "TransactionData",
    "AllowanceStatus",
    "ResolutionStatus",
    # CTF SDK Constants
    "MAX_UINT256",
    "ZERO_BYTES32",
    "INDEX_SET_YES",
    "INDEX_SET_NO",
    "BINARY_PARTITION",
    "GAS_ESTIMATES",
    # Constants
    "CLOB_BASE_URL",
    "GAMMA_BASE_URL",
    "DATA_API_BASE_URL",
    "CTF_EXCHANGE",
    "NEG_RISK_EXCHANGE",
    "CONDITIONAL_TOKENS",
    "NEG_RISK_ADAPTER",
    "USDC_POLYGON",
    "POLYGON_CHAIN_ID",
    # EIP-712
    "CLOB_AUTH_DOMAIN",
    "CLOB_AUTH_TYPES",
    "CLOB_AUTH_MESSAGE",
    "CTF_EXCHANGE_DOMAIN",
    "ORDER_TYPES",
    # Credentials & Config
    "ApiCredentials",
    "PolymarketConfig",
    # Enums
    "SignatureType",
    "OrderSide",
    "OrderType",
    "OrderStatus",
    "TradeStatus",
    # Market Data
    "GammaMarket",
    "PriceLevel",
    "OrderBook",
    "TokenPrice",
    # Orders
    "LimitOrderParams",
    "MarketOrderParams",
    "UnsignedOrder",
    "SignedOrder",
    "OrderResponse",
    "OpenOrder",
    # Positions & Trades
    "Position",
    "Trade",
    "BalanceAllowance",
    # Filters
    "MarketFilters",
    "OrderFilters",
    "TradeFilters",
    # Historical Data
    "PriceHistoryInterval",
    "HistoricalPrice",
    "PriceHistory",
    "HistoricalTrade",
    # Receipt Parser
    "PolymarketReceiptParser",
    "TradeResult",
    "RedemptionResult",
    "CtfParseResult",
    "CtfEvent",
    "PolymarketEventType",
    "TransferSingleData",
    "TransferBatchData",
    "PayoutRedemptionData",
    "Erc20TransferData",
    "TRANSFER_SINGLE_TOPIC",
    "TRANSFER_BATCH_TOPIC",
    "PAYOUT_REDEMPTION_TOPIC",
    # Market Making
    "Quote",
    "RiskParameters",
    "MM_MIN_PRICE",
    "MM_MAX_PRICE",
    "calculate_inventory_skew",
    "calculate_optimal_spread",
    "generate_quote_ladder",
    "should_requote",
    # Exceptions
    "PolymarketError",
    "PolymarketAPIError",
    "PolymarketAuthenticationError",
    "PolymarketCredentialsError",
    "PolymarketRateLimitError",
    "PolymarketOrderError",
    "PolymarketOrderNotFoundError",
    "PolymarketInsufficientBalanceError",
    "PolymarketInvalidPriceError",
    "PolymarketMinimumOrderError",
    "PolymarketMarketError",
    "PolymarketMarketNotFoundError",
    "PolymarketMarketClosedError",
    "PolymarketMarketNotResolvedError",
    "PolymarketRedemptionError",
    "PolymarketSignatureError",
    # Signals
    "SignalDirection",
    "SignalResult",
    "PredictionSignal",
    "NewsAPISignalProvider",
    "SocialSentimentProvider",
    "ModelPredictionProvider",
    "aggregate_signals",
    "combine_with_market_price",
]
