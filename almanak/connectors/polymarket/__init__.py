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
    unique hybrid architecture. See docs/internal/notes/tech-debt/polymarket-export-list.md
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
    from almanak.connectors.polymarket import (
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

# Lazy attribute access (VIB-4835). Polymarket's manifest-declared
# gateway-side settings fragment is composed into ``GatewaySettings``, so
# the package ``__init__`` can run during gateway boot. Eager-importing the
# strategy surface (``framework.intents.vocabulary``) would re-enter
# ``config.env`` mid-init and explode a circular import. PEP 562
# attribute lookup defers resolution until the symbol is actually read,
# which only happens from strategy-side callers after ``config.env`` is
# fully loaded. Strategy registration metadata is descriptor-owned in
# ``connector.py``.
from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .adapter import (
        POLYMARKET_GAS_ESTIMATES,
        OrderResult,
        PolymarketAdapter,
        RedeemResult,
    )
    from .clob_client import (
        ClobClient,
        TokenBucketRateLimiter,
    )
    from .ctf_sdk import (
        BINARY_PARTITION,
        GAS_ESTIMATES,
        INDEX_SET_NO,
        INDEX_SET_YES,
        MAX_UINT256,
        ZERO_BYTES32,
        AllowanceStatus,
        CollateralBreakdown,
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
    from .gateway_client import GatewayPolymarketClient
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
        BYTES32_ZERO,
        CLOB_AUTH_DOMAIN,
        CLOB_AUTH_MESSAGE,
        CLOB_AUTH_TYPES,
        CLOB_BASE_URL,
        COLLATERAL_OFFRAMP,
        COLLATERAL_ONRAMP,
        CONDITIONAL_TOKENS,
        CTF_EXCHANGE_V2,
        CTF_EXCHANGE_V2_DOMAIN_NAME,
        CTF_EXCHANGE_V2_DOMAIN_VERSION,
        DATA_API_BASE_URL,
        GAMMA_BASE_URL,
        NEG_RISK_ADAPTER,
        NEG_RISK_EXCHANGE_V2,
        ORDER_TYPES,
        POLYGON_CHAIN_ID,
        PUSD,
        USDC_NATIVE_POLYGON,
        USDCE_POLYGON,
        ApiCredentials,
        BalanceAllowance,
        GammaMarket,
        HistoricalPrice,
        HistoricalTrade,
        LimitOrderParams,
        MarketFilters,
        MarketOrderParams,
        OpenOrder,
        OrderBook,
        OrderFilters,
        OrderResponse,
        OrderSide,
        OrderStatus,
        OrderType,
        PolymarketConfig,
        Position,
        PriceHistory,
        PriceHistoryInterval,
        PriceLevel,
        SetupTxInfo,
        SignatureType,
        SignedOrder,
        TokenPrice,
        Trade,
        TradeFilters,
        TradeStatus,
        UnsignedOrder,
        build_ctf_exchange_domain,
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
    from .signer import (
        Signer,
        make_local_signer,
        make_remote_signer,
        signer_from_env,
    )

__all__ = [
    "AllowanceStatus",
    "ApiCredentials",
    "BINARY_PARTITION",
    "BYTES32_ZERO",
    "BalanceAllowance",
    "CLOB_AUTH_DOMAIN",
    "CLOB_AUTH_MESSAGE",
    "CLOB_AUTH_TYPES",
    "CLOB_BASE_URL",
    "COLLATERAL_OFFRAMP",
    "COLLATERAL_ONRAMP",
    "CONDITIONAL_TOKENS",
    "CTF_EXCHANGE_V2",
    "CTF_EXCHANGE_V2_DOMAIN_NAME",
    "CTF_EXCHANGE_V2_DOMAIN_VERSION",
    "ClobClient",
    "CollateralBreakdown",
    "CtfEvent",
    "CtfParseResult",
    "CtfSDK",
    "DATA_API_BASE_URL",
    "Erc20TransferData",
    "GAMMA_BASE_URL",
    "GAS_ESTIMATES",
    "GammaMarket",
    "GatewayPolymarketClient",
    "HistoricalPrice",
    "HistoricalTrade",
    "INDEX_SET_NO",
    "INDEX_SET_YES",
    "LimitOrderParams",
    "MAX_UINT256",
    "MM_MAX_PRICE",
    "MM_MIN_PRICE",
    "MarketFilters",
    "MarketOrderParams",
    "ModelPredictionProvider",
    "NEG_RISK_ADAPTER",
    "NEG_RISK_EXCHANGE_V2",
    "NewsAPISignalProvider",
    "ORDER_TYPES",
    "OpenOrder",
    "OrderBook",
    "OrderFilters",
    "OrderResponse",
    "OrderResult",
    "OrderSide",
    "OrderStatus",
    "OrderType",
    "PAYOUT_REDEMPTION_TOPIC",
    "POLYGON_CHAIN_ID",
    "POLYMARKET_GAS_ESTIMATES",
    "PUSD",
    "PayoutRedemptionData",
    "PolymarketAPIError",
    "PolymarketAdapter",
    "PolymarketAuthenticationError",
    "PolymarketConfig",
    "PolymarketCredentialsError",
    "PolymarketError",
    "PolymarketEventType",
    "PolymarketInsufficientBalanceError",
    "PolymarketInvalidPriceError",
    "PolymarketMarketClosedError",
    "PolymarketMarketError",
    "PolymarketMarketNotFoundError",
    "PolymarketMarketNotResolvedError",
    "PolymarketMinimumOrderError",
    "PolymarketOrderError",
    "PolymarketOrderNotFoundError",
    "PolymarketRateLimitError",
    "PolymarketReceiptParser",
    "PolymarketRedemptionError",
    "PolymarketSDK",
    "PolymarketSignatureError",
    "Position",
    "PredictionSignal",
    "PriceHistory",
    "PriceHistoryInterval",
    "PriceLevel",
    "Quote",
    "RedeemResult",
    "RedemptionResult",
    "ResolutionStatus",
    "RiskParameters",
    "SetupTxInfo",
    "SignalDirection",
    "SignalResult",
    "SignatureType",
    "SignedOrder",
    "Signer",
    "SocialSentimentProvider",
    "TRANSFER_BATCH_TOPIC",
    "TRANSFER_SINGLE_TOPIC",
    "TokenBucketRateLimiter",
    "TokenPrice",
    "Trade",
    "TradeFilters",
    "TradeResult",
    "TradeStatus",
    "TransactionData",
    "TransferBatchData",
    "TransferSingleData",
    "USDCE_POLYGON",
    "USDC_NATIVE_POLYGON",
    "UnsignedOrder",
    "ZERO_BYTES32",
    "aggregate_signals",
    "build_ctf_exchange_domain",
    "calculate_inventory_skew",
    "calculate_optimal_spread",
    "combine_with_market_price",
    "generate_quote_ladder",
    "make_local_signer",
    "make_remote_signer",
    "should_requote",
    "signer_from_env",
]

_LAZY: dict[str, tuple[str, str]] = {
    "AllowanceStatus": (".ctf_sdk", "AllowanceStatus"),
    "ApiCredentials": (".models", "ApiCredentials"),
    "BINARY_PARTITION": (".ctf_sdk", "BINARY_PARTITION"),
    "BYTES32_ZERO": (".models", "BYTES32_ZERO"),
    "BalanceAllowance": (".models", "BalanceAllowance"),
    "CLOB_AUTH_DOMAIN": (".models", "CLOB_AUTH_DOMAIN"),
    "CLOB_AUTH_MESSAGE": (".models", "CLOB_AUTH_MESSAGE"),
    "CLOB_AUTH_TYPES": (".models", "CLOB_AUTH_TYPES"),
    "CLOB_BASE_URL": (".models", "CLOB_BASE_URL"),
    "COLLATERAL_OFFRAMP": (".models", "COLLATERAL_OFFRAMP"),
    "COLLATERAL_ONRAMP": (".models", "COLLATERAL_ONRAMP"),
    "CONDITIONAL_TOKENS": (".models", "CONDITIONAL_TOKENS"),
    "CTF_EXCHANGE_V2": (".models", "CTF_EXCHANGE_V2"),
    "CTF_EXCHANGE_V2_DOMAIN_NAME": (".models", "CTF_EXCHANGE_V2_DOMAIN_NAME"),
    "CTF_EXCHANGE_V2_DOMAIN_VERSION": (".models", "CTF_EXCHANGE_V2_DOMAIN_VERSION"),
    "ClobClient": (".clob_client", "ClobClient"),
    "CollateralBreakdown": (".ctf_sdk", "CollateralBreakdown"),
    "CtfEvent": (".receipt_parser", "CtfEvent"),
    "CtfParseResult": (".receipt_parser", "CtfParseResult"),
    "CtfSDK": (".ctf_sdk", "CtfSDK"),
    "DATA_API_BASE_URL": (".models", "DATA_API_BASE_URL"),
    "Erc20TransferData": (".receipt_parser", "Erc20TransferData"),
    "GAMMA_BASE_URL": (".models", "GAMMA_BASE_URL"),
    "GAS_ESTIMATES": (".ctf_sdk", "GAS_ESTIMATES"),
    "GammaMarket": (".models", "GammaMarket"),
    "GatewayPolymarketClient": (".gateway_client", "GatewayPolymarketClient"),
    "HistoricalPrice": (".models", "HistoricalPrice"),
    "HistoricalTrade": (".models", "HistoricalTrade"),
    "INDEX_SET_NO": (".ctf_sdk", "INDEX_SET_NO"),
    "INDEX_SET_YES": (".ctf_sdk", "INDEX_SET_YES"),
    "LimitOrderParams": (".models", "LimitOrderParams"),
    "MAX_UINT256": (".ctf_sdk", "MAX_UINT256"),
    "MM_MAX_PRICE": (".market_making", "MAX_PRICE"),
    "MM_MIN_PRICE": (".market_making", "MIN_PRICE"),
    "MarketFilters": (".models", "MarketFilters"),
    "MarketOrderParams": (".models", "MarketOrderParams"),
    "ModelPredictionProvider": (".signals", "ModelPredictionProvider"),
    "NEG_RISK_ADAPTER": (".models", "NEG_RISK_ADAPTER"),
    "NEG_RISK_EXCHANGE_V2": (".models", "NEG_RISK_EXCHANGE_V2"),
    "NewsAPISignalProvider": (".signals", "NewsAPISignalProvider"),
    "ORDER_TYPES": (".models", "ORDER_TYPES"),
    "OpenOrder": (".models", "OpenOrder"),
    "OrderBook": (".models", "OrderBook"),
    "OrderFilters": (".models", "OrderFilters"),
    "OrderResponse": (".models", "OrderResponse"),
    "OrderResult": (".adapter", "OrderResult"),
    "OrderSide": (".models", "OrderSide"),
    "OrderStatus": (".models", "OrderStatus"),
    "OrderType": (".models", "OrderType"),
    "PAYOUT_REDEMPTION_TOPIC": (".receipt_parser", "PAYOUT_REDEMPTION_TOPIC"),
    "POLYGON_CHAIN_ID": (".models", "POLYGON_CHAIN_ID"),
    "POLYMARKET_GAS_ESTIMATES": (".adapter", "POLYMARKET_GAS_ESTIMATES"),
    "PUSD": (".models", "PUSD"),
    "PayoutRedemptionData": (".receipt_parser", "PayoutRedemptionData"),
    "PolymarketAPIError": (".exceptions", "PolymarketAPIError"),
    "PolymarketAdapter": (".adapter", "PolymarketAdapter"),
    "PolymarketAuthenticationError": (".exceptions", "PolymarketAuthenticationError"),
    "PolymarketConfig": (".models", "PolymarketConfig"),
    "PolymarketCredentialsError": (".exceptions", "PolymarketCredentialsError"),
    "PolymarketError": (".exceptions", "PolymarketError"),
    "PolymarketEventType": (".receipt_parser", "PolymarketEventType"),
    "PolymarketInsufficientBalanceError": (".exceptions", "PolymarketInsufficientBalanceError"),
    "PolymarketInvalidPriceError": (".exceptions", "PolymarketInvalidPriceError"),
    "PolymarketMarketClosedError": (".exceptions", "PolymarketMarketClosedError"),
    "PolymarketMarketError": (".exceptions", "PolymarketMarketError"),
    "PolymarketMarketNotFoundError": (".exceptions", "PolymarketMarketNotFoundError"),
    "PolymarketMarketNotResolvedError": (".exceptions", "PolymarketMarketNotResolvedError"),
    "PolymarketMinimumOrderError": (".exceptions", "PolymarketMinimumOrderError"),
    "PolymarketOrderError": (".exceptions", "PolymarketOrderError"),
    "PolymarketOrderNotFoundError": (".exceptions", "PolymarketOrderNotFoundError"),
    "PolymarketRateLimitError": (".exceptions", "PolymarketRateLimitError"),
    "PolymarketReceiptParser": (".receipt_parser", "PolymarketReceiptParser"),
    "PolymarketRedemptionError": (".exceptions", "PolymarketRedemptionError"),
    "PolymarketSDK": (".sdk", "PolymarketSDK"),
    "PolymarketSignatureError": (".exceptions", "PolymarketSignatureError"),
    "Position": (".models", "Position"),
    "PredictionSignal": (".signals", "PredictionSignal"),
    "PriceHistory": (".models", "PriceHistory"),
    "PriceHistoryInterval": (".models", "PriceHistoryInterval"),
    "PriceLevel": (".models", "PriceLevel"),
    "Quote": (".market_making", "Quote"),
    "RedeemResult": (".adapter", "RedeemResult"),
    "RedemptionResult": (".receipt_parser", "RedemptionResult"),
    "ResolutionStatus": (".ctf_sdk", "ResolutionStatus"),
    "RiskParameters": (".market_making", "RiskParameters"),
    "SetupTxInfo": (".models", "SetupTxInfo"),
    "SignalDirection": (".signals", "SignalDirection"),
    "SignalResult": (".signals", "SignalResult"),
    "SignatureType": (".models", "SignatureType"),
    "SignedOrder": (".models", "SignedOrder"),
    "Signer": (".signer", "Signer"),
    "SocialSentimentProvider": (".signals", "SocialSentimentProvider"),
    "TRANSFER_BATCH_TOPIC": (".receipt_parser", "TRANSFER_BATCH_TOPIC"),
    "TRANSFER_SINGLE_TOPIC": (".receipt_parser", "TRANSFER_SINGLE_TOPIC"),
    "TokenBucketRateLimiter": (".clob_client", "TokenBucketRateLimiter"),
    "TokenPrice": (".models", "TokenPrice"),
    "Trade": (".models", "Trade"),
    "TradeFilters": (".models", "TradeFilters"),
    "TradeResult": (".receipt_parser", "TradeResult"),
    "TradeStatus": (".models", "TradeStatus"),
    "TransactionData": (".ctf_sdk", "TransactionData"),
    "TransferBatchData": (".receipt_parser", "TransferBatchData"),
    "TransferSingleData": (".receipt_parser", "TransferSingleData"),
    "USDCE_POLYGON": (".models", "USDCE_POLYGON"),
    "USDC_NATIVE_POLYGON": (".models", "USDC_NATIVE_POLYGON"),
    "UnsignedOrder": (".models", "UnsignedOrder"),
    "ZERO_BYTES32": (".ctf_sdk", "ZERO_BYTES32"),
    "aggregate_signals": (".signals", "aggregate_signals"),
    "build_ctf_exchange_domain": (".models", "build_ctf_exchange_domain"),
    "calculate_inventory_skew": (".market_making", "calculate_inventory_skew"),
    "calculate_optimal_spread": (".market_making", "calculate_optimal_spread"),
    "combine_with_market_price": (".signals", "combine_with_market_price"),
    "generate_quote_ladder": (".market_making", "generate_quote_ladder"),
    "make_local_signer": (".signer", "make_local_signer"),
    "make_remote_signer": (".signer", "make_remote_signer"),
    "should_requote": (".market_making", "should_requote"),
    "signer_from_env": (".signer", "signer_from_env"),
}

_registered = False


def _register_once() -> None:
    """Compatibility no-op; strategy registration metadata lives in connector.py."""
    global _registered
    if _registered:
        return
    _registered = True


def __getattr__(name: str) -> Any:
    """PEP 562 lazy attribute access."""
    if name not in _LAZY:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    submodule, attr = _LAZY[name]
    import importlib

    module = importlib.import_module(submodule, package=__name__)
    value = getattr(module, attr)
    globals()[name] = value
    _register_once()
    return value
