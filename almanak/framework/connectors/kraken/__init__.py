"""Kraken CEX Connector.

This module provides the Kraken CEX adapter for executing trades,
deposits, and withdrawals on Kraken exchange.

Supported operations:
- Spot trading (market orders)
- Multi-chain withdrawals (Arbitrum, Optimism, Ethereum)
- Deposit tracking

Key features:
- Idempotent execution with userref/refid tracking
- Crash recovery for pending operations
- Exponential backoff polling
- Token/chain resolution for stack-v2 compatibility

Example:
    from almanak.framework.connectors.kraken import (
        KrakenAdapter,
        KrakenConfig,
        KrakenCredentials,
        ExecutionContext,
    )

    # Setup
    credentials = KrakenCredentials.from_env()
    config = KrakenConfig(credentials=credentials)
    adapter = KrakenAdapter(config)

    # Compile intent
    context = ExecutionContext(
        chain="arbitrum",
        wallet_address="0x...",
        token_decimals={"USDC": 6, "ETH": 18},
    )

    # From an intent with venue="kraken"
    bundle = adapter.compile_intent(intent, context)

    # Execute (typically done by orchestrator)
    for action in bundle.actions:
        key, result_id = await adapter.execute_action(action, context)
        details = await adapter.resolve_action(action, key, context)

    # Lower-level SDK usage
    from almanak.framework.connectors.kraken import KrakenSDK

    sdk = KrakenSDK(credentials)

    # Get balance
    balance = sdk.get_balance("USDC", chain="arbitrum")

    # Execute swap
    userref = sdk.generate_userref()
    txid = sdk.swap(
        asset_in="USDC",
        asset_out="ETH",
        amount_in=1000_000000,  # 1000 USDC
        decimals_in=6,
        userref=userref,
    )

    # Check status
    status = sdk.get_swap_status(txid, userref)
"""

# Exceptions
# Adapter
from .adapter import (
    ActionBundle,
    ActionType,
    CEXAction,
    ExecutionContext,
    KrakenAdapter,
    VenueType,
)
from .exceptions import (
    KrakenAPIError,
    KrakenAuthenticationError,
    KrakenChainNotSupportedError,
    KrakenDepositError,
    KrakenError,
    KrakenInsufficientFundsError,
    KrakenMinimumOrderError,
    KrakenOrderCancelledError,
    KrakenOrderError,
    KrakenOrderNotFoundError,
    KrakenRateLimitError,
    KrakenTimeoutError,
    KrakenUnknownAssetError,
    KrakenUnknownPairError,
    KrakenWithdrawalAddressNotWhitelistedError,
    KrakenWithdrawalError,
    KrakenWithdrawalLimitExceededError,
)

# Models
from .models import (
    CEXIdempotencyKey,
    CEXOperationType,
    CEXRiskConfig,
    KrakenBalance,
    KrakenConfig,
    KrakenCredentials,
    KrakenDepositStatus,
    KrakenMarketInfo,
    KrakenOrderStatus,
    KrakenWithdrawStatus,
)

# Receipt Resolver
from .receipt_resolver import (
    ExecutionDetails,
    KrakenReceiptResolver,
    TokenAmount,
)

# SDK
from .sdk import KrakenSDK

# Token/Chain Resolution
from .token_resolver import (
    KrakenChainMapper,
    KrakenTokenResolver,
    chain_mapper,
    token_resolver,
)

__all__ = [
    # Exceptions
    "KrakenError",
    "KrakenAPIError",
    "KrakenAuthenticationError",
    "KrakenRateLimitError",
    "KrakenInsufficientFundsError",
    "KrakenMinimumOrderError",
    "KrakenUnknownAssetError",
    "KrakenUnknownPairError",
    "KrakenWithdrawalError",
    "KrakenWithdrawalAddressNotWhitelistedError",
    "KrakenWithdrawalLimitExceededError",
    "KrakenDepositError",
    "KrakenOrderError",
    "KrakenOrderNotFoundError",
    "KrakenOrderCancelledError",
    "KrakenChainNotSupportedError",
    "KrakenTimeoutError",
    # Enums
    "KrakenOrderStatus",
    "KrakenWithdrawStatus",
    "KrakenDepositStatus",
    "CEXOperationType",
    "VenueType",
    "ActionType",
    # Credentials & Config
    "KrakenCredentials",
    "KrakenConfig",
    "CEXRiskConfig",
    # Models
    "KrakenMarketInfo",
    "KrakenBalance",
    "CEXIdempotencyKey",
    "CEXAction",
    "ActionBundle",
    "ExecutionContext",
    # Token/Chain Resolution
    "KrakenTokenResolver",
    "KrakenChainMapper",
    "token_resolver",
    "chain_mapper",
    # SDK
    "KrakenSDK",
    # Receipt Resolution
    "TokenAmount",
    "ExecutionDetails",
    "KrakenReceiptResolver",
    # Adapter
    "KrakenAdapter",
]
