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
    from almanak.connectors.kraken import (
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
    from almanak.connectors.kraken import KrakenSDK

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

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
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
    from .receipt_resolver import (
        ExecutionDetails,
        KrakenReceiptResolver,
        TokenAmount,
    )
    from .sdk import KrakenSDK
    from .token_resolver import (
        KrakenChainMapper,
        KrakenTokenResolver,
        chain_mapper,
        token_resolver,
    )

__all__ = [
    "ActionBundle",
    "ActionType",
    "CEXAction",
    "CEXIdempotencyKey",
    "CEXOperationType",
    "CEXRiskConfig",
    "ExecutionContext",
    "ExecutionDetails",
    "KrakenAPIError",
    "KrakenAdapter",
    "KrakenAuthenticationError",
    "KrakenBalance",
    "KrakenChainMapper",
    "KrakenChainNotSupportedError",
    "KrakenConfig",
    "KrakenCredentials",
    "KrakenDepositError",
    "KrakenDepositStatus",
    "KrakenError",
    "KrakenInsufficientFundsError",
    "KrakenMarketInfo",
    "KrakenMinimumOrderError",
    "KrakenOrderCancelledError",
    "KrakenOrderError",
    "KrakenOrderNotFoundError",
    "KrakenOrderStatus",
    "KrakenRateLimitError",
    "KrakenReceiptResolver",
    "KrakenSDK",
    "KrakenTimeoutError",
    "KrakenTokenResolver",
    "KrakenUnknownAssetError",
    "KrakenUnknownPairError",
    "KrakenWithdrawStatus",
    "KrakenWithdrawalAddressNotWhitelistedError",
    "KrakenWithdrawalError",
    "KrakenWithdrawalLimitExceededError",
    "TokenAmount",
    "VenueType",
    "chain_mapper",
    "token_resolver",
]

_LAZY: dict[str, tuple[str, str]] = {
    "ActionBundle": (".adapter", "ActionBundle"),
    "ActionType": (".adapter", "ActionType"),
    "CEXAction": (".adapter", "CEXAction"),
    "CEXIdempotencyKey": (".models", "CEXIdempotencyKey"),
    "CEXOperationType": (".models", "CEXOperationType"),
    "CEXRiskConfig": (".models", "CEXRiskConfig"),
    "ExecutionContext": (".adapter", "ExecutionContext"),
    "ExecutionDetails": (".receipt_resolver", "ExecutionDetails"),
    "KrakenAPIError": (".exceptions", "KrakenAPIError"),
    "KrakenAdapter": (".adapter", "KrakenAdapter"),
    "KrakenAuthenticationError": (".exceptions", "KrakenAuthenticationError"),
    "KrakenBalance": (".models", "KrakenBalance"),
    "KrakenChainMapper": (".token_resolver", "KrakenChainMapper"),
    "KrakenChainNotSupportedError": (".exceptions", "KrakenChainNotSupportedError"),
    "KrakenConfig": (".models", "KrakenConfig"),
    "KrakenCredentials": (".models", "KrakenCredentials"),
    "KrakenDepositError": (".exceptions", "KrakenDepositError"),
    "KrakenDepositStatus": (".models", "KrakenDepositStatus"),
    "KrakenError": (".exceptions", "KrakenError"),
    "KrakenInsufficientFundsError": (".exceptions", "KrakenInsufficientFundsError"),
    "KrakenMarketInfo": (".models", "KrakenMarketInfo"),
    "KrakenMinimumOrderError": (".exceptions", "KrakenMinimumOrderError"),
    "KrakenOrderCancelledError": (".exceptions", "KrakenOrderCancelledError"),
    "KrakenOrderError": (".exceptions", "KrakenOrderError"),
    "KrakenOrderNotFoundError": (".exceptions", "KrakenOrderNotFoundError"),
    "KrakenOrderStatus": (".models", "KrakenOrderStatus"),
    "KrakenRateLimitError": (".exceptions", "KrakenRateLimitError"),
    "KrakenReceiptResolver": (".receipt_resolver", "KrakenReceiptResolver"),
    "KrakenSDK": (".sdk", "KrakenSDK"),
    "KrakenTimeoutError": (".exceptions", "KrakenTimeoutError"),
    "KrakenTokenResolver": (".token_resolver", "KrakenTokenResolver"),
    "KrakenUnknownAssetError": (".exceptions", "KrakenUnknownAssetError"),
    "KrakenUnknownPairError": (".exceptions", "KrakenUnknownPairError"),
    "KrakenWithdrawStatus": (".models", "KrakenWithdrawStatus"),
    "KrakenWithdrawalAddressNotWhitelistedError": (".exceptions", "KrakenWithdrawalAddressNotWhitelistedError"),
    "KrakenWithdrawalError": (".exceptions", "KrakenWithdrawalError"),
    "KrakenWithdrawalLimitExceededError": (".exceptions", "KrakenWithdrawalLimitExceededError"),
    "TokenAmount": (".receipt_resolver", "TokenAmount"),
    "VenueType": (".adapter", "VenueType"),
    "chain_mapper": (".token_resolver", "chain_mapper"),
    "token_resolver": (".token_resolver", "token_resolver"),
}

_registered = False


def _register_once() -> None:
    """Fire ``register_connector`` once on first strategy-side access.

    Deferred so importing the connector's gateway-side surface during
    gateway boot does not pull ``framework.intents.vocabulary`` into the
    partially-initialised config-init chain (VIB-4835).
    """
    global _registered
    if _registered:
        return
    from almanak.connectors._strategy_base.registry import register_connector
    from almanak.framework.intents.vocabulary import IntentType

    register_connector(name="kraken", intents=(IntentType.SWAP,), chains=None)
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
