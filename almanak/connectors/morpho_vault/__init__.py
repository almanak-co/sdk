"""MetaMorpho Vault Connector.

This module provides adapters and utilities for interacting with MetaMorpho vaults,
the ERC-4626 vault layer that aggregates capital across Morpho Blue lending markets.

MetaMorpho Features:
- ERC-4626 compliant vault deposits and redemptions
- Passive yield optimization with curator-managed allocation
- Multi-market capital allocation across Morpho Blue markets
- Transparent share pricing via convertToAssets/convertToShares

Supported Chains:
- Ethereum
- Base

Example:
    from almanak.connectors.morpho_vault import (
        MetaMorphoAdapter,
        MetaMorphoConfig,
        MetaMorphoReceiptParser,
        MetaMorphoSDK,
        create_test_adapter,
    )
    from decimal import Decimal

    # Initialize adapter
    config = MetaMorphoConfig(chain="ethereum", wallet_address="0x...")
    adapter = MetaMorphoAdapter(config, gateway_client=gateway_client)

    # Deposit assets
    result = adapter.deposit(
        vault_address="0xBEEF01735c132Ada46AA9aA4c54623cAA92A64CB",
        amount=Decimal("1000"),
    )

    # Redeem all shares
    result = adapter.redeem(
        vault_address="0xBEEF01735c132Ada46AA9aA4c54623cAA92A64CB",
        shares="all",
    )

    # Parse transaction receipts
    parser = MetaMorphoReceiptParser()
    parse_result = parser.parse_receipt(receipt)
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .adapter import (
        MetaMorphoAdapter,
        MetaMorphoConfig,
        TransactionResult,
        create_test_adapter,
    )
    from .receipt_parser import (
        EVENT_NAME_TO_TYPE,
        EVENT_TOPICS,
        TOPIC_TO_EVENT,
        MetaMorphoEvent,
        MetaMorphoEventType,
        MetaMorphoReceiptParser,
        ParseResult,
        TransferEventData,
        VaultDepositEventData,
        VaultWithdrawEventData,
    )
    from .sdk import (
        SUPPORTED_CHAINS,
        DepositExceedsCapError,
        InsufficientSharesError,
        MetaMorphoSDK,
        MetaMorphoSDKError,
        RPCError,
        UnsupportedChainError,
        VaultInfo,
        VaultMarketConfig,
        VaultNotFoundError,
        VaultPosition,
    )

__all__ = [
    "DepositExceedsCapError",
    "EVENT_NAME_TO_TYPE",
    "EVENT_TOPICS",
    "InsufficientSharesError",
    "MetaMorphoAdapter",
    "MetaMorphoConfig",
    "MetaMorphoEvent",
    "MetaMorphoEventType",
    "MetaMorphoReceiptParser",
    "MetaMorphoSDK",
    "MetaMorphoSDKError",
    "ParseResult",
    "RPCError",
    "SUPPORTED_CHAINS",
    "TOPIC_TO_EVENT",
    "TransactionResult",
    "TransferEventData",
    "UnsupportedChainError",
    "VaultDepositEventData",
    "VaultInfo",
    "VaultMarketConfig",
    "VaultNotFoundError",
    "VaultPosition",
    "VaultWithdrawEventData",
    "create_test_adapter",
]

_LAZY: dict[str, tuple[str, str]] = {
    "DepositExceedsCapError": (".sdk", "DepositExceedsCapError"),
    "EVENT_NAME_TO_TYPE": (".receipt_parser", "EVENT_NAME_TO_TYPE"),
    "EVENT_TOPICS": (".receipt_parser", "EVENT_TOPICS"),
    "InsufficientSharesError": (".sdk", "InsufficientSharesError"),
    "MetaMorphoAdapter": (".adapter", "MetaMorphoAdapter"),
    "MetaMorphoConfig": (".adapter", "MetaMorphoConfig"),
    "MetaMorphoEvent": (".receipt_parser", "MetaMorphoEvent"),
    "MetaMorphoEventType": (".receipt_parser", "MetaMorphoEventType"),
    "MetaMorphoReceiptParser": (".receipt_parser", "MetaMorphoReceiptParser"),
    "MetaMorphoSDK": (".sdk", "MetaMorphoSDK"),
    "MetaMorphoSDKError": (".sdk", "MetaMorphoSDKError"),
    "ParseResult": (".receipt_parser", "ParseResult"),
    "RPCError": (".sdk", "RPCError"),
    "SUPPORTED_CHAINS": (".sdk", "SUPPORTED_CHAINS"),
    "TOPIC_TO_EVENT": (".receipt_parser", "TOPIC_TO_EVENT"),
    "TransactionResult": (".adapter", "TransactionResult"),
    "TransferEventData": (".receipt_parser", "TransferEventData"),
    "UnsupportedChainError": (".sdk", "UnsupportedChainError"),
    "VaultDepositEventData": (".receipt_parser", "VaultDepositEventData"),
    "VaultInfo": (".sdk", "VaultInfo"),
    "VaultMarketConfig": (".sdk", "VaultMarketConfig"),
    "VaultNotFoundError": (".sdk", "VaultNotFoundError"),
    "VaultPosition": (".sdk", "VaultPosition"),
    "VaultWithdrawEventData": (".receipt_parser", "VaultWithdrawEventData"),
    "create_test_adapter": (".adapter", "create_test_adapter"),
}

_registered = False


def _register_once() -> None:
    """Compatibility no-op; strategy registration lives in connector.py."""
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
