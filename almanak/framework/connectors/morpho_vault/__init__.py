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
    from almanak.framework.connectors.morpho_vault import (
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
    # Adapter
    "MetaMorphoAdapter",
    "MetaMorphoConfig",
    "TransactionResult",
    "create_test_adapter",
    # SDK
    "MetaMorphoSDK",
    "VaultInfo",
    "VaultPosition",
    "VaultMarketConfig",
    # SDK Exceptions
    "MetaMorphoSDKError",
    "VaultNotFoundError",
    "UnsupportedChainError",
    "RPCError",
    "DepositExceedsCapError",
    "InsufficientSharesError",
    # SDK Constants
    "SUPPORTED_CHAINS",
    # Receipt Parser
    "MetaMorphoReceiptParser",
    "MetaMorphoEvent",
    "MetaMorphoEventType",
    "ParseResult",
    # Event data classes
    "VaultDepositEventData",
    "VaultWithdrawEventData",
    "TransferEventData",
    # Event constants
    "EVENT_TOPICS",
    "TOPIC_TO_EVENT",
    "EVENT_NAME_TO_TYPE",
]
