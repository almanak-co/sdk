"""Ethena Connector.

This module provides an adapter for interacting with Ethena synthetic dollar protocol.

Ethena is a synthetic dollar protocol supporting:
- Stake USDe to receive sUSDe (yield-bearing)
- Unstake sUSDe to receive USDe (with cooldown period)

Supported chains:
- Ethereum (full staking + unstaking)

sUSDe is an ERC4626 vault token that accrues yield from delta-neutral strategies.

Example:
    from almanak.framework.connectors.ethena import EthenaAdapter, EthenaConfig

    config = EthenaConfig(
        chain="ethereum",
        wallet_address="0x...",
    )
    adapter = EthenaAdapter(config)

    # Stake USDe to receive sUSDe
    result = adapter.stake_usde(amount=Decimal("1000.0"))
"""

from .adapter import (
    # Constants
    DEFAULT_GAS_ESTIMATES,
    ETHENA_ADDRESSES,
    ETHENA_COOLDOWN_ASSETS_SELECTOR,
    ETHENA_COOLDOWN_SHARES_SELECTOR,
    ETHENA_DEPOSIT_SELECTOR,
    ETHENA_UNSTAKE_SELECTOR,
    # Adapter
    EthenaAdapter,
    EthenaConfig,
    # Data classes
    TransactionResult,
)
from .receipt_parser import (
    # Constants
    ETHENA_EVENT_SIGNATURES,
    EVENT_TOPICS,
    TOPIC_TO_EVENT,
    # Event type enum
    EthenaEventType,
    # Parser
    EthenaReceiptParser,
    # Event data classes
    ParseResult,
    StakeEventData,
    UnstakeEventData,
    WithdrawEventData,
)

__all__ = [
    # Adapter
    "EthenaAdapter",
    "EthenaConfig",
    # Data classes
    "TransactionResult",
    # Constants
    "ETHENA_ADDRESSES",
    "ETHENA_DEPOSIT_SELECTOR",
    "ETHENA_COOLDOWN_ASSETS_SELECTOR",
    "ETHENA_COOLDOWN_SHARES_SELECTOR",
    "ETHENA_UNSTAKE_SELECTOR",
    "DEFAULT_GAS_ESTIMATES",
    # Receipt parser
    "EthenaReceiptParser",
    "EthenaEventType",
    "StakeEventData",
    "WithdrawEventData",
    "UnstakeEventData",  # Backward compatibility alias for WithdrawEventData
    "ParseResult",
    "EVENT_TOPICS",
    "ETHENA_EVENT_SIGNATURES",  # Alias for EVENT_TOPICS
    "TOPIC_TO_EVENT",
]
