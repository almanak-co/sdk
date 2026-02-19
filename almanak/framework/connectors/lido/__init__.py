"""Lido Connector.

This module provides an adapter for interacting with Lido liquid staking protocol.

Lido is a decentralized liquid staking protocol supporting:
- Stake ETH to receive stETH
- Wrap stETH to wstETH (non-rebasing)
- Unwrap wstETH to stETH

Supported chains:
- Ethereum (full staking + wrap/unwrap)
- Arbitrum, Optimism, Polygon (wstETH only)

Example:
    from almanak.framework.connectors.lido import LidoAdapter, LidoConfig

    config = LidoConfig(
        chain="ethereum",
        wallet_address="0x...",
    )
    adapter = LidoAdapter(config)

    # Stake ETH to receive stETH
    result = adapter.stake(amount=Decimal("1.0"))

    # Wrap stETH to wstETH
    result = adapter.wrap(amount=Decimal("1.0"))
"""

from .adapter import (
    # Constants
    DEFAULT_GAS_ESTIMATES,
    LIDO_ADDRESSES,
    LIDO_STAKE_SELECTOR,
    LIDO_UNWRAP_SELECTOR,
    LIDO_WRAP_SELECTOR,
    # Adapter
    LidoAdapter,
    LidoConfig,
    # Data classes
    TransactionResult,
)
from .receipt_parser import (
    # Constants
    EVENT_TOPICS,
    TOPIC_TO_EVENT,
    # Event type enum
    LidoEventType,
    # Parser
    LidoReceiptParser,
    # Event data classes
    ParseResult,
    StakeEventData,
    UnwrapEventData,
    WithdrawalClaimedEventData,
    WithdrawalRequestedEventData,
    WrapEventData,
)

__all__ = [
    # Adapter
    "LidoAdapter",
    "LidoConfig",
    # Data classes
    "TransactionResult",
    # Constants
    "LIDO_ADDRESSES",
    "LIDO_STAKE_SELECTOR",
    "LIDO_WRAP_SELECTOR",
    "LIDO_UNWRAP_SELECTOR",
    "DEFAULT_GAS_ESTIMATES",
    # Receipt Parser
    "LidoReceiptParser",
    "LidoEventType",
    "StakeEventData",
    "WrapEventData",
    "UnwrapEventData",
    "WithdrawalRequestedEventData",
    "WithdrawalClaimedEventData",
    "ParseResult",
    "EVENT_TOPICS",
    "TOPIC_TO_EVENT",
]
