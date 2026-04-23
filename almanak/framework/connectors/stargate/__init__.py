"""Stargate Bridge Adapter Package.

This package provides the StargateBridgeAdapter for cross-chain transfers
via the Stargate protocol built on LayerZero messaging.

Stargate Protocol:
- Unified liquidity pools across chains
- LayerZero for cross-chain messaging
- Instant guaranteed finality
- Native asset transfers (no wrapped tokens)

Example:
    from almanak.framework.connectors.stargate import StargateBridgeAdapter

    adapter = StargateBridgeAdapter()
    quote = adapter.get_quote(
        token="USDC",
        amount=Decimal("1000"),
        from_chain="arbitrum",
        to_chain="optimism",
    )
"""

from .adapter import (
    STARGATE_CHAIN_IDS,
    STARGATE_POOL_IDS,
    STARGATE_ROUTER_ADDRESSES,
    STARGATE_SUPPORTED_TOKENS,
    StargateBridgeAdapter,
    StargateConfig,
    StargateError,
    StargateQuoteError,
    StargateStatusError,
    StargateTransactionError,
)
from .receipt_parser import StargateReceiptParser

__all__ = [
    "StargateBridgeAdapter",
    "StargateConfig",
    "StargateError",
    "StargateQuoteError",
    "StargateReceiptParser",
    "StargateTransactionError",
    "StargateStatusError",
    "STARGATE_CHAIN_IDS",
    "STARGATE_ROUTER_ADDRESSES",
    "STARGATE_POOL_IDS",
    "STARGATE_SUPPORTED_TOKENS",
]
