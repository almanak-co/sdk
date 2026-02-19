"""Hyperliquid Connector.

This module provides an adapter for interacting with Hyperliquid perpetual futures
exchange, supporting order management, position queries, and L1/L2 message signing.

Hyperliquid is a decentralized perpetual futures exchange supporting:
- Long and short positions with up to 50x leverage
- Limit and market orders with various time-in-force options
- Cross and isolated margin modes
- REST API and WebSocket for real-time data

Supported networks:
- Mainnet (api.hyperliquid.xyz)
- Testnet (api.hyperliquid-testnet.xyz)

Example:
    from almanak.framework.connectors.hyperliquid import HyperliquidAdapter, HyperliquidConfig

    config = HyperliquidConfig(
        network="mainnet",
        wallet_address="0x...",
        private_key="0x...",
    )
    adapter = HyperliquidAdapter(config)

    # Place a limit order
    result = adapter.place_order(
        asset="ETH",
        is_buy=True,
        size=Decimal("0.1"),
        price=Decimal("2000"),
    )

    # Check open orders
    orders = adapter.get_open_orders()

    # Get position
    position = adapter.get_position("ETH")
"""

from .adapter import (
    # Constants
    HYPERLIQUID_API_URLS,
    HYPERLIQUID_ASSETS,
    HYPERLIQUID_CHAIN_IDS,
    HYPERLIQUID_GAS_ESTIMATES,
    HYPERLIQUID_WS_URLS,
    CancelResult,
    # Signers
    EIP712Signer,
    ExternalSigner,
    # Main adapter
    HyperliquidAdapter,
    HyperliquidConfig,
    HyperliquidMarginMode,
    HyperliquidNetwork,
    HyperliquidOrder,
    HyperliquidOrderSide,
    HyperliquidOrderStatus,
    # Enums
    HyperliquidOrderType,
    # Position and Order types
    HyperliquidPosition,
    HyperliquidPositionSide,
    HyperliquidTimeInForce,
    MessageSigner,
    # Results
    OrderResult,
    SignedAction,
)

__all__ = [
    # Adapter
    "HyperliquidAdapter",
    "HyperliquidConfig",
    # Position and Order
    "HyperliquidPosition",
    "HyperliquidOrder",
    # Enums
    "HyperliquidOrderType",
    "HyperliquidOrderSide",
    "HyperliquidOrderStatus",
    "HyperliquidPositionSide",
    "HyperliquidTimeInForce",
    "HyperliquidMarginMode",
    "HyperliquidNetwork",
    # Results
    "OrderResult",
    "CancelResult",
    "SignedAction",
    # Signers
    "EIP712Signer",
    "ExternalSigner",
    "MessageSigner",
    # Constants
    "HYPERLIQUID_API_URLS",
    "HYPERLIQUID_WS_URLS",
    "HYPERLIQUID_CHAIN_IDS",
    "HYPERLIQUID_ASSETS",
    "HYPERLIQUID_GAS_ESTIMATES",
]
