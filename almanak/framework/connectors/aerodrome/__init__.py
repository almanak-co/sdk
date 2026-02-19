"""Aerodrome Finance Connector.

This package provides integration with Aerodrome Finance, a Solidly-based
AMM on Base chain. Aerodrome supports dual pool types:
- Volatile pools: x*y=k formula (0.3% fee)
- Stable pools: x^3*y + y^3*x formula (0.05% fee)

Key Features:
- Token swaps (exact input)
- Liquidity provision (add/remove)
- Fungible LP tokens (not NFT positions like Uniswap V3)

Example:
    from almanak.framework.connectors.aerodrome import AerodromeAdapter, AerodromeConfig

    config = AerodromeConfig(
        chain="base",
        wallet_address="0x...",
    )
    adapter = AerodromeAdapter(config)

    # Execute a volatile pool swap
    result = adapter.swap_exact_input(
        token_in="USDC",
        token_out="WETH",
        amount_in=Decimal("1000"),
        stable=False,
    )

    # Execute a stable pool swap
    result = adapter.swap_exact_input(
        token_in="USDC",
        token_out="USDbC",
        amount_in=Decimal("1000"),
        stable=True,
    )
"""

from .adapter import (
    AerodromeAdapter,
    AerodromeConfig,
    LiquidityResult,
    PoolType,
    SwapQuote,
    SwapResult,
    SwapType,
    TransactionData,
)
from .receipt_parser import (
    BURN_EVENT_TOPIC,
    EVENT_NAME_TO_TYPE,
    EVENT_TOPICS,
    MINT_EVENT_TOPIC,
    SWAP_EVENT_TOPIC,
    TOPIC_TO_EVENT,
    AerodromeEvent,
    AerodromeEventType,
    AerodromeReceiptParser,
    BurnEventData,
    MintEventData,
    ParsedLiquidityResult,
    ParsedSwapResult,
    ParseResult,
    SwapEventData,
    TransferEventData,
)
from .sdk import (
    AERODROME_ADDRESSES,
    AERODROME_GAS_ESTIMATES,
    MAX_UINT256,
    AerodromeSDK,
    AerodromeSDKError,
    InsufficientLiquidityError,
    PoolInfo,
    PoolNotFoundError,
    SwapRoute,
)

# Import SDK SwapQuote with alias to avoid naming conflict with adapter SwapQuote.
# Both classes serve different purposes:
# - SwapQuote (from adapter): High-level quote for adapter operations
# - SDKSwapQuote (from sdk): Low-level quote with protocol-specific details
# This pattern is intentional and consistent across connectors (uniswap_v3, traderjoe_v2, aerodrome).
from .sdk import (
    SwapQuote as SDKSwapQuote,
)

__all__ = [
    # SDK
    "AerodromeSDK",
    "PoolInfo",
    "SwapRoute",
    "SDKSwapQuote",
    "AerodromeSDKError",
    "PoolNotFoundError",
    "InsufficientLiquidityError",
    # Adapter
    "AerodromeAdapter",
    "AerodromeConfig",
    "SwapQuote",
    "SwapResult",
    "SwapType",
    "PoolType",
    "LiquidityResult",
    "TransactionData",
    # Receipt Parser
    "AerodromeReceiptParser",
    "AerodromeEvent",
    "AerodromeEventType",
    "SwapEventData",
    "MintEventData",
    "BurnEventData",
    "TransferEventData",
    "ParsedSwapResult",
    "ParsedLiquidityResult",
    "ParseResult",
    # Event Topics
    "EVENT_TOPICS",
    "TOPIC_TO_EVENT",
    "EVENT_NAME_TO_TYPE",
    "SWAP_EVENT_TOPIC",
    "MINT_EVENT_TOPIC",
    "BURN_EVENT_TOPIC",
    # Constants
    "AERODROME_ADDRESSES",
    "AERODROME_GAS_ESTIMATES",
    "MAX_UINT256",
]
