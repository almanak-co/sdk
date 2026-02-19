"""
Pendle Protocol Connector

This module provides integration with Pendle Finance, a permissionless
yield-trading protocol that enables:
- Tokenizing yield-bearing assets into PT (Principal) and YT (Yield) tokens
- Trading PT and YT on Pendle's AMM
- Providing liquidity to PT/SY pools
- Redeeming PT at maturity

Components:
- PendleSDK: Low-level protocol interactions
- PendleAdapter: ActionType to SDK mapping
- PendleReceiptParser: Transaction receipt parsing

Supported Chains:
- Arbitrum (primary)
- Ethereum

Example:
    from almanak.framework.connectors.pendle import (
        PendleSDK,
        PendleAdapter,
        PendleReceiptParser,
    )

    # Create SDK
    sdk = PendleSDK(rpc_url="https://arb1.arbitrum.io/rpc", chain="arbitrum")

    # Build swap transaction
    tx = sdk.build_swap_exact_token_for_pt(
        receiver="0x...",
        market="0x...",
        token_in="0x...",
        amount_in=10**18,
        min_pt_out=10**18,
    )
"""

from .adapter import (
    PendleAdapter,
    PendleLPParams,
    PendleRedeemParams,
    PendleSwapParams,
    get_pendle_adapter,
)
from .receipt_parser import (
    EVENT_TOPICS,
    BurnEventData,
    MintEventData,
    ParsedSwapResult,
    ParseResult,
    PendleEvent,
    PendleEventType,
    PendleReceiptParser,
    RedeemPYEventData,
    SwapEventData,
    TransferEventData,
)
from .sdk import (
    PENDLE_ADDRESSES,
    PENDLE_GAS_ESTIMATES,
    LiquidityParams,
    MarketInfo,
    PendleActionType,
    PendleQuote,
    PendleSDK,
    PendleTransactionData,
    SwapParams,
    get_pendle_sdk,
)

__all__ = [
    # SDK
    "PendleSDK",
    "PendleActionType",
    "PendleTransactionData",
    "PendleQuote",
    "MarketInfo",
    "SwapParams",
    "LiquidityParams",
    "get_pendle_sdk",
    "PENDLE_ADDRESSES",
    "PENDLE_GAS_ESTIMATES",
    # Adapter
    "PendleAdapter",
    "PendleSwapParams",
    "PendleLPParams",
    "PendleRedeemParams",
    "get_pendle_adapter",
    # Receipt Parser
    "PendleReceiptParser",
    "PendleEvent",
    "PendleEventType",
    "SwapEventData",
    "MintEventData",
    "BurnEventData",
    "RedeemPYEventData",
    "TransferEventData",
    "ParsedSwapResult",
    "ParseResult",
    "EVENT_TOPICS",
]
