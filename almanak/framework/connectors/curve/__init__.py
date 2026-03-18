"""Curve Finance Connector.

This module provides the Curve Finance adapter for executing swaps and
managing liquidity positions on Curve pools across multiple chains.

Supported chains:
- Ethereum
- Arbitrum

Supported operations:
- SWAP: Token swaps via Curve pools (StableSwap, CryptoSwap, Tricrypto)
- LP_OPEN: Add liquidity to Curve pools
- LP_CLOSE: Remove liquidity from Curve pools

Example:
    from almanak.framework.connectors.curve import CurveAdapter, CurveConfig

    config = CurveConfig(
        chain="ethereum",
        wallet_address="0x...",
    )
    adapter = CurveAdapter(config)

    # Execute a swap
    result = adapter.swap(
        pool_address="0xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7",  # 3pool
        token_in="USDC",
        token_out="DAI",
        amount_in=Decimal("1000"),
    )

    # Add liquidity
    lp_result = adapter.add_liquidity(
        pool_address="0xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7",
        amounts=[Decimal("1000"), Decimal("1000"), Decimal("1000")],  # DAI, USDC, USDT
    )
"""

from .adapter import (
    CURVE_ADDRESSES,
    CURVE_GAS_ESTIMATES,
    CURVE_POOLS,
    CurveAdapter,
    CurveConfig,
    LiquidityResult,
    PoolInfo,
    PoolType,
    SwapResult,
    TransactionData,
)
from .receipt_parser import (
    EVENT_TOPICS,
    TOPIC_TO_EVENT,
    AddLiquidityEventData,
    CurveEvent,
    CurveEventType,
    CurveReceiptParser,
    ParseResult,
    RemoveLiquidityEventData,
    SwapEventData,
)

__all__ = [
    # Adapter exports
    "CurveAdapter",
    "CurveConfig",
    "SwapResult",
    "LiquidityResult",
    "PoolInfo",
    "PoolType",
    "TransactionData",
    # Constants
    "CURVE_ADDRESSES",
    "CURVE_POOLS",
    "CURVE_GAS_ESTIMATES",
    # Receipt parser exports
    "CurveReceiptParser",
    "CurveEvent",
    "CurveEventType",
    "SwapEventData",
    "AddLiquidityEventData",
    "RemoveLiquidityEventData",
    "ParseResult",
    "EVENT_TOPICS",
    "TOPIC_TO_EVENT",
]
