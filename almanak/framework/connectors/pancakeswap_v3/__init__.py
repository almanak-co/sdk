"""PancakeSwap V3 Connector.

This module provides an adapter for interacting with PancakeSwap V3,
which is a Uniswap V3 fork with different fee tiers and addresses.

PancakeSwap V3 is a decentralized exchange supporting:
- Exact input swaps (swap specific amount of input token)
- Exact output swaps (receive specific amount of output token)
- Multiple fee tiers (100, 500, 2500, 10000 bps)

Supported chains:
- BNB Smart Chain (BSC)
- Ethereum
- Arbitrum

Example:
    from almanak.framework.connectors.pancakeswap_v3 import (
        PancakeSwapV3Adapter,
        PancakeSwapV3Config,
    )

    config = PancakeSwapV3Config(
        chain="bnb",
        wallet_address="0x...",
    )
    adapter = PancakeSwapV3Adapter(config)

    # Swap exact input
    result = adapter.swap_exact_input(
        token_in="USDT",
        token_out="WBNB",
        amount_in=Decimal("100"),
    )
"""

from .adapter import (
    # Constants
    DEFAULT_GAS_ESTIMATES,
    EXACT_INPUT_SINGLE_SELECTOR,
    EXACT_OUTPUT_SINGLE_SELECTOR,
    FEE_TIERS,
    PANCAKESWAP_V3_ADDRESSES,
    # Adapter
    PancakeSwapV3Adapter,
    PancakeSwapV3Config,
    # Data classes
    TransactionResult,
)
from .receipt_parser import (
    EVENT_NAME_TO_TYPE,
    EVENT_TOPICS,
    TOPIC_TO_EVENT,
    PancakeSwapV3EventType,
    PancakeSwapV3ReceiptParser,
    ParseResult,
    SwapEventData,
)

__all__ = [
    # Adapter
    "PancakeSwapV3Adapter",
    "PancakeSwapV3Config",
    # Receipt parser
    "PancakeSwapV3ReceiptParser",
    "PancakeSwapV3EventType",
    "SwapEventData",
    "ParseResult",
    # Data classes
    "TransactionResult",
    # Constants
    "PANCAKESWAP_V3_ADDRESSES",
    "FEE_TIERS",
    "EXACT_INPUT_SINGLE_SELECTOR",
    "EXACT_OUTPUT_SINGLE_SELECTOR",
    "DEFAULT_GAS_ESTIMATES",
    "EVENT_TOPICS",
    "TOPIC_TO_EVENT",
    "EVENT_NAME_TO_TYPE",
]
