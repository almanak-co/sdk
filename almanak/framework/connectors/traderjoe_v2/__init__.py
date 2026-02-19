"""TraderJoe Liquidity Book V2 Connector.

This module provides the TraderJoe V2 adapter for executing swaps and
managing liquidity positions on TraderJoe V2's Liquidity Book on Avalanche.

TraderJoe V2 Architecture:
- LBRouter: Main entry point for swaps and liquidity operations
- LBFactory: Creates and manages LBPair pools
- LBPair: Liquidity pool with discrete bins (not continuous ticks)

Key Concepts:
- Bin: Discrete price point (unlike Uniswap V3's continuous ticks)
- BinStep: Fee tier in basis points between bins (e.g., 20 = 0.2%)
- Fungible LP Tokens: ERC1155-like tokens for each bin (no NFTs)

Supported chains:
- Avalanche (Chain ID: 43114)

Example:
    from almanak.framework.connectors.traderjoe_v2 import TraderJoeV2Adapter, TraderJoeV2Config

    config = TraderJoeV2Config(
        chain="avalanche",
        wallet_address="0x...",
        rpc_url="https://api.avax.network/ext/bc/C/rpc",
    )
    adapter = TraderJoeV2Adapter(config)

    # Get a swap quote
    quote = adapter.get_swap_quote(
        token_in="WAVAX",
        token_out="USDC",
        amount_in=Decimal("1.0"),
        bin_step=20,
    )

    # Use SDK for lower-level operations
    from almanak.framework.connectors.traderjoe_v2 import TraderJoeV2SDK

    sdk = TraderJoeV2SDK(chain="avalanche", rpc_url="https://api.avax.network/ext/bc/C/rpc")
    pool = sdk.get_pool_address(wavax_addr, usdc_addr, bin_step=20)
"""

from .adapter import (
    LiquidityPosition,
    SwapQuote,
    SwapResult,
    SwapType,
    TraderJoeV2Adapter,
    TraderJoeV2Config,
    TransactionData,
)
from .receipt_parser import (
    DEPOSITED_TO_BINS_TOPIC,
    EVENT_NAME_TO_TYPE,
    EVENT_TOPICS,
    TOPIC_TO_EVENT,
    WITHDRAWN_FROM_BINS_TOPIC,
    LiquidityEventData,
    ParsedLiquidityResult,
    ParsedSwapResult,
    ParseResult,
    SwapEventData,
    TraderJoeV2Event,
    TraderJoeV2EventType,
    TraderJoeV2ReceiptParser,
    TransferEventData,
)
from .sdk import (
    BIN_ID_OFFSET,
    BIN_STEPS,
    DEFAULT_GAS_ESTIMATES,
    # Constants
    TRADERJOE_V2_ADDRESSES,
    InvalidBinStepError,
    PoolInfo,
    PoolNotFoundError,
    TraderJoeV2SDK,
    TraderJoeV2SDKError,
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
    # Adapter exports
    "TraderJoeV2Adapter",
    "TraderJoeV2Config",
    "SwapQuote",
    "SwapResult",
    "SwapType",
    "LiquidityPosition",
    "TransactionData",
    # SDK exports
    "TraderJoeV2SDK",
    "TraderJoeV2SDKError",
    "PoolNotFoundError",
    "InvalidBinStepError",
    "PoolInfo",
    "SDKSwapQuote",
    # Constants
    "TRADERJOE_V2_ADDRESSES",
    "BIN_STEPS",
    "DEFAULT_GAS_ESTIMATES",
    "BIN_ID_OFFSET",
    # Receipt parser exports
    "TraderJoeV2ReceiptParser",
    "TraderJoeV2Event",
    "TraderJoeV2EventType",
    "SwapEventData",
    "LiquidityEventData",
    "TransferEventData",
    "ParsedSwapResult",
    "ParsedLiquidityResult",
    "ParseResult",
    "EVENT_TOPICS",
    "TOPIC_TO_EVENT",
    "EVENT_NAME_TO_TYPE",
    "DEPOSITED_TO_BINS_TOPIC",
    "WITHDRAWN_FROM_BINS_TOPIC",
]
