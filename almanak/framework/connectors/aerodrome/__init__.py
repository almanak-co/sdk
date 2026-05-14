"""Aerodrome Finance Connector (Base L2).

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
    MAX_UINT128,
    AerodromeAdapter,
    AerodromeConfig,
    CLLiquidityResult,
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
    AerodromeSlipstreamReceiptParser,
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
    CLPositionInfo,
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
    "CLPositionInfo",
    "PoolInfo",
    "SwapRoute",
    "SDKSwapQuote",
    "AerodromeSDKError",
    "PoolNotFoundError",
    "InsufficientLiquidityError",
    # Adapter
    "AerodromeAdapter",
    "AerodromeConfig",
    "CLLiquidityResult",
    "SwapQuote",
    "SwapResult",
    "SwapType",
    "PoolType",
    "LiquidityResult",
    "TransactionData",
    "MAX_UINT128",
    # Receipt Parser
    "AerodromeReceiptParser",
    "AerodromeSlipstreamReceiptParser",
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

# Connector registration (VIB-4298). The registry powers the (connector,
# intent, chain) coverage gate in scripts/ci/check_connector_registry.py
# and will be consumed by PR 2's intent-test coverage check.
from almanak.framework.connectors.registry import register_connector  # noqa: E402
from almanak.framework.intents.vocabulary import IntentType  # noqa: E402

register_connector(
    name="aerodrome",
    intents=(
        IntentType.SWAP,
        IntentType.LP_OPEN,
        IntentType.LP_CLOSE,
        # NOTE: LP_COLLECT_FEES is intentionally omitted. Aerodrome Classic
        # (volatile/stable Solidly-fork pools) auto-compounds fees into pool
        # reserves and exposes no standalone collect() — see
        # compiler._compile_collect_fees. Aerodrome Slipstream (CL pools)
        # does support standalone collect, but ships under the separate
        # ``protocol="aerodrome_slipstream"`` literal and is not yet a
        # standalone connector entry. Re-add LP_COLLECT_FEES here only when
        # a Slipstream-specific connector is registered alongside it.
    ),
    # Optimism support (Velodrome V2 alias) is intentionally NOT declared here
    # yet: the intent-coverage gate enforces every (connector, intent, chain)
    # triple, and aerodrome declares SWAP + LP_OPEN + LP_CLOSE. VIB-4389 covers
    # SWAP × optimism; LP coverage lands separately via VIB-4390 (PR #2318).
    # That sister PR owns the chains=("base", "optimism") flip once both
    # SWAP and LP tests exist together.
    chains=("base",),
)
