"""Bridge Connectors.

This package contains adapters for cross-chain bridge protocols,
providing a unified interface for bridging assets between chains.

Available Bridges:
- Across: Fast bridge using optimistic verification (Arbitrum, Optimism, Base, Polygon, Ethereum)
- Stargate: LayerZero-based bridge for stablecoins and native assets

Example:
    from almanak.framework.connectors.bridges import BridgeAdapter, BridgeQuote, BridgeStatus

    # Get a quote for bridging
    quote = adapter.get_quote(
        token="USDC",
        amount=Decimal("1000"),
        from_chain="arbitrum",
        to_chain="optimism",
        max_slippage=Decimal("0.005"),
    )

    # Build the deposit transaction
    tx = adapter.build_deposit_tx(quote, recipient="0x...")
"""

from .across import (
    ACROSS_CHAIN_IDS,
    ACROSS_SPOKE_POOL_ADDRESSES,
    ACROSS_SUPPORTED_TOKENS,
    AcrossBridgeAdapter,
    AcrossConfig,
    AcrossError,
    AcrossQuoteError,
    AcrossStatusError,
    AcrossTransactionError,
)
from .base import (
    BridgeAdapter,
    BridgeError,
    BridgeQuote,
    BridgeQuoteError,
    BridgeRoute,
    BridgeStatus,
    BridgeStatusEnum,
    BridgeStatusError,
    BridgeTransactionError,
)
from .selector import (
    DEFAULT_RELIABILITY_SCORES,
    BridgeScore,
    BridgeSelectionResult,
    BridgeSelector,
    BridgeSelectorError,
    NoBridgeAvailableError,
    SelectionPriority,
)
from .stargate import (
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

__all__ = [
    # Abstract base class
    "BridgeAdapter",
    # Data classes
    "BridgeQuote",
    "BridgeStatus",
    "BridgeRoute",
    # Enums
    "BridgeStatusEnum",
    # Exceptions
    "BridgeError",
    "BridgeQuoteError",
    "BridgeTransactionError",
    "BridgeStatusError",
    # Across Adapter
    "AcrossBridgeAdapter",
    "AcrossConfig",
    "AcrossError",
    "AcrossQuoteError",
    "AcrossTransactionError",
    "AcrossStatusError",
    "ACROSS_CHAIN_IDS",
    "ACROSS_SPOKE_POOL_ADDRESSES",
    "ACROSS_SUPPORTED_TOKENS",
    # Stargate Adapter
    "StargateBridgeAdapter",
    "StargateConfig",
    "StargateError",
    "StargateQuoteError",
    "StargateTransactionError",
    "StargateStatusError",
    "STARGATE_CHAIN_IDS",
    "STARGATE_ROUTER_ADDRESSES",
    "STARGATE_POOL_IDS",
    "STARGATE_SUPPORTED_TOKENS",
    # Bridge Selector
    "BridgeSelector",
    "BridgeScore",
    "BridgeSelectionResult",
    "SelectionPriority",
    "BridgeSelectorError",
    "NoBridgeAvailableError",
    "DEFAULT_RELIABILITY_SCORES",
]
