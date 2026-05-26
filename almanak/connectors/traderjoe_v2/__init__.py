"""TraderJoe Liquidity Book V2 Connector.

This module provides the TraderJoe V2 adapter for executing swaps and
managing liquidity positions on TraderJoe V2's Liquidity Book.

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
- Arbitrum One (Chain ID: 42161)
- BNB Smart Chain (Chain ID: 56)
- Ethereum (Chain ID: 1)

Example:
    from almanak.connectors.traderjoe_v2 import TraderJoeV2Adapter, TraderJoeV2Config

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
    from almanak.connectors.traderjoe_v2 import TraderJoeV2SDK

    sdk = TraderJoeV2SDK(chain="avalanche", rpc_url="https://api.avax.network/ext/bc/C/rpc")
    pool = sdk.get_pool_address(wavax_addr, usdc_addr, bin_step=20)
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .adapter import (
        LiquidityPosition,
        SwapQuote,
        SwapResult,
        SwapType,
        TraderJoeV2Adapter,
        TraderJoeV2Config,
        TransactionData,
    )
    from .compiler import TraderJoeV2Compiler
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
        TRADERJOE_V2_ADDRESSES,
        InvalidBinStepError,
        PoolInfo,
        PoolNotFoundError,
        TraderJoeV2SDK,
        TraderJoeV2SDKError,
    )
    from .sdk import (
        SwapQuote as SDKSwapQuote,
    )

__all__ = [
    "BIN_ID_OFFSET",
    "BIN_STEPS",
    "DEFAULT_GAS_ESTIMATES",
    "DEPOSITED_TO_BINS_TOPIC",
    "EVENT_NAME_TO_TYPE",
    "EVENT_TOPICS",
    "InvalidBinStepError",
    "LiquidityEventData",
    "LiquidityPosition",
    "ParseResult",
    "ParsedLiquidityResult",
    "ParsedSwapResult",
    "PoolInfo",
    "PoolNotFoundError",
    "SDKSwapQuote",
    "SwapEventData",
    "SwapQuote",
    "SwapResult",
    "SwapType",
    "TOPIC_TO_EVENT",
    "TRADERJOE_V2_ADDRESSES",
    "TraderJoeV2Adapter",
    "TraderJoeV2Compiler",
    "TraderJoeV2Config",
    "TraderJoeV2Event",
    "TraderJoeV2EventType",
    "TraderJoeV2ReceiptParser",
    "TraderJoeV2SDK",
    "TraderJoeV2SDKError",
    "TransactionData",
    "TransferEventData",
    "WITHDRAWN_FROM_BINS_TOPIC",
]

_LAZY: dict[str, tuple[str, str]] = {
    "BIN_ID_OFFSET": (".sdk", "BIN_ID_OFFSET"),
    "BIN_STEPS": (".sdk", "BIN_STEPS"),
    "DEFAULT_GAS_ESTIMATES": (".sdk", "DEFAULT_GAS_ESTIMATES"),
    "DEPOSITED_TO_BINS_TOPIC": (".receipt_parser", "DEPOSITED_TO_BINS_TOPIC"),
    "EVENT_NAME_TO_TYPE": (".receipt_parser", "EVENT_NAME_TO_TYPE"),
    "EVENT_TOPICS": (".receipt_parser", "EVENT_TOPICS"),
    "InvalidBinStepError": (".sdk", "InvalidBinStepError"),
    "LiquidityEventData": (".receipt_parser", "LiquidityEventData"),
    "LiquidityPosition": (".adapter", "LiquidityPosition"),
    "ParseResult": (".receipt_parser", "ParseResult"),
    "ParsedLiquidityResult": (".receipt_parser", "ParsedLiquidityResult"),
    "ParsedSwapResult": (".receipt_parser", "ParsedSwapResult"),
    "PoolInfo": (".sdk", "PoolInfo"),
    "PoolNotFoundError": (".sdk", "PoolNotFoundError"),
    "SDKSwapQuote": (".sdk", "SwapQuote"),
    "SwapEventData": (".receipt_parser", "SwapEventData"),
    "SwapQuote": (".adapter", "SwapQuote"),
    "SwapResult": (".adapter", "SwapResult"),
    "SwapType": (".adapter", "SwapType"),
    "TOPIC_TO_EVENT": (".receipt_parser", "TOPIC_TO_EVENT"),
    "TRADERJOE_V2_ADDRESSES": (".sdk", "TRADERJOE_V2_ADDRESSES"),
    "TraderJoeV2Adapter": (".adapter", "TraderJoeV2Adapter"),
    "TraderJoeV2Compiler": (".compiler", "TraderJoeV2Compiler"),
    "TraderJoeV2Config": (".adapter", "TraderJoeV2Config"),
    "TraderJoeV2Event": (".receipt_parser", "TraderJoeV2Event"),
    "TraderJoeV2EventType": (".receipt_parser", "TraderJoeV2EventType"),
    "TraderJoeV2ReceiptParser": (".receipt_parser", "TraderJoeV2ReceiptParser"),
    "TraderJoeV2SDK": (".sdk", "TraderJoeV2SDK"),
    "TraderJoeV2SDKError": (".sdk", "TraderJoeV2SDKError"),
    "TransactionData": (".adapter", "TransactionData"),
    "TransferEventData": (".receipt_parser", "TransferEventData"),
    "WITHDRAWN_FROM_BINS_TOPIC": (".receipt_parser", "WITHDRAWN_FROM_BINS_TOPIC"),
}

_registered = False


def _register_once() -> None:
    """Fire ``register_connector`` once on first strategy-side access.

    Deferred so importing the connector's gateway-side surface during
    gateway boot does not pull ``framework.intents.vocabulary`` into the
    partially-initialised config-init chain (VIB-4835).
    """
    global _registered
    if _registered:
        return
    from almanak.connectors._strategy_base.registry import register_connector
    from almanak.framework.intents.vocabulary import IntentType

    register_connector(
        name="traderjoe_v2",
        intents=(IntentType.SWAP, IntentType.LP_OPEN, IntentType.LP_CLOSE, IntentType.LP_COLLECT_FEES),
        chains=("avalanche", "arbitrum", "bnb", "ethereum"),
    )
    _registered = True


def __getattr__(name: str) -> Any:
    """PEP 562 lazy attribute access."""
    if name not in _LAZY:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    submodule, attr = _LAZY[name]
    import importlib

    module = importlib.import_module(submodule, package=__name__)
    value = getattr(module, attr)
    globals()[name] = value
    _register_once()
    return value
