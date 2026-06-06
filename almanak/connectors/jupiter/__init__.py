"""Jupiter DEX Aggregator Protocol Connector.

Jupiter is the primary DEX aggregator on Solana, routing across
Raydium, Orca, Meteora, and other Solana AMMs.

This connector provides:
- JupiterClient: HTTP client for the Jupiter API v6
- JupiterAdapter: Adapter for converting SwapIntents to Solana transactions
- JupiterReceiptParser: Balance-delta parser for extracting swap results

Example:
    from almanak.connectors.jupiter import JupiterClient, JupiterAdapter, JupiterConfig

    config = JupiterConfig(wallet_address="your-solana-pubkey")
    client = JupiterClient(config)

    # Get a swap quote
    quote = client.get_quote(
        input_mint="EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
        output_mint="So11111111111111111111111111111111111111112",     # WSOL
        amount=1000000000,
        slippage_bps=50,
    )
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .adapter import JupiterAdapter
    from .client import (
        JupiterClient,
        JupiterConfig,
    )
    from .exceptions import (
        JupiterAPIError,
        JupiterConfigError,
        JupiterError,
        JupiterPriceImpactError,
        JupiterValidationError,
    )
    from .models import (
        JupiterQuote,
        JupiterRoutePlan,
        JupiterSwapTransaction,
    )
    from .receipt_parser import JupiterReceiptParser

__all__ = [
    "JupiterAPIError",
    "JupiterAdapter",
    "JupiterClient",
    "JupiterConfig",
    "JupiterConfigError",
    "JupiterError",
    "JupiterPriceImpactError",
    "JupiterQuote",
    "JupiterReceiptParser",
    "JupiterRoutePlan",
    "JupiterSwapTransaction",
    "JupiterValidationError",
]

_LAZY: dict[str, tuple[str, str]] = {
    "JupiterAPIError": (".exceptions", "JupiterAPIError"),
    "JupiterAdapter": (".adapter", "JupiterAdapter"),
    "JupiterClient": (".client", "JupiterClient"),
    "JupiterConfig": (".client", "JupiterConfig"),
    "JupiterConfigError": (".exceptions", "JupiterConfigError"),
    "JupiterError": (".exceptions", "JupiterError"),
    "JupiterPriceImpactError": (".exceptions", "JupiterPriceImpactError"),
    "JupiterQuote": (".models", "JupiterQuote"),
    "JupiterReceiptParser": (".receipt_parser", "JupiterReceiptParser"),
    "JupiterRoutePlan": (".models", "JupiterRoutePlan"),
    "JupiterSwapTransaction": (".models", "JupiterSwapTransaction"),
    "JupiterValidationError": (".exceptions", "JupiterValidationError"),
}

_registered = False


def _register_once() -> None:
    """Compatibility no-op; strategy registration lives in connector.py."""
    global _registered
    if _registered:
        return
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
