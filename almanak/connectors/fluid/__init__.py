"""Fluid DEX Connector — Phase 1 (Arbitrum swap surface + LP scaffolding).

Provides the Fluid DEX T1 integration surface on Arbitrum. Swaps currently
fail fast because all known T1 pools reject swaps at tested amounts; LP open
also fails fast while Liquidity-layer routing remains unsupported. LP close
uses the adapter encumbrance guard before building remove-liquidity calldata.

Scope (phase 1):
- Arbitrum only
- Swaps via swapIn() (compile path currently disabled)
- LP deposit deferred (Liquidity-layer routing causes reverts)
- LP close compile support for unencumbered positions

Key contracts (Arbitrum):
- DexFactory: 0x91716C4EDA1Fb55e84Bf8b4c7085f84285c19085
- DexResolver: 0x11D80CfF056Cef4F9E6d23da8672fE9873e5cC07

Example:
    from almanak.connectors.fluid import FluidAdapter, FluidConfig

    config = FluidConfig(
        chain="arbitrum",
        wallet_address="0x...",
        rpc_url="https://arb-mainnet.g.alchemy.com/v2/...",
    )
    adapter = FluidAdapter(config)
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .adapter import (
        FluidAdapter,
        FluidConfig,
        FluidPositionDetails,
    )
    from .compiler import FluidCompiler
    from .receipt_parser import FluidReceiptParser
    from .sdk import FluidSDK

__all__ = [
    "FluidAdapter",
    "FluidCompiler",
    "FluidConfig",
    "FluidPositionDetails",
    "FluidReceiptParser",
    "FluidSDK",
]

_LAZY: dict[str, tuple[str, str]] = {
    "FluidAdapter": (".adapter", "FluidAdapter"),
    "FluidCompiler": (".compiler", "FluidCompiler"),
    "FluidConfig": (".adapter", "FluidConfig"),
    "FluidPositionDetails": (".adapter", "FluidPositionDetails"),
    "FluidReceiptParser": (".receipt_parser", "FluidReceiptParser"),
    "FluidSDK": (".sdk", "FluidSDK"),
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
