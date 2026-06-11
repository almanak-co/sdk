"""Fluid DEX Connector — swap surface (Phase 1, VIB-5029).

Provides Fluid DEX exact-input swaps on arbitrum, base, ethereum, and
polygon. Fluid is routerless: each pool is a per-pair contract; pool
discovery and quoting go through the DexReservesResolver (quotes match
on-chain execution to the wei — Phase-0 validation, VIB-5028).

Out of scope here (later phases): fToken lending SUPPLY/WITHDRAW
(VIB-5030), vault BORROW/REPAY (VIB-5031), and LP via SmartLending /
smart vaults (VIB-5032 — direct pool LP is whitelist-gated on-chain).

Key contracts (identical addresses on all supported chains):
- DexFactory: 0x91716C4EDA1Fb55e84Bf8b4c7085f84285c19085
- DexReservesResolver: 0x05Bd8269A20C472b148246De20E6852091BF16Ff

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
    )
    from .compiler import FluidCompiler
    from .receipt_parser import FluidReceiptParser
    from .sdk import FluidSDK

__all__ = [
    "FluidAdapter",
    "FluidCompiler",
    "FluidConfig",
    "FluidReceiptParser",
    "FluidSDK",
]

_LAZY: dict[str, tuple[str, str]] = {
    "FluidAdapter": (".adapter", "FluidAdapter"),
    "FluidCompiler": (".compiler", "FluidCompiler"),
    "FluidConfig": (".adapter", "FluidConfig"),
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
