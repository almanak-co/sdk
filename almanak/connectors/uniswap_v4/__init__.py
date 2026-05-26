"""Uniswap V4 protocol connector.

Provides swap compilation, receipt parsing, and pool utilities for
Uniswap V4's singleton PoolManager architecture.

Key differences from V3:
- Singleton PoolManager contract (all pools in one contract)
- Pool keys include hooks address (currency0, currency1, fee, tickSpacing, hooks)
- Native ETH support (no mandatory WETH wrapping)
- Flash accounting model
- New Swap event signature from PoolManager

Example:
    from almanak.connectors.uniswap_v4 import UniswapV4Adapter

    adapter = UniswapV4Adapter(chain="arbitrum")
    bundle = adapter.compile_swap_intent(intent, price_oracle)
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .adapter import (
        UniswapV4Adapter,
        UniswapV4Config,
        UniswapV4UnsupportedPoolError,
    )
    from .compiler import UniswapV4Compiler
    from .hooks import (
        HookDataEncoder,
        HookFlags,
        PoolDiscoveryResult,
        PoolState,
        discover_pool,
    )
    from .receipt_parser import UniswapV4ReceiptParser
    from .sdk import UniswapV4SDK

__all__ = [
    "HookDataEncoder",
    "HookFlags",
    "PoolDiscoveryResult",
    "PoolState",
    "UniswapV4Adapter",
    "UniswapV4Compiler",
    "UniswapV4Config",
    "UniswapV4ReceiptParser",
    "UniswapV4SDK",
    "UniswapV4UnsupportedPoolError",
    "discover_pool",
]

_LAZY: dict[str, tuple[str, str]] = {
    "HookDataEncoder": (".hooks", "HookDataEncoder"),
    "HookFlags": (".hooks", "HookFlags"),
    "PoolDiscoveryResult": (".hooks", "PoolDiscoveryResult"),
    "PoolState": (".hooks", "PoolState"),
    "UniswapV4Adapter": (".adapter", "UniswapV4Adapter"),
    "UniswapV4Compiler": (".compiler", "UniswapV4Compiler"),
    "UniswapV4Config": (".adapter", "UniswapV4Config"),
    "UniswapV4ReceiptParser": (".receipt_parser", "UniswapV4ReceiptParser"),
    "UniswapV4SDK": (".sdk", "UniswapV4SDK"),
    "UniswapV4UnsupportedPoolError": (".adapter", "UniswapV4UnsupportedPoolError"),
    "discover_pool": (".hooks", "discover_pool"),
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
        name="uniswap_v4",
        intents=(IntentType.SWAP, IntentType.LP_OPEN, IntentType.LP_CLOSE, IntentType.LP_COLLECT_FEES),
        chains=("ethereum", "arbitrum", "base"),
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
