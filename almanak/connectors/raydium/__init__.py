"""Raydium CLMM concentrated liquidity connector.

Provides LP operations on Raydium CLMM pools on Solana:
- Open concentrated liquidity positions
- Close positions (decrease liquidity + burn NFT)

Unlike Jupiter/Kamino (REST API), Raydium CLMM builds instructions
locally using `solders` and submits via SolanaExecutionPlanner.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .adapter import (
        RaydiumAdapter,
        RaydiumConfig,
    )
    from .constants import CLMM_PROGRAM_ID
    from .exceptions import (
        RaydiumAPIError,
        RaydiumConfigError,
        RaydiumError,
        RaydiumPoolError,
        RaydiumTickError,
    )
    from .models import (
        RaydiumPool,
        RaydiumPosition,
        RaydiumTransactionBundle,
    )
    from .receipt_parser import RaydiumReceiptParser
    from .sdk import RaydiumCLMMSDK

__all__ = [
    "CLMM_PROGRAM_ID",
    "RaydiumAPIError",
    "RaydiumAdapter",
    "RaydiumCLMMSDK",
    "RaydiumConfig",
    "RaydiumConfigError",
    "RaydiumError",
    "RaydiumPool",
    "RaydiumPoolError",
    "RaydiumPosition",
    "RaydiumReceiptParser",
    "RaydiumTickError",
    "RaydiumTransactionBundle",
]

_LAZY: dict[str, tuple[str, str]] = {
    "CLMM_PROGRAM_ID": (".constants", "CLMM_PROGRAM_ID"),
    "RaydiumAPIError": (".exceptions", "RaydiumAPIError"),
    "RaydiumAdapter": (".adapter", "RaydiumAdapter"),
    "RaydiumCLMMSDK": (".sdk", "RaydiumCLMMSDK"),
    "RaydiumConfig": (".adapter", "RaydiumConfig"),
    "RaydiumConfigError": (".exceptions", "RaydiumConfigError"),
    "RaydiumError": (".exceptions", "RaydiumError"),
    "RaydiumPool": (".models", "RaydiumPool"),
    "RaydiumPoolError": (".exceptions", "RaydiumPoolError"),
    "RaydiumPosition": (".models", "RaydiumPosition"),
    "RaydiumReceiptParser": (".receipt_parser", "RaydiumReceiptParser"),
    "RaydiumTickError": (".exceptions", "RaydiumTickError"),
    "RaydiumTransactionBundle": (".models", "RaydiumTransactionBundle"),
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
    _registered = True
    try:
        from almanak.connectors._strategy_base.registry import register_connector
        from almanak.framework.intents.vocabulary import IntentType

        register_connector(name="raydium", intents=(IntentType.LP_OPEN, IntentType.LP_CLOSE), chains=("solana",))
    except Exception:
        _registered = False
        raise


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
