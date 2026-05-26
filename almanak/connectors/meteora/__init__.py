"""Meteora DLMM concentrated liquidity connector.

Provides LP operations on Meteora DLMM pools on Solana:
- Open concentrated liquidity positions (discrete bins)
- Close positions (remove liquidity + close account)

Unlike Raydium CLMM (NFT positions, continuous ticks), Meteora DLMM uses:
- Discrete price bins instead of continuous ticks
- Non-transferable Keypair-based position accounts (not NFTs)
- SpotBalanced strategy for even liquidity distribution
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .adapter import (
        MeteoraAdapter,
        MeteoraConfig,
    )
    from .constants import DLMM_PROGRAM_ID
    from .exceptions import (
        MeteoraAPIError,
        MeteoraError,
        MeteoraPoolError,
        MeteoraPositionError,
    )
    from .models import (
        MeteoraBin,
        MeteoraPool,
        MeteoraPosition,
    )
    from .receipt_parser import MeteoraReceiptParser
    from .sdk import MeteoraSDK

__all__ = [
    "DLMM_PROGRAM_ID",
    "MeteoraAPIError",
    "MeteoraAdapter",
    "MeteoraBin",
    "MeteoraConfig",
    "MeteoraError",
    "MeteoraPool",
    "MeteoraPoolError",
    "MeteoraPosition",
    "MeteoraPositionError",
    "MeteoraReceiptParser",
    "MeteoraSDK",
]

_LAZY: dict[str, tuple[str, str]] = {
    "DLMM_PROGRAM_ID": (".constants", "DLMM_PROGRAM_ID"),
    "MeteoraAPIError": (".exceptions", "MeteoraAPIError"),
    "MeteoraAdapter": (".adapter", "MeteoraAdapter"),
    "MeteoraBin": (".models", "MeteoraBin"),
    "MeteoraConfig": (".adapter", "MeteoraConfig"),
    "MeteoraError": (".exceptions", "MeteoraError"),
    "MeteoraPool": (".models", "MeteoraPool"),
    "MeteoraPoolError": (".exceptions", "MeteoraPoolError"),
    "MeteoraPosition": (".models", "MeteoraPosition"),
    "MeteoraPositionError": (".exceptions", "MeteoraPositionError"),
    "MeteoraReceiptParser": (".receipt_parser", "MeteoraReceiptParser"),
    "MeteoraSDK": (".sdk", "MeteoraSDK"),
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

    register_connector(name="meteora", intents=(IntentType.LP_OPEN, IntentType.LP_CLOSE), chains=("solana",))
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
