"""Orca Whirlpools concentrated liquidity connector.

Provides LP operations on Orca Whirlpool pools on Solana:
- Open concentrated liquidity positions
- Close positions (decrease liquidity + burn NFT)

Uses the same Q64.64 tick math as Raydium CLMM (reused from
connectors/raydium/math.py) and Anchor-style instruction encoding.

Reference: https://github.com/orca-so/whirlpools
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .adapter import (
        OrcaAdapter,
        OrcaConfig,
    )
    from .constants import WHIRLPOOL_PROGRAM_ID
    from .exceptions import (
        OrcaAPIError,
        OrcaConfigError,
        OrcaError,
        OrcaPoolError,
    )
    from .models import (
        OrcaPool,
        OrcaPosition,
        OrcaTransactionBundle,
    )
    from .receipt_parser import OrcaReceiptParser
    from .sdk import OrcaWhirlpoolSDK

__all__ = [
    "OrcaAPIError",
    "OrcaAdapter",
    "OrcaConfig",
    "OrcaConfigError",
    "OrcaError",
    "OrcaPool",
    "OrcaPoolError",
    "OrcaPosition",
    "OrcaReceiptParser",
    "OrcaTransactionBundle",
    "OrcaWhirlpoolSDK",
    "WHIRLPOOL_PROGRAM_ID",
]

_LAZY: dict[str, tuple[str, str]] = {
    "OrcaAPIError": (".exceptions", "OrcaAPIError"),
    "OrcaAdapter": (".adapter", "OrcaAdapter"),
    "OrcaConfig": (".adapter", "OrcaConfig"),
    "OrcaConfigError": (".exceptions", "OrcaConfigError"),
    "OrcaError": (".exceptions", "OrcaError"),
    "OrcaPool": (".models", "OrcaPool"),
    "OrcaPoolError": (".exceptions", "OrcaPoolError"),
    "OrcaPosition": (".models", "OrcaPosition"),
    "OrcaReceiptParser": (".receipt_parser", "OrcaReceiptParser"),
    "OrcaTransactionBundle": (".models", "OrcaTransactionBundle"),
    "OrcaWhirlpoolSDK": (".sdk", "OrcaWhirlpoolSDK"),
    "WHIRLPOOL_PROGRAM_ID": (".constants", "WHIRLPOOL_PROGRAM_ID"),
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

    register_connector(name="orca", intents=(IntentType.LP_OPEN, IntentType.LP_CLOSE), chains=("solana",))
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
