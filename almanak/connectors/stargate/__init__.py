"""Stargate Bridge Adapter Package.

This package provides the StargateBridgeAdapter for cross-chain transfers
via the Stargate protocol built on LayerZero messaging.

Stargate Protocol:
- Unified liquidity pools across chains
- LayerZero for cross-chain messaging
- Instant guaranteed finality
- Native asset transfers (no wrapped tokens)

Example:
    from almanak.connectors.stargate import StargateBridgeAdapter

    adapter = StargateBridgeAdapter()
    quote = adapter.get_quote(
        token="USDC",
        amount=Decimal("1000"),
        from_chain="arbitrum",
        to_chain="optimism",
    )
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .adapter import (
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
    from .receipt_parser import StargateReceiptParser

__all__ = [
    "STARGATE_CHAIN_IDS",
    "STARGATE_POOL_IDS",
    "STARGATE_ROUTER_ADDRESSES",
    "STARGATE_SUPPORTED_TOKENS",
    "StargateBridgeAdapter",
    "StargateConfig",
    "StargateError",
    "StargateQuoteError",
    "StargateReceiptParser",
    "StargateStatusError",
    "StargateTransactionError",
]

_LAZY: dict[str, tuple[str, str]] = {
    "STARGATE_CHAIN_IDS": (".adapter", "STARGATE_CHAIN_IDS"),
    "STARGATE_POOL_IDS": (".adapter", "STARGATE_POOL_IDS"),
    "STARGATE_ROUTER_ADDRESSES": (".adapter", "STARGATE_ROUTER_ADDRESSES"),
    "STARGATE_SUPPORTED_TOKENS": (".adapter", "STARGATE_SUPPORTED_TOKENS"),
    "StargateBridgeAdapter": (".adapter", "StargateBridgeAdapter"),
    "StargateConfig": (".adapter", "StargateConfig"),
    "StargateError": (".adapter", "StargateError"),
    "StargateQuoteError": (".adapter", "StargateQuoteError"),
    "StargateReceiptParser": (".receipt_parser", "StargateReceiptParser"),
    "StargateStatusError": (".adapter", "StargateStatusError"),
    "StargateTransactionError": (".adapter", "StargateTransactionError"),
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

        register_connector(
            name="stargate",
            intents=(IntentType.BRIDGE,),
            chains=("ethereum", "arbitrum", "optimism", "polygon", "base", "avalanche", "bnb"),
        )
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
