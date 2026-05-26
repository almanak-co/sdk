"""Kamino Finance Lending Protocol Connector.

Kamino is the primary lending protocol on Solana (~$2.8B TVL),
providing Aave-style lending/borrowing with a REST API.

This connector provides:
- KaminoClient: HTTP client for the Kamino Finance API
- KaminoAdapter: Adapter for converting lending intents to Solana transactions
- KaminoReceiptParser: Balance-delta parser for extracting lending results

Example:
    from almanak.connectors.kamino import KaminoClient, KaminoConfig

    config = KaminoConfig(wallet_address="your-solana-pubkey")
    client = KaminoClient(config)

    # Get reserves for the main market
    reserves = client.get_reserves()

    # Build a deposit transaction
    tx = client.deposit(reserve=reserves[0].address, amount="100.0")
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .adapter import KaminoAdapter
    from .client import (
        KAMINO_MAIN_MARKET,
        U64_MAX,
        KaminoClient,
        KaminoConfig,
    )
    from .exceptions import (
        KaminoAPIError,
        KaminoConfigError,
        KaminoError,
        KaminoValidationError,
    )
    from .models import (
        KaminoMarket,
        KaminoReserve,
        KaminoTransactionResponse,
    )
    from .receipt_parser import KaminoReceiptParser

__all__ = [
    "KAMINO_MAIN_MARKET",
    "KaminoAPIError",
    "KaminoAdapter",
    "KaminoClient",
    "KaminoConfig",
    "KaminoConfigError",
    "KaminoError",
    "KaminoMarket",
    "KaminoReceiptParser",
    "KaminoReserve",
    "KaminoTransactionResponse",
    "KaminoValidationError",
    "U64_MAX",
]

_LAZY: dict[str, tuple[str, str]] = {
    "KAMINO_MAIN_MARKET": (".client", "KAMINO_MAIN_MARKET"),
    "KaminoAPIError": (".exceptions", "KaminoAPIError"),
    "KaminoAdapter": (".adapter", "KaminoAdapter"),
    "KaminoClient": (".client", "KaminoClient"),
    "KaminoConfig": (".client", "KaminoConfig"),
    "KaminoConfigError": (".exceptions", "KaminoConfigError"),
    "KaminoError": (".exceptions", "KaminoError"),
    "KaminoMarket": (".models", "KaminoMarket"),
    "KaminoReceiptParser": (".receipt_parser", "KaminoReceiptParser"),
    "KaminoReserve": (".models", "KaminoReserve"),
    "KaminoTransactionResponse": (".models", "KaminoTransactionResponse"),
    "KaminoValidationError": (".exceptions", "KaminoValidationError"),
    "U64_MAX": (".client", "U64_MAX"),
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
            name="kamino",
            intents=(IntentType.SUPPLY, IntentType.BORROW, IntentType.REPAY, IntentType.WITHDRAW),
            chains=("solana",),
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
