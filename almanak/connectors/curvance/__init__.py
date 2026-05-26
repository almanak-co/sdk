"""Curvance protocol connector.

Curvance is a permissionless isolated-market lending protocol deployed on Monad.
Unlike Compound/Aave-style protocols with a single pool, Curvance deploys a
dedicated cToken (collateral, ERC-4626-style) and BorrowableCToken (debt side)
pair per market. The ``market_id`` used throughout the adapter is the
MarketManager address.

Key entry points:
    CurvanceAdapter      — high-level interface (supply_collateral, borrow, repay, withdraw)
    CurvanceConfig       — adapter configuration (chain, wallet, optional gateway client)
    CurvanceSDK          — low-level calldata/encoding helpers
    CurvanceReceiptParser — event parsing for ResultEnricher
    CURVANCE_MARKETS     — per-chain market registry (MarketManager -> cToken / BorrowableCToken)

Supported chains: Monad.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .adapter import (
        CurvanceAdapter,
        CurvanceConfig,
        CurvanceMarketInfo,
    )
    from .constants import (
        CURVANCE_MARKETS,
        CURVANCE_PROTOCOL_CONTRACTS,
    )
    from .receipt_parser import CurvanceReceiptParser
    from .sdk import CurvanceSDK

__all__ = [
    "CURVANCE_MARKETS",
    "CURVANCE_PROTOCOL_CONTRACTS",
    "CurvanceAdapter",
    "CurvanceConfig",
    "CurvanceMarketInfo",
    "CurvanceReceiptParser",
    "CurvanceSDK",
]

_LAZY: dict[str, tuple[str, str]] = {
    "CURVANCE_MARKETS": (".constants", "CURVANCE_MARKETS"),
    "CURVANCE_PROTOCOL_CONTRACTS": (".constants", "CURVANCE_PROTOCOL_CONTRACTS"),
    "CurvanceAdapter": (".adapter", "CurvanceAdapter"),
    "CurvanceConfig": (".adapter", "CurvanceConfig"),
    "CurvanceMarketInfo": (".adapter", "CurvanceMarketInfo"),
    "CurvanceReceiptParser": (".receipt_parser", "CurvanceReceiptParser"),
    "CurvanceSDK": (".sdk", "CurvanceSDK"),
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
        name="curvance",
        intents=(IntentType.SUPPLY, IntentType.BORROW, IntentType.REPAY, IntentType.WITHDRAW),
        chains=("monad",),
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
