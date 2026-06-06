"""Enso Finance Protocol Connector (DEX aggregator + routing API).

Enso is a routing and composable transaction protocol that aggregates
liquidity across multiple DEXs and lending protocols.

This connector provides:
- EnsoClient: SDK for interacting with the Enso Finance API
- EnsoAdapter: Adapter for converting intents to Enso transactions
- EnsoReceiptParser: Parser for extracting results from transaction receipts

Supports both same-chain and cross-chain swaps via Enso's bridge aggregation
(Stargate, LayerZero).

Example:
    from almanak.connectors.enso import EnsoClient, EnsoAdapter, EnsoConfig

    # Create client
    config = EnsoConfig(
        api_key="your-api-key",
        chain="base",
        wallet_address="0x...",
    )
    client = EnsoClient(config)

Lazy attribute access (VIB-4835)
--------------------------------
The strategy-facing surface (``EnsoClient``, ``EnsoAdapter``, …) is
exposed via PEP 562 lazy ``__getattr__``. This keeps the package's
``__init__`` cheap enough to be safely imported during gateway boot:
``almanak.gateway.core.settings`` does ``from almanak.connectors.enso.gateway.settings
import EnsoGatewaySettings``, which Python implements by first running
this ``__init__.py``. The strategy-side adapter pulls
``almanak.framework.intents.vocabulary`` whose package init triggers
``framework.intents.compiler`` → ``config.cli_runtime`` → ``config.env``;
that last module is the one importing ``GatewaySettings`` in the
first place, producing a circular import. Lazy attributes avoid the
cycle entirely: strategy code accessing ``EnsoAdapter`` triggers the
adapter import only after ``config.env`` is fully initialised.

Strategy registration metadata is descriptor-owned: ``connector.py`` is the
single source of truth. The ``_register_once()`` hook below remains only as an
idempotent compatibility no-op for the lazy-init contract.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .adapter import EnsoAdapter
    from .client import CHAIN_MAPPING, EnsoClient, EnsoConfig
    from .exceptions import (
        EnsoAPIError,
        EnsoConfigError,
        EnsoError,
        EnsoRouterRevertError,
        EnsoValidationError,
        PriceImpactExceedsThresholdError,
        check_known_router_revert,
    )
    from .models import (
        Hop,
        Quote,
        RouteParams,
        RouteTransaction,
        RoutingStrategy,
        Transaction,
    )
    from .receipt_parser import EnsoReceiptParser

__all__ = [
    "CHAIN_MAPPING",
    "EnsoAPIError",
    "EnsoAdapter",
    "EnsoClient",
    "EnsoConfig",
    "EnsoConfigError",
    "EnsoError",
    "EnsoReceiptParser",
    "EnsoRouterRevertError",
    "EnsoValidationError",
    "Hop",
    "PriceImpactExceedsThresholdError",
    "Quote",
    "RouteParams",
    "RouteTransaction",
    "RoutingStrategy",
    "Transaction",
    "check_known_router_revert",
]

# Map of public symbol → (submodule, attribute_name).
_LAZY: dict[str, tuple[str, str]] = {
    "EnsoAdapter": (".adapter", "EnsoAdapter"),
    "EnsoClient": (".client", "EnsoClient"),
    "EnsoConfig": (".client", "EnsoConfig"),
    "CHAIN_MAPPING": (".client", "CHAIN_MAPPING"),
    "EnsoAPIError": (".exceptions", "EnsoAPIError"),
    "EnsoConfigError": (".exceptions", "EnsoConfigError"),
    "EnsoError": (".exceptions", "EnsoError"),
    "EnsoRouterRevertError": (".exceptions", "EnsoRouterRevertError"),
    "EnsoValidationError": (".exceptions", "EnsoValidationError"),
    "PriceImpactExceedsThresholdError": (".exceptions", "PriceImpactExceedsThresholdError"),
    "check_known_router_revert": (".exceptions", "check_known_router_revert"),
    "Hop": (".models", "Hop"),
    "Quote": (".models", "Quote"),
    "RouteParams": (".models", "RouteParams"),
    "RouteTransaction": (".models", "RouteTransaction"),
    "RoutingStrategy": (".models", "RoutingStrategy"),
    "Transaction": (".models", "Transaction"),
    "EnsoReceiptParser": (".receipt_parser", "EnsoReceiptParser"),
}

_registered = False


def _register_once() -> None:
    """Compatibility no-op; strategy registration lives in connector.py."""
    global _registered
    if _registered:
        return
    _registered = True


def __getattr__(name: str) -> Any:
    """PEP 562 lazy attribute access.

    Resolves ``name`` to its underlying submodule and binds the symbol
    on the package so subsequent accesses skip this function.
    """
    if name not in _LAZY:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    submodule, attr = _LAZY[name]
    import importlib

    module = importlib.import_module(submodule, package=__name__)
    value = getattr(module, attr)
    globals()[name] = value
    _register_once()
    return value
