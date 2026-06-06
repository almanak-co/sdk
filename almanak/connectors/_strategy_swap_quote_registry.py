"""Boot registry for connector-owned swap quote providers."""

from __future__ import annotations

from almanak.connectors._connector import CONNECTOR_REGISTRY
from almanak.connectors._strategy_base.swap_quote_registry import (
    SWAP_QUOTE_REGISTRY,
    SwapQuoteConnector,
)


def _register_discovered_swap_quotes() -> None:
    """Register swap quote providers published by connector manifests."""
    for connector_manifest in CONNECTOR_REGISTRY.with_swap_quote():
        quote_ref = connector_manifest.swap_quote_connector
        if quote_ref is None:
            continue
        connector = quote_ref.instantiate()
        if not isinstance(connector, SwapQuoteConnector):
            raise TypeError(
                f"{quote_ref.module}.{quote_ref.attribute} must instantiate a SwapQuoteConnector, "
                f"got {type(connector).__qualname__}"
            )
        SWAP_QUOTE_REGISTRY.register(connector)


def ensure_swap_quote_registry_loaded() -> None:
    """Hydrate the swap quote registry from connector manifests."""
    _register_discovered_swap_quotes()


__all__ = ["SWAP_QUOTE_REGISTRY", "ensure_swap_quote_registry_loaded"]
