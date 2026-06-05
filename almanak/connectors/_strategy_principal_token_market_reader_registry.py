"""Strategy-side principal-token market reader registration site."""

from __future__ import annotations

from almanak.connectors._connector import CONNECTOR_REGISTRY
from almanak.connectors._strategy_base.principal_token_market_reader_registry import (
    PRINCIPAL_TOKEN_MARKET_READ_REGISTRY,
)

__all__ = ["PRINCIPAL_TOKEN_MARKET_READ_REGISTRY"]


def _register_discovered_principal_token_market_readers() -> None:
    """Register principal-token market readers published by connector manifests."""
    for connector_manifest in CONNECTOR_REGISTRY.with_principal_token_market_reader():
        reader_ref = connector_manifest.principal_token_market_reader
        assert reader_ref is not None
        PRINCIPAL_TOKEN_MARKET_READ_REGISTRY.register(reader_ref.instantiate())


def _register_all() -> None:
    """Register every descriptor-backed principal-token market reader."""
    _register_discovered_principal_token_market_readers()


_register_all()
