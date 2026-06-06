"""Strategy-side deferred refresh registration site."""

from __future__ import annotations

from almanak.connectors._connector import CONNECTOR_REGISTRY
from almanak.connectors._strategy_base.deferred_refresh_registry import (
    DEFERRED_REFRESH_REGISTRY,
)

__all__ = ["DEFERRED_REFRESH_REGISTRY"]


def _register_discovered_deferred_refresh() -> None:
    """Register deferred refresh providers published by connector manifests."""
    for connector_manifest in CONNECTOR_REGISTRY.with_deferred_refresh():
        deferred_refresh_ref = connector_manifest.deferred_refresh
        assert deferred_refresh_ref is not None
        DEFERRED_REFRESH_REGISTRY.register(deferred_refresh_ref.instantiate())


def _register_all() -> None:
    """Register every descriptor-backed deferred refresh provider."""
    _register_discovered_deferred_refresh()


_register_all()
