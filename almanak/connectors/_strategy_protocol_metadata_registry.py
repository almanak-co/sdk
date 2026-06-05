"""Strategy-side protocol metadata registration site."""

from __future__ import annotations

from almanak.connectors._connector import CONNECTOR_REGISTRY
from almanak.connectors._strategy_base.protocol_metadata_registry import (
    PROTOCOL_METADATA_REGISTRY,
)

__all__ = ["PROTOCOL_METADATA_REGISTRY"]


def _register_discovered_protocol_metadata() -> None:
    """Register protocol metadata providers published by connector manifests."""
    for connector_manifest in CONNECTOR_REGISTRY.with_protocol_metadata():
        if connector_manifest.protocol_metadata is None:
            continue
        PROTOCOL_METADATA_REGISTRY.register(connector_manifest.protocol_metadata.instantiate())


def _register_all() -> None:
    """Register every descriptor-backed protocol metadata provider."""
    _register_discovered_protocol_metadata()


_register_all()
