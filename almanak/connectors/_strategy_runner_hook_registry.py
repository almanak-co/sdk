"""Strategy-side runner-hook registration site.

Connectors publish runner-specific strategy hooks through
``CONNECTOR.runner_hook_connector``. This boot file loads only connector
descriptors and instantiates the connector-owned hook providers.
"""

from __future__ import annotations

from almanak.connectors._connector import CONNECTOR_REGISTRY
from almanak.connectors._strategy_base.runner_hook_registry import (
    STRATEGY_RUNNER_HOOK_REGISTRY,
)

__all__ = ["STRATEGY_RUNNER_HOOK_REGISTRY"]


def _register_discovered_runner_hooks() -> None:
    """Register runner-hook connectors published by connector manifests."""
    for connector_manifest in CONNECTOR_REGISTRY.with_runner_hooks():
        if connector_manifest.runner_hook_connector is None:
            continue
        STRATEGY_RUNNER_HOOK_REGISTRY.register(connector_manifest.runner_hook_connector.instantiate())


def _register_all() -> None:
    """Register every descriptor-backed strategy-runner hook connector."""
    _register_discovered_runner_hooks()


_register_all()
