"""Connector foundation — strategy-safe public surface.

Re-exports the foundation types that are safe to import from strategy
code. Gateway types (``GatewayConnector``, ``GatewayConnectorRegistry``,
``Gateway*Capability``) are deliberately NOT re-exported here. Reach
them via explicit submodule paths::

    from almanak.connectors._base.gateway_connector import GatewayConnector
    from almanak.connectors._base.gateway_registry import GatewayConnectorRegistry
    from almanak.connectors._base.gateway_capabilities import (
        GatewayServicerCapability,
        GatewayMarketLookupCapability,
        GatewayPoolKeyCacheCapability,
    )

The import-graph lint (``tests/static/test_strategy_import_boundary.py``)
hard-fails on any strategy-side file that imports from ``_base.gateway_*``
or any ``connectors/*/gateway/*`` submodule. See PR 2169 / VIB-4121.
"""

from .types import ProtocolKind, ProtocolName

__all__ = ["ProtocolKind", "ProtocolName"]
