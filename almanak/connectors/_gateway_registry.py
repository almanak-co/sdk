"""Outer-folder gateway-side connector registry.

Lives one level up from ``_base/`` because it imports every concrete
gateway-side connector class — and ``_base/`` must stay protocol-clean
(no concrete connector imports). Adding a new gateway-side connector
means adding one ``register`` call here.

Phase 0 (this PR) — empty. No connectors have migrated yet.

Phase 2 — each connector migration commit adds one line, e.g.::

    from almanak.connectors.aave_v3.gateway.connector import AaveV3GatewayConnector
    GATEWAY_REGISTRY.register(AaveV3GatewayConnector())

Strategy-side code MUST NOT import this module.
"""

from __future__ import annotations

from ._base.gateway_registry import GatewayConnectorRegistry

__all__ = ["GATEWAY_REGISTRY"]


GATEWAY_REGISTRY: GatewayConnectorRegistry = GatewayConnectorRegistry()


def _register_all() -> None:
    """Register every gateway-side connector with ``GATEWAY_REGISTRY``.

    Empty in Phase 0. Phase 2 commits add one import + one
    ``GATEWAY_REGISTRY.register(...)`` line per migrated connector.
    """


_register_all()
