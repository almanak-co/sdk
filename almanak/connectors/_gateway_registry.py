"""Outer-folder gateway-side connector registry.

Lives one level up from ``_base/`` because it owns gateway registry
bootstrap, and ``_base/`` must stay protocol-clean (no concrete connector
imports). It imports only connector descriptors; provider classes are loaded
from connector-owned lazy import references.

Connectors that publish ``almanak/connectors/<protocol>/connector.py`` with
a ``CONNECTOR.gateway_connector`` import reference are registered from that
connector object::

    CONNECTOR = Connector(
        name="<protocol>",
        kind=ProtocolKind.<KIND>,
        gateway_connector=ImportRef(
            module="almanak.connectors.<protocol>.gateway.provider",
            attribute="<Protocol>GatewayConnector",
            order=1,
        ),
    )

Strategy-side code MUST NOT import this module. This is enforced by
``tests/static/test_strategy_import_boundary.py``.
"""

from __future__ import annotations

from ._base.gateway_registry import GatewayConnectorRegistry
from ._connector import CONNECTOR_REGISTRY, ImportRef

__all__ = ["GATEWAY_REGISTRY"]


GATEWAY_REGISTRY: GatewayConnectorRegistry = GatewayConnectorRegistry()


def _register_discovered_gateway_connectors() -> None:
    """Register gateway connectors published by connector manifests."""
    gateway_refs: list[ImportRef] = []
    for connector in CONNECTOR_REGISTRY.all():
        gateway_refs.extend(connector.gateway_connector_refs)
    gateway_refs.sort(key=lambda ref: (ref.order is None, ref.order if ref.order is not None else 0))
    for gateway_ref in gateway_refs:
        connector = gateway_ref.instantiate()
        GATEWAY_REGISTRY.register(connector)


def _register_all() -> None:
    """Register every gateway-side connector with ``GATEWAY_REGISTRY``.

    Import targets are stored as strings on each connector descriptor, so
    loading this module does not import concrete provider classes until the
    gateway registry is bootstrapped.
    """
    _register_discovered_gateway_connectors()


_register_all()
