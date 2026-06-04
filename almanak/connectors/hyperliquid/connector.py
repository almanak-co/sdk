"""Hyperliquid connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
)

CONNECTOR = Connector(
    name="hyperliquid",
    kind=ProtocolKind.PERP,
    gateway_connector=ImportRef(
        module="almanak.connectors.hyperliquid.gateway.provider",
        attribute="HyperliquidGatewayConnector",
        order=15,
    ),
)

__all__ = ["CONNECTOR"]
