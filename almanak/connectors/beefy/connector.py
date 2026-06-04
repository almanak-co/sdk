"""Beefy connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
)

CONNECTOR = Connector(
    name="beefy",
    kind=ProtocolKind.VAULT,
    gateway_connector=ImportRef(
        module="almanak.connectors.beefy.gateway.provider",
        attribute="BeefyGatewayConnector",
        order=8,
    ),
)

__all__ = ["CONNECTOR"]
