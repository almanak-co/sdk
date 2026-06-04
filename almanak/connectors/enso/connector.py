"""Enso connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
)

CONNECTOR = Connector(
    name="enso",
    kind=ProtocolKind.SWAP,
    gateway_connector=ImportRef(
        module="almanak.connectors.enso.gateway.provider",
        attribute="EnsoGatewayConnector",
        order=10,
    ),
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.enso.receipt_parser_provider",
        attribute="EnsoReceiptParserConnector",
    ),
)

__all__ = ["CONNECTOR"]
