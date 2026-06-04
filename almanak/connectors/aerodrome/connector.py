"""Aerodrome connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
)

CONNECTOR = Connector(
    name="aerodrome",
    kind=ProtocolKind.LP,
    aliases=("aerodrome_slipstream",),
    gateway_connector=ImportRef(
        module="almanak.connectors.aerodrome.gateway.provider",
        attribute="AerodromeGatewayConnector",
        order=13,
    ),
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.aerodrome.receipt_parser_provider",
        attribute="AerodromeReceiptParserConnector",
    ),
)

__all__ = ["CONNECTOR"]
