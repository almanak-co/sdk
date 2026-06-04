"""Pendle connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
)

CONNECTOR = Connector(
    name="pendle",
    kind=ProtocolKind.YIELD_TRADING,
    gateway_connector=ImportRef(
        module="almanak.connectors.pendle.gateway.provider",
        attribute="PendleGatewayConnector",
        order=6,
    ),
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.pendle.receipt_parser_provider",
        attribute="PendleReceiptParserConnector",
    ),
)

__all__ = ["CONNECTOR"]
