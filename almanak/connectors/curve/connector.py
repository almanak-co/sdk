"""Curve connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
)

CONNECTOR = Connector(
    name="curve",
    kind=ProtocolKind.LP,
    gateway_connector=ImportRef(
        module="almanak.connectors.curve.gateway.provider",
        attribute="CurveGatewayConnector",
        order=24,
    ),
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.curve.receipt_parser_provider",
        attribute="CurveReceiptParserConnector",
    ),
)

__all__ = ["CONNECTOR"]
