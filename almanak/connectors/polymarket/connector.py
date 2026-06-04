"""Polymarket connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
)

CONNECTOR = Connector(
    name="polymarket",
    kind=ProtocolKind.PREDICTION_MARKET,
    gateway_connector=ImportRef(
        module="almanak.connectors.polymarket.gateway.provider",
        attribute="PolymarketGatewayConnector",
        order=11,
    ),
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.polymarket.receipt_parser_provider",
        attribute="PolymarketReceiptParserConnector",
    ),
)

__all__ = ["CONNECTOR"]
