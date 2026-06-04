"""Trader Joe V2 connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
)

CONNECTOR = Connector(
    name="traderjoe_v2",
    kind=ProtocolKind.LP,
    gateway_connector=ImportRef(
        module="almanak.connectors.traderjoe_v2.gateway.provider",
        attribute="TraderJoeV2GatewayConnector",
        order=19,
    ),
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.traderjoe_v2.receipt_parser_provider",
        attribute="TraderJoeV2ReceiptParserConnector",
    ),
)

__all__ = ["CONNECTOR"]
