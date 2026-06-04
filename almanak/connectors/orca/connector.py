"""Orca connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
)

CONNECTOR = Connector(
    name="orca",
    kind=ProtocolKind.LP,
    aliases=("orca_whirlpools",),
    gateway_connector=ImportRef(
        module="almanak.connectors.orca.gateway.provider",
        attribute="OrcaGatewayConnector",
        order=22,
    ),
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.orca.receipt_parser_provider",
        attribute="OrcaReceiptParserConnector",
    ),
)

__all__ = ["CONNECTOR"]
