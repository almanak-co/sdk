"""Raydium connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
)

CONNECTOR = Connector(
    name="raydium",
    kind=ProtocolKind.LP,
    aliases=("raydium_clmm",),
    gateway_connector=ImportRef(
        module="almanak.connectors.raydium.gateway.provider",
        attribute="RaydiumGatewayConnector",
        order=21,
    ),
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.raydium.receipt_parser_provider",
        attribute="RaydiumReceiptParserConnector",
    ),
)

__all__ = ["CONNECTOR"]
