"""Morpho Blue connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
)

CONNECTOR = Connector(
    name="morpho_blue",
    kind=ProtocolKind.LENDING,
    aliases=("morpho",),
    gateway_connector=ImportRef(
        module="almanak.connectors.morpho_blue.gateway.provider",
        attribute="MorphoBlueGatewayConnector",
        order=27,
    ),
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.morpho_blue.receipt_parser_provider",
        attribute="MorphoBlueReceiptParserConnector",
    ),
)

__all__ = ["CONNECTOR"]
