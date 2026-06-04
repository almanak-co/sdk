"""Meteora connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
)

CONNECTOR = Connector(
    name="meteora",
    kind=ProtocolKind.LP,
    aliases=("meteora_dlmm",),
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.meteora.receipt_parser_provider",
        attribute="MeteoraReceiptParserConnector",
    ),
)

__all__ = ["CONNECTOR"]
