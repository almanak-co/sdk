"""LiFi connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
)

CONNECTOR = Connector(
    name="lifi",
    kind=ProtocolKind.BRIDGE,
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.lifi.receipt_parser_provider",
        attribute="LiFiReceiptParserConnector",
    ),
)

__all__ = ["CONNECTOR"]
