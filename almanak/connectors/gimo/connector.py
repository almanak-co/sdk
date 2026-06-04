"""Gimo connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
)

CONNECTOR = Connector(
    name="gimo",
    kind=ProtocolKind.LENDING,
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.gimo.receipt_parser_provider",
        attribute="GimoReceiptParserConnector",
    ),
)

__all__ = ["CONNECTOR"]
