"""Kamino connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
)

CONNECTOR = Connector(
    name="kamino",
    kind=ProtocolKind.LENDING,
    aliases=("kamino_klend",),
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.kamino.receipt_parser_provider",
        attribute="KaminoReceiptParserConnector",
    ),
)

__all__ = ["CONNECTOR"]
