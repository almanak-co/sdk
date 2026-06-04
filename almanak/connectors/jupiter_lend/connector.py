"""Jupiter Lend connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
)

CONNECTOR = Connector(
    name="jupiter_lend",
    kind=ProtocolKind.LENDING,
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.jupiter_lend.receipt_parser_provider",
        attribute="JupiterLendReceiptParserConnector",
    ),
)

__all__ = ["CONNECTOR"]
