"""Silo V2 connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
)

CONNECTOR = Connector(
    name="silo_v2",
    kind=ProtocolKind.LENDING,
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.silo_v2.receipt_parser_provider",
        attribute="SiloV2ReceiptParserConnector",
    ),
)

__all__ = ["CONNECTOR"]
