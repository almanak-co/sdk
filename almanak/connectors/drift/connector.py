"""Drift connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
)

CONNECTOR = Connector(
    name="drift",
    kind=ProtocolKind.PERP,
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.drift.receipt_parser_provider",
        attribute="DriftReceiptParserConnector",
    ),
)

__all__ = ["CONNECTOR"]
