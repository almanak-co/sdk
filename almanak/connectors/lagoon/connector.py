"""Lagoon connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
)

CONNECTOR = Connector(
    name="lagoon",
    kind=ProtocolKind.VAULT,
    vault_tool_connector=ImportRef(
        module="almanak.connectors.lagoon.vault_tool_provider",
        attribute="LagoonVaultToolConnector",
        order=1,
    ),
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.lagoon.receipt_parser_provider",
        attribute="LagoonReceiptParserConnector",
    ),
)

__all__ = ["CONNECTOR"]
