"""Across connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
)

CONNECTOR = Connector(
    name="across",
    kind=ProtocolKind.BRIDGE,
    gas_estimate_connector=ImportRef(
        module="almanak.connectors.across.gas_estimate_provider",
        attribute="AcrossGasEstimateConnector",
    ),
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.across.receipt_parser_provider",
        attribute="AcrossReceiptParserConnector",
    ),
)

__all__ = ["CONNECTOR"]
