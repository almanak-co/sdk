"""PancakeSwap Perps connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
)

CONNECTOR = Connector(
    name="pancakeswap_perps",
    kind=ProtocolKind.PERP,
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.pancakeswap_perps.receipt_parser_provider",
        attribute="PancakeSwapPerpsReceiptParserConnector",
    ),
)

__all__ = ["CONNECTOR"]
