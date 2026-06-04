"""PancakeSwap V3 connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
)

CONNECTOR = Connector(
    name="pancakeswap_v3",
    kind=ProtocolKind.LP,
    gateway_connector=ImportRef(
        module="almanak.connectors.pancakeswap_v3.gateway.provider",
        attribute="PancakeSwapV3GatewayConnector",
        order=20,
    ),
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.pancakeswap_v3.receipt_parser_provider",
        attribute="PancakeSwapV3ReceiptParserConnector",
    ),
)

__all__ = ["CONNECTOR"]
