"""Uniswap V3 connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
)

CONNECTOR = Connector(
    name="uniswap_v3",
    kind=ProtocolKind.LP,
    aliases=("agni_finance",),
    gateway_connector=ImportRef(
        module="almanak.connectors.uniswap_v3.gateway.provider",
        attribute="UniswapV3GatewayConnector",
        order=12,
    ),
    gateway_connectors=(
        ImportRef(
            module="almanak.connectors.uniswap_v3.gateway.agni_provider",
            attribute="AgniFinanceGatewayConnector",
            order=26,
        ),
    ),
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.uniswap_v3.receipt_parser_provider",
        attribute="UniswapV3ReceiptParserConnector",
    ),
)

__all__ = ["CONNECTOR"]
