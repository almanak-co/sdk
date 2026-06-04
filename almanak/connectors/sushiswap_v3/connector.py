"""SushiSwap V3 connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
)

CONNECTOR = Connector(
    name="sushiswap_v3",
    kind=ProtocolKind.LP,
    gateway_connector=ImportRef(
        module="almanak.connectors.sushiswap_v3.gateway.provider",
        attribute="SushiSwapV3GatewayConnector",
        order=25,
    ),
    agent_read_connector=ImportRef(
        module="almanak.connectors.sushiswap_v3.agent_read_provider",
        attribute="SushiswapV3AgentReadConnector",
        order=5,
    ),
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.sushiswap_v3.receipt_parser_provider",
        attribute="SushiSwapV3ReceiptParserConnector",
    ),
)

__all__ = ["CONNECTOR"]
