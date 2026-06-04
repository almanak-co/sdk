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
    gas_estimate_connector=ImportRef(
        module="almanak.connectors.uniswap_v3.gas_estimate_provider",
        attribute="UniswapV3GasEstimateConnector",
    ),
    agent_read_connector=ImportRef(
        module="almanak.connectors.uniswap_v3.agent_read_provider",
        attribute="UniswapV3AgentReadConnector",
        order=1,
    ),
    agent_read_connectors=(
        ImportRef(
            module="almanak.connectors.uniswap_v3.agent_read_provider",
            attribute="AgniFinanceAgentReadConnector",
            order=2,
        ),
    ),
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.uniswap_v3.receipt_parser_provider",
        attribute="UniswapV3ReceiptParserConnector",
    ),
    contract_roles=ImportRef(
        module="almanak.connectors.uniswap_v3.contract_roles",
        attribute="CONTRACT_ROLES",
        order=1,
    ),
    swap_classification=ImportRef(
        module="almanak.connectors.uniswap_v3.swap_classification",
        attribute="SWAP_CLASSIFICATION",
        order=1,
    ),
    protocol_family=ImportRef(
        module="almanak.connectors.uniswap_v3.protocol_family",
        attribute="PROTOCOL_FAMILY",
    ),
)

__all__ = ["CONNECTOR"]
