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
    agent_read_connector=ImportRef(
        module="almanak.connectors.pancakeswap_v3.agent_read_provider",
        attribute="PancakeswapV3AgentReadConnector",
        order=4,
    ),
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.pancakeswap_v3.receipt_parser_provider",
        attribute="PancakeSwapV3ReceiptParserConnector",
    ),
    contract_monitoring=ImportRef(
        module="almanak.connectors.pancakeswap_v3.contract_monitoring",
        attribute="PANCAKESWAP_V3_CONTRACT_MONITORING_SPECS",
    ),
    contract_roles=ImportRef(
        module="almanak.connectors.pancakeswap_v3.contract_roles",
        attribute="CONTRACT_ROLES",
        order=4,
    ),
    swap_classification=ImportRef(
        module="almanak.connectors.pancakeswap_v3.swap_classification",
        attribute="SWAP_CLASSIFICATION",
        order=3,
    ),
    protocol_family=ImportRef(
        module="almanak.connectors.pancakeswap_v3.protocol_family",
        attribute="PROTOCOL_FAMILY",
    ),
)

__all__ = ["CONNECTOR"]
