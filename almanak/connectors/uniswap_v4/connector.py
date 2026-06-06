"""Uniswap V4 connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
)

CONNECTOR = Connector(
    name="uniswap_v4",
    kind=ProtocolKind.LP,
    gateway_connector=ImportRef(
        module="almanak.connectors.uniswap_v4.gateway.provider",
        attribute="UniswapV4GatewayConnector",
        order=1,
    ),
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.uniswap_v4.receipt_parser_provider",
        attribute="UniswapV4ReceiptParserConnector",
    ),
    contract_monitoring=ImportRef(
        module="almanak.connectors.uniswap_v4.contract_monitoring",
        attribute="UNISWAP_V4_CONTRACT_MONITORING_SPECS",
    ),
    runner_hook_connector=ImportRef(
        module="almanak.connectors.uniswap_v4.runner_hooks",
        attribute="UniswapV4RunnerHookConnector",
    ),
    contract_roles=ImportRef(
        module="almanak.connectors.uniswap_v4.contract_roles",
        attribute="CONTRACT_ROLES",
        order=2,
    ),
)

__all__ = ["CONNECTOR"]
