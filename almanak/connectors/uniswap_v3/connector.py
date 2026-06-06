"""Uniswap V3 connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
)
from almanak.connectors._strategy_base.address_table import AbiFamily, AddressTableSpec

_V3_ABI_FAMILIES = (AbiFamily.V3_FACTORY, AbiFamily.V3_NPM)

CONNECTOR = Connector(
    name="uniswap_v3",
    kind=ProtocolKind.LP,
    aliases=("agni_finance",),
    address_tables=(
        AddressTableSpec(
            protocol="uniswap_v3",
            module="almanak.connectors.uniswap_v3.addresses",
            attribute="UNISWAP_V3",
            abi_families=_V3_ABI_FAMILIES,
            abi_family_order=1,
        ),
        AddressTableSpec(
            protocol="agni_finance",
            module="almanak.connectors.uniswap_v3.addresses",
            attribute="AGNI_FINANCE",
            abi_families=_V3_ABI_FAMILIES,
            abi_family_order=2,
        ),
    ),
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
    contract_monitoring=ImportRef(
        module="almanak.connectors.uniswap_v3.contract_monitoring",
        attribute="UNISWAP_V3_CONTRACT_MONITORING_SPECS",
    ),
    runner_hook_connector=ImportRef(
        module="almanak.connectors.uniswap_v3.runner_hooks",
        attribute="UniswapV3RunnerHookConnector",
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
    strategy_intents=("SWAP", "LP_OPEN", "LP_CLOSE", "LP_COLLECT_FEES"),
    strategy_chains=("ethereum", "arbitrum", "optimism", "polygon", "base", "avalanche", "bnb", "monad"),
)

__all__ = ["CONNECTOR"]
