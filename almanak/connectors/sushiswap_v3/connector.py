"""SushiSwap V3 connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
)
from almanak.connectors._strategy_base.address_table import AbiFamily, AddressTableSpec
from almanak.connectors._strategy_base.protocol_ownership import SupportedChainsSpec

CONNECTOR = Connector(
    name="sushiswap_v3",
    kind=ProtocolKind.LP,
    address_tables=(
        AddressTableSpec(
            protocol="sushiswap_v3",
            module="almanak.connectors.sushiswap_v3.addresses",
            attribute="SUSHISWAP_V3",
            abi_families=(AbiFamily.V3_FACTORY, AbiFamily.V3_NPM),
            abi_family_order=4,
        ),
    ),
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
    contract_monitoring=ImportRef(
        module="almanak.connectors.sushiswap_v3.contract_monitoring",
        attribute="SUSHISWAP_V3_CONTRACT_MONITORING_SPECS",
    ),
    contract_roles=ImportRef(
        module="almanak.connectors.sushiswap_v3.contract_roles",
        attribute="CONTRACT_ROLES",
        order=3,
    ),
    swap_classification=ImportRef(
        module="almanak.connectors.sushiswap_v3.swap_classification",
        attribute="SWAP_CLASSIFICATION",
        order=2,
    ),
    protocol_family=ImportRef(
        module="almanak.connectors.sushiswap_v3.protocol_family",
        attribute="PROTOCOL_FAMILY",
    ),
    compiler=ImportRef(
        module="almanak.connectors.uniswap_v3.compiler",
        attribute="UniswapV3Compiler",
    ),
    supported_chains=SupportedChainsSpec(
        keys=("sushiswap_v3",),
        module="almanak.connectors.sushiswap_v3.supported_chains",
    ),
    strategy_intents=("SWAP", "LP_OPEN", "LP_CLOSE", "LP_COLLECT_FEES"),
    strategy_chains=("ethereum", "arbitrum", "base", "optimism", "polygon", "bnb"),
)

__all__ = ["CONNECTOR"]
