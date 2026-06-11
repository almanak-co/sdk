"""Uniswap V3 connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    DexVolumeDecl,
    FeeModelDecl,
    ImportRef,
)
from almanak.connectors._strategy_base.address_table import AbiFamily, AddressTableSpec
from almanak.connectors._strategy_base.protocol_ownership import CapabilitiesSpec, SupportedChainsSpec

_V3_ABI_FAMILIES = (AbiFamily.V3_FACTORY, AbiFamily.V3_NPM)

CONNECTOR = Connector(
    name="uniswap_v3",
    kind=ProtocolKind.LP,
    dex_volume=DexVolumeDecl(
        chains=("ethereum", "arbitrum", "base", "optimism", "polygon"),
        amm_family="v3_concentrated",
        aliases=("uni_v3",),
        generic_default=True,
    ),
    fee_model=FeeModelDecl(
        model=ImportRef(module="almanak.connectors.uniswap_v3.fee_model", attribute="UniswapV3FeeModel"),
        description="Uniswap V3 DEX fee model with tier-based fees",
        aliases=("uniswap", "uni_v3"),
    ),
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
    pool_reader=ImportRef(
        module="almanak.connectors.uniswap_v3.pool_reader",
        attribute="POOL_READER_SPEC",
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
    swap_quote_connector=ImportRef(
        module="almanak.connectors.uniswap_v3.swap_quote_provider",
        attribute="UniswapV3SwapQuoteConnector",
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
    compiler=ImportRef(
        module="almanak.connectors.uniswap_v3.compiler",
        attribute="UniswapV3Compiler",
    ),
    capabilities=CapabilitiesSpec(
        keys=("uniswap_v3",),
        module="almanak.connectors.uniswap_v3.capabilities",
    ),
    supported_chains=SupportedChainsSpec(
        keys=("uniswap_v3", "agni_finance"),
        module="almanak.connectors.uniswap_v3.supported_chains",
    ),
    primitive=ImportRef(
        module="almanak.connectors.uniswap_v3.primitive",
        attribute="PRIMITIVE",
    ),
    strategy_intents=("SWAP", "LP_OPEN", "LP_CLOSE", "LP_COLLECT_FEES"),
    strategy_chains=("ethereum", "arbitrum", "optimism", "polygon", "base", "avalanche", "bnb", "monad"),
)

__all__ = ["CONNECTOR"]
