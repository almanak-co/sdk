"""PancakeSwap V3 connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    BacktestStrategyTypeDecl,
    Connector,
    DexVolumeDecl,
    FeeModelDecl,
    ImportRef,
)
from almanak.connectors._strategy_base.address_table import AbiFamily, AddressTableSpec
from almanak.connectors._strategy_base.protocol_ownership import SupportedChainsSpec

CONNECTOR = Connector(
    name="pancakeswap_v3",
    kind=ProtocolKind.LP,
    dex_volume=DexVolumeDecl(
        chains=("ethereum", "arbitrum", "bsc", "base"),
        amm_family="v3_concentrated",
        aliases=("pancake_v3",),
    ),
    fee_model=FeeModelDecl(
        model=ImportRef(module="almanak.connectors.pancakeswap_v3.fee_model", attribute="PancakeSwapV3FeeModel"),
        description="PancakeSwap V3 DEX fee model with tier-based fees (0.01%, 0.05%, 0.25%, 1%)",
        aliases=("pancakeswap", "pancake_v3", "pcs_v3"),
    ),
    backtest_strategy_type=BacktestStrategyTypeDecl(strategy_type="lp", aliases=("pancakeswap",)),
    address_tables=(
        AddressTableSpec(
            protocol="pancakeswap_v3",
            module="almanak.connectors.pancakeswap_v3.addresses",
            attribute="PANCAKESWAP_V3",
            abi_families=(AbiFamily.V3_FACTORY, AbiFamily.V3_NPM),
            abi_family_order=3,
        ),
    ),
    gateway_connector=ImportRef(
        module="almanak.connectors.pancakeswap_v3.gateway.provider",
        attribute="PancakeSwapV3GatewayConnector",
        order=20,
    ),
    pool_reader=ImportRef(
        module="almanak.connectors.pancakeswap_v3.pool_reader",
        attribute="POOL_READER_SPEC",
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
    compiler=ImportRef(
        module="almanak.connectors.uniswap_v3.compiler",
        attribute="UniswapV3Compiler",
    ),
    supported_chains=SupportedChainsSpec(
        keys=("pancakeswap_v3",),
        module="almanak.connectors.pancakeswap_v3.supported_chains",
    ),
    strategy_intents=("SWAP", "LP_OPEN", "LP_CLOSE", "LP_COLLECT_FEES"),
    strategy_chains=("bnb", "ethereum", "arbitrum", "base"),
)

__all__ = ["CONNECTOR"]
