"""SushiSwap V3 connector manifest."""

from __future__ import annotations

from almanak.connectors._amm_lifecycle_declaration import (
    AmmCoreExecutionCell,
    build_amm_core_execution_declarations,
)
from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    BacktestStrategyTypeDecl,
    Connector,
    DexVolumeDecl,
    ImportRef,
    SupportedChainsSpec,
)
from almanak.connectors._strategy_base.address_table import AbiFamily, AddressTableSpec
from almanak.core.capability_obligations import ObligationId
from almanak.core.chains.arbitrum import DESCRIPTOR as ARBITRUM
from almanak.core.chains.base import DESCRIPTOR as BASE
from almanak.core.chains.bsc import DESCRIPTOR as BSC
from almanak.core.chains.ethereum import DESCRIPTOR as ETHEREUM
from almanak.core.chains.optimism import DESCRIPTOR as OPTIMISM
from almanak.core.chains.polygon import DESCRIPTOR as POLYGON
from almanak.core.intent_types import IntentType

_PROVIDER_REFS = {
    ObligationId.ASSET_RESOLUTION: "almanak.connectors.uniswap_v3.compiler:UniswapV3Compiler",
    ObligationId.VENUE_RESOLUTION: "almanak.connectors.uniswap_v3.compiler:UniswapV3Compiler",
    ObligationId.AMOUNT_PROTECTION: "almanak.connectors.uniswap_v3.compiler:UniswapV3Compiler",
    ObligationId.COMPILER: "almanak.connectors.uniswap_v3.compiler:UniswapV3Compiler",
    ObligationId.RECEIPT_EVIDENCE: "almanak.connectors.sushiswap_v3.receipt_parser:SushiSwapV3ReceiptParser",
    ObligationId.MONEY_LEGS: "almanak.connectors.sushiswap_v3.receipt_parser:SushiSwapV3ReceiptParser",
    ObligationId.PERMISSION_PLAN: "almanak.connectors.sushiswap_v3.permission_hints:PERMISSION_HINTS",
}


def _production_lifecycle_declarations():
    cells: list[AmmCoreExecutionCell] = []
    for chain, folder in (
        (ARBITRUM, "arbitrum"),
        (BASE, "base"),
        (BSC, "bnb"),
        (ETHEREUM, "ethereum"),
        (OPTIMISM, "optimism"),
        (POLYGON, "polygon"),
    ):
        cells.extend(
            (
                AmmCoreExecutionCell(
                    chain=chain,
                    intent=IntentType.SWAP,
                    real_fork_ref=f"tests/intents/{folder}/test_sushiswap_v3_swap.py",
                    obligation_gap_refs=((ObligationId.AMOUNT_PROTECTION, "ALM-3041"),),
                ),
                AmmCoreExecutionCell(
                    chain=chain,
                    intent=IntentType.LP_OPEN,
                    real_fork_ref=f"tests/intents/{folder}/test_sushiswap_v3_lp.py",
                ),
                AmmCoreExecutionCell(
                    chain=chain,
                    intent=IntentType.LP_CLOSE,
                    real_fork_ref=f"tests/intents/{folder}/test_sushiswap_v3_lp.py",
                    obligation_gap_refs=((ObligationId.AMOUNT_PROTECTION, "VIB-6220"),),
                ),
                AmmCoreExecutionCell(
                    chain=chain,
                    intent=IntentType.LP_COLLECT_FEES,
                    lane_gap_ref="VIB-5968",
                ),
            )
        )
    return build_amm_core_execution_declarations(
        protocol="sushiswap_v3",
        cells=tuple(cells),
        provider_refs=_PROVIDER_REFS,
        contract_version="sushiswap_v3.core_execution.v1",
    )


CONNECTOR = Connector(
    name="sushiswap_v3",
    kind=ProtocolKind.LP,
    dex_volume=DexVolumeDecl(
        chains=("ethereum",),
        amm_family="v3_concentrated",
        aliases=("sushi_v3",),
        liquidity_subgraph_ids={
            "ethereum": "2tGWMrDha4164KkFAfkU3rDCtuxGb4q1emXmFdLLzJ8x",
        },
        # The declared deployment is Messari-standard (liquidityPoolDailySnapshots),
        # not the uniswap-v3 fork schema.
        liquidity_query_family="messari_standard",
    ),
    # Legacy backtest detection key is the bare "sushiswap".
    backtest_strategy_type=BacktestStrategyTypeDecl(
        strategy_type="lp",
        name="sushiswap",
        lp_economic_family="concentrated",
        lp_economic_family_overrides={"sushiswap_v3": "concentrated"},
    ),
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
    pool_data=ImportRef(
        module="almanak.connectors.sushiswap_v3.pool_reader",
        attribute="POOL_DATA_SPEC",
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
    strategy_intents=(IntentType.SWAP, IntentType.LP_OPEN, IntentType.LP_CLOSE, IntentType.LP_COLLECT_FEES),
    supported_chains=SupportedChainsSpec(chains=(ETHEREUM, ARBITRUM, BASE, OPTIMISM, POLYGON, BSC)),
    lifecycle_declarations=_production_lifecycle_declarations(),
)

__all__ = ["CONNECTOR"]
