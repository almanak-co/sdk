"""PancakeSwap V3 connector manifest."""

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
    ExactVenueDataProviderDecl,
    FeeModelDecl,
    ImportRef,
    SupportedChainsSpec,
    VenueVerifierDecl,
)
from almanak.connectors._strategy_base.address_table import AbiFamily, AddressTableSpec
from almanak.core.capability_obligations import ExactTargetFeature, ObligationId
from almanak.core.chains.arbitrum import DESCRIPTOR as ARBITRUM
from almanak.core.chains.base import DESCRIPTOR as BASE
from almanak.core.chains.bsc import DESCRIPTOR as BSC
from almanak.core.chains.ethereum import DESCRIPTOR as ETHEREUM
from almanak.core.intent_types import IntentType
from almanak.framework.primitives.types import Primitive

_PROVIDER_REFS = {
    ObligationId.ASSET_RESOLUTION: "almanak.connectors.uniswap_v3.compiler:UniswapV3Compiler",
    ObligationId.VENUE_RESOLUTION: "almanak.connectors.uniswap_v3.compiler:UniswapV3Compiler",
    ObligationId.AMOUNT_PROTECTION: "almanak.connectors.uniswap_v3.compiler:UniswapV3Compiler",
    ObligationId.COMPILER: "almanak.connectors.uniswap_v3.compiler:UniswapV3Compiler",
    ObligationId.RECEIPT_EVIDENCE: "almanak.connectors.pancakeswap_v3.receipt_parser:PancakeSwapV3ReceiptParser",
    ObligationId.MONEY_LEGS: "almanak.connectors.pancakeswap_v3.receipt_parser:PancakeSwapV3ReceiptParser",
    ObligationId.PERMISSION_PLAN: "almanak.connectors.pancakeswap_v3.permission_hints:PERMISSION_HINTS",
}


def _production_lifecycle_declarations():
    cells: list[AmmCoreExecutionCell] = []
    for chain, folder in ((ARBITRUM, "arbitrum"), (BASE, "base"), (BSC, "bnb"), (ETHEREUM, "ethereum")):
        swap_gap = "VIB-5974" if chain in (ARBITRUM, ETHEREUM) else None
        cells.extend(
            (
                AmmCoreExecutionCell(
                    chain=chain,
                    intent=IntentType.SWAP,
                    real_fork_ref=None if swap_gap else f"tests/intents/{folder}/test_pancakeswap_v3_swap.py",
                    lane_gap_ref=swap_gap,
                    obligation_gap_refs=(((ObligationId.AMOUNT_PROTECTION, "ALM-3041"),) if swap_gap is None else ()),
                ),
                AmmCoreExecutionCell(
                    chain=chain,
                    intent=IntentType.LP_OPEN,
                    real_fork_ref=f"tests/intents/{folder}/test_pancakeswap_v3_lp.py",
                ),
                AmmCoreExecutionCell(
                    chain=chain,
                    intent=IntentType.LP_CLOSE,
                    real_fork_ref=f"tests/intents/{folder}/test_pancakeswap_v3_lp.py",
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
        protocol="pancakeswap_v3",
        cells=tuple(cells),
        provider_refs=_PROVIDER_REFS,
        contract_version="pancakeswap_v3.core_execution.v1",
    )


CONNECTOR = Connector(
    name="pancakeswap_v3",
    kind=ProtocolKind.LP,
    external_ids={"defillama": "pancakeswap-amm-v3"},
    dex_volume=DexVolumeDecl(
        chains=("ethereum", "arbitrum", "bsc", "base"),
        amm_family="v3_concentrated",
        aliases=("pancake_v3",),
        liquidity_subgraph_ids={
            # ethereum: the V3-native deployment has sick indexers; bsc:
            # frozen since 2025-08 — both point at current Messari-standard
            # deployments instead.
            "ethereum": "JAGXF8B14mpB8QGKnwhKTs5JxsQZBJQvbDGFcWwL7gbm",
            "arbitrum": "251MHFNN1rwjErXD2efWMpNS73SANZN8Ua192zw6iXve",
            "bsc": "ChmxqA9bX71cB2cQTRRULbWUBKoMRk7oh3JnpZShDQ2V",
            "base": "BHWNsedAHtmTCzXxCCDfhPmm6iN9rxUhoRHdHKyujic3",
        },
        liquidity_query_family_overrides={
            "ethereum": "messari_standard",
            "bsc": "messari_standard",
        },
    ),
    fee_model=FeeModelDecl(
        model=ImportRef(module="almanak.connectors.pancakeswap_v3.fee_model", attribute="PancakeSwapV3FeeModel"),
        description="PancakeSwap V3 DEX fee model with tier-based fees (0.01%, 0.05%, 0.25%, 1%)",
        aliases=("pancakeswap", "pancake_v3", "pcs_v3"),
    ),
    backtest_strategy_type=BacktestStrategyTypeDecl(
        strategy_type="lp", aliases=("pancakeswap",), lp_economic_family="concentrated"
    ),
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
    pool_data=ImportRef(
        module="almanak.connectors.pancakeswap_v3.pool_reader",
        attribute="POOL_DATA_SPEC",
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
    venue_verifiers=(
        VenueVerifierDecl(
            protocol="pancakeswap_v3",
            verifier=ImportRef(
                module="almanak.connectors._strategy_base.v3_venue_verifier",
                attribute="V3VenueVerifier",
            ),
            contract_version="v3_exact_pool.v1",
            binding_policy_version=1,
            chains=(ARBITRUM, BASE, BSC, ETHEREUM),
            primitives=(Primitive.LP, Primitive.SWAP),
            component_names=("fee",),
        ),
    ),
    exact_venue_data_providers=(
        ExactVenueDataProviderDecl(
            protocol="pancakeswap_v3",
            provider=ImportRef(
                module="almanak.connectors._strategy_base.v3_exact_data_provider",
                attribute="V3ExactVenueDataProvider",
            ),
            contract_version="v3_exact_data.v1",
            chains=(ARBITRUM, BASE, BSC, ETHEREUM),
            features=(ExactTargetFeature.OHLCV, ExactTargetFeature.TWAP),
        ),
    ),
    strategy_intents=(IntentType.SWAP, IntentType.LP_OPEN, IntentType.LP_CLOSE, IntentType.LP_COLLECT_FEES),
    supported_chains=SupportedChainsSpec(chains=(BSC, ETHEREUM, ARBITRUM, BASE)),
    lifecycle_declarations=_production_lifecycle_declarations(),
)

__all__ = ["CONNECTOR"]
