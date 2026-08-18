"""Aerodrome connector manifest."""

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
    FeeModelDecl,
    FungibleLpCloseDecl,
    ImportRef,
    MetadataAmountEncoding,
    SupportedChainsSpec,
    VenueVerifierDecl,
)
from almanak.connectors._strategy_base.address_table import AddressTableSpec
from almanak.core.capability_obligations import ObligationId
from almanak.core.chains.base import DESCRIPTOR as BASE
from almanak.core.chains.optimism import DESCRIPTOR as OPTIMISM
from almanak.core.intent_types import IntentType
from almanak.framework.primitives.types import Primitive

_PROVIDER_REFS = {
    ObligationId.ASSET_RESOLUTION: "almanak.connectors.aerodrome.compiler:AerodromeCompiler",
    ObligationId.VENUE_RESOLUTION: "almanak.connectors.aerodrome.compiler:AerodromeCompiler",
    ObligationId.AMOUNT_PROTECTION: "almanak.connectors.aerodrome.adapter:AerodromeAdapter",
    ObligationId.COMPILER: "almanak.connectors.aerodrome.compiler:AerodromeCompiler",
    ObligationId.RECEIPT_EVIDENCE: "almanak.connectors.aerodrome.receipt_parser:AerodromeReceiptParser",
    ObligationId.MONEY_LEGS: "almanak.connectors.aerodrome.receipt_parser:AerodromeReceiptParser",
    ObligationId.PERMISSION_PLAN: "almanak.connectors.aerodrome.permission_hints:PERMISSION_HINTS",
}

_SLIPSTREAM_PROVIDER_REFS = {
    **_PROVIDER_REFS,
    ObligationId.RECEIPT_EVIDENCE: "almanak.connectors.aerodrome.receipt_parser:AerodromeSlipstreamReceiptParser",
    ObligationId.PERMISSION_PLAN: "almanak.connectors.aerodrome.permission_hints:PERMISSION_HINTS_SLIPSTREAM",
}


def _production_lifecycle_declarations():
    cells: list[AmmCoreExecutionCell] = []
    for chain in (BASE, OPTIMISM):
        folder = chain.name
        cells.extend(
            (
                AmmCoreExecutionCell(
                    chain=chain,
                    intent=IntentType.SWAP,
                    real_fork_ref=f"tests/intents/{folder}/test_aerodrome_swap.py",
                    obligation_gap_refs=(((ObligationId.PERMISSION_PLAN, "VIB-6016"),) if chain is BASE else ()),
                ),
                AmmCoreExecutionCell(
                    chain=chain,
                    intent=IntentType.LP_OPEN,
                    real_fork_ref=f"tests/intents/{folder}/test_aerodrome_lp.py",
                    obligation_gap_refs=((ObligationId.AMOUNT_PROTECTION, "VIB-6223"),),
                ),
                AmmCoreExecutionCell(
                    chain=chain,
                    intent=IntentType.LP_CLOSE,
                    real_fork_ref=f"tests/intents/{folder}/test_aerodrome_lp.py",
                    obligation_gap_refs=((ObligationId.AMOUNT_PROTECTION, "VIB-6223"),),
                ),
            )
        )
    classic = build_amm_core_execution_declarations(
        protocol="aerodrome",
        cells=tuple(cells),
        provider_refs=_PROVIDER_REFS,
        contract_version="aerodrome.core_execution.v1",
    )
    slipstream = build_amm_core_execution_declarations(
        protocol="aerodrome_slipstream",
        cells=(
            AmmCoreExecutionCell(
                chain=BASE,
                intent=IntentType.SWAP,
                real_fork_ref="tests/intents/base/test_aerodrome_swap.py",
            ),
            AmmCoreExecutionCell(
                chain=BASE,
                intent=IntentType.LP_OPEN,
                real_fork_ref="tests/intents/base/test_aerodrome_slipstream_lp.py",
            ),
            AmmCoreExecutionCell(
                chain=BASE,
                intent=IntentType.LP_CLOSE,
                real_fork_ref="tests/intents/base/test_aerodrome_slipstream_lp.py",
                obligation_gap_refs=((ObligationId.AMOUNT_PROTECTION, "VIB-6235"),),
            ),
        ),
        provider_refs=_SLIPSTREAM_PROVIDER_REFS,
        contract_version="aerodrome_slipstream.core_execution.v1",
    )
    return classic + slipstream


CONNECTOR = Connector(
    name="aerodrome",
    kind=ProtocolKind.LP,
    # DefiLlama's yields catalog tracks Aerodrome classic (Solidly vAMM/sAMM)
    # under project "aerodrome-v1"; "aerodrome-v2" exists in no DefiLlama
    # namespace (yields projects are aerodrome-v1 / aerodrome-slipstream, and
    # api.llama.fi/protocol/aerodrome-v2 is Protocol-not-found).
    external_ids={"defillama": "aerodrome-v1"},
    # VIB-6162. Classic Solidly pools are fungible: the POOL CONTRACT IS THE LP TOKEN,
    # so a close that withdraws `balanceOf(wallet)` burns any LP the wallet already
    # held -- a user's own position, or another deployment's -- and books the proceeds
    # as this strategy's PnL. Clamping bounds the close to this deployment's own
    # outstanding liquidity.
    #
    # `raw`/18: Aerodrome writes BASE UNITS into position_events.liquidity where Curve
    # writes token units. Same column, different units per connector -- verified on
    # captured rows, and not inferable from the JSON shape, which is why it is declared
    # here rather than guessed by the framework.
    #
    # Clamped even though `fungible_lp` is False. The two flags are independent: that
    # one auto-registers the framework-default teardown post-condition, whose closure
    # rule is `balanceOf <= 10 wei` and would report a CORRECT clamped close as FAILED.
    # Aerodrome has no such post-condition registered (VIB-6487), so the clamp is safe
    # here and is NOT yet safe on Curve (VIB-6489).
    fungible_lp_close=FungibleLpCloseDecl(
        units="raw",
        decimals=18,
        clamp=True,
        identity=ImportRef(
            module="almanak.connectors._strategy_base.fungible_lp_identity",
            attribute="canonical_pool_key",
        ),
    ),
    dex_volume=DexVolumeDecl(
        chains=("base",),
        amm_family="solidly_v2",
        chain_default=("base",),
        # Slipstream (CL) volume rides the same connector; the canonical slug
        # callers pass is "aerodrome_slipstream", so the DEX-volume caller path
        # (DexVolumeRegistry → MultiDEXVolumeProvider) resolves it here instead
        # of falling through to the LOW-confidence unknown-protocol fallback.
        aliases=("aerodrome_slipstream",),
        liquidity_subgraph_ids={
            "base": "GENunSHWLBXm59mBSgPzQ8metBEp9YDfdqwFr91Av1UM",
        },
        # The declared subgraph is a uniswap-v3 fork exposing poolDayDatas;
        # the solidly pairDayDatas query hard-errors against it (ALM-2930).
        liquidity_query_family="v3_concentrated",
    ),
    fee_model=FeeModelDecl(
        model=ImportRef(module="almanak.connectors.aerodrome.fee_model", attribute="AerodromeFeeModel"),
        description="Aerodrome DEX fee model with stable/volatile pool distinction",
        aliases=("aero", "velodrome"),
    ),
    # Velodrome (the Optimism original Aerodrome forked) has no connector
    # package; this folder owns its backtest detection key, mirroring the
    # fee-model alias above.
    backtest_strategy_type=BacktestStrategyTypeDecl(
        strategy_type="lp",
        aliases=("velodrome",),
        # Plain aerodrome/velodrome pools are solidly-style fungible shares;
        # the slipstream product is concentrated liquidity.
        lp_economic_family="fungible",
        lp_economic_family_overrides={"aerodrome_slipstream": "concentrated", "velodrome_slipstream": "concentrated"},
    ),
    aliases=("aerodrome_slipstream",),
    address_tables=(
        AddressTableSpec(
            protocol="aerodrome",
            module="almanak.connectors.aerodrome.addresses",
            attribute="AERODROME",
        ),
    ),
    gateway_connector=ImportRef(
        module="almanak.connectors.aerodrome.gateway.provider",
        attribute="AerodromeGatewayConnector",
        order=13,
    ),
    gateway_connectors=(
        ImportRef(
            module="almanak.connectors.aerodrome.gateway.provider",
            attribute="AerodromeSlipstreamGatewayConnector",
            order=30,
        ),
    ),
    pool_data=ImportRef(
        module="almanak.connectors.aerodrome.pool_reader",
        attribute="POOL_DATA_SPECS",
    ),
    agent_read_connector=ImportRef(
        module="almanak.connectors.aerodrome.agent_read_provider",
        attribute="AerodromeSlipstreamAgentReadConnector",
        order=3,
    ),
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.aerodrome.receipt_parser_provider",
        attribute="AerodromeReceiptParserConnector",
    ),
    swap_quote_connector=ImportRef(
        module="almanak.connectors.aerodrome.swap_quote_provider",
        attribute="AerodromeSwapQuoteConnector",
    ),
    contract_monitoring=ImportRef(
        module="almanak.connectors.aerodrome.contract_monitoring",
        attribute="AERODROME_CONTRACT_MONITORING_SPECS",
    ),
    contract_roles=ImportRef(
        module="almanak.connectors.aerodrome.contract_roles",
        attribute="CONTRACT_ROLES",
        order=5,
    ),
    protocol_family=ImportRef(
        module="almanak.connectors.aerodrome.protocol_family",
        attribute="PROTOCOL_FAMILY",
    ),
    compiler=ImportRef(
        module="almanak.connectors.aerodrome.compiler",
        attribute="AerodromeCompiler",
    ),
    primitive=ImportRef(
        module="almanak.connectors.aerodrome.primitive",
        attribute="PRIMITIVE",
    ),
    venue_verifiers=(
        VenueVerifierDecl(
            protocol="aerodrome_slipstream",
            verifier=ImportRef(
                module="almanak.connectors.aerodrome.venue_verifier",
                attribute="SlipstreamVenueVerifier",
            ),
            contract_version="aerodrome_slipstream_exact_pool.v1",
            binding_policy_version=1,
            chains=(BASE,),
            primitives=(Primitive.LP,),
            component_names=("tick_spacing",),
        ),
    ),
    # Aerodrome's SWAP compiler ships amount_in as a human-readable Decimal (VIB-3747).
    metadata_amount_encoding=MetadataAmountEncoding(swap="human"),
    strategy_intents=(IntentType.SWAP, IntentType.LP_OPEN, IntentType.LP_CLOSE),
    supported_chains=SupportedChainsSpec(
        chains=(BASE, OPTIMISM),
        # Slipstream (concentrated liquidity) is deployed on Base only. Optimism
        # is Velodrome V2 — the Solidly-fork Classic pools — and `addresses.py`
        # carries no `cl_*` entries for it: no cl_factory, no cl_nft
        # (NonfungiblePositionManager), no cl_quoter. Without the override the
        # alias inherits the connector union and publishes an optimism row that
        # cannot compile an LP_OPEN. Optimism's concentrated venue is Velodrome
        # Slipstream, which has its own row.
        protocol_overrides={"aerodrome_slipstream": (BASE,)},
    ),
    lifecycle_declarations=_production_lifecycle_declarations(),
)

__all__ = ["CONNECTOR"]
