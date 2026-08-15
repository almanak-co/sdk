"""Curve connector manifest."""

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
    PositionReadDecl,
    SupportedChainsSpec,
)
from almanak.connectors._strategy_base.position_read_base import CURVE_LP
from almanak.connectors._strategy_base.protocol_ownership import CapabilitiesSpec
from almanak.core.capability_obligations import ObligationId
from almanak.core.chains.arbitrum import DESCRIPTOR as ARBITRUM
from almanak.core.chains.base import DESCRIPTOR as BASE
from almanak.core.chains.ethereum import DESCRIPTOR as ETHEREUM
from almanak.core.chains.optimism import DESCRIPTOR as OPTIMISM
from almanak.core.chains.polygon import DESCRIPTOR as POLYGON
from almanak.core.intent_types import IntentType

_PROVIDER_REFS = {
    ObligationId.ASSET_RESOLUTION: "almanak.connectors.curve.compiler:CurveCompiler",
    ObligationId.VENUE_RESOLUTION: "almanak.connectors.curve.compiler:CurveCompiler",
    ObligationId.AMOUNT_PROTECTION: "almanak.connectors.curve.compiler:CurveCompiler",
    ObligationId.COMPILER: "almanak.connectors.curve.compiler:CurveCompiler",
    ObligationId.RECEIPT_EVIDENCE: "almanak.connectors.curve.receipt_parser:CurveReceiptParser",
    ObligationId.MONEY_LEGS: "almanak.connectors.curve.receipt_parser:CurveReceiptParser.extract_primitive_money_legs",
    ObligationId.PERMISSION_PLAN: "almanak.connectors.curve.permission_hints:PERMISSION_HINTS",
}


def _production_lifecycle_declarations():
    evidence = (
        (ARBITRUM, "tests/intents/arbitrum/test_curve_swap.py", "tests/intents/arbitrum/test_curve_lp.py"),
        (BASE, "tests/intents/base/test_curve_swap.py", "tests/intents/base/test_curve_lp_base.py"),
        (
            ETHEREUM,
            "tests/intents/ethereum/test_curve_cryptoswap_swap.py",
            "tests/intents/ethereum/test_curve_3pool_lp.py",
        ),
        (
            OPTIMISM,
            "tests/intents/optimism/test_curve_crvusd_usdc_swap.py",
            "tests/intents/optimism/test_curve_lp.py",
        ),
        (POLYGON, "tests/intents/polygon/test_curve_swap.py", "tests/intents/polygon/test_curve_lp.py"),
    )
    cells: list[AmmCoreExecutionCell] = []
    for chain, swap_ref, lp_ref in evidence:
        cells.extend(
            (
                AmmCoreExecutionCell(
                    chain=chain,
                    intent=IntentType.SWAP,
                    real_fork_ref=None if chain is OPTIMISM else swap_ref,
                    lane_gap_ref="VIB-6668" if chain is OPTIMISM else None,
                ),
                AmmCoreExecutionCell(
                    chain=chain,
                    intent=IntentType.LP_OPEN,
                    real_fork_ref=lp_ref,
                    money_leg_evidence_ref=(
                        "tests/unit/execution/test_result_enricher_curve_lp_open_legs.py" if chain is ETHEREUM else None
                    ),
                ),
                AmmCoreExecutionCell(chain=chain, intent=IntentType.LP_CLOSE, real_fork_ref=lp_ref),
            )
        )
    return build_amm_core_execution_declarations(
        protocol="curve",
        cells=tuple(cells),
        provider_refs=_PROVIDER_REFS,
        contract_version="curve.core_execution.v1",
    )


CONNECTOR = Connector(
    name="curve",
    kind=ProtocolKind.LP,
    capabilities=CapabilitiesSpec(
        keys=("curve",),
        module="almanak.connectors.curve.capabilities",
    ),
    dex_volume=DexVolumeDecl(
        chains=("ethereum", "optimism"),
        amm_family="stableswap",
        aliases=("crv",),
        volume_data_source="curve_messari_subgraph",
        liquidity_subgraph_ids={
            "ethereum": "3fy93eAT56UJsRCEht8iFhfi6wjHWXtZ9dnnbQmvFopF",
            # NOTE: dead on the gateway ("subgraph not found: no
            # allocations"); kept as a pointer — queries fail fast and
            # degrade to fallback.
            "optimism": "CXDZPduZE6nWuWEkSzWkRoJSSJ6CneSqiDxdnhhURShX",
        },
        # Declared deployments are Messari-standard (liquidityPoolDailySnapshots),
        # matching volume_data_source above — not a Curve-native schema.
        liquidity_query_family="messari_standard",
    ),
    fee_model=FeeModelDecl(
        model=ImportRef(module="almanak.connectors.curve.fee_model", attribute="CurveFeeModel"),
        description="Curve Finance DEX fee model with dynamic fee calculation",
        aliases=("curve_fi", "crv"),
    ),
    backtest_strategy_type=BacktestStrategyTypeDecl(strategy_type="lp", lp_economic_family="fungible"),
    gateway_connector=ImportRef(
        module="almanak.connectors.curve.gateway.provider",
        attribute="CurveGatewayConnector",
        order=24,
    ),
    swap_quote_connector=ImportRef(
        module="almanak.connectors.curve.swap_quote_provider",
        attribute="CurveSwapQuoteConnector",
    ),
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.curve.receipt_parser_provider",
        attribute="CurveReceiptParserConnector",
    ),
    # VIB-5628: the Curve receipt parser has no gateway client of its own; on a
    # runner-injected sync ``(pool_address, chain) -> CurvePoolMetadata | None``
    # lookup to label exact-pool LP legs. The enricher threads the kwarg only to parsers
    # that declare it (mirrors the V4 ``pool_key_lookup`` carve-out).
    receipt_parser_kwargs=("pool_meta_lookup",),
    # VIB-5628: publish the uncurated-pool metadata lookup as a runner hook so the
    # framework runner never imports a concrete Curve module (coupling boundary).
    runner_hook_connector=ImportRef(
        module="almanak.connectors.curve.runner_hooks",
        attribute="CurveRunnerHookConnector",
    ),
    compiler=ImportRef(
        module="almanak.connectors.curve.compiler",
        attribute="CurveCompiler",
    ),
    # Protocol-neutral pool-data declaration with a connector-selected
    # get_dy/coins reader. It never dispatches through the slot0 family.
    pool_data=ImportRef(
        module="almanak.connectors.curve.pool_reader",
        attribute="POOL_DATA_SPEC",
    ),
    # Curve's SWAP compiler ships amount_in as a human-readable Decimal (VIB-3747).
    metadata_amount_encoding=MetadataAmountEncoding(swap="human"),
    # Curve LP positions are fungible ERC20 LP tokens: LPCloseIntent.position_id
    # is overloaded as the burn AMOUNT, never an NFT discriminator (VIB-4968).
    fungible_lp=True,
    # VIB-6162 / VIB-6489. Curve's units are DECLARED but the clamp is deliberately
    # OFF, and the two are not the same statement: being readable is not being safe to
    # clamp.
    #
    # `fungible_lp=True` above auto-registers the framework-default teardown
    # post-condition, whose closure rule is `balanceOf(wallet) <= 10 wei` -- exactly
    # zero. A clamped close leaves the user's own LP behind BY DESIGN, that
    # post-condition measures precisely that residual, and so a CORRECT close would
    # report FAILED. The defect is load-bearing for its own verification, so enabling
    # the clamp here and redesigning the post-condition must land together (VIB-6489)
    # with its own Curve fork proof.
    #
    # `token`: Curve writes TOKEN units into position_events.liquidity where Aerodrome
    # writes raw base units.
    #
    # No identity resolver, which the declaration's own validation requires whenever
    # clamp is True -- so this cannot be flipped to True without also supplying one.
    # That is intentional: it makes the unsafe half-migration fail at import time
    # rather than ship as a clamp that silently never engages.
    fungible_lp_close=FungibleLpCloseDecl(units="token", clamp=False),
    # On-chain LP repricing is framework-owned (CurveLpPositionReader:
    # lp_balance × live virtual_price × numeraire). Declaring the curve_lp kind
    # routes the valuer's capability dispatch through PositionReadRegistry instead
    # of the reader's old hardcoded {"curve"} set (VIB-5420). No builder — the
    # math is framework-valued, not connector-side.
    position_read=PositionReadDecl(kind=CURVE_LP),
    strategy_intents=(IntentType.SWAP, IntentType.LP_OPEN, IntentType.LP_CLOSE),
    supported_chains=SupportedChainsSpec(chains=(ETHEREUM, ARBITRUM, OPTIMISM, POLYGON, BASE)),
    lifecycle_declarations=_production_lifecycle_declarations(),
)

__all__ = ["CONNECTOR"]
