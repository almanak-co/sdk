"""GMX V2 connector manifest."""

from __future__ import annotations

from datetime import date

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    BacktestStrategyTypeDecl,
    Connector,
    FeeModelDecl,
    FundingHistoryDecl,
    ImportRef,
    LifecycleObligationDecl,
    PerpPriceHistoryDecl,
    PerpsReadDecl,
    SupportedChainsSpec,
)
from almanak.connectors._lifecycle_declaration_bundle import LifecycleClaimCell, LifecycleDeclarationBundle
from almanak.connectors._strategy_base.address_table import AddressTableSpec
from almanak.connectors._strategy_base.protocol_ownership import CapabilitiesSpec
from almanak.connectors.gmx_v2.backtest_risk import BACKTEST_RISK as _BACKTEST_RISK
from almanak.core.capability_obligations import (
    EvidenceKind,
    EvidenceRef,
    ObligationDeclaration,
    ObligationDisposition,
    ObligationId,
    Satisfied,
    Unsupported,
)
from almanak.core.chains.arbitrum import DESCRIPTOR as ARBITRUM
from almanak.core.chains.avalanche import DESCRIPTOR as AVALANCHE
from almanak.core.intent_types import IntentType

_PROVIDER_REFS = {
    ObligationId.ASSET_RESOLUTION: "almanak.connectors.gmx_v2.compiler:GMXV2Compiler",
    ObligationId.VENUE_RESOLUTION: "almanak.connectors.gmx_v2.compiler:GMXV2Compiler",
    ObligationId.AMOUNT_PROTECTION: "almanak.connectors.gmx_v2.acceptable_price:derive_acceptable_price_30dec",
    ObligationId.COMPILER: "almanak.connectors.gmx_v2.compiler:GMXV2Compiler",
    ObligationId.RECEIPT_EVIDENCE: "almanak.connectors.gmx_v2.receipt_parser:GMXv2ReceiptParser",
    ObligationId.MONEY_LEGS: "almanak.connectors.gmx_v2.receipt_parser:PerpFillData",
    ObligationId.PERMISSION_PLAN: "almanak.connectors.gmx_v2.permission_hints:PERMISSION_HINTS",
}


def _intent_evidence(chain: str, intent: IntentType) -> tuple[EvidenceRef, ...]:
    suffix = {
        IntentType.PERP_OPEN: "open",
        IntentType.PERP_CLOSE: "close",
        IntentType.PERP_CANCEL_ORDER: "cancel",
    }[intent]
    refs = [
        EvidenceRef(
            EvidenceKind.REAL_FORK,
            f"tests/intents/{chain}/test_gmx_v2_perp_{suffix}.py",
        )
    ]
    if intent is IntentType.PERP_OPEN:
        refs.append(
            EvidenceRef(
                EvidenceKind.REAL_FORK,
                f"tests/intents/{chain}/test_gmx_v2_perp_close.py",
            )
        )
    return tuple(refs)


def _core_dispositions(*, chain: str, intent: IntentType) -> tuple[ObligationDeclaration, ...]:
    exact_evidence = _intent_evidence(chain, intent)
    declarations = []
    for obligation in (
        ObligationId.ASSET_RESOLUTION,
        ObligationId.VENUE_RESOLUTION,
        ObligationId.AMOUNT_PROTECTION,
        ObligationId.COMPILER,
        ObligationId.RECEIPT_EVIDENCE,
        ObligationId.MONEY_LEGS,
        ObligationId.PERMISSION_PLAN,
    ):
        disposition: ObligationDisposition
        if obligation is ObligationId.MONEY_LEGS:
            disposition = Unsupported(
                reason="GMX terminal settlement lacks canonical typed money-leg evidence.",
                tracking_ref="VIB-6664",
                owner="SDK Capability Audit",
                review_by=date(2026, 10, 15),
            )
        elif obligation is ObligationId.ASSET_RESOLUTION and intent is IntentType.PERP_CANCEL_ORDER:
            disposition = Unsupported(
                reason="Order cancellation consumes only an order key and performs no asset resolution.",
                tracking_ref="VIB-6663",
                owner="SDK Capability Audit",
                review_by=date(2026, 10, 15),
            )
        elif obligation is ObligationId.AMOUNT_PROTECTION and intent is IntentType.PERP_CANCEL_ORDER:
            disposition = Unsupported(
                reason="Profile v1 has no typed amount-protection applicability rule for order cancellation.",
                tracking_ref="VIB-6663",
                owner="SDK Capability Audit",
                review_by=date(2026, 10, 15),
            )
        elif obligation is ObligationId.RECEIPT_EVIDENCE and intent is IntentType.PERP_OPEN:
            disposition = Unsupported(
                reason="Submission parsing is proven, but terminal keeper receipt correlation is not exact.",
                tracking_ref="VIB-6152",
                owner="SDK Capability Audit",
                review_by=date(2026, 10, 15),
            )
        else:
            evidence = exact_evidence
            if obligation is ObligationId.AMOUNT_PROTECTION:
                evidence += (
                    EvidenceRef(
                        EvidenceKind.CONTRACT_TEST,
                        "tests/unit/connectors/gmx_v2/test_gmx_v2_acceptable_price_vib6219.py",
                    ),
                )
            elif obligation is ObligationId.PERMISSION_PLAN:
                evidence += (
                    EvidenceRef(
                        EvidenceKind.CONTRACT_TEST,
                        "tests/unit/permissions/test_gmx_v2_manifest.py",
                    ),
                )
            disposition = Satisfied(
                provider_ref=_PROVIDER_REFS[obligation],
                contract_version="gmx_v2.async_core_execution.v1",
                test_evidence=evidence,
            )
        declarations.append(ObligationDeclaration(obligation, disposition))
    return tuple(declarations)


def _production_lifecycle_declarations():
    declarations: list[LifecycleObligationDecl] = []
    for chain in (ARBITRUM, AVALANCHE):
        for intent in (IntentType.PERP_OPEN, IntentType.PERP_CLOSE, IntentType.PERP_CANCEL_ORDER):
            suffix = intent.value.lower()
            bundle_id = f"gmx_v2.{chain.name}.{suffix}"
            declarations.extend(
                LifecycleDeclarationBundle(
                    bundle_id=bundle_id,
                    cells=(LifecycleClaimCell(protocol="gmx_v2", chain=chain, intent=intent),),
                    declarations=_core_dispositions(chain=chain.name, intent=intent),
                    source_ref=f"Connector.lifecycle_declarations[{bundle_id}]",
                    source_detail=(
                        "Exact submission identity plus terminal settlement/refund evidence; "
                        "submission alone is never treated as fulfillment."
                    ),
                ).expand()
            )
    return tuple(declarations)


CONNECTOR = Connector(
    name="gmx_v2",
    kind=ProtocolKind.PERP,
    fee_model=FeeModelDecl(
        model=ImportRef(module="almanak.connectors.gmx_v2.fee_model", attribute="GMXFeeModel"),
        name="gmx",
        description="GMX V2 perpetuals protocol fee model",
        aliases=("gmx_v2",),
    ),
    backtest_strategy_type=BacktestStrategyTypeDecl(strategy_type="perp", aliases=("gmx",)),
    address_tables=(
        AddressTableSpec(
            protocol="gmx_v2",
            module="almanak.connectors.gmx_v2.addresses",
            attribute="GMX_V2",
        ),
        AddressTableSpec(
            protocol="gmx_v2_tokens",
            module="almanak.connectors.gmx_v2.addresses",
            attribute="GMX_V2_TOKENS",
        ),
    ),
    gateway_connector=ImportRef(
        module="almanak.connectors.gmx_v2.gateway.provider",
        attribute="GmxV2GatewayConnector",
        order=14,
    ),
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.gmx_v2.receipt_parser_provider",
        attribute="GmxV2ReceiptParserConnector",
    ),
    runner_hook_connector=ImportRef(
        module="almanak.connectors.gmx_v2.runner_hooks",
        attribute="GmxV2RunnerHookConnector",
    ),
    contract_monitoring=ImportRef(
        module="almanak.connectors.gmx_v2.contract_monitoring",
        attribute="GMX_V2_CONTRACT_MONITORING_SPECS",
    ),
    compiler=ImportRef(
        module="almanak.connectors.gmx_v2.compiler",
        attribute="GMXV2Compiler",
    ),
    protocol_family=ImportRef(
        module="almanak.connectors.gmx_v2.protocol_family",
        attribute="PROTOCOL_FAMILY",
    ),
    # VIB-5116: on-chain closure verify (open positions + pending OrderVault
    # orders) and residual discovery of pending unfilled orders that hold
    # collateral but are not yet positions.
    teardown_post_condition=ImportRef(
        module="almanak.connectors.gmx_v2.teardown_post_condition",
        attribute="gmx_v2_teardown_post_condition",
    ),
    teardown_residual_discovery=ImportRef(
        module="almanak.connectors.gmx_v2.teardown_residual_discovery",
        attribute="gmx_v2_teardown_residual_discovery",
    ),
    # VIB-6287: alias tokens naming a GMX position, so the teardown union stops
    # enumerating one physical position as two when its producers disagree about
    # whether ``details["market"]`` holds a symbol or an address.
    perp_identity=ImportRef(
        module="almanak.connectors.gmx_v2.perp_identity",
        attribute="gmx_v2_perp_identity",
    ),
    capabilities=CapabilitiesSpec(
        keys=("gmx_v2",),
        module="almanak.connectors.gmx_v2.capabilities",
    ),
    primitive=ImportRef(
        module="almanak.connectors.gmx_v2.primitive",
        attribute="PRIMITIVE",
    ),
    perps_read=PerpsReadDecl(
        spec=ImportRef(module="almanak.connectors.gmx_v2.perps_read", attribute="PERPS_READ_SPEC"),
        aliases=("gmx",),
    ),
    funding_history=FundingHistoryDecl(
        venue="gmx_v2",
        chains=("arbitrum", "avalanche"),
        aliases=("gmx",),
        dynamic_markets=True,
        backtest_provider=ImportRef(
            module="almanak.connectors.gmx_v2.backtest_funding",
            attribute="GMXFundingProvider",
        ),
    ),
    perp_price_history=PerpPriceHistoryDecl(
        venue="gmx_v2",
        chains=(ARBITRUM, AVALANCHE),
        aliases=("gmx",),
        backtest_provider=ImportRef(
            module="almanak.connectors.gmx_v2.backtest_prices",
            attribute="GMXOracleDataProvider",
        ),
    ),
    backtest_risk=_BACKTEST_RISK,
    # VIB-5568 introduced cancellation for teardown recovery. ALM-3101 promotes
    # the same fail-closed verb to the public authoring surface: live strategies
    # must be able to replace stale pending orders without pretending that a
    # cancellation is a position close.
    strategy_intents=(IntentType.PERP_OPEN, IntentType.PERP_CLOSE, IntentType.PERP_CANCEL_ORDER),
    supported_chains=SupportedChainsSpec(chains=(ARBITRUM, AVALANCHE)),
    lifecycle_declarations=_production_lifecycle_declarations(),
)

__all__ = ["CONNECTOR"]
