"""Compound V3 connector manifest."""

from __future__ import annotations

from datetime import date

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    BacktestStrategyTypeDecl,
    Connector,
    FeeModelDecl,
    ImportRef,
    LendingReadDecl,
    LifecycleObligationDecl,
    SupportedChainsSpec,
    YieldPokeDecl,
)
from almanak.connectors._lifecycle_declaration_bundle import LifecycleClaimCell, LifecycleDeclarationBundle
from almanak.connectors._strategy_base.address_table import AddressTableSpec
from almanak.connectors._strategy_base.protocol_ownership import CapabilitiesSpec
from almanak.connectors.compound_v3.backtest_risk import BACKTEST_RISK as _BACKTEST_RISK
from almanak.core.capability_obligations import (
    EvidenceKind,
    EvidenceRef,
    ObligationDeclaration,
    ObligationId,
    Satisfied,
    Unsupported,
)
from almanak.core.chains.arbitrum import DESCRIPTOR as ARBITRUM
from almanak.core.chains.base import DESCRIPTOR as BASE
from almanak.core.chains.ethereum import DESCRIPTOR as ETHEREUM
from almanak.core.chains.optimism import DESCRIPTOR as OPTIMISM
from almanak.core.chains.polygon import DESCRIPTOR as POLYGON
from almanak.core.intent_types import IntentType

_PRODUCTION_CHAINS = (ARBITRUM, BASE, ETHEREUM, OPTIMISM, POLYGON)
_PRODUCTION_INTENTS = tuple(
    sorted(
        (IntentType.SUPPLY, IntentType.WITHDRAW, IntentType.BORROW, IntentType.REPAY),
        key=lambda intent: intent.value,
    )
)
_PROVIDER_REFS = {
    ObligationId.ASSET_RESOLUTION: "almanak.connectors.compound_v3.compiler:CompoundV3Compiler",
    ObligationId.VENUE_RESOLUTION: "almanak.connectors.compound_v3.adapter:CompoundV3Adapter",
    ObligationId.AMOUNT_PROTECTION: "almanak.connectors.compound_v3.adapter:CompoundV3Adapter",
    ObligationId.COMPILER: "almanak.connectors.compound_v3.compiler:CompoundV3Compiler",
    ObligationId.RECEIPT_EVIDENCE: "almanak.connectors.compound_v3.receipt_parser:CompoundV3ReceiptParser",
    ObligationId.PERMISSION_PLAN: "almanak.connectors.compound_v3.permission_hints:PERMISSION_HINTS",
}
_POLYGON_WIND_DOWN = "https://github.com/almanak-co/almanak-sdk-private/issues/3400"


def _satisfied_dispositions(evidence: tuple[EvidenceRef, ...]) -> tuple[ObligationDeclaration, ...]:
    satisfied = tuple(
        ObligationDeclaration(
            obligation,
            Satisfied(
                provider_ref=_PROVIDER_REFS[obligation],
                contract_version="compound_v3.core_execution.v1",
                test_evidence=evidence,
            ),
        )
        for obligation in (
            ObligationId.ASSET_RESOLUTION,
            ObligationId.VENUE_RESOLUTION,
            ObligationId.AMOUNT_PROTECTION,
            ObligationId.COMPILER,
            ObligationId.RECEIPT_EVIDENCE,
        )
    )
    return satisfied + (
        ObligationDeclaration(
            ObligationId.MONEY_LEGS,
            Unsupported(
                reason="Compound V3 does not emit canonical typed PrimitiveMoneyLegs for lending receipts.",
                tracking_ref="VIB-6660",
                owner="SDK Accounting",
                review_by=date(2026, 10, 15),
            ),
        ),
        ObligationDeclaration(
            ObligationId.PERMISSION_PLAN,
            Satisfied(
                provider_ref=_PROVIDER_REFS[ObligationId.PERMISSION_PLAN],
                contract_version="compound_v3.core_execution.v1",
                test_evidence=evidence,
            ),
        ),
    )


def _polygon_gap_dispositions(intent: IntentType) -> tuple[ObligationDeclaration, ...]:
    dispositions = []
    for obligation in ObligationId:
        if obligation not in {
            ObligationId.ASSET_RESOLUTION,
            ObligationId.VENUE_RESOLUTION,
            ObligationId.AMOUNT_PROTECTION,
            ObligationId.COMPILER,
            ObligationId.RECEIPT_EVIDENCE,
            ObligationId.MONEY_LEGS,
            ObligationId.PERMISSION_PLAN,
        }:
            continue
        if obligation is ObligationId.MONEY_LEGS:
            tracking_ref = "VIB-6660"
            reason = "Compound V3 does not emit canonical typed PrimitiveMoneyLegs for lending receipts."
            owner = "SDK Accounting"
        elif intent is IntentType.BORROW:
            tracking_ref = _POLYGON_WIND_DOWN
            reason = (
                "Compound governance wound down every Polygon collateral market; BORROW lacks a live executable path."
            )
            owner = "Connector - Compound V3"
        else:
            tracking_ref = "VIB-6661"
            reason = (
                "Polygon REPAY remains operational for existing debt, but the current real-fork proof fails in its "
                "new-borrow setup before exercising the exact repay path."
            )
            owner = "SDK Capability Audit"
        dispositions.append(
            ObligationDeclaration(
                obligation,
                Unsupported(
                    reason=reason,
                    tracking_ref=tracking_ref,
                    owner=owner,
                    review_by=date(2026, 10, 15),
                ),
            )
        )
    return tuple(dispositions)


def _production_lifecycle_declarations():
    declarations: list[LifecycleObligationDecl] = []
    for chain in _PRODUCTION_CHAINS:
        evidence = (EvidenceRef(EvidenceKind.REAL_FORK, f"tests/intents/{chain.name}/test_compound_v3_lending.py"),)
        working_intents = (IntentType.SUPPLY, IntentType.WITHDRAW) if chain is POLYGON else _PRODUCTION_INTENTS
        working_cells = tuple(
            sorted(
                (LifecycleClaimCell(protocol="compound_v3", chain=chain, intent=intent) for intent in working_intents),
                key=LifecycleClaimCell.sort_key,
            )
        )
        declarations.extend(
            LifecycleDeclarationBundle(
                bundle_id=f"compound_v3.{chain.name}.core_execution",
                cells=working_cells,
                declarations=_satisfied_dispositions(evidence),
                source_ref=f"Connector.lifecycle_declarations[compound_v3.{chain.name}.core_execution]",
                source_detail="Exact-chain four-layer lending intent evidence; typed money-leg gap remains owned.",
            ).expand()
        )
    for intent, suffix, source_detail in (
        (
            IntentType.BORROW,
            "wound_down_borrow",
            "Open #3400 records the chain-wide collateral-market wind-down and xfailed BORROW path.",
        ),
        (
            IntentType.REPAY,
            "repay_evidence_gap",
            "VIB-6661 owns an exact existing-debt REPAY proof independent from the wound-down setup path.",
        ),
    ):
        declarations.extend(
            LifecycleDeclarationBundle(
                bundle_id=f"compound_v3.polygon.{suffix}",
                cells=(LifecycleClaimCell(protocol="compound_v3", chain=POLYGON, intent=intent),),
                declarations=_polygon_gap_dispositions(intent),
                source_ref=f"Connector.lifecycle_declarations[compound_v3.polygon.{suffix}]",
                source_detail=source_detail,
            ).expand()
        )
    return tuple(declarations)


CONNECTOR = Connector(
    name="compound_v3",
    kind=ProtocolKind.LENDING,
    external_ids={"defillama": "compound-v3"},
    fee_model=FeeModelDecl(
        model=ImportRef(module="almanak.connectors.compound_v3.fee_model", attribute="CompoundV3FeeModel"),
        description="Compound V3 (Comet) lending protocol fee model",
        aliases=("compound", "comet"),
    ),
    backtest_strategy_type=BacktestStrategyTypeDecl(strategy_type="lending", aliases=("compound",)),
    address_tables=(
        AddressTableSpec(
            protocol="compound_v3",
            module="almanak.connectors.compound_v3.addresses",
            attribute="COMPOUND_V3_COMET_ADDRESSES",
        ),
    ),
    gateway_connector=ImportRef(
        module="almanak.connectors.compound_v3.gateway.provider",
        attribute="CompoundV3GatewayConnector",
        order=3,
    ),
    agent_read_connector=ImportRef(
        module="almanak.connectors.compound_v3.agent_read_provider",
        attribute="CompoundV3AgentReadConnector",
        order=6,
    ),
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.compound_v3.receipt_parser_provider",
        attribute="CompoundV3ReceiptParserConnector",
    ),
    compiler=ImportRef(
        module="almanak.connectors.compound_v3.compiler",
        attribute="CompoundV3Compiler",
    ),
    capabilities=CapabilitiesSpec(
        keys=("compound_v3",),
        module="almanak.connectors.compound_v3.capabilities",
    ),
    primitive=ImportRef(
        module="almanak.connectors.compound_v3.primitive",
        attribute="PRIMITIVE",
    ),
    # Market-scoped Comet reads (VIB-4929 PR-3b) + summed multi-collateral health (VIB-4851 PR-2).
    lending_read=LendingReadDecl(
        rate_history_chains=("ethereum", "arbitrum", "optimism", "polygon", "base"),
        backtest_default_supply_apy="0.025",
        backtest_default_borrow_apy="0.045",
        backtest_provider=ImportRef(
            module="almanak.connectors.compound_v3.backtest_apy",
            attribute="CompoundV3APYProvider",
        ),
        account_state=ImportRef(
            module="almanak.connectors.compound_v3.lending_read", attribute="ACCOUNT_STATE_READ_SPEC"
        ),
        market_table=ImportRef(
            module="almanak.connectors.compound_v3.addresses", attribute="COMPOUND_V3_ACCOUNT_STATE_MARKETS"
        ),
        market_health=ImportRef(
            module="almanak.connectors.compound_v3.lending_read", attribute="read_compound_v3_market_health"
        ),
        aliases=("comet", "compound", "compoundv3"),
    ),
    yield_poke=YieldPokeDecl(
        chains=("arbitrum",),
        poke=ImportRef(module="almanak.connectors.compound_v3.backtest_poke", attribute="poke_compound_v3"),
    ),
    backtest_risk=_BACKTEST_RISK,
    strategy_intents=(IntentType.SUPPLY, IntentType.BORROW, IntentType.REPAY, IntentType.WITHDRAW),
    supported_chains=SupportedChainsSpec(chains=(ETHEREUM, ARBITRUM, BASE, OPTIMISM, POLYGON)),
    lifecycle_declarations=_production_lifecycle_declarations(),
)

__all__ = ["CONNECTOR"]
