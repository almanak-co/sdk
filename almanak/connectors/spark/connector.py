"""Spark connector manifest."""

from __future__ import annotations

from datetime import date

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    BacktestStrategyTypeDecl,
    Connector,
    ImportRef,
    LendingReadDecl,
    LifecycleObligationDecl,
    MetadataAmountEncoding,
    SupportedChainsSpec,
)
from almanak.connectors._lifecycle_declaration_bundle import LifecycleClaimCell, LifecycleDeclarationBundle
from almanak.connectors._strategy_base.address_table import AddressTableSpec
from almanak.connectors._strategy_base.protocol_ownership import CapabilitiesSpec
from almanak.connectors.spark.backtest_risk import BACKTEST_RISK as _BACKTEST_RISK
from almanak.core.capability_obligations import (
    EvidenceKind,
    EvidenceRef,
    ObligationDeclaration,
    ObligationDisposition,
    ObligationId,
    Satisfied,
    Unsupported,
)
from almanak.core.chains.ethereum import DESCRIPTOR as ETHEREUM
from almanak.core.intent_types import IntentType

_PROVIDER_REFS = {
    ObligationId.ASSET_RESOLUTION: "almanak.connectors.spark.compiler:SparkCompiler",
    ObligationId.VENUE_RESOLUTION: "almanak.connectors.spark.adapter:SparkAdapter",
    ObligationId.AMOUNT_PROTECTION: "almanak.connectors.spark.adapter:SparkAdapter",
    ObligationId.COMPILER: "almanak.connectors.spark.compiler:SparkCompiler",
    ObligationId.RECEIPT_EVIDENCE: "almanak.connectors.spark.receipt_parser:SparkReceiptParser",
    ObligationId.MONEY_LEGS: "almanak.connectors.spark.receipt_parser:SparkReceiptParser.extract_primitive_money_legs",
    ObligationId.PERMISSION_PLAN: "almanak.connectors.spark.permission_hints:PERMISSION_HINTS",
}


def _core_dispositions(*, money_legs_supported: bool) -> tuple[ObligationDeclaration, ...]:
    real_fork = EvidenceRef(EvidenceKind.REAL_FORK, "tests/intents/ethereum/test_spark_lending.py")
    money_leg_contract = EvidenceRef(
        EvidenceKind.CONTRACT_TEST,
        "tests/unit/connectors/test_lending_money_legs_vib5218.py",
    )
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
        if obligation is ObligationId.MONEY_LEGS and not money_legs_supported:
            disposition = Unsupported(
                reason="Spark WITHDRAW/REPAY lack direct typed PrimitiveMoneyLegs and enrichment assertions.",
                tracking_ref="VIB-6661",
                owner="SDK Capability Audit",
                review_by=date(2026, 10, 15),
            )
        else:
            disposition = Satisfied(
                provider_ref=_PROVIDER_REFS[obligation],
                contract_version="spark.core_execution.v1",
                test_evidence=(real_fork, money_leg_contract)
                if obligation is ObligationId.MONEY_LEGS
                else (real_fork,),
            )
        declarations.append(ObligationDeclaration(obligation, disposition))
    return tuple(declarations)


def _production_lifecycle_declarations():
    declarations: list[LifecycleObligationDecl] = []
    for money_legs_supported, intents, suffix in (
        (True, (IntentType.BORROW, IntentType.SUPPLY), "core_execution"),
        (False, (IntentType.REPAY, IntentType.WITHDRAW), "money_leg_gap"),
    ):
        cells = tuple(LifecycleClaimCell(protocol="spark", chain=ETHEREUM, intent=intent) for intent in intents)
        declarations.extend(
            LifecycleDeclarationBundle(
                bundle_id=f"spark.ethereum.{suffix}",
                cells=cells,
                declarations=_core_dispositions(money_legs_supported=money_legs_supported),
                source_ref=f"Connector.lifecycle_declarations[spark.ethereum.{suffix}]",
                source_detail="Exact-chain real-fork evidence with explicit typed money-leg test gaps.",
            ).expand()
        )
    return tuple(declarations)


CONNECTOR = Connector(
    name="spark",
    kind=ProtocolKind.LENDING,
    backtest_strategy_type=BacktestStrategyTypeDecl(strategy_type="lending"),
    address_tables=(
        AddressTableSpec(
            protocol="spark",
            module="almanak.connectors.spark.addresses",
            attribute="SPARK",
        ),
    ),
    agent_read_connector=ImportRef(
        module="almanak.connectors.spark.agent_read_provider",
        attribute="SparkAgentReadConnector",
        order=6,
    ),
    gateway_connector=ImportRef(
        module="almanak.connectors.spark.gateway.provider",
        attribute="SparkGatewayConnector",
        order=29,
    ),
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.spark.receipt_parser_provider",
        attribute="SparkReceiptParserConnector",
    ),
    contract_roles=ImportRef(
        module="almanak.connectors.spark.contract_roles",
        attribute="CONTRACT_ROLES",
        order=10,
    ),
    compiler=ImportRef(
        module="almanak.connectors.spark.compiler",
        attribute="SparkCompiler",
    ),
    capabilities=CapabilitiesSpec(
        keys=("spark",),
        module="almanak.connectors.spark.capabilities",
    ),
    # Aave-fork reads: own opt-in attributes backed by the shared Aave-fork specs.
    # backtest_default_supply_apy / borrow_apy moved from interest.py (plan 022);
    # values verbatim from the pre-rewire hardcoded dict (0.05 / 0.055).
    lending_read=LendingReadDecl(
        # Gateway rate lane: served by SparkGatewayConnector's
        # GatewayLendingRateHistoryCapability via the fork-shared pipeline.
        rate_history_chains=("ethereum",),
        backtest_default_supply_apy="0.05",
        backtest_default_borrow_apy="0.055",
        backtest_provider=ImportRef(
            module="almanak.connectors.spark.backtest_apy",
            attribute="SparkAPYProvider",
        ),
        spec=ImportRef(module="almanak.connectors.spark.lending_read", attribute="LENDING_READ_SPEC"),
        account_state=ImportRef(module="almanak.connectors.spark.lending_read", attribute="ACCOUNT_STATE_READ_SPEC"),
    ),
    # Aave-fork compiler: lending metadata amounts are wei-encoded (VIB-3747).
    metadata_amount_encoding=MetadataAmountEncoding(lending="wei"),
    backtest_risk=_BACKTEST_RISK,
    strategy_intents=(IntentType.SUPPLY, IntentType.BORROW, IntentType.REPAY, IntentType.WITHDRAW),
    supported_chains=SupportedChainsSpec(chains=(ETHEREUM,)),
    lifecycle_declarations=_production_lifecycle_declarations(),
)

__all__ = ["CONNECTOR"]
