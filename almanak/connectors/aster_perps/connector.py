"""Aster Pro connector manifest."""

from __future__ import annotations

from datetime import date

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
    LifecycleObligationDecl,
    SupportedChainsSpec,
)
from almanak.connectors._lifecycle_declaration_bundle import LifecycleClaimCell, LifecycleDeclarationBundle
from almanak.connectors._strategy_base.address_table import AddressTableSpec
from almanak.core.capability_obligations import (
    EvidenceKind,
    EvidenceRef,
    NotApplicable,
    NotApplicableRuleId,
    ObligationDeclaration,
    ObligationId,
    Satisfied,
    SupportClaim,
    Unsupported,
)
from almanak.core.chains.bsc import DESCRIPTOR as BSC
from almanak.core.intent_types import IntentType

_GAPS_DOC = "https://github.com/almanak-co/almanak-sdk-private/blob/main/docs/internal/roadmap/aster-pro.md"
_CONTRACT = "aster_perps.cash_movement.v1"
_REVIEW_BY = date(2027, 1, 5)
_DEPOSIT_FORK_TEST = (EvidenceRef(EvidenceKind.REAL_FORK, "tests/intents/bnb/test_aster_perps_deposit.py"),)
_FUNDING_UNIT_TEST = (
    EvidenceRef(EvidenceKind.CONTRACT_TEST, "tests/unit/connectors/aster_perps/test_aster_pro_funding.py"),
)
_COMPILER = "almanak.connectors.aster_perps.compiler:AsterPerpsCompiler"


def _satisfied(provider_ref: str, evidence: tuple[EvidenceRef, ...]) -> Satisfied:
    return Satisfied(provider_ref=provider_ref, contract_version=_CONTRACT, test_evidence=evidence)


def _gap(reason: str, anchor: str) -> Unsupported:
    return Unsupported(
        reason=reason, tracking_ref=f"{_GAPS_DOC}#{anchor}", owner="SDK Connectors", review_by=_REVIEW_BY
    )


_NO_MONEY_LEGS = _gap(
    "Cash movement is asserted from vault events / venue responses but emits no typed PrimitiveMoneyLegs.",
    "typed-money-legs",
)
_PERMISSION_NA = NotApplicable(NotApplicableRuleId.PERMISSION_PLAN_NOT_REQUIRED)


def _lifecycle_declarations() -> tuple[LifecycleObligationDecl, ...]:
    deposit_core = (
        ObligationDeclaration(ObligationId.ASSET_RESOLUTION, _satisfied(_COMPILER, _DEPOSIT_FORK_TEST)),
        ObligationDeclaration(
            ObligationId.VENUE_RESOLUTION,
            _satisfied("almanak.connectors.aster_perps.addresses:ASTER_PRO", _DEPOSIT_FORK_TEST),
        ),
        ObligationDeclaration(ObligationId.AMOUNT_PROTECTION, _satisfied(_COMPILER, _DEPOSIT_FORK_TEST)),
        ObligationDeclaration(ObligationId.COMPILER, _satisfied(_COMPILER, _DEPOSIT_FORK_TEST)),
        ObligationDeclaration(
            ObligationId.RECEIPT_EVIDENCE,
            _satisfied("almanak.connectors.aster_perps.vault_events:decode_deposits", _DEPOSIT_FORK_TEST),
        ),
        ObligationDeclaration(ObligationId.MONEY_LEGS, _NO_MONEY_LEGS),
        ObligationDeclaration(ObligationId.PERMISSION_PLAN, _PERMISSION_NA),
    )
    deposit_anvil = tuple(
        ObligationDeclaration(obligation, _satisfied(_COMPILER, _DEPOSIT_FORK_TEST))
        for obligation in (
            ObligationId.ANVIL_FUNDING,
            ObligationId.ANVIL_GAS,
            ObligationId.ANVIL_QUOTE,
            ObligationId.ANVIL_FORK_READ,
            ObligationId.ANVIL_LIFECYCLE_EVIDENCE,
        )
    )
    service = "almanak.connectors.aster_perps.gateway.service:AsterPerpsServiceServicer"
    withdraw_core = (
        ObligationDeclaration(ObligationId.ASSET_RESOLUTION, _satisfied(_COMPILER, _FUNDING_UNIT_TEST)),
        ObligationDeclaration(ObligationId.VENUE_RESOLUTION, _satisfied(service, _FUNDING_UNIT_TEST)),
        ObligationDeclaration(ObligationId.AMOUNT_PROTECTION, _satisfied(service, _FUNDING_UNIT_TEST)),
        ObligationDeclaration(ObligationId.COMPILER, _satisfied(_COMPILER, _FUNDING_UNIT_TEST)),
        ObligationDeclaration(
            ObligationId.RECEIPT_EVIDENCE,
            _satisfied("almanak.connectors.aster_perps.execution:AsterOrderHandler", _FUNDING_UNIT_TEST),
        ),
        ObligationDeclaration(ObligationId.MONEY_LEGS, _NO_MONEY_LEGS),
        ObligationDeclaration(ObligationId.PERMISSION_PLAN, _PERMISSION_NA),
    )
    withdraw_anvil = tuple(
        ObligationDeclaration(
            obligation,
            _gap(
                "Off-chain venue withdrawal; no Anvil fork reproduces Aster's ledger.",
                "fork-testability-of-the-off-chain-withdrawal",
            ),
        )
        for obligation in (
            ObligationId.ANVIL_FUNDING,
            ObligationId.ANVIL_GAS,
            ObligationId.ANVIL_QUOTE,
            ObligationId.ANVIL_FORK_READ,
            ObligationId.ANVIL_LIFECYCLE_EVIDENCE,
        )
    )
    bundles = (
        ("deposit.core", IntentType.PERP_DEPOSIT, SupportClaim.CORE_EXECUTION, deposit_core),
        ("deposit.anvil", IntentType.PERP_DEPOSIT, SupportClaim.MANAGED_ANVIL_TESTABLE, deposit_anvil),
        ("withdraw.core", IntentType.PERP_WITHDRAW, SupportClaim.CORE_EXECUTION, withdraw_core),
        ("withdraw.anvil", IntentType.PERP_WITHDRAW, SupportClaim.MANAGED_ANVIL_TESTABLE, withdraw_anvil),
    )
    declarations: list[LifecycleObligationDecl] = []
    for suffix, intent, claim, obligations in bundles:
        declarations.extend(
            LifecycleDeclarationBundle(
                bundle_id=f"aster_perps.bsc.{suffix}",
                cells=(LifecycleClaimCell(protocol="aster_perps", chain=BSC, intent=intent, claim=claim),),
                declarations=obligations,
                source_ref=f"Connector.lifecycle_declarations[aster_perps.bsc.{suffix}]",
                source_detail="Aster Pro cash movement: vault deposit on-chain, withdrawal through the gateway.",
            ).expand()
        )
    return tuple(declarations)


CONNECTOR = Connector(
    name="aster_perps",
    kind=ProtocolKind.PERP,
    address_tables=(
        AddressTableSpec(
            protocol="aster_perps",
            module="almanak.connectors.aster_perps.addresses",
            attribute="ASTER_PRO",
        ),
    ),
    gateway_connector=ImportRef(
        module="almanak.connectors.aster_perps.gateway.provider",
        attribute="AsterPerpsGatewayConnector",
        order=31,
    ),
    gateway_settings=ImportRef(
        module="almanak.connectors.aster_perps.gateway.settings",
        attribute="AsterPerpsGatewaySettings",
        order=31,
    ),
    gateway_stub=ImportRef(
        module="almanak.connectors.aster_perps.gateway_stub",
        attribute="GATEWAY_STUB_SPEC",
    ),
    compiler=ImportRef(
        module="almanak.connectors.aster_perps.compiler",
        attribute="AsterPerpsCompiler",
    ),
    # The off-chain order lane: bundles with an ``order_request`` and no
    # transactions are executed by this handler instead of the on-chain orchestrator.
    prediction_execute=ImportRef(
        module="almanak.connectors.aster_perps.execution",
        attribute="EXECUTE_SPEC",
    ),
    venue_account_read=ImportRef(
        module="almanak.connectors.aster_perps.venue_account_read",
        attribute="VENUE_ACCOUNT_READ_SPEC",
    ),
    runner_hook_connector=ImportRef(
        module="almanak.connectors.aster_perps.runner_hooks",
        attribute="AsterPerpsRunnerHookConnector",
    ),
    teardown_post_condition=ImportRef(
        module="almanak.connectors.aster_perps.teardown_post_condition",
        attribute="aster_perps_teardown_post_condition",
    ),
    # Aster's agent approval accepts only EOA signatures (a Safe account is
    # rejected even when deployed on BSC), so a Safe-held account cannot trade.
    safe_supported=False,
    lifecycle_declarations=_lifecycle_declarations(),
    strategy_intents=(IntentType.PERP_OPEN, IntentType.PERP_CLOSE, IntentType.PERP_DEPOSIT, IntentType.PERP_WITHDRAW),
    supported_chains=SupportedChainsSpec(chains=(BSC,)),
)

__all__ = ["CONNECTOR"]
