"""Exact-cell authoring for audited AMM core-execution declarations.

This helper expands only caller-supplied cells.  It deliberately performs no
connector discovery and never treats a sibling chain or intent as evidence.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date

from almanak.connectors._connector_descriptor import LifecycleObligationDecl
from almanak.connectors._lifecycle_declaration_bundle import LifecycleClaimCell, LifecycleDeclarationBundle
from almanak.core.capability_obligations import (
    EvidenceKind,
    EvidenceRef,
    ExactTargetFeature,
    IntentSemantics,
    ObligationDeclaration,
    ObligationDisposition,
    ObligationId,
    Satisfied,
    SupportClaim,
    Unsupported,
)
from almanak.core.chains import ChainDescriptor
from almanak.core.intent_types import IntentType
from almanak.framework.capabilities.obligation_profiles import ClaimProfileKey, profile_for, semantics_for_intent
from almanak.framework.primitives.types import Primitive

__all__ = [
    "AmmCoreExecutionCell",
    "build_amm_core_execution_declarations",
    "build_amm_lane_gap_claim_declarations",
]

_CORE_OBLIGATIONS = (
    ObligationId.ASSET_RESOLUTION,
    ObligationId.VENUE_RESOLUTION,
    ObligationId.AMOUNT_PROTECTION,
    ObligationId.COMPILER,
    ObligationId.RECEIPT_EVIDENCE,
    ObligationId.MONEY_LEGS,
    ObligationId.PERMISSION_PLAN,
)
_REVIEW_BY = date(2026, 10, 15)


@dataclass(frozen=True)
class AmmCoreExecutionCell:
    """One explicitly audited AMM cell and its exact evidence disposition."""

    chain: ChainDescriptor
    intent: IntentType
    real_fork_ref: str | None = None
    lane_gap_ref: str | None = None
    obligation_gap_refs: tuple[tuple[ObligationId, str], ...] = ()
    money_leg_evidence_ref: str | None = None

    def __post_init__(self) -> None:
        _validate_cell_scope_and_lane(self)
        gap_obligations = _validate_obligation_gap_refs(self.obligation_gap_refs)
        _validate_evidence_compatibility(self, gap_obligations)
        _validate_optional_evidence_refs(self)


def _validate_cell_scope_and_lane(cell: AmmCoreExecutionCell) -> None:
    if type(cell.chain) is not ChainDescriptor:
        raise TypeError("AmmCoreExecutionCell.chain must be a ChainDescriptor")
    if type(cell.intent) is not IntentType:
        raise TypeError("AmmCoreExecutionCell.intent must be an IntentType")
    if cell.intent not in {
        IntentType.SWAP,
        IntentType.LP_OPEN,
        IntentType.LP_CLOSE,
        IntentType.LP_COLLECT_FEES,
    }:
        raise ValueError("AmmCoreExecutionCell.intent must be an AMM execution intent")
    if (cell.real_fork_ref is None) == (cell.lane_gap_ref is None):
        raise ValueError("exactly one of real_fork_ref or lane_gap_ref is required")


def _validate_obligation_gap_refs(refs: object) -> list[ObligationId]:
    if not isinstance(refs, tuple):
        raise TypeError("obligation_gap_refs must be a tuple")
    gap_obligations: list[ObligationId] = []
    for entry in refs:
        if not isinstance(entry, tuple) or len(entry) != 2:
            raise TypeError("obligation_gap_refs entries must be (ObligationId, tracking_ref) tuples")
        obligation, tracking_ref = entry
        if type(obligation) is not ObligationId or obligation not in _CORE_OBLIGATIONS:
            raise TypeError("obligation_gap_refs must use CORE_EXECUTION ObligationId values")
        if not isinstance(tracking_ref, str) or not tracking_ref.strip():
            raise ValueError("obligation gap tracking refs must be non-empty strings")
        gap_obligations.append(obligation)
    if len(gap_obligations) != len(set(gap_obligations)):
        raise ValueError("obligation_gap_refs contains duplicate obligations")
    return gap_obligations


def _validate_evidence_compatibility(
    cell: AmmCoreExecutionCell,
    gap_obligations: list[ObligationId],
) -> None:
    if ObligationId.MONEY_LEGS in gap_obligations and cell.money_leg_evidence_ref is not None:
        raise ValueError("money-leg evidence and a money-leg obligation gap are mutually exclusive")
    if cell.real_fork_ref is None and (cell.obligation_gap_refs or cell.money_leg_evidence_ref is not None):
        raise ValueError("lane gaps cannot carry positive obligation evidence")


def _validate_optional_evidence_refs(cell: AmmCoreExecutionCell) -> None:
    for field_name in ("real_fork_ref", "lane_gap_ref", "money_leg_evidence_ref"):
        value = getattr(cell, field_name)
        if value is not None and (not isinstance(value, str) or not value.strip()):
            raise ValueError(f"{field_name} must be None or a non-empty string")


def _unsupported(*, reason: str, tracking_ref: str) -> Unsupported:
    return Unsupported(
        reason=reason,
        tracking_ref=tracking_ref,
        owner="SDK Capability Audit",
        review_by=_REVIEW_BY,
    )


def _obligation_gap_reason(obligation: ObligationId) -> str:
    if obligation is ObligationId.AMOUNT_PROTECTION:
        return "The exact intent path lacks a declaration-grade protective amount contract."
    if obligation is ObligationId.PERMISSION_PLAN:
        return "The exact intent path lacks declaration-grade permission coverage across reachable routing variants."
    return f"The exact intent path lacks declaration-grade {obligation.value} evidence."


def _cell_declarations(
    cell: AmmCoreExecutionCell,
    *,
    provider_refs: Mapping[ObligationId, str],
    contract_version: str,
) -> tuple[ObligationDeclaration, ...]:
    declarations = []
    obligation_gap_refs = dict(cell.obligation_gap_refs)
    for obligation in _CORE_OBLIGATIONS:
        disposition: ObligationDisposition
        if cell.real_fork_ref is None:
            disposition = _unsupported(
                reason="The exact production cell lacks a non-xfailed four-layer real-fork execution lane.",
                tracking_ref=cell.lane_gap_ref or "",
            )
        elif obligation in obligation_gap_refs:
            disposition = _unsupported(
                reason=_obligation_gap_reason(obligation),
                tracking_ref=obligation_gap_refs[obligation],
            )
        elif obligation is ObligationId.MONEY_LEGS and cell.money_leg_evidence_ref is None:
            disposition = _unsupported(
                reason="The exact AMM path lacks canonical typed PrimitiveMoneyLegs evidence.",
                tracking_ref="VIB-6662",
            )
        else:
            evidence = [EvidenceRef(EvidenceKind.REAL_FORK, cell.real_fork_ref)]
            if obligation is ObligationId.MONEY_LEGS:
                evidence.append(EvidenceRef(EvidenceKind.CONTRACT_TEST, cell.money_leg_evidence_ref or ""))
            disposition = Satisfied(
                provider_ref=provider_refs[obligation],
                contract_version=contract_version,
                test_evidence=tuple(evidence),
            )
        declarations.append(ObligationDeclaration(obligation, disposition))
    return tuple(declarations)


def build_amm_core_execution_declarations(
    *,
    protocol: str,
    cells: tuple[AmmCoreExecutionCell, ...],
    provider_refs: Mapping[ObligationId, str],
    contract_version: str,
) -> tuple[LifecycleObligationDecl, ...]:
    """Expand explicit audited cells into the connector's canonical flat rules."""

    if set(provider_refs) != set(_CORE_OBLIGATIONS):
        raise ValueError("provider_refs must cover every CORE_EXECUTION obligation exactly")
    if not isinstance(cells, tuple) or not cells:
        raise ValueError("cells must be a non-empty tuple")
    if not all(type(cell) is AmmCoreExecutionCell for cell in cells):
        raise TypeError("cells must contain AmmCoreExecutionCell values")
    identities = [(cell.chain.name, cell.intent.value) for cell in cells]
    if len(identities) != len(set(identities)):
        raise ValueError("cells contains duplicate chain/intent identities")

    expanded: list[LifecycleObligationDecl] = []
    for cell in cells:
        bundle_id = f"{protocol}.{cell.chain.name}.{cell.intent.value.lower()}"
        expanded.extend(
            LifecycleDeclarationBundle(
                bundle_id=bundle_id,
                cells=(LifecycleClaimCell(protocol=protocol, chain=cell.chain, intent=cell.intent),),
                declarations=_cell_declarations(
                    cell,
                    provider_refs=provider_refs,
                    contract_version=contract_version,
                ),
                source_ref=f"Connector.lifecycle_declarations[{bundle_id}]",
                source_detail="Exact-chain, exact-intent evidence; sibling cells are never treated as proof.",
            ).expand()
        )
    return tuple(expanded)


def _lane_gap_claims(
    intent: IntentType,
    *,
    quote_feature: bool,
) -> tuple[tuple[SupportClaim, ExactTargetFeature | None], ...]:
    """The non-core claims the effective matrix raises for an AMM cell.

    Mirrors the matrix's own claim enumeration: position claims follow the
    intent's lifecycle semantics, the quote feature exists only for SWAP on a
    connector that binds a swap-quote provider, and every cell is asked whether
    it is managed-Anvil testable.
    """
    semantics = semantics_for_intent(intent)
    claims: list[tuple[SupportClaim, ExactTargetFeature | None]] = []
    if semantics is IntentSemantics.POSITION_OPEN_OR_INCREASE:
        claims.append((SupportClaim.POSITION_OPEN, None))
    elif semantics is IntentSemantics.POSITION_DECREASE_OR_CLOSE:
        claims.append((SupportClaim.POSITION_CLOSE, None))
    if semantics in (
        IntentSemantics.POSITION_OPEN_OR_INCREASE,
        IntentSemantics.POSITION_DECREASE_OR_CLOSE,
        IntentSemantics.POSITION_MAINTENANCE,
    ):
        claims.extend(((SupportClaim.FULL_LIFECYCLE_CERTIFICATION, None), (SupportClaim.VALUATION_READY, None)))
    if quote_feature and intent is IntentType.SWAP:
        claims.append((SupportClaim.EXACT_TARGET_DATA, ExactTargetFeature.QUOTE))
    claims.append((SupportClaim.MANAGED_ANVIL_TESTABLE, None))
    return tuple(claims)


def build_amm_lane_gap_claim_declarations(
    *,
    protocol: str,
    chain: ChainDescriptor,
    intents: tuple[IntentType, ...],
    tracking_ref: str,
    lp_primitive: Primitive,
    quote_feature: bool,
) -> tuple[LifecycleObligationDecl, ...]:
    """Declare every non-core claim of lane-gap AMM cells as tracked debt.

    Companion to :class:`AmmCoreExecutionCell` cells that carry ``lane_gap_ref``:
    those answer only the core-execution claim, while the matrix also raises the
    position, certification, valuation, quote, and managed-Anvil claims for the
    same cell. A chain whose four-layer lane exists but has not executed cannot
    evidence any of them, so each obligation is answered ``Unsupported`` under
    one tracking reference instead of surfacing as undeclared debt.
    """
    if not isinstance(intents, tuple) or not intents:
        raise ValueError("intents must be a non-empty tuple")
    expanded: list[LifecycleObligationDecl] = []
    for intent in intents:
        primitive = Primitive.SWAP if intent is IntentType.SWAP else lp_primitive
        for claim, feature in _lane_gap_claims(intent, quote_feature=quote_feature):
            profile = profile_for(
                ClaimProfileKey(
                    semantics=semantics_for_intent(intent),
                    primitive=primitive,
                    claim=claim,
                    exact_target_feature=feature,
                )
            )
            declarations = tuple(
                ObligationDeclaration(
                    obligation,
                    _unsupported(
                        reason="The exact production cell lacks a non-xfailed four-layer real-fork execution lane.",
                        tracking_ref=tracking_ref,
                    ),
                )
                for obligation in profile.obligation_ids
            )
            suffix = f".{feature.value}" if feature is not None else ""
            bundle_id = f"{protocol}.{chain.name}.{intent.value.lower()}.{claim.value}{suffix}"
            expanded.extend(
                LifecycleDeclarationBundle(
                    bundle_id=bundle_id,
                    cells=(
                        LifecycleClaimCell(
                            protocol=protocol,
                            chain=chain,
                            intent=intent,
                            claim=claim,
                            exact_target_feature=feature,
                        ),
                    ),
                    declarations=declarations,
                    source_ref=f"Connector.lifecycle_declarations[{bundle_id}]",
                    source_detail="Lane gap: the four-layer lane exists but has not executed for this exact cell.",
                ).expand()
            )
    return tuple(expanded)
