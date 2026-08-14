"""Contract tests for canonical lifecycle obligation profiles."""

from dataclasses import FrozenInstanceError
from datetime import date, datetime

import pytest

from almanak.core.capability_obligations import (
    EvidenceKind,
    EvidenceRef,
    ExactTargetFeature,
    IntentSemantics,
    NotApplicable,
    NotApplicableRuleId,
    ObligationDeclaration,
    ObligationId,
    ObligationState,
    Satisfied,
    SupportClaim,
    Unsupported,
)
from almanak.core.intent_types import IntentType
from almanak.framework.capabilities.obligation_profiles import (
    AuditedObligation,
    ClaimProfileKey,
    InvalidNotApplicableRule,
    ObligationAudit,
    ObligationProfile,
    ProfileObligation,
    ReportedObligationState,
    audit_profile,
    profile_for,
    semantics_for_intent,
)
from almanak.framework.primitives.types import Primitive


def test_profile_obligation_is_exported_with_the_public_profile_contract() -> None:
    from almanak.framework import capabilities

    assert capabilities.ProfileObligation is ProfileObligation


def _key(claim: SupportClaim) -> ClaimProfileKey:
    semantics = IntentSemantics.ATOMIC_EXECUTION
    primitive = Primitive.SWAP
    feature = None
    if claim in (SupportClaim.POSITION_OPEN, SupportClaim.FULL_LIFECYCLE_CERTIFICATION):
        semantics = IntentSemantics.POSITION_OPEN_OR_INCREASE
        primitive = Primitive.LP
    elif claim in (SupportClaim.POSITION_CLOSE, SupportClaim.VALUATION_READY):
        semantics = IntentSemantics.POSITION_DECREASE_OR_CLOSE
        primitive = Primitive.LP
    elif claim is SupportClaim.EXACT_TARGET_DATA:
        feature = ExactTargetFeature.QUOTE
    return ClaimProfileKey(
        semantics=semantics,
        primitive=primitive,
        claim=claim,
        exact_target_feature=feature,
    )


def _satisfied() -> Satisfied:
    return Satisfied(
        provider_ref="almanak.example:Provider",
        contract_version="1",
        test_evidence=(EvidenceRef(EvidenceKind.CONTRACT_TEST, "tests/example.py::test_contract"),),
    )


def _declarations_for(claim: SupportClaim) -> tuple[ObligationDeclaration, ...]:
    profile = profile_for(_key(claim))
    return tuple(ObligationDeclaration(row.obligation, _satisfied()) for row in profile.obligations)


def test_support_claim_and_obligation_state_wire_values_are_stable() -> None:
    assert [claim.value for claim in SupportClaim] == [
        "core_execution",
        "position_open",
        "position_close",
        "full_lifecycle_certification",
        "valuation_ready",
        "exact_target_data",
        "managed_anvil_testable",
    ]
    assert [state.value for state in ObligationState] == ["satisfied", "not_applicable", "unsupported"]
    assert "undeclared" not in ObligationState
    assert ReportedObligationState.UNDECLARED.value == "undeclared"


def test_obligation_identifier_wire_values_are_stable() -> None:
    assert [item.value for item in ObligationId] == [
        "asset_resolution",
        "venue_resolution",
        "amount_protection",
        "compiler",
        "receipt_evidence",
        "money_legs",
        "permission_plan",
        "open_execution",
        "close_execution",
        "position_identity",
        "registry_transition",
        "inventory_adapter",
        "position_reader",
        "position_observer",
        "close_planner",
        "certification_path",
        "position_enumeration",
        "block_anchored_observation",
        "accounting_durability",
        "restart_recovery",
        "canonical_asset_order",
        "valuation_policy",
        "exact_venue_binding",
        "target_data_provider",
        "anvil_funding",
        "anvil_gas",
        "anvil_quote",
        "anvil_fork_read",
        "anvil_lifecycle_evidence",
    ]


def test_every_intent_type_has_explicit_semantics_or_explicit_exclusion() -> None:
    assert {intent: semantics_for_intent(intent) for intent in IntentType}.keys() == set(IntentType)
    assert semantics_for_intent(IntentType.PREDICTION_SELL) is IntentSemantics.POSITION_DECREASE_OR_CLOSE
    assert semantics_for_intent(IntentType.PERP_CANCEL_ORDER) is IntentSemantics.NON_POSITION_RECOVERY
    assert semantics_for_intent(IntentType.OPEN_CDP) is IntentSemantics.UNAVAILABLE
    with pytest.raises(TypeError, match="IntentType"):
        semantics_for_intent("SWAP")  # type: ignore[arg-type]


@pytest.mark.parametrize("claim", list(SupportClaim))
def test_every_core_claim_has_a_non_empty_deterministic_profile(claim: SupportClaim) -> None:
    first = profile_for(_key(claim))
    second = profile_for(_key(claim))
    assert first == second
    assert first.obligation_ids
    assert len(first.obligation_ids) == len(set(first.obligation_ids))
    assert tuple(sorted(first.obligation_ids, key=list(ObligationId).index)) == first.obligation_ids


def test_profile_composition_pins_claim_boundaries() -> None:
    execution = set(profile_for(_key(SupportClaim.CORE_EXECUTION)).obligation_ids)
    position_open = set(profile_for(_key(SupportClaim.POSITION_OPEN)).obligation_ids)
    position_close = set(profile_for(_key(SupportClaim.POSITION_CLOSE)).obligation_ids)
    full = set(profile_for(_key(SupportClaim.FULL_LIFECYCLE_CERTIFICATION)).obligation_ids)

    assert execution < position_open
    assert execution < position_close
    assert position_open | position_close < full
    assert {
        ObligationId.POSITION_ENUMERATION,
        ObligationId.BLOCK_ANCHORED_OBSERVATION,
        ObligationId.ACCOUNTING_DURABILITY,
        ObligationId.RESTART_RECOVERY,
    } <= full
    assert set(profile_for(_key(SupportClaim.VALUATION_READY)).obligation_ids) == {
        ObligationId.POSITION_READER,
        ObligationId.CANONICAL_ASSET_ORDER,
        ObligationId.VALUATION_POLICY,
    }
    assert set(profile_for(_key(SupportClaim.EXACT_TARGET_DATA)).obligation_ids) == {
        ObligationId.ASSET_RESOLUTION,
        ObligationId.EXACT_VENUE_BINDING,
        ObligationId.TARGET_DATA_PROVIDER,
    }
    assert set(profile_for(_key(SupportClaim.MANAGED_ANVIL_TESTABLE)).obligation_ids) == {
        ObligationId.ANVIL_FUNDING,
        ObligationId.ANVIL_GAS,
        ObligationId.ANVIL_QUOTE,
        ObligationId.ANVIL_FORK_READ,
        ObligationId.ANVIL_LIFECYCLE_EVIDENCE,
    }
    assert position_open - execution == {
        ObligationId.OPEN_EXECUTION,
        ObligationId.POSITION_IDENTITY,
        ObligationId.REGISTRY_TRANSITION,
        ObligationId.INVENTORY_ADAPTER,
        ObligationId.POSITION_READER,
        ObligationId.POSITION_OBSERVER,
        ObligationId.CLOSE_PLANNER,
        ObligationId.CERTIFICATION_PATH,
    }
    assert position_close - execution == {
        ObligationId.CLOSE_EXECUTION,
        ObligationId.POSITION_IDENTITY,
        ObligationId.INVENTORY_ADAPTER,
        ObligationId.POSITION_READER,
        ObligationId.POSITION_OBSERVER,
        ObligationId.CLOSE_PLANNER,
        ObligationId.ACCOUNTING_DURABILITY,
    }


def test_audit_rejects_noncanonical_profiles_that_weaken_a_claim() -> None:
    key = _key(SupportClaim.FULL_LIFECYCLE_CERTIFICATION)
    weakened = ObligationProfile(key, (ProfileObligation(ObligationId.COMPILER),))
    with pytest.raises(ValueError, match="canonical core profile"):
        audit_profile(weakened, (ObligationDeclaration(ObligationId.COMPILER, _satisfied()),))


def test_audit_result_rejects_missing_or_misordered_rows() -> None:
    profile = profile_for(_key(SupportClaim.CORE_EXECUTION))
    with pytest.raises(ValueError, match="exactly cover"):
        ObligationAudit(profile, (), ())
    reversed_rows = tuple(AuditedObligation(item, _satisfied()) for item in reversed(profile.obligation_ids))
    with pytest.raises(ValueError, match="canonical order"):
        ObligationAudit(profile, reversed_rows, ())

    weakened = ObligationProfile(
        _key(SupportClaim.FULL_LIFECYCLE_CERTIFICATION),
        (ProfileObligation(ObligationId.COMPILER),),
    )
    with pytest.raises(ValueError, match="canonical core profile"):
        ObligationAudit(weakened, (AuditedObligation(ObligationId.COMPILER, _satisfied()),), ())


def test_audit_result_constructor_cannot_bypass_not_applicable_policy() -> None:
    profile = profile_for(_key(SupportClaim.CORE_EXECUTION))
    rows = tuple(
        AuditedObligation(
            item,
            NotApplicable(NotApplicableRuleId.PERMISSION_PLAN_NOT_REQUIRED)
            if item is ObligationId.COMPILER
            else _satisfied(),
        )
        for item in profile.obligation_ids
    )
    with pytest.raises(InvalidNotApplicableRule, match="cannot justify compiler"):
        ObligationAudit(profile, rows, ())


def test_each_omitted_required_obligation_is_reported_as_migration_only_undeclared() -> None:
    claim = SupportClaim.FULL_LIFECYCLE_CERTIFICATION
    profile = profile_for(_key(claim))
    declarations = _declarations_for(claim)
    for omitted in profile.obligation_ids:
        audit = audit_profile(profile, tuple(item for item in declarations if item.obligation is not omitted))
        assert audit.missing == (omitted,)
        assert not audit.declaration_complete
        row = next(item for item in audit.to_migration_dict()["obligations"] if item["obligation"] == omitted.value)
        assert row == {"obligation": omitted.value, "state": "undeclared"}


def test_unexpected_obligations_are_reported_separately() -> None:
    profile = profile_for(_key(SupportClaim.VALUATION_READY))
    declarations = _declarations_for(SupportClaim.VALUATION_READY) + (
        ObligationDeclaration(ObligationId.COMPILER, _satisfied()),
    )
    audit = audit_profile(profile, declarations)
    assert audit.missing == ()
    assert audit.unexpected == (ObligationId.COMPILER,)
    assert not audit.declaration_complete


def test_duplicate_declarations_are_rejected() -> None:
    profile = profile_for(_key(SupportClaim.CORE_EXECUTION))
    declaration = ObligationDeclaration(profile.obligation_ids[0], _satisfied())
    with pytest.raises(ValueError, match="duplicate declaration"):
        audit_profile(profile, (declaration, declaration))


def test_not_applicable_requires_a_typed_rule_allowed_for_the_exact_obligation() -> None:
    with pytest.raises(TypeError, match="NotApplicableRuleId"):
        NotApplicable("permission_plan_not_required")  # type: ignore[arg-type]

    profile = profile_for(_key(SupportClaim.CORE_EXECUTION))
    allowed = ObligationDeclaration(
        ObligationId.PERMISSION_PLAN,
        NotApplicable(NotApplicableRuleId.PERMISSION_PLAN_NOT_REQUIRED),
    )
    other = tuple(
        ObligationDeclaration(item, _satisfied())
        for item in profile.obligation_ids
        if item is not ObligationId.PERMISSION_PLAN
    )
    audit = audit_profile(profile, other + (allowed,))
    assert audit.claim_satisfied

    invalid = ObligationDeclaration(
        ObligationId.COMPILER,
        NotApplicable(NotApplicableRuleId.PERMISSION_PLAN_NOT_REQUIRED),
    )
    with pytest.raises(InvalidNotApplicableRule, match="cannot justify compiler"):
        audit_profile(
            profile, tuple(item for item in other if item.obligation is not ObligationId.COMPILER) + (invalid, allowed)
        )


@pytest.mark.parametrize("field", ["reason", "tracking_ref", "owner"])
def test_unsupported_requires_owned_tracking_metadata(field: str) -> None:
    values = {
        "reason": "provider is not implemented",
        "tracking_ref": "VIB-6649",
        "owner": "sdk-connectors",
        "review_by": date(2026, 9, 1),
    }
    values[field] = " "
    with pytest.raises(ValueError):
        Unsupported(**values)  # type: ignore[arg-type]


def test_unsupported_requires_valid_tracking_ref_and_typed_review_date() -> None:
    with pytest.raises(ValueError, match="tracking_ref"):
        Unsupported("gap", "not-a-ticket", "sdk", date(2026, 9, 1))
    with pytest.raises(TypeError, match="datetime.date"):
        Unsupported("gap", "VIB-6649", "sdk", datetime(2026, 9, 1))  # type: ignore[arg-type]


def test_unsupported_is_complete_but_never_satisfies_a_claim_and_expires() -> None:
    profile = profile_for(_key(SupportClaim.CORE_EXECUTION))
    unsupported = Unsupported("gap", "VIB-6649", "sdk-connectors", date(2026, 9, 1))
    declarations = tuple(
        ObligationDeclaration(item, unsupported if item is ObligationId.COMPILER else _satisfied())
        for item in profile.obligation_ids
    )
    audit = audit_profile(profile, declarations)
    assert audit.declaration_complete
    assert not audit.claim_satisfied
    assert audit.unsupported == (ObligationId.COMPILER,)
    assert audit.stale_unsupported(as_of=date(2026, 8, 31)) == ()
    assert audit.stale_unsupported(as_of=date(2026, 9, 1)) == (ObligationId.COMPILER,)


def test_profile_key_rejects_untyped_or_semantically_invalid_inputs() -> None:
    with pytest.raises(TypeError, match="IntentSemantics"):
        ClaimProfileKey("atomic_execution", Primitive.SWAP, SupportClaim.CORE_EXECUTION)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="Primitive"):
        ClaimProfileKey(IntentSemantics.ATOMIC_EXECUTION, "swap", SupportClaim.CORE_EXECUTION)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="position_open"):
        ClaimProfileKey(IntentSemantics.ATOMIC_EXECUTION, Primitive.SWAP, SupportClaim.POSITION_OPEN)
    with pytest.raises(ValueError, match="does not offer support claims"):
        ClaimProfileKey(IntentSemantics.UNAVAILABLE, Primitive.CDP, SupportClaim.CORE_EXECUTION)


def test_exact_target_feature_is_required_and_scoped() -> None:
    with pytest.raises(ValueError, match="required"):
        ClaimProfileKey(IntentSemantics.ATOMIC_EXECUTION, Primitive.SWAP, SupportClaim.EXACT_TARGET_DATA)
    with pytest.raises(ValueError, match="only"):
        ClaimProfileKey(
            IntentSemantics.ATOMIC_EXECUTION,
            Primitive.SWAP,
            SupportClaim.CORE_EXECUTION,
            ExactTargetFeature.QUOTE,
        )


def test_value_types_are_frozen_hashable_and_defensively_immutable() -> None:
    evidence = [EvidenceRef(EvidenceKind.CONTRACT_TEST, "tests/example.py::test_contract")]
    disposition = Satisfied("provider", "1", evidence)  # type: ignore[arg-type]
    evidence.append(EvidenceRef(EvidenceKind.REAL_FORK, "report.json"))
    assert len(disposition.test_evidence) == 1
    assert hash(disposition)
    with pytest.raises(FrozenInstanceError):
        disposition.provider_ref = "changed"  # type: ignore[misc]


def test_satisfied_requires_provider_contract_and_evidence() -> None:
    with pytest.raises(ValueError, match="provider_ref"):
        Satisfied("", "1", (EvidenceRef(EvidenceKind.CONTRACT_TEST, "test"),))
    with pytest.raises(ValueError, match="contract_version"):
        Satisfied("provider", "", (EvidenceRef(EvidenceKind.CONTRACT_TEST, "test"),))
    with pytest.raises(ValueError, match="at least one"):
        Satisfied("provider", "1", ())
