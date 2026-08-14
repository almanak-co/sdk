"""Core-owned lifecycle claim profiles and declaration auditing.

The rules in this module are pure and report-only.  They do not discover
connectors, gate compilation, alter teardown, or publish support.  The effective
matrix generator consumes them to produce connector capability cells.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from enum import StrEnum

from almanak.core.capability_obligations import (
    ExactTargetFeature,
    IntentSemantics,
    NotApplicable,
    NotApplicableRuleId,
    ObligationDeclaration,
    ObligationDisposition,
    ObligationId,
    ObligationState,
    Satisfied,
    SupportClaim,
    Unsupported,
)
from almanak.core.intent_types import IntentType
from almanak.framework.primitives.types import Primitive

__all__ = [
    "OBLIGATION_POLICY_VERSION",
    "AuditedObligation",
    "ClaimProfileKey",
    "InvalidNotApplicableRule",
    "ObligationAudit",
    "ObligationProfile",
    "ProfileObligation",
    "ReportedObligationState",
    "audit_profile",
    "profile_for",
    "semantics_for_intent",
]

OBLIGATION_POLICY_VERSION = 1


class InvalidNotApplicableRule(ValueError):
    """Raised when a typed N/A rule is invalid for its exact profile cell."""


class ReportedObligationState(StrEnum):
    """Migration report states; UNDECLARED is derived only from omission."""

    SATISFIED = ObligationState.SATISFIED.value
    NOT_APPLICABLE = ObligationState.NOT_APPLICABLE.value
    UNSUPPORTED = ObligationState.UNSUPPORTED.value
    UNDECLARED = "undeclared"


_INTENT_SEMANTICS: dict[IntentType, IntentSemantics] = {
    IntentType.SWAP: IntentSemantics.ATOMIC_EXECUTION,
    IntentType.LP_OPEN: IntentSemantics.POSITION_OPEN_OR_INCREASE,
    IntentType.LP_CLOSE: IntentSemantics.POSITION_DECREASE_OR_CLOSE,
    IntentType.BORROW: IntentSemantics.POSITION_OPEN_OR_INCREASE,
    IntentType.REPAY: IntentSemantics.POSITION_DECREASE_OR_CLOSE,
    IntentType.SUPPLY: IntentSemantics.POSITION_OPEN_OR_INCREASE,
    IntentType.WITHDRAW: IntentSemantics.POSITION_DECREASE_OR_CLOSE,
    IntentType.PERP_OPEN: IntentSemantics.POSITION_OPEN_OR_INCREASE,
    IntentType.PERP_CLOSE: IntentSemantics.POSITION_DECREASE_OR_CLOSE,
    IntentType.PERP_CANCEL_ORDER: IntentSemantics.NON_POSITION_RECOVERY,
    IntentType.PERP_WITHDRAW: IntentSemantics.NON_POSITION_RECOVERY,
    IntentType.BRIDGE: IntentSemantics.ATOMIC_EXECUTION,
    IntentType.ENSURE_BALANCE: IntentSemantics.NO_OPERATION,
    IntentType.FLASH_LOAN: IntentSemantics.ATOMIC_EXECUTION,
    IntentType.STAKE: IntentSemantics.POSITION_OPEN_OR_INCREASE,
    IntentType.UNSTAKE: IntentSemantics.POSITION_DECREASE_OR_CLOSE,
    IntentType.HOLD: IntentSemantics.NO_OPERATION,
    IntentType.PREDICTION_BUY: IntentSemantics.POSITION_OPEN_OR_INCREASE,
    IntentType.PREDICTION_SELL: IntentSemantics.POSITION_DECREASE_OR_CLOSE,
    IntentType.PREDICTION_REDEEM: IntentSemantics.POSITION_DECREASE_OR_CLOSE,
    IntentType.VAULT_DEPOSIT: IntentSemantics.POSITION_OPEN_OR_INCREASE,
    IntentType.VAULT_REDEEM: IntentSemantics.POSITION_DECREASE_OR_CLOSE,
    IntentType.VAULT_REALLOCATE: IntentSemantics.POSITION_MAINTENANCE,
    IntentType.VAULT_MANAGE: IntentSemantics.POSITION_MAINTENANCE,
    IntentType.LP_COLLECT_FEES: IntentSemantics.POSITION_MAINTENANCE,
    IntentType.WRAP_NATIVE: IntentSemantics.ATOMIC_EXECUTION,
    IntentType.UNWRAP_NATIVE: IntentSemantics.ATOMIC_EXECUTION,
    IntentType.DELEVERAGE: IntentSemantics.POSITION_DECREASE_OR_CLOSE,
    IntentType.LIQUIDATE: IntentSemantics.UNAVAILABLE,
    IntentType.OPEN_CDP: IntentSemantics.UNAVAILABLE,
    IntentType.MINT_STABLE: IntentSemantics.UNAVAILABLE,
    IntentType.REPAY_STABLE: IntentSemantics.UNAVAILABLE,
    IntentType.CLOSE_CDP: IntentSemantics.UNAVAILABLE,
}


def semantics_for_intent(intent: IntentType) -> IntentSemantics:
    """Return the explicit lifecycle semantics for every canonical intent."""
    if type(intent) is not IntentType:
        raise TypeError("intent must be an IntentType")
    return _INTENT_SEMANTICS[intent]


@dataclass(frozen=True, slots=True)
class ClaimProfileKey:
    """The exact semantic cell whose obligations are being requested."""

    semantics: IntentSemantics
    primitive: Primitive
    claim: SupportClaim
    exact_target_feature: ExactTargetFeature | None = None

    def __post_init__(self) -> None:
        if type(self.semantics) is not IntentSemantics:
            raise TypeError("semantics must be an IntentSemantics")
        if type(self.primitive) is not Primitive:
            raise TypeError("primitive must be a Primitive")
        if type(self.claim) is not SupportClaim:
            raise TypeError("claim must be a SupportClaim")
        if self.claim is SupportClaim.EXACT_TARGET_DATA:
            if type(self.exact_target_feature) is not ExactTargetFeature:
                raise ValueError("exact_target_feature is required for an exact_target_data claim")
        elif self.exact_target_feature is not None:
            raise ValueError("exact_target_feature is valid only for an exact_target_data claim")
        if self.semantics in (IntentSemantics.NO_OPERATION, IntentSemantics.UNAVAILABLE):
            raise ValueError(f"{self.semantics.value} does not offer support claims")
        if self.claim is SupportClaim.POSITION_OPEN and self.semantics is not IntentSemantics.POSITION_OPEN_OR_INCREASE:
            raise ValueError("position_open requires POSITION_OPEN_OR_INCREASE semantics")
        if (
            self.claim is SupportClaim.POSITION_CLOSE
            and self.semantics is not IntentSemantics.POSITION_DECREASE_OR_CLOSE
        ):
            raise ValueError("position_close requires POSITION_DECREASE_OR_CLOSE semantics")
        if self.claim in (SupportClaim.FULL_LIFECYCLE_CERTIFICATION, SupportClaim.VALUATION_READY):
            if self.semantics not in (
                IntentSemantics.POSITION_OPEN_OR_INCREASE,
                IntentSemantics.POSITION_DECREASE_OR_CLOSE,
                IntentSemantics.POSITION_MAINTENANCE,
            ):
                raise ValueError(f"{self.claim.value} requires position-bearing intent semantics")


@dataclass(frozen=True, slots=True)
class ProfileObligation:
    """One required obligation and the only N/A rules core policy permits."""

    obligation: ObligationId
    allowed_not_applicable_rules: frozenset[NotApplicableRuleId] = frozenset()

    def __post_init__(self) -> None:
        if type(self.obligation) is not ObligationId:
            raise TypeError("obligation must be an ObligationId")
        rules = frozenset(self.allowed_not_applicable_rules)
        if not all(type(rule) is NotApplicableRuleId for rule in rules):
            raise TypeError("allowed_not_applicable_rules must contain NotApplicableRuleId values")
        object.__setattr__(self, "allowed_not_applicable_rules", rules)


@dataclass(frozen=True, slots=True)
class ObligationProfile:
    """Canonical, ordered obligations for one claim profile cell."""

    key: ClaimProfileKey
    obligations: tuple[ProfileObligation, ...]
    policy_version: int = OBLIGATION_POLICY_VERSION

    def __post_init__(self) -> None:
        if type(self.key) is not ClaimProfileKey:
            raise TypeError("key must be a ClaimProfileKey")
        obligations = tuple(self.obligations)
        if not obligations:
            raise ValueError("an obligation profile cannot be empty")
        if not all(type(item) is ProfileObligation for item in obligations):
            raise TypeError("obligations must contain ProfileObligation values")
        identifiers = [item.obligation for item in obligations]
        if len(identifiers) != len(set(identifiers)):
            raise ValueError("an obligation profile cannot contain duplicate obligations")
        if type(self.policy_version) is not int or self.policy_version < 1:
            raise ValueError("policy_version must be an integer >= 1")
        object.__setattr__(self, "obligations", obligations)

    @property
    def obligation_ids(self) -> tuple[ObligationId, ...]:
        return tuple(item.obligation for item in self.obligations)


_PERMISSION_NA = frozenset({NotApplicableRuleId.PERMISSION_PLAN_NOT_REQUIRED})


def _row(obligation: ObligationId) -> ProfileObligation:
    allowed = _PERMISSION_NA if obligation is ObligationId.PERMISSION_PLAN else frozenset()
    return ProfileObligation(obligation=obligation, allowed_not_applicable_rules=allowed)


_EXECUTION = (
    ObligationId.ASSET_RESOLUTION,
    ObligationId.VENUE_RESOLUTION,
    ObligationId.AMOUNT_PROTECTION,
    ObligationId.COMPILER,
    ObligationId.RECEIPT_EVIDENCE,
    ObligationId.MONEY_LEGS,
    ObligationId.PERMISSION_PLAN,
)

_POSITION_OPEN = (
    ObligationId.OPEN_EXECUTION,
    ObligationId.POSITION_IDENTITY,
    ObligationId.REGISTRY_TRANSITION,
    ObligationId.INVENTORY_ADAPTER,
    ObligationId.POSITION_READER,
    ObligationId.POSITION_OBSERVER,
    ObligationId.CLOSE_PLANNER,
    ObligationId.CERTIFICATION_PATH,
)

_POSITION_CLOSE = (
    ObligationId.CLOSE_EXECUTION,
    ObligationId.POSITION_IDENTITY,
    ObligationId.INVENTORY_ADAPTER,
    ObligationId.POSITION_READER,
    ObligationId.POSITION_OBSERVER,
    ObligationId.CLOSE_PLANNER,
    ObligationId.ACCOUNTING_DURABILITY,
)

_FULL_LIFECYCLE = (
    ObligationId.POSITION_ENUMERATION,
    ObligationId.BLOCK_ANCHORED_OBSERVATION,
    ObligationId.ACCOUNTING_DURABILITY,
    ObligationId.RESTART_RECOVERY,
)

_VALUATION = (
    ObligationId.POSITION_READER,
    ObligationId.CANONICAL_ASSET_ORDER,
    ObligationId.VALUATION_POLICY,
)

_EXACT_TARGET = (
    ObligationId.ASSET_RESOLUTION,
    ObligationId.EXACT_VENUE_BINDING,
    ObligationId.TARGET_DATA_PROVIDER,
)

_MANAGED_ANVIL = (
    ObligationId.ANVIL_FUNDING,
    ObligationId.ANVIL_GAS,
    ObligationId.ANVIL_QUOTE,
    ObligationId.ANVIL_FORK_READ,
    ObligationId.ANVIL_LIFECYCLE_EVIDENCE,
)


def _canonical_union(*groups: tuple[ObligationId, ...]) -> tuple[ObligationId, ...]:
    selected = {item for group in groups for item in group}
    return tuple(item for item in ObligationId if item in selected)


def profile_for(key: ClaimProfileKey) -> ObligationProfile:
    """Build the immutable core profile for ``key``."""
    if type(key) is not ClaimProfileKey:
        raise TypeError("key must be a ClaimProfileKey")
    identifiers: tuple[ObligationId, ...]
    if key.claim is SupportClaim.CORE_EXECUTION:
        identifiers = _EXECUTION
    elif key.claim is SupportClaim.POSITION_OPEN:
        identifiers = _canonical_union(_EXECUTION, _POSITION_OPEN)
    elif key.claim is SupportClaim.POSITION_CLOSE:
        identifiers = _canonical_union(_EXECUTION, _POSITION_CLOSE)
    elif key.claim is SupportClaim.FULL_LIFECYCLE_CERTIFICATION:
        identifiers = _canonical_union(_EXECUTION, _POSITION_OPEN, _POSITION_CLOSE, _FULL_LIFECYCLE)
    elif key.claim is SupportClaim.VALUATION_READY:
        identifiers = _VALUATION
    elif key.claim is SupportClaim.EXACT_TARGET_DATA:
        identifiers = _EXACT_TARGET
    elif key.claim is SupportClaim.MANAGED_ANVIL_TESTABLE:
        identifiers = _MANAGED_ANVIL
    else:  # pragma: no cover - exhaustive enum defense
        raise AssertionError(f"unhandled support claim {key.claim!r}")
    return ObligationProfile(key=key, obligations=tuple(_row(item) for item in identifiers))


@dataclass(frozen=True, slots=True)
class AuditedObligation:
    """One required obligation after declaration audit."""

    obligation: ObligationId
    disposition: ObligationDisposition | None

    def __post_init__(self) -> None:
        if type(self.obligation) is not ObligationId:
            raise TypeError("obligation must be an ObligationId")
        if self.disposition is not None and type(self.disposition) not in (Satisfied, NotApplicable, Unsupported):
            raise TypeError("disposition must be Satisfied, NotApplicable, Unsupported, or None")

    @property
    def state(self) -> ReportedObligationState:
        if self.disposition is None:
            return ReportedObligationState.UNDECLARED
        return ReportedObligationState(self.disposition.state.value)

    def to_dict(self) -> dict[str, object]:
        result: dict[str, object] = {"obligation": self.obligation.value, "state": self.state.value}
        if self.disposition is None:
            return result
        if isinstance(self.disposition, Satisfied):
            result.update(
                provider_ref=self.disposition.provider_ref,
                contract_version=self.disposition.contract_version,
                test_evidence=[{"kind": ref.kind.value, "ref": ref.ref} for ref in self.disposition.test_evidence],
            )
        elif isinstance(self.disposition, NotApplicable):
            result.update(rule_ref=self.disposition.rule_id.value, reason_code=self.disposition.rule_id.value)
        elif isinstance(self.disposition, Unsupported):
            result.update(
                reason=self.disposition.reason,
                tracking_ref=self.disposition.tracking_ref,
                owner=self.disposition.owner,
                review_by=self.disposition.review_by.isoformat(),
            )
        else:  # pragma: no cover - ObligationDisposition is a closed union
            raise AssertionError(f"unhandled disposition {self.disposition!r}")
        return result


@dataclass(frozen=True, slots=True)
class ObligationAudit:
    """Deterministic completeness and support result for one profile."""

    profile: ObligationProfile
    obligations: tuple[AuditedObligation, ...]
    unexpected: tuple[ObligationId, ...]

    def __post_init__(self) -> None:
        if type(self.profile) is not ObligationProfile:
            raise TypeError("profile must be an ObligationProfile")
        if self.profile != profile_for(self.profile.key):
            raise ValueError("audit results require the canonical core profile for their key")
        obligations = tuple(self.obligations)
        if not all(type(item) is AuditedObligation for item in obligations):
            raise TypeError("obligations must contain AuditedObligation values")
        if tuple(item.obligation for item in obligations) != self.profile.obligation_ids:
            raise ValueError("audited obligations must exactly cover the profile in canonical order")
        for audited, required in zip(obligations, self.profile.obligations, strict=True):
            if isinstance(audited.disposition, NotApplicable):
                if audited.disposition.rule_id not in required.allowed_not_applicable_rules:
                    raise InvalidNotApplicableRule(
                        f"{audited.disposition.rule_id.value} cannot justify "
                        f"{required.obligation.value} for {self.profile.key.claim.value}"
                    )
        unexpected = tuple(self.unexpected)
        if not all(type(item) is ObligationId for item in unexpected):
            raise TypeError("unexpected must contain ObligationId values")
        canonical_unexpected = tuple(item for item in ObligationId if item in unexpected)
        if unexpected != canonical_unexpected or len(unexpected) != len(set(unexpected)):
            raise ValueError("unexpected obligations must be unique and canonically ordered")
        if set(unexpected) & set(self.profile.obligation_ids):
            raise ValueError("required obligations cannot also be unexpected")
        object.__setattr__(self, "obligations", obligations)
        object.__setattr__(self, "unexpected", unexpected)

    @property
    def missing(self) -> tuple[ObligationId, ...]:
        return tuple(item.obligation for item in self.obligations if item.disposition is None)

    @property
    def unsupported(self) -> tuple[ObligationId, ...]:
        return tuple(
            item.obligation
            for item in self.obligations
            if item.disposition is not None and item.disposition.state is ObligationState.UNSUPPORTED
        )

    @property
    def declaration_complete(self) -> bool:
        return not self.missing and not self.unexpected

    @property
    def claim_satisfied(self) -> bool:
        return self.declaration_complete and not self.unsupported

    def stale_unsupported(self, *, as_of: date) -> tuple[ObligationId, ...]:
        if type(as_of) is not date:
            raise TypeError("as_of must be a datetime.date")
        return tuple(
            item.obligation
            for item in self.obligations
            if isinstance(item.disposition, Unsupported) and item.disposition.is_review_due(as_of=as_of)
        )

    def to_migration_dict(self) -> dict[str, object]:
        """Serialize a report in which omission is explicitly UNDECLARED."""
        return {
            "policyVersion": self.profile.policy_version,
            "semantics": self.profile.key.semantics.value,
            "primitive": self.profile.key.primitive.value,
            "claim": self.profile.key.claim.value,
            "exactTargetFeature": (
                self.profile.key.exact_target_feature.value if self.profile.key.exact_target_feature else None
            ),
            "obligations": [item.to_dict() for item in self.obligations],
            "unexpected": [item.value for item in self.unexpected],
        }


def audit_profile(
    profile: ObligationProfile,
    declarations: tuple[ObligationDeclaration, ...],
) -> ObligationAudit:
    """Audit declarations without fabricating support from absent evidence."""
    if type(profile) is not ObligationProfile:
        raise TypeError("profile must be an ObligationProfile")
    if profile != profile_for(profile.key):
        raise ValueError("audit_profile accepts only the canonical core profile for its key")
    declarations = tuple(declarations)
    if not all(type(item) is ObligationDeclaration for item in declarations):
        raise TypeError("declarations must contain ObligationDeclaration values")
    by_id: dict[ObligationId, ObligationDeclaration] = {}
    for declaration in declarations:
        if declaration.obligation in by_id:
            raise ValueError(f"duplicate declaration for {declaration.obligation.value}")
        by_id[declaration.obligation] = declaration

    required = {item.obligation: item for item in profile.obligations}
    unexpected = tuple(item for item in ObligationId if item in by_id and item not in required)
    audited: list[AuditedObligation] = []
    for obligation in profile.obligations:
        candidate = by_id.get(obligation.obligation)
        if candidate is not None and isinstance(candidate.disposition, NotApplicable):
            if candidate.disposition.rule_id not in obligation.allowed_not_applicable_rules:
                raise InvalidNotApplicableRule(
                    f"{candidate.disposition.rule_id.value} cannot justify "
                    f"{obligation.obligation.value} for {profile.key.claim.value}"
                )
        audited.append(
            AuditedObligation(
                obligation=obligation.obligation,
                disposition=candidate.disposition if candidate is not None else None,
            )
        )
    return ObligationAudit(profile=profile, obligations=tuple(audited), unexpected=unexpected)
