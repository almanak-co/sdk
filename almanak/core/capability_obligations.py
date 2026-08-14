"""Canonical lifecycle-capability obligation vocabulary.

This module is a dependency-light boundary shared by connector declarations and
framework policy.  It deliberately contains no connector discovery, profile
composition, or runtime admission behavior.  Those rules live in
``almanak.framework.capabilities.obligation_profiles``.

The three :class:`ObligationState` values are the only states a final audited
declaration may carry.  ``UNDECLARED`` is intentionally absent: omission is a
migration-report result, never a declaration a connector can make.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from enum import StrEnum

__all__ = [
    "EvidenceKind",
    "EvidenceRef",
    "ExactTargetFeature",
    "IntentSemantics",
    "NotApplicable",
    "NotApplicableRuleId",
    "ObligationDeclaration",
    "ObligationDisposition",
    "ObligationId",
    "ObligationState",
    "Satisfied",
    "SupportClaim",
    "Unsupported",
]


class SupportClaim(StrEnum):
    """Stable, scoped claims the product may offer."""

    CORE_EXECUTION = "core_execution"
    POSITION_OPEN = "position_open"
    POSITION_CLOSE = "position_close"
    FULL_LIFECYCLE_CERTIFICATION = "full_lifecycle_certification"
    VALUATION_READY = "valuation_ready"
    EXACT_TARGET_DATA = "exact_target_data"
    MANAGED_ANVIL_TESTABLE = "managed_anvil_testable"


class IntentSemantics(StrEnum):
    """Lifecycle effect of an intent, independent of its protocol spelling."""

    ATOMIC_EXECUTION = "atomic_execution"
    POSITION_OPEN_OR_INCREASE = "position_open_or_increase"
    POSITION_DECREASE_OR_CLOSE = "position_decrease_or_close"
    POSITION_MAINTENANCE = "position_maintenance"
    NON_POSITION_RECOVERY = "non_position_recovery"
    NO_OPERATION = "no_operation"
    UNAVAILABLE = "unavailable"


class ExactTargetFeature(StrEnum):
    """Data feature bound to the exact venue named by a request."""

    QUOTE = "quote"
    TWAP = "twap"
    OHLCV = "ohlcv"
    DEPTH = "depth"
    REFERENCE_PRICE = "reference_price"


class ObligationId(StrEnum):
    """Stable identifiers for mechanically auditable capability obligations."""

    ASSET_RESOLUTION = "asset_resolution"
    VENUE_RESOLUTION = "venue_resolution"
    AMOUNT_PROTECTION = "amount_protection"
    COMPILER = "compiler"
    RECEIPT_EVIDENCE = "receipt_evidence"
    MONEY_LEGS = "money_legs"
    PERMISSION_PLAN = "permission_plan"

    OPEN_EXECUTION = "open_execution"
    CLOSE_EXECUTION = "close_execution"
    POSITION_IDENTITY = "position_identity"
    REGISTRY_TRANSITION = "registry_transition"
    INVENTORY_ADAPTER = "inventory_adapter"
    POSITION_READER = "position_reader"
    POSITION_OBSERVER = "position_observer"
    CLOSE_PLANNER = "close_planner"
    CERTIFICATION_PATH = "certification_path"

    POSITION_ENUMERATION = "position_enumeration"
    BLOCK_ANCHORED_OBSERVATION = "block_anchored_observation"
    ACCOUNTING_DURABILITY = "accounting_durability"
    RESTART_RECOVERY = "restart_recovery"

    CANONICAL_ASSET_ORDER = "canonical_asset_order"
    VALUATION_POLICY = "valuation_policy"

    EXACT_VENUE_BINDING = "exact_venue_binding"
    TARGET_DATA_PROVIDER = "target_data_provider"

    ANVIL_FUNDING = "anvil_funding"
    ANVIL_GAS = "anvil_gas"
    ANVIL_QUOTE = "anvil_quote"
    ANVIL_FORK_READ = "anvil_fork_read"
    ANVIL_LIFECYCLE_EVIDENCE = "anvil_lifecycle_evidence"


class ObligationState(StrEnum):
    """Final audited states.  Omission is invalid, not a fourth declaration."""

    SATISFIED = "satisfied"
    NOT_APPLICABLE = "not_applicable"
    UNSUPPORTED = "unsupported"


class NotApplicableRuleId(StrEnum):
    """Core-owned rules that may justify a structurally inapplicable obligation."""

    PERMISSION_PLAN_NOT_REQUIRED = "permission_plan_not_required"


class EvidenceKind(StrEnum):
    """Kinds of durable evidence that may support a satisfied obligation."""

    CONTRACT_TEST = "contract_test"
    INTENT_TEST = "intent_test"
    MANAGED_ANVIL = "managed_anvil"
    REAL_FORK = "real_fork"
    HOSTED_CONTRACT = "hosted_contract"
    GENERATED_MATRIX = "generated_matrix"


def _non_empty(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return value.strip()


@dataclass(frozen=True, slots=True)
class EvidenceRef:
    """A durable reference to evidence supporting one obligation."""

    kind: EvidenceKind
    ref: str

    def __post_init__(self) -> None:
        if type(self.kind) is not EvidenceKind:
            raise TypeError("kind must be an EvidenceKind")
        object.__setattr__(self, "ref", _non_empty(self.ref, field_name="ref"))


@dataclass(frozen=True, slots=True)
class Satisfied:
    """The obligation has a named provider contract and durable test evidence."""

    provider_ref: str
    contract_version: str
    test_evidence: tuple[EvidenceRef, ...]
    state: ObligationState = ObligationState.SATISFIED

    def __post_init__(self) -> None:
        object.__setattr__(self, "provider_ref", _non_empty(self.provider_ref, field_name="provider_ref"))
        object.__setattr__(
            self,
            "contract_version",
            _non_empty(self.contract_version, field_name="contract_version"),
        )
        evidence = tuple(self.test_evidence)
        if not evidence:
            raise ValueError("test_evidence must contain at least one EvidenceRef")
        if not all(type(item) is EvidenceRef for item in evidence):
            raise TypeError("test_evidence must contain only EvidenceRef values")
        if type(self.state) is not ObligationState or self.state is not ObligationState.SATISFIED:
            raise ValueError("Satisfied.state must be SATISFIED")
        object.__setattr__(self, "test_evidence", evidence)


@dataclass(frozen=True, slots=True)
class NotApplicable:
    """A typed core rule proves the obligation is structurally out of scope."""

    rule_id: NotApplicableRuleId
    state: ObligationState = ObligationState.NOT_APPLICABLE

    def __post_init__(self) -> None:
        if type(self.rule_id) is not NotApplicableRuleId:
            raise TypeError("rule_id must be a NotApplicableRuleId")
        if type(self.state) is not ObligationState or self.state is not ObligationState.NOT_APPLICABLE:
            raise ValueError("NotApplicable.state must be NOT_APPLICABLE")


_TRACKING_REF = re.compile(r"^(?:[A-Z][A-Z0-9]+-\d+|https://\S+)$")


@dataclass(frozen=True, slots=True)
class Unsupported:
    """A declared gap with an owner, tracking reference, and review date."""

    reason: str
    tracking_ref: str
    owner: str
    review_by: date
    state: ObligationState = ObligationState.UNSUPPORTED

    def __post_init__(self) -> None:
        object.__setattr__(self, "reason", _non_empty(self.reason, field_name="reason"))
        tracking_ref = _non_empty(self.tracking_ref, field_name="tracking_ref")
        if _TRACKING_REF.fullmatch(tracking_ref) is None:
            raise ValueError("tracking_ref must be a ticket identifier (for example VIB-123) or https URL")
        object.__setattr__(self, "tracking_ref", tracking_ref)
        object.__setattr__(self, "owner", _non_empty(self.owner, field_name="owner"))
        if type(self.review_by) is not date:
            raise TypeError("review_by must be a datetime.date")
        if type(self.state) is not ObligationState or self.state is not ObligationState.UNSUPPORTED:
            raise ValueError("Unsupported.state must be UNSUPPORTED")

    def is_review_due(self, *, as_of: date) -> bool:
        """Return whether the declaration must be reviewed on ``as_of``."""
        if type(as_of) is not date:
            raise TypeError("as_of must be a datetime.date")
        return as_of >= self.review_by


ObligationDisposition = Satisfied | NotApplicable | Unsupported


@dataclass(frozen=True, slots=True)
class ObligationDeclaration:
    """One explicit, final declaration for a stable obligation identifier."""

    obligation: ObligationId
    disposition: ObligationDisposition

    def __post_init__(self) -> None:
        if type(self.obligation) is not ObligationId:
            raise TypeError("obligation must be an ObligationId")
        if type(self.disposition) not in (Satisfied, NotApplicable, Unsupported):
            raise TypeError("disposition must be Satisfied, NotApplicable, or Unsupported")

    @property
    def state(self) -> ObligationState:
        return self.disposition.state
