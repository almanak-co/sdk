"""Deterministic effective lifecycle-capability matrix generation.

This module is an internal, report-only surface.  It combines exact connector
manifest cells with core obligation profiles, but it never treats the presence
of a provider hook as proof that an obligation is satisfied.  Existing manifest
fields and approved framework mechanisms are emitted as provider candidates;
only an explicit :class:`ResolvedDeclaration` may determine a final audited
state.

The pure derivation performs no provider imports, subprocesses, or network
calls. Default connector discovery imports manifest modules and scans the local
connector package; callers may inject connectors to avoid even that filesystem
discovery. JSON and Markdown deliberately omit timestamps and checkout metadata
so identical inputs produce byte-identical CI artifacts.
"""

from __future__ import annotations

import argparse
import json
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, fields, is_dataclass
from enum import Enum, StrEnum
from pathlib import PurePosixPath, PureWindowsPath
from typing import Any

from almanak.core.capability_obligations import (
    ExactTargetFeature,
    IntentSemantics,
    NotApplicable,
    ObligationDeclaration,
    ObligationId,
    Satisfied,
    SupportClaim,
    Unsupported,
)
from almanak.core.intent_types import IntentType
from almanak.framework.capabilities.obligation_profiles import (
    OBLIGATION_POLICY_VERSION,
    AuditedObligation,
    ClaimProfileKey,
    ObligationProfile,
    ReportedObligationState,
    audit_profile,
    profile_for,
    semantics_for_intent,
)
from almanak.framework.primitives.taxonomy import primitive_for, record_for
from almanak.framework.primitives.types import Primitive

__all__ = [
    "EFFECTIVE_MATRIX_SCHEMA_VERSION",
    "EFFECTIVE_DECLARATIONS",
    "CapabilityCellKey",
    "DeclarationResolver",
    "EffectiveCapabilityCell",
    "EffectiveCapabilityMatrix",
    "EffectiveDeclarationRule",
    "EffectiveObligation",
    "MatrixUniverse",
    "ProviderSource",
    "ResolvedDeclaration",
    "SourceKind",
    "SourceRole",
    "UniverseKind",
    "build_effective_capability_matrix",
    "render_markdown",
]

EFFECTIVE_MATRIX_SCHEMA_VERSION = 1


def _validate_durable_ref(value: str, field_name: str) -> None:
    if PurePosixPath(value).is_absolute() or PureWindowsPath(value).is_absolute():
        raise ValueError(f"{field_name} must be a durable non-absolute reference")


class SourceKind(StrEnum):
    """Stable origin classes for requirement, candidate, and declaration refs."""

    CORE_PROFILE = "core_profile"
    CONNECTOR_MANIFEST = "connector_manifest"
    FRAMEWORK_DEFAULT = "framework_default"
    APPROVED_ALTERNATIVE = "approved_alternative"
    CORE_RULE = "core_rule"


class SourceRole(StrEnum):
    """Whether a source requires, suggests, or resolves an obligation."""

    REQUIREMENT = "requirement"
    CANDIDATE = "candidate"
    DECLARATION = "declaration"


class UniverseKind(StrEnum):
    """Provenance for the claim-cell universe supplied to the generator."""

    REGISTERED_STRATEGY_SUPPORT = "registered_strategy_support"
    INJECTED_CLAIM_CELLS = "injected_claim_cells"


@dataclass(frozen=True, slots=True)
class MatrixUniverse:
    """Stable identity for the set of cells being audited."""

    kind: UniverseKind
    source_ref: str

    def __post_init__(self) -> None:
        if type(self.kind) is not UniverseKind:
            raise TypeError("kind must be a UniverseKind")
        if not isinstance(self.source_ref, str) or not self.source_ref.strip():
            raise ValueError("source_ref must be a non-empty string")
        _validate_durable_ref(self.source_ref.strip(), "source_ref")
        object.__setattr__(self, "source_ref", self.source_ref.strip())

    def to_dict(self) -> dict[str, str]:
        return {"kind": self.kind.value, "sourceRef": self.source_ref}


@dataclass(frozen=True, slots=True)
class ProviderSource:
    """Stable semantic provenance for one effective obligation."""

    kind: SourceKind
    role: SourceRole
    rule_id: str
    ref: str
    detail: str

    def __post_init__(self) -> None:
        if type(self.kind) is not SourceKind:
            raise TypeError("kind must be a SourceKind")
        if type(self.role) is not SourceRole:
            raise TypeError("role must be a SourceRole")
        for field_name in ("rule_id", "ref", "detail"):
            value = getattr(self, field_name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{field_name} must be a non-empty string")
            object.__setattr__(self, field_name, value.strip())
        _validate_durable_ref(self.ref, "ProviderSource.ref")

    def to_dict(self) -> dict[str, str]:
        return {
            "kind": self.kind.value,
            "role": self.role.value,
            "ruleId": self.rule_id,
            "ref": self.ref,
            "detail": self.detail,
        }


@dataclass(frozen=True, slots=True)
class ResolvedDeclaration:
    """An explicit audited declaration and the source that owns it."""

    declaration: ObligationDeclaration
    source: ProviderSource

    def __post_init__(self) -> None:
        if type(self.declaration) is not ObligationDeclaration:
            raise TypeError("declaration must be an ObligationDeclaration")
        if type(self.source) is not ProviderSource:
            raise TypeError("source must be a ProviderSource")
        if self.source.role is not SourceRole.DECLARATION:
            raise ValueError("a resolved declaration source must have DECLARATION role")


@dataclass(frozen=True, slots=True)
class EffectiveDeclarationRule:
    """One exact, reviewable canonical declaration rule consumed by CI output."""

    key: CapabilityCellKey
    resolved: ResolvedDeclaration

    def __post_init__(self) -> None:
        if type(self.key) is not CapabilityCellKey:
            raise TypeError("key must be a CapabilityCellKey")
        if type(self.resolved) is not ResolvedDeclaration:
            raise TypeError("resolved must be a ResolvedDeclaration")


@dataclass(frozen=True, slots=True)
class CapabilityCellKey:
    """Exact connector cell audited under one core support claim."""

    protocol: str
    chain: str
    intent: IntentType
    semantics: IntentSemantics
    primitive: Primitive
    claim: SupportClaim
    exact_target_feature: ExactTargetFeature | None = None

    def __post_init__(self) -> None:
        for field_name in ("protocol", "chain"):
            value = getattr(self, field_name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{field_name} must be a non-empty string")
            object.__setattr__(self, field_name, value.strip().lower())
        if type(self.intent) is not IntentType:
            raise TypeError("intent must be an IntentType")
        expected_semantics = semantics_for_intent(self.intent)
        if self.semantics is not expected_semantics:
            raise ValueError(
                f"semantics for {self.intent.value} must be {expected_semantics.value}, got {self.semantics!r}"
            )
        # ClaimProfileKey owns the remaining semantic/type validation.
        _ = self.profile_key

    @property
    def profile_key(self) -> ClaimProfileKey:
        return ClaimProfileKey(
            semantics=self.semantics,
            primitive=self.primitive,
            claim=self.claim,
            exact_target_feature=self.exact_target_feature,
        )

    def sort_key(self) -> tuple[object, ...]:
        return (
            self.protocol,
            self.chain,
            self.intent.value,
            self.claim.value,
            self.exact_target_feature.value if self.exact_target_feature else "",
        )


type DeclarationResolver = Callable[
    [Any, CapabilityCellKey, ObligationProfile],
    Iterable[ResolvedDeclaration],
]

# Core-owned framework-default and approved-alternative rules land here. Exact
# connector provider declarations live on Connector.lifecycle_declarations.
# An empty initial registry is deliberate; provider candidates are not proof.
EFFECTIVE_DECLARATIONS: tuple[EffectiveDeclarationRule, ...] = ()


def _connector_declaration_rules(connectors: tuple[Any, ...]) -> tuple[EffectiveDeclarationRule, ...]:
    rules: list[EffectiveDeclarationRule] = []
    for connector in connectors:
        for item in getattr(connector, "lifecycle_declarations", ()):
            record_for(item.intent.value)  # Fail loud before primitive_for's compatibility fallback.
            key = CapabilityCellKey(
                protocol=item.protocol,
                chain=item.chain,
                intent=item.intent,
                semantics=semantics_for_intent(item.intent),
                primitive=primitive_for(item.intent.value, item.protocol),
                claim=item.claim,
                exact_target_feature=item.exact_target_feature,
            )
            rules.append(
                EffectiveDeclarationRule(
                    key,
                    ResolvedDeclaration(
                        item.declaration,
                        ProviderSource(
                            SourceKind.CONNECTOR_MANIFEST,
                            SourceRole.DECLARATION,
                            item.rule_id,
                            item.source_ref,
                            item.source_detail,
                        ),
                    ),
                )
            )
    return tuple(rules)


def _index_declaration_rules(
    rules: tuple[EffectiveDeclarationRule, ...],
) -> dict[CapabilityCellKey, tuple[ResolvedDeclaration, ...]]:
    indexed: dict[CapabilityCellKey, list[ResolvedDeclaration]] = {}
    seen: set[tuple[CapabilityCellKey, ObligationId]] = set()
    for rule in rules:
        if type(rule) is not EffectiveDeclarationRule:
            raise TypeError("canonical declarations must contain EffectiveDeclarationRule values")
        obligation = rule.resolved.declaration.obligation
        profile = profile_for(rule.key.profile_key)
        if obligation not in profile.obligation_ids:
            raise ValueError(f"canonical declaration {obligation.value} is outside profile {rule.key.claim.value}")
        identity = (rule.key, obligation)
        if identity in seen:
            raise ValueError(f"duplicate canonical declaration for {rule.key.sort_key()!r}/{obligation.value}")
        seen.add(identity)
        indexed.setdefault(rule.key, []).append(rule.resolved)
    return {key: tuple(values) for key, values in indexed.items()}


@dataclass(frozen=True, slots=True)
class EffectiveObligation:
    """One audited obligation with requirement, candidate, and resolution refs."""

    audited: AuditedObligation
    sources: tuple[ProviderSource, ...]

    def __post_init__(self) -> None:
        if type(self.audited) is not AuditedObligation:
            raise TypeError("audited must be an AuditedObligation")
        sources = tuple(self.sources)
        if not sources or sources[0].role is not SourceRole.REQUIREMENT:
            raise ValueError("sources must begin with the core profile requirement")
        if not all(type(item) is ProviderSource for item in sources):
            raise TypeError("sources must contain ProviderSource values")
        declaration_sources = [item for item in sources if item.role is SourceRole.DECLARATION]
        if self.audited.disposition is None and declaration_sources:
            raise ValueError("an undeclared obligation cannot carry declaration provenance")
        if self.audited.disposition is not None and len(declaration_sources) != 1:
            raise ValueError("a declared obligation must carry exactly one declaration source")
        if isinstance(self.audited.disposition, Satisfied):
            _validate_durable_ref(self.audited.disposition.provider_ref, "Satisfied.provider_ref")
            for evidence in self.audited.disposition.test_evidence:
                _validate_durable_ref(evidence.ref, "EvidenceRef.ref")
        object.__setattr__(self, "sources", sources)

    @property
    def state(self) -> ReportedObligationState:
        return self.audited.state

    def to_dict(self) -> dict[str, object]:
        result = self.audited.to_dict()
        result["sources"] = [source.to_dict() for source in self.sources]
        return result


@dataclass(frozen=True, slots=True)
class EffectiveCapabilityCell:
    """One exact scoped claim and all of its audited obligations."""

    key: CapabilityCellKey
    obligations: tuple[EffectiveObligation, ...]
    policy_version: int = OBLIGATION_POLICY_VERSION

    def __post_init__(self) -> None:
        if type(self.key) is not CapabilityCellKey:
            raise TypeError("key must be a CapabilityCellKey")
        obligations = tuple(self.obligations)
        if not all(type(item) is EffectiveObligation for item in obligations):
            raise TypeError("obligations must contain EffectiveObligation values")
        expected = profile_for(self.key.profile_key).obligation_ids
        if tuple(item.audited.obligation for item in obligations) != expected:
            raise ValueError("effective obligations must exactly cover the canonical profile")
        if self.policy_version != OBLIGATION_POLICY_VERSION:
            raise ValueError(f"policy_version must be {OBLIGATION_POLICY_VERSION}")
        object.__setattr__(self, "obligations", obligations)

    @property
    def claim_satisfied(self) -> bool:
        return all(
            item.state in (ReportedObligationState.SATISFIED, ReportedObligationState.NOT_APPLICABLE)
            for item in self.obligations
        )

    @property
    def declaration_complete(self) -> bool:
        return all(item.state is not ReportedObligationState.UNDECLARED for item in self.obligations)

    def counts_by_state(self) -> dict[ReportedObligationState, int]:
        return {state: sum(item.state is state for item in self.obligations) for state in ReportedObligationState}

    def to_dict(self) -> dict[str, object]:
        counts = self.counts_by_state()
        return {
            "protocol": self.key.protocol,
            "chain": self.key.chain,
            "intent": self.key.intent.value,
            "semantics": self.key.semantics.value,
            "primitive": self.key.primitive.value,
            "claim": self.key.claim.value,
            "exactTargetFeature": self.key.exact_target_feature.value if self.key.exact_target_feature else None,
            "policyVersion": self.policy_version,
            "declarationComplete": self.declaration_complete,
            "claimSatisfied": self.claim_satisfied,
            "missing": [
                item.audited.obligation.value
                for item in self.obligations
                if item.state is ReportedObligationState.UNDECLARED
            ],
            "unsupported": [
                item.audited.obligation.value
                for item in self.obligations
                if item.state is ReportedObligationState.UNSUPPORTED
            ],
            "summary": {state.value: counts[state] for state in ReportedObligationState},
            "obligations": [item.to_dict() for item in self.obligations],
        }


@dataclass(frozen=True, slots=True)
class EffectiveCapabilityMatrix:
    """Immutable, canonically ordered effective capability cells."""

    cells: tuple[EffectiveCapabilityCell, ...]
    universe: MatrixUniverse
    schema_version: int = EFFECTIVE_MATRIX_SCHEMA_VERSION

    def __post_init__(self) -> None:
        cells = tuple(self.cells)
        if type(self.universe) is not MatrixUniverse:
            raise TypeError("universe must be a MatrixUniverse")
        if not all(type(item) is EffectiveCapabilityCell for item in cells):
            raise TypeError("cells must contain EffectiveCapabilityCell values")
        if tuple(sorted(cells, key=lambda item: item.key.sort_key())) != cells:
            raise ValueError("cells must be in canonical order")
        keys = [item.key for item in cells]
        if len(keys) != len(set(keys)):
            raise ValueError("matrix cannot contain duplicate capability cells")
        if self.schema_version != EFFECTIVE_MATRIX_SCHEMA_VERSION:
            raise ValueError(f"schema_version must be {EFFECTIVE_MATRIX_SCHEMA_VERSION}")
        object.__setattr__(self, "cells", cells)

    def counts_by_state(self) -> dict[ReportedObligationState, int]:
        return {state: sum(cell.counts_by_state()[state] for cell in self.cells) for state in ReportedObligationState}

    def to_dict(self) -> dict[str, object]:
        counts = self.counts_by_state()
        return {
            "schemaVersion": self.schema_version,
            "generatorContract": "effective_lifecycle_capabilities.v1",
            "policyVersion": OBLIGATION_POLICY_VERSION,
            "universe": self.universe.to_dict(),
            "policySourceRef": "almanak.framework.capabilities.obligation_profiles:profile_for",
            "summary": {
                "cells": len(self.cells),
                "obligations": sum(counts.values()),
                "claimsSatisfied": sum(cell.claim_satisfied for cell in self.cells),
                "claimsUnsatisfied": sum(not cell.claim_satisfied for cell in self.cells),
                **{state.value: counts[state] for state in ReportedObligationState},
            },
            "cells": [cell.to_dict() for cell in self.cells],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2, ensure_ascii=False) + "\n"


def _stable_value(value: object) -> str:
    module = getattr(value, "module", None)
    attribute = getattr(value, "attribute", None)
    if isinstance(module, str) and isinstance(attribute, str):
        return f"{module}:{attribute}"
    if isinstance(value, Enum):
        return str(value.value)
    if is_dataclass(value) and not isinstance(value, type):
        members = ",".join(f"{field.name}={_stable_value(getattr(value, field.name))}" for field in fields(value))
        return f"{type(value).__name__}({members})"
    if isinstance(value, Mapping):
        members = ",".join(
            f"{_stable_value(key)}:{_stable_value(item)}"
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        )
        return f"{{{members}}}"
    if isinstance(value, tuple | list | frozenset | set):
        return f"[{','.join(sorted(_stable_value(item) for item in value))}]"
    if value is None:
        return "null"
    if isinstance(value, str | int | float | bool):
        return str(value)
    return f"<{type(value).__module__}.{type(value).__qualname__}>"


def _manifest_candidate(field_name: str, value: object, detail: str) -> ProviderSource:
    return ProviderSource(
        kind=SourceKind.CONNECTOR_MANIFEST,
        role=SourceRole.CANDIDATE,
        rule_id=f"manifest.{field_name}.candidate.v1",
        ref=f"Connector.{field_name}={_stable_value(value)}",
        detail=detail,
    )


def _teardown_slugs(connector: Any) -> frozenset[str]:
    slugs = set(getattr(connector, "discovery_keys", ()) or ())
    slugs.update(getattr(connector, "compiler_protocols", ()) or ())
    slugs.add(connector.name)
    return frozenset(str(slug).lower() for slug in slugs)


def _field_protocols(connector: Any, field_name: str, value: object) -> frozenset[str]:
    if field_name == "supported_chains":
        return frozenset(_published_protocols(connector))
    if field_name == "compiler":
        return frozenset(connector.compiler_keys)
    if field_name == "receipt_parser_connector":
        return frozenset(connector.receipt_parser_keys)
    if field_name == "teardown_post_condition":
        return _teardown_slugs(connector)
    if field_name == "fungible_lp_close":
        return frozenset(getattr(value, "protocols", None) or (connector.name,))
    if field_name in ("lending_read", "perps_read", "position_read", "perp_price_history"):
        return frozenset((connector.name, *getattr(value, "aliases", ())))
    if field_name == "dex_volume":
        return frozenset((getattr(value, "name", None) or connector.name, *getattr(value, "aliases", ())))
    return frozenset((connector.name,))


_FIELD_PRIMITIVE_SCOPES: dict[str, frozenset[Primitive]] = {
    "swap_quote_connector": frozenset((Primitive.SWAP,)),
    "lending_read": frozenset((Primitive.LENDING,)),
    "perps_read": frozenset((Primitive.PERP,)),
    "perp_identity": frozenset((Primitive.PERP,)),
    "perp_price_history": frozenset((Primitive.PERP,)),
    "prediction_read": frozenset((Primitive.PREDICTION,)),
    "position_read": frozenset((Primitive.LP, Primitive.LP_V4, Primitive.VAULT)),
    "fungible_lp_close": frozenset((Primitive.LP, Primitive.LP_V4)),
    "teardown_residual_discovery": frozenset((Primitive.PERP,)),
    "dex_volume": frozenset((Primitive.SWAP, Primitive.LP, Primitive.LP_V4)),
    "pool_reader": frozenset((Primitive.SWAP, Primitive.LP, Primitive.LP_V4)),
}


def _field_applies(
    connector: Any,
    *,
    protocol: str,
    chain: str,
    intent: IntentType,
    field_name: str,
    value: object,
) -> bool:
    if protocol not in _field_protocols(connector, field_name, value):
        return False
    primitive_scope = _FIELD_PRIMITIVE_SCOPES.get(field_name)
    if primitive_scope is not None and primitive_for(intent.value, protocol) not in primitive_scope:
        return False
    chains = getattr(value, "chains", None)
    return not (field_name in ("dex_volume", "perp_price_history") and chains and chain not in chains)


def _field_candidate(
    connector: Any,
    key: CapabilityCellKey,
    field_name: str,
    detail: str,
) -> ProviderSource | None:
    value = getattr(connector, field_name, None)
    if value is None or value is False or value == ():
        return None
    if not _field_applies(
        connector,
        protocol=key.protocol,
        chain=key.chain,
        intent=key.intent,
        field_name=field_name,
        value=value,
    ):
        return None
    return _manifest_candidate(field_name, value, detail)


_FIELD_CANDIDATE_RULES: dict[ObligationId, tuple[tuple[str, str], ...]] = {
    ObligationId.VENUE_RESOLUTION: (
        ("supported_chains", "Exact protocol, intent, and chain routing candidate; not execution evidence."),
        ("compiler", "Compiler routing candidate; provider presence is not venue-binding proof."),
    ),
    ObligationId.COMPILER: (("compiler", "Intent compiler candidate scoped by this exact manifest cell."),),
    ObligationId.RECEIPT_EVIDENCE: (
        ("receipt_parser_connector", "Receipt parser candidate; intent-specific extraction is not implied."),
    ),
    ObligationId.MONEY_LEGS: (
        ("receipt_parser_connector", "Parser candidate only; typed money-leg emission requires separate evidence."),
        ("accounting_treatment", "Connector accounting treatment candidate; not durable money-leg evidence."),
    ),
    ObligationId.OPEN_EXECUTION: (
        ("compiler", "Matching intent compiler candidate; landed execution evidence is still required."),
    ),
    ObligationId.CLOSE_EXECUTION: (
        ("compiler", "Matching intent compiler candidate; landed execution evidence is still required."),
    ),
    ObligationId.POSITION_IDENTITY: (
        ("primitive", "Primitive identity candidate; registry identity compatibility remains unproved."),
        ("perp_identity", "Perp venue alias-token identity candidate."),
        ("fungible_lp_close", "Fungible-LP close identity candidate."),
    ),
    ObligationId.INVENTORY_ADAPTER: tuple(
        (field, "Read/inventory candidate; exact cell completeness and evidence remain required.")
        for field in (
            "position_read",
            "lending_read",
            "perps_read",
            "prediction_read",
            "teardown_residual_discovery",
        )
    ),
    ObligationId.POSITION_READER: tuple(
        (field, "Read/inventory candidate; exact cell completeness and evidence remain required.")
        for field in ("position_read", "lending_read", "perps_read", "prediction_read")
    ),
    ObligationId.POSITION_ENUMERATION: tuple(
        (field, "Read/inventory candidate; exact cell completeness and evidence remain required.")
        for field in (
            "position_read",
            "lending_read",
            "perps_read",
            "prediction_read",
            "teardown_residual_discovery",
        )
    ),
    ObligationId.ANVIL_FORK_READ: tuple(
        (field, "Read/inventory candidate; exact cell completeness and evidence remain required.")
        for field in ("position_read", "lending_read", "perps_read", "prediction_read")
    ),
    ObligationId.POSITION_OBSERVER: tuple(
        (field, "Reader candidate; observer composition and block anchoring are not implied.")
        for field in ("position_read", "lending_read", "perps_read", "prediction_read")
    ),
    ObligationId.CLOSE_PLANNER: (
        ("compiler", "Close compiler candidate; strategy-level planning and bounded sizing are not implied."),
        ("fungible_lp_close", "Owned-liquidity close-clamp candidate; not a complete close planner."),
    ),
    ObligationId.BLOCK_ANCHORED_OBSERVATION: (
        ("teardown_post_condition", "Hook may accept a block anchor; signature presence is not pinning proof."),
    ),
    ObligationId.ACCOUNTING_DURABILITY: (
        ("accounting_treatment", "Accounting treatment candidate; persistence durability is not implied."),
        ("primitive", "Primitive accounting candidate; persistence durability is not implied."),
    ),
    ObligationId.VALUATION_POLICY: tuple(
        (field, "Valuation input candidate; measured pricing policy is not implied.")
        for field in ("position_read", "lending_read", "perps_read", "prediction_read")
    ),
    ObligationId.EXACT_VENUE_BINDING: tuple(
        (field, "Venue-binding candidate; address/routing presence is not exact-target proof.")
        for field in ("address_tables", "pool_reader", "compiler")
    ),
    ObligationId.ANVIL_GAS: (("gas_estimate_connector", "Gas-estimate candidate; funded native gas is not implied."),),
    ObligationId.ANVIL_QUOTE: (("swap_quote_connector", "Quote candidate; managed-fork availability is not implied."),),
    ObligationId.ANVIL_LIFECYCLE_EVIDENCE: (
        ("teardown_post_condition", "Closure candidate; no managed-Anvil lifecycle artifact is implied."),
    ),
}


def _amount_protection_candidates(connector: Any, key: CapabilityCellKey) -> tuple[ProviderSource, ...]:
    quote = _field_candidate(
        connector,
        key,
        "swap_quote_connector",
        "Quote provider candidate; exact venue binding and amount protection remain unproved.",
    )
    if quote is None:
        return ()
    return (
        ProviderSource(
            kind=SourceKind.APPROVED_ALTERNATIVE,
            role=SourceRole.CANDIDATE,
            rule_id="alternative.quote_amount_protection.candidate.v1",
            ref=quote.ref,
            detail=quote.detail,
        ),
    )


def _certification_candidates(connector: Any, key: CapabilityCellKey) -> tuple[ProviderSource, ...]:
    candidates: list[ProviderSource] = []
    hook = _field_candidate(
        connector,
        key,
        "teardown_post_condition",
        "Four-valued closure hook candidate; presence alone never certifies closure.",
    )
    if hook is not None:
        candidates.append(hook)
    if getattr(connector, "fungible_lp", False) and key.protocol in _teardown_slugs(connector):
        candidates.append(
            ProviderSource(
                kind=SourceKind.FRAMEWORK_DEFAULT,
                role=SourceRole.CANDIDATE,
                rule_id="framework.fungible_lp_post_condition.candidate.v1",
                ref="framework:fungible_lp_post_condition",
                detail="Applicable framework-default candidate; chain/position evidence is still required.",
            )
        )
    if getattr(getattr(connector, "kind", None), "value", None) == "vault" and key.protocol in _teardown_slugs(
        connector
    ):
        candidates.append(
            ProviderSource(
                kind=SourceKind.FRAMEWORK_DEFAULT,
                role=SourceRole.CANDIDATE,
                rule_id="framework.erc4626_post_condition.candidate.v1",
                ref="framework:erc4626_post_condition",
                detail="Vault-kind framework-default candidate; exact deployment evidence is still required.",
            )
        )
    return tuple(candidates)


_TARGET_DATA_FIELDS: dict[ExactTargetFeature, tuple[str, ...]] = {
    ExactTargetFeature.QUOTE: ("swap_quote_connector",),
    ExactTargetFeature.TWAP: ("pool_reader",),
    ExactTargetFeature.OHLCV: ("dex_volume",),
    ExactTargetFeature.DEPTH: ("pool_reader",),
    ExactTargetFeature.REFERENCE_PRICE: ("perp_price_history", "principal_token_market_reader"),
}


def _target_data_candidates(
    connector: Any,
    key: CapabilityCellKey,
    feature: ExactTargetFeature | None,
) -> tuple[ProviderSource, ...]:
    if feature is None:
        return ()
    candidates = (
        _field_candidate(
            connector,
            key,
            field_name,
            "Feature provider candidate; exact venue/chain evidence remains required.",
        )
        for field_name in _TARGET_DATA_FIELDS.get(feature, ())
    )
    return tuple(candidate for candidate in candidates if candidate is not None)


def _candidate_sources(
    connector: Any,
    key: CapabilityCellKey,
    obligation: ObligationId,
) -> tuple[ProviderSource, ...]:
    """Return conservative provider signals; none is declaration evidence."""
    candidates: list[ProviderSource] = []
    for field_name, detail in _FIELD_CANDIDATE_RULES.get(obligation, ()):
        candidate = _field_candidate(connector, key, field_name, detail)
        if candidate is not None:
            candidates.append(candidate)
    if obligation is ObligationId.AMOUNT_PROTECTION:
        candidates.extend(_amount_protection_candidates(connector, key))
    elif obligation is ObligationId.CERTIFICATION_PATH:
        candidates.extend(_certification_candidates(connector, key))
    elif obligation is ObligationId.TARGET_DATA_PROVIDER:
        candidates.extend(_target_data_candidates(connector, key, key.exact_target_feature))

    return tuple(sorted(set(candidates), key=lambda item: (item.kind.value, item.rule_id, item.ref, item.detail)))


def _claim_specs(
    connector: Any,
    *,
    protocol: str,
    chain: str,
    intent: IntentType,
    semantics: IntentSemantics,
) -> tuple[tuple[SupportClaim, ExactTargetFeature | None], ...]:
    claims: list[tuple[SupportClaim, ExactTargetFeature | None]] = [
        (SupportClaim.CORE_EXECUTION, None),
    ]
    if semantics is IntentSemantics.POSITION_OPEN_OR_INCREASE:
        claims.append((SupportClaim.POSITION_OPEN, None))
    elif semantics is IntentSemantics.POSITION_DECREASE_OR_CLOSE:
        claims.append((SupportClaim.POSITION_CLOSE, None))
    if semantics in (
        IntentSemantics.POSITION_OPEN_OR_INCREASE,
        IntentSemantics.POSITION_DECREASE_OR_CLOSE,
        IntentSemantics.POSITION_MAINTENANCE,
    ):
        claims.extend(
            (
                (SupportClaim.FULL_LIFECYCLE_CERTIFICATION, None),
                (SupportClaim.VALUATION_READY, None),
            )
        )
    for feature, field_names in _TARGET_DATA_FIELDS.items():
        if feature is ExactTargetFeature.QUOTE and intent is not IntentType.SWAP:
            continue
        if any(
            (value := getattr(connector, field_name, None)) is not None
            and _field_applies(
                connector,
                protocol=protocol,
                chain=chain,
                intent=intent,
                field_name=field_name,
                value=value,
            )
            for field_name in field_names
        ):
            claims.append((SupportClaim.EXACT_TARGET_DATA, feature))
    claims.append((SupportClaim.MANAGED_ANVIL_TESTABLE, None))
    return tuple(sorted(claims, key=lambda item: list(SupportClaim).index(item[0])))


def _published_protocols(connector: Any) -> tuple[str, ...]:
    """Return executable connector-owned keys, never display-only matrix names."""
    return tuple(sorted(getattr(connector, "protocol_keys", (connector.name,))))


def _requirement_source(profile: ObligationProfile, obligation: ObligationId) -> ProviderSource:
    return ProviderSource(
        kind=SourceKind.CORE_PROFILE,
        role=SourceRole.REQUIREMENT,
        rule_id="core.profile_requirement.v1",
        ref=f"core-profile:{profile.key.claim.value}:{obligation.value}@v{profile.policy_version}",
        detail="Required by the canonical core obligation profile.",
    )


def _build_cell(
    connector: Any,
    key: CapabilityCellKey,
    declaration_resolver: DeclarationResolver | None,
) -> EffectiveCapabilityCell:
    profile = profile_for(key.profile_key)
    resolved = tuple(declaration_resolver(connector, key, profile)) if declaration_resolver else ()
    if not all(type(item) is ResolvedDeclaration for item in resolved):
        raise TypeError("declaration_resolver must return ResolvedDeclaration values")
    by_obligation: dict[ObligationId, ResolvedDeclaration] = {}
    for item in resolved:
        obligation = item.declaration.obligation
        if obligation in by_obligation:
            raise ValueError(f"conflicting resolved declarations for {obligation.value}")
        by_obligation[obligation] = item
    audit = audit_profile(profile, tuple(item.declaration for item in resolved))
    if audit.unexpected:
        unexpected = ", ".join(item.value for item in audit.unexpected)
        raise ValueError(f"resolved declarations are outside the canonical profile: {unexpected}")
    effective: list[EffectiveObligation] = []
    for audited in audit.obligations:
        resolved_item = by_obligation.get(audited.obligation)
        sources = (
            _requirement_source(profile, audited.obligation),
            *_candidate_sources(connector, key, audited.obligation),
            *((resolved_item.source,) if resolved_item is not None else ()),
        )
        effective.append(EffectiveObligation(audited=audited, sources=sources))
    return EffectiveCapabilityCell(key=key, obligations=tuple(effective), policy_version=profile.policy_version)


def _connector_by_protocol(connectors: tuple[Any, ...]) -> dict[str, Any]:
    by_protocol: dict[str, Any] = {}
    for connector in connectors:
        for protocol in _published_protocols(connector):
            if protocol in by_protocol and by_protocol[protocol] is not connector:
                raise ValueError(f"multiple connectors own injected protocol {protocol!r}")
            by_protocol[protocol] = connector
    return by_protocol


def _build_injected_cells(
    connectors: tuple[Any, ...],
    claim_cells: Iterable[CapabilityCellKey],
    declaration_resolver: DeclarationResolver | None,
) -> list[EffectiveCapabilityCell]:
    by_protocol = _connector_by_protocol(connectors)
    cells: list[EffectiveCapabilityCell] = []
    for key in claim_cells:
        if type(key) is not CapabilityCellKey:
            raise TypeError("claim_cells must contain CapabilityCellKey values")
        connector = by_protocol.get(key.protocol)
        if connector is None:
            raise ValueError(f"no connector owns injected protocol {key.protocol!r}")
        supported = tuple(connector.supported_chains_for(protocol=key.protocol, intent=key.intent) or ())
        if key.chain not in supported:
            raise ValueError(
                f"injected cell is outside manifest support: {key.protocol}/{key.chain}/{key.intent.value}"
            )
        record_for(key.intent.value)
        expected_primitive = primitive_for(key.intent.value, key.protocol)
        if key.primitive is not expected_primitive:
            raise ValueError(
                f"injected cell primitive must be {expected_primitive.value} for {key.protocol}/{key.intent.value}"
            )
        cells.append(_build_cell(connector, key, declaration_resolver))
    return cells


def _build_registered_cells(
    connectors: tuple[Any, ...],
    declaration_resolver: DeclarationResolver | None,
) -> list[EffectiveCapabilityCell]:
    cells: list[EffectiveCapabilityCell] = []
    for connector in connectors:
        intents = tuple(getattr(connector, "strategy_intents", None) or ())
        for protocol in _published_protocols(connector):
            for intent in intents:
                if type(intent) is not IntentType:
                    raise TypeError("connector strategy_intents must contain IntentType values")
                semantics = semantics_for_intent(intent)
                if semantics in (IntentSemantics.NO_OPERATION, IntentSemantics.UNAVAILABLE):
                    continue
                chains = tuple(connector.supported_chains_for(protocol=protocol, intent=intent) or ())
                if not chains:
                    continue
                record_for(intent.value)  # Fail loud before primitive_for's compatibility fallback.
                primitive = primitive_for(intent.value, protocol)
                for chain in chains:
                    for claim, feature in _claim_specs(
                        connector,
                        protocol=protocol,
                        chain=chain,
                        intent=intent,
                        semantics=semantics,
                    ):
                        key = CapabilityCellKey(
                            protocol=protocol,
                            chain=chain,
                            intent=intent,
                            semantics=semantics,
                            primitive=primitive,
                            claim=claim,
                            exact_target_feature=feature,
                        )
                        cells.append(_build_cell(connector, key, declaration_resolver))
    return cells


def build_effective_capability_matrix(
    connectors: Iterable[Any] | None = None,
    *,
    claim_cells: Iterable[CapabilityCellKey] | None = None,
    declaration_resolver: DeclarationResolver | None = None,
    universe_source_ref: str | None = None,
) -> EffectiveCapabilityMatrix:
    """Build exact on-chain cells without importing or executing provider refs.

    With ``claim_cells=None`` the universe is explicitly the wider registered
    SDK strategy surface.  A production or release inventory must be injected
    as exact claim cells with its own stable ``universe_source_ref``; connector
    manifests alone do not identify the Platform production subset.
    """
    using_default_connectors = connectors is None
    if connectors is None:
        from almanak.connectors._connector import CONNECTOR_DESCRIPTOR_REGISTRY  # noqa: PLC0415

        connectors = CONNECTOR_DESCRIPTOR_REGISTRY.with_strategy_support()
    connector_tuple = tuple(connectors)
    canonical_index: dict[CapabilityCellKey, tuple[ResolvedDeclaration, ...]] | None = None
    required_match_keys: set[CapabilityCellKey] = set()
    effective_resolver: DeclarationResolver
    if declaration_resolver is None:
        connector_rules = _connector_declaration_rules(connector_tuple)
        rules = (*EFFECTIVE_DECLARATIONS, *connector_rules)
        canonical_index = _index_declaration_rules(rules)
        required_match_keys.update(rule.key for rule in connector_rules)
        if using_default_connectors:
            required_match_keys.update(rule.key for rule in EFFECTIVE_DECLARATIONS)

        def canonical_resolver(
            _connector: Any,
            key: CapabilityCellKey,
            _profile: ObligationProfile,
        ) -> tuple[ResolvedDeclaration, ...]:
            assert canonical_index is not None
            return canonical_index.get(key, ())

        effective_resolver = canonical_resolver

    else:
        effective_resolver = declaration_resolver
    if claim_cells is not None:
        if not universe_source_ref:
            raise ValueError("universe_source_ref is required with injected claim_cells")
        cells = _build_injected_cells(connector_tuple, claim_cells, effective_resolver)
        universe = MatrixUniverse(UniverseKind.INJECTED_CLAIM_CELLS, universe_source_ref)
    else:
        cells = _build_registered_cells(connector_tuple, effective_resolver)
        universe = MatrixUniverse(
            UniverseKind.REGISTERED_STRATEGY_SUPPORT,
            universe_source_ref or "almanak.connectors._connector:CONNECTOR_DESCRIPTOR_REGISTRY",
        )
        if canonical_index is not None:
            generated_keys = {cell.key for cell in cells}
            unmatched = sorted(required_match_keys - generated_keys, key=CapabilityCellKey.sort_key)
            if unmatched:
                rendered = ", ".join("/".join(str(part) for part in key.sort_key()) for key in unmatched)
                raise ValueError(f"canonical lifecycle declarations have unmatched cell keys: {rendered}")
    cells.sort(key=lambda item: item.key.sort_key())
    return EffectiveCapabilityMatrix(cells=tuple(cells), universe=universe)


def _markdown_cell(value: object) -> str:
    return str(value).replace("\\", "\\\\").replace("|", "\\|").replace("\r\n", "<br>").replace("\n", "<br>")


def render_markdown(matrix: EffectiveCapabilityMatrix) -> str:
    """Render a stable, human-readable summary suitable for review diffs."""
    lines = [
        "# Effective lifecycle capability matrix",
        "",
        f"Schema: v{matrix.schema_version} · Obligation policy: v{OBLIGATION_POLICY_VERSION}",
        f"Universe: {_markdown_cell(matrix.universe.kind.value)} · Source: {_markdown_cell(matrix.universe.source_ref)}",
        "",
        "| Protocol | Chain | Intent | Primitive | Claim | Satisfied | N/A | Unsupported | Undeclared | Candidates |",
        "|---|---|---|---|---|---:|---:|---:|---:|---:|",
    ]
    for cell in matrix.cells:
        counts = cell.counts_by_state()
        candidate_count = sum(
            source.role is SourceRole.CANDIDATE for obligation in cell.obligations for source in obligation.sources
        )
        claim = cell.key.claim.value
        if cell.key.exact_target_feature is not None:
            claim = f"{claim}:{cell.key.exact_target_feature.value}"
        lines.append(
            f"| {_markdown_cell(cell.key.protocol)} | {_markdown_cell(cell.key.chain)} | "
            f"{_markdown_cell(cell.key.intent.value)} | {_markdown_cell(cell.key.primitive.value)} | "
            f"{_markdown_cell(claim)} | {counts[ReportedObligationState.SATISFIED]} | "
            f"{counts[ReportedObligationState.NOT_APPLICABLE]} | "
            f"{counts[ReportedObligationState.UNSUPPORTED]} | "
            f"{counts[ReportedObligationState.UNDECLARED]} | {candidate_count} |"
        )
    totals = matrix.counts_by_state()
    lines.extend(
        (
            "",
            f"Cells: {len(matrix.cells)} · Obligations: {sum(totals.values())} · "
            f"satisfied: {totals[ReportedObligationState.SATISFIED]} · "
            f"not applicable: {totals[ReportedObligationState.NOT_APPLICABLE]} · "
            f"unsupported: {totals[ReportedObligationState.UNSUPPORTED]} · "
            f"undeclared: {totals[ReportedObligationState.UNDECLARED]}",
            "",
            "Provider candidates are provenance signals only. They never satisfy an obligation without an explicit, "
            "versioned declaration and durable evidence.",
            "",
            "## Obligation provenance",
            "",
            "| Protocol | Chain | Intent | Claim | Obligation | State | Disposition | Sources |",
            "|---|---|---|---|---|---|---|---|",
        )
    )
    for cell in matrix.cells:
        claim = cell.key.claim.value
        if cell.key.exact_target_feature is not None:
            claim = f"{claim}:{cell.key.exact_target_feature.value}"
        for obligation in cell.obligations:
            sources = "; ".join(
                f"{source.role.value}:{source.kind.value}:{source.rule_id}:{source.ref}"
                for source in obligation.sources
            )
            lines.append(
                f"| {_markdown_cell(cell.key.protocol)} | {_markdown_cell(cell.key.chain)} | "
                f"{_markdown_cell(cell.key.intent.value)} | {_markdown_cell(claim)} | "
                f"{_markdown_cell(obligation.audited.obligation.value)} | {_markdown_cell(obligation.state.value)} | "
                f"{_markdown_cell(_disposition_summary(obligation.audited))} | {_markdown_cell(sources)} |"
            )
    lines.append("")
    return "\n".join(lines)


def _disposition_summary(audited: AuditedObligation) -> str:
    disposition = audited.disposition
    if disposition is None:
        return "missing"
    if isinstance(disposition, Satisfied):
        evidence = ",".join(item.ref for item in disposition.test_evidence)
        return f"{disposition.provider_ref}@{disposition.contract_version} evidence={evidence}"
    if isinstance(disposition, NotApplicable):
        return f"rule={disposition.rule_id.value}"
    if isinstance(disposition, Unsupported):
        return (
            f"owner={disposition.owner} tracking={disposition.tracking_ref} "
            f"review_by={disposition.review_by.isoformat()} reason={disposition.reason}"
        )
    raise AssertionError(f"unhandled disposition {disposition!r}")


def _main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate the internal effective lifecycle capability matrix")
    parser.add_argument("--format", choices=("json", "markdown"), default="json")
    args = parser.parse_args(argv)
    matrix = build_effective_capability_matrix()
    print(matrix.to_json() if args.format == "json" else render_markdown(matrix), end="")
    return 0


if __name__ == "__main__":  # pragma: no cover - exercised through the pure builder/renderer
    raise SystemExit(_main())
