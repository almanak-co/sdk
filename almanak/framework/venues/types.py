"""Neutral, writer-safe exact-venue identity and verification values.

The values in this module contain no connector discovery and perform no I/O.
Protocol-specific gateway verifiers produce :class:`VerifiedVenueBinding` or a
closed :class:`VenueBindingFailure`; compilers never receive an unverified
``ExactVenueBinding`` through the public construction API.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from almanak.core.asset_identity import AssetIdentity
from almanak.core.chains import ChainRegistry
from almanak.core.enums import ChainFamily
from almanak.framework.primitives.types import Primitive

VENUE_BINDING_SCHEMA_VERSION = 1

_ASCII_RE = re.compile(r"^[\x20-\x7e]+$")
_COMPONENT_NAME_RE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
_PROTOCOL_RE = re.compile(r"^[a-z][a-z0-9_]*$")
_VERSION_RE = re.compile(r"^[a-z0-9][a-z0-9._-]*$")
_EVM_ADDRESS_RE = re.compile(r"^0x[0-9a-f]{40}$")
_EVM_BYTES32_RE = re.compile(r"^0x[0-9a-f]{64}$")
_BLOCK_HASH_RE = re.compile(r"^0x[0-9a-f]{64}$")
_IMPORT_REF_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*:[A-Za-z_][A-Za-z0-9_]*$")
_SOLANA_ADDRESS_RE = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")
_BASE58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
_BASE58_INDEX = {character: index for index, character in enumerate(_BASE58_ALPHABET)}


def _base58_decoded_length(value: str) -> int:
    number = 0
    for character in value:
        number = number * 58 + _BASE58_INDEX[character]
    significant_bytes = (number.bit_length() + 7) // 8
    return len(value) - len(value.lstrip("1")) + significant_bytes


def _require_exact_str(value: object, field_name: str) -> str:
    if type(value) is not str:
        raise TypeError(f"{field_name} must be a string, got {type(value).__name__}")
    if not value or value != value.strip() or not _ASCII_RE.fullmatch(value):
        raise ValueError(f"{field_name} must be non-empty, trimmed printable ASCII")
    return value


def _canonical_chain(value: object, field_name: str) -> str:
    chain = _require_exact_str(value, field_name)
    descriptor = ChainRegistry.resolve(chain)
    if chain != descriptor.name:
        raise ValueError(f"{field_name} must use canonical chain name {descriptor.name!r}, got {chain!r}")
    return chain


def _canonical_protocol(value: object, field_name: str) -> str:
    protocol = _require_exact_str(value, field_name)
    if not _PROTOCOL_RE.fullmatch(protocol):
        raise ValueError(f"{field_name} must be a canonical lowercase underscore protocol key")
    return protocol


class VenueTargetRole(StrEnum):
    """Closed roles for physical venue identity and operational endpoints."""

    POOL = "pool"
    MARKET = "market"
    VAULT = "vault"
    PROGRAM = "program"
    FACTORY = "factory"
    ROUTER = "router"
    POSITION_MANAGER = "position_manager"
    PERMISSION_TARGET = "permission_target"


class VenueReferenceNamespace(StrEnum):
    """Closed reference encodings accepted by the version-1 venue contract."""

    EVM_ADDRESS = "evm_address"
    EVM_BYTES32 = "evm_bytes32"
    SOLANA_ADDRESS = "solana_address"


class VenueBindingFailureState(StrEnum):
    """Closed outcome classes that cannot be confused with verified success."""

    MISMATCHED = "mismatched"
    UNAVAILABLE = "unavailable"
    UNSUPPORTED = "unsupported"


class VenueBindingFailureReason(StrEnum):
    """Stable machine-readable reasons for exact-venue verification refusal."""

    TARGET_MISMATCH = "target_mismatch"
    FACTORY_MISMATCH = "factory_mismatch"
    ASSET_MISMATCH = "asset_mismatch"
    COMPONENT_MISMATCH = "component_mismatch"
    GATEWAY_UNAVAILABLE = "gateway_unavailable"
    BLOCK_UNAVAILABLE = "block_unavailable"
    STALE_EVIDENCE = "stale_evidence"
    UNSUPPORTED_CHAIN = "unsupported_chain"
    UNSUPPORTED_PROTOCOL = "unsupported_protocol"
    UNSUPPORTED_PRIMITIVE = "unsupported_primitive"


_IDENTITY_ROLES = frozenset(
    {VenueTargetRole.POOL, VenueTargetRole.MARKET, VenueTargetRole.VAULT, VenueTargetRole.PROGRAM}
)
_OPERATIONAL_ROLES = frozenset(VenueTargetRole) - _IDENTITY_ROLES
_REASONS_BY_STATE = {
    VenueBindingFailureState.MISMATCHED: frozenset(
        {
            VenueBindingFailureReason.TARGET_MISMATCH,
            VenueBindingFailureReason.FACTORY_MISMATCH,
            VenueBindingFailureReason.ASSET_MISMATCH,
            VenueBindingFailureReason.COMPONENT_MISMATCH,
        }
    ),
    VenueBindingFailureState.UNAVAILABLE: frozenset(
        {
            VenueBindingFailureReason.GATEWAY_UNAVAILABLE,
            VenueBindingFailureReason.BLOCK_UNAVAILABLE,
            VenueBindingFailureReason.STALE_EVIDENCE,
        }
    ),
    VenueBindingFailureState.UNSUPPORTED: frozenset(
        {
            VenueBindingFailureReason.UNSUPPORTED_CHAIN,
            VenueBindingFailureReason.UNSUPPORTED_PROTOCOL,
            VenueBindingFailureReason.UNSUPPORTED_PRIMITIVE,
        }
    ),
}


@dataclass(frozen=True, slots=True)
class VenueTargetRef:
    """One canonical physical-venue or operational contract reference."""

    role: VenueTargetRole
    reference_namespace: VenueReferenceNamespace
    reference: str

    def __post_init__(self) -> None:
        if type(self.role) is not VenueTargetRole:
            raise TypeError("VenueTargetRef.role must be a VenueTargetRole")
        if type(self.reference_namespace) is not VenueReferenceNamespace:
            raise TypeError("VenueTargetRef.reference_namespace must be a VenueReferenceNamespace")
        reference = _require_exact_str(self.reference, "VenueTargetRef.reference")
        if self.reference_namespace is VenueReferenceNamespace.EVM_ADDRESS:
            valid = _EVM_ADDRESS_RE.fullmatch(reference) is not None
        elif self.reference_namespace is VenueReferenceNamespace.EVM_BYTES32:
            valid = _EVM_BYTES32_RE.fullmatch(reference) is not None
        else:
            valid = _SOLANA_ADDRESS_RE.fullmatch(reference) is not None and _base58_decoded_length(reference) == 32
        if not valid:
            raise ValueError(
                f"VenueTargetRef.reference is not canonical {self.reference_namespace.value}: {reference!r}"
            )

    @property
    def sort_key(self) -> tuple[str, str, str]:
        return (self.role.value, self.reference_namespace.value, self.reference)

    def to_wire(self) -> dict[str, str]:
        return {
            "reference": self.reference,
            "referenceNamespace": self.reference_namespace.value,
            "role": self.role.value,
        }

    def validate_chain(self, chain: str) -> None:
        """Reject a reference encoding from a different execution family."""
        family = ChainRegistry.get(chain).family
        if family is ChainFamily.EVM:
            valid = self.reference_namespace in {
                VenueReferenceNamespace.EVM_ADDRESS,
                VenueReferenceNamespace.EVM_BYTES32,
            }
        elif family is ChainFamily.SOLANA:
            valid = self.reference_namespace is VenueReferenceNamespace.SOLANA_ADDRESS
        else:  # pragma: no cover - a new family must extend this closed contract
            valid = False
        if not valid:
            raise ValueError(f"{self.reference_namespace.value!r} venue reference is invalid on {chain!r}")


@dataclass(frozen=True, slots=True)
class VenueBindingComponent:
    """One canonical connector-owned venue-equality component."""

    name: str
    value: str

    def __post_init__(self) -> None:
        name = _require_exact_str(self.name, "VenueBindingComponent.name")
        _require_exact_str(self.value, "VenueBindingComponent.value")
        if not _COMPONENT_NAME_RE.fullmatch(name):
            raise ValueError("VenueBindingComponent.name must be canonical lowercase snake_case")

    @property
    def sort_key(self) -> tuple[str, str]:
        return (self.name, self.value)

    def to_wire(self) -> dict[str, str]:
        return {"name": self.name, "value": self.value}


@dataclass(frozen=True, slots=True)
class VenueObservedFact:
    """One immutable fact observed by a connector verifier at a block."""

    name: str
    value: str
    target_ref: VenueTargetRef | None = None

    def __post_init__(self) -> None:
        name = _require_exact_str(self.name, "VenueObservedFact.name")
        _require_exact_str(self.value, "VenueObservedFact.value")
        if not _COMPONENT_NAME_RE.fullmatch(name):
            raise ValueError("VenueObservedFact.name must be canonical lowercase snake_case")
        if self.target_ref is not None and type(self.target_ref) is not VenueTargetRef:
            raise TypeError("VenueObservedFact.target_ref must be None or VenueTargetRef")

    @property
    def sort_key(self) -> tuple[str, str, tuple[str, str, str]]:
        target_key = ("", "", "") if self.target_ref is None else self.target_ref.sort_key
        return (self.name, self.value, target_key)

    def to_wire(self) -> dict[str, object]:
        return {
            "name": self.name,
            "value": self.value,
            "targetRef": None if self.target_ref is None else self.target_ref.to_wire(),
        }


def _validate_exact_tuple(value: object, item_type: type, field_name: str, *, allow_empty: bool) -> tuple:
    if type(value) is not tuple:
        raise TypeError(f"{field_name} must be a tuple, got {type(value).__name__}")
    if not allow_empty and not value:
        raise ValueError(f"{field_name} must be non-empty")
    if any(type(item) is not item_type for item in value):
        raise TypeError(f"{field_name} must contain only exact {item_type.__name__} values")
    return value


def _require_canonical_order(value: tuple, field_name: str) -> None:
    expected = tuple(sorted(value, key=lambda item: item.sort_key))
    if value != expected:
        raise ValueError(f"{field_name} must be canonically ordered")
    if len(set(value)) != len(value):
        raise ValueError(f"{field_name} contains duplicate values")


@dataclass(frozen=True, slots=True, init=False)
class ExactVenueBinding:
    """Stable physical venue identity created only with verified evidence."""

    chain: str
    protocol: str
    primitive: Primitive
    identity_refs: tuple[VenueTargetRef, ...]
    binding_components: tuple[VenueBindingComponent, ...]
    ordered_assets: tuple[AssetIdentity, ...]
    binding_policy_version: int
    binding_hash: str

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        raise TypeError("ExactVenueBinding is created only by build_verified_venue_binding()")

    @classmethod
    def _verified(
        cls,
        *,
        chain: str,
        protocol: str,
        primitive: Primitive,
        identity_refs: tuple[VenueTargetRef, ...],
        binding_components: tuple[VenueBindingComponent, ...],
        ordered_assets: tuple[AssetIdentity, ...],
        binding_policy_version: int,
    ) -> ExactVenueBinding:
        canonical_chain = _canonical_chain(chain, "ExactVenueBinding.chain")
        canonical_protocol = _canonical_protocol(protocol, "ExactVenueBinding.protocol")
        if type(primitive) is not Primitive:
            raise TypeError("ExactVenueBinding.primitive must be a Primitive")
        refs = _validate_exact_tuple(
            identity_refs, VenueTargetRef, "ExactVenueBinding.identity_refs", allow_empty=False
        )
        if any(ref.role not in _IDENTITY_ROLES for ref in refs):
            raise ValueError("ExactVenueBinding.identity_refs may contain only physical venue roles")
        for ref in refs:
            ref.validate_chain(canonical_chain)
        _require_canonical_order(refs, "ExactVenueBinding.identity_refs")
        components = _validate_exact_tuple(
            binding_components,
            VenueBindingComponent,
            "ExactVenueBinding.binding_components",
            allow_empty=True,
        )
        _require_canonical_order(components, "ExactVenueBinding.binding_components")
        if len({component.name for component in components}) != len(components):
            raise ValueError("ExactVenueBinding.binding_components contains duplicate names")
        assets = _validate_exact_tuple(
            ordered_assets, AssetIdentity, "ExactVenueBinding.ordered_assets", allow_empty=False
        )
        if any(asset.chain != canonical_chain for asset in assets):
            raise ValueError("ExactVenueBinding.ordered_assets contains a cross-chain asset")
        if len(set(assets)) != len(assets):
            raise ValueError("ExactVenueBinding.ordered_assets contains duplicate assets")
        if type(binding_policy_version) is not int or binding_policy_version <= 0:
            raise ValueError("ExactVenueBinding.binding_policy_version must be a positive integer")

        instance = object.__new__(cls)
        object.__setattr__(instance, "chain", canonical_chain)
        object.__setattr__(instance, "protocol", canonical_protocol)
        object.__setattr__(instance, "primitive", primitive)
        object.__setattr__(instance, "identity_refs", refs)
        object.__setattr__(instance, "binding_components", components)
        object.__setattr__(instance, "ordered_assets", assets)
        object.__setattr__(instance, "binding_policy_version", binding_policy_version)
        object.__setattr__(instance, "binding_hash", hashlib.sha256(instance.canonical_preimage_bytes()).hexdigest())
        return instance

    def to_preimage_wire(self) -> dict[str, object]:
        return {
            "schemaVersion": VENUE_BINDING_SCHEMA_VERSION,
            "chain": self.chain,
            "protocol": self.protocol,
            "primitive": self.primitive.value,
            "identityRefs": [ref.to_wire() for ref in self.identity_refs],
            "bindingComponents": [component.to_wire() for component in self.binding_components],
            "orderedAssets": [asset.to_wire() for asset in self.ordered_assets],
            "bindingPolicyVersion": self.binding_policy_version,
        }

    def canonical_preimage_bytes(self) -> bytes:
        """Return the restricted RFC-8785-compatible version-1 hash preimage."""
        return json.dumps(
            self.to_preimage_wire(),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("utf-8")


def canonical_venue_binding_preimage_bytes(data: Mapping[str, Any]) -> bytes:
    """Validate and canonicalize an exact version-1 binding wire preimage.

    This validates persisted metadata without constructing a verified-success
    value: only a connector verifier may produce :class:`ExactVenueBinding`.
    """
    expected_keys = {
        "schemaVersion",
        "chain",
        "protocol",
        "primitive",
        "identityRefs",
        "bindingComponents",
        "orderedAssets",
        "bindingPolicyVersion",
    }
    if type(data) is not dict or set(data) != expected_keys:
        raise ValueError("venue binding preimage has an invalid version-1 shape")
    if type(data["schemaVersion"]) is not int or data["schemaVersion"] != VENUE_BINDING_SCHEMA_VERSION:
        raise ValueError("venue binding preimage has an unsupported schemaVersion")

    def require_list(name: str) -> list[Any]:
        value = data[name]
        if type(value) is not list:
            raise TypeError(f"venue binding {name} must be a list")
        return value

    refs = tuple(
        VenueTargetRef(
            role=VenueTargetRole(item["role"]),
            reference_namespace=VenueReferenceNamespace(item["referenceNamespace"]),
            reference=item["reference"],
        )
        for item in require_list("identityRefs")
        if type(item) is dict and set(item) == {"reference", "referenceNamespace", "role"}
    )
    components = tuple(
        VenueBindingComponent(name=item["name"], value=item["value"])
        for item in require_list("bindingComponents")
        if type(item) is dict and set(item) == {"name", "value"}
    )
    assets = tuple(AssetIdentity.from_wire(item) for item in require_list("orderedAssets"))
    if len(refs) != len(data["identityRefs"]) or len(components) != len(data["bindingComponents"]):
        raise ValueError("venue binding preimage contains malformed nested values")
    primitive_value = data["primitive"]
    if type(primitive_value) is not str:
        raise TypeError("venue binding primitive must be a string")
    binding = ExactVenueBinding._verified(
        chain=data["chain"],
        protocol=data["protocol"],
        primitive=Primitive(primitive_value),
        identity_refs=refs,
        binding_components=components,
        ordered_assets=assets,
        binding_policy_version=data["bindingPolicyVersion"],
    )
    if binding.to_preimage_wire() != data:
        raise ValueError("venue binding preimage is not canonical")
    return binding.canonical_preimage_bytes()


@dataclass(frozen=True, slots=True)
class VenueVerificationEvidence:
    """Block-anchored evidence emitted by one declared connector verifier."""

    chain: str
    verifier_ref: str
    verifier_contract_version: str
    block_number: int
    block_hash: str
    observed_facts: tuple[VenueObservedFact, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "chain", _canonical_chain(self.chain, "VenueVerificationEvidence.chain"))
        verifier_ref = _require_exact_str(self.verifier_ref, "VenueVerificationEvidence.verifier_ref")
        if not _IMPORT_REF_RE.fullmatch(verifier_ref):
            raise ValueError("VenueVerificationEvidence.verifier_ref must be 'absolute.module:attribute'")
        version = _require_exact_str(
            self.verifier_contract_version,
            "VenueVerificationEvidence.verifier_contract_version",
        )
        if not _VERSION_RE.fullmatch(version):
            raise ValueError("VenueVerificationEvidence.verifier_contract_version is not canonical")
        if type(self.block_number) is not int or self.block_number <= 0:
            raise ValueError("VenueVerificationEvidence.block_number must be a positive integer")
        if type(self.block_hash) is not str or not _BLOCK_HASH_RE.fullmatch(self.block_hash):
            raise ValueError("VenueVerificationEvidence.block_hash must be canonical lowercase bytes32")
        facts = _validate_exact_tuple(
            self.observed_facts,
            VenueObservedFact,
            "VenueVerificationEvidence.observed_facts",
            allow_empty=False,
        )
        _require_canonical_order(facts, "VenueVerificationEvidence.observed_facts")
        for fact in facts:
            if fact.target_ref is not None:
                fact.target_ref.validate_chain(self.chain)

    def to_wire(self) -> dict[str, object]:
        return {
            "chain": self.chain,
            "verifierRef": self.verifier_ref,
            "verifierContractVersion": self.verifier_contract_version,
            "blockNumber": self.block_number,
            "blockHash": self.block_hash,
            "observedFacts": [fact.to_wire() for fact in self.observed_facts],
        }


@dataclass(frozen=True, slots=True)
class VerifiedVenueBinding:
    """Compiler-consumable success coupling identity, endpoints, and evidence."""

    binding: ExactVenueBinding
    operational_refs: tuple[VenueTargetRef, ...]
    evidence: VenueVerificationEvidence

    def __post_init__(self) -> None:
        if type(self.binding) is not ExactVenueBinding:
            raise TypeError("VerifiedVenueBinding.binding must be ExactVenueBinding")
        refs = _validate_exact_tuple(
            self.operational_refs,
            VenueTargetRef,
            "VerifiedVenueBinding.operational_refs",
            allow_empty=False,
        )
        if any(ref.role not in _OPERATIONAL_ROLES for ref in refs):
            raise ValueError("VerifiedVenueBinding.operational_refs may contain only operational roles")
        for ref in refs:
            ref.validate_chain(self.binding.chain)
        _require_canonical_order(refs, "VerifiedVenueBinding.operational_refs")
        if set(refs) & set(self.binding.identity_refs):
            raise ValueError("VerifiedVenueBinding identity and operational refs overlap")
        if type(self.evidence) is not VenueVerificationEvidence:
            raise TypeError("VerifiedVenueBinding.evidence must be VenueVerificationEvidence")
        if self.evidence.chain != self.binding.chain:
            raise ValueError("VerifiedVenueBinding evidence and binding chains differ")
        observed_targets = {fact.target_ref for fact in self.evidence.observed_facts if fact.target_ref is not None}
        unobserved = [ref for ref in refs if ref not in observed_targets]
        if unobserved:
            raise ValueError(f"VerifiedVenueBinding operational refs lack observed facts: {unobserved!r}")


@dataclass(frozen=True, slots=True)
class VenueBindingFailure:
    """Closed fail-safe alternative to :class:`VerifiedVenueBinding`."""

    state: VenueBindingFailureState
    reason_code: VenueBindingFailureReason
    detail: str
    evidence: VenueVerificationEvidence | None = None

    def __post_init__(self) -> None:
        if type(self.state) is not VenueBindingFailureState:
            raise TypeError("VenueBindingFailure.state must be VenueBindingFailureState")
        if type(self.reason_code) is not VenueBindingFailureReason:
            raise TypeError("VenueBindingFailure.reason_code must be VenueBindingFailureReason")
        if self.reason_code not in _REASONS_BY_STATE[self.state]:
            raise ValueError(
                f"VenueBindingFailure reason {self.reason_code.value!r} is invalid for {self.state.value!r}"
            )
        _require_exact_str(self.detail, "VenueBindingFailure.detail")
        if self.evidence is not None and type(self.evidence) is not VenueVerificationEvidence:
            raise TypeError("VenueBindingFailure.evidence must be None or VenueVerificationEvidence")


VenueVerificationResult = VerifiedVenueBinding | VenueBindingFailure


def build_verified_venue_binding(
    *,
    chain: str,
    protocol: str,
    primitive: Primitive,
    identity_refs: tuple[VenueTargetRef, ...],
    binding_components: tuple[VenueBindingComponent, ...],
    ordered_assets: tuple[AssetIdentity, ...],
    binding_policy_version: int,
    operational_refs: tuple[VenueTargetRef, ...],
    evidence: VenueVerificationEvidence,
) -> VerifiedVenueBinding:
    """Construct verified success atomically; no unverified binding escapes."""
    binding = ExactVenueBinding._verified(
        chain=chain,
        protocol=protocol,
        primitive=primitive,
        identity_refs=identity_refs,
        binding_components=binding_components,
        ordered_assets=ordered_assets,
        binding_policy_version=binding_policy_version,
    )
    return VerifiedVenueBinding(binding=binding, operational_refs=operational_refs, evidence=evidence)


__all__ = [
    "VENUE_BINDING_SCHEMA_VERSION",
    "ExactVenueBinding",
    "VenueBindingComponent",
    "VenueBindingFailure",
    "VenueBindingFailureReason",
    "VenueBindingFailureState",
    "VenueObservedFact",
    "VenueReferenceNamespace",
    "VenueTargetRef",
    "VenueTargetRole",
    "VenueVerificationEvidence",
    "VenueVerificationResult",
    "VerifiedVenueBinding",
    "build_verified_venue_binding",
    "canonical_venue_binding_preimage_bytes",
]
