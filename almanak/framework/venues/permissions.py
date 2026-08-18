"""Permission-to-venue reconciliation for exact compiler bindings."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from .receipt import VenueReceiptCorrelationError
from .types import canonical_venue_binding_preimage_bytes

_EVM_ADDRESS_RE = re.compile(r"0x[0-9a-fA-F]{40}\Z")


@dataclass(frozen=True, slots=True)
class VenuePermissionEvidence:
    """Immutable join of a binding claim and its authorized target set."""

    binding_hash: str
    operational_targets: tuple[str, ...]


def _validated_binding_hash(bundle_metadata: Mapping[str, Any], expected: str | None) -> str:
    binding_hash = bundle_metadata.get("venue_binding_hash")
    if not isinstance(binding_hash, str) or len(binding_hash) != 64 or binding_hash.lower() != binding_hash:
        raise VenueReceiptCorrelationError("venue_binding_hash is not canonical lowercase SHA-256 hex")
    try:
        bytes.fromhex(binding_hash)
    except ValueError as exc:
        raise VenueReceiptCorrelationError("venue_binding_hash is not hexadecimal") from exc
    if expected != binding_hash:
        raise VenueReceiptCorrelationError("venue_binding_hash does not match the independently captured compiler hash")
    binding = bundle_metadata.get("venue_binding")
    if type(binding) is not dict:
        raise VenueReceiptCorrelationError("exact venue metadata is missing its canonical binding preimage")
    try:
        canonical = canonical_venue_binding_preimage_bytes(binding)
    except (TypeError, ValueError) as exc:
        raise VenueReceiptCorrelationError(f"venue binding preimage is invalid: {exc}") from exc
    if hashlib.sha256(canonical).hexdigest() != binding_hash:
        raise VenueReceiptCorrelationError("venue binding preimage does not match venue_binding_hash")
    return binding_hash


def _expected_operational_targets(bundle_metadata: Mapping[str, Any]) -> set[str]:
    raw_refs = bundle_metadata.get("venue_operational_refs")
    if not isinstance(raw_refs, list) or not raw_refs:
        raise VenueReceiptCorrelationError("exact venue metadata is missing venue_operational_refs")
    expected: set[str] = set()
    for ref in raw_refs:
        if not isinstance(ref, Mapping):
            raise VenueReceiptCorrelationError("exact venue metadata has a malformed operational reference")
        if ref.get("referenceNamespace") != "evm_address":
            raise VenueReceiptCorrelationError("exact venue metadata has a non-EVM operational reference")
        target = ref.get("reference")
        if not isinstance(target, str) or _EVM_ADDRESS_RE.fullmatch(target) is None:
            raise VenueReceiptCorrelationError("exact venue metadata has a malformed EVM operational target")
        expected.add(target.lower())
    if not expected:
        raise VenueReceiptCorrelationError("exact venue metadata has no EVM operational targets")
    return expected


def _permission_targets(permissions: Sequence[Mapping[str, Any]]) -> set[str]:
    actual: set[str] = set()
    for permission in permissions:
        if not isinstance(permission, Mapping):
            raise VenueReceiptCorrelationError("permission set contains a malformed entry")
        target = permission.get("target")
        if not isinstance(target, str) or _EVM_ADDRESS_RE.fullmatch(target) is None:
            raise VenueReceiptCorrelationError("permission set contains a malformed target")
        actual.add(target.lower())
    return actual


def reconcile_venue_permissions(
    *,
    bundle_metadata: Mapping[str, Any] | None,
    expected_binding_hash: str | None,
    permissions: Sequence[Mapping[str, Any]],
) -> VenuePermissionEvidence | None:
    """Ensure generated permissions contain the compiled operational refs.

    Unbound legacy bundles return ``None``. Exact bundles fail closed when the
    metadata is malformed, when the independently captured compiler hash is
    missing, or when an operational reference is absent from the permission
    set. Additional targets are allowed because generated manifests include
    token approvals and infrastructure dispatch targets alongside venue refs.
    This remains a pure SDK seam so permission generation can opt in without
    changing hosted schemas or legacy manifests.
    """
    if not bundle_metadata or "venue_binding_hash" not in bundle_metadata:
        if expected_binding_hash is not None:
            raise VenueReceiptCorrelationError("expected binding hash has no verified bundle metadata")
        return None
    binding_hash = _validated_binding_hash(bundle_metadata, expected_binding_hash)
    expected = _expected_operational_targets(bundle_metadata)
    actual = _permission_targets(permissions)
    if not expected.issubset(actual):
        raise VenueReceiptCorrelationError("permission targets are missing compiled exact venue operational refs")
    return VenuePermissionEvidence(binding_hash=binding_hash, operational_targets=tuple(sorted(expected)))


__all__ = ["VenuePermissionEvidence", "reconcile_venue_permissions"]
