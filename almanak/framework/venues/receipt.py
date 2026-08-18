"""Receipt correlation for compiler-verified physical venues."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from almanak.framework.venues.types import canonical_venue_binding_preimage_bytes


class VenueReceiptCorrelationError(ValueError):
    """Receipt facts do not match the compiler's verified venue binding."""


@dataclass(frozen=True, slots=True)
class VenueReceiptEvidence:
    """Typed result of joining compiler identity with confirmed receipt facts."""

    binding_hash: str
    pool_address: str


def _canonical_sha256(value: object, *, field: str) -> str:
    if not isinstance(value, str) or len(value) != 64 or value.lower() != value:
        raise VenueReceiptCorrelationError(f"{field} is not canonical lowercase SHA-256 hex")
    try:
        bytes.fromhex(value)
    except ValueError as exc:
        raise VenueReceiptCorrelationError(f"{field} is not hexadecimal") from exc
    return value


def _validated_binding_pool(
    bundle_metadata: Mapping[str, Any], *, expected_binding_hash: str | None
) -> tuple[str, str]:
    binding_hash = _canonical_sha256(bundle_metadata.get("venue_binding_hash"), field="venue_binding_hash")
    if expected_binding_hash != binding_hash:
        raise VenueReceiptCorrelationError("venue_binding_hash does not match the independently captured compiler hash")
    binding = bundle_metadata.get("venue_binding")
    if type(binding) is not dict:
        raise VenueReceiptCorrelationError("verified venue metadata is missing its canonical binding preimage")
    try:
        canonical = canonical_venue_binding_preimage_bytes(binding)
    except (TypeError, ValueError) as exc:
        raise VenueReceiptCorrelationError(f"venue binding preimage is invalid: {exc}") from exc
    if hashlib.sha256(canonical).hexdigest() != binding_hash:
        raise VenueReceiptCorrelationError("venue binding preimage does not match venue_binding_hash")

    refs = binding.get("identityRefs")
    if not isinstance(refs, list):
        raise VenueReceiptCorrelationError("venue binding identityRefs must be a list")
    pools = {
        ref.get("reference", "").lower()
        for ref in refs
        if isinstance(ref, dict)
        and ref.get("role") == "pool"
        and ref.get("referenceNamespace") == "evm_address"
        and isinstance(ref.get("reference"), str)
    }
    if len(pools) != 1:
        raise VenueReceiptCorrelationError("verified EVM venue binding must name exactly one physical pool")
    return binding_hash, next(iter(pools))


def correlate_verified_venue_receipts(
    *,
    bundle_metadata: Mapping[str, Any] | None,
    expected_binding_hash: str | None,
    receipts: Sequence[Mapping[str, Any]],
) -> str | None:
    """Return the verified binding hash after correlating receipts to its pool.

    Bundles without a verified binding remain unchanged.  A bundle that claims
    one must carry the exact canonical preimage, match the binding hash captured
    from compilation before execution, and have at least one confirmed receipt
    log emitted by its physical pool. This prevents receipt enrichment from
    silently describing a different venue than compilation.
    """
    evidence = reconcile_verified_venue_receipts(
        bundle_metadata=bundle_metadata,
        expected_binding_hash=expected_binding_hash,
        receipts=receipts,
    )
    return evidence.binding_hash if evidence is not None else None


def reconcile_verified_venue_receipts(
    *,
    bundle_metadata: Mapping[str, Any] | None,
    expected_binding_hash: str | None,
    receipts: Sequence[Mapping[str, Any]],
) -> VenueReceiptEvidence | None:
    """Join a compiled binding with confirmed receipts without mutating them.

    Legacy bundles return ``None``. Exact bundles fail closed on a missing
    independently captured hash, invalid preimage, non-success receipt, or an
    emitter other than the verified pool.
    """
    if not bundle_metadata or "venue_binding_hash" not in bundle_metadata:
        if expected_binding_hash is not None:
            raise VenueReceiptCorrelationError("expected binding hash has no verified bundle metadata")
        return None
    binding_hash, pool = _validated_binding_pool(
        bundle_metadata,
        expected_binding_hash=expected_binding_hash,
    )
    emitters: set[str] = set()
    for receipt in receipts:
        if type(receipt.get("status")) is not int or receipt["status"] != 1:
            raise VenueReceiptCorrelationError("venue receipt correlation requires status-1 confirmed receipts")
        logs = receipt.get("logs")
        if not isinstance(logs, list):
            continue
        for log in logs:
            address = log.get("address") if isinstance(log, dict) else None
            if isinstance(address, str):
                emitters.add(address.lower())
    if pool not in emitters:
        raise VenueReceiptCorrelationError(f"confirmed receipts contain no event emitted by verified venue pool {pool}")
    return VenueReceiptEvidence(binding_hash=binding_hash, pool_address=pool)


__all__ = [
    "VenueReceiptCorrelationError",
    "VenueReceiptEvidence",
    "correlate_verified_venue_receipts",
    "reconcile_verified_venue_receipts",
]
