"""Exact venue receipt-correlation contract."""

from __future__ import annotations

import hashlib
import json

import pytest

from almanak.framework.venues import (
    VenueReceiptCorrelationError,
    VenueReceiptEvidence,
    correlate_verified_venue_receipts,
    reconcile_venue_permissions,
    reconcile_verified_venue_receipts,
)

POOL = "0x1111111111111111111111111111111111111111"
TOKEN0 = "0x2222222222222222222222222222222222222222"
TOKEN1 = "0x3333333333333333333333333333333333333333"


def _metadata() -> dict[str, object]:
    binding: dict[str, object] = {
        "schemaVersion": 1,
        "chain": "base",
        "protocol": "uniswap_v3",
        "primitive": "swap",
        "identityRefs": [
            {
                "reference": POOL,
                "referenceNamespace": "evm_address",
                "role": "pool",
            }
        ],
        "bindingComponents": [{"name": "fee", "value": "500"}],
        "orderedAssets": [
            {"schemaVersion": 1, "chain": "base", "assetNamespace": "erc20", "assetReference": TOKEN0},
            {"schemaVersion": 1, "chain": "base", "assetNamespace": "erc20", "assetReference": TOKEN1},
        ],
        "bindingPolicyVersion": 1,
    }
    canonical = json.dumps(binding, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
    return {
        "venue_binding": binding,
        "venue_binding_hash": hashlib.sha256(canonical).hexdigest(),
        "venue_operational_refs": [
            {
                "reference": "0x4444444444444444444444444444444444444444",
                "referenceNamespace": "evm_address",
                "role": "router",
            }
        ],
    }


def test_correlates_canonical_binding_to_exact_pool_emitter() -> None:
    metadata = _metadata()
    expected = metadata["venue_binding_hash"]
    assert isinstance(expected, str)

    assert (
        correlate_verified_venue_receipts(
            bundle_metadata=metadata,
            expected_binding_hash=expected,
            receipts=({"status": 1, "logs": [{"address": POOL.upper().replace("0X", "0x")}]},),
        )
        == metadata["venue_binding_hash"]
    )

    evidence = reconcile_verified_venue_receipts(
        bundle_metadata=metadata,
        expected_binding_hash=expected,
        receipts=({"status": 1, "logs": [{"address": POOL}]},),
    )
    assert evidence == VenueReceiptEvidence(binding_hash=expected, pool_address=POOL)


def test_rejects_receipts_from_an_alternate_pool() -> None:
    metadata = _metadata()
    expected = metadata["venue_binding_hash"]
    assert isinstance(expected, str)
    with pytest.raises(VenueReceiptCorrelationError, match="no event emitted by verified venue pool"):
        correlate_verified_venue_receipts(
            bundle_metadata=metadata,
            expected_binding_hash=expected,
            receipts=({"status": 1, "logs": [{"address": "0x" + "44" * 20}]},),
        )


def test_rejects_tampered_binding_preimage() -> None:
    metadata = _metadata()
    expected = metadata["venue_binding_hash"]
    assert isinstance(expected, str)
    metadata["venue_binding"]["bindingComponents"][0]["value"] = "3000"  # type: ignore[index]

    with pytest.raises(VenueReceiptCorrelationError, match="preimage does not match"):
        correlate_verified_venue_receipts(
            bundle_metadata=metadata,
            expected_binding_hash=expected,
            receipts=(),
        )


def test_unverified_bundle_is_outside_the_correlation_contract() -> None:
    assert correlate_verified_venue_receipts(bundle_metadata={}, expected_binding_hash=None, receipts=()) is None


def test_permission_reconciliation_returns_typed_binding_evidence() -> None:
    metadata = _metadata()
    evidence = reconcile_venue_permissions(
        bundle_metadata=metadata,
        expected_binding_hash=metadata["venue_binding_hash"],
        permissions=[{"target": "0x4444444444444444444444444444444444444444"}],
    )
    assert evidence is not None
    assert evidence.binding_hash == metadata["venue_binding_hash"]


def test_permission_reconciliation_allows_unrelated_generated_targets() -> None:
    metadata = _metadata()
    evidence = reconcile_venue_permissions(
        bundle_metadata=metadata,
        expected_binding_hash=metadata["venue_binding_hash"],
        permissions=[
            {"target": "0x4444444444444444444444444444444444444444"},
            {"target": "0x5555555555555555555555555555555555555555"},
        ],
    )
    assert evidence is not None


def test_permission_reconciliation_rejects_missing_operational_target() -> None:
    metadata = _metadata()
    with pytest.raises(VenueReceiptCorrelationError, match="missing compiled"):
        reconcile_venue_permissions(
            bundle_metadata=metadata,
            expected_binding_hash=metadata["venue_binding_hash"],
            permissions=[{"target": "0x5555555555555555555555555555555555555555"}],
        )


@pytest.mark.parametrize(
    "bad_ref",
    [None, {"referenceNamespace": "other", "reference": "0x555"}, {"referenceNamespace": "evm_address"}],
)
def test_permission_reconciliation_rejects_malformed_operational_reference(bad_ref: object) -> None:
    metadata = _metadata()
    metadata["venue_operational_refs"].append(bad_ref)  # type: ignore[union-attr]
    with pytest.raises(VenueReceiptCorrelationError, match="operational"):
        reconcile_venue_permissions(
            bundle_metadata=metadata,
            expected_binding_hash=metadata["venue_binding_hash"],
            permissions=[{"target": "0x4444444444444444444444444444444444444444"}],
        )


def test_permission_reconciliation_rejects_malformed_permission_entry() -> None:
    with pytest.raises(VenueReceiptCorrelationError, match="permission set"):
        reconcile_venue_permissions(
            bundle_metadata=_metadata(),
            expected_binding_hash=_metadata()["venue_binding_hash"],
            permissions=[None],  # type: ignore[list-item]
        )


def test_permission_reconciliation_rejects_missing_independent_hash() -> None:
    metadata = _metadata()
    with pytest.raises(VenueReceiptCorrelationError, match="independently captured"):
        reconcile_venue_permissions(
            bundle_metadata=metadata,
            expected_binding_hash="0" * 64,
            permissions=[{"target": "0x4444444444444444444444444444444444444444"}],
        )


def test_rejects_self_hashed_but_schema_invalid_binding() -> None:
    metadata = _metadata()
    metadata["venue_binding"]["orderedAssets"] = []  # type: ignore[index]
    canonical = json.dumps(metadata["venue_binding"], sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
    metadata["venue_binding_hash"] = hashlib.sha256(canonical).hexdigest()
    expected = metadata["venue_binding_hash"]
    assert isinstance(expected, str)

    with pytest.raises(VenueReceiptCorrelationError, match="preimage is invalid"):
        correlate_verified_venue_receipts(
            bundle_metadata=metadata,
            expected_binding_hash=expected,
            receipts=(),
        )


def test_rejects_failed_receipt_even_when_pool_emitted_a_log() -> None:
    metadata = _metadata()
    expected = metadata["venue_binding_hash"]
    assert isinstance(expected, str)
    with pytest.raises(VenueReceiptCorrelationError, match="status-1"):
        correlate_verified_venue_receipts(
            bundle_metadata=metadata,
            expected_binding_hash=expected,
            receipts=({"status": 0, "logs": [{"address": POOL}]},),
        )


def test_rejects_valid_replacement_binding_with_recomputed_metadata_hash() -> None:
    metadata = _metadata()
    expected = metadata["venue_binding_hash"]
    assert isinstance(expected, str)
    replacement_pool = "0x" + "55" * 20
    metadata["venue_binding"]["identityRefs"][0]["reference"] = replacement_pool  # type: ignore[index]
    canonical = json.dumps(metadata["venue_binding"], sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
    metadata["venue_binding_hash"] = hashlib.sha256(canonical).hexdigest()

    with pytest.raises(VenueReceiptCorrelationError, match="independently captured compiler hash"):
        correlate_verified_venue_receipts(
            bundle_metadata=metadata,
            expected_binding_hash=expected,
            receipts=({"status": 1, "logs": [{"address": replacement_pool}]},),
        )
