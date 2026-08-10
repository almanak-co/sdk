"""Immutable portfolio-metrics baseline provenance contract."""

from decimal import Decimal

import pytest

from almanak.framework.portfolio.models import (
    BASELINE_PROVENANCE_MAX_BYTES,
    BaselineProvenance,
    BaselineProvenanceError,
    decode_baseline_provenance,
    encode_baseline_provenance,
    validate_baseline_provenance_initial_value,
    validate_immutable_baseline_update,
)


def test_deterministic_round_trip_preserves_unrelated_records() -> None:
    encoded = encode_baseline_provenance(
        BaselineProvenance(
            source="strategy_allocation_usd",
            initial_value_usd=Decimal("4.00"),
        ),
        positions_json='[{"z":2,"record_type":"unrelated","a":1}]',
    )

    assert encoded == (
        '[{"a":1,"record_type":"unrelated","z":2},'
        '{"initial_value_usd":"4.00","record_type":"accounting_baseline_provenance",'
        '"schema_version":1,"source":"strategy_allocation_usd"}]'
    )
    provenance = decode_baseline_provenance(encoded)
    assert provenance == BaselineProvenance(
        source="strategy_allocation_usd",
        initial_value_usd=Decimal("4.00"),
    )


def test_legacy_empty_list_is_absence_not_fabricated_provenance() -> None:
    assert decode_baseline_provenance("[]") is None


def test_provenance_value_must_equal_enclosing_metrics_baseline() -> None:
    payload = encode_baseline_provenance(
        BaselineProvenance(source="strategy_allocation_usd", initial_value_usd=Decimal("5"))
    )

    with pytest.raises(BaselineProvenanceError, match="must equal metrics initial_value_usd"):
        validate_baseline_provenance_initial_value(payload, initial_value_usd=Decimal("4"))


def test_provenance_schema_rejects_unknown_fields() -> None:
    payload = (
        '[{"initial_value_usd":"4","note":"sealed",'
        '"record_type":"accounting_baseline_provenance",'
        '"schema_version":1,"source":"strategy_allocation_usd"}]'
    )

    with pytest.raises(BaselineProvenanceError, match="unknown fields: note"):
        decode_baseline_provenance(payload)


def test_immutable_marker_compares_exact_decimal_text() -> None:
    existing = encode_baseline_provenance(
        BaselineProvenance(source="strategy_allocation_usd", initial_value_usd=Decimal("4.00"))
    )
    incoming = encode_baseline_provenance(
        BaselineProvenance(source="strategy_allocation_usd", initial_value_usd=Decimal("4"))
    )

    with pytest.raises(BaselineProvenanceError, match="cannot be removed or replaced"):
        validate_immutable_baseline_update(
            existing_positions_json=existing,
            incoming_positions_json=incoming,
            existing_initial_value_usd=Decimal("4"),
            incoming_initial_value_usd=Decimal("4"),
        )


def test_legacy_absence_is_immutable_and_cannot_be_backfilled() -> None:
    provenance = encode_baseline_provenance(
        BaselineProvenance(source="strategy_allocation_usd", initial_value_usd=Decimal("4"))
    )

    with pytest.raises(BaselineProvenanceError, match="cannot be backfilled"):
        validate_immutable_baseline_update(
            existing_positions_json="[]",
            incoming_positions_json=provenance,
            existing_initial_value_usd=Decimal("4"),
            incoming_initial_value_usd=Decimal("4"),
        )
    with pytest.raises(BaselineProvenanceError, match="initial_value_usd cannot be changed"):
        validate_immutable_baseline_update(
            existing_positions_json="[]",
            incoming_positions_json="[]",
            existing_initial_value_usd=Decimal("4"),
            incoming_initial_value_usd=Decimal("5"),
        )


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        ("{}", "must be a JSON list"),
        ("not-json", "must be valid JSON"),
        (
            '[{"record_type":"accounting_baseline_provenance","schema_version":2,'
            '"source":"strategy_allocation_usd","initial_value_usd":"4"}]',
            "unsupported baseline provenance schema_version=2",
        ),
        (
            '[{"record_type":"accounting_baseline_provenance","schema_version":1,'
            '"source":"unknown","initial_value_usd":"4"}]',
            "unsupported baseline provenance source",
        ),
        (
            '[{"record_type":"accounting_baseline_provenance","schema_version":1,'
            '"source":"strategy_allocation_usd","initial_value_usd":4}]',
            "must be exact decimal text",
        ),
        (
            '[{"record_type":"accounting_baseline_provenance","schema_version":1,'
            '"source":"strategy_allocation_usd","initial_value_usd":" 4 "}]',
            "must not contain surrounding whitespace",
        ),
    ],
)
def test_corrupt_records_fail_closed(payload: str, message: str) -> None:
    with pytest.raises(BaselineProvenanceError, match=message):
        decode_baseline_provenance(payload)


def test_duplicate_and_overwrite_are_refused() -> None:
    provenance = BaselineProvenance(
        source="strategy_allocation_usd",
        initial_value_usd=Decimal("4"),
    )
    encoded = encode_baseline_provenance(provenance)
    duplicate = encoded[:-1] + "," + encoded[1:]

    with pytest.raises(BaselineProvenanceError, match="duplicate"):
        decode_baseline_provenance(duplicate)
    with pytest.raises(BaselineProvenanceError, match="cannot be overwritten"):
        encode_baseline_provenance(provenance, positions_json=encoded)


def test_payload_size_is_bounded() -> None:
    oversized = '["' + ("x" * BASELINE_PROVENANCE_MAX_BYTES) + '"]'

    with pytest.raises(BaselineProvenanceError, match="exceeds"):
        decode_baseline_provenance(oversized)


@pytest.mark.parametrize("value", [Decimal("NaN"), Decimal("Infinity"), Decimal("-1")])
def test_initial_value_must_be_measured_non_negative(value: Decimal) -> None:
    with pytest.raises(BaselineProvenanceError, match="finite and non-negative"):
        BaselineProvenance(source="snapshot_total_value_usd", initial_value_usd=value)


def test_established_authority_allows_only_the_same_baseline() -> None:
    established = encode_baseline_provenance(
        BaselineProvenance(source="strategy_allocation_usd", initial_value_usd=Decimal("4"))
    )

    validate_immutable_baseline_update(
        existing_positions_json=established,
        incoming_positions_json=established,
        existing_initial_value_usd=Decimal("4.0"),
        incoming_initial_value_usd=Decimal("4.00"),
    )

    with pytest.raises(BaselineProvenanceError, match="cannot be removed or replaced"):
        validate_immutable_baseline_update(
            existing_positions_json=established,
            incoming_positions_json="[]",
            existing_initial_value_usd=Decimal("4"),
            incoming_initial_value_usd=Decimal("4"),
        )
    with pytest.raises(BaselineProvenanceError, match="initial_value_usd cannot be changed"):
        validate_immutable_baseline_update(
            existing_positions_json=established,
            incoming_positions_json=established,
            existing_initial_value_usd=Decimal("4"),
            incoming_initial_value_usd=Decimal("5"),
        )


def test_persisted_authority_contradiction_fails_closed() -> None:
    established = encode_baseline_provenance(
        BaselineProvenance(source="snapshot_total_value_usd", initial_value_usd=Decimal("4"))
    )

    with pytest.raises(BaselineProvenanceError, match="contradicts persisted"):
        validate_immutable_baseline_update(
            existing_positions_json=established,
            incoming_positions_json=established,
            existing_initial_value_usd=Decimal("5"),
            incoming_initial_value_usd=Decimal("4"),
        )
