"""Canonical ValueConfidence parsing, absence, and snapshot round-trips."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from almanak.framework.portfolio.models import (
    PortfolioSnapshot,
    ValueConfidence,
    serialize_value_confidence,
)


def _snapshot(confidence: ValueConfidence | str | None = None) -> PortfolioSnapshot:
    return PortfolioSnapshot(
        timestamp=datetime(2026, 7, 31, tzinfo=UTC),
        deployment_id="deployment:alm3086",
        total_value_usd=Decimal("100"),
        available_cash_usd=Decimal("25"),
        value_confidence=confidence,
    )


@pytest.mark.parametrize("confidence", list(ValueConfidence))
def test_every_value_round_trips_with_stable_serialization(confidence: ValueConfidence) -> None:
    assert ValueConfidence.parse(confidence) is confidence
    assert ValueConfidence.parse(confidence.value) is confidence
    assert serialize_value_confidence(confidence) == confidence.value

    restored = PortfolioSnapshot.from_dict(_snapshot(confidence).to_dict())
    assert restored.value_confidence is confidence
    assert restored.is_valid is (confidence != ValueConfidence.UNAVAILABLE)


@pytest.mark.parametrize("missing", [None, ""])
def test_absence_stays_unmeasured(missing: str | None) -> None:
    assert ValueConfidence.parse_optional(missing) is None
    assert serialize_value_confidence(None) == ""

    snapshot = _snapshot(missing)
    assert snapshot.value_confidence is None
    assert not snapshot.is_valid
    assert snapshot.to_dict()["value_confidence"] is None

    restored = PortfolioSnapshot.from_dict(snapshot.to_dict())
    assert restored.value_confidence is None
    assert not restored.is_valid


def test_missing_from_historical_dict_is_not_upgraded() -> None:
    data = _snapshot(ValueConfidence.HIGH).to_dict()
    data.pop("value_confidence")
    restored = PortfolioSnapshot.from_dict(data)
    assert restored.value_confidence is None
    assert not restored.is_valid


@pytest.mark.parametrize("unknown", ["high", " HIGH", "HIGH ", "MYSTERY"])
def test_unknown_strings_are_rejected_without_normalization(unknown: str) -> None:
    with pytest.raises(ValueError, match="invalid value_confidence"):
        ValueConfidence.parse_optional(unknown)


def test_non_string_boundary_value_is_rejected() -> None:
    with pytest.raises(ValueError, match="must be a string or ValueConfidence"):
        ValueConfidence.parse_optional(1)  # type: ignore[arg-type]
