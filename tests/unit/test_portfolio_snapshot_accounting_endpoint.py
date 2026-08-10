"""Measured accounting endpoint contract for portfolio snapshots."""

from types import SimpleNamespace

import pytest

from almanak.framework.portfolio.models import ValueConfidence, is_measured_accounting_snapshot


def _snapshot(*, deployed="4", cash="1", confidence=ValueConfidence.HIGH):
    return SimpleNamespace(
        total_value_usd=deployed,
        available_cash_usd=cash,
        value_confidence=confidence,
    )


@pytest.mark.parametrize("confidence", [ValueConfidence.HIGH, ValueConfidence.ESTIMATED, ValueConfidence.STALE])
def test_exact_measured_confidences_are_valid(confidence):
    assert is_measured_accounting_snapshot(_snapshot(confidence=confidence))


def test_measured_zero_is_valid():
    assert is_measured_accounting_snapshot(_snapshot(deployed="0", cash="0"))


@pytest.mark.parametrize(
    ("deployed", "cash", "confidence"),
    [
        ("0", "0", ValueConfidence.UNAVAILABLE),
        ("0", "0", None),
        (None, "0", ValueConfidence.HIGH),
        ("0", None, ValueConfidence.HIGH),
        ("NaN", "0", ValueConfidence.HIGH),
        ("Infinity", "0", ValueConfidence.HIGH),
        ("-1", "1", ValueConfidence.HIGH),
        (True, "0", ValueConfidence.HIGH),
    ],
)
def test_unmeasured_or_invalid_endpoint_is_rejected(deployed, cash, confidence):
    assert not is_measured_accounting_snapshot(_snapshot(deployed=deployed, cash=cash, confidence=confidence))


def test_unknown_confidence_is_rejected_without_normalization():
    assert not is_measured_accounting_snapshot(_snapshot(confidence="high"))
