"""Contract tests for the canonical atomic ledger/registry save mode."""

from __future__ import annotations

import pytest

from almanak.framework.state.ledger_registry_mode import LedgerRegistrySaveMode


@pytest.mark.parametrize(
    ("wire_value", "expected"),
    [
        ("", LedgerRegistrySaveMode.COMMIT),
        ("commit", LedgerRegistrySaveMode.COMMIT),
        ("registry_reconciliation", LedgerRegistrySaveMode.REGISTRY_RECONCILIATION),
    ],
)
def test_parse_wire_preserves_existing_values(wire_value, expected):
    assert LedgerRegistrySaveMode.parse_wire(wire_value) is expected


@pytest.mark.parametrize("wire_value", ["COMMIT", " commit ", "registry", "unknown"])
def test_parse_wire_rejects_every_unknown_value_deterministically(wire_value):
    with pytest.raises(ValueError) as exc_info:
        LedgerRegistrySaveMode.parse_wire(wire_value)

    assert str(exc_info.value) == (
        f"invalid ledger/registry save mode {wire_value!r}; expected one of: 'commit', 'registry_reconciliation'"
    )


def test_behavior_exhaustively_pins_each_atomic_write_set():
    assert LedgerRegistrySaveMode.COMMIT.behavior.writes_ledger is True
    assert LedgerRegistrySaveMode.COMMIT.behavior.is_reconciliation is False
    assert LedgerRegistrySaveMode.REGISTRY_RECONCILIATION.behavior.writes_ledger is False
    assert LedgerRegistrySaveMode.REGISTRY_RECONCILIATION.behavior.is_reconciliation is True


def test_wire_serializer_preserves_empty_default_for_commit():
    assert LedgerRegistrySaveMode.COMMIT.to_wire() == ""
    assert LedgerRegistrySaveMode.REGISTRY_RECONCILIATION.to_wire() == "registry_reconciliation"
