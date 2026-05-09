"""Parity test: post-T2 classifier returns the same category as pre-T2 for every (intent, protocol, token_out).

VIB-4162 (T2). Frozen truth table at
``tests/fixtures/accounting/legacy_classifier_truth_table.json`` is loaded
and every row is asserted against the post-T2 classifier (which delegates
to ``primitives.taxonomy.classify``). Drift in either direction (taxonomy
row mutated, classifier re-introduced local routing) FAILs.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from almanak.framework.accounting import classifier as accounting_classifier
from almanak.framework.intents.vocabulary import IntentType
from almanak.framework.primitives import taxonomy as primitives_taxonomy

_FIXTURE = Path(__file__).resolve().parents[2] / "fixtures" / "accounting" / "legacy_classifier_truth_table.json"


def _load_truth_table() -> list[dict]:
    return json.loads(_FIXTURE.read_text())


@pytest.mark.parametrize("row", _load_truth_table())
def test_classifier_parity_against_frozen_truth_table(row: dict) -> None:
    """Every (intent, protocol, token_out) row must match the pre-T2 category."""
    actual = accounting_classifier.classify(
        row["intent"], row["protocol"], row["token_out"]
    )
    assert actual.value == row["expected"]["category"], (
        f"classifier drift for ({row['intent']}, {row['protocol']!r}, {row['token_out']!r}): "
        f"expected {row['expected']['category']}, got {actual.value}"
    )


def test_classifier_classify_is_taxonomy_classify() -> None:
    """Delegation lock — re-introducing a local classify function FAILs."""
    assert accounting_classifier.classify is primitives_taxonomy.classify


def test_every_intenttype_value_has_taxonomy_row() -> None:
    """D2.M1 — exhaustiveness: a new IntentType without a TAXONOMY row FAILs."""
    for member in IntentType:
        # raises UnknownIntentTypeError on miss
        primitives_taxonomy.record_for(member.value)
