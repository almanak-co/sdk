"""Two-layer defense: classify() is soft (NO_ACCOUNTING), record_for() is strict (raises).

VIB-4162 (T2 D3.F1). The router-layer soft contract is preserved (frozen
in ``legacy_classifier_truth_table.json``); the strict layer below
fail-fasts.
"""

from __future__ import annotations

import pytest

from almanak.framework.accounting.classifier import classify
from almanak.framework.primitives.taxonomy import (
    UnknownIntentTypeError,
    record_for,
)
from almanak.framework.primitives.types import AccountingCategory


def test_classify_returns_no_accounting_for_unknown() -> None:
    """Soft contract — unknown intents resolve to NO_ACCOUNTING (skip writer)."""
    assert classify("FROBNICATE") == AccountingCategory.NO_ACCOUNTING


def test_strict_classify_raises_via_record_for() -> None:
    """Strict contract — unknown intents raise UnknownIntentTypeError."""
    with pytest.raises(UnknownIntentTypeError):
        record_for("FROBNICATE")
