"""The recorded decision-reason edit maps exactly each reference's pre-edit source."""

from pathlib import Path

import pytest

from almanak.strategy import StrategyDefinition
from tests.legacy_references import before_reasons, reason_edits, with_reviewed_reasons

ROOT = Path(__file__).parents[1]


@pytest.mark.parametrize("name", sorted(reason_edits()["references"]))
def test_the_reason_edit_reverses_to_the_recorded_source_and_back(name):
    current = StrategyDefinition.read(ROOT / "strategies" / name / "strategy.py").source
    before = before_reasons(name, current)
    assert before is not None and before != current
    assert with_reviewed_reasons(before) == current
    assert with_reviewed_reasons(current) == current


def test_other_sources_are_unchanged_and_never_reverse():
    name = next(iter(reason_edits()["references"]))
    assert with_reviewed_reasons("x = 1\n") == "x = 1\n"
    assert before_reasons(name, "x = 1\n") is None
    assert before_reasons("not_an_edited_reference", "x = 1\n") == "x = 1\n"
