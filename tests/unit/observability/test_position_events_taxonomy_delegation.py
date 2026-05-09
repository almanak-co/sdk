"""Parity test: post-T2 position_events resolves position_type via the taxonomy.

VIB-4162 (T2). Asserts:

* The legacy ``INTENT_TO_POSITION_TYPE`` constant is gone.
* For every position-producing intent, ``_seed_event`` produces a
  ``PositionEvent`` whose ``position_type`` matches the pre-T2 frozen
  truth table.
* For non-position intents, ``_seed_event`` returns ``None`` (matches
  pre-T2 behaviour).
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from almanak.framework.observability import position_events as pe

_FIXTURE = (
    Path(__file__).resolve().parents[2]
    / "fixtures"
    / "accounting"
    / "legacy_position_type_truth_table.json"
)


def _truth_table() -> list[dict]:
    return json.loads(_FIXTURE.read_text())


@dataclass
class _StubIntent:
    intent_type: str
    protocol: str = ""

    @property
    def value(self) -> str:  # parity with IntentType.value access
        return self.intent_type


def _seed(intent_type: str) -> Any:
    """Drive _seed_event with a minimal IntentEventContext."""
    intent = _StubIntent(intent_type=intent_type)
    ctx = pe.IntentEventContext(
        intent=intent,
        result=None,
        extracted={},
        deployment_id="test",
        chain="arbitrum",
        ledger_entry_id="le-1",
    )
    return pe._seed_event(ctx)


def test_intent_to_position_type_constant_removed() -> None:
    """Static delegation guard — re-introducing INTENT_TO_POSITION_TYPE FAILs."""
    assert not hasattr(pe, "INTENT_TO_POSITION_TYPE"), (
        "INTENT_TO_POSITION_TYPE was deleted in T2; re-introducing it as a "
        "local authoritative map defeats the taxonomy delegation"
    )


@pytest.mark.parametrize("row", _truth_table())
def test_seed_event_position_type_matches_frozen_truth_table(row: dict) -> None:
    intent_type = row["intent"]
    expected = row["expected"]["position_type"]
    if intent_type not in pe.INTENT_TO_EVENT_TYPE:
        # _seed_event returns None for non-position intents BEFORE the
        # taxonomy lookup, so the truth-table expected==None is satisfied
        # by the seed returning None.
        assert _seed(intent_type) is None
        assert expected is None
        return

    event = _seed(intent_type)
    assert event is not None, f"_seed_event returned None for known position intent {intent_type}"
    assert event.position_type == expected, (
        f"position_type drift for {intent_type}: expected {expected}, got {event.position_type}"
    )
