"""VIB-6597: quant evidence must use canonical wallet NAV, not position value."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

_SCRIPT = Path(__file__).parents[3] / "qa_lab" / "build_data_json.py"
_SPEC = importlib.util.spec_from_file_location("quant_build_data_json", _SCRIPT)
assert _SPEC is not None and _SPEC.loader is not None
_MODULE = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_MODULE)
_snapshot_facts = _MODULE._snapshot_facts


def _row(
    *,
    total: str | None,
    wallet_total: str | None,
    cash: str | None,
    positions: list[dict] | None = None,
    confidence: str = "HIGH",
) -> tuple:
    payload = {"schema_version": 1, "positions": positions or []}
    return (
        "2026-08-08T04:48:34+00:00",
        total,
        wallet_total,
        cash,
        "0",
        confidence,
        json.dumps(payload),
    )


def test_all_cash_snapshot_is_positive_nav_not_a_contradiction() -> None:
    """Closed BENQI shape: $0 positions plus $4.91 cash is $4.91 NAV."""
    facts = _snapshot_facts(
        [_row(total="0", cash="4.91234486581681560850090", wallet_total="4.91234486581681560850090")]
    )

    assert facts["nav_first"][1:] == pytest.approx([4.9123448658168155, 4.9123448658168155])
    assert "zero_nav_high_confidence" not in facts
    assert "snapshot_contradictions" not in facts


def test_mixed_lending_snapshot_nets_debt_before_comparing_wallet_nav() -> None:
    """BENQI borrow cash is an asset only after the matching debt is subtracted."""
    positions = [
        {"position_type": "SUPPLY", "value_usd": "4.00"},
        {"position_type": "BORROW", "value_usd": "-0.8000"},
    ]
    facts = _snapshot_facts(
        [_row(total="4.00", cash="1.71250898955025542096", wallet_total="4.91250898955025542096", positions=positions)]
    )

    assert facts["nav_first"][1:] == pytest.approx([4.912508989550256, 4.912508989550256])
    assert "snapshot_contradictions" not in facts
    assert "zero_nav_high_confidence" not in facts


def test_genuine_zero_equity_is_still_reported() -> None:
    facts = _snapshot_facts([_row(total="0", cash="0", wallet_total="0")])

    assert facts["nav_first"][1:] == [0.0, 0.0]
    assert facts["zero_nav_high_confidence"]["count"] == 1


def test_wallet_total_disagreement_remains_a_contradiction() -> None:
    """VIB-6062 stays detected through a semantically valid parity check."""
    facts = _snapshot_facts([_row(total="0", cash="5.8943", wallet_total="0")])

    assert facts["snapshot_contradictions"]["count"] == 1
