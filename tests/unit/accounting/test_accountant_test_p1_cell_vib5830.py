"""Unit tests for the P1 cell (perp position lifecycle) in
``almanak.framework.accounting.accountant_test``.

Background — VIB-5830: P1 was XFAIL-BY-CONSTRUCTION. The predicate matched
``event_type == "PERP_OPEN"`` against ``position_events``, but the two tables
speak DIFFERENT vocabularies:

* ``position_events.event_type``  → ``PositionEventType`` (OPEN / CLOSE / …),
  with the primitive carried separately on ``position_type`` (PERP);
* ``accounting_events.event_type`` → intent-type strings (PERP_OPEN / PERP_CLOSE).

So the string could never match and P1 reported ``XFAIL`` — "OPEN=False
CLOSE=False" — even against the canonical perp fixture, which holds a complete
PERP OPEN→CLOSE arc. The cell was structurally unable to PASS.

These tests pin BOTH failure directions, so the cell stays genuinely scorable:

* a PERP OPEN→CLOSE arc → PASS, and an OPEN-only (still-held) position → PASS,
  because a strategy mid-flight legitimately has no CLOSE yet;
* the ACCOUNTING vocabulary written onto ``position_events`` → XFAIL — this is
  the original bug, pinned so a revert cannot go unnoticed;
* LP / lending rows → XFAIL — those primitives share the OPEN/CLOSE verbs, so
  without the ``position_type`` scope an LP-only DB would score P1 PASS BY
  CONSTRUCTION, which is the same class of defect in the opposite direction;
* no ``position_events`` at all → XFAIL, never FAIL.
"""

from __future__ import annotations

from typing import Any

from almanak.framework.accounting.accountant_test import _cells_perp


def _pos_event(
    *,
    event_type: str,
    position_type: str = "PERP",
) -> dict[str, Any]:
    """A ``position_events`` row shaped like the canonical perp fixture."""
    return {
        "id": f"pe-{position_type}-{event_type}",
        "position_id": "perp:arbitrum:gmx_v2:wallet:ETH-USDC",
        "position_type": position_type,
        "event_type": event_type,
        "protocol": "gmx_v2",
        "chain": "arbitrum",
    }


def _p1(pos_events: list[dict[str, Any]]) -> Any:
    """Score P1 in isolation; perp accounting inputs are irrelevant to this cell."""
    cells = _cells_perp([], pos_events, {}, {})
    return next(c for c in cells if c.cell_id == "P1")


def test_p1_passes_on_full_open_close_arc() -> None:
    """The canonical fixture shape: a complete PERP OPEN→CLOSE arc."""
    cell = _p1([_pos_event(event_type="OPEN"), _pos_event(event_type="CLOSE")])
    assert cell.status == "PASS"
    assert "OPEN=True" in cell.diagnostic
    assert "CLOSE=True" in cell.diagnostic


def test_p1_passes_on_open_only_position_still_held() -> None:
    """A perp still being held has no CLOSE yet — lifecycle IS being recorded."""
    assert _p1([_pos_event(event_type="OPEN")]).status == "PASS"


def test_p1_xfails_on_close_only_missing_entry() -> None:
    """A CLOSE with no OPEN must NOT score green — that IS the lifecycle gap.

    P1 is named for entry/exit price, size, leverage and direction; the entry
    half of that lives on the OPEN event. A close-only DB means the entry was
    never recorded, so passing off the exit alone would hide the exact defect
    the cell exists to catch (Codex P2 on PR #3289).

    This is reachable only because VIB-5830 made the cell scorable at all:
    before the fix the vocabulary bug pinned both terms False, so the old
    ``has_open or has_close`` predicate was dead code. Pinned here so the
    ``or`` cannot creep back.
    """
    cell = _p1([_pos_event(event_type="CLOSE")])
    assert cell.status == "XFAIL"
    assert "OPEN=False" in cell.diagnostic


def test_p1_xfails_on_accounting_vocabulary_the_vib5830_bug() -> None:
    """VIB-5830 regression pin.

    ``position_events`` never carries PERP_OPEN / PERP_CLOSE — that is the
    ``accounting_events`` vocabulary. If the predicate is ever pointed back at
    those strings, the arc above stops matching and P1 reverts to
    XFAIL-by-construction. Rows written in the wrong vocabulary must NOT score.
    """
    cell = _p1(
        [
            _pos_event(event_type="PERP_OPEN"),
            _pos_event(event_type="PERP_CLOSE"),
        ]
    )
    assert cell.status == "XFAIL"


def test_p1_xfails_on_lp_rows_no_pass_by_construction() -> None:
    """LP shares the OPEN/CLOSE verbs — only ``position_type`` separates it.

    Without the PERP scope this LP-only DB would score P1 PASS, i.e. the perp
    lifecycle cell would be satisfied by a strategy that never touched a perp.
    """
    cell = _p1(
        [
            _pos_event(event_type="OPEN", position_type="LP"),
            _pos_event(event_type="CLOSE", position_type="LP"),
        ]
    )
    assert cell.status == "XFAIL"


def test_p1_xfails_on_lending_rows() -> None:
    """Same scope guard for the lending legs, which also emit OPEN/CLOSE."""
    cell = _p1(
        [
            _pos_event(event_type="OPEN", position_type="LENDING_COLLATERAL"),
            _pos_event(event_type="CLOSE", position_type="LENDING_DEBT"),
        ]
    )
    assert cell.status == "XFAIL"


def test_p1_xfails_when_no_position_events() -> None:
    """Nothing to score — XFAIL (unmeasured), never FAIL."""
    assert _p1([]).status == "XFAIL"
