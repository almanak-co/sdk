"""Unit tests for the PEN3 cell (open-PT mark-to-market) in
``almanak.framework.accounting.accountant_test``.

Background — VIB-5276: PEN3 ("open-PT mark-to-market / unrealised discount
accretion") was a HARDCODED ``XFAIL`` while the gateway PT implied-price path
was unwired. That path is now live (gateway ``GetPtPrice`` composing
``PT/USD = pt_to_asset_rate × underlying/USD``, VIB-5310; consumed by the
portfolio valuer's FIFO-inventory path, VIB-5316), surfacing an open Pendle PT as
a ``positions_json`` row tagged ``details.source == "pt_inventory_lots"``. PEN3
now READS that row.

These tests pin the Empty ≠ Zero contract the predicate enforces:

* a MEASURED open-PT row (``value_usd`` present, no ``mark_unmeasured`` flag) →
  PASS — and STALE / ESTIMATED ``price_confidence`` is still a measured mark;
* an honest-UNMEASURED row (``mark_unmeasured`` flag) → XFAIL citing the reason,
  NEVER FAIL;
* no open-PT inventory row at all → XFAIL ("nothing to mark"), NEVER FAIL;
* malformed ``positions_json`` → never FAIL (surfaced as a diagnostic note);
* the LATEST snapshot bearing a PT row wins.
"""

from __future__ import annotations

import json
from typing import Any

from almanak.framework.accounting.accountant_test import (
    _open_pt_inventory_rows,
    _pen3_open_pt_cell,
)

PT = "PT-wstETH-25JUN2026"


def _pt_inventory_position(
    *,
    value_usd: str = "26.51",
    cost_basis_usd: str = "21.44",
    unrealized_pnl_usd: str = "5.07",
    confidence: str = "HIGH",
    mark_unmeasured: bool = False,
    cost_basis_unmeasured: bool = False,
    unavailable_reason: str | None = None,
) -> dict[str, Any]:
    """A ``pt_inventory_lots`` position dict shaped like the portfolio valuer's
    ``_classify_pt_inventory`` / ``_pt_unmeasured_row`` output.

    Two distinct unmeasured shapes the valuer emits (Empty ≠ Zero):
      * ``mark_unmeasured`` — gateway price UNAVAILABLE (``_pt_unmeasured_row``);
        mark + cost + PnL all unmeasured.
      * ``cost_basis_unmeasured`` — the mark IS measured but the buy-time USD cost
        leg is missing (``_classify_pt_inventory`` ``cost_usd is None`` branch);
        cost + PnL carry a placeholder 0 paired with the flag.
    """
    details: dict[str, Any] = {
        "asset": PT,
        "pt_symbol": PT,
        "source": "pt_inventory_lots",
        "classification": "deployed_inventory",
        "quantity": "0.0123",
        "price_confidence": confidence,
        "price_source": "composition:getPtToAssetRate×aggregated",
    }
    if mark_unmeasured:
        details["mark_unmeasured"] = True
        details["cost_basis_unmeasured"] = True
        details["unrealized_pnl_unmeasured"] = True
        details["unavailable_reason"] = unavailable_reason or "price_unmeasured"
    elif cost_basis_unmeasured:
        details["cost_basis_unmeasured"] = True
        details["unrealized_pnl_unmeasured"] = True
        if unavailable_reason is not None:
            details["unavailable_reason"] = unavailable_reason
    return {
        "position_type": "TOKEN",
        "protocol": "pt",
        "chain": "arbitrum",
        "value_usd": value_usd,
        "label": f"PT inventory {PT}",
        "tokens": [PT],
        "details": details,
        "cost_basis_usd": cost_basis_usd,
        "unrealized_pnl_usd": unrealized_pnl_usd,
    }


def _snapshot(positions: list[dict[str, Any]], *, iteration: int, sid: str) -> dict[str, Any]:
    envelope = {"schema_version": 1, "positions": positions, "metadata": {}}
    return {"id": sid, "iteration_number": iteration, "positions_json": json.dumps(envelope)}


def _pen3(snapshots: list[dict[str, Any]]) -> Any:
    cell = _pen3_open_pt_cell(snapshots)
    assert cell.cell_id == "PEN3"
    return cell


class TestOpenPtInventoryRows:
    def test_extracts_pt_rows_iteration_ordered(self) -> None:
        snaps = [
            _snapshot([_pt_inventory_position(value_usd="20.0")], iteration=4, sid="s4"),
            _snapshot([_pt_inventory_position(value_usd="10.0")], iteration=1, sid="s1"),
        ]
        rows, unreadable = _open_pt_inventory_rows(snaps)
        assert not unreadable
        assert [r["value_usd"] for r in rows] == ["10.0", "20.0"]  # oldest→newest

    def test_malformed_json_flags_unreadable_not_crash(self) -> None:
        snaps = [{"id": "s1", "iteration_number": 1, "positions_json": "{not json"}]
        rows, unreadable = _open_pt_inventory_rows(snaps)
        assert rows == []
        assert unreadable is True

    def test_ignores_non_pt_positions(self) -> None:
        non_pt = {"position_type": "LP", "details": {"source": "uniswap_v3"}, "value_usd": "5"}
        snaps = [_snapshot([non_pt], iteration=1, sid="s1")]
        rows, unreadable = _open_pt_inventory_rows(snaps)
        assert rows == []
        assert not unreadable


class TestPen3Cell:
    def test_measured_pt_row_passes(self) -> None:
        snaps = [_snapshot([_pt_inventory_position()], iteration=1, sid="s1")]
        cell = _pen3(snaps)
        assert cell.status == "PASS"
        assert "value_usd=26.51" in cell.diagnostic

    def test_stale_confidence_still_passes(self) -> None:
        """STALE / ESTIMATED is a measured mark — Empty ≠ Zero keys on the
        mark_unmeasured flag, not on price_confidence. (Exact shape of the
        frozen pendle_pt fixture row.)"""
        snaps = [_snapshot([_pt_inventory_position(confidence="STALE")], iteration=1, sid="s1")]
        cell = _pen3(snaps)
        assert cell.status == "PASS"
        assert "confidence=STALE" in cell.diagnostic

    def test_unmeasured_pt_row_xfails_never_fails(self) -> None:
        pos = _pt_inventory_position(
            value_usd="0",
            cost_basis_usd="0",
            unrealized_pnl_usd="0",
            mark_unmeasured=True,
            unavailable_reason="price_unmeasured",
        )
        snaps = [_snapshot([pos], iteration=1, sid="s1")]
        cell = _pen3(snaps)
        assert cell.status == "XFAIL"
        assert cell.status != "FAIL"
        assert "price_unmeasured" in cell.diagnostic

    def test_measured_mark_but_unmeasured_cost_basis_xfails(self) -> None:
        """Gemini high-pri (PR #3010): the valuer can price the mark yet flag
        cost_basis_unmeasured when the buy-time USD cost leg is missing — the
        persisted cost_basis_usd / unrealized_pnl_usd are then placeholder 0
        (Empty ≠ Zero). PEN3's claim is the *unrealised* discount accretion, so a
        measured mark with an unmeasured cost basis must XFAIL, never PASS with a
        fabricated $0 PnL."""
        pos = _pt_inventory_position(
            value_usd="26.51",
            cost_basis_usd="0",
            unrealized_pnl_usd="0",
            confidence="HIGH",
            cost_basis_unmeasured=True,
        )
        snaps = [_snapshot([pos], iteration=1, sid="s1")]
        cell = _pen3(snaps)
        assert cell.status == "XFAIL"
        assert cell.status != "FAIL"
        assert "cost_basis_unmeasured" in cell.diagnostic

    def test_no_pt_inventory_row_xfails(self) -> None:
        non_pt = {"position_type": "LP", "details": {"source": "uniswap_v3"}, "value_usd": "5"}
        snaps = [_snapshot([non_pt], iteration=1, sid="s1")]
        cell = _pen3(snaps)
        assert cell.status == "XFAIL"
        assert "nothing to mark-to-market" in cell.diagnostic

    def test_malformed_positions_json_does_not_fail(self) -> None:
        snaps = [{"id": "s1", "iteration_number": 1, "positions_json": "{not json"}]
        cell = _pen3(snaps)
        assert cell.status == "XFAIL"
        assert cell.status != "FAIL"
        assert "malformed positions_json" in cell.diagnostic

    def test_latest_snapshot_wins(self) -> None:
        """An earlier unmeasured row + a later measured row → PASS (reads newest)."""
        early = _pt_inventory_position(
            value_usd="0", mark_unmeasured=True, unavailable_reason="price_unmeasured"
        )
        late = _pt_inventory_position(value_usd="26.51", confidence="STALE")
        snaps = [
            _snapshot([late], iteration=4, sid="s4"),
            _snapshot([early], iteration=1, sid="s1"),
        ]
        cell = _pen3(snaps)
        assert cell.status == "PASS"
