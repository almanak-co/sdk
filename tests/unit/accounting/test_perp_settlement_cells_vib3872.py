"""VIB-3872 WI-4 — P3/P5 Accountant-Test cells score off the PERP_SETTLEMENT join.

On an ASYNC GMX round trip the submission ``PERP_CLOSE`` payload carries
measured-unavailable nulls (realized PnL / settled fees only exist after the keeper
fills the order — VIB-5717). The Phase-2 ``PERP_SETTLEMENT`` event carries the
measured economics; these tests pin that P3 (fee split) and P5 (realized PnL) now
PASS via the settlement join (by ``submission_ledger_entry_id`` ↔ the close row's
``ledger_entry_id``) while the inline-payload path and the XFAIL fallbacks stay
intact. Mutation-resistant: each direction is pinned.
"""

from __future__ import annotations

from typing import Any

from almanak.framework.accounting.accountant_test import _cells_perp


def _cell(cells: list[Any], cell_id: str) -> Any:
    return next(c for c in cells if c.cell_id == cell_id)


def _close(ledger_entry_id: str = "lc") -> dict[str, Any]:
    return {"id": "c1", "event_type": "PERP_CLOSE", "ledger_entry_id": ledger_entry_id}


def _settlement(ledger_entry_id: str = "lc") -> dict[str, Any]:
    return {"id": "s1", "event_type": "PERP_SETTLEMENT", "ledger_entry_id": ledger_entry_id}


def test_p3_p5_score_via_executed_settlement() -> None:
    acct_events = [_close(), _settlement()]
    acct_payloads = {
        # Realistic async submission close: measured-unavailable nulls.
        "c1": {"realized_pnl_usd": None, "close_fee_usd": None, "open_fee_usd": None},
        # The measured settlement (the honest source).
        "s1": {
            "settlement_state": "EXECUTED",
            "submission_ledger_entry_id": "lc",
            "realized_pnl_usd": "-30.96",
            "position_fee_usd": "2.87",
        },
    }
    cells = _cells_perp(acct_events, [], acct_payloads, {})
    assert _cell(cells, "P3").status == "PASS"
    assert "PERP_SETTLEMENT" in _cell(cells, "P3").diagnostic
    assert _cell(cells, "P5").status == "PASS"
    assert "PERP_SETTLEMENT" in _cell(cells, "P5").diagnostic


def test_p3_p5_xfail_without_settlement() -> None:
    acct_events = [_close()]
    acct_payloads = {"c1": {"realized_pnl_usd": None, "close_fee_usd": None, "open_fee_usd": None}}
    cells = _cells_perp(acct_events, [], acct_payloads, {})
    assert _cell(cells, "P3").status == "XFAIL"
    assert _cell(cells, "P5").status == "XFAIL"


def test_non_executed_settlement_does_not_score() -> None:
    # A CANCELLED/FROZEN settlement carries no measured fill → cells stay XFAIL.
    acct_events = [_close(), _settlement()]
    acct_payloads = {
        "c1": {"realized_pnl_usd": None, "close_fee_usd": None, "open_fee_usd": None},
        "s1": {
            "settlement_state": "CANCELLED",
            "submission_ledger_entry_id": "lc",
            "realized_pnl_usd": None,
            "position_fee_usd": None,
        },
    }
    cells = _cells_perp(acct_events, [], acct_payloads, {})
    assert _cell(cells, "P3").status == "XFAIL"
    assert _cell(cells, "P5").status == "XFAIL"


def test_unlinked_settlement_does_not_score() -> None:
    # A settlement whose submission link does not match the close is not credited.
    acct_events = [_close(ledger_entry_id="lc"), _settlement(ledger_entry_id="other")]
    acct_payloads = {
        "c1": {"realized_pnl_usd": None, "close_fee_usd": None, "open_fee_usd": None},
        "s1": {"settlement_state": "EXECUTED", "submission_ledger_entry_id": "other", "realized_pnl_usd": "-5"},
    }
    cells = _cells_perp(acct_events, [], acct_payloads, {})
    assert _cell(cells, "P5").status == "XFAIL"


def test_settlement_provenance_wins_over_inline() -> None:
    # FIX-2: when a close carries STALE inline economics AND a linked EXECUTED
    # settlement, the measured settlement is the honest source — the diagnostic must
    # name PERP_SETTLEMENT, not the inline payload. (Both score PASS; provenance is
    # the point.) Mutation-resistant: reverting to inline-first names PERP_*_PAYLOAD.
    acct_events = [_close(), _settlement()]
    acct_payloads = {
        "c1": {"realized_pnl_usd": "999.0", "close_fee_usd": "999.0", "open_fee_usd": "999.0"},
        "s1": {
            "settlement_state": "EXECUTED",
            "submission_ledger_entry_id": "lc",
            "realized_pnl_usd": "-30.96",
            "position_fee_usd": "2.87",
        },
    }
    cells = _cells_perp(acct_events, [], acct_payloads, {})
    assert _cell(cells, "P3").status == "PASS"
    assert "PERP_SETTLEMENT" in _cell(cells, "P3").diagnostic
    assert _cell(cells, "P5").status == "PASS"
    assert "PERP_SETTLEMENT" in _cell(cells, "P5").diagnostic


def test_inline_payload_still_scores() -> None:
    # Backward-compat: an inline close realized_pnl / fee still scores (no settlement).
    acct_events = [{"id": "c1", "event_type": "PERP_CLOSE", "ledger_entry_id": "lc"}]
    acct_payloads = {"c1": {"realized_pnl_usd": "5.0", "close_fee_usd": "0.5"}}
    cells = _cells_perp(acct_events, [], acct_payloads, {})
    assert _cell(cells, "P3").status == "PASS"
    assert "PERP_*_PAYLOAD" in _cell(cells, "P3").diagnostic
    assert _cell(cells, "P5").status == "PASS"
    assert "PERP_CLOSE.realized_pnl_usd" in _cell(cells, "P5").diagnostic
