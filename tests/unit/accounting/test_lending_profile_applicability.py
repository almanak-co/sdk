"""Declarative applicability contract for lending scorecard L6.

The same accounting evidence must not decide its own applicability. A pure
SUPPLY → BORROW → REPAY → WITHDRAW lifecycle has no SWAP by design, while a
leverage loop with the same coarse lending lifecycle requires a borrow→swap
leg. The selected scorecard profile is the authority: absence of a SWAP is
never inspected to turn a failure into N/A.
"""

from __future__ import annotations

from almanak.framework.accounting.accountant_test import SCORECARD_PROFILES
from almanak.framework.accounting.scorecard_profiles import ScorecardCtx


def _l6(profile_name: str, *, swap_pnl: str | None | object = ...):
    acct_events = [
        {"id": 1, "event_type": "SUPPLY"},
        {"id": 2, "event_type": "BORROW"},
        {"id": 3, "event_type": "REPAY"},
        {"id": 4, "event_type": "WITHDRAW"},
    ]
    payloads = {
        1: {"asset": "USDC"},
        2: {"asset": "USDT"},
        3: {"asset": "USDT", "principal_repaid_usd": "1", "interest_paid_usd": "0"},
        4: {"asset": "USDC", "interest_accrued_usd": "0"},
    }
    if swap_pnl is not ...:
        acct_events.append({"id": 5, "event_type": "SWAP"})
        payloads[5] = {"token_in": "USDT", "realized_pnl_usd": swap_pnl}
    ctx = ScorecardCtx(
        pos_events=[],
        acct_events=acct_events,
        snapshots=[],
        acct_payloads=payloads,
        payload_errors={},
        position_state_rows=[],
    )
    cells = SCORECARD_PROFILES[profile_name].cells(ctx)
    return next(cell for cell in cells if cell.cell_id == "L6")


def test_same_no_swap_evidence_fails_looping_but_skips_explicit_lending_lifecycle() -> None:
    """Profile selection, not observed SWAP absence, controls L6 applicability."""
    looping = _l6("looping")
    lending_lifecycle = _l6("lending_lifecycle")

    assert looping.status == "FAIL"
    assert "never appeared as SWAP.token_in" in looping.diagnostic

    assert lending_lifecycle.status == "SKIP"
    assert "explicit lending-lifecycle profile" in lending_lifecycle.diagnostic


def test_lending_lifecycle_scores_an_observed_borrow_to_swap_leg() -> None:
    """A misdeclared pure-lending run cannot hide actual loop attribution."""
    measured = _l6("lending_lifecycle", swap_pnl="0")
    unmeasured = _l6("lending_lifecycle", swap_pnl=None)

    assert measured.status == "PASS"
    assert unmeasured.status == "FAIL"
    assert "realized_pnl_usd=null" in unmeasured.diagnostic
