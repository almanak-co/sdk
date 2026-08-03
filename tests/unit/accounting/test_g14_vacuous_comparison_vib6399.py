"""G14 must not report a comparison it never performed (VIB-6399 / VIB-6310).

``_cell_g14_sdk_eq_onchain`` ``continue``d past every row whose
``delta_vs_protocol_pct`` could not be read, then asserted
``f"all {len(position_state_rows)} position_state rows within 1bp of on-chain
state"``. On a run where every row is unmeasured it compared **zero** rows and
reported a successful comparison against on-chain state that never happened —
the Empty != Zero rule (blueprint 27) violated inside the validation layer
itself. VIB-6399 caught it on a Hyperliquid perp run whose report simultaneously
scored G14/G15 PASS "claiming Track C coverage" and P2/P4/P6 XFAIL "needs Track
C"; VIB-6310 caught the same cell passing over 121 lending rows with no
``health_factor``.

What these tests are built to catch, beyond the headline:

* **A fix that closes only the route the ticket names.** Three separate skip
  routes reach the vacuous PASS — ``None``, ``""``, and a present-but-unparseable
  value. VIB-6399 names only the first. ``test_every_unreadable_route_*``
  covers all three; closing one leaves a cell that still cannot fail.

* **A fix that is merely "always XFAIL".** A guard that never lets the cell PASS
  is as useless as one that never lets it FAIL, and it would satisfy a test that
  only asserted the defect case. ``test_a_measured_zero_delta_is_compared`` and
  ``test_a_real_breach_still_fails`` are the discriminating controls: a
  ``Decimal("0")`` delta is a MEASURED zero and must be compared and PASS, and a
  breach must still FAIL.

* **A blast radius wider than the defect.** The row-absent branch must keep
  returning XFAIL. If it were changed to SKIP, the six zero-row fixtures would
  each regress XFAIL->SKIP and the ratchet impact would grow from 3 fixtures to
  9 — for a condition those fixtures already report correctly.
  ``test_row_absent_branch_is_untouched`` and the corpus tests pin that.

* **Collateral movement in G15.** VIB-6399 names G15 as having "the same shape".
  Measured against the corpus it does not: G15 takes its real coverage branch in
  every rowful DB and its vacuous branch in none, so it is a genuine measurement
  today and is deliberately NOT changed here.
  ``test_g15_is_unchanged_and_still_measures_real_coverage`` pins that a later
  round does not "fix" it into agreement with the ticket.
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from almanak.framework.accounting.accountant_test import (
    _cell_g14_sdk_eq_onchain,
    run_against_sqlite,
)

_FIXTURE_BASE = Path(__file__).resolve().parents[2] / "fixtures" / "accounting"

# Fixture dir -> scoring profile, mirroring scripts/ci/check_accounting_ratchet.py.
_PROFILES = {
    "lp": "lp",
    "looping": "looping",
    "perp": "perp",
    "pendle_pt": "pendle_pt",
    "pendle_lp": "pendle_lp",
    "pendle_pt_held": "pendle_pt",
    "lp_curve": "curve_lp",
    "lp_curve_tricrypto": "curve_lp",
    "settlement": "settlement",
}

# Measured, not assumed: the only three fixtures carrying Track C ROWS. Each has
# exactly 2 rows and every delta is NULL, so each scored a vacuous G14 PASS
# before this change. The other six have zero rows and already XFAILed.
_ROWFUL_FIXTURES = ("pendle_lp", "lp_curve", "lp_curve_tricrypto")


def _row(delta):
    return {"id": 1, "position_key": "k", "delta_vs_protocol_pct": delta}


def _cell(fixture: str, cell_id: str):
    db = _FIXTURE_BASE / fixture / "expected_baseline.sqlite"
    report = run_against_sqlite(db, primitive=_PROFILES[fixture], strict_lifecycle=True)
    for cell in report.cells:
        if cell.cell_id == cell_id:
            return cell
    raise AssertionError(f"{cell_id} missing from {fixture} report")


# ── the defect: rows present, nothing comparable ────────────────────────────


@pytest.mark.parametrize(
    ("label", "delta"),
    [
        ("null", None),  # the route VIB-6399 names — the corpus-wide case
        ("empty_string", ""),  # the parser emitted nothing
        ("whitespace", "   "),  # ditto, and not caught by a bare `== ""`
        ("unparseable", "n/a"),  # present but not a number — NOT named in the ticket
        ("unparseable_dict", {"a": 1}),
    ],
)
def test_every_unreadable_route_refuses_to_pass(label: str, delta) -> None:
    """Each skip route must reach XFAIL, never PASS. Closing one is not enough."""
    result = _cell_g14_sdk_eq_onchain([], [_row(delta), _row(delta)])

    assert result.status == "XFAIL", (label, result.diagnostic)
    assert result.decomposition["rows_compared"] == "0", (label, result.decomposition)
    assert result.decomposition["rows_present"] == "2", (label, result.decomposition)
    # The old message asserted the strongest positive claim over an empty set.
    assert "within 1bp of on-chain state" not in result.diagnostic, result.diagnostic


def test_the_diagnostic_states_what_was_compared_not_what_was_present() -> None:
    """A flat assertion that omits the compared count cannot be told from an empty scan.

    This is VIB-6399's second acceptance criterion, and it is the property that
    would have made the defect visible without reading any logic: the old
    f-string interpolated ``len(position_state_rows)`` — the INPUT length — into
    a sentence about what had been verified.
    """
    result = _cell_g14_sdk_eq_onchain([], [_row(None), _row(None), _row(None)])

    assert "3 position_state row(s) present but 0 carried a comparable" in result.diagnostic
    # Every unreadable reason is counted separately (Empty != Zero); a merged
    # count would hide which producer stage is at fault.
    assert "unmeasured=3" in result.diagnostic
    assert "not-emitted=0" in result.diagnostic
    assert "unparseable=0" in result.diagnostic


def test_the_three_unreadable_reasons_are_counted_separately() -> None:
    result = _cell_g14_sdk_eq_onchain([], [_row(None), _row(""), _row("n/a")])

    assert result.status == "XFAIL", result.diagnostic
    assert result.decomposition["rows_null"] == "1", result.decomposition
    assert result.decomposition["rows_empty"] == "1", result.decomposition
    assert result.decomposition["rows_unparseable"] == "1", result.decomposition
    assert result.decomposition["rows_compared"] == "0", result.decomposition


# ── discriminating controls: the cell must still be able to PASS and to FAIL ──


@pytest.mark.parametrize("zero", ["0", "0.0", 0, Decimal("0")])
def test_a_measured_zero_delta_is_compared(zero) -> None:
    """``Decimal("0")`` is a MEASURED zero — a perfect match, not a missing value.

    This is the Empty != Zero line, and it is also what stops the fix degrading
    into "G14 never passes". A guard that swept measured zeros into the
    unmeasured bucket would XFAIL the single most likely shape of a genuinely
    correct on-chain reconciliation.
    """
    result = _cell_g14_sdk_eq_onchain([], [_row(zero)])

    assert result.status == "PASS", result.diagnostic
    assert result.decomposition["rows_compared"] == "1", result.decomposition
    assert result.decomposition["rows_null"] == "0", result.decomposition


def test_a_real_breach_still_fails() -> None:
    """Negative control: the cell retains the ability to FAIL on real data."""
    result = _cell_g14_sdk_eq_onchain([], [_row("0.5")])

    assert result.status == "FAIL", result.diagnostic
    assert result.decomposition["rows_compared"] == "1", result.decomposition


def test_a_breach_is_not_masked_by_unmeasured_neighbours() -> None:
    """One measured breach among unmeasured rows must still FAIL, not XFAIL.

    The zero-compared XFAIL is ranked BELOW the breach FAIL deliberately: if the
    ordering were reversed, adding unmeasured rows to a failing run would soften
    it, and XFAIL outranks FAIL on the ratchet.
    """
    result = _cell_g14_sdk_eq_onchain([], [_row("0.5"), _row(None), _row(None)])

    assert result.status == "FAIL", result.diagnostic
    assert result.decomposition["rows_compared"] == "1", result.decomposition
    assert result.decomposition["rows_null"] == "2", result.decomposition


def test_partial_coverage_passes_but_says_how_partial() -> None:
    """Some rows measured and in tolerance is a PASS — over the compared subset only."""
    result = _cell_g14_sdk_eq_onchain([], [_row("0.00001"), _row(None)])

    assert result.status == "PASS", result.diagnostic
    assert "1 of 2 position_state rows compared" in result.diagnostic
    assert result.decomposition["rows_compared"] == "1", result.decomposition


# ── blast radius: the row-absent branch must not move ────────────────────────


def test_row_absent_branch_is_untouched() -> None:
    """Zero rows keeps the original XFAIL, worded for Track C absence.

    Not cosmetic. Six of the nine frozen fixtures sit on this branch at XFAIL.
    Returning SKIP here instead would regress all six on the status ratchet
    (SKIP ranks below XFAIL) — nine fixtures moved to fix a defect present in
    three.
    """
    result = _cell_g14_sdk_eq_onchain([], [])

    assert result.status == "XFAIL", result.diagnostic
    assert "no position_state_snapshots rows for this run" in result.diagnostic


# ── corpus: the change moves exactly the rows it should ──────────────────────


def test_no_fixture_scores_a_vacuous_g14_pass() -> None:
    """The whole point: not one frozen fixture may report a comparison it did not make."""
    passing = [f for f in _PROFILES if _cell(f, "G14").status == "PASS"]

    assert passing == [], f"G14 PASSes on {passing} — every corpus delta is NULL"


@pytest.mark.parametrize("fixture", _ROWFUL_FIXTURES)
def test_rowful_fixtures_xfail_naming_the_uncompared_rows(fixture: str) -> None:
    """The three fixtures that DID score the vacuous PASS now say what happened.

    A discriminating control, not a smoke test: if a later change made G14 XFAIL
    unconditionally these would still pass, which is why
    ``test_a_measured_zero_delta_is_compared`` sits above.
    """
    cell = _cell(fixture, "G14")

    assert cell.status == "XFAIL", cell.diagnostic
    assert cell.decomposition["rows_present"] == "2", cell.decomposition
    assert cell.decomposition["rows_compared"] == "0", cell.decomposition
    assert cell.decomposition["rows_null"] == "2", cell.decomposition


@pytest.mark.parametrize(
    "fixture", [f for f in _PROFILES if f not in _ROWFUL_FIXTURES]
)
def test_zero_row_fixtures_take_the_absent_branch(fixture: str) -> None:
    """The other six carry no Track C rows and must stay on the untouched branch."""
    cell = _cell(fixture, "G14")

    assert cell.status == "XFAIL", cell.diagnostic
    assert "no position_state_snapshots rows for this run" in cell.diagnostic


# ── G15 must NOT move ────────────────────────────────────────────────────────


@pytest.mark.parametrize("fixture", _ROWFUL_FIXTURES)
def test_g15_is_unchanged_and_still_measures_real_coverage(fixture: str) -> None:
    """G15 keeps its PASS, and keeps earning it.

    VIB-6399 asserts G15 "has the same shape". It does not, and the distinction
    is the difference between a fix and a regression: G15 compares an expected
    open-position count against an actual Track C row count, and on these
    fixtures that is 2 real comparisons, not zero. Its vacuous
    "no snapshots reported open positions" branch fires nowhere in the corpus.
    Changing it would remove a working measurement to satisfy a ticket's
    phrasing.
    """
    cell = _cell(fixture, "G15")

    assert cell.status == "PASS", cell.diagnostic
    # The REAL coverage branch, not the "no snapshots reported open positions" one.
    assert "every snapshot with open positions has Track C coverage" in cell.diagnostic
    assert "no snapshots reported open positions" not in cell.diagnostic


# ── NaN: the value that constructs and then explodes ─────────────────────────


@pytest.mark.parametrize("nan", ["nan", "NaN", "-nan", "snan", float("nan")])
def test_a_nan_delta_is_unreadable_and_does_not_crash_the_report(nan) -> None:
    """A NaN must land in the unparseable bucket, not propagate out of the cell.

    Found by the Phase 4 UAT evaluator probing past the card's ``"n/a"``. The
    distinction that makes this its own case: ``"n/a"`` fails ``Decimal()``
    CONSTRUCTION and is caught, whereas ``Decimal("nan")`` constructs perfectly
    well and then raises ``InvalidOperation`` on ``abs(delta) > eps_pct`` — which
    sits outside the try. So the exception escaped ``_cell_g14_sdk_eq_onchain``
    and took the entire Accountant Test report down with it: not a wrong verdict,
    no verdict at all.

    This is in scope for the change that introduced the three-bucket taxonomy,
    because that taxonomy claims to classify every unreadable value and a NaN
    landed in none of them. `json.dumps(float("nan"))` emits bare ``NaN`` and a
    float pipeline emits ``nan``, so the input is not exotic.
    """
    result = _cell_g14_sdk_eq_onchain([], [_row(nan)])

    assert result.status == "XFAIL", result.diagnostic
    assert result.decomposition["rows_unparseable"] == "1", result.decomposition
    assert result.decomposition["rows_compared"] == "0", result.decomposition


@pytest.mark.parametrize("inf", ["inf", "-Infinity", "Infinity"])
def test_an_infinite_delta_is_a_real_breach_not_an_unreadable_value(inf) -> None:
    """Infinity is deliberately NOT swept into the unparseable bucket.

    ``abs(inf) > eps`` is well defined, so an infinite deviation from on-chain
    state is a comparison the cell CAN make, and the answer is "breached". Sweeping
    it in with NaN would convert a real failure into a soft XFAIL — the exact
    direction this whole change exists to prevent.
    """
    result = _cell_g14_sdk_eq_onchain([], [_row(inf)])

    assert result.status == "FAIL", result.diagnostic
    assert result.decomposition["rows_compared"] == "1", result.decomposition


def test_the_bucket_counts_always_partition_the_rows() -> None:
    """rows_present == compared + null + empty + unparseable, over a mixed input.

    The identity is what lets a reader trust the counts as a partition rather than
    four independent tallies. A row that reached none of the four buckets — which
    is what a NaN did before the guard above — breaks it.
    """
    rows = [_row(v) for v in (None, "", "   ", "n/a", "nan", float("nan"), "0", "0.5", 0)]
    result = _cell_g14_sdk_eq_onchain([], rows)
    d = result.decomposition

    total = (
        int(d["rows_compared"]) + int(d["rows_null"]) + int(d["rows_empty"]) + int(d["rows_unparseable"])
    )
    assert total == int(d["rows_present"]) == len(rows), d
    # 3 compared ("0", "0.5", 0); 1 null; 2 empty-ish; 3 unparseable.
    assert d["rows_compared"] == "3", d
    assert d["rows_null"] == "1", d
    assert d["rows_empty"] == "2", d
    assert d["rows_unparseable"] == "3", d
