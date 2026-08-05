"""G6 must not reconcile two different intervals (VIB-5854).

The wallet method brackets ``[priced[0] … priced[-1]]``; the component method
sums every typed row in the DB. When a ledger row predates ``priced[0]`` the two
methods measure different intervals, and the residue lands in ``gap`` with no
attribution.

Two things these tests are deliberately built to catch:

* **The inert version.** A guard that fires on nothing proves nothing, so the
  frozen corpus is used as a *discriminating* control: exactly one of the ten
  fixtures (``pendle_lp``, baseline 2s late) must flip, and the other nine must
  not move. A change that greened all ten, or none, would pass a weaker test.
* **The harmful version.** The tempting repair — window ``Σ_gas`` to the wallet
  bracket — makes the two numbers agree by dropping real spend from the
  component side too. ``test_gap_is_unchanged_by_the_guard`` pins that the
  arithmetic did NOT move, so a later "fix" that quietly windows the gas term
  turns this file red.
"""

from __future__ import annotations

import shutil
import sqlite3
from decimal import Decimal
from pathlib import Path

import pytest

from almanak.framework.accounting.accountant_test import (
    _baseline_window_coverage,
    run_against_sqlite,
)

_FIXTURE_BASE = Path(__file__).resolve().parents[2] / "fixtures" / "accounting"

# Fixture dir -> scoring profile, mirroring scripts/ci/check_accounting_ratchet.py.
_PROFILES = {
    # KNOWN LIMITATION (VIB-6569): this map MIRRORS
    # `check_accounting_ratchet._FIXTURE_SCORING_PROFILE` by hand. Nothing
    # enforces parity — a wrong entry scores a fixture under the wrong cell
    # pack and this test still passes, measuring something else. The durable
    # fix is to import the canonical map (as `_generate_post_t2_baselines.py`
    # now does); tracked in VIB-6569.
    "lp": "lp",
    "looping": "looping",
    "looping_debt_open": "looping",  # VIB-6560
    "perp": "perp",
    "pendle_pt": "pendle_pt",
    "pendle_lp": "pendle_lp",
    "pendle_pt_held": "pendle_pt",
    "lp_curve": "curve_lp",
    "lp_curve_tricrypto": "curve_lp",
    "settlement": "settlement",
}

# The ONE fixture whose baseline snapshot post-dates its first ledger row.
# Measured, not assumed: LP_OPEN at 03:21:59 vs priced[0] at 03:22:01.
_LATE_BASELINE_FIXTURE = "pendle_lp"


def _g6(primitive: str):
    db = _FIXTURE_BASE / primitive / "expected_baseline.sqlite"
    report = run_against_sqlite(db, primitive=_PROFILES[primitive], strict_lifecycle=True)
    for cell in report.cells:
        if cell.cell_id == "G6":
            return cell
    raise AssertionError(f"G6 absent from report for {primitive}")


# --------------------------------------------------------------- pure function


def test_endpoint_absent_is_unmeasured_not_covered() -> None:
    cov = _baseline_window_coverage(None, [{"timestamp": "2026-01-01T00:00:00+00:00"}])
    assert cov.measurable is False
    # Empty != Zero: no claim either way, and no fabricated 0 magnitude.
    assert cov.gas_before_usd is None


def test_unparseable_endpoint_timestamp_is_unmeasured() -> None:
    cov = _baseline_window_coverage({"timestamp": "not-a-timestamp"}, [{"timestamp": "2026-01-01T00:00:00+00:00"}])
    assert cov.measurable is False
    assert cov.gas_before_usd is None


def test_empty_ledger_covers_and_measures_zero() -> None:
    cov = _baseline_window_coverage({"timestamp": "2026-01-01T00:00:00+00:00"}, [])
    assert cov.measurable is True
    assert cov.covers is True
    # Measured zero, NOT unmeasured — the distinction the floor depends on.
    assert cov.gas_before_usd == Decimal(0)
    assert cov.late_by is None


def test_row_after_endpoint_does_not_trip() -> None:
    cov = _baseline_window_coverage(
        {"timestamp": "2026-01-01T00:00:00+00:00"},
        [{"timestamp": "2026-01-01T00:00:05+00:00", "gas_usd": "1.5"}],
    )
    assert cov.covers is True
    assert cov.rows_before == 0
    assert cov.gas_before_usd == Decimal(0)


def test_row_at_exactly_the_endpoint_does_not_trip() -> None:
    """Boundary: the comparison is ``<``, not ``<=``.

    ``lp_curve`` and ``lp_curve_tricrypto`` both carry a ledger row whose
    timestamp EQUALS their first priced snapshot, so an off-by-one here would
    flip two real fixtures and destroy the discriminating control below.
    """
    cov = _baseline_window_coverage(
        {"timestamp": "2026-01-01T00:00:00+00:00"},
        [{"timestamp": "2026-01-01T00:00:00+00:00", "gas_usd": "1.5"}],
    )
    assert cov.covers is True
    assert cov.gas_before_usd == Decimal(0)


def test_row_before_endpoint_trips_and_reports_magnitude() -> None:
    cov = _baseline_window_coverage(
        {"timestamp": "2026-01-01T00:00:10+00:00"},
        [
            {"timestamp": "2026-01-01T00:00:00+00:00", "gas_usd": "1.25"},
            {"timestamp": "2026-01-01T00:00:20+00:00", "gas_usd": "9.99"},
        ],
    )
    assert cov.covers is False
    assert cov.rows_before == 1
    # Only the pre-window row's gas — the post-window row must not be swept in.
    assert cov.gas_before_usd == Decimal("1.25")
    assert cov.late_by == "10s"


def test_untimed_ledger_row_is_unmeasured_never_before() -> None:
    """Empty != Zero, and the trap this guard must not inherit.

    ``evaluate_cells`` sorts with ``r.get("timestamp") or ""``, which sorts a row
    with no timestamp FIRST (VIB-6348). Reusing that convention here would make
    every untimed row look pre-window and fire the invariant on fixtures whose
    baseline is perfectly placed.
    """
    for missing in (None, ""):
        cov = _baseline_window_coverage(
            {"timestamp": "2026-01-01T00:00:10+00:00"},
            [{"timestamp": missing, "gas_usd": "1.25"}],
        )
        assert cov.covers is True, f"untimed ({missing!r}) row must not trip the invariant"
        assert cov.rows_before == 0
        assert cov.rows_without_timestamp == 1
        # Its gas is NOT booked into the pre-window magnitude either.
        assert cov.gas_before_usd == Decimal(0)


def test_pre_window_row_with_unmeasured_gas_is_counted_not_zeroed() -> None:
    cov = _baseline_window_coverage(
        {"timestamp": "2026-01-01T00:00:10+00:00"},
        [
            {"timestamp": "2026-01-01T00:00:00+00:00", "gas_usd": None},
            {"timestamp": "2026-01-01T00:00:01+00:00", "gas_usd": "2.00"},
        ],
    )
    assert cov.covers is False
    assert cov.rows_before == 2
    assert cov.gas_before_unmeasured_rows == 1
    # The measured row still contributes to the subtotal; the unmeasured one is
    # surfaced, not silently added as zero.
    assert cov.gas_before_usd == Decimal("2.00")
    # But the AGGREGATE is not measured — Empty != Zero applies to the total, not
    # only to its terms. A subtotal published as a total would let the ratchet
    # lock a floor below the true value, and would let the verdict call a gap
    # "explained" on partial evidence.
    assert cov.gas_before_complete is False
    assert cov.gas_before_measured is None


def test_fully_measured_pre_window_gas_is_a_real_aggregate() -> None:
    cov = _baseline_window_coverage(
        {"timestamp": "2026-01-01T00:00:10+00:00"},
        [{"timestamp": "2026-01-01T00:00:00+00:00", "gas_usd": "2.00"}],
    )
    assert cov.gas_before_complete is True
    assert cov.gas_before_measured == Decimal("2.00")


# ------------------------------------------------------- the frozen corpus


def test_exactly_one_fixture_has_a_late_baseline() -> None:
    """The discriminating control.

    If this ever reports zero, the guard has gone inert. If it reports every
    fixture in ``_PROFILES``, the guard has become a blanket XFAIL and the cell
    stops meaning anything.
    """
    late = [p for p in _PROFILES if _g6(p).status == "XFAIL" and "does not cover" in _g6(p).diagnostic]
    assert late == [_LATE_BASELINE_FIXTURE]


def test_late_baseline_fixture_xfails_with_the_magnitude_attributed() -> None:
    cell = _g6(_LATE_BASELINE_FIXTURE)
    assert cell.status == "XFAIL"
    decomp = cell.decomposition
    assert decomp["initial_endpoint_covers_run"] == "False"
    assert decomp["ledger_rows_before_initial_endpoint"] == "1"
    # 99.99% of this fixture's $0.70026 gap is the gas spent before the baseline.
    assert Decimal(decomp["gas_usd_before_initial_endpoint"]) == Decimal("0.70019296838")
    assert Decimal(decomp["gap_usd"]) - Decimal(decomp["gas_usd_before_initial_endpoint"]) < Decimal("0.0001")


def test_covered_fixtures_measure_zero_not_empty() -> None:
    """A covered window must record measured zero, never "" (unmeasured).

    The floor in ``expected_cells.json`` is only a guard if the value backing it
    is a measurement. An "" here would drop the floor for that fixture and the
    gate would report "no floor" rather than "floor satisfied".
    """
    for primitive in _PROFILES:
        if primitive == _LATE_BASELINE_FIXTURE:
            continue
        decomp = _g6(primitive).decomposition
        assert decomp["initial_endpoint_covers_run"] == "True", primitive
        assert decomp["gas_usd_before_initial_endpoint"] == "0", primitive


@pytest.mark.parametrize("primitive", ["lp_curve", "lp_curve_tricrypto"])
def test_unmeasured_null_buckets_still_fail_and_are_not_softened(primitive: str) -> None:
    """Precedence: the guard ranks BELOW the null check.

    An unmeasured component bucket is a real books gap and a more specific
    diagnosis. The window guard must never convert one into an XFAIL.
    """
    cell = _g6(primitive)
    assert cell.status == "FAIL"
    assert "unmeasured nulls" in cell.diagnostic


def test_gap_is_unchanged_by_the_guard() -> None:
    """The arithmetic must NOT move — this is a diagnosis, not a repair.

    Pins the committed floors. Windowing ``Σ_gas`` to the wallet bracket (the
    tempting repair VIB-5854 rejects) would shrink these gaps, so this assertion
    is what turns red if someone later ships it.
    """
    assert Decimal(_g6("pendle_lp").decomposition["gap_usd"]) == Decimal("0.700260884980000000000")
    assert Decimal(_g6("lp").decomposition["gap_usd"]) == Decimal("10.0")


# ------------------------------------------------- end-to-end negative control


def test_a_late_baseline_does_not_excuse_an_unrelated_gap(tmp_path: Path) -> None:
    """The waiver is bounded by what the late baseline actually EXPLAINS.

    This test asserted ``XFAIL`` in its first form, and that was wrong — the
    panel caught it. ``lp``'s gap is $10.00; injecting one pre-baseline row
    attributes $0.50 of it. Waiving the whole cell on 5% of the evidence is
    strictly WORSE than having no guard, because ``XFAIL`` outranks ``FAIL``:
    a run that fails on ``main`` today would have soft-passed here.

    So the mechanism must still fire (the window IS uncovered, and the magnitude
    IS attributed) while the verdict stays ``FAIL`` on the $9.50 residue, and the
    diagnostic must name BOTH defects — otherwise fixing the baseline reads as
    enough to close the cell when it is not.
    """
    src = _FIXTURE_BASE / "lp" / "expected_baseline.sqlite"
    db = tmp_path / "lp.sqlite"
    shutil.copy(src, db)

    before = run_against_sqlite(db, primitive="lp", strict_lifecycle=True)
    g6_before = next(c for c in before.cells if c.cell_id == "G6")
    assert g6_before.status == "FAIL"
    assert g6_before.decomposition["initial_endpoint_covers_run"] == "True"

    conn = sqlite3.connect(str(db))
    try:
        first_snap = conn.execute("SELECT MIN(timestamp) FROM portfolio_snapshots").fetchone()[0]
        target_id, gas = conn.execute(
            "SELECT id, gas_usd FROM transaction_ledger ORDER BY timestamp LIMIT 1"
        ).fetchone()
        # One second before the baseline — the same 1-3s shape seen in the real
        # captures (pendle_lp 2s, GMX R1 3s), not an exaggerated offset.
        moved = first_snap.replace("T", " ")[:19]
        conn.execute(
            "UPDATE transaction_ledger SET timestamp = datetime(?, '-1 second') WHERE id = ?",
            (moved, target_id),
        )
        conn.commit()
    finally:
        conn.close()

    after = run_against_sqlite(db, primitive="lp", strict_lifecycle=True)
    g6_after = next(c for c in after.cells if c.cell_id == "G6")
    decomp = g6_after.decomposition

    # The mechanism fired: the window is uncovered and the spend is attributed.
    assert decomp["initial_endpoint_covers_run"] == "False"
    assert decomp["ledger_rows_before_initial_endpoint"] == "1"
    assert Decimal(decomp["gas_usd_before_initial_endpoint"]) == Decimal(str(gas))

    # But it explains only $0.50 of a $10.00 gap, so the cell still FAILS on the
    # residue rather than waiving the run.
    assert g6_after.status == "FAIL"
    residual = Decimal(decomp["window_residual_usd"])
    # Against the SIGNED discrepancy, not the gap. This fixture's discrepancy is
    # already negative (wallet $3.00 - component $13.00), so the residue is
    # |-10.00 - 0.50| = $10.50, NOT gap - gas = $9.50. The earlier form of this
    # assertion encoded the sign-blind arithmetic and passed only because a $9.50
    # residue exceeds ε just as a $10.50 one does — the wrong number, right
    # verdict, which is exactly how the defect survived its own regression test.
    signed = Decimal(decomp["wallet_pnl_usd"]) - Decimal(decomp["component_pnl_usd"])
    assert signed < 0
    assert residual == abs(signed - Decimal(str(gas)))
    assert residual > Decimal(decomp["ε_threshold_usd"])

    # And the diagnostic names both defects — a reader must not come away
    # thinking the late baseline is the whole story.
    assert "does not cover" in g6_after.diagnostic
    assert "not explained by that spend" in g6_after.diagnostic
    # The residue is a quantity in its own right — it exceeds the $0.50 spend here
    # and can exceed the gap elsewhere, so the diagnostic must not call it a
    # portion of either. Two rounds of this message did exactly that.
    assert f"${residual} of" not in g6_after.diagnostic
    assert f"residual=${residual} > ε=" in g6_after.diagnostic
    assert "will NOT close this cell" in g6_after.diagnostic


def test_a_books_error_of_the_OPPOSITE_sign_is_not_waived(tmp_path: Path) -> None:
    """The residue is measured against the SIGNED discrepancy, not ``gap``.

    ``gap`` is already ``abs(wallet - component)``. The late-baseline mechanism
    has exactly one sign — pre-baseline gas is booked by the component method and
    is invisible to the wallet method, so it can only push ``wallet - component``
    positive by G. Testing ``|gap - G|`` therefore loses the sign, and understates
    the residue whenever an unrelated books error drives the discrepancy negative
    — always toward "explained", never away from it.

    At ``signed = -G`` the sign-blind form reports a residue of exactly ZERO while
    the true residue is ``2G``. Measured here on the $165k ``pendle_lp`` capture:
    a $1.40 error, 14x ε, certified "not a books error" and soft-passed as XFAIL.

    The first fix for the panel's finding passed on this fixture's natural
    (positive) sign and failed here — the whole defect was one unexercised axis.
    """
    src = _FIXTURE_BASE / "pendle_lp" / "expected_baseline.sqlite"
    db = tmp_path / "pendle_lp.sqlite"
    shutil.copy(src, db)

    # Raise the OPENING equity so wallet_pnl falls by ~2G: the discrepancy keeps
    # its magnitude and flips sign. Nothing about the ledger, the gas, or the
    # coverage measurement changes — only which way the residue points.
    conn = sqlite3.connect(str(db))
    try:
        rid, cash = conn.execute(
            "SELECT id, available_cash_usd FROM portfolio_snapshots ORDER BY iteration_number, timestamp LIMIT 1"
        ).fetchone()
        conn.execute(
            "UPDATE portfolio_snapshots SET available_cash_usd = ? WHERE id = ?",
            (str(Decimal(str(cash)) + Decimal("1.400318020160000000000")), rid),
        )
        conn.commit()
    finally:
        conn.close()

    cell = next(
        c for c in run_against_sqlite(db, primitive="pendle_lp", strict_lifecycle=True).cells if c.cell_id == "G6"
    )
    decomp = cell.decomposition
    signed = Decimal(decomp["wallet_pnl_usd"]) - Decimal(decomp["component_pnl_usd"])
    gas = Decimal(decomp["gas_usd_before_initial_endpoint"])

    # Precondition: the setup really did flip the sign, so this is not vacuous.
    assert signed < 0, f"setup failed to flip the sign (signed={signed})"
    assert decomp["initial_endpoint_covers_run"] == "False"

    # The residue is |signed - G|, NOT |gap - G|. The sign-blind form would report
    # ~0 here and waive the run.
    assert Decimal(decomp["window_residual_usd"]) == abs(signed - gas)
    assert Decimal(decomp["window_residual_usd"]) > Decimal(decomp["ε_threshold_usd"])

    assert cell.status == "FAIL"
    assert "not a books error" not in cell.diagnostic


def test_a_near_zero_gap_can_still_hide_a_large_residue(tmp_path: Path) -> None:
    """Two omissions can cancel, and the diagnostic must not misdescribe that.

    When the pre-baseline spend is offset by an opposite error, ``gap`` collapses
    toward zero while the residue stays large. The cell must still FAIL — and it
    must not print ``gap > ε`` for a gap that is comfortably UNDER ε, nor call the
    residue a part "of" a quantity smaller than itself.
    """
    src = _FIXTURE_BASE / "pendle_lp" / "expected_baseline.sqlite"
    db = tmp_path / "pendle_lp.sqlite"
    shutil.copy(src, db)

    conn = sqlite3.connect(str(db))
    try:
        rid, cash = conn.execute(
            "SELECT id, available_cash_usd FROM portfolio_snapshots ORDER BY iteration_number, timestamp LIMIT 1"
        ).fetchone()
        # Cancel the gap almost exactly: raise opening equity by the gap itself.
        conn.execute(
            "UPDATE portfolio_snapshots SET available_cash_usd = ? WHERE id = ?",
            (str(Decimal(str(cash)) + Decimal("0.700260884980000000000")), rid),
        )
        conn.commit()
    finally:
        conn.close()

    cell = next(
        c for c in run_against_sqlite(db, primitive="pendle_lp", strict_lifecycle=True).cells if c.cell_id == "G6"
    )
    decomp = cell.decomposition
    gap = Decimal(decomp["gap_usd"])
    eps = Decimal(decomp["ε_threshold_usd"])
    residual = Decimal(decomp["window_residual_usd"])

    # Precondition: the gap really did collapse below ε while the residue did not.
    assert gap < eps, f"setup failed to cancel the gap (gap={gap}, eps={eps})"
    assert residual > eps

    assert cell.status == "FAIL"
    # The threshold sentence must cite the quantity that actually decided it.
    assert f"residual=${residual} > ε=${eps}" in cell.diagnostic
    # And must never claim the gap exceeded ε when it did not.
    assert f"gap=${gap} > ε=" not in cell.diagnostic
    # The residue is not a part "of" a smaller gap.
    # Not "of it" — that string appears nowhere in the codebase, so asserting its
    # absence could never fail and read as coverage while protecting nothing. The
    # live risk is the residue being written as a portion of the spend or the gap.
    assert f"${residual} of that spend" not in cell.diagnostic
    assert f"${residual} of the gap" not in cell.diagnostic


def test_unmeasurable_pre_window_gas_cannot_explain_a_gap(tmp_path: Path) -> None:
    """ "Explained" needs a measured total, not a subtotal.

    With the pre-baseline row's own gas unmeasured, how much of the gap it
    accounts for is unknowable. The cell must refuse to waive on that evidence —
    an unmeasured aggregate is not a small one.
    """
    src = _FIXTURE_BASE / "pendle_lp" / "expected_baseline.sqlite"
    db = tmp_path / "pendle_lp.sqlite"
    shutil.copy(src, db)

    # pendle_lp's gap IS explained by its pre-baseline gas (that is why it XFAILs).
    # Blank only that row's gas: nothing else about the run changes.
    conn = sqlite3.connect(str(db))
    try:
        first_snap = conn.execute("SELECT MIN(timestamp) FROM portfolio_snapshots").fetchone()[0]
        conn.execute("UPDATE transaction_ledger SET gas_usd = '' WHERE timestamp < ?", (first_snap,))
        conn.commit()
    finally:
        conn.close()

    cell = next(
        c for c in run_against_sqlite(db, primitive="pendle_lp", strict_lifecycle=True).cells if c.cell_id == "G6"
    )
    assert cell.status == "FAIL"
    # Empty != Zero: the aggregate is unmeasured, not zero.
    assert cell.decomposition["gas_usd_before_initial_endpoint"] == ""
    assert cell.decomposition["gas_usd_before_initial_endpoint_unmeasured_count"] == "1"
    assert cell.decomposition["window_residual_usd"] == ""
    assert "cannot be established" in cell.diagnostic
    # And it must not then assert the very conclusion it just disclaimed. On this
    # run the gap sits three orders of magnitude UNDER ε, so telling an operator to
    # reconcile a residue would send them after an error that is probably not there
    # — the actionable fix here is to populate gas_usd.
    assert "reconcile the residue" not in cell.diagnostic
    assert "Populate gas_usd" in cell.diagnostic
    assert "TWO defects" not in cell.diagnostic
