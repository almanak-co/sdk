"""VIB-6345 — G5 must discriminate, and its delta must not change sign.

Before this suite, ``_cell_g5_initial_vs_current`` returned PASS whenever both
sides parsed. On mainnet R1 (2026-08-01) it reported ``+$5.709`` on a GMX perp
round-trip that lost ``$0.61`` on-chain, and passed — because it subtracted a
*deployed-only* baseline from an *equity* current value.

The numbers in ``R1_*`` below are the real persisted rows from
``docs/internal/gmx-readiness/r1-20260801/almanak_state-r1c.db``, not invented
ones: a test that fabricates the value production computes can green a
non-functional fix.
"""

from __future__ import annotations

from decimal import Decimal

from almanak.framework.accounting.accountant_test import _cell_g5_initial_vs_current

# The R1 capture, verbatim from portfolio_metrics / portfolio_snapshots.
R1_INITIAL = "4.995035872593559900814399999"
R1_SNAP0_DEPLOYED = "4.995035872593559900814399999"
R1_SNAP0_CASH = "6.0841818147029043624"
R1_SNAP_FINAL_CASH = "10.70438231735181704"


def _metrics(initial: str) -> list[dict[str, object]]:
    return [{"initial_value_usd": initial}]


def _snap(ts: str, deployed: object, cash: object) -> dict[str, object]:
    """A production-shaped snapshot row.

    ``value_confidence`` is included because the writer ALWAYS stamps it —
    measured across all nine committed fixtures, 0 rows carry a null or empty
    confidence. Omitting it here made these fixtures unfaithful to the real
    table and hid the UNAVAILABLE hole grok found on PR #3539.
    """
    return {
        "timestamp": ts,
        "total_value_usd": deployed,
        "available_cash_usd": cash,
        "value_confidence": "HIGH",
    }


def test_r1_or_drop_baseline_fails_and_names_the_dropped_cash() -> None:
    """The motivating defect. A gate that cannot fail here is decoration."""
    result = _cell_g5_initial_vs_current(
        _metrics(R1_INITIAL),
        [
            _snap("2026-08-01T18:26:45+00:00", R1_SNAP0_DEPLOYED, R1_SNAP0_CASH),
            _snap("2026-08-01T18:31:15+00:00", "0", R1_SNAP_FINAL_CASH),
        ],
        ledger=None,
    )
    assert result.status == "FAIL"
    assert "excludes the cash leg" in result.diagnostic
    # The operator must be told the size of the error, not just that one exists.
    assert "6.0841818147029043624" in result.diagnostic


def test_r1_delta_is_no_longer_wrong_signed() -> None:
    """R1 lost money. The reported delta must be negative.

    The pre-fix cell reported ``+5.709`` (current equity minus deployed-only
    baseline). Asserting only "FAIL" would let a fix ship that still prints a
    profit on a losing run, so the sign is pinned independently of the verdict.
    """
    result = _cell_g5_initial_vs_current(
        _metrics(R1_INITIAL),
        [
            _snap("2026-08-01T18:26:45+00:00", R1_SNAP0_DEPLOYED, R1_SNAP0_CASH),
            _snap("2026-08-01T18:31:15+00:00", "0", R1_SNAP_FINAL_CASH),
        ],
        ledger=None,
    )
    assert "delta=$-" in result.diagnostic, result.diagnostic
    assert "delta=$5.709" not in result.diagnostic


def test_coherent_baseline_passes() -> None:
    """A baseline equal to full opening equity is the healthy shape."""
    result = _cell_g5_initial_vs_current(
        _metrics("100"),
        [
            _snap("2026-05-09T00:00:00+00:00", "40", "60"),
            _snap("2026-05-09T01:00:00+00:00", "0", "103"),
        ],
        ledger=None,
    )
    assert result.status == "PASS", result.diagnostic
    assert "delta=$3" in result.diagnostic


def test_all_deployed_opening_is_not_a_false_positive() -> None:
    """``initial == deployed`` is CORRECT when there was no cash to drop.

    Without this case the signature would fire on every fully-deployed
    strategy, and the cell would be traded for a different false verdict.
    """
    result = _cell_g5_initial_vs_current(
        _metrics("100"),
        [
            _snap("2026-05-09T00:00:00+00:00", "100", "0"),
            _snap("2026-05-09T01:00:00+00:00", "105", "0"),
        ],
        ledger=None,
    )
    assert result.status == "PASS", result.diagnostic


def test_unmeasured_cash_does_not_fire_the_signature() -> None:
    """Empty ≠ Zero: an unmeasured cash column cannot establish a drop."""
    result = _cell_g5_initial_vs_current(
        _metrics("100"),
        [
            _snap("2026-05-09T00:00:00+00:00", "100", None),
            _snap("2026-05-09T01:00:00+00:00", "105", "0"),
        ],
        ledger=None,
    )
    assert result.status == "PASS", result.diagnostic
    assert "dropped the cash leg" not in result.diagnostic


def test_late_baseline_is_reported_on_an_otherwise_healthy_run() -> None:
    """A baseline captured after the first tx only ever understates cost.

    Reported rather than asserted — the delta is real, just short by whatever
    the first transaction already spent.
    """
    result = _cell_g5_initial_vs_current(
        _metrics("100"),
        [
            _snap("2026-05-09T00:00:05+00:00", "40", "60"),
            _snap("2026-05-09T01:00:00+00:00", "0", "103"),
        ],
        ledger=[{"timestamp": "2026-05-09T00:00:00+00:00"}],
    )
    assert result.status == "PASS", result.diagnostic
    assert "baseline is LATE" in result.diagnostic
    assert "by 5s" in result.diagnostic


def test_baseline_not_late_when_snapshot_precedes_the_first_ledger_row() -> None:
    result = _cell_g5_initial_vs_current(
        _metrics("100"),
        [
            _snap("2026-05-09T00:00:00+00:00", "40", "60"),
            _snap("2026-05-09T01:00:00+00:00", "0", "103"),
        ],
        ledger=[{"timestamp": "2026-05-09T00:00:30+00:00"}],
    )
    assert "baseline is LATE" not in result.diagnostic


def test_verdict_is_independent_of_sqlite_row_order() -> None:
    """``_table_rows`` issues no ORDER BY.

    VIB-6287 measured the same unordered-read trap on the position registry,
    where identical inputs yielded different answers depending on row order.
    A PnL delta whose endpoints move with SQLite's row order is not a
    measurement.
    """
    snapshots = [
        _snap("2026-05-09T00:00:00+00:00", "40", "60"),
        _snap("2026-05-09T01:00:00+00:00", "0", "103"),
    ]
    forward = _cell_g5_initial_vs_current(_metrics("100"), snapshots, ledger=None)
    reverse = _cell_g5_initial_vs_current(_metrics("100"), list(reversed(snapshots)), ledger=None)
    assert forward.status == reverse.status
    assert forward.diagnostic == reverse.diagnostic


def test_unmeasured_opening_equity_fails_rather_than_assuming_zero() -> None:
    result = _cell_g5_initial_vs_current(
        _metrics("100"),
        [
            _snap("2026-05-09T00:00:00+00:00", None, None),
            _snap("2026-05-09T01:00:00+00:00", "0", "103"),
        ],
        ledger=None,
    )
    assert result.status == "FAIL"
    assert "no measured equity" in result.diagnostic


def test_dust_cash_does_not_manufacture_a_failure() -> None:
    """Sub-cent cash is not a discarded leg."""
    result = _cell_g5_initial_vs_current(
        _metrics("100"),
        [
            _snap("2026-05-09T00:00:00+00:00", "100", str(Decimal("0.001"))),
            _snap("2026-05-09T01:00:00+00:00", "101", "0"),
        ],
        ledger=None,
    )
    assert result.status == "PASS", result.diagnostic


# ---------------------------------------------------------------------------
# Panel round 1 (PR #3539): three findings from codex + grok, all confirmed
# against the writer's real contracts before being fixed.
# ---------------------------------------------------------------------------


def _snap_c(ts: str, deployed: object, cash: object, confidence: object = "HIGH") -> dict[str, object]:
    """``_snap`` with an explicit confidence stamp (including absent/unknown)."""
    s = _snap(ts, deployed, cash)
    s["value_confidence"] = confidence
    return s


def test_leading_unavailable_snapshot_cannot_become_the_pnl_opening() -> None:
    """grok HIGH. An UNAVAILABLE row is the runner's FAILURE contract, not a zero.

    ``runner_state._make_unavailable_snapshot`` stamps ``total_value_usd=0`` AND
    ``available_cash_usd=0`` so the equity curve has no holes, and that row is
    persisted even though it never establishes ``initial_value_usd``. Anchoring
    the opening endpoint to it reports the entire final balance as profit — the
    same wrong-signed PASS this cell exists to kill, re-entered by a new door.

    Without the confidence filter this returns PASS with delta=+$10.70 on a
    book that made nothing.
    """
    result = _cell_g5_initial_vs_current(
        _metrics(R1_INITIAL),
        [
            _snap_c("2026-08-01T18:26:00+00:00", "0", "0", "UNAVAILABLE"),
            _snap_c("2026-08-01T18:26:45+00:00", R1_SNAP0_DEPLOYED, R1_SNAP0_CASH),
            _snap_c("2026-08-01T18:31:15+00:00", "0", R1_SNAP_FINAL_CASH),
        ],
        ledger=None,
    )
    assert "delta=$10.7" not in result.diagnostic, result.diagnostic
    # The UNAVAILABLE row is skipped, so the real opening is snapshot 2 and the
    # or-drop signature is reachable again.
    assert result.status == "FAIL"
    assert "excludes the cash leg" in result.diagnostic


def test_all_unavailable_snapshots_fail_rather_than_fabricate_an_opening() -> None:
    result = _cell_g5_initial_vs_current(
        _metrics("100"),
        [
            _snap_c("2026-05-09T00:00:00+00:00", "0", "0", "UNAVAILABLE"),
            _snap_c("2026-05-09T01:00:00+00:00", "0", "0", "UNAVAILABLE"),
        ],
        ledger=None,
    )
    assert result.status == "FAIL"
    assert "anchor a PnL endpoint" in result.diagnostic


def test_missing_or_unknown_confidence_cannot_anchor() -> None:
    """Empty != Zero applies to the confidence stamp itself."""
    for bad in ("", None, "WAT"):
        result = _cell_g5_initial_vs_current(
            _metrics("100"),
            [
                _snap_c("2026-05-09T00:00:00+00:00", "40", "60", bad),  # type: ignore[arg-type]
                _snap_c("2026-05-09T01:00:00+00:00", "0", "103"),
            ],
            ledger=None,
        )
        # Assert the VERDICT, not the absence of one string. The previous
        # revision asserted only `"delta=$103" not in diagnostic`, which held
        # on the UN-fixed code too (the unfiltered delta is $3, not $103), so
        # the missing/unknown limb of this guard shipped with no negative
        # control at all. Found by the #3539 delta review.
        assert result.status == "FAIL", f"{bad!r}: {result.diagnostic}"
        assert "anchor a PnL endpoint" in result.diagnostic, f"{bad!r}: {result.diagnostic}"


def test_stale_confidence_still_anchors() -> None:
    """STALE was measured, just old. Freshness is G9's job, not G5's."""
    result = _cell_g5_initial_vs_current(
        _metrics("100"),
        [
            _snap_c("2026-05-09T00:00:00+00:00", "40", "60", "STALE"),
            _snap_c("2026-05-09T01:00:00+00:00", "0", "103", "STALE"),
        ],
        ledger=None,
    )
    assert result.status == "PASS", result.diagnostic
    assert "delta=$3" in result.diagnostic


def test_every_value_confidence_member_is_classified() -> None:
    """Census. A new ValueConfidence member must be decided, not defaulted.

    Defaulting a new member to "cannot anchor" would quietly disable G5; to
    "can anchor" would quietly re-open the UNAVAILABLE hole. Either way the
    silence is the bug, so this fails until someone chooses.
    """
    from almanak.framework.accounting.accountant_test import (
        _G5_ANCHORING_CONFIDENCES,
        _G5_REFUSED_CONFIDENCES,
    )
    from almanak.framework.portfolio.models import ValueConfidence

    members = {m.value for m in ValueConfidence}
    assert members == _G5_ANCHORING_CONFIDENCES | _G5_REFUSED_CONFIDENCES, (
        f"unclassified ValueConfidence member(s): "
        f"{members - (_G5_ANCHORING_CONFIDENCES | _G5_REFUSED_CONFIDENCES)}. "
        f"Decide whether each may anchor a G5 PnL endpoint."
    )
    # Disjointness is the half the previous revision missed: it kept its own
    # copy of the refused set, so moving UNAVAILABLE INTO the anchoring set
    # left the union unchanged and the census still passed.
    assert not (_G5_ANCHORING_CONFIDENCES & _G5_REFUSED_CONFIDENCES), (
        f"confidence classified both ways: {_G5_ANCHORING_CONFIDENCES & _G5_REFUSED_CONFIDENCES}"
    )


def test_unorderable_timestamps_fail_rather_than_use_sqlite_row_order() -> None:
    """grok MEDIUM. Falling back to caller order was the wrong direction.

    ``_table_rows`` issues no ORDER BY, so the "preserved historical behaviour"
    was SQLite row order. With BOTH endpoints now positional, that could PASS
    with a flipped delta. Refusing to order is information, not a detail.
    """
    result = _cell_g5_initial_vs_current(
        _metrics("4.99"),
        [
            _snap_c("2026-08-01T18:31:15+00:00", "0", "10.70"),
            _snap_c("", "4.99", "6.08"),
        ],
        ledger=None,
    )
    assert result.status == "FAIL"
    assert "cannot order snapshots" in result.diagnostic


def test_or_drop_diagnostic_does_not_claim_the_defect_exclusively() -> None:
    """codex P2 + grok MEDIUM, converged independently.

    The DB carries no baseline-provenance column, so this shape is produced by
    BOTH the VIB-6349 defect and the VIB-3882 allocation contract. The cell
    still FAILs — loud is the correct direction for a gate — but it must not
    assert a cause it cannot know. Accepted tradeoff, pinned here.
    """
    result = _cell_g5_initial_vs_current(
        _metrics("100"),
        [
            _snap_c("2026-05-09T00:00:00+00:00", "100", "15"),
            _snap_c("2026-05-09T01:00:00+00:00", "0", "118"),
        ],
        ledger=None,
    )
    assert result.status == "FAIL"
    assert "VIB-6349" in result.diagnostic
    assert "VIB-3882" in result.diagnostic
    assert "allocation_usd" in result.diagnostic
    # Must not state the defect as fact.
    assert "dropped the cash leg" not in result.diagnostic


def test_exact_confidence_vocabulary_is_not_case_folded() -> None:
    """A folded stamp re-trusts what ValueConfidence.parse deliberately refuses.

    ``portfolio/models.py`` is explicit: "accepting different casing or
    surrounding whitespace would turn an unknown boundary value into a trusted
    one". ``ValueConfidence.parse('high')`` raises. G5 must agree.
    """
    for folded in ("high", " HIGH ", "High"):
        result = _cell_g5_initial_vs_current(
            _metrics("100"),
            [
                _snap_c("2026-05-09T00:00:00+00:00", "40", "60", folded),
                _snap_c("2026-05-09T01:00:00+00:00", "0", "103"),
            ],
            ledger=None,
        )
        assert result.status == "FAIL", f"{folded!r} was trusted: {result.diagnostic}"


def test_single_measured_snapshot_fails_and_says_nothing_was_refused() -> None:
    """The real single-snapshot run — the case that changed user-visible behaviour.

    The delta review flagged that only the all-UNAVAILABLE path covered this,
    so the diagnostic claimed "the rest are unmeasured" when nothing had been
    refused at all — the same assert-a-cause-you-cannot-know defect this PR
    removed from the or-drop message.
    """
    result = _cell_g5_initial_vs_current(
        _metrics("100"),
        [_snap_c("2026-05-09T00:00:00+00:00", "40", "60")],
        ledger=None,
    )
    assert result.status == "FAIL"
    assert "have 1 of 1" in result.diagnostic
    assert "none were refused" in result.diagnostic


def test_trailing_unavailable_row_is_disclosed_not_silently_dropped() -> None:
    """current must not silently become a stale row.

    Dropping a trailing 0/0 UNAVAILABLE row is correct — using it would
    fabricate a total loss — but the delta then stops at the last MEASURED
    point, and a cell that reports a number as if it covered the whole run is
    the failure mode this cell exists to kill, at lower amplitude.
    """
    result = _cell_g5_initial_vs_current(
        _metrics("100"),
        [
            _snap_c("2026-05-09T00:00:00+00:00", "100", "0"),
            _snap_c("2026-05-09T01:00:00+00:00", "50", "0"),
            _snap_c("2026-05-09T02:00:00+00:00", "0", "0", "UNAVAILABLE"),
        ],
        ledger=None,
    )
    assert "NOTE current is NOT the run's last snapshot" in result.diagnostic
    assert "delta=$-50" in result.diagnostic
