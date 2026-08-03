"""Proving tests for the Accountant-Test ratchet's NUMERIC floor (VIB-4226 §1a).

Why this exists: the status ratchet compares an enum, and ``FAIL`` is the bottom
of its partial order. G6 — the reconciliation cell — sits at ``FAIL`` on 6 of the
9 frozen fixtures, so before this floor existed a reconciliation gap could grow
without bound and the gate stayed green. Measured on the debt-open fixture: a
$500 borrow misread as $500 of profit scores G6=FAIL gap=$501; correcting it
scores G6=FAIL gap=$1. Same status, 500x difference.

The tests below are pure-function tests over the two maps the gate builds, so
they need no SQLite and no framework import.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from scripts.ci.check_accounting_ratchet import (
    _classify_metric,
    evaluate_primitive_metrics,
)


def _one(manifest, live):
    verdicts = evaluate_primitive_metrics("looping", manifest, live)
    assert len(verdicts) == 1
    return verdicts[0]


# --------------------------------------------------------------- the core move
def test_growing_gap_is_a_regression():
    """THE motivating defect: the gap grew and the status did not move."""
    v = _one({"G6": {"gap_usd": "1.0"}}, {"G6": {"gap_usd": "501.0"}})
    assert v.verdict == "METRIC_REGRESSION"
    assert v.is_failure


def test_shrinking_gap_is_an_improvement_not_a_failure():
    v = _one({"G6": {"gap_usd": "501.0"}}, {"G6": {"gap_usd": "1.0"}})
    assert v.verdict == "METRIC_IMPROVED"
    assert not v.is_failure


def test_unchanged_gap_is_expected():
    v = _one({"G6": {"gap_usd": "8.002"}}, {"G6": {"gap_usd": "8.002"}})
    assert v.verdict == "EXPECTED"
    assert not v.is_failure


# ------------------------------------------------------------- fails CLOSED
def test_metric_the_cell_stopped_reporting_is_drift_not_a_pass():
    """A vanished metric must never read as 'floor satisfied'.

    This is the failure mode the whole gate exists to prevent, one level down:
    a cell that quietly stops emitting its gap would otherwise be indistinguishable
    from a cell whose gap is fine.
    """
    v = _one({"G6": {"gap_usd": "8.002"}}, {"G6": {}})
    assert v.verdict == "METRIC_DRIFT"
    assert v.is_failure


def test_cell_absent_from_live_report_is_drift():
    v = _one({"G6": {"gap_usd": "8.002"}}, {})
    assert v.verdict == "METRIC_DRIFT"
    assert v.is_failure


def test_undeclared_metric_key_fails_closed():
    """An undeclared metric has no known direction, so 'better' is undefined."""
    v = _one({"G6": {"totally_made_up": "1.0"}}, {"G6": {"totally_made_up": "0.0"}})
    assert v.verdict == "METRIC_DRIFT"
    assert v.is_failure


# "NaN-ish" is NOT a valid Decimal, but the bare "NaN" and "Infinity" literals
# ARE — Decimal parses them without raising. The first version of these cases
# carried only "NaN-ish" and so looked like it covered non-finite input while
# testing nothing of the sort. Both literals are pinned explicitly below.
_UNREADABLE = ["", None, "not-a-number", "NaN-ish", "NaN", "-NaN", "Infinity", "-Infinity"]


@pytest.mark.parametrize("bad", _UNREADABLE)
def test_unreadable_floor_fails_closed(bad):
    v = _one({"G6": {"gap_usd": bad}}, {"G6": {"gap_usd": "8.002"}})
    assert v.verdict == "METRIC_DRIFT"
    assert v.is_failure


@pytest.mark.parametrize("bad", _UNREADABLE)
def test_unreadable_live_value_fails_closed(bad):
    v = _one({"G6": {"gap_usd": "8.002"}}, {"G6": {"gap_usd": bad}})
    assert v.verdict == "METRIC_DRIFT"
    assert v.is_failure


@pytest.mark.parametrize("bad", ["NaN", "Infinity", "-Infinity"])
def test_non_finite_never_raises_out_of_the_gate(bad):
    """A non-finite input must produce a VERDICT, never an uncaught exception.

    Before this guard these raised ``InvalidOperation`` straight out of
    ``_classify_metric`` — from the ``max()`` in ``_metric_tolerance``, not from
    the multiply (``abs(Decimal("NaN")) * x`` returns ``NaN`` silently) — so the
    gate died with a stack trace instead of reporting drift. A crash is not a
    verdict. ``-Infinity`` as a live value is the opposite and worse case: it
    compared below any floor and was reported as ``METRIC_IMPROVED``.
    """
    for floor, live in ((bad, "1.0"), ("1.0", bad)):
        v = _classify_metric("looping", "G6", "gap_usd", floor, live)
        assert v.verdict == "METRIC_DRIFT"


@pytest.mark.parametrize(
    ("floor", "live"),
    [("-1.0", "0.5"), ("10.0", "-0.5"), ("-5", "-10")],
)
def test_negative_magnitude_fails_closed(floor, live):
    """A negative gap is a corrupted input, not an excellent score.

    G6 emits ``abs(wallet_pnl - component_pnl)``, so a negative can only reach
    the gate through a hand-edited manifest — where a negative floor would sit
    below every real value forever.
    """
    v = _classify_metric("looping", "G6", "gap_usd", floor, live)
    assert v.verdict == "METRIC_DRIFT"
    assert v.is_failure


# ------------------------------------------------------------------- tolerance
def test_representation_noise_is_absorbed():
    """1e-9 relative slack absorbs a last-digit change, not a real move."""
    v = _classify_metric("looping", "G6", "gap_usd", "10.0", "10.000000001")
    assert v.verdict == "EXPECTED"


def test_a_real_move_just_above_the_noise_floor_is_caught():
    v = _classify_metric("looping", "G6", "gap_usd", "10.0", "10.01")
    assert v.verdict == "METRIC_REGRESSION"


def test_zero_floor_still_catches_growth():
    """A perfect gap must not become an unfalsifiable floor via a 0-width band."""
    v = _classify_metric("looping", "G6", "gap_usd", "0", "0.0001")
    assert v.verdict == "METRIC_REGRESSION"


def test_zero_floor_tolerates_exact_zero():
    v = _classify_metric("looping", "G6", "gap_usd", "0", "0")
    assert v.verdict == "EXPECTED"


# ----------------------------------------------------------------- book-keeping
def test_only_manifest_named_metrics_are_checked():
    """A live decomposition carries dozens of diagnostics; flooring all of them
    would turn any incidental diagnostic change into a gate failure."""
    verdicts = evaluate_primitive_metrics(
        "looping",
        {"G6": {"gap_usd": "8.002"}},
        {"G6": {"gap_usd": "8.002", "wallet_pnl_usd": "5", "capital_usd": "1006"}},
    )
    assert [v.metric for v in verdicts] == ["gap_usd"]


def test_empty_manifest_metrics_checks_nothing():
    assert evaluate_primitive_metrics("looping", {}, {"G6": {"gap_usd": "1"}}) == []


def test_verdict_carries_the_numbers_for_the_operator():
    v = _one({"G6": {"gap_usd": "1.0"}}, {"G6": {"gap_usd": "501.0"}})
    assert v.floor == Decimal("1.0")
    assert v.live == Decimal("501.0")
    assert v.cell_id == "G6"
    assert v.primitive == "looping"
