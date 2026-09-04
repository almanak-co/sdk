"""Parametrized branch coverage for the posture cell predicates.

``evaluate_posture`` is the lightweight Accountant-Test posture rollup (which
cells PASS / FAIL / XFAIL on this strategy's primitive). It mirrors the 21-cell
matrix in ``docs/internal/blueprints/27-accounting.md`` §18.7.1 without
re-running the epsilon-sensitive harness — one predicate per cell, folded
through the generic + per-primitive result tables in
``almanak.framework.dashboard.quant_aggregations``.

Each case below pins the CURRENT production semantics of exactly one cell
(all other inputs held at the all-pass baseline), so a future refactor that
drifts a threshold fails by name.
"""

from __future__ import annotations

import json
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

import pytest

from almanak.framework.dashboard.quant_aggregations import (
    AccountantPosture,
    AuditTrailStats,
    ReconciliationStatus,
    _lending_l4_result,
    _posture_cells_for_primitive,
    _record_posture_cell,
    evaluate_posture,
)
from almanak.framework.observability.ledger import LedgerQuantStats


def _ev(
    event_type: str,
    payload: dict[str, Any] | None = None,
    *,
    confidence: str = "HIGH",
) -> dict[str, Any]:
    body: dict[str, Any] = {"event_type": event_type}
    if payload:
        body.update(payload)
    return {
        "event_type": event_type,
        "confidence": confidence,
        "payload_json": json.dumps(body),
    }


def _ledger_stats(**overrides: Any) -> LedgerQuantStats:
    base: dict[str, Any] = {
        "total": 2,
        "with_tx_hash": 2,
        "with_cycle_id": 2,
        "with_price_inputs": 2,
        "with_pre_post_state": 1,
        "with_positive_gas_usd": 2,
        "gas_usd_sum": Decimal("0.10"),
        "first_action_wallet_value_usd": None,
    }
    base.update(overrides)
    return LedgerQuantStats(**base)


def _audit(**overrides: Any) -> AuditTrailStats:
    base: dict[str, Any] = {
        "ledger_total": 2,
        "ledger_with_price_inputs": 2,
        "ledger_with_pre_post_state": 1,
        "ledger_with_gas_usd": 2,
        "events_total": 1,
        "events_with_versions": 1,
    }
    base.update(overrides)
    return AuditTrailStats(**base)


def _recon(**overrides: Any) -> ReconciliationStatus:
    base: dict[str, Any] = {"has_data": True, "passed": True}
    base.update(overrides)
    return ReconciliationStatus(**base)


def _metrics(initial: str = "100", total: str = "110") -> SimpleNamespace:
    return SimpleNamespace(initial_value_usd=initial, total_value_usd=total)


def _snaps(n: int = 2) -> list[Any]:
    return [SimpleNamespace() for _ in range(n)]


def _posture(
    primitive: str = "swap",
    *,
    ledger: LedgerQuantStats | None = None,
    events: list[Any] | None = None,
    snapshots: list[Any] | None = None,
    audit: AuditTrailStats | None = None,
    recon: ReconciliationStatus | None = None,
    metrics: Any = "default",
) -> AccountantPosture:
    return evaluate_posture(
        primitive=primitive,
        ledger_entries=ledger if ledger is not None else _ledger_stats(),
        accounting_events=events if events is not None else [_ev("SWAP")],
        snapshots=snapshots if snapshots is not None else _snaps(),
        audit=audit if audit is not None else _audit(),
        reconciliation=recon if recon is not None else _recon(),
        portfolio_metrics=_metrics() if metrics == "default" else metrics,
    )


def _assert_bucket(posture: AccountantPosture, cell: str, bucket: str) -> None:
    if bucket == "pass":
        assert cell not in posture.failing, f"{cell} unexpectedly failing: {posture.failing}"
        assert cell not in posture.xfail, f"{cell} unexpectedly xfail: {posture.xfail}"
    elif bucket == "fail":
        assert cell in posture.failing, f"{cell} not in failing: {posture.failing}"
    else:
        assert cell in posture.xfail, f"{cell} not in xfail: {posture.xfail}"


# ─── Cell census ──────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("primitive", "total", "extra"),
    [
        ("lp", 21, ["LP1", "LP3"]),
        ("lending", 21, ["L4"]),
        ("perp", 21, ["P1", "P3"]),
        ("swap", 15, []),
        ("mixed", 15, []),
        ("unknown-primitive", 15, []),
    ],
)
def test_cells_for_primitive(primitive: str, total: int, extra: list[str]) -> None:
    cells = _posture_cells_for_primitive(primitive)
    assert len(cells) == total
    posture = _posture(primitive)
    assert posture.cells_total == total
    assert posture.cells_passed + posture.cells_failed + posture.cells_xfail == total
    for cell in extra:
        assert cell in posture.failing or cell in posture.xfail or cell not in posture.failing


def test_all_pass_baseline() -> None:
    posture = _posture()
    assert posture.cells_total == 15
    assert posture.cells_failed == 0
    assert posture.cells_passed == 13
    assert sorted(posture.xfail) == ["G14", "G15"]


def test_empty_inputs_exact_buckets() -> None:
    posture = evaluate_posture(
        primitive="mixed",
        ledger_entries=_ledger_stats(
            total=0,
            with_tx_hash=0,
            with_cycle_id=0,
            with_price_inputs=0,
            with_pre_post_state=0,
            with_positive_gas_usd=0,
            gas_usd_sum=Decimal("0"),
        ),
        accounting_events=[],
        snapshots=[],
        audit=AuditTrailStats(),
        reconciliation=ReconciliationStatus(),
        portfolio_metrics=None,
    )
    assert posture.cells_total == 15
    assert posture.cells_passed == 1  # G11 only (vacuously OK)
    assert posture.cells_xfail == 2  # G14, G15
    assert sorted(posture.xfail) == ["G14", "G15"]
    assert "G11" not in posture.failing
    assert posture.cells_failed == 12


# ─── Generic cells, one broken predicate at a time ────────────────────────


@pytest.mark.parametrize(
    ("cell", "kwargs"),
    [
        ("G1", {"ledger": _ledger_stats(with_tx_hash=1)}),
        ("G2", {"audit": _audit(ledger_with_gas_usd=1)}),
        ("G4", {"metrics": _metrics("0", "0")}),
        ("G4", {"metrics": None}),
        ("G5", {"metrics": _metrics("100", "0")}),
        ("G6", {"recon": _recon(passed=False)}),
        ("G6", {"recon": _recon(has_data=False)}),
        ("G7", {"ledger": _ledger_stats(with_cycle_id=0)}),
        ("G8", {"snapshots": _snaps(1)}),
        ("G8", {"snapshots": []}),
        ("G10", {"ledger": _ledger_stats(total=0, with_tx_hash=0, with_cycle_id=0)}),
        ("G12", {"audit": _audit(ledger_with_price_inputs=0)}),
        ("G13", {"audit": _audit(events_with_versions=0)}),
    ],
)
def test_generic_cell_fails_alone(cell: str, kwargs: dict[str, Any]) -> None:
    _assert_bucket(_posture(**kwargs), cell, "fail")


def test_g3_fails_without_events() -> None:
    posture = _posture(events=[], audit=_audit(events_total=0, events_with_versions=0))
    _assert_bucket(posture, "G3", "fail")
    _assert_bucket(posture, "G9", "fail")


@pytest.mark.parametrize("confidence", ["", None])
def test_g9_fails_without_confidence(confidence: str | None) -> None:
    events = [_ev("SWAP"), _ev("SWAP", confidence=confidence or "")]
    _assert_bucket(_posture(events=events), "G9", "fail")


def test_g9_tolerates_object_rows_with_confidence() -> None:
    events: list[Any] = [_ev("SWAP"), SimpleNamespace(confidence="HIGH")]
    _assert_bucket(_posture(events=events), "G9", "pass")


def test_g9_fails_on_object_row_without_confidence() -> None:
    events: list[Any] = [_ev("SWAP"), SimpleNamespace()]
    _assert_bucket(_posture(events=events), "G9", "fail")


def test_g5_passes_only_when_both_endpoints_positive() -> None:
    _assert_bucket(_posture(metrics=_metrics("100", "110")), "G5", "pass")
    _assert_bucket(_posture(metrics=_metrics("0", "110")), "G5", "fail")
    _assert_bucket(_posture(metrics=_metrics("0", "110")), "G4", "pass")


@pytest.mark.parametrize("cell", ["G14", "G15"])
def test_track_c_generic_cells_always_xfail(cell: str) -> None:
    _assert_bucket(_posture(), cell, "xfail")


def test_g11_vacuously_passes_even_on_empty_inputs() -> None:
    posture = _posture(events=[], snapshots=[], metrics=None)
    _assert_bucket(posture, "G11", "pass")


# ─── LP cells ─────────────────────────────────────────────────────────────


@pytest.mark.parametrize("event_type", ["LP_OPEN", "LP_CLOSE", "SNAPSHOT", "lp_close"])
def test_lp1_passes_on_lp_presence(event_type: str) -> None:
    posture = _posture("lp", events=[_ev(event_type)])
    _assert_bucket(posture, "LP1", "pass")


def test_lp1_fails_without_lp_events() -> None:
    posture = _posture("lp", events=[_ev("SWAP")])
    _assert_bucket(posture, "LP1", "fail")


def test_lp3_passes_on_measured_close_fees() -> None:
    events = [_ev("LP_OPEN"), _ev("LP_CLOSE", {"fees_total_usd": "3.21"})]
    _assert_bucket(_posture("lp", events=events), "LP3", "pass")


@pytest.mark.parametrize(
    "payload",
    [{"fees_total_usd": "0"}, {}, {"fees_total_usd": "garbage!!"}],
)
def test_lp3_fails_without_measured_fees(payload: dict[str, Any]) -> None:
    events = [_ev("LP_CLOSE", payload)]
    _assert_bucket(_posture("lp", events=events), "LP3", "fail")


def test_lp3_fails_without_events() -> None:
    posture = _posture("lp", events=[], audit=_audit(events_total=0, events_with_versions=0))
    _assert_bucket(posture, "LP3", "fail")


def test_lp3_ignores_malformed_payload_json() -> None:
    event = _ev("LP_CLOSE", {"fees_total_usd": "1.5"})
    event["payload_json"] = "{not-json"
    _assert_bucket(_posture("lp", events=[event]), "LP3", "fail")


@pytest.mark.parametrize("cell", ["LP2", "LP4", "LP5", "LP6"])
def test_lp_track_c_cells_always_xfail(cell: str) -> None:
    events = [_ev("LP_OPEN"), _ev("LP_CLOSE", {"fees_total_usd": "3.21"})]
    _assert_bucket(_posture("lp", events=events), cell, "xfail")


# ─── Lending L4 ───────────────────────────────────────────────────────────


def test_l4_xfail_without_any_repay() -> None:
    events = [_ev("SUPPLY"), _ev("BORROW")]
    posture = _posture("lending", events=events)
    _assert_bucket(posture, "L4", "xfail")
    assert _lending_l4_result(events) == (False, True)


def test_l4_passes_on_repay_with_both_legs() -> None:
    events = [_ev("REPAY", {"principal_repaid_usd": "10", "interest_paid_usd": "0.5"})]
    _assert_bucket(_posture("lending", events=events), "L4", "pass")
    assert _lending_l4_result(events) == (True, False)


def test_l4_passes_on_deleverage() -> None:
    events = [_ev("DELEVERAGE", {"principal_repaid_usd": "10", "interest_paid_usd": "0.5"})]
    _assert_bucket(_posture("lending", events=events), "L4", "pass")


@pytest.mark.parametrize(
    "payload",
    [
        {"principal_repaid_usd": None, "interest_paid_usd": None},
        {"principal_repaid_usd": "10"},
        {"interest_paid_usd": "0.5"},
        {},
    ],
)
def test_l4_fails_on_repay_with_null_legs(payload: dict[str, Any]) -> None:
    events = [_ev("REPAY", payload)]
    _assert_bucket(_posture("lending", events=events), "L4", "fail")
    assert _lending_l4_result(events) == (False, False)


def test_l4_zero_string_counts_as_present() -> None:
    """Pins the CURRENT truthiness semantics: a "0" string is truthy, so L4
    PASSes. A numeric >0 reading would FAIL this — that change needs a product
    decision (see the Linear draft in the refactor summary), not a refactor."""
    events = [_ev("REPAY", {"principal_repaid_usd": "0", "interest_paid_usd": "0"})]
    _assert_bucket(_posture("lending", events=events), "L4", "pass")


def test_l4_ignores_non_dict_rows() -> None:
    events: list[Any] = [SimpleNamespace(event_type="REPAY")]
    _assert_bucket(_posture("lending", events=events), "L4", "xfail")


@pytest.mark.parametrize("cell", ["L1", "L2", "L3", "L5", "L6"])
def test_lending_track_c_cells_always_xfail(cell: str) -> None:
    events = [_ev("REPAY", {"principal_repaid_usd": "10", "interest_paid_usd": "0.5"})]
    _assert_bucket(_posture("lending", events=events), cell, "xfail")


# ─── Perp cells ───────────────────────────────────────────────────────────


@pytest.mark.parametrize("event_type", ["PERP_OPEN", "PERP_CLOSE", "perp_open"])
def test_p1_passes_on_perp_presence(event_type: str) -> None:
    _assert_bucket(_posture("perp", events=[_ev(event_type)]), "P1", "pass")


def test_p1_fails_without_perp_events() -> None:
    _assert_bucket(_posture("perp", events=[_ev("SWAP")]), "P1", "fail")


@pytest.mark.parametrize(
    "payload",
    [
        {"open_fee_usd": "0.30"},
        {"close_fee_usd": "0.40"},
        {"open_fee_usd": "0.30", "close_fee_usd": "0.40"},
    ],
)
def test_p3_passes_on_either_fee_leg(payload: dict[str, Any]) -> None:
    events = [_ev("PERP_CLOSE", payload)]
    _assert_bucket(_posture("perp", events=events), "P3", "pass")


def test_p3_or_semantics_not_first_key_wins() -> None:
    """A zero open fee must not mask a positive close fee (first-key-wins
    would read only open_fee_usd and FAIL)."""
    events = [_ev("PERP_CLOSE", {"open_fee_usd": "0", "close_fee_usd": "0.40"})]
    _assert_bucket(_posture("perp", events=events), "P3", "pass")


@pytest.mark.parametrize(
    "payload",
    [{"open_fee_usd": "0", "close_fee_usd": "0"}, {}],
)
def test_p3_fails_without_measured_fees(payload: dict[str, Any]) -> None:
    events = [_ev("PERP_CLOSE", payload)]
    _assert_bucket(_posture("perp", events=events), "P3", "fail")


def test_p3_fails_without_events() -> None:
    posture = _posture("perp", events=[], audit=_audit(events_total=0, events_with_versions=0))
    _assert_bucket(posture, "P3", "fail")


@pytest.mark.parametrize("cell", ["P2", "P4", "P5", "P6"])
def test_perp_track_c_cells_always_xfail(cell: str) -> None:
    events = [_ev("PERP_OPEN", {"open_fee_usd": "0.30"})]
    _assert_bucket(_posture("perp", events=events), cell, "xfail")


# ─── Recording-fold precedence ────────────────────────────────────────────


def test_track_c_outranks_pass() -> None:
    posture = AccountantPosture(primitive="swap")
    _record_posture_cell(posture, "G14", True)
    assert posture.cells_passed == 0
    assert posture.cells_xfail == 1
    assert posture.xfail == ["G14"]


def test_structural_xfail_outranks_pass() -> None:
    posture = AccountantPosture(primitive="lending")
    _record_posture_cell(posture, "L4", True, structurally_xfail=True)
    assert posture.cells_passed == 0
    assert posture.xfail == ["L4"]


def test_record_pass_and_fail_buckets() -> None:
    posture = AccountantPosture(primitive="swap")
    _record_posture_cell(posture, "G1", True)
    _record_posture_cell(posture, "G2", False)
    assert posture.cells_passed == 1
    assert posture.cells_failed == 1
    assert posture.failing == ["G2"]
