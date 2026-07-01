"""Unit tests for the bespoke Curve LP cell pack (CURVE1–CURVE6) — VIB-5430.

The Curve cell pack replaces the generic V3 ``_cells_lp`` tick cells (LP1 range
exposure, LP2 in-range-time fraction) with Curve-shaped cells. The contract this
file pins:

  * On a *rangeless* fixture (no ``tick_lower`` / ``tick_upper`` / ``in_range``
    anywhere) the pack NEVER returns FAIL for the former tick cells — the
    structural inapplicability is converted to an honest PASS (on what Curve
    genuinely books) or XFAIL (a single-sided deposit's undefined IL), never the
    false "books-broken" FAIL the V3 pack produced.
  * The pack PASSes the cells Curve genuinely books (funded open legs, collected
    close legs, decomposition, LP-token-balance liquidity).
  * Empty != Zero: a measured-zero collected coin / fee counts as booked; only
    ``None`` / ``""`` is unmeasured.

These are pure-function tests over synthetic row dicts plus one offline
end-to-end pass over the frozen ``lp_curve`` fixture, so they need no live chain.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from almanak.framework.accounting.accountant_test import (
    SCORECARD_PROFILES,
    _cells_curve_lp,
    run_against_sqlite,
)

_REPO_ROOT = Path(__file__).resolve().parents[3]
_FIXTURE = _REPO_ROOT / "tests" / "fixtures" / "accounting" / "lp_curve" / "expected_baseline.sqlite"

# All six Curve cell ids, in pack order.
_CURVE_IDS = ("CURVE1", "CURVE2", "CURVE3", "CURVE4", "CURVE5", "CURVE6")


def _by_id(cells: list[Any]) -> dict[str, Any]:
    return {c.cell_id: c for c in cells}


def _open_event(event_id: str = "o1") -> dict[str, Any]:
    return {"id": event_id, "event_type": "LP_OPEN"}


def _close_event(event_id: str = "c1") -> dict[str, Any]:
    return {"id": event_id, "event_type": "LP_CLOSE"}


def _lp_pos_event(event_type: str, **fields: Any) -> dict[str, Any]:
    base = {"position_type": "LP", "event_type": event_type}
    base.update(fields)
    return base


# ─────────────────────────────────────────────────────────────────────────
# Profile registration
# ─────────────────────────────────────────────────────────────────────────


def test_curve_lp_profile_registered() -> None:
    """The ``curve_lp`` profile is registered and rides the LP primitive (no
    ``Primitive.CURVE_LP`` — the enum is AST-frozen)."""
    from almanak.framework.primitives.types import Primitive

    assert "curve_lp" in SCORECARD_PROFILES
    profile = SCORECARD_PROFILES["curve_lp"]
    assert profile.canonical_primitive == Primitive.LP
    assert profile.required_lifecycle == ("LP_OPEN", "LP_CLOSE")


# ─────────────────────────────────────────────────────────────────────────
# The core contract: tick cells N/A, NOT FAIL, on a rangeless fixture
# ─────────────────────────────────────────────────────────────────────────


def test_rangeless_fixture_never_fails_tick_cells() -> None:
    """A healthy rangeless Curve round-trip: CURVE1/CURVE2 PASS and NO Curve cell
    is FAIL purely because ticks are absent (the V3 pack scored LP1/LP2 FAIL)."""
    acct_events = [_open_event(), _close_event()]
    acct_payloads = {
        "o1": {"amount0": "10", "amount1": None, "unavailable_reason": "Curve fee USD unavailable"},
        "c1": {"amount0": None, "amount1": None},
    }
    pos_events = [
        # NOTE: zero tick fields anywhere — a Curve pool is rangeless.
        _lp_pos_event("OPEN", amount0="0", amount1="10000000"),
        _lp_pos_event(
            "CLOSE",
            amount0="1416766220028388602",
            amount1="1436299",
            fees_token0=0,
            fees_token1=0,
            attribution_json=(
                '{"position_type": "LP", "net_pnl_usd": "1.0", '
                '"principal_deposited_usd": "10.0", "principal_recovered_usd": "11.0", '
                '"price_pnl_usd": "1.0"}'
            ),
        ),
    ]
    position_state_rows = [
        {"position_type": "LP", "liquidity": 9622944479479046273},
        {"position_type": "LP", "liquidity": 9622944479479046273},
    ]
    cells = _by_id(_cells_curve_lp(acct_events, pos_events, acct_payloads, {}, position_state_rows))

    assert set(cells) == set(_CURVE_IDS)
    # No Curve cell is FAIL on a healthy rangeless fixture.
    assert all(c.status != "FAIL" for c in cells.values()), {k: v.status for k, v in cells.items()}
    assert cells["CURVE1"].status == "PASS"  # funded open leg
    assert cells["CURVE2"].status == "PASS"  # collected close legs (off position_event)
    assert cells["CURVE5"].status == "PASS"  # decomposition
    assert cells["CURVE6"].status == "PASS"  # LP-token balance == liquidity


def test_no_curve_cell_reads_ticks() -> None:
    """Sanity: identical inputs with and without tick fields score identically —
    the pack must never key on ticks (the whole point of the bespoke profile)."""
    acct_events = [_open_event(), _close_event()]
    acct_payloads = {"o1": {"amount0": "10"}, "c1": {"amount0": "5"}}
    base_pos = [
        _lp_pos_event("OPEN", amount0="10"),
        _lp_pos_event("CLOSE", amount0="5", fees_token0=0),
    ]
    ticked_pos = [
        _lp_pos_event("OPEN", amount0="10", tick_lower=-100, tick_upper=100),
        _lp_pos_event("CLOSE", amount0="5", fees_token0=0, tick_lower=-100, tick_upper=100),
    ]
    rows = [{"position_type": "LP", "liquidity": 5}]
    without = _by_id(_cells_curve_lp(acct_events, base_pos, acct_payloads, {}, rows))
    with_ticks = _by_id(_cells_curve_lp(acct_events, ticked_pos, acct_payloads, {}, rows))
    assert {k: v.status for k, v in without.items()} == {k: v.status for k, v in with_ticks.items()}


# ─────────────────────────────────────────────────────────────────────────
# Per-cell behaviour
# ─────────────────────────────────────────────────────────────────────────


def test_curve4_single_sided_is_xfail_not_fail() -> None:
    """A single-sided deposit (one funded coin leg) has no HODL counterfactual →
    IL undefined → CURVE4 XFAIL (NOT FAIL)."""
    acct_payloads = {"o1": {"amount0": "10", "amount1": None}}
    cells = _by_id(_cells_curve_lp([_open_event()], [], acct_payloads, {}, []))
    assert cells["CURVE4"].status == "XFAIL"
    assert "single-sided" in cells["CURVE4"].diagnostic


def test_curve4_two_sided_reuses_lp4_sanity() -> None:
    """A 2-sided deposit that emits no ``il_usd`` reuses the generic LP4 predicate
    (which XFAILs when il_usd is absent) — under the CURVE4 id."""
    acct_payloads = {"o1": {"amount0": "10", "amount1": "10"}, "c1": {"amount0": "5", "amount1": "5"}}
    cells = _by_id(_cells_curve_lp([_open_event(), _close_event()], [], acct_payloads, {}, []))
    # 2-sided but no il_usd emitted → XFAIL (generic LP4 sanity), still under CURVE4.
    assert cells["CURVE4"].cell_id == "CURVE4"
    assert cells["CURVE4"].status == "XFAIL"
    assert "single-sided" not in cells["CURVE4"].diagnostic


def test_curve2_empty_not_zero_measured_zero_counts() -> None:
    """A proportional / imbalanced close that returns a MEASURED-ZERO coin still
    booked that leg — Empty != Zero, so CURVE2 PASSes on a measured-zero amount."""
    acct_events = [_close_event()]
    acct_payloads = {"c1": {"amount0": None, "amount1": None}}
    # imbalanced close: coin0 returned 0 (measured), coin1 unmeasured.
    pos_events = [_lp_pos_event("CLOSE", amount0="0", amount1=None)]
    cells = _by_id(_cells_curve_lp(acct_events, pos_events, acct_payloads, {}, []))
    assert cells["CURVE2"].status == "PASS"


def test_curve2_unmeasured_legs_xfail_not_fail() -> None:
    """When neither the payload nor the close position_event books any collected
    coin leg, CURVE2 is XFAIL (capability gap), never a false FAIL."""
    acct_events = [_close_event()]
    acct_payloads = {"c1": {"amount0": None, "amount1": None}}
    pos_events = [_lp_pos_event("CLOSE", amount0=None, amount1="")]
    cells = _by_id(_cells_curve_lp(acct_events, pos_events, acct_payloads, {}, []))
    assert cells["CURVE2"].status == "XFAIL"


def test_curve3_unavailable_reason_is_honest_pass() -> None:
    """Curve fees are USD-unavailable by design; an explicit ``unavailable_reason``
    is an honest known-unknown → CURVE3 PASS (no measured fees, no silent gap)."""
    acct_events = [_open_event()]
    acct_payloads = {"o1": {"amount0": "10", "unavailable_reason": "fee USD unavailable"}}
    cells = _by_id(_cells_curve_lp(acct_events, [], acct_payloads, {}, []))
    assert cells["CURVE3"].status == "PASS"
    assert "unavailable" in cells["CURVE3"].diagnostic.lower()


def test_curve3_no_fee_no_reason_is_xfail() -> None:
    """No measured fee and no unavailable_reason is a silent gap → CURVE3 XFAIL."""
    acct_events = [_open_event()]
    acct_payloads = {"o1": {"amount0": "10"}}
    cells = _by_id(_cells_curve_lp(acct_events, [], acct_payloads, {}, []))
    assert cells["CURVE3"].status == "XFAIL"


def test_curve3_measured_zero_fee_is_pass() -> None:
    """A measured-zero fee leg on the close position_event PASSes (Empty != Zero)."""
    acct_events = [_close_event()]
    acct_payloads = {"c1": {"amount0": "5"}}
    pos_events = [_lp_pos_event("CLOSE", amount0="5", fees_token0=0, fees_token1=0)]
    cells = _by_id(_cells_curve_lp(acct_events, pos_events, acct_payloads, {}, []))
    assert cells["CURVE3"].status == "PASS"


def test_curve6_no_state_rows_is_xfail() -> None:
    """No LP rows in position_state_snapshots → CURVE6 XFAIL (no liquidity to
    measure), never FAIL."""
    cells = _by_id(_cells_curve_lp([_open_event()], [], {"o1": {"amount0": "10"}}, {}, []))
    assert cells["CURVE6"].status == "XFAIL"


def test_curve6_zero_liquidity_is_fail() -> None:
    """LP rows present but all zero liquidity is a genuine observer gap → FAIL
    (Empty != Zero: 0 is a measured-zero balance, not 'no measurement')."""
    rows = [{"position_type": "LP", "liquidity": 0}, {"position_type": "LP", "liquidity": "0"}]
    cells = _by_id(_cells_curve_lp([_open_event()], [], {"o1": {"amount0": "10"}}, {}, rows))
    assert cells["CURVE6"].status == "FAIL"


def test_curve1_payload_validation_error_fails_loud() -> None:
    """A payload that failed Pydantic validation surfaces FAIL on CURVE1 (the
    payload-block contract), not a silent XFAIL."""
    acct_events = [_open_event()]
    cells = _by_id(_cells_curve_lp(acct_events, [], {}, {"o1": "schema mismatch: amount0"}, []))
    assert cells["CURVE1"].status == "FAIL"


def test_curve3_payload_validation_error_fails_loud() -> None:
    """A schema-broken LP payload must FAIL loud on CURVE3, never score PASS/XFAIL
    off a partially-read payload (VIB-5430 review fix — CURVE3 payload-block)."""
    cells = _by_id(_cells_curve_lp([_open_event()], [], {}, {"o1": "schema mismatch: fees0_collected"}, []))
    assert cells["CURVE3"].status == "FAIL"


def test_curve4_payload_validation_error_fails_loud() -> None:
    """A broken LP_OPEN payload must FAIL loud on CURVE4, not take the single-sided
    XFAIL path off zero funded legs read from a bad payload (VIB-5430 review fix)."""
    cells = _by_id(_cells_curve_lp([_open_event()], [], {}, {"o1": "schema mismatch: amount0"}, []))
    assert cells["CURVE4"].status == "FAIL"


# ─────────────────────────────────────────────────────────────────────────
# Offline end-to-end over the frozen fixture
# ─────────────────────────────────────────────────────────────────────────


def test_frozen_fixture_scores_match_manifest() -> None:
    """Scoring the frozen lp_curve fixture under the curve_lp profile yields the
    re-baselined verdicts: CURVE1/2/3/5/6 PASS, CURVE4 XFAIL, no Curve FAIL."""
    report = run_against_sqlite(str(_FIXTURE), primitive="curve_lp", strict_lifecycle=True)
    curve = {c.cell_id: c.status for c in report.cells if c.cell_id.startswith("CURVE")}
    assert curve == {
        "CURVE1": "PASS",
        "CURVE2": "PASS",
        "CURVE3": "PASS",
        "CURVE4": "XFAIL",
        "CURVE5": "PASS",
        "CURVE6": "PASS",
    }
    # G6 stays an honest FAIL (M1-5 wallet-side gap) — not masked by this PR.
    g6 = next(c for c in report.cells if c.cell_id == "G6")
    assert g6.status == "FAIL"
