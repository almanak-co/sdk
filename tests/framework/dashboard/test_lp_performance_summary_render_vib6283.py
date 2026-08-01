"""VIB-6283 — what the LP Performance Summary actually PRINTS.

RC5 of this ticket is invisible to every Decimal-level assertion in the suite:
the panel formatted at 2 dp with no ``precise_small`` path, so a real
``Decimal("0.0066680447828332196032")`` fee — the median LP fee in the run
corpus — rendered ``$0.00``. Sourcing the number correctly and then printing
``$0.00`` is the same bug with a better provenance; wiring alone would have
shipped inert.

Only the rendered STRING can catch that class. These tests drive
``_render_performance_summary`` under a real Streamlit runtime via
``AppTest.from_function`` (the pattern established in
``test_detail_header.py``) and assert the literal tile text.

Four contracts, and the last two are a matched pair — a fix that satisfies one
by breaking the other has traded one wrong render for another:

1. A measured $88.5057… prints ``$88.51``, not ``$0.00``.
2. A genuine sub-cent value prints something other than ``$0.00``.
3. ``None`` (UNMEASURED) prints ``—`` and never ``$0.00``.
4. ``Decimal("0")`` (a MEASURED zero) prints ``$0.00`` and never ``—``.

The $88.51 is not an invented number: it is
``fees_total_usd = 88.50577177198135678775255617`` from the LP_CLOSE row of
``tests/fixtures/lp_dashboard_vib6283/anvil_fee_induction_run.json``, the fork
run whose fees were independently measured on-chain two ways (~$88.48) while
this panel rendered ``$0.00``.

AppTest pickles nothing — ``from_function`` re-executes the function's SOURCE
as a script — so each driver below is self-contained and its constants are
inline literals. ``test_render_literals_match_the_committed_fork_fixture``
pins those literals back to the fixture so they cannot drift from the run they
claim to reproduce.
"""

from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

from streamlit.testing.v1 import AppTest

# ---------------------------------------------------------------------------
# Driver functions executed inside AppTest.from_function.
#
# MUST be top-level and self-contained: all imports and constants inline.
# Each patches the two gateway RPCs ``load_strategy_money`` reads (lazily, off
# ``almanak.framework.dashboard.sections``) with a context manager, so the
# patch cannot leak into a later test in the same process.
# ---------------------------------------------------------------------------


def _drive_measured_fork_numbers() -> None:
    """The real fork run, after close: $88.5057… fees, $0.0411… IL."""
    from decimal import Decimal
    from unittest import mock

    from almanak.framework.dashboard import sections
    from almanak.framework.dashboard.gateway_client import CostStackInfo, PnLSummary
    from almanak.framework.dashboard.templates.lp_dashboard import _render_performance_summary

    pnl = PnLSummary(
        deployed_usd=Decimal("1261191.665331584048588828107"),
        nav_usd=Decimal("1261434.306192221506426206997"),
        lifetime_pnl_usd=None,
        lifetime_pnl_pct=None,
        net_apr_pct=None,
        max_drawdown_pct=Decimal("0"),
        current_drawdown_pct=Decimal("0"),
        value_confidence="HIGH",
        age_days=0,
        deployed_capital_usd=Decimal("1261091.439477805964811117964"),
        available_cash_usd=Decimal("0"),
        open_position_count=1,
        primary_risk_kind="lp",
        primary_risk_label="Range",
        primary_risk_value="in-range",
        primary_risk_color="green",
    )
    cost = CostStackInfo(
        cost_gas_usd=Decimal("2.3755149327301"),
        cost_protocol_fees_usd=Decimal("0"),
        cost_slippage_usd=Decimal("0"),
        fees_earned_usd=Decimal("88.50577177198135678775255617"),
        interest_paid_usd=Decimal("0"),
        interest_earned_usd=Decimal("0"),
        funding_paid_usd=Decimal("0"),
        funding_earned_usd=Decimal("0"),
        realized_pnl_usd=Decimal("342.866714415541615089033"),
        il_usd=Decimal("0.041188405836821282508"),
    )
    with (
        mock.patch.object(sections, "get_pnl_summary", return_value=pnl),
        mock.patch.object(sections, "get_cost_stack", return_value=cost),
    ):
        # The caller dict carries NO money — that is the point of the fix. Any
        # money-shaped key here is stripped upstream by reject_caller_money_keys.
        _render_performance_summary({}, "deployment:b3816ff5ddb8")


def _drive_sub_cent_fee() -> None:
    """The corpus-median LP fee (~$0.0067) — the value a 2-dp formatter eats."""
    from decimal import Decimal
    from unittest import mock

    from almanak.framework.dashboard import sections
    from almanak.framework.dashboard.gateway_client import CostStackInfo, PnLSummary
    from almanak.framework.dashboard.templates.lp_dashboard import _render_performance_summary

    pnl = PnLSummary(
        deployed_usd=Decimal("100"),
        nav_usd=Decimal("100"),
        lifetime_pnl_usd=None,
        lifetime_pnl_pct=None,
        net_apr_pct=None,
        max_drawdown_pct=Decimal("0"),
        current_drawdown_pct=Decimal("0"),
        value_confidence="HIGH",
        age_days=0,
        deployed_capital_usd=Decimal("100"),
        available_cash_usd=Decimal("0"),
        open_position_count=1,
        primary_risk_kind="lp",
        primary_risk_label="Range",
        primary_risk_value="in-range",
        primary_risk_color="green",
    )
    cost = CostStackInfo(
        cost_gas_usd=Decimal("0"),
        cost_protocol_fees_usd=Decimal("0"),
        cost_slippage_usd=Decimal("0"),
        fees_earned_usd=Decimal("0.0066680447828332196032"),
        interest_paid_usd=Decimal("0"),
        interest_earned_usd=Decimal("0"),
        funding_paid_usd=Decimal("0"),
        funding_earned_usd=Decimal("0"),
        realized_pnl_usd=Decimal("0"),
        il_usd=Decimal("-0.0012345"),
    )
    with (
        mock.patch.object(sections, "get_pnl_summary", return_value=pnl),
        mock.patch.object(sections, "get_cost_stack", return_value=cost),
    ):
        _render_performance_summary({}, "deployment:subcent")


def _drive_unmeasured() -> None:
    """Cost stack unavailable: fees, IL and Net PnL are all UNMEASURED."""
    from decimal import Decimal
    from unittest import mock

    from almanak.framework.dashboard import sections
    from almanak.framework.dashboard.gateway_client import PnLSummary
    from almanak.framework.dashboard.templates.lp_dashboard import _render_performance_summary

    pnl = PnLSummary(
        deployed_usd=Decimal("100"),
        nav_usd=Decimal("100"),
        lifetime_pnl_usd=None,
        lifetime_pnl_pct=None,
        net_apr_pct=None,
        max_drawdown_pct=Decimal("0"),
        current_drawdown_pct=Decimal("0"),
        value_confidence="HIGH",
        age_days=0,
        deployed_capital_usd=Decimal("100"),
        available_cash_usd=Decimal("0"),
        open_position_count=1,
        primary_risk_kind="lp",
        primary_risk_label="Range",
        primary_risk_value="in-range",
        primary_risk_color="green",
    )
    with (
        mock.patch.object(sections, "get_pnl_summary", return_value=pnl),
        mock.patch.object(sections, "get_cost_stack", return_value=None),
    ):
        _render_performance_summary({}, "deployment:unmeasured")


def _drive_measured_zero() -> None:
    """A close that genuinely earned nothing: MEASURED zeros everywhere."""
    from decimal import Decimal
    from unittest import mock

    from almanak.framework.dashboard import sections
    from almanak.framework.dashboard.gateway_client import CostStackInfo, PnLSummary
    from almanak.framework.dashboard.templates.lp_dashboard import _render_performance_summary

    pnl = PnLSummary(
        deployed_usd=Decimal("100"),
        nav_usd=Decimal("150"),
        lifetime_pnl_usd=None,
        lifetime_pnl_pct=None,
        net_apr_pct=None,
        max_drawdown_pct=Decimal("0"),
        current_drawdown_pct=Decimal("0"),
        value_confidence="HIGH",
        age_days=0,
        # nav − cash == deployed_capital ⇒ zero unrealized, so Strategy PnL is
        # a MEASURED zero rather than an accident of the realized leg.
        deployed_capital_usd=Decimal("100"),
        available_cash_usd=Decimal("50"),
        open_position_count=1,
        primary_risk_kind="lp",
        primary_risk_label="Range",
        primary_risk_value="in-range",
        primary_risk_color="green",
    )
    cost = CostStackInfo(
        cost_gas_usd=Decimal("0"),
        cost_protocol_fees_usd=Decimal("0"),
        cost_slippage_usd=Decimal("0"),
        fees_earned_usd=Decimal("0"),
        interest_paid_usd=Decimal("0"),
        interest_earned_usd=Decimal("0"),
        funding_paid_usd=Decimal("0"),
        funding_earned_usd=Decimal("0"),
        realized_pnl_usd=Decimal("0"),
        il_usd=Decimal("0"),
    )
    with (
        mock.patch.object(sections, "get_pnl_summary", return_value=pnl),
        mock.patch.object(sections, "get_cost_stack", return_value=cost),
    ):
        _render_performance_summary({}, "deployment:measuredzero")


def _drive_headline_and_panel_together() -> None:
    """Render the page headline AND the LP panel from the same inputs.

    The original defect was these two disagreeing on one page load.
    """
    from decimal import Decimal
    from unittest import mock

    from almanak.framework.dashboard import sections
    from almanak.framework.dashboard.gateway_client import CostStackInfo, PnLSummary
    from almanak.framework.dashboard.pages._detail_header import render_money_trail
    from almanak.framework.dashboard.templates.lp_dashboard import _render_performance_summary

    pnl = PnLSummary(
        deployed_usd=Decimal("1261191.665331584048588828107"),
        nav_usd=Decimal("1261434.306192221506426206997"),
        lifetime_pnl_usd=None,
        lifetime_pnl_pct=None,
        net_apr_pct=None,
        max_drawdown_pct=Decimal("0"),
        current_drawdown_pct=Decimal("0"),
        value_confidence="HIGH",
        age_days=0,
        deployed_capital_usd=Decimal("1261091.439477805964811117964"),
        available_cash_usd=Decimal("0"),
        open_position_count=1,
        primary_risk_kind="lp",
        primary_risk_label="Range",
        primary_risk_value="in-range",
        primary_risk_color="green",
    )
    cost = CostStackInfo(
        cost_gas_usd=Decimal("2.3755149327301"),
        cost_protocol_fees_usd=Decimal("0"),
        cost_slippage_usd=Decimal("0"),
        fees_earned_usd=Decimal("88.50577177198135678775255617"),
        interest_paid_usd=Decimal("0"),
        interest_earned_usd=Decimal("0"),
        funding_paid_usd=Decimal("0"),
        funding_earned_usd=Decimal("0"),
        realized_pnl_usd=Decimal("342.866714415541615089033"),
        il_usd=Decimal("0.041188405836821282508"),
    )
    with (
        mock.patch.object(sections, "get_pnl_summary", return_value=pnl),
        mock.patch.object(sections, "get_cost_stack", return_value=cost),
    ):
        render_money_trail(pnl, cost)
        _render_performance_summary({}, "deployment:b3816ff5ddb8")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def _tile(at: AppTest, label: str) -> str:
    return next(m for m in at.metric if m.label == label).value


def test_measured_fees_render_the_real_number_not_zero() -> None:
    """THE motivating render. $88.5057… of fees, measured on-chain two ways,
    must print ``$88.51``. It printed ``$0.00`` for fourteen months.

    The driver's constants are inline literals (``AppTest.from_function``
    re-executes the function's SOURCE as a script, so module-level names are
    unavailable inside it). They are pinned back to the committed fork run
    here, so a driver cannot drift from the run it claims to reproduce.
    """
    fixture = Path(__file__).resolve().parents[2] / "fixtures" / "lp_dashboard_vib6283" / "anvil_fee_induction_run.json"
    rows = json.loads(fixture.read_text())["accounting_events"]
    close = json.loads(next(r for r in rows if r["event_type"] == "LP_CLOSE")["payload_json"])
    source = Path(__file__).read_text()
    assert Decimal(close["fees_total_usd"]) == Decimal("88.50577177198135678775255617")
    for literal in (close["fees_total_usd"], close["il_usd"], close["realized_pnl_usd"]):
        assert literal in source, f"driver literal drifted from the fixture: {literal}"

    at = AppTest.from_function(_drive_measured_fork_numbers).run(timeout=30)

    assert not at.exception, f"Unexpected exception: {at.exception}"
    assert _tile(at, "Total Fees") == "$88.51"
    assert _tile(at, "Total Fees") != "$0.00"


def test_impermanent_loss_renders_usd_from_accounting_not_a_dead_percent() -> None:
    """IL was a PERCENT tile fed by ``impermanent_loss_pct`` — a key with ZERO
    writers codebase-wide, so it could only ever print ``+0.00%``. It is now a
    USD amount sourced from the LP_CLOSE ``il_usd`` the same run recorded."""
    at = AppTest.from_function(_drive_measured_fork_numbers).run(timeout=30)

    assert not at.exception, f"Unexpected exception: {at.exception}"
    il = _tile(at, "Impermanent Loss")
    assert il == "$0.04"
    assert il != "+0.00%"
    assert not il.endswith("%"), il


def test_net_pnl_tile_renders_the_accounting_number_not_a_fabricated_zero() -> None:
    """``net_pnl_usd`` was read from ``session_state`` and never written, so
    this tile could only print ``$+0.00`` while the headline on the same page
    showed a real number."""
    at = AppTest.from_function(_drive_measured_fork_numbers).run(timeout=30)

    assert not at.exception, f"Unexpected exception: {at.exception}"
    net = _tile(at, "Strategy Net PnL")
    assert net not in ("$0.00", "$+0.00", "+$0.00"), net
    # realized 342.8667… + fees 88.5057… − gas 2.3755… + unrealized 342.8667…
    assert net == "+$771.86", net


def test_sub_cent_fee_does_not_collapse_to_zero() -> None:
    """RC5 — the regression that would re-hide every real number.

    A 2-dp formatter turns the corpus-median LP fee (~$0.0067) into ``$0.00``.
    Correct sourcing plus a lossy formatter is still a dashboard that reports
    zero fees on a position that earned some.
    """
    at = AppTest.from_function(_drive_sub_cent_fee).run(timeout=30)

    assert not at.exception, f"Unexpected exception: {at.exception}"
    fees = _tile(at, "Total Fees")
    assert fees != "$0.00", "a real sub-cent fee collapsed to $0.00 — RC5 has regressed"
    assert fees == "$0.006668", fees
    # Same for the IL tile, including the sign on a sub-cent loss.
    il = _tile(at, "Impermanent Loss")
    assert il != "$0.00", il
    assert il == "-$0.001234", il


def test_unmeasured_money_renders_a_dash_never_a_zero() -> None:
    """Empty ≠ Zero on the rendered string. With the cost stack unavailable all
    three money tiles are UNMEASURED; ``$0.00`` there is the original defect."""
    at = AppTest.from_function(_drive_unmeasured).run(timeout=30)

    assert not at.exception, f"Unexpected exception: {at.exception}"
    for label in ("Total Fees", "Impermanent Loss", "Strategy Net PnL"):
        value = _tile(at, label)
        assert value == "—", f"{label} rendered {value!r}, expected the unmeasured dash"
        assert value != "$0.00"
        assert value != "+0.00%"


def test_measured_zero_renders_zero_never_a_dash() -> None:
    """The inverse conflation, and the reason this is a matched pair.

    A close that genuinely earned nothing is a MEASURED zero. Rendering "—"
    for it would hide a real fact — the fix must not buy Empty≠Zero by turning
    every zero into "unknown".
    """
    at = AppTest.from_function(_drive_measured_zero).run(timeout=30)

    assert not at.exception, f"Unexpected exception: {at.exception}"
    for label in ("Total Fees", "Impermanent Loss", "Strategy Net PnL"):
        value = _tile(at, label)
        assert value == "$0.00", f"{label} rendered {value!r}, expected a measured $0.00"
        assert value != "—"


def test_panel_net_pnl_and_page_headline_print_the_same_money() -> None:
    """One computation, two presentations — asserted on the RENDERED text.

    The headline (``Strategy PnL``) and the LP panel (``Net PnL``) are drawn by
    different owners; before this fix they contradicted each other on the same
    page load. The panel adds a leading ``+`` on a gain (the headline carries
    only a ``-`` on a loss), so the sign prefix is normalised — the money must
    match character for character.
    """
    at = AppTest.from_function(_drive_headline_and_panel_together).run(timeout=30)

    assert not at.exception, f"Unexpected exception: {at.exception}"
    headline = _tile(at, "Strategy PnL")
    panel = _tile(at, "Strategy Net PnL")
    assert panel.lstrip("+") == headline, f"panel {panel!r} != headline {headline!r}"
    assert headline != "$0.00"
