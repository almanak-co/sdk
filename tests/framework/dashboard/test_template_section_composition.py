"""Contract tests for the 5 framework dashboard templates.

These tests assert that every ``render_*_dashboard()`` template helper
calls each baked-in section helper exactly once and in the canonical
order (PnL → Cost Stack → Trade Tape).

Why we test this explicitly: the bake-in PR moved ``render_pnl_section``,
``render_cost_stack_section``, and ``render_trade_tape_section`` calls
into the templates so every template-using strategy ships with full
accounting. A future regression that drops one of those calls — or a
copy-paste that wires them manually on top of the template and double-
renders — would silently change the on-screen surface for hundreds of
strategies. These tests catch both classes of regression.
"""

from __future__ import annotations

from contextlib import ExitStack
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.dashboard.templates import (
    LPDashboardConfig,
    PerpDashboardConfig,
    PredictionDashboardConfig,
    TADashboardConfig,
    get_aave_v3_config,
    render_lending_dashboard,
    render_lp_dashboard,
    render_perp_dashboard,
    render_prediction_dashboard,
    render_ta_dashboard,
)


def _fake_columns(*args: Any, **kwargs: Any):
    """Replacement for ``st.columns`` that accepts an int or a list/tuple of ratios.

    Streamlit's real ``columns`` returns a tuple of DeltaGenerators of the
    requested length; the templates unpack the result (``c1, c2, c3 = st.columns(3)``)
    so a plain MagicMock fails on iteration. Returning a tuple of MagicMocks
    matches the unpack contract without needing a real Streamlit runtime.
    """
    spec = args[0] if args else 1
    n = spec if isinstance(spec, int) else len(spec)
    return tuple(MagicMock() for _ in range(n))


def _track_call(label: str, calls: list[tuple[str, Any]]):
    """Side-effect that records ``(label, first-positional-arg)`` per invocation.

    Capturing the first positional arg lets the tests assert the section
    helpers receive the right ``deployment_id`` — catches refactors that
    misroute the arg (e.g. passing ``strategy_config`` by mistake).
    """

    def _side_effect(*args: Any, **kwargs: Any) -> None:
        calls.append((label, args[0] if args else None))

    return _side_effect


def _enter_template_patches(
    stack: ExitStack,
    module_path: str,
    calls: list[tuple[str, Any]],
    extra_targets: tuple[str, ...] = (),
) -> None:
    """Activate the standard patches needed to render a template in a unit test.

    Patches the three section helpers with side-effect tracers (so call
    order is recorded into ``calls``) and replaces ``st`` with a MagicMock
    whose ``columns`` returns a real tuple. ``extra_targets`` covers the
    plot helpers each template imports — they're patched to no-ops so the
    test doesn't require live plotly/streamlit context.
    """
    st_mock = MagicMock()
    st_mock.columns.side_effect = _fake_columns

    stack.enter_context(patch(f"{module_path}.render_pnl_section", side_effect=_track_call("pnl", calls)))
    stack.enter_context(patch(f"{module_path}.render_cost_stack_section", side_effect=_track_call("cost", calls)))
    stack.enter_context(patch(f"{module_path}.render_trade_tape_section", side_effect=_track_call("tape", calls)))
    stack.enter_context(patch(f"{module_path}.st", st_mock))
    for target in extra_targets:
        stack.enter_context(patch(f"{module_path}.{target}"))


@pytest.fixture
def call_log() -> list[tuple[str, Any]]:
    return []


_EXPECTED_ORDER = [("pnl", "strat-1"), ("cost", "strat-1"), ("tape", "strat-1")]


# ---------------------------------------------------------------------------
# Per-template tests — each asserts: PnL → Cost Stack → Trade Tape, exactly once.
# ---------------------------------------------------------------------------


class TestTATemplateSectionOrdering:
    """``render_ta_dashboard`` must call PnL → Cost Stack → Trade Tape exactly once."""

    def test_sections_called_once_in_order(self, call_log: list[tuple[str, Any]]) -> None:
        with ExitStack() as stack:
            _enter_template_patches(
                stack,
                "almanak.framework.dashboard.templates.ta_dashboard",
                call_log,
                extra_targets=("plot_price_with_signals", "make_subplots", "go"),
            )
            render_ta_dashboard("strat-1", {}, {}, TADashboardConfig(indicator_name="RSI"))
        assert call_log == _EXPECTED_ORDER


class TestLPTemplateSectionOrdering:
    """``render_lp_dashboard`` must call PnL → Cost Stack → Trade Tape exactly once."""

    def test_sections_called_once_in_order(self, call_log: list[tuple[str, Any]]) -> None:
        # Provide every LP_CRITICAL_KEYS so the missing-keys warn-path doesn't fire
        session_state: dict[str, Any] = {
            "position_id": None,
            "range_lower": None,
            "range_upper": None,
            "total_value_usd": "0",
            "is_active": False,
            "current_price": None,
            "in_range": None,
            "token0_amount": 0,
            "token1_amount": 0,
        }
        with ExitStack() as stack:
            _enter_template_patches(
                stack,
                "almanak.framework.dashboard.templates.lp_dashboard",
                call_log,
                extra_targets=(
                    "plot_liquidity_distribution",
                    "plot_positions_over_time",
                    "plot_fee_accumulation",
                    "plot_impermanent_loss",
                ),
            )
            render_lp_dashboard("strat-1", {}, session_state, LPDashboardConfig())
        assert call_log == _EXPECTED_ORDER


class TestLendingTemplateSectionOrdering:
    """``render_lending_dashboard`` must call PnL → Cost Stack → Trade Tape exactly once."""

    def test_sections_called_once_in_order(self, call_log: list[tuple[str, Any]]) -> None:
        with ExitStack() as stack:
            _enter_template_patches(
                stack,
                "almanak.framework.dashboard.templates.lending_dashboard",
                call_log,
                extra_targets=(
                    "plot_health_factor_gauge",
                    "plot_ltv_ratio",
                    "plot_collateral_breakdown",
                    "plot_lending_rates_comparison",
                ),
            )
            render_lending_dashboard("strat-1", {}, {}, get_aave_v3_config())
        assert call_log == _EXPECTED_ORDER


class TestPerpTemplateSectionOrdering:
    """``render_perp_dashboard`` must call PnL → Cost Stack → Trade Tape exactly once."""

    def test_sections_called_once_in_order(self, call_log: list[tuple[str, Any]]) -> None:
        # ``has_position`` falsy so the position-dashboard branch is skipped — the
        # baked-in section calls happen regardless.
        session_state: dict[str, Any] = {"has_position": False}
        with ExitStack() as stack:
            _enter_template_patches(
                stack,
                "almanak.framework.dashboard.templates.perp_dashboard",
                call_log,
                extra_targets=(
                    "plot_perp_position_dashboard",
                    "plot_leverage_gauge",
                    "plot_funding_rate_history",
                    "plot_liquidation_levels",
                ),
            )
            render_perp_dashboard("strat-1", {}, session_state, PerpDashboardConfig())
        assert call_log == _EXPECTED_ORDER


class TestPredictionTemplateSectionOrdering:
    """``render_prediction_dashboard`` must call PnL → Cost Stack → Trade Tape exactly once."""

    def test_sections_called_once_in_order(self, call_log: list[tuple[str, Any]]) -> None:
        with ExitStack() as stack:
            _enter_template_patches(
                stack,
                "almanak.framework.dashboard.templates.prediction_dashboard",
                call_log,
                extra_targets=(
                    "plot_prediction_position",
                    "plot_arbitrage_opportunity",
                    "plot_probability_over_time",
                    "plot_market_outcomes",
                    "plot_prediction_pnl_breakdown",
                ),
            )
            render_prediction_dashboard("strat-1", {}, {}, PredictionDashboardConfig())
        assert call_log == _EXPECTED_ORDER
