"""Content-area renderers for the strategy detail page.

Extracted from ``detail.py`` (Phase 5c of the Dashboard refactor plan) to isolate
the main-content orchestration (two-column PnL + profile + position layout) and
the bridge / position-lifecycle / timeline lower-stack from the surrounding page
layout. All functions in this module are pure Streamlit UI renderers; the
individual sub-renderers they call (``render_pnl_chart``, ``render_profile_charts``,
``render_position_summary``, ``render_multi_chain_position_summary``,
``render_bridge_transfers``, ``render_position_lifecycle``,
``render_timeline_events``) stay in ``detail`` - only the orchestration plus the
``try/except + st.code(traceback)`` boilerplate is moved.

Public helpers:
    * ``_safe_render`` - decorator-style helper that collapses the ``try/except
      + st.error + st.code(traceback.format_exc())`` pattern that was duplicated
      five times inside ``detail.page``.
    * ``render_main_content_columns`` - two-column layout (left: PnL chart +
      profile charts; right: single- or multi-chain position summary).
    * ``render_bridge_and_lifecycle`` - bridge transfers (multi-chain only) +
      position lifecycle (SQLite) + recent timeline events, with dividers
      preserved in the original sequence.

Behaviour is preserved verbatim - including known latent issues tracked in
issues #1711-#1716 - so this module is a pure refactor.
"""

from __future__ import annotations

import traceback
from collections.abc import Callable

import streamlit as st

from almanak.framework.dashboard.models import Strategy


def _safe_render(fn: Callable[[Strategy], None], strategy: Strategy, section_name: str) -> None:
    """Invoke a section renderer with the original ``try/except`` boilerplate.

    The original ``detail.page`` wrapped each content section in an identical
    ``try: ... except Exception as e: st.error(...); st.code(traceback.format_exc())``
    block. This helper preserves that pattern exactly so the refactor is
    behaviour-preserving: the user-visible error banner and the full traceback
    ``st.code`` block are rendered on failure.

    Args:
        fn: A zero-return sub-renderer that takes the ``Strategy`` as its only
            positional argument (e.g. ``render_pnl_chart``).
        strategy: The strategy being rendered; forwarded to ``fn`` unchanged.
        section_name: Human-readable label used in the error banner - matches
            the phrasing of the original inline ``st.error`` calls (for example
            ``"PnL chart"`` renders as ``Error rendering PnL chart: <exc>``).

    Notes:
        ``BaseException`` subclasses that are not ``Exception`` (``KeyboardInterrupt``,
        ``SystemExit``) are intentionally NOT caught - same as the original
        ``except Exception`` clauses.
    """
    try:
        fn(strategy)
    except Exception as e:  # noqa: BLE001 - preserving prior bare-Exception behaviour
        st.error(f"Error rendering {section_name}: {e}")
        st.code(traceback.format_exc())


def render_main_content_columns(strategy: Strategy) -> None:
    """Render the two-column main-content layout.

    Left column (ratio 2):
        * ``Portfolio Performance (7 days)`` section header.
        * ``render_pnl_chart`` - portfolio-value + PnL tabs.
        * ``render_profile_charts`` - strategy-profile chart pack (TA / LP /
          LENDING / PERPS auto-detected).

    Right column (ratio 1):
        * ``render_multi_chain_position_summary`` when ``strategy.is_multi_chain``
          is truthy, otherwise ``render_position_summary``. This mirrors the
          original inline branch verbatim.

    Each sub-renderer is wrapped in ``_safe_render`` so an exception in one
    section does not take down the whole page.

    Args:
        strategy: The strategy whose detail page is being rendered.
    """
    # Imported lazily to avoid a circular import with ``detail`` (which imports
    # this module for delegation).
    from almanak.framework.dashboard.pages.detail import (
        render_multi_chain_position_summary,
        render_pnl_chart,
        render_position_summary,
        render_profile_charts,
    )

    left_col, right_col = st.columns([2, 1])

    with left_col:
        st.markdown("### Portfolio Performance (7 days)")
        _safe_render(render_pnl_chart, strategy, "PnL chart")
        _safe_render(render_profile_charts, strategy, "strategy insights")

    with right_col:
        if strategy.is_multi_chain:
            _safe_render(render_multi_chain_position_summary, strategy, "position summary")
        else:
            _safe_render(render_position_summary, strategy, "position summary")


def render_bridge_and_lifecycle(strategy: Strategy) -> None:
    """Render the bridge / lifecycle / timeline lower-stack.

    Order matches the original ``detail.page`` layout:
        1. Bridge transfers section - only when ``strategy.is_multi_chain`` AND
           ``strategy.bridge_transfers`` is non-empty. The divider after this
           section is emitted inside the same branch to match the original.
        2. Position lifecycle (SQLite-backed) - always attempted; the renderer
           no-ops silently when the local DB is missing.
        3. Recent timeline events (``limit=10``).

    Each major section is wrapped in ``_safe_render`` so a failure in one does
    not cascade. The bridge section is NOT wrapped because the original code
    only guarded lifecycle / timeline with try/except and preserving behaviour
    means leaving bridge unwrapped.

    Args:
        strategy: The strategy whose detail page is being rendered.
    """
    # Imported lazily to avoid a circular import with ``detail``.
    from almanak.framework.dashboard.pages.detail import (
        render_bridge_transfers,
        render_lp_position_history,
        render_position_lifecycle,
        render_positions_summary,
        render_timeline_events,
    )

    # Bridge Transfers section for multi-chain strategies - only emitted when
    # the strategy both spans chains and has at least one bridge transfer in
    # its history. Divider lives inside the branch to preserve the original
    # layout (no divider when the section is absent).
    if strategy.is_multi_chain and strategy.bridge_transfers:
        render_bridge_transfers(strategy)
        st.divider()

    # Positions table — one row per position from ``position_registry``,
    # defaults to open + closed so a torn-down strategy still surfaces what
    # it held. ``render_position_lifecycle`` (events) follows underneath.
    _safe_render(render_positions_summary, strategy, "positions summary")

    # Position Lifecycle (VIB-2777) - wrapped to surface SQLite/parse errors
    # without taking down the rest of the page.
    _safe_render(render_position_lifecycle, strategy, "position lifecycle")

    # LP position-range history — same plot the LP template uses
    # (``plot_positions_over_time``), surfaced for strategies that don't
    # ship a custom dashboard. No-ops when there are no LP events.
    _safe_render(render_lp_position_history, strategy, "LP position history")

    st.divider()

    # Trade tape (Senior-Quant primary) + Activity log (heartbeat-style
    # operational events). The trade tape reads as a broker statement
    # (one row per intent, joined with accounting + position events);
    # the activity log keeps the existing event stream for telemetry
    # (STRATEGY_STUCK, EXECUTION_FAILED, BRIDGE_*, etc).
    from almanak.framework.dashboard.pages.trade_tape import render_trade_tape

    tape_tab, activity_tab = st.tabs(["📒 Trade tape", "🔔 Activity log"])
    with tape_tab:
        _safe_render(
            lambda s: render_trade_tape(s.id, limit=50),
            strategy,
            "trade tape",
        )
    with activity_tab:
        _safe_render(
            lambda s: render_timeline_events(s, limit=100),
            strategy,
            "timeline events",
        )
