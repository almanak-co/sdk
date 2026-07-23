"""Basic demo-agnostic custom dashboard — shared, WITH charts.

A single, reusable ``render_basic_dashboard`` that any strategy without a
bespoke dashboard can adopt by re-exporting it as ``render_custom_dashboard``
from its ``dashboard/ui.py`` (the interface name the loader discovers, see
``almanak.framework.dashboard.custom.loader.get_dashboard_render_function``).

It composes the framework's deployment-scoped section renderers, which draw
real Plotly time-series and tables from gateway-backed data:
  * ``render_pnl_section``          — NAV / deployed / PnL metric cards
  * ``render_nav_history_section``  — NAV + PnL + drawdown OVER TIME (interactive range) chart
  * ``render_cost_stack_section``   — gas / fees / slippage / earn bars
  * ``render_position_lifecycle_section`` — OPEN/CLOSE events + PnL attribution table
  * ``render_trade_tape_section``   — per-trade tape

Each panel is isolated behind ``_safe`` so a transient gateway/RPC hiccup
degrades that one panel to a notice instead of blanking the page.

This module imports ``streamlit`` (transitively via ``sections``) and therefore
lives under ``dashboard/custom/`` alongside the other streamlit-using renderers,
never eagerly re-exported from ``almanak.framework.dashboard.__init__`` — the
gateway image strips ``streamlit`` (VIB-4048).
"""

from __future__ import annotations

from typing import Any

import streamlit as st

from almanak.framework.dashboard.sections import (
    render_cost_stack_section,
    render_nav_history_section,
    render_perp_positions_section,
    render_pnl_section,
    render_position_lifecycle_section,
    render_trade_tape_section,
)


def _safe(label: str, fn: Any, *args: Any, **kwargs: Any) -> None:
    try:
        fn(*args, **kwargs)
    except Exception as exc:  # noqa: BLE001 - a dashboard panel must degrade, not crash
        st.info(f"{label}: temporarily unavailable ({type(exc).__name__}). Auto-refresh will retry.")


def render_basic_dashboard(
    deployment_id: str,
    strategy_config: dict[str, Any],
    api_client: Any,
    session_state: dict[str, Any],
    *,
    include_perp_section: bool = False,
) -> None:
    """Render the shared, demo-agnostic charted dashboard for ``deployment_id``.

    Chain / protocol are read from ``strategy_config`` at runtime, so a single
    implementation serves every demo without per-strategy hardcoding.

    ``include_perp_section=True`` adds the snapshot-derived perp position story
    (:func:`render_perp_positions_section`) directly under the PnL cards — perp
    demos opt in; the section deliberately always renders a header (outage vs
    empty-book honesty, VIB-5942), so it is NOT wired unconditionally into the
    non-perp demos that share this layout.
    """
    cfg = strategy_config or {}
    chain = str(cfg.get("chain", "—"))
    protocol = str(cfg.get("protocol") or cfg.get("teardown_protocol") or cfg.get("rate_protocol") or "")

    st.title("Strategy Dashboard")
    caption = f"deployment `{deployment_id}` · chain **{chain}**"
    if protocol:
        caption += f" · {protocol}"
    st.caption(caption)

    _safe("PnL summary", render_pnl_section, deployment_id)
    if include_perp_section:
        _safe("Perp positions", render_perp_positions_section, deployment_id)
    _safe("NAV / PnL / drawdown over time", render_nav_history_section, deployment_id, default_range="All")
    _safe("Cost stack", render_cost_stack_section, deployment_id)
    _safe("Positions", render_position_lifecycle_section, deployment_id, api_client)
    _safe("Trade tape", render_trade_tape_section, deployment_id)
