"""probe_avalanche_lp_lifecycle dashboard.

Baseline auto-generated dashboard (VIB-5115). Renders the framework's standard
audit sections -- PnL, cost stack, and trade tape -- sourced from the strategy's
accounting tables by deployment_id. Replace with a strategy-specific view as
the strategy matures.
"""

from __future__ import annotations

from typing import Any

import streamlit as st

from almanak.framework.dashboard import (
    render_cost_stack_section,
    render_pnl_section,
    render_trade_tape_section,
)


def render_custom_dashboard(
    deployment_id: str,
    strategy_config: dict[str, Any],
    api_client: Any,
    session_state: dict[str, Any],
) -> None:
    """Render the baseline dashboard for probe_avalanche_lp_lifecycle."""
    st.title("probe_avalanche_lp_lifecycle")

    chain = strategy_config.get("chain")
    if chain:
        st.markdown(f"**Chain:** `{chain}`")
    st.markdown(f"**Deployment ID:** `{deployment_id}`")
    st.divider()

    render_pnl_section(deployment_id)
    render_cost_stack_section(deployment_id)
    render_trade_tape_section(deployment_id)
