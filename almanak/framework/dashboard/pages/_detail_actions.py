"""Operator-action handlers for the strategy detail page.

Extracted from ``detail.py`` (Phase 5a of the Dashboard refactor plan) to isolate
the operator-facing action surface (pause, resume, config, refresh, bump-gas,
close) from the surrounding page layout. All functions in this module are pure
Streamlit UI renderers; the underlying action dispatch lives in
``detail.call_strategy_action`` and remains directly unit-testable without a
Streamlit runtime.

Public helpers:
    * ``render_action_row`` - buttons grid (and click handling).
    * ``handle_action_result`` - post-click feedback rendering.
    * ``render_gas_bump_dialog`` - the gas-bump prompt shown for STUCK strategies.
"""

from __future__ import annotations

import streamlit as st

from almanak.framework.dashboard.config import SystemHealth
from almanak.framework.dashboard.models import Strategy, StrategyStatus
from almanak.framework.dashboard.pages.detail import call_strategy_action


def _action_result_key(strategy_id: str) -> str:
    """Session-state key for the pending action result banner."""
    return f"action_result_{strategy_id}"


def _gas_dialog_key(strategy_id: str) -> str:
    """Session-state key for the gas-bump dialog visibility flag."""
    return f"show_gas_dialog_{strategy_id}"


def render_action_row(strategy: Strategy, health: SystemHealth) -> None:
    """Render the operator action button grid for a strategy.

    Mirrors the original inline block in ``detail.page`` verbatim in behaviour:
    five columns (pause/resume, config, refresh, bump-gas, close), with
    disabled state driven by ``SystemHealth.can_execute``.

    Args:
        strategy: The strategy whose detail page is being rendered.
        health: Current system health; drives button enablement.
    """
    st.markdown("### Actions")

    can_pause_resume = health.can_execute("pause_resume")
    can_bump_gas = health.can_execute("bump_gas")
    # Probe execute_teardown feature flag for parity with previous behaviour
    # (result currently unused but retained for side-effect symmetry).
    health.can_execute("execute_teardown")

    # Show warning if CLI isn't running
    if not health.cli_running:
        st.info(
            "**CLI Not Running** - Some actions are disabled. "
            "Start the strategy runner CLI to enable Pause/Resume, Bump Gas, and Execute Teardown.",
            icon="\u2139\ufe0f",
        )

    action_col1, action_col2, action_col3, action_col4, action_col5 = st.columns(5)

    action_result_key = _action_result_key(strategy.id)

    with action_col1:
        if strategy.status == StrategyStatus.RUNNING:
            if st.button("\u23f8\ufe0f Pause", use_container_width=True, disabled=not can_pause_resume):
                with st.spinner(f"Pausing {strategy.name}..."):
                    result = call_strategy_action(strategy.id, "pause")
                st.session_state[action_result_key] = result
                st.rerun()
        else:
            if st.button("\u25b6\ufe0f Resume", use_container_width=True, disabled=not can_pause_resume):
                with st.spinner(f"Resuming {strategy.name}..."):
                    result = call_strategy_action(strategy.id, "resume")
                st.session_state[action_result_key] = result
                st.rerun()

    with action_col2:
        if st.button("\u2699\ufe0f Config", use_container_width=True):
            st.query_params["page"] = "config"
            st.rerun()

    with action_col3:
        if st.button("\U0001f504 Refresh", use_container_width=True):
            st.toast("Refreshing strategy data...")
            st.rerun()

    with action_col4:
        if strategy.status == StrategyStatus.STUCK:
            if st.button("\u26fd Bump Gas", use_container_width=True, disabled=not can_bump_gas):
                st.session_state[_gas_dialog_key(strategy.id)] = True
                st.rerun()

    with action_col5:
        # Close Strategy button - preview always available, execution requires CLI.
        if st.button("\U0001f6aa Close Strategy", use_container_width=True, type="secondary"):
            st.query_params["page"] = "teardown"
            st.query_params["strategy_id"] = strategy.id
            st.rerun()


def handle_action_result(strategy_id: str) -> None:
    """Render the success / error banner for the last operator action.

    The result is popped after rendering so it displays exactly once,
    matching the prior inline behaviour.

    Args:
        strategy_id: The strategy identifier whose result to render.
    """
    action_result_key = _action_result_key(strategy_id)
    if action_result_key not in st.session_state:
        return

    result = st.session_state[action_result_key]
    if result.get("success"):
        st.success(result.get("message", "Action completed successfully"))
    else:
        error_msg = result.get("error", "Action failed")
        if result.get("connection_error"):
            st.warning(f"API not available: {error_msg}")
        else:
            st.error(error_msg)
    # Clear result after showing
    del st.session_state[action_result_key]


def render_gas_bump_dialog(strategy_id: str) -> None:
    """Render the gas-bump dialog (if toggled open) and handle submit/cancel.

    Args:
        strategy_id: The strategy the bump applies to.
    """
    gas_dialog_key = _gas_dialog_key(strategy_id)
    if not st.session_state.get(gas_dialog_key):
        return

    action_result_key = _action_result_key(strategy_id)

    st.markdown("---")
    st.markdown("#### Bump Gas Price")
    st.caption("Enter a higher gas price to speed up the pending transaction")

    col1, col2, col3 = st.columns([2, 1, 1])
    with col1:
        new_gas_price = st.number_input(
            "New Gas Price (Gwei)",
            min_value=0.1,
            max_value=1000.0,
            value=1.0,
            step=0.1,
            key=f"gas_price_input_{strategy_id}",
        )
    with col2:
        if st.button("Submit", key=f"submit_gas_{strategy_id}"):
            with st.spinner("Bumping gas price..."):
                result = call_strategy_action(strategy_id, "bump-gas", {"gas_price_gwei": new_gas_price})
            st.session_state[action_result_key] = result
            st.session_state[gas_dialog_key] = False
            st.rerun()
    with col3:
        if st.button("Cancel", key=f"cancel_gas_{strategy_id}"):
            st.session_state[gas_dialog_key] = False
            st.rerun()
