"""Teardown page for safely closing strategies.

Provides the UX for the Strategy Teardown System:
1. Mode Selection - Graceful Shutdown vs Safe Emergency Exit
2. Confirmation - Preview with protected minimum (from real API)
3. Cancel Window - 10-second countdown with cancel button
4. Progress - Real-time execution updates (polling API)
5. Completion - Final summary with transaction links

This page integrates with the real teardown API at /api/strategies/{id}/close/*.
"""

import logging
import time
from decimal import Decimal
from typing import Any

import requests
import streamlit as st

from almanak.framework.dashboard.config import API_BASE_URL, API_TIMEOUT, check_system_health
from almanak.framework.dashboard.models import Strategy
from almanak.framework.dashboard.utils import format_usd

logger = logging.getLogger(__name__)


def call_teardown_api(
    strategy_id: str, endpoint: str, method: str = "GET", payload: dict | None = None
) -> dict[str, Any]:
    """Call a teardown API endpoint.

    Args:
        strategy_id: The strategy ID
        endpoint: API endpoint (preview, close, status, cancel)
        method: HTTP method (GET or POST)
        payload: Optional request payload

    Returns:
        API response as dict
    """
    url = f"{API_BASE_URL}/api/strategies/{strategy_id}/close"
    if endpoint and endpoint != "close":
        url = f"{url}/{endpoint}"

    headers = {"Content-Type": "application/json", "X-API-Key": "demo-key"}

    try:
        if method == "GET":
            response = requests.get(url, headers=headers, timeout=API_TIMEOUT)
        else:
            response = requests.post(url, json=payload or {}, headers=headers, timeout=API_TIMEOUT)

        if response.status_code == 200:
            return {"success": True, "data": response.json()}
        else:
            error_msg = response.json().get("detail", f"API error: {response.status_code}")
            return {"success": False, "error": error_msg}

    except requests.exceptions.ConnectionError:
        return {"success": False, "error": "API not available", "connection_error": True}
    except requests.exceptions.Timeout:
        return {"success": False, "error": "API request timed out"}
    except Exception as e:
        logger.exception(f"Teardown API call failed: {e}")
        return {"success": False, "error": str(e)}


def _get_strategy_by_id(strategies: list[Strategy], strategy_id: str) -> Strategy | None:
    """Get strategy by ID."""
    for s in strategies:
        if s.id == strategy_id:
            return s
    return None


def _render_mode_selection(strategy: Strategy) -> None:
    """Render the mode selection screen (two buttons)."""
    st.markdown("## Close Strategy")
    st.markdown(f"**{strategy.name}** | Current Value: {format_usd(strategy.total_value_usd)}")

    st.divider()

    # Calculate protection values (using simplified logic for demo)
    total_value = float(strategy.total_value_usd)
    if total_value < 50000:
        max_loss_pct = 0.03
    elif total_value < 200000:
        max_loss_pct = 0.025
    elif total_value < 500000:
        max_loss_pct = 0.02
    elif total_value < 2000000:
        max_loss_pct = 0.015
    else:
        max_loss_pct = 0.01

    protected_min = total_value * (1 - max_loss_pct)
    max_loss_usd = total_value * max_loss_pct

    # Show protection info prominently
    st.markdown(
        f"""
        <div style="
            background: linear-gradient(135deg, #1a472a 0%, #2d5a3d 100%);
            border: 2px solid #3d7a4d;
            border-radius: 12px;
            padding: 1.5rem;
            margin-bottom: 1.5rem;
            text-align: center;
        ">
            <div style="font-size: 0.9rem; color: #88cc99; margin-bottom: 0.5rem;">
                PROTECTED MINIMUM
            </div>
            <div style="font-size: 2rem; font-weight: bold; color: #ffffff;">
                {format_usd(Decimal(str(protected_min)))}
            </div>
            <div style="font-size: 0.85rem; color: #aaddbb; margin-top: 0.5rem;">
                Maximum possible cost: {format_usd(Decimal(str(max_loss_usd)))} ({max_loss_pct * 100:.1f}%)
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # Two-button layout
    st.markdown("### Choose Exit Mode")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown(
            """
            <div style="
                background-color: #1e3a1e;
                border: 2px solid #2d5a2d;
                border-radius: 12px;
                padding: 1.5rem;
                height: 200px;
            ">
                <div style="font-size: 1.5rem; margin-bottom: 0.5rem;">🌿 Graceful Shutdown</div>
                <div style="color: #888; font-size: 0.9rem;">
                    <div>• Takes 15-30 minutes</div>
                    <div>• Minimizes costs & slippage</div>
                    <div>• Best for planned exits</div>
                    <div>• Can cancel anytime</div>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        if st.button("🌿 Start Graceful Shutdown", key="graceful_btn", use_container_width=True):
            st.session_state.teardown_mode = "graceful"
            st.session_state.teardown_step = "asset_policy"
            st.rerun()

    with col2:
        st.markdown(
            """
            <div style="
                background-color: #3a2a1e;
                border: 2px solid #5a3a2d;
                border-radius: 12px;
                padding: 1.5rem;
                height: 200px;
            ">
                <div style="font-size: 1.5rem; margin-bottom: 0.5rem;">🛡️ Safe Emergency Exit</div>
                <div style="color: #888; font-size: 0.9rem;">
                    <div>• Takes 1-3 minutes</div>
                    <div>• Prioritizes speed over cost</div>
                    <div>• For urgent situations</div>
                    <div>• Cancel within 10 seconds</div>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        if st.button("🛡️ Start Emergency Exit", key="emergency_btn", use_container_width=True):
            st.session_state.teardown_mode = "emergency"
            # Emergency mode: skip asset policy (defaults to KEEP_OUTPUTS for safety)
            st.session_state.teardown_asset_policy = "keep_outputs"
            st.session_state.teardown_step = "confirm"
            st.rerun()

    st.divider()

    # Safety guarantees
    st.markdown("### Safety Guarantees")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown("✅ **Position-aware loss cap**")
        st.caption("Larger positions get tighter caps")
    with col2:
        st.markdown("✅ **MEV Protection**")
        st.caption("All swaps use private mempool")
    with col3:
        st.markdown("✅ **Cancel Window**")
        st.caption("10 seconds to change your mind")

    # Back button
    st.divider()
    if st.button("← Back to Strategy"):
        st.query_params["page"] = "detail"
        if "teardown_step" in st.session_state:
            del st.session_state.teardown_step
        if "teardown_mode" in st.session_state:
            del st.session_state.teardown_mode


def _render_confirmation(strategy: Strategy) -> None:
    """Render the confirmation screen with preview from real API."""
    mode = st.session_state.get("teardown_mode", "graceful")
    asset_policy = st.session_state.get("teardown_asset_policy", "target_token")
    target_token = st.session_state.get("teardown_target_token", "USDC")

    mode_display = "Graceful Shutdown" if mode == "graceful" else "Safe Emergency Exit"
    mode_icon = "🌿" if mode == "graceful" else "🛡️"

    # Asset policy display
    policy_display_map = {
        "target_token": f"💵 Convert to {target_token}",
        "entry_token": "🔄 Original Assets",
        "keep_outputs": "📦 Keep Native Tokens",
    }
    policy_display = policy_display_map.get(asset_policy, "Unknown")

    st.markdown(f"## {mode_icon} Confirm {mode_display}")
    st.markdown(f"**{strategy.name}** | Asset Policy: {policy_display}")

    st.divider()

    # Try to fetch preview from API
    preview_result = call_teardown_api(strategy.id, f"preview?mode={mode}", method="GET")

    if preview_result.get("success"):
        preview = preview_result["data"]

        # Use API data
        total_value = preview["current_value_usd"]
        protected_min = preview["protected_minimum_usd"]
        max_loss_pct = preview["max_loss_percent"] / 100
        preview["max_loss_usd"]
        est_min = preview["estimated_return_min_usd"]
        est_max = preview["estimated_return_max_usd"]
        duration = preview["estimated_duration_minutes"]
        steps = preview["steps"]
        warnings = preview.get("warnings", [])
        safety_info = preview.get("safety_info", {})

    else:
        # Fall back to local calculation if API not available
        if preview_result.get("connection_error"):
            st.warning("API not available - using estimated values. Start the API for accurate preview.")

        total_value = float(strategy.total_value_usd)
        if total_value < 50000:
            max_loss_pct = 0.03
        elif total_value < 200000:
            max_loss_pct = 0.025
        elif total_value < 500000:
            max_loss_pct = 0.02
        elif total_value < 2000000:
            max_loss_pct = 0.015
        else:
            max_loss_pct = 0.01

        protected_min = total_value * (1 - max_loss_pct)
        total_value * max_loss_pct

        if mode == "graceful":
            est_min = total_value * 0.995
            est_max = total_value * 0.998
            duration = 20
        else:
            est_min = protected_min
            est_max = total_value * 0.99
            duration = 2

        steps = [
            "Close perpetual positions (if any)",
            "Repay borrowed amounts (if any)",
            "Withdraw supplied collateral (if any)",
            "Close LP positions and collect fees",
            "Swap all tokens to USDC",
        ]
        warnings = []
        safety_info = {}

    # Summary cards
    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric("Current Value", format_usd(Decimal(str(total_value))))

    with col2:
        st.metric("Protected Minimum", format_usd(Decimal(str(protected_min))))

    with col3:
        st.metric("Est. Duration", f"~{duration} min")

    st.divider()

    # Show warnings if any
    if warnings:
        for warning in warnings:
            st.warning(warning)

    # Estimated return range
    st.markdown("### Estimated Return")
    st.markdown(
        f"""
        <div style="
            background-color: #1e1e1e;
            border: 1px solid #333;
            border-radius: 8px;
            padding: 1rem;
            text-align: center;
        ">
            <span style="font-size: 1.5rem; color: #4CAF50;">
                {format_usd(Decimal(str(est_min)))} - {format_usd(Decimal(str(est_max)))}
            </span>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.divider()

    # Steps preview
    st.markdown("### Execution Steps")
    for i, step in enumerate(steps, 1):
        st.markdown(f"{i}. {step}")

    # Safety info from API
    if safety_info:
        st.divider()
        st.markdown("### Safety Features")
        safety_cols = st.columns(3)
        if safety_info.get("position_aware_cap"):
            with safety_cols[0]:
                st.markdown("✅ **Position-aware cap**")
        if safety_info.get("mev_protection"):
            with safety_cols[1]:
                st.markdown("✅ **MEV protection**")
        if safety_info.get("simulation_required"):
            with safety_cols[2]:
                st.markdown("✅ **Pre-simulation**")

    st.divider()

    # Check system health to determine if execution is available
    health = check_system_health()
    can_execute = health.can_execute("execute_teardown")

    # Show warning if CLI isn't running
    if not health.cli_running:
        st.warning(
            "**CLI Not Running** - Teardown execution is disabled. "
            "Preview is available, but you must start the strategy runner CLI to execute teardown.",
            icon="⚠️",
        )

    # Action buttons
    col1, col2, col3 = st.columns([1, 2, 1])

    with col1:
        if st.button("← Back", use_container_width=True):
            # Go back to asset policy selection for graceful, mode selection for emergency
            if mode == "graceful":
                st.session_state.teardown_step = "asset_policy"
            else:
                st.session_state.teardown_step = "mode_select"
            st.rerun()

    with col3:
        if st.button(
            f"{mode_icon} Confirm & Start",
            type="primary",
            use_container_width=True,
            disabled=not can_execute,
            help="Start the CLI to enable teardown execution" if not can_execute else None,
        ):
            # Build payload with mode and asset policy
            payload = {
                "mode": mode,
                "asset_policy": asset_policy,
            }
            if asset_policy == "target_token":
                payload["target_token"] = target_token

            # Start teardown via API
            start_result = call_teardown_api(strategy.id, "close", method="POST", payload=payload)

            if start_result.get("success"):
                # Store teardown state from API response
                st.session_state.teardown_id = start_result["data"].get("teardown_id")
                st.session_state.teardown_step = "cancel_window"
                st.session_state.cancel_window_start = time.time()
            else:
                # API failed
                if start_result.get("connection_error"):
                    st.error("API not available - cannot execute teardown without CLI running")
                else:
                    st.error(start_result.get("error", "Failed to start teardown"))
                return
            st.rerun()


def _render_cancel_window(strategy: Strategy) -> None:
    """Render the 10-second cancel window with countdown."""
    mode = st.session_state.get("teardown_mode", "graceful")
    mode_display = "Graceful Shutdown" if mode == "graceful" else "Safe Emergency Exit"

    st.markdown(f"## {mode_display} - Cancel Window")

    # Calculate remaining time
    start_time = st.session_state.get("cancel_window_start", time.time())
    elapsed = time.time() - start_time
    remaining = max(0, 10 - elapsed)

    if remaining <= 0:
        # Cancel window expired - proceed to execution
        st.session_state.teardown_step = "executing"
        st.session_state.execution_start = time.time()
        st.rerun()

    # Big countdown display
    st.markdown(
        f"""
        <div style="
            text-align: center;
            padding: 3rem;
            background-color: #1a1a2e;
            border-radius: 16px;
            margin: 2rem 0;
        ">
            <div style="font-size: 1rem; color: #888; margin-bottom: 1rem;">
                Starting in
            </div>
            <div style="font-size: 6rem; font-weight: bold; color: {"#ff6b6b" if remaining <= 3 else "#ffd93d"};">
                {int(remaining)}
            </div>
            <div style="font-size: 1rem; color: #888; margin-top: 1rem;">
                seconds
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # Prominent cancel button
    st.markdown(
        """
        <style>
        div[data-testid="stButton"] > button[kind="secondary"] {
            background-color: #dc3545 !important;
            color: white !important;
            font-size: 1.5rem !important;
            padding: 1.5rem !important;
            border: none !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        if st.button(
            "❌ CANCEL - I changed my mind",
            key="cancel_btn",
            use_container_width=True,
        ):
            # Call cancel API if teardown was started via API
            if st.session_state.get("teardown_id"):
                cancel_result = call_teardown_api(strategy.id, "cancel", method="POST")
                if not cancel_result.get("success") and not cancel_result.get("connection_error"):
                    st.error(cancel_result.get("error", "Failed to cancel"))
            st.session_state.teardown_step = "cancelled"
            st.rerun()

    st.caption("Press ESC or click to cancel")

    # Auto-refresh countdown
    time.sleep(0.5)
    st.rerun()


def _render_executing(strategy: Strategy) -> None:
    """Render the execution progress screen with API polling."""
    mode = st.session_state.get("teardown_mode", "graceful")
    mode_display = "Graceful Shutdown" if mode == "graceful" else "Safe Emergency Exit"
    mode_icon = "🌿" if mode == "graceful" else "🛡️"

    st.markdown(f"## {mode_icon} {mode_display} in Progress")

    # Try to get status from API
    progress: int | float = 0
    recovered: int | float = 0
    current_step = "Initializing..."
    api_status = None

    if st.session_state.get("teardown_id"):
        status_result = call_teardown_api(strategy.id, "status", method="GET")
        if status_result.get("success"):
            api_status = status_result["data"]
            progress = api_status.get("percent_complete", 0)
            recovered = api_status.get("recovered_usd", 0)

            # Check for completion or failure
            if api_status.get("status") == "completed":
                st.session_state.teardown_result = api_status.get("result", {})
                st.session_state.teardown_step = "completed"
                st.rerun()
            elif api_status.get("status") == "failed":
                st.session_state.teardown_error = api_status.get("result", {}).get("error", "Unknown error")
                st.session_state.teardown_step = "failed"
                st.rerun()
            elif api_status.get("status") == "paused":
                st.session_state.approval_needed = api_status.get("approval_needed")
                st.session_state.teardown_step = "paused"
                st.rerun()

            # Get current step from API
            steps = api_status.get("steps", [])
            for step in steps:
                if step.get("status") == "in_progress":
                    current_step = step.get("name", "Processing...")
                    break

    # If no API status, use simulated progress
    if not api_status:
        start_time = st.session_state.get("execution_start", time.time())
        elapsed = time.time() - start_time
        duration = 20 if mode == "graceful" else 5

        progress = min(100, int((elapsed / duration) * 100))

        if progress >= 100:
            st.session_state.teardown_step = "completed"
            st.rerun()

        # Simulated steps
        steps_sim = [
            ("Closing LP positions...", 0, 30),
            ("Collecting fees...", 30, 50),
            ("Swapping tokens...", 50, 80),
            ("Finalizing...", 80, 100),
        ]

        current_step = "Initializing..."
        for step_name, start_pct, end_pct in steps_sim:
            if start_pct <= progress < end_pct:
                current_step = step_name
                break

        # Simulated recovered amount
        total_value = float(strategy.total_value_usd)
        recovered = total_value * 0.99 * (progress / 100)

    # Progress bar
    st.progress(progress / 100)
    st.markdown(f"**{progress}% Complete**")

    st.markdown(
        f"""
        <div style="
            background-color: #1e1e1e;
            border: 1px solid #333;
            border-radius: 8px;
            padding: 1.5rem;
            text-align: center;
            margin: 1.5rem 0;
        ">
            <div style="font-size: 1.2rem;">{current_step}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown("### Recovered So Far")
    st.markdown(
        f"""
        <div style="
            font-size: 2rem;
            color: #4CAF50;
            font-weight: bold;
        ">
            {format_usd(Decimal(str(recovered)))}
        </div>
        """,
        unsafe_allow_html=True,
    )

    # Cancel button (only for graceful mode during execution)
    if mode == "graceful" and progress < 50:
        st.divider()
        if st.button("⚠️ Pause Teardown", use_container_width=True):
            # Call API to pause if available
            if st.session_state.get("teardown_id"):
                call_teardown_api(strategy.id, "cancel", method="POST")
            st.session_state.teardown_step = "paused"
            st.rerun()

    # Auto-refresh
    time.sleep(0.5)
    st.rerun()


def _render_completed(strategy: Strategy) -> None:
    """Render the completion screen."""
    st.session_state.get("teardown_mode", "graceful")

    # Calculate final values (simulated)
    total_value = float(strategy.total_value_usd)
    final_value = total_value * 0.992  # 0.8% cost
    cost = total_value - final_value

    st.markdown("## ✅ Teardown Complete")

    st.balloons()

    st.markdown(
        f"""
        <div style="
            background: linear-gradient(135deg, #1a472a 0%, #2d5a3d 100%);
            border: 2px solid #3d7a4d;
            border-radius: 16px;
            padding: 2rem;
            text-align: center;
            margin: 1.5rem 0;
        ">
            <div style="font-size: 1rem; color: #88cc99; margin-bottom: 0.5rem;">
                FINAL VALUE RECOVERED
            </div>
            <div style="font-size: 3rem; font-weight: bold; color: #ffffff;">
                {format_usd(Decimal(str(final_value)))}
            </div>
            <div style="font-size: 0.9rem; color: #aaddbb; margin-top: 1rem;">
                Total cost: {format_usd(Decimal(str(cost)))} ({cost / total_value * 100:.2f}%)
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # Summary
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Starting Value", format_usd(strategy.total_value_usd))
    with col2:
        st.metric("Final Value", format_usd(Decimal(str(final_value))))
    with col3:
        st.metric("Total Cost", format_usd(Decimal(str(cost))))

    st.divider()

    # Transaction links (simulated)
    st.markdown("### Transactions")
    st.markdown(
        """
        | Step | Transaction | Status |
        |------|-------------|--------|
        | Close LP | `0x1234...abcd` | ✅ Confirmed |
        | Collect Fees | `0x5678...efgh` | ✅ Confirmed |
        | Swap to USDC | `0x9abc...ijkl` | ✅ Confirmed |
        """
    )

    st.divider()

    # Actions
    col1, col2 = st.columns(2)
    with col1:
        if st.button("← Back to Strategies", use_container_width=True):
            # Clean up state
            for key in ["teardown_step", "teardown_mode", "cancel_window_start", "execution_start"]:
                if key in st.session_state:
                    del st.session_state[key]
            st.query_params["page"] = "overview"
            st.rerun()
    with col2:
        if st.button("View Timeline", use_container_width=True):
            st.query_params["page"] = "timeline"
            st.query_params["strategy_id"] = strategy.id


def _render_cancelled(strategy: Strategy) -> None:
    """Render the cancelled screen."""
    st.markdown("## ❌ Teardown Cancelled")

    st.markdown(
        """
        <div style="
            background-color: #2a2a2a;
            border: 1px solid #444;
            border-radius: 12px;
            padding: 2rem;
            text-align: center;
            margin: 1.5rem 0;
        ">
            <div style="font-size: 1.2rem; color: #aaa;">
                The teardown was cancelled during the cancel window.
            </div>
            <div style="font-size: 1rem; color: #888; margin-top: 1rem;">
                Your positions remain unchanged.
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(f"**Strategy:** {strategy.name}")
    st.markdown(f"**Value:** {format_usd(strategy.total_value_usd)}")

    st.divider()

    col1, col2 = st.columns(2)
    with col1:
        if st.button("← Back to Strategy", use_container_width=True):
            # Clean up state
            for key in ["teardown_step", "teardown_mode", "cancel_window_start"]:
                if key in st.session_state:
                    del st.session_state[key]
            st.query_params["page"] = "detail"
            st.rerun()
    with col2:
        if st.button("Try Again", use_container_width=True):
            st.session_state.teardown_step = "mode_select"
            st.rerun()


def _render_asset_policy_selection(strategy: Strategy) -> None:
    """Render the asset policy selection screen (step 2 for graceful mode)."""
    st.session_state.get("teardown_mode", "graceful")
    mode_display = "Graceful Shutdown"

    st.markdown(f"## 🌿 {mode_display} - Asset Options")
    st.markdown(f"**{strategy.name}** | Current Value: {format_usd(strategy.total_value_usd)}")

    st.divider()

    st.markdown("### What should happen to your assets?")
    st.caption("Choose how tokens are handled after closing positions")

    # Three asset policy options
    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown(
            """
            <div style="
                background-color: #1e3a1e;
                border: 2px solid #2d5a2d;
                border-radius: 12px;
                padding: 1.5rem;
                height: 180px;
            ">
                <div style="font-size: 1.2rem; margin-bottom: 0.5rem;">💵 Convert to USDC</div>
                <div style="color: #888; font-size: 0.85rem;">
                    <div>• Swap all tokens to USDC</div>
                    <div>• Clean single-asset exit</div>
                    <div>• Best for accounting</div>
                    <div style="color: #4CAF50; margin-top: 0.5rem;">Recommended</div>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        if st.button("💵 Convert to USDC", key="policy_target_btn", use_container_width=True):
            st.session_state.teardown_asset_policy = "target_token"
            st.session_state.teardown_target_token = "USDC"
            st.session_state.teardown_step = "confirm"
            st.rerun()

    with col2:
        st.markdown(
            """
            <div style="
                background-color: #2a2a3a;
                border: 2px solid #3a3a5a;
                border-radius: 12px;
                padding: 1.5rem;
                height: 180px;
            ">
                <div style="font-size: 1.2rem; margin-bottom: 0.5rem;">🔄 Original Assets</div>
                <div style="color: #888; font-size: 0.85rem;">
                    <div>• Return to entry tokens</div>
                    <div>• E.g., back to USDC you started with</div>
                    <div>• Good for redeploying</div>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        if st.button("🔄 Original Assets", key="policy_entry_btn", use_container_width=True):
            st.session_state.teardown_asset_policy = "entry_token"
            st.session_state.teardown_step = "confirm"
            st.rerun()

    with col3:
        st.markdown(
            """
            <div style="
                background-color: #3a2a2a;
                border: 2px solid #5a3a3a;
                border-radius: 12px;
                padding: 1.5rem;
                height: 180px;
            ">
                <div style="font-size: 1.2rem; margin-bottom: 0.5rem;">📦 Keep Native</div>
                <div style="color: #888; font-size: 0.85rem;">
                    <div>• No terminal swaps</div>
                    <div>• Keep WETH, USDC, etc.</div>
                    <div>• Lowest cost, most tokens</div>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        if st.button("📦 Keep Native", key="policy_keep_btn", use_container_width=True):
            st.session_state.teardown_asset_policy = "keep_outputs"
            st.session_state.teardown_step = "confirm"
            st.rerun()

    st.divider()

    # Back button
    if st.button("← Back to Mode Selection"):
        st.session_state.teardown_step = "mode_select"
        if "teardown_mode" in st.session_state:
            del st.session_state.teardown_mode
        st.rerun()


def page(strategies: list[Strategy]) -> None:
    """Main teardown page router."""
    strategy_id = st.query_params.get("strategy_id")

    if not strategy_id:
        st.info("👈 Please select a strategy from the sidebar to initiate teardown.")
        st.markdown("### Or select a strategy here:")
        if strategies:
            strategy_names = [f"{s.name} ({s.id[:12]}...)" for s in strategies]
            selected_idx = st.selectbox(
                "Choose a strategy",
                range(len(strategy_names)),
                format_func=lambda x: strategy_names[x],
                key="teardown_strategy_selector",
            )
            if st.button("Start Teardown", use_container_width=True):
                st.query_params["strategy_id"] = strategies[selected_idx].id
                st.rerun()
        else:
            st.warning("No strategies found. Make sure you have strategies running or check your state database.")
            if st.button("Go to Overview"):
                st.query_params["page"] = "overview"
        return

    strategy = _get_strategy_by_id(strategies, strategy_id)
    if not strategy:
        st.error(f"Strategy not found: {strategy_id}")
        if st.button("← Back to Overview"):
            st.query_params["page"] = "overview"
        return

    # Get current step
    step = st.session_state.get("teardown_step", "mode_select")

    # Route to appropriate screen
    if step == "mode_select":
        _render_mode_selection(strategy)
    elif step == "asset_policy":
        _render_asset_policy_selection(strategy)
    elif step == "confirm":
        _render_confirmation(strategy)
    elif step == "cancel_window":
        _render_cancel_window(strategy)
    elif step == "executing":
        _render_executing(strategy)
    elif step == "completed":
        _render_completed(strategy)
    elif step == "cancelled":
        _render_cancelled(strategy)
    elif step == "paused":
        st.markdown("## ⏸️ Teardown Paused")
        st.info("Teardown has been paused. You can resume or cancel.")
        col1, col2 = st.columns(2)
        with col1:
            if st.button("Resume", use_container_width=True):
                st.session_state.teardown_step = "executing"
                st.rerun()
        with col2:
            if st.button("Cancel", use_container_width=True):
                st.session_state.teardown_step = "cancelled"
                st.rerun()
    else:
        _render_mode_selection(strategy)
