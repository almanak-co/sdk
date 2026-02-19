"""Reusable UI components for the Almanak Operator Dashboard.

Contains shared components used across multiple pages.
Wires action buttons to real API endpoints.
"""

import logging
from datetime import datetime
from typing import Any

import requests
import streamlit as st

from almanak.framework.dashboard.config import API_BASE_URL, API_TIMEOUT
from almanak.framework.dashboard.models import AvailableAction, OperatorCard
from almanak.framework.dashboard.theme import get_chain_color, get_severity_color
from almanak.framework.dashboard.utils import format_chain_badge, format_usd, get_action_label, get_severity_icon

logger = logging.getLogger(__name__)


def call_operator_action(
    strategy_id: str, action: AvailableAction, params: dict[str, Any] | None = None
) -> dict[str, Any]:
    """Call an operator action API endpoint.

    Args:
        strategy_id: The strategy ID
        action: The action to execute
        params: Optional action-specific parameters

    Returns:
        API response as dict with success/error info
    """
    # Map AvailableAction to API endpoint
    action_endpoints = {
        AvailableAction.PAUSE: "pause",
        AvailableAction.RESUME: "resume",
        AvailableAction.BUMP_GAS: "bump-gas",
        AvailableAction.CANCEL_TX: "cancel-tx",
        AvailableAction.EMERGENCY_UNWIND: "emergency-unwind",
    }

    endpoint = action_endpoints.get(action)
    if not endpoint:
        return {"success": False, "error": f"Unknown action: {action}"}

    url = f"{API_BASE_URL}/api/strategies/{strategy_id}/{endpoint}"
    headers = {"Content-Type": "application/json", "X-API-Key": "demo-key"}

    try:
        response = requests.post(
            url,
            json=params or {},
            headers=headers,
            timeout=API_TIMEOUT,
        )

        if response.status_code == 200:
            return response.json()
        elif response.status_code == 404:
            return {"success": False, "error": f"Strategy {strategy_id} not found"}
        elif response.status_code == 400:
            error_detail = response.json().get("detail", "Bad request")
            return {"success": False, "error": error_detail}
        else:
            return {"success": False, "error": f"API error: {response.status_code}"}

    except requests.exceptions.ConnectionError:
        return {
            "success": False,
            "error": "API not available. Ensure the API server is running.",
            "connection_error": True,
        }
    except requests.exceptions.Timeout:
        return {"success": False, "error": "API request timed out"}
    except Exception as e:
        logger.exception(f"Operator action API call failed: {e}")
        return {"success": False, "error": str(e)}


def render_operator_card(card: OperatorCard, strategy_name: str) -> None:
    """Render the operator card for a stuck strategy."""
    severity_color = get_severity_color(card.severity)
    severity_icon = get_severity_icon(card.severity)

    # Chain badge if alert is chain-specific
    chain_badge_html = ""
    if card.alert_chain:
        chain_color = get_chain_color(card.alert_chain)
        chain_badge_html = format_chain_badge(card.alert_chain, chain_color)

    # Convert hex to rgba
    r = int(severity_color[1:3], 16)
    g = int(severity_color[3:5], 16)
    b = int(severity_color[5:7], 16)
    reason_text = card.reason.value.replace("_", " ").title()
    position_at_risk = format_usd(card.position_at_risk_usd)

    # Main card container - split into separate markdown calls for reliability
    st.markdown(
        f"""<div style="background-color: rgba({r}, {g}, {b}, 0.1); border: 2px solid {severity_color}; border-radius: 12px; padding: 1.5rem; margin-bottom: 1rem;">
<div style="display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 1rem; flex-wrap: wrap; gap: 0.5rem;">
<div><span style="font-size: 1.5rem;">{severity_icon}</span> <span style="font-size: 1.3rem; font-weight: bold; color: {severity_color};">{card.severity.value} - {reason_text}</span> {chain_badge_html}</div>
<span style="background-color: {severity_color}33; color: {severity_color}; padding: 0.25rem 0.75rem; border-radius: 16px; font-size: 0.85rem; font-weight: bold;">Position at Risk: {position_at_risk}</span>
</div>
<div style="color: #ccc; font-size: 1rem; line-height: 1.5;">{card.risk_description}</div>
</div>""",
        unsafe_allow_html=True,
    )

    # Context details in expander
    with st.expander("View Technical Context", expanded=False):
        for key, value in card.context.items():
            if isinstance(value, list):
                st.markdown(f"**{key.replace('_', ' ').title()}:**")
                for item in value:
                    if isinstance(item, dict):
                        st.json(item)
                    else:
                        st.write(f"  - {item}")
            else:
                st.markdown(f"**{key.replace('_', ' ').title()}:** `{value}`")

    # Auto-remediation countdown
    if card.auto_remediation and card.auto_remediation.enabled and card.auto_remediation.scheduled_at:
        time_until = (card.auto_remediation.scheduled_at - datetime.now()).total_seconds()
        if time_until > 0:
            action_label = get_action_label(card.auto_remediation.action)
            st.markdown(
                f"""
                <div style="
                    background-color: rgba(33, 150, 243, 0.1);
                    border: 1px solid #2196f3;
                    border-radius: 8px;
                    padding: 1rem;
                    margin-bottom: 1rem;
                ">
                    <div style="display: flex; align-items: center; gap: 0.5rem;">
                        <span style="font-size: 1.2rem;">⏱️</span>
                        <span style="color: #2196f3; font-weight: bold;">Auto-Remediation Scheduled</span>
                    </div>
                    <div style="margin-top: 0.5rem; color: #ccc;">
                        <strong>{action_label}</strong> will execute automatically in <strong>{int(time_until)} seconds</strong>
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )
            st.progress(
                1 - (time_until / card.auto_remediation.trigger_after_seconds),
                text=f"Auto-remediation in {int(time_until)}s",
            )

    # Recommended action section
    st.markdown("### Recommended Action")
    recommended = next((a for a in card.suggested_actions if a.is_recommended), None)
    if recommended:
        action_label = get_action_label(recommended.action)
        st.markdown(
            f"""
            <div style="
                background-color: rgba(0, 200, 83, 0.1);
                border: 2px solid #00c853;
                border-radius: 8px;
                padding: 1rem;
                margin-bottom: 1rem;
            ">
                <div style="display: flex; align-items: center; gap: 0.5rem;">
                    <span style="font-size: 1.2rem;">✅</span>
                    <span style="color: #00c853; font-weight: bold; font-size: 1.1rem;">{action_label}</span>
                </div>
                <div style="margin-top: 0.5rem; color: #ccc;">
                    {recommended.description}
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    # All action buttons
    st.markdown("### Available Actions")

    # Use session state for confirmation dialogs
    confirm_key = f"confirm_action_{card.strategy_id}"
    if confirm_key not in st.session_state:
        st.session_state[confirm_key] = None

    # Render action buttons in columns
    num_actions = len(card.available_actions)
    cols = st.columns(min(num_actions, 4))

    for idx, action in enumerate(card.available_actions):
        col_idx = idx % 4
        with cols[col_idx]:
            action_label = get_action_label(action)
            is_recommended = any(a.action == action and a.is_recommended for a in card.suggested_actions)

            if st.button(
                action_label,
                key=f"action_{card.strategy_id}_{action.value}",
                use_container_width=True,
                type="primary" if is_recommended else "secondary",
            ):
                st.session_state[confirm_key] = action

    # Confirmation dialog
    if st.session_state[confirm_key]:
        action = st.session_state[confirm_key]
        action_label = get_action_label(action)

        # Find the suggested action for description
        suggested = next((a for a in card.suggested_actions if a.action == action), None)
        description = suggested.description if suggested else f"Execute {action.value}"

        st.warning(f"**Confirm Action: {action_label}**\n\n{description}")

        col1, col2 = st.columns(2)
        with col1:
            if st.button("Confirm", key=f"confirm_yes_{card.strategy_id}", use_container_width=True, type="primary"):
                # Build params from suggested action
                params = suggested.params if suggested else {}

                # Call real API
                with st.spinner(f"Executing {action_label}..."):
                    result = call_operator_action(card.strategy_id, action, params)

                # Store result and clear confirmation
                st.session_state[confirm_key] = None
                if result.get("success"):
                    st.session_state[f"action_result_{card.strategy_id}"] = {
                        "success": True,
                        "action": action,
                        "message": result.get("message", f"{action_label} executed successfully"),
                    }
                else:
                    error_msg = result.get("error", "Action failed")
                    st.session_state[f"action_result_{card.strategy_id}"] = {
                        "success": False,
                        "action": action,
                        "message": error_msg,
                        "connection_error": result.get("connection_error", False),
                    }
                st.rerun()

        with col2:
            if st.button("Cancel", key=f"confirm_no_{card.strategy_id}", use_container_width=True):
                st.session_state[confirm_key] = None
                st.rerun()

    # Show action result feedback
    result_key = f"action_result_{card.strategy_id}"
    if result_key in st.session_state and st.session_state[result_key]:
        result = st.session_state[result_key]
        if result["success"]:
            st.success(result["message"])
        elif result.get("connection_error"):
            st.warning(f"API not available: {result['message']}")
        else:
            st.error(result["message"])
        # Clear after showing
        del st.session_state[result_key]


def render_back_button(label: str, target_page: str, **query_params) -> bool:
    """Render a back button that navigates to the target page.

    Args:
        label: Button label (e.g., "Back to Overview")
        target_page: Target page name
        **query_params: Additional query parameters to set

    Returns:
        True if button was clicked
    """
    if st.button(f"← {label}"):
        st.query_params["page"] = target_page
        for key, value in query_params.items():
            if value is not None:
                st.query_params[key] = str(value)
            elif key in st.query_params:
                del st.query_params[key]
        return True
    return False
