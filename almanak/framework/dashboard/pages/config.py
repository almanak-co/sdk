"""Config editor page for the Almanak Operator Dashboard.

Allows editing strategy configuration parameters.
Loads real config from strategy config.json files.
"""

import json
import logging
from decimal import Decimal
from pathlib import Path
from typing import Any

import requests
import streamlit as st

from almanak.framework.dashboard.config import (
    API_BASE_URL,
    API_TIMEOUT,
    CONFIG_PARAM_DEFINITIONS,
)
from almanak.framework.dashboard.models import Strategy, StrategyConfig

logger = logging.getLogger(__name__)

# Default config values for parameters not in config file
DEFAULT_CONFIG_VALUES = {
    "max_slippage": Decimal("0.005"),
    "trade_size_usd": Decimal("1000"),
    "rebalance_threshold": Decimal("0.05"),
    "min_health_factor": Decimal("1.5"),
    "max_leverage": Decimal("3"),
    "daily_loss_limit_usd": Decimal("500"),
}


def load_strategy_config(strategy: Strategy) -> StrategyConfig:
    """Load config from strategy's config.json file.

    Args:
        strategy: Strategy object (may have config_path set)

    Returns:
        StrategyConfig populated from real file or defaults
    """
    config_data = {}

    # Try to load from config_path if available
    if strategy.config_path:
        config_file = Path(strategy.config_path)
        if config_file.exists():
            try:
                config_data = json.loads(config_file.read_text())
                logger.info(f"Loaded config from {config_file}")
            except (OSError, json.JSONDecodeError) as e:
                logger.warning(f"Failed to load config from {config_file}: {e}")

    # If no config_path, try to find it in strategies directory
    if not config_data:
        strategies_root = Path(__file__).parent.parent.parent.parent / "strategies"
        for category in ["demo", "production", "incubating", "poster_child"]:
            # Try strategy.id directly
            config_file = strategies_root / category / strategy.id / "config.json"
            if config_file.exists():
                try:
                    config_data = json.loads(config_file.read_text())
                    logger.info(f"Found config at {config_file}")
                    break
                except (OSError, json.JSONDecodeError):
                    continue

            # Try matching by strategy name
            category_dir = strategies_root / category
            if category_dir.exists():
                for subdir in category_dir.iterdir():
                    if subdir.is_dir() and subdir.name in strategy.id:
                        config_file = subdir / "config.json"
                        if config_file.exists():
                            try:
                                config_data = json.loads(config_file.read_text())
                                logger.info(f"Found config at {config_file}")
                                break
                            except (OSError, json.JSONDecodeError):
                                continue

    # Build StrategyConfig with values from file or defaults
    def get_decimal(key: str, default: Decimal) -> Decimal:
        if key in config_data:
            try:
                return Decimal(str(config_data[key]))
            except (ValueError, TypeError):
                return default
        return default

    return StrategyConfig(
        strategy_id=strategy.id,
        strategy_name=strategy.name,
        max_slippage=get_decimal("max_slippage", DEFAULT_CONFIG_VALUES["max_slippage"]),
        trade_size_usd=get_decimal("trade_size_usd", DEFAULT_CONFIG_VALUES["trade_size_usd"]),
        rebalance_threshold=get_decimal("rebalance_threshold", DEFAULT_CONFIG_VALUES["rebalance_threshold"]),
        min_health_factor=get_decimal("min_health_factor", DEFAULT_CONFIG_VALUES["min_health_factor"]),
        max_leverage=get_decimal("max_leverage", DEFAULT_CONFIG_VALUES["max_leverage"]),
        daily_loss_limit_usd=get_decimal("daily_loss_limit_usd", DEFAULT_CONFIG_VALUES["daily_loss_limit_usd"]),
        last_updated=None,
        update_count=0,
        config_history=[],
    )


def render_risk_guard_guidance(guidance: list[dict[str, Any]]) -> None:
    """Render Risk Guard guidance when configuration updates are blocked."""
    st.markdown("### Risk Guard Blocked This Action")
    st.markdown(
        "The following configuration changes exceed safety limits. "
        "Review the guidance below to understand why and how to proceed."
    )

    for item in guidance:
        limit_name = item.get("limit_name", "Unknown Limit")
        field_name = item.get("field_name", "unknown")
        requested_value = item.get("requested_value", "N/A")
        limit_value = item.get("limit_value", "N/A")
        explanation = item.get("explanation", "No explanation available.")
        suggestion = item.get("suggestion", "Contact your administrator.")

        # Create an expander for each guidance item
        with st.expander(f"Limit: {limit_name}", expanded=True):
            col1, col2 = st.columns(2)
            with col1:
                st.markdown(f"**Requested Value:** `{requested_value}`")
            with col2:
                st.markdown(f"**Maximum Allowed:** `{limit_value}`")

            st.markdown("---")
            st.markdown("**Why this limit exists:**")
            st.markdown(f"> {explanation}")

            st.markdown("---")
            st.markdown("**Suggestion:**")
            st.info(suggestion)

            st.markdown(f"*Field: `{field_name}`*")


def call_config_update_api(
    strategy_id: str, updates: dict[str, Decimal], api_base_url: str = API_BASE_URL
) -> dict[str, Any]:
    """Call the config update API endpoint."""
    url = f"{api_base_url}/api/strategies/{strategy_id}/config"

    # Convert Decimal values to strings for JSON serialization
    payload = {"updates": {field: str(value) for field, value in updates.items()}}

    try:
        response = requests.post(
            url,
            json=payload,
            headers={"Content-Type": "application/json", "X-API-Key": "demo-key"},
            timeout=API_TIMEOUT,
        )

        if response.status_code == 200:
            return response.json()
        else:
            return {
                "success": False,
                "error": f"API returned status {response.status_code}: {response.text}",
            }

    except requests.exceptions.ConnectionError:
        # API not running - return error, don't simulate success
        return {
            "success": False,
            "error": "API server is not running. Start the API with `python -m src.api.main` to apply config changes.",
            "api_unavailable": True,
        }
    except requests.exceptions.Timeout:
        return {
            "success": False,
            "error": "API request timed out",
        }
    except Exception as e:
        return {
            "success": False,
            "error": f"API request failed: {str(e)}",
        }


def render_config_param_input(
    param_name: str,
    current_value: Decimal,
    definition: dict[str, Any],
    key_prefix: str,
) -> Decimal | None:
    """Render an input control for a config parameter."""
    label = definition["label"]
    description = definition["description"]
    unit = definition["unit"]
    multiplier = definition["multiplier"]
    min_val = definition["min"]
    max_val = definition["max"]
    step = definition["step"]
    input_type = definition["input_type"]

    # Convert current value for display
    display_value = float(current_value) * multiplier

    # Display parameter header with description
    st.markdown(f"**{label}**")
    st.caption(f"{description} (Valid range: {min_val}{unit} - {max_val}{unit})")

    # Create input based on type
    widget_key = f"{key_prefix}_{param_name}"

    if input_type == "slider":
        new_display_value = st.slider(
            label=f"{label} ({unit})" if unit else label,
            min_value=min_val,
            max_value=max_val,
            value=display_value,
            step=step,
            key=widget_key,
            label_visibility="collapsed",
        )
    else:  # number input
        col1, col2 = st.columns([3, 1])
        with col1:
            new_display_value = st.number_input(
                label=f"{label}",
                min_value=min_val,
                max_value=max_val,
                value=display_value,
                step=step,
                key=widget_key,
                label_visibility="collapsed",
            )
        with col2:
            if unit:
                st.markdown(f"<div style='padding-top: 0.5rem;'>{unit}</div>", unsafe_allow_html=True)

    # Convert back to Decimal
    new_value = Decimal(str(new_display_value / multiplier))

    # Return new value if changed
    if new_value != current_value:
        return new_value
    return None


def render_config_history(config: StrategyConfig) -> None:
    """Render the config change history."""
    if not config.config_history:
        st.info("No config change history available.")
        return

    # Sort history by timestamp descending
    sorted_history = sorted(config.config_history, key=lambda h: h.timestamp, reverse=True)

    for entry in sorted_history:
        time_str = entry.timestamp.strftime("%Y-%m-%d %H:%M")

        # Build changes summary
        changes_html_parts = []
        for field_name, change in entry.changes.items():
            param_def = CONFIG_PARAM_DEFINITIONS.get(field_name, {})
            label = param_def.get("label", field_name)
            unit = param_def.get("unit", "")
            multiplier = param_def.get("multiplier", 1)

            old_display = float(change["old"]) * multiplier
            new_display = float(change["new"]) * multiplier

            changes_html_parts.append(
                f"<li><strong>{label}:</strong> {old_display:.2f}{unit} -> {new_display:.2f}{unit}</li>"
            )

        changes_html = "<ul style='margin: 0.5rem 0; padding-left: 1.5rem;'>" + "".join(changes_html_parts) + "</ul>"

        st.markdown(
            f"""
            <div style="
                background-color: #1e1e1e;
                border: 1px solid #333;
                border-left: 3px solid #2196f3;
                border-radius: 0 8px 8px 0;
                padding: 1rem;
                margin-bottom: 0.5rem;
            ">
                <div style="display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 0.5rem;">
                    <div>
                        <strong>Version {entry.version}</strong>
                        <span style="color: #888; margin-left: 1rem;">{time_str}</span>
                    </div>
                    <span style="color: #888; font-size: 0.85rem;">{entry.changed_by}</span>
                </div>
                <div style="color: #ccc;">
                    {changes_html}
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )


def page(strategies: list[Strategy]) -> None:
    """Render the config editor page.

    Args:
        strategies: List of all strategy data objects
    """
    try:
        # Always show a test message to verify page is rendering
        st.markdown("### Config Editor Page")

        # Debug info
        if st.session_state.get("show_debug", False):
            with st.expander("🔍 Config Page Debug"):
                st.write(f"Strategies count: {len(strategies)}")
                st.write(f"Query params: {dict(st.query_params)}")
                st.write(
                    f"CONFIG_PARAM_DEFINITIONS keys: {list(CONFIG_PARAM_DEFINITIONS.keys()) if CONFIG_PARAM_DEFINITIONS else 'NOT LOADED'}"
                )
                st.write(f"CONFIG_PARAM_DEFINITIONS type: {type(CONFIG_PARAM_DEFINITIONS)}")

        # Get strategy ID from query params
        strategy_id = st.query_params.get("strategy_id")

        # Get strategy ID from query params
        strategy_id = st.query_params.get("strategy_id")

        if not strategy_id:
            st.info("👈 Please select a strategy from the sidebar to edit its configuration.")
            st.markdown("### Or select a strategy here:")
            if strategies:
                strategy_names = [f"{s.name} ({s.id[:12]}...)" for s in strategies]
                selected_idx = st.selectbox(
                    "Choose a strategy",
                    range(len(strategy_names)),
                    format_func=lambda x: strategy_names[x],
                    key="config_strategy_selector",
                )
                if st.button("Edit Config", use_container_width=True):
                    st.query_params["strategy_id"] = strategies[selected_idx].id
                    st.rerun()
            else:
                st.warning("No strategies found. Make sure you have strategies running or check your state database.")
                if st.button("Go to Overview"):
                    st.query_params["page"] = "overview"
            return

        strategy = next((s for s in strategies if s.id == strategy_id), None)

        if not strategy:
            st.error(f"Strategy {strategy_id} not found.")
            if st.button("Go to Overview"):
                st.query_params["page"] = "overview"
            return

        # Back button
        if st.button("← Back to Strategy Detail"):
            st.query_params["page"] = "detail"

        # Header
        st.markdown(f"## Config Editor: {strategy.name}")
        st.markdown(f"**Chain:** {strategy.chain.upper()} | **Protocol:** {strategy.protocol}")

        st.divider()

        # Warning about changes
        st.info(
            "**Important:** Changes take effect on the next strategy iteration. "
            "The strategy will continue using current values until the current iteration completes."
        )

        # Load current config from real config file
        try:
            config = load_strategy_config(strategy)
            if st.session_state.get("show_debug", False):
                st.write(f"✅ Config loaded: {config.strategy_name}")
        except Exception as e:
            st.error(f"Failed to load config: {e}")
            import traceback

            with st.expander("Error Details"):
                st.code(traceback.format_exc())
            return

        # Show config source info
        if strategy.config_path:
            st.caption(f"Config loaded from: `{strategy.config_path}`")
        else:
            st.caption("Using default configuration values")

        # Initialize session state for config editor
        config_state_key = f"config_editor_{strategy_id}"
        if config_state_key not in st.session_state:
            st.session_state[config_state_key] = {
                "pending_changes": {},
                "show_history": False,
            }

        # Create two columns for the editor layout
        col1, col2 = st.columns([2, 1])

        with col1:
            # Trading Parameters Section
            st.markdown("### Trading Parameters")

            # Check if CONFIG_PARAM_DEFINITIONS is loaded
            if not CONFIG_PARAM_DEFINITIONS:
                st.error("CONFIG_PARAM_DEFINITIONS not loaded! Check imports.")
                return

            trading_params = [p for p, d in CONFIG_PARAM_DEFINITIONS.items() if d.get("category") == "trading"]

            if not trading_params:
                st.warning("No trading parameters found in CONFIG_PARAM_DEFINITIONS")
                if st.session_state.get("show_debug", False):
                    st.write(f"Available params: {list(CONFIG_PARAM_DEFINITIONS.keys())}")
            else:
                st.caption(f"Found {len(trading_params)} trading parameters")

            for param_name in trading_params:
                definition = CONFIG_PARAM_DEFINITIONS[param_name]
                current_value = getattr(config, param_name)

                new_value = render_config_param_input(
                    param_name=param_name,
                    current_value=current_value,
                    definition=definition,
                    key_prefix=f"config_{strategy_id}",
                )

                if new_value is not None:
                    st.session_state[config_state_key]["pending_changes"][param_name] = new_value
                elif param_name in st.session_state[config_state_key]["pending_changes"]:
                    # Value was changed back to original
                    if st.session_state[config_state_key]["pending_changes"][param_name] == current_value:
                        del st.session_state[config_state_key]["pending_changes"][param_name]

                st.markdown("---")

            st.markdown("### Risk Parameters")

            risk_params = [p for p, d in CONFIG_PARAM_DEFINITIONS.items() if d.get("category") == "risk"]

            if not risk_params:
                st.warning("No risk parameters found in CONFIG_PARAM_DEFINITIONS")

            for param_name in risk_params:
                definition = CONFIG_PARAM_DEFINITIONS[param_name]
                current_value = getattr(config, param_name)

                new_value = render_config_param_input(
                    param_name=param_name,
                    current_value=current_value,
                    definition=definition,
                    key_prefix=f"config_{strategy_id}",
                )

                if new_value is not None:
                    st.session_state[config_state_key]["pending_changes"][param_name] = new_value
                elif param_name in st.session_state[config_state_key]["pending_changes"]:
                    # Value was changed back to original
                    if st.session_state[config_state_key]["pending_changes"][param_name] == current_value:
                        del st.session_state[config_state_key]["pending_changes"][param_name]

                st.markdown("---")

        with col2:
            # Summary Panel
            st.markdown("### Current Configuration")

            # Show last updated
            if config.last_updated:
                st.markdown(f"**Last Updated:** {config.last_updated.strftime('%Y-%m-%d %H:%M')}")
            st.markdown(f"**Update Count:** {config.update_count}")

            st.divider()

            # Show pending changes summary
            pending = st.session_state[config_state_key]["pending_changes"]
            if pending:
                st.markdown("### Pending Changes")
                for field_name, new_value in pending.items():
                    definition = CONFIG_PARAM_DEFINITIONS.get(field_name, {})
                    label = definition.get("label", field_name)
                    unit = definition.get("unit", "")
                    multiplier = definition.get("multiplier", 1)

                    old_value = getattr(config, field_name)
                    old_display = float(old_value) * multiplier
                    new_display = float(new_value) * multiplier

                    st.markdown(
                        f"""
                        <div style="
                            background-color: rgba(255, 193, 7, 0.1);
                            border: 1px solid #ffc107;
                            border-radius: 4px;
                            padding: 0.5rem;
                            margin-bottom: 0.5rem;
                        ">
                            <strong>{label}</strong><br/>
                            <span style="color: #888;">{old_display:.2f}{unit}</span>
                            <span style="color: #ffc107;"> -> </span>
                            <span style="color: #00c853;">{new_display:.2f}{unit}</span>
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )
            else:
                st.markdown("### No Pending Changes")
                st.caption("Modify parameters on the left to see changes here.")

            st.divider()

            # View History button
            if st.button("View Change History", use_container_width=True):
                st.session_state[config_state_key]["show_history"] = not st.session_state[config_state_key][
                    "show_history"
                ]

        st.divider()

        # Action buttons
        col1, col2, col3 = st.columns(3)

        with col1:
            apply_disabled = len(st.session_state[config_state_key]["pending_changes"]) == 0
            if st.button(
                "Apply Changes",
                type="primary",
                use_container_width=True,
                disabled=apply_disabled,
            ):
                # Call API to update config
                pending_changes = st.session_state[config_state_key]["pending_changes"]
                result = call_config_update_api(strategy_id, pending_changes)

                if result.get("success"):
                    st.session_state[f"config_result_{strategy_id}"] = {
                        "success": True,
                        "message": result.get("message", "Configuration updated successfully!"),
                        "simulated": result.get("simulated", False),
                    }
                    # Clear pending changes
                    st.session_state[config_state_key]["pending_changes"] = {}
                    st.rerun()
                else:
                    st.session_state[f"config_result_{strategy_id}"] = {
                        "success": False,
                        "message": result.get("error", "Failed to update configuration"),
                        "guidance": result.get("guidance"),
                    }
                    st.rerun()

        with col2:
            if st.button("Reset to Defaults", use_container_width=True):
                # Reset all inputs to defaults
                st.session_state[config_state_key]["pending_changes"] = {}
                # Reset individual widget values
                for param_name in CONFIG_PARAM_DEFINITIONS.keys():
                    widget_key = f"config_{strategy_id}_{param_name}"
                    if widget_key in st.session_state:
                        del st.session_state[widget_key]
                st.rerun()

        with col3:
            if st.button("Discard Changes", use_container_width=True):
                # Clear pending changes
                st.session_state[config_state_key]["pending_changes"] = {}
                # Reset individual widget values
                for param_name in CONFIG_PARAM_DEFINITIONS.keys():
                    widget_key = f"config_{strategy_id}_{param_name}"
                    if widget_key in st.session_state:
                        del st.session_state[widget_key]
                st.rerun()

        # Show result feedback
        result_key = f"config_result_{strategy_id}"
        if result_key in st.session_state and st.session_state[result_key]:
            result = st.session_state[result_key]
            if result["success"]:
                if result.get("simulated"):
                    st.warning(f"{result['message']}")
                else:
                    st.success(f"{result['message']}")
            else:
                st.error(f"{result['message']}")
                # Display Risk Guard guidance if available
                guidance = result.get("guidance")
                if guidance:
                    render_risk_guard_guidance(guidance)
            # Clear result after showing
            del st.session_state[result_key]

        # Show history if toggled
        if st.session_state[config_state_key].get("show_history"):
            st.divider()
            st.markdown("## Configuration Change History")
            render_config_history(config)

    except Exception as e:
        st.error(f"Error rendering config page: {e}")
        import traceback

        with st.expander("Full Error Traceback"):
            st.code(traceback.format_exc())
        st.info("Please check the error details above and report this issue.")
