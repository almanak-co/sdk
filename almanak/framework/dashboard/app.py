"""Almanak Strategy Framework v2.0 - Operator Dashboard

Streamlit-based dashboard for monitoring and managing DeFi trading strategies.
Provides strategy overview, status monitoring, and operator actions.

This is the main entry point that sets up navigation and routing.
"""

import sys
import time
from datetime import datetime
from pathlib import Path

import streamlit as st

# Add project root to path for imports
# app.py is at almanak/framework/dashboard/app.py, so we need to go up 4 levels
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from almanak.framework.dashboard.config import INITIAL_SIDEBAR_STATE, LAYOUT, PAGE_ICON
from almanak.framework.dashboard.data_source import get_all_strategies
from almanak.framework.dashboard.pages import config as config_page
from almanak.framework.dashboard.pages import detail, library, overview, teardown, timeline
from almanak.framework.dashboard.theme import CUSTOM_CSS

PAGE_TITLE = "Almanak Command Center"

# Import custom dashboard integration (safe - doesn't crash if no custom dashboards)
try:
    from almanak.framework.dashboard.custom import (
        discover_custom_dashboards as _discover_custom_dashboards,
    )
    from almanak.framework.dashboard.custom import (
        render_custom_dashboard_safe as _render_custom_dashboard_safe,
    )
    from almanak.framework.dashboard.custom.renderer import create_mock_api_client as _create_mock_api_client

    CUSTOM_DASHBOARDS_AVAILABLE = True
    discover_custom_dashboards = _discover_custom_dashboards
    render_custom_dashboard_safe = _render_custom_dashboard_safe
    create_mock_api_client = _create_mock_api_client
except ImportError:
    CUSTOM_DASHBOARDS_AVAILABLE = False

    def discover_custom_dashboards(*args: object, **kwargs: object) -> list:
        return []

    render_custom_dashboard_safe = None  # type: ignore[assignment]
    create_mock_api_client = None  # type: ignore[assignment]


def render_custom_dashboard_page(
    dashboard_name: str,
    custom_dashboards: list,
    strategies: list,
) -> None:
    """Render a custom dashboard page with error boundary.

    Args:
        dashboard_name: Name of the custom dashboard to render
        custom_dashboards: List of discovered custom dashboards
        strategies: List of strategy data objects
    """
    # Find the dashboard info
    dashboard_info = None
    for d in custom_dashboards:
        if d.strategy_name == dashboard_name:
            dashboard_info = d
            break

    if dashboard_info is None:
        st.error(f"Custom dashboard not found: {dashboard_name}")
        if st.button("Return to Overview"):
            st.query_params["page"] = "overview"
            if "custom_dashboard" in st.query_params:
                del st.query_params["custom_dashboard"]
        return

    # Back button
    col1, col2 = st.columns([1, 5])
    with col1:
        if st.button("← Back"):
            st.query_params["page"] = "overview"
            if "custom_dashboard" in st.query_params:
                del st.query_params["custom_dashboard"]
    with col2:
        st.markdown(f"### {dashboard_info.icon or '📊'} {dashboard_info.display_name}")

    st.divider()

    # Find matching strategy config
    strategy_config = {}
    strategy_id = dashboard_info.strategy_name
    for s in strategies:
        if s.id == dashboard_name or dashboard_name in s.id:
            strategy_config = {
                "name": s.name,
                "status": s.status.value,
                "total_value": float(s.total_value_usd),
            }
            strategy_id = s.id
            break

    # Create API client
    api_client = None
    # if create_mock_api_client:
    #     api_client = create_mock_api_client()

    # Render the custom dashboard with error boundary
    if render_custom_dashboard_safe is not None:
        render_custom_dashboard_safe(
            dashboard_info=dashboard_info,
            strategy_id=strategy_id,
            strategy_config=strategy_config,
            api_client=api_client,
            session_state=dict(st.session_state),
        )
    else:
        st.warning("Custom dashboard rendering not available")


def main() -> None:
    """Main dashboard application."""
    st.set_page_config(
        page_title=PAGE_TITLE,
        page_icon=PAGE_ICON,
        layout=LAYOUT,
        initial_sidebar_state=INITIAL_SIDEBAR_STATE,
    )

    # Apply custom CSS
    st.markdown(CUSTOM_CSS, unsafe_allow_html=True)

    # Initialize session state for refresh functionality
    if "last_refresh" not in st.session_state:
        st.session_state.last_refresh = datetime.now()
    if "auto_refresh" not in st.session_state:
        st.session_state.auto_refresh = False
    if "refresh_interval" not in st.session_state:
        st.session_state.refresh_interval = 30  # seconds

    # Discover custom dashboards (safe - returns empty list if none)
    custom_dashboards: list = []
    if CUSTOM_DASHBOARDS_AVAILABLE:
        try:
            custom_dashboards = discover_custom_dashboards()
        except Exception:
            pass  # Silently fail - core dashboard continues working

    # Header with refresh controls
    col1, col2, col3, col4, col5 = st.columns([3, 1, 1, 1, 1])
    with col1:
        st.markdown(f"# {PAGE_ICON} {PAGE_TITLE}")
    with col2:
        if st.button("🔄 Refresh", help="Manually refresh data"):
            st.session_state.last_refresh = datetime.now()
            st.rerun()
    with col5:
        # Debug toggle
        st.session_state.show_debug = st.checkbox(
            "🐛 Debug", value=st.session_state.get("show_debug", False), help="Show debug information"
        )
    with col3:
        auto_refresh = st.toggle(
            "Auto",
            value=st.session_state.auto_refresh,
            help="Enable auto-refresh",
        )
        if auto_refresh != st.session_state.auto_refresh:
            st.session_state.auto_refresh = auto_refresh
            if auto_refresh:
                st.session_state.last_refresh = datetime.now()
            st.rerun()
    with col4:
        interval = st.selectbox(
            "Interval",
            options=[10, 30, 60, 120],
            index=[10, 30, 60, 120].index(st.session_state.refresh_interval),
            format_func=lambda x: f"{x}s",
            label_visibility="collapsed",
            disabled=not st.session_state.auto_refresh,
        )
        if interval != st.session_state.refresh_interval:
            st.session_state.refresh_interval = interval

    # Show last refresh time and countdown if auto-refresh enabled
    if st.session_state.auto_refresh:
        elapsed = (datetime.now() - st.session_state.last_refresh).total_seconds()
        remaining = max(0, st.session_state.refresh_interval - elapsed)
        st.caption(
            f"🟢 Auto-refresh ON | Last: {st.session_state.last_refresh.strftime('%H:%M:%S')} | "
            f"Next in: {int(remaining)}s"
        )
        # Auto-refresh when interval elapsed
        if elapsed >= st.session_state.refresh_interval:
            st.session_state.last_refresh = datetime.now()
            time.sleep(0.1)  # Small delay to prevent rapid loops
            st.rerun()
    else:
        st.caption(f"Last refreshed: {st.session_state.last_refresh.strftime('%H:%M:%S')}")

    # Get strategies
    try:
        strategies = get_all_strategies()
    except Exception as e:
        st.error(f"Error loading strategies: {e}")
        import traceback

        st.code(traceback.format_exc())
        strategies = []

    # Debug info (can be removed later)
    if st.session_state.get("show_debug", False):
        with st.expander("Debug Info"):
            st.write(f"Strategies found: {len(strategies)}")
            if strategies:
                st.write("Strategy IDs:", [s.id for s in strategies])
            st.write("Current page:", st.query_params.get("page", "overview"))
            st.write("Strategy ID param:", st.query_params.get("strategy_id"))

    # Get current page from query params
    current_page = st.query_params.get("page", "overview")

    # Route to appropriate page based on query params
    try:
        if current_page == "custom_dashboard":
            dashboard_name = st.query_params.get("custom_dashboard")
            if dashboard_name:
                render_custom_dashboard_page(dashboard_name, custom_dashboards, strategies)
            else:
                st.error("No custom dashboard specified")
                overview.page(strategies)
        elif current_page == "library":
            library.page()
        elif current_page == "config":
            config_page.page(strategies)
        elif current_page == "timeline":
            timeline.page(strategies)
        elif current_page == "detail":
            detail.page(strategies)
        elif current_page == "teardown":
            teardown.page(strategies)
        else:
            overview.page(strategies)
    except Exception as e:
        st.error(f"Error rendering page '{current_page}': {e}")
        import traceback

        with st.expander("Error Details"):
            st.code(traceback.format_exc())
        # Fallback to overview
        if current_page != "overview":
            st.info("Returning to overview page...")
            st.query_params["page"] = "overview"
            st.rerun()

    # Sidebar navigation
    with st.sidebar:
        st.markdown("### Navigation")

        if st.button("Command Center", key="nav_overview", use_container_width=True):
            st.query_params.clear()
            st.query_params["page"] = "overview"
            st.rerun()

        if st.button("Strategy Library", key="nav_library", use_container_width=True):
            st.query_params.clear()
            st.query_params["page"] = "library"
            st.rerun()

        st.divider()

        # Strategy selector - show above Pages
        st.markdown("### Select Strategy")
        if strategies:
            strategy_options = ["None"] + [f"{s.name} ({s.id[:8]}...)" for s in strategies]
            current_strategy_id = st.query_params.get("strategy_id")
            current_index = 0
            if current_strategy_id:
                # Find the index of the current strategy
                for idx, s in enumerate(strategies, start=1):
                    if s.id == current_strategy_id:
                        current_index = idx
                        break

            selected_strategy_display = st.selectbox(
                "Choose a strategy",
                options=range(len(strategy_options)),
                format_func=lambda x: strategy_options[x],
                index=current_index,
                key="sidebar_strategy_selector",
                label_visibility="collapsed",
            )

            # Update query params if strategy selection changed
            if selected_strategy_display > 0:
                selected_strategy = strategies[selected_strategy_display - 1]
                if selected_strategy.id != current_strategy_id:
                    st.query_params["strategy_id"] = selected_strategy.id
                    # If on overview, switch to detail page
                    if st.query_params.get("page") == "overview":
                        st.query_params["page"] = "detail"
                    st.rerun()
            elif current_strategy_id and selected_strategy_display == 0:
                # Strategy was deselected
                if "strategy_id" in st.query_params:
                    del st.query_params["strategy_id"]
                st.rerun()
        else:
            st.caption("No strategies available")

        st.divider()

        st.markdown("### Pages")

        if st.button("📊 Detail", key="nav_detail", use_container_width=True):
            st.query_params["page"] = "detail"
            st.rerun()

        if st.button("📜 Timeline", key="nav_timeline", use_container_width=True):
            st.query_params["page"] = "timeline"
            st.rerun()

        if st.button("⚙️ Config", key="nav_config", use_container_width=True):
            st.query_params["page"] = "config"
            st.rerun()

        if st.button("🚪 Teardown", key="nav_teardown", use_container_width=True):
            st.query_params["page"] = "teardown"
            st.rerun()

        # Custom dashboards section (only if any discovered)
        if custom_dashboards:
            st.divider()
            st.markdown("### Strategy Dashboards")

            for dashboard in custom_dashboards:
                icon = dashboard.icon or "📊"
                if st.button(
                    f"{icon} {dashboard.display_name}",
                    key=f"nav_{dashboard.strategy_name}",
                    use_container_width=True,
                ):
                    st.query_params["page"] = "custom_dashboard"
                    st.query_params["custom_dashboard"] = dashboard.strategy_name

        st.divider()
        st.markdown("### Info")
        st.caption("Click 'Refresh Data' to update strategy information.")
        st.caption("Data is a snapshot - refresh for latest values.")


if __name__ == "__main__":
    main()
