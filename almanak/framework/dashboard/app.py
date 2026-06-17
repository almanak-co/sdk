"""Almanak Strategy Framework v2.0 - Operator Dashboard

Streamlit-based dashboard for monitoring and managing DeFi trading strategies.
Provides strategy overview, status monitoring, and operator actions.

This is the main entry point that sets up navigation and routing.
"""

import logging
import sys
from datetime import datetime
from pathlib import Path
from time import sleep as _sleep
from typing import Protocol

import streamlit as st
from streamlit.runtime.scriptrunner import RerunException, StopException

logger = logging.getLogger(__name__)

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
REFRESH_INTERVAL_OPTIONS = [10, 30, 60, 120]
STRATEGY_AWARE_PAGES = {"detail", "timeline", "config", "teardown", "custom_dashboard"}


class _CustomDashboardNavInfo(Protocol):
    strategy_name: str
    display_name: str
    icon: str | None


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


# crap-allowlist: VIB-4722 mechanical deployment_id rename in existing high-CRAP function.
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
    deployment_id = dashboard_info.strategy_name
    for s in strategies:
        if s.id == dashboard_name or dashboard_name in s.id:
            strategy_config = {
                "name": s.name,
                "status": s.status.value,
                "total_value": float(s.total_value_usd),
            }
            deployment_id = s.id
            break

    # Create API client
    api_client = None
    # if create_mock_api_client:
    #     api_client = create_mock_api_client()

    # Render the custom dashboard with error boundary
    if render_custom_dashboard_safe is not None:
        render_custom_dashboard_safe(
            dashboard_info=dashboard_info,
            deployment_id=deployment_id,
            strategy_config=strategy_config,
            api_client=api_client,
            session_state=dict(st.session_state),
        )
    else:
        st.warning("Custom dashboard rendering not available")


def _configure_dashboard_shell() -> None:
    st.set_page_config(
        page_title=PAGE_TITLE,
        page_icon=PAGE_ICON,
        layout=LAYOUT,
        initial_sidebar_state=INITIAL_SIDEBAR_STATE,
    )
    st.markdown(CUSTOM_CSS, unsafe_allow_html=True)


def _initialize_refresh_state() -> None:
    if "last_refresh" not in st.session_state:
        st.session_state.last_refresh = datetime.now()
    if "auto_refresh" not in st.session_state:
        st.session_state.auto_refresh = False
    if "refresh_interval" not in st.session_state:
        st.session_state.refresh_interval = 30  # seconds


def _discover_custom_dashboards_safe() -> list:
    if not CUSTOM_DASHBOARDS_AVAILABLE:
        return []
    try:
        return discover_custom_dashboards()
    except Exception:
        return []


def _handle_manual_refresh() -> None:
    if st.button("🔄 Refresh", help="Manually refresh data"):
        st.session_state.last_refresh = datetime.now()
        st.rerun()


def _render_debug_toggle() -> None:
    st.session_state.show_debug = st.checkbox(
        "🐛 Debug",
        value=st.session_state.get("show_debug", False),
        help="Show debug information",
    )


def _handle_auto_refresh_toggle() -> None:
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


def _render_refresh_interval_selector() -> None:
    interval = st.selectbox(
        "Interval",
        options=REFRESH_INTERVAL_OPTIONS,
        index=REFRESH_INTERVAL_OPTIONS.index(st.session_state.refresh_interval),
        format_func=lambda x: f"{x}s",
        label_visibility="collapsed",
        disabled=not st.session_state.auto_refresh,
    )
    if interval != st.session_state.refresh_interval:
        st.session_state.refresh_interval = interval


def _render_header_controls() -> None:
    col1, col2, col3, col4, col5 = st.columns([3, 1, 1, 1, 1])
    with col1:
        st.markdown(f"# {PAGE_ICON} {PAGE_TITLE}")
    with col2:
        _handle_manual_refresh()
    with col5:
        _render_debug_toggle()
    with col3:
        _handle_auto_refresh_toggle()
    with col4:
        _render_refresh_interval_selector()


def _render_auto_refresh_caption() -> None:
    elapsed = (datetime.now() - st.session_state.last_refresh).total_seconds()
    remaining = max(0, st.session_state.refresh_interval - elapsed)
    st.caption(
        f"🟢 Auto-refresh ON | Last: {st.session_state.last_refresh.strftime('%H:%M:%S')} | Next in: {int(remaining)}s"
    )
    if elapsed >= st.session_state.refresh_interval:
        st.session_state.last_refresh = datetime.now()
        st.rerun()
        return

    # Streamlit needs a scheduled rerun to keep the countdown moving.
    _sleep(1)
    st.rerun()


def _render_refresh_status() -> None:
    if st.session_state.auto_refresh:
        _render_auto_refresh_caption()
    else:
        st.caption(f"Last refreshed: {st.session_state.last_refresh.strftime('%H:%M:%S')}")


def _load_dashboard_strategies() -> list:
    try:
        return get_all_strategies()
    except Exception as e:
        st.error(f"Error loading strategies: {e}")
        import traceback

        st.code(traceback.format_exc())
        return []


def _render_debug_info(strategies: list) -> None:
    if st.session_state.get("show_debug", False):
        with st.expander("Debug Info"):
            st.write(f"Strategies found: {len(strategies)}")
            if strategies:
                st.write("Deployment IDs:", [s.id for s in strategies])
            st.write("Current page:", st.query_params.get("page", "overview"))
            st.write("Deployment ID param:", st.query_params.get("deployment_id"))


def _render_custom_dashboard_route(custom_dashboards: list, strategies: list) -> None:
    dashboard_name = st.query_params.get("custom_dashboard")
    if dashboard_name:
        render_custom_dashboard_page(dashboard_name, custom_dashboards, strategies)
    else:
        st.error("No custom dashboard specified")
        overview.page(strategies)


def _render_page_route(current_page: str, custom_dashboards: list, strategies: list) -> None:
    if current_page == "custom_dashboard":
        _render_custom_dashboard_route(custom_dashboards, strategies)
        return
    if current_page == "library":
        library.page()
        return

    strategy_page = {
        "config": config_page.page,
        "timeline": timeline.page,
        "detail": detail.page,
        "teardown": teardown.page,
    }.get(current_page, overview.page)
    strategy_page(strategies)


def _render_page_error(current_page: str, error: Exception) -> None:
    logger.exception("Error rendering page '%s'", current_page)
    st.error(f"Error rendering page '{current_page}': {error}")
    import traceback

    with st.expander("Error Details"):
        st.code(traceback.format_exc())
    if current_page != "overview":
        st.info("Returning to overview page...")
        st.query_params["page"] = "overview"
        st.rerun()


def _render_current_page(custom_dashboards: list, strategies: list) -> None:
    current_page = st.query_params.get("page", "overview")
    try:
        _render_page_route(current_page, custom_dashboards, strategies)
    except (RerunException, StopException):
        # Streamlit control-flow exceptions must propagate — catching them
        # kills the rerun/stop mechanism and can crash the process (VIB-2431).
        raise
    except Exception as e:
        _render_page_error(current_page, e)


def _render_sidebar_nav_button(label: str, page: str, key: str, *, clear_query: bool = False) -> None:
    if st.button(label, key=key, use_container_width=True):
        if clear_query:
            st.query_params.clear()
        st.query_params["page"] = page
        st.rerun()


def _render_sidebar_navigation() -> None:
    st.markdown("### Navigation")
    _render_sidebar_nav_button("Command Center", "overview", "nav_overview", clear_query=True)
    _render_sidebar_nav_button("Strategy Library", "library", "nav_library", clear_query=True)


def _strategy_option_labels(strategies: list) -> list[str]:
    return ["None"] + [f"{s.name} ({s.id[:8]}...)" for s in strategies]


def _current_strategy_index(strategies: list, current_deployment_id: str | None) -> int:
    if not current_deployment_id:
        return 0
    for idx, strategy in enumerate(strategies, start=1):
        if strategy.id == current_deployment_id:
            return idx
    return 0


def _handle_selected_sidebar_strategy(
    strategies: list,
    selected_strategy_display: int,
    current_deployment_id: str | None,
) -> None:
    selected_strategy = strategies[selected_strategy_display - 1]
    if selected_strategy.id == current_deployment_id:
        return

    st.query_params["deployment_id"] = selected_strategy.id
    if st.query_params.get("page") not in STRATEGY_AWARE_PAGES:
        st.query_params["page"] = "detail"
    st.rerun()


def _clear_sidebar_strategy_selection(current_deployment_id: str | None) -> None:
    if not current_deployment_id:
        return
    if "deployment_id" in st.query_params:
        del st.query_params["deployment_id"]
    st.rerun()


def _handle_sidebar_strategy_selection(
    strategies: list,
    selected_strategy_display: int,
    current_deployment_id: str | None,
) -> None:
    if selected_strategy_display > 0:
        _handle_selected_sidebar_strategy(strategies, selected_strategy_display, current_deployment_id)
        return
    if selected_strategy_display == 0:
        _clear_sidebar_strategy_selection(current_deployment_id)


def _render_sidebar_strategy_selector(strategies: list) -> None:
    st.markdown("### Select Strategy")
    if not strategies:
        st.caption("No strategies available")
        return

    current_deployment_id = st.query_params.get("deployment_id")
    selected_strategy_display = st.selectbox(
        "Choose a strategy",
        options=range(len(strategies) + 1),
        format_func=lambda x: _strategy_option_labels(strategies)[x],
        index=_current_strategy_index(strategies, current_deployment_id),
        key="sidebar_strategy_selector",
        label_visibility="collapsed",
    )
    _handle_sidebar_strategy_selection(strategies, selected_strategy_display, current_deployment_id)


def _render_sidebar_page_buttons() -> None:
    st.markdown("### Pages")
    _render_sidebar_nav_button("📊 Detail", "detail", "nav_detail")
    _render_sidebar_nav_button("📜 Timeline", "timeline", "nav_timeline")
    _render_sidebar_nav_button("⚙️ Config", "config", "nav_config")
    _render_sidebar_nav_button("🚪 Teardown", "teardown", "nav_teardown")


def _render_custom_dashboard_nav_button(dashboard: _CustomDashboardNavInfo) -> None:
    icon = getattr(dashboard, "icon", None) or "📊"
    if st.button(
        f"{icon} {dashboard.display_name}",
        key=f"nav_{dashboard.strategy_name}",
        use_container_width=True,
    ):
        st.query_params["page"] = "custom_dashboard"
        st.query_params["custom_dashboard"] = dashboard.strategy_name


def _render_sidebar_custom_dashboards(custom_dashboards: list) -> None:
    if not custom_dashboards:
        return
    st.divider()
    st.markdown("### Strategy Dashboards")
    for dashboard in custom_dashboards:
        _render_custom_dashboard_nav_button(dashboard)


def _render_sidebar_info() -> None:
    st.divider()
    st.markdown("### Info")
    st.caption("Click 'Refresh Data' to update strategy information.")
    st.caption("Data is a snapshot - refresh for latest values.")


def _render_sidebar(strategies: list, custom_dashboards: list) -> None:
    with st.sidebar:
        _render_sidebar_navigation()
        st.divider()
        _render_sidebar_strategy_selector(strategies)
        st.divider()
        _render_sidebar_page_buttons()
        _render_sidebar_custom_dashboards(custom_dashboards)
        _render_sidebar_info()


def main() -> None:
    """Main dashboard application."""
    _configure_dashboard_shell()
    _initialize_refresh_state()

    custom_dashboards = _discover_custom_dashboards_safe()
    _render_header_controls()
    _render_refresh_status()

    strategies = _load_dashboard_strategies()
    _render_debug_info(strategies)
    _render_current_page(custom_dashboards, strategies)
    _render_sidebar(strategies, custom_dashboards)


if __name__ == "__main__":
    main()
