"""Custom Dashboard Renderer.

Renders custom dashboards with error boundaries and shared context.
All custom dashboards receive a gateway-backed API client that provides
controlled access to strategy data through the gateway.
"""

import logging
import traceback
from typing import Any

import streamlit as st

from .api_client import DashboardAPIClient, create_api_client
from .discoverer import CustomDashboardInfo
from .loader import (
    DashboardInterfaceError,
    DashboardLoadError,
    get_dashboard_render_function,
    load_dashboard_module,
)

logger = logging.getLogger(__name__)


def render_custom_dashboard_safe(
    dashboard_info: CustomDashboardInfo,
    strategy_id: str | None = None,
    strategy_config: dict | None = None,
    api_client: DashboardAPIClient | Any | None = None,
    gateway_client: Any | None = None,
    session_state: dict | None = None,
) -> bool:
    """Safely render a custom dashboard with error boundary.

    This function wraps custom dashboard rendering in a try/except
    to ensure errors don't crash the core dashboard.

    Args:
        dashboard_info: Dashboard metadata from discovery
        strategy_id: Currently selected strategy ID
        strategy_config: Strategy configuration dictionary
        api_client: DashboardAPIClient instance (preferred) or legacy API client
        gateway_client: GatewayDashboardClient for creating api_client if not provided
        session_state: Shared session state dictionary

    Returns:
        True if rendering succeeded, False if there was an error
    """
    # Provide defaults
    if strategy_id is None:
        strategy_id = dashboard_info.strategy_name

    if strategy_config is None:
        strategy_config = {}

    if session_state is None:
        session_state = {}

    # Create gateway-backed API client if not provided
    if api_client is None:
        if gateway_client is not None:
            # Create a scoped API client for this dashboard
            api_client = create_api_client(gateway_client, strategy_id)
        else:
            # Try to get gateway client from the dashboard module
            try:
                from almanak.framework.dashboard.gateway_client import (
                    GatewayConnectionError,
                    get_dashboard_client,
                )

                gw_client_inner = get_dashboard_client()
                connection_handled = False
                if not gw_client_inner.is_connected:
                    try:
                        logger.debug(f"Attempting to connect to gateway for {strategy_id}")
                        gw_client_inner.connect()
                    except GatewayConnectionError as conn_err:
                        logger.warning(
                            f"Gateway connection failed for {strategy_id}: {conn_err}, using mock API client"
                        )
                        api_client = create_mock_api_client()
                        connection_handled = True  # Signal that we've handled this case

                if not connection_handled and gw_client_inner.is_connected:
                    api_client = create_api_client(gw_client_inner, strategy_id)
                elif api_client is None:  # gw_client_inner exists but failed to connect for other reason
                    api_client = create_mock_api_client()
                    logger.warning(f"Gateway not connected, using mock API client for {strategy_id}")
            except Exception as e:  # noqa: BLE001
                # Fallback to mock client on any unexpected error
                logger.debug(f"Failed to create gateway API client: {e}")
                api_client = create_mock_api_client()

    # Show loading indicator while importing
    with st.spinner(f"Loading {dashboard_info.display_name} dashboard..."):
        try:
            # Load the module
            module = load_dashboard_module(
                dashboard_path=dashboard_info.dashboard_path,
                strategy_name=dashboard_info.strategy_name,
            )

            # Get render function
            render_func = get_dashboard_render_function(module)

        except DashboardLoadError as e:
            _render_load_error(dashboard_info, str(e))
            return False

        except DashboardInterfaceError as e:
            _render_interface_error(dashboard_info, str(e))
            return False

    # Render the dashboard with error boundary
    try:
        render_func(
            strategy_id=strategy_id,
            strategy_config=strategy_config,
            api_client=api_client,
            session_state=session_state,
        )
        return True

    except Exception as e:
        _render_runtime_error(dashboard_info, e)
        return False


def _render_load_error(dashboard_info: CustomDashboardInfo, error_msg: str) -> None:
    """Render a dashboard load error message."""
    st.error("Dashboard Load Error")

    st.markdown(f"""
    **Failed to load custom dashboard:** {dashboard_info.display_name}

    **Error:** {error_msg}

    **Dashboard path:** `{dashboard_info.dashboard_path}`

    This usually means:
    - The dashboard module has a syntax error
    - Required imports are missing
    - The ui.py file doesn't exist
    """)

    logger.error(f"Failed to load dashboard {dashboard_info.strategy_name}: {error_msg}")

    if st.button("Return to Overview", key="load_error_return"):
        st.session_state["current_page"] = "overview"
        st.rerun()


def _render_interface_error(dashboard_info: CustomDashboardInfo, error_msg: str) -> None:
    """Render an interface error message."""
    st.error("Dashboard Interface Error")

    st.markdown(f"""
    **Dashboard doesn't implement required interface:** {dashboard_info.display_name}

    **Error:** {error_msg}

    Custom dashboards must implement this function:

    ```python
    def render_custom_dashboard(
        strategy_id: str,
        strategy_config: dict,
        api_client: APIClient,
        session_state: dict,
    ) -> None:
        # Your dashboard code here
        st.title("My Custom Dashboard")
        ...
    ```
    """)

    logger.error(f"Interface error in {dashboard_info.strategy_name}: {error_msg}")

    if st.button("Return to Overview", key="interface_error_return"):
        st.session_state["current_page"] = "overview"
        st.rerun()


def _render_runtime_error(dashboard_info: CustomDashboardInfo, error: Exception) -> None:
    """Render a runtime error message with traceback."""
    st.error("Dashboard Runtime Error")

    # Get traceback
    tb_str = traceback.format_exc()

    st.markdown(f"""
    **An error occurred while rendering:** {dashboard_info.display_name}

    **Error type:** `{type(error).__name__}`

    **Error message:** {str(error)}
    """)

    # Show traceback in expander
    with st.expander("Show full traceback"):
        st.code(tb_str, language="python")

    logger.error(
        f"Runtime error in dashboard {dashboard_info.strategy_name}: {error}",
        exc_info=True,
    )

    col1, col2 = st.columns(2)

    with col1:
        if st.button("Return to Overview", key="runtime_error_return"):
            st.session_state["current_page"] = "overview"
            st.rerun()

    with col2:
        if st.button("Retry", key="runtime_error_retry"):
            st.rerun()


def create_mock_api_client():
    """Create a mock API client for custom dashboards.

    Returns a simple object that provides read-only access to
    strategy data without requiring a running API server.
    """

    class MockAPIClient:
        """Mock API client for custom dashboards."""

        def get_strategy_state(self, strategy_id: str) -> dict:
            """Get current strategy state (mock)."""
            return {
                "strategy_id": strategy_id,
                "status": "RUNNING",
                "total_value": 0.0,
                "pnl": 0.0,
            }

        def get_timeline(
            self,
            strategy_id: str,
            limit: int = 50,
            event_type: str | None = None,
        ) -> list[dict]:
            """Get timeline events (reads from cache file if available)."""
            import json
            from pathlib import Path

            # Try to load from cache file
            cache_file = Path(__file__).parent.parent.parent.parent / ".dashboard_events.json"

            if cache_file.exists():
                try:
                    with open(cache_file) as f:
                        events_data = json.load(f)

                    # Get events for this strategy or any if not found
                    events = events_data.get(strategy_id, [])
                    if not events:
                        # Try to get events from any strategy
                        for _sid, evts in events_data.items():
                            if evts:
                                events = evts
                                break

                    # Filter by event type if specified
                    if event_type:
                        events = [e for e in events if e.get("event_type") == event_type]

                    # Sort by timestamp descending and limit
                    events.sort(
                        key=lambda e: e.get("timestamp", ""),
                        reverse=True,
                    )

                    return events[:limit]

                except Exception as e:
                    logger.warning(f"Error loading events from cache: {e}")

            return []

        def pause_strategy(self, strategy_id: str, reason: str) -> dict:
            """Pause a strategy (mock - not implemented)."""
            return {"status": "not_implemented", "message": "Mock API client"}

        def resume_strategy(self, strategy_id: str) -> dict:
            """Resume a strategy (mock - not implemented)."""
            return {"status": "not_implemented", "message": "Mock API client"}

    return MockAPIClient()
