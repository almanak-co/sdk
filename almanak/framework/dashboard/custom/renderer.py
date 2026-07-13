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


def _spinner_label(display_name: str | None) -> str:
    """Build the loading-spinner label for a custom dashboard.

    display_name often already ends in "Dashboard" (e.g. "Strategy
    Dashboard"), so only append the suffix when it isn't already there —
    avoids "Loading Strategy Dashboard dashboard...". Tolerates malformed
    metadata (None / non-str / trailing whitespace) since display_name
    comes from user-authored metadata.json.
    """
    name = str(display_name or "").strip()
    if not name:
        return "dashboard"
    if name.lower().endswith("dashboard"):
        return name
    return f"{name} dashboard"


def _resolve_api_client(
    deployment_id: str,
    api_client: DashboardAPIClient | Any | None,
    gateway_client: Any | None,
) -> DashboardAPIClient | Any:
    """Resolve the API client for a custom dashboard render.

    Returns the caller-supplied client untouched if one was given; otherwise
    builds one from gateway_client, or discovers the dashboard gateway client,
    falling back to a mock client on any connection failure.
    """
    if api_client is not None:
        return api_client

    if gateway_client is not None:
        # Create a scoped API client for this dashboard
        return create_api_client(gateway_client, deployment_id)

    # Try to get gateway client from the dashboard module
    try:
        from almanak.framework.dashboard.gateway_client import (
            GatewayConnectionError,
            get_dashboard_client,
        )

        gw_client_inner = get_dashboard_client()
        if not gw_client_inner.is_connected:
            try:
                logger.debug(f"Attempting to connect to gateway for {deployment_id}")
                gw_client_inner.connect()
            except GatewayConnectionError as conn_err:
                logger.warning(f"Gateway connection failed for {deployment_id}: {conn_err}, using mock API client")
                return create_mock_api_client()

        if gw_client_inner.is_connected:
            return create_api_client(gw_client_inner, deployment_id)

        # gw_client_inner exists but failed to connect for other reason
        logger.warning(f"Gateway not connected, using mock API client for {deployment_id}")
        return create_mock_api_client()
    except Exception as e:  # noqa: BLE001
        # Fallback to mock client on any unexpected error
        logger.debug(f"Failed to create gateway API client: {e}")
        return create_mock_api_client()


def render_custom_dashboard_safe(
    dashboard_info: CustomDashboardInfo,
    deployment_id: str | None = None,
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
        deployment_id: Currently selected deployment ID
        strategy_config: Strategy configuration dictionary
        api_client: DashboardAPIClient instance (preferred) or legacy API client
        gateway_client: GatewayDashboardClient for creating api_client if not provided
        session_state: Shared session state dictionary

    Returns:
        True if rendering succeeded, False if there was an error
    """
    # Provide defaults
    if deployment_id is None:
        deployment_id = dashboard_info.strategy_name

    if strategy_config is None:
        strategy_config = {}

    if session_state is None:
        session_state = {}

    # Create gateway-backed API client if not provided
    api_client = _resolve_api_client(deployment_id, api_client, gateway_client)

    # Show loading indicator while importing.
    with st.spinner(f"Loading {_spinner_label(dashboard_info.display_name)}..."):
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
            deployment_id=deployment_id,
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
        deployment_id: str,
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


def _render_error_recovery_buttons() -> None:
    """Render the shared "Return to Overview" / "Retry" recovery buttons.

    Both the gateway-classified and the generic runtime-error branches need
    these in-pane recovery actions — extracted so a gateway auth/unreachable
    error (the most common live-mainnet case) doesn't leave the operator without
    a way out (VIB-4047, CodeRabbit).
    """
    col1, col2 = st.columns(2)

    with col1:
        if st.button("Return to Overview", key="runtime_error_return"):
            st.session_state["current_page"] = "overview"
            st.rerun()

    with col2:
        if st.button("Retry", key="runtime_error_retry"):
            st.rerun()


def _render_runtime_error(dashboard_info: CustomDashboardInfo, error: Exception) -> None:
    """Render a runtime error message.

    VIB-4047: the most common runtime error on a live mainnet dashboard is a
    gateway auth/connection failure surfacing from a data call. Route those to
    the shared LOUD + CLEAN banner (clean actionable message, red for
    auth/unreachable) and never dump a raw ``_InactiveRpcError`` traceback into
    the pane — the full traceback is logged and only shown behind the debug
    flag. Genuinely-unexpected errors keep a clean summary with the same
    debug-gated traceback.
    """
    from almanak.framework.dashboard.error_ui import (
        GatewayErrorKind,
        classify_gateway_error,
        dashboard_debug_enabled,
        render_gateway_error,
    )

    if classify_gateway_error(error) is not GatewayErrorKind.OTHER:
        render_gateway_error(error, context=f"the {dashboard_info.display_name} dashboard", raw=str(error))
        logger.error(
            f"Runtime error in dashboard {dashboard_info.strategy_name}: {error}",
            exc_info=True,
        )
        # A gateway auth/unreachable error must still offer the recovery actions
        # (VIB-4047, CodeRabbit) — the early return used to skip them.
        _render_error_recovery_buttons()
        return

    st.error("Dashboard Runtime Error")
    st.markdown(f"""
    **An error occurred while rendering:** {dashboard_info.display_name}

    **Error type:** `{type(error).__name__}`

    **Error message:** {str(error)}
    """)

    # Raw traceback is debug-only (never leaked into a user-facing pane).
    if dashboard_debug_enabled():
        with st.expander("Show full traceback (debug)"):
            st.code(traceback.format_exc(), language="python")

    logger.error(
        f"Runtime error in dashboard {dashboard_info.strategy_name}: {error}",
        exc_info=True,
    )

    _render_error_recovery_buttons()


def create_mock_api_client():
    """Create a mock API client for custom dashboards.

    Returns a simple object that provides read-only access to
    strategy data without requiring a running API server.
    """

    class MockAPIClient:
        """Mock API client for custom dashboards."""

        def get_strategy_state(self, deployment_id: str) -> dict:
            """Get current strategy state (mock)."""
            return {
                "deployment_id": deployment_id,
                "status": "RUNNING",
                "total_value": 0.0,
                "pnl": 0.0,
            }

        def get_timeline(
            self,
            deployment_id: str,
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
                    events = events_data.get(deployment_id, [])
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

        def pause_strategy(self, deployment_id: str, reason: str) -> dict:
            """Pause a strategy (mock - not implemented)."""
            return {"status": "not_implemented", "message": "Mock API client"}

        def resume_strategy(self, deployment_id: str) -> dict:
            """Resume a strategy (mock - not implemented)."""
            return {"status": "not_implemented", "message": "Mock API client"}

        # VIB-4347: mock parity for the new DashboardAPIClient market-data
        # methods. Each returns ``[]`` — synthetic fixture data is **never**
        # returned from a mock because it would silently fool custom dashboards
        # running in fallback/demo mode into rendering meaningful-looking
        # charts off thin air. If a test needs OHLCV / position fixtures it
        # must inject them explicitly.
        def get_ohlcv(
            self,
            token: str,
            quote: str = "USD",
            timeframe: str = "1h",
            limit: int = 168,
            chain: str | None = None,
            pool_address: str | None = None,
        ) -> list[dict]:
            """Mock get_ohlcv — returns empty list."""
            return []

        def get_position_events(
            self,
            position_types: list[str] | None = None,
        ) -> list[dict]:
            """Mock get_position_events — returns empty list."""
            return []

        def get_position_history(
            self,
            position_id: str,
        ) -> list[dict]:
            """Mock get_position_history — returns empty list."""
            return []

    return MockAPIClient()
