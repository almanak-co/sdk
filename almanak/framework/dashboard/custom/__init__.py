"""Custom Dashboard Integration Module.

This module provides discovery, loading, and rendering of custom
strategy dashboards within the core framework dashboard.

Custom dashboards receive a DashboardAPIClient that provides controlled,
gateway-backed access to strategy data.
"""

from .api_client import (
    DashboardAPIClient,
    create_api_client,
)
from .discoverer import (
    CustomDashboardInfo,
    discover_custom_dashboards,
)
from .loader import (
    DashboardInterfaceError,
    DashboardLoadError,
    get_dashboard_render_function,
    load_dashboard_module,
)
from .renderer import (
    create_mock_api_client,
    render_custom_dashboard_safe,
)

__all__ = [
    "CustomDashboardInfo",
    "DashboardAPIClient",
    "create_api_client",
    "create_mock_api_client",
    "discover_custom_dashboards",
    "load_dashboard_module",
    "get_dashboard_render_function",
    "DashboardLoadError",
    "DashboardInterfaceError",
    "render_custom_dashboard_safe",
]
