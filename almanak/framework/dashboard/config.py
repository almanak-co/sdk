"""Configuration constants for the Almanak Operator Dashboard.

Centralizes all configuration values for easy modification.
"""

import logging
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Literal

import requests
import streamlit as st

logger = logging.getLogger(__name__)

# Page configuration
PAGE_ICON = "\U0001f4ca"  # Chart emoji
LAYOUT: Literal["centered", "wide"] = "wide"
INITIAL_SIDEBAR_STATE: Literal["auto", "expanded", "collapsed"] = "collapsed"

# Pagination
ITEMS_PER_PAGE = 20
TIMELINE_PAGE_SIZE_OPTIONS = [10, 20, 50]

# Block explorer URLs by chain
BLOCK_EXPLORER_URLS: dict[str, str] = {
    "ethereum": "https://etherscan.io/tx/",
    "arbitrum": "https://arbiscan.io/tx/",
    "optimism": "https://optimistic.etherscan.io/tx/",
    "polygon": "https://polygonscan.com/tx/",
    "base": "https://basescan.org/tx/",
}

# API configuration
API_BASE_URL = "http://localhost:8000"
API_TIMEOUT = 10  # seconds

# Parameter definitions for the config editor UI
CONFIG_PARAM_DEFINITIONS: dict[str, dict[str, Any]] = {
    "max_slippage": {
        "label": "Max Slippage",
        "description": "Maximum allowed slippage for swaps",
        "unit": "%",
        "multiplier": 100,  # Convert decimal to percentage for display
        "min": 0.1,
        "max": 10.0,
        "step": 0.1,
        "input_type": "slider",
        "category": "trading",
    },
    "trade_size_usd": {
        "label": "Trade Size",
        "description": "Default trade size in USD",
        "unit": "USD",
        "multiplier": 1,
        "min": 10.0,
        "max": 100000.0,
        "step": 100.0,
        "input_type": "number",
        "category": "trading",
    },
    "rebalance_threshold": {
        "label": "Rebalance Threshold",
        "description": "Position deviation threshold before rebalancing",
        "unit": "%",
        "multiplier": 100,
        "min": 1.0,
        "max": 50.0,
        "step": 0.5,
        "input_type": "slider",
        "category": "trading",
    },
    "min_health_factor": {
        "label": "Min Health Factor",
        "description": "Minimum health factor before reducing leverage",
        "unit": "",
        "multiplier": 1,
        "min": 1.1,
        "max": 5.0,
        "step": 0.1,
        "input_type": "slider",
        "category": "risk",
    },
    "max_leverage": {
        "label": "Max Leverage",
        "description": "Maximum allowed leverage multiplier",
        "unit": "x",
        "multiplier": 1,
        "min": 1.0,
        "max": 10.0,
        "step": 0.5,
        "input_type": "slider",
        "category": "risk",
    },
    "daily_loss_limit_usd": {
        "label": "Daily Loss Limit",
        "description": "Maximum allowed loss per day (triggers pause)",
        "unit": "USD",
        "multiplier": 1,
        "min": 0.0,
        "max": 50000.0,
        "step": 100.0,
        "input_type": "number",
        "category": "risk",
    },
}

# Default values for reset
DEFAULT_CONFIG_VALUES: dict[str, Decimal] = {
    "max_slippage": Decimal("0.005"),
    "trade_size_usd": Decimal("1000"),
    "rebalance_threshold": Decimal("0.05"),
    "min_health_factor": Decimal("1.5"),
    "max_leverage": Decimal("3"),
    "daily_loss_limit_usd": Decimal("500"),
}


# =============================================================================
# Health Check Utilities
# =============================================================================


@dataclass
class SystemHealth:
    """System health status for the dashboard."""

    api_available: bool = False
    api_status: str = "unavailable"  # "healthy", "degraded", "unavailable"
    runners_active: int = 0
    running_strategies: list[str] = field(default_factory=list)
    features: dict[str, bool] = field(default_factory=dict)
    error: str | None = None

    @property
    def cli_running(self) -> bool:
        """Check if any CLI runners are active."""
        return self.runners_active > 0

    def can_execute(self, feature: str) -> bool:
        """Check if a feature can be executed.

        Args:
            feature: Feature name (e.g., "pause_resume", "execute_teardown")

        Returns:
            True if the feature is available
        """
        return self.features.get(feature, False)


def _check_health_via_gateway(health: SystemHealth) -> SystemHealth:
    """Fall back to gateway instance registry to determine system health.

    When the REST API (port 8000) is unavailable, check the gateway gRPC
    instance registry to see if strategies are running.
    """
    try:
        from almanak.framework.dashboard.gateway_client import get_dashboard_client

        client = get_dashboard_client()
        if not client.is_connected:
            client.connect()

        strategies = client.list_strategies()
        running = [s for s in strategies if s.status == "RUNNING"]
        health.runners_active = len(running)
        health.running_strategies = [s.strategy_id for s in running]

        has_runners = len(running) > 0
        health.api_available = True
        health.api_status = "healthy" if has_runners else "degraded"
        health.features = {
            "view_strategies": True,
            "view_timeline": True,
            "view_config": True,
            "preview_teardown": True,
            "api_available": True,
            "pause_resume": has_runners,
            "bump_gas": has_runners,
            "cancel_tx": has_runners,
            "execute_teardown": has_runners,
            "hot_reload_config": has_runners,
        }
    except Exception:  # noqa: BLE001
        logger.exception("Gateway health fallback failed")
        health.features = {
            "view_strategies": True,
            "view_timeline": True,
            "view_config": True,
            "preview_teardown": False,
            "api_available": False,
            "pause_resume": False,
            "bump_gas": False,
            "cancel_tx": False,
            "execute_teardown": False,
            "hot_reload_config": False,
        }
    return health


def check_system_health() -> SystemHealth:
    """Check overall system health by calling the health API.

    Returns:
        SystemHealth with current status and feature availability
    """
    # Check cache first (cache for 5 seconds)
    cache_key = "_system_health_cache"
    cache_time_key = "_system_health_cache_time"

    if cache_key in st.session_state:
        import time

        cached_time = st.session_state.get(cache_time_key, 0)
        if time.time() - cached_time < 5:  # 5 second cache
            return st.session_state[cache_key]

    health = SystemHealth()

    try:
        response = requests.get(
            f"{API_BASE_URL}/api/health",
            timeout=2,  # Short timeout for health check
        )
        if response.status_code == 200:
            data = response.json()
            health.api_available = True
            health.api_status = data.get("status", "healthy")
            health.runners_active = data.get("runners_active", 0)
            health.running_strategies = [s.get("strategy_id", "") for s in data.get("running_strategies", [])]
            health.features = data.get(
                "features",
                {
                    "view_strategies": True,
                    "view_timeline": True,
                    "view_config": True,
                    "preview_teardown": True,
                    "api_available": True,
                    "pause_resume": False,
                    "bump_gas": False,
                    "cancel_tx": False,
                    "execute_teardown": False,
                    "hot_reload_config": False,
                },
            )
        else:
            health.error = f"API returned status {response.status_code}"
    except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
        health.error = str(e)
        # REST API not running - fall back to gateway instance registry
        health = _check_health_via_gateway(health)
    except Exception as e:
        health.error = str(e)

    # Cache the result
    import time

    st.session_state[cache_key] = health
    st.session_state[cache_time_key] = time.time()

    return health


def check_strategy_health(strategy_id: str) -> dict[str, Any]:
    """Check health for a specific strategy.

    Args:
        strategy_id: The strategy to check

    Returns:
        Dict with running status and available features
    """
    try:
        response = requests.get(
            f"{API_BASE_URL}/api/health/strategies/{strategy_id}",
            timeout=2,
        )
        if response.status_code == 200:
            return response.json()
    except Exception:
        pass

    # Default response when API unavailable
    return {
        "running": False,
        "strategy_id": strategy_id,
        "features": {
            "pause_resume": False,
            "bump_gas": False,
            "cancel_tx": False,
            "execute_teardown": False,
            "hot_reload_config": False,
        },
        "message": "API not available",
    }


def render_system_status_badge() -> None:
    """Render a system status badge in the UI."""
    health = check_system_health()

    if health.api_available and health.cli_running:
        st.success(f"System Online | {health.runners_active} strategy running", icon="\u2705")
    elif health.api_available:
        st.warning("API Online | No strategies running (CLI not started)", icon="\u26a0\ufe0f")
    else:
        st.error("API Offline | Start the API server to enable actions", icon="\u274c")
