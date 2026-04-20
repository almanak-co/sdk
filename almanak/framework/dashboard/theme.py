"""Theme and styling constants for the Almanak Operator Dashboard.

Provides centralized color schemes and styling for consistent UI.
"""

from almanak.framework.dashboard.models import (
    ChainHealthStatus,
    Severity,
    StrategyStatus,
    TimelineEventType,
)

# Status colors
STATUS_COLORS: dict[StrategyStatus, str] = {
    StrategyStatus.RUNNING: "#00c853",  # Green
    StrategyStatus.STUCK: "#ffc107",  # Yellow/Amber
    StrategyStatus.PAUSED: "#9e9e9e",  # Gray
    StrategyStatus.ERROR: "#f44336",  # Red
}

# Severity colors
SEVERITY_COLORS: dict[Severity, str] = {
    Severity.LOW: "#2196f3",  # Blue
    Severity.MEDIUM: "#ffc107",  # Yellow/Amber
    Severity.HIGH: "#ff9800",  # Orange
    Severity.CRITICAL: "#f44336",  # Red
}

# Timeline event colors
EVENT_COLORS: dict[TimelineEventType, str] = {
    TimelineEventType.TRADE: "#2196f3",
    TimelineEventType.SWAP: "#2196f3",
    TimelineEventType.REBALANCE: "#9c27b0",
    TimelineEventType.DEPOSIT: "#00c853",
    TimelineEventType.WITHDRAWAL: "#ff9800",
    TimelineEventType.LP_OPEN: "#00c853",
    TimelineEventType.LP_CLOSE: "#f44336",
    TimelineEventType.BORROW: "#ff9800",
    TimelineEventType.REPAY: "#00c853",
    TimelineEventType.ALERT: "#ffc107",
    TimelineEventType.ERROR: "#f44336",
    TimelineEventType.CONFIG_UPDATE: "#607d8b",
    TimelineEventType.STATE_CHANGE: "#2196f3",
    TimelineEventType.TRANSACTION_SUBMITTED: "#2196f3",
    TimelineEventType.TRANSACTION_CONFIRMED: "#00c853",
    TimelineEventType.TRANSACTION_FAILED: "#f44336",
    TimelineEventType.TRANSACTION_REVERTED: "#f44336",
    TimelineEventType.STRATEGY_STARTED: "#00c853",
    TimelineEventType.STRATEGY_PAUSED: "#ffc107",
    TimelineEventType.STRATEGY_RESUMED: "#00c853",
    TimelineEventType.STRATEGY_STOPPED: "#9e9e9e",
    TimelineEventType.OPERATOR_ACTION_EXECUTED: "#607d8b",
    TimelineEventType.RISK_GUARD_TRIGGERED: "#ff9800",
    TimelineEventType.CIRCUIT_BREAKER_TRIGGERED: "#f44336",
    # Bridge events
    TimelineEventType.BRIDGE_INITIATED: "#00bcd4",  # Cyan
    TimelineEventType.BRIDGE_COMPLETED: "#00c853",  # Green
    TimelineEventType.BRIDGE_FAILED: "#f44336",  # Red
}

# Chain colors - distinctive colors for each supported chain
CHAIN_COLORS: dict[str, str] = {
    "ethereum": "#627eea",  # Ethereum blue
    "arbitrum": "#28a0f0",  # Arbitrum blue
    "optimism": "#ff0420",  # Optimism red
    "base": "#0052ff",  # Base blue
    "polygon": "#8247e5",  # Polygon purple
    "avalanche": "#e84142",  # Avalanche red
    "bsc": "#f0b90b",  # BSC yellow
    "linea": "#61dfff",  # Linea cyan
    "zksync": "#8c8dfc",  # zkSync purple
}

# Chain health status colors
CHAIN_HEALTH_COLORS: dict[ChainHealthStatus, str] = {
    ChainHealthStatus.HEALTHY: "#00c853",  # Green
    ChainHealthStatus.DEGRADED: "#ffc107",  # Yellow
    ChainHealthStatus.UNAVAILABLE: "#f44336",  # Red
}

# PnL colors
PNL_POSITIVE_COLOR = "#00c853"
PNL_NEGATIVE_COLOR = "#f44336"

# UI colors
CARD_BACKGROUND = "#1e1e1e"
CARD_BORDER = "#333"
APP_BACKGROUND = "#121212"

# Default neutral color
DEFAULT_COLOR = "#9e9e9e"


def get_status_color(status: StrategyStatus) -> str:
    """Get color for strategy status."""
    return STATUS_COLORS.get(status, DEFAULT_COLOR)


def get_severity_color(severity: Severity) -> str:
    """Get color for severity level."""
    return SEVERITY_COLORS.get(severity, DEFAULT_COLOR)


def get_timeline_event_color(event_type: TimelineEventType) -> str:
    """Get color for timeline event type."""
    return EVENT_COLORS.get(event_type, DEFAULT_COLOR)


def get_pnl_color(value: float) -> str:
    """Get color based on PnL value (positive/negative)."""
    return PNL_POSITIVE_COLOR if value >= 0 else PNL_NEGATIVE_COLOR


def get_chain_color(chain: str) -> str:
    """Get color for a chain."""
    return CHAIN_COLORS.get(chain.lower(), DEFAULT_COLOR)


def get_chain_health_color(status: ChainHealthStatus) -> str:
    """Get color for chain health status."""
    return CHAIN_HEALTH_COLORS.get(status, DEFAULT_COLOR)


# Custom CSS for dark theme
CUSTOM_CSS = """
<style>
.stApp {
    background-color: #121212;
}
.stMetric {
    background-color: #1e1e1e;
    padding: 1rem;
    border-radius: 8px;
}
/* Hide Streamlit's auto-generated pages navigation */
[data-testid="stSidebarNav"] {
    display: none !important;
}
</style>
"""


def get_plotly_layout(height: int = 400) -> dict:
    """Get default Plotly layout for consistent chart styling.

    Args:
        height: Chart height in pixels

    Returns:
        Dictionary of Plotly layout settings
    """
    return {
        "template": "plotly_dark",
        "paper_bgcolor": "rgba(0, 0, 0, 0)",
        "plot_bgcolor": "rgba(0, 0, 0, 0.1)",
        "height": height,
        "margin": {"l": 20, "r": 20, "t": 40, "b": 20},
        "font": {"color": "white", "size": 12},
    }
