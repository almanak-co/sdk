"""Almanak Strategy Framework v2.0 - Dashboard

Public exports for dashboard data access, rendering, and PM integration.
External consumers (PM dashboard, custom UIs) should import from here.

Usage::

    from almanak.framework.dashboard import (
        DashboardDataClient,
        Strategy,
        render_strategy_detail,
        render_strategy_timeline,
        strategy_from_pm_dict,
    )
"""

from almanak.framework.dashboard.adapters import (
    render_strategy_detail,
    render_strategy_timeline,
    strategy_from_pm_dict,
)
from almanak.framework.dashboard.data_client import (
    DashboardDataClient,
    PnLDataPoint,
    PortfolioMetricsSummary,
    TradeRecord,
)
from almanak.framework.dashboard.gateway_client import (
    GatewayConnectionError,
    GatewayDashboardClient,
    LedgerTradeRecord,
    PositionInfo,
    StrategyDetails,
    StrategySummary,
    TimelineEvent,
)
from almanak.framework.dashboard.models import Strategy

__all__ = [
    # Primary client (protobuf-free)
    "DashboardDataClient",
    # Data types
    "GatewayConnectionError",
    "GatewayDashboardClient",
    "LedgerTradeRecord",
    "PnLDataPoint",
    "PortfolioMetricsSummary",
    "PositionInfo",
    "Strategy",
    "StrategyDetails",
    "StrategySummary",
    "TimelineEvent",
    "TradeRecord",
    # PM integration
    "render_strategy_detail",
    "render_strategy_timeline",
    "strategy_from_pm_dict",
]
