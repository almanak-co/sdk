"""Almanak Strategy Framework v2.0 - Dashboard

Public exports for dashboard data access, rendering, and PM integration.
External consumers (PM dashboard, custom UIs) should import from here.

For strategy authors writing a ``dashboard/ui.py`` for their strategy,
the recommended convention is to frame ``render_custom_dashboard()``
with three section helpers (VIB-3969) so accounting is visually QA'able
locally and on the hosted platform from the same single-source code
path:

  - ``render_pnl_section(strategy_id)`` — top, the 5-second eyeball
  - ``render_cost_stack_section(strategy_id)`` — bottom, life-to-date costs
  - ``render_trade_tape_section(strategy_id)`` — bottom, TX-level audit

Usage::

    from almanak.framework.dashboard import (
        DashboardDataClient,
        Strategy,
        render_cost_stack_section,
        render_pnl_section,
        render_strategy_detail,
        render_strategy_timeline,
        render_trade_tape_section,
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
    AuditPosture,
    CostStackInfo,
    GatewayConnectionError,
    GatewayDashboardClient,
    LedgerTradeRecord,
    PnLSummary,
    PositionInfo,
    StrategyDetails,
    StrategySummary,
    TimelineEvent,
)
from almanak.framework.dashboard.models import Strategy
from almanak.framework.dashboard.sections import (
    render_cost_stack_section,
    render_pnl_section,
    render_trade_tape_section,
)

__all__ = [
    # Focused gateway data slices (VIB-3969)
    "AuditPosture",
    "CostStackInfo",
    # Primary client (protobuf-free)
    "DashboardDataClient",
    # Data types
    "GatewayConnectionError",
    "GatewayDashboardClient",
    "LedgerTradeRecord",
    "PnLDataPoint",
    "PnLSummary",
    "PortfolioMetricsSummary",
    "PositionInfo",
    "Strategy",
    "StrategyDetails",
    "StrategySummary",
    "TimelineEvent",
    "TradeRecord",
    # Custom-dashboard section helpers
    "render_cost_stack_section",
    "render_pnl_section",
    # PM integration
    "render_strategy_detail",
    "render_strategy_timeline",
    "render_trade_tape_section",
    "strategy_from_pm_dict",
]
