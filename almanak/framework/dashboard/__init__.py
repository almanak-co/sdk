"""Almanak Strategy Framework v2.0 - Dashboard

Public exports for dashboard data access, rendering, and PM integration.
External consumers (PM dashboard, custom UIs) should import from here.

The streamlit-using ``render_*_section`` helpers are resolved lazily via
:pep:`562` ``__getattr__`` so that gateway-side consumers — which import
``almanak.framework.dashboard.quant_aggregations`` to build PnL / cost-stack
RPC responses — do not transitively pay the cost of loading ``streamlit`` at
package init. The gateway image strips ``streamlit`` (see
``deploy/docker/strip-list-gateway.txt``); an eager re-export here would
``ModuleNotFoundError`` on every dashboard RPC in production (VIB-4048).
Regression guard: ``tests/gateway/test_imports_lean.py``.

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

from typing import TYPE_CHECKING

from almanak._lazy import LazySpec, build_lazy_module_dispatch
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

if TYPE_CHECKING:
    from almanak.framework.dashboard.sections import (
        render_cost_stack_section,
        render_pnl_section,
        render_trade_tape_section,
    )
    from almanak.framework.dashboard.sections_operator import (
        render_reconciliation_operator_panel,
    )
    from almanak.framework.dashboard.sections_reconciliation import (
        render_position_range_history_section,
        render_positions_section,
        render_reconciliation_report_section,
    )

# Submodules whose import drags in streamlit. Resolved lazily so the
# package init stays streamlit-free; the gateway sidecar image (which
# strips streamlit) can call into other submodules of this package
# without tripping ModuleNotFoundError.
_LAZY_IMPORTS: dict[str, LazySpec] = {
    "render_cost_stack_section": ".sections",
    "render_pnl_section": ".sections",
    "render_trade_tape_section": ".sections",
    # Phase 3 (VIB-4495) — Phase 1 RPC-backed section helpers
    "render_position_range_history_section": ".sections_reconciliation",
    "render_positions_section": ".sections_reconciliation",
    "render_reconciliation_report_section": ".sections_reconciliation",
    # Operator-only — Phase 4's CI lint enforces no renderer-side imports
    "render_reconciliation_operator_panel": ".sections_operator",
}

__getattr__, __dir__ = build_lazy_module_dispatch(_LAZY_IMPORTS, package=__name__, namespace=globals())


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
    # Custom-dashboard section helpers (lazy — pull streamlit on first access)
    "render_cost_stack_section",
    "render_pnl_section",
    # Phase 3 / VIB-4495 — Phase 1 RPC-backed sections (lazy)
    "render_position_range_history_section",
    "render_positions_section",
    "render_reconciliation_operator_panel",
    "render_reconciliation_report_section",
    # PM integration
    "render_strategy_detail",
    "render_strategy_timeline",
    "render_trade_tape_section",
    "strategy_from_pm_dict",
]
