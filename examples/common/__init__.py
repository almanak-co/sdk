"""Common utilities for backtest examples."""

from examples.common.chart_helpers import (
    calculate_drawdown,
    calculate_equity_curve,
    generate_complete_chart,
    generate_metrics_table,
)
from examples.common.data_providers import (
    LPRangeDataProvider,
    LendingDataProvider,
    RSITriggerDataProvider,
    SyntheticDataProvider,
)

__all__ = [
    "SyntheticDataProvider",
    "RSITriggerDataProvider",
    "LPRangeDataProvider",
    "LendingDataProvider",
    "calculate_equity_curve",
    "calculate_drawdown",
    "generate_complete_chart",
    "generate_metrics_table",
]
