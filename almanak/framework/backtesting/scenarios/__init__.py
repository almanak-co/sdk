"""Crisis scenario definitions and backtest runner for stress-testing strategies.

This module provides pre-defined historical crisis scenarios and functions for
running backtests during these periods. Each scenario represents a significant
market event that caused substantial price volatility and/or market disruption.

Scenarios:
    - BLACK_THURSDAY: March 2020 COVID crash
    - TERRA_COLLAPSE: May 2022 UST/LUNA de-peg
    - FTX_COLLAPSE: November 2022 FTX bankruptcy

Usage:
    from almanak.framework.backtesting.scenarios import (
        CrisisScenario,
        BLACK_THURSDAY,
        TERRA_COLLAPSE,
        FTX_COLLAPSE,
        PREDEFINED_SCENARIOS,
        get_scenario_by_name,
        run_crisis_backtest,
        run_crisis_backtest_sync,
    )

    # Use a predefined scenario
    result = await run_crisis_backtest(
        strategy=my_strategy,
        scenario=BLACK_THURSDAY,
        backtester=backtester,
        initial_capital_usd=Decimal("10000"),
    )

    # Create a custom scenario
    custom = CrisisScenario(
        name="custom_crisis",
        start_date=datetime(2023, 3, 10),
        end_date=datetime(2023, 3, 15),
        description="Silicon Valley Bank collapse",
    )
    result = await run_crisis_backtest(strategy, custom, backtester)
"""

from almanak.framework.backtesting.scenarios.crisis import (
    BLACK_THURSDAY,
    FTX_COLLAPSE,
    PREDEFINED_SCENARIOS,
    TERRA_COLLAPSE,
    CrisisScenario,
    get_scenario_by_name,
)
from almanak.framework.backtesting.scenarios.crisis_runner import (
    CrisisBacktestConfig,
    CrisisBacktestResult,
    build_crisis_metrics,
    compare_crisis_to_normal,
    run_crisis_backtest,
    run_crisis_backtest_sync,
    run_multiple_crisis_backtests,
    run_multiple_crisis_backtests_sync,
)

__all__ = [
    # Crisis scenario definitions
    "CrisisScenario",
    "BLACK_THURSDAY",
    "TERRA_COLLAPSE",
    "FTX_COLLAPSE",
    "PREDEFINED_SCENARIOS",
    "get_scenario_by_name",
    # Crisis backtest runner
    "CrisisBacktestConfig",
    "CrisisBacktestResult",
    "build_crisis_metrics",
    "compare_crisis_to_normal",
    "run_crisis_backtest",
    "run_crisis_backtest_sync",
    "run_multiple_crisis_backtests",
    "run_multiple_crisis_backtests_sync",
]
