"""Backtesting module for strategy evaluation.

This module provides a dual-engine backtesting system:

1. **PnL Backtester**: Historical simulation engine that backtests strategies
   against historical price data without executing real transactions.

2. **Paper Trader**: Real-time fork execution engine that validates strategies
   by executing actual transactions on a local Anvil fork of mainnet.

Usage:
    # PnL Backtesting (historical simulation)
    from almanak.framework.backtesting import PnLBacktester, PnLBacktestConfig, BacktestResult

    config = PnLBacktestConfig(
        start_time=datetime(2024, 1, 1),
        end_time=datetime(2024, 6, 1),
        token_funding=[
            {
                "symbol": "USDC",
                "address": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
                "chain": "arbitrum",
                "amount": "10000",
                "amount_type": "usd",
            }
        ],
    )
    backtester = PnLBacktester(data_provider, fee_models, slippage_models)
    result = await backtester.backtest(strategy, config)
    print(result.summary())

    # Paper Trading (real-time fork execution)
    from almanak.framework.backtesting import PaperTrader, PaperTraderConfig

    config = PaperTraderConfig(
        chain="arbitrum",
        rpc_url="https://arb1.arbitrum.io/rpc",
        deployment_id="my_strategy",
        tick_interval_seconds=60,
    )
    trader = PaperTrader(fork_manager, portfolio_tracker, config)
    result = await trader.run(strategy, duration_seconds=3600)
    print(f"PnL: ${result.metrics.net_pnl_usd}")
"""

# Shared models
# LP Performance Tracking
from typing import TYPE_CHECKING

from .lp_performance import (
    LPPerformanceReport,
    LPPerformanceTracker,
    LPSnapshot,
)

# Mock strategy (shared fallback — see #1701)
from .mock_strategy import MockBacktestStrategy, make_mock_strategy_class
from .models import (
    BacktestEngine as BacktestEngineType,
)
from .models import (
    BacktestMetrics,
    BacktestResult,
    EquityPoint,
    IntentType,
    TradeRecord,
)

# Paper Trader (real-time fork execution)
from .paper import (
    PaperPortfolioTracker,
    PaperTrade,
    PaperTradeableStrategy,
    PaperTradeError,
    PaperTradeErrorType,
    PaperTradeEventCallback,
    PaperTradeEventType,
    PaperTrader,
    PaperTraderConfig,
    PaperTradingSummary,
    RollingForkManager,
    create_market_snapshot_from_fork,
)

# PnL Backtester (historical simulation)
from .pnl import (
    OHLCV,
    BacktestableStrategy,
    BacktestTask,
    ChainlinkDataProvider,
    CoinGeckoDataProvider,
    HistoricalDataConfig,
    HistoricalDataProvider,
    MarketState,
    ParallelBacktestResult,
    PnLBacktestConfig,
    PnLBacktester,
    PortfolioAggregator,
    PositionType,
    SimulatedFill,
    SimulatedPortfolio,
    SimulatedPosition,
    create_market_snapshot_from_state,
    run_parallel_backtests,
    run_parallel_backtests_sync,
)

# Report generation is re-exported lazily via __getattr__ below:
# report_generator hard-requires jinja2, which ships in the `backtest`
# extra, not the base install. An eager import here made EVERY
# `almanak strat backtest` subcommand crash on a base install (VIB-5620),
# defeating the CLI lazy group's zero-import contract. The TYPE_CHECKING
# block keeps the symbols resolvable for type checkers and IDEs without a
# runtime import.
if TYPE_CHECKING:
    from .report_generator import (
        ReportResult,
        generate_report,
        generate_report_from_json,
    )

_REPORT_GENERATOR_EXPORTS = frozenset(
    {
        "ReportResult",
        "generate_report",
        "generate_report_from_json",
    }
)


def __getattr__(name: str):
    if name in _REPORT_GENERATOR_EXPORTS:
        from . import report_generator

        return getattr(report_generator, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


# Crisis scenarios
from .scenarios import (
    BLACK_THURSDAY,
    FTX_COLLAPSE,
    PREDEFINED_SCENARIOS,
    TERRA_COLLAPSE,
    CrisisScenario,
    get_scenario_by_name,
)

# Visualization
from .visualization import (
    ChartConfig,
    ChartResult,
    DrawdownPeriod,
    generate_drawdown_chart_html,
    generate_equity_chart_html,
    generate_pnl_distribution_html,
    plot_equity_curve,
)

__all__ = [
    # Shared models
    "BacktestEngineType",
    "BacktestMetrics",
    "BacktestResult",
    "EquityPoint",
    "IntentType",
    "TradeRecord",
    # PnL Backtester (historical simulation)
    "PnLBacktester",
    "PnLBacktestConfig",
    "SimulatedPortfolio",
    "SimulatedPosition",
    "SimulatedFill",
    "PositionType",
    "PortfolioAggregator",
    "HistoricalDataProvider",
    "HistoricalDataConfig",
    "MarketState",
    "OHLCV",
    "CoinGeckoDataProvider",
    "ChainlinkDataProvider",
    "BacktestableStrategy",
    "create_market_snapshot_from_state",
    # Paper Trader (real-time fork execution)
    "PaperTrader",
    "PaperTraderConfig",
    "PaperTradingSummary",
    "PaperTrade",
    "PaperTradeError",
    "PaperTradeErrorType",
    "RollingForkManager",
    "PaperPortfolioTracker",
    "PaperTradeableStrategy",
    "PaperTradeEventType",
    "PaperTradeEventCallback",
    "create_market_snapshot_from_fork",
    # Parallel execution
    "BacktestTask",
    "ParallelBacktestResult",
    "run_parallel_backtests",
    "run_parallel_backtests_sync",
    # Crisis scenarios
    "CrisisScenario",
    "BLACK_THURSDAY",
    "TERRA_COLLAPSE",
    "FTX_COLLAPSE",
    "PREDEFINED_SCENARIOS",
    "get_scenario_by_name",
    # Visualization
    "plot_equity_curve",
    "ChartConfig",
    "ChartResult",
    "DrawdownPeriod",
    "generate_equity_chart_html",
    "generate_pnl_distribution_html",
    "generate_drawdown_chart_html",
    # LP Performance Tracking
    "LPPerformanceTracker",
    "LPPerformanceReport",
    "LPSnapshot",
    # Mock strategy (CLI fallback)
    "MockBacktestStrategy",
    "make_mock_strategy_class",
    # Report generation
    "ReportResult",
    "generate_report",
    "generate_report_from_json",
]
