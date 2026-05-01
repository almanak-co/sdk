"""CLI command for running backtests.

This package provides CLI commands for all backtesting engines:

1. **PnL Backtest** (new): Historical simulation using price data
   Usage: almanak strat backtest pnl --strategy <name> --start <date> --end <date>

2. **Parameter Sweep** (new): Multi-parameter optimization in parallel
   Usage: almanak strat backtest sweep --strategy <name> --start <date> --end <date> \
          --param "name:val1,val2,val3" [--parallel N]

3. **Paper Trading** (new): Real-time simulation on Anvil forks with PnL tracking
   Usage: almanak strat backtest paper start --strategy <name> --chain <chain>
          almanak strat backtest paper stop --strategy <name>
          almanak strat backtest paper status --strategy <name>

4. **Block Backtest** (legacy): Block-based simulation using Anvil forks
   Usage: almanak strat backtest block --strategy <name> --days <n> --chain <chain>

Examples:
    # PnL backtest with date range
    almanak strat backtest pnl -s dynamic_lp --start 2024-01-01 --end 2024-06-01

    # PnL backtest with custom settings
    almanak strat backtest pnl -s mean_reversion --start 2024-01-01 --end 2024-03-01 \
        --interval 3600 --initial-capital 50000 --output results.json

    # Parameter sweep
    almanak strat backtest sweep -s momentum --start 2024-01-01 --end 2024-06-01 \
        --param "window:10,20,30" --param "threshold:0.5,1.0" --parallel 8

    # Paper trading - start, check, stop
    almanak strat backtest paper start -s momentum_v1 --chain arbitrum --initial-eth 10
    almanak strat backtest paper status -s momentum_v1
    almanak strat backtest paper stop -s momentum_v1

    # Legacy block-based backtest
    almanak strat backtest block -s dynamic_lp --days 7 --chain arbitrum

Module-level re-exports are resolved lazily via PEP 562 ``__getattr__`` so
that importing this package (notably from ``almanak/cli/cli.py`` at every
``almanak strat run`` startup) does not transitively load the backtesting
engine (optuna, sqlalchemy, plotly, matplotlib, …) until a re-exported
symbol is actually accessed. Subcommand modules likewise register on the
click group only when click first resolves them — see
``LazyBacktestGroup`` in ``group.py``.

The eager imports inside the ``TYPE_CHECKING`` block keep mypy and IDE
autocomplete fully accurate; at runtime each name resolves on first
attribute access and is cached on the module's ``globals()``.
"""

from typing import TYPE_CHECKING

# The click group object itself is cheap (it's a LazyBacktestGroup
# instance). Subcommand modules are loaded on first lookup.
from .group import backtest as backtest

if TYPE_CHECKING:
    # Re-exports from almanak.framework.backtesting and friends. These
    # exist for backward compatibility (``from almanak.framework.cli.backtest
    # import X``) and for ``unittest.mock.patch`` targets in tests.
    from ...backtesting import (
        BacktestResult,
        CoinGeckoDataProvider,
        PaperPortfolioTracker,
        PaperTrader,
        PaperTraderConfig,
        PaperTradingSummary,
        PnLBacktestConfig,
        PnLBacktester,
        RollingForkManager,
    )
    from ...backtesting.paper.background import (
        BackgroundPaperTrader,
        PaperTraderState,
        PIDFile,
    )
    from ...backtesting.pnl.config_loader import (
        ConfigLoadError,
        load_config_from_result,
    )
    from ...backtesting.pnl.logging_utils import configure_backtest_logging
    from ...backtesting.scenarios import (
        PREDEFINED_SCENARIOS,
        CrisisBacktestConfig,
        CrisisBacktestResult,
        CrisisScenario,
        build_crisis_metrics,
        compare_crisis_to_normal,
        get_scenario_by_name,
        run_crisis_backtest,
    )
    from ...backtesting.visualization import save_chart
    from ...data.cache import CacheStats, DataCache
    from ...strategies import get_strategy, list_strategies
    from .advanced import (
        monte_carlo_backtest,
        print_crisis_backtest_results,
        print_monte_carlo_results,
        print_walk_forward_results,
        scenario_backtest,
        walk_forward_backtest,
    )
    from .block import block_backtest
    from .helpers import (
        _BACKTEST_WALLET,
        BLOCKS_PER_DAY,
        DEFAULT_END_BLOCKS,
        PAPER_STATE_DIR,
        AggregatedParamResult,
        BacktestContext,
        SweepParameter,
        SweepResult,
        _create_backtest_strategy,
        _parse_duration,
        calculate_block_step,
        days_to_blocks,
        delete_paper_session_state,
        format_duration,
        generate_combinations,
        get_block_range,
        get_paper_state_file,
        is_process_running,
        list_paper_sessions,
        list_strategies_fn,
        load_paper_session_state,
        load_strategy_config,
        parse_date,
        parse_param_string,
        print_detailed_results,
        print_results_summary,
        save_paper_session_state,
        update_paper_session_status,
        write_json_output,
    )
    from .paper import (
        _run_paper_trading_foreground,
        dashboard_cmd,
        get_paper_log_file,
        paper,
        paper_logs,
        paper_resume,
        paper_start,
        paper_status,
        paper_stop,
    )
    from .pnl import pnl_backtest
    from .sweep import (
        _aggregate_multi_period_results,
        _print_multi_period_results,
        _run_parallel_sweep,
        _run_sweep_task_worker,
        _SweepTask,
        load_optimization_config,
        optimize_backtest,
        parse_param_ranges_from_config,
        print_optimization_results,
        print_sweep_results_table,
        run_parallel_sweeps,
        run_sweep_backtest,
        sweep_backtest,
    )


# --- Lazy resolution dispatch ---------------------------------------------
# Maps each public re-exported name to (absolute submodule, attribute on
# that submodule). Absolute paths avoid leading-dot ambiguity and let
# ``importlib.import_module(rel_module)`` work without a ``package=`` arg.
_LAZY_IMPORTS: dict[str, tuple[str, str]] = {
    # almanak.framework.backtesting (top-level re-exports) ------------------
    "BacktestResult": ("almanak.framework.backtesting", "BacktestResult"),
    "CoinGeckoDataProvider": ("almanak.framework.backtesting", "CoinGeckoDataProvider"),
    "PaperPortfolioTracker": ("almanak.framework.backtesting", "PaperPortfolioTracker"),
    "PaperTrader": ("almanak.framework.backtesting", "PaperTrader"),
    "PaperTraderConfig": ("almanak.framework.backtesting", "PaperTraderConfig"),
    "PaperTradingSummary": ("almanak.framework.backtesting", "PaperTradingSummary"),
    "PnLBacktestConfig": ("almanak.framework.backtesting", "PnLBacktestConfig"),
    "PnLBacktester": ("almanak.framework.backtesting", "PnLBacktester"),
    "RollingForkManager": ("almanak.framework.backtesting", "RollingForkManager"),
    "BackgroundPaperTrader": ("almanak.framework.backtesting.paper.background", "BackgroundPaperTrader"),
    "PaperTraderState": ("almanak.framework.backtesting.paper.background", "PaperTraderState"),
    "PIDFile": ("almanak.framework.backtesting.paper.background", "PIDFile"),
    "ConfigLoadError": ("almanak.framework.backtesting.pnl.config_loader", "ConfigLoadError"),
    "load_config_from_result": ("almanak.framework.backtesting.pnl.config_loader", "load_config_from_result"),
    "configure_backtest_logging": ("almanak.framework.backtesting.pnl.logging_utils", "configure_backtest_logging"),
    "PREDEFINED_SCENARIOS": ("almanak.framework.backtesting.scenarios", "PREDEFINED_SCENARIOS"),
    "CrisisBacktestConfig": ("almanak.framework.backtesting.scenarios", "CrisisBacktestConfig"),
    "CrisisBacktestResult": ("almanak.framework.backtesting.scenarios", "CrisisBacktestResult"),
    "CrisisScenario": ("almanak.framework.backtesting.scenarios", "CrisisScenario"),
    "build_crisis_metrics": ("almanak.framework.backtesting.scenarios", "build_crisis_metrics"),
    "compare_crisis_to_normal": ("almanak.framework.backtesting.scenarios", "compare_crisis_to_normal"),
    "get_scenario_by_name": ("almanak.framework.backtesting.scenarios", "get_scenario_by_name"),
    "run_crisis_backtest": ("almanak.framework.backtesting.scenarios", "run_crisis_backtest"),
    "save_chart": ("almanak.framework.backtesting.visualization", "save_chart"),
    "CacheStats": ("almanak.framework.data.cache", "CacheStats"),
    "DataCache": ("almanak.framework.data.cache", "DataCache"),
    "get_strategy": ("almanak.framework.strategies", "get_strategy"),
    "list_strategies": ("almanak.framework.strategies", "list_strategies"),
    # almanak.framework.cli.backtest.advanced -------------------------------
    "monte_carlo_backtest": ("almanak.framework.cli.backtest.advanced", "monte_carlo_backtest"),
    "print_crisis_backtest_results": ("almanak.framework.cli.backtest.advanced", "print_crisis_backtest_results"),
    "print_monte_carlo_results": ("almanak.framework.cli.backtest.advanced", "print_monte_carlo_results"),
    "print_walk_forward_results": ("almanak.framework.cli.backtest.advanced", "print_walk_forward_results"),
    "scenario_backtest": ("almanak.framework.cli.backtest.advanced", "scenario_backtest"),
    "walk_forward_backtest": ("almanak.framework.cli.backtest.advanced", "walk_forward_backtest"),
    # almanak.framework.cli.backtest.block ----------------------------------
    "block_backtest": ("almanak.framework.cli.backtest.block", "block_backtest"),
    # almanak.framework.cli.backtest.helpers --------------------------------
    "_BACKTEST_WALLET": ("almanak.framework.cli.backtest.helpers", "_BACKTEST_WALLET"),
    "BLOCKS_PER_DAY": ("almanak.framework.cli.backtest.helpers", "BLOCKS_PER_DAY"),
    "DEFAULT_END_BLOCKS": ("almanak.framework.cli.backtest.helpers", "DEFAULT_END_BLOCKS"),
    "PAPER_STATE_DIR": ("almanak.framework.cli.backtest.helpers", "PAPER_STATE_DIR"),
    "AggregatedParamResult": ("almanak.framework.cli.backtest.helpers", "AggregatedParamResult"),
    "BacktestContext": ("almanak.framework.cli.backtest.helpers", "BacktestContext"),
    "SweepParameter": ("almanak.framework.cli.backtest.helpers", "SweepParameter"),
    "SweepResult": ("almanak.framework.cli.backtest.helpers", "SweepResult"),
    "_create_backtest_strategy": ("almanak.framework.cli.backtest.helpers", "_create_backtest_strategy"),
    "_parse_duration": ("almanak.framework.cli.backtest.helpers", "_parse_duration"),
    "calculate_block_step": ("almanak.framework.cli.backtest.helpers", "calculate_block_step"),
    "days_to_blocks": ("almanak.framework.cli.backtest.helpers", "days_to_blocks"),
    "delete_paper_session_state": ("almanak.framework.cli.backtest.helpers", "delete_paper_session_state"),
    "format_duration": ("almanak.framework.cli.backtest.helpers", "format_duration"),
    "generate_combinations": ("almanak.framework.cli.backtest.helpers", "generate_combinations"),
    "get_block_range": ("almanak.framework.cli.backtest.helpers", "get_block_range"),
    "get_paper_state_file": ("almanak.framework.cli.backtest.helpers", "get_paper_state_file"),
    "is_process_running": ("almanak.framework.cli.backtest.helpers", "is_process_running"),
    "list_paper_sessions": ("almanak.framework.cli.backtest.helpers", "list_paper_sessions"),
    "list_strategies_fn": ("almanak.framework.cli.backtest.helpers", "list_strategies_fn"),
    "load_paper_session_state": ("almanak.framework.cli.backtest.helpers", "load_paper_session_state"),
    "load_strategy_config": ("almanak.framework.cli.backtest.helpers", "load_strategy_config"),
    "parse_date": ("almanak.framework.cli.backtest.helpers", "parse_date"),
    "parse_param_string": ("almanak.framework.cli.backtest.helpers", "parse_param_string"),
    "print_detailed_results": ("almanak.framework.cli.backtest.helpers", "print_detailed_results"),
    "print_results_summary": ("almanak.framework.cli.backtest.helpers", "print_results_summary"),
    "save_paper_session_state": ("almanak.framework.cli.backtest.helpers", "save_paper_session_state"),
    "update_paper_session_status": ("almanak.framework.cli.backtest.helpers", "update_paper_session_status"),
    "write_json_output": ("almanak.framework.cli.backtest.helpers", "write_json_output"),
    # almanak.framework.cli.backtest.paper ----------------------------------
    "_run_paper_trading_foreground": ("almanak.framework.cli.backtest.paper", "_run_paper_trading_foreground"),
    "dashboard_cmd": ("almanak.framework.cli.backtest.paper", "dashboard_cmd"),
    "get_paper_log_file": ("almanak.framework.cli.backtest.paper", "get_paper_log_file"),
    "paper": ("almanak.framework.cli.backtest.paper", "paper"),
    "paper_logs": ("almanak.framework.cli.backtest.paper", "paper_logs"),
    "paper_resume": ("almanak.framework.cli.backtest.paper", "paper_resume"),
    "paper_start": ("almanak.framework.cli.backtest.paper", "paper_start"),
    "paper_status": ("almanak.framework.cli.backtest.paper", "paper_status"),
    "paper_stop": ("almanak.framework.cli.backtest.paper", "paper_stop"),
    # almanak.framework.cli.backtest.pnl ------------------------------------
    "pnl_backtest": ("almanak.framework.cli.backtest.pnl", "pnl_backtest"),
    # almanak.framework.cli.backtest.sweep ----------------------------------
    "_SweepTask": ("almanak.framework.cli.backtest.sweep", "_SweepTask"),
    "_aggregate_multi_period_results": ("almanak.framework.cli.backtest.sweep", "_aggregate_multi_period_results"),
    "_print_multi_period_results": ("almanak.framework.cli.backtest.sweep", "_print_multi_period_results"),
    "_run_parallel_sweep": ("almanak.framework.cli.backtest.sweep", "_run_parallel_sweep"),
    "_run_sweep_task_worker": ("almanak.framework.cli.backtest.sweep", "_run_sweep_task_worker"),
    "load_optimization_config": ("almanak.framework.cli.backtest.sweep", "load_optimization_config"),
    "optimize_backtest": ("almanak.framework.cli.backtest.sweep", "optimize_backtest"),
    "parse_param_ranges_from_config": ("almanak.framework.cli.backtest.sweep", "parse_param_ranges_from_config"),
    "print_optimization_results": ("almanak.framework.cli.backtest.sweep", "print_optimization_results"),
    "print_sweep_results_table": ("almanak.framework.cli.backtest.sweep", "print_sweep_results_table"),
    "run_parallel_sweeps": ("almanak.framework.cli.backtest.sweep", "run_parallel_sweeps"),
    "run_sweep_backtest": ("almanak.framework.cli.backtest.sweep", "run_sweep_backtest"),
    "sweep_backtest": ("almanak.framework.cli.backtest.sweep", "sweep_backtest"),
}


__all__ = ["backtest", *sorted(name for name in _LAZY_IMPORTS if not name.startswith("_"))]


def __getattr__(name: str) -> object:
    import importlib

    if name in _LAZY_IMPORTS:
        module_path, attr_name = _LAZY_IMPORTS[name]
        module = importlib.import_module(module_path)
        attr = getattr(module, attr_name)
        globals()[name] = attr
        return attr
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(set(__all__) | set(_LAZY_IMPORTS) | set(globals()))


# For `python -m almanak.framework.cli.backtest`, see __main__.py
