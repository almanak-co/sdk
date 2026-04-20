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
"""

# Import the click group first — submodules register commands on it via decorators
# Re-export from backtesting imports that were available in the original module namespace
# (needed for patch() targets in tests)
from ...backtesting import (  # noqa: F401
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
from ...backtesting.paper.background import BackgroundPaperTrader, PaperTraderState, PIDFile  # noqa: F401
from ...backtesting.pnl.config_loader import ConfigLoadError, load_config_from_result  # noqa: F401
from ...backtesting.pnl.logging_utils import configure_backtest_logging  # noqa: F401
from ...backtesting.scenarios import (  # noqa: F401
    PREDEFINED_SCENARIOS,
    CrisisBacktestConfig,
    CrisisBacktestResult,
    CrisisScenario,
    build_crisis_metrics,
    compare_crisis_to_normal,
    get_scenario_by_name,
    run_crisis_backtest,
)
from ...backtesting.visualization import save_chart  # noqa: F401
from ...data.cache import CacheStats, DataCache  # noqa: F401
from ...strategies import get_strategy, list_strategies  # noqa: F401
from . import advanced as _advanced  # noqa: F401  — registers: walk-forward, monte-carlo, scenario
from . import block as _block  # noqa: F401  — registers: block
from . import paper as _paper  # noqa: F401  — registers: paper (start/stop/status/logs/resume), dashboard

# Import submodules to register their commands on the backtest group.
# Each submodule uses @backtest.command() or @backtest.group() decorators at import time.
from . import pnl as _pnl  # noqa: F401  — registers: pnl
from . import sweep as _sweep  # noqa: F401  — registers: sweep, optimize
from .advanced import (  # noqa: F401
    monte_carlo_backtest,
    print_crisis_backtest_results,
    print_monte_carlo_results,
    print_walk_forward_results,
    scenario_backtest,
    walk_forward_backtest,
)
from .block import block_backtest  # noqa: F401
from .group import backtest

# Re-export all symbols that were importable from the original backtest.py module.
# This ensures backward compatibility for tests and any code doing:
#   from almanak.framework.cli.backtest import <symbol>
from .helpers import (  # noqa: F401
    # Helper functions
    _BACKTEST_WALLET,
    # Configuration constants
    BLOCKS_PER_DAY,
    DEFAULT_END_BLOCKS,
    PAPER_STATE_DIR,
    # Data classes
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
from .paper import (  # noqa: F401
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

# Re-export click commands so they can be imported directly
from .pnl import pnl_backtest  # noqa: F401
from .sweep import (  # noqa: F401
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

# For `python -m almanak.framework.cli.backtest`, see __main__.py
