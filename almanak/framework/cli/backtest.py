"""CLI command for running backtests.

This module provides CLI commands for all backtesting engines:

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

import asyncio
import json
import os
import re
import signal
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import click
from dotenv import load_dotenv

from almanak.framework.anvil.fork_manager import CHAIN_IDS

from ..backtesting import (
    BacktestResult,
    CoinGeckoDataProvider,
    PaperPortfolioTracker,
    # Paper trader imports
    PaperTrader,
    PaperTraderConfig,
    PaperTradingSummary,
    PnLBacktestConfig,
    # New PnL backtester imports
    PnLBacktester,
    RollingForkManager,
)
from ..backtesting.paper.background import BackgroundPaperTrader
from ..backtesting.pnl.config_loader import ConfigLoadError, load_config_from_result
from ..backtesting.pnl.logging_utils import configure_backtest_logging
from ..backtesting.scenarios import (
    PREDEFINED_SCENARIOS,
    CrisisBacktestConfig,
    CrisisBacktestResult,
    CrisisScenario,
    build_crisis_metrics,
    compare_crisis_to_normal,
    get_scenario_by_name,
    run_crisis_backtest,
)
from ..backtesting.visualization import save_chart
from ..data.cache import CacheStats, DataCache
from ..strategies import get_strategy, list_strategies

# =============================================================================
# Configuration
# =============================================================================

# Approximate blocks per day for each chain
# Calculated as: 86400 seconds/day / block_time
BLOCKS_PER_DAY: dict[str, int] = {
    "ethereum": 7200,  # ~12s blocks
    "arbitrum": 345600,  # ~0.25s blocks
    "optimism": 43200,  # ~2s blocks
    "polygon": 43200,  # ~2s blocks
    "base": 43200,  # ~2s blocks
    "avalanche": 43200,  # ~2s blocks
}

# Default recent block numbers for testing (when --end-block not specified)
# These are placeholder values - in production, would query chain for current block
DEFAULT_END_BLOCKS: dict[str, int] = {
    "ethereum": 19000000,
    "arbitrum": 170000000,
    "optimism": 115000000,
    "polygon": 52000000,
    "base": 10000000,
    "avalanche": 40000000,
}


# =============================================================================
# Helper Functions
# =============================================================================

# Placeholder wallet address for backtest strategy instantiation.
# Backtests don't execute real transactions so the address is irrelevant.
_BACKTEST_WALLET = "0x" + "0" * 40


def _create_backtest_strategy(
    strategy_class: Any,
    config: dict[str, Any],
    chain: str,
) -> Any:
    """Instantiate a strategy for backtesting.

    IntentStrategy subclasses require (config, chain, wallet_address).
    Simpler classes may accept only (config,) or no arguments.
    This helper tries each signature in order.

    Args:
        strategy_class: The strategy class to instantiate.
        config: Strategy configuration dict.
        chain: Target chain (e.g. "arbitrum").

    Returns:
        An instantiated strategy object.
    """
    # 1. Try IntentStrategy signature: (config, chain, wallet_address)
    try:
        return strategy_class(config, chain, _BACKTEST_WALLET)
    except TypeError:
        pass

    # 2. Try simple signature: (config,)
    try:
        return strategy_class(config)
    except TypeError:
        pass

    # 3. Fall back to no-arg constructor (mock strategies)
    return strategy_class()


def days_to_blocks(days: int, chain: str) -> int:
    """Convert days to approximate block count for a chain.

    Args:
        days: Number of days
        chain: Target blockchain

    Returns:
        Approximate number of blocks
    """
    blocks_per_day = BLOCKS_PER_DAY.get(chain, 7200)
    return days * blocks_per_day


def get_block_range(
    days: int,
    chain: str,
    end_block: int | None = None,
) -> tuple[int, int]:
    """Calculate start and end blocks for a backtest.

    Args:
        days: Number of days to backtest
        chain: Target blockchain
        end_block: Optional end block (defaults to recent block)

    Returns:
        Tuple of (start_block, end_block)
    """
    if end_block is None:
        end_block = DEFAULT_END_BLOCKS.get(chain, 19000000)

    blocks_needed = days_to_blocks(days, chain)
    start_block = max(0, end_block - blocks_needed)

    return start_block, end_block


def calculate_block_step(days: int, chain: str) -> int:
    """Calculate an appropriate block step for the backtest duration.

    Aims for roughly 100-1000 iterations for reasonable performance.

    Args:
        days: Number of days
        chain: Target blockchain

    Returns:
        Block step size
    """
    total_blocks = days_to_blocks(days, chain)

    # Target ~500 iterations
    target_iterations = 500
    step = max(1, total_blocks // target_iterations)

    # Round to nice numbers for readability
    if step >= 10000:
        step = (step // 10000) * 10000
    elif step >= 1000:
        step = (step // 1000) * 1000
    elif step >= 100:
        step = (step // 100) * 100
    elif step >= 10:
        step = (step // 10) * 10

    return step


def format_duration(seconds: float) -> str:
    """Format duration in human-readable format.

    Args:
        seconds: Duration in seconds

    Returns:
        Formatted string (e.g., "2m 30s")
    """
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        secs = seconds % 60
        return f"{minutes}m {secs:.0f}s"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        return f"{hours}h {minutes}m"


def print_results_summary(result: Any) -> None:
    """Print a formatted results summary to stdout.

    Args:
        result: Backtest result to display (legacy or new format)
    """
    click.echo(result.summary())


def print_detailed_results(result: Any) -> None:
    """Print detailed results including trade list (for legacy block-based backtest).

    Args:
        result: Backtest result to display (legacy format with block_number)
    """
    print_results_summary(result)

    if result.trades:
        click.echo()
        click.echo("-" * 60)
        click.echo("TRADE HISTORY")
        click.echo("-" * 60)

        for i, trade in enumerate(result.trades, 1):
            pnl_usd = getattr(trade, "pnl_usd", Decimal("0"))
            gas_cost_usd = getattr(trade, "gas_cost_usd", Decimal("0"))
            block_number = getattr(trade, "block_number", "N/A")
            intent_type_value = (
                trade.intent_type.value if hasattr(trade.intent_type, "value") else str(trade.intent_type)
            )
            pnl_sign = "+" if pnl_usd >= 0 else ""
            click.echo(
                f"{i:3}. Block {block_number}: "
                f"{intent_type_value:10} "
                f"{pnl_sign}${pnl_usd:,.2f} "
                f"(gas: ${gas_cost_usd:,.2f})"
            )

        click.echo("-" * 60)


def write_json_output(result: Any, output_path: Path) -> None:
    """Write full backtest results to a JSON file.

    Args:
        result: Backtest result (legacy or new format)
        output_path: Path to output file
    """
    output_data: dict[str, Any] = result.to_dict()
    output_data["_meta"] = {
        "generated_at": datetime.now(UTC).isoformat(),
        "generator": "almanak backtest",
    }

    with open(output_path, "w") as f:
        json.dump(output_data, f, indent=2, default=str)

    click.echo(f"Results written to: {output_path}")


@dataclass
class BacktestContext:
    """Context for backtest execution."""

    strategy_name: str
    days: int
    chain: str
    start_block: int
    end_block: int
    block_step: int
    initial_balance: Decimal
    gas_price: Decimal
    archive_rpc: str | None
    output_path: Path | None
    verbose: bool


def load_strategy_config(strategy_name: str, chain: str) -> dict[str, Any]:
    """Load default configuration for a strategy.

    This attempts to load config from a JSON file or creates
    a minimal default configuration.

    Args:
        strategy_name: Name of the strategy
        chain: Target chain

    Returns:
        Configuration dictionary
    """
    # Strip demo_/incubating_ prefix to get the directory name
    # e.g. "demo_uniswap_rsi" -> "uniswap_rsi"
    dir_name = strategy_name
    if dir_name.startswith("demo_"):
        dir_name = dir_name[len("demo_") :]
    elif dir_name.startswith("incubating_"):
        dir_name = dir_name[len("incubating_") :]

    # Look for config file in standard locations
    config_paths = [
        Path(f"configs/{strategy_name}.json"),
        Path(f"configs/{strategy_name}_{chain}.json"),
        Path(f"src/strategies/{strategy_name}/config.json"),
        # Demo and incubating strategy directories
        Path(f"strategies/demo/{dir_name}/config.json"),
        Path(f"strategies/demo/{strategy_name}/config.json"),
        Path(f"strategies/incubating/{dir_name}/config.json"),
        Path(f"strategies/incubating/{strategy_name}/config.json"),
    ]

    for path in config_paths:
        if path.exists():
            with open(path) as f:
                config: dict[str, Any] = json.load(f)
                click.echo(f"Loaded config from: {path}")
                return config

    # Return default minimal config
    return {
        "strategy_id": f"backtest-{strategy_name}-{datetime.now().strftime('%Y%m%d%H%M%S')}",
        "wallet_address": "0x" + "0" * 40,  # Placeholder
    }


# =============================================================================
# CLI Group
# =============================================================================


@click.group("backtest")
def backtest() -> None:
    """
    Run backtests for Almanak strategies.

    \b
    Commands (ordered by typical workflow):
      pnl           Single backtest with historical price data
      sweep         Grid search over parameter combinations
      optimize      Bayesian hyperparameter tuning (Optuna TPE)
      walk-forward  Walk-forward optimization with overfitting detection
      monte-carlo   Monte Carlo simulation with synthetic price paths
      scenario      Crisis scenario stress testing
      paper         Paper trading on Anvil forks (live-like simulation)
      dashboard     Interactive Streamlit dashboard

    \b
    Multi-period support (sweep & optimize):
      --periods "2024-quarterly"  Test across Q1-Q4 in one command
      --periods "2024-monthly"    Test across all 12 months
      --periods "rolling-6m"      Six rolling 6-month windows

    \b
    Examples:
      # Single PnL backtest
      almanak backtest pnl -s my_strat --start 2024-01-01 --end 2024-06-01

    \b
      # Grid search over parameters
      almanak backtest sweep -s my_strat --start 2024-01-01 --end 2024-06-01 \\
          --param "window:10,20,30" --param "threshold:0.5,1.0"

    \b
      # Multi-period sweep (test robustness across quarters)
      almanak backtest sweep -s my_strat --periods "2024-quarterly" \\
          --param "window:10,20,30"

    \b
      # Bayesian optimization (finds optimal params automatically)
      almanak backtest optimize -s my_strat --start 2024-01-01 --end 2024-06-01 \\
          --config-file optimize_config.json --n-trials 100

    \b
      # Paper trading
      almanak backtest paper start -s my_strat --chain arbitrum

    \b
      # List available strategies
      almanak backtest pnl --list-strategies
    """
    # Load .env from current directory so backtest commands pick up
    # API keys (COINGECKO_API_KEY, THEGRAPH_API_KEY, ALCHEMY_API_KEY, etc.)
    # the same way 'almanak strat run' does from its working directory.
    env_file = Path.cwd() / ".env"
    if load_dotenv(dotenv_path=env_file):
        click.echo(f"Loaded environment from: {env_file}")


# Alias to avoid conflict with --list-strategies option
list_strategies_fn = list_strategies


# =============================================================================
# PnL Backtest Subcommand (NEW)
# =============================================================================


def parse_date(ctx: Any, param: Any, value: str | None) -> datetime | None:
    """Parse date string to datetime object."""
    if value is None:
        return None
    try:
        # Try common formats
        for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y-%m-%dT%H:%M:%S"):
            try:
                return datetime.strptime(value, fmt).replace(tzinfo=UTC)
            except ValueError:
                continue
        raise click.BadParameter(f"Cannot parse date '{value}'. Use YYYY-MM-DD format.")
    except Exception as e:
        raise click.BadParameter(str(e)) from e


@backtest.command("pnl")
@click.option(
    "--strategy",
    "-s",
    required=False,
    default=None,
    help="Name of the strategy to backtest",
)
@click.option(
    "--start",
    required=False,
    default=None,
    callback=parse_date,
    help="Start date (YYYY-MM-DD)",
)
@click.option(
    "--end",
    required=False,
    default=None,
    callback=parse_date,
    help="End date (YYYY-MM-DD)",
)
@click.option(
    "--interval",
    type=int,
    default=3600,
    help="Interval between ticks in seconds (default: 3600 = 1 hour)",
)
@click.option(
    "--initial-capital",
    type=float,
    default=10000.0,
    help="Initial portfolio balance in USD (default: 10000)",
)
@click.option(
    "--output",
    "-o",
    type=click.Path(exists=False),
    default=None,
    help="Output file for full JSON results (optional)",
)
@click.option(
    "--chain",
    "-c",
    type=str,
    default="arbitrum",
    help="Target blockchain (default: arbitrum)",
)
@click.option(
    "--tokens",
    type=str,
    default="WETH,USDC",
    help="Comma-separated list of tokens to track (default: WETH,USDC)",
)
@click.option(
    "--gas-price",
    type=float,
    default=30.0,
    help="Gas price in Gwei for cost estimation (default: 30)",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    default=False,
    help="Show detailed output including trade history",
)
@click.option(
    "--list-strategies",
    is_flag=True,
    default=False,
    help="List all available strategies and exit",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Show configuration without running backtest",
)
@click.option(
    "--config-file",
    type=click.Path(exists=True),
    default=None,
    help="Path to strategy config JSON file",
)
@click.option(
    "--warm-cache",
    is_flag=True,
    default=False,
    help="Pre-warm data cache before running backtest. Reduces API calls during backtest.",
)
@click.option(
    "--chart",
    is_flag=True,
    default=False,
    help="Generate equity curve chart alongside JSON results",
)
@click.option(
    "--chart-format",
    type=click.Choice(["png", "html"], case_sensitive=False),
    default="png",
    help="Chart output format: 'png' for static image, 'html' for interactive (default: png)",
)
@click.option(
    "--report",
    is_flag=True,
    default=False,
    help="Generate HTML report with charts, metrics, and trade log",
)
@click.option(
    "--benchmark",
    type=click.Choice(["eth_hold", "btc_hold", "defi_index"], case_sensitive=False),
    default="eth_hold",
    help="Benchmark for comparison: eth_hold (default), btc_hold, or defi_index",
)
@click.option(
    "--from-result",
    type=click.Path(exists=True),
    default=None,
    help="Load backtest config from a previous result JSON file. Overrides --start, --end, etc.",
)
def pnl_backtest(
    strategy: str | None,
    start: datetime | None,
    end: datetime | None,
    interval: int,
    initial_capital: float,
    output: str | None,
    chain: str,
    tokens: str,
    gas_price: float,
    verbose: bool,
    list_strategies: bool,
    dry_run: bool,
    config_file: str | None,
    warm_cache: bool,
    chart: bool,
    chart_format: str,
    report: bool,
    benchmark: str,
    from_result: str | None,
) -> None:
    """
    Run a PnL backtest using historical price data.

    This command simulates strategy execution against historical price data
    from CoinGecko. It calculates performance metrics including PnL, Sharpe
    ratio, max drawdown, and win rate WITHOUT executing actual transactions.

    The PnL backtester is ideal for:
    - Rapid strategy iteration and testing
    - Long-duration backtests (months to years)
    - Parameter optimization and sensitivity analysis

    Benchmark Options:
    - eth_hold: Buy and hold ETH (default)
    - btc_hold: Buy and hold BTC
    - defi_index: Weighted basket of DeFi tokens (UNI, AAVE, LINK, etc.)

    Reproducibility:
    Use --from-result to re-run a backtest with the exact same configuration
    as a previous run. This loads the config from a saved result JSON file.

    Examples:

        # Basic backtest for 6 months
        almanak backtest pnl -s dynamic_lp --start 2024-01-01 --end 2024-06-01

        # Custom settings with JSON output
        almanak backtest pnl -s mean_reversion --start 2024-01-01 --end 2024-03-01 \\
            --interval 3600 --initial-capital 50000 --output results.json

        # Backtest with BTC benchmark comparison
        almanak backtest pnl -s my_strategy --start 2024-01-01 --end 2024-06-01 \\
            --benchmark btc_hold

        # Re-run a backtest from a previous result (reproducibility)
        almanak backtest pnl -s my_strategy --from-result results/previous_run.json

        # List available strategies
        almanak backtest pnl --list-strategies
    """
    # Handle --list-strategies flag
    if list_strategies:
        available = list_strategies_fn()
        if available:
            click.echo("Available strategies:")
            for name in sorted(available):
                click.echo(f"  - {name}")
        else:
            click.echo("No strategies registered.")
            click.echo()
            click.echo("Strategies must be registered in the factory. See:")
            click.echo("  almanak strat new --help")
        return

    # Handle --from-result flag to load config from previous backtest
    pnl_config: PnLBacktestConfig | None = None
    loaded_from_result = False
    result_metadata: dict[str, Any] = {}

    if from_result:
        try:
            click.echo(f"Loading config from previous result: {from_result}")
            load_result = load_config_from_result(from_result)
            pnl_config = load_result.config
            result_metadata = load_result.metadata
            loaded_from_result = True

            # Show warnings if any
            if load_result.warnings:
                click.echo()
                click.echo("Warnings:", err=True)
                for warning in load_result.warnings:
                    click.echo(f"  - {warning}", err=True)
                click.echo()

            # Show metadata info
            if result_metadata:
                sdk_version = result_metadata.get("sdk_version", "unknown")
                config_created = result_metadata.get("config_created_at", "unknown")
                click.echo(f"Original SDK version: {sdk_version}")
                click.echo(f"Config created at: {config_created}")

        except FileNotFoundError as e:
            click.echo(f"Error: {e}", err=True)
            raise click.Abort() from e
        except ConfigLoadError as e:
            click.echo(f"Error loading config: {e}", err=True)
            raise click.Abort() from e

    # Validate required arguments for actual backtest (if not loaded from result)
    if not loaded_from_result:
        if not strategy:
            raise click.UsageError("Missing option '--strategy' / '-s'. Required for backtesting.")
        if not start:
            raise click.UsageError("Missing option '--start'. Required for backtesting.")
        if not end:
            raise click.UsageError("Missing option '--end'. Required for backtesting.")

    # Strategy is always required
    if not strategy:
        raise click.UsageError("Missing option '--strategy' / '-s'. Required for backtesting.")

    # Validate strategy exists
    available_strategies = list_strategies_fn()
    if strategy not in available_strategies and available_strategies:
        click.echo(f"Error: Unknown strategy '{strategy}'", err=True)
        click.echo(f"Available strategies: {', '.join(sorted(available_strategies))}", err=True)
        click.echo()
        click.echo("Create a new strategy with: almanak strat new --name <name>", err=True)
        raise click.Abort()

    # Prepare output path
    output_path = Path(output) if output else None

    # Configure logging based on verbose flag
    # In verbose mode: DEBUG level for detailed trade execution logs
    # In normal mode: INFO level and above only
    configure_backtest_logging(verbose=verbose)

    # Create PnL backtest config if not loaded from result
    if not loaded_from_result:
        # Parse tokens list
        token_list = [t.strip().upper() for t in tokens.split(",")]

        pnl_config = PnLBacktestConfig(
            start_time=start,  # type: ignore[arg-type]
            end_time=end,  # type: ignore[arg-type]
            interval_seconds=interval,
            initial_capital_usd=Decimal(str(initial_capital)),
            chain=chain,
            tokens=token_list,
            gas_price_gwei=Decimal(str(gas_price)),
            include_gas_costs=True,
        )
    else:
        # Use loaded config's values for display
        token_list = pnl_config.tokens  # type: ignore[union-attr]

    assert pnl_config is not None, "Config must be set at this point"

    # Display configuration
    click.echo("=" * 60)
    click.echo("PNL BACKTEST CONFIGURATION")
    if loaded_from_result:
        click.echo(f"(Loaded from: {from_result})")
    click.echo("=" * 60)
    click.echo(f"Strategy: {strategy}")
    click.echo(f"Chain: {pnl_config.chain}")
    click.echo(
        f"Period: {pnl_config.start_time.date()} -> {pnl_config.end_time.date()} ({pnl_config.duration_days:.1f} days)"
    )
    click.echo(f"Interval: {pnl_config.interval_seconds}s ({pnl_config.interval_seconds / 3600:.1f} hours)")
    click.echo(f"Initial Capital: ${pnl_config.initial_capital_usd:,.2f}")
    click.echo(f"Tokens: {', '.join(token_list)}")
    click.echo(f"Gas Price: {pnl_config.gas_price_gwei} Gwei")
    click.echo(f"Estimated Ticks: ~{pnl_config.estimated_ticks:,}")
    click.echo(f"Warm Cache: {'Yes' if warm_cache else 'No'}")

    if output_path:
        click.echo(f"Output: {output_path}")

    click.echo("=" * 60)

    # Handle dry run
    if dry_run:
        click.echo()
        click.echo("Dry run - backtest not executed.")
        return

    # Load strategy configuration
    if config_file:
        with open(config_file) as f:
            strategy_config = json.load(f)
        click.echo(f"Loaded config from: {config_file}")
    else:
        strategy_config = load_strategy_config(strategy, chain)

    # Get strategy class and create instance
    try:
        strategy_class = get_strategy(strategy)
    except ValueError:
        # If no strategies registered, create a mock strategy for demo
        click.echo()
        click.echo("Warning: No strategies registered in factory.", err=True)
        click.echo("Running with mock strategy for demonstration.", err=True)
        click.echo()

        # Create a minimal mock strategy compatible with BacktestableStrategy
        from ..strategies import MarketSnapshot

        class MockPnLStrategy:
            """Mock strategy for PnL backtesting demonstration."""

            strategy_id: str = f"mock-{datetime.now().strftime('%Y%m%d%H%M%S')}"

            def __init__(self, config: dict[str, Any]) -> None:
                self.config = config
                self._iteration = 0

            def decide(self, market: MarketSnapshot) -> dict[str, Any] | None:
                self._iteration += 1
                # Return a hold intent (no action)
                return None

        strategy_class = MockPnLStrategy

    # Create strategy instance
    strategy_instance = _create_backtest_strategy(strategy_class, strategy_config, chain)

    # Ensure strategy has a non-empty strategy_id.
    # IntentStrategy.__init__ sets _strategy_id="" which shadows StrategyBase's config-based value,
    # so we check for falsy values, not just missing attributes.
    existing_id = getattr(strategy_instance, "strategy_id", "")
    if not existing_id:
        fallback_id = (
            strategy_config.get("strategy_id")
            or strategy_config.get("name")
            or strategy
            or strategy_instance.__class__.__name__
        )
        # Set the private attribute to avoid property/setter issues
        if hasattr(strategy_instance, "_strategy_id"):
            strategy_instance._strategy_id = fallback_id
        else:
            strategy_instance.strategy_id = fallback_id

    # Create data provider — enable persistent SQLite cache and resilient retry
    # for backtest workloads to survive CoinGecko 429 rate limits on free tier
    click.echo()
    click.echo("Initializing CoinGecko data provider...")
    from ..backtesting.pnl.providers.coingecko import RetryConfig

    data_provider = CoinGeckoDataProvider(
        retry_config=RetryConfig.for_backtest(),
        persistent_cache=True,
        historical_cache_ttl=0,  # No TTL — historical prices are immutable
    )

    # Initialize data cache for tracking stats
    cache: DataCache | None = None
    cache_stats: CacheStats | None = None

    # Warm cache if requested
    if warm_cache:
        click.echo()
        click.echo("Warming data cache...")
        cache = DataCache(ttl_seconds=0)  # No TTL for backtest data
        cache.reset_stats()

        async def warm_cache_async() -> int:
            """Pre-fetch OHLCV data and store in cache."""
            from ..data.cache import CacheKey, OHLCVData

            total_cached = 0
            try:
                for token in token_list:
                    try:
                        cache_start = start or pnl_config.start_time
                        cache_end = end or pnl_config.end_time
                        ohlcv_data = await data_provider.get_ohlcv(token, cache_start, cache_end, interval)
                        items = []
                        for ohlcv in ohlcv_data:
                            key = CacheKey(
                                token=token.upper(),
                                timestamp=ohlcv.timestamp,
                                interval=f"{interval}s",
                            )
                            data = OHLCVData(
                                open=ohlcv.open,
                                high=ohlcv.high,
                                low=ohlcv.low,
                                close=ohlcv.close,
                                volume=ohlcv.volume if hasattr(ohlcv, "volume") else None,
                            )
                            items.append((key, data))

                        if cache is not None:
                            cached_count = cache.set_batch(items)
                            total_cached += cached_count
                            click.echo(f"  Cached {cached_count} data points for {token}")

                    except Exception as e:
                        click.echo(f"  Warning: Failed to cache data for {token}: {e}", err=True)
            finally:
                await data_provider.close()

            return total_cached

        try:
            total_points = asyncio.run(warm_cache_async())
            click.echo(f"Cache warming complete: {total_points} total data points")
        except Exception as e:
            click.echo(f"Warning: Cache warming failed: {e}", err=True)
            click.echo("Proceeding with backtest without pre-warmed cache...")

        # Create fresh data provider — warm_cache_async closed the previous one
        # to avoid "Event loop is closed" errors from stale aiohttp sessions
        data_provider = CoinGeckoDataProvider(
            retry_config=RetryConfig.for_backtest(),
            persistent_cache=True,
            historical_cache_ttl=0,
        )

    # Create backtester
    backtester = PnLBacktester(
        data_provider=data_provider,
        fee_models={},  # Will use defaults
        slippage_models={},  # Will use defaults
    )

    # Run backtest
    click.echo()
    click.echo("Starting PnL backtest...")
    click.echo()

    try:
        # Run the async backtest
        result = asyncio.run(backtester.backtest(strategy_instance, pnl_config))
    except Exception as e:
        click.echo(f"Error running backtest: {e}", err=True)
        sys.exit(1)

    # Collect cache statistics if cache was used
    if cache is not None:
        cache_stats = cache.stats

    # Display results
    click.echo()
    click.echo("=" * 60)
    click.echo("BACKTEST RESULTS")
    click.echo("=" * 60)
    click.echo(result.summary())

    # Display benchmark comparison
    if benchmark and start and end:
        click.echo()
        click.echo("-" * 60)
        click.echo(f"BENCHMARK COMPARISON ({benchmark.upper()})")
        click.echo("-" * 60)

        try:
            from ..backtesting.pnl.calculators.benchmark import (
                calculate_alpha,
                calculate_beta,
                calculate_information_ratio,
            )
            from ..backtesting.pnl.providers.benchmark import (
                Benchmark,
                get_benchmark_returns,
                get_benchmark_total_return,
            )

            benchmark_enum = Benchmark.from_string(benchmark)

            # Fetch benchmark data (combined into single asyncio.run to avoid event loop issues)
            async def _fetch_benchmark():
                returns = await get_benchmark_returns(benchmark_enum, start, end, interval)
                total = await get_benchmark_total_return(benchmark_enum, start, end)
                return returns, total

            benchmark_returns, benchmark_total = asyncio.run(_fetch_benchmark())

            # Calculate strategy returns from equity curve
            if result.equity_curve and len(result.equity_curve) >= 2:
                strategy_returns = []
                for i in range(1, len(result.equity_curve)):
                    prev_val = result.equity_curve[i - 1].value_usd
                    curr_val = result.equity_curve[i].value_usd
                    if prev_val > 0:
                        strategy_returns.append((curr_val - prev_val) / prev_val)
                    else:
                        strategy_returns.append(Decimal("0"))

                # Align lengths
                min_len = min(len(strategy_returns), len(benchmark_returns))
                if min_len >= 2:
                    strategy_returns = strategy_returns[:min_len]
                    benchmark_returns = benchmark_returns[:min_len]

                    # Calculate metrics
                    info_ratio = calculate_information_ratio(strategy_returns, benchmark_returns)
                    beta_val = calculate_beta(strategy_returns, benchmark_returns)

                    strategy_total = (
                        result.metrics.total_return_pct / Decimal("100")
                        if result.metrics.total_return_pct
                        else Decimal("0")
                    )
                    alpha_val = calculate_alpha(strategy_total, benchmark_total, beta_val)

                    # Display results
                    click.echo(f"Benchmark Return: {float(benchmark_total) * 100:+.2f}%")
                    click.echo(f"Strategy Return:  {float(strategy_total) * 100:+.2f}%")
                    excess = float(strategy_total - benchmark_total) * 100
                    click.echo(f"Excess Return:    {excess:+.2f}%")
                    click.echo()
                    click.echo(f"Information Ratio: {float(info_ratio):.3f}")
                    click.echo(f"Beta:              {float(beta_val):.3f}")
                    click.echo(f"Alpha:             {float(alpha_val) * 100:+.2f}%")
                else:
                    click.echo("Insufficient data for benchmark comparison.")
            else:
                click.echo("No equity curve data for benchmark comparison.")

        except Exception as e:
            click.echo(f"Could not calculate benchmark metrics: {e}")

        click.echo("-" * 60)

    # Display cache statistics if cache warming was enabled
    if cache_stats is not None:
        click.echo()
        click.echo("-" * 60)
        click.echo("CACHE STATISTICS")
        click.echo("-" * 60)
        click.echo(f"Total Entries: {cache_stats.total_entries:,}")
        click.echo(f"Cache Hits: {cache_stats.hits:,}")
        click.echo(f"Cache Misses: {cache_stats.misses:,}")
        click.echo(f"Expired: {cache_stats.expired:,}")
        click.echo(f"Hit Rate: {cache_stats.hit_rate() * 100:.1f}%")
        click.echo("-" * 60)

    if verbose and result.trades:
        click.echo()
        click.echo("-" * 60)
        click.echo("TRADE HISTORY")
        click.echo("-" * 60)

        for i, trade in enumerate(result.trades, 1):
            pnl_sign = "+" if trade.pnl_usd >= 0 else ""
            click.echo(
                f"{i:3}. {trade.timestamp.strftime('%Y-%m-%d %H:%M')}: "
                f"{trade.intent_type.value:10} "
                f"{pnl_sign}${trade.pnl_usd:,.2f} "
                f"(fee: ${trade.fee_usd:,.2f}, gas: ${trade.gas_cost_usd:,.2f})"
            )

        click.echo("-" * 60)

    # Write JSON output if requested
    if output_path:
        click.echo()
        output_data = result.to_dict()
        output_data["_meta"] = {
            "generated_at": datetime.now(UTC).isoformat(),
            "generator": "almanak backtest pnl",
            "engine": "pnl",
            "benchmark": benchmark,
        }

        # Include cache stats in output if available
        if cache_stats is not None:
            output_data["cache_stats"] = cache_stats.to_dict()

        with open(output_path, "w") as f:
            json.dump(output_data, f, indent=2, default=str)

        click.echo(f"Results written to: {output_path}")

    # Generate chart if requested
    if chart:
        click.echo()
        click.echo("Generating equity curve chart...")

        # Determine chart output path
        if output_path:
            # Save chart alongside JSON results with same base name
            chart_extension = ".html" if chart_format.lower() == "html" else ".png"
            chart_path = output_path.with_suffix(chart_extension)
        else:
            # Save in current directory with strategy name
            safe_strategy_name = strategy.replace("/", "_").replace("\\", "_") if strategy else "backtest"
            chart_extension = ".html" if chart_format.lower() == "html" else ".png"
            chart_path = Path(f"equity_curve_{safe_strategy_name}{chart_extension}")

        # Generate the chart
        chart_result = save_chart(
            result=result,
            format=chart_format.lower(),
            path=chart_path,
            show_drawdown=True,
            show_trades=True,
        )

        if chart_result.success:
            click.echo(f"Chart saved to: {chart_result.file_path}")
            if chart_result.drawdown_periods:
                click.echo(f"  Highlighted {len(chart_result.drawdown_periods)} drawdown period(s)")
            if chart_result.trade_markers:
                click.echo(f"  Marked {len(chart_result.trade_markers)} trade(s)")
        else:
            click.echo(f"Warning: Failed to generate chart: {chart_result.error}", err=True)

    # Generate HTML report if requested
    if report:
        from ..backtesting.report_generator import generate_report

        click.echo()
        click.echo("Generating HTML report...")

        # Determine report output path
        if output_path:
            # Save report alongside JSON results with same base name
            report_path = output_path.with_suffix(".html")
        else:
            # Save in current directory with strategy name
            safe_strategy_name = strategy.replace("/", "_").replace("\\", "_") if strategy else "backtest"
            report_path = Path(f"backtest_report_{safe_strategy_name}.html")

        # Generate the report
        report_result = generate_report(result, output_path=report_path)

        if report_result.success:
            click.echo(f"Report saved to: {report_result.file_path}")
        else:
            click.echo(f"Warning: Failed to generate report: {report_result.error}", err=True)

    # Post-backtest tip: mention sweep/optimize for users who may not know about them
    click.echo()
    click.echo("Tip: Try 'almanak backtest sweep' to test multiple parameter combinations,")
    click.echo("     or 'almanak backtest optimize' for Bayesian hyperparameter tuning.")


# =============================================================================
# Parameter Sweep Subcommand (NEW)
# =============================================================================


@dataclass
class SweepParameter:
    """A parameter with multiple values to sweep."""

    name: str
    values: list[str]


@dataclass
class SweepResult:
    """Result from a single parameter combination backtest."""

    params: dict[str, str]
    result: BacktestResult
    sharpe_ratio: Decimal
    total_return_pct: Decimal
    max_drawdown_pct: Decimal
    win_rate: Decimal
    total_trades: int
    period_name: str = ""  # populated when using multi-period sweeps


@dataclass
class AggregatedParamResult:
    """Aggregated results for one parameter set across multiple periods."""

    params: dict[str, str]
    per_period: list[SweepResult]
    avg_sharpe: float
    avg_return_pct: float
    avg_max_dd_pct: float
    avg_trades: float
    cumulative_pnl: float
    sharpe_std: float  # lower = more robust across periods


def parse_param_string(param_str: str) -> SweepParameter:
    """Parse a parameter string in format 'name:val1,val2,val3'.

    Args:
        param_str: Parameter string to parse

    Returns:
        SweepParameter with name and values

    Raises:
        click.BadParameter: If format is invalid
    """
    if ":" not in param_str:
        raise click.BadParameter(f"Invalid parameter format: '{param_str}'. Expected 'name:val1,val2,val3'")

    name, values_str = param_str.split(":", 1)
    name = name.strip()

    if not name:
        raise click.BadParameter("Parameter name cannot be empty")

    values = [v.strip() for v in values_str.split(",") if v.strip()]

    if not values:
        raise click.BadParameter(f"Parameter '{name}' has no values")

    return SweepParameter(name=name, values=values)


def generate_combinations(params: list[SweepParameter]) -> list[dict[str, str]]:
    """Generate all combinations of parameter values (cartesian product).

    Args:
        params: List of parameters with their values

    Returns:
        List of dictionaries, each representing one parameter combination
    """
    if not params:
        return [{}]

    # Use itertools.product for cartesian product
    import itertools

    names = [p.name for p in params]
    value_lists = [p.values for p in params]

    combinations = []
    for values in itertools.product(*value_lists):
        combo = dict(zip(names, values, strict=False))
        combinations.append(combo)

    return combinations


async def run_sweep_backtest(
    strategy_class: Any,
    base_config: dict[str, Any],
    pnl_config: PnLBacktestConfig,
    data_provider: CoinGeckoDataProvider,
    params: dict[str, str],
) -> SweepResult:
    """Run a single backtest with specific parameter values.

    Args:
        strategy_class: Strategy class to instantiate
        base_config: Base strategy configuration
        pnl_config: PnL backtest configuration
        data_provider: Historical data provider
        params: Parameter values for this run

    Returns:
        SweepResult with backtest results and key metrics
    """
    # Create strategy config with overridden parameters
    strategy_config = base_config.copy()
    for name, value in params.items():
        # Try to convert value to appropriate type
        try:
            # Try float first (covers Decimal too)
            strategy_config[name] = float(value)
        except ValueError:
            # Keep as string if not numeric
            strategy_config[name] = value

    # Create strategy instance
    strategy_instance = _create_backtest_strategy(strategy_class, strategy_config, pnl_config.chain)
    # Set params as attributes for strategies that don't read config dict
    if not hasattr(strategy_instance, "config") or not isinstance(getattr(strategy_instance, "config", None), dict):
        for name, value in params.items():
            try:
                setattr(strategy_instance, name, float(value))
            except ValueError:
                setattr(strategy_instance, name, value)

    # Ensure strategy has a non-empty strategy_id (same pattern as PnL backtest)
    existing_id = getattr(strategy_instance, "strategy_id", "")
    if not existing_id:
        param_str = "_".join(f"{k}{v}" for k, v in params.items())
        fallback_id = f"sweep-{param_str}" if param_str else "sweep"
        if hasattr(strategy_instance, "_strategy_id"):
            strategy_instance._strategy_id = fallback_id
        else:
            strategy_instance.strategy_id = fallback_id

    # Create backtester
    backtester = PnLBacktester(
        data_provider=data_provider,
        fee_models={},
        slippage_models={},
    )

    # Run backtest
    result = await backtester.backtest(strategy_instance, pnl_config)

    # Extract key metrics
    metrics = result.metrics

    return SweepResult(
        params=params,
        result=result,
        sharpe_ratio=metrics.sharpe_ratio if metrics.sharpe_ratio else Decimal("0"),
        total_return_pct=metrics.total_return_pct if metrics.total_return_pct else Decimal("0"),
        max_drawdown_pct=metrics.max_drawdown_pct if metrics.max_drawdown_pct else Decimal("0"),
        win_rate=metrics.win_rate if metrics.win_rate else Decimal("0"),
        total_trades=metrics.total_trades,
    )


async def run_parallel_sweeps(
    strategy_class: Any,
    base_config: dict[str, Any],
    pnl_config: PnLBacktestConfig,
    data_provider: CoinGeckoDataProvider,
    combinations: list[dict[str, str]],
    parallel: int,
) -> list[SweepResult]:
    """Run multiple backtests in parallel.

    Args:
        strategy_class: Strategy class to instantiate
        base_config: Base strategy configuration
        pnl_config: PnL backtest configuration
        data_provider: Historical data provider
        combinations: List of parameter combinations to test
        parallel: Number of parallel workers

    Returns:
        List of SweepResult objects
    """
    import asyncio

    results: list[SweepResult] = []
    semaphore = asyncio.Semaphore(parallel)

    async def run_with_semaphore(params: dict[str, str]) -> SweepResult:
        async with semaphore:
            return await run_sweep_backtest(
                strategy_class=strategy_class,
                base_config=base_config,
                pnl_config=pnl_config,
                data_provider=data_provider,
                params=params,
            )

    # Create tasks for all combinations
    tasks = [run_with_semaphore(combo) for combo in combinations]

    # Run with progress indication
    for i, task in enumerate(asyncio.as_completed(tasks)):
        result = await task
        results.append(result)
        # Progress indicator
        click.echo(f"  Completed {i + 1}/{len(combinations)}: {result.params}")

    return results


def print_sweep_results_table(results: list[SweepResult], params: list[SweepParameter]) -> None:
    """Print a comparison table of sweep results sorted by Sharpe ratio.

    Args:
        results: List of SweepResult objects
        params: List of swept parameters (for column headers)
    """
    # Sort by Sharpe ratio (descending)
    sorted_results = sorted(results, key=lambda r: r.sharpe_ratio, reverse=True)

    # Build header
    param_names = [p.name for p in params]
    header_parts = ["Rank"] + param_names + ["Sharpe", "Return%", "MaxDD%", "WinRate", "Trades"]
    header = " | ".join(f"{h:>10}" for h in header_parts)

    click.echo()
    click.echo("=" * len(header))
    click.echo("PARAMETER SWEEP RESULTS (sorted by Sharpe ratio)")
    click.echo("=" * len(header))
    click.echo()
    click.echo(header)
    click.echo("-" * len(header))

    for rank, result in enumerate(sorted_results, 1):
        row_parts = [str(rank)]

        # Add parameter values
        for name in param_names:
            row_parts.append(result.params.get(name, "N/A"))

        # Add metrics
        row_parts.append(f"{result.sharpe_ratio:.3f}")
        row_parts.append(f"{result.total_return_pct:.2f}")
        row_parts.append(f"{result.max_drawdown_pct:.2f}")
        row_parts.append(f"{result.win_rate:.2f}")
        row_parts.append(str(result.total_trades))

        row = " | ".join(f"{v:>10}" for v in row_parts)
        click.echo(row)

    click.echo("-" * len(header))
    click.echo()

    # Show best combination
    if sorted_results:
        best = sorted_results[0]
        click.echo("Best combination:")
        for name, value in best.params.items():
            click.echo(f"  {name}: {value}")
        click.echo(f"  Sharpe ratio: {best.sharpe_ratio:.4f}")
        click.echo(f"  Total return: {best.total_return_pct:.2f}%")


def _aggregate_multi_period_results(
    results: list[SweepResult],
    combinations: list[dict[str, str]],
) -> list[AggregatedParamResult]:
    """Aggregate sweep results across periods for each parameter combination.

    Groups results by parameter combination, computes avg metrics, and
    returns sorted by avg Sharpe ratio (descending).
    """
    import math

    # Group by param combination (use sorted tuple of items as key)
    groups: dict[tuple, list[SweepResult]] = {}
    for r in results:
        key = tuple(sorted(r.params.items()))
        groups.setdefault(key, []).append(r)

    aggregated: list[AggregatedParamResult] = []
    for key, group in groups.items():
        n = len(group)
        sharpes = [float(r.sharpe_ratio) for r in group]
        avg_sharpe = sum(sharpes) / n
        avg_return = sum(float(r.total_return_pct) for r in group) / n
        avg_dd = sum(float(r.max_drawdown_pct) for r in group) / n
        avg_trades = sum(r.total_trades for r in group) / n

        # Cumulative PnL: sum net_pnl_usd from each period's metrics
        cum_pnl = 0.0
        for r in group:
            if r.result and r.result.metrics and r.result.metrics.net_pnl_usd:
                cum_pnl += float(r.result.metrics.net_pnl_usd)

        # Sharpe std dev (lower = more robust)
        if n > 1:
            variance = sum((s - avg_sharpe) ** 2 for s in sharpes) / (n - 1)
            sharpe_std = math.sqrt(variance)
        else:
            sharpe_std = 0.0

        aggregated.append(
            AggregatedParamResult(
                params=dict(key),
                per_period=group,
                avg_sharpe=avg_sharpe,
                avg_return_pct=avg_return,
                avg_max_dd_pct=avg_dd,
                avg_trades=avg_trades,
                cumulative_pnl=cum_pnl,
                sharpe_std=sharpe_std,
            )
        )

    aggregated.sort(key=lambda x: x.avg_sharpe, reverse=True)
    return aggregated


def _print_multi_period_results(
    results: list[SweepResult],
    aggregated: list[AggregatedParamResult],
    params: list[SweepParameter],
) -> None:
    """Print multi-period sweep results: per-period detail + aggregated summary."""
    param_names = [p.name for p in params]

    # Per-period detail table
    click.echo()
    click.echo("=" * 100)
    click.echo("PER-PERIOD DETAIL")
    click.echo("=" * 100)
    header_parts = param_names + ["Period", "Sharpe", "Return%", "MaxDD%", "Trades"]
    header = " | ".join(f"{h:>12}" for h in header_parts)
    click.echo(header)
    click.echo("-" * len(header))

    for r in results:
        row = []
        for name in param_names:
            row.append(r.params.get(name, "N/A"))
        row.append(r.period_name[:12])
        row.append(f"{r.sharpe_ratio:+.3f}")
        row.append(f"{r.total_return_pct:+.2f}")
        row.append(f"{r.max_drawdown_pct:.2f}")
        row.append(str(r.total_trades))
        click.echo(" | ".join(f"{v:>12}" for v in row))

    click.echo()

    # Aggregated summary table
    click.echo("=" * 100)
    click.echo("AGGREGATED RESULTS (sorted by avg Sharpe ratio)")
    click.echo("=" * 100)
    agg_header_parts = (
        ["Rank"] + param_names + ["AvgSharpe", "SharpeStd", "AvgReturn%", "AvgMaxDD%", "AvgTrades", "CumPnL"]
    )
    agg_header = " | ".join(f"{h:>12}" for h in agg_header_parts)
    click.echo(agg_header)
    click.echo("-" * len(agg_header))

    for rank, a in enumerate(aggregated, 1):
        row = [str(rank)]
        for name in param_names:
            row.append(a.params.get(name, "N/A"))
        row.append(f"{a.avg_sharpe:+.3f}")
        row.append(f"{a.sharpe_std:.3f}")
        row.append(f"{a.avg_return_pct:+.2f}")
        row.append(f"{a.avg_max_dd_pct:.2f}")
        row.append(f"{a.avg_trades:.1f}")
        row.append(f"${a.cumulative_pnl:+,.0f}")
        click.echo(" | ".join(f"{v:>12}" for v in row))

    click.echo("-" * len(agg_header))

    # Winner announcement
    if aggregated:
        winner = aggregated[0]
        click.echo()
        click.echo("WINNER (best avg Sharpe across all periods):")
        for name, value in winner.params.items():
            click.echo(f"  {name}: {value}")
        click.echo(f"  Avg Sharpe: {winner.avg_sharpe:+.4f} (std: {winner.sharpe_std:.4f})")
        click.echo(f"  Avg Return: {winner.avg_return_pct:+.2f}%")
        click.echo(f"  Cumulative PnL: ${winner.cumulative_pnl:+,.2f}")
        click.echo()


def _run_parallel_sweep(
    strategy_class: Any,
    base_config: dict[str, Any],
    pnl_config: PnLBacktestConfig,
    combinations: list[dict[str, str]],
    workers: int,
    sweep_params: list[SweepParameter],
) -> list[SweepResult]:
    """Run parameter sweep using true parallel execution (multiprocessing).

    This function uses ProcessPoolExecutor to distribute work across multiple
    CPU cores for better performance on CPU-bound backtest operations.

    Args:
        strategy_class: Strategy class to instantiate
        base_config: Base strategy configuration
        pnl_config: PnL backtest configuration template
        combinations: List of parameter combinations to test
        workers: Number of worker processes
        sweep_params: List of swept parameters

    Returns:
        List of SweepResult objects
    """
    from concurrent.futures import ProcessPoolExecutor, as_completed

    from tqdm import tqdm

    # Create tasks with all necessary data for worker processes
    tasks = [
        _SweepTask(
            strategy_class_name=strategy_class.__module__ + "." + strategy_class.__name__,
            base_config=base_config,
            pnl_config_dict=pnl_config.to_dict(),
            params=combo,
            task_index=i,
        )
        for i, combo in enumerate(combinations)
    ]

    results: list[SweepResult] = []

    # Run with ProcessPoolExecutor
    with ProcessPoolExecutor(max_workers=workers) as executor:
        # Submit all tasks
        future_to_task = {executor.submit(_run_sweep_task_worker, task): task for task in tasks}

        # Process results with progress bar
        with tqdm(total=len(tasks), desc="Parameter sweep (parallel)", unit="backtest", ncols=100) as pbar:
            for future in as_completed(future_to_task):
                task = future_to_task[future]
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    # Handle worker exceptions
                    click.echo(f"  Error in worker for params {task.params}: {e}", err=True)
                    results.append(
                        SweepResult(
                            params=task.params,
                            result=BacktestResult(  # type: ignore[call-arg]
                                strategy_id="error",
                                start_time=pnl_config.start_time,
                                end_time=pnl_config.end_time,
                                trades=[],
                                success=False,
                                error=str(e),
                            ),
                            sharpe_ratio=Decimal("0"),
                            total_return_pct=Decimal("0"),
                            max_drawdown_pct=Decimal("0"),
                            win_rate=Decimal("0"),
                            total_trades=0,
                        )
                    )
                pbar.update(1)

    return results


@dataclass
class _SweepTask:
    """Task specification for parallel sweep worker.

    Contains all data needed to run a single backtest in a worker process.
    Must be picklable for multiprocessing.
    """

    strategy_class_name: str  # Fully qualified class name for import
    base_config: dict[str, Any]
    pnl_config_dict: dict[str, Any]
    params: dict[str, str]
    task_index: int


def _run_sweep_task_worker(task: _SweepTask) -> SweepResult:
    """Worker function to run a single sweep task in a subprocess.

    This function is executed in a separate process via ProcessPoolExecutor.
    It recreates all necessary objects since they can't be pickled directly.

    Args:
        task: SweepTask containing all data needed for the backtest

    Returns:
        SweepResult with backtest results
    """
    import importlib

    # Import strategy class dynamically
    module_name, class_name = task.strategy_class_name.rsplit(".", 1)
    try:
        module = importlib.import_module(module_name)
        strategy_class = getattr(module, class_name)
    except (ImportError, AttributeError):
        # Fallback: try to get from strategies registry
        from ..strategies import get_strategy

        # Extract just the class name for registry lookup
        try:
            strategy_class = get_strategy(class_name.lower().replace("strategy", ""))
        except ValueError:
            # Create a mock if all else fails
            from ..strategies import MarketSnapshot

            class MockWorkerStrategy:
                strategy_id = "mock-worker"

                def __init__(self, config: dict[str, Any]) -> None:
                    self.config = config

                def decide(self, market: MarketSnapshot) -> dict[str, Any] | None:
                    return None

            strategy_class = MockWorkerStrategy

    # Create strategy config with overridden parameters
    strategy_config = task.base_config.copy()
    for name, value in task.params.items():
        try:
            strategy_config[name] = float(value)
        except ValueError:
            strategy_config[name] = value

    # Create strategy instance - resolve chain from config override, then decorator metadata
    from .run import get_default_chain

    worker_chain = (
        task.base_config.get("chain") or task.pnl_config_dict.get("chain") or get_default_chain(strategy_class)
    )
    strategy_instance = _create_backtest_strategy(strategy_class, strategy_config, worker_chain)
    # Set params as attributes for strategies that don't read config dict
    if not hasattr(strategy_instance, "config") or not isinstance(getattr(strategy_instance, "config", None), dict):
        for name, value in task.params.items():
            try:
                setattr(strategy_instance, name, float(value))
            except ValueError:
                setattr(strategy_instance, name, value)

    existing_id = getattr(strategy_instance, "strategy_id", "")
    if not existing_id:
        param_str = "_".join(f"{k}{v}" for k, v in task.params.items())
        fallback_id = f"sweep-{param_str}" if param_str else "sweep"
        if hasattr(strategy_instance, "_strategy_id"):
            strategy_instance._strategy_id = fallback_id
        else:
            strategy_instance.strategy_id = fallback_id

    # Recreate PnL config (remove computed properties first)
    pnl_config_dict = task.pnl_config_dict.copy()
    for key in ["duration_seconds", "duration_days", "estimated_ticks"]:
        pnl_config_dict.pop(key, None)
    pnl_config = PnLBacktestConfig.from_dict(pnl_config_dict)

    # Create data provider and backtester
    data_provider = CoinGeckoDataProvider()
    backtester = PnLBacktester(
        data_provider=data_provider,
        fee_models={},
        slippage_models={},
    )

    # Run backtest
    result = asyncio.run(backtester.backtest(strategy_instance, pnl_config))

    # Extract metrics
    metrics = result.metrics

    return SweepResult(
        params=task.params,
        result=result,
        sharpe_ratio=metrics.sharpe_ratio if metrics.sharpe_ratio else Decimal("0"),
        total_return_pct=metrics.total_return_pct if metrics.total_return_pct else Decimal("0"),
        max_drawdown_pct=metrics.max_drawdown_pct if metrics.max_drawdown_pct else Decimal("0"),
        win_rate=metrics.win_rate if metrics.win_rate else Decimal("0"),
        total_trades=metrics.total_trades,
    )


@backtest.command("sweep")
@click.option(
    "--strategy",
    "-s",
    required=True,
    help="Name of the strategy to backtest",
)
@click.option(
    "--start",
    required=False,
    default=None,
    callback=parse_date,
    help="Start date (YYYY-MM-DD). Not required when using --periods.",
)
@click.option(
    "--end",
    required=False,
    default=None,
    callback=parse_date,
    help="End date (YYYY-MM-DD). Not required when using --periods.",
)
@click.option(
    "--periods",
    type=str,
    default=None,
    help=(
        "Named period preset or path to JSON file for multi-period evaluation. "
        "Presets: 2024-quarterly, 2024-monthly, 2025-quarterly, rolling-6m. "
        "Replaces --start/--end. Each param combination is tested across all periods."
    ),
)
@click.option(
    "--param",
    "-p",
    "params",
    multiple=True,
    help="Parameter to sweep (format: 'name:val1,val2,val3'). Can be used multiple times.",
)
@click.option(
    "--parallel",
    is_flag=True,
    default=False,
    help="Enable true parallel execution using multiple CPU cores (multiprocessing)",
)
@click.option(
    "--workers",
    "-j",
    type=int,
    default=None,
    help="Number of parallel workers. Defaults to CPU count - 1. With --parallel uses processes, otherwise uses async concurrency.",
)
@click.option(
    "--interval",
    type=int,
    default=3600,
    help="Interval between ticks in seconds (default: 3600 = 1 hour)",
)
@click.option(
    "--initial-capital",
    type=float,
    default=10000.0,
    help="Initial portfolio balance in USD (default: 10000)",
)
@click.option(
    "--chain",
    "-c",
    type=str,
    default="arbitrum",
    help="Target blockchain (default: arbitrum)",
)
@click.option(
    "--tokens",
    type=str,
    default="WETH,USDC",
    help="Comma-separated list of tokens to track (default: WETH,USDC)",
)
@click.option(
    "--output",
    "-o",
    type=click.Path(exists=False),
    default=None,
    help="Output file for full JSON results (optional)",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Show parameter combinations without running backtests",
)
@click.option(
    "--report",
    is_flag=True,
    default=False,
    help="Generate HTML report for the best parameter combination",
)
def sweep_backtest(
    strategy: str,
    start: datetime | None,
    end: datetime | None,
    periods: str | None,
    params: tuple[str, ...],
    parallel: bool,
    workers: int | None,
    interval: int,
    initial_capital: float,
    chain: str,
    tokens: str,
    output: str | None,
    dry_run: bool,
    report: bool,
) -> None:
    """
    Run parameter sweep across multiple parameter combinations.

    This command runs backtests for all combinations of the specified
    parameter values and outputs a comparison table sorted by Sharpe ratio.

    The --param flag accepts values in the format 'name:val1,val2,val3'.
    Use it multiple times to sweep multiple parameters.

    Execution modes:

        - Without --parallel: Uses async concurrency (single process, lighter weight)

        - With --parallel: Uses multiprocessing (multiple processes, better for CPU-bound)

    Examples:

        # Sweep a single parameter (async mode)
        almanak backtest sweep -s mean_reversion \\
            --start 2024-01-01 --end 2024-06-01 \\
            --param "threshold:0.01,0.02,0.03"

        # Sweep multiple parameters
        almanak backtest sweep -s momentum \\
            --start 2024-01-01 --end 2024-03-01 \\
            --param "window:10,20,30" \\
            --param "threshold:0.5,1.0,1.5"

        # Run with true parallel execution using 8 workers
        almanak backtest sweep -s grid_trader \\
            --start 2024-01-01 --end 2024-06-01 \\
            --param "grid_size:5,10,15" \\
            --param "spread:0.001,0.002,0.003" \\
            --parallel --workers 8

        # Parallel with default workers (CPU count - 1)
        almanak backtest sweep -s test_strategy \\
            --start 2024-01-01 --end 2024-02-01 \\
            --param "a:1,2,3" --parallel

        # Dry run to see combinations
        almanak backtest sweep -s test_strategy \\
            --start 2024-01-01 --end 2024-02-01 \\
            --param "a:1,2,3" --param "b:x,y" --dry-run

    \b
        # Multi-period sweep (test robustness across quarters)
        almanak backtest sweep -s momentum \\
            --periods "2024-quarterly" \\
            --param "window:10,20,30"

    \b
        # Multi-period sweep (monthly windows)
        almanak backtest sweep -s momentum \\
            --periods "2024-monthly" \\
            --param "threshold:0.01,0.02,0.03"
    """
    # Parse parameters
    if not params:
        raise click.UsageError("At least one --param is required. Use format: --param 'name:val1,val2,val3'")

    sweep_params: list[SweepParameter] = []
    for param_str in params:
        try:
            sweep_params.append(parse_param_string(param_str))
        except click.BadParameter as e:
            raise click.UsageError(str(e)) from e

    # Validate --start/--end vs --periods
    from ..backtesting.pnl.periods import BacktestPeriod, resolve_periods

    multi_period_mode = False
    backtest_periods: list[BacktestPeriod] = []

    if periods is not None:
        if start is not None or end is not None:
            raise click.UsageError("Cannot use --periods together with --start/--end. Use one or the other.")
        try:
            backtest_periods = resolve_periods(periods)
        except (ValueError, json.JSONDecodeError) as e:
            raise click.UsageError(str(e)) from e
        multi_period_mode = True
    else:
        if start is None or end is None:
            raise click.UsageError("Either --start and --end, or --periods is required.")
        backtest_periods = [BacktestPeriod(name="single", start=start, end=end)]

    # Validate strategy exists
    available_strategies = list_strategies_fn()
    if strategy not in available_strategies and available_strategies:
        click.echo(f"Error: Unknown strategy '{strategy}'", err=True)
        click.echo(f"Available strategies: {', '.join(sorted(available_strategies))}", err=True)
        raise click.Abort()

    # Generate all combinations
    combinations = generate_combinations(sweep_params)
    total_combinations = len(combinations)

    # Parse tokens list
    token_list = [t.strip().upper() for t in tokens.split(",")]

    # Display configuration
    click.echo("=" * 60)
    click.echo("PARAMETER SWEEP CONFIGURATION")
    click.echo("=" * 60)
    click.echo(f"Strategy: {strategy}")
    click.echo(f"Chain: {chain}")
    if multi_period_mode:
        click.echo(f"Periods: {periods} ({len(backtest_periods)} windows)")
        for bp in backtest_periods:
            click.echo(f"  - {bp.name}: {bp.start.date()} -> {bp.end.date()}")
    else:
        click.echo(f"Period: {backtest_periods[0].start.date()} -> {backtest_periods[0].end.date()}")
    click.echo(f"Interval: {interval}s ({interval / 3600:.1f} hours)")
    click.echo(f"Initial Capital: ${initial_capital:,.2f}")
    click.echo(f"Tokens: {', '.join(token_list)}")
    click.echo()
    click.echo("Parameters to sweep:")
    for p in sweep_params:
        click.echo(f"  {p.name}: {', '.join(p.values)}")
    click.echo()
    total_runs = total_combinations * len(backtest_periods)
    click.echo(f"Total combinations: {total_combinations}")
    if multi_period_mode:
        click.echo(f"Total runs: {total_runs} ({total_combinations} combinations x {len(backtest_periods)} periods)")

    # Determine execution mode and worker count
    if parallel:
        import os

        effective_workers = workers if workers is not None else max(1, (os.cpu_count() or 1) - 1)
        effective_workers = min(effective_workers, total_runs)
        click.echo("Execution mode: Parallel (multiprocessing)")
        click.echo(f"Workers: {effective_workers}")
    else:
        effective_workers = workers if workers is not None else 4
        click.echo("Execution mode: Async (concurrent)")
        click.echo(f"Concurrency: {effective_workers}")

    if output:
        click.echo(f"Output: {output}")

    click.echo("=" * 60)

    # Handle dry run
    if dry_run:
        click.echo()
        if multi_period_mode:
            click.echo(f"Parameter combinations x periods (dry run, {total_runs} total):")
        else:
            click.echo("Parameter combinations (dry run):")
        click.echo("-" * 40)
        for i, combo in enumerate(combinations, 1):
            params_str = ", ".join(f"{k}={v}" for k, v in combo.items())
            if multi_period_mode:
                for bp in backtest_periods:
                    click.echo(f"  {params_str}  |  {bp.name}")
            else:
                click.echo(f"  {i}. {params_str}")
        click.echo("-" * 40)
        click.echo()
        click.echo("Dry run - no backtests executed.")
        return

    # Load strategy
    try:
        strategy_class = get_strategy(strategy)
    except ValueError:
        click.echo()
        click.echo("Warning: No strategies registered in factory.", err=True)
        click.echo("Running with mock strategy for demonstration.", err=True)
        click.echo()

        from ..strategies import MarketSnapshot

        class MockSweepStrategy:
            """Mock strategy for sweep demonstration."""

            strategy_id: str = "mock-sweep"

            def __init__(self, config: dict[str, Any]) -> None:
                self.config = config

            def decide(self, market: MarketSnapshot) -> dict[str, Any] | None:
                return None

        strategy_class = MockSweepStrategy

    # Load base strategy config
    base_config = load_strategy_config(strategy, chain)

    # Create data provider
    click.echo()
    click.echo("Initializing CoinGecko data provider...")
    data_provider = CoinGeckoDataProvider()

    # Run sweep (single-period or multi-period)
    all_results: list[SweepResult] = []

    if multi_period_mode:
        click.echo(f"Starting multi-period sweep ({total_runs} total runs)...")
    else:
        click.echo(f"Starting parameter sweep ({total_combinations} combinations)...")
    click.echo()

    try:
        for bp in backtest_periods:
            pnl_config = PnLBacktestConfig(
                start_time=bp.start,
                end_time=bp.end,
                interval_seconds=interval,
                initial_capital_usd=Decimal(str(initial_capital)),
                chain=chain,
                tokens=token_list,
                gas_price_gwei=Decimal("30"),
                include_gas_costs=True,
                allow_degraded_data=True,
                # Skip preflight for multi-combo sweeps (N combinations * preflight = slow);
                # keep it enabled for single-combo runs where cost is negligible
                preflight_validation=total_combinations <= 1,
                fail_on_preflight_error=False,
            )

            if multi_period_mode:
                click.echo(f"--- Period: {bp.name} ({bp.start.date()} -> {bp.end.date()}) ---")

            if parallel:
                period_results = _run_parallel_sweep(
                    strategy_class=strategy_class,
                    base_config=base_config,
                    pnl_config=pnl_config,
                    combinations=combinations,
                    workers=effective_workers,
                    sweep_params=sweep_params,
                )
            else:
                period_results = asyncio.run(
                    run_parallel_sweeps(
                        strategy_class=strategy_class,
                        base_config=base_config,
                        pnl_config=pnl_config,
                        data_provider=data_provider,
                        combinations=combinations,
                        parallel=effective_workers,
                    )
                )

            # Tag results with period name
            for r in period_results:
                r.period_name = bp.name
            all_results.extend(period_results)

    except Exception as e:
        click.echo(f"Error during sweep: {e}", err=True)
        sys.exit(1)

    # Display results
    if multi_period_mode and len(backtest_periods) > 1:
        aggregated = _aggregate_multi_period_results(all_results, combinations)
        _print_multi_period_results(all_results, aggregated, sweep_params)
    else:
        print_sweep_results_table(all_results, sweep_params)

    # Write JSON output if requested
    if output:
        output_path = Path(output)
        output_data: dict[str, Any] = {
            "sweep_config": {
                "strategy": strategy,
                "periods": [
                    {"name": bp.name, "start": bp.start.isoformat(), "end": bp.end.isoformat()}
                    for bp in backtest_periods
                ],
                "interval_seconds": interval,
                "initial_capital_usd": str(initial_capital),
                "chain": chain,
                "tokens": token_list,
                "parameters": [{"name": p.name, "values": p.values} for p in sweep_params],
                "total_combinations": total_combinations,
                "multi_period": multi_period_mode,
            },
            "results": [
                {
                    "params": r.params,
                    "period": r.period_name,
                    "sharpe_ratio": str(r.sharpe_ratio),
                    "total_return_pct": str(r.total_return_pct),
                    "max_drawdown_pct": str(r.max_drawdown_pct),
                    "win_rate": str(r.win_rate),
                    "total_trades": r.total_trades,
                }
                for r in all_results
            ],
            "_meta": {
                "generated_at": datetime.now(UTC).isoformat(),
                "generator": "almanak backtest sweep",
                "engine": "pnl",
            },
        }

        # Add best_params for easy consumption by downstream tools
        # Secondary sort on params tuple ensures deterministic output on ties
        if all_results:
            if multi_period_mode and len(backtest_periods) > 1:
                agg = _aggregate_multi_period_results(all_results, combinations)
                if agg:
                    best_agg = sorted(agg, key=lambda x: (x.avg_sharpe, sorted(x.params.items())), reverse=True)[0]
                    output_data["best_params"] = best_agg.params
            else:
                best_single = max(all_results, key=lambda x: (x.sharpe_ratio, sorted(x.params.items())))
                output_data["best_params"] = best_single.params

        if multi_period_mode and len(backtest_periods) > 1:
            aggregated = _aggregate_multi_period_results(all_results, combinations)
            output_data["aggregated"] = [
                {
                    "params": a.params,
                    "avg_sharpe": a.avg_sharpe,
                    "avg_return_pct": a.avg_return_pct,
                    "avg_max_dd_pct": a.avg_max_dd_pct,
                    "avg_trades": a.avg_trades,
                    "cumulative_pnl": a.cumulative_pnl,
                    "sharpe_std": a.sharpe_std,
                }
                for a in sorted(aggregated, key=lambda x: x.avg_sharpe, reverse=True)
            ]

        with open(output_path, "w") as f:
            json.dump(output_data, f, indent=2)

        click.echo(f"Results written to: {output_path}")

    # Generate HTML report for best result if requested
    if report and all_results:
        from ..backtesting.report_generator import generate_report

        click.echo()
        click.echo("Generating HTML report for best parameter combination...")

        # Get best result: in multi-period mode, align with aggregated winner
        if multi_period_mode and len(backtest_periods) > 1:
            aggregated = _aggregate_multi_period_results(all_results, combinations)
            winner_params = aggregated[0].params if aggregated else all_results[0].params
            candidate_results = [r for r in all_results if r.params == winner_params]
            best_result = max(candidate_results, key=lambda x: x.sharpe_ratio)
        else:
            best_result = max(all_results, key=lambda x: x.sharpe_ratio)

        # Determine report output path
        if output:
            report_path = Path(output).with_suffix(".html")
        else:
            safe_strategy_name = strategy.replace("/", "_").replace("\\", "_")
            report_path = Path(f"backtest_report_{safe_strategy_name}_sweep.html")

        # Generate the report
        report_result = generate_report(best_result.result, output_path=report_path)

        if report_result.success:
            click.echo(f"Report saved to: {report_result.file_path}")
            click.echo(f"  Best params: {best_result.params}")
        else:
            click.echo(f"Warning: Failed to generate report: {report_result.error}", err=True)


# =============================================================================
# Bayesian Optimization Subcommand (NEW)
# =============================================================================


def load_optimization_config(config_path: Path) -> dict[str, Any]:
    """Load optimization configuration from JSON file.

    The config file should have the following structure:
    {
        "param_ranges": {
            "param_name": {
                "type": "continuous|discrete|categorical",
                "min": <value>,  // for continuous/discrete
                "max": <value>,  // for continuous/discrete
                "step": <value>, // optional, for discrete
                "log": true,     // optional, for continuous (log-uniform)
                "choices": [...]  // for categorical
            },
            ...
        },
        "objective": "sharpe_ratio",  // optional, default sharpe_ratio
        "n_trials": 50,               // optional, default 50
        "patience": 10,               // optional, for early stopping
        "min_delta": 0.0              // optional, minimum improvement
    }

    Args:
        config_path: Path to the JSON config file

    Returns:
        Dictionary with param_ranges and optional settings
    """
    with open(config_path) as f:
        config: dict[str, Any] = json.load(f)

    if "param_ranges" not in config:
        raise click.BadParameter(f"Config file must contain 'param_ranges' key. Got: {list(config.keys())}")

    return config


def parse_param_ranges_from_config(
    config: dict[str, Any],
) -> dict[str, Any]:
    """Parse parameter ranges from config dict to OptunaTuner format.

    Converts the JSON config format to the OptunaParamRanges format expected
    by OptunaTuner.

    Args:
        config: Config dictionary with param_ranges

    Returns:
        Dictionary mapping param names to ParamRange objects or legacy tuples
    """
    from ..backtesting.pnl.optuna_tuner import (
        categorical,
        continuous,
        discrete,
    )

    param_ranges: dict[str, Any] = {}

    for name, spec in config.get("param_ranges", {}).items():
        if isinstance(spec, dict):
            param_type = spec.get("type", "continuous").lower()

            if param_type == "categorical":
                choices = spec.get("choices", [])
                if not choices:
                    raise click.BadParameter(f"Categorical parameter '{name}' requires 'choices' list")
                param_ranges[name] = categorical(choices)

            elif param_type == "discrete":
                min_val = spec.get("min")
                max_val = spec.get("max")
                step = spec.get("step")
                if min_val is None or max_val is None:
                    raise click.BadParameter(f"Discrete parameter '{name}' requires 'min' and 'max'")
                param_ranges[name] = discrete(int(min_val), int(max_val), step=step)

            elif param_type == "continuous":
                min_val = spec.get("min")
                max_val = spec.get("max")
                step = spec.get("step")
                log = spec.get("log", False)
                if min_val is None or max_val is None:
                    raise click.BadParameter(f"Continuous parameter '{name}' requires 'min' and 'max'")
                # Convert to Decimal for financial parameters
                if isinstance(min_val, str) or isinstance(max_val, str):
                    min_val = Decimal(str(min_val))
                    max_val = Decimal(str(max_val))
                param_ranges[name] = continuous(min_val, max_val, step=step, log=log)

            else:
                raise click.BadParameter(
                    f"Unknown parameter type '{param_type}' for '{name}'. Use: continuous, discrete, or categorical"
                )
        elif isinstance(spec, list):
            # Legacy format: list means categorical
            param_ranges[name] = spec
        elif isinstance(spec, tuple):
            # Legacy format: tuple means range
            param_ranges[name] = spec
        else:
            raise click.BadParameter(f"Invalid parameter spec for '{name}': {spec}")

    return param_ranges


def print_optimization_results(
    result: Any,
    objective: str,
) -> None:
    """Print optimization results in a formatted way.

    Args:
        result: OptimizationResult from OptunaTuner
        objective: Name of the objective metric
    """
    click.echo()
    click.echo("=" * 60)
    click.echo("OPTIMIZATION RESULTS")
    click.echo("=" * 60)
    click.echo()
    click.echo(f"Objective: {objective}")
    click.echo(f"Direction: {result.direction}")
    click.echo(f"Total Trials: {result.n_trials}")
    click.echo(f"Best Trial: #{result.best_trial_number}")
    click.echo()

    if result.stopped_early:
        click.echo(f"Early Stopping: Yes (patience exhausted after {result.trials_without_improvement} trials)")
    else:
        click.echo("Early Stopping: No (completed all trials)")

    click.echo()
    click.echo("-" * 60)
    click.echo("BEST PARAMETERS")
    click.echo("-" * 60)
    for name, value in result.best_params.items():
        if isinstance(value, Decimal):
            click.echo(f"  {name}: {value}")
        elif isinstance(value, float):
            click.echo(f"  {name}: {value:.6f}")
        else:
            click.echo(f"  {name}: {value}")

    click.echo("-" * 60)
    click.echo()
    click.echo(f"Best {objective}: {result.best_value:.6f}")
    click.echo()
    click.echo("=" * 60)


@backtest.command("optimize")
@click.option(
    "--strategy",
    "-s",
    required=True,
    help="Name of the strategy to optimize",
)
@click.option(
    "--start",
    required=False,
    default=None,
    callback=parse_date,
    help="Start date (YYYY-MM-DD). Not required when using --periods.",
)
@click.option(
    "--end",
    required=False,
    default=None,
    callback=parse_date,
    help="End date (YYYY-MM-DD). Not required when using --periods.",
)
@click.option(
    "--periods",
    type=str,
    default=None,
    help=(
        "Named period preset or JSON file for multi-period evaluation. "
        "Each trial is scored on the average metric across all periods. "
        "Presets: 2024-quarterly, 2024-monthly, rolling-6m."
    ),
)
@click.option(
    "--config-file",
    "-f",
    type=click.Path(exists=True),
    required=True,
    help="Path to optimization config JSON file with parameter ranges",
)
@click.option(
    "--objective",
    type=click.Choice(
        [
            "sharpe_ratio",
            "sortino_ratio",
            "calmar_ratio",
            "total_return_pct",
            "annualized_return_pct",
            "max_drawdown_pct",
            "profit_factor",
            "win_rate",
            "net_pnl_usd",
        ]
    ),
    default=None,
    help="Metric to optimize (default: from config or sharpe_ratio)",
)
@click.option(
    "--n-trials",
    "-n",
    type=int,
    default=None,
    help="Number of optimization trials (default: from config or 50)",
)
@click.option(
    "--patience",
    type=int,
    default=None,
    help="Early stopping patience - trials without improvement (default: from config)",
)
@click.option(
    "--interval",
    type=int,
    default=3600,
    help="Interval between ticks in seconds (default: 3600 = 1 hour)",
)
@click.option(
    "--initial-capital",
    type=float,
    default=10000.0,
    help="Initial portfolio balance in USD (default: 10000)",
)
@click.option(
    "--chain",
    "-c",
    type=str,
    default="arbitrum",
    help="Target blockchain (default: arbitrum)",
)
@click.option(
    "--tokens",
    type=str,
    default="WETH,USDC",
    help="Comma-separated list of tokens to track (default: WETH,USDC)",
)
@click.option(
    "--output",
    "-o",
    type=click.Path(exists=False),
    default=None,
    help="Output file for optimization history JSON (optional)",
)
@click.option(
    "--seed",
    type=int,
    default=None,
    help="Random seed for reproducibility",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Show configuration without running optimization",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    default=False,
    help="Show progress bar and detailed logging",
)
def optimize_backtest(
    strategy: str,
    start: datetime | None,
    end: datetime | None,
    periods: str | None,
    config_file: str,
    objective: str | None,
    n_trials: int | None,
    patience: int | None,
    interval: int,
    initial_capital: float,
    chain: str,
    tokens: str,
    output: str | None,
    seed: int | None,
    dry_run: bool,
    verbose: bool,
) -> None:
    """
    Run Bayesian optimization to find optimal strategy parameters.

    This command uses Optuna's Tree-structured Parzen Estimator (TPE) sampler
    to efficiently explore the parameter space and find configurations that
    maximize (or minimize) the specified objective metric.

    The optimization config file (--config-file) must contain parameter ranges.
    These are typically your strategy's own parameters (the ones your decide()
    method uses), but PnLBacktestConfig fields are also supported:

    \b
    {
        "param_ranges": {
            "rsi_oversold": {"type": "discrete", "min": 20, "max": 40, "step": 5},
            "rsi_overbought": {"type": "discrete", "min": 60, "max": 80, "step": 5},
            "trade_size_usd": {"type": "continuous", "min": 100, "max": 5000},
            "mode": {"type": "categorical", "choices": ["aggressive", "conservative"]}
        },
        "objective": "sharpe_ratio",
        "n_trials": 50,
        "patience": 10
    }

    Strategy param names are merged into the strategy config dict.
    PnLBacktestConfig field names are applied to the backtest config.
    The optimizer automatically routes each key to the right place.

    Parameter types:
    \b
        - continuous: Float range with optional log scale and step
        - discrete: Integer range with optional step
        - categorical: List of choices (strings, ints, or floats)

    Supported objectives:
    \b
        - sharpe_ratio (default, maximize)
        - sortino_ratio (maximize)
        - calmar_ratio (maximize)
        - total_return_pct (maximize)
        - annualized_return_pct (maximize)
        - max_drawdown_pct (minimize)
        - profit_factor (maximize)
        - win_rate (maximize)
        - net_pnl_usd (maximize)

    Examples:

    \b
        # Optimize RSI strategy parameters
        almanak backtest optimize -s uniswap_rsi \\
            --start 2024-01-01 --end 2024-06-01 \\
            --config-file optimize_config.json

    \b
        # With custom objective and more trials
        almanak backtest optimize -s mean_reversion \\
            --start 2024-01-01 --end 2024-03-01 \\
            --config-file config.json \\
            --objective sortino_ratio \\
            --n-trials 100 --patience 20 \\
            --output results.json

    \b
        # Dry run to verify config is parsed correctly
        almanak backtest optimize -s test_strategy \\
            --start 2024-01-01 --end 2024-02-01 \\
            --config-file config.json --dry-run

    \b
        # Multi-period optimization (avg metric across quarters)
        almanak backtest optimize -s momentum \\
            --periods "2024-quarterly" \\
            --config-file config.json --n-trials 100
    """
    from ..backtesting.pnl.optuna_tuner import OptunaTuner
    from ..backtesting.pnl.periods import BacktestPeriod, resolve_periods

    # Validate --start/--end vs --periods
    if periods is not None:
        if start is not None or end is not None:
            raise click.UsageError("Cannot use --periods together with --start/--end. Use one or the other.")
        try:
            backtest_periods = resolve_periods(periods)
        except (ValueError, json.JSONDecodeError) as e:
            raise click.UsageError(str(e)) from e
    else:
        if start is None or end is None:
            raise click.UsageError("Either --start and --end, or --periods is required.")
        backtest_periods = [BacktestPeriod(name="single", start=start, end=end)]

    # Load optimization config
    config_path = Path(config_file)
    try:
        opt_config = load_optimization_config(config_path)
    except Exception as e:
        click.echo(f"Error loading config file: {e}", err=True)
        raise click.Abort() from None

    # Parse parameter ranges
    try:
        param_ranges = parse_param_ranges_from_config(opt_config)
    except click.BadParameter as e:
        click.echo(f"Error parsing config: {e}", err=True)
        raise click.Abort() from None

    if not param_ranges:
        raise click.UsageError(
            "No parameter ranges defined in config file. Add 'param_ranges' with at least one parameter."
        )

    # Determine settings (CLI args override config file)
    effective_objective = objective or opt_config.get("objective", "sharpe_ratio")
    effective_n_trials = n_trials or opt_config.get("n_trials", 50)
    effective_patience = patience or opt_config.get("patience")
    min_delta = opt_config.get("min_delta", 0.0)

    # Validate strategy exists
    available_strategies = list_strategies_fn()
    if strategy not in available_strategies and available_strategies:
        click.echo(f"Error: Unknown strategy '{strategy}'", err=True)
        click.echo(f"Available strategies: {', '.join(sorted(available_strategies))}", err=True)
        raise click.Abort()

    # Parse tokens list
    token_list = [t.strip().upper() for t in tokens.split(",")]

    # Display configuration
    click.echo("=" * 60)
    click.echo("BAYESIAN OPTIMIZATION CONFIGURATION")
    click.echo("=" * 60)
    click.echo(f"Strategy: {strategy}")
    click.echo(f"Chain: {chain}")
    if len(backtest_periods) > 1:
        click.echo(f"Periods: {periods} ({len(backtest_periods)} windows)")
        for bp in backtest_periods:
            click.echo(f"  - {bp.name}: {bp.start.date()} -> {bp.end.date()}")
        click.echo("  (each trial scored on avg metric across all periods)")
    else:
        bp = backtest_periods[0]
        click.echo(f"Period: {bp.start.date()} -> {bp.end.date()}")
    click.echo(f"Interval: {interval}s ({interval / 3600:.1f} hours)")
    click.echo(f"Initial Capital: ${initial_capital:,.2f}")
    click.echo(f"Tokens: {', '.join(token_list)}")
    click.echo()
    click.echo(f"Objective: {effective_objective}")
    click.echo(f"Trials: {effective_n_trials}")
    if effective_patience:
        click.echo(f"Early Stopping: patience={effective_patience}, min_delta={min_delta}")
    else:
        click.echo("Early Stopping: disabled")
    if seed:
        click.echo(f"Random Seed: {seed}")
    click.echo()
    click.echo("Parameters to optimize:")
    for name, spec in param_ranges.items():
        if hasattr(spec, "param_type"):
            # ParamRange object
            if spec.param_type.value == "categorical":
                click.echo(f"  {name}: categorical {spec.choices}")
            elif spec.param_type.value == "discrete":
                step_str = f", step={spec.step}" if spec.step else ""
                click.echo(f"  {name}: discrete [{spec.low}, {spec.high}{step_str}]")
            else:
                log_str = " (log)" if spec.log else ""
                step_str = f", step={spec.step}" if spec.step else ""
                click.echo(f"  {name}: continuous [{spec.low}, {spec.high}{step_str}]{log_str}")
        else:
            click.echo(f"  {name}: {spec}")

    if output:
        click.echo(f"Output: {output}")

    click.echo("=" * 60)

    # Handle dry run
    if dry_run:
        click.echo()
        click.echo("Dry run - optimization not executed.")
        return

    # Load strategy
    try:
        strategy_class = get_strategy(strategy)
    except ValueError:
        click.echo()
        click.echo("Warning: No strategies registered in factory.", err=True)
        click.echo("Running with mock strategy for demonstration.", err=True)
        click.echo()

        from ..strategies import MarketSnapshot

        class MockOptimizeStrategy:
            """Mock strategy for optimization demonstration."""

            strategy_id: str = "mock-optimize"

            def __init__(self, config: dict[str, Any]) -> None:
                self.config = config

            def decide(self, market: MarketSnapshot) -> dict[str, Any] | None:
                return None

        strategy_class = MockOptimizeStrategy

    # Load base strategy config
    base_config = load_strategy_config(strategy, chain)

    # Create PnL backtest configs (one per period)
    pnl_configs: list[PnLBacktestConfig] = []
    for bp in backtest_periods:
        pnl_configs.append(
            PnLBacktestConfig(
                start_time=bp.start,
                end_time=bp.end,
                interval_seconds=interval,
                initial_capital_usd=Decimal(str(initial_capital)),
                chain=chain,
                tokens=token_list,
                gas_price_gwei=Decimal("30"),
                include_gas_costs=True,
                allow_degraded_data=True,
                # Preflight is expensive per trial; run once on first config only
                preflight_validation=(len(pnl_configs) == 0),
                fail_on_preflight_error=False,
            )
        )
    # Use first config as base for OptunaTuner
    pnl_config = pnl_configs[0]

    # Create data provider factory
    def create_data_provider() -> CoinGeckoDataProvider:
        return CoinGeckoDataProvider()

    # Create strategy factory that accepts optional config overrides
    def create_strategy(config_overrides: dict[str, Any] | None = None) -> Any:
        effective_config = {**base_config, **(config_overrides or {})}
        return _create_backtest_strategy(strategy_class, effective_config, chain)

    # Create backtester factory
    def create_backtester(
        data_provider: Any,
        fee_models: dict[str, Any],
        slippage_models: dict[str, Any],
    ) -> PnLBacktester:
        return PnLBacktester(
            data_provider=data_provider,
            fee_models=fee_models,
            slippage_models=slippage_models,
        )

    # Create OptunaTuner
    click.echo()
    click.echo("Initializing Optuna optimizer...")
    tuner = OptunaTuner(
        objective_metric=effective_objective,
        sampler_seed=seed,
        patience=effective_patience,
        min_delta=min_delta,
        log_level="INFO" if verbose else "WARNING",
    )

    # Run optimization
    multi_period = len(pnl_configs) > 1
    if multi_period:
        click.echo(
            f"Starting multi-period Bayesian optimization ({effective_n_trials} trials x {len(pnl_configs)} periods)..."
        )
    else:
        click.echo(f"Starting Bayesian optimization ({effective_n_trials} trials)...")
    click.echo()

    try:
        result = asyncio.run(
            tuner.optimize(
                strategy_factory=create_strategy,
                data_provider_factory=create_data_provider,
                backtester_factory=create_backtester,
                base_config=pnl_config,
                param_ranges=param_ranges,
                n_trials=effective_n_trials,
                show_progress=verbose,
                patience=effective_patience,
                min_delta=min_delta,
                extra_configs=pnl_configs[1:] if multi_period else None,
                strategy_config=base_config,
            )
        )
    except Exception as e:
        click.echo(f"Error during optimization: {e}", err=True)
        sys.exit(1)

    # Display results
    print_optimization_results(result, effective_objective)

    # Write output if requested
    if output:
        output_path = Path(output)
        try:
            history = tuner.export_history()
            history.save(output_path)
            click.echo(f"Optimization history written to: {output_path}")
        except Exception as e:
            click.echo(f"Warning: Could not save history: {e}", err=True)


# =============================================================================
# Walk-Forward Optimization Subcommand
# =============================================================================


def print_walk_forward_results(
    result: Any,
    objective: str,
) -> None:
    """Print walk-forward optimization results in a formatted way.

    Args:
        result: WalkForwardResult from run_walk_forward_optimization
        objective: Name of the objective metric
    """
    click.echo()
    click.echo("=" * 70)
    click.echo("WALK-FORWARD OPTIMIZATION RESULTS")
    click.echo("=" * 70)
    click.echo()
    click.echo(f"Objective Metric: {objective}")
    click.echo(f"Total Windows: {result.total_windows}")
    click.echo(f"Successful Windows: {result.successful_windows}")
    click.echo()

    # Aggregate performance
    click.echo("-" * 70)
    click.echo("AGGREGATE PERFORMANCE")
    click.echo("-" * 70)
    click.echo(f"  Avg Train {objective}: {result.avg_train_objective:.4f}")
    click.echo(f"  Avg Test {objective}:  {result.avg_test_objective:.4f}")
    click.echo(f"  Avg Overfitting Ratio: {result.avg_overfitting_ratio:.4f}")
    click.echo(f"  Avg Generalization Score: {result.avg_generalization_score:.4f}")
    click.echo()
    click.echo(f"  Combined Test PnL: ${result.combined_test_pnl_usd:,.2f}")
    click.echo(f"  Combined Test Return: {result.combined_test_return_pct:.2f}%")
    click.echo()

    # Overfitting analysis
    if result.is_overfit:
        click.echo("⚠️  WARNING: Potential overfitting detected!")
        click.echo("   Training performance significantly exceeds test performance.")
    else:
        click.echo("✓ No significant overfitting detected.")

    click.echo()

    # Parameter stability
    if result.parameter_stability:
        click.echo("-" * 70)
        click.echo("PARAMETER STABILITY")
        click.echo("-" * 70)

        if result.has_parameter_instability:
            click.echo("⚠️  WARNING: Some parameters show instability across windows:")
            for param_name in result.unstable_parameters:
                stability = result.parameter_stability[param_name]
                click.echo(f"   - {param_name}: CV={stability.cv:.2%} (threshold={stability.stability_threshold:.0%})")
        else:
            click.echo("✓ All parameters show stable optimization across windows.")

        click.echo()
        click.echo("Parameter Statistics:")
        for name, stability in result.parameter_stability.items():
            stable_marker = "✓" if stability.is_stable else "⚠️"
            if stability.cv != float("inf"):
                click.echo(
                    f"  {stable_marker} {name}: mean={stability.mean:.4f}, "
                    f"std={stability.std:.4f}, CV={stability.cv:.2%}"
                )
            else:
                click.echo(f"  {stable_marker} {name}: categorical (values vary)")

    click.echo()

    # Per-window summary
    click.echo("-" * 70)
    click.echo("PER-WINDOW RESULTS")
    click.echo("-" * 70)
    click.echo(
        f"{'Window':<8} {'Train Start':<12} {'Test End':<12} {'Train Obj':<12} {'Test Obj':<12} {'Overfit Ratio':<14}"
    )
    click.echo("-" * 70)

    for window_result in result.windows:
        window = window_result.window
        train_start = window.train_start.strftime("%Y-%m-%d")
        test_end = window.test_end.strftime("%Y-%m-%d")
        click.echo(
            f"{window.window_index:<8} {train_start:<12} {test_end:<12} "
            f"{window_result.train_objective_value:<12.4f} "
            f"{window_result.test_objective_value:<12.4f} "
            f"{window_result.overfitting_ratio:<14.4f}"
        )

    click.echo("-" * 70)
    click.echo()


@backtest.command("walk-forward")
@click.option(
    "--strategy",
    "-s",
    required=True,
    help="Name of the strategy to optimize",
)
@click.option(
    "--start",
    required=True,
    callback=parse_date,
    help="Start date (YYYY-MM-DD)",
)
@click.option(
    "--end",
    required=True,
    callback=parse_date,
    help="End date (YYYY-MM-DD)",
)
@click.option(
    "--config-file",
    "-f",
    type=click.Path(exists=True),
    required=True,
    help="Path to optimization config JSON file with parameter ranges",
)
@click.option(
    "--train-days",
    type=int,
    default=90,
    help="Training window size in days (default: 90)",
)
@click.option(
    "--test-days",
    type=int,
    default=30,
    help="Test window size in days (default: 30)",
)
@click.option(
    "--step-days",
    type=int,
    default=None,
    help="Step size in days between windows (default: test-days, non-overlapping)",
)
@click.option(
    "--gap-days",
    type=int,
    default=0,
    help="Gap between train and test in days (default: 0)",
)
@click.option(
    "--min-windows",
    type=int,
    default=2,
    help="Minimum number of windows required (default: 2)",
)
@click.option(
    "--objective",
    type=click.Choice(
        [
            "sharpe_ratio",
            "sortino_ratio",
            "calmar_ratio",
            "total_return_pct",
            "annualized_return_pct",
            "max_drawdown_pct",
            "profit_factor",
            "win_rate",
            "net_pnl_usd",
        ]
    ),
    default=None,
    help="Metric to optimize (default: from config or sharpe_ratio)",
)
@click.option(
    "--n-trials",
    "-n",
    type=int,
    default=None,
    help="Number of optimization trials per window (default: from config or 50)",
)
@click.option(
    "--patience",
    type=int,
    default=None,
    help="Early stopping patience per window (default: from config)",
)
@click.option(
    "--interval",
    type=int,
    default=3600,
    help="Interval between ticks in seconds (default: 3600 = 1 hour)",
)
@click.option(
    "--initial-capital",
    type=float,
    default=10000.0,
    help="Initial portfolio balance in USD (default: 10000)",
)
@click.option(
    "--chain",
    "-c",
    type=str,
    default="arbitrum",
    help="Target blockchain (default: arbitrum)",
)
@click.option(
    "--tokens",
    type=str,
    default="WETH,USDC",
    help="Comma-separated list of tokens to track (default: WETH,USDC)",
)
@click.option(
    "--output",
    "-o",
    type=click.Path(exists=False),
    default=None,
    help="Output file for full JSON results (optional)",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Show configuration without running walk-forward optimization",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    default=False,
    help="Show progress bar and detailed logging",
)
def walk_forward_backtest(
    strategy: str,
    start: datetime,
    end: datetime,
    config_file: str,
    train_days: int,
    test_days: int,
    step_days: int | None,
    gap_days: int,
    min_windows: int,
    objective: str | None,
    n_trials: int | None,
    patience: int | None,
    interval: int,
    initial_capital: float,
    chain: str,
    tokens: str,
    output: str | None,
    dry_run: bool,
    verbose: bool,
) -> None:
    """
    Run walk-forward optimization for robust parameter tuning.

    Walk-forward optimization addresses overfitting by:

    \b
    1. Splitting data into multiple train/test windows
    2. Optimizing parameters on each training window
    3. Testing with optimal parameters on out-of-sample test windows
    4. Aggregating out-of-sample results for realistic performance estimates

    This provides a more realistic estimate of live trading performance than
    a single in-sample optimization.

    The optimization config file (--config-file) must contain parameter ranges.
    These are typically your strategy's own parameters (the ones your decide()
    method uses), but PnLBacktestConfig fields are also supported:

    \b
    {
        "param_ranges": {
            "rsi_oversold": {"type": "discrete", "min": 20, "max": 40, "step": 5},
            "rsi_overbought": {"type": "discrete", "min": 60, "max": 80, "step": 5},
            "trade_size_usd": {"type": "continuous", "min": 100, "max": 5000},
            "mode": {"type": "categorical", "choices": ["aggressive", "conservative"]}
        },
        "objective": "sharpe_ratio",
        "n_trials": 50,
        "patience": 10
    }

    Strategy param names are merged into the strategy config dict.
    PnLBacktestConfig field names are applied to the backtest config.
    The optimizer automatically routes each key to the right place.

    Window configuration:

    \b
        --train-days: Training window for parameter optimization
        --test-days: Out-of-sample testing window
        --step-days: How far to advance between windows (default: test-days)
        --gap-days: Gap between train end and test start (implementation lag)

    Examples:

    \b
        # Basic walk-forward with 90-day train, 30-day test
        almanak backtest walk-forward -s uniswap_rsi \\
            --start 2023-01-01 --end 2024-01-01 \\
            --config-file optimize_config.json \\
            --train-days 90 --test-days 30

    \b
        # With custom settings and output
        almanak backtest walk-forward -s mean_reversion \\
            --start 2023-01-01 --end 2024-06-01 \\
            --config-file config.json \\
            --train-days 60 --test-days 20 --step-days 20 \\
            --objective sortino_ratio \\
            --n-trials 100 \\
            --output results.json

    \b
        # Dry run to verify configuration
        almanak backtest walk-forward -s test_strategy \\
            --start 2023-01-01 --end 2024-01-01 \\
            --config-file config.json --dry-run
    """
    from ..backtesting.pnl import (
        WalkForwardConfig,
        run_walk_forward_optimization,
    )

    # Load optimization config
    config_path = Path(config_file)
    try:
        opt_config = load_optimization_config(config_path)
    except Exception as e:
        click.echo(f"Error loading config file: {e}", err=True)
        raise click.Abort() from None

    # Parse parameter ranges
    try:
        param_ranges = parse_param_ranges_from_config(opt_config)
    except click.BadParameter as e:
        click.echo(f"Error parsing config: {e}", err=True)
        raise click.Abort() from None

    if not param_ranges:
        raise click.UsageError(
            "No parameter ranges defined in config file. Add 'param_ranges' with at least one parameter."
        )

    # Determine settings (CLI args override config file)
    effective_objective = objective or opt_config.get("objective", "sharpe_ratio")
    effective_n_trials = n_trials or opt_config.get("n_trials", 50)
    effective_patience = patience or opt_config.get("patience")

    # Create walk-forward config
    wf_config = WalkForwardConfig.from_days(
        train_days=train_days,
        test_days=test_days,
        step_days=step_days,
        gap_days=gap_days,
        min_windows=min_windows,
    )

    # Validate strategy exists
    available_strategies = list_strategies_fn()
    if strategy not in available_strategies and available_strategies:
        click.echo(f"Error: Unknown strategy '{strategy}'", err=True)
        click.echo(f"Available strategies: {', '.join(sorted(available_strategies))}", err=True)
        raise click.Abort()

    # Parse tokens list
    token_list = [t.strip().upper() for t in tokens.split(",")]

    # Estimate number of windows
    total_duration = (end - start).days
    window_size = train_days + gap_days + test_days
    effective_step = step_days if step_days else test_days
    estimated_windows = max(0, (total_duration - window_size) // effective_step + 1)

    # Display configuration
    click.echo("=" * 70)
    click.echo("WALK-FORWARD OPTIMIZATION CONFIGURATION")
    click.echo("=" * 70)
    click.echo(f"Strategy: {strategy}")
    click.echo(f"Chain: {chain}")
    click.echo(f"Period: {start.date()} -> {end.date()} ({total_duration} days)")
    click.echo(f"Interval: {interval}s ({interval / 3600:.1f} hours)")
    click.echo(f"Initial Capital: ${initial_capital:,.2f}")
    click.echo(f"Tokens: {', '.join(token_list)}")
    click.echo()
    click.echo("Window Configuration:")
    click.echo(f"  Train Window: {train_days} days")
    click.echo(f"  Test Window: {test_days} days")
    click.echo(f"  Step Size: {effective_step} days")
    click.echo(f"  Gap: {gap_days} days")
    click.echo(f"  Min Windows: {min_windows}")
    click.echo(f"  Estimated Windows: ~{estimated_windows}")
    click.echo()
    click.echo(f"Optimization: {effective_objective}")
    click.echo(f"Trials per Window: {effective_n_trials}")
    if effective_patience:
        click.echo(f"Early Stopping: patience={effective_patience}")
    else:
        click.echo("Early Stopping: disabled")
    click.echo()
    click.echo("Parameters to optimize:")
    for name, spec in param_ranges.items():
        if hasattr(spec, "param_type"):
            # ParamRange object
            if spec.param_type.value == "categorical":
                click.echo(f"  {name}: categorical {spec.choices}")
            elif spec.param_type.value == "discrete":
                step_str = f", step={spec.step}" if spec.step else ""
                click.echo(f"  {name}: discrete [{spec.low}, {spec.high}{step_str}]")
            else:
                log_str = " (log)" if spec.log else ""
                step_str = f", step={spec.step}" if spec.step else ""
                click.echo(f"  {name}: continuous [{spec.low}, {spec.high}{step_str}]{log_str}")
        else:
            click.echo(f"  {name}: {spec}")

    if output:
        click.echo(f"Output: {output}")

    click.echo("=" * 70)

    # Handle dry run
    if dry_run:
        click.echo()
        click.echo("Dry run - walk-forward optimization not executed.")
        return

    # Load strategy
    try:
        strategy_class = get_strategy(strategy)
    except ValueError:
        click.echo()
        click.echo("Warning: No strategies registered in factory.", err=True)
        click.echo("Running with mock strategy for demonstration.", err=True)
        click.echo()

        from ..strategies import MarketSnapshot

        class MockWalkForwardStrategy:
            """Mock strategy for walk-forward demonstration."""

            strategy_id: str = "mock-walk-forward"

            def __init__(self, config: dict[str, Any]) -> None:
                self.config = config

            def decide(self, market: MarketSnapshot) -> dict[str, Any] | None:
                return None

        strategy_class = MockWalkForwardStrategy

    # Load base strategy config
    base_config = load_strategy_config(strategy, chain)

    # Create PnL backtest config
    pnl_config = PnLBacktestConfig(
        start_time=start,
        end_time=end,
        interval_seconds=interval,
        initial_capital_usd=Decimal(str(initial_capital)),
        chain=chain,
        tokens=token_list,
        gas_price_gwei=Decimal("30"),
        include_gas_costs=True,
    )

    # Create data provider factory
    def create_data_provider() -> CoinGeckoDataProvider:
        return CoinGeckoDataProvider()

    # Create strategy factory that accepts optional config overrides
    def create_strategy(config_overrides: dict[str, Any] | None = None) -> Any:
        effective_config = {**base_config, **(config_overrides or {})}
        return _create_backtest_strategy(strategy_class, effective_config, chain)

    # Create backtester factory
    def create_backtester(
        data_provider: Any,
        fee_models: dict[str, Any],
        slippage_models: dict[str, Any],
    ) -> PnLBacktester:
        return PnLBacktester(
            data_provider=data_provider,
            fee_models=fee_models,
            slippage_models=slippage_models,
        )

    # Run walk-forward optimization
    click.echo()
    click.echo(
        f"Starting walk-forward optimization (~{estimated_windows} windows, {effective_n_trials} trials per window)..."
    )
    click.echo()

    try:
        result = asyncio.run(
            run_walk_forward_optimization(
                strategy_factory=create_strategy,
                data_provider_factory=create_data_provider,
                backtester_factory=create_backtester,
                base_config=pnl_config,
                param_ranges=param_ranges,
                wf_config=wf_config,
                objective_metric=effective_objective,
                n_trials_per_window=effective_n_trials,
                patience=effective_patience,
                show_progress=verbose,
                strategy_config=base_config,
            )
        )
    except ValueError as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"Error during walk-forward optimization: {e}", err=True)
        sys.exit(1)

    # Display results
    print_walk_forward_results(result, effective_objective)

    # Write output if requested
    if output:
        output_path = Path(output)
        try:
            result_dict = result.to_dict()
            with open(output_path, "w") as f:
                json.dump(result_dict, f, indent=2, default=str)
            click.echo(f"Walk-forward results written to: {output_path}")
        except Exception as e:
            click.echo(f"Warning: Could not save results: {e}", err=True)


# =============================================================================
# Monte Carlo Simulation Subcommand
# =============================================================================


def print_monte_carlo_results(
    result: Any,
) -> None:
    """Print Monte Carlo simulation results in a formatted way.

    Args:
        result: MonteCarloSimulationResult from run_monte_carlo
    """
    click.echo()
    click.echo("=" * 70)
    click.echo("MONTE CARLO SIMULATION RESULTS")
    click.echo("=" * 70)
    click.echo()
    click.echo(f"Paths Simulated: {result.n_paths}")
    click.echo(f"Successful: {result.n_successful}")
    click.echo(f"Failed: {result.n_failed}")
    click.echo()

    # Return statistics
    click.echo("-" * 70)
    click.echo("RETURN DISTRIBUTION")
    click.echo("-" * 70)
    click.echo(f"  Mean Return:        {float(result.return_mean) * 100:+.2f}%")
    click.echo(f"  Std Dev:            {float(result.return_std) * 100:.2f}%")
    click.echo()
    click.echo("  Confidence Intervals:")
    click.echo(f"    5th percentile:   {float(result.return_percentile_5th) * 100:+.2f}%")
    click.echo(f"    25th percentile:  {float(result.return_percentile_25th) * 100:+.2f}%")
    click.echo(f"    50th percentile:  {float(result.return_percentile_50th) * 100:+.2f}%  (median)")
    click.echo(f"    75th percentile:  {float(result.return_percentile_75th) * 100:+.2f}%")
    click.echo(f"    95th percentile:  {float(result.return_percentile_95th) * 100:+.2f}%")
    click.echo()
    click.echo(
        f"  90% CI: [{float(result.return_percentile_5th) * 100:+.2f}%, "
        f"{float(result.return_percentile_95th) * 100:+.2f}%]"
    )
    click.echo()

    # Drawdown statistics
    click.echo("-" * 70)
    click.echo("DRAWDOWN ANALYSIS")
    click.echo("-" * 70)
    click.echo(f"  Mean Max Drawdown:   {float(result.max_drawdown_mean) * 100:.2f}%")
    click.echo(f"  Worst Max Drawdown:  {float(result.max_drawdown_worst) * 100:.2f}%")
    click.echo(f"  95th Percentile:     {float(result.max_drawdown_percentile_95th) * 100:.2f}%")
    click.echo()

    # Drawdown threshold probabilities
    if result.probability_drawdown_exceeds_threshold:
        click.echo("  Probability of Drawdown Exceeding Threshold:")
        for threshold_str, prob in sorted(result.probability_drawdown_exceeds_threshold.items()):
            threshold_pct = float(threshold_str) * 100
            prob_pct = float(prob) * 100
            click.echo(f"    > {threshold_pct:.0f}%:  {prob_pct:.1f}%")
    click.echo()

    # Risk probabilities
    click.echo("-" * 70)
    click.echo("RISK PROBABILITIES")
    click.echo("-" * 70)
    click.echo(f"  P(Negative Return):  {float(result.probability_negative_return) * 100:.1f}%")
    click.echo(f"  P(Loss > 10%):       {float(result.probability_loss_exceeds_10pct) * 100:.1f}%")
    click.echo(f"  P(Loss > 20%):       {float(result.probability_loss_exceeds_20pct) * 100:.1f}%")
    click.echo(f"  P(Gain > 10%):       {float(result.probability_gain_exceeds_10pct) * 100:.1f}%")
    click.echo()

    # Sharpe ratio statistics
    if result.sharpe_mean is not None:
        click.echo("-" * 70)
        click.echo("SHARPE RATIO DISTRIBUTION")
        click.echo("-" * 70)
        click.echo(f"  Mean Sharpe:  {float(result.sharpe_mean):.3f}")
        if result.sharpe_std is not None:
            click.echo(f"  Std Dev:      {float(result.sharpe_std):.3f}")
        click.echo()

    click.echo("=" * 70)


@backtest.command("monte-carlo")
@click.option(
    "--strategy",
    "-s",
    required=True,
    help="Name of the strategy to simulate",
)
@click.option(
    "--start",
    required=True,
    callback=parse_date,
    help="Start date for historical data (YYYY-MM-DD)",
)
@click.option(
    "--end",
    required=True,
    callback=parse_date,
    help="End date for historical data (YYYY-MM-DD)",
)
@click.option(
    "--n-paths",
    "-n",
    type=int,
    default=100,
    help="Number of price paths to simulate (default: 100)",
)
@click.option(
    "--method",
    type=click.Choice(["gbm", "bootstrap"]),
    default="gbm",
    help="Price path generation method (default: gbm - Geometric Brownian Motion)",
)
@click.option(
    "--interval",
    type=int,
    default=3600,
    help="Interval between ticks in seconds (default: 3600 = 1 hour)",
)
@click.option(
    "--initial-capital",
    type=float,
    default=10000.0,
    help="Initial portfolio balance in USD (default: 10000)",
)
@click.option(
    "--chain",
    "-c",
    type=str,
    default="arbitrum",
    help="Target blockchain (default: arbitrum)",
)
@click.option(
    "--tokens",
    type=str,
    default="WETH,USDC",
    help="Comma-separated list of tokens to track (default: WETH,USDC)",
)
@click.option(
    "--base-token",
    type=str,
    default="WETH",
    help="Token to simulate price paths for (default: WETH)",
)
@click.option(
    "--parallel-workers",
    "-j",
    type=int,
    default=4,
    help="Number of parallel workers for backtests (default: 4)",
)
@click.option(
    "--seed",
    type=int,
    default=None,
    help="Random seed for reproducibility",
)
@click.option(
    "--output",
    "-o",
    type=click.Path(exists=False),
    default=None,
    help="Output file for full JSON results (optional)",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Show configuration without running simulation",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    default=False,
    help="Show progress during simulation",
)
def monte_carlo_backtest(
    strategy: str,
    start: datetime,
    end: datetime,
    n_paths: int,
    method: str,
    interval: int,
    initial_capital: float,
    chain: str,
    tokens: str,
    base_token: str,
    parallel_workers: int,
    seed: int | None,
    output: str | None,
    dry_run: bool,
    verbose: bool,
) -> None:
    """
    Run Monte Carlo simulation to analyze strategy performance distribution.

    Monte Carlo simulation generates multiple randomized price paths based on
    historical price statistics (drift and volatility) and runs the strategy
    across all paths. This provides:

    \b
    - Confidence intervals for returns (5th to 95th percentile)
    - Probability of negative returns or specific loss levels
    - Distribution of maximum drawdowns
    - Statistical robustness analysis

    Price Path Methods:
    \b
        - gbm: Geometric Brownian Motion (default)
               Uses historical drift and volatility to generate realistic paths
        - bootstrap: Bootstrap resampling of historical returns

    Examples:

    \b
        # Basic Monte Carlo with 100 paths
        almanak backtest monte-carlo -s momentum \\
            --start 2024-01-01 --end 2024-06-01 \\
            --n-paths 100

    \b
        # Higher fidelity with 1000 paths and parallel execution
        almanak backtest monte-carlo -s mean_reversion \\
            --start 2024-01-01 --end 2024-06-01 \\
            --n-paths 1000 --parallel-workers 8 \\
            --output monte_carlo_results.json

    \b
        # With reproducible seed
        almanak backtest monte-carlo -s dynamic_lp \\
            --start 2024-01-01 --end 2024-06-01 \\
            --n-paths 500 --seed 42

    \b
        # Dry run to verify configuration
        almanak backtest monte-carlo -s test_strategy \\
            --start 2024-01-01 --end 2024-06-01 --dry-run
    """
    from ..backtesting.pnl import (
        MonteCarloConfig,
        MonteCarloPathGenerator,
        PathGenerationMethod,
        PricePathConfig,
        run_monte_carlo,
    )

    # Validate strategy exists
    available_strategies = list_strategies_fn()
    if strategy not in available_strategies and available_strategies:
        click.echo(f"Error: Unknown strategy '{strategy}'", err=True)
        click.echo(f"Available strategies: {', '.join(sorted(available_strategies))}", err=True)
        raise click.Abort()

    # Parse tokens list
    token_list = [t.strip().upper() for t in tokens.split(",")]
    base_token_upper = base_token.strip().upper()

    # Calculate duration
    duration_days = (end - start).days

    # Determine method enum
    path_method = PathGenerationMethod.BOOTSTRAP if method == "bootstrap" else PathGenerationMethod.GBM

    # Display configuration
    click.echo("=" * 70)
    click.echo("MONTE CARLO SIMULATION CONFIGURATION")
    click.echo("=" * 70)
    click.echo(f"Strategy: {strategy}")
    click.echo(f"Chain: {chain}")
    click.echo(f"Historical Period: {start.date()} -> {end.date()} ({duration_days} days)")
    click.echo(f"Interval: {interval}s ({interval / 3600:.1f} hours)")
    click.echo(f"Initial Capital: ${initial_capital:,.2f}")
    click.echo(f"Tokens: {', '.join(token_list)}")
    click.echo(f"Base Token (simulated): {base_token_upper}")
    click.echo()
    click.echo("Simulation Parameters:")
    click.echo(f"  Number of Paths: {n_paths}")
    click.echo(f"  Generation Method: {method.upper()}")
    click.echo(f"  Parallel Workers: {parallel_workers}")
    if seed is not None:
        click.echo(f"  Random Seed: {seed}")
    else:
        click.echo("  Random Seed: None (random)")

    if output:
        click.echo(f"Output: {output}")

    click.echo("=" * 70)

    # Handle dry run
    if dry_run:
        click.echo()
        click.echo("Dry run - Monte Carlo simulation not executed.")
        return

    # Load strategy
    try:
        strategy_class = get_strategy(strategy)
    except ValueError:
        click.echo()
        click.echo("Warning: No strategies registered in factory.", err=True)
        click.echo("Running with mock strategy for demonstration.", err=True)
        click.echo()

        from ..strategies import MarketSnapshot

        class MockMonteCarloStrategy:
            """Mock strategy for Monte Carlo demonstration."""

            strategy_id: str = "mock-monte-carlo"

            def __init__(self, config: dict[str, Any]) -> None:
                self.config = config

            def decide(self, market: MarketSnapshot) -> dict[str, Any] | None:
                return None

        strategy_class = MockMonteCarloStrategy

    # Load base strategy config
    base_config = load_strategy_config(strategy, chain)

    # Create strategy instance
    strategy_instance = _create_backtest_strategy(strategy_class, base_config, chain)

    # Create PnL backtest config
    pnl_config = PnLBacktestConfig(
        start_time=start,
        end_time=end,
        interval_seconds=interval,
        initial_capital_usd=Decimal(str(initial_capital)),
        chain=chain,
        tokens=token_list,
        gas_price_gwei=Decimal("30"),
        include_gas_costs=True,
    )

    # Create data provider to fetch historical prices for path generation
    data_provider = CoinGeckoDataProvider()

    click.echo()
    click.echo(f"Fetching historical prices for {base_token_upper}...")

    # Fetch historical prices using get_ohlcv and extract close prices
    async def fetch_historical_prices() -> list[Decimal]:
        """Fetch historical prices from CoinGecko."""
        ohlcv_data = await data_provider.get_ohlcv(
            base_token_upper,
            start,
            end,
            interval,
        )
        await data_provider.close()
        # Extract close prices from OHLCV data
        return [ohlcv.close for ohlcv in ohlcv_data]

    try:
        historical_prices = asyncio.run(fetch_historical_prices())
    except Exception as e:
        # If CoinGecko fails, generate synthetic historical data
        click.echo(f"Warning: Could not fetch historical prices: {e}", err=True)
        click.echo("Using synthetic historical data for demonstration...", err=True)

        # Generate synthetic historical prices (random walk)
        import random

        if seed is not None:
            random.seed(seed)
        n_steps = max(10, duration_days * (86400 // interval))
        price = Decimal("3000")  # Start price
        historical_prices = [price]
        for _ in range(n_steps - 1):
            change = Decimal(str(random.gauss(0, 0.02)))  # 2% daily volatility
            price = price * (1 + change)
            historical_prices.append(price)

    click.echo(f"Historical prices: {len(historical_prices)} data points")
    click.echo()

    # Generate price paths
    click.echo(f"Generating {n_paths} price paths using {method.upper()} method...")

    path_config = PricePathConfig(
        method=path_method,
        n_paths=n_paths,
        seed=seed,
    )

    generator = MonteCarloPathGenerator(config=path_config)
    paths = generator.generate_price_paths(
        historical_prices=historical_prices,
    )

    click.echo(f"Generated {paths.n_paths} paths with {paths.n_steps} steps each")
    click.echo(f"Estimated drift: {float(paths.drift) * 100:.2f}% annualized")
    click.echo(f"Estimated volatility: {float(paths.volatility) * 100:.2f}% annualized")
    click.echo()

    # Create Monte Carlo config
    mc_config = MonteCarloConfig(
        n_paths=n_paths,
        parallel_workers=parallel_workers,
        base_token=base_token_upper,
        quote_token="USDC",
        seed=seed,
        collect_individual_results=False,  # Save memory
    )

    # Progress callback for verbose mode
    def progress_callback(completed: int, total: int) -> None:
        if verbose:
            pct = (completed / total) * 100
            click.echo(f"  Progress: {completed}/{total} paths ({pct:.1f}%)", nl=False)
            click.echo("\r", nl=False)

    if verbose:
        mc_config.progress_callback = progress_callback

    # Run Monte Carlo simulation
    click.echo(f"Running Monte Carlo simulation ({n_paths} paths, {parallel_workers} workers)...")
    click.echo()

    try:
        result = asyncio.run(
            run_monte_carlo(
                strategy=strategy_instance,
                paths=paths,
                backtest_config=pnl_config,
                mc_config=mc_config,
            )
        )
    except Exception as e:
        click.echo(f"Error during Monte Carlo simulation: {e}", err=True)
        sys.exit(1)

    if verbose:
        click.echo()  # Clear progress line

    # Display results
    print_monte_carlo_results(result)

    # Write output if requested
    if output:
        output_path = Path(output)
        try:
            result_dict = result.to_dict()
            with open(output_path, "w") as f:
                json.dump(result_dict, f, indent=2, default=str)
            click.echo(f"Monte Carlo results written to: {output_path}")
        except Exception as e:
            click.echo(f"Warning: Could not save results: {e}", err=True)


# =============================================================================
# Crisis Scenario Backtest Subcommand
# =============================================================================


def print_crisis_backtest_results(result: CrisisBacktestResult) -> None:
    """Print crisis backtest results in a formatted way.

    Args:
        result: CrisisBacktestResult from run_crisis_backtest
    """
    click.echo()
    click.echo("=" * 70)
    click.echo("CRISIS SCENARIO BACKTEST RESULTS")
    click.echo("=" * 70)
    click.echo()

    # Scenario information
    click.echo(f"Scenario: {result.scenario.name}")
    click.echo(f"Description: {result.scenario.description[:100]}...")
    click.echo(
        f"Period: {result.scenario.start_date.strftime('%Y-%m-%d')} to "
        f"{result.scenario.end_date.strftime('%Y-%m-%d')} "
        f"({result.scenario_duration_days} days)"
    )
    click.echo()

    # Performance metrics
    click.echo("-" * 70)
    click.echo("CRISIS PERIOD PERFORMANCE")
    click.echo("-" * 70)
    click.echo(f"  Total Return:     {float(result.total_return_during_crisis) * 100:+.2f}%")
    click.echo(f"  Max Drawdown:     {float(result.max_drawdown_during_crisis) * 100:.2f}%")
    click.echo(f"  Sharpe Ratio:     {float(result.result.metrics.sharpe_ratio):.3f}")
    click.echo(f"  Sortino Ratio:    {float(result.result.metrics.sortino_ratio):.3f}")
    click.echo(f"  Volatility:       {float(result.result.metrics.volatility) * 100:.2f}%")
    click.echo()

    # Trade statistics
    click.echo("-" * 70)
    click.echo("TRADE STATISTICS")
    click.echo("-" * 70)
    click.echo(f"  Total Trades:     {result.result.metrics.total_trades}")
    click.echo(f"  Winning Trades:   {result.result.metrics.winning_trades}")
    click.echo(f"  Losing Trades:    {result.result.metrics.losing_trades}")
    click.echo(f"  Win Rate:         {float(result.result.metrics.win_rate) * 100:.1f}%")
    click.echo()

    # Crisis-specific metrics
    crisis_metrics = result.crisis_metrics
    if crisis_metrics:
        click.echo("-" * 70)
        click.echo("CRISIS-SPECIFIC METRICS")
        click.echo("-" * 70)

        # Drawdown timing
        if crisis_metrics.get("days_to_trough", 0) > 0:
            click.echo(f"  Days to Trough:   {crisis_metrics['days_to_trough']}")

        # Recovery metrics
        recovery_time = crisis_metrics.get("recovery_time_days")
        if recovery_time is not None:
            click.echo(f"  Recovery Time:    {recovery_time} days")
        else:
            click.echo("  Recovery Time:    Did not fully recover")

        recovery_pct = crisis_metrics.get("recovery_pct", "0")
        if recovery_pct:
            click.echo(f"  Recovery %:       {float(recovery_pct) * 100:.1f}%")

        # Total costs during crisis
        total_costs = crisis_metrics.get("total_costs_usd", "0")
        if total_costs:
            click.echo(f"  Total Costs:      ${float(total_costs):,.2f}")

        click.echo()

    # Normal period comparison if available
    if crisis_metrics and crisis_metrics.get("normal_period_comparison"):
        comparison = crisis_metrics["normal_period_comparison"]
        click.echo("-" * 70)
        click.echo("CRISIS VS NORMAL PERIOD COMPARISON")
        click.echo("-" * 70)

        return_diff = comparison.get("return_diff_pct", "0")
        click.echo(f"  Return Difference:      {float(return_diff) * 100:+.2f}%")

        vol_ratio = comparison.get("volatility_ratio", "1")
        click.echo(f"  Volatility Ratio:       {float(vol_ratio):.2f}x")

        dd_ratio = comparison.get("drawdown_ratio", "1")
        click.echo(f"  Drawdown Ratio:         {float(dd_ratio):.2f}x")

        sharpe_diff = comparison.get("sharpe_diff", "0")
        click.echo(f"  Sharpe Difference:      {float(sharpe_diff):+.3f}")

        win_rate_diff = comparison.get("win_rate_diff", "0")
        click.echo(f"  Win Rate Difference:    {float(win_rate_diff) * 100:+.1f}%")

        stress_score = comparison.get("stress_resilience_score", "50")
        click.echo(f"  Stress Resilience:      {float(stress_score):.0f}/100")

        click.echo()

    click.echo("=" * 70)


@backtest.command("scenario")
@click.option(
    "--strategy",
    "-s",
    required=False,
    default=None,
    help="Name of the strategy to backtest",
)
@click.option(
    "--scenario",
    "-sc",
    required=False,
    default=None,
    help="Predefined scenario name (black_thursday, terra_collapse, ftx_collapse) or 'custom' for custom date range",
)
@click.option(
    "--start",
    required=False,
    default=None,
    callback=parse_date,
    help="Custom scenario start date (YYYY-MM-DD). Required if --scenario=custom",
)
@click.option(
    "--end",
    required=False,
    default=None,
    callback=parse_date,
    help="Custom scenario end date (YYYY-MM-DD). Required if --scenario=custom",
)
@click.option(
    "--name",
    required=False,
    default=None,
    help="Custom scenario name (used with --scenario=custom)",
)
@click.option(
    "--description",
    required=False,
    default=None,
    help="Custom scenario description (used with --scenario=custom)",
)
@click.option(
    "--interval",
    type=int,
    default=3600,
    help="Interval between ticks in seconds (default: 3600 = 1 hour)",
)
@click.option(
    "--initial-capital",
    type=float,
    default=10000.0,
    help="Initial portfolio balance in USD (default: 10000)",
)
@click.option(
    "--chain",
    "-c",
    type=str,
    default="arbitrum",
    help="Target blockchain (default: arbitrum)",
)
@click.option(
    "--tokens",
    type=str,
    default="WETH,USDC",
    help="Comma-separated list of tokens to track (default: WETH,USDC)",
)
@click.option(
    "--gas-price",
    type=float,
    default=50.0,
    help="Gas price in gwei (default: 50 for crisis periods)",
)
@click.option(
    "--mev/--no-mev",
    default=True,
    help="Enable/disable MEV simulation (default: enabled for crisis)",
)
@click.option(
    "--compare-normal",
    is_flag=True,
    default=False,
    help="Compare crisis performance to a normal period (same duration, different dates)",
)
@click.option(
    "--normal-start",
    required=False,
    default=None,
    callback=parse_date,
    help="Start date for normal period comparison (defaults to 30 days before crisis)",
)
@click.option(
    "--output",
    "-o",
    type=click.Path(exists=False),
    default=None,
    help="Output file for full JSON results (optional)",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Show configuration without running backtest",
)
@click.option(
    "--list-scenarios",
    is_flag=True,
    default=False,
    help="List all available predefined scenarios",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    default=False,
    help="Show detailed progress during backtest",
)
def scenario_backtest(
    strategy: str | None,
    scenario: str | None,
    start: datetime | None,
    end: datetime | None,
    name: str | None,
    description: str | None,
    interval: int,
    initial_capital: float,
    chain: str,
    tokens: str,
    gas_price: float,
    mev: bool,
    compare_normal: bool,
    normal_start: datetime | None,
    output: str | None,
    dry_run: bool,
    list_scenarios: bool,
    verbose: bool,
) -> None:
    """
    Run a backtest during a historical crisis scenario.

    This command runs your strategy during a specific crisis period to stress-test
    its behavior under extreme market conditions. Use predefined scenarios or
    define a custom crisis period.

    \b
    Predefined Scenarios:
        - black_thursday: March 2020 COVID crash (BTC -52%, ETH -55%)
        - terra_collapse: May 2022 UST/LUNA de-peg ($60B wiped out)
        - ftx_collapse: November 2022 FTX bankruptcy (BTC -26%)

    \b
    Key Features:
        - Automatic crisis period date range
        - Crisis-specific metrics (recovery time, stress resilience)
        - Optional comparison to normal period performance
        - Higher default gas prices for crisis volatility

    Examples:

    \b
        # List available scenarios
        almanak backtest scenario --list-scenarios

    \b
        # Run Black Thursday scenario
        almanak backtest scenario -s momentum --scenario black_thursday

    \b
        # Run with normal period comparison
        almanak backtest scenario -s dynamic_lp --scenario terra_collapse \\
            --compare-normal

    \b
        # Custom crisis scenario
        almanak backtest scenario -s mean_reversion --scenario custom \\
            --start 2023-03-10 --end 2023-03-15 \\
            --name svb_collapse --description "Silicon Valley Bank collapse"

    \b
        # Dry run to verify configuration
        almanak backtest scenario -s test --scenario ftx_collapse --dry-run
    """
    from datetime import timedelta

    # Handle --list-scenarios
    if list_scenarios:
        click.echo()
        click.echo("=" * 70)
        click.echo("AVAILABLE CRISIS SCENARIOS")
        click.echo("=" * 70)
        click.echo()
        for scenario_name, scenario_obj in PREDEFINED_SCENARIOS.items():
            click.echo(f"  {scenario_name}")
            click.echo(
                f"    Period: {scenario_obj.start_date.strftime('%Y-%m-%d')} to "
                f"{scenario_obj.end_date.strftime('%Y-%m-%d')} "
                f"({scenario_obj.duration_days} days)"
            )
            click.echo(f"    Description: {scenario_obj.description[:80]}...")
            click.echo()
        click.echo("Use --scenario <name> to run a backtest with a specific scenario.")
        click.echo("Use --scenario custom with --start/--end for custom date range.")
        click.echo("=" * 70)
        return

    # Validate required parameters when not listing scenarios
    if strategy is None:
        click.echo("Error: --strategy is required. Use -s <strategy_name>.", err=True)
        raise click.Abort()

    # Validate scenario selection
    if scenario is None:
        click.echo("Error: --scenario is required. Use --list-scenarios to see options.", err=True)
        raise click.Abort()

    # Resolve scenario
    crisis_scenario: CrisisScenario | None = None

    if scenario.lower() == "custom":
        # Custom scenario requires start and end dates
        if start is None or end is None:
            click.echo("Error: --start and --end are required for custom scenarios.", err=True)
            raise click.Abort()
        crisis_scenario = CrisisScenario(
            name=name or "custom_scenario",
            start_date=start,
            end_date=end,
            description=description or f"Custom crisis scenario from {start.date()} to {end.date()}",
        )
    else:
        # Look up predefined scenario
        crisis_scenario = get_scenario_by_name(scenario)
        if crisis_scenario is None:
            click.echo(f"Error: Unknown scenario '{scenario}'", err=True)
            click.echo("Available scenarios:", err=True)
            for name_key in PREDEFINED_SCENARIOS.keys():
                click.echo(f"  - {name_key}", err=True)
            click.echo("Use --scenario custom with --start/--end for custom dates.", err=True)
            raise click.Abort()

    # Validate strategy exists
    available_strategies = list_strategies_fn()
    if strategy not in available_strategies and available_strategies:
        click.echo(f"Error: Unknown strategy '{strategy}'", err=True)
        click.echo(f"Available strategies: {', '.join(sorted(available_strategies))}", err=True)
        raise click.Abort()

    # Parse tokens list
    token_list = [t.strip().upper() for t in tokens.split(",")]

    # Display configuration
    click.echo()
    click.echo("=" * 70)
    click.echo("CRISIS SCENARIO BACKTEST CONFIGURATION")
    click.echo("=" * 70)
    click.echo(f"Strategy: {strategy}")
    click.echo(f"Chain: {chain}")
    click.echo()
    click.echo("Scenario:")
    click.echo(f"  Name: {crisis_scenario.name}")
    click.echo(
        f"  Period: {crisis_scenario.start_date.strftime('%Y-%m-%d')} to "
        f"{crisis_scenario.end_date.strftime('%Y-%m-%d')} "
        f"({crisis_scenario.duration_days} days)"
    )
    click.echo(f"  Description: {crisis_scenario.description[:80]}...")
    click.echo()
    click.echo("Configuration:")
    click.echo(f"  Interval: {interval}s ({interval / 3600:.1f} hours)")
    click.echo(f"  Initial Capital: ${initial_capital:,.2f}")
    click.echo(f"  Tokens: {', '.join(token_list)}")
    click.echo(f"  Gas Price: {gas_price:.0f} gwei")
    click.echo(f"  MEV Simulation: {'Enabled' if mev else 'Disabled'}")
    click.echo(f"  Compare to Normal: {'Yes' if compare_normal else 'No'}")

    if compare_normal:
        # Calculate normal period dates
        if normal_start is None:
            # Default: 30 days before crisis start
            normal_start = crisis_scenario.start_date - timedelta(days=30 + crisis_scenario.duration_days)
        normal_end = normal_start + timedelta(days=crisis_scenario.duration_days)
        click.echo(f"  Normal Period: {normal_start.strftime('%Y-%m-%d')} to {normal_end.strftime('%Y-%m-%d')}")

    if output:
        click.echo(f"Output: {output}")

    click.echo("=" * 70)

    # Handle dry run
    if dry_run:
        click.echo()
        click.echo("Dry run - crisis backtest not executed.")
        return

    # Load strategy
    try:
        strategy_class = get_strategy(strategy)
    except ValueError:
        click.echo()
        click.echo("Warning: No strategies registered in factory.", err=True)
        click.echo("Running with mock strategy for demonstration.", err=True)
        click.echo()

        from ..strategies import MarketSnapshot

        class MockCrisisStrategy:
            """Mock strategy for crisis scenario demonstration."""

            strategy_id: str = "mock-crisis"

            def __init__(self, config: dict[str, Any]) -> None:
                self.config = config

            def decide(self, market: MarketSnapshot) -> dict[str, Any] | None:
                return None

        strategy_class = MockCrisisStrategy

    # Load base strategy config
    base_config = load_strategy_config(strategy, chain)

    # Create strategy instance
    strategy_instance = _create_backtest_strategy(strategy_class, base_config, chain)

    # Create PnL backtester
    data_provider = CoinGeckoDataProvider()
    backtester = PnLBacktester(
        data_provider=data_provider,
        fee_models={},
        slippage_models={},
    )

    # Create crisis backtest config
    crisis_config = CrisisBacktestConfig(
        scenario=crisis_scenario,
        initial_capital_usd=Decimal(str(initial_capital)),
        interval_seconds=interval,
        chain=chain,
        tokens=token_list,
        gas_price_gwei=Decimal(str(gas_price)),
        mev_simulation_enabled=mev,
    )

    click.echo()
    click.echo(f"Running crisis backtest for scenario '{crisis_scenario.name}'...")

    if verbose:
        click.echo(f"  Period: {crisis_scenario.duration_days} days")
        click.echo(f"  Estimated ticks: {crisis_scenario.duration_days * 86400 // interval}")

    # Run crisis backtest
    try:
        result = asyncio.run(
            run_crisis_backtest(
                strategy=strategy_instance,
                scenario=crisis_scenario,
                backtester=backtester,
                config=crisis_config,
            )
        )
    except Exception as e:
        click.echo(f"Error during crisis backtest: {e}", err=True)
        sys.exit(1)

    # Run normal period comparison if requested
    if compare_normal:
        click.echo()
        click.echo("Running normal period backtest for comparison...")

        # Create normal period config
        normal_end_dt = normal_start + timedelta(days=crisis_scenario.duration_days) if normal_start else None
        if normal_start and normal_end_dt:
            normal_pnl_config = PnLBacktestConfig(
                start_time=normal_start,
                end_time=normal_end_dt,
                interval_seconds=interval,
                initial_capital_usd=Decimal(str(initial_capital)),
                chain=chain,
                tokens=token_list,
                gas_price_gwei=Decimal(str(gas_price)),
                mev_simulation_enabled=mev,
            )

            try:
                normal_result = asyncio.run(backtester.backtest(strategy_instance, normal_pnl_config))

                # Build comparison metrics
                comparison = compare_crisis_to_normal(result.result, normal_result)

                # Update crisis result with comparison
                result.crisis_metrics["normal_period_comparison"] = comparison

                # Update the crisis_results on the BacktestResult too
                crisis_metrics_obj = build_crisis_metrics(result.result, crisis_scenario, normal_result)
                result.result.crisis_results = crisis_metrics_obj

            except Exception as e:
                click.echo(f"Warning: Normal period backtest failed: {e}", err=True)
                click.echo("Continuing with crisis results only...")

    # Display results
    print_crisis_backtest_results(result)

    # Write output if requested
    if output:
        output_path = Path(output)
        try:
            result_dict = result.to_dict()
            with open(output_path, "w") as f:
                json.dump(result_dict, f, indent=2, default=str)
            click.echo(f"Crisis backtest results written to: {output_path}")
        except Exception as e:
            click.echo(f"Warning: Could not save results: {e}", err=True)


# =============================================================================
# Block Backtest Subcommand (REMOVED - was legacy)
# =============================================================================


@backtest.command("block", hidden=True)
def block_backtest() -> None:
    """
    [REMOVED] Block-based backtest using Anvil forks.

    This command has been removed as of v2.0. Please use one of the following:

        - 'almanak backtest pnl': Historical simulation with price data (recommended)
        - 'almanak backtest paper': Live-like simulation on Anvil forks

    Migration Guide:

        # Instead of:
        almanak backtest block -s my_strategy --days 7 --chain arbitrum

        # Use PnL backtester (no Anvil required):
        almanak backtest pnl -s my_strategy --start 2024-01-01 --end 2024-01-08

        # Or Paper Trader (for live-like execution):
        almanak backtest paper start -s my_strategy --chain arbitrum

    See: almanak/framework/backtesting/MIGRATION.md for full migration guide.
    """
    click.echo("=" * 70, err=True)
    click.echo("ERROR: 'almanak backtest block' has been removed.", err=True)
    click.echo("=" * 70, err=True)
    click.echo(err=True)
    click.echo("The block-based backtest engine contained placeholder code that", err=True)
    click.echo("produced unreliable results (random PnL, hardcoded prices).", err=True)
    click.echo(err=True)
    click.echo("Please use one of the production-ready alternatives:", err=True)
    click.echo(err=True)
    click.echo("  1. PnL Backtester (recommended for most use cases):", err=True)
    click.echo("     almanak backtest pnl -s <strategy> --start 2024-01-01 --end 2024-06-01", err=True)
    click.echo(err=True)
    click.echo("  2. Paper Trader (for live-like execution on Anvil forks):", err=True)
    click.echo("     almanak backtest paper start -s <strategy> --chain arbitrum", err=True)
    click.echo(err=True)
    click.echo("See: almanak/framework/backtesting/MIGRATION.md for full migration guide.", err=True)
    click.echo("=" * 70, err=True)
    sys.exit(1)


# =============================================================================
# Paper Trading Session Management
# =============================================================================


PAPER_STATE_DIR = Path.home() / ".almanak" / "paper_sessions"


def get_paper_state_file(strategy_id: str) -> Path:
    """Get the state file path for a paper trading session.

    Args:
        strategy_id: Strategy identifier

    Returns:
        Path to the session state file
    """
    PAPER_STATE_DIR.mkdir(parents=True, exist_ok=True)
    return PAPER_STATE_DIR / f"{strategy_id}.json"


def save_paper_session_state(
    strategy_id: str,
    pid: int,
    config: PaperTraderConfig,
    start_time: datetime,
) -> None:
    """Save paper trading session state to disk.

    Args:
        strategy_id: Strategy identifier
        pid: Process ID of the background session
        config: PaperTraderConfig used for the session
        start_time: Session start time
    """
    state_file = get_paper_state_file(strategy_id)
    state = {
        "strategy_id": strategy_id,
        "pid": pid,
        "config": config.to_dict(),
        "start_time": start_time.isoformat(),
        "status": "running",
    }
    with open(state_file, "w") as f:
        json.dump(state, f, indent=2)


def load_paper_session_state(strategy_id: str) -> dict[str, Any] | None:
    """Load paper trading session state from disk.

    Args:
        strategy_id: Strategy identifier

    Returns:
        Session state dictionary or None if not found
    """
    state_file = get_paper_state_file(strategy_id)
    if not state_file.exists():
        return None
    try:
        with open(state_file) as f:
            state: dict[str, Any] = json.load(f)
            return state
    except Exception:
        return None


def update_paper_session_status(strategy_id: str, status: str, summary: dict[str, Any] | None = None) -> None:
    """Update paper trading session status.

    Args:
        strategy_id: Strategy identifier
        status: New status (running, stopped, completed, error)
        summary: Optional session summary data
    """
    state_file = get_paper_state_file(strategy_id)
    if not state_file.exists():
        return
    try:
        with open(state_file) as f:
            state = json.load(f)
        state["status"] = status
        state["last_updated"] = datetime.now(UTC).isoformat()
        if summary:
            state["summary"] = summary
        with open(state_file, "w") as f:
            json.dump(state, f, indent=2)
    except Exception:
        pass


def delete_paper_session_state(strategy_id: str) -> None:
    """Delete paper trading session state file.

    Args:
        strategy_id: Strategy identifier
    """
    state_file = get_paper_state_file(strategy_id)
    if state_file.exists():
        state_file.unlink()


def is_process_running(pid: int) -> bool:
    """Check if a process is still running.

    Args:
        pid: Process ID

    Returns:
        True if process is running
    """
    try:
        os.kill(pid, 0)  # Signal 0 doesn't kill, just checks
        return True
    except OSError:
        return False


def list_paper_sessions() -> list[dict[str, Any]]:
    """List all paper trading sessions.

    Returns:
        List of session state dictionaries
    """
    if not PAPER_STATE_DIR.exists():
        return []

    sessions = []
    for state_file in PAPER_STATE_DIR.glob("*.json"):
        try:
            with open(state_file) as f:
                state = json.load(f)
                # Check if process is still running
                pid = state.get("pid")
                if pid and not is_process_running(pid):
                    state["status"] = "stopped (process not found)"
                sessions.append(state)
        except Exception:
            pass

    return sessions


# =============================================================================
# Paper Trading Subcommand Group
# =============================================================================


def _parse_duration(duration_str: str) -> int | None:
    """Parse a human-readable duration string into seconds.

    Supports: '30s', '5m', '1h', '2h30m', '90m', '1h30m15s'.

    Returns:
        Total seconds, or None if the string is unparseable.
    """
    duration_str = duration_str.strip().lower()
    if not duration_str:
        return None

    # Try pure integer (treat as seconds)
    if duration_str.isdigit():
        total = int(duration_str)
        return total if total > 0 else None

    pattern = re.compile(r"(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?$")
    match = pattern.match(duration_str)
    if not match or not any(match.groups()):
        return None

    hours = int(match.group(1) or 0)
    minutes = int(match.group(2) or 0)
    seconds = int(match.group(3) or 0)
    total = hours * 3600 + minutes * 60 + seconds
    return total if total > 0 else None


@backtest.group("paper")
def paper() -> None:
    """
    Paper trading - real-time simulation on Anvil forks with PnL tracking.

    Paper trading executes real transactions on a local Anvil fork,
    providing accurate simulation of DeFi interactions without risking
    real capital. Includes PnL tracking, equity curves, and trade journals.

    Subcommands:

        start  - Start a paper trading session (foreground or background)

        stop   - Stop a running paper trading session

        status - Check the status of a paper trading session

        logs   - View logs from a paper trading session

    Examples:

        # Start paper trading
        almanak strat backtest paper start -s momentum_v1 --chain arbitrum

        # Check status
        almanak strat backtest paper status -s momentum_v1

        # View logs
        almanak strat backtest paper logs -s momentum_v1

        # Follow logs in real-time
        almanak strat backtest paper logs -s momentum_v1 --follow

        # Stop paper trading
        almanak strat backtest paper stop -s momentum_v1

        # List all sessions
        almanak strat backtest paper status --all
    """
    pass


@paper.command("start")
@click.option(
    "--strategy",
    "-s",
    required=True,
    help="Name of the strategy to paper trade",
)
@click.option(
    "--chain",
    "-c",
    type=click.Choice(list(CHAIN_IDS.keys())),
    default="arbitrum",
    help="Target blockchain (default: arbitrum)",
)
@click.option(
    "--initial-eth",
    type=float,
    default=10.0,
    help="Initial ETH balance for paper wallet (default: 10)",
)
@click.option(
    "--initial-tokens",
    type=str,
    default="",
    help="Initial token balances as 'TOKEN:AMOUNT,TOKEN:AMOUNT' (e.g., 'USDC:10000,WETH:5')",
)
@click.option(
    "--tick-interval",
    type=int,
    default=60,
    help="Interval between trading ticks in seconds (default: 60)",
)
@click.option(
    "--max-ticks",
    type=int,
    default=None,
    help="Maximum number of ticks to run (default: unlimited)",
)
@click.option(
    "--duration",
    type=str,
    default=None,
    help="Session duration as human-readable string (e.g., '5m', '1h', '30s'). Mutually exclusive with --max-ticks.",
)
@click.option(
    "--rpc-url",
    type=str,
    default=None,
    help="Archive RPC URL to fork from (default: from environment)",
)
@click.option(
    "--anvil-port",
    type=int,
    default=8546,
    help="Port to run Anvil on (default: 8546)",
)
@click.option(
    "--no-reset-fork",
    is_flag=True,
    default=False,
    help="Don't reset fork to latest block each tick",
)
@click.option(
    "--output",
    "-o",
    type=click.Path(exists=False),
    default=None,
    help="Output file for session results (optional)",
)
@click.option(
    "--foreground",
    "-f",
    is_flag=True,
    default=False,
    help="Run in foreground instead of background",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Show configuration without starting session",
)
def paper_start(
    strategy: str,
    chain: str,
    initial_eth: float,
    initial_tokens: str,
    tick_interval: int,
    max_ticks: int | None,
    duration: str | None,
    rpc_url: str | None,
    anvil_port: int,
    no_reset_fork: bool,
    output: str | None,
    foreground: bool,
    dry_run: bool,
) -> None:
    """
    Start a paper trading session.

    Paper trading executes strategy decisions on a local Anvil fork,
    providing accurate simulation with real DeFi protocol interactions
    and full PnL tracking.

    The session runs in the background by default. Use --foreground to
    run interactively, or use 'paper status' to check progress.

    Prerequisites:
        - Foundry (Anvil) must be installed: curl -L https://foundry.paradigm.xyz | bash
        - Archive RPC access for the target chain

    Examples:

        # Basic paper trading on Arbitrum
        almanak strat backtest paper start -s momentum_v1 --chain arbitrum

        # With custom initial balances
        almanak strat backtest paper start -s lending_loop \\
            --chain arbitrum \\
            --initial-eth 20 \\
            --initial-tokens "USDC:50000,WETH:10"

        # Run for limited ticks
        almanak strat backtest paper start -s test_strategy --max-ticks 100

        # Run for a specific duration
        almanak strat backtest paper start -s test_strategy --duration 5m

        # Run in foreground (blocks terminal)
        almanak strat backtest paper start -s test_strategy --foreground
    """
    # Resolve --duration into max_ticks (mutually exclusive with --max-ticks)
    if duration is not None and max_ticks is not None:
        click.echo("Error: --duration and --max-ticks are mutually exclusive.", err=True)
        raise click.Abort()

    if duration is not None:
        if tick_interval <= 0:
            click.echo("Error: --tick-interval must be greater than 0.", err=True)
            raise click.Abort()
        duration_seconds = _parse_duration(duration)
        if duration_seconds is None:
            click.echo(
                f"Error: Invalid duration '{duration}'. Use format like '30s', '5m', '1h', '2h30m'.",
                err=True,
            )
            raise click.Abort()
        # +1 because the first tick executes immediately; N ticks = (N-1) sleep intervals
        max_ticks = max(1, duration_seconds // tick_interval + 1)
        click.echo(f"Duration {duration} -> {max_ticks} ticks at {tick_interval}s interval")
    # Validate strategy exists
    available_strategies = list_strategies_fn()
    if strategy not in available_strategies and available_strategies:
        click.echo(f"Error: Unknown strategy '{strategy}'", err=True)
        click.echo(f"Available strategies: {', '.join(sorted(available_strategies))}", err=True)
        raise click.Abort()

    # Check if session already running (via BackgroundPaperTrader)
    existing_state = load_paper_session_state(strategy)
    if existing_state and existing_state.get("status") == "running":
        pid = existing_state.get("pid")
        if pid and is_process_running(pid):
            click.echo(f"Error: Paper trading session for '{strategy}' is already running (PID: {pid})", err=True)
            click.echo(f"Use 'almanak strat backtest paper stop -s {strategy}' to stop it first.", err=True)
            raise click.Abort()

    # Determine RPC URL
    if rpc_url is None:
        # Try environment variables
        chain_upper = chain.upper()
        env_var_names = [
            f"ALMANAK_{chain_upper}_RPC_URL",
            f"{chain_upper}_RPC_URL",
            "ALMANAK_RPC_URL",
            "RPC_URL",
        ]
        for env_var in env_var_names:
            rpc_url = os.environ.get(env_var)
            if rpc_url:
                break

        if not rpc_url:
            click.echo(f"Error: No RPC URL provided for chain '{chain}'", err=True)
            click.echo(f"Set one of: {', '.join(env_var_names[:2])}", err=True)
            click.echo("Or use --rpc-url option", err=True)
            raise click.Abort()

    # Parse initial tokens from CLI flags
    cli_tokens: dict[str, Decimal] = {}
    if initial_tokens:
        try:
            for pair in initial_tokens.split(","):
                pair = pair.strip()
                if not pair:
                    continue
                if ":" not in pair:
                    raise ValueError(f"Invalid token entry '{pair}'")
                token, amount = pair.split(":", 1)
                token_key = token.strip().upper()
                amount_str = amount.strip()
                if not token_key or not amount_str:
                    raise ValueError(f"Invalid token entry '{pair}'")
                cli_tokens[token_key] = Decimal(amount_str)
        except Exception as e:
            click.echo(f"Error parsing initial-tokens: {e}", err=True)
            click.echo("Expected format: 'TOKEN:AMOUNT,TOKEN:AMOUNT'", err=True)
            raise click.Abort() from e

    # Load anvil_funding from strategy config.json if available (VIB-202)
    # CLI flags override config values when both are provided.
    config_eth: Decimal | None = None
    config_tokens: dict[str, Decimal] = {}
    try:
        strategy_config = load_strategy_config(strategy, chain)
        anvil_funding = strategy_config.get("anvil_funding", {})
        if anvil_funding:
            if not isinstance(anvil_funding, dict):
                raise ValueError(
                    f"anvil_funding must be an object mapping TOKEN->AMOUNT, got {type(anvil_funding).__name__}"
                )
            click.echo(f"Found anvil_funding in config: {anvil_funding}", err=True)
            # Use the same native token set as the gateway to correctly route
            # native gas tokens (ETH, AVAX, MNT, etc.) per chain.
            from almanak.gateway.managed import ManagedGateway

            native_symbols = ManagedGateway.NATIVE_TOKEN_SYMBOLS
            for token_name, amount in anvil_funding.items():
                token_upper = str(token_name).upper()
                if token_upper in native_symbols:
                    config_eth = Decimal(str(amount))
                else:
                    config_tokens[token_upper] = Decimal(str(amount))
    except Exception as e:
        click.echo(
            f"Warning: ignoring invalid anvil_funding in strategy config: {e}",
            err=True,
        )

    # Merge: config provides defaults, CLI flags override
    # initial_eth default is 10.0 from click — only use config_eth if CLI wasn't explicitly set
    # We detect "user explicitly passed --initial-eth" by checking the click context
    ctx = click.get_current_context()
    cli_eth_explicit = ctx.get_parameter_source("initial_eth") != click.core.ParameterSource.DEFAULT
    if config_eth is not None and not cli_eth_explicit:
        initial_eth = float(config_eth)
    # For tokens: start with config, overlay CLI
    parsed_tokens: dict[str, Decimal] = {**config_tokens, **cli_tokens}

    # Prepare output path
    output_path = Path(output) if output else None

    # Create config
    try:
        paper_config = PaperTraderConfig(
            chain=chain,
            rpc_url=rpc_url,
            strategy_id=strategy,
            initial_eth=Decimal(str(initial_eth)),
            initial_tokens=parsed_tokens,
            tick_interval_seconds=tick_interval,
            max_ticks=max_ticks,
            anvil_port=anvil_port,
            reset_fork_every_tick=not no_reset_fork,
        )
    except ValueError as e:
        click.echo(f"Configuration error: {e}", err=True)
        raise click.Abort() from e

    # Display configuration
    click.echo("=" * 60)
    click.echo("PAPER TRADING CONFIGURATION")
    click.echo("=" * 60)
    click.echo(f"Strategy: {strategy}")
    click.echo(f"Chain: {chain} (ID: {paper_config.chain_id})")
    click.echo(f"Initial ETH: {initial_eth}")
    if parsed_tokens:
        tokens_str = ", ".join(f"{k}: {v}" for k, v in parsed_tokens.items())
        click.echo(f"Initial Tokens: {tokens_str}")
    click.echo(f"Tick Interval: {tick_interval}s ({tick_interval / 60:.1f} min)")
    if max_ticks:
        click.echo(f"Max Ticks: {max_ticks}")
        if paper_config.max_duration_minutes:
            click.echo(f"Max Duration: ~{paper_config.max_duration_minutes:.1f} min")
    else:
        click.echo("Max Ticks: unlimited")
    click.echo(f"Anvil Port: {anvil_port}")
    click.echo(f"Reset Fork Each Tick: {not no_reset_fork}")
    click.echo(f"Mode: {'Foreground' if foreground else 'Background'}")

    if output_path:
        click.echo(f"Output: {output_path}")

    click.echo("=" * 60)

    # Handle dry run
    if dry_run:
        click.echo()
        click.echo("Dry run - paper trading not started.")
        return

    # Get strategy instance
    try:
        strategy_class = get_strategy(strategy)
    except ValueError:
        click.echo()
        click.echo("Warning: No strategies registered in factory.", err=True)
        click.echo("Running with mock strategy for demonstration.", err=True)
        click.echo()

        # Create a minimal mock strategy
        from ..data.market_snapshot import MarketSnapshot

        class MockPaperStrategy:
            """Mock strategy for paper trading demonstration."""

            strategy_id: str = strategy

            def __init__(self) -> None:
                self._iteration = 0

            def decide(self, market: MarketSnapshot) -> dict[str, Any] | None:
                self._iteration += 1
                # Return None (HOLD) for mock
                return None

        strategy_class = MockPaperStrategy

    # Create strategy instance
    strategy_config = load_strategy_config(strategy, chain)
    strategy_instance = _create_backtest_strategy(strategy_class, strategy_config, chain)

    # Ensure strategy has strategy_id
    if not hasattr(strategy_instance, "strategy_id"):
        strategy_instance.strategy_id = strategy

    if foreground:
        # Run in foreground (blocking)
        click.echo()
        click.echo("Starting paper trading in foreground...")
        click.echo("Press Ctrl+C to stop.")
        click.echo()

        try:
            summary = asyncio.run(
                _run_paper_trading_foreground(
                    strategy_instance=strategy_instance,
                    paper_config=paper_config,
                    output_path=output_path,
                )
            )

            # Display results
            click.echo()
            click.echo("=" * 60)
            click.echo("PAPER TRADING RESULTS")
            click.echo("=" * 60)
            click.echo(summary.summary())

            if output_path:
                output_data = summary.to_dict()
                output_data["_meta"] = {
                    "generated_at": datetime.now(UTC).isoformat(),
                    "generator": "almanak strat backtest paper",
                    "engine": "paper",
                }
                with open(output_path, "w") as f:
                    json.dump(output_data, f, indent=2, default=str)
                click.echo()
                click.echo(f"Results written to: {output_path}")

        except KeyboardInterrupt:
            click.echo()
            click.echo("Paper trading interrupted by user.")

    else:
        # Run in background using BackgroundPaperTrader
        click.echo()
        click.echo("Starting paper trading in background...")

        bg_trader = BackgroundPaperTrader(
            config=paper_config,
        )

        # Resolve strategy module/class via registry for correct paths
        strategy_cls = get_strategy(strategy)

        try:
            pid = bg_trader.start(
                strategy_module=strategy_cls.__module__,
                strategy_class=strategy_cls.__name__,
                strategy_config=strategy_config,
            )
        except RuntimeError as e:
            click.echo(f"Error: {e}", err=True)
            raise click.Abort() from e

        # Also save to the CLI session state for backwards compatibility
        start_time = datetime.now(UTC)
        save_paper_session_state(
            strategy_id=strategy,
            pid=pid,
            config=paper_config,
            start_time=start_time,
        )

        click.echo()
        click.echo(f"Paper trading session started for '{strategy}' (PID: {pid})")
        click.echo(f"State directory: {bg_trader.state_dir}")
        click.echo(f"Log file: {bg_trader.log_file}")
        click.echo()
        click.echo("To check status:")
        click.echo(f"  almanak strat backtest paper status -s {strategy}")
        click.echo()
        click.echo("To view logs:")
        click.echo(f"  almanak strat backtest paper logs -s {strategy}")
        click.echo()
        click.echo("To stop:")
        click.echo(f"  almanak strat backtest paper stop -s {strategy}")


async def _run_paper_trading_foreground(
    strategy_instance: Any,
    paper_config: PaperTraderConfig,
    output_path: Path | None,
) -> PaperTradingSummary:
    """Run paper trading in foreground mode.

    Args:
        strategy_instance: Strategy to paper trade
        paper_config: Paper trader configuration
        output_path: Optional output path for results

    Returns:
        PaperTradingSummary with session results
    """
    # Create fork manager with direct parameters
    fork_manager = RollingForkManager(
        rpc_url=paper_config.rpc_url,
        chain=paper_config.chain,
        anvil_port=paper_config.anvil_port,
        startup_timeout_seconds=paper_config.startup_timeout_seconds,
        auto_impersonate=paper_config.auto_impersonate,
        block_time=paper_config.block_time,
    )

    # Create portfolio tracker
    portfolio_tracker = PaperPortfolioTracker(
        strategy_id=paper_config.strategy_id,
        chain=paper_config.chain,
    )

    # Create paper trader
    trader = PaperTrader(
        fork_manager=fork_manager,
        portfolio_tracker=portfolio_tracker,
        config=paper_config,
    )

    # Run paper trading
    summary = await trader.run_loop(
        strategy=strategy_instance,
        max_ticks=paper_config.max_ticks,
    )

    return summary


@paper.command("stop")
@click.option(
    "--strategy",
    "-s",
    required=True,
    help="Name of the strategy to stop",
)
@click.option(
    "--force",
    is_flag=True,
    default=False,
    help="Force stop (kill process)",
)
def paper_stop(strategy: str, force: bool) -> None:
    """
    Stop a running paper trading session.

    This command signals the paper trading process to stop gracefully.
    The current tick will complete before stopping.

    Use --force to immediately terminate the process.

    Examples:

        # Graceful stop
        almanak strat backtest paper stop -s momentum_v1

        # Force stop
        almanak strat backtest paper stop -s momentum_v1 --force
    """
    # Try BackgroundPaperTrader first (engine state dir)
    # We need a minimal config just to locate the state files
    bg_trader = BackgroundPaperTrader(
        config=PaperTraderConfig(
            chain="arbitrum",  # Doesn't matter for stop, just need strategy_id
            rpc_url="http://localhost:8545",
            strategy_id=strategy,
        ),
    )

    bg_status = bg_trader.get_status()

    if bg_status.is_running:
        click.echo(f"Session: {strategy}")
        click.echo(f"Status: running (PID: {bg_status.pid})")

        if force:
            # Force kill
            if bg_status.pid:
                try:
                    os.kill(bg_status.pid, signal.SIGKILL)
                    click.echo(f"Process {bg_status.pid} killed forcefully.")
                except (ProcessLookupError, PermissionError) as e:
                    click.echo(f"Error: {e}", err=True)
        else:
            stopped = bg_trader.stop()
            if stopped:
                click.echo("Session stopped gracefully.")
            else:
                click.echo("Failed to stop session. Try --force.", err=True)

        # Also clean up CLI session state
        update_paper_session_status(strategy, "stopped")
        return

    # Fallback: check CLI session state
    state = load_paper_session_state(strategy)
    if not state:
        click.echo(f"No paper trading session found for '{strategy}'", err=True)
        return

    pid = state.get("pid")
    status = state.get("status", "unknown")

    click.echo(f"Session: {strategy}")
    click.echo(f"Status: {status}")
    click.echo(f"PID: {pid}")

    if status != "running":
        click.echo()
        click.echo(f"Session is not running (status: {status})")
        click.echo("Cleaning up session state...")
        delete_paper_session_state(strategy)
        click.echo("Done.")
        return

    if pid:
        if not is_process_running(pid):
            click.echo()
            click.echo("Process is no longer running.")
            update_paper_session_status(strategy, "stopped")
            click.echo("Session state updated.")
            return

        try:
            if force:
                os.kill(pid, signal.SIGKILL)
                click.echo()
                click.echo(f"Process {pid} killed forcefully.")
            else:
                os.kill(pid, signal.SIGTERM)
                click.echo()
                click.echo(f"Stop signal sent to process {pid}.")
                click.echo("Waiting for graceful shutdown...")

            update_paper_session_status(strategy, "stopped")
            click.echo("Session stopped.")

        except ProcessLookupError:
            click.echo()
            click.echo("Process no longer exists.")
            update_paper_session_status(strategy, "stopped")

        except PermissionError:
            click.echo()
            click.echo(f"Permission denied to stop process {pid}", err=True)
            click.echo("Try running with appropriate permissions.", err=True)

    else:
        click.echo()
        click.echo("No PID recorded for this session.")
        update_paper_session_status(strategy, "stopped")


@paper.command("status")
@click.option(
    "--strategy",
    "-s",
    required=False,
    default=None,
    help="Name of the strategy to check",
)
@click.option(
    "--all",
    "-a",
    "show_all",
    is_flag=True,
    default=False,
    help="Show all paper trading sessions",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    default=False,
    help="Show detailed session information",
)
def paper_status(strategy: str | None, show_all: bool, verbose: bool) -> None:
    """
    Check the status of paper trading sessions.

    Shows information about running and completed paper trading sessions.
    Reads from both the engine state directory (~/.almanak/paper/) and
    the CLI session directory for comprehensive status.

    Examples:

        # Check specific strategy
        almanak strat backtest paper status -s momentum_v1

        # List all sessions
        almanak strat backtest paper status --all

        # Detailed view
        almanak strat backtest paper status -s momentum_v1 --verbose
    """
    if show_all:
        # Show all sessions
        sessions = list_paper_sessions()
        if not sessions:
            click.echo("No paper trading sessions found.")
            return

        click.echo("=" * 60)
        click.echo("PAPER TRADING SESSIONS")
        click.echo("=" * 60)

        for session in sessions:
            strategy_id = session.get("strategy_id", "unknown")
            status = session.get("status", "unknown")
            pid = session.get("pid", "N/A")
            start_time = session.get("start_time", "N/A")

            # Check if process is running
            if pid and isinstance(pid, int):
                if is_process_running(pid):
                    status_display = f"running (PID: {pid})"
                else:
                    status_display = "stopped (process not found)"
            else:
                status_display = status

            click.echo(f"\nStrategy: {strategy_id}")
            click.echo(f"  Status: {status_display}")
            click.echo(f"  Started: {start_time}")

            if verbose:
                config = session.get("config", {})
                click.echo(f"  Chain: {config.get('chain', 'N/A')}")
                click.echo(f"  Tick Interval: {config.get('tick_interval_seconds', 'N/A')}s")
                click.echo(f"  Max Ticks: {config.get('max_ticks', 'unlimited')}")

                summary = session.get("summary")
                if summary:
                    click.echo(f"  Trades: {summary.get('successful_trades', 0)}")
                    click.echo(f"  Errors: {summary.get('failed_trades', 0)}")

        click.echo()
        return

    if not strategy:
        click.echo("Error: Please specify --strategy or use --all to list all sessions", err=True)
        raise click.Abort()

    # Check specific strategy -- try BackgroundPaperTrader engine state first
    bg_trader = BackgroundPaperTrader(
        config=PaperTraderConfig(
            chain="arbitrum",
            rpc_url="http://localhost:8545",
            strategy_id=strategy,
        ),
    )
    bg_status = bg_trader.get_status()

    if bg_status.is_running or bg_status.tick_count > 0:
        click.echo("=" * 60)
        click.echo(f"PAPER TRADING STATUS: {strategy}")
        click.echo("=" * 60)
        click.echo(f"Status: {'running' if bg_status.is_running else bg_status.status} (PID: {bg_status.pid or 'N/A'})")
        if bg_status.session_start:
            click.echo(f"Started: {bg_status.session_start.strftime('%Y-%m-%d %H:%M:%S')}")
        click.echo(f"Ticks: {bg_status.tick_count}")
        click.echo(f"Trades: {bg_status.trade_count}")
        click.echo(f"Errors: {bg_status.error_count}")
        if bg_status.last_save:
            click.echo(f"Last Save: {bg_status.last_save.strftime('%Y-%m-%d %H:%M:%S')}")
        if bg_status.can_resume:
            click.echo(f"Can Resume: yes (resume_count: {bg_status.resume_count})")
        return

    # Fallback: check CLI session state
    state = load_paper_session_state(strategy)
    if not state:
        click.echo(f"No paper trading session found for '{strategy}'")
        click.echo()
        click.echo("To start a session:")
        click.echo(f"  almanak strat backtest paper start -s {strategy}")
        return

    click.echo("=" * 60)
    click.echo(f"PAPER TRADING STATUS: {strategy}")
    click.echo("=" * 60)

    status = state.get("status", "unknown")
    pid = state.get("pid")
    start_time = state.get("start_time", "N/A")
    last_updated = state.get("last_updated")

    # Check if process is actually running
    if pid and isinstance(pid, int):
        if is_process_running(pid):
            actual_status = f"running (PID: {pid})"
        else:
            actual_status = "stopped (process not found)"
    else:
        actual_status = status

    click.echo(f"Status: {actual_status}")
    click.echo(f"Started: {start_time}")

    if last_updated:
        click.echo(f"Last Updated: {last_updated}")

    config = state.get("config", {})
    click.echo()
    click.echo("Configuration:")
    click.echo(f"  Chain: {config.get('chain', 'N/A')}")
    click.echo(f"  Tick Interval: {config.get('tick_interval_seconds', 'N/A')}s")
    click.echo(f"  Max Ticks: {config.get('max_ticks', 'unlimited')}")
    click.echo(f"  Initial ETH: {config.get('initial_eth', 'N/A')}")

    initial_tokens = config.get("initial_tokens", {})
    if initial_tokens:
        tokens_str = ", ".join(f"{k}: {v}" for k, v in initial_tokens.items())
        click.echo(f"  Initial Tokens: {tokens_str}")

    summary = state.get("summary")
    if summary:
        click.echo()
        click.echo("Results:")
        click.echo(f"  Duration: {summary.get('duration', 'N/A')}")
        click.echo(f"  Total Trades: {summary.get('total_trades', 0)}")
        click.echo(f"  Successful: {summary.get('successful_trades', 0)}")
        click.echo(f"  Failed: {summary.get('failed_trades', 0)}")
        if summary.get("pnl_usd"):
            click.echo(f"  PnL: ${float(summary['pnl_usd']):,.2f}")


def get_paper_log_file(strategy_id: str) -> Path:
    """Get the log file path for a paper trading session.

    Checks both the background paper trader state directory (~/.almanak/paper/)
    and the session state directory (~/.almanak/paper_sessions/) for log files.

    Args:
        strategy_id: Strategy identifier

    Returns:
        Path to the log file (may not exist)
    """
    # Primary location: background paper trader state directory
    background_log = Path.home() / ".almanak" / "paper" / f"{strategy_id}.log"
    if background_log.exists():
        return background_log

    # Fallback: session state directory
    session_log = PAPER_STATE_DIR / f"{strategy_id}.log"
    return session_log


@paper.command("logs")
@click.option(
    "--strategy",
    "-s",
    required=True,
    help="Name of the strategy to view logs for",
)
@click.option(
    "--lines",
    "-n",
    type=int,
    default=100,
    help="Number of lines to display (default: 100)",
)
@click.option(
    "--follow",
    "-f",
    is_flag=True,
    default=False,
    help="Follow log output in real-time (like tail -f)",
)
@click.option(
    "--all",
    "-a",
    "show_all",
    is_flag=True,
    default=False,
    help="Show all log entries (ignores --lines)",
)
def paper_logs(strategy: str, lines: int, follow: bool, show_all: bool) -> None:
    """
    View logs from a paper trading session.

    Displays recent log entries from the paper trading background process.
    Use --follow to watch logs in real-time (similar to tail -f).

    Examples:

        # View last 100 lines
        almanak strat backtest paper logs -s momentum_v1

        # View last 500 lines
        almanak strat backtest paper logs -s momentum_v1 -n 500

        # View all logs
        almanak strat backtest paper logs -s momentum_v1 --all

        # Follow logs in real-time
        almanak strat backtest paper logs -s momentum_v1 --follow
    """
    log_file = get_paper_log_file(strategy)

    if not log_file.exists():
        click.echo(f"No log file found for strategy '{strategy}'", err=True)
        click.echo()
        click.echo("Possible reasons:")
        click.echo("  - No paper trading session has been run for this strategy")
        click.echo("  - The session was run in foreground mode (no log file created)")
        click.echo("  - The log file was deleted")
        click.echo()
        click.echo(f"Expected log file location: {log_file}")
        return

    click.echo(f"Log file: {log_file}")
    click.echo("=" * 60)

    if follow:
        # Follow mode - continuously read new lines
        click.echo("Following logs... (press Ctrl+C to stop)")
        click.echo()

        try:
            import time as time_module

            with open(log_file) as f:
                # Move to end of file
                f.seek(0, 2)

                while True:
                    line = f.readline()
                    if line:
                        click.echo(line, nl=False)
                    else:
                        time_module.sleep(0.5)
        except KeyboardInterrupt:
            click.echo()
            click.echo("Stopped following logs.")
        except OSError as e:
            click.echo(f"Error reading log file: {e}", err=True)

    else:
        # Standard mode - read last N lines or all
        try:
            with open(log_file) as f:
                all_lines = f.readlines()

            if show_all:
                lines_to_show = all_lines
            else:
                lines_to_show = all_lines[-lines:]

            if not lines_to_show:
                click.echo("(Log file is empty)")
                return

            for line in lines_to_show:
                click.echo(line, nl=False)

            click.echo()
            click.echo("=" * 60)
            click.echo(f"Showing {len(lines_to_show)} of {len(all_lines)} total lines")

            if len(all_lines) > len(lines_to_show):
                click.echo(f"Use --all to see all {len(all_lines)} lines")
                click.echo("Use --follow to watch logs in real-time")

        except OSError as e:
            click.echo(f"Error reading log file: {e}", err=True)


# =============================================================================
# Dashboard Subcommand
# =============================================================================


@backtest.command("dashboard")
@click.option(
    "--port",
    "-p",
    type=int,
    default=8501,
    help="Port to run the Streamlit dashboard (default: 8501)",
)
@click.option(
    "--no-browser",
    is_flag=True,
    default=False,
    help="Don't automatically open the browser",
)
@click.option(
    "--host",
    type=str,
    default="localhost",
    help="Host to bind the server (default: localhost)",
)
def dashboard_cmd(port: int, no_browser: bool, host: str) -> None:
    """
    Launch the interactive backtest dashboard.

    Opens a Streamlit web application for exploring and comparing
    backtest results. Upload JSON files or provide them via command line.

    Examples:

        # Launch dashboard with default settings
        almanak backtest dashboard

        # Launch on custom port
        almanak backtest dashboard --port 8080

        # Launch without auto-opening browser
        almanak backtest dashboard --no-browser

        # Allow external connections
        almanak backtest dashboard --host 0.0.0.0
    """
    import subprocess
    import webbrowser
    from pathlib import Path as PathLib

    # Build Streamlit command
    cmd = [
        sys.executable,
        "-m",
        "streamlit",
        "run",
        "--server.port",
        str(port),
        "--server.address",
        host,
    ]

    # Disable browser if requested
    if no_browser:
        cmd.extend(["--server.headless", "true"])

    # Add the module path
    # Streamlit needs the actual file path, not the module
    try:
        from almanak.framework.backtesting.dashboard import app as dashboard_app

        dashboard_path = PathLib(dashboard_app.__file__)
        cmd.append(str(dashboard_path))
    except ImportError as e:
        click.echo("Error: Could not find dashboard module.", err=True)
        click.echo("Make sure Streamlit is installed: pip install streamlit", err=True)
        raise click.Abort() from e

    # Display startup info
    click.echo("=" * 60)
    click.echo("BACKTEST DASHBOARD")
    click.echo("=" * 60)
    click.echo("Starting Streamlit dashboard...")
    click.echo(f"Host: {host}")
    click.echo(f"Port: {port}")

    url = f"http://{host}:{port}"
    click.echo(f"URL: {url}")
    click.echo()
    click.echo("Press Ctrl+C to stop the server")
    click.echo("=" * 60)

    # Open browser automatically unless disabled
    if not no_browser:
        # Small delay to let server start
        def open_browser() -> None:
            import time

            time.sleep(1.5)
            webbrowser.open(url)

        import threading

        threading.Thread(target=open_browser, daemon=True).start()

    # Run Streamlit
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        click.echo(f"Error: Streamlit failed to start (exit code {e.returncode})", err=True)
        raise click.Abort() from e
    except FileNotFoundError as e:
        click.echo("Error: Streamlit not found. Install it with: pip install streamlit", err=True)
        raise click.Abort() from e
    except KeyboardInterrupt:
        click.echo("\nDashboard stopped.")


if __name__ == "__main__":
    backtest()
