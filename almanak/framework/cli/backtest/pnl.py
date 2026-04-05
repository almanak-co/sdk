"""PnL backtest CLI command.

This module provides the `pnl` subcommand for historical price-based backtesting.
"""

import asyncio
import json
import sys
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import click

from ...backtesting import (
    CoinGeckoDataProvider,
    PnLBacktestConfig,
    PnLBacktester,
)
from ...backtesting.pnl.config_loader import ConfigLoadError, load_config_from_result
from ...backtesting.pnl.logging_utils import configure_backtest_logging
from ...backtesting.visualization import save_chart
from ...data.cache import CacheStats, DataCache
from ...strategies import get_strategy
from .group import backtest
from .helpers import (
    _create_backtest_strategy,
    list_strategies_fn,
    load_strategy_config,
    parse_date,
)


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

        from ...strategies import MarketSnapshot

        class MockPnLStrategy:
            """Mock strategy for PnL backtesting demonstration."""

            strategy_id: str = f"mock-{datetime.now().strftime('%Y%m%d%H%M%S')}"

            def __init__(self, config: dict[str, Any]) -> None:
                self.config = config
                self._iteration = 0

            def decide(self, market: MarketSnapshot) -> dict[str, Any] | None:
                self._iteration += 1
                return None

        strategy_class = MockPnLStrategy

    # Create strategy instance
    strategy_instance = _create_backtest_strategy(strategy_class, strategy_config, chain)

    # Ensure strategy has a non-empty strategy_id.
    existing_id = getattr(strategy_instance, "strategy_id", "")
    if not existing_id:
        fallback_id = (
            strategy_config.get("strategy_id")
            or strategy_config.get("name")
            or strategy
            or strategy_instance.__class__.__name__
        )
        if hasattr(strategy_instance, "_strategy_id"):
            strategy_instance._strategy_id = fallback_id
        else:
            strategy_instance.strategy_id = fallback_id

    # Create data provider
    click.echo()
    click.echo("Initializing CoinGecko data provider...")
    from ...backtesting.pnl.providers.coingecko import RetryConfig

    data_provider = CoinGeckoDataProvider(
        retry_config=RetryConfig.for_backtest(),
        persistent_cache=True,
        historical_cache_ttl=0,
    )

    # Initialize data cache for tracking stats
    cache: DataCache | None = None
    cache_stats: CacheStats | None = None

    # Warm cache if requested
    if warm_cache:
        click.echo()
        click.echo("Warming data cache...")
        cache = DataCache(ttl_seconds=0)
        cache.reset_stats()

        async def warm_cache_async() -> int:
            """Pre-fetch OHLCV data and store in cache."""
            from ...data.cache import CacheKey, OHLCVData

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

        # Create fresh data provider
        data_provider = CoinGeckoDataProvider(
            retry_config=RetryConfig.for_backtest(),
            persistent_cache=True,
            historical_cache_ttl=0,
        )

    # Create backtester
    backtester = PnLBacktester(
        data_provider=data_provider,
        fee_models={},
        slippage_models={},
    )

    # Run backtest
    click.echo()
    click.echo("Starting PnL backtest...")
    click.echo()

    try:
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
            from ...backtesting.pnl.calculators.benchmark import (
                calculate_alpha,
                calculate_beta,
                calculate_information_ratio,
            )
            from ...backtesting.pnl.providers.benchmark import (
                Benchmark,
                get_benchmark_returns,
                get_benchmark_total_return,
            )

            benchmark_enum = Benchmark.from_string(benchmark)

            async def _fetch_benchmark():
                returns = await get_benchmark_returns(benchmark_enum, start, end, interval)
                total = await get_benchmark_total_return(benchmark_enum, start, end)
                return returns, total

            benchmark_returns, benchmark_total = asyncio.run(_fetch_benchmark())

            if result.equity_curve and len(result.equity_curve) >= 2:
                strategy_returns = []
                for i in range(1, len(result.equity_curve)):
                    prev_val = result.equity_curve[i - 1].value_usd
                    curr_val = result.equity_curve[i].value_usd
                    if prev_val > 0:
                        strategy_returns.append((curr_val - prev_val) / prev_val)
                    else:
                        strategy_returns.append(Decimal("0"))

                min_len = min(len(strategy_returns), len(benchmark_returns))
                if min_len >= 2:
                    strategy_returns = strategy_returns[:min_len]
                    benchmark_returns = benchmark_returns[:min_len]

                    info_ratio = calculate_information_ratio(strategy_returns, benchmark_returns)
                    beta_val = calculate_beta(strategy_returns, benchmark_returns)

                    strategy_total = (
                        result.metrics.total_return_pct / Decimal("100")
                        if result.metrics.total_return_pct
                        else Decimal("0")
                    )
                    alpha_val = calculate_alpha(strategy_total, benchmark_total, beta_val)

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

    # Display cache statistics
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

        if cache_stats is not None:
            output_data["cache_stats"] = cache_stats.to_dict()

        with open(output_path, "w") as f:
            json.dump(output_data, f, indent=2, default=str)

        click.echo(f"Results written to: {output_path}")

    # Generate chart if requested
    if chart:
        click.echo()
        click.echo("Generating equity curve chart...")

        if output_path:
            chart_extension = ".html" if chart_format.lower() == "html" else ".png"
            chart_path = output_path.with_suffix(chart_extension)
        else:
            safe_strategy_name = strategy.replace("/", "_").replace("\\", "_") if strategy else "backtest"
            chart_extension = ".html" if chart_format.lower() == "html" else ".png"
            chart_path = Path(f"equity_curve_{safe_strategy_name}{chart_extension}")

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
        from ...backtesting.report_generator import generate_report

        click.echo()
        click.echo("Generating HTML report...")

        if output_path:
            report_path = output_path.with_suffix(".html")
        else:
            safe_strategy_name = strategy.replace("/", "_").replace("\\", "_") if strategy else "backtest"
            report_path = Path(f"backtest_report_{safe_strategy_name}.html")

        report_result = generate_report(result, output_path=report_path)

        if report_result.success:
            click.echo(f"Report saved to: {report_result.file_path}")
        else:
            click.echo(f"Warning: Failed to generate report: {report_result.error}", err=True)

    # Post-backtest tip
    click.echo()
    click.echo("Tip: Try 'almanak backtest sweep' to test multiple parameter combinations,")
    click.echo("     or 'almanak backtest optimize' for Bayesian hyperparameter tuning.")
