"""Advanced backtest CLI commands: walk-forward, monte-carlo, scenario.

This module provides advanced backtesting subcommands for robust optimization,
statistical simulation, and crisis scenario stress testing.
"""

import asyncio
import json
import sys
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import click

from almanak.core.chains import DEFAULT_CHAIN

from ...backtesting import (
    CoinGeckoDataProvider,
    PnLBacktestConfig,
    PnLBacktester,
)
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
from ...strategies import get_strategy
from .group import backtest
from .helpers import (
    _create_backtest_strategy,
    list_strategies_fn,
    load_strategy_config,
    parse_date,
)
from .sweep import load_optimization_config, parse_param_ranges_from_config

# =============================================================================
# Walk-Forward Results Display
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
        click.echo("\u26a0\ufe0f  WARNING: Potential overfitting detected!")
        click.echo("   Training performance significantly exceeds test performance.")
    else:
        click.echo("\u2713 No significant overfitting detected.")

    click.echo()

    # Parameter stability
    if result.parameter_stability:
        click.echo("-" * 70)
        click.echo("PARAMETER STABILITY")
        click.echo("-" * 70)

        if result.has_parameter_instability:
            click.echo("\u26a0\ufe0f  WARNING: Some parameters show instability across windows:")
            for param_name in result.unstable_parameters:
                stability = result.parameter_stability[param_name]
                click.echo(f"   - {param_name}: CV={stability.cv:.2%} (threshold={stability.stability_threshold:.0%})")
        else:
            click.echo("\u2713 All parameters show stable optimization across windows.")

        click.echo()
        click.echo("Parameter Statistics:")
        for name, stability in result.parameter_stability.items():
            stable_marker = "\u2713" if stability.is_stable else "\u26a0\ufe0f"
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


# =============================================================================
# Monte Carlo Results Display
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


# =============================================================================
# Crisis Scenario Results Display
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
        click.echo(f"  Return Difference:      {float(return_diff):+.2f}%")

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


# =============================================================================
# Walk-Forward Command
# =============================================================================


# crap-allowlist: VIB-4062 — pre-existing CC=26 in walk-forward CLI; touched only to repoint MarketSnapshot import
@backtest.command("walk-forward")
@click.option("--strategy", "-s", required=True, help="Name of the strategy to optimize")
@click.option("--start", required=True, callback=parse_date, help="Start date (YYYY-MM-DD)")
@click.option("--end", required=True, callback=parse_date, help="End date (YYYY-MM-DD)")
@click.option(
    "--config-file",
    "-f",
    type=click.Path(exists=True),
    required=True,
    help="Path to optimization config JSON file with parameter ranges",
)
@click.option("--train-days", type=int, default=90, help="Training window size in days (default: 90)")
@click.option("--test-days", type=int, default=30, help="Test window size in days (default: 30)")
@click.option(
    "--step-days",
    type=int,
    default=None,
    help="Step size in days between windows (default: test-days, non-overlapping)",
)
@click.option("--gap-days", type=int, default=0, help="Gap between train and test in days (default: 0)")
@click.option("--min-windows", type=int, default=2, help="Minimum number of windows required (default: 2)")
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
@click.option("--patience", type=int, default=None, help="Early stopping patience per window (default: from config)")
@click.option("--interval", type=int, default=3600, help="Interval between ticks in seconds (default: 3600 = 1 hour)")
@click.option(
    "--initial-capital", type=float, default=10000.0, help="Initial portfolio balance in USD (default: 10000)"
)
@click.option("--chain", "-c", type=str, default=DEFAULT_CHAIN, help=f"Target blockchain (default: {DEFAULT_CHAIN})")
@click.option(
    "--tokens", type=str, default="WETH,USDC", help="Comma-separated list of tokens to track (default: WETH,USDC)"
)
@click.option(
    "--output", "-o", type=click.Path(exists=False), default=None, help="Output file for full JSON results (optional)"
)
@click.option(
    "--dry-run", is_flag=True, default=False, help="Show configuration without running walk-forward optimization"
)
@click.option("--verbose", "-v", is_flag=True, default=False, help="Show progress bar and detailed logging")
def walk_forward_backtest(  # noqa: C901
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
    from ...backtesting.pnl import (
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

        from ...market import MarketSnapshot

        class MockWalkForwardStrategy:
            """Mock strategy for walk-forward demonstration."""

            deployment_id: str = "mock-walk-forward"

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
        # gas_price_gwei omitted: chain-aware default (VIB-5088)
        include_gas_costs=True,
    )

    # Create factories
    def create_data_provider() -> CoinGeckoDataProvider:
        return CoinGeckoDataProvider()

    def create_strategy(config_overrides: dict[str, Any] | None = None) -> Any:
        effective_config = {**base_config, **(config_overrides or {})}
        return _create_backtest_strategy(strategy_class, effective_config, chain)

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
# Monte Carlo Command
# =============================================================================


# crap-allowlist: VIB-4062 — pre-existing CC=17 in monte-carlo CLI; same import-only touch
@backtest.command("monte-carlo")
@click.option("--strategy", "-s", required=True, help="Name of the strategy to simulate")
@click.option("--start", required=True, callback=parse_date, help="Start date for historical data (YYYY-MM-DD)")
@click.option("--end", required=True, callback=parse_date, help="End date for historical data (YYYY-MM-DD)")
@click.option("--n-paths", "-n", type=int, default=100, help="Number of price paths to simulate (default: 100)")
@click.option(
    "--method",
    type=click.Choice(["gbm", "bootstrap"]),
    default="gbm",
    help="Price path generation method (default: gbm - Geometric Brownian Motion)",
)
@click.option("--interval", type=int, default=3600, help="Interval between ticks in seconds (default: 3600 = 1 hour)")
@click.option(
    "--initial-capital", type=float, default=10000.0, help="Initial portfolio balance in USD (default: 10000)"
)
@click.option("--chain", "-c", type=str, default=DEFAULT_CHAIN, help=f"Target blockchain (default: {DEFAULT_CHAIN})")
@click.option(
    "--tokens", type=str, default="WETH,USDC", help="Comma-separated list of tokens to track (default: WETH,USDC)"
)
@click.option("--base-token", type=str, default="WETH", help="Token to simulate price paths for (default: WETH)")
@click.option(
    "--parallel-workers", "-j", type=int, default=4, help="Number of parallel workers for backtests (default: 4)"
)
@click.option("--seed", type=int, default=None, help="Random seed for reproducibility")
@click.option(
    "--output", "-o", type=click.Path(exists=False), default=None, help="Output file for full JSON results (optional)"
)
@click.option("--dry-run", is_flag=True, default=False, help="Show configuration without running simulation")
@click.option("--verbose", "-v", is_flag=True, default=False, help="Show progress during simulation")
def monte_carlo_backtest(  # noqa: C901
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
    from ...backtesting.pnl import (
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

        from ...market import MarketSnapshot

        class MockMonteCarloStrategy:
            """Mock strategy for Monte Carlo demonstration."""

            deployment_id: str = "mock-monte-carlo"

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
        # gas_price_gwei omitted: chain-aware default (VIB-5088)
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
        return [ohlcv.close for ohlcv in ohlcv_data]

    try:
        historical_prices = asyncio.run(fetch_historical_prices())
    except Exception as e:
        # If CoinGecko fails, generate synthetic historical data
        click.echo(f"Warning: Could not fetch historical prices: {e}", err=True)
        click.echo("Using synthetic historical data for demonstration...", err=True)

        import random

        if seed is not None:
            random.seed(seed)
        n_steps = max(10, duration_days * (86400 // interval))
        price = Decimal("3000")
        historical_prices = [price]
        for _ in range(n_steps - 1):
            change = Decimal(str(random.gauss(0, 0.02)))
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
        collect_individual_results=False,
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
        click.echo()

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
# Scenario Command
# =============================================================================


# crap-allowlist: VIB-4062 — pre-existing CC=31 in scenario CLI; same import-only touch
@backtest.command("scenario")
@click.option("--strategy", "-s", required=False, default=None, help="Name of the strategy to backtest")
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
@click.option("--name", required=False, default=None, help="Custom scenario name (used with --scenario=custom)")
@click.option(
    "--description", required=False, default=None, help="Custom scenario description (used with --scenario=custom)"
)
@click.option("--interval", type=int, default=3600, help="Interval between ticks in seconds (default: 3600 = 1 hour)")
@click.option(
    "--initial-capital", type=float, default=10000.0, help="Initial portfolio balance in USD (default: 10000)"
)
@click.option("--chain", "-c", type=str, default=DEFAULT_CHAIN, help=f"Target blockchain (default: {DEFAULT_CHAIN})")
@click.option(
    "--tokens", type=str, default="WETH,USDC", help="Comma-separated list of tokens to track (default: WETH,USDC)"
)
@click.option("--gas-price", type=float, default=50.0, help="Gas price in gwei (default: 50 for crisis periods)")
@click.option("--mev/--no-mev", default=True, help="Enable/disable MEV simulation (default: enabled for crisis)")
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
    "--output", "-o", type=click.Path(exists=False), default=None, help="Output file for full JSON results (optional)"
)
@click.option("--dry-run", is_flag=True, default=False, help="Show configuration without running backtest")
@click.option("--list-scenarios", is_flag=True, default=False, help="List all available predefined scenarios")
@click.option("--verbose", "-v", is_flag=True, default=False, help="Show detailed progress during backtest")
def scenario_backtest(  # noqa: C901
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

    if scenario is None:
        click.echo("Error: --scenario is required. Use --list-scenarios to see options.", err=True)
        raise click.Abort()

    # Resolve scenario
    crisis_scenario: CrisisScenario | None = None

    if scenario.lower() == "custom":
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
        if normal_start is None:
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

        from ...market import MarketSnapshot

        class MockCrisisStrategy:
            """Mock strategy for crisis scenario demonstration."""

            deployment_id: str = "mock-crisis"

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

                comparison = compare_crisis_to_normal(result.result, normal_result)
                result.crisis_metrics["normal_period_comparison"] = comparison

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
