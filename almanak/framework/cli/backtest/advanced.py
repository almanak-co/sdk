"""Advanced backtest CLI commands: walk-forward, monte-carlo, scenario.

This module provides advanced backtesting subcommands for robust optimization,
statistical simulation, and crisis scenario stress testing.
"""

import asyncio
import json
import sys
from dataclasses import dataclass
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
from .run_helpers import build_token_address_map
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


@dataclass
class _WalkForwardSettings:
    objective: str
    n_trials: int
    patience: int | None


@dataclass
class _WalkForwardWindowSummary:
    total_duration_days: int
    effective_step_days: int
    estimated_windows: int


@dataclass
class _WalkForwardRunContext:
    strategy: str
    start: datetime
    end: datetime
    chain: str
    token_list: list[str]
    interval: int
    output_label: str | None
    output_path: Path | None
    train_days: int
    test_days: int
    step_days: int | None
    gap_days: int
    min_windows: int
    wf_config: Any
    param_ranges: dict[str, Any]
    settings: _WalkForwardSettings
    window_summary: _WalkForwardWindowSummary
    verbose: bool


@dataclass
class _WalkForwardFactories:
    strategy_factory: Any
    data_provider_factory: Any
    backtester_factory: Any


def _load_walk_forward_inputs(
    *,
    config_file: str,
    objective: str | None,
    n_trials: int | None,
    patience: int | None,
) -> tuple[dict[str, Any], _WalkForwardSettings]:
    config_path = Path(config_file)
    try:
        opt_config = load_optimization_config(config_path)
    except Exception as e:
        click.echo(f"Error loading config file: {e}", err=True)
        raise click.Abort() from None

    try:
        param_ranges = parse_param_ranges_from_config(opt_config)
    except click.BadParameter as e:
        click.echo(f"Error parsing config: {e}", err=True)
        raise click.Abort() from None

    if not param_ranges:
        raise click.UsageError(
            "No parameter ranges defined in config file. Add 'param_ranges' with at least one parameter."
        )

    settings = _WalkForwardSettings(
        objective=objective or opt_config.get("objective", "sharpe_ratio"),
        n_trials=n_trials or opt_config.get("n_trials", 50),
        patience=patience or opt_config.get("patience"),
    )
    return param_ranges, settings


def _validate_walk_forward_strategy(strategy: str) -> None:
    available_strategies = list_strategies_fn()
    if strategy not in available_strategies and available_strategies:
        click.echo(f"Error: Unknown strategy '{strategy}'", err=True)
        click.echo(f"Available strategies: {', '.join(sorted(available_strategies))}", err=True)
        raise click.Abort()


def _estimate_walk_forward_windows(
    *,
    start: datetime,
    end: datetime,
    train_days: int,
    test_days: int,
    step_days: int | None,
    gap_days: int,
) -> _WalkForwardWindowSummary:
    total_duration = (end - start).days
    window_size = train_days + gap_days + test_days
    effective_step = step_days if step_days else test_days
    estimated_windows = max(0, (total_duration - window_size) // effective_step + 1)
    return _WalkForwardWindowSummary(
        total_duration_days=total_duration,
        effective_step_days=effective_step,
        estimated_windows=estimated_windows,
    )


def _build_walk_forward_context(
    *,
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
    chain: str,
    tokens: str,
    output: str | None,
    verbose: bool,
) -> _WalkForwardRunContext:
    from ...backtesting.pnl import WalkForwardConfig

    param_ranges, settings = _load_walk_forward_inputs(
        config_file=config_file,
        objective=objective,
        n_trials=n_trials,
        patience=patience,
    )
    wf_config = WalkForwardConfig.from_days(
        train_days=train_days,
        test_days=test_days,
        step_days=step_days,
        gap_days=gap_days,
        min_windows=min_windows,
    )
    _validate_walk_forward_strategy(strategy)

    return _WalkForwardRunContext(
        strategy=strategy,
        start=start,
        end=end,
        chain=chain,
        token_list=_parse_cli_tokens(tokens),
        interval=interval,
        output_label=output,
        output_path=Path(output) if output else None,
        train_days=train_days,
        test_days=test_days,
        step_days=step_days,
        gap_days=gap_days,
        min_windows=min_windows,
        wf_config=wf_config,
        param_ranges=param_ranges,
        settings=settings,
        window_summary=_estimate_walk_forward_windows(
            start=start,
            end=end,
            train_days=train_days,
            test_days=test_days,
            step_days=step_days,
            gap_days=gap_days,
        ),
        verbose=verbose,
    )


def _format_walk_forward_param_range(name: str, spec: Any) -> str:
    if not hasattr(spec, "param_type"):
        return f"  {name}: {spec}"
    if spec.param_type.value == "categorical":
        return f"  {name}: categorical {spec.choices}"
    if spec.param_type.value == "discrete":
        step_str = f", step={spec.step}" if spec.step else ""
        return f"  {name}: discrete [{spec.low}, {spec.high}{step_str}]"
    log_str = " (log)" if spec.log else ""
    step_str = f", step={spec.step}" if spec.step else ""
    return f"  {name}: continuous [{spec.low}, {spec.high}{step_str}]{log_str}"


def _print_walk_forward_configuration(ctx: _WalkForwardRunContext) -> None:
    click.echo("=" * 70)
    click.echo("WALK-FORWARD OPTIMIZATION CONFIGURATION")
    click.echo("=" * 70)
    click.echo(f"Strategy: {ctx.strategy}")
    click.echo(f"Chain: {ctx.chain}")
    click.echo(f"Period: {ctx.start.date()} -> {ctx.end.date()} ({ctx.window_summary.total_duration_days} days)")
    click.echo(f"Interval: {ctx.interval}s ({ctx.interval / 3600:.1f} hours)")
    click.echo(f"Tokens: {', '.join(ctx.token_list)}")
    click.echo()
    click.echo("Window Configuration:")
    click.echo(f"  Train Window: {ctx.train_days} days")
    click.echo(f"  Test Window: {ctx.test_days} days")
    click.echo(f"  Step Size: {ctx.window_summary.effective_step_days} days")
    click.echo(f"  Gap: {ctx.gap_days} days")
    click.echo(f"  Min Windows: {ctx.min_windows}")
    click.echo(f"  Estimated Windows: ~{ctx.window_summary.estimated_windows}")
    click.echo()
    click.echo(f"Optimization: {ctx.settings.objective}")
    click.echo(f"Trials per Window: {ctx.settings.n_trials}")
    if ctx.settings.patience:
        click.echo(f"Early Stopping: patience={ctx.settings.patience}")
    else:
        click.echo("Early Stopping: disabled")
    click.echo()
    click.echo("Parameters to optimize:")
    for name, spec in ctx.param_ranges.items():
        click.echo(_format_walk_forward_param_range(name, spec))

    if ctx.output_label:
        click.echo(f"Output: {ctx.output_label}")

    click.echo("=" * 70)


def _handle_walk_forward_dry_run() -> None:
    click.echo()
    click.echo("Dry run - walk-forward optimization not executed.")


def _resolve_walk_forward_strategy_class(strategy: str) -> Any:
    try:
        return get_strategy(strategy)
    except ValueError:
        click.echo()
        click.echo("Warning: No strategies registered in factory.", err=True)
        click.echo("Running with mock strategy for demonstration.", err=True)
        click.echo()

        from ...backtesting import make_mock_strategy_class

        return make_mock_strategy_class("mock-walk-forward")


def _build_walk_forward_pnl_config(
    ctx: _WalkForwardRunContext,
    *,
    token_funding: list[dict[str, Any]] | None,
) -> PnLBacktestConfig:
    return PnLBacktestConfig(
        start_time=ctx.start,
        end_time=ctx.end,
        interval_seconds=ctx.interval,
        token_funding=token_funding,
        chain=ctx.chain,
        tokens=ctx.token_list,
        # gas_price_gwei omitted: chain-aware default (VIB-5088)
        include_gas_costs=True,
    )


def _build_walk_forward_factories(
    *,
    strategy_class: Any,
    base_config: dict[str, Any],
    chain: str,
    token_addresses: dict[str, Any],
) -> _WalkForwardFactories:
    def create_data_provider() -> CoinGeckoDataProvider:
        return CoinGeckoDataProvider(token_addresses=token_addresses)

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

    return _WalkForwardFactories(
        strategy_factory=create_strategy,
        data_provider_factory=create_data_provider,
        backtester_factory=create_backtester,
    )


def _run_walk_forward(
    *,
    ctx: _WalkForwardRunContext,
    factories: _WalkForwardFactories,
    pnl_config: PnLBacktestConfig,
    base_config: dict[str, Any],
) -> Any:
    from ...backtesting.pnl import run_walk_forward_optimization

    click.echo()
    click.echo(
        "Starting walk-forward optimization "
        f"(~{ctx.window_summary.estimated_windows} windows, {ctx.settings.n_trials} trials per window)..."
    )
    click.echo()

    try:
        return asyncio.run(
            run_walk_forward_optimization(
                strategy_factory=factories.strategy_factory,
                data_provider_factory=factories.data_provider_factory,
                backtester_factory=factories.backtester_factory,
                base_config=pnl_config,
                param_ranges=ctx.param_ranges,
                wf_config=ctx.wf_config,
                objective_metric=ctx.settings.objective,
                n_trials_per_window=ctx.settings.n_trials,
                patience=ctx.settings.patience,
                show_progress=ctx.verbose,
                strategy_config=base_config,
            )
        )
    except ValueError as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"Error during walk-forward optimization: {e}", err=True)
        sys.exit(1)


def _write_walk_forward_output(output_path: Path | None, result: Any) -> None:
    if output_path is None:
        return
    try:
        result_dict = result.to_dict()
        with open(output_path, "w") as f:
            json.dump(result_dict, f, indent=2, default=str)
        click.echo(f"Walk-forward results written to: {output_path}")
    except Exception as e:
        click.echo(f"Warning: Could not save results: {e}", err=True)


def _parse_cli_tokens(tokens: str) -> list[str]:
    token_list = [token.strip().upper() for token in tokens.split(",")]
    if not token_list or any(not token for token in token_list):
        raise click.BadParameter("Expected comma-separated non-empty token symbols.", param_hint="--tokens")
    return token_list


def _write_json_output(output: str | Path | None, result: Any, label: str) -> None:
    if output is None:
        return
    output_path = Path(output)
    try:
        result_dict = result.to_dict()
        with open(output_path, "w") as f:
            json.dump(result_dict, f, indent=2, default=str)
        click.echo(f"{label} results written to: {output_path}")
    except Exception as e:
        click.echo(f"Warning: Could not save results: {e}", err=True)


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
    ctx = _build_walk_forward_context(
        strategy=strategy,
        start=start,
        end=end,
        config_file=config_file,
        train_days=train_days,
        test_days=test_days,
        step_days=step_days,
        gap_days=gap_days,
        min_windows=min_windows,
        objective=objective,
        n_trials=n_trials,
        patience=patience,
        interval=interval,
        chain=chain,
        tokens=tokens,
        output=output,
        verbose=verbose,
    )
    _print_walk_forward_configuration(ctx)

    if dry_run:
        _handle_walk_forward_dry_run()
        return

    strategy_class = _resolve_walk_forward_strategy_class(ctx.strategy)
    base_config = load_strategy_config(ctx.strategy, ctx.chain)
    pnl_config = _build_walk_forward_pnl_config(ctx, token_funding=base_config.get("token_funding"))

    # Resolve the SYMBOL -> (chain, address) map once for every provider the
    # factory builds (Refinement R1). Natives resolve via the chain registry.
    token_addresses = build_token_address_map(
        strategy_config=base_config,
        tracked_tokens=ctx.token_list,
        chain=ctx.chain,
    )
    factories = _build_walk_forward_factories(
        strategy_class=strategy_class,
        base_config=base_config,
        chain=ctx.chain,
        token_addresses=token_addresses,
    )
    result = _run_walk_forward(
        ctx=ctx,
        factories=factories,
        pnl_config=pnl_config,
        base_config=base_config,
    )

    print_walk_forward_results(result, ctx.settings.objective)
    _write_walk_forward_output(ctx.output_path, result)


# =============================================================================
# Monte Carlo Command
# =============================================================================


@dataclass
class _MonteCarloRunContext:
    strategy: str
    start: datetime
    end: datetime
    n_paths: int
    method: str
    path_method: Any
    interval: int
    chain: str
    token_list: list[str]
    base_token: str
    parallel_workers: int
    seed: int | None
    output: str | None
    verbose: bool

    @property
    def duration_days(self) -> int:
        return (self.end - self.start).days


def _build_monte_carlo_context(
    *,
    strategy: str,
    start: datetime,
    end: datetime,
    n_paths: int,
    method: str,
    interval: int,
    chain: str,
    tokens: str,
    base_token: str,
    parallel_workers: int,
    seed: int | None,
    output: str | None,
    verbose: bool,
) -> _MonteCarloRunContext:
    from ...backtesting.pnl import PathGenerationMethod

    _validate_walk_forward_strategy(strategy)
    return _MonteCarloRunContext(
        strategy=strategy,
        start=start,
        end=end,
        n_paths=n_paths,
        method=method,
        path_method=PathGenerationMethod.BOOTSTRAP if method == "bootstrap" else PathGenerationMethod.GBM,
        interval=interval,
        chain=chain,
        token_list=_parse_cli_tokens(tokens),
        base_token=base_token.strip().upper(),
        parallel_workers=parallel_workers,
        seed=seed,
        output=output,
        verbose=verbose,
    )


def _print_monte_carlo_configuration(ctx: _MonteCarloRunContext) -> None:
    click.echo("=" * 70)
    click.echo("MONTE CARLO SIMULATION CONFIGURATION")
    click.echo("=" * 70)
    click.echo(f"Strategy: {ctx.strategy}")
    click.echo(f"Chain: {ctx.chain}")
    click.echo(f"Historical Period: {ctx.start.date()} -> {ctx.end.date()} ({ctx.duration_days} days)")
    click.echo(f"Interval: {ctx.interval}s ({ctx.interval / 3600:.1f} hours)")
    click.echo(f"Tokens: {', '.join(ctx.token_list)}")
    click.echo(f"Base Token (simulated): {ctx.base_token}")
    click.echo()
    click.echo("Simulation Parameters:")
    click.echo(f"  Number of Paths: {ctx.n_paths}")
    click.echo(f"  Generation Method: {ctx.method.upper()}")
    click.echo(f"  Parallel Workers: {ctx.parallel_workers}")
    click.echo(f"  Random Seed: {ctx.seed}" if ctx.seed is not None else "  Random Seed: None (random)")
    if ctx.output:
        click.echo(f"Output: {ctx.output}")
    click.echo("=" * 70)


def _handle_monte_carlo_dry_run() -> None:
    click.echo()
    click.echo("Dry run - Monte Carlo simulation not executed.")


def _resolve_demo_strategy_class(strategy: str, deployment_id: str) -> Any:
    try:
        return get_strategy(strategy)
    except ValueError:
        click.echo()
        click.echo("Warning: No strategies registered in factory.", err=True)
        click.echo("Running with mock strategy for demonstration.", err=True)
        click.echo()

        from ...backtesting import make_mock_strategy_class

        return make_mock_strategy_class(deployment_id)


def _build_monte_carlo_pnl_config(
    ctx: _MonteCarloRunContext,
    *,
    token_funding: list[dict[str, Any]] | None,
) -> PnLBacktestConfig:
    return PnLBacktestConfig(
        start_time=ctx.start,
        end_time=ctx.end,
        interval_seconds=ctx.interval,
        token_funding=token_funding,
        chain=ctx.chain,
        tokens=ctx.token_list,
        # gas_price_gwei omitted: chain-aware default (VIB-5088)
        include_gas_costs=True,
    )


async def _fetch_ohlcv_closes(
    data_provider: CoinGeckoDataProvider,
    ctx: _MonteCarloRunContext,
) -> list[Decimal]:
    ohlcv_data = await data_provider.get_ohlcv(
        ctx.base_token,
        ctx.start,
        ctx.end,
        ctx.interval,
    )
    await data_provider.close()
    return [ohlcv.close for ohlcv in ohlcv_data]


def _synthetic_monte_carlo_prices(ctx: _MonteCarloRunContext) -> list[Decimal]:
    import random

    if ctx.seed is not None:
        random.seed(ctx.seed)
    n_steps = max(10, ctx.duration_days * (86400 // ctx.interval))
    price = Decimal("3000")
    historical_prices = [price]
    for _ in range(n_steps - 1):
        change = Decimal(str(random.gauss(0, 0.02)))
        price = price * (1 + change)
        historical_prices.append(price)
    return historical_prices


def _load_monte_carlo_historical_prices(
    data_provider: CoinGeckoDataProvider,
    ctx: _MonteCarloRunContext,
) -> list[Decimal]:
    click.echo()
    click.echo(f"Fetching historical prices for {ctx.base_token}...")
    try:
        return asyncio.run(_fetch_ohlcv_closes(data_provider, ctx))
    except Exception as e:
        click.echo(f"Warning: Could not fetch historical prices: {e}", err=True)
        click.echo("Using synthetic historical data for demonstration...", err=True)
        return _synthetic_monte_carlo_prices(ctx)


def _generate_monte_carlo_paths(ctx: _MonteCarloRunContext, historical_prices: list[Decimal]) -> Any:
    from ...backtesting.pnl import MonteCarloPathGenerator, PricePathConfig

    click.echo(f"Generating {ctx.n_paths} price paths using {ctx.method.upper()} method...")
    path_config = PricePathConfig(method=ctx.path_method, n_paths=ctx.n_paths, seed=ctx.seed)
    generator = MonteCarloPathGenerator(config=path_config)
    paths = generator.generate_price_paths(historical_prices=historical_prices)
    click.echo(f"Generated {paths.n_paths} paths with {paths.n_steps} steps each")
    click.echo(f"Estimated drift: {float(paths.drift) * 100:.2f}% annualized")
    click.echo(f"Estimated volatility: {float(paths.volatility) * 100:.2f}% annualized")
    click.echo()
    return paths


def _build_monte_carlo_config(ctx: _MonteCarloRunContext) -> Any:
    from ...backtesting.pnl import MonteCarloConfig

    mc_config = MonteCarloConfig(
        n_paths=ctx.n_paths,
        parallel_workers=ctx.parallel_workers,
        base_token=ctx.base_token,
        quote_token="USDC",
        seed=ctx.seed,
        collect_individual_results=False,
    )

    def progress_callback(completed: int, total: int) -> None:
        if ctx.verbose:
            pct = (completed / total) * 100
            click.echo(f"  Progress: {completed}/{total} paths ({pct:.1f}%)", nl=False)
            click.echo("\r", nl=False)

    if ctx.verbose:
        mc_config.progress_callback = progress_callback
    return mc_config


def _run_monte_carlo_simulation(
    *,
    strategy_instance: Any,
    paths: Any,
    pnl_config: PnLBacktestConfig,
    mc_config: Any,
    ctx: _MonteCarloRunContext,
) -> Any:
    from ...backtesting.pnl import run_monte_carlo

    click.echo(f"Running Monte Carlo simulation ({ctx.n_paths} paths, {ctx.parallel_workers} workers)...")
    click.echo()
    try:
        return asyncio.run(
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
    ctx = _build_monte_carlo_context(
        strategy=strategy,
        start=start,
        end=end,
        n_paths=n_paths,
        method=method,
        interval=interval,
        chain=chain,
        tokens=tokens,
        base_token=base_token,
        parallel_workers=parallel_workers,
        seed=seed,
        output=output,
        verbose=verbose,
    )
    _print_monte_carlo_configuration(ctx)

    if dry_run:
        _handle_monte_carlo_dry_run()
        return

    strategy_class = _resolve_demo_strategy_class(ctx.strategy, "mock-monte-carlo")
    base_config = load_strategy_config(ctx.strategy, ctx.chain)
    strategy_instance = _create_backtest_strategy(strategy_class, base_config, ctx.chain)
    pnl_config = _build_monte_carlo_pnl_config(ctx, token_funding=base_config.get("token_funding"))
    token_addresses = build_token_address_map(
        strategy_config=base_config,
        tracked_tokens=ctx.token_list,
        chain=ctx.chain,
    )
    data_provider = CoinGeckoDataProvider(token_addresses=token_addresses)
    historical_prices = _load_monte_carlo_historical_prices(data_provider, ctx)
    click.echo(f"Historical prices: {len(historical_prices)} data points")
    click.echo()
    paths = _generate_monte_carlo_paths(ctx, historical_prices)
    mc_config = _build_monte_carlo_config(ctx)
    result = _run_monte_carlo_simulation(
        strategy_instance=strategy_instance,
        paths=paths,
        pnl_config=pnl_config,
        mc_config=mc_config,
        ctx=ctx,
    )

    if ctx.verbose:
        click.echo()

    print_monte_carlo_results(result)
    _write_json_output(ctx.output, result, "Monte Carlo")


# =============================================================================
# Scenario Command
# =============================================================================


@dataclass
class _ScenarioRunContext:
    strategy: str
    crisis_scenario: CrisisScenario
    interval: int
    chain: str
    token_list: list[str]
    gas_price: float
    mev: bool
    compare_normal: bool
    normal_start: datetime | None
    normal_end: datetime | None
    output: str | None
    verbose: bool


def _print_available_crisis_scenarios() -> None:
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


def _require_scenario_cli_inputs(strategy: str | None, scenario: str | None) -> tuple[str, str]:
    if strategy is None:
        click.echo("Error: --strategy is required. Use -s <strategy_name>.", err=True)
        raise click.Abort()
    if scenario is None:
        click.echo("Error: --scenario is required. Use --list-scenarios to see options.", err=True)
        raise click.Abort()
    return strategy, scenario


def _resolve_custom_crisis_scenario(
    *,
    start: datetime | None,
    end: datetime | None,
    name: str | None,
    description: str | None,
) -> CrisisScenario:
    if start is None or end is None:
        click.echo("Error: --start and --end are required for custom scenarios.", err=True)
        raise click.Abort()
    if end <= start:
        click.echo("Error: --end must be after --start for custom scenarios.", err=True)
        raise click.Abort()
    return CrisisScenario(
        name=name or "custom_scenario",
        start_date=start,
        end_date=end,
        description=description or f"Custom crisis scenario from {start.date()} to {end.date()}",
    )


def _resolve_crisis_scenario(
    *,
    scenario: str,
    start: datetime | None,
    end: datetime | None,
    name: str | None,
    description: str | None,
) -> CrisisScenario:
    if scenario.lower() == "custom":
        return _resolve_custom_crisis_scenario(start=start, end=end, name=name, description=description)

    crisis_scenario = get_scenario_by_name(scenario)
    if crisis_scenario is not None:
        return crisis_scenario

    click.echo(f"Error: Unknown scenario '{scenario}'", err=True)
    click.echo("Available scenarios:", err=True)
    for name_key in PREDEFINED_SCENARIOS.keys():
        click.echo(f"  - {name_key}", err=True)
    click.echo("Use --scenario custom with --start/--end for custom dates.", err=True)
    raise click.Abort()


def _normal_period_for_scenario(
    crisis_scenario: CrisisScenario,
    normal_start: datetime | None,
    compare_normal: bool,
) -> tuple[datetime | None, datetime | None]:
    if not compare_normal:
        return None, None
    from datetime import timedelta

    resolved_start = normal_start or crisis_scenario.start_date - timedelta(days=30 + crisis_scenario.duration_days)
    return resolved_start, resolved_start + timedelta(days=crisis_scenario.duration_days)


def _build_scenario_context(
    *,
    strategy: str | None,
    scenario: str | None,
    start: datetime | None,
    end: datetime | None,
    name: str | None,
    description: str | None,
    interval: int,
    chain: str,
    tokens: str,
    gas_price: float,
    mev: bool,
    compare_normal: bool,
    normal_start: datetime | None,
    output: str | None,
    verbose: bool,
) -> _ScenarioRunContext:
    strategy_name, scenario_name = _require_scenario_cli_inputs(strategy, scenario)
    crisis_scenario = _resolve_crisis_scenario(
        scenario=scenario_name,
        start=start,
        end=end,
        name=name,
        description=description,
    )
    _validate_walk_forward_strategy(strategy_name)
    resolved_normal_start, normal_end = _normal_period_for_scenario(crisis_scenario, normal_start, compare_normal)
    return _ScenarioRunContext(
        strategy=strategy_name,
        crisis_scenario=crisis_scenario,
        interval=interval,
        chain=chain,
        token_list=_parse_cli_tokens(tokens),
        gas_price=gas_price,
        mev=mev,
        compare_normal=compare_normal,
        normal_start=resolved_normal_start,
        normal_end=normal_end,
        output=output,
        verbose=verbose,
    )


def _print_scenario_configuration(ctx: _ScenarioRunContext) -> None:
    scenario = ctx.crisis_scenario
    click.echo()
    click.echo("=" * 70)
    click.echo("CRISIS SCENARIO BACKTEST CONFIGURATION")
    click.echo("=" * 70)
    click.echo(f"Strategy: {ctx.strategy}")
    click.echo(f"Chain: {ctx.chain}")
    click.echo()
    click.echo("Scenario:")
    click.echo(f"  Name: {scenario.name}")
    click.echo(
        f"  Period: {scenario.start_date.strftime('%Y-%m-%d')} to "
        f"{scenario.end_date.strftime('%Y-%m-%d')} "
        f"({scenario.duration_days} days)"
    )
    click.echo(f"  Description: {scenario.description[:80]}...")
    click.echo()
    click.echo("Configuration:")
    click.echo(f"  Interval: {ctx.interval}s ({ctx.interval / 3600:.1f} hours)")
    click.echo(f"  Tokens: {', '.join(ctx.token_list)}")
    click.echo(f"  Gas Price: {ctx.gas_price:.0f} gwei")
    click.echo(f"  MEV Simulation: {'Enabled' if ctx.mev else 'Disabled'}")
    click.echo(f"  Compare to Normal: {'Yes' if ctx.compare_normal else 'No'}")
    if ctx.normal_start and ctx.normal_end:
        click.echo(f"  Normal Period: {ctx.normal_start.strftime('%Y-%m-%d')} to {ctx.normal_end.strftime('%Y-%m-%d')}")
    if ctx.output:
        click.echo(f"Output: {ctx.output}")
    click.echo("=" * 70)


def _handle_scenario_dry_run() -> None:
    click.echo()
    click.echo("Dry run - crisis backtest not executed.")


def _build_crisis_backtester(base_config: dict[str, Any], ctx: _ScenarioRunContext) -> PnLBacktester:
    token_addresses = build_token_address_map(
        strategy_config=base_config,
        tracked_tokens=ctx.token_list,
        chain=ctx.chain,
    )
    data_provider = CoinGeckoDataProvider(token_addresses=token_addresses)
    return PnLBacktester(data_provider=data_provider, fee_models={}, slippage_models={})


def _build_crisis_config(
    ctx: _ScenarioRunContext,
    *,
    token_funding: list[dict[str, Any]] | None,
) -> CrisisBacktestConfig:
    return CrisisBacktestConfig(
        scenario=ctx.crisis_scenario,
        token_funding=token_funding,
        interval_seconds=ctx.interval,
        chain=ctx.chain,
        tokens=ctx.token_list,
        gas_price_gwei=Decimal(str(ctx.gas_price)),
        mev_simulation_enabled=ctx.mev,
    )


def _run_crisis_scenario(
    *,
    strategy_instance: Any,
    backtester: PnLBacktester,
    crisis_config: CrisisBacktestConfig,
    ctx: _ScenarioRunContext,
) -> CrisisBacktestResult:
    click.echo()
    click.echo(f"Running crisis backtest for scenario '{ctx.crisis_scenario.name}'...")
    if ctx.verbose:
        click.echo(f"  Period: {ctx.crisis_scenario.duration_days} days")
        click.echo(f"  Estimated ticks: {ctx.crisis_scenario.duration_days * 86400 // ctx.interval}")

    try:
        return asyncio.run(
            run_crisis_backtest(
                strategy=strategy_instance,
                scenario=ctx.crisis_scenario,
                backtester=backtester,
                config=crisis_config,
            )
        )
    except Exception as e:
        click.echo(f"Error during crisis backtest: {e}", err=True)
        sys.exit(1)


def _maybe_add_normal_period_comparison(
    *,
    result: CrisisBacktestResult,
    strategy_instance: Any,
    backtester: PnLBacktester,
    ctx: _ScenarioRunContext,
    token_funding: list[dict[str, Any]] | None,
) -> None:
    if not ctx.compare_normal or ctx.normal_start is None or ctx.normal_end is None:
        return

    click.echo()
    click.echo("Running normal period backtest for comparison...")
    normal_pnl_config = PnLBacktestConfig(
        start_time=ctx.normal_start,
        end_time=ctx.normal_end,
        interval_seconds=ctx.interval,
        token_funding=token_funding,
        chain=ctx.chain,
        tokens=ctx.token_list,
        gas_price_gwei=Decimal(str(ctx.gas_price)),
        mev_simulation_enabled=ctx.mev,
    )

    try:
        normal_result = asyncio.run(backtester.backtest(strategy_instance, normal_pnl_config))
        comparison = compare_crisis_to_normal(result.result, normal_result)
        result.crisis_metrics["normal_period_comparison"] = comparison
        result.result.crisis_results = build_crisis_metrics(result.result, ctx.crisis_scenario, normal_result)
    except Exception as e:
        click.echo(f"Warning: Normal period backtest failed: {e}", err=True)
        click.echo("Continuing with crisis results only...")


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
    if list_scenarios:
        _print_available_crisis_scenarios()
        return

    ctx = _build_scenario_context(
        strategy=strategy,
        scenario=scenario,
        start=start,
        end=end,
        name=name,
        description=description,
        interval=interval,
        chain=chain,
        tokens=tokens,
        gas_price=gas_price,
        mev=mev,
        compare_normal=compare_normal,
        normal_start=normal_start,
        output=output,
        verbose=verbose,
    )
    _print_scenario_configuration(ctx)

    if dry_run:
        _handle_scenario_dry_run()
        return

    strategy_class = _resolve_demo_strategy_class(ctx.strategy, "mock-crisis")
    base_config = load_strategy_config(ctx.strategy, ctx.chain)
    strategy_instance = _create_backtest_strategy(strategy_class, base_config, ctx.chain)
    backtester = _build_crisis_backtester(base_config, ctx)
    crisis_config = _build_crisis_config(ctx, token_funding=base_config.get("token_funding"))
    result = _run_crisis_scenario(
        strategy_instance=strategy_instance,
        backtester=backtester,
        crisis_config=crisis_config,
        ctx=ctx,
    )
    _maybe_add_normal_period_comparison(
        result=result,
        strategy_instance=strategy_instance,
        backtester=backtester,
        ctx=ctx,
        token_funding=base_config.get("token_funding"),
    )
    print_crisis_backtest_results(result)
    _write_json_output(ctx.output, result, "Crisis backtest")
