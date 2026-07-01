"""Parameter sweep and Bayesian optimization CLI commands.

This module provides the `sweep` and `optimize` subcommands for parameter
grid search and Bayesian hyperparameter tuning.
"""

import asyncio
import json
import math
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import click

from almanak.core.chains import DEFAULT_CHAIN


def _strict_numeric_sweep_value(name: str, value: str) -> float:
    try:
        return float(value)
    except (ValueError, TypeError) as e:
        raise click.UsageError(f"--numeric-param '{name}': value {value!r} is not numeric ({e}).") from e


def _try_float_sweep_value(value: str) -> float | None:
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def _float_round_trips(value: str, coerced: float) -> bool:
    return str(coerced) == value or f"{coerced:g}" == value


def _emit_nonfinite_sweep_warning(name: str, value: str, coerced: float) -> None:
    click.echo(
        f"Warning: sweep param '{name}={value}' was coerced to {coerced}; "
        "this is almost certainly not what you meant. "
        "Pass --numeric-param to enforce numeric typing, or quote the "
        "value as a categorical string (no sibling floats) to keep it as-is.",
        err=True,
    )


def _emit_roundtrip_sweep_warning(name: str, value: str, coerced: float) -> None:
    click.echo(
        f"Warning: sweep param '{name}={value}' was coerced to float "
        f"{coerced}; the string no longer round-trips. "
        f"Use --numeric-param {name} to opt in explicitly, or keep it "
        "categorical by ensuring no value in the list is numeric.",
        err=True,
    )


def _maybe_warn_ambiguous_sweep_value(
    name: str,
    value: str,
    coerced: float,
    *,
    warned_ambiguous: set[tuple[str, str]],
    emit_warnings: bool,
) -> None:
    key = (name, value)
    if key in warned_ambiguous:
        return
    if math.isnan(coerced) or math.isinf(coerced):
        warned_ambiguous.add(key)
        if emit_warnings:
            _emit_nonfinite_sweep_warning(name, value, coerced)
        return
    if not _float_round_trips(value, coerced):
        warned_ambiguous.add(key)
        if emit_warnings:
            _emit_roundtrip_sweep_warning(name, value, coerced)


def _coerce_sweep_value(
    name: str,
    value: str,
    *,
    numeric_param_names: frozenset[str],
    warned_ambiguous: set[tuple[str, str]],
    emit_warnings: bool = True,
) -> Any:
    """Coerce a sweep parameter value to an appropriate Python type.

    Issue #1702: the original behaviour was a blanket ``float(value)`` inside
    a ``try/except ValueError``. That silently coerces strings the author
    intended as categorical identifiers — e.g. a zero-padded token id
    ``"0001"`` becomes ``1.0``, ``"1e5"`` becomes ``100000.0``, and the
    special strings ``"inf"`` / ``"nan"`` become float infinities / NaN
    without any indication to the caller.

    The narrow fix here preserves the public CLI surface byte-for-byte:

    - When ``name`` is in ``numeric_param_names`` (``--numeric-param`` flag),
      we require a strict numeric parse. Any ``ValueError`` is re-raised as
      ``click.UsageError`` so the run fails fast.
    - Otherwise we replicate the historical ``float()`` coercion but emit a
      one-line stderr warning when the coerced value does not round-trip
      back to the exact original string (i.e. the coercion changed semantics),
      and a distinct warning for ``inf`` / ``nan`` which are almost never
      intentional sweep values. Warnings are emitted once per (name, value)
      pair to avoid spamming when the same combo appears across periods.

    Issue #1756: ``emit_warnings`` gates the ``click.echo`` side effect.
    ``warned_ambiguous`` is process-local (workers cannot share a set across
    the pickle boundary), so per-worker dedup is insufficient when a sweep
    runs N periods × M workers — the same ``(name, value)`` pair still emits
    up to N×M duplicate warnings. The sweep command now emits warnings once
    in a parent-side pre-pass (``_preflight_emit_ambiguous_warnings``) before
    any worker spawns, then invokes worker-side coercion with
    ``emit_warnings=False``. The coercion logic itself is unchanged so the
    resulting Python value is identical in either mode. ``warned_ambiguous``
    is retained and still populated even when ``emit_warnings`` is False so
    that any in-process re-invocation (e.g. the same combo appearing for
    strategy-config and strategy-instance attribute assignment) remains
    idempotent.

    Full type-tagging (``--param 'name:val1,val2:str'``) would be a cleaner
    long-term fix but breaks the current CLI contract — deferred to a
    dedicated deprecation cycle.
    """
    if name in numeric_param_names:
        return _strict_numeric_sweep_value(name, value)

    coerced = _try_float_sweep_value(value)
    if coerced is None:
        return value

    _maybe_warn_ambiguous_sweep_value(
        name,
        value,
        coerced,
        warned_ambiguous=warned_ambiguous,
        emit_warnings=emit_warnings,
    )
    return coerced


def _preflight_emit_ambiguous_warnings(
    combinations: list[dict[str, str]],
    numeric_param_names: frozenset[str],
) -> None:
    """Emit the #1702 ambiguous-coercion warnings once per sweep run (#1756).

    Walks the full sweep-param matrix in the parent process and invokes
    ``_coerce_sweep_value`` with a single shared ``warned_ambiguous`` set so
    that each unique ``(name, value)`` ambiguous pair is warned about exactly
    once. Workers (sequential or ``ProcessPoolExecutor``) subsequently run
    the same coercion with ``emit_warnings=False``, producing the same
    coerced Python values without duplicating the stderr output.

    Design decisions:

    - UX choice: warnings fire **once per sweep run** (not per period). The
      same ambiguity is a static property of the ``--param`` input and does
      not become more informative when re-emitted per period. This matches
      the bar of #1756 ("hoist to sweep-scoped parent pass").
    - Parent-only: the shared dedup set never crosses the pickle boundary
      into ``ProcessPoolExecutor`` workers, sidestepping both inter-process
      mutability and startup cost.
    - Exception neutrality: this helper is a side-effect-only pre-pass. If a
      value would raise (e.g. a ``--numeric-param`` value fails strict
      parsing), ``_preflight_validate_numeric_params`` is the authoritative
      check — numeric-param names are skipped here to avoid double-raising.
    """
    if not combinations:
        return
    warned: set[tuple[str, str]] = set()
    for combo in combinations:
        for name, value in combo.items():
            if name in numeric_param_names:
                # Strict-numeric coercion is not ambiguous — the author
                # opted in. `_preflight_validate_numeric_params` owns the
                # fail-fast path for invalid numeric values.
                continue
            _coerce_sweep_value(
                name,
                value,
                numeric_param_names=numeric_param_names,
                warned_ambiguous=warned,
                emit_warnings=True,
            )


def _preflight_validate_numeric_params(
    combinations: list[dict[str, str]],
    numeric_param_names: frozenset[str],
) -> None:
    """Fail fast if any combination contains a non-numeric ``--numeric-param`` value.

    Issue #1702: without this parent-side check, invalid numeric values only
    get rejected inside worker subprocesses (via ``_coerce_sweep_value``),
    where ``click.UsageError`` is caught by the broad ``except Exception`` in
    ``_run_parallel_sweep``'s result loop and converted into a synthetic
    failed ``SweepResult``. The overall command then exits 0 and produces
    ranked output from a misconfigured sweep — breaking the "run aborts"
    contract documented for ``--numeric-param``.

    Running the strict ``float()`` parse up here, once, guarantees the same
    abort semantics in sequential and ``--parallel`` modes.
    """
    for combo in combinations:
        for name, value in combo.items():
            if name not in numeric_param_names:
                continue
            try:
                float(value)
            except (ValueError, TypeError) as e:
                raise click.UsageError(f"--numeric-param '{name}': value {value!r} is not numeric ({e}).") from e


from ...backtesting import (
    BacktestMetrics,
    BacktestResult,
    CoinGeckoDataProvider,
    PnLBacktestConfig,
    PnLBacktester,
)
from ...backtesting.models import BacktestEngine
from ...strategies import get_strategy
from ..chain_resolution import get_default_chain
from .group import backtest
from .helpers import (
    AggregatedParamResult,
    SweepParameter,
    SweepResult,
    _create_backtest_strategy,
    generate_combinations,
    list_strategies_fn,
    load_strategy_config,
    parse_date,
)
from .run_helpers import (
    build_pnl_config,
    build_token_address_map,
    parse_token_list,
    resolve_strategy_class_or_mock,
)

# =============================================================================
# Sweep Helpers
# =============================================================================


def _coerce_sweep_params(
    *,
    base_config: dict[str, Any],
    params: dict[str, str],
    numeric_param_names: frozenset[str],
    emit_ambiguity_warnings: bool,
) -> tuple[dict[str, Any], set[tuple[str, str]]]:
    warned: set[tuple[str, str]] = set()
    strategy_config = base_config.copy()
    for name, value in params.items():
        strategy_config[name] = _coerce_sweep_value(
            name,
            value,
            numeric_param_names=numeric_param_names,
            warned_ambiguous=warned,
            emit_warnings=emit_ambiguity_warnings,
        )
    return strategy_config, warned


def _apply_sweep_param_attributes(
    strategy_instance: Any,
    *,
    params: dict[str, str],
    numeric_param_names: frozenset[str],
    warned: set[tuple[str, str]],
    emit_ambiguity_warnings: bool,
) -> None:
    config = getattr(strategy_instance, "config", None)
    if isinstance(config, dict):
        return
    for name, value in params.items():
        setattr(
            strategy_instance,
            name,
            _coerce_sweep_value(
                name,
                value,
                numeric_param_names=numeric_param_names,
                warned_ambiguous=warned,
                emit_warnings=emit_ambiguity_warnings,
            ),
        )


def _sweep_result_from_metrics(params: dict[str, str], result: Any) -> SweepResult:
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


async def run_sweep_backtest(
    strategy_class: Any,
    base_config: dict[str, Any],
    pnl_config: PnLBacktestConfig,
    data_provider: CoinGeckoDataProvider,
    params: dict[str, str],
    *,
    numeric_param_names: frozenset[str] = frozenset(),
    emit_ambiguity_warnings: bool = True,
) -> SweepResult:
    """Run a single backtest with specific parameter values.

    Args:
        strategy_class: Strategy class to instantiate
        base_config: Base strategy configuration
        pnl_config: PnL backtest configuration
        data_provider: Historical data provider
        params: Parameter values for this run
        numeric_param_names: Set of parameter names the caller has marked
            as strictly numeric via ``--numeric-param`` (#1702). Values for
            those names must parse as float or the call raises
            ``click.UsageError``.
        emit_ambiguity_warnings: When True (default), the #1702 ambiguous-
            coercion warnings are emitted on stderr so direct programmatic
            callers (not going through the ``sweep_backtest`` CLI command)
            still see ``"0001" -> 1.0`` / ``"1e5" -> 100000.0`` noted. The
            CLI passes False after running its sweep-scoped parent pre-pass
            (``_preflight_emit_ambiguous_warnings``, #1756) so workers and
            per-period loops do not duplicate the stderr output.

    Returns:
        SweepResult with backtest results and key metrics
    """
    strategy_config, warned = _coerce_sweep_params(
        base_config=base_config,
        params=params,
        numeric_param_names=numeric_param_names,
        emit_ambiguity_warnings=emit_ambiguity_warnings,
    )
    strategy_instance = _create_backtest_strategy(strategy_class, strategy_config, pnl_config.chain)
    _apply_sweep_param_attributes(
        strategy_instance,
        params=params,
        numeric_param_names=numeric_param_names,
        warned=warned,
        emit_ambiguity_warnings=emit_ambiguity_warnings,
    )
    _ensure_sweep_worker_deployment_id(strategy_instance, params)
    backtester = PnLBacktester(data_provider=data_provider, fee_models={}, slippage_models={})
    result = await backtester.backtest(strategy_instance, pnl_config)
    return _sweep_result_from_metrics(params, result)


async def run_parallel_sweeps(
    strategy_class: Any,
    base_config: dict[str, Any],
    pnl_config: PnLBacktestConfig,
    data_provider: CoinGeckoDataProvider,
    combinations: list[dict[str, str]],
    parallel: int,
    *,
    numeric_param_names: frozenset[str] = frozenset(),
    emit_ambiguity_warnings: bool = True,
) -> list[SweepResult]:
    """Run multiple backtests in parallel.

    Args:
        strategy_class: Strategy class to instantiate
        base_config: Base strategy configuration
        pnl_config: PnL backtest configuration
        data_provider: Historical data provider
        combinations: List of parameter combinations to test
        parallel: Number of parallel workers
        numeric_param_names: Forwarded to ``run_sweep_backtest`` — see
            issue #1702 for the float coercion semantics.
        emit_ambiguity_warnings: Forwarded to ``run_sweep_backtest``. Default
            True preserves the #1702 warning surface for direct callers;
            the sweep CLI passes False after running the sweep-scoped
            parent pre-pass (#1756).

    Returns:
        List of SweepResult objects
    """
    import asyncio

    results: list[SweepResult] = []
    semaphore = asyncio.Semaphore(parallel)

    # Wrap the shared dedup set so that when the caller opts into warnings
    # via `emit_ambiguity_warnings=True`, an ambiguous (name, value) pair
    # emits once across all semaphore-serialised concurrent sweeps — not
    # once per combination. Previously each call built its own `warned` set
    # inside `run_sweep_backtest`, so a single ambiguous value shared across
    # M combinations could still produce M duplicate warnings in the async
    # path. When warnings are disabled (CLI path), the set is ignored.
    shared_warned: set[tuple[str, str]] = set()

    async def run_with_semaphore(params: dict[str, str]) -> SweepResult:
        async with semaphore:
            # First-pass: surface #1702 warnings once across the shared set
            # using the current call's parameter values. This is a parallel-
            # to-sweep-scoped-pre-pass at the async helper level, so direct
            # callers of `run_parallel_sweeps` see deduped warnings even
            # without running `_preflight_emit_ambiguous_warnings`.
            if emit_ambiguity_warnings:
                for name, value in params.items():
                    if name in numeric_param_names:
                        continue
                    _coerce_sweep_value(
                        name,
                        value,
                        numeric_param_names=numeric_param_names,
                        warned_ambiguous=shared_warned,
                        emit_warnings=True,
                    )
            return await run_sweep_backtest(
                strategy_class=strategy_class,
                base_config=base_config,
                pnl_config=pnl_config,
                data_provider=data_provider,
                params=params,
                numeric_param_names=numeric_param_names,
                # Inner call is always silent: we just emitted the warnings
                # above (or the caller disabled them entirely).
                emit_ambiguity_warnings=False,
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
    *,
    numeric_param_names: frozenset[str] = frozenset(),
    emit_ambiguity_warnings: bool = True,
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
        emit_ambiguity_warnings: When True (default), the parent process
            walks ``combinations`` once before spawning workers and emits
            the #1702 ambiguous-coercion warnings (deduped). The ``_SweepTask``
            then crosses the pickle boundary with its own
            ``emit_ambiguity_warnings=False``, so workers never duplicate
            the stderr output. The CLI passes False here because
            ``_preflight_emit_ambiguous_warnings`` has already run at the
            top-level sweep entry point (#1756).

    Returns:
        List of SweepResult objects
    """
    from concurrent.futures import ProcessPoolExecutor, as_completed

    from tqdm import tqdm

    # Resolve the strategy's default chain in the parent process so workers
    # do not need to re-import ``..run`` inside each subprocess (#1703).
    parent_default_chain = get_default_chain(strategy_class)

    # #1756: fire the sweep-scoped ambiguity warnings from this parent
    # process before any worker spawns, exactly once per unique (name,
    # value) pair. When the CLI invoked us, the top-level pre-pass has
    # already run and `emit_ambiguity_warnings` is False.
    if emit_ambiguity_warnings:
        _preflight_emit_ambiguous_warnings(combinations, numeric_param_names)

    # Create tasks with all necessary data for worker processes. Workers
    # always receive `emit_ambiguity_warnings=False`: either this parent
    # just emitted the warnings, or the caller (CLI) already did.
    tasks = [
        _SweepTask(
            strategy_class_name=strategy_class.__module__ + "." + strategy_class.__name__,
            base_config=base_config,
            pnl_config_dict=pnl_config.to_dict(),
            params=combo,
            task_index=i,
            default_chain=parent_default_chain,
            numeric_param_names=numeric_param_names,
            emit_ambiguity_warnings=False,
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
                    # Handle worker exceptions.
                    # #1752: `BacktestResult.success` is a @property derived from
                    # `error is None`, NOT a constructor field. Passing `success=False`
                    # previously raised TypeError, meaning the error-handler itself
                    # crashed and propagated rather than recording a failed SweepResult.
                    # Correct pattern: set `error=str(e)` and let `success` derive from it.
                    # Required BacktestResult fields (engine, deployment_id, start_time,
                    # end_time, metrics) are populated explicitly, and the metadata
                    # fields (chain and period) are propagated from pnl_config
                    # so failed results carry the same run metadata as
                    # successful ones rather than silently falling back to the
                    # dataclass defaults.
                    click.echo(f"  Error in worker for params {task.params}: {e}", err=True)
                    results.append(
                        SweepResult(
                            params=task.params,
                            result=BacktestResult(
                                engine=BacktestEngine.PNL,
                                deployment_id="error",
                                start_time=pnl_config.start_time,
                                end_time=pnl_config.end_time,
                                metrics=BacktestMetrics(),
                                trades=[],
                                initial_portfolio_value_usd=Decimal("0"),
                                final_capital_usd=Decimal("0"),
                                chain=pnl_config.chain,
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

    ``default_chain`` is resolved once in the parent process (via
    ``get_default_chain(strategy_class)``) and passed down explicitly so
    workers never have to re-import ``..run`` and re-derive it from the
    class's ``STRATEGY_METADATA`` (#1703). When the dynamic re-import
    of the strategy class succeeds and its metadata is still accessible,
    the worker still prefers ``base_config`` / ``pnl_config_dict`` values
    before falling back to ``default_chain``.
    """

    strategy_class_name: str  # Fully qualified class name for import
    base_config: dict[str, Any]
    pnl_config_dict: dict[str, Any]
    params: dict[str, str]
    task_index: int
    default_chain: str = DEFAULT_CHAIN
    # Names marked via `--numeric-param`; forwarded through the pickle
    # boundary so workers apply the same strict coercion as the parent
    # process would (#1702).
    numeric_param_names: frozenset[str] = frozenset()
    # #1756: workers default to silent coercion because the parent-side
    # pre-pass in `_run_parallel_sweep` already emitted the deduped warnings.
    # Preserved as an explicit field so the shape of the task is auditable
    # at the pickle boundary and so old-pickle compatibility (defaulting to
    # False) keeps behaviour consistent with the hosted path.
    emit_ambiguity_warnings: bool = False


def _resolve_sweep_worker_strategy_class(task: _SweepTask) -> Any:
    import importlib

    module_name, class_name = task.strategy_class_name.rsplit(".", 1)
    try:
        module = importlib.import_module(module_name)
        return getattr(module, class_name)
    except (ImportError, AttributeError):
        from ...strategies import get_strategy

        try:
            return get_strategy(class_name.lower().replace("strategy", ""))
        except ValueError:
            from ...backtesting import make_mock_strategy_class

            return make_mock_strategy_class("mock-worker")


def _coerce_sweep_params_for_worker(task: _SweepTask) -> tuple[dict[str, Any], set[tuple[str, str]]]:
    return _coerce_sweep_params(
        base_config=task.base_config,
        params=task.params,
        numeric_param_names=task.numeric_param_names,
        emit_ambiguity_warnings=task.emit_ambiguity_warnings,
    )


def _resolve_sweep_worker_chain(task: _SweepTask, strategy_class: Any) -> str:
    fallback_chain = task.default_chain or get_default_chain(strategy_class)
    return task.base_config.get("chain") or task.pnl_config_dict.get("chain") or fallback_chain


def _apply_sweep_worker_param_attributes(
    strategy_instance: Any,
    task: _SweepTask,
    warned: set[tuple[str, str]],
) -> None:
    _apply_sweep_param_attributes(
        strategy_instance,
        params=task.params,
        numeric_param_names=task.numeric_param_names,
        warned=warned,
        emit_ambiguity_warnings=task.emit_ambiguity_warnings,
    )


def _ensure_sweep_worker_deployment_id(strategy_instance: Any, params: dict[str, str]) -> None:
    existing_id = getattr(strategy_instance, "deployment_id", "")
    if existing_id:
        return
    param_str = "_".join(f"{key}{value}" for key, value in params.items())
    fallback_id = f"sweep-{param_str}" if param_str else "sweep"
    if hasattr(strategy_instance, "_deployment_id"):
        strategy_instance._deployment_id = fallback_id
    else:
        strategy_instance.deployment_id = fallback_id


def _pnl_config_from_sweep_task(task: _SweepTask) -> PnLBacktestConfig:
    pnl_config_dict = task.pnl_config_dict.copy()
    for key in ["duration_seconds", "duration_days", "estimated_ticks"]:
        pnl_config_dict.pop(key, None)
    return PnLBacktestConfig.from_dict(pnl_config_dict)


def _build_sweep_worker_backtester(
    *,
    strategy_config: dict[str, Any],
    pnl_config: PnLBacktestConfig,
    worker_chain: str,
) -> PnLBacktester:
    token_addresses = build_token_address_map(
        strategy_config=strategy_config,
        tracked_tokens=list(pnl_config.tokens),
        chain=worker_chain,
    )
    data_provider = CoinGeckoDataProvider(token_addresses=token_addresses)
    return PnLBacktester(data_provider=data_provider, fee_models={}, slippage_models={})


def _sweep_result_from_backtest(task: _SweepTask, result: Any) -> SweepResult:
    return _sweep_result_from_metrics(task.params, result)


# crap-allowlist: pre-existing sweep-worker body (cc=19 on main, unchanged by this PR); the only
# addition is a build_token_address_map call + provider kwarg for dynamic coin-id resolution.
# Score is coverage-driven (subprocess worker, no unit harness). Coverage backfill / decomposition
# tracked as a follow-up (file under AGI - Strategist / VibeCoders).
def _run_sweep_task_worker(task: _SweepTask) -> SweepResult:
    """Worker function to run a single sweep task in a subprocess.

    This function is executed in a separate process via ProcessPoolExecutor.
    It recreates all necessary objects since they can't be pickled directly.

    Args:
        task: SweepTask containing all data needed for the backtest

    Returns:
        SweepResult with backtest results
    """
    strategy_class = _resolve_sweep_worker_strategy_class(task)
    strategy_config, warned = _coerce_sweep_params_for_worker(task)
    worker_chain = _resolve_sweep_worker_chain(task, strategy_class)
    strategy_instance = _create_backtest_strategy(strategy_class, strategy_config, worker_chain)
    _apply_sweep_worker_param_attributes(strategy_instance, task, warned)
    _ensure_sweep_worker_deployment_id(strategy_instance, task.params)
    pnl_config = _pnl_config_from_sweep_task(task)
    backtester = _build_sweep_worker_backtester(
        strategy_config=strategy_config,
        pnl_config=pnl_config,
        worker_chain=worker_chain,
    )
    result = asyncio.run(backtester.backtest(strategy_instance, pnl_config))
    return _sweep_result_from_backtest(task, result)


# =============================================================================
# Optimization Helpers
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


def _param_type_from_spec(name: str, spec: dict[str, Any]) -> str:
    raw_type = spec.get("type", "continuous")
    if not isinstance(raw_type, str):
        raise click.BadParameter(f"Parameter '{name}' type must be a string")
    return raw_type.lower()


def _required_range_bounds(name: str, spec: dict[str, Any], param_type: str) -> tuple[Any, Any]:
    min_val = spec.get("min")
    max_val = spec.get("max")
    if min_val is None or max_val is None:
        raise click.BadParameter(f"{param_type.title()} parameter '{name}' requires 'min' and 'max'")
    return min_val, max_val


def _parse_categorical_param_range(name: str, spec: dict[str, Any], categorical: Any) -> Any:
    choices = spec.get("choices", [])
    if not choices:
        raise click.BadParameter(f"Categorical parameter '{name}' requires 'choices' list")
    return categorical(choices)


def _parse_discrete_param_range(name: str, spec: dict[str, Any], discrete: Any) -> Any:
    min_val, max_val = _required_range_bounds(name, spec, "discrete")
    try:
        low = int(min_val)
        high = int(max_val)
    except (TypeError, ValueError) as e:
        raise click.BadParameter(f"Discrete parameter '{name}' requires integer bounds") from e
    return discrete(low, high, step=spec.get("step"))


def _parse_continuous_param_range(name: str, spec: dict[str, Any], continuous: Any) -> Any:
    min_val, max_val = _required_range_bounds(name, spec, "continuous")
    if isinstance(min_val, str) or isinstance(max_val, str):
        min_val = Decimal(str(min_val))
        max_val = Decimal(str(max_val))
    return continuous(min_val, max_val, step=spec.get("step"), log=spec.get("log", False))


def _parse_dict_param_range(
    name: str,
    spec: dict[str, Any],
    *,
    categorical: Any,
    continuous: Any,
    discrete: Any,
) -> Any:
    param_type = _param_type_from_spec(name, spec)
    if param_type == "categorical":
        return _parse_categorical_param_range(name, spec, categorical)
    if param_type == "discrete":
        return _parse_discrete_param_range(name, spec, discrete)
    if param_type == "continuous":
        return _parse_continuous_param_range(name, spec, continuous)
    raise click.BadParameter(
        f"Unknown parameter type '{param_type}' for '{name}'. Use: continuous, discrete, or categorical"
    )


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
    from ...backtesting.pnl.optuna_tuner import (
        categorical,
        continuous,
        discrete,
    )

    param_ranges: dict[str, Any] = {}

    for name, spec in config.get("param_ranges", {}).items():
        if isinstance(spec, dict):
            param_ranges[name] = _parse_dict_param_range(
                name,
                spec,
                categorical=categorical,
                continuous=continuous,
                discrete=discrete,
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


# =============================================================================
# Phase helpers (Phase 5B.3 extractions)
# =============================================================================


def _parse_sweep_params(params: tuple[str, ...]) -> list[SweepParameter]:
    """Phase S1: parse the repeated `--param NAME:v1,v2,...` CLI flags.

    Validates that at least one `--param` was supplied and surfaces any
    per-flag parse error as a `click.UsageError` with the original message.
    """
    from .helpers import parse_param_string as _parse_param_string

    if not params:
        raise click.UsageError("At least one --param is required. Use format: --param 'name:val1,val2,val3'")

    sweep_params: list[SweepParameter] = []
    for param_str in params:
        try:
            sweep_params.append(_parse_param_string(param_str))
        except click.BadParameter as e:
            raise click.UsageError(str(e)) from e
    return sweep_params


def _resolve_backtest_periods(
    periods: str | None,
    start: datetime | None,
    end: datetime | None,
) -> tuple[bool, list[Any]]:
    """Phase S2: resolve `--periods` vs `--start/--end` into a period list.

    Returns:
        Tuple of ``(multi_period_mode, backtest_periods)``. ``multi_period_mode``
        is True when ``--periods`` was supplied.

    Raises:
        click.UsageError: on mutual-exclusion violation or missing required args.
    """
    from ...backtesting.pnl.periods import BacktestPeriod, resolve_periods

    if periods is not None:
        if start is not None or end is not None:
            raise click.UsageError("Cannot use --periods together with --start/--end. Use one or the other.")
        try:
            return True, resolve_periods(periods)
        except (ValueError, json.JSONDecodeError) as e:
            raise click.UsageError(str(e)) from e

    if start is None or end is None:
        raise click.UsageError("Either --start and --end, or --periods is required.")
    return False, [BacktestPeriod(name="single", start=start, end=end)]


@dataclass
class _SweepRunContext:
    """Bundle of validated config passed to sweep phase helpers.

    Mirrors ``SweepBacktestContext`` in spirit but is sweep-local so that
    phase helpers stay side-effect-compatible with the original inline
    implementation without forcing premature consolidation. Kept private so
    that callers still invoke the public CLI entry point.
    """

    strategy: str
    chain: str
    token_list: list[str]
    interval: int
    output_path: Path | None
    multi_period_mode: bool
    backtest_periods: list[Any]
    sweep_params: list[SweepParameter]
    combinations: list[dict[str, str]]
    periods_spec: str | None  # raw --periods arg, used only for banner echo
    # Names marked with `--numeric-param` (#1702). Empty by default so the
    # historical "try-float-then-fallback-to-string" behaviour is retained.
    numeric_param_names: frozenset[str] = frozenset()


def _print_sweep_configuration(
    ctx: _SweepRunContext,
    *,
    parallel: bool,
    effective_workers: int,
) -> None:
    """Phase S5: emit the PARAMETER SWEEP CONFIGURATION banner.

    Preserves the original stdout ordering, spacing, and byte-for-byte
    formatting — tests grep-assert several of these lines verbatim.
    """
    total_combinations = len(ctx.combinations)
    total_runs = total_combinations * len(ctx.backtest_periods)

    click.echo("=" * 60)
    click.echo("PARAMETER SWEEP CONFIGURATION")
    click.echo("=" * 60)
    click.echo(f"Strategy: {ctx.strategy}")
    click.echo(f"Chain: {ctx.chain}")
    if ctx.multi_period_mode:
        click.echo(f"Periods: {ctx.periods_spec} ({len(ctx.backtest_periods)} windows)")
        for bp in ctx.backtest_periods:
            click.echo(f"  - {bp.name}: {bp.start.date()} -> {bp.end.date()}")
    else:
        click.echo(f"Period: {ctx.backtest_periods[0].start.date()} -> {ctx.backtest_periods[0].end.date()}")
    click.echo(f"Interval: {ctx.interval}s ({ctx.interval / 3600:.1f} hours)")
    click.echo(f"Tokens: {', '.join(ctx.token_list)}")
    click.echo()
    click.echo("Parameters to sweep:")
    for p in ctx.sweep_params:
        click.echo(f"  {p.name}: {', '.join(p.values)}")
    click.echo()
    click.echo(f"Total combinations: {total_combinations}")
    if ctx.multi_period_mode:
        click.echo(
            f"Total runs: {total_runs} ({total_combinations} combinations x {len(ctx.backtest_periods)} periods)"
        )

    if parallel:
        click.echo("Execution mode: Parallel (multiprocessing)")
        click.echo(f"Workers: {effective_workers}")
    else:
        click.echo("Execution mode: Async (concurrent)")
        click.echo(f"Concurrency: {effective_workers}")

    if ctx.output_path:
        click.echo(f"Output: {ctx.output_path}")

    click.echo("=" * 60)


def _compute_worker_count(parallel: bool, workers: int | None, total_runs: int) -> int:
    """Phase S5 tail: derive the effective worker/concurrency count.

    In parallel mode defaults to ``max(1, cpu_count - 1)`` and is capped at
    ``total_runs`` (no benefit to more workers than runs). In async mode the
    default is 4 (historical value; kept to preserve behaviour).
    """
    if parallel:
        import os

        effective = workers if workers is not None else max(1, (os.cpu_count() or 1) - 1)
        return min(effective, total_runs) if total_runs > 0 else effective
    return workers if workers is not None else 4


def _normalize_numeric_param_names(numeric_params: tuple[str, ...]) -> frozenset[str]:
    return frozenset(name.strip() for name in numeric_params if name.strip())


def _validate_sweep_strategy(strategy: str) -> None:
    available_strategies = list_strategies_fn()
    if strategy in available_strategies or not available_strategies:
        return

    click.echo(f"Error: Unknown strategy '{strategy}'", err=True)
    click.echo(f"Available strategies: {', '.join(sorted(available_strategies))}", err=True)
    raise click.Abort()


def _validate_numeric_param_names(
    *,
    numeric_param_names: frozenset[str],
    sweep_params: list[SweepParameter],
) -> None:
    known_names = {p.name for p in sweep_params}
    unknown_numeric = numeric_param_names - known_names
    if not unknown_numeric:
        return

    raise click.UsageError(
        f"--numeric-param refers to unknown sweep parameter(s): "
        f"{', '.join(sorted(unknown_numeric))}. Known params: "
        f"{', '.join(sorted(known_names)) or '(none)'}."
    )


def _build_sweep_run_context(
    *,
    strategy: str,
    start: datetime | None,
    end: datetime | None,
    periods: str | None,
    params: tuple[str, ...],
    numeric_params: tuple[str, ...],
    interval: int,
    chain: str,
    tokens: str,
    output: str | None,
) -> _SweepRunContext:
    sweep_params = _parse_sweep_params(params)
    numeric_param_names = _normalize_numeric_param_names(numeric_params)
    multi_period_mode, backtest_periods = _resolve_backtest_periods(periods, start, end)

    _validate_sweep_strategy(strategy)

    combinations = generate_combinations(sweep_params)
    _validate_numeric_param_names(numeric_param_names=numeric_param_names, sweep_params=sweep_params)
    if numeric_param_names:
        _preflight_validate_numeric_params(combinations, numeric_param_names)
    _preflight_emit_ambiguous_warnings(combinations, numeric_param_names)

    return _SweepRunContext(
        strategy=strategy,
        chain=chain,
        token_list=parse_token_list(tokens),
        interval=interval,
        output_path=Path(output) if output else None,
        multi_period_mode=multi_period_mode,
        backtest_periods=backtest_periods,
        sweep_params=sweep_params,
        combinations=combinations,
        periods_spec=periods,
        numeric_param_names=numeric_param_names,
    )


def _build_sweep_data_provider(ctx: _SweepRunContext, base_config: dict[str, Any]) -> CoinGeckoDataProvider:
    click.echo()
    click.echo("Initializing CoinGecko data provider...")
    token_addresses = build_token_address_map(
        strategy_config=base_config,
        tracked_tokens=ctx.token_list,
        chain=ctx.chain,
    )
    return CoinGeckoDataProvider(token_addresses=token_addresses)


def _print_sweep_start(ctx: _SweepRunContext, total_runs: int) -> None:
    if ctx.multi_period_mode:
        click.echo(f"Starting multi-period sweep ({total_runs} total runs)...")
    else:
        click.echo(f"Starting parameter sweep ({len(ctx.combinations)} combinations)...")
    click.echo()


def _handle_sweep_dry_run(ctx: _SweepRunContext) -> bool:
    """Phase S6: emit the dry-run combinations block and signal early exit.

    Returns True if the caller received ``--dry-run`` and should ``return``
    without executing backtests. The caller is responsible for checking the
    ``--dry-run`` flag and only invoking this helper when it is set.
    """
    total_runs = len(ctx.combinations) * len(ctx.backtest_periods)
    click.echo()
    if ctx.multi_period_mode:
        click.echo(f"Parameter combinations x periods (dry run, {total_runs} total):")
    else:
        click.echo("Parameter combinations (dry run):")
    click.echo("-" * 40)
    for i, combo in enumerate(ctx.combinations, 1):
        params_str = ", ".join(f"{k}={v}" for k, v in combo.items())
        if ctx.multi_period_mode:
            for bp in ctx.backtest_periods:
                click.echo(f"  {params_str}  |  {bp.name}")
        else:
            click.echo(f"  {i}. {params_str}")
    click.echo("-" * 40)
    click.echo()
    click.echo("Dry run - no backtests executed.")
    return True


def _run_sweep_over_periods(
    ctx: _SweepRunContext,
    *,
    strategy_class: Any,
    base_config: dict[str, Any],
    data_provider: CoinGeckoDataProvider,
    parallel: bool,
    effective_workers: int,
) -> list[SweepResult]:
    """Phase S9: loop over periods, running sweeps in parallel or async mode.

    Preserves the original "one event loop per period" shape — `asyncio.run`
    is called once per period when in async mode, matching the behaviour that
    per-period fixtures/cleanup relies on. Also preserves the
    `preflight_validation=total_combinations <= 1` heuristic and the error
    message ``"Error during sweep: {e}"`` with `sys.exit(1)`.
    """
    total_combinations = len(ctx.combinations)
    all_results: list[SweepResult] = []

    try:
        for bp in ctx.backtest_periods:
            pnl_config = build_pnl_config(
                start_time=bp.start,
                end_time=bp.end,
                interval_seconds=ctx.interval,
                chain=ctx.chain,
                tokens=ctx.token_list,
                token_funding=base_config.get("token_funding"),
                # gas_price_gwei omitted: chain-aware default (VIB-5088)
                include_gas_costs=True,
                allow_degraded_data=True,
                preflight_validation=total_combinations <= 1,
                fail_on_preflight_error=False,
            )

            if ctx.multi_period_mode:
                click.echo(f"--- Period: {bp.name} ({bp.start.date()} -> {bp.end.date()}) ---")

            # #1756: the CLI entry point (`sweep_backtest`) already ran
            # `_preflight_emit_ambiguous_warnings` once before entering the
            # period loop. Pass `emit_ambiguity_warnings=False` so neither
            # mode re-emits the warnings per period / per worker.
            if parallel:
                period_results = _run_parallel_sweep(
                    strategy_class=strategy_class,
                    base_config=base_config,
                    pnl_config=pnl_config,
                    combinations=ctx.combinations,
                    workers=effective_workers,
                    sweep_params=ctx.sweep_params,
                    numeric_param_names=ctx.numeric_param_names,
                    emit_ambiguity_warnings=False,
                )
            else:
                period_results = asyncio.run(
                    run_parallel_sweeps(
                        strategy_class=strategy_class,
                        base_config=base_config,
                        pnl_config=pnl_config,
                        data_provider=data_provider,
                        combinations=ctx.combinations,
                        parallel=effective_workers,
                        numeric_param_names=ctx.numeric_param_names,
                        emit_ambiguity_warnings=False,
                    )
                )

            for r in period_results:
                r.period_name = bp.name
            all_results.extend(period_results)

    except Exception as e:
        click.echo(f"Error during sweep: {e}", err=True)
        sys.exit(1)

    return all_results


def _display_sweep_results(
    ctx: _SweepRunContext,
    all_results: list[SweepResult],
) -> None:
    """Phase S10: render the results table(s).

    For multi-period runs with more than one period the aggregated +
    per-period tables are emitted; otherwise the single-period results table.
    """
    if ctx.multi_period_mode and len(ctx.backtest_periods) > 1:
        aggregated = _aggregate_multi_period_results(all_results, ctx.combinations)
        _print_multi_period_results(all_results, aggregated, ctx.sweep_params)
    else:
        print_sweep_results_table(all_results, ctx.sweep_params)


def _write_sweep_json(
    ctx: _SweepRunContext,
    all_results: list[SweepResult],
) -> None:
    """Phase S11: write full JSON results to ``ctx.output_path``.

    Preserves the exact schema — external tooling reads this file. Keys
    include ``sweep_config``, ``results``, ``_meta``, and (for multi-period
    runs with >1 periods) ``aggregated``. ``best_params`` is appended when
    results are non-empty. No-op when ``ctx.output_path`` is None.
    """
    if ctx.output_path is None:
        return

    total_combinations = len(ctx.combinations)
    output_data: dict[str, Any] = {
        "sweep_config": {
            "strategy": ctx.strategy,
            "periods": [
                {"name": bp.name, "start": bp.start.isoformat(), "end": bp.end.isoformat()}
                for bp in ctx.backtest_periods
            ],
            "interval_seconds": ctx.interval,
            "chain": ctx.chain,
            "tokens": ctx.token_list,
            "parameters": [{"name": p.name, "values": p.values} for p in ctx.sweep_params],
            "total_combinations": total_combinations,
            "multi_period": ctx.multi_period_mode,
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

    if all_results:
        if ctx.multi_period_mode and len(ctx.backtest_periods) > 1:
            agg = _aggregate_multi_period_results(all_results, ctx.combinations)
            if agg:
                best_agg = sorted(agg, key=lambda x: (x.avg_sharpe, sorted(x.params.items())), reverse=True)[0]
                output_data["best_params"] = best_agg.params
        else:
            best_single = max(all_results, key=lambda x: (x.sharpe_ratio, sorted(x.params.items())))
            output_data["best_params"] = best_single.params

    if ctx.multi_period_mode and len(ctx.backtest_periods) > 1:
        aggregated = _aggregate_multi_period_results(all_results, ctx.combinations)
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

    with open(ctx.output_path, "w") as f:
        json.dump(output_data, f, indent=2)

    click.echo(f"Results written to: {ctx.output_path}")


def _generate_sweep_report(
    ctx: _SweepRunContext,
    all_results: list[SweepResult],
) -> None:
    """Phase S12: generate an HTML report for the best parameter combination.

    No-op when ``all_results`` is empty. Matches the original winner
    selection: for multi-period sweeps, pick the aggregated winner, then the
    best Sharpe across that winner's per-period results.
    """
    if not all_results:
        return

    from ...backtesting.report_generator import generate_report

    click.echo()
    click.echo("Generating HTML report for best parameter combination...")

    if ctx.multi_period_mode and len(ctx.backtest_periods) > 1:
        aggregated = _aggregate_multi_period_results(all_results, ctx.combinations)
        winner_params = aggregated[0].params if aggregated else all_results[0].params
        candidate_results = [r for r in all_results if r.params == winner_params]
        best_result = max(candidate_results, key=lambda x: x.sharpe_ratio)
    else:
        best_result = max(all_results, key=lambda x: x.sharpe_ratio)

    if ctx.output_path:
        report_path = ctx.output_path.with_suffix(".html")
    else:
        safe_strategy_name = ctx.strategy.replace("/", "_").replace("\\", "_")
        report_path = Path(f"backtest_report_{safe_strategy_name}_sweep.html")

    report_result = generate_report(best_result.result, output_path=report_path)

    if report_result.success:
        click.echo(f"Report saved to: {report_result.file_path}")
        click.echo(f"  Best params: {best_result.params}")
    else:
        click.echo(f"Warning: Failed to generate report: {report_result.error}", err=True)


# =============================================================================
# Sweep Command
# =============================================================================


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
    "--numeric-param",
    "-P",
    "numeric_params",
    multiple=True,
    help=(
        "Mark a sweep parameter as strictly numeric. Values must parse as "
        "float or the run aborts (vs the historical silent float() coercion "
        "which could turn '0001' into 1.0). Pass the parameter name, e.g. "
        "-P threshold. Repeatable."
    ),
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
    "--chain",
    "-c",
    type=str,
    default=DEFAULT_CHAIN,
    help=f"Target blockchain (default: {DEFAULT_CHAIN})",
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
    numeric_params: tuple[str, ...],
    parallel: bool,
    workers: int | None,
    interval: int,
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
    ctx = _build_sweep_run_context(
        strategy=strategy,
        start=start,
        end=end,
        periods=periods,
        params=params,
        numeric_params=numeric_params,
        interval=interval,
        chain=chain,
        tokens=tokens,
        output=output,
    )

    # Phase S5: compute worker count + print configuration banner
    total_runs = len(ctx.combinations) * len(ctx.backtest_periods)
    effective_workers = _compute_worker_count(parallel, workers, total_runs)
    _print_sweep_configuration(ctx, parallel=parallel, effective_workers=effective_workers)

    # Phase S6: --dry-run early exit
    if dry_run:
        _handle_sweep_dry_run(ctx)
        return

    # Phase S7: resolve strategy class (mock fallback preserved)
    strategy_class = resolve_strategy_class_or_mock(strategy, allow_mock=True)

    # Phase S8: load base strategy config + data provider
    base_config = load_strategy_config(strategy, chain)
    data_provider = _build_sweep_data_provider(ctx, base_config)
    _print_sweep_start(ctx, total_runs)

    # Phase S9: run sweep across all periods
    all_results = _run_sweep_over_periods(
        ctx,
        strategy_class=strategy_class,
        base_config=base_config,
        data_provider=data_provider,
        parallel=parallel,
        effective_workers=effective_workers,
    )

    # Phase S10: display results
    _display_sweep_results(ctx, all_results)

    # Phase S11: optional JSON output
    _write_sweep_json(ctx, all_results)

    # Phase S12: optional HTML report
    if report:
        _generate_sweep_report(ctx, all_results)


# =============================================================================
# Optimize Command
# =============================================================================


@dataclass
class _OptimizationSettings:
    objective: str
    n_trials: int
    patience: int | None
    min_delta: float


@dataclass
class _OptimizationRunContext:
    strategy: str
    chain: str
    token_list: list[str]
    interval: int
    output_label: str | None
    output_path: Path | None
    periods_spec: str | None
    backtest_periods: list[Any]
    param_ranges: dict[str, Any]
    settings: _OptimizationSettings
    seed: int | None
    verbose: bool


@dataclass
class _OptimizationFactories:
    strategy_factory: Any
    data_provider_factory: Any
    backtester_factory: Any


def _load_optimization_inputs(
    *,
    config_file: str,
    objective: str | None,
    n_trials: int | None,
    patience: int | None,
) -> tuple[dict[str, Any], _OptimizationSettings]:
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

    settings = _OptimizationSettings(
        objective=objective or opt_config.get("objective", "sharpe_ratio"),
        n_trials=n_trials or opt_config.get("n_trials", 50),
        patience=patience or opt_config.get("patience"),
        min_delta=opt_config.get("min_delta", 0.0),
    )
    return param_ranges, settings


def _validate_optimization_strategy(strategy: str) -> None:
    available_strategies = list_strategies_fn()
    if strategy not in available_strategies and available_strategies:
        click.echo(f"Error: Unknown strategy '{strategy}'", err=True)
        click.echo(f"Available strategies: {', '.join(sorted(available_strategies))}", err=True)
        raise click.Abort()


def _build_optimization_context(
    *,
    strategy: str,
    start: datetime | None,
    end: datetime | None,
    periods: str | None,
    config_file: str,
    objective: str | None,
    n_trials: int | None,
    patience: int | None,
    interval: int,
    chain: str,
    tokens: str,
    output: str | None,
    seed: int | None,
    verbose: bool,
) -> _OptimizationRunContext:
    _, backtest_periods = _resolve_backtest_periods(periods, start, end)
    param_ranges, settings = _load_optimization_inputs(
        config_file=config_file,
        objective=objective,
        n_trials=n_trials,
        patience=patience,
    )
    _validate_optimization_strategy(strategy)

    return _OptimizationRunContext(
        strategy=strategy,
        chain=chain,
        token_list=[t.strip().upper() for t in tokens.split(",")],
        interval=interval,
        output_label=output,
        output_path=Path(output) if output else None,
        periods_spec=periods,
        backtest_periods=backtest_periods,
        param_ranges=param_ranges,
        settings=settings,
        seed=seed,
        verbose=verbose,
    )


def _format_optimization_param_range(name: str, spec: Any) -> str:
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


def _print_optimization_configuration(ctx: _OptimizationRunContext) -> None:
    click.echo("=" * 60)
    click.echo("BAYESIAN OPTIMIZATION CONFIGURATION")
    click.echo("=" * 60)
    click.echo(f"Strategy: {ctx.strategy}")
    click.echo(f"Chain: {ctx.chain}")
    if len(ctx.backtest_periods) > 1:
        click.echo(f"Periods: {ctx.periods_spec} ({len(ctx.backtest_periods)} windows)")
        for bp in ctx.backtest_periods:
            click.echo(f"  - {bp.name}: {bp.start.date()} -> {bp.end.date()}")
        click.echo("  (each trial scored on avg metric across all periods)")
    else:
        bp = ctx.backtest_periods[0]
        click.echo(f"Period: {bp.start.date()} -> {bp.end.date()}")
    click.echo(f"Interval: {ctx.interval}s ({ctx.interval / 3600:.1f} hours)")
    click.echo(f"Tokens: {', '.join(ctx.token_list)}")
    click.echo()
    click.echo(f"Objective: {ctx.settings.objective}")
    click.echo(f"Trials: {ctx.settings.n_trials}")
    if ctx.settings.patience:
        click.echo(f"Early Stopping: patience={ctx.settings.patience}, min_delta={ctx.settings.min_delta}")
    else:
        click.echo("Early Stopping: disabled")
    if ctx.seed:
        click.echo(f"Random Seed: {ctx.seed}")
    click.echo()
    click.echo("Parameters to optimize:")
    for name, spec in ctx.param_ranges.items():
        click.echo(_format_optimization_param_range(name, spec))

    if ctx.output_label:
        click.echo(f"Output: {ctx.output_label}")

    click.echo("=" * 60)


def _handle_optimization_dry_run() -> None:
    click.echo()
    click.echo("Dry run - optimization not executed.")


def _resolve_optimization_strategy_class(strategy: str) -> Any:
    try:
        return get_strategy(strategy)
    except ValueError:
        click.echo()
        click.echo("Warning: No strategies registered in factory.", err=True)
        click.echo("Running with mock strategy for demonstration.", err=True)
        click.echo()

        # Issue #1701: shared mock (preserves id "mock-optimize" exactly).
        from ...backtesting import make_mock_strategy_class

        return make_mock_strategy_class("mock-optimize")


def _build_optimization_pnl_configs(
    ctx: _OptimizationRunContext,
    *,
    token_funding: list[dict[str, Any]] | None,
) -> list[PnLBacktestConfig]:
    pnl_configs: list[PnLBacktestConfig] = []
    for bp in ctx.backtest_periods:
        pnl_configs.append(
            PnLBacktestConfig(
                start_time=bp.start,
                end_time=bp.end,
                interval_seconds=ctx.interval,
                token_funding=token_funding,
                chain=ctx.chain,
                tokens=ctx.token_list,
                # gas_price_gwei omitted: chain-aware default (VIB-5088)
                include_gas_costs=True,
                allow_degraded_data=True,
                preflight_validation=(len(pnl_configs) == 0),
                fail_on_preflight_error=False,
            )
        )
    return pnl_configs


def _build_optimization_factories(
    *,
    strategy_class: Any,
    base_config: dict[str, Any],
    chain: str,
    token_addresses: dict[str, Any],
) -> _OptimizationFactories:
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

    return _OptimizationFactories(
        strategy_factory=create_strategy,
        data_provider_factory=create_data_provider,
        backtester_factory=create_backtester,
    )


def _create_optimization_tuner(ctx: _OptimizationRunContext) -> Any:
    from ...backtesting.pnl.optuna_tuner import OptunaTuner

    click.echo()
    click.echo("Initializing Optuna optimizer...")
    return OptunaTuner(
        objective_metric=ctx.settings.objective,
        sampler_seed=ctx.seed,
        patience=ctx.settings.patience,
        min_delta=ctx.settings.min_delta,
        log_level="INFO" if ctx.verbose else "WARNING",
    )


def _run_optimization(
    *,
    ctx: _OptimizationRunContext,
    tuner: Any,
    factories: _OptimizationFactories,
    pnl_configs: list[PnLBacktestConfig],
    base_config: dict[str, Any],
) -> Any:
    multi_period = len(pnl_configs) > 1
    if multi_period:
        click.echo(
            f"Starting multi-period Bayesian optimization ({ctx.settings.n_trials} trials x {len(pnl_configs)} periods)..."
        )
    else:
        click.echo(f"Starting Bayesian optimization ({ctx.settings.n_trials} trials)...")
    click.echo()

    try:
        return asyncio.run(
            tuner.optimize(
                strategy_factory=factories.strategy_factory,
                data_provider_factory=factories.data_provider_factory,
                backtester_factory=factories.backtester_factory,
                base_config=pnl_configs[0],
                param_ranges=ctx.param_ranges,
                n_trials=ctx.settings.n_trials,
                show_progress=ctx.verbose,
                patience=ctx.settings.patience,
                min_delta=ctx.settings.min_delta,
                extra_configs=pnl_configs[1:] if multi_period else None,
                strategy_config=base_config,
            )
        )
    except Exception as e:
        click.echo(f"Error during optimization: {e}", err=True)
        sys.exit(1)


def _write_optimization_history(output_path: Path | None, tuner: Any) -> None:
    if output_path is None:
        return
    try:
        history = tuner.export_history()
        history.save(output_path)
        click.echo(f"Optimization history written to: {output_path}")
    except Exception as e:
        click.echo(f"Warning: Could not save history: {e}", err=True)


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
    "--chain",
    "-c",
    type=str,
    default=DEFAULT_CHAIN,
    help=f"Target blockchain (default: {DEFAULT_CHAIN})",
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
    ctx = _build_optimization_context(
        strategy=strategy,
        start=start,
        end=end,
        periods=periods,
        config_file=config_file,
        objective=objective,
        n_trials=n_trials,
        patience=patience,
        interval=interval,
        chain=chain,
        tokens=tokens,
        output=output,
        seed=seed,
        verbose=verbose,
    )
    _print_optimization_configuration(ctx)

    if dry_run:
        _handle_optimization_dry_run()
        return

    strategy_class = _resolve_optimization_strategy_class(strategy)
    base_config = load_strategy_config(strategy, chain)
    pnl_configs = _build_optimization_pnl_configs(ctx, token_funding=base_config.get("token_funding"))

    # Resolve the SYMBOL -> (chain, address) map once; reused by every provider
    # the factory builds (Refinement R1). Natives resolve via the chain registry.
    token_addresses = build_token_address_map(
        strategy_config=base_config,
        tracked_tokens=ctx.token_list,
        chain=ctx.chain,
    )
    factories = _build_optimization_factories(
        strategy_class=strategy_class,
        base_config=base_config,
        chain=ctx.chain,
        token_addresses=token_addresses,
    )
    tuner = _create_optimization_tuner(ctx)
    result = _run_optimization(
        ctx=ctx,
        tuner=tuner,
        factories=factories,
        pnl_configs=pnl_configs,
        base_config=base_config,
    )

    # Display results
    print_optimization_results(result, ctx.settings.objective)

    # Write output if requested
    _write_optimization_history(ctx.output_path, tuner)
