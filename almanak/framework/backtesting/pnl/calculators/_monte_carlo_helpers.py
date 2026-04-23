"""Phase helpers for ``run_monte_carlo``.

``run_monte_carlo`` naturally decomposes into five independent phases:

1. Runtime-default resolution (``mc_config`` / ``fee_models`` /
   ``slippage_models``) — :func:`resolve_runtime_defaults`.
2. Path-count determination + warning — :func:`determine_paths_to_run`.
3. Per-path backtest dispatch (sequential OR semaphored parallel) —
   :func:`dispatch_backtests`.
4. All-failed early return — :func:`build_empty_result`.
5. Aggregation (returns / drawdowns / probabilities / Sharpe) —
   :func:`aggregate_successful_results`.

Splitting these out lets ``run_monte_carlo`` stay well under CC 12 while
preserving the exact numerical behaviour, progress-callback ordering and
``asyncio.gather`` semantics the current code has.

All helpers in this module are plain async/sync Python functions — no
network, no gateway calls. The "backtester runner" is injected as a
callable so tests can run without constructing a real ``PnLBacktester``.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from decimal import Decimal
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from almanak.framework.backtesting.pnl.calculators.monte_carlo import PricePathResult
    from almanak.framework.backtesting.pnl.calculators.monte_carlo_runner import (
        MonteCarloConfig,
        MonteCarloPathBacktestResult,
        MonteCarloSimulationResult,
    )

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Phase 1: runtime default resolution
# ---------------------------------------------------------------------------


def resolve_runtime_defaults(
    mc_config: MonteCarloConfig | None,
    fee_models: dict | None,
    slippage_models: dict | None,
) -> tuple[MonteCarloConfig, dict, dict]:
    """Fill in defaults for ``mc_config`` / ``fee_models`` / ``slippage_models``.

    The ``PnLBacktester`` / ``DefaultFeeModel`` / ``DefaultSlippageModel``
    imports must stay inside ``run_monte_carlo`` to avoid a circular import,
    so those classes are resolved lazily inside this helper via the same
    import-here pattern the original code used.
    """
    # Local imports (circular-import avoidance, unchanged semantics).
    from almanak.framework.backtesting.pnl.calculators.monte_carlo_runner import (
        MonteCarloConfig,
    )
    from almanak.framework.backtesting.pnl.engine import (
        DefaultFeeModel,
        DefaultSlippageModel,
    )

    if mc_config is None:
        mc_config = MonteCarloConfig()
    if fee_models is None:
        fee_models = {"default": DefaultFeeModel()}
    if slippage_models is None:
        slippage_models = {"default": DefaultSlippageModel()}
    return mc_config, fee_models, slippage_models


# ---------------------------------------------------------------------------
# Phase 2: path-count determination
# ---------------------------------------------------------------------------


def determine_paths_to_run(
    paths: PricePathResult,
    requested: int,
    logger_: logging.Logger | None = None,
) -> int:
    """Clamp ``requested`` to the number of available paths.

    Emits a WARNING log (identical wording to the pre-refactor code) when
    the request exceeds available paths. Returns ``min(requested, available)``.

    ``logger_`` lets the caller (``monte_carlo_runner``) provide its own
    logger so ``record.name`` matches the pre-refactor runner module. When
    omitted, the helper falls back to this module's logger.
    """
    n_available = len(paths.paths)
    n_to_run = min(requested, n_available)
    if n_to_run < requested:
        (logger_ or logger).warning(f"Requested {requested} paths but only {n_available} available")
    return n_to_run


# ---------------------------------------------------------------------------
# Phase 3: dispatch
# ---------------------------------------------------------------------------


# Signature of the per-path runner injected by ``run_monte_carlo``.
# ``path_index`` is the only variable parameter per call.
PathRunner = Callable[[int], Awaitable["MonteCarloPathBacktestResult"]]


async def dispatch_backtests(
    n_paths_to_run: int,
    parallel_workers: int,
    run_path: PathRunner,
    progress_callback: Callable[[int, int], None] | None,
) -> list[MonteCarloPathBacktestResult]:
    """Run ``run_path(i)`` for ``i in range(n_paths_to_run)``.

    Preserves the pre-refactor semantics exactly:

    * ``parallel_workers > 1`` -> semaphore-bounded ``asyncio.gather`` with
      the progress callback fired inside each task once the path finishes
      (same ``path_idx + 1`` argument the old code emitted).
    * ``parallel_workers <= 1`` -> simple sequential loop with the progress
      callback fired after each awaited path.
    """
    if parallel_workers > 1:
        return await _dispatch_parallel(
            n_paths_to_run=n_paths_to_run,
            parallel_workers=parallel_workers,
            run_path=run_path,
            progress_callback=progress_callback,
        )
    return await _dispatch_sequential(
        n_paths_to_run=n_paths_to_run,
        run_path=run_path,
        progress_callback=progress_callback,
    )


async def _dispatch_parallel(
    *,
    n_paths_to_run: int,
    parallel_workers: int,
    run_path: PathRunner,
    progress_callback: Callable[[int, int], None] | None,
) -> list[MonteCarloPathBacktestResult]:
    semaphore = asyncio.Semaphore(parallel_workers)

    async def _one(path_idx: int) -> MonteCarloPathBacktestResult:
        async with semaphore:
            result = await run_path(path_idx)
            if progress_callback:
                progress_callback(path_idx + 1, n_paths_to_run)
            return result

    tasks = [_one(i) for i in range(n_paths_to_run)]
    return list(await asyncio.gather(*tasks))


async def _dispatch_sequential(
    *,
    n_paths_to_run: int,
    run_path: PathRunner,
    progress_callback: Callable[[int, int], None] | None,
) -> list[MonteCarloPathBacktestResult]:
    results: list[MonteCarloPathBacktestResult] = []  # noqa: F821
    for i in range(n_paths_to_run):
        result = await run_path(i)
        results.append(result)
        if progress_callback:
            progress_callback(i + 1, n_paths_to_run)
    return results


# ---------------------------------------------------------------------------
# Phase 4: all-failed empty result
# ---------------------------------------------------------------------------


def build_empty_result(
    *,
    n_paths_to_run: int,
    n_failed: int,
    results: list[MonteCarloPathBacktestResult],
    mc_config: MonteCarloConfig,
    paths: PricePathResult,
) -> MonteCarloSimulationResult:
    """Build the empty result used when every path failed.

    The numerical fields match the original code byte-for-byte: all zeros
    except ``probability_negative_return`` which is 1 (no successes => every
    outcome counted as non-positive), and ``individual_results`` only
    included when ``collect_individual_results`` is true.
    """
    from almanak.framework.backtesting.pnl.calculators.monte_carlo_runner import (
        MonteCarloSimulationResult,
    )

    return MonteCarloSimulationResult(
        n_paths=n_paths_to_run,
        n_successful=0,
        n_failed=n_failed,
        return_mean=Decimal("0"),
        return_std=Decimal("0"),
        return_percentile_5th=Decimal("0"),
        return_percentile_25th=Decimal("0"),
        return_percentile_50th=Decimal("0"),
        return_percentile_75th=Decimal("0"),
        return_percentile_95th=Decimal("0"),
        max_drawdown_mean=Decimal("0"),
        max_drawdown_worst=Decimal("0"),
        max_drawdown_percentile_95th=Decimal("0"),
        probability_negative_return=Decimal("1"),
        probability_loss_exceeds_10pct=Decimal("0"),
        probability_loss_exceeds_20pct=Decimal("0"),
        probability_gain_exceeds_10pct=Decimal("0"),
        individual_results=results if mc_config.collect_individual_results else [],
        price_paths_config=paths.to_dict(),
        monte_carlo_config=mc_config.to_dict(),
    )


# ---------------------------------------------------------------------------
# Phase 5: aggregation
# ---------------------------------------------------------------------------


def _calculate_percentile(values: list[Decimal], percentile: float) -> Decimal:
    """Pick the value at ``percentile`` from a SORTED list of Decimals.

    Uses the same index calculation as the pre-refactor private helper
    (``int((p/100) * (n-1))`` clamped to ``[0, n-1]``). Returns
    ``Decimal("0")`` for empty input.
    """
    if not values:
        return Decimal("0")
    idx = int((percentile / 100) * (len(values) - 1))
    idx = max(0, min(idx, len(values) - 1))
    return values[idx]


def _calculate_std(values: list[Decimal], mean: Decimal) -> Decimal:
    """Sample standard deviation via Newton's method square root.

    Matches the pre-refactor private helper exactly:
    * ``< 2`` values -> ``Decimal("0")``.
    * Sample variance (N-1 denominator).
    * 50 Newton iterations starting from ``x = variance``.
    * Non-positive variance short-circuits to zero.
    """
    if len(values) < 2:
        return Decimal("0")
    variance = sum((v - mean) ** 2 for v in values) / Decimal(len(values) - 1)
    if variance <= 0:
        return Decimal("0")
    x = variance
    for _ in range(50):
        x = (x + variance / x) / 2
    return x


def _return_statistics(
    successful: list[MonteCarloPathBacktestResult],
) -> tuple[list[Decimal], Decimal, Decimal]:
    returns = sorted(r.final_return for r in successful)
    mean = sum(returns) / Decimal(len(returns))
    std = _calculate_std(returns, mean)
    return returns, mean, std


def _drawdown_statistics(
    successful: list[MonteCarloPathBacktestResult],
) -> tuple[list[Decimal], Decimal]:
    drawdowns = sorted(r.max_drawdown for r in successful)
    mean = sum(drawdowns) / Decimal(len(drawdowns))
    return drawdowns, mean


def _return_probabilities(
    successful: list[MonteCarloPathBacktestResult],
) -> tuple[Decimal, Decimal, Decimal, Decimal]:
    n = Decimal(len(successful))
    prob_negative = Decimal(sum(1 for r in successful if r.final_return < 0)) / n
    prob_loss_10 = Decimal(sum(1 for r in successful if r.final_return < Decimal("-0.1"))) / n
    prob_loss_20 = Decimal(sum(1 for r in successful if r.final_return < Decimal("-0.2"))) / n
    prob_gain_10 = Decimal(sum(1 for r in successful if r.final_return > Decimal("0.1"))) / n
    return prob_negative, prob_loss_10, prob_loss_20, prob_gain_10


def _drawdown_threshold_probabilities(
    successful: list[MonteCarloPathBacktestResult],
    thresholds: list[Decimal],
) -> dict[str, Decimal]:
    n = Decimal(len(successful))
    out: dict[str, Decimal] = {}
    for threshold in thresholds:
        count_exceeds = sum(1 for r in successful if r.max_drawdown > threshold)
        out[str(threshold)] = Decimal(count_exceeds) / n
    return out


def _sharpe_statistics(
    successful: list[MonteCarloPathBacktestResult],
) -> tuple[Decimal | None, Decimal | None]:
    sharpes = [r.sharpe_ratio for r in successful if r.sharpe_ratio is not None]
    if not sharpes:
        return None, None
    mean = sum(sharpes) / Decimal(len(sharpes))
    std = _calculate_std(sharpes, mean)
    return mean, std


def aggregate_successful_results(
    *,
    results: list[MonteCarloPathBacktestResult],
    successful: list[MonteCarloPathBacktestResult],
    n_paths_to_run: int,
    n_failed: int,
    mc_config: MonteCarloConfig,
    paths: PricePathResult,
) -> MonteCarloSimulationResult:
    """Aggregate successful per-path results into a ``MonteCarloSimulationResult``.

    Pre-condition: ``successful`` is non-empty (caller must use
    :func:`build_empty_result` when that's not the case).
    """
    from almanak.framework.backtesting.pnl.calculators.monte_carlo_runner import (
        MonteCarloSimulationResult,
    )

    returns, return_mean, return_std = _return_statistics(successful)
    drawdowns, max_drawdown_mean = _drawdown_statistics(successful)
    prob_negative, prob_loss_10, prob_loss_20, prob_gain_10 = _return_probabilities(successful)
    prob_drawdown_exceeds = _drawdown_threshold_probabilities(successful, mc_config.drawdown_thresholds)
    sharpe_mean, sharpe_std = _sharpe_statistics(successful)

    return MonteCarloSimulationResult(
        n_paths=n_paths_to_run,
        n_successful=len(successful),
        n_failed=n_failed,
        return_mean=return_mean,
        return_std=return_std,
        return_percentile_5th=_calculate_percentile(returns, 5),
        return_percentile_25th=_calculate_percentile(returns, 25),
        return_percentile_50th=_calculate_percentile(returns, 50),
        return_percentile_75th=_calculate_percentile(returns, 75),
        return_percentile_95th=_calculate_percentile(returns, 95),
        max_drawdown_mean=max_drawdown_mean,
        max_drawdown_worst=drawdowns[-1] if drawdowns else Decimal("0"),
        max_drawdown_percentile_95th=_calculate_percentile(drawdowns, 95),
        probability_negative_return=prob_negative,
        probability_loss_exceeds_10pct=prob_loss_10,
        probability_loss_exceeds_20pct=prob_loss_20,
        probability_gain_exceeds_10pct=prob_gain_10,
        probability_drawdown_exceeds_threshold=prob_drawdown_exceeds,
        sharpe_mean=sharpe_mean,
        sharpe_std=sharpe_std,
        individual_results=results if mc_config.collect_individual_results else [],
        price_paths_config=paths.to_dict(),
        monte_carlo_config=mc_config.to_dict(),
    )


__all__ = [
    "PathRunner",
    "aggregate_successful_results",
    "build_empty_result",
    "determine_paths_to_run",
    "dispatch_backtests",
    "resolve_runtime_defaults",
]
