"""PnL backtest CLI command.

This module provides the `pnl` subcommand for historical price-based backtesting.
"""

import asyncio
import json
import logging
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import aiohttp
import click

from almanak.core.chains import DEFAULT_CHAIN

from ...backtesting import (
    CoinGeckoDataProvider,
    PnLBacktestConfig,
    PnLBacktester,
)
from ...backtesting.config import BacktestDataConfig
from ...backtesting.exceptions import DataSourceUnavailableError
from ...backtesting.models import BacktestResult
from ...backtesting.pnl.config_loader import ConfigLoadError, load_config_from_result
from ...backtesting.pnl.logging_utils import configure_backtest_logging
from ...backtesting.visualization import save_chart
from ...data.cache import CacheStats, DataCache
from ...strategies import get_strategy
from ._backtest_context import PnLBacktestContext
from .group import backtest
from .helpers import (
    _create_backtest_strategy,
    list_strategies_fn,
    load_strategy_config,
    parse_date,
)
from .run_helpers import (
    ensure_deployment_id,
    parse_token_list,
    validate_strategy_is_registered,
)

logger = logging.getLogger(__name__)


# Threshold below which a partial warm-cache run emits a prominent warning.
# A dedicated constant keeps the CI/reproducibility contract visible — if we
# ever want this tunable it becomes a flag rather than a magic number.
_WARM_CACHE_SUCCESS_RATIO_WARN_THRESHOLD = 0.5

# CLI-flag remediation for the LP adapter's missing-volume fail-loud
# (VIB-4849). The engine-level error speaks in config-field terms; this hint
# names the `backtest pnl` flags that map onto them. Emitted both when the
# error propagates out of `backtester.backtest(...)` and when the engine's
# error handler captures it into a partial result.
_MISSING_VOLUME_HINT = (
    "Hint: the engine refuses to fabricate LP volume by default. Re-run with "
    "--pool-volume-usd-daily <usd> (and ideally --pool-liquidity-usd <usd>) to "
    "supply real pool numbers, or --allow-volume-fallback to accept the "
    "LOW-confidence volume_multiplier heuristic. --historical-volume requires a "
    "pool address on the position and a reachable gateway DEX-volume lane."
)


@dataclass
class WarmCacheOutcome:
    """Summary of a warm-cache run.

    Previously `_warm_cache_async` returned just `total_cached` (int),
    which silently discarded per-token + overall failure information.
    Issue #1698 surfaces both counts so callers can (a) warn operators
    below a success-ratio threshold and (b) abort under `--strict-warm`.
    """

    total_cached: int
    successful_warms: int
    total_tokens: int
    # True iff the outer `asyncio.run` / outer loop failed catastrophically
    # (separate from per-token errors which increment `total_tokens` but
    # leave `successful_warms` unchanged).
    overall_failed: bool = False
    overall_error: str | None = None

    @property
    def success_ratio(self) -> float:
        if self.total_tokens == 0:
            return 1.0
        return self.successful_warms / self.total_tokens


# =============================================================================
# Phase helpers (Phase 5B.1 extractions)
# =============================================================================


def _handle_list_strategies() -> bool:
    """Phase 1: emit `--list-strategies` output and signal early exit.

    Returns:
        True if the caller should return immediately (list was displayed).
    """
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
    return True


def _load_config_from_result(
    from_result: str,
) -> tuple[PnLBacktestConfig, dict[str, Any], bool]:
    """Phase 2: resolve `--from-result` into config + metadata.

    Preserves the original stdout/stderr ordering: info line first, warnings
    next (on stderr), then metadata echo. FileNotFoundError and
    ConfigLoadError both become `click.Abort` with identical prefix strings.

    Returns:
        Tuple of (pnl_config, result_metadata, loaded_from_result=True).
    """
    try:
        click.echo(f"Loading config from previous result: {from_result}")
        load_result = load_config_from_result(from_result)
        pnl_config = load_result.config
        result_metadata = load_result.metadata

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

        return pnl_config, result_metadata, True

    except FileNotFoundError as e:
        click.echo(f"Error: {e}", err=True)
        raise click.Abort() from e
    except ConfigLoadError as e:
        click.echo(f"Error loading config: {e}", err=True)
        raise click.Abort() from e


def _validate_and_build_context(
    strategy: str | None,
    start: datetime | None,
    end: datetime | None,
    interval: int,
    initial_capital: float,
    chain: str,
    tokens: str,
    gas_price: float,
    output: str | None,
    loaded_from_result: bool,
    pnl_config: PnLBacktestConfig | None,
) -> PnLBacktestContext:
    """Phases 3+4: validate required args and produce a `PnLBacktestContext`.

    When `loaded_from_result` is False, constructs a fresh `PnLBacktestConfig`
    from the CLI args; otherwise reuses the loaded `pnl_config` and derives
    the display token list from it.
    """
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

    # Validate strategy exists. The `get_strategy()` call in the backtest body below
    # raises ValueError when the registry has no matching entry, but we surface the
    # richer discovery guidance here so it isn't shadowed by later failure paths.
    # VIB-2917: previously fell back to a silent mock strategy that produced fake
    # results; now the strategy must be discoverable via `./strategy.py` in cwd or
    # via `./strategies/<name>/strategy.py` (optionally $ALMANAK_STRATEGIES_DIR).
    validate_strategy_is_registered(strategy)

    # Create PnL backtest config if not loaded from result
    if not loaded_from_result:
        token_list = parse_token_list(tokens)
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

    # Explicit runtime guard. `pnl_config` can only be None at this point if
    # `--from-result` neither loaded one (earlier branch) nor did we build one
    # from CLI args (the `loaded_from_result` branch above) — in practice that
    # state should be unreachable, but `-O` would strip a bare `assert`, so we
    # raise a `click.Abort` with a user-visible stderr line instead (#1700).
    if pnl_config is None:
        click.echo(
            "Error: internal error — PnL backtest config was not constructed. "
            "Pass --strategy/--start/--end or --from-result.",
            err=True,
        )
        raise click.Abort()

    if loaded_from_result:
        # Use loaded config's values for display (guard above proves non-None).
        token_list = pnl_config.tokens

    return PnLBacktestContext(
        strategy=strategy,
        pnl_config=pnl_config,
        token_list=token_list,
        output_path=Path(output) if output else None,
        loaded_from_result=loaded_from_result,
        start=start,
        end=end,
        interval=interval,
    )


def _print_pnl_configuration(
    ctx: PnLBacktestContext,
    from_result: str | None,
    warm_cache: bool,
) -> None:
    """Phase 5: emit the configuration banner block.

    Preserves the original click.echo ordering and formatting byte-for-byte.
    """
    pnl_config = ctx.pnl_config
    click.echo("=" * 60)
    click.echo("PNL BACKTEST CONFIGURATION")
    if ctx.loaded_from_result:
        click.echo(f"(Loaded from: {from_result})")
    click.echo("=" * 60)
    click.echo(f"Strategy: {ctx.strategy}")
    click.echo(f"Chain: {pnl_config.chain}")
    click.echo(
        f"Period: {pnl_config.start_time.date()} -> {pnl_config.end_time.date()} ({pnl_config.duration_days:.1f} days)"
    )
    click.echo(f"Interval: {pnl_config.interval_seconds}s ({pnl_config.interval_seconds / 3600:.1f} hours)")
    click.echo(f"Initial Capital: ${pnl_config.initial_capital_usd:,.2f}")
    click.echo(f"Tokens: {', '.join(ctx.token_list)}")
    click.echo(f"Gas Price: {pnl_config.gas_price_gwei} Gwei")
    click.echo(f"Estimated Ticks: ~{pnl_config.estimated_ticks:,}")
    click.echo(f"Warm Cache: {'Yes' if warm_cache else 'No'}")

    if ctx.output_path:
        click.echo(f"Output: {ctx.output_path}")

    click.echo("=" * 60)


def _build_volume_data_config(
    historical_volume: bool | None,
    pool_volume_usd_daily: float | None,
    pool_liquidity_usd: float | None,
    allow_volume_fallback: bool,
) -> BacktestDataConfig | None:
    """Phase 5b: map the LP volume-source flags onto a `BacktestDataConfig`.

    Returns None when no volume flag was provided so the backtester keeps the
    historical no-`data_config` behaviour on every other invocation path. The
    refuse-to-fabricate default (VIB-4849) is preserved: nothing here opts in
    to the LOW-confidence heuristic unless `--allow-volume-fallback` was passed
    explicitly, in which case a prominent warning is emitted on stderr.

    Also echoes the chosen volume-source settings on stdout so the run record
    shows where LP fee numbers came from.
    """
    if (
        historical_volume is None
        and pool_volume_usd_daily is None
        and pool_liquidity_usd is None
        and not allow_volume_fallback
    ):
        return None

    if historical_volume is not None:
        click.echo(f"LP Historical Volume: {'enabled' if historical_volume else 'disabled'}")
    if pool_volume_usd_daily is not None:
        click.echo(f"LP Pool Volume (explicit): ${pool_volume_usd_daily:,.2f}/day")
    if pool_liquidity_usd is not None:
        click.echo(f"LP Pool Liquidity (explicit): ${pool_liquidity_usd:,.2f}")
    if allow_volume_fallback:
        click.echo("LP Volume Fallback: enabled (LOW confidence)")
        click.echo(
            "Warning: --allow-volume-fallback accepts the LOW-confidence "
            "volume_multiplier heuristic (position value x multiplier) when no "
            "real volume data is available. LP fee estimates can be off by an "
            "order of magnitude.",
            err=True,
        )

    kwargs: dict[str, Any] = {"allow_volume_fallback": allow_volume_fallback}
    if historical_volume is not None:
        kwargs["use_historical_volume"] = historical_volume
    if pool_volume_usd_daily is not None:
        kwargs["explicit_pool_volume_usd_daily"] = Decimal(str(pool_volume_usd_daily))
    if pool_liquidity_usd is not None:
        kwargs["explicit_pool_liquidity_usd"] = Decimal(str(pool_liquidity_usd))
    return BacktestDataConfig(**kwargs)


# =============================================================================
# Phase 5B.2 extractions: execution + output helpers
# =============================================================================


async def _warm_cache_async(
    data_provider: CoinGeckoDataProvider,
    cache: DataCache,
    token_list: list[str],
    start: datetime | None,
    end: datetime | None,
    interval: int,
    pnl_config: PnLBacktestConfig,
) -> WarmCacheOutcome:
    """Pre-fetch OHLCV data into `cache` and return a structured outcome.

    Issue #1698: previous contract returned just `total_cached` (int),
    silently losing per-token failure info. We now track
    `successful_warms / total_tokens` so the caller can:

    - emit a warning below a success threshold, and
    - honour a `--strict-warm` flag that makes any per-token failure fatal.

    Per-token failures still log a warning on stderr (unchanged stderr
    contract), and the outer `DataProvider.close()` still runs in the
    `finally` block regardless of per-token errors.
    """
    from ...data.cache import CacheKey, OHLCVData

    total_cached = 0
    successful_warms = 0
    total_tokens = len(token_list)
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

                cached_count = cache.set_batch(items)
                total_cached += cached_count
                successful_warms += 1
                click.echo(f"  Cached {cached_count} data points for {token}")

            except Exception as e:
                click.echo(f"  Warning: Failed to cache data for {token}: {e}", err=True)
    finally:
        await data_provider.close()

    return WarmCacheOutcome(
        total_cached=total_cached,
        successful_warms=successful_warms,
        total_tokens=total_tokens,
    )


def _warm_cache(
    ctx: PnLBacktestContext,
    start: datetime | None,
    end: datetime | None,
    interval: int,
    *,
    strict: bool = False,
) -> DataCache | None:
    """Phase 9: pre-warm the OHLCV cache for the backtest.

    Creates a fresh `DataCache`, uses a dedicated `CoinGeckoDataProvider` to
    pre-fetch OHLCV data for each token, then closes that provider. The
    caller creates its own provider for the backtest run itself — we do NOT
    reuse the warming provider.

    Issue #1698:
    - Tracks `successful_warms / total_tokens` via `WarmCacheOutcome`.
    - When `strict=True` (driven by `--strict-warm` on the CLI), any
      per-token failure OR any overall `asyncio.run` failure aborts with
      `click.Abort` — critical for CI/reproducibility runs where a silently
      partial cache changes downstream results.
    - When `strict=False` (default, preserves historical behaviour), failures
      emit the original banner strings byte-for-byte, and a new prominent
      warning is added when the success ratio falls below
      `_WARM_CACHE_SUCCESS_RATIO_WARN_THRESHOLD`.

    Returns:
        The `DataCache` instance (populated or empty) on success.
        Returns `None` when overall warming fails (e.g. `asyncio.run` raises,
        including provider-close errors inside `_warm_cache_async`'s
        `finally`). Returning `None` avoids handing callers a partially
        populated cache that would misrepresent downstream cache-stat
        reporting. Callers already treat `None`/missing cache safely by
        building a fresh provider/cache for the backtest run. In strict mode
        we still raise `click.Abort` before returning.
    """
    from ...backtesting.pnl.providers.coingecko import RetryConfig

    click.echo()
    click.echo("Warming data cache...")
    cache = DataCache(ttl_seconds=0)
    cache.reset_stats()

    data_provider = CoinGeckoDataProvider(
        retry_config=RetryConfig.for_backtest(),
        persistent_cache=True,
        historical_cache_ttl=0,
    )

    outcome: WarmCacheOutcome
    overall_failed = False
    try:
        outcome = asyncio.run(
            _warm_cache_async(
                data_provider=data_provider,
                cache=cache,
                token_list=ctx.token_list,
                start=start,
                end=end,
                interval=interval,
                pnl_config=ctx.pnl_config,
            )
        )
        click.echo(
            f"Cache warming complete: {outcome.total_cached} total data points "
            f"({outcome.successful_warms}/{outcome.total_tokens} tokens successful)"
        )
    except Exception as e:
        # Preserved stderr + stdout strings — external log scrapers grep both.
        click.echo(f"Warning: Cache warming failed: {e}", err=True)
        click.echo("Proceeding with backtest without pre-warmed cache...")
        outcome = WarmCacheOutcome(
            total_cached=0,
            successful_warms=0,
            total_tokens=len(ctx.token_list),
            overall_failed=True,
            overall_error=str(e),
        )
        overall_failed = True

    partial = outcome.successful_warms < outcome.total_tokens
    if strict and (outcome.overall_failed or partial):
        # CI / reproducibility contract: do not silently continue with a
        # partial cache when the user explicitly opted into strict mode.
        if outcome.overall_failed:
            msg = f"Strict warm-cache: overall warming failed ({outcome.overall_error})."
        else:
            msg = (
                f"Strict warm-cache: only {outcome.successful_warms}/{outcome.total_tokens} tokens warmed successfully."
            )
        click.echo(f"Error: {msg}", err=True)
        raise click.Abort()

    if not strict and partial and outcome.total_tokens > 0:
        ratio = outcome.success_ratio
        if ratio < _WARM_CACHE_SUCCESS_RATIO_WARN_THRESHOLD:
            click.echo(
                f"Warning: warm cache only succeeded for {outcome.successful_warms}/"
                f"{outcome.total_tokens} tokens ({ratio:.0%}). Results may be unreliable. "
                "Use --strict-warm to fail fast on partial cache warms.",
                err=True,
            )

    # On overall failure (e.g. `asyncio.run` raised, including provider-close
    # errors in `_warm_cache_async`'s `finally`) we cannot reliably report how
    # many tokens were cached before the exception. Returning the partial
    # `cache` object would make the downstream `Proceeding with backtest
    # without pre-warmed cache...` message contradict `cache.stats`, so drop
    # the cache here. Callers already handle `None` safely (they build a
    # fresh provider/cache for the backtest run regardless).
    if overall_failed:
        return None

    return cache


def _run_backtest(
    backtester: PnLBacktester,
    strategy_instance: Any,
    pnl_config: PnLBacktestConfig,
) -> BacktestResult:
    """Phase 10: run the backtest in a fresh event loop.

    Preserves the exact error string `"Error running backtest: {e}"` and the
    `sys.exit(1)` exit code from the original inline block. Any exception from
    `asyncio.run(backtester.backtest(...))` is surfaced via stderr and ends
    the process.

    A missing-volume `DataSourceUnavailableError` (the VIB-4849 fail-loud) is
    additionally followed by a hint pointing at the CLI flags that resolve it,
    since the engine-level remediation speaks in config-field terms.
    """
    try:
        return asyncio.run(backtester.backtest(strategy_instance, pnl_config))
    except DataSourceUnavailableError as e:
        click.echo(f"Error running backtest: {e}", err=True)
        if e.data_type == "volume":
            click.echo(_MISSING_VOLUME_HINT, err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"Error running backtest: {e}", err=True)
        sys.exit(1)


def _emit_missing_volume_hint_for_result(result: BacktestResult) -> None:
    """Surface the CLI-flag hint when a result carries a missing-volume error.

    The engine's `BacktestErrorHandler` classifies the LP adapter's
    `DataSourceUnavailableError` as fatal and *stops* the simulation, but
    `backtester.backtest(...)` then returns the partial result (with the error
    recorded in `result.error` / `result.errors`) instead of raising. Without
    this check the CLI would print an empty results block and exit 0 with the
    remediation buried in the JSON output.
    """
    candidates: list[str] = []
    if result.error:
        candidates.append(result.error)
    for record in result.errors:
        if record.get("error_type") == "DataSourceUnavailableError":
            candidates.append(str(record.get("error_message", "")))

    if not any("volume data source" in message for message in candidates):
        return

    click.echo()
    click.echo(
        "Error: the backtest stopped early because no acceptable LP volume source was available (see results above).",
        err=True,
    )
    click.echo(_MISSING_VOLUME_HINT, err=True)


def _compute_strategy_returns(equity_curve: list[Any]) -> list[Decimal]:
    """Return per-step Decimal returns from an equity curve.

    Preserves the original inline loop's behaviour: when `prev_val <= 0` the
    step's return is recorded as `Decimal("0")` rather than raising. Consumers
    must still compare `len(returns)` to `len(benchmark_returns)` — this
    function does not truncate.
    """
    returns: list[Decimal] = []
    for i in range(1, len(equity_curve)):
        prev_val = equity_curve[i - 1].value_usd
        curr_val = equity_curve[i].value_usd
        if prev_val > 0:
            returns.append((curr_val - prev_val) / prev_val)
        else:
            returns.append(Decimal("0"))
    return returns


async def _fetch_benchmark_returns(
    benchmark: str,
    start: datetime,
    end: datetime,
    interval: int,
) -> tuple[list[Decimal], Decimal]:
    """Fetch benchmark period returns and total return for the window.

    Lifted from the inline `_fetch_benchmark` coroutine. Uses the `Benchmark`
    enum resolver so callers pass the raw `--benchmark` CLI value.
    """
    from ...backtesting.pnl.providers.benchmark import (
        Benchmark,
        get_benchmark_returns,
        get_benchmark_total_return,
    )

    benchmark_enum = Benchmark.from_string(benchmark)
    returns = await get_benchmark_returns(benchmark_enum, start, end, interval)
    total = await get_benchmark_total_return(benchmark_enum, start, end)
    return returns, total


def _print_benchmark_comparison(
    ctx: PnLBacktestContext,
    result: BacktestResult,
    benchmark: str,
    start: datetime | None,
    end: datetime | None,
    interval: int,
) -> None:
    """Phase 12: render the benchmark comparison block.

    No-op when `benchmark`/`start`/`end` are not all present (matches the
    original guard). Catches a narrow tuple of expected network / data
    errors (#1699) — `TimeoutError` (builtin alias for
    `asyncio.TimeoutError`), `aiohttp.ClientError`, `ValueError`, and
    `KeyError`. Unexpected exceptions propagate so they surface as bugs
    instead of being silently masked by the banner line. The full traceback
    is logged at DEBUG so operators running under `--verbose` still see
    exactly what went wrong.
    """
    if not (benchmark and start and end):
        return

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

        benchmark_returns, benchmark_total = asyncio.run(_fetch_benchmark_returns(benchmark, start, end, interval))

        if result.equity_curve and len(result.equity_curve) >= 2:
            strategy_returns = _compute_strategy_returns(result.equity_curve)

            min_len = min(len(strategy_returns), len(benchmark_returns))
            if min_len >= 2:
                strategy_returns = strategy_returns[:min_len]
                benchmark_returns = benchmark_returns[:min_len]

                info_ratio = calculate_information_ratio(strategy_returns, benchmark_returns)
                beta_val = calculate_beta(strategy_returns, benchmark_returns)

                # total_return_pct is a percentage (e.g. 15 for 15%); divide by 100 to
                # get the ratio that calculate_alpha expects (same convention as benchmark_total).
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

    except (TimeoutError, aiohttp.ClientError, ValueError, KeyError) as e:
        # Preserved error string — external log scrapers grep this line.
        click.echo(f"Could not calculate benchmark metrics: {e}")
        logger.debug("Benchmark comparison failed", exc_info=True)

    click.echo("-" * 60)
    # Note: ctx param is accepted for signature symmetry with other helpers.
    # Read-only usage lets future enhancements (e.g. chain-specific benchmarks)
    # avoid a signature churn.
    del ctx


def _print_cache_stats(cache_stats: CacheStats | None) -> None:
    """Phase 13: render the cache statistics block.

    No-op when `cache_stats is None` (matches the original guard).
    """
    if cache_stats is None:
        return

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


def _print_verbose_trades(result: BacktestResult, verbose: bool) -> None:
    """Phase 14: render the verbose trade history block.

    No-op unless both `verbose=True` and `result.trades` is non-empty, matching
    the original guard.
    """
    if not (verbose and result.trades):
        return

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


def _write_json_output(
    result: BacktestResult,
    output_path: Path | None,
    benchmark: str,
    cache_stats: CacheStats | None,
) -> None:
    """Phase 15: write the full JSON result to `output_path` if requested.

    Preserves the exact JSON schema: top-level keys come from `result.to_dict()`
    with a `_meta` dict appended (generated_at, generator, engine, benchmark)
    and `cache_stats` appended only when stats were collected. Key order and
    naming are load-bearing — external tooling reads this file.
    """
    if output_path is None:
        return

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


def _chart_output_path(
    strategy: str | None,
    output_path: Path | None,
    chart_format: str,
) -> Path:
    """Derive the chart output path.

    Mirrors the inline rules: `.html` for html, `.png` otherwise; alongside
    `output_path` when provided; otherwise `equity_curve_<strategy>.<ext>` in
    the current directory.
    """
    chart_extension = ".html" if chart_format.lower() == "html" else ".png"
    if output_path:
        return output_path.with_suffix(chart_extension)
    safe_strategy_name = strategy.replace("/", "_").replace("\\", "_") if strategy else "backtest"
    return Path(f"equity_curve_{safe_strategy_name}{chart_extension}")


def _generate_chart(
    result: BacktestResult,
    strategy: str | None,
    output_path: Path | None,
    chart_format: str,
) -> None:
    """Phase 16: generate the equity curve chart via `save_chart`.

    Emits the same status lines and counts as the original inline block.
    """
    click.echo()
    click.echo("Generating equity curve chart...")

    chart_path = _chart_output_path(strategy, output_path, chart_format)

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


def _generate_html_report(
    result: BacktestResult,
    strategy: str | None,
    output_path: Path | None,
) -> None:
    """Phase 17: generate an HTML report via `generate_report`.

    NOTE: a shared `write_html_report` helper is planned for
    `run_helpers.py` but is not yet available from 5B.1. The logic here is
    inlined to match the original byte-for-byte; consolidate in 5B.3 when the
    sweep command adopts the same helper.
    """
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


# crap-allowlist: VIB-4722 mechanical deployment_id rename in existing high-CRAP function.
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
    "--strict-warm",
    is_flag=True,
    default=False,
    help=(
        "With --warm-cache, abort if any token fails to warm (or the overall "
        "warm-up fails). Intended for CI / reproducibility runs where a "
        "partially warmed cache would silently change results."
    ),
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
@click.option(
    "--historical-volume/--no-historical-volume",
    "historical_volume",
    default=None,
    help=(
        "Fetch historical pool volume for LP fee accrual (gateway-backed DEX volume "
        "lane; requires a pool address on the position). Defaults to the engine "
        "default (enabled). When the lookup yields nothing the engine still refuses "
        "to fabricate volume unless --pool-volume-usd-daily or "
        "--allow-volume-fallback is provided."
    ),
)
@click.option(
    "--pool-volume-usd-daily",
    type=click.FloatRange(min=0),
    default=None,
    help=(
        "Explicit daily pool volume in USD for LP fee accrual (HIGH confidence, no "
        "subgraph or gateway dependency). 0 is a valid measured zero."
    ),
)
@click.option(
    "--pool-liquidity-usd",
    type=click.FloatRange(min=0, min_open=True),
    default=None,
    help=(
        "Explicit pool TVL in USD used as the LP liquidity-share denominator. Pair "
        "with --pool-volume-usd-daily for a fully user-specified fee estimate."
    ),
)
@click.option(
    "--allow-volume-fallback",
    is_flag=True,
    default=False,
    help=(
        "Opt in to the LOW-confidence volume_multiplier heuristic when no real LP "
        "volume data is available. Off by default: the engine refuses to fabricate "
        "volume and fails loud instead (VIB-4849)."
    ),
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
    strict_warm: bool,
    chart: bool,
    chart_format: str,
    report: bool,
    benchmark: str,
    from_result: str | None,
    historical_volume: bool | None,
    pool_volume_usd_daily: float | None,
    pool_liquidity_usd: float | None,
    allow_volume_fallback: bool,
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

    Exit codes:
    - 0: backtest completed (recoverable data errors may still be recorded
      in the result's error summary)
    - 1: backtest failed -- the engine raised, or it stopped on a fatal
      error and returned a partial result; results and JSON output are
      still printed/written before exiting
    - 2: usage error (invalid flags or arguments)

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

        # LP backtest with explicit pool volume + TVL (no subgraph dependency)
        almanak backtest pnl -s my_lp_strategy --start 2025-11-01 --end 2025-11-15 \\
            --pool-volume-usd-daily 5000000 --pool-liquidity-usd 2000000

        # LP backtest accepting the LOW-confidence volume heuristic
        almanak backtest pnl -s my_lp_strategy --start 2025-11-01 --end 2025-11-15 \\
            --allow-volume-fallback

        # List available strategies
        almanak backtest pnl --list-strategies
    """
    # Phase 1: --list-strategies early exit
    if list_strategies:
        _handle_list_strategies()
        return

    # Phase 2: --from-result load (may abort)
    pnl_config: PnLBacktestConfig | None = None
    loaded_from_result = False
    if from_result:
        pnl_config, _result_metadata, loaded_from_result = _load_config_from_result(from_result)

    # Phases 3+4: validate + build context
    ctx = _validate_and_build_context(
        strategy=strategy,
        start=start,
        end=end,
        interval=interval,
        initial_capital=initial_capital,
        chain=chain,
        tokens=tokens,
        gas_price=gas_price,
        output=output,
        loaded_from_result=loaded_from_result,
        pnl_config=pnl_config,
    )

    # Configure logging based on verbose flag
    configure_backtest_logging(verbose=verbose)

    # Phase 5: display configuration banner
    _print_pnl_configuration(ctx, from_result, warm_cache)

    # Phase 5b: LP volume-source flags -> BacktestDataConfig (None when no flag
    # was passed, preserving the historical no-data_config behaviour). Runs
    # before the dry-run early return so flag echo + the LOW-confidence
    # fallback warning appear on every invocation path.
    volume_data_config = _build_volume_data_config(
        historical_volume=historical_volume,
        pool_volume_usd_daily=pool_volume_usd_daily,
        pool_liquidity_usd=pool_liquidity_usd,
        allow_volume_fallback=allow_volume_fallback,
    )

    # `--strict-warm` without `--warm-cache` is inert — surface it on every
    # invocation path (including `--dry-run`) so the contract is uniform.
    # Must run BEFORE the dry-run early return; otherwise dry-run invocations
    # silently swallow the warning even though the flag is equally inert.
    if strict_warm and not warm_cache:
        click.echo(
            "Warning: --strict-warm has no effect without --warm-cache.",
            err=True,
        )

    # Phase 6: --dry-run early exit
    if dry_run:
        click.echo()
        click.echo("Dry run - backtest not executed.")
        return

    # Phase 7: load strategy configuration and build instance
    if config_file:
        with open(config_file) as f:
            strategy_config = json.load(f)
        click.echo(f"Loaded config from: {config_file}")
    else:
        strategy_config = load_strategy_config(ctx.strategy, ctx.pnl_config.chain)

    # Resolve strategy class. The earlier validation guarantees the strategy is
    # registered, so get_strategy() must not raise here.
    strategy_class = get_strategy(ctx.strategy)
    strategy_instance = _create_backtest_strategy(strategy_class, strategy_config, ctx.pnl_config.chain)

    fallback_id = (
        strategy_config.get("deployment_id")
        or strategy_config.get("name")
        or ctx.strategy
        or strategy_instance.__class__.__name__
    )
    ensure_deployment_id(strategy_instance, fallback=fallback_id)

    # Phase 8: initialize data provider for the backtest run
    click.echo()
    click.echo("Initializing CoinGecko data provider...")
    from ...backtesting.pnl.providers.coingecko import RetryConfig

    # Phase 9: warm cache (uses its own provider internally; closes it when done).
    # The `--strict-warm without --warm-cache` no-op warning is emitted earlier,
    # before the dry-run early return, so it fires on every invocation path.
    cache: DataCache | None = None
    if warm_cache:
        cache = _warm_cache(ctx, start, end, interval, strict=strict_warm)

    # Fresh data provider for the backtest run. Matches the original two-step
    # sequence: warming uses a throwaway provider (closed in its own finally),
    # then we create this one for the actual backtest.
    data_provider = CoinGeckoDataProvider(
        retry_config=RetryConfig.for_backtest(),
        persistent_cache=True,
        historical_cache_ttl=0,
    )

    # Phase 10: run the backtest
    backtester = PnLBacktester(
        data_provider=data_provider,
        fee_models={},
        slippage_models={},
        data_config=volume_data_config,
    )

    click.echo()
    click.echo("Starting PnL backtest...")
    click.echo()

    result = _run_backtest(backtester, strategy_instance, ctx.pnl_config)

    cache_stats: CacheStats | None = cache.stats if cache is not None else None

    # Phase 11: display results summary
    click.echo()
    click.echo("=" * 60)
    click.echo("BACKTEST RESULTS")
    click.echo("=" * 60)
    click.echo(result.summary())

    # Missing-volume fail-loud captured into the result by the engine's error
    # handler (rather than raised) still gets the CLI-flag hint.
    _emit_missing_volume_hint_for_result(result)

    # Phases 12-14: print benchmark, cache, verbose-trade sections
    _print_benchmark_comparison(ctx, result, benchmark, start, end, interval)
    _print_cache_stats(cache_stats)
    _print_verbose_trades(result, verbose)

    # Phases 15-17: write JSON + optional chart + optional HTML report
    _write_json_output(result, ctx.output_path, benchmark, cache_stats)
    if chart:
        _generate_chart(result, ctx.strategy, ctx.output_path, chart_format)
    if report:
        _generate_html_report(result, ctx.strategy, ctx.output_path)

    # Phase 18: exit-code contract. The engine converts fatal simulation
    # errors into a partial result with `error` set instead of raising
    # (`build_error_result`, pnl/_engine_helpers.py), so without this gate
    # scripts/CI would see exit 0 for a backtest that processed zero ticks.
    # All artifacts above (results block, JSON output, chart, report) are
    # still produced first — the JSON carries the diagnostic detail, the
    # exit code carries the verdict. Recoverable errors in `result.errors`
    # with no fatal `result.error` keep exit 0: the simulation completed.
    if not result.success:
        click.echo()
        click.echo(f"Error: backtest failed: {result.error}", err=True)
        sys.exit(1)

    # Phase 19: post-backtest tip
    click.echo()
    click.echo("Tip: Try 'almanak backtest sweep' to test multiple parameter combinations,")
    click.echo("     or 'almanak backtest optimize' for Bayesian hyperparameter tuning.")
