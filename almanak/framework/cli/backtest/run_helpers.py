"""Shared backtest CLI helpers consumed by `pnl_backtest` and `sweep_backtest`.

Extracted in Phase 5B.1 of the CLI CC reduction plan. Functions here are
deliberately minimal and side-effect-compatible with the original inline
implementations so that extracting them does not change observable behaviour:
- `click.echo` strings stay byte-for-byte identical
- Error surface (`click.Abort`, `click.UsageError`) is preserved
- Exit codes are preserved
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

import click

from ...backtesting import PnLBacktestConfig
from ...strategies import get_strategy
from .helpers import list_strategies_fn


def validate_strategy_is_registered(strategy: str) -> None:
    """Ensure `strategy` is registered, aborting with discovery guidance if not.

    Unifies the richer guidance block previously inline in `pnl_backtest`
    (pnl.py:285-298). `sweep_backtest` (sweep.py:901-906) will adopt this
    variant in 5B.3 to replace its terser message.

    Raises:
        click.Abort: if `strategy` is not registered. Output strings match
            the original pnl inline block verbatim; reviewers grep-assert
            these in smoke tests.
    """
    available_strategies = list_strategies_fn()
    if strategy in available_strategies:
        return

    click.echo(f"Error: Strategy '{strategy}' is not registered.", err=True)
    if available_strategies:
        click.echo(
            f"Available strategies: {', '.join(sorted(available_strategies))}",
            err=True,
        )
    click.echo()
    click.echo("The backtest command discovers strategies by:", err=True)
    click.echo("  1. Importing ./strategy.py in the current working directory", err=True)
    click.echo(
        "  2. Scanning ./strategies/ (or $ALMANAK_STRATEGIES_DIR) for <name>/strategy.py",
        err=True,
    )
    click.echo()
    click.echo("Either cd into the strategy directory or set ALMANAK_STRATEGIES_DIR.", err=True)
    click.echo(
        "See registered strategies with: almanak strat backtest pnl --list-strategies",
        err=True,
    )
    click.echo("Create a new strategy with: almanak strat new --name <name>", err=True)
    raise click.Abort()


def parse_token_list(tokens: str) -> list[str]:
    """Split a comma-separated `--tokens` string into an upper-cased list.

    Matches the inline `[t.strip().upper() for t in tokens.split(",")]`
    pattern used in both pnl and sweep commands.
    """
    return [t.strip().upper() for t in tokens.split(",")]


def ensure_deployment_id(strategy_instance: Any, *, fallback: str) -> None:
    """Ensure a strategy instance has a non-empty `deployment_id`.

    Mirrors the `_deployment_id`-before-`deployment_id` attribute-setter dance
    from the original inline code: some strategies expose `deployment_id` as a
    read-only property backed by `_deployment_id`, so we prefer assigning the
    private attribute when present. Only runs if the instance does not
    already have a truthy `deployment_id`.

    Args:
        strategy_instance: Instantiated strategy object.
        fallback: Value to assign when `deployment_id` is missing or empty.
    """
    existing_id = getattr(strategy_instance, "deployment_id", "")
    if existing_id:
        return
    if hasattr(strategy_instance, "_deployment_id"):
        strategy_instance._deployment_id = fallback
    else:
        strategy_instance.deployment_id = fallback


def resolve_strategy_class_or_mock(strategy: str, *, allow_mock: bool) -> Any:
    """Resolve a strategy class by name.

    Args:
        strategy: Strategy name (must already pass
            `validate_strategy_is_registered` when `allow_mock=False`).
        allow_mock: If True, fall back to the shared
            `MockBacktestStrategy` (bound to id ``mock-sweep``) when the
            factory has no registered strategy — preserves the existing
            sweep fallback path. If False, a missing strategy escalates
            to `click.Abort` so pnl retains its VIB-2917 no-silent-
            fallback contract.

    Returns:
        The strategy class (real or mock).
    """
    try:
        return get_strategy(strategy)
    except ValueError:
        if not allow_mock:
            # pnl path: validate_strategy_is_registered should have already
            # handled this; raising Abort here matches its behaviour and
            # keeps output matching when the registry becomes inconsistent
            # between the pre-check and this call.
            click.echo(f"Error: Strategy '{strategy}' is not registered.", err=True)
            raise click.Abort() from None

        click.echo()
        click.echo("Warning: No strategies registered in factory.", err=True)
        click.echo("Running with mock strategy for demonstration.", err=True)
        click.echo()

        # Issue #1701: single consolidated mock. Preserved id "mock-sweep"
        # to keep external output byte-for-byte identical.
        from ...backtesting import make_mock_strategy_class

        return make_mock_strategy_class("mock-sweep")


def build_pnl_config(
    *,
    start_time: datetime,
    end_time: datetime,
    interval_seconds: int,
    initial_capital: float,
    chain: str,
    tokens: list[str],
    gas_price_gwei: float = 30.0,
    include_gas_costs: bool = True,
    allow_degraded_data: bool | None = None,
    preflight_validation: bool | None = None,
    fail_on_preflight_error: bool | None = None,
) -> PnLBacktestConfig:
    """Construct a `PnLBacktestConfig` from CLI-shaped scalar arguments.

    Centralises the `Decimal(str(initial_capital))` / `Decimal(str(gas_price))`
    coercion repeated inline in `pnl_backtest` and `sweep_backtest`.

    The sweep-only robustness kwargs (`allow_degraded_data`,
    `preflight_validation`, `fail_on_preflight_error`) default to ``None`` so
    that pnl callers can omit them entirely — when None, we fall through to the
    `PnLBacktestConfig` dataclass defaults (``True``/``True``/``True``). The
    sweep command overrides these three with values that match the original
    sweep inline construction (``True``/``<first-period-only>``/``False``).

    Note: existing float-coercion behaviour is preserved. Issue #1702 tracks
    moving these to `Decimal` in a dedicated follow-up — 5B.3 is a pure
    refactor and must not change behaviour.
    """
    kwargs: dict[str, Any] = {
        "start_time": start_time,
        "end_time": end_time,
        "interval_seconds": interval_seconds,
        "initial_capital_usd": Decimal(str(initial_capital)),
        "chain": chain,
        "tokens": tokens,
        "gas_price_gwei": Decimal(str(gas_price_gwei)),
        "include_gas_costs": include_gas_costs,
    }
    if allow_degraded_data is not None:
        kwargs["allow_degraded_data"] = allow_degraded_data
    if preflight_validation is not None:
        kwargs["preflight_validation"] = preflight_validation
    if fail_on_preflight_error is not None:
        kwargs["fail_on_preflight_error"] = fail_on_preflight_error
    return PnLBacktestConfig(**kwargs)
