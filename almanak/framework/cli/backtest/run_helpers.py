"""Shared backtest CLI helpers consumed by `pnl_backtest` and `sweep_backtest`.

Extracted in Phase 5B.1 of the CLI CC reduction plan. Functions here are
deliberately minimal and side-effect-compatible with the original inline
implementations so that extracting them does not change observable behaviour:
- `click.echo` strings stay byte-for-byte identical
- Error surface (`click.Abort`, `click.UsageError`) is preserved
- Exit codes are preserved
"""

from __future__ import annotations

from typing import Any

import click

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


def ensure_strategy_id(strategy_instance: Any, *, fallback: str) -> None:
    """Ensure a strategy instance has a non-empty `strategy_id`.

    Mirrors the `_strategy_id`-before-`strategy_id` attribute-setter dance
    from the original inline code: some strategies expose `strategy_id` as a
    read-only property backed by `_strategy_id`, so we prefer assigning the
    private attribute when present. Only runs if the instance does not
    already have a truthy `strategy_id`.

    Args:
        strategy_instance: Instantiated strategy object.
        fallback: Value to assign when `strategy_id` is missing or empty.
    """
    existing_id = getattr(strategy_instance, "strategy_id", "")
    if existing_id:
        return
    if hasattr(strategy_instance, "_strategy_id"):
        strategy_instance._strategy_id = fallback
    else:
        strategy_instance.strategy_id = fallback


def resolve_strategy_class_or_mock(strategy: str, *, allow_mock: bool) -> Any:
    """Resolve a strategy class by name.

    Args:
        strategy: Strategy name (must already pass
            `validate_strategy_is_registered` when `allow_mock=False`).
        allow_mock: If True, fall back to a minimal `MockSweepStrategy`
            when the factory has no registered strategy — preserves the
            existing sweep fallback path (sweep.py:978-1000). If False,
            a missing strategy escalates to `click.Abort` so pnl retains
            its VIB-2917 no-silent-fallback contract.

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

        from ...strategies import MarketSnapshot

        class MockSweepStrategy:
            """Mock strategy for sweep demonstration."""

            strategy_id: str = "mock-sweep"

            def __init__(self, config: dict[str, Any]) -> None:
                self.config = config

            def decide(self, market: MarketSnapshot) -> dict[str, Any] | None:
                return None

        return MockSweepStrategy
