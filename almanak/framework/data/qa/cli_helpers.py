"""Shared helpers for the ``qa-data`` CLI command.

Extracted in Phase 6 of the CC reduction plan. Each helper here is a pure,
side-effect-compatible extraction of an inline block in ``qa_data`` (see
``almanak/framework/data/qa/cli.py``). The goal is to shrink ``qa_data``
without changing observable behaviour:

- ``click.echo`` strings stay byte-for-byte identical.
- ``sys.exit`` codes are preserved (1 on failure, 0 on success).
- Error surface (FileNotFoundError / ValueError from ``load_config``) is
  kept, with the same guidance echoed to stderr.

Helpers intentionally accept concrete arguments rather than the Click
context so they can be unit-tested without a CLI runner.
"""

from __future__ import annotations

import logging
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import click

from .config import QAConfig, load_config


def configure_logging(verbose: bool) -> None:
    """Configure root logging for the ``qa-data`` command.

    Matches the inline behaviour in ``qa_data``:
    - ``verbose=True``  -> DEBUG level
    - ``verbose=False`` -> INFO level
    - Format and datefmt are fixed so log lines stay identical across runs.

    Args:
        verbose: Whether to enable DEBUG-level logging.
    """
    log_level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def load_qa_config_or_exit(config_file: str | None) -> QAConfig:
    """Load a ``QAConfig`` from disk, exiting with status 1 on error.

    Preserves the two distinct echo paths from the original inline block:
    - Non-None ``config_file`` -> ``"Loaded config from: <path>"``
    - None ``config_file``     -> ``"Loaded default config"``

    And the two distinct failure paths:
    - ``FileNotFoundError`` -> ``"Error: <e>"`` on stderr, exit 1.
    - ``ValueError``        -> ``"Invalid config: <e>"`` on stderr, exit 1.

    Args:
        config_file: Optional path to a YAML config file. ``None`` uses the
            bundled default (``config.yaml``).

    Returns:
        Loaded ``QAConfig``.
    """
    try:
        if config_file:
            config = load_config(config_file)
            click.echo(f"Loaded config from: {config_file}")
        else:
            config = load_config()
            click.echo("Loaded default config")
    except FileNotFoundError as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)
    except ValueError as e:
        click.echo(f"Invalid config: {e}", err=True)
        sys.exit(1)

    return config


def apply_cli_overrides(
    config: QAConfig,
    chain: str | None,
    days: int | None,
) -> QAConfig:
    """Return a new ``QAConfig`` with CLI ``--chain`` / ``--days`` applied.

    Mirrors the inline precedence in the original ``qa_data`` body:
    - If ``chain`` is set: rebuild with the new chain, and use ``days`` if
      provided, otherwise keep ``config.historical_days``.
    - Else if ``days`` is set (and ``chain`` is not): rebuild with just the
      new ``historical_days``.
    - Otherwise the original ``config`` is returned unchanged.

    This function deliberately constructs a fresh ``QAConfig`` (rather than
    mutating fields) because ``QAConfig`` is a dataclass and downstream
    callers may hold references to the original.

    Args:
        config: Base configuration loaded from YAML.
        chain: Optional ``--chain`` override.
        days: Optional ``--days`` override.

    Returns:
        Either the original ``config`` (if neither override is set) or a
        new ``QAConfig`` with overrides applied.
    """
    if chain:
        return QAConfig(
            chain=chain,
            historical_days=days if days else config.historical_days,
            timeframe=config.timeframe,
            rsi_period=config.rsi_period,
            thresholds=config.thresholds,
            popular_tokens=config.popular_tokens,
            additional_tokens=config.additional_tokens,
            dex_tokens=config.dex_tokens,
        )
    if days:
        return QAConfig(
            chain=config.chain,
            historical_days=days,
            timeframe=config.timeframe,
            rsi_period=config.rsi_period,
            thresholds=config.thresholds,
            popular_tokens=config.popular_tokens,
            additional_tokens=config.additional_tokens,
            dex_tokens=config.dex_tokens,
        )
    return config


def print_startup_banner(
    config: QAConfig,
    output_path: Path,
    skip_plots: bool,
    test_name: str | None,
) -> None:
    """Print the startup banner exactly as the original inline block did.

    The banner is deliberately identical byte-for-byte to the previous
    inline implementation so that any operator tooling that scrapes this
    output (and our own smoke tests) continues to work.

    Args:
        config: Resolved QA configuration (post-overrides).
        output_path: Directory where the QA report will be written.
        skip_plots: Whether plot generation is disabled.
        test_name: Name of the single test being run, or ``None`` for all.
    """
    click.echo()
    click.echo("=" * 60)
    click.echo("ALMANAK DATA QA FRAMEWORK")
    click.echo("=" * 60)
    click.echo(f"Chain: {config.chain}")
    click.echo(f"Historical days: {config.historical_days}")
    click.echo(f"Timeframe: {config.timeframe}")
    click.echo(f"RSI period: {config.rsi_period}")
    click.echo(f"Popular tokens: {', '.join(config.popular_tokens)}")
    click.echo(f"Additional tokens: {', '.join(config.additional_tokens)}")
    click.echo(f"DEX tokens: {', '.join(config.dex_tokens)}")
    click.echo(f"Output: {output_path}")
    click.echo(f"Skip plots: {skip_plots}")
    if test_name:
        click.echo(f"Running test: {test_name}")
    else:
        click.echo("Running: All tests")
    click.echo("=" * 60)
    click.echo()


def summarize_category(
    results: Sequence[Any],
    label: str,
) -> None:
    """Echo a single Phase F per-category summary line.

    Replaces the 5x near-identical inline blocks in ``qa_data`` that each
    computed ``passed / total`` and echoed a status line for one result
    category (CEX Spot, DEX Spot, CEX Historical, DEX Historical, RSI).

    Empty result lists are a no-op: the original inline block was guarded
    by ``if report.<category>_results:``, so nothing is echoed when the
    category ran zero tests. Preserving that skip is part of the
    byte-identical operator-output contract.

    ``label`` is written verbatim -- the caller is responsible for its
    column alignment (the original inline strings used a fixed 20-char
    wide label ending in ``:`` plus padding). Passing the label fully
    formatted keeps this helper trivially testable and identity-preserving.

    Args:
        results: Category result list from ``QAReport`` (items must have a
            ``.passed`` attribute; list may be empty).
        label: Pre-aligned label string, e.g. ``"CEX Spot Prices:    "``.
            Written verbatim; no trimming or padding is applied.
    """
    if not results:
        return
    passed = sum(1 for r in results if r.passed)
    total = len(results)
    status = "PASS" if passed == total else "FAIL"
    click.echo(f"{label}{passed}/{total} [{status}]")


def echo_category_failures(
    results: Sequence[Any],
    label: str,
) -> None:
    """Echo the Phase G failure detail lines for one category.

    Replaces the 5x near-identical inline ``for`` loops in ``qa_data`` that
    each iterated ``<category>_results`` and echoed one line per failing
    entry. Iteration order is preserved so operator output stays stable.

    The failure line format is locked to the original inline block:

        ``  - <label> <token>: <error or 'validation failed'>``

    An empty result list is a no-op -- the original code looped over empty
    lists implicitly. A category with zero failing entries is also a no-op.

    Args:
        results: Category result list from ``QAReport``. Items must expose
            ``.passed`` (bool), ``.token`` (str), and ``.error`` (str | None).
        label: Failure-line category prefix, e.g. ``"CEX Spot"`` or ``"RSI"``.
            Note this differs from the summary label (no trailing padding).
    """
    for r in results:
        if not r.passed:
            click.echo(f"  - {label} {r.token}: {r.error or 'validation failed'}")


__all__ = [
    "apply_cli_overrides",
    "configure_logging",
    "echo_category_failures",
    "load_qa_config_or_exit",
    "print_startup_banner",
    "summarize_category",
]
