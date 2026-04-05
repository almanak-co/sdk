"""Legacy block-based backtest CLI command (REMOVED).

This module provides the `block` subcommand which displays a migration message.
"""

import sys

import click

from .group import backtest


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
