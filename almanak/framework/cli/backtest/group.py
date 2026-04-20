"""Backtest CLI group definition.

This module defines the top-level `backtest` click group that all subcommands
register onto. It is imported by all submodule files.
"""

from pathlib import Path

import click
from dotenv import load_dotenv


@click.group("backtest")
def backtest() -> None:
    """
    Run backtests for Almanak strategies.

    \b
    Commands (ordered by typical workflow):
      pnl           Single backtest with historical price data
      sweep         Grid search over parameter combinations
      optimize      Bayesian hyperparameter tuning (Optuna TPE)
      walk-forward  Walk-forward optimization with overfitting detection
      monte-carlo   Monte Carlo simulation with synthetic price paths
      scenario      Crisis scenario stress testing
      paper         Paper trading on Anvil forks (live-like simulation)
      dashboard     Interactive Streamlit dashboard

    \b
    Multi-period support (sweep & optimize):
      --periods "2024-quarterly"  Test across Q1-Q4 in one command
      --periods "2024-monthly"    Test across all 12 months
      --periods "rolling-6m"      Six rolling 6-month windows

    \b
    Examples:
      # Single PnL backtest
      almanak backtest pnl -s my_strat --start 2024-01-01 --end 2024-06-01

    \b
      # Grid search over parameters
      almanak backtest sweep -s my_strat --start 2024-01-01 --end 2024-06-01 \\
          --param "window:10,20,30" --param "threshold:0.5,1.0"

    \b
      # Multi-period sweep (test robustness across quarters)
      almanak backtest sweep -s my_strat --periods "2024-quarterly" \\
          --param "window:10,20,30"

    \b
      # Bayesian optimization (finds optimal params automatically)
      almanak backtest optimize -s my_strat --start 2024-01-01 --end 2024-06-01 \\
          --config-file optimize_config.json --n-trials 100

    \b
      # Paper trading
      almanak backtest paper start -s my_strat --chain arbitrum

    \b
      # List available strategies
      almanak backtest pnl --list-strategies
    """
    # Load .env from current directory so backtest commands pick up
    # API keys (COINGECKO_API_KEY, THEGRAPH_API_KEY, ALCHEMY_API_KEY, etc.)
    # the same way 'almanak strat run' does from its working directory.
    env_file = Path.cwd() / ".env"
    if load_dotenv(dotenv_path=env_file):
        click.echo(f"Loaded environment from: {env_file}")
