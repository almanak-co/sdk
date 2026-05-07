"""Backtest CLI group definition.

Defines the top-level ``backtest`` click group plus a lazy
``MultiCommand`` subclass that only imports a subcommand's submodule when
click resolves that subcommand. Importing this module is therefore cheap:
no ``framework.backtesting`` deps (optuna, sqlalchemy, plotly, matplotlib,
…) load until the user actually invokes ``almanak strat backtest <cmd>``.

Subcommands continue to register themselves via ``@backtest.command(...)``
or ``@backtest.group(...)`` decorators in their own files; the lazy group
just defers the import that runs those decorators.
"""

import importlib
from pathlib import Path
from typing import ClassVar

import click

from almanak.config.env import _load_dotenv_once


class LazyBacktestGroup(click.Group):
    """Click group that loads its subcommand modules on first lookup.

    The ``framework.cli.backtest`` package is imported eagerly by
    ``almanak/cli/cli.py`` (``from almanak.framework.cli import backtest as
    framework_backtest_group``) so ``almanak strat run`` inside the deployed
    strategy container loads it on every startup. Each subcommand module
    pulls in the backtesting engine (optuna, sqlalchemy, plotly, …), which
    is irrelevant to a running strategy. By deferring the submodule import
    to ``get_command`` / ``list_commands`` time, the strategy container
    pays zero backtesting cost.
    """

    # Map a click subcommand name to the relative submodule that defines
    # it. Multiple commands can map to the same submodule — importing the
    # submodule once registers all its decorated commands. Keep this in
    # sync with the @backtest.command / @backtest.group decorator names
    # across the package; the test in
    # tests/framework/cli/test_imports_lean.py guards against unwanted
    # eager imports.
    _SUBCOMMAND_MODULES: ClassVar[dict[str, str]] = {
        "pnl": "pnl",
        "sweep": "sweep",
        "optimize": "sweep",
        "paper": "paper",
        "dashboard": "paper",
        "block": "block",
        "walk-forward": "advanced",
        "monte-carlo": "advanced",
        "scenario": "advanced",
    }

    def list_commands(self, ctx: click.Context) -> list[str]:
        # ``--help`` (and shell completion) want the full subcommand list,
        # so force-load every submodule before delegating to the base
        # implementation.
        for submodule in set(self._SUBCOMMAND_MODULES.values()):
            self._ensure_loaded(submodule)
        return super().list_commands(ctx)

    def get_command(self, ctx: click.Context, name: str) -> click.Command | None:
        if name in self._SUBCOMMAND_MODULES and name not in self.commands:
            self._ensure_loaded(self._SUBCOMMAND_MODULES[name])
        return super().get_command(ctx, name)

    @staticmethod
    def _ensure_loaded(submodule: str) -> None:
        importlib.import_module(f"almanak.framework.cli.backtest.{submodule}")


@click.group("backtest", cls=LazyBacktestGroup)
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
    # Load .env from current directory through the config-service boundary
    # so backtest commands pick up API keys (COINGECKO_API_KEY,
    # THEGRAPH_API_KEY, ALCHEMY_API_KEY, etc.) the same way
    # 'almanak strat run' does from its working directory.
    env_file = Path.cwd() / ".env"
    if env_file.exists():
        _load_dotenv_once(str(env_file))
        click.echo(f"Loaded environment from: {env_file}")
