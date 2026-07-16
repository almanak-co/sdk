"""Single source of truth for the ``almanak strat run`` option set (VIB-5846).

``almanak strat run`` is declared by two commands: the user-facing wrapper
``strategy_run`` in :mod:`almanak.cli.cli` and the framework command
:func:`almanak.framework.cli.run.run` that the wrapper (and ``strat test``)
reaches via ``ctx.invoke``. Historically each hand-maintained its own
``@click.option`` stack; the wrapper silently drifted and dropped
``--debug`` / ``--list`` / ``--dashboard-mode`` / ``--simulate-tx`` — those
flags were rejected at the CLI while the framework default was used with no
error anywhere (VIB-5063 / VIB-5846).

``strategy_run_options`` is that one stack. Both commands apply it, so the two
declarations can no longer diverge;
``tests/unit/cli/test_strategy_run_teardown_after.py::test_strat_run_options_do_not_drift_from_framework_run``
asserts the two commands' Click options stay identical as a CI backstop.

The framework command is *never* registered under a Click group; it is only
reached via ``ctx.invoke`` with every param passed explicitly, so its option
*defaults* are inert. The wrapper is the sole Click-parsed surface, which is why
``--interval``'s shared default is the ``None`` sentinel the wrapper needs to
distinguish "user passed nothing" (resolve from ``[tool.almanak.run].interval``
or ``DEFAULT_STRAT_RUN_INTERVAL``) from an explicit value. The framework body
coerces a leftover ``None`` back to ``DEFAULT_STRAT_RUN_INTERVAL`` defensively.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import click

from almanak.config.cli_options import gateway_client_options

# Loop-interval bounds. Single source of truth: both the wrapper's
# ``--interval`` / ``[tool.almanak.run].interval`` validation and any future
# consumer import these from here rather than redefining them.
DEFAULT_STRAT_RUN_INTERVAL = 60
MIN_STRAT_RUN_INTERVAL = 5
MAX_STRAT_RUN_INTERVAL = 3600


def strategy_run_options(func: Callable[..., Any]) -> Callable[..., Any]:
    """Apply the shared ``almanak strat run`` option stack to ``func``.

    Applied bottom-up (Click stacks decorators in reverse), so the declarations
    below read top-to-bottom in ``--help``. Used by both the framework ``run``
    command and the ``strat run`` wrapper so the two can never drift (VIB-5846).
    """
    func = click.option(
        "--working-dir",
        "-d",
        type=click.Path(exists=True),
        default=".",
        help="Working directory containing the strategy files. Defaults to the current directory.",
    )(func)
    func = click.option(
        "--config",
        "-c",
        "config_file",
        type=click.Path(exists=True),
        default=None,
        help="Path to strategy config JSON file.",
    )(func)
    func = click.option(
        "--once",
        is_flag=True,
        default=False,
        help="Run single iteration then exit.",
    )(func)
    func = click.option(
        "--interval",
        "-i",
        type=int,
        default=None,
        help="Loop interval in seconds. Defaults to [tool.almanak.run].interval or 60.",
    )(func)
    func = click.option(
        "--dry-run",
        is_flag=True,
        default=False,
        help="Execute decide() but don't submit transactions.",
    )(func)
    func = click.option(
        "--list",
        "list_all",
        is_flag=True,
        default=False,
        help="List all available strategies and exit.",
    )(func)
    func = click.option(
        "--verbose",
        "-v",
        is_flag=True,
        default=False,
        help="Enable verbose output (detailed strategy info).",
    )(func)
    func = click.option(
        "--debug",
        is_flag=True,
        default=False,
        help="Enable debug output (includes Web3/HTTP logs).",
    )(func)
    func = click.option(
        "--dashboard",
        is_flag=True,
        default=False,
        help="Launch live dashboard alongside strategy execution.",
    )(func)
    func = click.option(
        "--dashboard-port",
        type=int,
        default=8501,
        help="Port to run the dashboard on (default: 8501).",
    )(func)
    func = click.option(
        "--dashboard-mode",
        type=click.Choice(["hosted-parity", "command-center"], case_sensitive=False),
        default="hosted-parity",
        help=(
            "Dashboard layout. 'hosted-parity' (default) mirrors the hosted "
            "platform: one strategy, one gateway, no multi-strategy navigation. "
            "'command-center' opens the repo-wide browser. Standalone mode "
            "(--dashboard from a non-strategy folder) always uses Command Center."
        ),
    )(func)
    func = click.option(
        "--simulate-tx/--no-simulate-tx",
        "simulate_tx",
        default=None,
        help="Enable/disable transaction simulation via Tenderly/Alchemy before submission. "
        "Default: use SIMULATION_ENABLED env var",
    )(func)
    func = click.option(
        "--network",
        "-n",
        type=click.Choice(["mainnet", "anvil"], case_sensitive=False),
        default=None,
        help="Network environment: 'mainnet' for production RPC, 'anvil' for local fork testing "
        "(auto-starts Anvil on a free port). For paper trading with PnL tracking, use "
        "'almanak strat backtest paper'. Overrides config.json 'network' field.",
    )(func)
    func = gateway_client_options(func)
    func = click.option(
        "--fresh",
        is_flag=True,
        default=False,
        help="Start from a clean slate: clear persisted strategy state before running "
        "instead of resuming it (useful for fresh Anvil forks, or to recover from a "
        "desynced restart). Default is to resume existing state; the boot banner and "
        "log report whether this run RESUMED or started FRESH.",
    )(func)
    func = click.option(
        "--copy-mode",
        type=click.Choice(["live", "shadow", "replay"], case_sensitive=False),
        default=None,
        help="Copy-trading mode override for this run.",
    )(func)
    func = click.option(
        "--copy-shadow",
        is_flag=True,
        default=False,
        help="Enable copy-trading shadow mode (decisioning only, no submissions).",
    )(func)
    func = click.option(
        "--copy-replay-file",
        type=click.Path(exists=False),
        default=None,
        help="Replay file (JSON/JSONL CopySignal fixtures) for copy-trading replay mode.",
    )(func)
    func = click.option(
        "--copy-strict",
        is_flag=True,
        default=False,
        help="Enable strict copy-trading schema + fail-closed validation.",
    )(func)
    func = click.option(
        "--no-gateway",
        "no_gateway",
        is_flag=True,
        default=False,
        help="Do not auto-start a gateway; connect to an existing one.",
    )(func)
    func = click.option(
        "--anvil-port",
        "anvil_ports",
        multiple=True,
        help="Use existing Anvil instance: CHAIN=PORT (e.g., --anvil-port arbitrum=8545). Repeatable.",
    )(func)
    func = click.option(
        "--keep-anvil",
        is_flag=True,
        default=False,
        help="Keep managed Anvil fork(s) running after the runner exits (incl. after a "
        "graceful teardown), detached in their own session, for post-run/post-teardown "
        "inspection or a sealed audit. You must kill the fork PID(s) yourself afterwards.",
    )(func)
    func = click.option(
        "--wallet",
        type=click.Choice(["default", "isolated"], case_sensitive=False),
        default="default",
        help="Wallet mode for Anvil: 'isolated' derives a unique wallet per strategy for balance isolation.",
    )(func)
    func = click.option(
        "--log-file",
        type=click.Path(dir_okay=False),
        default=None,
        help="Write JSON logs to this file (in addition to console output). Useful for AI agent analysis.",
    )(func)
    func = click.option(
        "--reset-fork",
        "reset_fork",
        is_flag=True,
        default=False,
        help="Reset Anvil fork to latest mainnet block before each iteration (requires --network anvil). "
        "Gives live on-chain state for fork testing.",
    )(func)
    func = click.option(
        "--max-iterations",
        type=int,
        default=None,
        help="Maximum number of iterations to run before exiting cleanly. "
        "Without this flag, continuous mode runs indefinitely.",
    )(func)
    func = click.option(
        "--teardown-after",
        is_flag=True,
        default=False,
        help="After --once iteration, automatically teardown (close all positions). "
        "Useful for CI/testing to avoid accumulating stale positions on-chain.",
    )(func)
    return func
