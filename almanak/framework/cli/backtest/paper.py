"""Paper trading and dashboard CLI commands.

This module provides the `paper` subcommand group (start, stop, status, logs, resume)
and the `dashboard` subcommand for interactive backtest exploration.
"""

import asyncio
import json
import os
import signal
import sys
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import click

from almanak.config.cli_runtime import cli_runtime_config_from_env

from ...anvil.fork_manager import CHAIN_IDS
from ...backtesting import (
    PaperPortfolioTracker,
    PaperTrader,
    PaperTraderConfig,
    PaperTradingSummary,
    RollingForkManager,
)
from ...backtesting.paper.background import BackgroundPaperTrader
from ...strategies import get_strategy
from .group import backtest
from .helpers import (
    _create_backtest_strategy,
    delete_paper_session_state,
    is_process_running,
    load_paper_session_state,
    load_strategy_config,
    save_paper_session_state,
    update_paper_session_status,
)
from .paper_helpers import (
    abort_if_session_running,
    apply_preset,
    build_resume_config,
    compute_resume_max_ticks,
    load_funding_from_config,
    load_resume_state,
    merge_funding,
    parse_initial_tokens_arg,
    print_paper_config,
    render_all_sessions,
    render_single_session_status,
    reset_dead_or_abort,
    resolve_max_ticks_from_duration,
    resolve_resume_rpc_url,
    resolve_rpc_url,
    validate_strategy_registered,
)

# =============================================================================
# Paper Trading Subcommand Group
# =============================================================================


@backtest.group("paper")
def paper() -> None:
    """
    Paper trading - real-time simulation on Anvil forks with PnL tracking.

    Paper trading executes real transactions on a local Anvil fork,
    providing accurate simulation of DeFi interactions without risking
    real capital. Includes PnL tracking, equity curves, and trade journals.

    Subcommands:

        start  - Start a paper trading session (foreground or background)

        stop   - Stop a running paper trading session

        status - Check the status of a paper trading session

        logs   - View logs from a paper trading session

    Examples:

        # Start paper trading
        almanak strat backtest paper start -s momentum_v1 --chain arbitrum

        # Check status
        almanak strat backtest paper status -s momentum_v1

        # View logs
        almanak strat backtest paper logs -s momentum_v1

        # Follow logs in real-time
        almanak strat backtest paper logs -s momentum_v1 --follow

        # Stop paper trading
        almanak strat backtest paper stop -s momentum_v1

        # List all sessions
        almanak strat backtest paper status --all
    """
    pass


# crap-allowlist: Phase 5e (#2097) replaces direct os.environ.get reads with the typed
# cli_runtime_config_from_env() and chain_rpc_url_from_env() helpers — no new branches,
# no new behaviour. VIB-4062 codemod (legacy MarketSnapshot import → canonical) also
# touches this function. Pre-existing CC=55 in the paper-start CLI; click-command
# refactor is tracked separately.
@paper.command("start")
@click.option("--strategy", "-s", required=True, help="Name of the strategy to paper trade")
@click.option(
    "--chain",
    "-c",
    type=click.Choice(list(CHAIN_IDS.keys())),
    default="arbitrum",
    help="Target blockchain (default: arbitrum)",
)
@click.option("--initial-eth", type=float, default=10.0, help="Initial ETH balance for paper wallet (default: 10)")
@click.option(
    "--initial-tokens",
    type=str,
    default="",
    help="Initial token balances as 'TOKEN:AMOUNT,TOKEN:AMOUNT' (e.g., 'USDC:10000,WETH:5')",
)
@click.option("--tick-interval", type=int, default=60, help="Interval between trading ticks in seconds (default: 60)")
@click.option("--max-ticks", type=int, default=None, help="Maximum number of ticks to run (default: unlimited)")
@click.option(
    "--duration",
    type=str,
    default=None,
    help="Session duration as human-readable string (e.g., '5m', '1h', '30s'). Mutually exclusive with --max-ticks.",
)
@click.option("--rpc-url", type=str, default=None, help="Archive RPC URL to fork from (default: from environment)")
@click.option("--anvil-port", type=int, default=8546, help="Port to run Anvil on (default: 8546)")
@click.option("--no-reset-fork", is_flag=True, default=False, help="Don't reset fork to latest block each tick")
@click.option(
    "--output", "-o", type=click.Path(exists=False), default=None, help="Output file for session results (optional)"
)
@click.option("--foreground", "-f", is_flag=True, default=False, help="Run in foreground instead of background")
@click.option("--dry-run", is_flag=True, default=False, help="Show configuration without starting session")
@click.option(
    "--strict-bootstrap",
    is_flag=True,
    default=False,
    help="Abort if any token has zero balance after wallet bootstrap (VIB-2377)",
)
@click.option(
    "--preset",
    type=click.Choice(["execution-validation", "yield-validation"]),
    default=None,
    help=(
        "Paper trading preset. "
        "'execution-validation' (default): rolling fork reset for TX smoke testing. "
        "'yield-validation': persistent fork with time advancement for lending yield measurement."
    ),
)
def paper_start(
    strategy: str,
    chain: str,
    initial_eth: float,
    initial_tokens: str,
    tick_interval: int,
    max_ticks: int | None,
    duration: str | None,
    rpc_url: str | None,
    anvil_port: int,
    no_reset_fork: bool,
    output: str | None,
    foreground: bool,
    dry_run: bool,
    strict_bootstrap: bool,
    preset: str | None,
) -> None:
    """
    Start a paper trading session.

    Paper trading executes strategy decisions on a local Anvil fork,
    providing accurate simulation with real DeFi protocol interactions
    and full PnL tracking.

    The session runs in the background by default. Use --foreground to
    run interactively, or use 'paper status' to check progress.

    Prerequisites:
        - Foundry (Anvil) must be installed: curl -L https://foundry.paradigm.xyz | bash
        - Archive RPC access for the target chain

    Examples:

        # Basic paper trading on Arbitrum
        almanak strat backtest paper start -s momentum_v1 --chain arbitrum

        # With custom initial balances
        almanak strat backtest paper start -s lending_loop \\
            --chain arbitrum \\
            --initial-eth 20 \\
            --initial-tokens "USDC:50000,WETH:10"

        # Run for limited ticks
        almanak strat backtest paper start -s test_strategy --max-ticks 100

        # Run for a specific duration
        almanak strat backtest paper start -s test_strategy --duration 5m

        # Run in foreground (blocks terminal)
        almanak strat backtest paper start -s test_strategy --foreground
    """
    # Resolve --duration into max_ticks (mutually exclusive with --max-ticks)
    max_ticks = resolve_max_ticks_from_duration(duration, max_ticks, tick_interval)

    validate_strategy_registered(strategy)
    abort_if_session_running(strategy)
    rpc_url = resolve_rpc_url(rpc_url, chain)

    cli_tokens = parse_initial_tokens_arg(initial_tokens)

    # Load bootstrap / anvil_funding from strategy config.json (VIB-202, VIB-2375)
    config_eth, config_tokens, config_bootstrap, strategy_config = load_funding_from_config(strategy, chain)

    # Merge: config provides defaults, CLI flags override
    ctx = click.get_current_context()
    cli_eth_explicit = ctx.get_parameter_source("initial_eth") != click.core.ParameterSource.DEFAULT
    initial_eth, parsed_tokens = merge_funding(initial_eth, cli_eth_explicit, config_eth, config_tokens, cli_tokens)

    # Prepare output path
    output_path = Path(output) if output else None

    # Create config
    try:
        # Check typed config for relaxed price mode (useful for tokens
        # without price feeds, e.g. Pendle PT tokens) (VIB-2562).
        relaxed_prices = cli_runtime_config_from_env().allow_hardcoded_prices

        paper_config = PaperTraderConfig(
            chain=chain,
            rpc_url=rpc_url,
            strategy_id=strategy,
            initial_eth=Decimal(str(initial_eth)),
            initial_tokens=parsed_tokens,
            bootstrap=config_bootstrap,
            strict_bootstrap=strict_bootstrap,
            tick_interval_seconds=tick_interval,
            max_ticks=max_ticks,
            anvil_port=anvil_port,
            reset_fork_every_tick=not no_reset_fork,
            strict_price_mode=not relaxed_prices,
        )
    except ValueError as e:
        click.echo(f"Configuration error: {e}", err=True)
        raise click.Abort() from e

    apply_preset(paper_config, preset)
    print_paper_config(
        strategy=strategy,
        chain=chain,
        paper_config=paper_config,
        initial_eth=initial_eth,
        parsed_tokens=parsed_tokens,
        tick_interval=tick_interval,
        max_ticks=max_ticks,
        anvil_port=anvil_port,
        no_reset_fork=no_reset_fork,
        foreground=foreground,
        output_path=output_path,
        strategy_config=strategy_config,
    )

    # Handle dry run
    if dry_run:
        click.echo()
        click.echo("Dry run - paper trading not started.")
        return

    # Get strategy instance
    try:
        strategy_class = get_strategy(strategy)
    except ValueError:
        click.echo()
        click.echo("Warning: No strategies registered in factory.", err=True)
        click.echo("Running with mock strategy for demonstration.", err=True)
        click.echo()

        from ...market import MarketSnapshot

        class MockPaperStrategy:
            """Mock strategy for paper trading demonstration."""

            strategy_id: str = strategy

            def __init__(self) -> None:
                self._iteration = 0

            def decide(self, market: MarketSnapshot) -> dict[str, Any] | None:
                self._iteration += 1
                return None

        strategy_class = MockPaperStrategy

    # Create strategy instance
    strategy_config = load_strategy_config(strategy, chain)
    strategy_instance = _create_backtest_strategy(strategy_class, strategy_config, chain)

    # Ensure strategy has strategy_id
    if not hasattr(strategy_instance, "strategy_id"):
        strategy_instance.strategy_id = strategy

    if foreground:
        # Run in foreground (blocking)
        click.echo()
        click.echo("Starting paper trading in foreground...")
        click.echo("Press Ctrl+C to stop.")
        click.echo()

        try:
            summary = asyncio.run(
                _run_paper_trading_foreground(
                    strategy_instance=strategy_instance,
                    paper_config=paper_config,
                    output_path=output_path,
                )
            )

            # Display results
            click.echo()
            click.echo("=" * 60)
            click.echo("PAPER TRADING RESULTS")
            click.echo("=" * 60)
            click.echo(summary.summary())

            if output_path:
                output_data = summary.to_dict()
                output_data["_meta"] = {
                    "generated_at": datetime.now(UTC).isoformat(),
                    "generator": "almanak strat backtest paper",
                    "engine": "paper",
                }
                with open(output_path, "w") as f:
                    json.dump(output_data, f, indent=2, default=str)
                click.echo()
                click.echo(f"Results written to: {output_path}")

        except KeyboardInterrupt:
            click.echo()
            click.echo("Paper trading interrupted by user.")

    else:
        # Run in background using BackgroundPaperTrader
        click.echo()
        click.echo("Starting paper trading in background...")

        bg_trader = BackgroundPaperTrader(
            config=paper_config,
        )

        # Resolve strategy module/class via registry for correct paths
        strategy_cls = get_strategy(strategy)

        try:
            pid = bg_trader.start(
                strategy_module=strategy_cls.__module__,
                strategy_class=strategy_cls.__name__,
                strategy_config=strategy_config,
            )
        except RuntimeError as e:
            click.echo(f"Error: {e}", err=True)
            raise click.Abort() from e

        # Also save to the CLI session state for backwards compatibility
        start_time = datetime.now(UTC)
        save_paper_session_state(
            strategy_id=strategy,
            pid=pid,
            config=paper_config,
            start_time=start_time,
        )

        click.echo()
        click.echo(f"Paper trading session started for '{strategy}' (PID: {pid})")
        click.echo(f"State directory: {bg_trader.state_dir}")
        click.echo(f"Log file: {bg_trader.log_file}")
        click.echo()
        click.echo("To check status:")
        click.echo(f"  almanak strat backtest paper status -s {strategy}")
        click.echo()
        click.echo("To view logs:")
        click.echo(f"  almanak strat backtest paper logs -s {strategy}")
        click.echo()
        click.echo("To stop:")
        click.echo(f"  almanak strat backtest paper stop -s {strategy}")


async def _run_paper_trading_foreground(
    strategy_instance: Any,
    paper_config: PaperTraderConfig,
    output_path: Path | None,
) -> PaperTradingSummary:
    """Run paper trading in foreground mode."""
    fork_manager = RollingForkManager(
        rpc_url=paper_config.rpc_url,
        chain=paper_config.chain,
        anvil_port=paper_config.anvil_port,
        startup_timeout_seconds=paper_config.startup_timeout_seconds,
        auto_impersonate=paper_config.auto_impersonate,
        block_time=paper_config.block_time,
    )

    portfolio_tracker = PaperPortfolioTracker(
        strategy_id=paper_config.strategy_id,
        chain=paper_config.chain,
    )

    trader = PaperTrader(
        fork_manager=fork_manager,
        portfolio_tracker=portfolio_tracker,
        config=paper_config,
    )

    summary = await trader.run_loop(
        strategy=strategy_instance,
        max_ticks=paper_config.max_ticks,
    )

    return summary


@paper.command("stop")
@click.option("--strategy", "-s", required=True, help="Name of the strategy to stop")
@click.option("--force", is_flag=True, default=False, help="Force stop (kill process)")
def paper_stop(strategy: str, force: bool) -> None:
    """
    Stop a running paper trading session.

    This command signals the paper trading process to stop gracefully.
    The current tick will complete before stopping.

    Use --force to immediately terminate the process.

    Examples:

        # Graceful stop
        almanak strat backtest paper stop -s momentum_v1

        # Force stop
        almanak strat backtest paper stop -s momentum_v1 --force
    """
    # Try BackgroundPaperTrader first (engine state dir)
    bg_trader = BackgroundPaperTrader(
        config=PaperTraderConfig(
            chain="arbitrum",
            rpc_url="http://localhost:8545",
            strategy_id=strategy,
        ),
    )

    bg_status = bg_trader.get_status()

    if bg_status.is_running:
        click.echo(f"Session: {strategy}")
        click.echo(f"Status: running (PID: {bg_status.pid})")

        if force:
            if bg_status.pid:
                try:
                    os.kill(bg_status.pid, signal.SIGKILL)
                    click.echo(f"Process {bg_status.pid} killed forcefully.")
                except (ProcessLookupError, PermissionError) as e:
                    click.echo(f"Error: {e}", err=True)
        else:
            stopped = bg_trader.stop()
            if stopped:
                click.echo("Session stopped gracefully.")
            else:
                click.echo("Failed to stop session. Try --force.", err=True)

        update_paper_session_status(strategy, "stopped")
        return

    # Fallback: check CLI session state
    state = load_paper_session_state(strategy)
    if not state:
        click.echo(f"No paper trading session found for '{strategy}'", err=True)
        return

    pid = state.get("pid")
    status = state.get("status", "unknown")

    click.echo(f"Session: {strategy}")
    click.echo(f"Status: {status}")
    click.echo(f"PID: {pid}")

    if status != "running":
        click.echo()
        click.echo(f"Session is not running (status: {status})")
        click.echo("Cleaning up session state...")
        delete_paper_session_state(strategy)
        click.echo("Done.")
        return

    if pid:
        if not is_process_running(pid):
            click.echo()
            click.echo("Process is no longer running.")
            update_paper_session_status(strategy, "stopped")
            click.echo("Session state updated.")
            return

        try:
            if force:
                os.kill(pid, signal.SIGKILL)
                click.echo()
                click.echo(f"Process {pid} killed forcefully.")
            else:
                os.kill(pid, signal.SIGTERM)
                click.echo()
                click.echo(f"Stop signal sent to process {pid}.")
                click.echo("Waiting for graceful shutdown...")

            update_paper_session_status(strategy, "stopped")
            click.echo("Session stopped.")

        except ProcessLookupError:
            click.echo()
            click.echo("Process no longer exists.")
            update_paper_session_status(strategy, "stopped")

        except PermissionError:
            click.echo()
            click.echo(f"Permission denied to stop process {pid}", err=True)
            click.echo("Try running with appropriate permissions.", err=True)

    else:
        click.echo()
        click.echo("No PID recorded for this session.")
        update_paper_session_status(strategy, "stopped")


@paper.command("resume")
@click.option("--strategy", "-s", required=True, help="Name of the strategy to resume")
@click.option(
    "--duration",
    type=str,
    default=None,
    help="Additional duration to run (e.g., '1h', '48h'). Extends max_ticks from current count.",
)
@click.option(
    "--max-ticks",
    type=int,
    default=None,
    help="New max tick count (absolute, not additional). Mutually exclusive with --duration.",
)
def paper_resume(strategy: str, duration: str | None, max_ticks: int | None) -> None:
    """
    Resume a stopped paper trading session.

    Continues a previously stopped session from where it left off,
    preserving tick count, trades, errors, and balances.

    Handles dead process detection, state status reset, PID cleanup,
    and max_ticks extension automatically.

    Examples:

        # Resume for another 48 hours
        almanak strat backtest paper resume -s momentum_v1 --duration 48h

        # Resume with a specific tick limit
        almanak strat backtest paper resume -s momentum_v1 --max-ticks 5000

        # Resume indefinitely (until manual stop)
        almanak strat backtest paper resume -s momentum_v1
    """
    if duration is not None and max_ticks is not None:
        click.echo("Error: --duration and --max-ticks are mutually exclusive.", err=True)
        raise click.Abort()

    bg_trader, state = load_resume_state(strategy)
    reset_dead_or_abort(state, bg_trader, strategy)

    if not state.can_resume():
        click.echo(f"Error: Cannot resume session (status={state.status}).", err=True)
        raise click.Abort()

    saved_config = state.config
    if not saved_config:
        click.echo("Error: Saved state has no config. Cannot resume.", err=True)
        raise click.Abort()

    chain = saved_config.get("chain", "arbitrum")
    tick_interval = saved_config.get("tick_interval_seconds", 60) or 60
    rpc_url = resolve_resume_rpc_url(saved_config.get("rpc_url"), chain)
    new_max_ticks = compute_resume_max_ticks(
        duration,
        max_ticks,
        saved_config.get("max_ticks"),
        state.tick_count,
        tick_interval,
    )

    resume_config = build_resume_config(
        saved_config=saved_config,
        strategy=strategy,
        chain=chain,
        rpc_url=rpc_url,
        new_max_ticks=new_max_ticks,
        tick_interval=tick_interval,
    )

    bg_trader_resume = BackgroundPaperTrader(config=resume_config)

    # Resolve strategy module/class
    strategy_cls = get_strategy(strategy)
    strategy_config = load_strategy_config(strategy, chain)

    click.echo()
    click.echo(f"  Resuming: {strategy}")
    click.echo(f"  Chain: {chain}")
    click.echo(f"  Current ticks: {state.tick_count}")
    click.echo(f"  Trades so far: {len(state.trades)}")
    if new_max_ticks:
        remaining = new_max_ticks - state.tick_count
        click.echo(f"  Remaining ticks: {remaining}")
    else:
        click.echo("  Duration: unlimited")
    click.echo()

    try:
        pid = bg_trader_resume.resume(
            strategy_module=strategy_cls.__module__,
            strategy_class=strategy_cls.__name__,
            strategy_config=strategy_config,
        )
    except (RuntimeError, FileNotFoundError) as e:
        click.echo(f"Error: {e}", err=True)
        raise click.Abort() from e

    # Update CLI session state
    update_paper_session_status(strategy, "running")
    save_paper_session_state(
        strategy_id=strategy,
        pid=pid,
        config=resume_config,
        start_time=state.session_start,
    )

    click.echo(f"  Resumed in background (PID: {pid})")
    click.echo()
    click.echo(f"  Status: almanak strat backtest paper status -s {strategy}")
    click.echo(f"  Logs:   almanak strat backtest paper logs -s {strategy}")
    click.echo(f"  Stop:   almanak strat backtest paper stop -s {strategy}")


@paper.command("status")
@click.option("--strategy", "-s", required=False, default=None, help="Name of the strategy to check")
@click.option("--all", "-a", "show_all", is_flag=True, default=False, help="Show all paper trading sessions")
@click.option("--verbose", "-v", is_flag=True, default=False, help="Show detailed session information")
def paper_status(strategy: str | None, show_all: bool, verbose: bool) -> None:
    """
    Check the status of paper trading sessions.

    Shows information about running and completed paper trading sessions.
    Reads from both the engine state directory (~/.almanak/paper/) and
    the CLI session directory for comprehensive status.

    Examples:

        # Check specific strategy
        almanak strat backtest paper status -s momentum_v1

        # List all sessions
        almanak strat backtest paper status --all

        # Detailed view
        almanak strat backtest paper status -s momentum_v1 --verbose
    """
    if show_all:
        render_all_sessions(verbose)
        return

    if not strategy:
        click.echo("Error: Please specify --strategy or use --all to list all sessions", err=True)
        raise click.Abort()

    render_single_session_status(strategy)


def get_paper_log_file(strategy_id: str) -> Path:
    """Get the log file path for a paper trading session."""
    from .helpers import PAPER_STATE_DIR

    background_log = Path.home() / ".almanak" / "paper" / f"{strategy_id}.log"
    if background_log.exists():
        return background_log

    session_log = PAPER_STATE_DIR / f"{strategy_id}.log"
    return session_log


@paper.command("logs")
@click.option("--strategy", "-s", required=True, help="Name of the strategy to view logs for")
@click.option("--lines", "-n", type=int, default=100, help="Number of lines to display (default: 100)")
@click.option("--follow", "-f", is_flag=True, default=False, help="Follow log output in real-time (like tail -f)")
@click.option("--all", "-a", "show_all", is_flag=True, default=False, help="Show all log entries (ignores --lines)")
def paper_logs(strategy: str, lines: int, follow: bool, show_all: bool) -> None:
    """
    View logs from a paper trading session.

    Displays recent log entries from the paper trading background process.
    Use --follow to watch logs in real-time (similar to tail -f).

    Examples:

        # View last 100 lines
        almanak strat backtest paper logs -s momentum_v1

        # View last 500 lines
        almanak strat backtest paper logs -s momentum_v1 -n 500

        # View all logs
        almanak strat backtest paper logs -s momentum_v1 --all

        # Follow logs in real-time
        almanak strat backtest paper logs -s momentum_v1 --follow
    """
    log_file = get_paper_log_file(strategy)

    if not log_file.exists():
        click.echo(f"No log file found for strategy '{strategy}'", err=True)
        click.echo()
        click.echo("Possible reasons:")
        click.echo("  - No paper trading session has been run for this strategy")
        click.echo("  - The session was run in foreground mode (no log file created)")
        click.echo("  - The log file was deleted")
        click.echo()
        click.echo(f"Expected log file location: {log_file}")
        return

    click.echo(f"Log file: {log_file}")
    click.echo("=" * 60)

    if follow:
        click.echo("Following logs... (press Ctrl+C to stop)")
        click.echo()

        try:
            import time as time_module

            with open(log_file) as f:
                f.seek(0, 2)

                while True:
                    line = f.readline()
                    if line:
                        click.echo(line, nl=False)
                    else:
                        time_module.sleep(0.5)
        except KeyboardInterrupt:
            click.echo()
            click.echo("Stopped following logs.")
        except OSError as e:
            click.echo(f"Error reading log file: {e}", err=True)

    else:
        try:
            with open(log_file) as f:
                all_lines = f.readlines()

            if show_all:
                lines_to_show = all_lines
            else:
                lines_to_show = all_lines[-lines:]

            if not lines_to_show:
                click.echo("(Log file is empty)")
                return

            for line in lines_to_show:
                click.echo(line, nl=False)

            click.echo()
            click.echo("=" * 60)
            click.echo(f"Showing {len(lines_to_show)} of {len(all_lines)} total lines")

            if len(all_lines) > len(lines_to_show):
                click.echo(f"Use --all to see all {len(all_lines)} lines")
                click.echo("Use --follow to watch logs in real-time")

        except OSError as e:
            click.echo(f"Error reading log file: {e}", err=True)


# =============================================================================
# Dashboard Subcommand
# =============================================================================


@backtest.command("dashboard")
@click.option("--port", "-p", type=int, default=8501, help="Port to run the Streamlit dashboard (default: 8501)")
@click.option("--no-browser", is_flag=True, default=False, help="Don't automatically open the browser")
@click.option("--host", type=str, default="localhost", help="Host to bind the server (default: localhost)")
def dashboard_cmd(port: int, no_browser: bool, host: str) -> None:
    """
    Launch the interactive backtest dashboard.

    Opens a Streamlit web application for exploring and comparing
    backtest results. Upload JSON files or provide them via command line.

    Examples:

        # Launch dashboard with default settings
        almanak backtest dashboard

        # Launch on custom port
        almanak backtest dashboard --port 8080

        # Launch without auto-opening browser
        almanak backtest dashboard --no-browser

        # Allow external connections
        almanak backtest dashboard --host 0.0.0.0
    """
    import subprocess
    import webbrowser
    from pathlib import Path as PathLib

    cmd = [
        sys.executable,
        "-m",
        "streamlit",
        "run",
        "--server.port",
        str(port),
        "--server.address",
        host,
    ]

    if no_browser:
        cmd.extend(["--server.headless", "true"])

    try:
        from almanak.framework.backtesting.dashboard import app as dashboard_app

        dashboard_path = PathLib(dashboard_app.__file__)
        cmd.append(str(dashboard_path))
    except ImportError as e:
        click.echo("Error: Could not find dashboard module.", err=True)
        click.echo("Make sure Streamlit is installed: pip install streamlit", err=True)
        raise click.Abort() from e

    click.echo("=" * 60)
    click.echo("BACKTEST DASHBOARD")
    click.echo("=" * 60)
    click.echo("Starting Streamlit dashboard...")
    click.echo(f"Host: {host}")
    click.echo(f"Port: {port}")

    url = f"http://{host}:{port}"
    click.echo(f"URL: {url}")
    click.echo()
    click.echo("Press Ctrl+C to stop the server")
    click.echo("=" * 60)

    if not no_browser:

        def open_browser() -> None:
            import time

            time.sleep(1.5)
            webbrowser.open(url)

        import threading

        threading.Thread(target=open_browser, daemon=True).start()

    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        click.echo(f"Error: Streamlit failed to start (exit code {e.returncode})", err=True)
        raise click.Abort() from e
    except FileNotFoundError as e:
        click.echo("Error: Streamlit not found. Install it with: pip install streamlit", err=True)
        raise click.Abort() from e
    except KeyboardInterrupt:
        click.echo("\nDashboard stopped.")
