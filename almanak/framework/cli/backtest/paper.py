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

from ...anvil.fork_manager import CHAIN_IDS
from ...backtesting import (
    PaperPortfolioTracker,
    PaperTrader,
    PaperTraderConfig,
    PaperTradingSummary,
    RollingForkManager,
)
from ...backtesting.paper.background import BackgroundPaperTrader, PaperTraderState, PIDFile
from ...strategies import get_strategy
from .group import backtest
from .helpers import (
    _create_backtest_strategy,
    _parse_duration,
    delete_paper_session_state,
    is_process_running,
    list_paper_sessions,
    list_strategies_fn,
    load_paper_session_state,
    load_strategy_config,
    save_paper_session_state,
    update_paper_session_status,
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
    if duration is not None and max_ticks is not None:
        click.echo("Error: --duration and --max-ticks are mutually exclusive.", err=True)
        raise click.Abort()

    if duration is not None:
        if tick_interval <= 0:
            click.echo("Error: --tick-interval must be greater than 0.", err=True)
            raise click.Abort()
        duration_seconds = _parse_duration(duration)
        if duration_seconds is None:
            click.echo(
                f"Error: Invalid duration '{duration}'. Use format like '30s', '5m', '1h', '2h30m'.",
                err=True,
            )
            raise click.Abort()
        # +1 because the first tick executes immediately; N ticks = (N-1) sleep intervals
        max_ticks = max(1, duration_seconds // tick_interval + 1)
        click.echo(f"Duration {duration} -> {max_ticks} ticks at {tick_interval}s interval")
    # Validate strategy exists
    available_strategies = list_strategies_fn()
    if strategy not in available_strategies and available_strategies:
        click.echo(f"Error: Unknown strategy '{strategy}'", err=True)
        click.echo(f"Available strategies: {', '.join(sorted(available_strategies))}", err=True)
        raise click.Abort()

    # Check if session already running (via BackgroundPaperTrader)
    existing_state = load_paper_session_state(strategy)
    if existing_state and existing_state.get("status") == "running":
        pid = existing_state.get("pid")
        if pid and is_process_running(pid):
            click.echo(f"Error: Paper trading session for '{strategy}' is already running (PID: {pid})", err=True)
            click.echo(f"Use 'almanak strat backtest paper stop -s {strategy}' to stop it first.", err=True)
            raise click.Abort()

    # Determine RPC URL
    if rpc_url is None:
        chain_upper = chain.upper()
        env_var_names = [
            f"ALMANAK_{chain_upper}_RPC_URL",
            f"{chain_upper}_RPC_URL",
            "ALMANAK_RPC_URL",
            "RPC_URL",
        ]
        for env_var in env_var_names:
            rpc_url = os.environ.get(env_var)
            if rpc_url:
                break

        if not rpc_url:
            click.echo(f"Error: No RPC URL provided for chain '{chain}'", err=True)
            click.echo(f"Set one of: {', '.join(env_var_names[:2])}", err=True)
            click.echo("Or use --rpc-url option", err=True)
            raise click.Abort()

    # Parse initial tokens from CLI flags
    cli_tokens: dict[str, Decimal] = {}
    if initial_tokens:
        try:
            for pair in initial_tokens.split(","):
                pair = pair.strip()
                if not pair:
                    continue
                if ":" not in pair:
                    raise ValueError(f"Invalid token entry '{pair}'")
                token, amount = pair.split(":", 1)
                token_key = token.strip()
                amount_str = amount.strip()
                if not token_key or not amount_str:
                    raise ValueError(f"Invalid token entry '{pair}'")
                cli_tokens[token_key] = Decimal(amount_str)
        except Exception as e:
            click.echo(f"Error parsing initial-tokens: {e}", err=True)
            click.echo("Expected format: 'TOKEN:AMOUNT,TOKEN:AMOUNT'", err=True)
            raise click.Abort() from e

    # Load bootstrap / anvil_funding from strategy config.json (VIB-202, VIB-2375)
    config_eth: Decimal | None = None
    config_tokens: dict[str, Decimal] = {}
    config_bootstrap: dict[str, dict[str, Decimal]] = {}
    strategy_config: dict[str, Any] | None = None

    def _parse_funding_dict(
        funding: dict,
        native_symbols: frozenset[str],
        source: str,
    ) -> tuple[Decimal | None, dict[str, Decimal]]:
        """Parse a flat token->amount dict into (native_eth, erc20_tokens)."""
        eth_val: Decimal | None = None
        tokens: dict[str, Decimal] = {}
        for token_name, amount in funding.items():
            token_str = str(token_name)
            if token_str.upper() in native_symbols:
                eth_val = Decimal(str(amount))
            elif token_str.startswith(("0x", "0X")) and len(token_str) == 42:
                from eth_utils import to_checksum_address

                try:
                    tokens[to_checksum_address(token_str)] = Decimal(str(amount))
                except (ValueError, TypeError) as e:
                    click.echo(
                        f"Warning: ignoring invalid token address in {source}: {token_str} ({e})",
                        err=True,
                    )
                    continue
            else:
                tokens[token_str] = Decimal(str(amount))
        return eth_val, tokens

    try:
        strategy_config = load_strategy_config(strategy, chain)
        from almanak.gateway.managed import ManagedGateway

        native_symbols = ManagedGateway.NATIVE_TOKEN_SYMBOLS

        paper_trading_block = strategy_config.get("paper_trading", {})
        bootstrap_raw = paper_trading_block.get("bootstrap", {}) if isinstance(paper_trading_block, dict) else {}

        if bootstrap_raw and isinstance(bootstrap_raw, dict):
            click.echo(f"Found paper_trading.bootstrap in config: {bootstrap_raw}", err=True)
            for chain_key, chain_tokens_raw in bootstrap_raw.items():
                if isinstance(chain_tokens_raw, dict):
                    chain_eth, chain_toks = _parse_funding_dict(
                        chain_tokens_raw,
                        native_symbols,
                        f"paper_trading.bootstrap[{chain_key}]",
                    )
                    bootstrap_entry: dict[str, Decimal] = {}
                    if chain_eth is not None:
                        bootstrap_entry["ETH"] = chain_eth
                    bootstrap_entry.update(chain_toks)
                    config_bootstrap[chain_key.lower()] = bootstrap_entry
            current_chain_bootstrap = dict(config_bootstrap.get(chain.lower(), {}))
            if current_chain_bootstrap:
                config_eth = current_chain_bootstrap.pop("ETH", None)
                config_tokens.update(current_chain_bootstrap)

        if not config_bootstrap:
            anvil_funding = strategy_config.get("anvil_funding", {})
            if anvil_funding:
                if not isinstance(anvil_funding, dict):
                    raise ValueError(
                        f"anvil_funding must be an object mapping TOKEN->AMOUNT, got {type(anvil_funding).__name__}"
                    )
                click.echo(f"Found anvil_funding in config: {anvil_funding}", err=True)
                config_eth, config_tokens = _parse_funding_dict(anvil_funding, native_symbols, "anvil_funding")
    except Exception as e:
        click.echo(
            f"Warning: ignoring invalid bootstrap/anvil_funding in strategy config: {e}",
            err=True,
        )

    # Merge: config provides defaults, CLI flags override
    ctx = click.get_current_context()
    cli_eth_explicit = ctx.get_parameter_source("initial_eth") != click.core.ParameterSource.DEFAULT
    if config_eth is not None and not cli_eth_explicit:
        initial_eth = float(config_eth)
    parsed_tokens: dict[str, Decimal] = {**config_tokens, **cli_tokens}

    # Prepare output path
    output_path = Path(output) if output else None

    # Create config
    try:
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
        )
    except ValueError as e:
        click.echo(f"Configuration error: {e}", err=True)
        raise click.Abort() from e

    # Display configuration
    click.echo("=" * 60)
    click.echo("PAPER TRADING CONFIGURATION")
    click.echo("=" * 60)
    click.echo(f"Strategy: {strategy}")
    click.echo(f"Chain: {chain} (ID: {paper_config.chain_id})")
    click.echo(f"Initial ETH: {initial_eth}")
    if parsed_tokens:
        tokens_str = ", ".join(f"{k}: {v}" for k, v in parsed_tokens.items())
        click.echo(f"Initial Tokens: {tokens_str}")
    click.echo(f"Tick Interval: {tick_interval}s ({tick_interval / 60:.1f} min)")
    if max_ticks:
        click.echo(f"Max Ticks: {max_ticks}")
        if paper_config.max_duration_minutes:
            click.echo(f"Max Duration: ~{paper_config.max_duration_minutes:.1f} min")
    else:
        click.echo("Max Ticks: unlimited")
    click.echo(f"Anvil Port: {anvil_port}")
    click.echo(f"Reset Fork Each Tick: {not no_reset_fork}")
    click.echo(f"Mode: {'Foreground' if foreground else 'Background'}")

    if output_path:
        click.echo(f"Output: {output_path}")

    # Warn if force_action is set
    force_action = strategy_config.get("force_action") if strategy_config else None
    if force_action:
        click.echo()
        click.echo(
            f"  WARNING: force_action='{force_action}' detected in config. "
            f"Strategy will bypass indicator logic for every tick.",
            err=True,
        )

    click.echo("=" * 60)

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

        from ...data.market_snapshot import MarketSnapshot

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

    # Load saved state to get config
    bg_trader = BackgroundPaperTrader(
        config=PaperTraderConfig(
            chain="arbitrum",
            rpc_url="http://placeholder",
            strategy_id=strategy,
        ),
    )

    if not bg_trader.state_file.exists():
        click.echo(f"Error: No saved state found for '{strategy}'.", err=True)
        click.echo("Use 'paper start' to begin a new session.", err=True)
        raise click.Abort()

    state = PaperTraderState.load(bg_trader.state_file)

    # Handle dead process
    if state.status == "running":
        if state.pid and not is_process_running(state.pid):
            click.echo(f"Process {state.pid} is no longer running. Resetting status to stopped.")
            state.status = "stopped"
            state.save(bg_trader.state_file)
            pid_file = PIDFile(path=bg_trader.pid_file_path, strategy_id=strategy)
            pid_file.release()
        else:
            click.echo(f"Error: Session '{strategy}' is still running (PID: {state.pid}).", err=True)
            click.echo(f"Use 'paper stop -s {strategy}' first.", err=True)
            raise click.Abort()

    if not state.can_resume():
        click.echo(f"Error: Cannot resume session (status={state.status}).", err=True)
        raise click.Abort()

    # Reconstruct config from saved state
    saved_config = state.config
    if not saved_config:
        click.echo("Error: Saved state has no config. Cannot resume.", err=True)
        raise click.Abort()

    chain = saved_config.get("chain", "arbitrum")
    tick_interval = saved_config.get("tick_interval_seconds", 60) or 60

    # Determine RPC URL
    rpc_url = saved_config.get("rpc_url")
    if not rpc_url or "***" in str(rpc_url):
        chain_upper = chain.upper()
        for env_var in [f"ALMANAK_{chain_upper}_RPC_URL", f"{chain_upper}_RPC_URL", "ALMANAK_RPC_URL", "RPC_URL"]:
            rpc_url = os.environ.get(env_var)
            if rpc_url:
                break
        if not rpc_url:
            click.echo(f"Error: Saved RPC URL is masked and no env var found for '{chain}'.", err=True)
            click.echo(f"Set ALMANAK_{chain_upper}_RPC_URL or use 'paper start' instead.", err=True)
            raise click.Abort()

    # Handle --duration -> extend max_ticks
    new_max_ticks = saved_config.get("max_ticks")
    if duration is not None:
        duration_seconds = _parse_duration(duration)
        if duration_seconds is None:
            click.echo(f"Error: Invalid duration '{duration}'. Use format like '1h', '48h'.", err=True)
            raise click.Abort()
        additional_ticks = max(1, duration_seconds // tick_interval + 1)
        new_max_ticks = state.tick_count + additional_ticks
        click.echo(f"Duration {duration} -> {additional_ticks} additional ticks (total: {new_max_ticks})")
    elif max_ticks is not None:
        if max_ticks <= state.tick_count:
            click.echo(
                f"Error: --max-ticks ({max_ticks}) must be greater than current tick count ({state.tick_count}).",
                err=True,
            )
            raise click.Abort()
        new_max_ticks = max_ticks

    # Build the resume config
    resume_config = PaperTraderConfig(
        chain=chain,
        rpc_url=rpc_url,
        strategy_id=strategy,
        tick_interval_seconds=tick_interval,
        max_ticks=new_max_ticks,
        anvil_port=saved_config.get("anvil_port", 8546),
        reset_fork_every_tick=saved_config.get("reset_fork_every_tick", True),
        initial_eth=Decimal(str(saved_config.get("initial_eth", "10"))),
        initial_tokens={k: Decimal(str(v)) for k, v in saved_config.get("initial_tokens", {}).items()},
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
        sessions = list_paper_sessions()
        if not sessions:
            click.echo("No paper trading sessions found.")
            return

        click.echo("=" * 60)
        click.echo("PAPER TRADING SESSIONS")
        click.echo("=" * 60)

        for session in sessions:
            strategy_id = session.get("strategy_id", "unknown")
            status = session.get("status", "unknown")
            pid = session.get("pid", "N/A")
            start_time = session.get("start_time", "N/A")

            if pid and isinstance(pid, int):
                if is_process_running(pid):
                    status_display = f"running (PID: {pid})"
                else:
                    status_display = "stopped (process not found)"
            else:
                status_display = status

            click.echo(f"\nStrategy: {strategy_id}")
            click.echo(f"  Status: {status_display}")
            click.echo(f"  Started: {start_time}")

            if verbose:
                config = session.get("config", {})
                click.echo(f"  Chain: {config.get('chain', 'N/A')}")
                click.echo(f"  Tick Interval: {config.get('tick_interval_seconds', 'N/A')}s")
                click.echo(f"  Max Ticks: {config.get('max_ticks', 'unlimited')}")

                summary = session.get("summary")
                if summary:
                    click.echo(f"  Trades: {summary.get('successful_trades', 0)}")
                    click.echo(f"  Errors: {summary.get('failed_trades', 0)}")

        click.echo()
        return

    if not strategy:
        click.echo("Error: Please specify --strategy or use --all to list all sessions", err=True)
        raise click.Abort()

    # Check specific strategy
    bg_trader = BackgroundPaperTrader(
        config=PaperTraderConfig(
            chain="arbitrum",
            rpc_url="http://localhost:8545",
            strategy_id=strategy,
        ),
    )
    bg_status = bg_trader.get_status()

    if bg_status.is_running or bg_status.tick_count > 0:
        click.echo("=" * 60)
        click.echo(f"PAPER TRADING STATUS: {strategy}")
        click.echo("=" * 60)
        click.echo(f"Status: {'running' if bg_status.is_running else bg_status.status} (PID: {bg_status.pid or 'N/A'})")
        if bg_status.session_start:
            click.echo(f"Started: {bg_status.session_start.strftime('%Y-%m-%d %H:%M:%S')}")
        click.echo(f"Ticks: {bg_status.tick_count}")
        click.echo(f"Trades: {bg_status.trade_count}")
        click.echo(f"Errors: {bg_status.error_count}")
        if bg_status.last_save:
            click.echo(f"Last Save: {bg_status.last_save.strftime('%Y-%m-%d %H:%M:%S')}")
        if bg_status.can_resume:
            click.echo(f"Can Resume: yes (resume_count: {bg_status.resume_count})")
        return

    # Fallback: check CLI session state
    state = load_paper_session_state(strategy)
    if not state:
        click.echo(f"No paper trading session found for '{strategy}'")
        click.echo()
        click.echo("To start a session:")
        click.echo(f"  almanak strat backtest paper start -s {strategy}")
        return

    click.echo("=" * 60)
    click.echo(f"PAPER TRADING STATUS: {strategy}")
    click.echo("=" * 60)

    status = state.get("status", "unknown")
    pid = state.get("pid")
    start_time = state.get("start_time", "N/A")
    last_updated = state.get("last_updated")

    if pid and isinstance(pid, int):
        if is_process_running(pid):
            actual_status = f"running (PID: {pid})"
        else:
            actual_status = "stopped (process not found)"
    else:
        actual_status = status

    click.echo(f"Status: {actual_status}")
    click.echo(f"Started: {start_time}")

    if last_updated:
        click.echo(f"Last Updated: {last_updated}")

    config = state.get("config", {})
    click.echo()
    click.echo("Configuration:")
    click.echo(f"  Chain: {config.get('chain', 'N/A')}")
    click.echo(f"  Tick Interval: {config.get('tick_interval_seconds', 'N/A')}s")
    click.echo(f"  Max Ticks: {config.get('max_ticks', 'unlimited')}")
    click.echo(f"  Initial ETH: {config.get('initial_eth', 'N/A')}")

    initial_tokens = config.get("initial_tokens", {})
    if initial_tokens:
        tokens_str = ", ".join(f"{k}: {v}" for k, v in initial_tokens.items())
        click.echo(f"  Initial Tokens: {tokens_str}")

    summary = state.get("summary")
    if summary:
        click.echo()
        click.echo("Results:")
        click.echo(f"  Duration: {summary.get('duration', 'N/A')}")
        click.echo(f"  Total Trades: {summary.get('total_trades', 0)}")
        click.echo(f"  Successful: {summary.get('successful_trades', 0)}")
        click.echo(f"  Failed: {summary.get('failed_trades', 0)}")
        if summary.get("pnl_usd"):
            click.echo(f"  PnL: ${float(summary['pnl_usd']):,.2f}")


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
