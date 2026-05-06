"""Helpers for the `paper` CLI subcommands (start / resume / status).

Each function is a thin, deterministic unit extracted from the original
Click command bodies in `paper.py`. Their role is to keep the Click
commands themselves close to "parse options -> call helpers -> print",
mirroring the cli/run.py Phase 4 refactor pattern.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

import click

from ...backtesting import PaperTraderConfig
from ...backtesting.paper.background import (
    BackgroundPaperTrader,
    PaperTraderState,
    PIDFile,
)
from .helpers import (
    _parse_duration,
    is_process_running,
    list_paper_sessions,
    list_strategies_fn,
    load_paper_session_state,
    load_strategy_config,
)

if TYPE_CHECKING:
    from ...backtesting.paper.background import BackgroundStatus


# =============================================================================
# paper_start helpers
# =============================================================================


def resolve_max_ticks_from_duration(
    duration: str | None,
    max_ticks: int | None,
    tick_interval: int,
) -> int | None:
    """Resolve `--duration` into a max-ticks count; enforces mutual exclusion.

    Returns the original `max_ticks` if `duration` is None.
    """
    if duration is not None and max_ticks is not None:
        click.echo("Error: --duration and --max-ticks are mutually exclusive.", err=True)
        raise click.Abort()

    if duration is None:
        return max_ticks

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
    resolved = max(1, duration_seconds // tick_interval + 1)
    click.echo(f"Duration {duration} -> {resolved} ticks at {tick_interval}s interval")
    return resolved


def validate_strategy_registered(strategy: str) -> None:
    """Abort with a friendly message if the strategy name is unknown."""
    available = list_strategies_fn()
    # If the registry is empty, fall through to the mock-strategy fallback.
    if strategy not in available and available:
        click.echo(f"Error: Unknown strategy '{strategy}'", err=True)
        click.echo(f"Available strategies: {', '.join(sorted(available))}", err=True)
        raise click.Abort()


def abort_if_session_running(strategy: str) -> None:
    """Abort if a paper-trading session for `strategy` is already running."""
    existing = load_paper_session_state(strategy)
    if not existing or existing.get("status") != "running":
        return
    pid = existing.get("pid")
    if pid and is_process_running(pid):
        click.echo(f"Error: Paper trading session for '{strategy}' is already running (PID: {pid})", err=True)
        click.echo(f"Use 'almanak strat backtest paper stop -s {strategy}' to stop it first.", err=True)
        raise click.Abort()


def resolve_rpc_url(rpc_url: str | None, chain: str) -> str:
    """Resolve an RPC URL from CLI arg or chain-specific env vars; abort if missing."""
    if rpc_url:
        return rpc_url

    chain_upper = chain.upper()
    env_var_names = [
        f"ALMANAK_{chain_upper}_RPC_URL",
        f"{chain_upper}_RPC_URL",
        "ALMANAK_RPC_URL",
        "RPC_URL",
    ]
    for env_var in env_var_names:
        value = os.environ.get(env_var)
        if value:
            return value

    click.echo(f"Error: No RPC URL provided for chain '{chain}'", err=True)
    click.echo(f"Set one of: {', '.join(env_var_names[:2])}", err=True)
    click.echo("Or use --rpc-url option", err=True)
    raise click.Abort()


def parse_initial_tokens_arg(initial_tokens: str) -> dict[str, Decimal]:
    """Parse a `TOKEN:AMOUNT,TOKEN:AMOUNT` CLI string into a dict.

    Aborts the Click context on parse failure.
    """
    if not initial_tokens:
        return {}

    cli_tokens: dict[str, Decimal] = {}
    try:
        for raw in initial_tokens.split(","):
            pair = raw.strip()
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
    return cli_tokens


def parse_funding_dict(
    funding: dict,
    native_symbols: frozenset[str],
    source: str,
) -> tuple[Decimal | None, dict[str, Decimal]]:
    """Parse a flat token->amount dict into (native_eth, erc20_tokens).

    Native tokens (ETH/MNT/AVAX/...) collapse onto a single eth_val slot;
    addresses are checksummed; ambiguous addresses log a warning and skip.
    """
    eth_val: Decimal | None = None
    tokens: dict[str, Decimal] = {}
    for token_name, amount in funding.items():
        token_str = str(token_name)
        if token_str.upper() in native_symbols:
            eth_val = Decimal(str(amount))
            continue
        if token_str.startswith(("0x", "0X")) and len(token_str) == 42:
            from eth_utils import to_checksum_address

            try:
                tokens[to_checksum_address(token_str)] = Decimal(str(amount))
            except (ValueError, TypeError) as e:
                click.echo(
                    f"Warning: ignoring invalid token address in {source}: {token_str} ({e})",
                    err=True,
                )
            continue
        tokens[token_str] = Decimal(str(amount))
    return eth_val, tokens


def load_funding_from_config(
    strategy: str,
    chain: str,
) -> tuple[Decimal | None, dict[str, Decimal], dict[str, dict[str, Decimal]], dict[str, Any] | None]:
    """Load bootstrap / anvil_funding from the strategy config for `chain`.

    Returns (config_eth, config_tokens, config_bootstrap, strategy_config).
    Errors are non-fatal: a warning is logged and empty values are returned.
    """
    config_eth: Decimal | None = None
    config_tokens: dict[str, Decimal] = {}
    config_bootstrap: dict[str, dict[str, Decimal]] = {}
    strategy_config: dict[str, Any] | None = None

    try:
        strategy_config = load_strategy_config(strategy, chain)
        from almanak.gateway.managed import ManagedGateway

        native_symbols = ManagedGateway.NATIVE_TOKEN_SYMBOLS

        paper_block = strategy_config.get("paper_trading", {})
        bootstrap_raw = paper_block.get("bootstrap", {}) if isinstance(paper_block, dict) else {}

        if bootstrap_raw and isinstance(bootstrap_raw, dict):
            click.echo(f"Found paper_trading.bootstrap in config: {bootstrap_raw}", err=True)
            config_bootstrap = _build_bootstrap_map(bootstrap_raw, native_symbols)
            current = dict(config_bootstrap.get(chain.lower(), {}))
            if current:
                config_eth = current.pop("ETH", None)
                config_tokens.update(current)

        if not config_bootstrap:
            anvil_funding = strategy_config.get("anvil_funding", {})
            if anvil_funding:
                if not isinstance(anvil_funding, dict):
                    raise ValueError(
                        f"anvil_funding must be an object mapping TOKEN->AMOUNT, got {type(anvil_funding).__name__}"
                    )
                click.echo(f"Found anvil_funding in config: {anvil_funding}", err=True)
                config_eth, config_tokens = parse_funding_dict(anvil_funding, native_symbols, "anvil_funding")
    except Exception as e:
        click.echo(
            f"Warning: ignoring invalid bootstrap/anvil_funding in strategy config: {e}",
            err=True,
        )

    return config_eth, config_tokens, config_bootstrap, strategy_config


def _build_bootstrap_map(
    bootstrap_raw: dict,
    native_symbols: frozenset[str],
) -> dict[str, dict[str, Decimal]]:
    """Convert a `paper_trading.bootstrap` block into a chain->{TOKEN: amount} map."""
    result: dict[str, dict[str, Decimal]] = {}
    for chain_key, chain_tokens_raw in bootstrap_raw.items():
        if not isinstance(chain_tokens_raw, dict):
            continue
        chain_eth, chain_toks = parse_funding_dict(
            chain_tokens_raw,
            native_symbols,
            f"paper_trading.bootstrap[{chain_key}]",
        )
        entry: dict[str, Decimal] = {}
        if chain_eth is not None:
            entry["ETH"] = chain_eth
        entry.update(chain_toks)
        result[chain_key.lower()] = entry
    return result


def merge_funding(
    initial_eth_cli: float,
    cli_eth_explicit: bool,
    config_eth: Decimal | None,
    config_tokens: dict[str, Decimal],
    cli_tokens: dict[str, Decimal],
) -> tuple[float, dict[str, Decimal]]:
    """Merge config-supplied funding with CLI overrides; CLI wins."""
    initial_eth = initial_eth_cli
    if config_eth is not None and not cli_eth_explicit:
        initial_eth = float(config_eth)
    parsed_tokens = {**config_tokens, **cli_tokens}
    return initial_eth, parsed_tokens


def apply_preset(paper_config: PaperTraderConfig, preset: str | None) -> None:
    """Mutate `paper_config` in place to apply preset overrides (VIB-2636)."""
    if preset == "yield-validation":
        from ...backtesting.paper.config import ForkLifecycle

        paper_config.fork_lifecycle = ForkLifecycle.PERSISTENT
        paper_config.reset_fork_every_tick = False
        paper_config.yield_poker_enabled = True
        paper_config.use_rich_valuation = True
        paper_config.position_reconciler_enabled = True
        click.echo(
            click.style(
                "Preset: yield-validation (persistent fork, interest accrual enabled)",
                fg="cyan",
            )
        )
        return

    label = preset or "execution-validation"
    click.echo(
        click.style(
            f"Preset: {label} (rolling fork reset, TX smoke testing)",
            fg="cyan",
        )
    )


def print_paper_config(
    *,
    strategy: str,
    chain: str,
    paper_config: PaperTraderConfig,
    initial_eth: float,
    parsed_tokens: dict[str, Decimal],
    tick_interval: int,
    max_ticks: int | None,
    anvil_port: int,
    no_reset_fork: bool,
    foreground: bool,
    output_path: Any,
    strategy_config: dict[str, Any] | None,
) -> None:
    """Render the paper-trading configuration banner."""
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

    force_action = strategy_config.get("force_action") if strategy_config else None
    if force_action:
        click.echo()
        click.echo(
            f"  WARNING: force_action='{force_action}' detected in config. "
            f"Strategy will bypass indicator logic for every tick.",
            err=True,
        )

    click.echo("=" * 60)


# =============================================================================
# paper_resume helpers
# =============================================================================


def load_resume_state(strategy: str) -> tuple[BackgroundPaperTrader, PaperTraderState]:
    """Locate the saved paper-trader state for `strategy` or abort."""
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
    return bg_trader, state


def reset_dead_or_abort(
    state: PaperTraderState,
    bg_trader: BackgroundPaperTrader,
    strategy: str,
) -> None:
    """If the saved session is dead, mark it stopped; if alive, abort."""
    if state.status != "running":
        return
    if state.pid and not is_process_running(state.pid):
        click.echo(f"Process {state.pid} is no longer running. Resetting status to stopped.")
        state.status = "stopped"
        state.save(bg_trader.state_file)
        pid_file = PIDFile(path=bg_trader.pid_file_path, strategy_id=strategy)
        pid_file.release()
        return

    click.echo(f"Error: Session '{strategy}' is still running (PID: {state.pid}).", err=True)
    click.echo(f"Use 'paper stop -s {strategy}' first.", err=True)
    raise click.Abort()


def resolve_resume_rpc_url(saved_rpc_url: Any, chain: str) -> str:
    """Pick an RPC URL for resume — falls back to env vars when masked or missing."""
    rpc_url = saved_rpc_url
    if rpc_url and "***" not in str(rpc_url):
        return str(rpc_url)

    chain_upper = chain.upper()
    for env_var in [f"ALMANAK_{chain_upper}_RPC_URL", f"{chain_upper}_RPC_URL", "ALMANAK_RPC_URL", "RPC_URL"]:
        value = os.environ.get(env_var)
        if value:
            return value

    click.echo(f"Error: Saved RPC URL is masked and no env var found for '{chain}'.", err=True)
    click.echo(f"Set ALMANAK_{chain_upper}_RPC_URL or use 'paper start' instead.", err=True)
    raise click.Abort()


def compute_resume_max_ticks(
    duration: str | None,
    max_ticks: int | None,
    saved_max_ticks: int | None,
    tick_count: int,
    tick_interval: int,
) -> int | None:
    """Resolve the new max-ticks for resume from --duration/--max-ticks vs saved."""
    if duration is not None:
        duration_seconds = _parse_duration(duration)
        if duration_seconds is None:
            click.echo(f"Error: Invalid duration '{duration}'. Use format like '1h', '48h'.", err=True)
            raise click.Abort()
        additional = max(1, duration_seconds // tick_interval + 1)
        new_max = tick_count + additional
        click.echo(f"Duration {duration} -> {additional} additional ticks (total: {new_max})")
        return new_max

    if max_ticks is not None:
        if max_ticks <= tick_count:
            click.echo(
                f"Error: --max-ticks ({max_ticks}) must be greater than current tick count ({tick_count}).",
                err=True,
            )
            raise click.Abort()
        return max_ticks

    return saved_max_ticks


def build_resume_config(
    saved_config: dict[str, Any],
    strategy: str,
    chain: str,
    rpc_url: str,
    new_max_ticks: int | None,
    tick_interval: int,
) -> PaperTraderConfig:
    """Construct a `PaperTraderConfig` from the saved state for resume.

    Preserves every field that was on the saved config — including preset-driven
    flags like ``bootstrap``, ``strict_bootstrap``, ``strict_price_mode``,
    ``fork_lifecycle``, ``yield_poker_enabled``, ``use_rich_valuation``, and
    ``position_reconciler_enabled`` — and only overrides the fields that
    legitimately change on resume.
    """
    import dataclasses

    valid_fields = {f.name for f in dataclasses.fields(PaperTraderConfig)}
    kwargs: dict[str, Any] = {k: v for k, v in saved_config.items() if k in valid_fields}

    # JSON round-trips: rehydrate Decimal-typed fields stored as strings/floats.
    if "initial_eth" in kwargs:
        kwargs["initial_eth"] = Decimal(str(kwargs["initial_eth"]))
    if "initial_tokens" in kwargs and isinstance(kwargs["initial_tokens"], dict):
        kwargs["initial_tokens"] = {k: Decimal(str(v)) for k, v in kwargs["initial_tokens"].items()}
    if "oracle_divergence_threshold" in kwargs:
        kwargs["oracle_divergence_threshold"] = Decimal(str(kwargs["oracle_divergence_threshold"]))
    if "bootstrap" in kwargs and isinstance(kwargs["bootstrap"], dict):
        kwargs["bootstrap"] = {
            chain_key: {tok: Decimal(str(amt)) for tok, amt in toks.items()}
            for chain_key, toks in kwargs["bootstrap"].items()
            if isinstance(toks, dict)
        }

    kwargs.update(
        chain=chain,
        rpc_url=rpc_url,
        strategy_id=strategy,
        tick_interval_seconds=tick_interval,
        max_ticks=new_max_ticks,
    )
    return PaperTraderConfig(**kwargs)


# =============================================================================
# paper_status helpers
# =============================================================================


def _format_pid_status(pid: Any, fallback_status: str) -> str:
    """Resolve a session's display status from its PID + recorded status."""
    if pid and isinstance(pid, int):
        return f"running (PID: {pid})" if is_process_running(pid) else "stopped (process not found)"
    return fallback_status


def render_session_row(session: dict[str, Any], verbose: bool) -> None:
    """Render one row of the multi-session listing."""
    strategy_id = session.get("strategy_id", "unknown")
    status = session.get("status", "unknown")
    pid = session.get("pid", "N/A")
    start_time = session.get("start_time", "N/A")

    status_display = _format_pid_status(pid, status)

    click.echo(f"\nStrategy: {strategy_id}")
    click.echo(f"  Status: {status_display}")
    click.echo(f"  Started: {start_time}")

    if not verbose:
        return

    config = session.get("config", {})
    click.echo(f"  Chain: {config.get('chain', 'N/A')}")
    click.echo(f"  Tick Interval: {config.get('tick_interval_seconds', 'N/A')}s")
    click.echo(f"  Max Ticks: {config.get('max_ticks', 'unlimited')}")

    summary = session.get("summary")
    if summary:
        click.echo(f"  Trades: {summary.get('successful_trades', 0)}")
        click.echo(f"  Errors: {summary.get('failed_trades', 0)}")


def render_all_sessions(verbose: bool) -> None:
    """Render the `--all` listing."""
    sessions = list_paper_sessions()
    if not sessions:
        click.echo("No paper trading sessions found.")
        return

    click.echo("=" * 60)
    click.echo("PAPER TRADING SESSIONS")
    click.echo("=" * 60)
    for session in sessions:
        render_session_row(session, verbose)
    click.echo()


def render_bg_status(strategy: str, bg_status: BackgroundStatus) -> None:
    """Render status sourced from BackgroundPaperTrader state."""
    click.echo("=" * 60)
    click.echo(f"PAPER TRADING STATUS: {strategy}")
    click.echo("=" * 60)
    running_label = "running" if bg_status.is_running else bg_status.status
    click.echo(f"Status: {running_label} (PID: {bg_status.pid or 'N/A'})")
    if bg_status.session_start:
        click.echo(f"Started: {bg_status.session_start.strftime('%Y-%m-%d %H:%M:%S')}")
    click.echo(f"Ticks: {bg_status.tick_count}")
    click.echo(f"Trades: {bg_status.trade_count}")
    click.echo(f"Errors: {bg_status.error_count}")
    if bg_status.last_save:
        click.echo(f"Last Save: {bg_status.last_save.strftime('%Y-%m-%d %H:%M:%S')}")
    if bg_status.can_resume:
        click.echo(f"Can Resume: yes (resume_count: {bg_status.resume_count})")


def render_fallback_state(strategy: str, state: dict[str, Any]) -> None:
    """Render status sourced from the legacy CLI-side session file."""
    click.echo("=" * 60)
    click.echo(f"PAPER TRADING STATUS: {strategy}")
    click.echo("=" * 60)

    pid = state.get("pid")
    actual_status = _format_pid_status(pid, state.get("status", "unknown"))
    click.echo(f"Status: {actual_status}")
    click.echo(f"Started: {state.get('start_time', 'N/A')}")
    last_updated = state.get("last_updated")
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
        _render_summary_block(summary)


def _render_summary_block(summary: dict[str, Any]) -> None:
    """Render the trailing 'Results' block for a fallback session."""
    click.echo()
    click.echo("Results:")
    click.echo(f"  Duration: {summary.get('duration', 'N/A')}")
    click.echo(f"  Total Trades: {summary.get('total_trades', 0)}")
    click.echo(f"  Successful: {summary.get('successful_trades', 0)}")
    click.echo(f"  Failed: {summary.get('failed_trades', 0)}")
    if summary.get("pnl_usd"):
        click.echo(f"  PnL: ${float(summary['pnl_usd']):,.2f}")


def render_single_session_status(strategy: str) -> None:
    """Render status for a single strategy (bg first, then CLI fallback)."""
    bg_trader = BackgroundPaperTrader(
        config=PaperTraderConfig(
            chain="arbitrum",
            rpc_url="http://localhost:8545",
            strategy_id=strategy,
        ),
    )
    bg_status = bg_trader.get_status()
    if bg_status.is_running or bg_status.tick_count > 0:
        render_bg_status(strategy, bg_status)
        return

    state = load_paper_session_state(strategy)
    if not state:
        click.echo(f"No paper trading session found for '{strategy}'")
        click.echo()
        click.echo("To start a session:")
        click.echo(f"  almanak strat backtest paper start -s {strategy}")
        return

    render_fallback_state(strategy, state)


def utc_now() -> datetime:
    """Indirection for `datetime.now(UTC)` so tests can patch it cleanly."""
    return datetime.now(UTC)
