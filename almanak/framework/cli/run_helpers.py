"""Helper functions extracted from `framework/cli/run.py:run` (Phase 4a).

Pure refactor: these helpers encapsulate well-defined chunks of the `run()`
CLI orchestration. No behavior change. Each helper preserves the exact
click.echo output, exit codes, and side effects (env mutations, atexit
registrations) of the original inlined code.

Scope of 4a (this module):
    - _configure_logging_and_validate — phase 1 of run()
    - _handle_list_all              — phase 4 of run()
    - _load_strategy_class          — phase 6 of run()
    - _discover_and_load_config     — phase 7 of run()
    - _print_startup_banner         — phase 10 of run()

Later phases (4b-4e) will extend this module for gateway/identity/state,
component init, and execution wrappers. See `jazzy-tinkering-zephyr.md`.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

import click

if TYPE_CHECKING:
    from ..strategies.intent_strategy import IntentStrategy

logger = logging.getLogger(__name__)


def _configure_logging_and_validate(
    *,
    verbose: bool,
    debug: bool,
    log_file: str | None,
    once: bool,
    teardown_after: bool,
) -> None:
    """Configure structured logging and validate setup-stage flag combinations.

    Mirrors the pre-gateway block in `run()` (phase 1):
        - Select log level (debug > verbose > info).
        - Configure logging with console format.
        - Optionally add a JSON file handler at DEBUG level.
        - Suppress or enable third-party (web3/urllib3/...) loggers.
        - Validate `--teardown-after` requires `--once` (hard exit on mismatch).

    Args:
        verbose: Whether `--verbose` was set (DEBUG for src.* modules).
        debug: Whether `--debug` was set (DEBUG everywhere, including third-party).
        log_file: Optional path to write JSON logs to.
        once: Whether `--once` was set (required by `--teardown-after`).
        teardown_after: Whether `--teardown-after` was set.
    """
    from ..utils.logging import LogFormat, LogLevel, add_file_handler, configure_logging

    # Determine log level: debug > verbose > default (info)
    if debug:
        log_level = LogLevel.DEBUG
    elif verbose:
        log_level = LogLevel.DEBUG  # Verbose shows DEBUG for src.* modules
    else:
        log_level = LogLevel.INFO

    # Use console format for human-readable output
    configure_logging(level=log_level, format=LogFormat.CONSOLE)

    # Add JSON file handler if --log-file is specified
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        add_file_handler(str(log_path), level=LogLevel.DEBUG)
        click.echo(f"Logging to file: {log_path} (JSON format)")

    # Control third-party logger verbosity based on --debug flag
    # By default, suppress Web3/HTTP noise unless --debug is specified
    if not debug:
        # Suppress third-party debug logs (keep only WARNING+)
        logging.getLogger("web3").setLevel(logging.WARNING)
        logging.getLogger("urllib3").setLevel(logging.WARNING)
        logging.getLogger("aiohttp").setLevel(logging.WARNING)
        logging.getLogger("asyncio").setLevel(logging.WARNING)
        logging.getLogger("httpx").setLevel(logging.WARNING)
        logging.getLogger("httpcore").setLevel(logging.WARNING)
    else:
        # --debug flag: allow all debug logs including third-party
        logging.getLogger("web3").setLevel(logging.DEBUG)
        logging.getLogger("urllib3").setLevel(logging.DEBUG)

    # Validate --teardown-after requires --once
    if teardown_after and not once:
        click.echo("Error: --teardown-after requires --once.", err=True)
        sys.exit(1)


def _handle_list_all(list_all: bool, gateway_client: Any) -> bool:
    """Handle the `--list-all` early-exit branch.

    Returns True if the branch was handled (caller should return). Returns
    False if `list_all` is False and normal execution should continue.

    Note: `gateway_client` is accepted to preserve the original call-site
    position in `run()` (the branch sits between gateway setup and strategy
    loading). It is not used by the current listing logic, but passing it
    keeps the signature aligned with the surrounding context and lets future
    filtering (e.g., gateway-aware chain resolution) be added without churn.
    """
    if not list_all:
        return False

    # Imports deferred to avoid circular imports at module load time.
    # `is_multi_chain_strategy` and `get_strategy_chains` live in run.py;
    # `get_strategy` and `list_strategies` are re-exported by it and also
    # available directly on the strategies package.
    from ..strategies import get_strategy, list_strategies
    from .run import get_strategy_chains, is_multi_chain_strategy

    available = list_strategies()
    if available:
        click.echo("Registered strategies:")
        for name in sorted(available):
            # Mark multi-chain strategies
            try:
                strat_class = get_strategy(name)
                if is_multi_chain_strategy(strat_class):
                    chains = get_strategy_chains(strat_class)
                    click.echo(f"  - {name} [multi-chain: {', '.join(chains)}]")
                else:
                    click.echo(f"  - {name}")
            except Exception:
                click.echo(f"  - {name}")
    else:
        click.echo("No strategies registered in the factory.")
    click.echo()
    click.echo("To run a strategy, cd into its directory and run:")
    click.echo("  almanak strat run --once")
    return True


def _load_strategy_class(
    working_dir: str,
    preloaded: type[IntentStrategy[Any]] | None,
) -> type[IntentStrategy[Any]]:
    """Load the strategy class from `<working_dir>/strategy.py`.

    If `preloaded` is provided (managed-gateway mode loads the class early to
    extract decorator metadata for chain selection), short-circuit and return
    it unchanged. Otherwise load from the filesystem. Missing strategy.py or
    load errors exit the process with code 1 (preserves the original
    `sys.exit(1)` behavior, not a raised exception).
    """
    from .intent_debug import load_strategy_from_file

    strategy_file = Path(working_dir) / "strategy.py"
    if not strategy_file.exists():
        click.echo(f"Error: No strategy.py found in {working_dir}", err=True)
        click.echo()
        click.echo("Make sure you're in a strategy directory or use --working-dir:")
        click.echo("  almanak strat run -d almanak/demo_strategies/uniswap_rsi --once")
        sys.exit(1)

    if preloaded is not None:
        return preloaded

    loaded, error = load_strategy_from_file(strategy_file)
    if not loaded:
        click.echo(f"Error loading strategy from {strategy_file}: {error}", err=True)
        sys.exit(1)
    return loaded


def _discover_and_load_config(
    *,
    working_dir: str,
    config_file: str | None,
    strategy_class: type[IntentStrategy[Any]],
    copy_mode: str | None,
    copy_shadow: bool,
    copy_replay_file: str | None,
    copy_strict: bool,
    dry_run: bool,
) -> tuple[dict[str, Any], bool, bool, str | None, str | None]:
    """Discover config file, load it, apply copy-trading overrides.

    Mirrors phase 7 of `run()`:
        1. Auto-discover config.json / config.yaml / config.yml in working_dir
           if `config_file` was not provided.
        2. Load config via `load_strategy_config` (exit 1 on error).
        3. Determine multi-chain mode via `is_multi_chain_strategy(..., config=...)`.
        4. Apply `--copy-mode` / `--copy-shadow` / `--copy-replay-file` /
           `--copy-strict` overrides to `strategy_config["copy_trading"]`.
        5. Compute `effective_dry_run = dry_run OR copy_shadow OR
           copy_mode in {shadow, replay}`.

    The caller (`run()`) still owns the `strategy_chains` list and refines it
    from `strategy_config["chains"]` when `multi_chain` is True (that lookup
    is local to the caller and was kept inline to avoid threading
    `strategy_chains` through this helper).

    Returns:
        strategy_config: loaded & override-applied config dict.
        multi_chain: bool (from `is_multi_chain_strategy(..., config=...)`).
        effective_dry_run: bool.
        resolved_config_file: the config file path the discovery step settled
            on (may be None if no explicit path and no auto-discovery match).
        normalized_copy_mode: lowercased form of `--copy-mode` (or None). The
            caller uses this downstream when attaching copy-trading metadata
            to the strategy instance; returning it avoids re-computing
            `copy_mode.lower()` in the caller.
    """
    from .run import is_multi_chain_strategy, load_strategy_config

    strategy_name = strategy_class.__name__
    resolved_config_file = config_file

    # Auto-discover config file from working directory if not explicitly provided
    if not resolved_config_file:
        for candidate_name in ["config.json", "config.yaml", "config.yml"]:
            candidate = Path(working_dir) / candidate_name
            if candidate.exists():
                resolved_config_file = str(candidate)
                break

    # Load strategy configuration FIRST to get chain (if specified)
    try:
        strategy_config = load_strategy_config(strategy_name, resolved_config_file)
    except Exception as e:
        click.echo(f"Error loading strategy config: {e}", err=True)
        sys.exit(1)

    # Now determine multi-chain mode from config (not decorator supported_chains,
    # which is portability metadata). A "chains" list with >1 entry in config.json
    # signals the strategy should execute across multiple chains simultaneously.
    multi_chain = is_multi_chain_strategy(strategy_class, config=strategy_config)

    normalized_copy_mode = copy_mode.lower() if copy_mode is not None else None

    # Apply copy-trading runtime overrides from CLI flags.
    if any([normalized_copy_mode, copy_shadow, copy_replay_file, copy_strict]):
        ct_config = strategy_config.get("copy_trading")
        if ct_config is None:
            ct_config = {}
            strategy_config["copy_trading"] = ct_config
        if not isinstance(ct_config, dict):
            raise click.ClickException("copy_trading config must be an object when using copy override flags")

        raw_policy = ct_config.get("execution_policy", {})
        if raw_policy is not None and not isinstance(raw_policy, dict):
            raise click.ClickException("copy_trading.execution_policy must be an object when present")
        execution_policy = dict(raw_policy or {})
        if normalized_copy_mode is not None:
            execution_policy["copy_mode"] = normalized_copy_mode
        if copy_shadow:
            execution_policy["shadow"] = True
            execution_policy["copy_mode"] = "shadow"
        if copy_replay_file:
            execution_policy["replay_file"] = copy_replay_file
            execution_policy["copy_mode"] = "replay"
        if copy_strict:
            execution_policy["strict"] = True

        ct_config["execution_policy"] = execution_policy

    effective_dry_run = (
        dry_run or copy_shadow or (normalized_copy_mode in {"shadow", "replay"}) or (copy_replay_file is not None)
    )

    return strategy_config, multi_chain, effective_dry_run, resolved_config_file, normalized_copy_mode


def _print_startup_banner(
    *,
    strategy_name: str,
    strategy_id: str,
    run_id: str,
    is_resume: bool,
    existing_state_info: dict[str, Any] | None,
    once: bool,
    fresh: bool,
    multi_chain: bool,
    strategy_chains: list[str],
    strategy_protocols: Any,
    runtime_config: Any,
    interval: int,
    max_iterations: int | None,
    effective_dry_run: bool,
    strategy_config: dict[str, Any],
    gateway_host: str,
    gateway_port: int,
    dashboard: bool,
) -> None:
    """Print the startup banner for `almanak strat run`.

    Pure print helper. All fields are taken as keyword arguments to keep the
    helper free of magic: no attribute-lookups on an opaque context, no
    implicit defaulting. The set of click.echo calls (including conditionals
    and colors) matches the inlined block in `run()` (phase 10) byte-for-byte.
    """
    click.echo("=" * 60)
    click.echo("ALMANAK STRATEGY RUNNER")
    click.echo("=" * 60)
    click.echo(f"Strategy: {strategy_name}")
    click.echo(f"Deployment ID: {strategy_id}")
    click.echo(f"Run ID: {run_id}")
    if is_resume:
        click.secho("Mode: RESUME (existing state found)", fg="yellow", bold=True)
        if existing_state_info:
            click.echo(f"  State version: {existing_state_info['version']}, keys: {existing_state_info['keys']}")
        if once and not fresh:
            click.secho(
                "WARNING: Loading state from a previous run. "
                "If this is unexpected, re-run with --fresh to start clean.",
                fg="red",
                bold=True,
            )
    else:
        click.secho("Mode: FRESH START (no existing state)", fg="green", bold=True)
    if multi_chain:
        click.echo(f"Chains: {', '.join(strategy_chains)}")
        click.echo(f"Protocols: {strategy_protocols}")
    else:
        click.echo(f"Chain: {runtime_config.chain}")
    safe_mode_str = " (Safe)" if runtime_config.is_safe_mode else ""
    click.echo(f"Wallet: {runtime_config.execution_address}{safe_mode_str}")
    exec_desc = "Single run" if once else f"Continuous (every {interval}s)"
    if max_iterations and not once:
        exec_desc += f", max {max_iterations} iterations"
    click.echo(f"Execution: {exec_desc}")
    click.echo(f"Dry run: {effective_dry_run}")
    if isinstance(strategy_config.get("copy_trading"), dict):
        copy_execution_policy = strategy_config["copy_trading"].get("execution_policy", {})
        copy_mode_label = copy_execution_policy.get("copy_mode", "live")
        click.echo(f"Copy mode: {copy_mode_label}")
        if copy_execution_policy.get("replay_file"):
            click.echo(f"Copy replay file: {copy_execution_policy.get('replay_file')}")
    click.secho(f"Gateway: {gateway_host}:{gateway_port}", fg="cyan")
    if dashboard:
        click.echo("Dashboard: Will launch alongside strategy")
    click.echo("=" * 60)
