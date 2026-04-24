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

Scope of 4b (this module):
    - _setup_gateway                — phase 2 of run()
    - _wire_token_resolver          — phase 3 of run()
    - _resolve_identity             — phase 8 of run()
    - _detect_state_resume          — phase 9 of run()

Scope of 4c (this module):
    - _instantiate_strategy         — phase 11 of run()
    - _build_runtime_config         — phase 12 of run()
    - _build_components             — phase 13 of run()
    - _build_cleanup_fn             — phase 14 of run()

Scope of 4d (this module):
    - _start_dashboard_background   — phase 5a of run()
    - _stop_dashboard               — phase 5b of run()
    - _handle_standalone_dashboard  — phase 5c of run()
    - _run_once                     — phase 15 of run()
    - _run_continuous               — phase 16 of run()

Phase 4e (extended tests) will add additional coverage. See
`jazzy-tinkering-zephyr.md`.
"""

from __future__ import annotations

import inspect
import json
import logging
import os
import sys
from collections.abc import Callable, Coroutine
from pathlib import Path
from typing import TYPE_CHECKING, Any

import click

from ..strategies.metadata import LEGACY_COMPAT_DATA_REQUIREMENTS, StrategyDataRequirements
from ._run_context import ComponentBundle, IdentityInfo, ResumeInfo

if TYPE_CHECKING:
    from ..strategies.intent_strategy import IntentStrategy

logger = logging.getLogger(__name__)


class _DryRunVaultEarlyExit(Exception):
    """Signal an intentional exit-0 from `_maybe_auto_deploy_vault`.

    Raised when `--dry-run --network anvil` is used against a strategy with
    a placeholder vault address: we skip auto-deploy but still want `run()`
    to unwind `cleanup_fn` (closing providers, gateway, Solana fork manager)
    before exiting 0. See #1682.

    The partial `ComponentBundle` built so far is attached so the caller can
    feed it to `_build_cleanup_fn` (runner is None at this point; cleanup
    only touches providers + gateway + Solana fork).
    """

    def __init__(self, components: ComponentBundle | None = None) -> None:
        super().__init__("--dry-run: placeholder vault on Anvil")
        self.components = components


def _normalize_quick_chains(raw: Any) -> list[str]:
    """Normalize a quick-config ``chains`` value into a list of chain names.

    User-authored config files may set ``chains`` to a single string scalar
    (``chains: arbitrum``) or a list (``chains: [arbitrum, base]``). Anything
    else — an int, a dict, None — is treated as "no chains specified" rather
    than blindly coerced. In particular, ``list({"base": 1})`` yields
    ``["base"]``, which would silently use dict keys as chain names; we
    reject that case explicitly.

    Args:
        raw: The raw value read from the config's ``chains`` field.

    Returns:
        A list of chain names, or an empty list if ``raw`` is neither a
        string nor a list.
    """
    if isinstance(raw, str):
        return [raw]
    if isinstance(raw, list):
        return [str(chain) for chain in raw]
    return []


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


def _wire_token_resolver(gateway_client: Any) -> None:
    """Wire the gateway channel into the global TokenResolver.

    Mirrors phase 3 of `run()`: after the gateway is live (managed or
    external), the TokenResolver needs the gRPC channel so it can resolve
    arbitrary ERC-20 addresses on-chain when a symbol misses the static
    registry.
    """
    from ..data.tokens import get_token_resolver

    resolver = get_token_resolver()
    resolver.set_gateway_channel(gateway_client.channel)


def _detect_state_resume(state_db_path: Path, deployment_id: str) -> ResumeInfo:
    """Detect whether a deployment has prior state (RESUME vs FRESH START).

    Mirrors phase 9 of `run()`. Quick SQLite read of `strategy_state`
    filtered by `strategy_id = deployment_id`. All errors (missing DB file,
    corrupt schema, connection failure, JSON-parse errors) are swallowed and
    logged at DEBUG — the caller treats them as "no resume", identical to
    the inlined behavior.

    Returns:
        ResumeInfo with is_resume=True if a matching row was found.
    """
    import sqlite3

    if not state_db_path.exists():
        return ResumeInfo(is_resume=False, version=None, state_keys=[])

    try:
        conn = sqlite3.connect(str(state_db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.execute(
            "SELECT strategy_id, version, state_data FROM strategy_state WHERE strategy_id = ?",
            (deployment_id,),
        )
        row = cursor.fetchone()
        conn.close()
        if row is None:
            return ResumeInfo(is_resume=False, version=None, state_keys=[])
        try:
            state_data = json.loads(row["state_data"]) if row["state_data"] else {}
            keys = list(state_data.keys()) if state_data else []
        except Exception:
            keys = []
        return ResumeInfo(is_resume=True, version=row["version"], state_keys=keys)
    except Exception as e:
        logger.debug(f"Could not check for existing state: {e}")
        return ResumeInfo(is_resume=False, version=None, state_keys=[])


def _resolve_identity(
    *,
    strategy_config: dict[str, Any],
    fresh: bool,
    multi_chain: bool,
    strategy_chains: list[str],
    config_display_name: str,
    cli_id_override: str | None,
    gateway_network: str,
) -> IdentityInfo:
    """Resolve deployment_id/run_id, backfill old state rows, and handle `--fresh`.

    Mirrors phase 8 of `run()`. This helper:
        1. Computes `identity_chain` (the strategy's chain or a
           comma-separated sorted multi-chain signature).
        2. Resolves `deployment_id` via `resolve_deployment_id()`.
        3. Generates an ephemeral `run_id`.
        4. Writes both into `strategy_config` (mutated in place, matching
           the original inlined behavior).
        5. If the deployment_id differs from the config's display name, runs
           `SQLiteStore.backfill_deployment_id` to migrate old rows. A
           failure inside the backfill is logged at DEBUG and swallowed —
           startup must not crash if the migration can't run.
        6. If `fresh=True`, deletes `strategy_state` (and `teardown_requests`
           if present) rows. On Anvil, scope is ALL rows (VIB-2573). On
           mainnet, scope is just the current strategy_id.

    Args:
        strategy_config: strategy config dict. MUTATED: strategy_id and
            run_id are written in, matching the original code.
        fresh: True if `--fresh` was passed.
        multi_chain: True if the strategy runs on multiple chains.
        strategy_chains: chains the strategy is configured for (only used
            when multi_chain=True).
        config_display_name: the human-facing strategy name (pre-normalized
            before the ":" suffix is stripped).
        cli_id_override: explicit `--id <override>` value, or None.
        gateway_network: "anvil" triggers anvil-scope fresh deletion.

    Returns:
        IdentityInfo snapshotting (deployment_id, run_id, strategy_name,
        migrated).
    """
    from almanak.framework.runner.identity import generate_run_id, resolve_deployment_id

    # Resolve deployment_id now that wallet + chain are known (VIB-2764).
    # For multi-chain strategies, hash all chains so different chain combinations
    # produce distinct deployment_ids (e.g., [arbitrum,base] vs [arbitrum,optimism]).
    identity_chain = str(strategy_config.get("chain", ""))
    if multi_chain and strategy_chains:
        identity_chain = ",".join(sorted(str(c).lower() for c in strategy_chains))
    deployment_id = resolve_deployment_id(
        strategy_name=config_display_name,
        wallet_address=strategy_config.get("wallet_address", ""),
        chain=identity_chain,
        cli_id=cli_id_override,
    )
    strategy_config["strategy_id"] = deployment_id
    run_id = generate_run_id()
    strategy_config["run_id"] = run_id

    # Backfill: migrate data from bare strategy name to deployment_id (VIB-2767).
    # Uses the centralized SQLiteStore.backfill_deployment_id helper so that ALL
    # tables (including timeline_events) are migrated consistently with _db_lock.
    migrated = False
    if deployment_id != config_display_name:
        state_db_path = Path(os.environ.get("ALMANAK_STATE_DB") or "./almanak_state.db")
        if state_db_path.exists():
            try:
                import asyncio as _asyncio_backfill

                from almanak.framework.state.backends.sqlite import SQLiteConfig, SQLiteStore

                backfill_config = SQLiteConfig(db_path=str(state_db_path))
                backfill_store = SQLiteStore(backfill_config)
                loop = _asyncio_backfill.new_event_loop()
                try:
                    total_migrated = loop.run_until_complete(
                        backfill_store.backfill_deployment_id(config_display_name, deployment_id)
                    )
                    if total_migrated > 0:
                        migrated = True
                        click.echo(f"Migrated {total_migrated} rows from '{config_display_name}' to '{deployment_id}'")
                finally:
                    loop.run_until_complete(backfill_store.close())
                    loop.close()
            except Exception as e:
                logger.debug("Backfill migration skipped: %s", e)

    # Handle --fresh flag: clear state to prevent cross-strategy contamination.
    # VIB-2573: On Anvil, clear ALL strategy state (not just current strategy)
    # to prevent TokenNotFoundError from stale state referencing wrong-chain tokens.
    # On mainnet, only clear the current strategy's state (preserve other strategies).
    strategy_id = strategy_config["strategy_id"]
    if fresh:
        state_db_path = Path(os.environ.get("ALMANAK_STATE_DB") or "./almanak_state.db")
        if state_db_path.exists():
            try:
                import sqlite3

                is_anvil = gateway_network == "anvil"
                with sqlite3.connect(str(state_db_path)) as conn:
                    if is_anvil:
                        # Clear ALL state on Anvil — previous strategy runs on
                        # different chains leave state that can contaminate the gateway
                        cursor = conn.execute("DELETE FROM strategy_state")
                    else:
                        cursor = conn.execute(
                            "DELETE FROM strategy_state WHERE strategy_id = ?",
                            (strategy_id,),
                        )
                    deleted = cursor.rowcount
                    # Also clear teardown requests
                    try:
                        if is_anvil:
                            teardown_cursor = conn.execute("DELETE FROM teardown_requests")
                        else:
                            teardown_cursor = conn.execute(
                                "DELETE FROM teardown_requests WHERE strategy_id = ?",
                                (strategy_id,),
                            )
                        teardown_deleted = teardown_cursor.rowcount
                    except sqlite3.OperationalError:
                        teardown_deleted = 0  # Table may not exist
                if deleted > 0 or teardown_deleted > 0:
                    parts = []
                    if deleted > 0:
                        parts.append("state")
                    if teardown_deleted > 0:
                        parts.append("teardown requests")
                    scope = "all strategies" if is_anvil else f"strategy '{strategy_id}'"
                    click.secho(
                        f"Cleared {' and '.join(parts)} for {scope} (--fresh flag)",
                        fg="yellow",
                    )
                else:
                    click.echo(f"No existing state for strategy '{strategy_id}' (--fresh flag)")
            except sqlite3.Error as e:
                click.echo(f"Failed to clear strategy state: {e}", err=True)
        else:
            click.echo("No existing state to clear (--fresh flag)")

    return IdentityInfo(
        deployment_id=deployment_id,
        run_id=run_id,
        strategy_name=config_display_name,
        migrated=migrated,
    )


def _setup_gateway(
    *,
    working_dir: str,
    config_file: str | None,
    network: str | None,
    gateway_host: str,
    gateway_port: int,
    no_gateway: bool,
    anvil_ports: tuple[str, ...],
    wallet: str,
    keep_anvil: bool,
    reset_fork: bool,
    once: bool,
) -> tuple[Any, Any, str, int, str, str | None, str | None, type[IntentStrategy[Any]] | None]:
    """Set up the gateway (managed auto-start or connect to external).

    Mirrors phase 2 of `run()`. This is the CC-heaviest chunk in the phase
    map, encompassing:
        - `--wallet isolated` env mutation (derives a per-strategy key and
          overwrites `ALMANAK_PRIVATE_KEY`), with `--network anvil` guard.
        - `--anvil-port` CSV parsing into `external_anvil_ports`.
        - Early-load of the strategy class so decorator metadata is
          available for Anvil chain detection (runs before the later
          `_load_strategy_class` call in `run()`).
        - Anvil chain resolution from config.json/decorator + external ports.
        - `ALMANAK_GATEWAY_AUTH_TOKEN` / `GATEWAY_AUTH_TOKEN` /
          `ALMANAK_GATEWAY_ALLOW_INSECURE` handling.
        - Random session auth token for non-test-network managed gateways
          (VIB-520).
        - `find_available_gateway_port` and `ManagedGateway.start()` with
          anvil-appropriate timeouts.
        - atexit registration of `managed_gateway.stop` at the same point
          in the flow as the original.
        - `GatewayClient.wait_for_ready()` health wait.

    Args:
        working_dir: strategy directory (searched for strategy.py and config).
        config_file: explicit `--config` path (or None for auto-discovery).
        network: `--network` CLI flag (or None for default).
        gateway_host: `--gateway-host` (normalized to 127.0.0.1 if "localhost").
        gateway_port: `--gateway-port`.
        no_gateway: `--no-gateway` (connect to existing gateway instead of
            auto-starting managed).
        anvil_ports: tuple of CHAIN=PORT strings from `--anvil-port`.
        wallet: `--wallet` ("default" or "isolated").
        keep_anvil: `--keep-anvil` (skip Anvil teardown on exit).
        reset_fork: `--reset-fork` (Anvil-only).
        once: `--once` (used to suppress `--reset-fork` note in single-run).

    Returns:
        (gateway_client, managed_gateway, effective_host, gateway_port,
         gateway_network, session_auth_token, isolated_wallet_address,
         early_strategy_class).

        managed_gateway is None when `--no-gateway` was used.
        isolated_wallet_address is None unless `--wallet isolated` derived one.
        early_strategy_class is None if strategy.py wasn't present or failed
        to load (retried later by `_load_strategy_class`).
    """
    import atexit
    import uuid

    from almanak.gateway.core.settings import GatewaySettings
    from almanak.gateway.managed import ManagedGateway, find_available_gateway_port

    from ..gateway_client import GatewayClient, GatewayClientConfig
    from .intent_debug import load_strategy_from_file

    # Normalize "localhost" to "127.0.0.1" (gateway binds to 127.0.0.1)
    effective_host = "127.0.0.1" if gateway_host == "localhost" else gateway_host

    managed_gateway: ManagedGateway | None = None
    early_strategy_class: type[IntentStrategy[Any]] | None = None
    external_anvil_ports: dict[str, int] = {}

    # Resolve network mode early because later runner setup uses the same value
    # for both managed and pre-existing gateway flows.
    if anvil_ports and not network and not no_gateway:
        network = "anvil"
    gateway_network = network or "mainnet"

    # --wallet isolated requires a managed gateway (derives wallet + funds Anvil fork)
    if wallet == "isolated" and no_gateway:
        raise click.ClickException(
            "--wallet isolated requires a managed gateway (incompatible with --no-gateway). "
            "Remove --no-gateway to let the CLI start its own gateway + Anvil fork."
        )

    if no_gateway:
        if anvil_ports:
            raise click.ClickException("--anvil-port requires a managed gateway (remove --no-gateway).")
        if keep_anvil:
            raise click.ClickException("--keep-anvil requires a managed gateway (remove --no-gateway).")
        # --wallet isolated requires the managed gateway (which auto-funds the derived wallet)
        if wallet == "isolated":
            raise click.ClickException(
                "--wallet isolated requires a managed gateway (remove --no-gateway). "
                "The managed gateway auto-funds the derived wallet on Anvil."
            )

        # --no-gateway: connect to an existing gateway, fail if unavailable
        click.echo(f"Connecting to existing gateway at {effective_host}:{gateway_port}...")
        # Read ALMANAK_GATEWAY_AUTH_TOKEN with fallback to GATEWAY_AUTH_TOKEN for backward compatibility
        auth_token = os.environ.get("ALMANAK_GATEWAY_AUTH_TOKEN") or os.environ.get("GATEWAY_AUTH_TOKEN")
        gateway_config = GatewayClientConfig(host=effective_host, port=gateway_port, auth_token=auth_token)
        gateway_client = GatewayClient(gateway_config)
        gateway_client.connect()

        click.echo("Waiting for gateway to become ready...")
        if not gateway_client.wait_for_ready(timeout=60.0, interval=5.0):
            gateway_client.disconnect()
            click.echo()
            click.secho("ERROR: Gateway is not running or not healthy", fg="red", bold=True)
            click.echo()
            click.echo("The gateway sidecar is required for all strategy operations.")
            click.echo("Start the gateway first with:")
            click.echo()
            click.echo("  almanak gateway")
            click.echo()
            raise click.ClickException(f"Gateway not available at {effective_host}:{gateway_port}")

        click.secho(f"Connected to existing gateway at {effective_host}:{gateway_port}", fg="green")
        return (
            gateway_client,
            None,
            effective_host,
            gateway_port,
            gateway_network,
            None,
            None,
            None,
        )

    # Default: auto-start a managed gateway
    try:
        gateway_port = find_available_gateway_port(effective_host, gateway_port)
    except RuntimeError as e:
        click.echo()
        click.secho(f"ERROR: {e}", fg="red", bold=True)
        click.echo()
        click.echo("Set a specific port with:")
        click.echo()
        click.echo("  almanak strat run --gateway-port <port>")
        click.echo()
        click.echo("Or connect to an existing gateway:")
        click.echo()
        click.echo("  almanak strat run --no-gateway --gateway-port <port>")
        click.echo()
        raise click.ClickException(str(e)) from None

    # Parse --anvil-port values into dict
    for entry in anvil_ports:
        if "=" not in entry:
            raise click.ClickException(
                f"Invalid --anvil-port format: '{entry}'. Expected CHAIN=PORT (e.g., arbitrum=8545)"
            )
        chain_name, port_str = entry.split("=", 1)
        chain_name = chain_name.strip().lower()
        if not chain_name:
            raise click.ClickException(f"Invalid --anvil-port format: '{entry}'. Chain name cannot be empty.")
        try:
            port = int(port_str)
        except ValueError:
            raise click.ClickException(f"Invalid port in --anvil-port: '{port_str}'") from None
        if not (1 <= port <= 65535):
            raise click.ClickException(f"Invalid port in --anvil-port: '{port_str}'. Expected 1-65535.")
        if chain_name in external_anvil_ports:
            raise click.ClickException(f"Duplicate --anvil-port for chain '{chain_name}'.")
        external_anvil_ports[chain_name] = port

    if keep_anvil and gateway_network != "anvil":
        click.echo("Warning: --keep-anvil has no effect without --network anvil or --anvil-port.")

    # Early-load strategy class so decorator metadata is available for chain detection.
    # This must happen before gateway startup so Anvil forks target the correct chain.
    _early_strategy_file = Path(working_dir) / "strategy.py"
    if _early_strategy_file.exists():
        early_strategy_class, _early_err = load_strategy_from_file(_early_strategy_file)
        if _early_err:
            logger.debug(f"Early strategy load failed (will retry later): {_early_err}")

    # Import get_default_chain lazily from .run to avoid circular-import.
    from .run import get_default_chain

    # Determine which chains need Anvil forks
    anvil_chains: list[str] = []
    anvil_funding: dict[str, float | int | str] = {}
    if gateway_network == "anvil":
        # Quick-read config for chain info and anvil_funding
        resolved_config_path: Path | None = Path(config_file) if config_file else None
        if resolved_config_path is None:
            for name in ["config.json", "config.yaml", "config.yml"]:
                candidate = Path(working_dir) / name
                if candidate.exists():
                    resolved_config_path = candidate
                    break
        if resolved_config_path and resolved_config_path.exists():
            # Malformed config files must not crash gateway startup. Swallow
            # parse errors here and fall through with an empty dict; the
            # full loader later in `_discover_and_load_config` will surface
            # a proper user-facing error if the file is truly broken.
            try:
                with open(resolved_config_path) as f:
                    if resolved_config_path.suffix.lower() in [".yaml", ".yml"]:
                        import yaml

                        quick_config = yaml.safe_load(f)
                    else:
                        quick_config = json.load(f)
            except Exception as e:
                logger.debug("Quick config probe failed for %s: %s", resolved_config_path, e)
                quick_config = {}
            # `yaml.safe_load` returns None for empty files, and a user-supplied
            # config could parse to a scalar or list. Coerce anything non-dict
            # to an empty dict so `.get()` is always safe.
            if not isinstance(quick_config, dict):
                quick_config = {}
            chain_val = quick_config.get("chain")
            chains_val = quick_config.get("chains")
            if chains_val is not None:
                # Normalize via helper: wraps strings into single-element lists,
                # coerces lists to list[str], and ignores non-str/non-list values
                # (ints, dicts) rather than silently using dict keys as chains.
                anvil_chains = _normalize_quick_chains(chains_val)
            elif chain_val:
                anvil_chains = [chain_val]
            anvil_funding = quick_config.get("anvil_funding", {})

        # Fall back to decorator metadata if config.json has no chain
        if not anvil_chains and early_strategy_class:
            decorator_chain = get_default_chain(early_strategy_class)
            if decorator_chain:
                anvil_chains = [decorator_chain]

        # Add externally-specified chains to anvil_chains if not already present
        for ext_chain in external_anvil_ports:
            if ext_chain not in anvil_chains:
                anvil_chains.append(ext_chain)

        # Solana uses solana-test-validator (not Anvil); filter it out so
        # ManagedGateway doesn't try to start a RollingForkManager for it.
        # The Solana fork is started separately below in the runner setup.
        NON_EVM_CHAINS = {"solana"}
        evm_anvil_chains = [c for c in anvil_chains if c.lower() not in NON_EVM_CHAINS]
        solana_anvil = any(c.lower() in NON_EVM_CHAINS for c in anvil_chains)
        anvil_chains = evm_anvil_chains

        if not anvil_chains and not solana_anvil:
            click.echo(
                "Warning: --network anvil specified but no chain found in config or decorator. "
                "Gateway will start without Anvil forks."
            )

    # Wallet isolation: derive a unique wallet per strategy on Anvil
    isolated_wallet_address: str | None = None
    if wallet == "isolated" and gateway_network == "anvil":
        from almanak.gateway.managed import derive_isolated_wallet

        master_key = os.environ.get("ALMANAK_PRIVATE_KEY", "")
        if not master_key:
            raise click.ClickException("--wallet isolated requires ALMANAK_PRIVATE_KEY to be set")
        # Use the strategy directory name as the derivation seed
        strategy_seed = Path(working_dir).resolve().name
        derived_key, isolated_wallet_address = derive_isolated_wallet(master_key, strategy_seed)
        # Override the env var so LocalRuntimeConfig.from_env() picks up the derived key
        os.environ["ALMANAK_PRIVATE_KEY"] = derived_key
        click.echo(
            f"Wallet: isolated ({isolated_wallet_address[:10]}...{isolated_wallet_address[-4:]}) "
            f"[derived from strategy '{strategy_seed}']"
        )
    elif wallet == "isolated" and gateway_network != "anvil":
        raise click.ClickException("--wallet isolated is only supported with --network anvil")

    # Validate --reset-fork requires --network anvil
    if reset_fork and gateway_network != "anvil":
        raise click.ClickException("--reset-fork is only supported with --network anvil")
    if reset_fork and once:
        click.echo("Note: --reset-fork has no effect with --once (fork is already fresh at startup)")

    # When using isolated wallets, pass the derived key to the gateway so its
    # signer matches the funded wallet. GatewaySettings reads ALMANAK_GATEWAY_PRIVATE_KEY
    # (not ALMANAK_PRIVATE_KEY), so we must pass it explicitly.
    gateway_private_key = os.environ.get("ALMANAK_PRIVATE_KEY") if isolated_wallet_address else None

    # Ensure gateway knows the strategy's chain for on-chain pricing.
    # For anvil mode, anvil_chains is already populated above.
    # For mainnet, read chain from config or decorator metadata so the MarketService
    # uses the correct Chainlink oracle chain instead of defaulting to arbitrum.
    gateway_chains = anvil_chains
    if not gateway_chains:
        resolved_config_path_gw: Path | None = Path(config_file) if config_file else None
        if resolved_config_path_gw is None:
            for name in ["config.json", "config.yaml", "config.yml"]:
                candidate = Path(working_dir) / name
                if candidate.exists():
                    resolved_config_path_gw = candidate
                    break
        if resolved_config_path_gw and resolved_config_path_gw.exists():
            # Same defensive treatment as the Anvil probe above: a malformed
            # config must not crash gateway startup at this early peek.
            try:
                with open(resolved_config_path_gw) as f:
                    if resolved_config_path_gw.suffix.lower() in [".yaml", ".yml"]:
                        import yaml

                        _quick = yaml.safe_load(f)
                    else:
                        _quick = json.load(f)
            except Exception as e:
                logger.debug("Quick config probe failed for %s: %s", resolved_config_path_gw, e)
                _quick = {}
            if not isinstance(_quick, dict):
                _quick = {}
            _chains_val = _quick.get("chains")
            _chain_val = _quick.get("chain")
            if _chains_val is not None:
                gateway_chains = _normalize_quick_chains(_chains_val)
            elif _chain_val:
                gateway_chains = [_chain_val]

        # Fall back to decorator metadata if config has no chain
        if not gateway_chains and early_strategy_class:
            decorator_chain = get_default_chain(early_strategy_class)
            if decorator_chain:
                gateway_chains = [decorator_chain]

    # Security: generate a random session token for the managed gateway so it
    # is never running without authentication, even on mainnet (VIB-520).
    # For anvil/sepolia we still use allow_insecure for convenience.
    is_test_network = gateway_network in ("anvil", "sepolia")
    session_auth_token = None if is_test_network else uuid.uuid4().hex

    gateway_kwargs: dict[str, Any] = {
        "grpc_host": effective_host,
        "grpc_port": gateway_port,
        "network": gateway_network,
        "allow_insecure": is_test_network,
        "metrics_enabled": False,
        "audit_enabled": False,
        "chains": gateway_chains,
    }
    # On test networks, force auth_token=None as an explicit kwarg so it wins
    # over any ALMANAK_GATEWAY_AUTH_TOKEN loaded from .env. Without this, the
    # server attaches AuthInterceptor while the client (allow_insecure=True)
    # sends no token, producing UNAUTHENTICATED on every gRPC call (VIB-3032).
    if is_test_network:
        gateway_kwargs["auth_token"] = None
    elif session_auth_token:
        gateway_kwargs["auth_token"] = session_auth_token
    if gateway_private_key:
        gateway_kwargs["private_key"] = gateway_private_key
    gateway_settings = GatewaySettings(**gateway_kwargs)

    if anvil_chains:
        click.echo(
            f"Starting managed gateway on {effective_host}:{gateway_port} "
            f"(network={gateway_network}, anvil chains: {', '.join(anvil_chains)})..."
        )
    else:
        click.echo(f"Starting managed gateway on {effective_host}:{gateway_port} (network={gateway_network})...")
    managed_gateway = ManagedGateway(
        gateway_settings,
        anvil_chains=anvil_chains,
        wallet_address=isolated_wallet_address,
        anvil_funding=anvil_funding,
        external_anvil_ports=external_anvil_ports,
        keep_anvil=keep_anvil,
    )
    # Anvil forks need extra startup time (forking from mainnet RPC).
    # Archive-RPC chains (Avalanche, Ethereum, Polygon) are slower to fork:
    # cold-cache Anvil startup against an archive node can take 60-90s, and
    # the default 30s is too short. A 30s timeout causes the managed gateway to
    # abort before the Anvil fork finishes, leaving a daemon gRPC thread running
    # that races with the main process's shutdown sequence — producing the
    # "absl::InitializeLog() called multiple times" error.
    #
    # Derive the outer timeout from the per-fork budgets so multi-archive-chain
    # configs (e.g. [ethereum, polygon]) also get enough time. ManagedGateway
    # starts forks sequentially, so the total Anvil budget is:
    #   sum(per_fork_timeout) + gateway_server_warmup_headroom
    # Keep _ARCHIVE_CHAINS_SLOW_FORK in sync with
    # ManagedGateway.ARCHIVE_RPC_REQUIRED_CHAINS in gateway/managed.py.
    # Canonicalize chain names via resolve_chain_name so aliases such as
    # "avax" -> "avalanche" and "eth" -> "ethereum" are classified correctly.
    _ARCHIVE_CHAINS_SLOW_FORK = frozenset({"polygon", "ethereum", "avalanche"})
    _GATEWAY_WARMUP_HEADROOM = 30.0  # server start + prewarm after all forks ready
    if anvil_chains:
        try:
            from almanak.core.constants import resolve_chain_name as _resolve_chain

            def _canonical(c: str) -> str:
                try:
                    return _resolve_chain(c)
                except ValueError:
                    return c.strip().lower()

        except ImportError:

            def _canonical(c: str) -> str:
                return c.strip().lower()

        anvil_fork_budget = sum(90.0 if _canonical(c) in _ARCHIVE_CHAINS_SLOW_FORK else 30.0 for c in anvil_chains)
        startup_timeout = anvil_fork_budget + _GATEWAY_WARMUP_HEADROOM
    else:
        startup_timeout = 10.0
    try:
        managed_gateway.start(timeout=startup_timeout)
    except RuntimeError as e:
        click.echo()
        click.secho(f"ERROR: Failed to start managed gateway: {e}", fg="red", bold=True)
        click.echo()
        raise click.ClickException("Managed gateway startup failed") from e

    # Register atexit handler as safety net for sys.exit() paths that skip cleanup
    atexit.register(managed_gateway.stop)

    click.secho(f"Managed gateway started on {effective_host}:{gateway_port}", fg="green")

    # Connect client to the managed gateway (use same session token)
    gateway_config = GatewayClientConfig(host=effective_host, port=gateway_port, auth_token=session_auth_token)
    gateway_client = GatewayClient(gateway_config)
    gateway_client.connect()

    click.echo("Waiting for gateway to become ready...")
    if not gateway_client.wait_for_ready(timeout=60.0, interval=5.0):
        managed_gateway.stop()
        gateway_client.disconnect()
        raise click.ClickException(
            "Managed gateway started but health check failed. Check gateway logs above for details."
        )

    return (
        gateway_client,
        managed_gateway,
        effective_host,
        gateway_port,
        gateway_network,
        session_auth_token,
        isolated_wallet_address,
        early_strategy_class,
    )


# ---------------------------------------------------------------------------
# Phase 14 — Cleanup helper
# ---------------------------------------------------------------------------


def _build_cleanup_fn(
    *,
    gateway_client: Any,
    managed_gateway: Any,
    keep_anvil: bool,
    components: ComponentBundle,
) -> Callable[[], Coroutine[Any, Any, None]]:
    """Build an async cleanup closure that tears down all run-time resources.

    Mirrors phase 14 of `run()`. Returns a zero-arg async function that the
    caller awaits in the single-run and loop finally blocks. Preserves the
    original None-checks and hasattr-close checks exactly so that callers
    with partial component initialization don't blow up during shutdown.

    Args:
        gateway_client: The connected `GatewayClient` (or None).
        managed_gateway: Optional `ManagedGateway` handle (None when
            `--no-gateway` was used).
        keep_anvil: Preserve the `--keep-anvil` behavior: if the managed
            gateway owns Anvil forks, print their ports/PIDs before
            ``managed_gateway.stop()``.
        components: `ComponentBundle` carrying `ohlcv_provider`, `price_oracle`
            and `solana_fork_mgr` (any may be None).

    Returns:
        A coroutine factory. Call as ``await cleanup_fn()``.
    """

    async def cleanup_resources() -> None:
        if components.ohlcv_provider is not None:
            await components.ohlcv_provider.close()
        if components.price_oracle is not None and hasattr(components.price_oracle, "close"):
            await components.price_oracle.close()
        if gateway_client is not None:
            gateway_client.disconnect()
        if components.solana_fork_mgr is not None:
            try:
                await components.solana_fork_mgr.stop()
                click.echo("  Stopped solana-test-validator")
            except Exception as e:
                logger.debug(f"Failed to stop solana-test-validator: {e}")
        if managed_gateway is not None:
            if keep_anvil and managed_gateway._anvil_managers:
                for chain, mgr in managed_gateway._anvil_managers.items():
                    port = mgr.anvil_port
                    pid = mgr._process.pid if mgr._process else "unknown"
                    click.echo(f"Anvil for {chain} still running on port {port} (PID {pid})")
            managed_gateway.stop()

    return cleanup_resources


# ---------------------------------------------------------------------------
# Phase 11 — Strategy instantiation
# ---------------------------------------------------------------------------


def _instantiate_strategy(
    *,
    strategy_class: type,
    strategy_config: dict[str, Any],
    runtime_config: Any,
    multi_chain: bool,
    strategy_chains: list[str],
    chain_wallets: dict[str, str],
) -> Any:
    """Instantiate the strategy class with the right config-type branch.

    Mirrors phase 11 of `run()`. Two call conventions are supported:

    1. ``IntentStrategy`` subclasses: discovered via ``issubclass`` check.
       The helper tries to resolve a dataclass config type from
       ``__orig_bases__``, converts numeric fields to ``Decimal`` as needed,
       wraps plain dicts in ``DictConfigWrapper``, then introspects
       ``__init__`` to filter optional kwargs (``chains`` / ``chain_wallets``)
       so older strategies without ``**kwargs`` don't TypeError.
    2. Other classes (``StrategyBase`` subclasses, test doubles): try the
       config-dict convention first, fall back to the no-arg constructor
       on TypeError.

    Exits the process with status 1 on any failure (preserving the original
    top-level except block's behavior — `click.echo(..., err=True)` +
    `sys.exit(1)`).

    Args:
        strategy_class: The loaded strategy class.
        strategy_config: Parsed strategy config dict (chain / wallet_address
            already injected).
        runtime_config: The runtime config object. Single-chain mode reads
            ``runtime_config.chain``; multi-chain reads the first entry of
            ``strategy_chains``.
        multi_chain: True when the strategy runs across multiple chains.
        strategy_chains: Chains the strategy is configured for. Only used
            when ``multi_chain`` is True.
        chain_wallets: Mapping of chain -> wallet address resolved from the
            gateway's WalletRegistry. Empty when unused.

    Returns:
        The constructed strategy instance.
    """
    from decimal import Decimal
    from typing import get_args, get_type_hints

    from .run import DictConfigWrapper

    IntentStrategyRuntime = _intent_strategy_runtime()

    try:
        if issubclass(strategy_class, IntentStrategyRuntime):
            # IntentStrategy requires specific parameters
            primary_chain = strategy_chains[0] if multi_chain else runtime_config.chain

            # Check if strategy has a config class (generic parameter)
            # Try to get the config type from __orig_bases__
            config_instance: Any = strategy_config
            try:
                bases = getattr(strategy_class, "__orig_bases__", [])
                for base in bases:
                    args = get_args(base)
                    if args and hasattr(args[0], "__dataclass_fields__"):
                        # Found dataclass config type - create instance with defaults
                        config_class = args[0]

                        # Convert numeric values to Decimal where needed
                        type_hints = get_type_hints(config_class)
                        converted_config: dict[str, Any] = {}
                        # Track fields that are NOT in the dataclass (excluding runtime + framework meta-keys)
                        runtime_fields = {"strategy_id", "chain", "wallet_address"}
                        # Meta-keys consumed by the CLI/framework, not by strategy config classes
                        framework_meta_keys = {"anvil_funding", "strategy_display_name"}
                        unknown_fields = []
                        for k, v in strategy_config.items():
                            if k in config_class.__dataclass_fields__:
                                field_type = type_hints.get(k)
                                # Convert int/float/str to Decimal for Decimal fields
                                if field_type == Decimal and isinstance(v, int | float | str):
                                    try:
                                        converted_config[k] = Decimal(str(v))
                                    except Exception:
                                        converted_config[k] = v
                                else:
                                    converted_config[k] = v
                            elif k not in runtime_fields and k not in framework_meta_keys:
                                unknown_fields.append(k)

                        # Use dataclass config, filtering out unknown fields
                        # (runtime fields like strategy_id/chain are handled separately)
                        if unknown_fields:
                            logger.debug(
                                f"Config class {config_class.__name__} ignoring unknown fields: {unknown_fields}"
                            )
                            click.echo(f"  Config class: {config_class.__name__} (ignored: {unknown_fields})")
                        else:
                            click.echo(f"  Config class: {config_class.__name__}")
                        config_instance = config_class(**converted_config) if converted_config else config_class()
                        break
            except Exception as e:
                logger.debug(f"Could not infer config class: {e}")
                # Fall back to using dict or default config
                pass

            # Wrap dict config in DictConfigWrapper for compatibility
            if isinstance(config_instance, dict):
                config_instance = DictConfigWrapper(config_instance)
                click.echo("  Config wrapped in DictConfigWrapper")

            # Resolve wallet for strategy construction
            strat_wallet = runtime_config.execution_address
            if chain_wallets:
                strat_wallet = chain_wallets.get(primary_chain, strat_wallet)

            # Build kwargs, then filter to only those the strategy __init__ accepts.
            # This prevents TypeError for user strategies that don't accept **kwargs
            # or newer framework params like chains/chain_wallets.
            # Base kwargs are always safe (IntentStrategy.__init__ requires them).
            base_kwargs: dict[str, Any] = {
                "config": config_instance,
                "chain": primary_chain,
                "wallet_address": strat_wallet,
            }
            # Optional kwargs only included when non-None (multi-chain mode).
            optional_kwargs: dict[str, Any] = {}
            if chain_wallets:
                optional_kwargs["chains"] = list(chain_wallets.keys())
                optional_kwargs["chain_wallets"] = chain_wallets
            init_kwargs = {**base_kwargs, **optional_kwargs}
            try:
                sig = inspect.signature(strategy_class.__init__)
                params = sig.parameters
                # If __init__ accepts **kwargs, pass everything
                has_var_keyword = any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values())
                if not has_var_keyword:
                    # Filter to only accepted parameter names
                    init_kwargs = {k: v for k, v in init_kwargs.items() if k in params}
            except (ValueError, TypeError) as exc:
                # Introspection failed — fall back to base kwargs only to avoid
                # injecting unexpected kwargs like 'chains' (VIB-1987).
                logger.debug("Strategy __init__ introspection failed, using base kwargs only: %s", exc)
                init_kwargs = base_kwargs

            strategy_instance = strategy_class(**init_kwargs)
        else:
            # Try dict config first, then no config
            cls_any: Any = strategy_class
            try:
                strategy_instance = cls_any(strategy_config)
            except TypeError:
                strategy_instance = cls_any()

        click.echo("Strategy instance created successfully")
        return strategy_instance

    except Exception as e:
        click.echo(f"Error creating strategy instance: {e}", err=True)
        sys.exit(1)


def _intent_strategy_runtime() -> type:
    """Deferred import of `IntentStrategy` to avoid circular-import risk.

    `run_helpers` is imported by `run`, so we can't eagerly import
    `..strategies.IntentStrategy` at module load (strategies pulls gateway
    modules that transitively hit `.run`). Doing the import at call time
    keeps the dependency one-way.
    """
    from ..strategies import IntentStrategy as _IntentStrategy

    return _IntentStrategy


# ---------------------------------------------------------------------------
# Phase 12 — Runtime config wiring
# ---------------------------------------------------------------------------


def _build_runtime_config(
    *,
    no_gateway: bool,
    multi_chain: bool,
    resolved_network: str,
    config_chain: str | None,
    strategy_chains: list[str],
    strategy_protocols: Any,
    gateway_client: Any,
    strategy_config: dict[str, Any],
) -> tuple[Any, dict[str, str]]:
    """Build the runtime config (Local / MultiChain / Gateway) and register chains.

    Mirrors phase 12 of `run()`. Dispatches between three flavours:

    1. **Sidecar deployment** (``--no-gateway`` without ``ALMANAK_PRIVATE_KEY``,
       single-chain only): build a `GatewayRuntimeConfig` pinned to
       ``config_chain`` + resolved wallet address. Multi-chain sidecar is
       handled by the ``elif multi_chain`` branch below.
    2. **Multi-chain**: ``MultiChainRuntimeConfig.from_env`` with Anvil
       fallback on `MissingEnvironmentVariableError` + PRIVATE_KEY.
    3. **Single-chain default**: ``LocalRuntimeConfig.from_env`` with the
       same Anvil fallback.

    After config construction, runs Safe-mode preflight validation
    (skipped in sidecar mode and when `ALMANAK_GATEWAY_WALLETS` is set) and
    calls ``gateway_client.register_chains`` to pre-warm the gateway's
    WalletRegistry. The function mutates ``strategy_config`` in place to
    inject ``chain`` and ``wallet_address`` keys.

    Returns:
        ``(runtime_config, chain_wallets)`` — the constructed runtime config
        and the resolved chain->wallet map (empty dict when
        ALMANAK_GATEWAY_WALLETS is not set).
    """
    from ..execution.config import (
        GatewayRuntimeConfig,
        LocalRuntimeConfig,
        MissingEnvironmentVariableError,
        MultiChainRuntimeConfig,
    )
    from ..execution.gas.constants import CHAIN_GAS_PRICE_CAPS_GWEI, DEFAULT_GAS_PRICE_CAP_GWEI
    from .run import ANVIL_DEFAULT_ADDRESS, ANVIL_DEFAULT_PRIVATE_KEY, _validate_safe_mode_preflight

    runtime_config: Any

    # Sidecar deployment mode: --no-gateway without a local private key.
    # The gateway handles all signing and RPC; we only need chain + wallet address.
    # Multi-chain sidecar is handled by the `elif multi_chain` branch below,
    # which supports ALMANAK_GATEWAY_WALLETS for per-chain wallet resolution.
    if no_gateway and not os.environ.get("ALMANAK_PRIVATE_KEY") and not multi_chain:
        resolved_chain = config_chain or None
        if not resolved_chain:
            raise click.ClickException(
                "Chain must be specified in config.json or strategy decorator for sidecar deployment mode."
            )

        safe_address = os.environ.get("ALMANAK_SAFE_ADDRESS")
        wallet_address = safe_address or os.environ.get("ALMANAK_EOA_ADDRESS")
        if not wallet_address and not os.environ.get("ALMANAK_GATEWAY_WALLETS"):
            raise click.ClickException(
                "Sidecar mode (--no-gateway without ALMANAK_PRIVATE_KEY) requires "
                "ALMANAK_SAFE_ADDRESS, ALMANAK_EOA_ADDRESS, or ALMANAK_GATEWAY_WALLETS to be set."
            )
        # When ALMANAK_GATEWAY_WALLETS is set, wallet_address is resolved later
        # by register_chains() from the gateway's WalletRegistry.
        wallet_address = wallet_address or ""

        default_gas_cap = CHAIN_GAS_PRICE_CAPS_GWEI.get(resolved_chain, DEFAULT_GAS_PRICE_CAP_GWEI)
        runtime_config = GatewayRuntimeConfig(
            chain=resolved_chain,
            wallet_address=wallet_address,
            is_safe=bool(safe_address),
            max_gas_price_gwei=default_gas_cap,
        )
        click.echo(f"Sidecar deployment mode: chain={resolved_chain}, wallet={wallet_address}")

    elif multi_chain:
        try:
            runtime_config = MultiChainRuntimeConfig.from_env(
                chains=strategy_chains,
                protocols=strategy_protocols,
                network=resolved_network,
            )
            click.echo(f"Multi-chain config loaded for: {', '.join(strategy_chains)}")
        except MissingEnvironmentVariableError as e:
            if resolved_network == "anvil" and e.var_name.endswith("PRIVATE_KEY"):
                click.echo(f"No ALMANAK_PRIVATE_KEY set. Using default Anvil wallet: {ANVIL_DEFAULT_ADDRESS}")
                if sys.stdin.isatty():
                    if not click.confirm("Continue with this wallet?", default=True):
                        sys.exit(0)
                else:
                    click.echo("(non-interactive, accepting default Anvil wallet)")
                os.environ["ALMANAK_PRIVATE_KEY"] = ANVIL_DEFAULT_PRIVATE_KEY
                try:
                    runtime_config = MultiChainRuntimeConfig.from_env(
                        chains=strategy_chains,
                        protocols=strategy_protocols,
                        network=resolved_network,
                    )
                except Exception as retry_err:
                    click.echo(f"Error loading configuration after setting default key: {retry_err}", err=True)
                    sys.exit(1)
                click.echo(f"Multi-chain config loaded for: {', '.join(strategy_chains)}")
            elif no_gateway and e.var_name.endswith("PRIVATE_KEY"):
                click.echo(
                    "Error: Multi-chain sidecar mode requires ALMANAK_GATEWAY_WALLETS or ALMANAK_PRIVATE_KEY.",
                    err=True,
                )
                click.echo(
                    "Set ALMANAK_GATEWAY_WALLETS with per-chain wallet config, or provide ALMANAK_PRIVATE_KEY.",
                    err=True,
                )
                sys.exit(1)
            else:
                if e.var_name.endswith("PRIVATE_KEY"):
                    click.echo("Error: ALMANAK_PRIVATE_KEY is required for mainnet execution.", err=True)
                    click.echo("Set it in your .env file or environment.", err=True)
                else:
                    click.echo(f"Error loading multi-chain configuration: {e}", err=True)
                    click.echo()
                    click.echo("Required environment variables for multi-chain:")
                    click.echo("  ALMANAK_PRIVATE_KEY          - Wallet private key")
                    click.echo()
                    click.echo("RPC access (one of these, or leave empty for free public RPCs):")
                    for chain in strategy_chains:
                        click.echo(f"  ALMANAK_{chain.upper()}_RPC_URL  - Per-chain RPC URL")
                    click.echo("  RPC_URL                      - Generic RPC endpoint URL")
                    click.echo("  ALCHEMY_API_KEY              - Alchemy API key (fallback)")
                sys.exit(1)
        except Exception as e:
            click.echo(f"Error loading multi-chain configuration: {e}", err=True)
            click.echo()
            click.echo("Required environment variables for multi-chain:")
            click.echo("  ALMANAK_PRIVATE_KEY          - Wallet private key")
            click.echo()
            click.echo("RPC access (one of these, or leave empty for free public RPCs):")
            for chain in strategy_chains:
                click.echo(f"  ALMANAK_{chain.upper()}_RPC_URL  - Per-chain RPC URL")
            click.echo("  RPC_URL                      - Generic RPC endpoint URL")
            click.echo("  ALCHEMY_API_KEY              - Alchemy API key (fallback)")
            sys.exit(1)
    else:
        try:
            # Pass chain and network from strategy config for dynamic RPC URL building
            runtime_config = LocalRuntimeConfig.from_env(chain=config_chain, network=resolved_network)
        except MissingEnvironmentVariableError as e:
            if resolved_network == "anvil" and e.var_name.endswith("PRIVATE_KEY"):
                click.echo(f"No ALMANAK_PRIVATE_KEY set. Using default Anvil wallet: {ANVIL_DEFAULT_ADDRESS}")
                if sys.stdin.isatty():
                    if not click.confirm("Continue with this wallet?", default=True):
                        sys.exit(0)
                else:
                    click.echo("(non-interactive, accepting default Anvil wallet)")
                os.environ["ALMANAK_PRIVATE_KEY"] = ANVIL_DEFAULT_PRIVATE_KEY
                try:
                    runtime_config = LocalRuntimeConfig.from_env(chain=config_chain, network=resolved_network)
                except Exception as retry_err:
                    click.echo(f"Error loading configuration after setting default key: {retry_err}", err=True)
                    sys.exit(1)
            else:
                if e.var_name.endswith("PRIVATE_KEY"):
                    click.echo("Error: ALMANAK_PRIVATE_KEY is required for mainnet execution.", err=True)
                    click.echo("Set it in your .env file or environment.", err=True)
                else:
                    click.echo(f"Error loading configuration: {e}", err=True)
                    click.echo()
                    click.echo("Required environment variables:")
                    click.echo("  ALMANAK_PRIVATE_KEY          - Wallet private key")
                    click.echo()
                    click.echo("RPC access (one of these, or leave empty for free public RPCs):")
                    click.echo("  ALMANAK_ARBITRUM_RPC_URL     - Per-chain RPC URL (highest priority)")
                    click.echo("  ALMANAK_RPC_URL              - Generic RPC endpoint URL")
                    click.echo("  RPC_URL                      - Generic RPC endpoint URL")
                    click.echo("  ALCHEMY_API_KEY              - Alchemy API key (fallback)")
                    click.echo()
                    click.echo("Optional environment variables:")
                    click.echo("  ALMANAK_MAX_GAS_PRICE_GWEI - Max gas price (default: chain-specific; Anvil: 9999)")
                    click.echo("  ALMANAK_TX_TIMEOUT_SECONDS - Tx timeout (default: 120)")
                    click.echo("  ALMANAK_SIMULATION_ENABLED - Enable simulation (default: false)")
                sys.exit(1)
        except Exception as e:
            click.echo(f"Error loading configuration: {e}", err=True)
            click.echo()
            click.echo("Required environment variables:")
            click.echo("  ALMANAK_PRIVATE_KEY          - Wallet private key")
            click.echo()
            click.echo("RPC access (one of these, or leave empty for free public RPCs):")
            click.echo("  ALMANAK_ARBITRUM_RPC_URL     - Per-chain RPC URL (highest priority)")
            click.echo("  ALMANAK_RPC_URL              - Generic RPC endpoint URL")
            click.echo("  RPC_URL                      - Generic RPC endpoint URL")
            click.echo("  ALCHEMY_API_KEY              - Alchemy API key (fallback)")
            click.echo()
            click.echo("Optional environment variables:")
            click.echo("  ALMANAK_MAX_GAS_PRICE_GWEI - Max gas price (default: chain-specific; Anvil: 9999)")
            click.echo("  ALMANAK_TX_TIMEOUT_SECONDS - Tx timeout (default: 120)")
            click.echo("  ALMANAK_SIMULATION_ENABLED - Enable simulation (default: false)")
            sys.exit(1)

    # Preflight checks for Safe mode consistency between framework and gateway
    # Only check when the CLI manages the gateway (env vars are local).
    # With --no-gateway the env vars may live on a remote host.
    # Skip when ALMANAK_GATEWAY_WALLETS is set — the gateway's WalletRegistry
    # handles wallet/signer configuration per chain, not local env vars.
    gateway_wallets_configured = bool(os.environ.get("ALMANAK_GATEWAY_WALLETS"))
    if runtime_config.is_safe_mode and not no_gateway and not gateway_wallets_configured:
        error = _validate_safe_mode_preflight(runtime_config.execution_address)
        if error:
            click.secho(f"ERROR: {error}", fg="red", err=True)
            sys.exit(1)

    # ---- Register chains with gateway to resolve per-chain wallet addresses ----
    # When ALMANAK_GATEWAY_WALLETS is set, the gateway's WalletRegistry provides
    # per-chain wallet addresses. Call register_chains() to pre-warm orchestrators
    # and get the resolved wallet map.
    chain_wallets: dict[str, str] = {}
    if gateway_wallets_configured:
        try:
            register_chain_list = strategy_chains if multi_chain else [str(config_chain)]
            chain_wallets = gateway_client.register_chains(register_chain_list)

            if chain_wallets:
                primary_chain = register_chain_list[0]
                primary_wallet = chain_wallets.get(primary_chain, "")

                # Update runtime_config with resolved wallet address
                runtime_config.wallet_address = primary_wallet

                unique_addrs = {v.lower() for v in chain_wallets.values()}
                is_uniform = len(unique_addrs) <= 1

                if is_uniform:
                    click.echo(
                        f"Gateway wallet registry: uniform wallet {primary_wallet[:12]}... on {len(chain_wallets)} chain(s)"
                    )
                else:
                    click.echo("Gateway wallet registry: non-uniform wallets")
                    for ch, addr in chain_wallets.items():
                        click.echo(f"  {ch}: {addr}")
        except Exception as e:
            click.secho(f"WARNING: register_chains() failed: {e}", fg="yellow", err=True)
            click.echo("Falling back to legacy wallet resolution.", err=True)
            logger.warning("register_chains() failed: %s", e)

    # Ensure chain and wallet_address are set in strategy config.
    # When ALMANAK_CHAIN env override is in play we also rewrite a pre-existing
    # config.json chain so the strategy class itself sees the override (its
    # MarketSnapshot, balance lookups, and on-chain queries all key off
    # strategy_config["chain"], not runtime_config alone).
    env_chain = (os.environ.get("ALMANAK_CHAIN") or "").strip().lower() or None
    if "chain" not in strategy_config:
        if multi_chain:
            strategy_config["chain"] = strategy_chains[0]
        else:
            assert isinstance(runtime_config, LocalRuntimeConfig | GatewayRuntimeConfig)
            strategy_config["chain"] = runtime_config.chain
    elif env_chain and not multi_chain:
        _existing = strategy_config.get("chain")
        _existing_norm = _existing.strip().lower() if isinstance(_existing, str) else ""
        if _existing_norm != env_chain:
            strategy_config["chain"] = env_chain
    # Runtime-resolved wallet wins (see #1684). A stale `wallet_address` left in
    # config.json must not drive deployment_id when the runtime signs from a
    # different wallet -- state would attach to the wrong identity.
    if chain_wallets:
        primary = strategy_chains[0] if multi_chain else str(config_chain)
        resolved_wallet = chain_wallets.get(primary, runtime_config.execution_address)
    else:
        resolved_wallet = runtime_config.execution_address
    if resolved_wallet:
        strategy_config["wallet_address"] = resolved_wallet

    return runtime_config, chain_wallets


# ---------------------------------------------------------------------------
# Phase 13 — Component initialization
# ---------------------------------------------------------------------------


def _get_data_requirements(strategy_instance: Any) -> StrategyDataRequirements:
    """Return the strategy's declared data requirements, falling back to legacy compat.

    Strategies decorated with @almanak_strategy that omit data_requirements get
    LEGACY_COMPAT_DATA_REQUIREMENTS (all services wired eagerly) to preserve
    pre-VIB-3392 behavior. Strategies without a STRATEGY_METADATA attribute
    (no decorator, test stubs) also fall back to legacy compat.
    """
    metadata = getattr(strategy_instance, "STRATEGY_METADATA", None)
    if metadata is None:
        return LEGACY_COMPAT_DATA_REQUIREMENTS
    dr = getattr(metadata, "data_requirements", None)
    if dr is None:
        return LEGACY_COMPAT_DATA_REQUIREMENTS
    return dr


def _build_orchestrator_and_providers(
    *,
    multi_chain: bool,
    runtime_config: Any,
    strategy_chains: list[str],
    strategy_config: dict[str, Any],
    resolved_network: str,
    gateway_client: Any,
    chain_wallets: dict[str, str],
    strategy_instance: Any,
    components: ComponentBundle,
) -> None:
    """Build orchestrator, price/balance/OHLCV providers, and wire indicators.

    Populates ``components.execution_orchestrator``, ``components.price_oracle``,
    ``components.balance_provider``, ``components.ohlcv_provider``, and
    ``components.solana_fork_mgr`` (Solana + Anvil only). Dispatches between
    multi-chain and single-chain paths preserving the exact click.echo output
    and ordering of the inlined code.
    """
    from decimal import Decimal

    from almanak.gateway.data.balance import Web3BalanceProvider  # noqa: F401 — historical import site
    from almanak.gateway.data.price import CoinGeckoPriceSource, PriceAggregator  # noqa: F401

    from ..data.balance.gateway_provider import GatewayBalanceProvider
    from ..data.price.gateway_oracle import GatewayPriceOracle
    from ..execution.multichain import MultiChainOrchestrator
    from .run import (
        _get_orca_pool_accounts,
        _init_prediction_provider,
        _wire_core_providers,
        _wire_indicators,
        create_routing_ohlcv_provider,
    )

    requirements = _get_data_requirements(strategy_instance)
    ohlcv_provider: Any = None

    execution_orchestrator: Any
    if multi_chain:
        from ..data.balance.gateway_multichain import MultiChainGatewayBalanceProvider
        from ..execution.gateway_orchestrator import GatewayExecutionOrchestrator  # noqa: F401

        # Resolve effective wallet address (from chain_wallets or runtime_config)
        effective_wallet = runtime_config.execution_address
        if chain_wallets:
            effective_wallet = chain_wallets.get(strategy_chains[0], effective_wallet)

        if not effective_wallet:
            raise click.ClickException(
                "No wallet address resolved for multi-chain execution. "
                "Ensure ALMANAK_GATEWAY_WALLETS is configured correctly and the gateway is reachable."
            )

        click.echo("  Using gateway-backed providers for multi-chain...")
        price_oracle = GatewayPriceOracle(gateway_client, default_chain=strategy_chains[0])
        balance_provider = GatewayBalanceProvider(
            client=gateway_client,
            wallet_address=effective_wallet,
            chain=strategy_chains[0],
        )
        execution_orchestrator = MultiChainOrchestrator.from_gateway(
            gateway_client=gateway_client,
            chains=strategy_chains,
            wallet_address=effective_wallet,
            max_gas_price_gwei=runtime_config.max_gas_price_gwei,
            chain_wallets=chain_wallets or None,
        )

        # Create multi-chain balance provider for the strategy
        multi_chain_balance_provider = MultiChainGatewayBalanceProvider(
            client=gateway_client,
            wallet_address=effective_wallet,
            chains=strategy_chains,
            chain_wallets=chain_wallets or None,
        )

        # Set multi-chain providers on strategy if it's an IntentStrategy
        if hasattr(strategy_instance, "set_multi_chain_providers"):
            strategy_instance.set_multi_chain_providers(
                balance_provider=multi_chain_balance_provider,
            )
            click.echo("  Multi-chain providers set on strategy")

        if requirements.indicators:
            # NOTE: In multi-chain mode, OHLCV routing is bound to the first chain.
            # For CEX-listed tokens this is fine (Binance data is chain-agnostic).
            # For DeFi-native tokens on secondary chains, GeckoTerminal pool search
            # may resolve to the wrong network. Per-chain providers would require
            # passing chain context through the indicator callables, which is a larger change.
            ohlcv_provider = create_routing_ohlcv_provider(
                gateway_client=gateway_client,
                chain=strategy_chains[0],
                strategy_config=strategy_config,
            )
            _wire_indicators(strategy_instance, ohlcv_provider, price_oracle, balance_provider)
        elif requirements.price or requirements.balance:
            # indicators=False: wire price/balance directly without OHLCV or indicator calculators
            _wire_core_providers(strategy_instance, price_oracle, balance_provider)

        rate_monitor_wired = False
        if requirements.lending_rates:
            try:
                from ..data.rates import RateMonitor

                primary_chain = strategy_chains[0]
                chain_rpc_url = runtime_config.rpc_urls.get(primary_chain)
                rate_monitor = RateMonitor(chain=primary_chain, rpc_url=chain_rpc_url)
                strategy_instance._rate_monitor = rate_monitor
                rate_monitor_wired = True
            except Exception as e:
                logger.debug(f"Rate monitor not available: {e}")

        funding_wired = False
        if requirements.funding_rates:
            try:
                from ..data.funding import GatewayFundingRateProvider

                primary_chain = strategy_chains[0]
                funding_provider = GatewayFundingRateProvider(gateway_client=gateway_client, chain=primary_chain)
                strategy_instance._funding_rate_provider = funding_provider
                funding_wired = True
            except (ImportError, ValueError, RuntimeError) as e:
                logger.warning(
                    "Funding rate provider init failed for chain=%s: %s",
                    strategy_chains[0],
                    e,
                    exc_info=True,
                )

        _wired = []
        if getattr(strategy_instance, "_price_oracle", None) is not None:
            _wired.append("price")
        if getattr(strategy_instance, "_balance_provider", None) is not None:
            _wired.append("balance")
        if getattr(strategy_instance, "_indicator_provider", None) is not None:
            _wired.append("indicators")
        if rate_monitor_wired:
            _wired.append("lending_rates")
        if funding_wired:
            _wired.append("funding_rates")
        click.echo(f"  Injected strategy data services: {', '.join(_wired)}")
        click.echo(f"  Multi-chain orchestrator created for {len(strategy_chains)} chains")
    else:
        # Single-chain setup - always use gateway-backed providers
        from ..execution.config import GatewayRuntimeConfig, LocalRuntimeConfig
        from ..execution.gateway_orchestrator import GatewayExecutionOrchestrator

        assert isinstance(runtime_config, LocalRuntimeConfig | GatewayRuntimeConfig)

        # Resolve effective wallet address (from chain_wallets or runtime_config)
        sc_effective_wallet = runtime_config.execution_address
        if chain_wallets:
            sc_effective_wallet = chain_wallets.get(runtime_config.chain, sc_effective_wallet)

        click.echo("  Using gateway-backed providers...")
        price_oracle = GatewayPriceOracle(gateway_client, default_chain=runtime_config.chain)
        balance_provider = GatewayBalanceProvider(
            client=gateway_client,
            wallet_address=sc_effective_wallet,
            chain=runtime_config.chain,
        )

        # For Solana + --network anvil, start local solana-test-validator
        if runtime_config.chain.lower() == "solana" and resolved_network == "anvil":
            from ..anvil.solana_fork_manager import SolanaForkManager

            solana_rpc_url = os.environ.get("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com")
            # Clone any pool/account addresses declared in the strategy config
            _extra_clone = []
            if strategy_config and isinstance(strategy_config, dict):
                for _key in ("pool_address", "pool_a_address", "pool_b_address"):
                    _addr = strategy_config.get(_key)
                    if _addr and isinstance(_addr, str):
                        _extra_clone.append(_addr)
                # For Orca Whirlpool strategies, also pre-clone vault + tick array accounts
                _orca_accounts = _get_orca_pool_accounts(strategy_config)
                if _orca_accounts:
                    click.echo(f"  Pre-cloning {len(_orca_accounts)} Orca pool accounts (vaults + tick arrays)")
                    _extra_clone.extend(_orca_accounts)
            if _extra_clone:
                click.echo(f"  Cloning {len(_extra_clone)} account(s) from mainnet")
            solana_fork_mgr = SolanaForkManager(
                rpc_url=solana_rpc_url,
                validator_port=int(os.environ.get("SOLANA_VALIDATOR_PORT", "8899")),
                clone_accounts=_extra_clone,
            )
            click.echo("  Starting local solana-test-validator...")
            import asyncio as _aio

            started = _aio.get_event_loop().run_until_complete(solana_fork_mgr.start())
            if not started:
                raise click.ClickException(
                    "Failed to start solana-test-validator. "
                    "Ensure Solana CLI tools are installed: "
                    'sh -c "$(curl -sSfL https://release.anza.xyz/stable/install)"'
                )
            click.echo(f"  solana-test-validator running at {solana_fork_mgr.get_rpc_url()}")

            # Fund the wallet
            _aio.get_event_loop().run_until_complete(
                solana_fork_mgr.fund_wallet(runtime_config.wallet_address, Decimal("100"))
            )
            _aio.get_event_loop().run_until_complete(
                solana_fork_mgr.fund_tokens(
                    runtime_config.wallet_address,
                    {"USDC": Decimal("10000"), "USDT": Decimal("10000")},
                )
            )
            click.echo("  Wallet funded with 100 SOL + 10K USDC + 10K USDT")
            components.solana_fork_mgr = solana_fork_mgr

        # All chains (including Solana) use GatewayExecutionOrchestrator
        execution_orchestrator = GatewayExecutionOrchestrator(
            client=gateway_client,
            chain=runtime_config.chain,
            wallet_address=sc_effective_wallet,
            max_gas_price_gwei=runtime_config.max_gas_price_gwei,
        )
        click.echo("  Gateway-backed providers created")

        if requirements.indicators:
            # Create indicator calculators using routed OHLCV provider (CEX + DEX fallback)
            ohlcv_provider = create_routing_ohlcv_provider(
                gateway_client=gateway_client,
                chain=runtime_config.chain,
                strategy_config=strategy_config,
            )
            _wire_indicators(strategy_instance, ohlcv_provider, price_oracle, balance_provider)
        elif requirements.price or requirements.balance:
            # indicators=False: wire price/balance directly without OHLCV or indicator calculators
            _wire_core_providers(strategy_instance, price_oracle, balance_provider)

        # Initialize prediction market provider for strategies that explicitly
        # declare polymarket support. Non-polymarket strategies skip this
        # entirely (including Polygon runs) to avoid irrelevant warnings.
        if hasattr(strategy_instance, "_prediction_provider"):
            _init_prediction_provider(strategy_instance, chain=runtime_config.chain, gateway_client=gateway_client)

        rate_monitor_wired = False
        if requirements.lending_rates:
            try:
                from ..data.rates import RateMonitor

                rpc_url = getattr(runtime_config, "rpc_url", None)
                rate_monitor = RateMonitor(chain=runtime_config.chain, rpc_url=rpc_url)
                strategy_instance._rate_monitor = rate_monitor
                rate_monitor_wired = True
            except Exception as e:
                logger.debug(f"Rate monitor not available: {e}")

        funding_wired = False
        if requirements.funding_rates:
            try:
                from ..data.funding import GatewayFundingRateProvider

                funding_provider = GatewayFundingRateProvider(gateway_client=gateway_client, chain=runtime_config.chain)
                strategy_instance._funding_rate_provider = funding_provider
                funding_wired = True
            except (ImportError, ValueError, RuntimeError) as e:
                logger.warning(
                    "Funding rate provider init failed for chain=%s: %s",
                    runtime_config.chain,
                    e,
                    exc_info=True,
                )

        _wired = []
        if getattr(strategy_instance, "_price_oracle", None) is not None:
            _wired.append("price")
        if getattr(strategy_instance, "_balance_provider", None) is not None:
            _wired.append("balance")
        if getattr(strategy_instance, "_indicator_provider", None) is not None:
            _wired.append("indicators")
        if rate_monitor_wired:
            _wired.append("lending_rates")
        if funding_wired:
            _wired.append("funding_rates")
        click.echo(f"  Injected strategy data services: {', '.join(_wired)}")

    components.execution_orchestrator = execution_orchestrator
    components.price_oracle = price_oracle
    components.balance_provider = balance_provider
    components.ohlcv_provider = ohlcv_provider


def _init_copy_trading(
    *,
    strategy_instance: Any,
    strategy_config: dict[str, Any],
    runtime_config: Any,
    gateway_client: Any,
    price_oracle: Any,
    normalized_copy_mode: str | None,
    copy_replay_file: str | None,
    copy_shadow: bool,
    copy_strict: bool,
    multi_chain: bool,
) -> None:
    """Wire copy-trading components onto the strategy (v1 or v2 config).

    Preserves the inlined block's attribute-injection order and branch
    ordering (v1-only vs v2 coexist: v1 attributes are set first, then v2
    overrides them when valid). Fails fast with ClickException on invalid
    config in strict mode; logs a warning otherwise.

    Runs only in single-chain mode (mirroring the original code placement).
    """
    if not strategy_config.get("copy_trading"):
        return
    if multi_chain:
        # Defense-in-depth guard. `_build_components` pre-validates this same
        # combination BEFORE building any providers, so direct callers of
        # `_init_copy_trading` still get a clear failure instead of a silent
        # skip (see #1683).
        raise click.ClickException(
            "copy_trading is not yet supported for multi-chain strategies. "
            "Remove the copy_trading block or configure the strategy as single-chain."
        )

    from decimal import Decimal

    from ..connectors.contract_registry import get_default_registry
    from ..data.wallet_activity import WalletActivityProvider
    from ..services.copy_circuit_breaker import CopyCircuitBreaker
    from ..services.copy_intent_builder import CopyIntentBuilder
    from ..services.copy_ledger import CopyLedger
    from ..services.copy_policy_engine import CopyPolicyEngine
    from ..services.copy_signal_engine import CopySignalEngine
    from ..services.copy_trading_models import (
        CopyTradingConfig,
        CopyTradingConfigError,
        CopyTradingConfigV2,
    )
    from ..services.wallet_monitor import WalletMonitor, WalletMonitorConfig
    from ..testing.copy_replay import CopyReplayRunner
    from .run import create_sync_price_oracle_func

    ct_raw = strategy_config["copy_trading"]
    click.echo("  Copy trading config detected, initializing components...")

    if not isinstance(ct_raw, dict):
        raise click.ClickException("copy_trading config must be an object")

    ct_config = CopyTradingConfig.from_config(ct_raw)
    ct_v2: CopyTradingConfigV2 | None = None
    strict_requested = bool(copy_strict or ct_raw.get("strict") or ct_raw.get("execution_policy", {}).get("strict"))
    try:
        ct_v2 = CopyTradingConfigV2.from_config(ct_raw)
    except CopyTradingConfigError as e:
        if strict_requested:
            raise click.ClickException(f"Invalid copy_trading config in strict mode: {e}") from e
        click.echo("  Warning: copy_trading strict schema validation failed, using legacy-compatible mode")

    registry = get_default_registry()

    # Group leaders by chain for multi-chain monitoring
    leaders_by_chain: dict[str, list[str]] = {}
    if ct_v2 is not None:
        for leader in ct_v2.leaders:
            leader_chain = (leader.chain or runtime_config.chain).lower()
            leaders_by_chain.setdefault(leader_chain, []).append(leader.address)
    else:
        for leader_dict in ct_config.leaders:
            leader_chain = str(leader_dict.get("chain", runtime_config.chain)).lower()
            leaders_by_chain.setdefault(leader_chain, []).append(leader_dict["address"])

    # Create one WalletMonitor per unique chain
    wallet_monitors: dict[str, WalletMonitor] = {}
    for chain, addresses in leaders_by_chain.items():
        monitor_config = WalletMonitorConfig(
            leader_addresses=addresses,
            chain=chain,
            poll_interval_seconds=(
                ct_v2.monitoring.poll_interval_seconds
                if ct_v2 is not None
                else ct_config.monitoring.get("poll_interval_seconds", 12)
            ),
            lookback_blocks=(
                ct_v2.monitoring.lookback_blocks
                if ct_v2 is not None
                else ct_config.monitoring.get("lookback_blocks", 50)
            ),
            confirmation_depth=(
                ct_v2.monitoring.confirmation_depth
                if ct_v2 is not None
                else ct_config.monitoring.get("confirmation_depth", 1)
            ),
        )
        wallet_monitors[chain] = WalletMonitor(config=monitor_config, gateway_client=gateway_client)

    # Create price function for signal engine from gateway price oracle
    copy_price_fn = None
    if price_oracle is not None:
        sync_price = create_sync_price_oracle_func(price_oracle)

        def _copy_price_fn(symbol: str, chain: str) -> Decimal | None:
            try:
                return sync_price(symbol, "USD", chain)
            except Exception:
                return None

        copy_price_fn = _copy_price_fn

    engine = CopySignalEngine(
        registry=registry,
        max_age_seconds=(
            ct_v2.monitoring.max_signal_age_seconds
            if ct_v2 is not None
            else ct_config.monitoring.get("max_signal_age_seconds", 300)
        ),
        price_fn=copy_price_fn,
        strict_token_resolution=strict_requested,
    )

    activity_provider = WalletActivityProvider(
        signal_engine=engine,
        wallet_monitors=wallet_monitors,
    )

    # Runtime-inject copy trading attributes (not declared on IntentStrategy)
    strat_any: Any = strategy_instance
    strat_any._wallet_activity_provider = activity_provider
    strat_any._copy_mode = normalized_copy_mode
    strat_any._copy_replay_file = copy_replay_file
    strat_any._copy_strict = strict_requested

    if ct_v2 is not None:
        strat_any._copy_config_v2 = ct_v2
        strat_any._copy_policy_engine = CopyPolicyEngine(
            config=ct_v2,
            reference_price_fn=copy_price_fn,
        )
        strat_any._copy_intent_builder = CopyIntentBuilder(config=ct_v2)
        strat_any._copy_circuit_breaker = CopyCircuitBreaker.from_copy_config(ct_v2)
        ledger_db_path = ct_raw.get("ledger", {}).get("db_path") if isinstance(ct_raw.get("ledger"), dict) else None
        strat_any._copy_ledger = CopyLedger(ledger_db_path or "./almanak_copy_ledger.db")

        exec_policy = ct_v2.execution_policy
        copy_mode_resolved = str(exec_policy.copy_mode)
        replay_path = exec_policy.replay_file
        if normalized_copy_mode is not None:
            copy_mode_resolved = normalized_copy_mode
        if copy_replay_file:
            replay_path = copy_replay_file
            copy_mode_resolved = "replay"
        if copy_shadow:
            copy_mode_resolved = "shadow"

        strat_any._copy_mode = copy_mode_resolved
        strat_any._copy_replay_file = replay_path

        if copy_mode_resolved == "replay" and replay_path:
            replay_runner = CopyReplayRunner(config=ct_v2)
            replay_signals = replay_runner.load_signals(replay_path)
            activity_provider.inject_signals(replay_signals)
            click.echo(f"  Copy replay loaded: {len(replay_signals)} signal(s) from {replay_path}")

    chains_str = ", ".join(sorted(leaders_by_chain.keys()))
    click.echo(f"  Copy trading initialized: monitoring {len(ct_config.leaders)} leader(s) on {chains_str}")


def _maybe_auto_deploy_vault(
    *,
    strategy_config: dict[str, Any],
    resolved_network: str,
    effective_dry_run: bool,
    config_chain: str | None,
    runtime_config: Any,
    gateway_client: Any,
    execution_orchestrator: Any,
    state_manager: Any,
    strategy_instance: Any,
    strategy_id: str,
) -> Any:
    """Return a VaultLifecycleManager or None, auto-deploying on Anvil if placeholder.

    Mirrors the vault-lifecycle block in phase 13. When
    ``strategy_config["vault"]`` is absent, returns None. Otherwise loads the
    Lagoon adapters, auto-deploys on Anvil if the vault_address is a
    placeholder (exiting with 0 under ``--dry-run``), and wires the vault's
    persistence callback so state changes flow back through the strategy's
    state manager.

    CRITICAL: This helper MUST run BEFORE the `StrategyRunner` is constructed,
    so the runner picks up the patched ``vault_address`` on initialization.
    """
    if not strategy_config.get("vault"):
        return None

    from ..connectors.lagoon import LagoonVaultAdapter, LagoonVaultSDK
    from ..vault.config import VaultConfig
    from ..vault.lifecycle import VAULT_STATE_KEY, VaultLifecycleManager
    from .run import _auto_deploy_lagoon_vault, _has_placeholder_vault_address

    vault_raw = strategy_config["vault"]

    # Auto-deploy Lagoon vault on Anvil if placeholder address detected
    if resolved_network == "anvil" and _has_placeholder_vault_address(vault_raw):
        if effective_dry_run:
            click.secho(
                "  [DRY-RUN] Vault has placeholder address -- skipping auto-deploy",
                fg="yellow",
            )
            click.echo("  Deploy manually or run without --dry-run on Anvil")
            # Bubble up to `run()` so cleanup_fn runs before exit-0 (see #1682).
            raise _DryRunVaultEarlyExit()

        click.echo("  Placeholder vault address detected -- auto-deploying on Anvil...")
        vault_raw = _auto_deploy_lagoon_vault(
            vault_raw,
            strategy_config.get("chain") or config_chain or "ethereum",
            runtime_config,
            gateway_client,
            execution_orchestrator,
        )

    vault_config = VaultConfig(**vault_raw)
    vault_chain = strategy_config.get("chain", "")
    vault_sdk = LagoonVaultSDK(gateway_client, chain=vault_chain)
    vault_adapter = LagoonVaultAdapter(vault_sdk)

    # Extract initial vault state from persisted strategy state.
    # State loading is deferred to the async phase for IntentStrategy, so we
    # load the raw state here directly from the state manager (safe to use
    # asyncio.run() because we are still in the sync Click command, before any
    # event loop is started).
    initial_vault_state = None
    try:
        import asyncio as _asyncio

        _raw_state_data = _asyncio.run(state_manager.load_state(strategy_id))
        if _raw_state_data and _raw_state_data.state:
            initial_vault_state = _raw_state_data.state.get(VAULT_STATE_KEY)
    except Exception as _e:  # noqa: BLE001
        logger.debug("Could not load persisted state for vault init (strategy_id=%s): %s", strategy_id, _e)
        # No persisted state — VaultLifecycleManager uses defaults
    # Fallback: also check in-memory strategy state (for StrategyBase subclasses)
    if initial_vault_state is None:
        for attr in ("persistent_state", "state"):
            store = getattr(strategy_instance, attr, None)
            if isinstance(store, dict):
                initial_vault_state = store.get(VAULT_STATE_KEY)
                break

    # Wire persistence callback: vault state changes are saved into the
    # strategy's persistent_state dict and persisted via the gateway state manager.
    def _persist_vault_state(vault_state_dict: dict) -> None:
        for attr in ("persistent_state", "state"):
            store = getattr(strategy_instance, attr, None)
            if isinstance(store, dict):
                store[VAULT_STATE_KEY] = vault_state_dict
                if hasattr(strategy_instance, "save_state"):
                    strategy_instance.save_state()
                return

    vault_lifecycle = VaultLifecycleManager(
        vault_config=vault_config,
        vault_sdk=vault_sdk,
        vault_adapter=vault_adapter,
        execution_orchestrator=execution_orchestrator,
        strategy_id=strategy_id,
        initial_vault_state=initial_vault_state,
        persistence_callback=_persist_vault_state,
    )
    click.echo(
        f"  Vault lifecycle initialized: "
        f"address={vault_config.vault_address}, "
        f"underlying={vault_config.underlying_token}, "
        f"interval={vault_config.settlement_interval_minutes}min"
    )
    return vault_lifecycle


def _reconciliation_enforcement_from_env() -> bool:
    """Return True iff ``ALMANAK_RECONCILIATION_ENFORCEMENT`` opts the CLI back
    into fail-closed reconciliation.

    Default is observation mode (False) until VIB-3348 block-anchored balance
    reads close the false-positive race. Truthy values: ``1``, ``true``, ``yes``
    (case-insensitive, surrounding whitespace tolerated). Anything else — unset,
    empty, ``0``, ``false``, arbitrary strings — returns False.
    """
    return os.environ.get("ALMANAK_RECONCILIATION_ENFORCEMENT", "").strip().lower() in (
        "1",
        "true",
        "yes",
    )


def _build_runner(
    *,
    interval: int,
    effective_dry_run: bool,
    strategy_id: str,
    components: ComponentBundle,
    vault_lifecycle: Any,
) -> Any:
    """Construct the `StrategyRunner` with all safety components wired in.

    Reads orchestrator + price_oracle + balance_provider + state_manager
    from ``components``, creates the safety components
    (CircuitBreaker/StuckDetector/OperatorCardGenerator/EmergencyManager),
    and attaches them all to a new StrategyRunner. The runner and safety
    components are written back into ``components`` for downstream phases.
    """
    from ..execution.circuit_breaker import CircuitBreaker
    from ..runner import RunnerConfig, StrategyRunner
    from ..services.emergency_manager import EmergencyManager
    from ..services.operator_card_generator import OperatorCardGenerator
    from ..services.stuck_detector import StuckDetector

    # Create runner config
    runner_config = RunnerConfig(
        default_interval_seconds=interval,
        dry_run=effective_dry_run,
        enable_state_persistence=True,
        enable_alerting=False,  # No alert manager configured
        reconciliation_enforcement=_reconciliation_enforcement_from_env(),
    )

    # Create safety components for fail-closed execution
    circuit_breaker = CircuitBreaker(strategy_id=strategy_id)
    stuck_detector = StuckDetector()
    operator_card_generator = OperatorCardGenerator()
    emergency_manager = EmergencyManager()

    runner = StrategyRunner(
        price_oracle=components.price_oracle,
        balance_provider=components.balance_provider,
        execution_orchestrator=components.execution_orchestrator,
        state_manager=components.state_manager,
        config=runner_config,
        vault_lifecycle=vault_lifecycle,
        circuit_breaker=circuit_breaker,
        stuck_detector=stuck_detector,
        operator_card_generator=operator_card_generator,
        emergency_manager=emergency_manager,
    )

    components.runner = runner
    components.circuit_breaker = circuit_breaker
    components.stuck_detector = stuck_detector
    components.operator_card_generator = operator_card_generator
    components.emergency_manager = emergency_manager

    return runner


def _build_components(
    *,
    strategy_instance: Any,
    strategy_config: dict[str, Any],
    runtime_config: Any,
    strategy_chains: list[str],
    multi_chain: bool,
    resolved_network: str,
    gateway_client: Any,
    chain_wallets: dict[str, str],
    interval: int,
    effective_dry_run: bool,
    strategy_id: str,
    normalized_copy_mode: str | None,
    copy_replay_file: str | None,
    copy_shadow: bool,
    copy_strict: bool,
    config_chain: str | None,
) -> ComponentBundle:
    """Construct the full runtime component bundle (orchestrator -> runner).

    Mirrors phase 13 of `run()`. Internal ordering is load-bearing:

    1. Build orchestrator + price/balance/OHLCV providers + indicators +
       rate/funding providers + optional Solana fork manager.
    2. Wire copy-trading components (single-chain only) — attaches
       attributes like `_copy_mode`/`_copy_replay_file` that some
       strategies consume on first-iteration setup.
    3. Create the gateway-backed state manager and bind it to the strategy.
    4. Auto-deploy the vault if placeholder and build VaultLifecycleManager.
       MUST run before runner construction so the runner sees the patched
       vault_address.
    5. Build the `StrategyRunner` and safety components.

    Exits with status 1 on any unhandled exception (preserving the original
    top-level except block's behavior).
    """
    components = ComponentBundle()

    # Pre-flight validation: fail BEFORE building any gateway-backed resources
    # so a rejected config doesn't leak providers / orchestrators / sockets.
    # This mirrors the defence-in-depth check inside `_init_copy_trading` but
    # runs at a point where no cleanup_fn has been constructed yet (see #1683
    # and CR comment on PR #1689).
    if multi_chain and strategy_config.get("copy_trading"):
        raise click.ClickException(
            "copy_trading is not yet supported for multi-chain strategies. "
            "Remove the copy_trading block or configure the strategy as single-chain."
        )

    try:
        click.echo("Initializing components...")

        _build_orchestrator_and_providers(
            multi_chain=multi_chain,
            runtime_config=runtime_config,
            strategy_chains=strategy_chains,
            strategy_config=strategy_config,
            resolved_network=resolved_network,
            gateway_client=gateway_client,
            chain_wallets=chain_wallets,
            strategy_instance=strategy_instance,
            components=components,
        )

        # Copy-trading comes AFTER provider wiring so the price_oracle is
        # available to drive the signal-engine price_fn. Single-chain only
        # (the original code places this branch inside the else:).
        _init_copy_trading(
            strategy_instance=strategy_instance,
            strategy_config=strategy_config,
            runtime_config=runtime_config,
            gateway_client=gateway_client,
            price_oracle=components.price_oracle,
            normalized_copy_mode=normalized_copy_mode,
            copy_replay_file=copy_replay_file,
            copy_shadow=copy_shadow,
            copy_strict=copy_strict,
            multi_chain=multi_chain,
        )

        # Create state manager - always use gateway-backed state manager
        from ..state.gateway_state_manager import GatewayStateManager

        state_manager = GatewayStateManager(gateway_client)
        click.echo("  Using gateway-backed state manager")
        components.state_manager = state_manager

        # Inject state manager into strategy for persistence.
        # State loading is deferred to the async setup phase (run_once_with_cleanup /
        # run_loop_with_cleanup) so that load_state_async() can be awaited properly.
        if hasattr(strategy_instance, "set_state_manager"):
            strategy_instance.set_state_manager(state_manager, strategy_id)

        # Vault auto-deploy MUST happen before runner construction so that the
        # StrategyRunner sees the patched vault_address when it initializes.
        vault_lifecycle = _maybe_auto_deploy_vault(
            strategy_config=strategy_config,
            resolved_network=resolved_network,
            effective_dry_run=effective_dry_run,
            config_chain=config_chain,
            runtime_config=runtime_config,
            gateway_client=gateway_client,
            execution_orchestrator=components.execution_orchestrator,
            state_manager=state_manager,
            strategy_instance=strategy_instance,
            strategy_id=strategy_id,
        )

        _build_runner(
            interval=interval,
            effective_dry_run=effective_dry_run,
            strategy_id=strategy_id,
            components=components,
            vault_lifecycle=vault_lifecycle,
        )

        click.echo("Components initialized successfully")

    except click.ClickException:
        # Preserve explicit ClickException (e.g., missing wallet address)
        # so the caller sees the same message as the original code.
        raise
    except _DryRunVaultEarlyExit as e:
        # Attach the partial component bundle so the caller can still run
        # cleanup_fn before exiting 0 (see #1682).
        e.components = components
        raise
    except SystemExit:
        # Preserve any sys.exit() inside the helpers.
        raise
    except Exception as e:
        click.echo(f"Error initializing components: {e}", err=True)
        logger.exception("Component initialization failed")
        sys.exit(1)

    return components


# ---------------------------------------------------------------------------
# Phase 5 — Dashboard helpers
# ---------------------------------------------------------------------------


def _start_dashboard_background(
    *,
    port: int,
    gateway_host: str = "127.0.0.1",
    gateway_port: int = 50051,
) -> Any:
    """Launch the Streamlit dashboard as a background subprocess.

    Mirrors the nested ``start_dashboard_background`` previously defined
    inside ``run()``. Behavior-preserving: probes the requested port with
    a transient socket bind, falls back to 8502-8509 if busy, and returns
    ``None`` on any launch failure (no streamlit, spawn error, no free port).

    Args:
        port: The requested dashboard port.
        gateway_host: Gateway host for the dashboard env (GATEWAY_HOST).
        gateway_port: Gateway port for the dashboard env (GATEWAY_PORT).

    Returns:
        A ``subprocess.Popen`` handle, or ``None`` if launch failed.
    """
    import importlib.util
    import socket
    import subprocess

    if importlib.util.find_spec("streamlit") is None:
        click.echo("Error: streamlit not found. Install with: pip install streamlit", err=True)
        return None

    def is_port_available(p: int) -> bool:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind(("localhost", p))
                return True
            except OSError:
                return False

    actual_port = port
    if not is_port_available(actual_port):
        click.echo(f"Warning: Dashboard port {actual_port} is already in use.", err=True)
        for alt_port in range(8502, 8510):
            if is_port_available(alt_port):
                actual_port = alt_port
                click.echo(f"Using alternative dashboard port: {actual_port}", err=True)
                break
        else:
            click.echo(
                f"Error: Could not find an available port for dashboard. "
                f"Please free up port {port} or specify a different port with --dashboard-port",
                err=True,
            )
            return None

    project_root = Path(__file__).parent.parent.parent.parent
    dashboard_path = project_root / "almanak" / "framework" / "dashboard" / "app.py"

    # Pass gateway connection info to the dashboard subprocess
    env = os.environ.copy()
    env["GATEWAY_HOST"] = gateway_host
    env["GATEWAY_PORT"] = str(gateway_port)

    try:
        process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "streamlit",
                "run",
                str(dashboard_path),
                "--server.port",
                str(actual_port),
                "--server.headless",
                "false",
            ],
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        click.echo(f"Dashboard started at http://localhost:{actual_port}")
        return process
    except Exception as e:
        click.echo(f"Error launching dashboard: {e}", err=True)
        return None


def _stop_dashboard(process: Any) -> None:
    """Terminate the background dashboard process (best-effort).

    Mirrors the nested ``stop_dashboard`` previously defined inside
    ``run()``. ``None`` process is a no-op. Terminates first, falls back
    to kill on any exception during terminate/wait.

    Args:
        process: The ``subprocess.Popen`` handle returned by
            ``_start_dashboard_background`` (may be ``None``).
    """
    if process is None:
        return
    try:
        process.terminate()
        process.wait(timeout=5)
    except Exception:
        try:
            process.kill()
        except Exception:
            pass


def _handle_standalone_dashboard(
    *,
    working_dir: str,
    dashboard: bool,
    dashboard_port: int,
    gateway_host: str,
    gateway_port: int,
) -> bool:
    """Handle the standalone dashboard early-exit branch.

    Mirrors the block in ``run()``::

        if dashboard and working_dir == ".":
            <launch banner + block on Ctrl+C>
            return

    Launches the dashboard as a background subprocess, prints the banner,
    and blocks on ``process.wait()`` until interrupted. On ``KeyboardInterrupt``
    tears the dashboard down and returns ``True``. When launch fails,
    exits with status 1 (preserving original semantics).

    Args:
        working_dir: CLI ``--working-dir`` (standalone path iff ``"."``).
        dashboard: CLI ``--dashboard`` flag.
        dashboard_port: CLI ``--dashboard-port`` flag.
        gateway_host: Effective gateway host (post-``_setup_gateway``).
        gateway_port: Effective gateway port (post-``_setup_gateway``).

    Returns:
        ``True`` if the branch handled the request (caller must ``return``),
        ``False`` otherwise.
    """
    if not (dashboard and working_dir == "."):
        return False

    click.echo()
    click.echo("=" * 60)
    click.echo("LAUNCHING DASHBOARD (standalone mode)")
    click.echo("=" * 60)
    click.echo("Press Ctrl+C to stop")
    proc = _start_dashboard_background(
        port=dashboard_port,
        gateway_host=gateway_host,
        gateway_port=gateway_port,
    )
    if proc is None:
        sys.exit(1)
    try:
        proc.wait()
    except KeyboardInterrupt:
        _stop_dashboard(proc)
        click.echo("Dashboard stopped.")
    return True


# ---------------------------------------------------------------------------
# Phase 15 — --once execution
# ---------------------------------------------------------------------------


def _run_once(
    *,
    runner: Any,
    strategy_instance: Any,
    state_manager: Any,
    cleanup_fn: Callable[[], Coroutine[Any, Any, None]],
    teardown_after: bool,
) -> int:
    """Execute a single strategy iteration (and optional teardown) and return exit code.

    Synchronous wrapper that mirrors the ``if once:`` block in ``run()``.
    Owns the outer ``asyncio.run(run_once_with_cleanup())`` call plus the
    exit-code resolution and the top-level error/except handling. Keeping
    this sync preserves the original ``asyncio.run`` boundary and lets
    ``KeyboardInterrupt`` semantics remain identical to the inlined code.

    Behavior-preserving:

        * Restores persisted strategy state and copy-trading cursor
          (inside the async wrapper).
        * Runs a single iteration, captures portfolio snapshot, emits summary.
        * If ``teardown_after`` is True, runs a second iteration after
          registering a TeardownRequest.
        * Persists copy-trading cursor and flushes pending saves.
        * Always runs gateway-integration teardown and ``cleanup_fn`` in
          ``finally``.

    Returns:
        Exit code: ``0`` on success, ``1`` on iteration or teardown failure
        (or unhandled exception).
    """
    import asyncio

    # Lazy-import so tests that monkeypatch these modules observe the fakes.
    from ..runner import IterationStatus

    # Runtime-local reference for format_iteration_result (lives in run.py to
    # avoid moving unrelated code; deferred import breaks the cycle).
    from .run import format_iteration_result

    click.echo()
    click.echo("Running single iteration...")
    click.echo()

    async def run_once_with_cleanup() -> tuple[Any, Any]:
        """Run single iteration, optional teardown, and cleanup resources."""
        # Guarded layout ensures cleanup_fn() always runs, even if
        # setup_gateway_integration or teardown_gateway_integration raise.
        # Mirrors the Phase 4a copy_replay_file safety fix (always-run cleanup).
        gateway_integration_ready = False
        try:
            runner.setup_gateway_integration(strategy_instance)
            gateway_integration_ready = True
            # Restore persisted strategy state (e.g. position_id after restart)
            if hasattr(strategy_instance, "load_state_async"):
                if await strategy_instance.load_state_async():
                    click.secho("  Strategy state restored from persistence", fg="yellow")
                else:
                    click.echo("  No previous state found (fresh start)")

            # Restore copy trading cursor state (mirrors run_loop pattern)
            activity_provider = getattr(strategy_instance, "_wallet_activity_provider", None)
            if activity_provider is not None:
                try:
                    ct_state = await state_manager.load_state(strategy_instance.strategy_id)
                    if ct_state is not None and "copy_trading_state" in ct_state.state:
                        activity_provider.set_state(ct_state.state["copy_trading_state"])
                except Exception as e:
                    logger.warning(f"Failed to restore copy trading state: {e}")

            result = await runner.run_iteration(strategy_instance)

            # Capture portfolio snapshot after --once iteration
            # (run_loop does this automatically but run_iteration does not)
            if runner.config.enable_state_persistence:
                await runner._capture_portfolio_snapshot(
                    strategy=strategy_instance,
                    iteration_number=runner._total_iterations,
                )

            # Emit structured iteration summary for JSONL log analysis
            runner._emit_iteration_summary(result, chain=getattr(strategy_instance, "chain", None))

            # --- teardown-after: signal + second iteration ---
            teardown_result = None
            if teardown_after:
                click.echo()
                click.echo("Teardown requested -- closing positions...")

                from almanak.framework.teardown import get_teardown_state_manager
                from almanak.framework.teardown.models import TeardownMode, TeardownRequest

                strategy_id = strategy_instance.strategy_id or strategy_instance.STRATEGY_NAME
                manager = get_teardown_state_manager()
                manager.create_request(
                    TeardownRequest(
                        strategy_id=strategy_id,
                        mode=TeardownMode.SOFT,
                        reason="--teardown-after flag (CI cleanup)",
                        requested_by="cli",
                    )
                )

                teardown_result = await runner.run_iteration(strategy_instance)
                runner._emit_iteration_summary(teardown_result, chain=getattr(strategy_instance, "chain", None))
                click.echo(format_iteration_result(teardown_result))

            # Persist copy trading cursor state
            if activity_provider is not None:
                try:
                    ct_state = await state_manager.load_state(strategy_instance.strategy_id)
                    if ct_state is None:
                        from almanak.framework.state.state_manager import StateData

                        ct_state = StateData(
                            strategy_id=strategy_instance.strategy_id,
                            version=0,
                            state={},
                        )
                    ct_state.state["copy_trading_state"] = activity_provider.get_state()
                    await state_manager.save_state(ct_state, expected_version=ct_state.version)
                except Exception as e:
                    logger.warning(f"Failed to persist copy trading state: {e}")

            # Flush any pending state saves before cleanup
            # (run_loop does this automatically, but run_iteration doesn't)
            if hasattr(strategy_instance, "flush_pending_saves"):
                try:
                    await strategy_instance.flush_pending_saves()
                except Exception as e:
                    logger.warning(f"Error flushing pending saves: {e}")
            return result, teardown_result
        finally:
            # Nested try/finally guarantees cleanup_fn() runs even if
            # teardown_gateway_integration raises. The ready-guard avoids
            # calling teardown when setup itself failed (pairing invariant).
            try:
                if gateway_integration_ready:
                    runner.teardown_gateway_integration(strategy_instance.strategy_id)
            finally:
                await cleanup_fn()

    try:
        result, teardown_result = asyncio.run(run_once_with_cleanup())
        click.echo(format_iteration_result(result))

        # Determine exit code: main iteration + optional teardown
        if teardown_result is not None:
            # With --teardown-after: both iteration and teardown must succeed
            teardown_ok = teardown_result.status == IterationStatus.TEARDOWN
            if result.success and teardown_ok:
                click.echo()
                click.echo("Iteration and teardown completed successfully.")
                return 0
            click.echo()
            if not result.success:
                click.echo(f"Iteration failed: {result.error}")
            if not teardown_ok:
                click.echo(f"Teardown failed: {teardown_result.error or teardown_result.status.value}")
            return 1
        if result.success:
            click.echo()
            click.echo("Iteration completed successfully.")
            return 0
        click.echo()
        click.echo(f"Iteration failed: {result.error}")
        return 1

    except Exception as e:
        click.echo(f"Error running iteration: {e}", err=True)
        logger.exception("Iteration failed")
        return 1


# ---------------------------------------------------------------------------
# Phase 16 — Continuous execution
# ---------------------------------------------------------------------------


def _run_continuous(
    *,
    runner: Any,
    strategy_instance: Any,
    cleanup_fn: Callable[[], Coroutine[Any, Any, None]],
    interval: int,
    max_iterations: int | None,
    reset_fork: bool,
    managed_gateway: Any,
) -> int:
    """Execute the continuous run loop and return exit code.

    Synchronous wrapper that mirrors the ``else:`` block in ``run()``. Owns
    the outer ``asyncio.run(run_loop_with_cleanup())`` call, the
    ``KeyboardInterrupt`` fresh-loop cleanup (``asyncio.run(cleanup_fn())``),
    and the exit-code resolution. Keeping this sync preserves the original
    boundary: ``KeyboardInterrupt`` is raised from ``asyncio.run`` into the
    enclosing try/except, not into the coroutine itself.

    Behavior-preserving:

        * Registers runner signal handlers.
        * Wires an ``on_iteration`` echo callback.
        * Builds a ``pre_iteration`` callback if ``reset_fork`` is set and a
          managed gateway owns forks (raises ``CriticalCallbackError`` on
          reset failure).
        * Restores persisted strategy state inside the loop wrapper.
        * Runs ``runner.run_loop`` with the wired callbacks.
        * On ``KeyboardInterrupt`` requests shutdown and runs cleanup in a
          fresh event loop (matches original behavior).

    Returns:
        Exit code: ``2`` on signal-triggered stop, ``1`` on
        max-iterations-all-failed or unhandled exception, ``0`` otherwise.
    """
    import asyncio

    from ..runner.strategy_runner import CriticalCallbackError

    # Runtime-local reference for format_iteration_result.
    from .run import format_iteration_result

    if sys.stdout.isatty():
        click.echo()
        click.echo("Starting continuous execution...")
        click.echo("Press Ctrl+C to stop gracefully.")
        click.echo()

    # Set up signal handlers for graceful shutdown
    runner.setup_signal_handlers()

    def on_iteration(result: Any) -> None:
        """Callback for each iteration."""
        timestamp = result.timestamp.strftime("%Y-%m-%d %H:%M:%S")
        click.echo(f"[{timestamp}] {format_iteration_result(result)}")

    # Build pre-iteration callback for --reset-fork
    pre_iteration_cb: Callable[[], None] | None = None
    if reset_fork and managed_gateway is not None:

        def pre_iteration_cb() -> None:
            click.echo("Resetting Anvil fork to latest block...")
            ok = managed_gateway.reset_anvil_forks()
            if ok:
                click.echo("Fork reset complete.")
            else:
                raise CriticalCallbackError(
                    "Anvil fork reset failed. Cannot continue with stale fork state. "
                    "Remove --reset-fork to run without fork resets."
                )

    async def run_loop_with_cleanup() -> None:
        """Run loop and cleanup resources."""
        try:
            # Restore persisted strategy state (e.g. position_id after restart)
            if hasattr(strategy_instance, "load_state_async"):
                if await strategy_instance.load_state_async():
                    click.secho("  Strategy state restored from persistence", fg="yellow")
                else:
                    click.echo("  No previous state found (fresh start)")

            await runner.run_loop(
                strategy=strategy_instance,
                interval_seconds=interval,
                iteration_callback=on_iteration,
                pre_iteration_callback=pre_iteration_cb,
                max_iterations=max_iterations,
            )
        finally:
            await cleanup_fn()

    try:
        asyncio.run(run_loop_with_cleanup())
        click.echo()

        # Exit 2 when stopped by signal (SIGTERM/SIGINT) so K8s sees a
        # pod failure and retries.  Check this first so it takes
        # precedence over the max-iterations branch.
        if runner._signal_received:
            click.echo("Runner stopped by signal.")
            return 2

        # Return a failure exit code when max_iterations is set and every
        # single iteration failed (no successful iterations at all).
        if max_iterations and runner._successful_iterations == 0 and runner._total_iterations > 0:
            click.echo(f"Runner completed {runner._total_iterations} iterations with 0 successes.")
            return 1

        click.echo("Runner stopped gracefully.")
        return 0

    except KeyboardInterrupt:
        click.echo()
        click.echo("Shutdown requested. Stopping...")
        runner.request_shutdown()
        # Run cleanup in a new event loop since the previous one was interrupted
        asyncio.run(cleanup_fn())
        return 0

    except Exception as e:
        click.echo(f"Error in run loop: {e}", err=True)
        logger.exception("Run loop failed")
        return 1
