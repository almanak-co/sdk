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

Later phases (4c-4e) will extend this module for runtime config, component
init, and execution wrappers. See `jazzy-tinkering-zephyr.md`.
"""

from __future__ import annotations

import json
import logging
import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

import click

from ._run_context import IdentityInfo, ResumeInfo

if TYPE_CHECKING:
    from ..strategies.intent_strategy import IntentStrategy

logger = logging.getLogger(__name__)


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
    # Anvil forks need extra startup time (forking from mainnet RPC)
    startup_timeout = 30.0 if anvil_chains else 10.0
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
