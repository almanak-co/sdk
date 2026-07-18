"""almanak strat run — setup, config discovery, identity, state-resume.

Split from run_helpers.py; import via the run_helpers facade externally.
"""

from __future__ import annotations

import contextvars
import json
import logging
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

import click

from ._run_context import IdentityInfo, ResumeInfo

if TYPE_CHECKING:
    from ..strategies.intent_strategy import IntentStrategy

# pinned: tests + operator filters key on the historical module path
logger = logging.getLogger("almanak.framework.cli.run_helpers")


def _require_strategy_deployment_id(strategy_instance: Any, *, operation: str) -> str:
    deployment_id = (getattr(strategy_instance, "deployment_id", "") or "").strip()
    if not deployment_id:
        raise RuntimeError(f"{operation} requires a resolved deployment_id")
    return deployment_id


# ContextVar plumb for the test-only signing-key fallback (#2100). Set by
# `almanak strat test` (in `almanak/cli/cli.py`) around its `ctx.invoke` call
# so the framework runtime can pick up the Anvil-default key without:
#   (a) mutating ``os.environ`` as a global side-channel — the original sin
#       this PR is correcting, or
#   (b) threading a ``runtime_private_key`` parameter through `run()` itself
#       — `run` is a god function on the CRAP gate's bubble (cc=29, cov=17%),
#       and adding wiring lines through it pushes the diff over.
# The ContextVar is read by `_setup_gateway` and `_build_runtime_config` only
# when no explicit kwarg was passed, so direct callers (tests) keep their
# precedence-clean kwarg-first behaviour. After `_setup_gateway` resolves the
# effective signing key (isolated-wallet derived > caller-plumbed > env), it
# writes the result back to the same ContextVar so `_build_runtime_config`
# sees the same key without needing a return-tuple field.
_runtime_private_key_override: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "almanak_runtime_private_key_override", default=None
)


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

    def __init__(self, components: Any | None = None) -> None:
        super().__init__("--dry-run: placeholder vault on Anvil")
        self.components = components


def _configure_logging_and_validate(
    *,
    verbose: bool,
    debug: bool,
    log_file: str | None,
    once: bool,
    teardown_after: bool,
    max_iterations: int | None = None,
) -> None:
    """Configure structured logging and validate setup-stage flag combinations.

    Covers the pre-gateway setup at the top of `run()`:
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

    # Validate --teardown-after requires --once.
    # NOTE: ``--teardown-after`` only fires from ``_run_once`` today. The
    # continuous loop (``_run_continuous`` for ``--max-iterations`` /
    # interval-based runs) does not yet honour the flag; relaxing this
    # validation without that wiring would silently drop the teardown
    # request. Tracked for the post-ship "Accounting Foundation" epic.
    if teardown_after and not once:
        click.echo("Error: --teardown-after requires --once.", err=True)
        sys.exit(1)


def _anchor_strategy_folder_env(working_dir: str) -> None:
    """Anchor ``ALMANAK_STRATEGY_FOLDER`` to the resolved ``working_dir``.

    VIB-3761: every local artifact (DB, logs, lock) is keyed off the
    strategy folder, so 10 strategies launched from the same cwd cannot
    collide on a shared ``./almanak_state.db`` (the April 29 silent-failure
    root cause).

    Sets the env var only when the operator did not already set it
    explicitly so test/operator overrides win. No-op when the resolved
    path is not a directory.

    The env mutation that used to live inline in ``run()`` now happens
    through ``set_strategy_folder`` (the single allowlisted setter in
    ``local_paths.py``). Called from ``_setup_gateway`` rather than
    ``run()`` so path anchoring still happens before any local-path
    resolution without bloating the top-level orchestrator.
    """
    from almanak.framework.local_paths import set_strategy_folder, strategy_folder_env

    resolved = Path(working_dir).expanduser().resolve()
    if resolved.is_dir() and not strategy_folder_env():
        set_strategy_folder(resolved)


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

    Centralizes the config-loading segment of `run()`:
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
    deployment_id: str,
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
    and colors) matches the prior inline startup banner byte-for-byte.
    """
    click.echo("=" * 60)
    click.echo("ALMANAK STRATEGY RUNNER")
    click.echo("=" * 60)
    click.echo(f"Strategy: {strategy_name}")
    click.echo(f"Deployment ID: {deployment_id}")
    click.echo(f"Run ID: {run_id}")
    # Resume detection in hosted mode lives in the gateway against Postgres,
    # not in the runner CLI — the runner has no SQLite to inspect. Avoid
    # the misleading "FRESH START" banner that would otherwise print on
    # every hosted iteration regardless of actual Postgres state.
    from almanak.framework.deployment import is_hosted

    # Log the resume mode to the structured logger too (VIB-5155 / ALM-2719):
    # the click banner only reaches stdout, but operators triaging a
    # stale-state desync need the resume decision in the JSON log file. The
    # ``fresh`` flag is included so a `--fresh` boot is unambiguous in the log.
    if is_hosted():
        resume_mode = "hosted"
    elif is_resume:
        resume_mode = "resume"
    else:
        resume_mode = "fresh-start"
    logger.info(
        "Boot resume mode for %s: %s (--fresh=%s, version=%s)",
        deployment_id,
        resume_mode,
        fresh,
        (existing_state_info or {}).get("version"),
    )

    if is_hosted():
        click.secho(
            "Mode: HOSTED (state managed by gateway via Postgres)",
            fg="cyan",
            bold=True,
        )
    elif is_resume:
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

    After the gateway is live (managed or external), the TokenResolver needs
    the gRPC channel so it can resolve arbitrary ERC-20 addresses on-chain
    when a symbol misses the static registry.
    """
    from ..data.tokens import get_token_resolver

    resolver = get_token_resolver()
    resolver.set_gateway_channel(gateway_client.channel)


def _detect_state_resume(state_db_path: Path, deployment_id: str) -> ResumeInfo:
    """Detect whether a deployment has prior state (RESUME vs FRESH START).

    Quick SQLite read of `strategy_state` filtered by its single canonical
    identity column `deployment_id` (blueprint 29 §3). All errors (missing
    DB file, corrupt schema, connection failure, JSON-parse errors) are
    swallowed and logged at DEBUG — the caller treats them as "no resume".

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
            "SELECT deployment_id, version, state_data FROM strategy_state WHERE deployment_id = ?",
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


def _echo_resume_banner(strategy_instance: Any) -> None:
    """Echo the state-restore banner, distinguishing RESUMED vs RESUMED-TERMINAL.

    Called after ``load_state_async()`` returned True (persisted state restored).
    A normal mid-lifecycle resume is benign (crash-recovery rehydration). A resume
    into a **terminal** lifecycle state — reported by the strategy's optional
    ``is_lifecycle_complete()`` hook (VIB-5887) — means ``decide()`` will HOLD for
    the run; combined with fresh wallet capital that is the silent-no-op class the
    runner-side ``resume_terminal_guard`` warns on (with the balance number). Here
    we only surface the distinction on stdout for the local operator; the
    substantive, balance-aware warning + structured sentinel lives in the runner.

    The literal ``"Strategy state restored from persistence"`` is preserved in both
    branches (operator log filters + tests key on it).
    """
    terminal = False
    hook = getattr(strategy_instance, "is_lifecycle_complete", None)
    if callable(hook):
        try:
            terminal = bool(hook())
        except Exception:  # noqa: BLE001 - a strategy-owned hook must never fault boot
            terminal = False
    if terminal:
        click.secho(
            "  Strategy state restored from persistence — RESUMED-TERMINAL "
            "(prior lifecycle COMPLETE; will HOLD unless state is reset — see VIB-5887 warning)",
            fg="red",
            bold=True,
        )
    else:
        click.secho("  Strategy state restored from persistence (RESUMED)", fg="yellow")


# Strategy DECISION state + derived aggregates + pending signals. Safe to wipe
# on every ``--fresh`` (Anvil AND real networks): these carry no record of real
# executed on-chain activity, so resetting them just makes the strategy
# re-derive its position from a clean slate. ``portfolio_snapshots`` /
# ``portfolio_metrics`` are derived aggregates (rebuilt from the ledger), and
# ``migration_state`` is boot bookkeeping.
_FRESH_DECISION_STATE_TABLES = [
    "strategy_state",
    "teardown_requests",
    "portfolio_snapshots",
    "portfolio_metrics",
    "clob_orders",
    "migration_state",
]

# The IMMUTABLE record of REAL executed activity — "the books" (blueprint 27:
# "Every executed intent produces a LedgerEntry — an immutable record of the
# trade"; position events track "immutable-ID positions").
#
# On a REAL network the on-chain positions/trades these rows describe still
# exist after a restart, so ``--fresh`` MUST preserve them: deleting them makes
# the books lie about trades that actually landed on-chain (VIB-5784 — a
# ``--fresh`` relaunch wiped a prior launch's already-executed balancing SWAP,
# leaving zero ``transaction_ledger`` / ``accounting_events`` rows for it).
# On Anvil the fork is reset between runs, so the described on-chain state no
# longer exists; those rows become stale cross-fork references that MUST be
# cleared (VIB-2573).
_FRESH_ONCHAIN_RECORD_TABLES = [
    "transaction_ledger",
    "accounting_events",
    "accounting_outbox",
    "position_events",
    "position_state_snapshots",
    "position_registry",
]

# Full set keyed on the canonical ``deployment_id`` column. Used by the Anvil
# wipe-everything path; the real-network path clears only decision state.
_FRESH_DEPLOYMENT_ID_TABLES = _FRESH_DECISION_STATE_TABLES + _FRESH_ONCHAIN_RECORD_TABLES


def _fresh_clear_state(conn: Any, deployment_id: str, is_anvil: bool) -> int:  # noqa: C901
    """Clear ``--fresh`` state rows for a strategy (or all strategies on Anvil).

    VIB-4722 renamed deployment-scoped SQLite identity columns to
    ``deployment_id``. This helper clears only by that canonical identity; old
    local DB files must run migrations before their rows are in scope.

    Scope depends on the network (VIB-5784):

    - **Anvil**: the fork is reset between runs, so every row — including the
      accounting/ledger record — describes on-chain state that no longer exists.
      Wipe ALL rows across ALL strategies for a clean cross-fork slate
      (VIB-2573).
    - **Real network**: the strategy's on-chain positions/trades survive a
      restart, so the immutable on-chain record (``_FRESH_ONCHAIN_RECORD_TABLES``)
      MUST survive too — otherwise a ``--fresh`` relaunch erases the books for
      trades that already landed on-chain. Only the target deployment's DECISION
      state (``_FRESH_DECISION_STATE_TABLES``) is reset.

    Returns the total number of rows deleted.
    """
    import sqlite3

    def _columns(table: str) -> set[str]:
        try:
            return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}  # noqa: S608
        except sqlite3.OperationalError:
            return set()

    total = 0
    with conn:
        if is_anvil:
            for table in _FRESH_DEPLOYMENT_ID_TABLES:
                try:
                    total += conn.execute(f"DELETE FROM {table}").rowcount  # noqa: S608
                except sqlite3.OperationalError:
                    pass
        else:
            # Preserve the immutable on-chain record; reset only decision state.
            for table in _FRESH_DECISION_STATE_TABLES:
                columns = _columns(table)
                if "deployment_id" in columns:
                    try:
                        total += conn.execute(
                            f"DELETE FROM {table} WHERE deployment_id = ?",  # noqa: S608
                            (deployment_id,),
                        ).rowcount
                    except sqlite3.OperationalError:
                        pass
    return total


def _resolve_identity(
    *,
    strategy_config: dict[str, Any],
    fresh: bool,
    multi_chain: bool,
    strategy_chains: list[str],
    config_display_name: str,
    gateway_network: str,
) -> IdentityInfo:
    """Resolve deployment_id/run_id and handle `--fresh`.

    This helper:
        1. Computes `identity_chain` (the strategy's chain or a
           comma-separated sorted multi-chain signature).
        2. Resolves `deployment_id` via `resolve_deployment_id()` — hosted
           ⇒ `ALMANAK_DEPLOYMENT_ID`; local ⇒ `deployment:{sha256(...)}`
           (blueprint 29 §2). There is no `--id` flag and no bare-name
           fallback: a local run with no resolvable wallet + chain raises a
           fatal boot error.
        3. Generates an ephemeral `run_id`.
        4. Writes both into `strategy_config` (mutated in place, matching
           the original inlined behavior).
        5. If `fresh=True`, clears state. On Anvil, scope is ALL rows across
           ALL tables (VIB-2573 — the fork reset invalidates every row). On a
           real network, scope is the current deployment_id's DECISION state
           only; the immutable on-chain accounting/ledger record is preserved
           (VIB-5784 — a `--fresh` relaunch must not erase the books for trades
           that already landed on-chain).

    Args:
        strategy_config: strategy config dict. MUTATED: deployment_id and
            run_id are written in, matching the original code.
        fresh: True if `--fresh` was passed.
        multi_chain: True if the strategy runs on multiple chains.
        strategy_chains: chains the strategy is configured for (only used
            when multi_chain=True).
        config_display_name: the human-facing strategy name (used only for
            the IdentityInfo snapshot; NOT an input to the id hash).
        gateway_network: "anvil" triggers anvil-scope fresh deletion.

    Returns:
        IdentityInfo snapshotting (deployment_id, run_id, strategy_name).
    """
    from almanak.framework.runner.identity import generate_run_id, resolve_deployment_id

    # Resolve deployment_id now that wallet + chain are known
    # (resolution runs AFTER _apply_strategy_config_wallet — blueprint 29 §2.2).
    # For multi-chain strategies, hash all chains so different chain combinations
    # produce distinct deployment_ids (e.g., [arbitrum,base] vs [arbitrum,optimism]).
    identity_chain = str(strategy_config.get("chain", ""))
    if multi_chain and strategy_chains:
        identity_chain = ",".join(sorted(str(c).lower() for c in strategy_chains))
    deployment_id = resolve_deployment_id(
        wallet_address=strategy_config.get("wallet_address", ""),
        chain=identity_chain,
    )
    strategy_config["deployment_id"] = deployment_id
    run_id = generate_run_id()
    strategy_config["run_id"] = run_id

    from almanak.framework.deployment import is_local

    # Handle --fresh flag: clear state to prevent cross-strategy contamination.
    # VIB-2573: On Anvil, clear ALL strategy state (not just current strategy)
    # to prevent TokenNotFoundError from stale state referencing wrong-chain tokens.
    # On mainnet, only clear the current strategy's state (preserve other strategies).
    #
    # --fresh is a SQLite-only operation. Hosted mode has no local DB to clear;
    # the platform recreates the agent if a clean state is required.
    deployment_id = strategy_config["deployment_id"]
    if fresh and not is_local():
        raise click.ClickException(
            "--fresh is not supported in hosted mode (ALMANAK_IS_HOSTED set). "
            "Hosted state lives in Postgres; recreate the agent to start clean."
        )
    if fresh:
        from almanak.framework.local_paths import local_db_path

        state_db_path = local_db_path()
        if state_db_path.exists():
            try:
                import sqlite3
                from contextlib import closing

                is_anvil = gateway_network == "anvil"
                # ``closing`` guarantees the connection is released even on the
                # error path — a leaked WAL connection can otherwise block the
                # checkpoint that flushes the runner's writes to the main DB.
                with closing(sqlite3.connect(str(state_db_path))) as conn:
                    total_deleted = _fresh_clear_state(conn, deployment_id, is_anvil)
                scope = "all strategies" if is_anvil else f"strategy '{deployment_id}'"
                # On real networks the immutable on-chain accounting/ledger record
                # is preserved (VIB-5784); only decision state is reset.
                cleared_desc = "all state" if is_anvil else "decision state (on-chain accounting history preserved)"
                if total_deleted > 0:
                    click.secho(
                        f"Cleared {cleared_desc} for {scope} (--fresh flag)",
                        fg="yellow",
                    )
                else:
                    # Anvil attempts to clear ALL state; a real network attempts
                    # only decision state — report the scope actually attempted.
                    empty_desc = "state" if is_anvil else "decision state"
                    click.echo(f"No existing {empty_desc} for {scope} (--fresh flag)")
            except Exception as e:
                raise click.ClickException(
                    f"--fresh cleanup failed; refusing to start with potentially dirty state: {e}"
                ) from e
        else:
            click.echo("No existing state to clear (--fresh flag)")

    return IdentityInfo(
        deployment_id=deployment_id,
        run_id=run_id,
        strategy_name=config_display_name,
    )
