"""Private orchestration helpers for `almanak strat run`.

This module holds the mechanics that make `run()` work without forcing the CLI
entrypoint itself to carry every branch and side effect:

- setup and validation
- gateway bootstrap and dashboard lifecycle
- strategy/config discovery
- identity/runtime resolution
- component assembly and cleanup
- single-iteration / lifecycle / continuous execution

The goal is behavioral parity with the historical inline implementation, but
grouped by responsibility instead of by extraction order.
"""

from __future__ import annotations

import asyncio
import contextvars
import inspect
import json
import logging
import sys
import time
from collections.abc import Callable, Coroutine
from pathlib import Path
from typing import TYPE_CHECKING, Any, NoReturn

import click

from almanak.config.cli_runtime import almanak_chain_from_env, anvil_port_for_chain

from ..strategies.metadata import LEGACY_COMPAT_DATA_REQUIREMENTS, StrategyDataRequirements
from ._run_context import ComponentBundle, IdentityInfo, ResumeInfo, RuntimeBootstrap, StrategyBootstrap

if TYPE_CHECKING:
    from ..strategies.intent_strategy import IntentStrategy

logger = logging.getLogger(__name__)


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


def _normalize_anvil_funding(raw: Any) -> dict[str, int | float | str]:
    """Normalize a quick-config ``anvil_funding`` value into a safe dict (VIB-3876).

    User-authored config files may set ``anvil_funding`` to a malformed value
    — a list (``anvil_funding: [WETH, USDC]``), a string, an int — all of
    which would propagate to ``ManagedGateway._anvil_funding`` and then crash
    inside ``_fund_anvil_wallets()`` on ``.items()``. Validate the shape
    here (a dict of ``{token_symbol: amount}``) and emit a warning + safe
    fallback for anything else, rather than letting the gateway boot fail
    with an opaque ``AttributeError`` mid-startup.

    Token amounts may be ints, floats, or strings (the gateway accepts strings
    for high-precision Decimal values like wstETH); other value types
    (booleans, dicts, lists) are dropped with a warning.

    Args:
        raw: The raw value read from the config's ``anvil_funding`` field.

    Returns:
        A ``{token_symbol: amount}`` dict suitable for ``ManagedGateway``,
        or an empty dict if ``raw`` is malformed.
    """
    if not isinstance(raw, dict):
        if raw not in (None, {}):
            logger.warning(
                "Ignoring malformed anvil_funding (%s); expected dict[str, int | float | str], got %r",
                type(raw).__name__,
                raw,
            )
        return {}
    cleaned: dict[str, int | float | str] = {}
    for key, value in raw.items():
        if not isinstance(key, str):
            logger.warning(
                "Ignoring anvil_funding entry with non-string key %r (value=%r)",
                key,
                value,
            )
            continue
        if isinstance(value, bool) or not isinstance(value, int | float | str):
            # bool is a subclass of int — reject it explicitly so True/False
            # don't silently get treated as 1/0 fund amounts.
            logger.warning(
                "Ignoring anvil_funding[%s] — expected int | float | str, got %s (value=%r)",
                key,
                type(value).__name__,
                value,
            )
            continue
        cleaned[key] = value
    return cleaned


# Chains that run on solana-test-validator, not Anvil. Filtered out of
# ``anvil_chains`` so ``ManagedGateway`` doesn't try to start a RollingForkManager
# for them; tracked separately via ``solana_anvil_needed`` so the caller can wire
# up ``SolanaForkManager`` (run_helpers.py:1837-1884 / cli/teardown.py VIB-3878).
NON_ANVIL_CHAINS: frozenset[str] = frozenset({"solana"})


def _resolve_anvil_chain_dispatch(
    network: str,
    primary_chain: str | None,
    config_dict: dict[str, Any],
) -> tuple[list[str], bool]:
    """Decide which forks the teardown CLI must start (VIB-3878).

    Returns ``(anvil_chains, solana_anvil_needed)``:

    - ``anvil_chains`` — EVM chains to pass to ``ManagedGateway(anvil_chains=...)``.
      Empty when ``network != "anvil"`` or when the strategy is Solana-only.
    - ``solana_anvil_needed`` — ``True`` when ``--network anvil`` and the
      strategy declares a Solana chain (single ``chain`` field or in ``chains``
      list). Caller is responsible for spinning up a ``SolanaForkManager``.

    The two flags are independent; multi-chain strategies (e.g. EVM + Solana)
    set both. Mirrors ``run_helpers.py:907-910`` so ``strat run`` and
    ``strat teardown`` agree on which forks to start.
    """
    if network != "anvil":
        return [], False

    chains_val = config_dict.get("chains")
    if isinstance(chains_val, str | list):
        all_chains = _normalize_quick_chains(chains_val)
    else:
        if chains_val is not None:
            # Malformed config (int, dict, None-equivalent): warn and fall
            # back to ``primary_chain`` rather than silently starting nothing.
            # CodeRabbit P2 — turning a recoverable typo into a "start nothing"
            # path is exactly the silent-failure VIB-3819 was fixing for.
            logger.warning(
                "Ignoring malformed config['chains'] (%s); falling back to primary chain. value=%r",
                type(chains_val).__name__,
                chains_val,
            )
        all_chains = [primary_chain] if primary_chain else []

    # Normalize once: strip whitespace + lowercase. Otherwise a config like
    # ``chains: [" solana "]`` would slip past the NON_ANVIL_CHAINS filter and
    # the solana_anvil_needed check, sending "solana" to ``ManagedGateway`` as
    # an Anvil fork target (which would fail). CodeRabbit P_minor.
    normalized_chains = [str(c).strip().lower() for c in all_chains]
    anvil_chains = [c for c in normalized_chains if c not in NON_ANVIL_CHAINS]
    solana_anvil_needed = "solana" in normalized_chains
    return anvil_chains, solana_anvil_needed


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


# Tables keyed on the canonical `deployment_id` column, cleared by --fresh.
_FRESH_DEPLOYMENT_ID_TABLES = [
    "strategy_state",
    "teardown_requests",
    "portfolio_snapshots",
    "portfolio_metrics",
    "transaction_ledger",
    "position_events",
    "accounting_events",
    "accounting_outbox",
    "position_state_snapshots",
    "clob_orders",
    "position_registry",
    "migration_state",
]


def _fresh_clear_state(conn: Any, deployment_id: str, is_anvil: bool) -> int:  # noqa: C901
    """Delete all state rows for a strategy (or all strategies on Anvil).

    VIB-4722 renamed deployment-scoped SQLite identity columns to
    ``deployment_id``. This helper clears only by that canonical identity; old
    local DB files must run migrations before their rows are in scope.

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
            for table in _FRESH_DEPLOYMENT_ID_TABLES:
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
        5. If `fresh=True`, deletes `strategy_state` (and `teardown_requests`
           if present) rows. On Anvil, scope is ALL rows (VIB-2573). On
           mainnet, scope is just the current deployment_id.

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

                is_anvil = gateway_network == "anvil"
                total_deleted = _fresh_clear_state(sqlite3.connect(str(state_db_path)), deployment_id, is_anvil)
                scope = "all strategies" if is_anvil else f"strategy '{deployment_id}'"
                if total_deleted > 0:
                    click.secho(
                        f"Cleared all state for {scope} (--fresh flag)",
                        fg="yellow",
                    )
                else:
                    click.echo(f"No existing state for {scope} (--fresh flag)")
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


def _validate_no_gateway_flags(
    *, no_gateway: bool, anvil_ports: tuple[str, ...], wallet: str, keep_anvil: bool
) -> None:
    """Raise ClickException for `--no-gateway` flag combinations that need a managed gateway."""
    if not no_gateway:
        return
    if anvil_ports:
        raise click.ClickException("--anvil-port requires a managed gateway (remove --no-gateway).")
    if keep_anvil:
        raise click.ClickException("--keep-anvil requires a managed gateway (remove --no-gateway).")
    if wallet == "isolated":
        raise click.ClickException(
            "--wallet isolated requires a managed gateway (remove --no-gateway). "
            "The managed gateway auto-funds the derived wallet on Anvil."
        )


def _attach_external_gateway(
    *,
    effective_host: str,
    gateway_port: int,
    gateway_network: str,
    runtime_private_key: str | None,
) -> tuple[Any, Any, str, int, str, str | None, str | None, type[IntentStrategy[Any]] | None]:
    """Connect to an existing gateway (`--no-gateway` path) and return the standard tuple."""
    from ..gateway_client import GatewayClient, GatewayClientConfig

    click.echo(f"Connecting to existing gateway at {effective_host}:{gateway_port}...")
    # Read the typed gateway auth token with fallback to the legacy unprefixed
    # GATEWAY_AUTH_TOKEN. Both flow through the config service, but
    # we narrow to the gateway + cli factories rather than ``load_config()``
    # so an unrelated submodel validation error (e.g. malformed
    # ``ANVIL_*_PORT``) cannot block ``--no-gateway`` startup (PR #2152 review).
    from almanak.config import cli_runtime_config_from_env
    from almanak.config.env import gateway_config_from_env

    auth_token = gateway_config_from_env().auth_token or cli_runtime_config_from_env().legacy_gateway_auth_token
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
    # No managed gateway here, so no isolated-wallet path runs; whatever
    # the caller plumbed in (or the contextvar) is already the right key
    # for `_build_runtime_config` to read via the ContextVar. Mirror it
    # explicitly so direct `_setup_gateway` callers (test paths) that
    # set ``runtime_private_key=…`` see it propagated downstream too.
    if runtime_private_key is not None:
        _runtime_private_key_override.set(runtime_private_key)
    return (gateway_client, None, effective_host, gateway_port, gateway_network, None, None, None)


def _find_available_gateway_port_or_raise(effective_host: str, gateway_port: int) -> int:
    """Wrap `find_available_gateway_port` with the user-facing error UX."""
    from almanak.gateway.managed import find_available_gateway_port

    try:
        return find_available_gateway_port(effective_host, gateway_port)
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


def _parse_anvil_port_overrides(anvil_ports: tuple[str, ...]) -> dict[str, int]:
    """Parse `--anvil-port CHAIN=PORT` entries, raising ClickException on malformed input."""
    parsed: dict[str, int] = {}
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
        if chain_name in parsed:
            raise click.ClickException(f"Duplicate --anvil-port for chain '{chain_name}'.")
        parsed[chain_name] = port
    return parsed


def _early_load_strategy_class(working_dir: str) -> type[IntentStrategy[Any]] | None:
    """Eager-load strategy.py so decorator metadata is available before gateway startup."""
    from .intent_debug import load_strategy_from_file

    strategy_file = Path(working_dir) / "strategy.py"
    if not strategy_file.exists():
        return None
    cls, err = load_strategy_from_file(strategy_file)
    if err:
        logger.debug(f"Early strategy load failed (will retry later): {err}")
    return cls


def _resolve_quick_config_path(working_dir: str, config_file: str | None) -> Path | None:
    """Pick the explicit `--config` path or auto-discover config.json/yaml/yml in `working_dir`."""
    if config_file:
        return Path(config_file)
    for name in ("config.json", "config.yaml", "config.yml"):
        candidate = Path(working_dir) / name
        if candidate.exists():
            return candidate
    return None


def _quick_load_config_dict(path: Path) -> dict[str, Any]:
    """Best-effort parse of a quick-probe config file; malformed input yields an empty dict."""
    try:
        with open(path) as f:
            if path.suffix.lower() in [".yaml", ".yml"]:
                import yaml

                parsed = yaml.safe_load(f)
            else:
                parsed = json.load(f)
    except Exception as e:
        logger.debug("Quick config probe failed for %s: %s", path, e)
        return {}
    # `yaml.safe_load` returns None for empty files, and a user-supplied
    # config could parse to a scalar or list. Coerce anything non-dict
    # to an empty dict so `.get()` is always safe.
    return parsed if isinstance(parsed, dict) else {}


def _chains_from_quick_config(quick_config: dict[str, Any]) -> list[str]:
    """Extract `chains` (preferred) or `chain` from a quick-config dict."""
    chains_val = quick_config.get("chains")
    if chains_val is not None:
        # Normalize via helper: wraps strings into single-element lists,
        # coerces lists to list[str], and ignores non-str/non-list values
        # (ints, dicts) rather than silently using dict keys as chains.
        return _normalize_quick_chains(chains_val)
    chain_val = quick_config.get("chain")
    # Treat non-string scalars as absent so a malformed `chain: 123` doesn't
    # blow up `c.lower()` downstream — the real config loader will surface a
    # proper error later.
    if isinstance(chain_val, str) and chain_val.strip():
        return [chain_val]
    return []


def _resolve_anvil_chains_and_funding(
    *,
    working_dir: str,
    config_file: str | None,
    early_strategy_class: type[IntentStrategy[Any]] | None,
    external_anvil_ports: dict[str, int],
) -> tuple[list[str], dict[str, float | int | str]]:
    """Resolve EVM chains needing Anvil forks (and their funding) for `--network anvil`."""
    # Import get_default_chain lazily from .run to avoid circular-import.
    from .run import get_default_chain

    anvil_chains: list[str] = []
    anvil_funding: dict[str, float | int | str] = {}

    config_path = _resolve_quick_config_path(working_dir, config_file)
    if config_path and config_path.exists():
        # Malformed config files must not crash gateway startup. Swallow
        # parse errors here and fall through with an empty dict; the
        # full loader later in `_discover_and_load_config` will surface
        # a proper user-facing error if the file is truly broken.
        quick_config = _quick_load_config_dict(config_path)
        anvil_chains = _chains_from_quick_config(quick_config)
        anvil_funding = _normalize_anvil_funding(quick_config.get("anvil_funding", {}))

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

    if not evm_anvil_chains and not solana_anvil:
        click.echo(
            "Warning: --network anvil specified but no chain found in config or decorator. "
            "Gateway will start without Anvil forks."
        )

    return evm_anvil_chains, anvil_funding


def _resolve_gateway_chains_for_mainnet(
    *,
    working_dir: str,
    config_file: str | None,
    early_strategy_class: type[IntentStrategy[Any]] | None,
) -> list[str]:
    """Resolve the chain list passed to the gateway for non-anvil networks."""
    from .run import get_default_chain

    chains: list[str] = []
    config_path = _resolve_quick_config_path(working_dir, config_file)
    if config_path and config_path.exists():
        # Same defensive treatment as the Anvil probe above: a malformed
        # config must not crash gateway startup at this early peek.
        chains = _chains_from_quick_config(_quick_load_config_dict(config_path))

    # Fall back to decorator metadata if config has no chain
    if not chains and early_strategy_class:
        decorator_chain = get_default_chain(early_strategy_class)
        if decorator_chain:
            chains = [decorator_chain]
    return chains


def _derive_isolated_wallet_or_none(
    *,
    wallet: str,
    gateway_network: str,
    working_dir: str,
    runtime_private_key: str | None = None,
) -> tuple[str | None, str | None]:
    """Derive an isolated wallet+key for `--wallet isolated`; otherwise return (None, None).

    Resolution order for the master key: caller-plumbed ``runtime_private_key``
    (or its contextvar fallback set by `almanak strat test`) > the typed
    ``GatewayConfig.private_key`` (populated from
    ``ALMANAK_PRIVATE_KEY`` via the ``_apply_gateway_env_fallbacks`` ladder).
    Honours the kwarg-first, no-env signing-key plumbing the rest of
    ``_setup_gateway`` documents (#2100).
    """
    if wallet != "isolated":
        return None, None
    if gateway_network != "anvil":
        raise click.ClickException("--wallet isolated is only supported with --network anvil")

    from almanak.gateway.managed import derive_isolated_wallet

    master_key = runtime_private_key
    if master_key is None:
        master_key = _runtime_private_key_override.get()
    if not master_key:
        # Read through the typed gateway config rather than directly
        # off ``os.environ`` so the config-boundary lint stays clean. The
        # ``_apply_gateway_env_fallbacks`` ladder still honours
        # ``ALMANAK_PRIVATE_KEY``.
        from almanak.config.env import gateway_config_from_env

        master_key = gateway_config_from_env().private_key or ""
    if not master_key:
        raise click.ClickException(
            "--wallet isolated requires a private key — pass `runtime_private_key=...` or set ALMANAK_PRIVATE_KEY"
        )
    # Use the strategy directory name as the derivation seed
    strategy_seed = Path(working_dir).resolve().name
    derived_key, isolated_wallet_address = derive_isolated_wallet(master_key, strategy_seed)
    # The derived key is plumbed via the runtime-config kwarg + gateway
    # private_key argument below — no os.environ mutation (#2100).
    click.echo(
        f"Wallet: isolated ({isolated_wallet_address[:10]}...{isolated_wallet_address[-4:]}) "
        f"[derived from strategy '{strategy_seed}']"
    )
    return isolated_wallet_address, derived_key


def _resolve_signing_key(*, isolated_wallet_private_key: str | None, runtime_private_key: str | None) -> str | None:
    """Resolve the effective signing key and publish it on `_runtime_private_key_override` (#2100).

    Order: derived isolated-wallet key > caller-plumbed `runtime_private_key` kwarg
    (or its contextvar fallback set by `almanak strat test`) > None for env fallback.
    GatewaySettings reads ALMANAK_GATEWAY_PRIVATE_KEY (not ALMANAK_PRIVATE_KEY) for its
    prefixed env source, so an explicit kwarg is the only way to feed it the unprefixed
    ALMANAK_PRIVATE_KEY equivalent the CLI just resolved. The resolved value is published
    on `_runtime_private_key_override` so the downstream `_build_runtime_config` reads
    the same key — no extra return-tuple slot needed (which would have flagged every
    line of the destructure inside `run` against the CRAP gate).
    """
    if runtime_private_key is None:
        runtime_private_key = _runtime_private_key_override.get()
    effective = isolated_wallet_private_key or runtime_private_key
    if effective is not None:
        _runtime_private_key_override.set(effective)
    return effective


def _build_gateway_settings(
    *,
    effective_host: str,
    gateway_port: int,
    gateway_network: str,
    gateway_chains: list[str],
    gateway_private_key: str | None,
) -> tuple[Any, str | None]:
    """Assemble `gateway_kwargs` and call `gateway_config_from_env`; returns (settings, session_token)."""
    import uuid

    from almanak.config.env import gateway_config_from_env

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
    # Route through the config service so unprefixed ALMANAK_* and
    # Polymarket-ladder fallbacks resolve identically to gateway boot.
    return gateway_config_from_env(**gateway_kwargs), session_auth_token


def _start_managed_gateway_and_connect(
    *,
    gateway_settings: Any,
    anvil_chains: list[str],
    isolated_wallet_address: str | None,
    anvil_funding: dict[str, float | int | str],
    external_anvil_ports: dict[str, int],
    keep_anvil: bool,
    effective_host: str,
    gateway_port: int,
    gateway_network: str,
    session_auth_token: str | None,
) -> tuple[Any, Any]:
    """Start the managed gateway, register atexit cleanup, and connect a client. Returns (client, managed)."""
    import atexit

    from almanak.gateway.managed import ManagedGateway

    from ..gateway_client import GatewayClient, GatewayClientConfig
    from ._anvil_timeout import compute_anvil_startup_timeout

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
    # Per-chain Anvil startup-timeout policy lives in _anvil_timeout.py
    # (VIB-3877) so the run + teardown CLI paths can never drift. See that
    # module for the cold-cache-fork-vs-gRPC-race rationale this guards.
    startup_timeout = compute_anvil_startup_timeout(anvil_chains)
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
    return gateway_client, managed_gateway


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
    runtime_private_key: str | None = None,
) -> tuple[Any, Any, str, int, str, str | None, str | None, type[IntentStrategy[Any]] | None]:
    """Set up the gateway (managed auto-start or connect to external).

    Orchestrates the gateway bootstrap that used to live inline in `run()`:
        - `--wallet isolated` derivation (returns a per-strategy derived
          key alongside the wallet address; the caller plumbs it into
          `_build_runtime_config` via the `runtime_private_key` kwarg
          instead of mutating `ALMANAK_PRIVATE_KEY`), with `--network anvil` guard.
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
        runtime_private_key: Optional caller-plumbed private key. When the
            ``--wallet isolated`` path does not run, this value (if non-None)
            is forwarded to the managed gateway as its ``private_key`` kwarg
            so the gateway sees the same key the runtime config will receive
            via ``_build_runtime_config``. Used by ``almanak strat test`` to
            inject ``ANVIL_DEFAULT_PRIVATE_KEY`` without mutating
            ``os.environ`` (#2100). The isolated-wallet derived key, when
            present, takes precedence over this argument because the
            funded-wallet identity must match what the gateway signs as.

    Returns:
        (gateway_client, managed_gateway, effective_host, gateway_port,
         gateway_network, session_auth_token, isolated_wallet_address,
         early_strategy_class).

        managed_gateway is None when `--no-gateway` was used.
        isolated_wallet_address is None unless `--wallet isolated` derived one.
        The precedence-resolved signing key (isolated-wallet derived key >
        caller-plumbed ``runtime_private_key`` > None for env fallback) is
        propagated to ``_build_runtime_config`` via the
        ``_runtime_private_key_override`` ContextVar (#2100). Returning it in
        the tuple kept the older helper contract but tripped the CRAP gate on
        every line of the destructure inside ``run`` — the ContextVar keeps
        the same end-to-end semantics with zero footprint in `run`.
        early_strategy_class is None if strategy.py wasn't present or failed
        to load (retried later by `_load_strategy_class`).
    """
    # VIB-3761: anchor ``ALMANAK_STRATEGY_FOLDER`` before any
    # downstream helper resolves a local path. Done here (not in ``run()``)
    # because every gateway-setup downstream — ``local_db_path``, the
    # managed gateway's SQLite store, ``_resolve_identity`` — reads the env
    # var; pinning it inside this allowlisted helper keeps the `run` body
    # free of CRAP-gated diff churn.
    _anchor_strategy_folder_env(working_dir)

    # Normalize "localhost" to "127.0.0.1" (gateway binds to 127.0.0.1)
    effective_host = "127.0.0.1" if gateway_host == "localhost" else gateway_host

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
        _validate_no_gateway_flags(no_gateway=no_gateway, anvil_ports=anvil_ports, wallet=wallet, keep_anvil=keep_anvil)
        return _attach_external_gateway(
            effective_host=effective_host,
            gateway_port=gateway_port,
            gateway_network=gateway_network,
            runtime_private_key=runtime_private_key,
        )

    # Default: auto-start a managed gateway
    gateway_port = _find_available_gateway_port_or_raise(effective_host, gateway_port)
    external_anvil_ports = _parse_anvil_port_overrides(anvil_ports)

    if keep_anvil and gateway_network != "anvil":
        click.echo("Warning: --keep-anvil has no effect without --network anvil or --anvil-port.")

    # Early-load strategy class so decorator metadata is available for chain detection.
    # This must happen before gateway startup so Anvil forks target the correct chain.
    early_strategy_class = _early_load_strategy_class(working_dir)

    # Determine which chains need Anvil forks
    anvil_chains: list[str] = []
    anvil_funding: dict[str, float | int | str] = {}
    if gateway_network == "anvil":
        anvil_chains, anvil_funding = _resolve_anvil_chains_and_funding(
            working_dir=working_dir,
            config_file=config_file,
            early_strategy_class=early_strategy_class,
            external_anvil_ports=external_anvil_ports,
        )

    # Wallet isolation: derive a unique wallet per strategy on Anvil
    isolated_wallet_address, isolated_wallet_private_key = _derive_isolated_wallet_or_none(
        wallet=wallet,
        gateway_network=gateway_network,
        working_dir=working_dir,
        runtime_private_key=runtime_private_key,
    )

    # Validate --reset-fork requires --network anvil
    if reset_fork and gateway_network != "anvil":
        raise click.ClickException("--reset-fork is only supported with --network anvil")
    if reset_fork and once:
        click.echo("Note: --reset-fork has no effect with --once (fork is already fresh at startup)")

    gateway_private_key = _resolve_signing_key(
        isolated_wallet_private_key=isolated_wallet_private_key, runtime_private_key=runtime_private_key
    )

    # Ensure gateway knows the strategy's chain for on-chain pricing.
    # For anvil mode, anvil_chains is already populated above.
    # For mainnet, read chain from config or decorator metadata so the MarketService
    # uses the correct Chainlink oracle chain instead of defaulting to arbitrum.
    gateway_chains = anvil_chains or _resolve_gateway_chains_for_mainnet(
        working_dir=working_dir, config_file=config_file, early_strategy_class=early_strategy_class
    )

    gateway_settings, session_auth_token = _build_gateway_settings(
        effective_host=effective_host,
        gateway_port=gateway_port,
        gateway_network=gateway_network,
        gateway_chains=gateway_chains,
        gateway_private_key=gateway_private_key,
    )

    gateway_client, managed_gateway = _start_managed_gateway_and_connect(
        gateway_settings=gateway_settings,
        anvil_chains=anvil_chains,
        isolated_wallet_address=isolated_wallet_address,
        anvil_funding=anvil_funding,
        external_anvil_ports=external_anvil_ports,
        keep_anvil=keep_anvil,
        effective_host=effective_host,
        gateway_port=gateway_port,
        gateway_network=gateway_network,
        session_auth_token=session_auth_token,
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
# Cleanup helper
# ---------------------------------------------------------------------------


def _build_cleanup_fn(
    *,
    gateway_client: Any,
    managed_gateway: Any,
    keep_anvil: bool,
    components: ComponentBundle,
) -> Callable[[], Coroutine[Any, Any, None]]:
    """Build an async cleanup closure that tears down all run-time resources.

    Returns a zero-arg async function that the caller awaits in the
    single-run and loop finally blocks. Preserves the original None-checks
    and hasattr-close checks exactly so that callers with partial component
    initialization don't blow up during shutdown.

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
# Strategy instantiation
# ---------------------------------------------------------------------------


def _instantiate_strategy(  # noqa: C901
    *,
    strategy_class: type,
    strategy_config: dict[str, Any],
    runtime_config: Any,
    multi_chain: bool,
    strategy_chains: list[str],
    chain_wallets: dict[str, str],
) -> Any:
    """Instantiate the strategy class with the right config-type branch.

    Two call conventions are supported:

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
                        runtime_fields = {"deployment_id", "chain", "wallet_address"}
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
                        # (runtime fields like deployment_id/chain are handled separately)
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

            # Apply a per-deployment ``quote_asset`` override from config.json, if
            # present. Definition-only: resolved + frozen here at boot; the SDK does
            # not branch on it. The @almanak_strategy decorator's quote_asset is the
            # default when config.json omits it.
            _qa_override = strategy_config.get("quote_asset")
            if _qa_override is not None and hasattr(strategy_instance, "apply_quote_asset_override"):
                strategy_instance.apply_quote_asset_override(_qa_override)
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
# Runtime config wiring
# ---------------------------------------------------------------------------


def _resolve_effective_signing_key(
    runtime_private_key: str | None,
    *,
    config_chain: str | None,
) -> str | None:
    """Apply kwarg-over-env precedence to surface the effective signing key (#2100).

    Used by ``_build_runtime_config`` to drive sidecar-vs-local dispatch with
    the same precedence the downstream ``from_env`` calls use. ``None`` means
    "no kwarg, fall through to env"; ``""`` is the documented force-empty
    override (treated as "no key" by callers, which keeps the legacy
    "empty value -> sidecar" semantic).

    Solana single-chain strategies use ``SOLANA_PRIVATE_KEY`` (base58 Ed25519)
    instead of ``ALMANAK_PRIVATE_KEY`` (hex secp256k1) as the canonical env
    var — mirroring the rule in
    ``almanak.config.runtime._resolve_private_key_from_env``. Without this
    branch, a Solana strategy with ``--no-gateway`` and only
    ``SOLANA_PRIVATE_KEY`` set would falsely take the sidecar branch even
    though ``runtime_config_from_env`` is fully able to load.
    """
    if runtime_private_key is not None:
        # Honour the explicit kwarg before touching the typed config — the
        # kwarg-first contract documented above must hold even when an
        # unrelated submodel would fail validation (PR #2152 review).
        return runtime_private_key

    # Narrow to ``gateway_config_from_env`` rather than ``load_config()`` so
    # a malformed unrelated submodel (backtest, cli, connectors) cannot block
    # signing-key resolution. ``GatewayConfig`` already carries the
    # ``ALMANAK_PRIVATE_KEY`` / ``SOLANA_PRIVATE_KEY`` canonical fallback ladder.
    from almanak.config.env import gateway_config_from_env

    _gw = gateway_config_from_env()
    # VIB-4803: route SVM chains through the ChainFamily adapter.
    from almanak.framework.chain_family import SvmFamily as _SvmFamily
    from almanak.framework.chain_family import family_for as _family_for

    if isinstance(_family_for((config_chain or "").strip()), _SvmFamily):
        # The typed ``GatewayConfig.solana_private_key`` carries
        # SOLANA_PRIVATE_KEY via the canonical env-fallback ladder; falling
        # back to ``private_key`` (ALMANAK_PRIVATE_KEY) preserves the legacy
        # "Solana strategy with hex key" path.
        return _gw.solana_private_key or _gw.private_key or None
    return _gw.private_key or None


def _resolve_runtime_private_key_kwarg(runtime_private_key: str | None) -> str | None:
    """Fall back to the ``_runtime_private_key_override`` ContextVar when no
    explicit kwarg was passed.

    Direct callers (tests, strategy harnesses) keep their kwarg-first semantics
    — passing ``None`` *intentionally* still falls through to the contextvar,
    matching the documented "no kwarg" path. Pass ``""`` to force the
    no-local-key branch; that empty string is preserved verbatim.
    """
    if runtime_private_key is not None:
        return runtime_private_key
    return _runtime_private_key_override.get()


def _build_sidecar_runtime_config(*, config_chain: str | None) -> Any:
    """Build the ``GatewayRuntimeConfig`` used in single-chain sidecar mode.

    Sidecar mode (``--no-gateway`` without a local private key) means the
    gateway handles all signing and RPC; the framework just needs a chain and
    the wallet address it resolves to. ``ALMANAK_SAFE_ADDRESS`` /
    ``ALMANAK_EOA_ADDRESS`` / ``ALMANAK_GATEWAY_WALLETS`` are checked in that
    order; the gateway-wallets path leaves ``wallet_address`` empty for the
    later ``register_chains()`` call to populate.
    """
    from almanak.core.chains import ChainRegistry

    from ..execution.config import GatewayRuntimeConfig
    from ..execution.gas.constants import DEFAULT_GAS_PRICE_CAP_GWEI

    if not config_chain:
        raise click.ClickException(
            "Chain must be specified in config.json or strategy decorator for sidecar deployment mode."
        )
    from almanak.config import cli_runtime_config_from_env as _cli_cfg

    _cli = _cli_cfg()
    safe_address = _cli.safe_address
    wallet_address = safe_address or _cli.eoa_address
    if not wallet_address and not _cli.gateway_wallets_configured:
        raise click.ClickException(
            "Sidecar mode (--no-gateway without ALMANAK_PRIVATE_KEY) requires "
            "ALMANAK_SAFE_ADDRESS, ALMANAK_EOA_ADDRESS, or ALMANAK_GATEWAY_WALLETS to be set."
        )
    wallet_address = wallet_address or ""
    descriptor = ChainRegistry.try_resolve(config_chain)
    default_gas_cap = (
        descriptor.gas.price_cap_gwei
        if descriptor is not None and descriptor.gas.price_cap_gwei is not None
        else DEFAULT_GAS_PRICE_CAP_GWEI
    )
    runtime_config = GatewayRuntimeConfig(
        chain=config_chain,
        wallet_address=wallet_address,
        is_safe=bool(safe_address),
        max_gas_price_gwei=default_gas_cap,
    )
    click.echo(f"Sidecar deployment mode: chain={config_chain}, wallet={wallet_address}")
    return runtime_config


def _echo_local_env_help() -> None:
    """User-facing help text shown when single-chain config loading fails."""
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


def _echo_multichain_env_help(strategy_chains: list[str]) -> None:
    """User-facing help text shown when multi-chain config loading fails."""
    click.echo("Required environment variables for multi-chain:")
    click.echo("  ALMANAK_PRIVATE_KEY          - Wallet private key")
    click.echo()
    click.echo("RPC access (one of these, or leave empty for free public RPCs):")
    for chain in strategy_chains:
        click.echo(f"  ALMANAK_{chain.upper()}_RPC_URL  - Per-chain RPC URL")
    click.echo("  RPC_URL                      - Generic RPC endpoint URL")
    click.echo("  ALCHEMY_API_KEY              - Alchemy API key (fallback)")


def _accept_anvil_default_wallet_or_exit() -> None:
    """Echo the Anvil-default wallet notice; honour ``isatty`` confirm prompt."""
    from .run import ANVIL_DEFAULT_ADDRESS

    click.echo(f"No ALMANAK_PRIVATE_KEY set. Using default Anvil wallet: {ANVIL_DEFAULT_ADDRESS}")
    if sys.stdin.isatty():
        if not click.confirm("Continue with this wallet?", default=True):
            sys.exit(0)
    else:
        click.echo("(non-interactive, accepting default Anvil wallet)")


def _load_local_runtime_config(
    *,
    config_chain: str | None,
    resolved_network: str,
    runtime_private_key: str | None,
) -> Any:
    """Build a ``LocalRuntimeConfig`` with Anvil-default fallback and verbose errors.

    Routes env reads through :func:`almanak.config.runtime.runtime_config_from_env`
    and converts to the dataclass shape via
    :meth:`LocalRuntimeConfig.from_runtime_config`.
    On ``MissingEnvironmentVariableError`` for ``PRIVATE_KEY`` while on
    Anvil, a second attempt plumbs ``ANVIL_DEFAULT_PRIVATE_KEY`` via the
    typed kwarg (#2100). Anything else exits with the canonical help text.
    """
    from almanak.config.runtime import (
        MissingEnvironmentVariableError,
        runtime_config_from_env,
    )

    from ..execution.config import LocalRuntimeConfig
    from .run import ANVIL_DEFAULT_PRIVATE_KEY

    try:
        rc = runtime_config_from_env(
            chain=config_chain,
            network=resolved_network,
            private_key=runtime_private_key,
        )
        return LocalRuntimeConfig.from_runtime_config(rc)
    except MissingEnvironmentVariableError as e:
        if resolved_network == "anvil" and e.var_name.endswith("PRIVATE_KEY"):
            _accept_anvil_default_wallet_or_exit()
            try:
                rc = runtime_config_from_env(
                    chain=config_chain,
                    network=resolved_network,
                    private_key=ANVIL_DEFAULT_PRIVATE_KEY,
                )
                return LocalRuntimeConfig.from_runtime_config(rc)
            except Exception as retry_err:
                click.echo(f"Error loading configuration after setting default key: {retry_err}", err=True)
                sys.exit(1)
        if e.var_name.endswith("PRIVATE_KEY"):
            click.echo("Error: ALMANAK_PRIVATE_KEY is required for mainnet execution.", err=True)
            click.echo("Set it in your .env file or environment.", err=True)
        else:
            click.echo(f"Error loading configuration: {e}", err=True)
            click.echo()
            _echo_local_env_help()
        sys.exit(1)
    except Exception as e:
        click.echo(f"Error loading configuration: {e}", err=True)
        click.echo()
        _echo_local_env_help()
        sys.exit(1)


def _load_multichain_runtime_config(
    *,
    strategy_chains: list[str],
    strategy_protocols: Any,
    resolved_network: str,
    runtime_private_key: str | None,
    no_gateway: bool,
) -> Any:
    """Build a ``MultiChainRuntimeConfig`` with Anvil-default fallback and verbose errors.

    Routes env reads through :func:`almanak.config.runtime.runtime_config_from_env`
    and converts to the dataclass shape via
    :meth:`MultiChainRuntimeConfig.from_runtime_config`. The Anvil-default
    retry and the multi-chain-sidecar guard match the legacy semantics
    (#2100).
    """
    from almanak.config.runtime import (
        MissingEnvironmentVariableError,
        runtime_config_from_env,
    )

    from ..execution.config import MultiChainRuntimeConfig
    from .run import ANVIL_DEFAULT_PRIVATE_KEY

    try:
        rc = runtime_config_from_env(
            chains=strategy_chains,
            protocols=strategy_protocols,
            network=resolved_network,
            private_key=runtime_private_key,
        )
        runtime_config = MultiChainRuntimeConfig.from_runtime_config(rc)
        click.echo(f"Multi-chain config loaded for: {', '.join(strategy_chains)}")
        return runtime_config
    except MissingEnvironmentVariableError as e:
        if resolved_network == "anvil" and e.var_name.endswith("PRIVATE_KEY"):
            _accept_anvil_default_wallet_or_exit()
            try:
                rc = runtime_config_from_env(
                    chains=strategy_chains,
                    protocols=strategy_protocols,
                    network=resolved_network,
                    private_key=ANVIL_DEFAULT_PRIVATE_KEY,
                )
                runtime_config = MultiChainRuntimeConfig.from_runtime_config(rc)
            except Exception as retry_err:
                click.echo(f"Error loading configuration after setting default key: {retry_err}", err=True)
                sys.exit(1)
            click.echo(f"Multi-chain config loaded for: {', '.join(strategy_chains)}")
            return runtime_config
        if no_gateway and e.var_name.endswith("PRIVATE_KEY"):
            click.echo(
                "Error: Multi-chain sidecar mode requires ALMANAK_GATEWAY_WALLETS or ALMANAK_PRIVATE_KEY.",
                err=True,
            )
            click.echo(
                "Set ALMANAK_GATEWAY_WALLETS with per-chain wallet config, or provide ALMANAK_PRIVATE_KEY.",
                err=True,
            )
            sys.exit(1)
        if e.var_name.endswith("PRIVATE_KEY"):
            click.echo("Error: ALMANAK_PRIVATE_KEY is required for mainnet execution.", err=True)
            click.echo("Set it in your .env file or environment.", err=True)
        else:
            click.echo(f"Error loading multi-chain configuration: {e}", err=True)
            click.echo()
            _echo_multichain_env_help(strategy_chains)
        sys.exit(1)
    except Exception as e:
        click.echo(f"Error loading multi-chain configuration: {e}", err=True)
        click.echo()
        _echo_multichain_env_help(strategy_chains)
        sys.exit(1)


def _register_chain_wallets(
    *,
    multi_chain: bool,
    strategy_chains: list[str],
    config_chain: str | None,
    gateway_client: Any,
    runtime_config: Any,
) -> dict[str, str]:
    """Register chains with the gateway and pin ``runtime_config.wallet_address``.

    Only meaningful when ``ALMANAK_GATEWAY_WALLETS`` is set; otherwise returns
    ``{}``. Mutates ``runtime_config.wallet_address`` to the gateway-resolved
    primary wallet so the runtime signs through the same identity the gateway
    uses for accounting.
    """
    from almanak.config import cli_runtime_config_from_env as _cli_cfg

    if not _cli_cfg().gateway_wallets_configured:
        return {}
    try:
        register_chain_list = strategy_chains if multi_chain else [str(config_chain)]
        chain_wallets = gateway_client.register_chains(register_chain_list)
        if chain_wallets:
            primary_chain = register_chain_list[0]
            primary_wallet = chain_wallets.get(primary_chain, "")
            runtime_config.wallet_address = primary_wallet
            unique_addrs = {v.lower() for v in chain_wallets.values()}
            if len(unique_addrs) <= 1:
                click.echo(
                    f"Gateway wallet registry: uniform wallet {primary_wallet[:12]}... on {len(chain_wallets)} chain(s)"
                )
            else:
                click.echo("Gateway wallet registry: non-uniform wallets")
                for ch, addr in chain_wallets.items():
                    click.echo(f"  {ch}: {addr}")
        return chain_wallets
    except Exception as e:
        click.secho(f"WARNING: register_chains() failed: {e}", fg="yellow", err=True)
        click.echo("Falling back to legacy wallet resolution.", err=True)
        logger.warning("register_chains() failed: %s", e)
        return {}


def _apply_strategy_config_chain(
    *,
    strategy_config: dict[str, Any],
    multi_chain: bool,
    strategy_chains: list[str],
    runtime_config: Any,
) -> None:
    """Inject ``chain`` into ``strategy_config`` (mutating).

    Sources, in order: existing ``chain`` field (with ``ALMANAK_CHAIN`` env
    override applied for single-chain strategies — keeps the strategy class's
    MarketSnapshot/balance lookups in sync with the runtime); else first
    declared chain when multi-chain; else ``runtime_config.chain``.
    """
    from ..execution.config import GatewayRuntimeConfig, LocalRuntimeConfig

    if "chain" not in strategy_config:
        if multi_chain:
            strategy_config["chain"] = strategy_chains[0]
        else:
            assert isinstance(runtime_config, LocalRuntimeConfig | GatewayRuntimeConfig)
            strategy_config["chain"] = runtime_config.chain
        return
    if multi_chain:
        return
    # ``ALMANAK_CHAIN`` is the canonical single-chain override; the
    # runtime-config layer reads it via ``runtime_config_from_env`` already.
    # Here we only need the raw value to compare against the strategy
    # config's own ``chain`` field; reading ``os.environ`` would re-introduce
    # the boundary lint hit. Source the value through the runtime-config
    # factory's same lookup path by going through the canonical helper.
    from almanak.config.cli_runtime import _almanak_chain_env

    env_chain = _almanak_chain_env()
    if not env_chain:
        return
    existing = strategy_config.get("chain")
    existing_norm = existing.strip().lower() if isinstance(existing, str) else ""
    if existing_norm != env_chain:
        strategy_config["chain"] = env_chain


def _apply_strategy_config_wallet(
    *,
    strategy_config: dict[str, Any],
    multi_chain: bool,
    strategy_chains: list[str],
    config_chain: str | None,
    runtime_config: Any,
    chain_wallets: dict[str, str],
) -> None:
    """Inject ``wallet_address`` into ``strategy_config`` (mutating).

    Runtime-resolved wallet wins (see #1684) so a stale ``wallet_address`` in
    ``config.json`` never drives ``deployment_id`` when the runtime is signing
    from a different identity. Prefers the gateway-registered chain wallet
    when ``ALMANAK_GATEWAY_WALLETS`` was set.
    """
    if chain_wallets:
        primary = strategy_chains[0] if multi_chain else str(config_chain)
        resolved_wallet = chain_wallets.get(primary, runtime_config.execution_address)
    else:
        resolved_wallet = runtime_config.execution_address
    if resolved_wallet:
        strategy_config["wallet_address"] = resolved_wallet


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
    runtime_private_key: str | None = None,
) -> tuple[Any, dict[str, str]]:
    """Build the runtime config (Local / MultiChain / Gateway) and register chains.

    Three-way dispatch over the loader helpers
    (``_build_sidecar_runtime_config`` / ``_load_multichain_runtime_config`` /
    ``_load_local_runtime_config``) plus Safe-mode preflight, gateway
    chain-wallet registration, and ``strategy_config`` mutations.

    Args:
        runtime_private_key: Optional explicit private key (kwarg-only). When
            non-None, plumbed into the underlying ``from_env(private_key=...)``
            call so the kwarg wins over env (#2100). Empty-string forces the
            sidecar dispatch path identically to an unset env var. When None,
            falls back to the ``_runtime_private_key_override`` ContextVar set
            by ``almanak strat test`` and by ``_setup_gateway`` after isolated-
            wallet derivation.

    Returns:
        ``(runtime_config, chain_wallets)`` — empty dict when
        ALMANAK_GATEWAY_WALLETS is not set.
    """
    from .run import _validate_safe_mode_preflight

    runtime_private_key = _resolve_runtime_private_key_kwarg(runtime_private_key)
    effective_private_key = _resolve_effective_signing_key(
        runtime_private_key,
        config_chain=config_chain,
    )

    if no_gateway and not effective_private_key and not multi_chain:
        runtime_config = _build_sidecar_runtime_config(config_chain=config_chain)
    elif multi_chain:
        runtime_config = _load_multichain_runtime_config(
            strategy_chains=strategy_chains,
            strategy_protocols=strategy_protocols,
            resolved_network=resolved_network,
            runtime_private_key=runtime_private_key,
            no_gateway=no_gateway,
        )
    else:
        runtime_config = _load_local_runtime_config(
            config_chain=config_chain,
            resolved_network=resolved_network,
            runtime_private_key=runtime_private_key,
        )

    # Safe-mode preflight only when the CLI manages the gateway (env vars
    # are local). Skip when ALMANAK_GATEWAY_WALLETS is set — the gateway's
    # WalletRegistry handles signer configuration per chain.
    from almanak.config import cli_runtime_config_from_env as _cli_cfg_for_wallets

    gateway_wallets_configured = _cli_cfg_for_wallets().gateway_wallets_configured
    if runtime_config.is_safe_mode and not no_gateway and not gateway_wallets_configured:
        error = _validate_safe_mode_preflight(runtime_config.execution_address)
        if error:
            click.secho(f"ERROR: {error}", fg="red", err=True)
            sys.exit(1)

    chain_wallets = _register_chain_wallets(
        multi_chain=multi_chain,
        strategy_chains=strategy_chains,
        config_chain=config_chain,
        gateway_client=gateway_client,
        runtime_config=runtime_config,
    )
    _apply_strategy_config_chain(
        strategy_config=strategy_config,
        multi_chain=multi_chain,
        strategy_chains=strategy_chains,
        runtime_config=runtime_config,
    )
    _apply_strategy_config_wallet(
        strategy_config=strategy_config,
        multi_chain=multi_chain,
        strategy_chains=strategy_chains,
        config_chain=config_chain,
        runtime_config=runtime_config,
        chain_wallets=chain_wallets,
    )
    return runtime_config, chain_wallets


# ---------------------------------------------------------------------------
# Component initialization
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


# crap-allowlist: #2097 replaces direct os.environ.get reads with the typed
# cli_runtime_config_from_env() — no new branches, no new behaviour. Function refactor
# is tracked separately; allowlist matches the documented escape hatch for this
# config-boundary cutover.
def _build_orchestrator_and_providers(  # noqa: C901
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
    from ..data.ohlcv import create_ohlcv_stack
    from ..data.price.gateway_oracle import GatewayPriceOracle
    from ..execution.multichain import MultiChainOrchestrator
    from .run import (
        _get_orca_pool_accounts,
        _init_prediction_provider,
        _wire_core_providers,
        _wire_indicators,
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
            ohlcv_stack = create_ohlcv_stack(
                gateway_client=gateway_client,
                chain=strategy_chains[0],
                pool_address=strategy_config.get("pool_address") if strategy_config else None,
            )
            ohlcv_provider = ohlcv_stack.provider
            # VIB-4347: stamp the sync OHLCVRouter on the strategy so
            # ``MarketSnapshot.ohlcv(...)`` resolves to the same routed gateway-backed
            # pipes the indicator path already uses. Shared router instance = shared
            # disk cache + TTL.
            strategy_instance._ohlcv_router = ohlcv_stack.router
            _wire_indicators(strategy_instance, ohlcv_provider, price_oracle, balance_provider)
        elif requirements.price or requirements.balance:
            # indicators=False: wire price/balance directly without OHLCV or indicator calculators
            _wire_core_providers(strategy_instance, price_oracle, balance_provider)

        # MarketSnapshot needs the gateway client to do gateway-routed eth_calls
        # (e.g. position_health). Wire it unconditionally; methods that need it
        # check for None at call time.
        strategy_instance._gateway_client = gateway_client

        rate_monitor_wired = False
        if requirements.lending_rates:
            try:
                from ..data.rates import RateMonitor

                primary_chain = strategy_chains[0]
                chain_rpc_url = runtime_config.rpc_urls.get(primary_chain)
                # _internal=True: framework wiring of the gateway-backed rate
                # source onto MarketSnapshot is the canonical lending-rate lane,
                # not a deprecated strategy-side bypass (VIB-4869).
                rate_monitor = RateMonitor(chain=primary_chain, rpc_url=chain_rpc_url, _internal=True)
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

        # For Solana + --network anvil, start local solana-test-validator.
        # VIB-4803: route through the ChainFamily adapter.
        from almanak.framework.chain_family import SvmFamily as _SvmFamily
        from almanak.framework.chain_family import family_for as _family_for

        if isinstance(_family_for(runtime_config.chain), _SvmFamily) and resolved_network == "anvil":
            from almanak.config import cli_runtime_config_from_env as _solana_cli_cfg

            from ..anvil.solana_fork_manager import SolanaForkManager

            _solana_cli = _solana_cli_cfg()
            solana_rpc_url = _solana_cli.solana_rpc_url
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
                validator_port=_solana_cli.solana_validator_port,
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
            ohlcv_stack = create_ohlcv_stack(
                gateway_client=gateway_client,
                chain=runtime_config.chain,
                pool_address=strategy_config.get("pool_address") if strategy_config else None,
            )
            ohlcv_provider = ohlcv_stack.provider
            # VIB-4347: stamp the sync OHLCVRouter on the strategy so
            # ``MarketSnapshot.ohlcv(...)`` resolves to the same routed gateway-backed
            # pipes the indicator path already uses. Shared router instance = shared
            # disk cache + TTL.
            strategy_instance._ohlcv_router = ohlcv_stack.router
            _wire_indicators(strategy_instance, ohlcv_provider, price_oracle, balance_provider)
        elif requirements.price or requirements.balance:
            # indicators=False: wire price/balance directly without OHLCV or indicator calculators
            _wire_core_providers(strategy_instance, price_oracle, balance_provider)

        # Initialize prediction market provider for strategies that explicitly
        # declare polymarket support. Non-polymarket strategies skip this
        # entirely (including Polygon runs) to avoid irrelevant warnings.
        if hasattr(strategy_instance, "_prediction_provider"):
            _init_prediction_provider(strategy_instance, chain=runtime_config.chain, gateway_client=gateway_client)

        # MarketSnapshot needs the gateway client to do gateway-routed eth_calls
        # (e.g. position_health). Wire it unconditionally; methods that need it
        # check for None at call time.
        strategy_instance._gateway_client = gateway_client

        rate_monitor_wired = False
        if requirements.lending_rates:
            try:
                from ..data.rates import RateMonitor

                rpc_url = getattr(runtime_config, "rpc_url", None)
                # _internal=True: framework wiring of the gateway-backed rate
                # source onto MarketSnapshot is the canonical lending-rate lane,
                # not a deprecated strategy-side bypass (VIB-4869).
                rate_monitor = RateMonitor(chain=runtime_config.chain, rpc_url=rpc_url, _internal=True)
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


def _init_copy_trading(  # noqa: C901
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

    from almanak.connectors._strategy_base.contract_registry import get_default_registry

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
    deployment_id: str,
) -> Any:
    """Return a VaultLifecycleManager or None, auto-deploying on Anvil if placeholder.

    Mirrors the prior vault-lifecycle block in `run()`. When
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

    from ..vault.capability import default_vault_protocol, get_vault_tool_capability
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
    vault_protocol = default_vault_protocol()
    vault_capability = get_vault_tool_capability(vault_protocol)
    vault_sdk = vault_capability.build_sdk(gateway_client, vault_chain)
    vault_adapter = vault_capability.build_adapter(vault_sdk)

    # Extract initial vault state from persisted strategy state.
    # State loading is deferred to the async phase for IntentStrategy, so we
    # load the raw state here directly from the state manager (safe to use
    # asyncio.run() because we are still in the sync Click command, before any
    # event loop is started).
    initial_vault_state = None
    try:
        import asyncio as _asyncio

        _raw_state_data = _asyncio.run(state_manager.load_state(deployment_id))
        if _raw_state_data and _raw_state_data.state:
            initial_vault_state = _raw_state_data.state.get(VAULT_STATE_KEY)
    except Exception as _e:  # noqa: BLE001
        logger.debug("Could not load persisted state for vault init (deployment_id=%s): %s", deployment_id, _e)
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
        deployment_id=deployment_id,
        initial_vault_state=initial_vault_state,
        persistence_callback=_persist_vault_state,
        receipt_parser_protocol=vault_protocol,
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
    empty, ``0``, ``false``, arbitrary strings — returns False. Read via the
    typed CLI-runtime config during the config-service cutover.
    """
    from almanak.config import cli_runtime_config_from_env as _cli_cfg

    return _cli_cfg().reconciliation_enforcement


def _build_runner(
    *,
    interval: int,
    effective_dry_run: bool,
    deployment_id: str,
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
    circuit_breaker = CircuitBreaker(deployment_id=deployment_id)
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
    deployment_id: str,
    normalized_copy_mode: str | None,
    copy_replay_file: str | None,
    copy_shadow: bool,
    copy_strict: bool,
    config_chain: str | None,
) -> ComponentBundle:
    """Construct the full runtime component bundle (orchestrator -> runner).

    Internal ordering is load-bearing:

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
            strategy_instance.set_state_manager(state_manager, deployment_id)

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
            deployment_id=deployment_id,
        )

        _build_runner(
            interval=interval,
            effective_dry_run=effective_dry_run,
            deployment_id=deployment_id,
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
# Dashboard helpers
# ---------------------------------------------------------------------------


def _build_dashboard_subprocess_env(
    *,
    gateway_host: str,
    gateway_port: int,
    auth_token: str | None,
    mode: str,
    deployment_id: str | None,
    strategy_working_dir: str | None,
    strategy_config: dict[str, Any] | None,
) -> dict[str, str]:
    """Build the env mapping handed to the dashboard subprocess.

    Encapsulates gateway-connection forwarding (host/port/auth-token,
    plus the stale-``GATEWAY_AUTH_TOKEN`` strip from VIB-520) AND the
    hosted-parity scoping channel (deployment_id, working_dir, and the
    optional pre-resolved runtime config). Extracted out of
    ``_start_dashboard_background`` so that function stays under the
    CRAP complexity cap as additional env channels are added.
    """
    import json as _json

    from almanak.config.cli_runtime import subprocess_env_with_overrides

    overrides: dict[str, str] = {
        "GATEWAY_HOST": gateway_host,
        "GATEWAY_PORT": str(gateway_port),
    }
    if auth_token:
        overrides["ALMANAK_GATEWAY_AUTH_TOKEN"] = auth_token

    if mode == "hosted-parity" and deployment_id and strategy_working_dir:
        # Tell app_single.py which strategy to scope to and where to find
        # its ``dashboard/ui.py`` and ``config.json``. The dashboard reads
        # these from os.environ; do NOT rely on cwd because Streamlit's
        # child cwd is not the strategy's working dir.
        overrides["ALMANAK_DASHBOARD_DEPLOYMENT_ID"] = deployment_id
        overrides["ALMANAK_DASHBOARD_WORKING_DIR"] = str(Path(strategy_working_dir).resolve())
        # Forward the RESOLVED + MUTATED runtime config (post-bootstrap)
        # so the dashboard sees the same values the running strategy sees
        # — covers ``--config`` pointing outside working_dir AND copy-
        # trading / chain runtime overrides AND the resolved deployment_id
        # field. Without this, app_single re-reads working_dir/config.json
        # and renders stale values (Codex P2 on PR #2372).
        if strategy_config is not None:
            try:
                # ``default=str`` so Decimal / datetime / Path / etc. in the
                # strategy_config serialise to a string rather than crashing
                # the subprocess at boot (strategy configs frequently carry
                # Decimal for range bounds, fee tiers, target_ltv, …).
                # Lossy: the dashboard receives strings, not the typed
                # objects — but the alternative (TypeError → fall back to
                # stale on-disk config) is worse.
                overrides["ALMANAK_DASHBOARD_STRATEGY_CONFIG"] = _json.dumps(strategy_config, default=str)
            except (TypeError, ValueError):
                logger.warning(
                    "Failed to serialise strategy_config for dashboard subprocess; "
                    "app_single will fall back to working_dir/config.json (may be stale)."
                )

    env = subprocess_env_with_overrides(overrides)
    if auth_token:
        # Drop the legacy unprefixed shape so a stale .env value can't shadow
        # the session token in the spawned child (VIB-520).
        env.pop("GATEWAY_AUTH_TOKEN", None)
    return env


def _start_dashboard_background(
    *,
    port: int,
    gateway_host: str = "127.0.0.1",
    gateway_port: int = 50051,
    auth_token: str | None = None,
    mode: str = "command-center",
    deployment_id: str | None = None,
    strategy_working_dir: str | None = None,
    strategy_config: dict[str, Any] | None = None,
) -> Any:
    """Launch the Streamlit dashboard as a background subprocess.

    Mirrors the nested ``start_dashboard_background`` previously defined
    inside ``run()``. Behavior-preserving for ``mode == "command-center"``:
    probes the requested port with a transient socket bind, falls back to
    8502-8509 if busy, and returns ``None`` on any launch failure (no
    streamlit, spawn error, no free port).

    Args:
        port: The requested dashboard port.
        gateway_host: Gateway host for the dashboard env (GATEWAY_HOST).
        gateway_port: Gateway port for the dashboard env (GATEWAY_PORT).
        auth_token: Managed-gateway session token, exported in the
            subprocess env as ``ALMANAK_GATEWAY_AUTH_TOKEN`` so the
            dashboard's ``GatewayClient`` authenticates against the same
            ephemeral token the managed gateway is enforcing on mainnet.
            Without forwarding, the subprocess inherits whatever happens
            to be in the parent's env (``ALMANAK_GATEWAY_AUTH_TOKEN`` /
            ``GATEWAY_AUTH_TOKEN`` from a stale ``.env``) — but on
            mainnet the managed gateway always rolls a fresh
            ``uuid.uuid4().hex`` (VIB-520), so the inherited value never
            matches and every dashboard gRPC call returns UNAUTHENTICATED.
        mode: ``"hosted-parity"`` (single-strategy, mirrors hosted image —
            ``app_single.py``) or ``"command-center"`` (multi-strategy
            navigation — ``app.py``). Hosted-parity requires
            ``deployment_id`` and ``strategy_working_dir``.
        deployment_id: Resolved deployment_id the dashboard scopes to. Required
            for ``mode == "hosted-parity"``; ignored otherwise.
        strategy_working_dir: Strategy folder containing ``config.json`` and
            (optionally) ``dashboard/ui.py``. Required for
            ``mode == "hosted-parity"``; ignored otherwise.
        strategy_config: Resolved + mutated runtime strategy config dict
            (post ``_load_strategy_bootstrap`` / ``_prepare_runtime_bootstrap``,
            so it reflects ``--config`` overrides AND copy-trading flags AND
            the resolved ``deployment_id``). Serialized to JSON and exported
            as ``ALMANAK_DASHBOARD_STRATEGY_CONFIG``. The dashboard prefers
            this over re-reading ``working_dir/config.json`` so custom
            dashboards see the same config the running strategy sees
            (Codex P2 on PR #2372 — fixes the case where ``--config`` points
            outside ``working_dir`` or runtime overrides have mutated the
            config since startup).

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
    dashboard_dir = project_root / "almanak" / "framework" / "dashboard"
    if mode == "hosted-parity":
        if not deployment_id or not strategy_working_dir:
            click.echo(
                "Error: hosted-parity dashboard requires deployment_id and "
                "strategy_working_dir; falling back to Command Center.",
                err=True,
            )
            dashboard_path = dashboard_dir / "app.py"
            mode = "command-center"
        else:
            dashboard_path = dashboard_dir / "app_single.py"
    elif mode == "command-center":
        dashboard_path = dashboard_dir / "app.py"
    else:
        click.echo(f"Error: unknown dashboard mode {mode!r}", err=True)
        return None

    # Build the subprocess env (gateway connection + hosted-parity scoping
    # if applicable). Extracted to a helper to keep this function under
    # the CRAP complexity cap as new env channels are added.
    env = _build_dashboard_subprocess_env(
        gateway_host=gateway_host,
        gateway_port=gateway_port,
        auth_token=auth_token,
        mode=mode,
        deployment_id=deployment_id,
        strategy_working_dir=strategy_working_dir,
        strategy_config=strategy_config,
    )

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
    auth_token: str | None = None,
    dashboard_mode: str = "command-center",
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
        auth_token: Managed-gateway session token, forwarded to the
            dashboard subprocess so it authenticates against the same
            ephemeral token the managed gateway is enforcing.

    Returns:
        ``True`` if the branch handled the request (caller must ``return``),
        ``False`` otherwise.
    """
    if not (dashboard and working_dir == "."):
        return False

    # Standalone dashboard (no strategy directory) always opens Command
    # Center — hosted-parity scoping requires a deployment id/dir context
    # that doesn't exist here. If the operator explicitly passed
    # ``--dashboard-mode=hosted-parity``, surface a one-line warning so
    # they know their flag was overridden rather than silently ignored
    # (Claude pr-auditor Important #3 on PR #2372).
    if dashboard_mode.lower() == "hosted-parity":
        click.echo(
            "Warning: --dashboard-mode=hosted-parity ignored in standalone mode "
            "(no strategy context). Opening Command Center.",
            err=True,
        )

    click.echo()
    click.echo("=" * 60)
    click.echo("LAUNCHING DASHBOARD (standalone mode)")
    click.echo("=" * 60)
    click.echo("Press Ctrl+C to stop")
    proc = _start_dashboard_background(
        port=dashboard_port,
        gateway_host=gateway_host,
        gateway_port=gateway_port,
        auth_token=auth_token,
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
# Strategy-run orchestration
# ---------------------------------------------------------------------------


def _maybe_handle_run_early_exit(
    *,
    list_all: bool,
    gateway_client: Any,
    working_dir: str,
    dashboard: bool,
    dashboard_port: int,
    gateway_host: str,
    gateway_port: int,
    auth_token: str | None,
    dashboard_mode: str = "command-center",
) -> bool:
    """Handle early-return `run()` branches before strategy bootstrap."""
    if _handle_list_all(list_all, gateway_client):
        return True

    return _handle_standalone_dashboard(
        working_dir=working_dir,
        dashboard=dashboard,
        dashboard_port=dashboard_port,
        gateway_host=gateway_host,
        gateway_port=gateway_port,
        auth_token=auth_token,
        dashboard_mode=dashboard_mode,
    )


def _maybe_start_dashboard_process(
    *,
    dashboard: bool,
    dashboard_port: int,
    gateway_host: str,
    gateway_port: int,
    auth_token: str | None,
    mode: str = "command-center",
    deployment_id: str | None = None,
    strategy_working_dir: str | None = None,
    strategy_config: dict[str, Any] | None = None,
) -> Any:
    """Start the dashboard sidecar when requested, registering cleanup.

    See ``_start_dashboard_background`` for the meaning of ``mode``,
    ``deployment_id``, ``strategy_working_dir``, and ``strategy_config``
    — they're forwarded verbatim and only consulted when
    ``mode == "hosted-parity"``.
    """
    import atexit

    if not dashboard:
        return None

    dashboard_process = _start_dashboard_background(
        port=dashboard_port,
        gateway_host=gateway_host,
        gateway_port=gateway_port,
        auth_token=auth_token,
        mode=mode,
        deployment_id=deployment_id,
        strategy_working_dir=strategy_working_dir,
        strategy_config=strategy_config,
    )
    if dashboard_process is not None:
        atexit.register(_stop_dashboard, dashboard_process)
    return dashboard_process


def _load_strategy_bootstrap(
    *,
    working_dir: str,
    config_file: str | None,
    copy_mode: str | None,
    copy_shadow: bool,
    copy_replay_file: str | None,
    copy_strict: bool,
    dry_run: bool,
    early_strategy_class: Any,
) -> StrategyBootstrap:
    """Load strategy class, config, and resolved chain metadata for `run()`."""
    from .run import get_strategy_chains, get_strategy_protocols

    strategy_class = _load_strategy_class(working_dir, early_strategy_class)
    strategy_name = strategy_class.__name__
    click.echo(f"Loaded strategy: {strategy_name}")

    strategy_chains = get_strategy_chains(strategy_class)
    strategy_protocols = get_strategy_protocols(strategy_class)

    (
        strategy_config,
        multi_chain,
        effective_dry_run,
        resolved_config_file,
        normalized_copy_mode,
    ) = _discover_and_load_config(
        working_dir=working_dir,
        config_file=config_file,
        strategy_class=strategy_class,
        copy_mode=copy_mode,
        copy_shadow=copy_shadow,
        copy_replay_file=copy_replay_file,
        copy_strict=copy_strict,
        dry_run=dry_run,
    )
    strategy_chains = _refine_strategy_chains(
        strategy_chains=strategy_chains,
        strategy_config=strategy_config,
        multi_chain=multi_chain,
    )
    config_display_name = _normalize_strategy_display_name(raw_name=strategy_config.get("deployment_id", strategy_name))
    strategy_config["strategy_display_name"] = config_display_name

    return StrategyBootstrap(
        strategy_class=strategy_class,
        strategy_name=strategy_name,
        strategy_config=strategy_config,
        multi_chain=multi_chain,
        config_file=resolved_config_file,
        normalized_copy_mode=normalized_copy_mode,
        strategy_chains=strategy_chains,
        strategy_protocols=strategy_protocols,
        config_display_name=config_display_name,
        effective_dry_run=effective_dry_run,
    )


def _refine_strategy_chains(
    *,
    strategy_chains: list[str],
    strategy_config: dict[str, Any],
    multi_chain: bool,
) -> list[str]:
    """Use config-specified chains when a multi-chain strategy provides them."""
    if not multi_chain:
        return strategy_chains

    config_chains = strategy_config.get("chains", [])
    if isinstance(config_chains, list) and len(config_chains) > 1:
        return config_chains
    return strategy_chains


def _normalize_strategy_display_name(*, raw_name: Any) -> str:
    """Strip any persisted deployment-id suffix from the display name."""
    name = "" if raw_name is None else str(raw_name)
    return name.split(":", 1)[0]


def _maybe_echo_chain_override(
    *,
    env_chain: str | None,
    config_chain: str | None,
    config_chain_norm: str,
    config_chain_raw: Any,
) -> None:
    """Emit the env-over-config chain banner only when the env actually won."""
    if env_chain != config_chain:
        return
    if env_chain == config_chain_norm:
        return
    click.echo(f"Chain override: ALMANAK_CHAIN={env_chain} (config.json: {config_chain_raw or 'unset'})")


def _resolve_config_chain_with_echo(
    *,
    strategy_class: Any,
    strategy_config: dict[str, Any],
    multi_chain: bool,
) -> str | None:
    """Resolve the effective chain context and preserve override echoing."""
    from .run import resolve_strategy_chain

    env_chain = almanak_chain_from_env()
    config_chain = resolve_strategy_chain(
        strategy_class,
        strategy_config,
        env_chain=env_chain,
        multi_chain=multi_chain,
    )
    config_chain_raw = strategy_config.get("chain")
    config_chain_norm = config_chain_raw.strip().lower() if isinstance(config_chain_raw, str) else ""
    if env_chain:
        _maybe_echo_chain_override(
            env_chain=env_chain,
            config_chain=config_chain,
            config_chain_norm=config_chain_norm,
            config_chain_raw=config_chain_raw,
        )

    return config_chain


def _echo_anvil_network_banner(*, config_chain: str | None) -> None:
    """Echo the local fork endpoint when `run()` targets Anvil."""
    anvil_port = anvil_port_for_chain(config_chain or "arbitrum") or 8545
    click.echo(f"Network: ANVIL (local fork at http://127.0.0.1:{anvil_port})")


def _resolve_network_with_echo(*, network: str | None, config_chain: str | None) -> str:
    """Resolve the effective network and preserve the Anvil banner."""
    resolved_network = "mainnet"
    if network:
        resolved_network = network
    if resolved_network == "anvil":
        _echo_anvil_network_banner(config_chain=config_chain)
    return resolved_network


def _prepare_runtime_bootstrap(
    *,
    strategy_bootstrap: StrategyBootstrap,
    no_gateway: bool,
    network: str | None,
    gateway_client: Any,
    gateway_network: str,
    fresh: bool,
) -> RuntimeBootstrap:
    """Resolve runtime config and stable identity for `run()`."""
    config_chain = _resolve_config_chain_with_echo(
        strategy_class=strategy_bootstrap.strategy_class,
        strategy_config=strategy_bootstrap.strategy_config,
        multi_chain=strategy_bootstrap.multi_chain,
    )
    resolved_network = _resolve_network_with_echo(
        network=network,
        config_chain=config_chain,
    )
    runtime_config, chain_wallets = _build_runtime_config(
        no_gateway=no_gateway,
        multi_chain=strategy_bootstrap.multi_chain,
        resolved_network=resolved_network,
        config_chain=config_chain,
        strategy_chains=strategy_bootstrap.strategy_chains,
        strategy_protocols=strategy_bootstrap.strategy_protocols,
        gateway_client=gateway_client,
        strategy_config=strategy_bootstrap.strategy_config,
    )
    identity_info = _resolve_identity(
        strategy_config=strategy_bootstrap.strategy_config,
        fresh=fresh,
        multi_chain=strategy_bootstrap.multi_chain,
        strategy_chains=strategy_bootstrap.strategy_chains,
        config_display_name=strategy_bootstrap.config_display_name,
        gateway_network=gateway_network,
    )
    return RuntimeBootstrap(
        config_chain=config_chain,
        resolved_network=resolved_network,
        runtime_config=runtime_config,
        chain_wallets=chain_wallets,
        deployment_id=strategy_bootstrap.strategy_config["deployment_id"],
        run_id=identity_info.run_id,
    )


def _load_resume_state(
    *,
    deployment_id: str,
) -> tuple[bool, dict[str, Any] | None]:
    """Load local SQLite resume metadata when the deployment mode is local."""
    from almanak.framework.deployment import is_local
    from almanak.framework.local_paths import local_db_path as _local_db_path

    if not is_local():
        return False, None

    state_db_path = _local_db_path()
    resume_info = _detect_state_resume(state_db_path, deployment_id)
    if not resume_info.is_resume:
        return False, None

    return True, {"version": resume_info.version, "keys": resume_info.state_keys}


def _echo_strategy_runtime_summary(
    *,
    strategy_class: Any,
    multi_chain: bool,
    strategy_chains: list[str],
) -> None:
    """Emit the final strategy-class summary banner."""
    click.echo(f"Strategy class loaded: {strategy_class.__name__}")
    if multi_chain:
        click.echo(f"  Multi-chain: Yes ({len(strategy_chains)} chains)")


def _cleanup_after_dry_run_vault_exit(
    *,
    gateway_client: Any,
    managed_gateway: Any,
    keep_anvil: bool,
    components: Any,
    dashboard_process: Any,
) -> NoReturn:
    """Unwind resources for the intentional dry-run vault early-exit path."""
    early_cleanup = _build_cleanup_fn(
        gateway_client=gateway_client,
        managed_gateway=managed_gateway,
        keep_anvil=keep_anvil,
        components=components,
    )
    try:
        asyncio.run(early_cleanup())
    except Exception:  # pragma: no cover - cleanup best-effort
        logger.exception("Cleanup failed during dry-run vault early exit")
    _stop_dashboard(dashboard_process)
    sys.exit(0)


def _build_components_or_exit(
    *,
    strategy_instance: Any,
    strategy_config: dict[str, Any],
    runtime_config: Any,
    strategy_chains: list[str],
    multi_chain: bool,
    resolved_network: str,
    gateway_client: Any,
    chain_wallets: Any,
    interval: int,
    effective_dry_run: bool,
    deployment_id: str,
    normalized_copy_mode: str | None,
    copy_replay_file: str | None,
    copy_shadow: bool,
    copy_strict: bool,
    config_chain: str | None,
    managed_gateway: Any,
    keep_anvil: bool,
    dashboard_process: Any,
) -> Any:
    """Build run-time components, preserving the dry-run vault early-exit path."""
    try:
        return _build_components(
            strategy_instance=strategy_instance,
            strategy_config=strategy_config,
            runtime_config=runtime_config,
            strategy_chains=strategy_chains,
            multi_chain=multi_chain,
            resolved_network=resolved_network,
            gateway_client=gateway_client,
            chain_wallets=chain_wallets,
            interval=interval,
            effective_dry_run=effective_dry_run,
            deployment_id=deployment_id,
            normalized_copy_mode=normalized_copy_mode,
            copy_replay_file=copy_replay_file,
            copy_shadow=copy_shadow,
            copy_strict=copy_strict,
            config_chain=config_chain,
        )
    except _DryRunVaultEarlyExit as early:
        partial_components = early.components or ComponentBundle()
        _cleanup_after_dry_run_vault_exit(
            gateway_client=gateway_client,
            managed_gateway=managed_gateway,
            keep_anvil=keep_anvil,
            components=partial_components,
            dashboard_process=dashboard_process,
        )


def _execute_run_mode(
    *,
    test_actions: list[str] | None,
    once: bool,
    teardown_after: bool,
    test_json: bool,
    runner: Any,
    strategy_instance: Any,
    state_manager: Any,
    cleanup_fn: Any,
    interval: int,
    max_iterations: int | None,
    reset_fork: bool,
    managed_gateway: Any,
) -> int:
    """Dispatch to the lifecycle, once, or continuous execution lane."""
    if test_actions is not None:
        return _run_test_lifecycle(
            runner=runner,
            strategy_instance=strategy_instance,
            state_manager=state_manager,
            cleanup_fn=cleanup_fn,
            actions=test_actions,
            teardown=teardown_after,
            json_output=test_json,
        )

    if once:
        return _run_once(
            runner=runner,
            strategy_instance=strategy_instance,
            state_manager=state_manager,
            cleanup_fn=cleanup_fn,
            teardown_after=teardown_after,
        )

    return _run_continuous(
        runner=runner,
        strategy_instance=strategy_instance,
        cleanup_fn=cleanup_fn,
        interval=interval,
        max_iterations=max_iterations,
        reset_fork=reset_fork,
        managed_gateway=managed_gateway,
    )


# ---------------------------------------------------------------------------
# Single-iteration execution
# ---------------------------------------------------------------------------


def _run_once(  # noqa: C901
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
        # Mirrors the copy_replay_file safety fix (always-run cleanup).
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
                    ct_state = await state_manager.load_state(strategy_instance.deployment_id)
                    if ct_state is not None and "copy_trading_state" in ct_state.state:
                        activity_provider.set_state(ct_state.state["copy_trading_state"])
                except Exception as e:
                    logger.warning(f"Failed to restore copy trading state: {e}")

            # VIB-3944: rebuild lending FIFO lots from durable accounting_events.
            # The continuous run_loop entry point does this in initialize_run_loop
            # but --once / --teardown-after bypass that path. Without rebuild,
            # Earlier teardown flows could land a REPAY with no matching BORROW lot and the
            # writer cannot emit interest_delta_usd → L4 Accountant Test fails.
            # Run AFTER setup_gateway_integration so the gRPC channel is up.
            from ..runner._run_loop_helpers import (
                hydrate_recent_open_events_cache,
                reconstruct_lending_basis_store,
            )

            reconstruct_lending_basis_store(
                runner,
                strategy_instance,
                _require_strategy_deployment_id(
                    strategy_instance,
                    operation="reconstruct_lending_basis_store",
                ),
            )

            # VIB-4086 — same cross-process restart hole for the
            # position_events recent-open cache. Without hydration, the
            # ``--once --teardown-after`` can close a position
            # opened in a prior process with no in-memory bracket /
            # tokens to carry forward, landing the CLOSE row with empty
            # token0/token1/value_usd (the LP6 ship gate this PR closes).
            await hydrate_recent_open_events_cache(runner, strategy_instance)

            # VIB-3762: route --once snapshot persistence through the
            # mode-aware wrapper so accounting failures surface the same
            # way as continuous-mode failures (live -> ACCOUNTING_FAILED,
            # paper/dry-run -> ERROR log + continue). Direct calls to
            # ``_capture_portfolio_snapshot`` were the bypass that hid
            # April 29's silent accounting failures.
            import time as _time

            from ..runner._run_loop_helpers import capture_snapshot_with_accounting

            iteration_start_monotonic = _time.monotonic()
            result = await runner.run_iteration(strategy_instance)
            result = await capture_snapshot_with_accounting(
                runner=runner,
                strategy=strategy_instance,
                deployment_id=_require_strategy_deployment_id(
                    strategy_instance,
                    operation="capture_snapshot_with_accounting",
                ),
                result=result,
                iteration_start_monotonic=iteration_start_monotonic,
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

                deployment_id = _require_strategy_deployment_id(
                    strategy_instance,
                    operation="teardown_after",
                )
                manager = get_teardown_state_manager()
                manager.create_request(
                    TeardownRequest(
                        deployment_id=deployment_id,
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
                    ct_state = await state_manager.load_state(strategy_instance.deployment_id)
                    if ct_state is None:
                        from almanak.framework.state.state_manager import StateData

                        ct_state = StateData(
                            deployment_id=strategy_instance.deployment_id,
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
                    runner.teardown_gateway_integration(strategy_instance.deployment_id)
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
# Test lifecycle — drive force_action sequence + optional teardown
# ---------------------------------------------------------------------------


def _run_test_lifecycle(  # noqa: C901
    *,
    runner: Any,
    strategy_instance: Any,
    state_manager: Any,
    cleanup_fn: Callable[[], Coroutine[Any, Any, None]],
    actions: list[str],
    teardown: bool,
    json_output: bool,
) -> int:
    """Execute a force-action lifecycle test.

    Drives each value in ``actions`` as a single ``--once`` iteration with
    ``strategy_instance.force_action`` mutated between iterations, optionally
    followed by a teardown iteration. State (position id, on-chain side
    effects, runner cycle counter) flows through naturally because all
    iterations share one strategy instance.

    Stops on the first failed iteration (fail-fast). Always runs cleanup.

    Returns:
        Exit code: ``0`` if every iteration (and the teardown, if requested)
        passed, ``1`` otherwise.
    """
    import asyncio
    import json
    import logging as _logging

    from ..runner import IterationStatus
    from ..runner.runner_models import IterationResult

    class _BufferingHandler(_logging.Handler):
        """Captures WARN+ERROR log records into a ring buffer for later inspection.

        Used to attach framework-side diagnostics (REVERT DIAGNOSTIC blocks,
        non-retryable error notices, gas warnings, etc.) to failed steps in
        the JSON output. Successful steps don't carry this data — it would
        bloat the response.
        """

        def __init__(self, max_records: int = 200) -> None:
            super().__init__(level=_logging.WARNING)
            # Each record carries a monotonic id so per-step slicing stays correct
            # even after the ring buffer has dropped older entries (id != list index).
            self.records: list[tuple[int, str]] = []
            self.next_id = 0
            self.max_records = max_records

        def emit(self, record: _logging.LogRecord) -> None:
            try:
                msg = self.format(record)
            except Exception:
                msg = record.getMessage()
            if len(self.records) >= self.max_records:
                self.records.pop(0)
            self.records.append((self.next_id, msg))
            self.next_id += 1

        def slice_since(self, cursor: int) -> list[str]:
            """Return all records emitted since the given cursor (monotonic id)."""
            return [msg for idx, msg in self.records if idx >= cursor]

    log_buffer = _BufferingHandler(max_records=200)
    log_buffer.setFormatter(_logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
    _logging.getLogger().addHandler(log_buffer)

    # Owned at the outer scope so the exception handler below can salvage
    # whatever steps completed before run_iteration() raised.
    action_results: list[dict] = []
    teardown_result_dict: dict | None = None

    async def run_lifecycle_with_cleanup() -> tuple[list[dict], dict | None]:  # noqa: C901
        nonlocal action_results, teardown_result_dict
        gateway_integration_ready = False
        try:
            runner.setup_gateway_integration(strategy_instance)
            gateway_integration_ready = True

            # Mirror _run_once state hooks: restore persisted strategy state and
            # copy-trading cursor before any iteration, so test runs see the same
            # startup conditions production does. load_state_async failures must
            # propagate (fatal in _run_once); copy-trading restore is best-effort
            # (also matches _run_once).
            if hasattr(strategy_instance, "load_state_async"):
                await strategy_instance.load_state_async()
            activity_provider = getattr(strategy_instance, "_wallet_activity_provider", None)
            if activity_provider is not None:
                try:
                    ct_state = await state_manager.load_state(strategy_instance.deployment_id)
                    if ct_state is not None and "copy_trading_state" in ct_state.state:
                        activity_provider.set_state(ct_state.state["copy_trading_state"])
                except Exception as e:
                    logger.warning(f"Failed to restore copy trading state: {e}")

            # VIB-3944: same cross-process FIFO rebuild as _run_once. The
            # test-lifecycle path drives multiple force_action iterations + an
            # optional teardown in a single CLI invocation, but the in-memory
            # FIFO store is empty if a previous CLI process opened the borrow.
            from ..runner._run_loop_helpers import (
                hydrate_recent_open_events_cache,
                reconstruct_lending_basis_store,
            )

            reconstruct_lending_basis_store(
                runner,
                strategy_instance,
                _require_strategy_deployment_id(
                    strategy_instance,
                    operation="reconstruct_lending_basis_store",
                ),
            )

            # VIB-4086 — symmetric cache hydration for the test-lifecycle
            # path. See `_run_once` above for the full rationale.
            await hydrate_recent_open_events_cache(runner, strategy_instance)

            # Single predicate: an action passes iff status is SUCCESS or HOLD.
            # Used identically by per-step failure_logs, fail-fast, and the final
            # summary so the three never disagree.
            deployment_id = _require_strategy_deployment_id(
                strategy_instance,
                operation="strat_test_lifecycle",
            )
            action_pass_statuses = (IterationStatus.SUCCESS.value, IterationStatus.HOLD.value)
            for action in actions:
                strategy_instance.force_action = action
                if not json_output:
                    click.echo(f"\n→ force_action={action!r}")
                logs_before = log_buffer.next_id
                try:
                    iteration_start_monotonic = time.monotonic()
                    result = await runner.run_iteration(strategy_instance)
                    # Capture portfolio snapshot per iteration through the
                    # canonical helper so the live-mode ``ACCOUNTING_FAILED``
                    # escalation contract (VIB-3762) is honoured. Direct
                    # calls to ``_capture_portfolio_snapshot`` here were the
                    # April 29 silent-failure shape: a live ledger-write
                    # exception in the snapshot path was swallowed by the
                    # surrounding ``except Exception`` and the loop carried
                    # on with a half-persisted iteration.
                    from almanak.framework.runner._run_loop_helpers import (
                        capture_snapshot_with_accounting,
                    )

                    result = await capture_snapshot_with_accounting(
                        runner=runner,
                        strategy=strategy_instance,
                        deployment_id=deployment_id,
                        result=result,
                        iteration_start_monotonic=iteration_start_monotonic,
                    )
                    runner._emit_iteration_summary(result, chain=getattr(strategy_instance, "chain", None))
                except Exception as exc:
                    # Record the raise as a synthetic failed step and break — but DO NOT
                    # propagate; the teardown block below must still run so positions
                    # opened by prior successful actions get unwound. Use IterationResult
                    # so the step shape matches normal steps from result.to_dict().
                    logger.exception("run_iteration raised for action %r", action)
                    synthetic = IterationResult(
                        status=IterationStatus.STRATEGY_ERROR,
                        error=f"run_iteration raised: {exc!r}",
                        deployment_id=deployment_id,
                    )
                    action_results.append(
                        {
                            "action": action,
                            **synthetic.to_dict(),
                            "failure_logs": log_buffer.slice_since(logs_before),
                        }
                    )
                    if not json_output:
                        click.echo(f"  raised: {exc!r}", err=True)
                    break
                entry = {"action": action, **result.to_dict()}
                action_passed = result.status.value in action_pass_statuses
                if not action_passed:
                    entry["failure_logs"] = log_buffer.slice_since(logs_before)
                action_results.append(entry)
                if not action_passed:
                    if not json_output:
                        click.echo(f"  failed: {result.error or result.status.value}", err=True)
                    break  # fail-fast

            # Always run teardown when requested — even if an earlier action
            # failed, we still want to clean up any positions opened by prior
            # successful actions in this run. Teardown is a no-op for
            # strategies whose generate_teardown_intents returns [] when no
            # position is open.
            if teardown:
                from almanak.framework.teardown import get_teardown_state_manager
                from almanak.framework.teardown.models import TeardownMode, TeardownRequest

                strategy_instance.force_action = ""
                deployment_id = _require_strategy_deployment_id(
                    strategy_instance,
                    operation="strat_test_teardown",
                )
                if not json_output:
                    click.echo("\n→ teardown")
                # Capture log cursor BEFORE create_request so a state-manager
                # failure here (locked DB / schema mismatch) is also surfaced as
                # a synthetic teardown step instead of escaping to the outer handler.
                logs_before = log_buffer.next_id
                try:
                    get_teardown_state_manager().create_request(
                        TeardownRequest(
                            deployment_id=deployment_id,
                            mode=TeardownMode.SOFT,
                            reason="strat test --teardown",
                            requested_by="cli",
                        )
                    )
                    td_iteration_start = time.monotonic()
                    td_result = await runner.run_iteration(strategy_instance)
                    # Same accounting-snapshot wrapper as the force-action
                    # iteration above — without this, a live teardown's
                    # ledger-write failure during the post-iteration snapshot
                    # would be swallowed (the canonical April 29 silent-
                    # failure shape).
                    from almanak.framework.runner._run_loop_helpers import (
                        capture_snapshot_with_accounting,
                    )

                    td_result = await capture_snapshot_with_accounting(
                        runner=runner,
                        strategy=strategy_instance,
                        deployment_id=deployment_id,
                        result=td_result,
                        iteration_start_monotonic=td_iteration_start,
                    )
                    runner._emit_iteration_summary(td_result, chain=getattr(strategy_instance, "chain", None))
                except Exception as exc:
                    # Materialize a failed teardown step instead of letting the
                    # exception escape — symmetric with the action loop above so
                    # JSON consumers see the failure_logs and a teardown step entry.
                    # Use IterationResult so the step shape matches normal steps.
                    logger.exception("teardown raised (create_request or run_iteration)")
                    synthetic = IterationResult(
                        status=IterationStatus.STRATEGY_ERROR,
                        error=f"run_iteration raised: {exc!r}",
                        deployment_id=deployment_id,
                    )
                    teardown_result_dict = {
                        "action": "teardown",
                        **synthetic.to_dict(),
                        "failure_logs": log_buffer.slice_since(logs_before),
                    }
                    if not json_output:
                        click.echo(f"  teardown raised: {exc!r}", err=True)
                else:
                    teardown_result_dict = {"action": "teardown", **td_result.to_dict()}
                    teardown_passed = td_result.status.value == IterationStatus.TEARDOWN.value
                    if not teardown_passed:
                        teardown_result_dict["failure_logs"] = log_buffer.slice_since(logs_before)
                        if not json_output:
                            click.echo(
                                f"  teardown failed: {td_result.error or td_result.status.value}",
                                err=True,
                            )

            # Persist copy trading cursor state (mirrors _run_once).
            if activity_provider is not None:
                try:
                    ct_state = await state_manager.load_state(strategy_instance.deployment_id)
                    if ct_state is None:
                        from almanak.framework.state.state_manager import StateData

                        ct_state = StateData(
                            deployment_id=strategy_instance.deployment_id,
                            version=0,
                            state={},
                        )
                    ct_state.state["copy_trading_state"] = activity_provider.get_state()
                    await state_manager.save_state(ct_state, expected_version=ct_state.version)
                except Exception as e:
                    logger.warning(f"Failed to persist copy trading state: {e}")

            if hasattr(strategy_instance, "flush_pending_saves"):
                try:
                    await strategy_instance.flush_pending_saves()
                except Exception as e:
                    logger.warning(f"Error flushing pending saves: {e}")

            return action_results, teardown_result_dict
        finally:
            try:
                if gateway_integration_ready:
                    runner.teardown_gateway_integration(strategy_instance.deployment_id)
            finally:
                await cleanup_fn()

    try:
        action_results, teardown_result_dict = asyncio.run(run_lifecycle_with_cleanup())
    except Exception as e:
        logger.exception("Test lifecycle failed")
        if json_output:
            partial_steps: list[dict] = list(action_results)
            if teardown_result_dict is not None:
                partial_steps.append(teardown_result_dict)
            # Reflect the real per-step pass state so summary doesn't contradict steps —
            # the exception itself (e.g. flush_pending_saves / cleanup) may have fired
            # AFTER all action and teardown iterations already passed.
            partial_actions_ok = all(
                step["status"] in (IterationStatus.SUCCESS.value, IterationStatus.HOLD.value) for step in action_results
            )
            if not teardown:
                partial_teardown_ok: bool | None = None
            elif teardown_result_dict is None:
                partial_teardown_ok = False
            else:
                partial_teardown_ok = teardown_result_dict["status"] == IterationStatus.TEARDOWN.value
            click.echo(
                json.dumps(
                    {
                        "summary": {
                            "all_passed": False,  # exception always means run failed overall
                            "skipped": False,
                            "skip_reason": None,
                            "steps_run": len(partial_steps),
                            "actions_passed": partial_actions_ok,
                            "teardown_passed": partial_teardown_ok,
                            "error": str(e),
                        },
                        "steps": partial_steps,
                    },
                    default=str,  # mirror success path — preserve datetime/Decimal/etc.
                )
            )
        else:
            click.echo(f"Error running test lifecycle: {e}", err=True)
        return 1
    finally:
        _logging.getLogger().removeHandler(log_buffer)

    # teardown_passed is None ("not applicable") when --teardown wasn't requested,
    # True when teardown ran and reached IterationStatus.TEARDOWN, False otherwise.
    # Same convention as the exception path, so JSON consumers see one shape.
    teardown_ok: bool | None
    if not teardown:
        teardown_ok = None
    elif teardown_result_dict is None:
        teardown_ok = False  # asked for but never executed (logic error)
    else:
        teardown_ok = teardown_result_dict["status"] == IterationStatus.TEARDOWN.value
    # all([]) is True — teardown-only runs (no actions) correctly identity to True here
    # and rely on teardown_ok for the final verdict.
    actions_ok = all(r["status"] in (IterationStatus.SUCCESS.value, IterationStatus.HOLD.value) for r in action_results)
    # Treat teardown_ok=None (not applicable) as a non-blocker for all_passed.
    all_passed = actions_ok and (teardown_ok is None or teardown_ok)

    if json_output:
        steps: list[dict] = list(action_results)
        if teardown_result_dict is not None:
            steps.append(teardown_result_dict)
        payload = {
            "deployment_id": _require_strategy_deployment_id(
                strategy_instance,
                operation="strat_test_json_output",
            ),
            "summary": {
                "all_passed": all_passed,
                "steps_run": len(steps),
                "actions_passed": actions_ok,
                "teardown_passed": teardown_ok,
            },
            "steps": steps,
        }
        click.echo(json.dumps(payload, indent=2, default=str))
    else:
        click.echo()
        if all_passed:
            click.echo("Test lifecycle passed.")
        else:
            click.echo("Test lifecycle failed.")

    return 0 if all_passed else 1


# ---------------------------------------------------------------------------
# Continuous execution
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
