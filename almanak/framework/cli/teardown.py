# MULTI-WALLET: Teardown currently uses a single wallet address. When multi-wallet
# support is enabled, teardown must iterate per-chain wallets to close all positions.
"""CLI command for managing strategy teardowns.

Usage:
    # Execute teardown directly from working directory (like `almanak strat run`)
    almanak strat teardown -d <strategy_dir>
    almanak strat teardown -d <strategy_dir> --preview
    almanak strat teardown -d <strategy_dir> --mode emergency

    # Request a teardown (async, picked up by runner)
    almanak strat teardown request --strategy <name> --mode <graceful|emergency>

    # Check teardown status
    almanak strat teardown status --strategy <name>

    # Cancel a pending teardown
    almanak strat teardown cancel --strategy <name>

    # List all active teardowns
    almanak strat teardown list

Examples:
    # Direct execution (recommended)
    almanak strat teardown -d almanak/demo_strategies/aerodrome_lp --preview
    almanak strat teardown -d almanak/demo_strategies/aave_borrow --mode graceful

    # Async request (picked up by strategy runner)
    almanak strat teardown request --strategy uniswap_lp --mode graceful
    almanak strat teardown request --strategy aave_leverage --mode emergency --reason "Market crash"
    almanak strat teardown status --strategy uniswap_lp
    almanak strat teardown cancel --strategy uniswap_lp
    almanak strat teardown list
"""

import asyncio
import importlib.util
import json
import logging
import sys
import time
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ..execution.gateway_orchestrator import GatewayExecutionOrchestrator
    from ..gateway_client import GatewayClient
    from ..runner import StrategyRunner

import click

from almanak.config.cli_options import gateway_client_options

logger = logging.getLogger(__name__)

from ..teardown import (
    TeardownAssetPolicy,
    TeardownMode,
    TeardownPhase,
    TeardownRequest,
    TeardownStatus,
    get_teardown_state_manager,
)

# =============================================================================
# Strategy-folder resolution (VIB-3835)
# =============================================================================
#
# Every teardown subcommand that reads/writes ``teardown_requests`` needs to
# know which strategy folder owns the SQLite DB. The local-DB rule is folder-
# scoped (1 strategy = 1 folder = 1 DB = 1 gateway, plan §B / VIB-3761) so the
# CLI must resolve the folder explicitly — silently falling through to the
# per-user utility DB caused the May 1 mainnet teardown failure.
#
# Resolution order (mirrors `strat run` / VIB-3835):
#   1. Explicit ``-d / --working-dir`` flag.
#   2. ``ALMANAK_STRATEGY_FOLDER`` env var (set by `strat run` in its own process,
#      so a teardown run from inside that process inherits the right folder).
#   3. ``./`` (cwd) if it contains config.json (or strategy.py).
#   4. HARD FAIL — no fallback to utility DB.
#
# The resolver exports ``ALMANAK_STRATEGY_FOLDER`` so any downstream code that
# reads ``local_paths.local_strategy_db_path()`` sees the same folder.

_STRATEGY_FOLDER_HINT = (
    "Pass --working-dir / -d <path>, or run from a strategy folder.\n"
    "  A strategy folder must contain config.json, config.yaml, "
    "config.yml, or strategy.py."
)


def _looks_like_strategy_folder(path: Path) -> bool:
    """Compatibility shim — delegates to the canonical helper in ``local_paths``.

    Kept as a private name to avoid touching every call site in this module
    while sharing one implementation across ``almanak strat run``,
    ``almanak strat teardown`` and ``almanak gateway`` (VIB-3761/-3835).
    """
    from almanak.framework.local_paths import looks_like_strategy_folder

    return looks_like_strategy_folder(path)


def _resolve_and_export_strategy_folder(working_dir: str | None) -> Path:
    """Resolve the strategy folder and export it in the env for downstream code.

    Raises ``click.ClickException`` with the canonical remediation hint when
    no folder can be resolved. Always exits non-zero on failure so operator
    scripts can detect a misconfigured invocation rather than silently writing
    to the wrong DB.
    """
    from almanak.framework.local_paths import set_strategy_folder

    # Step 1: explicit -d flag wins.
    if working_dir is not None:
        candidate = Path(working_dir).expanduser().resolve()
        if not candidate.is_dir():
            raise click.ClickException(f"--working-dir does not exist or is not a directory: {candidate}")
        if not _looks_like_strategy_folder(candidate):
            raise click.ClickException(
                f"--working-dir does not look like a strategy folder: {candidate}\n  {_STRATEGY_FOLDER_HINT}"
            )
        set_strategy_folder(candidate)
        # Reset any singleton state managers cached from a prior path so the
        # exported folder takes effect for this invocation.
        _reset_teardown_state_singleton()
        return candidate

    # Step 2: respect a folder already exported by a parent process (e.g. a
    # `strat run` that shells out to `teardown` for some scripted flow).
    from almanak.framework.local_paths import strategy_folder_env

    env_folder = strategy_folder_env()
    if env_folder and env_folder.strip():
        candidate = Path(env_folder.strip()).expanduser().resolve()
        if candidate.is_dir() and _looks_like_strategy_folder(candidate):
            # Mirror Steps 1 and 3: reset the singleton so a parent process
            # that imported `get_teardown_state_manager` before exporting the
            # env var doesn't cache the wrong DB path.
            _reset_teardown_state_singleton()
            return candidate
        # Fall through — env var is stale or points at a non-strategy dir.

    # Step 3: try cwd.
    cwd = Path.cwd().resolve()
    if _looks_like_strategy_folder(cwd):
        set_strategy_folder(cwd)
        _reset_teardown_state_singleton()
        return cwd

    # Step 4: hard-fail.
    raise click.ClickException(f"no strategy folder resolved.\n  {_STRATEGY_FOLDER_HINT}")


def _build_no_op_teardown_message(deployment_id: str) -> str:
    """Canonical no-op teardown log line (VIB-3705).

    Single source of truth so the two CLI call sites (the empty-positions
    branch around line 814 and the empty-intents branch around line 1027)
    produce byte-identical output. QA harnesses grep this line to
    distinguish "no work was required" from a real teardown completion.
    """
    return f"Teardown: no open positions for strategy {deployment_id}; nothing to close. Exiting 0."


def _reset_teardown_state_singleton() -> None:
    """Clear the cached ``TeardownStateManager`` so the next call re-resolves.

    The singleton in ``get_teardown_state_manager`` caches the DB path on first
    call; if a CLI invocation sets ``ALMANAK_STRATEGY_FOLDER`` after the cache
    was populated (e.g. via a prior import), subsequent calls would otherwise
    hit the wrong DB.

    VIB-4049 PR2 moved the singleton from ``state_manager.py`` to
    ``framework/teardown/__init__.py`` and exposed a documented helper —
    delegate to it instead of reaching into module internals.
    """
    from almanak.framework.teardown import reset_teardown_state_manager

    reset_teardown_state_manager()


def _get_teardown_state_manager_or_die():
    """``get_teardown_state_manager()`` wrapped to surface ``LocalPathError``
    as a CLI-friendly ``click.ClickException`` instead of a raw traceback.

    The strict resolver (``local_strategy_db_path``) raises ``LocalPathError``
    when no strategy folder is resolvable — which happens in hosted mode or
    when an operator runs a CLI subcommand without ``-d``/cwd context. The
    error message already contains the canonical remediation hint; turning
    it into a ``ClickException`` ensures Click formats it as a single red
    error line and exits non-zero, matching every other CLI failure mode.
    """
    from ..local_paths import LocalPathError

    try:
        return get_teardown_state_manager()
    except LocalPathError as exc:
        raise click.ClickException(str(exc)) from exc


# =============================================================================
# CLI accounting wiring (VIB-3839)
# =============================================================================
#
# The CLI ``teardown execute`` lane needs to drive the same per-intent commit
# pipeline (enrich → ledger → outbox+fire → sidecar) and pre/post snapshot
# brackets the runner-loop lane already drives via VIB-3773. Without this
# wiring, every closing tx (LP_CLOSE, REPAY, swap-back, …) lands on-chain
# but the SDK records zero rows in transaction_ledger / position_events /
# portfolio_snapshots / portfolio_metrics / accounting_events — the operator
# sees "Teardown completed" with an empty audit trail.
#
# ``runner_helpers`` is ``functools.partial(fn, runner)`` for two callables;
# both need a real :class:`StrategyRunner` to read attributes like
# ``_write_ledger_entry``, ``_portfolio_valuer``, ``_last_cycle_id``, etc.
# So the CLI lane builds a minimal runner from the gateway-backed pieces it
# already has (price_oracle, balance_provider, execution_orchestrator,
# state_manager) and passes it through ``build_runner_helpers``.


async def _build_cli_teardown_runner(
    *,
    gateway_client: "GatewayClient",
    price_oracle: dict[str, Decimal] | None,
    orchestrator: "GatewayExecutionOrchestrator",
    chain: str,
    wallet_address: str,
) -> "StrategyRunner":
    """Construct a minimal :class:`StrategyRunner` for the CLI teardown lane.

    The returned runner is *not* attached to an iteration loop — it exists
    purely so :func:`build_runner_helpers` can bind ``commit_teardown_intent``
    and ``capture_teardown_snapshot_with_accounting`` against it. The runner's
    state_manager + accounting_processor write to the same gateway-backed
    backend the runner-loop lane uses, so the same five accounting tables
    light up for CLI-driven teardowns.

    ``price_oracle`` is the symbol→Decimal price map produced by
    ``MarketSnapshot.get_price_oracle_dict()``. Per the existing CLI
    behaviour, a None price oracle keeps compilation working with
    placeholder prices; the same falls through here.
    """
    from ..data.balance.gateway_provider import GatewayBalanceProvider
    from ..runner import RunnerConfig, StrategyRunner
    from ..state.gateway_state_manager import GatewayStateManager

    state_manager = GatewayStateManager(gateway_client)
    await state_manager.initialize()

    balance_provider = GatewayBalanceProvider(
        client=gateway_client,
        wallet_address=wallet_address,
        chain=chain,
    )

    runner_config = RunnerConfig(
        # Interval is irrelevant — there is no iteration loop. Pick the
        # default to avoid surfacing a magic value.
        default_interval_seconds=30,
        dry_run=False,
        enable_state_persistence=True,
        # CLI teardown writes to the deferred-write log on accounting failure
        # (VIB-3773 inverted contract — never block the next risk-reducing
        # intent). No alerts are routed through the runner from this lane.
        enable_alerting=False,
    )

    # ``GatewayExecutionOrchestrator`` and ``GatewayStateManager`` are
    # duck-compatible with ``ExecutionOrchestrator`` and ``StateManager``
    # respectively (the runner only calls a narrow set of methods that
    # both share). The StrategyRunner ctor's nominal types don't reflect
    # this duck-typing, so we annotate the call rather than widen the
    # ctor — the runner-loop's CLI fallback (cli/run_helpers.py) does
    # the same.
    runner = StrategyRunner(
        price_oracle=price_oracle,  # type: ignore[arg-type]
        balance_provider=balance_provider,
        execution_orchestrator=orchestrator,  # type: ignore[arg-type]
        state_manager=state_manager,  # type: ignore[arg-type]
        config=runner_config,
    )
    return runner


# =============================================================================
# Helper Functions
# =============================================================================


def format_status(status: TeardownStatus) -> str:
    """Format a teardown status with color."""
    colors = {
        TeardownStatus.PENDING: "yellow",
        TeardownStatus.CANCEL_WINDOW: "cyan",
        TeardownStatus.EXECUTING: "blue",
        TeardownStatus.PAUSED: "magenta",
        TeardownStatus.COMPLETED: "green",
        TeardownStatus.CANCELLED: "white",
        TeardownStatus.FAILED: "red",
    }
    return click.style(status.value, fg=colors.get(status, "white"))


def format_mode(mode: TeardownMode) -> str:
    """Format a teardown mode with color."""
    if mode == TeardownMode.SOFT:
        return click.style("GRACEFUL", fg="green")
    return click.style("EMERGENCY", fg="red")


def format_datetime(dt: datetime | None) -> str:
    """Format a datetime for display."""
    if not dt:
        return "-"
    return dt.strftime("%Y-%m-%d %H:%M:%S UTC")


def format_progress(request: TeardownRequest) -> str:
    """Format teardown progress."""
    total = request.positions_total
    closed = request.positions_closed
    failed = request.positions_failed

    if total == 0:
        return "-"

    progress = f"{closed}/{total}"
    if failed > 0:
        progress += click.style(f" ({failed} failed)", fg="red")

    return progress


# =============================================================================
# Strategy Loading Helpers
# =============================================================================


def load_strategy_from_file(file_path: Path) -> tuple[type | None, str | None]:
    """Load a strategy class from a Python file.

    Args:
        file_path: Path to the strategy Python file

    Returns:
        Tuple of (strategy_class, error_message)
    """
    from ..strategies.intent_strategy import IntentStrategy

    if not file_path.exists():
        return None, f"File not found: {file_path}"

    if not file_path.suffix == ".py":
        return None, f"Expected .py file, got: {file_path}"

    try:
        spec = importlib.util.spec_from_file_location("strategy_module", file_path)
        if spec is None or spec.loader is None:
            return None, f"Could not load module spec from {file_path}"

        # Add strategy directory to sys.path so local imports resolve
        strategy_dir = str(file_path.parent)
        sys.path.insert(0, strategy_dir)
        try:
            module = importlib.util.module_from_spec(spec)
            sys.modules["strategy_module"] = module
            spec.loader.exec_module(module)
        finally:
            sys.path.remove(strategy_dir)

        # Find concrete IntentStrategy subclasses (skip abstract base classes
        # like StatelessStrategy that may be imported but not instantiable).
        strategy_classes = []
        for name in dir(module):
            obj = getattr(module, name)
            if (
                isinstance(obj, type)
                and obj is not IntentStrategy
                and issubclass(obj, IntentStrategy)
                and not getattr(obj, "__abstractmethods__", frozenset())
            ):
                strategy_classes.append(obj)

        if not strategy_classes:
            return None, "No concrete IntentStrategy subclass found in file"

        # Prefer the most-derived class (defined in this file, not just imported)
        if len(strategy_classes) > 1:
            local_classes = [c for c in strategy_classes if c.__module__ == module.__name__]
            if local_classes:
                strategy_classes = local_classes

        return strategy_classes[0], None

    except Exception as e:
        return None, f"Error loading strategy: {str(e)}"


def _build_deployment_id_candidates(strategy: Any, config_dict: dict[str, Any]) -> list[str]:
    """Build deployment_id candidates for state restore."""
    candidates: list[str] = []

    def _add_candidate(value: Any) -> None:
        if not isinstance(value, str) or not value.strip():
            return
        clean_value = value.strip()
        candidates.append(clean_value)

    _add_candidate(config_dict.get("deployment_id"))
    _add_candidate(getattr(strategy, "deployment_id", ""))

    seen: set[str] = set()
    unique_candidates: list[str] = []
    for candidate in candidates:
        if candidate not in seen:
            unique_candidates.append(candidate)
            seen.add(candidate)
    return unique_candidates


def _restore_strategy_state_for_teardown(
    strategy: Any,
    strategy_class: type,
    config_dict: dict[str, Any],
    gateway_client: Any,
) -> None:
    """Restore strategy state before computing teardown positions."""
    if not hasattr(strategy, "set_state_manager"):
        logger.debug("Strategy %s does not expose state persistence hooks", strategy_class.__name__)
        return

    from ..state.gateway_state_manager import GatewayStateManager

    candidates = _build_deployment_id_candidates(strategy, config_dict)
    if not candidates:
        logger.info("No deployment_id candidates available for teardown state restore")
        return

    state_manager = GatewayStateManager(gateway_client)
    for deployment_id in candidates:
        logger.info("Attempting teardown state restore for deployment_id=%s", deployment_id)
        try:
            strategy.set_state_manager(state_manager, deployment_id)
        except Exception as e:
            logger.warning("Failed to inject state manager for deployment_id=%s: %s", deployment_id, e)
            continue

        try:
            if hasattr(strategy, "load_state_async"):
                loaded = asyncio.run(strategy.load_state_async())
            elif hasattr(strategy, "load_state"):
                loaded = strategy.load_state()
            else:
                loaded = False

            if loaded:
                logger.info("Restored strategy state for teardown (deployment_id=%s)", deployment_id)
                return
            logger.info("No persisted strategy state for deployment_id=%s", deployment_id)
        except Exception as e:
            logger.warning("State restore failed for deployment_id=%s: %s", deployment_id, e)

    logger.info("No persisted strategy state restored for teardown (candidates=%s)", candidates)


def _inject_balance_provider(
    strategy: Any,
    gateway_client: Any,
    chain: str,
    wallet_address: str,
) -> None:
    """Inject a gateway-backed balance provider into the strategy.

    This enables generate_teardown_intents() to call market.balance() during
    teardown. Without this, strategies that check balances before deciding
    teardown amounts crash with ValueError.
    """
    if not hasattr(strategy, "_balance_provider"):
        return

    try:
        from ..data.balance.gateway_provider import GatewayBalanceProvider
        from .run import create_sync_balance_func

        balance_provider = GatewayBalanceProvider(
            client=gateway_client,
            wallet_address=wallet_address,
            chain=chain,
        )

        # Create a price oracle for USD conversion (best-effort)
        price_oracle = None
        try:
            from ..data.price.gateway_oracle import GatewayPriceOracle

            price_oracle = GatewayPriceOracle(gateway_client, default_chain=chain)
        except Exception as e:
            logger.debug("Could not create GatewayPriceOracle for teardown, balance injection will be skipped: %s", e)

        if price_oracle:
            strategy._balance_provider = create_sync_balance_func(balance_provider, price_oracle)
            logger.info("Injected gateway balance provider for teardown (chain=%s)", chain)
        else:
            logger.debug("Skipped balance provider injection -- no price oracle available")
    except Exception as e:
        logger.warning("Could not inject balance provider for teardown: %s", e)


# =============================================================================
# CLI Commands
# =============================================================================


@click.group()
def teardown():
    """Manage strategy teardowns.

    The teardown system allows safely closing all positions for a strategy.
    Teardowns can be initiated via CLI, dashboard, config, or risk guards.
    """
    pass


@teardown.command("execute")
@click.option(
    "--working-dir",
    "-d",
    type=click.Path(exists=True),
    default=".",
    help="Working directory containing the strategy files.",
)
@click.option(
    "--config",
    "-c",
    "config_file",
    type=click.Path(exists=True),
    default=None,
    help="Path to strategy config JSON file.",
)
@click.option(
    "--mode",
    "-m",
    type=click.Choice(["graceful", "emergency"]),
    default="graceful",
    help="Teardown mode: graceful (slower, lower cost) or emergency (faster, higher slippage).",
)
@click.option(
    "--preview",
    is_flag=True,
    default=False,
    help="Preview teardown without executing.",
)
@click.option(
    "--force",
    "-f",
    is_flag=True,
    default=False,
    help="Skip confirmation prompt.",
)
@gateway_client_options
@click.option(
    "--no-gateway",
    "no_gateway",
    is_flag=True,
    default=False,
    help="Do not auto-start a gateway; connect to an existing one.",
)
@click.option(
    "--discover",
    "discover",
    is_flag=True,
    default=False,
    help=(
        "Discover LP positions on-chain instead of relying on the strategy's "
        "local state. Use this when the gateway was restarted, when local "
        "state is lost, or when closing orphaned positions from a prior run."
    ),
)
@click.option(
    "--include-empty",
    is_flag=True,
    default=False,
    help=(
        "When used with --discover, also surface zero-liquidity NFT positions "
        "(already withdrawn but not burned). Useful for cleaning up residual "
        "NFTs."
    ),
)
@click.option(
    "--network",
    "-n",
    default=None,
    type=click.Choice(["mainnet", "anvil"], case_sensitive=False),
    help="Network type: 'mainnet' (default) or 'anvil' to connect to an already-running Anvil fork.",
)
@click.option(
    "--no-accounting",
    "no_accounting",
    is_flag=True,
    default=False,
    help=(
        "Skip wiring the augmentation pipeline. None of "
        "transaction_ledger / accounting_events / position_events / "
        "portfolio_snapshots / portfolio_metrics will be updated, and the "
        "pre/post snapshot brackets are skipped. Use only for known-broken "
        "environments — the books and on-chain reality will diverge. Default "
        "is to hard-fail when wiring fails so the operator never moves real "
        "funds without books."
    ),
)
def execute_teardown(  # noqa: C901
    working_dir: str,
    config_file: str | None,
    mode: str,
    preview: bool,
    force: bool,
    gateway_host: str,
    gateway_port: int,
    no_gateway: bool,
    discover: bool,
    include_empty: bool,
    network: str | None,
    no_accounting: bool,
):
    """Execute teardown directly from a strategy working directory.

    This command loads a strategy from its working directory and immediately
    executes a teardown to close all open positions. A managed gateway is
    auto-started by default (like ``strat run``). Use --no-gateway to connect
    to an already-running gateway instead.

    Examples:

        # Preview what will be closed (auto-starts gateway)
        almanak strat teardown execute -d almanak/demo_strategies/aerodrome_lp --preview

        # Execute graceful teardown
        almanak strat teardown execute -d almanak/demo_strategies/aerodrome_lp

        # Emergency teardown (faster, accepts higher slippage)
        almanak strat teardown execute -d almanak/demo_strategies/aave_borrow --mode emergency

        # Connect to an existing gateway instead of auto-starting
        almanak strat teardown execute -d almanak/demo_strategies/uniswap_lp --no-gateway

        # Skip confirmation
        almanak strat teardown execute -d almanak/demo_strategies/uniswap_lp --force
    """
    click.echo("=" * 60)
    click.echo("ALMANAK STRATEGY TEARDOWN")
    click.echo("=" * 60)

    # Fail fast on incompatible option combinations before any filesystem or
    # resolver work. This is a pure option validator — it must fire before
    # the strategy-folder resolver so `--no-gateway --network anvil <bad-d>`
    # surfaces the option conflict, not the folder error.
    from .teardown_helpers import validate_teardown_options

    validate_teardown_options(no_gateway, network)

    # VIB-3838: route -d through the same resolver as request/status/list/
    # cancel. Validates is_dir + _looks_like_strategy_folder, exports
    # ALMANAK_STRATEGY_FOLDER, resets the cached singleton. Hard-fails with
    # the canonical "does not look like a strategy folder" message instead
    # of falling through to a noisier failure later in strategy loading.
    working_path = _resolve_and_export_strategy_folder(working_dir)

    # Load environment from .env through the boundary helper.
    from almanak.config.env import _load_dotenv_once
    from almanak.core.redaction import install_redaction

    env_file = working_path / ".env"
    if env_file.exists():
        _load_dotenv_once(str(env_file))
        click.echo(f"Loaded environment from: {env_file}")

    # Install secret redaction after env is loaded so all secrets are registered.
    install_redaction()

    # Find strategy.py
    strategy_file = working_path / "strategy.py"
    if not strategy_file.exists():
        raise click.ClickException(f"No strategy.py found in {working_dir}")

    # Load strategy class
    strategy_class, error = load_strategy_from_file(strategy_file)
    if error or strategy_class is None:
        raise click.ClickException(f"Failed to load strategy: {error}")

    click.echo(f"Loaded strategy: {strategy_class.__name__}")

    # Load config (auto-discovers config.{json,yaml,yml} when -c not given).
    from .teardown_helpers import load_strategy_config_dict

    config_dict, config_file = load_strategy_config_dict(working_path, config_file)
    if config_file:
        click.echo(f"Loaded config from: {config_file}")

    # Resolve chain: config.json override first, then decorator metadata
    from .run import get_default_chain

    chain = config_dict.get("chain") or get_default_chain(strategy_class)

    # Resolve wallet address up-front (VIB-3819): the managed-gateway path
    # below needs it to pre-fund the Anvil fork during boot. Both gateway
    # paths (--no-gateway and managed) and the strategy-instantiation block
    # below consume it.
    # Missing wallet_address is non-fatal here — managed gateway can still
    # boot (only Anvil pre-funding is skipped). Hard-fail happens at
    # strategy instantiation.
    from .teardown_helpers import resolve_wallet_address, setup_gateway

    wallet_address = resolve_wallet_address(config_dict)

    # Gateway setup: connect to an existing gateway (--no-gateway) or
    # auto-start a managed one. The helper handles --no-gateway connect/
    # health-check, managed-gateway port discovery, auth-token generation,
    # Anvil chain dispatch, atexit registration, and the final health
    # check. Returns GatewaySetupResult bundling the artifacts the rest of
    # the function consumes.
    _gw_setup = setup_gateway(
        no_gateway=no_gateway,
        gateway_host=gateway_host,
        gateway_port=gateway_port,
        network=network,
        chain=chain,
        config_dict=config_dict,
        wallet_address=wallet_address,
    )
    gateway_client = _gw_setup.client
    managed_gateway = _gw_setup.managed_gateway
    gateway_port = _gw_setup.gateway_port
    solana_anvil_needed = _gw_setup.solana_anvil_needed

    # VIB-3878: spin up solana-test-validator for Solana strategies under
    # --network anvil. Funds the wallet and registers atexit cleanup. Returns
    # None when not needed (i.e. solana_anvil_needed=False, e.g. --no-gateway
    # or pure-EVM strategy). The handle's stop() is idempotent so the
    # finally block + atexit safety-net won't double-stop.
    from .teardown_helpers import setup_solana_fork

    solana_handle = (
        setup_solana_fork(
            config_dict=config_dict,
            wallet_address=wallet_address,
            managed_gateway=managed_gateway,
        )
        if (not no_gateway and solana_anvil_needed)
        else None
    )

    from .teardown_helpers import (
        build_market_and_oracle,
        discover_positions,
        display_position_summary,
        display_unknown_value_warning,
        generate_teardown_intents_for_cli,
        get_resolver_for_cleanup,
        instantiate_strategy_with_state,
        print_no_op_if_empty_and_signal_return,
    )

    # Stash the resolver up-front so the cleanup `finally` always has it,
    # even when an exception fires before strategy instantiation completes.
    resolver = get_resolver_for_cleanup()

    try:
        # Phase 6: TokenResolver wiring → strategy instantiation → balance
        # provider injection → state restoration. Hard-fails on missing
        # wallet_address (VIB-3819 — managed-gateway boot survives without
        # it but strategy instantiation does not).
        strategy = instantiate_strategy_with_state(
            strategy_class=strategy_class,
            config_dict=config_dict,
            chain=chain,
            wallet_address=wallet_address,
            gateway_client=gateway_client,
            inject_balance_provider=_inject_balance_provider,
            restore_strategy_state=_restore_strategy_state_for_teardown,
        )

        click.echo(f"Strategy: {strategy_class.__name__}")
        click.echo(f"Chain: {chain}")
        click.echo(f"Wallet: {wallet_address}")
        click.echo(f"Mode: {mode}")
        click.echo("-" * 60)

        # Phase 7: positions via --discover NPM scan or strategy's own
        # tracking. VIB-3705 no-op early return — print canonical no-op
        # message + (when not --discover) the discovery tip and exit 0.
        positions = discover_positions(
            strategy=strategy,
            strategy_class=strategy_class,
            discover=discover,
            include_empty=include_empty,
            gateway_client=gateway_client,
            chain=chain,
            wallet_address=wallet_address,
        )
        if print_no_op_if_empty_and_signal_return(
            positions=positions,
            strategy=strategy,
            strategy_class=strategy_class,
            discover=discover,
            no_op_message_builder=_build_no_op_teardown_message,
        ):
            return

        # Phase 8a: positions table + SafetyGuard warning when --discover
        # couldn't price them (PR #1522 CodeRabbit major).
        total_value, unknown_value_count = display_position_summary(positions)
        display_unknown_value_warning(unknown_value_count)

        # Phase 8b: market snapshot early so preview matches execution.
        market, price_oracle = build_market_and_oracle(strategy)

        # Phase 8c: intents via --discover synthesis or strategy method.
        intents = generate_teardown_intents_for_cli(
            strategy=strategy,
            mode_str=mode,
            market=market,
            discover=discover,
            positions=positions,
        )

        if preview:
            click.echo("\n[PREVIEW MODE] No changes will be made.")
            return

        # Confirmation
        from .teardown_helpers import prompt_teardown_confirmation

        if not prompt_teardown_confirmation(force):
            return

        # Phase 9b: build the synchronous teardown machinery (orchestrator,
        # compiler, state adapter, cycle ids) + emit the --no-accounting
        # operator warning if applicable.
        click.echo("\nExecuting teardown...")
        from .teardown_helpers import build_teardown_machinery, run_teardown_with_brackets

        machinery = build_teardown_machinery(
            gateway_client=gateway_client,
            chain=chain,
            wallet_address=wallet_address,
            price_oracle=price_oracle,
            no_accounting=no_accounting,
        )

        # Phase 9c: run the teardown end-to-end inside an async context with
        # the VIB-3773 accounting boundary preserved (cycle-id swap on both
        # surfaces, pre/post snapshot brackets with degraded-but-continue
        # contract, restore cycle ids in finally).
        try:
            result = asyncio.run(
                run_teardown_with_brackets(
                    machinery=machinery,
                    strategy=strategy,
                    mode_str=mode,
                    market=market,
                    discover=discover,
                    positions=positions,
                    intents=intents,
                    no_accounting=no_accounting,
                    gateway_client=gateway_client,
                    price_oracle=price_oracle,
                    chain=chain,
                    wallet_address=wallet_address,
                    build_cli_teardown_runner=_build_cli_teardown_runner,
                )
            )
        except Exception as e:
            logger.error("Teardown execution failed", exc_info=True)
            raise click.ClickException(f"Teardown execution failed: {e}") from e

        # Display results + record lifecycle in teardown_requests (VIB-3920).
        from .teardown_helpers import (
            display_teardown_result,
            update_teardown_requests_lifecycle,
        )

        deployment_id_for_log = strategy.deployment_id
        display_teardown_result(result, deployment_id_for_log, _build_no_op_teardown_message)
        update_teardown_requests_lifecycle(
            deployment_id_for_log,
            mode,
            result,
            _get_teardown_state_manager_or_die,
        )

        if not result.success:
            sys.exit(1)
    finally:
        from .teardown_helpers import cleanup_teardown_resources

        cleanup_teardown_resources(
            resolver=resolver,
            gateway_client=gateway_client,
            solana_handle=solana_handle,
            managed_gateway=managed_gateway,
        )


@teardown.command()
@click.option(
    "--working-dir",
    "-d",
    type=click.Path(),
    default=None,
    help=(
        "Strategy folder owning the local DB. Resolves like `strat run -d`: "
        "explicit flag → ALMANAK_STRATEGY_FOLDER → cwd if it has config.json. "
        "Hard-fails when nothing resolves (no utility-DB fallback)."
    ),
)
@click.option(
    "--strategy",
    "-s",
    required=True,
    help="Deployment ID or name to teardown",
)
@click.option(
    "--mode",
    "-m",
    type=click.Choice(["graceful", "emergency"]),
    default="graceful",
    help="Teardown mode: graceful (15-30 min) or emergency (1-3 min)",
)
@click.option(
    "--asset-policy",
    "-a",
    type=click.Choice(["target", "entry", "keep"]),
    default="target",
    help="Asset policy: target (USDC), entry (original), keep (native)",
)
@click.option(
    "--target-token",
    "-t",
    default="USDC",
    help="Target token for consolidation (default: USDC)",
)
@click.option(
    "--reason",
    "-r",
    default=None,
    help="Reason for teardown (optional)",
)
@click.option(
    "--force",
    "-f",
    is_flag=True,
    help="Skip confirmation prompt",
)
@click.option(
    "--wait",
    is_flag=True,
    default=False,
    help=(
        "Block until the runner reaches a terminal state (completed/failed/"
        "cancelled). Without --wait the command returns as soon as the request "
        "is written, which is fire-and-forget and does NOT confirm pickup."
    ),
)
@click.option(
    "--timeout",
    type=int,
    default=300,
    help="Seconds to wait for terminal state when --wait is set (default: 300).",
)
def request(
    working_dir: str | None,
    strategy: str,
    mode: str,
    asset_policy: str,
    target_token: str,
    reason: str | None,
    force: bool,
    wait: bool,
    timeout: int,
):
    """Request a teardown for a strategy.

    This creates a teardown request that will be picked up by the strategy
    runner on the next iteration. The strategy will then execute the teardown
    with the specified parameters.

    Token consolidation (VIB-5011): after positions are closed and verified,
    graceful teardowns with --asset-policy target (default) or entry swap
    residual strategy tokens above the $5 dust floor into the target token.
    Emergency mode and --asset-policy keep skip consolidation. The dust floor
    is a TeardownConfig setting (token_consolidation.min_swap_value_usd) —
    there is intentionally no CLI flag because the request row has no column
    for it; override it programmatically via TeardownConfig if needed.

    Examples:
        # Graceful teardown with default settings (folder-scoped)
        almanak strat teardown request -d strategies/my_strat -s uniswap_lp --mode graceful

        # Emergency teardown keeping native tokens
        almanak strat teardown request -d strategies/my_strat -s aave_lev --mode emergency --asset-policy keep

        # Block until the runner finishes (recommended for interactive use)
        almanak strat teardown request -d strategies/my_strat -s uniswap_lp --wait --timeout 300

        # Teardown with reason
        almanak strat teardown request -d strategies/my_strat -s gmx_perp --reason "Rebalancing"
    """
    _resolve_and_export_strategy_folder(working_dir)

    if timeout <= 0:
        raise click.ClickException("--timeout must be a positive number of seconds")

    # Map asset policy
    policy_map = {
        "target": TeardownAssetPolicy.TARGET_TOKEN,
        "entry": TeardownAssetPolicy.ENTRY_TOKEN,
        "keep": TeardownAssetPolicy.KEEP_OUTPUTS,
    }
    asset_policy_enum = policy_map[asset_policy]

    # Map mode
    mode_enum = TeardownMode.SOFT if mode == "graceful" else TeardownMode.HARD

    # Show confirmation
    click.echo()
    click.echo(click.style("Teardown Request Summary", bold=True, fg="cyan"))
    click.echo(f"  Strategy:     {strategy}")
    click.echo(f"  Mode:         {format_mode(mode_enum)}")
    click.echo(f"  Asset Policy: {asset_policy_enum.value}")
    if asset_policy_enum == TeardownAssetPolicy.TARGET_TOKEN:
        click.echo(f"  Target Token: {target_token}")
    if reason:
        click.echo(f"  Reason:       {reason}")
    click.echo()

    # Confirm unless forced
    if not force:
        if not click.confirm("Create this teardown request?"):
            click.echo("Cancelled.")
            return

    # Create the request
    manager = _get_teardown_state_manager_or_die()

    # Check for existing active request
    existing = manager.get_active_request(strategy)
    if existing:
        click.echo(
            click.style(
                f"\nWarning: Strategy {strategy} already has an active teardown request "
                f"(status: {existing.status.value}). Replace it?",
                fg="yellow",
            )
        )
        if not force and not click.confirm("Replace existing request?"):
            click.echo("Cancelled.")
            return

    teardown_request = TeardownRequest(
        deployment_id=strategy,
        mode=mode_enum,
        asset_policy=asset_policy_enum,
        target_token=target_token,
        reason=reason,
        requested_by="cli",
    )

    manager.create_request(teardown_request)

    click.echo()
    click.echo(click.style("Teardown request created!", fg="green"))

    if not wait:
        click.echo("The strategy will pick up this request on the next iteration.")
        click.echo(f"Use 'almanak strat teardown status -d <folder> -s {strategy}' to monitor progress.")
        return

    # --wait: poll until terminal state or timeout.
    exit_code = _wait_for_terminal_state(manager, strategy, timeout)
    if exit_code != 0:
        sys.exit(exit_code)


def _echo_warnings(warnings: list, prefix: str) -> None:
    """Echo up to 5 consolidation warnings with a uniform prefix."""
    for warning in warnings[:5]:
        click.echo(f"{prefix}{warning}")


def _consolidation_payload(manager: Any, deployment_id: str) -> dict | None:
    """Best-effort read of ``result_json["consolidation"]`` (display-only).

    Duck-typed on the SQLite manager's ``get_result_payload`` accessor —
    gateway-backed managers without it yield ``None`` (render nothing).
    """
    getter = getattr(manager, "get_result_payload", None)
    if not callable(getter):
        return None
    try:
        payload = getter(deployment_id)
    except Exception:  # noqa: BLE001 — display-only, never block the CLI
        return None
    consolidation = (payload or {}).get("consolidation")
    return consolidation if isinstance(consolidation, dict) else None


def _render_verification_status(manager: Any, deployment_id: str) -> None:
    """Render the closure-verification confidence from ``result_json`` (VIB-2932 / VIB-5472).

    Output only; never raises and never changes exit codes. Makes an UNVERIFIED
    closure visible on the operator's primary surface — the count was reported but
    not chain-confirmed, so the operator must verify on-chain before trusting it.
    A CHAIN_VERIFIED closure is shown in green as positive confirmation; NOT_RUN
    and a missing field are silent (nothing to assert).
    """
    getter = getattr(manager, "get_result_payload", None)
    if not callable(getter):
        return
    try:
        payload = getter(deployment_id)
    except Exception:  # noqa: BLE001 — display-only, never block the CLI
        return
    status = (payload or {}).get("verification_status")
    if status == "chain_verified":
        click.secho("  closure verification: chain_verified", fg="green")
    elif status == "unverified":
        click.secho(
            "  closure verification: UNVERIFIED — positions reported closed by execution "
            "but NOT chain-confirmed; verify on-chain before trusting the count.",
            fg="yellow",
        )
    elif status == "failed":
        click.secho(
            "  closure verification: FAILED — closure could not be confirmed on-chain "
            "(residual position, post-condition error, or verifier exception).",
            fg="red",
        )


def _render_consolidation_summary(manager: Any, deployment_id: str) -> None:
    """Render the token-consolidation outcome from ``result_json`` (VIB-5011).

    Output only; never raises and never changes exit codes — ``--wait``
    stays 0 when consolidation failed but closure succeeded.
    """
    consolidation = _consolidation_payload(manager, deployment_id)
    if consolidation is None:
        return
    succeeded = consolidation.get("succeeded") or 0
    warnings = consolidation.get("warnings") or []
    if consolidation.get("failed"):
        click.secho(
            f"  WARNING: {consolidation['failed']} consolidation swap(s) failed; "
            "wallet holds residual non-target tokens.",
            fg="yellow",
        )
        _echo_warnings(warnings, "    - ")
    elif succeeded:
        target = consolidation.get("target_token") or "target token"
        click.echo(f"  consolidated {succeeded} token(s) → {target}")
        # Warnings matter on success too — e.g. the wallet-scope disclosure
        # or per-token skips (no price) ride along with a successful run.
        _echo_warnings(warnings, "    - ")
    elif not consolidation.get("planned"):
        _echo_warnings(warnings, "  consolidation: ")


def _wait_for_terminal_state(manager: Any, deployment_id: str, timeout: int) -> int:
    """Poll ``teardown_requests`` until terminal status or timeout.

    Surfaces state transitions progressively (acknowledged, started, phase
    changes, position counts) so the operator sees what the runner is doing
    in real time. Returns a CLI exit code:

      * 0 — COMPLETED
      * 1 — FAILED, CANCELLED, or timeout

    Implementation note: Polls every 2s (cheap SQLite read) and dedupes
    transitions on a small per-state cache so the output stays compact.
    A 2s poll is fast enough that an interactive operator sees the runner
    pick up the request within the first iteration tick.
    """
    poll_interval = 2.0
    deadline = time.monotonic() + timeout

    click.echo()
    click.echo(click.style(f"Waiting (timeout={timeout}s)...", fg="cyan"))
    click.echo("  pending — request created, waiting for runner to pick up")

    try:
        return _poll_for_terminal_state(manager, deployment_id, timeout, poll_interval, deadline)
    except KeyboardInterrupt:
        # VIB-3837: the runner is the only writer of the teardown_requests
        # row, so interrupting the CLI's wait does NOT cancel or partially
        # commit the teardown — it just stops the local poll. Tell the
        # operator how to pick the wait back up and exit 130 (128+SIGINT).
        # The resume hint must include ``-d <folder>`` because the in-process
        # ``ALMANAK_STRATEGY_FOLDER`` export dies with this CLI process; a
        # follow-up command from another cwd would otherwise hard-fail at the
        # resolver. Read the folder we exported earlier (always populated by
        # ``_resolve_and_export_strategy_folder``).
        from almanak.framework.local_paths import strategy_folder_env

        strategy_folder = strategy_folder_env() or "<folder>"
        click.echo()
        click.echo(
            click.style(
                "Interrupted. The runner will continue executing the teardown — "
                f"check 'almanak strat teardown status -d {strategy_folder} -s {deployment_id}'.",
                fg="yellow",
            )
        )
        return 130


def _poll_for_terminal_state(
    manager: Any,
    deployment_id: str,
    timeout: int,
    poll_interval: float,
    deadline: float,
) -> int:
    """Polling loop body for ``_wait_for_terminal_state``.

    Extracted so the wrapper can catch ``KeyboardInterrupt`` once around the
    whole loop instead of around every ``time.sleep`` (VIB-3837).
    """
    seen_acknowledged = False
    seen_started = False
    last_phase: TeardownPhase | None = None
    last_progress: tuple[int, int, int] | None = None

    while True:
        request_row = manager.get_request(deployment_id)
        if request_row is None:
            # Production code never deletes terminal rows — `mark_completed` /
            # `mark_failed` / `mark_cancelled` preserve them. So a missing row
            # mid-wait means one of: (a) external process pruned the table,
            # (b) the resolver is reading a different DB than the runner,
            # (c) the row was never created. None of these are safe to report
            # as "COMPLETED — exit 0"; that masks exactly the silent-failure
            # class VIB-3835 closes. Surface as a non-zero error instead.
            click.echo()
            click.secho(
                f"Error: teardown_requests row for {deployment_id} disappeared while waiting.",
                fg="red",
                bold=True,
            )
            click.echo(
                "  This usually means the CLI and the runner are reading different DBs "
                "(check ALMANAK_STRATEGY_FOLDER / -d), or the row was deleted out-of-band."
            )
            return 1

        # Acknowledged transition.
        if not seen_acknowledged and request_row.acknowledged_at is not None:
            seen_acknowledged = True
            click.echo(
                f"  acknowledged — runner picked up the request at {format_datetime(request_row.acknowledged_at)}"
            )

        # Started transition.
        if not seen_started and request_row.started_at is not None:
            seen_started = True
            click.echo(
                "  started — runner began closing positions at "
                f"{format_datetime(request_row.started_at)} "
                f"({request_row.positions_total} position(s) total)"
            )

        # Phase / progress lines, only on change.
        if request_row.current_phase is not None and request_row.current_phase != last_phase:
            last_phase = request_row.current_phase
            click.echo(f"  phase — {last_phase.value}")

        progress = (
            request_row.positions_closed,
            request_row.positions_failed,
            request_row.positions_total,
        )
        if progress != last_progress and progress[2] > 0:
            last_progress = progress
            click.echo(f"  progress — closed={progress[0]}, failed={progress[1]}, total={progress[2]}")

        # Terminal states.
        status = request_row.status
        if status == TeardownStatus.COMPLETED:
            click.echo()
            click.echo(click.style(f"Teardown completed for {deployment_id}.", fg="green"))
            click.echo(
                f"  positions_closed={request_row.positions_closed}, "
                f"positions_failed={request_row.positions_failed}, "
                f"total={request_row.positions_total}"
            )
            # VIB-2932 / VIB-5472: surface the closure-verification confidence so
            # an UNVERIFIED (reported-but-not-chain-confirmed) closure is visible.
            _render_verification_status(manager, deployment_id)
            # VIB-5011: render the token-consolidation outcome. Exit code
            # stays 0 even when consolidation swaps failed — the closure
            # itself succeeded and on-chain risk is removed.
            _render_consolidation_summary(manager, deployment_id)
            return 0
        if status == TeardownStatus.FAILED:
            click.echo()
            click.secho(f"Teardown FAILED for {deployment_id}.", fg="red", bold=True)
            click.echo(
                f"  positions_closed={request_row.positions_closed}, "
                f"positions_failed={request_row.positions_failed}, "
                f"total={request_row.positions_total}"
            )
            return 1
        if status == TeardownStatus.CANCELLED:
            click.echo()
            click.secho(f"Teardown CANCELLED for {deployment_id}.", fg="yellow", bold=True)
            return 1

        # Timeout. Distinguish "runner never picked up" from "runner is
        # working but didn't finish" — the operator needs different
        # remediations for each.
        if time.monotonic() >= deadline:
            click.echo()
            if seen_acknowledged or seen_started:
                click.secho(
                    f"Error: teardown did not reach a terminal state within {timeout}s.",
                    fg="red",
                    bold=True,
                )
                click.echo(
                    f"  The runner is still working. Check progress: 'almanak strat teardown status -d <folder> -s {deployment_id}'."
                )
            else:
                click.secho(
                    f"Error: timeout waiting for runner acknowledgement ({timeout}s).",
                    fg="red",
                    bold=True,
                )
                click.echo(
                    f"  Is the runner running? Check 'almanak strat teardown status -d <folder> -s {deployment_id}'."
                )
            return 1

        time.sleep(poll_interval)


@teardown.command()
@click.option(
    "--working-dir",
    "-d",
    type=click.Path(),
    default=None,
    help=(
        "Strategy folder owning the local DB. Resolves like `strat run -d`. "
        "Hard-fails when no strategy folder resolves."
    ),
)
@click.option(
    "--strategy",
    "-s",
    required=True,
    help="Deployment ID or name to check",
)
@click.option(
    "--json",
    "-j",
    "as_json",
    is_flag=True,
    help="Output as JSON",
)
def status(working_dir: str | None, strategy: str, as_json: bool):
    """Check teardown status for a strategy.

    Shows the current state of any teardown request for the specified strategy.

    Examples:
        almanak strat teardown status -d strategies/my_strat -s uniswap_lp
        almanak strat teardown status -d strategies/my_strat -s aave_lev --json
    """
    _resolve_and_export_strategy_folder(working_dir)
    manager = _get_teardown_state_manager_or_die()
    request = manager.get_request(strategy)

    if not request:
        click.echo(f"No teardown request found for strategy: {strategy}")
        return

    if as_json:
        click.echo(json.dumps(request.to_dict(), indent=2))
        return

    # Display formatted status
    click.echo()
    click.echo(click.style(f"Teardown Status: {strategy}", bold=True, fg="cyan"))
    click.echo(f"  Status:       {format_status(request.status)}")
    click.echo(f"  Mode:         {format_mode(request.mode)}")
    click.echo(f"  Asset Policy: {request.asset_policy.value}")
    if request.asset_policy == TeardownAssetPolicy.TARGET_TOKEN:
        click.echo(f"  Target Token: {request.target_token}")
    if request.current_phase:
        click.echo(f"  Phase:        {request.current_phase.value}")
    click.echo(f"  Progress:     {format_progress(request)}")
    # Terminal payload (from result_json) — only present on COMPLETED rows
    # (mark_completed writes result_json; mark_failed does not — a FAILED row's
    # failure is already loud via Status + error_message).
    if request.status == TeardownStatus.COMPLETED:
        # VIB-2932 / VIB-5472: closure-verification confidence.
        _render_verification_status(manager, strategy)
        # VIB-5011: terminal consolidation summary.
        _render_consolidation_summary(manager, strategy)
    click.echo()
    click.echo(click.style("Timestamps:", bold=True))
    click.echo(f"  Requested:    {format_datetime(request.requested_at)}")
    click.echo(f"  Acknowledged: {format_datetime(request.acknowledged_at)}")
    click.echo(f"  Started:      {format_datetime(request.started_at)}")
    click.echo(f"  Completed:    {format_datetime(request.completed_at)}")
    if request.reason:
        click.echo()
        click.echo(f"  Reason: {request.reason}")
    click.echo()

    # Show actions
    if request.is_active:
        if request.can_cancel:
            click.echo(
                click.style(
                    "Tip: Use 'almanak strat teardown cancel --strategy " + strategy + "' to cancel",
                    fg="yellow",
                )
            )


@teardown.command()
@click.option(
    "--working-dir",
    "-d",
    type=click.Path(),
    default=None,
    help=(
        "Strategy folder owning the local DB. Resolves like `strat run -d`. "
        "Hard-fails when no strategy folder resolves."
    ),
)
@click.option(
    "--strategy",
    "-s",
    required=True,
    help="Deployment ID or name to cancel",
)
@click.option(
    "--force",
    "-f",
    is_flag=True,
    help="Skip confirmation prompt",
)
def cancel(working_dir: str | None, strategy: str, force: bool):
    """Cancel a pending or in-progress teardown.

    For graceful mode: Cancellable anytime before completion.
    For emergency mode: Only cancellable during the 10-second window.

    Examples:
        almanak strat teardown cancel -d strategies/my_strat -s uniswap_lp
        almanak strat teardown cancel -d strategies/my_strat -s aave_lev --force
    """
    _resolve_and_export_strategy_folder(working_dir)
    manager = _get_teardown_state_manager_or_die()
    request = manager.get_active_request(strategy)

    if not request:
        click.echo(f"No active teardown request found for strategy: {strategy}")
        return

    if not request.can_cancel:
        click.echo(
            click.style(
                f"Cannot cancel: teardown is past the cancel deadline (status: {request.status.value})",
                fg="red",
            )
        )
        return

    # Show current state
    click.echo()
    click.echo(f"Current teardown status: {format_status(request.status)}")
    click.echo(f"Mode: {format_mode(request.mode)}")
    click.echo()

    # Confirm
    if not force:
        if not click.confirm("Cancel this teardown?"):
            click.echo("Cancelled.")
            return

    # Request cancellation
    success = manager.request_cancel(strategy)

    if success:
        click.echo(click.style("Cancellation requested!", fg="green"))
        click.echo("The strategy will cancel on its next iteration.")
    else:
        click.echo(click.style("Failed to request cancellation.", fg="red"))


@teardown.command(name="list")
@click.option(
    "--working-dir",
    "-d",
    type=click.Path(),
    default=None,
    help=(
        "Strategy folder owning the local DB. Resolves like `strat run -d`. "
        "Hard-fails when no strategy folder resolves."
    ),
)
@click.option(
    "--all",
    "-a",
    "show_all",
    is_flag=True,
    help="Show all requests (including completed/cancelled)",
)
@click.option(
    "--json",
    "-j",
    "as_json",
    is_flag=True,
    help="Output as JSON",
)
def list_teardowns(working_dir: str | None, show_all: bool, as_json: bool):
    """List teardown requests in the strategy-folder DB.

    By default, shows only active teardowns. Use --all to include
    completed and cancelled requests.

    Examples:
        almanak strat teardown list -d strategies/my_strat
        almanak strat teardown list -d strategies/my_strat --all
        almanak strat teardown list -d strategies/my_strat --json
    """
    _resolve_and_export_strategy_folder(working_dir)
    manager = _get_teardown_state_manager_or_die()

    if show_all:
        requests = manager.get_all_requests()
    else:
        requests = manager.get_all_active_requests()

    if not requests:
        if as_json:
            click.echo("[]")
            return
        click.echo("No active teardown requests found.")
        return

    if as_json:
        click.echo(json.dumps([r.to_dict() for r in requests], indent=2))
        return

    # Display table
    click.echo()
    click.echo(click.style("Active Teardown Requests", bold=True, fg="cyan"))
    click.echo()

    # Header
    click.echo(f"{'Strategy':<25} {'Status':<15} {'Mode':<12} {'Phase':<20} {'Progress':<15}")
    click.echo("-" * 90)

    for req in requests:
        phase = req.current_phase.value if req.current_phase else "-"
        progress = format_progress(req)
        click.echo(
            f"{req.deployment_id:<25} "
            f"{format_status(req.status):<15} "
            f"{format_mode(req.mode):<12} "
            f"{phase:<20} "
            f"{progress:<15}"
        )

    click.echo()


# =============================================================================
# CLI Entry Point
# =============================================================================


def main():
    """CLI entry point."""
    teardown()


if __name__ == "__main__":
    main()
