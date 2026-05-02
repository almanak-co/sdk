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
import os
import sys
import time
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import click

logger = logging.getLogger(__name__)

from ..teardown import (
    PositionType,
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
    "  A strategy folder must contain config.json (and strategy.py)."
)


def _looks_like_strategy_folder(path: Path) -> bool:
    """Return True if ``path`` contains a config.json (or strategy.py).

    config.json is the primary signal — every Almanak strategy folder has one.
    strategy.py is accepted as a fallback because some incubating strategies
    rely on the decorator-driven config and ship without a config.json.
    """
    if not path.is_dir():
        return False
    if (path / "config.json").exists():
        return True
    if (path / "config.yaml").exists() or (path / "config.yml").exists():
        return True
    return (path / "strategy.py").exists()


def _resolve_and_export_strategy_folder(working_dir: str | None) -> Path:
    """Resolve the strategy folder and export it in the env for downstream code.

    Raises ``click.ClickException`` with the canonical remediation hint when
    no folder can be resolved. Always exits non-zero on failure so operator
    scripts can detect a misconfigured invocation rather than silently writing
    to the wrong DB.
    """
    # Step 1: explicit -d flag wins.
    if working_dir is not None:
        candidate = Path(working_dir).expanduser().resolve()
        if not candidate.is_dir():
            raise click.ClickException(f"--working-dir does not exist or is not a directory: {candidate}")
        if not _looks_like_strategy_folder(candidate):
            raise click.ClickException(
                f"--working-dir does not look like a strategy folder: {candidate}\n  {_STRATEGY_FOLDER_HINT}"
            )
        os.environ["ALMANAK_STRATEGY_FOLDER"] = str(candidate)
        # Reset any singleton state managers cached from a prior path so the
        # exported folder takes effect for this invocation.
        _reset_teardown_state_singleton()
        return candidate

    # Step 2: respect a folder already exported by a parent process (e.g. a
    # `strat run` that shells out to `teardown` for some scripted flow).
    env_folder = os.environ.get("ALMANAK_STRATEGY_FOLDER")
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
        os.environ["ALMANAK_STRATEGY_FOLDER"] = str(cwd)
        _reset_teardown_state_singleton()
        return cwd

    # Step 4: hard-fail.
    raise click.ClickException(f"no strategy folder resolved.\n  {_STRATEGY_FOLDER_HINT}")


def _build_no_op_teardown_message(strategy_id: str) -> str:
    """Canonical no-op teardown log line (VIB-3705).

    Single source of truth so the two CLI call sites (the empty-positions
    branch around line 814 and the empty-intents branch around line 1027)
    produce byte-identical output. QA harnesses grep this line to
    distinguish "no work was required" from a real teardown completion.
    """
    return f"Teardown: no open positions for strategy {strategy_id}; nothing to close. Exiting 0."


def _reset_teardown_state_singleton() -> None:
    """Clear the cached ``TeardownStateManager`` so the next call re-resolves.

    The singleton in ``get_teardown_state_manager`` caches the DB path on first
    call; if a CLI invocation sets ``ALMANAK_STRATEGY_FOLDER`` after the cache
    was populated (e.g. via a prior import), subsequent calls would otherwise
    hit the wrong DB.
    """
    from ..teardown import state_manager as state_manager_module

    state_manager_module._default_manager = None


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


def _build_strategy_id_candidates(strategy: Any, strategy_class: type, config_dict: dict[str, Any]) -> list[str]:
    """Build strategy_id candidates for state restore."""
    candidates: list[str] = []

    def _add_candidate(value: Any) -> None:
        if not isinstance(value, str) or not value.strip():
            return
        clean_value = value.strip()
        candidates.append(clean_value)
        if ":" in clean_value:
            prefix = clean_value.split(":", maxsplit=1)[0].strip()
            if prefix:
                candidates.append(prefix)

    _add_candidate(config_dict.get("strategy_id"))
    _add_candidate(getattr(strategy, "strategy_id", ""))
    _add_candidate(getattr(strategy, "name", ""))
    _add_candidate(getattr(strategy, "STRATEGY_NAME", ""))
    _add_candidate(strategy_class.__name__)

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

    candidates = _build_strategy_id_candidates(strategy, strategy_class, config_dict)
    if not candidates:
        logger.info("No strategy_id candidates available for teardown state restore")
        return

    state_manager = GatewayStateManager(gateway_client)
    for strategy_id in candidates:
        logger.info("Attempting teardown state restore for strategy_id=%s", strategy_id)
        try:
            strategy.set_state_manager(state_manager, strategy_id)
        except Exception as e:
            logger.warning("Failed to inject state manager for strategy_id=%s: %s", strategy_id, e)
            continue

        try:
            if hasattr(strategy, "load_state_async"):
                loaded = asyncio.run(strategy.load_state_async())
            elif hasattr(strategy, "load_state"):
                loaded = strategy.load_state()
            else:
                loaded = False

            if loaded:
                logger.info("Restored strategy state for teardown (strategy_id=%s)", strategy_id)
                return
            logger.info("No persisted strategy state for strategy_id=%s", strategy_id)
        except Exception as e:
            logger.warning("State restore failed for strategy_id=%s: %s", strategy_id, e)

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
@click.option(
    "--gateway-host",
    default="localhost",
    envvar="GATEWAY_HOST",
    help="Gateway sidecar hostname.",
)
@click.option(
    "--gateway-port",
    default=50051,
    type=int,
    envvar="GATEWAY_PORT",
    help="Gateway sidecar gRPC port.",
)
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
def execute_teardown(
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
    from eth_account import Account

    click.echo("=" * 60)
    click.echo("ALMANAK STRATEGY TEARDOWN")
    click.echo("=" * 60)

    working_path = Path(working_dir).resolve()

    # Export ALMANAK_STRATEGY_FOLDER so the strategy-scoped DB resolver
    # (local_strategy_db_path, VIB-3835) sees the same folder the operator
    # passed via -d. Mirrors `strat run` and matches the request/status/list/
    # cancel subcommands' resolver. We export unconditionally here because
    # execute_teardown's -d defaults to cwd, and a cwd-scoped invocation that
    # forgot to set the env var would otherwise have downstream adapters
    # raise LocalPathError.
    if working_path.is_dir():
        os.environ["ALMANAK_STRATEGY_FOLDER"] = str(working_path)
        _reset_teardown_state_singleton()

    # Load environment from .env if present
    from dotenv import load_dotenv

    from almanak.core.redaction import install_redaction

    env_file = working_path / ".env"
    if env_file.exists():
        load_dotenv(env_file)
        click.echo(f"Loaded environment from: {env_file}")

    # Install secret redaction after env is loaded so all secrets are registered.
    install_redaction()

    # Fail fast on incompatible option combinations before touching the filesystem.
    if no_gateway and network is not None:
        raise click.ClickException(
            "--network only applies when the managed gateway is auto-started. Remove --network or remove --no-gateway."
        )

    # Find strategy.py
    strategy_file = working_path / "strategy.py"
    if not strategy_file.exists():
        raise click.ClickException(f"No strategy.py found in {working_dir}")

    # Load strategy class
    strategy_class, error = load_strategy_from_file(strategy_file)
    if error or strategy_class is None:
        raise click.ClickException(f"Failed to load strategy: {error}")

    click.echo(f"Loaded strategy: {strategy_class.__name__}")

    # Load config
    if config_file is None:
        for name in ["config.json", "config.yaml", "config.yml"]:
            potential = working_path / name
            if potential.exists():
                config_file = str(potential)
                break

    if config_file:
        click.echo(f"Loaded config from: {config_file}")
        with open(config_file) as f:
            if config_file.endswith((".yaml", ".yml")):
                import yaml

                config_dict = yaml.safe_load(f) or {}
            else:
                config_dict = json.load(f)
    else:
        config_dict = {}

    # Resolve chain: config.json override first, then decorator metadata
    from .run import get_default_chain

    chain = config_dict.get("chain") or get_default_chain(strategy_class)

    # Gateway setup: auto-start a managed gateway or connect to an existing one
    import atexit

    from almanak.gateway.core.settings import GatewaySettings
    from almanak.gateway.managed import ManagedGateway, find_available_gateway_port

    from ..gateway_client import GatewayClient, GatewayClientConfig

    # Normalize "localhost" to "127.0.0.1" (gateway binds to 127.0.0.1)
    effective_host = "127.0.0.1" if gateway_host == "localhost" else gateway_host

    managed_gateway: ManagedGateway | None = None

    if no_gateway:
        # --no-gateway: connect to an existing gateway, fail if unavailable
        click.echo(f"Connecting to existing gateway at {effective_host}:{gateway_port}...")
        gateway_config = GatewayClientConfig(host=effective_host, port=gateway_port)
        gateway_client = GatewayClient(gateway_config)
        gateway_client.connect()

        if not gateway_client.health_check():
            gateway_client.disconnect()
            click.echo()
            click.secho("ERROR: Gateway is not running or not healthy", fg="red", bold=True)
            click.echo()
            click.echo("The gateway sidecar is required for teardown operations.")
            click.echo("Start the gateway first with:")
            click.echo()
            click.echo("  almanak gateway")
            click.echo()
            raise click.ClickException(f"Gateway not available at {effective_host}:{gateway_port}")

        click.secho(f"Connected to existing gateway at {effective_host}:{gateway_port}", fg="green")
    else:
        # Default: auto-start a managed gateway
        try:
            gateway_port = find_available_gateway_port(effective_host, gateway_port)
        except RuntimeError as e:
            click.echo()
            click.secho(f"ERROR: {e}", fg="red", bold=True)
            click.echo()
            click.echo("Set a specific port with:")
            click.echo()
            click.echo("  almanak strat teardown execute --gateway-port <port>")
            click.echo()
            click.echo("Or connect to an existing gateway:")
            click.echo()
            click.echo("  almanak strat teardown execute --no-gateway --gateway-port <port>")
            click.echo()
            logger.error("Managed gateway failed to start", exc_info=True)
            raise click.ClickException(str(e)) from e

        # Security: generate a random session token for the managed gateway so it
        # is never running without authentication on mainnet (matching run.py pattern).
        import uuid

        session_auth_token = uuid.uuid4().hex

        resolved_network = network or "mainnet"
        gateway_settings = GatewaySettings(
            grpc_host=effective_host,
            grpc_port=gateway_port,
            network=resolved_network,
            allow_insecure=resolved_network == "anvil",
            metrics_enabled=False,
            audit_enabled=False,
            chains=[chain] if chain else [],
            auth_token=session_auth_token,
        )

        click.echo(
            f"Starting managed gateway on {effective_host}:{gateway_port} (network={resolved_network}, chain={chain})..."
        )
        managed_gateway = ManagedGateway(gateway_settings)
        try:
            managed_gateway.start(timeout=10.0)
        except RuntimeError as e:
            logger.error("Managed gateway startup failed", exc_info=True)
            click.echo()
            click.secho(f"ERROR: Failed to start managed gateway: {e}", fg="red", bold=True)
            click.echo()
            raise click.ClickException("Managed gateway startup failed") from e

        # Register atexit handler as safety net for sys.exit() paths that skip cleanup
        atexit.register(managed_gateway.stop)

        click.secho(f"Managed gateway started on {effective_host}:{gateway_port}", fg="green")

        # Connect client to the managed gateway
        gateway_config = GatewayClientConfig(host=effective_host, port=gateway_port, auth_token=session_auth_token)
        gateway_client = GatewayClient(gateway_config)
        gateway_client.connect()

        if not gateway_client.health_check():
            managed_gateway.stop()
            gateway_client.disconnect()
            raise click.ClickException(
                "Managed gateway started but health check failed. Check gateway logs above for details."
            )

    # Wire gateway channel into TokenResolver for on-chain token discovery
    from ..data.tokens import get_token_resolver

    resolver = get_token_resolver()

    try:
        resolver.set_gateway_channel(gateway_client.channel)

        # Resolve wallet address from config first, then private key.
        wallet_address = config_dict.get("wallet_address")
        if not wallet_address:
            private_key = os.environ.get("ALMANAK_PRIVATE_KEY", "")
            if not private_key:
                raise click.ClickException(
                    "Could not determine wallet address. Set config.wallet_address or ALMANAK_PRIVATE_KEY."
                )
            wallet_address = Account.from_key(private_key).address

        # Create config object -- deferred import to avoid heavy run.py import cascade (VIB-522)
        from almanak.framework.cli.run import DictConfigWrapper

        config_obj = DictConfigWrapper(config_dict)

        # Instantiate strategy
        try:
            strategy = strategy_class(
                config=config_obj,
                chain=chain,
                wallet_address=wallet_address,
            )
        except Exception as e:
            logger.error("Failed to instantiate strategy", exc_info=True)
            raise click.ClickException(f"Failed to instantiate strategy: {e}") from e

        # Inject balance provider so generate_teardown_intents() can use market.balance().
        # Without this, custom strategies that check balances during teardown crash.
        _inject_balance_provider(strategy, gateway_client, chain, wallet_address)

        _restore_strategy_state_for_teardown(
            strategy=strategy,
            strategy_class=strategy_class,
            config_dict=config_dict,
            gateway_client=gateway_client,
        )

        click.echo(f"Strategy: {strategy_class.__name__}")
        click.echo(f"Chain: {chain}")
        click.echo(f"Wallet: {wallet_address}")
        click.echo(f"Mode: {mode}")
        click.echo("-" * 60)

        # Get positions. --discover bypasses the strategy's local state and
        # reads NPM contracts directly via the gateway, so orphaned positions
        # (e.g. after a gateway restart lost the in-memory tracking) remain
        # recoverable. Without --discover, the strategy's own tracking is
        # authoritative — it knows value_usd, health factors, and
        # non-LP positions the on-chain scan wouldn't surface.
        if discover:
            from ..teardown.discovery import discover_lp_positions, to_teardown_summary

            click.echo("\nDiscovering LP positions on-chain...")

            async def _do_discover():
                return await discover_lp_positions(
                    client=gateway_client,
                    chain=chain,
                    wallet=wallet_address,
                    include_zero_liquidity=include_empty,
                )

            try:
                discovered = asyncio.run(_do_discover())
            except Exception as e:
                logger.error("On-chain discovery failed", exc_info=True)
                raise click.ClickException(f"On-chain discovery failed: {e}") from e

            positions = to_teardown_summary(
                strategy_id=getattr(strategy, "strategy_id", strategy_class.__name__),
                chain=chain,
                positions=discovered,
            )
            click.echo(f"  Found {len(discovered)} on-chain LP position(s).")
        else:
            try:
                positions = strategy.get_open_positions()
            except Exception as e:
                logger.error("Failed to get positions from strategy", exc_info=True)
                raise click.ClickException(f"Failed to get positions: {e}") from e

        if not positions.positions:
            # VIB-3705: emit the canonical no-op success message that QA
            # harnesses (and CI) can grep for to distinguish "no work was
            # required" from "real teardown failure". Returning here yields
            # exit 0 via Click's normal command-return semantics — swap-only
            # / HOLD-state strategies (uniswap_v4_swap_*, fluid_swap_*,
            # edge_yield_*_fluiddex, edge_yield_base_univ4) hit this branch
            # whenever the wallet's balance for the strategy's quote/target
            # token is 0, and treating that as exit 1 produced 5+ false
            # failures in the April 28-29 QA batch.
            strategy_id_for_log = getattr(strategy, "strategy_id", strategy_class.__name__)
            no_op_msg = _build_no_op_teardown_message(strategy_id_for_log)
            click.echo()
            click.secho(no_op_msg, fg="green")
            logger.info(no_op_msg)
            if not discover:
                click.echo(
                    "Tip: if positions were opened by a previous gateway instance, "
                    "rerun with --discover to scan NPM contracts on-chain."
                )
            return

        # Display positions
        click.echo(f"\nOpen Positions ({len(positions.positions)}):")
        total_value = Decimal("0")
        unknown_value_count = 0
        for i, pos in enumerate(positions.positions, 1):
            click.echo(f"  {i}. [{pos.position_type.value}] {pos.protocol} on {pos.chain}")
            click.echo(f"     Position ID: {pos.position_id}")
            # Some test doubles don't expose `details` — tolerate that while
            # still checking the flag on real PositionInfo instances.
            pos_details = getattr(pos, "details", None) or {}
            if pos_details.get("value_usd_unknown"):
                click.echo("     Value: unknown (discovered on-chain, not priced)")
                unknown_value_count += 1
            else:
                click.echo(f"     Value: ${pos.value_usd:,.2f}")
            if pos.health_factor:
                click.echo(f"     Health Factor: {pos.health_factor:.2f}")
            total_value += pos.value_usd

        click.echo(f"\nTotal Value: ${total_value:,.2f}")

        # Loud warning when --discover couldn't price positions. SafetyGuard
        # uses total_value_usd to pick the loss cap, and $0 maps to the
        # *most permissive* 3% tier (calculate_max_acceptable_loss). A
        # mispriced $1M LP would otherwise get the same cap as a $100
        # position. Flag this so the operator knows to double-check
        # (CodeRabbit major, PR #1522).
        if unknown_value_count > 0:
            click.echo()
            click.secho(
                f"WARNING: {unknown_value_count} position(s) discovered without USD pricing. "
                "Teardown safety caps will be computed as if total value = $0, which uses the "
                "MOST PERMISSIVE loss tier. Review the tick ranges above before executing.",
                fg="yellow",
                bold=True,
            )

        # Create market snapshot early so the preview intents match what will execute
        market = None
        price_oracle = None
        try:
            if hasattr(strategy, "create_market_snapshot"):
                market = strategy.create_market_snapshot()
                if hasattr(market, "get_price_oracle_dict"):
                    fetched = market.get_price_oracle_dict()
                    price_oracle = fetched if fetched is not None else None
                    if price_oracle:
                        click.echo(f"\n  Using real prices for {len(price_oracle)} tokens")
        except Exception as e:
            click.echo(f"\n  Warning: Could not get market prices ({e}), using placeholders")

        # Generate teardown intents with market so preview matches execution.
        # In --discover mode the strategy has no record of the discovered
        # positions, so we synthesize LPCloseIntents directly from the NPM
        # data instead of calling strategy.generate_teardown_intents().
        internal_mode = TeardownMode.SOFT if mode == "graceful" else TeardownMode.HARD
        if discover:
            from ..intents.vocabulary import LPCloseIntent

            # Graceful teardowns collect fees (extra tx per position, but
            # captures accrued yield). Emergency teardowns skip fee collection
            # to minimise wall-clock time and gas — the operator has already
            # signalled "close fast, accept worst case" by picking emergency
            # mode, so the extra collect() call is not worth the delay.
            collect_fees_default = internal_mode == TeardownMode.SOFT
            intents = [
                LPCloseIntent(
                    position_id=pos.position_id,
                    protocol=pos.protocol,
                    chain=pos.chain,
                    collect_fees=collect_fees_default,
                )
                for pos in positions.positions
                if pos.position_type == PositionType.LP
            ]
        else:
            try:
                try:
                    intents = strategy.generate_teardown_intents(internal_mode, market=market)
                except TypeError as exc:
                    if "market" in str(exc):
                        intents = strategy.generate_teardown_intents(internal_mode)
                    else:
                        raise
            except Exception as e:
                logger.error("Failed to generate teardown intents", exc_info=True)
                raise click.ClickException(f"Failed to generate teardown intents: {e}") from e

        click.echo(f"\nTeardown Steps ({len(intents)}):")
        for i, intent in enumerate(intents, 1):
            intent_type = getattr(intent, "intent_type", "UNKNOWN")
            if hasattr(intent_type, "value"):
                intent_type = intent_type.value
            click.echo(f"  {i}. {intent_type}")

        if preview:
            click.echo("\n[PREVIEW MODE] No changes will be made.")
            return

        # Confirmation
        if not force:
            click.echo("\n" + "=" * 60)
            click.echo("WARNING: This will close all positions listed above.")
            click.echo("=" * 60)
            if not click.confirm("Do you want to proceed?"):
                click.echo("Teardown cancelled.")
                return

        # Execute teardown
        click.echo("\nExecuting teardown...")

        from ..execution.gateway_orchestrator import GatewayExecutionOrchestrator
        from ..intents.compiler import IntentCompiler, IntentCompilerConfig
        from ..teardown.teardown_manager import TeardownManager

        # Create orchestrator via gateway
        orchestrator = GatewayExecutionOrchestrator(client=gateway_client, chain=chain, wallet_address=wallet_address)

        # market and price_oracle were created above (before preview) so the
        # operator confirms the same intents that will execute.

        # Create compiler with real prices if available.
        # gateway_client is mandatory: LP_CLOSE compilation queries on-chain state
        # (ERC20 LP balances for Aerodrome, position liquidity for Uniswap V3).
        # Without it every on-chain query returns None and compilation fails silently.
        compiler_config = IntentCompilerConfig(allow_placeholder_prices=price_oracle is None)
        compiler = IntentCompiler(
            chain=chain,
            wallet_address=wallet_address,
            rpc_url=None,  # Will use gateway
            price_oracle=price_oracle,
            config=compiler_config,
            gateway_client=gateway_client,
        )

        # Create teardown manager with state persistence (VIB-2924)
        from almanak.framework.local_paths import LocalPathError
        from almanak.framework.teardown.state_manager import TeardownStateAdapter

        # VIB-3835: TeardownStateAdapter() resolves through the strict
        # strategy-scoped path resolver. Surface LocalPathError as a clean CLI
        # error (canonical remediation hint already in the message) rather
        # than letting it bubble out as a raw traceback.
        try:
            teardown_state_adapter = TeardownStateAdapter()
        except LocalPathError as exc:
            raise click.ClickException(str(exc)) from exc
        teardown_manager = TeardownManager(
            orchestrator=orchestrator,  # type: ignore[arg-type]  # duck-typed orchestrator
            compiler=compiler,
            state_manager=teardown_state_adapter,
        )

        # Execute with progress callback. When --discover is active the
        # strategy has no knowledge of the on-chain-discovered positions, so
        # pass the already-built summary and intents straight through;
        # otherwise let the manager derive them from the strategy as normal.
        async def run_teardown():
            async def on_progress(pct: int, msg: str):
                click.echo(f"  [{pct}%] {msg}")

            kwargs = {
                "strategy": strategy,
                "mode": mode,
                "on_progress": on_progress,
                "market": market,
            }
            if discover:
                kwargs["precomputed_positions"] = positions
                kwargs["precomputed_intents"] = intents

            result = await teardown_manager.execute(**kwargs)
            return result

        try:
            result = asyncio.run(run_teardown())
        except Exception as e:
            logger.error("Teardown execution failed", exc_info=True)
            raise click.ClickException(f"Teardown execution failed: {e}") from e

        # Display results
        click.echo("\n" + "=" * 60)
        # VIB-3705: TeardownManager returns _empty_result(success=True,
        # intents_total=0) when ``generate_teardown_intents()`` returned an
        # empty list (Branch 2 of the "nothing to do" taxonomy). The post-
        # execution summary loses signal in that case — "0/0 intents,
        # $0 starting value" reads like a degenerate result rather than
        # the explicit no-op success it actually is. Surface the canonical
        # no-op log so QA harnesses can distinguish it from an executed
        # teardown.
        strategy_id_for_log = getattr(strategy, "strategy_id", strategy_class.__name__)
        if result.success and result.intents_total == 0:
            no_op_msg = _build_no_op_teardown_message(strategy_id_for_log)
            click.secho(no_op_msg, fg="green")
            logger.info(no_op_msg)
        elif result.success:
            click.echo(click.style("[SUCCESS] Teardown completed successfully!", fg="green"))
        else:
            click.echo(click.style(f"[FAILED] Teardown failed: {result.error}", fg="red"))

        click.echo(f"  Duration: {result.duration_seconds:.1f}s")
        click.echo(f"  Intents executed: {result.intents_succeeded}/{result.intents_total}")
        if result.intents_failed > 0:
            click.echo(f"  Intents failed: {result.intents_failed}")
        click.echo(f"  Starting value: ${result.starting_value_usd:,.2f}")
        click.echo(f"  Final value: ${result.final_value_usd:,.2f}")
        click.echo(f"  Total costs: ${result.total_costs_usd:,.2f}")
        click.echo("=" * 60)

        if not result.success:
            sys.exit(1)
    finally:
        resolver.set_gateway_channel(None)
        gateway_client.disconnect()
        if managed_gateway is not None:
            managed_gateway.stop()


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
    help="Strategy ID or name to teardown",
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
        strategy_id=strategy,
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


def _wait_for_terminal_state(manager: Any, strategy_id: str, timeout: int) -> int:
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

    seen_acknowledged = False
    seen_started = False
    last_phase: TeardownPhase | None = None
    last_progress: tuple[int, int, int] | None = None

    click.echo()
    click.echo(click.style(f"Waiting (timeout={timeout}s)...", fg="cyan"))
    click.echo("  pending — request created, waiting for runner to pick up")

    while True:
        request_row = manager.get_request(strategy_id)
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
                f"Error: teardown_requests row for {strategy_id} disappeared while waiting.",
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
            click.echo(click.style(f"Teardown completed for {strategy_id}.", fg="green"))
            click.echo(
                f"  positions_closed={request_row.positions_closed}, "
                f"positions_failed={request_row.positions_failed}, "
                f"total={request_row.positions_total}"
            )
            return 0
        if status == TeardownStatus.FAILED:
            click.echo()
            click.secho(f"Teardown FAILED for {strategy_id}.", fg="red", bold=True)
            click.echo(
                f"  positions_closed={request_row.positions_closed}, "
                f"positions_failed={request_row.positions_failed}, "
                f"total={request_row.positions_total}"
            )
            return 1
        if status == TeardownStatus.CANCELLED:
            click.echo()
            click.secho(f"Teardown CANCELLED for {strategy_id}.", fg="yellow", bold=True)
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
                    f"  The runner is still working. Check progress: 'almanak strat teardown status -d <folder> -s {strategy_id}'."
                )
            else:
                click.secho(
                    f"Error: timeout waiting for runner acknowledgement ({timeout}s).",
                    fg="red",
                    bold=True,
                )
                click.echo(
                    f"  Is the runner running? Check 'almanak strat teardown status -d <folder> -s {strategy_id}'."
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
    help="Strategy ID or name to check",
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
    help="Strategy ID or name to cancel",
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
            f"{req.strategy_id:<25} "
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
