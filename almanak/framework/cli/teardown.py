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
    almanak strat teardown -d strategies/demo/aerodrome_lp --preview
    almanak strat teardown -d strategies/demo/aave_borrow --mode graceful

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
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import click

logger = logging.getLogger(__name__)

from ..teardown import (
    TeardownAssetPolicy,
    TeardownMode,
    TeardownRequest,
    TeardownStatus,
    get_teardown_state_manager,
)

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

        # Find IntentStrategy subclasses
        strategy_classes = []
        for name in dir(module):
            obj = getattr(module, name)
            if isinstance(obj, type) and obj is not IntentStrategy and issubclass(obj, IntentStrategy):
                strategy_classes.append(obj)

        if not strategy_classes:
            return None, "No IntentStrategy subclass found in file"

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
    if not hasattr(strategy, "set_state_manager") or not hasattr(strategy, "load_state"):
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
            if strategy.load_state():
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

            price_oracle = GatewayPriceOracle(gateway_client)
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
def execute_teardown(
    working_dir: str,
    config_file: str | None,
    mode: str,
    preview: bool,
    force: bool,
    gateway_host: str,
    gateway_port: int,
    no_gateway: bool,
):
    """Execute teardown directly from a strategy working directory.

    This command loads a strategy from its working directory and immediately
    executes a teardown to close all open positions. A managed gateway is
    auto-started by default (like ``strat run``). Use --no-gateway to connect
    to an already-running gateway instead.

    Examples:

        # Preview what will be closed (auto-starts gateway)
        almanak strat teardown execute -d strategies/demo/aerodrome_lp --preview

        # Execute graceful teardown
        almanak strat teardown execute -d strategies/demo/aerodrome_lp

        # Emergency teardown (faster, accepts higher slippage)
        almanak strat teardown execute -d strategies/demo/aave_borrow --mode emergency

        # Connect to an existing gateway instead of auto-starting
        almanak strat teardown execute -d strategies/demo/uniswap_lp --no-gateway

        # Skip confirmation
        almanak strat teardown execute -d strategies/demo/uniswap_lp --force
    """
    import os

    from eth_account import Account

    click.echo("=" * 60)
    click.echo("ALMANAK STRATEGY TEARDOWN")
    click.echo("=" * 60)

    working_path = Path(working_dir).resolve()

    # Load environment from .env if present
    from dotenv import load_dotenv

    from almanak.core.redaction import install_redaction

    env_file = working_path / ".env"
    if env_file.exists():
        load_dotenv(env_file)
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
            raise click.ClickException(str(e)) from None

        gateway_settings = GatewaySettings(
            grpc_host=effective_host,
            grpc_port=gateway_port,
            network="mainnet",
            allow_insecure=True,
            metrics_enabled=False,
            audit_enabled=False,
        )

        click.echo(f"Starting managed gateway on {effective_host}:{gateway_port} (network=mainnet)...")
        managed_gateway = ManagedGateway(gateway_settings)
        try:
            managed_gateway.start(timeout=10.0)
        except RuntimeError as e:
            click.echo()
            click.secho(f"ERROR: Failed to start managed gateway: {e}", fg="red", bold=True)
            click.echo()
            raise click.ClickException("Managed gateway startup failed") from e

        # Register atexit handler as safety net for sys.exit() paths that skip cleanup
        atexit.register(managed_gateway.stop)

        click.secho(f"Managed gateway started on {effective_host}:{gateway_port}", fg="green")

        # Connect client to the managed gateway
        gateway_config = GatewayClientConfig(host=effective_host, port=gateway_port)
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
            raise click.ClickException(f"Failed to instantiate strategy: {e}") from None

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

        # Get positions
        try:
            positions = strategy.get_open_positions()
        except Exception as e:
            raise click.ClickException(f"Failed to get positions: {e}") from None

        if not positions.positions:
            click.echo("\nNo open positions found. Nothing to teardown.")
            return

        # Display positions
        click.echo(f"\nOpen Positions ({len(positions.positions)}):")
        total_value = Decimal("0")
        for i, pos in enumerate(positions.positions, 1):
            click.echo(f"  {i}. [{pos.position_type.value}] {pos.protocol} on {pos.chain}")
            click.echo(f"     Position ID: {pos.position_id}")
            click.echo(f"     Value: ${pos.value_usd:,.2f}")
            if pos.health_factor:
                click.echo(f"     Health Factor: {pos.health_factor:.2f}")
            total_value += pos.value_usd

        click.echo(f"\nTotal Value: ${total_value:,.2f}")

        # Create market snapshot early so the preview intents match what will execute
        market = None
        price_oracle = None
        try:
            if hasattr(strategy, "create_market_snapshot"):
                market = strategy.create_market_snapshot()
                if hasattr(market, "get_price_oracle_dict"):
                    price_oracle = market.get_price_oracle_dict() or None
                    if price_oracle:
                        click.echo(f"\n  Using real prices for {len(price_oracle)} tokens")
        except Exception as e:
            click.echo(f"\n  Warning: Could not get market prices ({e}), using placeholders")

        # Generate teardown intents with market so preview matches execution
        internal_mode = TeardownMode.SOFT if mode == "graceful" else TeardownMode.HARD
        try:
            try:
                intents = strategy.generate_teardown_intents(internal_mode, market=market)
            except TypeError as exc:
                if "market" in str(exc):
                    intents = strategy.generate_teardown_intents(internal_mode)
                else:
                    raise
        except Exception as e:
            raise click.ClickException(f"Failed to generate teardown intents: {e}") from None

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

        # Create compiler with real prices if available
        compiler_config = IntentCompilerConfig(allow_placeholder_prices=price_oracle is None)
        compiler = IntentCompiler(
            chain=chain,
            wallet_address=wallet_address,
            rpc_url=None,  # Will use gateway
            price_oracle=price_oracle,
            config=compiler_config,
        )

        # Create teardown manager
        teardown_manager = TeardownManager(
            orchestrator=orchestrator,  # type: ignore[arg-type]  # duck-typed orchestrator
            compiler=compiler,
        )

        # Execute with progress callback
        async def run_teardown():
            async def on_progress(pct: int, msg: str):
                click.echo(f"  [{pct}%] {msg}")

            result = await teardown_manager.execute(
                strategy=strategy,
                mode=mode,
                on_progress=on_progress,
                market=market,
            )
            return result

        try:
            result = asyncio.run(run_teardown())
        except Exception as e:
            raise click.ClickException(f"Teardown execution failed: {e}") from None

        # Display results
        click.echo("\n" + "=" * 60)
        if result.success:
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
def request(
    strategy: str,
    mode: str,
    asset_policy: str,
    target_token: str,
    reason: str | None,
    force: bool,
):
    """Request a teardown for a strategy.

    This creates a teardown request that will be picked up by the strategy
    runner on the next iteration. The strategy will then execute the teardown
    with the specified parameters.

    Examples:
        # Graceful teardown with default settings
        almanak strat teardown request --strategy uniswap_lp --mode graceful

        # Emergency teardown keeping native tokens
        almanak strat teardown request --strategy aave_leverage --mode emergency --asset-policy keep

        # Teardown with reason
        almanak strat teardown request --strategy gmx_perp --mode graceful --reason "Rebalancing"
    """
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
    manager = get_teardown_state_manager()

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
    click.echo("The strategy will pick up this request on the next iteration.")
    click.echo(f"Use 'almanak strat teardown status --strategy {strategy}' to monitor progress.")


@teardown.command()
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
def status(strategy: str, as_json: bool):
    """Check teardown status for a strategy.

    Shows the current state of any teardown request for the specified strategy.

    Examples:
        almanak strat teardown status --strategy uniswap_lp
        almanak strat teardown status --strategy aave_leverage --json
    """
    manager = get_teardown_state_manager()
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
def cancel(strategy: str, force: bool):
    """Cancel a pending or in-progress teardown.

    For graceful mode: Cancellable anytime before completion.
    For emergency mode: Only cancellable during the 10-second window.

    Examples:
        almanak strat teardown cancel --strategy uniswap_lp
        almanak strat teardown cancel --strategy aave_leverage --force
    """
    manager = get_teardown_state_manager()
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
def list_teardowns(show_all: bool, as_json: bool):
    """List teardown requests.

    By default, shows only active teardowns. Use --all to include
    completed and cancelled requests.

    Examples:
        almanak strat teardown list
        almanak strat teardown list --all
        almanak strat teardown list --json
    """
    manager = get_teardown_state_manager()

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
