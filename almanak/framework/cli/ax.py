"""``almanak ax`` -- direct DeFi action execution from the command line.

One-shot DeFi commands (swap, bridge, balance, price, LP) without writing strategy files.
Uses the existing ToolExecutor + PolicyEngine infrastructure with a thin CLI shim.

Auto-starts a gateway if none is running. Use ``--network anvil`` for local testing.
"""

from __future__ import annotations

import asyncio
import sys
import time
from typing import TYPE_CHECKING

import click

from almanak.config.cli_options import gateway_client_options
from almanak.core.chains import DEFAULT_CHAIN, ChainRegistry
from almanak.framework.data.models import _NATIVE_TO_WRAPPED

if TYPE_CHECKING:
    from almanak.gateway.managed import ManagedGateway


def _action_options(fn):
    """Add hidden --yes/--dry-run/--json to action subcommands.

    Allows users to place these flags after the subcommand name
    (e.g. ``almanak ax unwrap WETH 0.001 --yes``) in addition to the
    canonical group-level position. The subcommand value is OR-merged
    with the group value so either placement works.
    """
    fn = click.option("--yes", "-y", "sub_yes", is_flag=True, default=False, hidden=True)(fn)
    fn = click.option("--dry-run", "sub_dry_run", is_flag=True, default=False, hidden=True)(fn)
    fn = click.option("--json", "sub_json_output", is_flag=True, default=False, hidden=True)(fn)
    return fn


def _merge_flags(ctx, sub_yes=False, sub_dry_run=False, sub_json_output=False):
    """Merge subcommand-level flags with group-level flags."""
    yes = ctx.obj["yes"] or sub_yes
    dry_run = ctx.obj["dry_run"] or sub_dry_run
    json_output = ctx.obj["json_output"] or sub_json_output
    return yes, dry_run, json_output


@click.group(invoke_without_command=True)
@gateway_client_options
@click.option(
    "--chain",
    "-c",
    default=DEFAULT_CHAIN,
    envvar="ALMANAK_CHAIN",
    help=f"Default chain (default: {DEFAULT_CHAIN}).",
)
@click.option(
    "--wallet",
    "-w",
    default=None,
    envvar="ALMANAK_WALLET_ADDRESS",
    help="Wallet address. Auto-derived from ALMANAK_PRIVATE_KEY if not set.",
)
@click.option(
    "--max-trade-usd",
    type=float,
    default=10000,
    help="Max single trade size in USD (default: 10000).",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Simulate only, do not submit transactions.",
)
@click.option(
    "--json",
    "json_output",
    is_flag=True,
    default=False,
    help="Output results as JSON instead of human-readable tables.",
)
@click.option(
    "--yes",
    "-y",
    is_flag=True,
    default=False,
    help="Skip confirmation prompts (for non-interactive / AI agent use).",
)
@click.option(
    "--natural",
    "-n",
    default=None,
    type=str,
    help='Natural language mode: describe what you want in plain English (e.g. -n "swap 5 USDC to ETH").',
)
@click.option(
    "--network",
    type=click.Choice(["mainnet", "anvil"], case_sensitive=False),
    default=None,
    envvar="ALMANAK_GATEWAY_NETWORK",
    help="Network mode. Auto-starts a gateway if none is running (default: mainnet).",
)
@click.pass_context
def ax(ctx, gateway_host, gateway_port, chain, wallet, max_trade_usd, dry_run, json_output, yes, natural, network):
    """Execute DeFi actions directly from the command line.

    One-shot commands for swaps, balance checks, price queries, and more.
    No strategy files needed -- just run ``almanak ax <action>``.

    Auto-starts a gateway if none is running. Use --network to control
    mainnet vs Anvil, or connect to an existing gateway via --gateway-host/port.

    \b
    Network mode (Anvil vs Mainnet):
      --network anvil    Local Anvil fork (free, safe testing)
      --network mainnet  Real transactions (default)
      (omit)             Connects to existing gateway, or starts mainnet

    \b
    Safety model (TTY detection):
      - Interactive terminal: simulate -> preview -> confirm (unless --yes)
      - Non-interactive (piped/scripted): fails unless --yes is passed
      - --dry-run: simulate only, never submit

    \b
    Modes:
      Structured (default):
        almanak ax swap USDC ETH 100
        almanak ax balance USDC
        almanak ax price ETH

      Natural language (--natural / -n):
        almanak ax -n "swap 5 USDC to ETH on base"
        almanak ax -n "what's the price of ETH?"
        almanak ax -n "check my USDC balance"

    \b
    Examples:
        almanak ax balance USDC                    # Check USDC balance
        almanak ax price ETH                       # Get ETH price
        almanak ax swap USDC ETH 100 --dry-run     # Simulate a swap
        almanak ax swap USDC ETH 100               # Execute after confirmation
        almanak ax lp-info 123456                  # View LP position details
        almanak ax lp-close 123456                 # Close an LP position
        almanak ax pool WBTC WETH                  # Pool state (price, TVL, etc.)
        almanak ax bridge USDC 100 --from-chain arbitrum --to-chain base  # Bridge tokens
        almanak ax -n "swap 5 USDC to WETH on base"  # Natural language mode
        almanak ax --network anvil swap USDC ETH 100  # Auto-start Anvil gateway
    """
    # Load .env from current directory (same as strat run) so env vars like
    # ALCHEMY_API_KEY, ALMANAK_PRIVATE_KEY, etc. are available to the gateway.
    # Also check .power-env (extended config). Both files are loaded — the
    # boundary helper is path-aware so each distinct file loads exactly once.
    # ``.env`` is checked first, and python-dotenv's ``override=False`` makes
    # values from the first-loaded file win where keys overlap, so
    # ``.env`` wins over ``.power-env``.
    from pathlib import Path

    from almanak.config.env import _load_dotenv_once

    for env_name in (".env", ".power-env"):
        env_file = Path.cwd() / env_name
        if env_file.exists():
            _load_dotenv_once(str(env_file))

    ctx.ensure_object(dict)
    ctx.obj["gateway_host"] = gateway_host
    ctx.obj["gateway_port"] = gateway_port
    ctx.obj["chain"] = chain
    ctx.obj["wallet"] = wallet or ""
    ctx.obj["max_trade_usd"] = max_trade_usd
    ctx.obj["dry_run"] = dry_run
    ctx.obj["json_output"] = json_output
    ctx.obj["yes"] = yes
    ctx.obj["network"] = network

    # If --natural is provided, handle NL mode and skip subcommand dispatch
    if natural is not None:
        _handle_natural_language(ctx, natural)
        ctx.exit()

    # If no subcommand and no --natural, show help
    if ctx.invoked_subcommand is None:
        click.echo(ctx.get_help())


def _handle_natural_language(ctx: click.Context, text: str):
    """Handle the --natural flag: interpret text via LLM, confirm, execute."""
    from almanak.framework.agent_tools.llm_client import LLMConfig, LLMConfigError
    from almanak.framework.cli.ax_natural import NaturalLanguageError, interpret_natural_language
    from almanak.framework.cli.ax_render import (
        check_safety_gate,
        render_error,
        render_interpretation,
        render_result,
        render_simulation,
    )

    json_output = ctx.obj["json_output"]
    dry_run = ctx.obj["dry_run"]
    yes = ctx.obj["yes"]
    chain = ctx.obj["chain"]

    # Load LLM config from env
    llm_config = LLMConfig.from_env()
    if not llm_config.api_key:
        render_error(
            "Natural language mode requires an LLM API key.\n"
            "Set the AGENT_LLM_API_KEY environment variable to an Anthropic API key.\n"
            "Get one at: https://console.anthropic.com\n\n"
            "  export AGENT_LLM_API_KEY=sk-ant-...\n\n"
            "Or use structured syntax: almanak ax swap USDC ETH 100",
            json_output=json_output,
        )
        sys.exit(1)

    # Interpret via LLM
    try:
        action = asyncio.run(interpret_natural_language(text, chain, llm_config))
    except LLMConfigError as e:
        render_error(str(e), json_output=json_output)
        sys.exit(1)
    except NaturalLanguageError as e:
        render_error(str(e), json_output=json_output)
        sys.exit(1)
    except Exception as e:
        render_error(
            f"Failed to interpret request: {e}\nTry structured syntax: almanak ax swap USDC ETH 100",
            json_output=json_output,
        )
        sys.exit(1)

    # Always show interpretation (even with --yes)
    render_interpretation(action.tool_name, action.arguments, json_output=json_output)

    # Inject chain default if not in arguments
    if "chain" not in action.arguments:
        action.arguments["chain"] = chain

    # Determine if this is a write action
    from almanak.framework.agent_tools.catalog import RiskTier, get_default_catalog

    catalog = get_default_catalog()
    tool_def = catalog.get(action.tool_name)
    is_write = tool_def is not None and tool_def.risk_tier in (RiskTier.MEDIUM, RiskTier.HIGH)

    try:
        if is_write and dry_run:
            action.arguments["dry_run"] = True
            response = _run_tool(ctx, action.tool_name, action.arguments)
            render_simulation(response, json_output=json_output)
            if response.status == "error":
                sys.exit(1)
            return

        if is_write:
            # Build a human-readable description from the interpreted action
            parts = [action.tool_name]
            for k, v in action.arguments.items():
                parts.append(f"{k}={v}")
            action_desc = " ".join(parts)
            proceed = check_safety_gate(dry_run=False, yes=yes, action_description=action_desc)
            if not proceed:
                click.echo("Cancelled.")
                return

        response = _run_tool(ctx, action.tool_name, action.arguments)
        render_result(response, json_output=json_output, title=action.tool_name)
        if response.status == "error":
            sys.exit(1)
    except click.ClickException:
        raise
    except Exception as e:
        render_error(str(e), json_output=json_output)
        sys.exit(1)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_executor(ctx: click.Context):
    """Create executor from context options, reusing if already created.

    If no gateway is running on the target host:port, auto-starts a
    ManagedGateway in a background thread (like ``almanak strat run``).
    The managed gateway is stored on ctx.obj and stopped on process exit.
    """
    if "executor" in ctx.obj:
        return ctx.obj["executor"], ctx.obj["client"]

    from almanak.config import load_config
    from almanak.framework.agent_tools.cli_executor import create_cli_executor

    host = ctx.obj["gateway_host"]
    port = ctx.obj["gateway_port"]
    network = ctx.obj.get("network")

    # Read the auth token via the typed config service so the CLI can reach
    # a gateway that was started by another process (e.g. a long-running
    # strategy) and therefore has auth enabled. ``GatewayConfig.auth_token``
    # carries the typed ALMANAK_GATEWAY_AUTH_TOKEN value;
    # ``CliRuntimeConfig.legacy_gateway_auth_token`` carries the unprefixed
    # GATEWAY_AUTH_TOKEN fallback. Bug 5 of the 0G DogFooding report
    # (2026-04-16).
    _cfg = load_config()
    env_auth_token = _cfg.gateway.auth_token or _cfg.cli.legacy_gateway_auth_token or None

    # Try connecting to an existing gateway first.
    # Suppress gateway_client logger during the quick probe so users don't
    # see scary "Gateway not ready / connection refused" messages when no
    # gateway is running yet (expected path before auto-start).
    import logging as _logging

    gc_logger = _logging.getLogger("almanak.framework.gateway_client")
    prev_level = gc_logger.level
    gc_logger.setLevel(_logging.CRITICAL)
    try:
        executor, client = create_cli_executor(
            gateway_host=host,
            gateway_port=port,
            chain=ctx.obj["chain"],
            wallet_address=ctx.obj["wallet"],
            max_single_trade_usd=ctx.obj["max_trade_usd"],
            connect_timeout=2.0,  # quick probe
            auth_token=env_auth_token,
        )
        # Remember the token so follow-up commands in the same CLI session
        # (e.g. the auto-start path below) can reuse it.
        if env_auth_token is not None:
            ctx.obj.setdefault("gateway_auth_token", env_auth_token)
        ctx.obj["executor"] = executor
        ctx.obj["client"] = client
        return executor, client
    except click.ClickException:
        pass  # No gateway running -- auto-start one below
    finally:
        gc_logger.setLevel(prev_level)

    # Auto-start a managed gateway (mainnet or anvil)
    managed = _start_managed_gateway(ctx, host, port, network)
    ctx.obj["managed_gateway"] = managed

    # Now connect to the freshly-started gateway (use session auth token)
    executor, client = create_cli_executor(
        gateway_host=managed.host,
        gateway_port=managed.port,
        chain=ctx.obj["chain"],
        wallet_address=ctx.obj["wallet"],
        max_single_trade_usd=ctx.obj["max_trade_usd"],
        auth_token=ctx.obj.get("gateway_auth_token"),
    )
    ctx.obj["executor"] = executor
    ctx.obj["client"] = client
    return executor, client


# crap-allowlist: Phase 1 (#2097) routes the existing GatewaySettings(...) construction
# through gateway_config_from_env(...) — no complexity added. Function refactor is
# tracked separately; allowlist is the documented escape hatch for no-op cutovers.
def _start_managed_gateway(
    ctx: click.Context,
    host: str,
    port: int,
    network: str | None,
) -> ManagedGateway:
    """Auto-start a gateway in a background thread.

    Mirrors the behavior of ``almanak strat run`` managed gateway,
    including auth token generation and private key forwarding.
    """
    import atexit
    import uuid

    from almanak.config import load_config
    from almanak.config.env import gateway_config_from_env
    from almanak.gateway.managed import ManagedGateway, find_available_gateway_port, is_port_in_use

    resolved_network = network or "mainnet"
    chain = ctx.obj["chain"]

    # Find an available port (prefer the requested one)
    if is_port_in_use(host, port):
        gw_port = find_available_gateway_port(host, port + 1)
    else:
        gw_port = port

    # Match strat run's security model: generate session auth token for
    # mainnet, allow_insecure for test networks (anvil/sepolia).
    is_test_network = resolved_network in ("anvil", "sepolia")
    session_auth_token = None if is_test_network else uuid.uuid4().hex

    gateway_kwargs: dict = {
        "grpc_host": host,
        "grpc_port": gw_port,
        "network": resolved_network,
        "allow_insecure": is_test_network,
        "chains": [chain],
        "metrics_enabled": False,
        "audit_enabled": False,
    }
    if session_auth_token:
        gateway_kwargs["auth_token"] = session_auth_token

    # Forward private key so the gateway can sign transactions. The typed
    # ``GatewayConfig.private_key`` carries the same ALMANAK_PRIVATE_KEY
    # value that the legacy direct-env-read used (populated by the Phase 1
    # ``_apply_gateway_env_fallbacks`` ladder).
    private_key = load_config().gateway.private_key or ""
    if private_key:
        gateway_kwargs["private_key"] = private_key

    # Phase 1: route through the config service so the same env-fallback
    # ladders apply here as for the gateway subcommand and managed gateway.
    settings = gateway_config_from_env(**gateway_kwargs)
    anvil_chains = [chain] if resolved_network == "anvil" else []

    # Status messages go to STDERR so they don't corrupt the CLI's actual
    # stdout payload — critical for ``--json`` output mode where callers
    # parse stdout directly.
    click.echo(
        click.style("Auto-starting gateway", bold=True) + f" ({resolved_network}) on {host}:{gw_port}...",
        err=True,
    )

    managed = ManagedGateway(
        settings=settings,
        anvil_chains=anvil_chains,
        wallet_address=ctx.obj["wallet"] or None,
    )

    try:
        managed.start(timeout=30.0)
    except Exception as e:
        raise click.ClickException(
            f"Failed to auto-start gateway: {e}\n"
            f"Start one manually: almanak gateway"
            + (f" --network {resolved_network}" if resolved_network != "mainnet" else "")
        ) from None

    # Ensure cleanup on exit
    atexit.register(managed.stop)

    # Update context so _run_tool connects to the right port
    ctx.obj["gateway_host"] = managed.host
    ctx.obj["gateway_port"] = managed.port

    click.echo(click.style("Gateway ready.", fg="green", bold=True), err=True)
    ctx.obj["gateway_auth_token"] = session_auth_token
    return managed


def _run_tool(ctx: click.Context, tool_name: str, arguments: dict):
    """Execute a tool call and return the ToolResponse."""
    executor, client = _get_executor(ctx)
    try:
        return asyncio.run(executor.execute(tool_name, arguments))
    finally:
        # Only disconnect if no managed gateway (one-shot external connection).
        # With a managed gateway, keep the connection alive for potential
        # follow-up commands in the same process.
        if "managed_gateway" not in ctx.obj:
            client.disconnect()


# ---------------------------------------------------------------------------
# almanak ax price <token>
# ---------------------------------------------------------------------------


@ax.command()
@click.argument("token")
@click.option(
    "--chain",
    "-c",
    "sub_chain",
    default=None,
    help="Chain to query (overrides the group-level --chain when both are set).",
)
@click.pass_context
def price(ctx, token, sub_chain):
    """Get the current USD price of a token.

    \b
    Examples:
        almanak ax price ETH
        almanak ax price USDC --chain base
        almanak ax price ETH --json
    """
    from almanak.framework.cli.ax_render import render_error, render_result

    json_output = ctx.obj["json_output"]
    # Subcommand-level --chain wins over group-level when provided. This
    # matches Click's "more specific placement wins" convention and keeps
    # both invocation styles working:
    #   almanak ax price ETH --chain base          (sub)
    #   almanak ax --chain base price ETH          (group)
    #
    # We update ctx.obj["chain"] (not just the tool arg) because downstream
    # infrastructure -- _get_executor() and _start_managed_gateway() -- reads
    # the chain from ctx.obj to initialize the executor / gateway client /
    # managed gateway. Passing the override only to the tool args would leave
    # the gateway pointed at the group-level chain (or default).
    if sub_chain is not None:
        ctx.obj["chain"] = sub_chain
    try:
        response = _run_tool(
            ctx,
            "get_price",
            {
                "token": token,
                "chain": ctx.obj["chain"],
            },
        )
        render_result(response, json_output=json_output, title=f"Price: {token.upper()}")
        if response.status == "error":
            sys.exit(1)
    except click.ClickException:
        raise
    except Exception as e:
        render_error(str(e), json_output=json_output)
        sys.exit(1)


# ---------------------------------------------------------------------------
# almanak ax resolve <token>
# ---------------------------------------------------------------------------


@ax.command()
@click.argument("token")
@click.option(
    "--gateway/--no-gateway",
    default=True,
    help=(
        "Dynamic fallback via the gateway (CoinGecko/Jupiter/on-chain ERC20) "
        "when the token isn't in the static registry. Default ON — matches "
        "what humans expect. Pass --no-gateway for a strict offline lookup "
        "(useful in AI-agent code-generation loops where determinism and "
        "speed matter more than coverage)."
    ),
)
@click.option(
    "--verify/--no-verify",
    default=True,
    help=(
        "Verify the contract exists on-chain after a static-registry hit. "
        "Default ON — catches addresses that are in the registry but not "
        "deployed on the requested chain. Pass --no-verify to skip the "
        "on-chain check (faster, offline)."
    ),
)
@click.pass_context
def resolve(ctx, token, gateway: bool, verify: bool):
    """Resolve a token symbol or address to its metadata on a chain.

    Checks, in order: memory cache -> disk cache -> static JSON registry
    -> symbol aliases. If ``--gateway`` (default), also tries the gateway's
    dynamic path (CoinGecko/Jupiter for symbols, on-chain ERC20 for
    addresses). With ``--no-gateway``, stops at the static layers for a
    fast, offline, deterministic lookup.

    After a registry hit, ``--verify`` (default) makes an ``eth_getCode`` RPC
    call to confirm the contract is actually deployed on the requested chain.
    This catches silent false positives where a token address is in the
    registry for chain A but passed with chain B. Uses the running gateway
    if reachable; otherwise falls back to a direct JSON-RPC call using
    the configured RPC URL (``ALCHEMY_API_KEY`` or chain-specific env var).
    Native tokens (ETH, AVAX, etc.) skip the bytecode check automatically.

    ``--gateway`` requires a reachable gateway at ``--gateway-host/port``
    (default ``localhost:50051``). If none is reachable the command
    still answers from the static registry and flags the miss instead of
    hanging.

    \b
    Examples:
        almanak ax -c arbitrum resolve USDC
        almanak ax -c arbitrum --json resolve USDC
        almanak ax -c arbitrum --json resolve LUME              # tries dynamic
        almanak ax -c arbitrum --no-gateway resolve LUME        # static only
        almanak ax -c arbitrum --no-verify resolve USDC         # skip on-chain check
        almanak ax -c arbitrum resolve 0xaf88d065e77c8cC2239327C5EDb3A432268e5831

    Exit codes:
        0 -- token resolved and on-chain verification passed (or skipped).
        1 -- token not found (address / symbol unknown on this chain).
        2 -- invalid input (e.g. malformed address).
        3 -- token found in registry but ``eth_getCode`` returned empty
             bytecode: address is NOT deployed on this chain.
    """
    import json as _json

    from almanak.framework.cli.ax_render import render_error
    from almanak.framework.data.tokens import create_token_resolver
    from almanak.framework.data.tokens.exceptions import (
        InvalidTokenAddressError,
        TokenNotFoundError,
        TokenResolutionError,
    )

    json_output = ctx.obj["json_output"]
    chain = ctx.obj["chain"]

    # Fast path: when gateway is permitted, try a static-only resolution first.
    # If the token lives in memory cache / disk cache / tokens.json / aliases
    # we skip the ~1-2s cost of auto-spawning a ManagedGateway entirely.  The
    # slow path below kicks in only when the static layer genuinely misses —
    # i.e., exactly when the gateway's dynamic path could add value.
    if gateway:
        static_only = create_token_resolver()
        try:
            resolved = static_only.resolve(token, chain, skip_gateway=True, log_errors=False)
            # For on-chain verification on the fast path, prefer the gateway gRPC
            # channel if one is already running (avoids a direct outbound HTTP
            # socket from the CLI process in gateway-managed environments).
            # We only probe — never spawn a ManagedGateway — so the fast path
            # stays fast even when no gateway is running.
            fast_channel = _open_channel_if_reachable(ctx) if verify else None
            try:
                contract_verified = (
                    _check_contract_deployed(resolved.address, chain, gateway_channel=fast_channel) if verify else None
                )
            finally:
                _close_channel(fast_channel)
            _render_resolved_token(resolved, chain=chain, json_output=json_output, contract_verified=contract_verified)
            if contract_verified is False:
                sys.exit(3)
            return
        except InvalidTokenAddressError as e:
            render_error(f"Invalid address: {e}", json_output=json_output)
            sys.exit(2)
        except TokenNotFoundError:
            # Static miss — fall through to the gateway-enabled path.
            # Do NOT catch TokenResolutionError (parent): AmbiguousTokenError
            # and any future static-layer resolution errors must propagate
            # so the outer handler maps them to the correct exit code /
            # JSON payload instead of being silently retried dynamically.
            pass

    resolver, gateway_channel, gateway_note = _build_resolver_for_cli(ctx, use_gateway=gateway)

    # Use try/finally to guarantee gateway_channel is always closed regardless
    # of which exception (known or unexpected) exits this block.
    try:
        try:
            resolved = resolver.resolve(token, chain, skip_gateway=not gateway, log_errors=False)
        except InvalidTokenAddressError as e:
            render_error(f"Invalid address: {e}", json_output=json_output)
            sys.exit(2)
        except TokenNotFoundError as e:
            if json_output:
                payload = {
                    "status": "not_found",
                    "token": token,
                    "chain": chain,
                    "suggestions": list(e.suggestions or []),
                }
                if gateway_note:
                    payload["gateway"] = gateway_note
                payload["hint"] = (
                    "If you have the contract address, pass it directly or use resolver.register_token() in your strategy."
                )
                click.echo(_json.dumps(payload, indent=2))
            else:
                if gateway_note:
                    click.echo(f"(gateway: {gateway_note})", err=True)
                render_error(f"Token not found: {e}", json_output=False)
            sys.exit(1)
        except TokenResolutionError as e:
            # Covers ``TokenResolutionTimeoutError``, ``AmbiguousTokenError``,
            # and any future subclass. The exit-code contract promises 1 for
            # "couldn't resolve" (not_found-shaped), 2 for "malformed input"
            # (the ``InvalidTokenAddressError`` branch above). An ambiguous
            # or timed-out resolution is functionally "couldn't resolve",
            # so it falls under exit 1. The JSON payload carries the error
            # class name so callers can branch on the specifics if they want.
            if json_output:
                payload = {
                    "status": "error",
                    "token": token,
                    "chain": chain,
                    "error_type": type(e).__name__,
                    "error": str(e),
                    "suggestions": list(getattr(e, "suggestions", []) or []),
                }
                if gateway_note:
                    payload["gateway"] = gateway_note
                click.echo(_json.dumps(payload, indent=2))
            else:
                if gateway_note:
                    click.echo(f"(gateway: {gateway_note})", err=True)
                render_error(f"{type(e).__name__}: {e}", json_output=False)
            sys.exit(1)

        # Run on-chain verification before closing the channel so the gateway
        # gRPC path is still available when a channel was created above.
        contract_verified = (
            _check_contract_deployed(resolved.address, chain, gateway_channel=gateway_channel) if verify else None
        )
    finally:
        # Always close the channel — including on unexpected exceptions, sys.exit
        # calls (which raise SystemExit), and KeyboardInterrupt.
        _close_channel(gateway_channel)

    _render_resolved_token(resolved, chain=chain, json_output=json_output, contract_verified=contract_verified)
    if contract_verified is False:
        sys.exit(3)


def _render_resolved_token(resolved, *, chain: str, json_output: bool, contract_verified: bool | None = None) -> None:
    """Render a resolved token either as JSON or as a friendly CLI summary.

    Extracted so the fast-path (static-only) and slow-path (gateway) in
    ``resolve`` share identical output formatting.

    Args:
        resolved: The ResolvedToken object.
        chain: Chain name string (for display).
        json_output: When True, emit machine-readable JSON.
        contract_verified: Result of the on-chain ``eth_getCode`` check:
            True  -- bytecode found; contract is deployed on this chain.
            False -- no bytecode; address is not deployed on this chain.
            None  -- check was skipped (native token, --no-verify, or RPC
                     unavailable).
    """
    import json as _json

    payload = {
        "symbol": resolved.symbol,
        "address": resolved.address,
        "decimals": resolved.decimals,
        "chain": resolved.chain.value.lower(),
        "chain_id": resolved.chain_id,
        "name": resolved.name,
        "coingecko_id": resolved.coingecko_id,
        "is_stablecoin": resolved.is_stablecoin,
        "is_native": resolved.is_native,
        "is_wrapped_native": resolved.is_wrapped_native,
        "bridge_type": resolved.bridge_type.value,
        "source": resolved.source,
        "is_verified": resolved.is_verified,
        "contract_verified": contract_verified,
    }

    if json_output:
        click.echo(_json.dumps(payload, indent=2))
    else:
        click.echo(f"{resolved.symbol} on {chain}")
        click.echo(f"  address     {resolved.address}")
        click.echo(f"  decimals    {resolved.decimals}")
        click.echo(f"  name        {resolved.name or '-'}")
        click.echo(f"  coingecko   {resolved.coingecko_id or '-'}")
        click.echo(f"  source      {resolved.source}")
        click.echo(f"  stablecoin  {'yes' if resolved.is_stablecoin else 'no'}")
        if contract_verified is False:
            click.echo(
                click.style(
                    f"  WARNING: address not deployed on {chain} (eth_getCode returned empty bytecode)",
                    fg="yellow",
                    bold=True,
                )
            )
        elif contract_verified is None and not resolved.is_native:
            # Only emit the skipped note when verification was expected but
            # couldn't run (RPC unavailable). Native tokens are intentionally
            # skipped and don't need a note.
            pass  # Silently skip -- don't spam the user when RPC is simply absent


def _open_channel_if_reachable(ctx: click.Context):
    """Return a gRPC channel to the configured gateway if it is already
    running, or ``None`` if no gateway is listening on the configured
    host:port.

    Unlike ``_build_resolver_for_cli``, this helper NEVER auto-spawns a
    ``ManagedGateway``.  It is only used for the fast-path verification
    in ``resolve`` where we want to use the gateway's RPC credentials when
    the user already has one running, but must not impose gateway startup
    latency on the common static-hit case.
    """
    from almanak.config import load_config

    host = ctx.obj.get("gateway_host", "localhost")
    port = ctx.obj.get("gateway_port", 50051)

    if not _gateway_is_reachable(host, port):
        return None

    try:
        import grpc

        _cfg = load_config()
        gateway_auth_token = (
            ctx.obj.get("gateway_auth_token") or _cfg.gateway.auth_token or _cfg.cli.legacy_gateway_auth_token
        )
        channel = grpc.insecure_channel(f"{host}:{port}")
        if gateway_auth_token:
            from almanak.framework.gateway_client import _AuthClientInterceptor

            channel = grpc.intercept_channel(channel, _AuthClientInterceptor(gateway_auth_token))
        return channel
    except Exception:
        return None


def _close_channel(channel) -> None:
    """Silently close a gRPC channel, ignoring any errors.

    Extracted to avoid repetition in the ``resolve`` command's exception
    handlers, each of which must close the channel before exiting.
    """
    if channel is not None:
        try:
            channel.close()
        except Exception:
            pass


def _build_resolver_for_cli(ctx, *, use_gateway: bool):
    """Return ``(resolver, channel_or_None, gateway_note)``.

    When ``use_gateway`` is True, create a short-lived resolver instance
    bound to the configured gateway host/port. This keeps ``ax resolve``
    isolated from the process-wide resolver singleton used by long-lived
    runtimes.

    If the configured gateway isn't reachable we auto-start a
    ``ManagedGateway`` on the fly — same behaviour as the other ax
    subcommands (``swap``, ``balance``, ...).  Without this step
    ``ax resolve`` would silently fall through to the static registry
    and look like the token doesn't exist, which is the wrong answer
    for anything the gateway's dynamic path would have found (Pendle
    PT/YT/SY, CoinGecko-only tokens, etc.).

    If the auto-start itself fails we fall back to static-only
    resolution and return a human-readable note explaining what we
    tried.
    """
    from almanak.framework.data.tokens import create_token_resolver

    resolver = create_token_resolver()

    if not use_gateway:
        return resolver, None, None

    from almanak.config import load_config

    host = ctx.obj.get("gateway_host", "localhost")
    port = ctx.obj.get("gateway_port", 50051)
    network = ctx.obj.get("network")
    # Match ``_get_executor``: fall back to the typed gateway auth values
    # (ALMANAK_GATEWAY_AUTH_TOKEN via ``GatewayConfig.auth_token``,
    # legacy unprefixed ``GATEWAY_AUTH_TOKEN`` via
    # ``CliRuntimeConfig.legacy_gateway_auth_token``) when no CLI-provided
    # token is on the context. ``ax resolve`` frequently attaches to a
    # gateway started by another process (a long-running strategy, or the
    # shared sidecar in CI), and that process is the one that exported the
    # env var — without this fallback, auth-enabled gateways reject the probe.
    _cfg = load_config()
    gateway_auth_token = (
        ctx.obj.get("gateway_auth_token") or _cfg.gateway.auth_token or _cfg.cli.legacy_gateway_auth_token
    )

    try:
        import grpc
    except Exception as e:  # grpc import failure
        return resolver, None, f"gRPC unavailable: {e}"

    # Probe the configured host:port. If nothing's listening, auto-start a
    # ManagedGateway the same way _get_executor does for swap/balance.
    if not _gateway_is_reachable(host, port):
        try:
            managed = _start_managed_gateway(ctx, host, port, network)
            ctx.obj["managed_gateway"] = managed
            # _start_managed_gateway may have picked a different free port.
            host = ctx.obj.get("gateway_host", host)
            port = ctx.obj.get("gateway_port", port)
            gateway_auth_token = ctx.obj.get("gateway_auth_token", gateway_auth_token)
        except Exception as e:
            return resolver, None, (f"no gateway running on {host}:{port} and auto-start failed: {e}")

    try:
        channel = grpc.insecure_channel(f"{host}:{port}")
        if gateway_auth_token:
            from almanak.framework.gateway_client import _AuthClientInterceptor

            channel = grpc.intercept_channel(channel, _AuthClientInterceptor(gateway_auth_token))
    except Exception as e:  # grpc channel construction failure
        return resolver, None, f"could not build gRPC channel to {host}:{port}: {e}"

    resolver = create_token_resolver(gateway_channel=channel)
    return resolver, channel, f"attempted dynamic lookup via {host}:{port}"


def _check_contract_deployed(
    address: str,
    chain: str,
    *,
    gateway_channel=None,
    timeout: float = 4.0,
) -> bool | None:
    """Return whether a contract is deployed at ``address`` on ``chain``.

    Makes an ``eth_getCode`` JSON-RPC call and checks for non-empty bytecode.
    This is the standard way to distinguish a deployed contract (non-empty
    bytecode) from an EOA or undeployed address (``"0x"`` / empty).

    Only applies to EVM chains. Solana tokens and other non-EVM chains are
    not checked and this function returns ``None`` for them.

    Args:
        address: Checksummed or lowercase EVM contract address.
        chain: Chain name (e.g. ``"arbitrum"``, ``"ethereum"``).
        gateway_channel: Optional open gRPC channel to an existing gateway.
            When provided, the call is routed through the gateway's RPC
            service (no direct outbound socket from this process). When
            ``None``, falls back to a direct JSON-RPC HTTP call using the
            RPC URL from ``get_rpc_url(chain)`` (``ALCHEMY_API_KEY`` or
            chain-specific env var).
        timeout: Per-call timeout in seconds.

    Returns:
        ``True``  -- bytecode found; address is a deployed contract.
        ``False`` -- ``eth_getCode`` returned ``"0x"``; not deployed.
        ``None``  -- check could not be performed (unsupported chain,
                     RPC unavailable, or address is a native token
                     placeholder like ``"0xEeeee..."``)
    """
    # Native token sentinel address — no bytecode expected.
    # Use the canonical constant and normalise to lowercase for comparison so
    # checksummed / mixed-case inputs are handled uniformly.
    from almanak.framework.data.tokens.defaults import NATIVE_SENTINEL

    _SKIP_ADDRESSES = {
        NATIVE_SENTINEL.lower(),
        "0x0000000000000000000000000000000000000000",
    }
    if address and address.lower() in _SKIP_ADDRESSES:
        return None

    # Skip non-EVM addresses (Solana base58, etc. — no '0x' prefix).
    if not address or not address.startswith("0x") or len(address) != 42:
        return None

    import json as _json
    import logging as _logging

    _log = _logging.getLogger(__name__)

    # Prefer the gateway gRPC path when a channel is already open — avoids
    # an additional outbound socket from the CLI process.
    if gateway_channel is not None:
        try:
            from almanak.gateway.proto import gateway_pb2, gateway_pb2_grpc

            rpc_stub = gateway_pb2_grpc.RpcServiceStub(gateway_channel)
            response = rpc_stub.Call(
                gateway_pb2.RpcRequest(
                    chain=chain,
                    method="eth_getCode",
                    params=_json.dumps([address, "latest"]),
                    id="ax-resolve-verify",
                ),
                timeout=timeout,
            )
            if response.success and response.result:
                code = _json.loads(response.result)
                return code not in (None, "0x", "0x0", "")
            _log.debug("Gateway eth_getCode returned failure for %s on %s: %s", address, chain, response.error)
            return None
        except Exception as exc:
            _log.debug("Gateway eth_getCode check failed for %s on %s: %s", address, chain, exc)
            # Fall through to direct HTTP path.

    # Direct JSON-RPC fallback: use the configured RPC URL for the chain.
    # This is architecturally correct in the CLI layer (not inside the
    # strategy container), same pattern as almanak/framework/cli/permissions.py.
    try:
        from almanak.gateway.utils import get_rpc_url

        rpc_url = get_rpc_url(chain)
    except Exception as exc:
        _log.debug("No RPC URL available for %s; skipping on-chain verification: %s", chain, exc)
        return None

    try:
        import urllib.request

        req_body = _json.dumps(
            {
                "jsonrpc": "2.0",
                "method": "eth_getCode",
                "params": [address, "latest"],
                "id": 1,
            }
        ).encode()
        req = urllib.request.Request(
            rpc_url,
            data=req_body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        import urllib.error

        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310 -- CLI layer, not framework
                data = _json.loads(resp.read())
        except urllib.error.URLError as exc:
            _log.debug("eth_getCode HTTP request failed for %s on %s: %s", address, chain, exc)
            return None

        if not isinstance(data, dict) or data.get("error") is not None:
            _log.debug("eth_getCode RPC error for %s on %s: %s", address, chain, data)
            return None
        code = data.get("result")
        return code not in (None, "0x", "0x0", "")
    except Exception as exc:
        _log.debug("eth_getCode check failed for %s on %s: %s", address, chain, exc)
        return None


def _gateway_is_reachable(host: str, port: int, timeout: float = 0.5) -> bool:
    """Return True if a TCP socket can connect to ``host:port`` within ``timeout``.

    Used as a quick probe before deciding whether to auto-start a managed
    gateway.  We check the TCP layer instead of opening a gRPC channel
    because a gRPC call would only fail lazily on first request, too late
    to decide whether to spawn.
    """
    import socket

    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


# ---------------------------------------------------------------------------
# almanak ax balance <token>
# ---------------------------------------------------------------------------


@ax.command()
@click.argument("token")
@click.option(
    "--chain",
    "-c",
    "sub_chain",
    default=None,
    help="Chain to query (overrides the group-level --chain when both are set).",
)
@click.pass_context
def balance(ctx, token, sub_chain):
    """Get the balance of a token in your wallet.

    \b
    Examples:
        almanak ax balance USDC
        almanak ax balance ETH --chain base
        almanak ax balance WETH --json
    """
    from almanak.framework.cli.ax_render import render_error, render_result

    json_output = ctx.obj["json_output"]
    # Subcommand-level --chain wins over group-level when provided. This
    # matches Click's "more specific placement wins" convention and keeps
    # both invocation styles working:
    #   almanak ax balance ETH --chain base        (sub)
    #   almanak ax --chain base balance ETH        (group)
    #
    # We update ctx.obj["chain"] (not just the tool arg) because downstream
    # infrastructure -- _get_executor() and _start_managed_gateway() -- reads
    # the chain from ctx.obj to initialize the executor / gateway client /
    # managed gateway. Passing the override only to the tool args would leave
    # the gateway pointed at the group-level chain (or default).
    if sub_chain is not None:
        ctx.obj["chain"] = sub_chain
    try:
        response = _run_tool(
            ctx,
            "get_balance",
            {
                "token": token,
                "chain": ctx.obj["chain"],
            },
        )
        render_result(response, json_output=json_output, title=f"Balance: {token.upper()}")
        if response.status == "error":
            sys.exit(1)
    except click.ClickException:
        raise
    except Exception as e:
        render_error(str(e), json_output=json_output)
        sys.exit(1)


# ---------------------------------------------------------------------------
# almanak ax swap <from_token> <to_token> <amount>
# ---------------------------------------------------------------------------


@ax.command()
@click.argument("from_token")
@click.argument("to_token")
@click.argument("amount")
@click.option(
    "--slippage",
    type=int,
    default=50,
    help="Max slippage in basis points (default: 50 = 0.5%).",
)
@click.option(
    "--protocol",
    default=None,
    help="Specific DEX protocol (default: best available).",
)
@click.option(
    "--chain",
    "-c",
    "sub_chain",
    default=None,
    help="Chain to swap on (overrides the group-level --chain when both are set).",
)
@_action_options
@click.pass_context
def swap(ctx, from_token, to_token, amount, slippage, protocol, sub_chain, sub_yes, sub_dry_run, sub_json_output):
    """Swap tokens on a DEX.

    \b
    Examples:
        almanak ax swap USDC ETH 100                    # Swap 100 USDC to ETH
        almanak ax swap USDC ETH 100 --dry-run           # Simulate only
        almanak ax swap USDC ETH 100 --slippage 100      # 1% slippage
        almanak ax swap USDC ETH 100 --chain base --yes  # Skip confirmation
    """
    from almanak.framework.cli.ax_render import (
        check_safety_gate,
        render_error,
        render_result,
        render_simulation,
    )

    yes, dry_run, json_output = _merge_flags(ctx, sub_yes, sub_dry_run, sub_json_output)

    # Subcommand-level --chain wins over group-level when provided. This
    # matches Click's "more specific placement wins" convention and keeps
    # both invocation styles working:
    #   almanak ax swap USDC ETH 100 --chain base   (sub)
    #   almanak ax --chain base swap USDC ETH 100   (group)
    #
    # We update ctx.obj["chain"] (not just the tool arg) because downstream
    # infrastructure -- _get_executor() and _start_managed_gateway() -- reads
    # the chain from ctx.obj to initialize the executor / gateway client /
    # managed gateway. Passing the override only to the tool args would leave
    # the gateway pointed at the group-level chain (or default).
    if sub_chain is not None:
        ctx.obj["chain"] = sub_chain

    _chain = ctx.obj["chain"]
    _network = ctx.obj.get("network")

    # Detect native token output -- DEX swaps produce the wrapped version (e.g. WETH on Ethereum).
    # Check chain-aware: only show the hint when to_token IS this chain's native token.
    _chain_descriptor = ChainRegistry.try_resolve(_chain)
    _chain_native = _chain_descriptor.native.symbol if _chain_descriptor is not None else "ETH"
    wrapped_name = _NATIVE_TO_WRAPPED.get(to_token.upper()) if to_token.upper() == _chain_native else None
    is_native_output = wrapped_name is not None
    _ax_flags = f"--chain {_chain}" + (f" --network {_network}" if _network and _network != "mainnet" else "")

    action_desc = f"Swap {amount} {from_token.upper()} -> {to_token.upper()} on {_chain}"

    try:
        # Build tool arguments
        args = {
            "token_in": from_token,
            "token_out": to_token,
            "amount": amount,
            "slippage_bps": slippage,
            "chain": ctx.obj["chain"],
        }
        if protocol:
            args["protocol"] = protocol

        # Dry-run: simulate only
        if dry_run:
            args["dry_run"] = True
            response = _run_tool(ctx, "swap_tokens", args)
            render_simulation(response, json_output=json_output)
            if is_native_output and not json_output and response.status != "error":
                click.echo(
                    f"\nNote: DEX swaps produce {wrapped_name}, not native {to_token.upper()}. "
                    f"To get native {to_token.upper()}, run: almanak ax {_ax_flags} unwrap {wrapped_name} <amount>"
                )
            if response.status == "error":
                sys.exit(1)
            return

        # Safety gate: confirm before executing
        if is_native_output and not json_output:
            action_desc += (
                f" (output will be {wrapped_name} -- use 'almanak ax {_ax_flags} unwrap' for native {to_token.upper()})"
            )
        proceed = check_safety_gate(dry_run=False, yes=yes, action_description=action_desc)
        if not proceed:
            click.echo("Cancelled.")
            return

        # Execute
        response = _run_tool(ctx, "swap_tokens", args)
        render_result(response, json_output=json_output, title="Swap")
        if is_native_output and not json_output and response.status != "error":
            click.echo(
                f"\nTip: Output is {wrapped_name} (ERC-20). To unwrap to native {to_token.upper()}: "
                f"almanak ax {_ax_flags} unwrap {wrapped_name} <amount>"
            )
        if response.status == "error":
            sys.exit(1)
    except click.ClickException:
        raise
    except Exception as e:
        render_error(str(e), json_output=json_output)
        sys.exit(1)


# ---------------------------------------------------------------------------
# almanak ax lp-close <position_id>
# ---------------------------------------------------------------------------


@ax.command("lp-close")
@click.argument("position_id")
@click.option(
    "--protocol",
    default="uniswap_v3",
    help="LP protocol (default: uniswap_v3).",
)
@click.option(
    "--no-collect-fees",
    is_flag=True,
    default=False,
    help="Skip collecting accrued fees.",
)
@_action_options
@click.pass_context
def lp_close(ctx, position_id, protocol, no_collect_fees, sub_yes, sub_dry_run, sub_json_output):
    """Close (fully withdraw) a liquidity position.

    Removes all liquidity and collects accrued fees by default.
    Returns the withdrawn token amounts.

    \b
    Examples:
        almanak ax lp-close 123456                         # Close LP #123456
        almanak ax lp-close 123456 --dry-run               # Simulate only
        almanak ax lp-close 123456 --protocol uniswap_v3   # Explicit protocol
        almanak ax lp-close 123456 --no-collect-fees        # Skip fee collection
    """
    from almanak.framework.cli.ax_render import (
        check_safety_gate,
        render_error,
        render_result,
        render_simulation,
    )

    yes, dry_run, json_output = _merge_flags(ctx, sub_yes, sub_dry_run, sub_json_output)

    action_desc = f"Close LP position #{position_id} ({protocol}) on {ctx.obj['chain']}"

    try:
        args = {
            "position_id": position_id,
            "amount": "all",
            "collect_fees": not no_collect_fees,
            "chain": ctx.obj["chain"],
            "protocol": protocol,
        }

        if dry_run:
            args["dry_run"] = True
            response = _run_tool(ctx, "close_lp_position", args)
            render_simulation(response, json_output=json_output)
            if response.status == "error":
                sys.exit(1)
            return

        proceed = check_safety_gate(dry_run=False, yes=yes, action_description=action_desc)
        if not proceed:
            click.echo("Cancelled.")
            return

        response = _run_tool(ctx, "close_lp_position", args)
        render_result(response, json_output=json_output, title=f"LP Close: #{position_id}")
        if response.status == "error":
            sys.exit(1)
    except click.ClickException:
        raise
    except Exception as e:
        render_error(str(e), json_output=json_output)
        sys.exit(1)


# ---------------------------------------------------------------------------
# almanak ax lp-info <position_id>
# ---------------------------------------------------------------------------


@ax.command("lp-info")
@click.argument("position_id")
@click.option(
    "--protocol",
    default="uniswap_v3",
    help="LP protocol (default: uniswap_v3).",
)
@click.option(
    "--network",
    "lp_network",
    type=click.Choice(["mainnet", "anvil"], case_sensitive=False),
    default="mainnet",
    help="Network to query (default: mainnet). Use 'anvil' for local fork.",
)
@click.pass_context
def lp_info(ctx, position_id, protocol, lp_network):
    """Get details about an existing LP position.

    Shows range, liquidity, accrued fees, and in-range status.
    Queries mainnet by default so live positions are always accessible,
    even when a local Anvil gateway is running.

    \b
    Examples:
        almanak ax lp-info 123456                      # View LP #123456 on mainnet
        almanak ax lp-info 123456 --json               # JSON output
        almanak ax lp-info 123456 --protocol uniswap_v3
        almanak ax lp-info 123456 --network anvil      # Query local Anvil fork
    """
    from almanak.framework.cli.ax_render import render_error, render_result

    json_output = ctx.obj["json_output"]
    try:
        response = _run_tool(
            ctx,
            "get_lp_position",
            {
                "position_id": position_id,
                "chain": ctx.obj["chain"],
                "protocol": protocol,
                "network": lp_network,
            },
        )
        render_result(response, json_output=json_output, title=f"LP Position: #{position_id}")
        if response.status == "error":
            sys.exit(1)
    except click.ClickException:
        raise
    except Exception as e:
        render_error(str(e), json_output=json_output)
        sys.exit(1)


# ---------------------------------------------------------------------------
# almanak ax lp-list / lending-list / portfolio -- read-only discovery (VIB-2995)
# ---------------------------------------------------------------------------


@ax.command("lp-list")
@click.option("--protocol", default="uniswap_v3", help="LP protocol (default: uniswap_v3).")
@click.option(
    "--wallet-override",
    default=None,
    help="Override the wallet address (default: ax's configured wallet).",
)
@click.option(
    "--include-empty",
    is_flag=True,
    default=False,
    help="Include positions with zero liquidity (burned or fully withdrawn).",
)
@click.option(
    "--network",
    "lp_network",
    type=click.Choice(["mainnet", "anvil"], case_sensitive=False),
    default=None,
    help="Network to query. Defaults to group-level --network, then 'mainnet'.",
)
@click.pass_context
def lp_list(ctx, protocol, wallet_override, include_empty, lp_network):
    """List all LP positions owned by your wallet on a chain.

    Enumerates NonfungiblePositionManager.tokenOfOwnerByIndex + positions().
    Pair with `ax lp-info <id>` to drill into any specific position.

    \b
    Examples:
        almanak ax lp-list                                 # Arbitrum Uniswap V3
        almanak ax --chain base lp-list                    # Base
        almanak ax lp-list --include-empty                 # Include burned/empty
        almanak ax --chain zerog lp-list --protocol uniswap_v3 --network mainnet
    """
    from almanak.framework.cli.ax_render import render_error, render_result

    json_output = ctx.obj["json_output"]
    effective_network = lp_network or ctx.obj.get("network") or "mainnet"
    try:
        response = _run_tool(
            ctx,
            "list_lp_positions",
            {
                "chain": ctx.obj["chain"],
                "protocol": protocol,
                "wallet_address": wallet_override or "",
                "include_empty": include_empty,
                "network": effective_network,
            },
        )
        render_result(response, json_output=json_output, title=f"LP Positions ({ctx.obj['chain']})")
        if response.status == "error":
            sys.exit(1)
    except click.ClickException:
        raise
    except Exception as e:
        render_error(str(e), json_output=json_output)
        sys.exit(1)


@ax.command("lending-list")
@click.option("--protocol", default="aave_v3", help="Lending protocol (default: aave_v3).")
@click.option(
    "--wallet-override",
    default=None,
    help="Override the wallet address (default: ax's configured wallet).",
)
@click.option(
    "--network",
    "lend_network",
    type=click.Choice(["mainnet", "anvil"], case_sensitive=False),
    default=None,
    help="Network to query. Defaults to group-level --network, then 'mainnet'.",
)
@click.pass_context
def lending_list(ctx, protocol, wallet_override, lend_network):
    """List a wallet's lending positions (account totals + health factor).

    v1 queries Aave V3's Pool.getUserAccountData — the same totals you see
    at the top of Aave's dashboard. Per-reserve breakdown is a follow-up.

    \b
    Examples:
        almanak ax lending-list                    # Arbitrum Aave V3
        almanak ax --chain base lending-list       # Base Aave V3
    """
    from almanak.framework.cli.ax_render import render_error, render_result

    json_output = ctx.obj["json_output"]
    effective_network = lend_network or ctx.obj.get("network") or "mainnet"
    try:
        response = _run_tool(
            ctx,
            "list_lending_positions",
            {
                "chain": ctx.obj["chain"],
                "protocol": protocol,
                "wallet_address": wallet_override or "",
                "network": effective_network,
            },
        )
        render_result(response, json_output=json_output, title=f"Lending ({protocol}, {ctx.obj['chain']})")
        if response.status == "error":
            sys.exit(1)
    except click.ClickException:
        raise
    except Exception as e:
        render_error(str(e), json_output=json_output)
        sys.exit(1)


@ax.command("lending-reserves")
@click.option("--protocol", default="aave_v3", help="Lending protocol (default: aave_v3).")
@click.option("--asset", default="", help="Filter to a single reserve symbol (e.g. 'WMATIC').")
@click.pass_context
def lending_reserves(ctx, protocol, asset):
    """List a lending market's reserves with borrowable / active flags.

    Read-only. Shows, per reserve, whether borrowing is enabled, whether the
    reserve is active/frozen, and its LTV — so you can pick a borrowable asset
    before configuring a strategy, instead of discovering a supply-only reserve
    at the borrow step of a lifecycle run. Reserves are enumerated live from the
    market's PoolDataProvider, not a curated list.

    Network follows the group-level ``--network`` (which also controls
    gateway auto-start): use ``almanak ax --network anvil lending-reserves``
    for a local Anvil fork.

    \b
    Examples:
        almanak ax --chain polygon lending-reserves          # Aave V3 Polygon
        almanak ax --chain polygon lending-reserves --asset WMATIC
        almanak ax --chain base --json lending-reserves
    """
    from almanak.framework.cli.ax_render import render_error, render_result

    json_output = ctx.obj["json_output"]
    effective_network = ctx.obj.get("network") or "mainnet"
    try:
        response = _run_tool(
            ctx,
            "list_lending_reserves",
            {
                "chain": ctx.obj["chain"],
                "protocol": protocol,
                "asset": asset,
                "network": effective_network,
            },
        )
        # The generic renderer prints the reserves list as one flat repr, which
        # is unreadable for an operator scanning 20+ reserves. Render a real
        # column table for the human path; keep --json untouched for automation.
        if json_output or response.status == "error":
            render_result(response, json_output=json_output, title=f"Lending reserves ({protocol}, {ctx.obj['chain']})")
        else:
            _render_reserves_table(response, protocol=protocol, chain=ctx.obj["chain"])
        if response.status == "error":
            sys.exit(1)
    except click.ClickException:
        raise
    except Exception as e:
        render_error(str(e), json_output=json_output)
        sys.exit(1)


def _render_reserves_table(response, *, protocol: str, chain: str) -> None:
    """Human-readable column table for `ax lending-reserves` (VIB-4925).

    Columns: SYMBOL ADDRESS BORROW COLLAT ACTIVE FROZEN LTV%. A reserve whose
    config read failed shows ``err`` in the flag columns and its error text on
    a trailing line, so a single dead reserve is visible, not hidden.
    """
    data = response.data or {}
    reserves = data.get("reserves", [])
    header = click.style(f"Lending reserves ({protocol}, {chain})", bold=True)
    click.echo(f"\n{header}  —  {data.get('count', len(reserves))} reserves @ {data.get('pool_data_provider', '?')}")
    if data.get("truncated"):
        reason = data.get("truncation_reason") or "capped"
        click.echo(
            click.style(
                f"  ! list truncated ({reason}) — showing {data.get('count')} of {data.get('total_matched')} reserves",
                fg="yellow",
            )
        )

    def _flag(v) -> str:
        return "—" if v is None else ("yes" if v else "no")

    def _ltv(v) -> str:
        # Empty != Zero: ltv_bps == 0 is a real "no borrowing power" value (show
        # 0.0%); only None / unmeasured renders as "—".
        return "—" if v is None else f"{v / 100:.1f}%"

    rows = []
    for r in reserves:
        rows.append(
            (
                str(r.get("symbol", "")),
                str(r.get("address", "")),
                "err" if r.get("error") else _flag(r.get("borrowing_enabled")),
                "err" if r.get("error") else _flag(r.get("usage_as_collateral_enabled")),
                "err" if r.get("error") else _flag(r.get("is_active")),
                "err" if r.get("error") else _flag(r.get("is_frozen")),
                "—" if r.get("error") else _ltv(r.get("ltv_bps")),
            )
        )
    headers = ("SYMBOL", "ADDRESS", "BORROW", "COLLAT", "ACTIVE", "FROZEN", "LTV")
    widths = [
        max(len(headers[i]), *(len(row[i]) for row in rows)) if rows else len(headers[i]) for i in range(len(headers))
    ]
    click.echo("  ".join(h.ljust(widths[i]) for i, h in enumerate(headers)))
    click.echo("  ".join("-" * widths[i] for i in range(len(headers))))
    for row in rows:
        click.echo("  ".join(str(row[i]).ljust(widths[i]) for i in range(len(headers))))
    # Surface any per-reserve read errors below the table (not hidden).
    for r in reserves:
        if r.get("error"):
            click.echo(click.style(f"  ! {r.get('symbol', '?')}: {r['error']}", fg="yellow"))


@ax.command("portfolio")
@click.option(
    "--tokens",
    default=None,
    help="Comma-separated ERC20 symbols to include in the balance snapshot (e.g. 'USDC,WETH').",
)
@click.option(
    "--wallet-override",
    default=None,
    help="Override the wallet address (default: ax's configured wallet).",
)
@click.option(
    "--network",
    "pf_network",
    type=click.Choice(["mainnet", "anvil"], case_sensitive=False),
    default=None,
    help="Network to query. Defaults to group-level --network, then 'mainnet'.",
)
@click.pass_context
def portfolio(ctx, tokens, wallet_override, pf_network):
    """Aggregate snapshot: native + ERC20 balances, LP positions, lending.

    Read-only. Combines list_lp_positions + list_lending_positions + native
    balance + (optional) batch_get_balances for a chain in one call.

    \b
    Examples:
        almanak ax portfolio                                   # Arbitrum, no ERC20 list
        almanak ax portfolio --tokens USDC,WETH,ARB             # With balances
        almanak ax --chain base portfolio --tokens USDC,WETH    # Different chain
    """
    from almanak.framework.cli.ax_render import render_error, render_result

    json_output = ctx.obj["json_output"]
    # Strip + drop empty entries so "USDC,,WETH," doesn't trigger a bogus
    # "" lookup that fails resolver validation. (CodeRabbit PR #1536.)
    token_list = [t for t in (s.strip() for s in tokens.split(",")) if t] if tokens else []
    effective_network = pf_network or ctx.obj.get("network") or "mainnet"
    try:
        response = _run_tool(
            ctx,
            "get_portfolio",
            {
                "chain": ctx.obj["chain"],
                "wallet_address": wallet_override or "",
                "tokens": token_list,
                "network": effective_network,
            },
        )
        render_result(response, json_output=json_output, title=f"Portfolio ({ctx.obj['chain']})")
        if response.status == "error":
            sys.exit(1)
    except click.ClickException:
        raise
    except Exception as e:
        render_error(str(e), json_output=json_output)
        sys.exit(1)


# ---------------------------------------------------------------------------
# almanak ax lending-supply / lending-borrow / lending-repay / lending-withdraw
# ---------------------------------------------------------------------------


def _uses_isolated_markets(protocol: str) -> bool:
    """True if the protocol uses isolated markets (requires --market-id).

    Derived from the connector-owned capability declaration
    (``requires_market_id`` in each connector's ``capabilities.py`` — Morpho
    Blue and Curvance today) instead of a hardcoded protocol set (VIB-4851
    B3). Normalizes hyphens and whitespace to underscores so ``morpho-blue``
    and ``"morpho blue"`` are accepted alongside ``morpho_blue`` — consistent
    with the schema-layer protocol-key normalization.
    """
    from almanak.connectors._strategy_base.capabilities_registry import get_protocol_capabilities
    from almanak.framework.agent_tools.schemas import _normalize_protocol_key

    return bool(get_protocol_capabilities(_normalize_protocol_key(protocol)).get("requires_market_id"))


def _guard_market_id_flag(protocol: str, market_id: str | None) -> None:
    """Reject --market-id on protocols that ignore it.

    The schema / intent layer accepts ``market_id=None`` for any protocol, so
    downstream won't complain — but an operator who typed ``--market-id`` and
    it silently had no effect deserves a clear error. Only isolated-market
    protocols (Morpho Blue, Curvance) accept per-market routing; others
    (Aave V3, Compound, Spark, ...) use a unified pool.
    """
    if market_id is not None and not _uses_isolated_markets(protocol):
        raise click.UsageError(
            f"--market-id is only supported on isolated-market protocols "
            f"(morpho_blue, curvance); got protocol={protocol}"
        )


def _run_lending_tool(
    ctx: click.Context,
    tool_name: str,
    args: dict,
    action_desc: str,
    title: str,
    sub_yes: bool,
    sub_dry_run: bool,
    sub_json_output: bool,
) -> None:
    """Shared plumbing for ax lending-* commands.

    Mirrors the dry-run/confirm/execute loop used by ax swap and ax lp-close
    so all four lending CLIs behave identically.
    """
    from almanak.framework.cli.ax_render import (
        check_safety_gate,
        render_error,
        render_result,
        render_simulation,
    )

    yes, dry_run, json_output = _merge_flags(ctx, sub_yes, sub_dry_run, sub_json_output)

    try:
        if dry_run:
            args = {**args, "dry_run": True}
            response = _run_tool(ctx, tool_name, args)
            render_simulation(response, json_output=json_output)
            if response.status == "error":
                sys.exit(1)
            return

        proceed = check_safety_gate(dry_run=False, yes=yes, action_description=action_desc)
        if not proceed:
            click.echo("Cancelled.")
            return

        response = _run_tool(ctx, tool_name, args)
        render_result(response, json_output=json_output, title=title)
        if response.status == "error":
            sys.exit(1)
    except click.ClickException:
        raise
    except Exception as e:
        render_error(str(e), json_output=json_output)
        sys.exit(1)


@ax.command("lending-supply")
@click.argument("token")
@click.argument("amount")
@click.option("--protocol", default="aave_v3", help="Lending protocol (default: aave_v3).")
@click.option("--no-collateral", is_flag=True, default=False, help="Supply without enabling as collateral.")
@click.option("--market-id", default=None, help="Market id for isolated-market protocols (e.g. Morpho Blue).")
@_action_options
@click.pass_context
def lending_supply(ctx, token, amount, protocol, no_collateral, market_id, sub_yes, sub_dry_run, sub_json_output):
    """Supply tokens to a lending protocol.

    \b
    Examples:
        almanak ax lending-supply USDC 100
        almanak ax lending-supply USDC 100 --protocol aave_v3 --dry-run
        almanak ax lending-supply WETH 0.5 --no-collateral
        almanak ax lending-supply USDC 100 --protocol morpho_blue --market-id 0x...
    """
    _guard_market_id_flag(protocol, market_id)

    args: dict = {
        "token": token,
        "amount": amount,
        "protocol": protocol,
        "use_as_collateral": not no_collateral,
        "chain": ctx.obj["chain"],
    }
    if market_id:
        args["market_id"] = market_id
    _run_lending_tool(
        ctx,
        "supply_lending",
        args,
        action_desc=f"Supply {amount} {token.upper()} to {protocol} on {ctx.obj['chain']}",
        title=f"Lending Supply: {amount} {token.upper()}",
        sub_yes=sub_yes,
        sub_dry_run=sub_dry_run,
        sub_json_output=sub_json_output,
    )


@ax.command("lending-borrow")
@click.argument("token")
@click.argument("amount")
@click.option("--collateral", "collateral_token", required=True, help="Collateral token symbol.")
@click.option(
    "--collateral-amount",
    required=True,
    help="Collateral amount (decimal) or 'all' to use full balance.",
)
@click.option("--protocol", default="aave_v3", help="Lending protocol (default: aave_v3).")
@click.option("--market-id", default=None, help="Market id for isolated-market protocols (e.g. Morpho Blue).")
@_action_options
@click.pass_context
def lending_borrow(
    ctx,
    token,
    amount,
    collateral_token,
    collateral_amount,
    protocol,
    market_id,
    sub_yes,
    sub_dry_run,
    sub_json_output,
):
    """Borrow tokens from a lending protocol.

    \b
    Examples:
        almanak ax lending-borrow USDC 100 --collateral WETH --collateral-amount 0.1
        almanak ax lending-borrow USDC 100 --collateral WETH --collateral-amount all --dry-run
        almanak ax lending-borrow USDC 100 --collateral WETH --collateral-amount 0.1 \\
            --protocol morpho_blue --market-id 0x...
    """
    _guard_market_id_flag(protocol, market_id)

    args: dict = {
        "token": token,
        "amount": amount,
        "collateral_token": collateral_token,
        "collateral_amount": collateral_amount,
        "protocol": protocol,
        "chain": ctx.obj["chain"],
    }
    if market_id:
        args["market_id"] = market_id
    _run_lending_tool(
        ctx,
        "borrow_lending",
        args,
        action_desc=(
            f"Borrow {amount} {token.upper()} against {collateral_amount} "
            f"{collateral_token.upper()} on {protocol} ({ctx.obj['chain']})"
        ),
        title=f"Lending Borrow: {amount} {token.upper()}",
        sub_yes=sub_yes,
        sub_dry_run=sub_dry_run,
        sub_json_output=sub_json_output,
    )


@ax.command("lending-repay")
@click.argument("token")
@click.argument("amount", required=False, default=None)
@click.option(
    "--full",
    "repay_full",
    is_flag=True,
    default=False,
    help="Repay the entire outstanding debt (shortcut for amount='all').",
)
@click.option("--protocol", default="aave_v3", help="Lending protocol (default: aave_v3).")
@click.option("--market-id", default=None, help="Market id for isolated-market protocols (e.g. Morpho Blue).")
@_action_options
@click.pass_context
def lending_repay(ctx, token, amount, repay_full, protocol, market_id, sub_yes, sub_dry_run, sub_json_output):
    """Repay a lending position.

    \b
    Examples:
        almanak ax lending-repay USDC 100            # Repay 100 USDC
        almanak ax lending-repay USDC --full          # Repay full debt (= amount 'all')
        almanak ax lending-repay USDC all             # Same as --full
        almanak ax lending-repay USDC --full --protocol morpho_blue --market-id 0x...
    """
    if repay_full:
        if amount is not None and str(amount).lower() != "all":
            raise click.UsageError("--full conflicts with a non-'all' positional amount; pass one or the other")
        resolved_amount = "all"
    elif amount is None:
        raise click.UsageError("Pass an amount, or use --full for full repayment")
    else:
        resolved_amount = amount

    _guard_market_id_flag(protocol, market_id)

    args: dict = {
        "token": token,
        "amount": resolved_amount,
        "protocol": protocol,
        "chain": ctx.obj["chain"],
    }
    if market_id:
        args["market_id"] = market_id
    _run_lending_tool(
        ctx,
        "repay_lending",
        args,
        action_desc=f"Repay {resolved_amount} {token.upper()} on {protocol} ({ctx.obj['chain']})",
        title=f"Lending Repay: {resolved_amount} {token.upper()}",
        sub_yes=sub_yes,
        sub_dry_run=sub_dry_run,
        sub_json_output=sub_json_output,
    )


@ax.command("lending-withdraw")
@click.argument("token")
@click.argument("amount", required=False, default=None)
@click.option(
    "--all",
    "withdraw_all",
    is_flag=True,
    default=False,
    help=(
        "Withdraw the full supplied balance (shortcut for amount='all'). "
        "Queries the protocol for the current balance at execution time."
    ),
)
@click.option("--protocol", default="aave_v3", help="Lending protocol (default: aave_v3).")
@click.option(
    "--market-id",
    default=None,
    help="Morpho Blue market id. Rejected for non-Morpho protocols (unified pools don't use markets).",
)
@click.option(
    "--loan-token/--collateral",
    "is_loan_token",
    default=False,
    help=(
        "Morpho Blue only: --loan-token withdraws the loan token, "
        "--collateral (default) withdraws the collateral token."
    ),
)
@_action_options
@click.pass_context
def lending_withdraw(
    ctx,
    token,
    amount,
    withdraw_all,
    protocol,
    market_id,
    is_loan_token,
    sub_yes,
    sub_dry_run,
    sub_json_output,
):
    """Withdraw supplied tokens from a lending protocol.

    \b
    Examples:
        almanak ax lending-withdraw USDC 100         # Withdraw 100 USDC
        almanak ax lending-withdraw USDC --all        # Withdraw full supplied balance
        almanak ax lending-withdraw USDC all          # Same as --all
        almanak ax lending-withdraw USDC --all --protocol morpho_blue --market-id 0xabc...
    """
    if withdraw_all:
        if amount is not None and str(amount).lower() != "all":
            raise click.UsageError("--all conflicts with a non-'all' positional amount; pass one or the other")
        resolved_amount = "all"
    elif amount is None:
        raise click.UsageError("Pass an amount, or use --all to withdraw full supplied balance")
    else:
        resolved_amount = amount

    # Reject Morpho-only flags on non-Morpho protocols rather than silently
    # ignoring them. Downstream WithdrawIntent would accept the fields without
    # complaint (CodeRabbit PR #1535 review), leaving the operator to wonder
    # why their --market-id or --loan-token had no effect.
    _guard_market_id_flag(protocol, market_id)
    # --loan-token remains Morpho-specific (Curvance has no analogous flag);
    # reject it on any other isolated-market protocol too.
    from almanak.connectors._strategy_base.lending_read_registry import LendingReadRegistry
    from almanak.framework.agent_tools.schemas import _normalize_protocol_key

    # fold (spaces + hyphens -> underscores) IN FRONT of registry call --
    # same pattern as executor.py:1407 (Plan 027 Step 5).
    _accepts_collateral_flag = LendingReadRegistry.accepts_is_collateral(_normalize_protocol_key(protocol))
    if is_loan_token and not _accepts_collateral_flag:
        raise click.UsageError(f"--loan-token is only supported on Morpho Blue; got protocol={protocol}")

    args: dict = {
        "token": token,
        "amount": resolved_amount,
        "protocol": protocol,
        "chain": ctx.obj["chain"],
    }
    # is_collateral and market_id are only forwarded when the protocol
    # declares accepts_is_collateral=True on its LendingReadDecl.
    if _accepts_collateral_flag:
        args["is_collateral"] = not is_loan_token
        if market_id:
            args["market_id"] = market_id

    _run_lending_tool(
        ctx,
        "withdraw_lending",
        args,
        action_desc=f"Withdraw {resolved_amount} {token.upper()} from {protocol} ({ctx.obj['chain']})",
        title=f"Lending Withdraw: {resolved_amount} {token.upper()}",
        sub_yes=sub_yes,
        sub_dry_run=sub_dry_run,
        sub_json_output=sub_json_output,
    )


# ---------------------------------------------------------------------------
# almanak ax pool <token_a> <token_b>
# ---------------------------------------------------------------------------


def _pool_title_suffix(protocol: str, fee_tier: int) -> str:
    """Render the ``ax pool`` title fee-tier suffix from registry family facts.

    Protocols in ``TICK_SPACING_FEE_DISPLAY`` (e.g. aerodrome_slipstream)
    render ``tick_spacing=<N>``; every other protocol renders a percentage.
    The raw CLI value is normalized first so aliases ("aerodrome-slipstream",
    "Aerodrome Slipstream") resolve the same way the registry keys are stored
    (CodeRabbit, PR #2778). Shared with the unit test so production and test
    exercise one code path.

    Function-scope boot import satisfies the strategy-side lean-import
    contract (pattern: compiler_constants.py:533).
    """
    from almanak.connectors._strategy_protocol_family_registry import (
        PROTOCOL_FAMILY_REGISTRY,
        ProtocolFamily,
    )
    from almanak.framework.agent_tools.schemas import _normalize_protocol_key

    normalized_protocol = _normalize_protocol_key(protocol)
    if normalized_protocol in PROTOCOL_FAMILY_REGISTRY.members(ProtocolFamily.TICK_SPACING_FEE_DISPLAY):
        return f"tick_spacing={fee_tier}"
    return f"{fee_tier / 10000:.2f}%"


@ax.command("pool")
@click.argument("token_a")
@click.argument("token_b")
@click.option(
    "--fee-tier",
    type=int,
    default=3000,
    help="Pool fee tier in hundredths of a bip (default: 3000 = 0.3%).",
)
@click.option(
    "--protocol",
    default="uniswap_v3",
    help="DEX protocol (default: uniswap_v3).",
)
@click.pass_context
def pool(ctx, token_a, token_b, fee_tier, protocol):
    """Get details about a liquidity pool.

    Shows current price, tick, liquidity, volume, fees, and TVL.

    \b
    Examples:
        almanak ax pool WBTC WETH                      # WBTC-WETH pool info
        almanak ax pool USDC ETH --fee-tier 500         # 0.05% fee tier
        almanak ax pool WBTC WETH --json                # JSON output
    """
    from almanak.framework.cli.ax_render import render_error, render_result

    json_output = ctx.obj["json_output"]
    try:
        response = _run_tool(
            ctx,
            "get_pool_state",
            {
                "token_a": token_a,
                "token_b": token_b,
                "fee_tier": fee_tier,
                "chain": ctx.obj["chain"],
                "protocol": protocol,
            },
        )
        title_suffix = _pool_title_suffix(protocol, fee_tier)
        render_result(
            response,
            json_output=json_output,
            title=f"Pool: {token_a.upper()}/{token_b.upper()} ({title_suffix})",
        )
        if response.status == "error":
            sys.exit(1)
    except click.ClickException:
        raise
    except Exception as e:
        render_error(str(e), json_output=json_output)
        sys.exit(1)


# ---------------------------------------------------------------------------
# almanak ax bridge <token> <amount> --from-chain <chain> --to-chain <chain>
# ---------------------------------------------------------------------------


@ax.command()
@click.argument("token")
@click.argument("amount")
@click.option(
    "--from-chain",
    required=True,
    help="Source chain (e.g. 'arbitrum', 'base', 'ethereum').",
)
@click.option(
    "--to-chain",
    required=True,
    help="Destination chain (e.g. 'arbitrum', 'base', 'ethereum').",
)
@click.option(
    "--slippage",
    type=int,
    default=50,
    help="Max slippage in basis points (default: 50 = 0.5%).",
)
@click.option(
    "--bridge",
    "preferred_bridge",
    default=None,
    help="Preferred bridge adapter (e.g. 'across', 'stargate').",
)
@_action_options
@click.pass_context
def bridge(ctx, token, amount, from_chain, to_chain, slippage, preferred_bridge, sub_yes, sub_dry_run, sub_json_output):
    """Bridge tokens from one chain to another.

    \b
    Examples:
        almanak ax bridge USDC 100 --from-chain arbitrum --to-chain base
        almanak ax bridge ETH 0.5 --from-chain ethereum --to-chain arbitrum --dry-run
        almanak ax bridge USDC 100 --from-chain base --to-chain arbitrum --bridge across
        almanak ax bridge USDC 50 --from-chain arbitrum --to-chain base --yes
    """
    from almanak.framework.cli.ax_render import (
        check_safety_gate,
        render_error,
        render_result,
        render_simulation,
    )

    yes, dry_run, json_output = _merge_flags(ctx, sub_yes, sub_dry_run, sub_json_output)

    action_desc = f"Bridge {amount} {token.upper()} from {from_chain} to {to_chain}"

    try:
        args = {
            "token": token,
            "amount": amount,
            "from_chain": from_chain,
            "to_chain": to_chain,
            "slippage_bps": slippage,
        }
        if preferred_bridge:
            args["preferred_bridge"] = preferred_bridge

        # Dry-run: simulate only
        if dry_run:
            args["dry_run"] = True
            response = _run_tool(ctx, "bridge_tokens", args)
            render_simulation(response, json_output=json_output)
            if response.status == "error":
                sys.exit(1)
            return

        # Safety gate: confirm before executing
        proceed = check_safety_gate(dry_run=False, yes=yes, action_description=action_desc)
        if not proceed:
            click.echo("Cancelled.")
            return

        # Execute
        response = _run_tool(ctx, "bridge_tokens", args)
        render_result(response, json_output=json_output, title="Bridge")
        if response.status == "error":
            sys.exit(1)
    except click.ClickException:
        raise
    except Exception as e:
        render_error(str(e), json_output=json_output)
        sys.exit(1)


# ---------------------------------------------------------------------------
# almanak ax unwrap <token> <amount>
# ---------------------------------------------------------------------------


@ax.command()
@click.argument("token")
@click.argument("amount")
@click.option(
    "--chain",
    "-c",
    "sub_chain",
    default=None,
    help="Chain to unwrap on (overrides the group-level --chain when both are set).",
)
@_action_options
@click.pass_context
def unwrap(ctx, token, amount, sub_chain, sub_yes, sub_dry_run, sub_json_output):
    """Unwrap wrapped native tokens (e.g. WETH -> ETH, WMATIC -> MATIC).

    \b
    Examples:
        almanak ax unwrap WETH 0.002                    # Unwrap 0.002 WETH to ETH
        almanak ax unwrap WETH all                      # Unwrap all WETH
        almanak ax unwrap WETH 0.002 --dry-run          # Simulate only
        almanak ax unwrap WMATIC 1.0 --chain polygon    # Unwrap WMATIC on Polygon
    """
    from almanak.framework.cli.ax_render import (
        check_safety_gate,
        render_error,
        render_result,
        render_simulation,
    )

    yes, dry_run, json_output = _merge_flags(ctx, sub_yes, sub_dry_run, sub_json_output)

    # Subcommand-level --chain wins over group-level when provided. This
    # matches Click's "more specific placement wins" convention and keeps
    # both invocation styles working:
    #   almanak ax unwrap WETH 0.002 --chain base   (sub)
    #   almanak ax --chain base unwrap WETH 0.002   (group)
    #
    # We update ctx.obj["chain"] (not just the tool arg) because downstream
    # infrastructure -- _get_executor() and _start_managed_gateway() -- reads
    # the chain from ctx.obj to initialize the executor / gateway client /
    # managed gateway. Passing the override only to the tool args would leave
    # the gateway pointed at the group-level chain (or default).
    if sub_chain is not None:
        ctx.obj["chain"] = sub_chain

    action_desc = f"Unwrap {amount} {token.upper()} to native on {ctx.obj['chain']}"

    try:
        args = {
            "token": token,
            "amount": amount,
            "chain": ctx.obj["chain"],
        }

        if dry_run:
            args["dry_run"] = True
            response = _run_tool(ctx, "unwrap_native", args)
            render_simulation(response, json_output=json_output)
            if response.status == "error":
                sys.exit(1)
            return

        proceed = check_safety_gate(dry_run=False, yes=yes, action_description=action_desc)
        if not proceed:
            click.echo("Cancelled.")
            return

        response = _run_tool(ctx, "unwrap_native", args)
        render_result(response, json_output=json_output, title="Unwrap")
        if response.status == "error":
            sys.exit(1)
    except click.ClickException:
        raise
    except Exception as e:
        render_error(str(e), json_output=json_output)
        sys.exit(1)


# ---------------------------------------------------------------------------
# almanak ax bundle-list / bundle-clear
# ---------------------------------------------------------------------------


@ax.command("bundle-list")
@click.pass_context
def bundle_list(ctx):
    """List compiled intent bundles cached on disk.

    Bundles produced by ``ax run compile_intent`` persist to
    ``${XDG_CACHE_HOME:-~/.cache}/almanak/bundles/`` so a follow-up
    ``ax run execute_compiled_bundle '{"bundle_id":"..."}'`` from a new
    shell can still find them. Expired entries are shown with remaining
    TTL <= 0; run ``ax bundle-clear --expired`` to prune them.
    """
    import json as json_mod

    from almanak.framework.agent_tools.bundle_cache import BundleCache

    json_output = ctx.obj["json_output"]
    cache = BundleCache()
    now = time.time()
    entries = cache.list_entries()

    if json_output:
        payload = [
            {
                "bundle_id": bid,
                "chain": entry.chain,
                "age_seconds": round(entry.age_seconds(now), 2),
                "ttl_seconds": entry.ttl_seconds,
                "expired": entry.is_expired(now),
                "intent_type": entry.args.get("intent_type"),
            }
            for bid, entry in entries
        ]
        click.echo(json_mod.dumps({"cache_dir": str(cache.cache_dir), "entries": payload}, indent=2))
        return

    if not entries:
        click.echo(f"No cached bundles in {cache.cache_dir}")
        return

    click.echo(f"\nCached bundles ({len(entries)}) in {cache.cache_dir}:")
    click.echo("-" * 100)
    click.echo(f"  {'bundle_id':<38} {'chain':<10} {'intent':<20} {'age':>6} {'ttl':>6} state")
    click.echo("-" * 100)
    for bundle_id, entry in entries:
        age = int(entry.age_seconds(now))
        intent_type = str(entry.args.get("intent_type") or "-")[:20]
        state = "EXPIRED" if entry.is_expired(now) else "live"
        color = "red" if state == "EXPIRED" else "green"
        click.echo(
            f"  {bundle_id:<38} {entry.chain:<10} {intent_type:<20} "
            f"{age:>5}s {entry.ttl_seconds:>5}s {click.style(state, fg=color)}"
        )
    click.echo()


@ax.command("bundle-clear")
@click.option("--expired", is_flag=True, default=False, help="Only remove entries past their TTL.")
@click.option("--yes", is_flag=True, default=False, help="Skip interactive confirmation.")
@click.pass_context
def bundle_clear(ctx, expired, yes):
    """Remove cached compiled bundles from disk.

    Default is to clear all entries; pass ``--expired`` to prune only
    entries past their TTL.
    """
    import json as json_mod

    from almanak.framework.agent_tools.bundle_cache import BundleCache

    json_output = ctx.obj["json_output"]
    cache = BundleCache()
    entries = cache.list_entries()

    if expired:
        now = time.time()
        target_count = sum(1 for _, e in entries if e.is_expired(now))
    else:
        target_count = len(entries)

    if target_count == 0:
        msg = "No expired bundles to clear." if expired else f"No bundles to clear in {cache.cache_dir}"
        if json_output:
            click.echo(json_mod.dumps({"removed": 0, "cache_dir": str(cache.cache_dir)}))
        else:
            click.echo(msg)
        return

    # Respect the group-level --yes/-y flag so ``ax --yes bundle-clear`` works
    # without an additional subcommand flag, matching other ax commands.
    effective_yes = yes or ctx.obj.get("yes", False)
    if not effective_yes and not json_output:
        target = "expired" if expired else "all"
        if not click.confirm(f"Remove {target_count} {target} bundle(s) from {cache.cache_dir}?"):
            click.echo("Cancelled.")
            return

    removed = cache.prune_expired() if expired else cache.clear()

    if json_output:
        click.echo(json_mod.dumps({"removed": removed, "cache_dir": str(cache.cache_dir)}))
    else:
        click.echo(f"Removed {removed} bundle(s) from {cache.cache_dir}")


# ---------------------------------------------------------------------------
# almanak ax tools
# ---------------------------------------------------------------------------


@ax.command("tools")
@click.option(
    "--category",
    type=click.Choice(["data", "planning", "action", "state"], case_sensitive=False),
    default=None,
    help="Filter tools by category.",
)
@click.option(
    "--describe",
    default=None,
    metavar="TOOL_NAME",
    help="Show full argument schema for a specific tool.",
)
@click.pass_context
def tools_list(ctx, category, describe):
    """List all available tools in the catalog.

    \b
    Examples:
        almanak ax tools                            # List all tools
        almanak ax tools --category action          # Only action tools
        almanak ax tools --describe supply_lending   # Show argument schema
        almanak ax tools --json                     # JSON output
    """
    import json as json_mod

    from almanak.framework.agent_tools.catalog import ToolCategory, get_default_catalog
    from almanak.framework.cli.ax_render import render_error

    json_output = ctx.obj["json_output"]
    catalog = get_default_catalog()

    # --describe: show full schema for a specific tool
    if describe:
        tool_def = catalog.get(describe)
        if tool_def is None:
            available = [t.name for t in catalog.list_tools()]
            render_error(
                f"Unknown tool: '{describe}'. Available: {', '.join(available)}",
                json_output=json_output,
            )
            sys.exit(1)

        if json_output:
            schema = tool_def.input_json_schema()
            schema["_meta"] = {
                "name": tool_def.name,
                "category": tool_def.category.value,
                "risk_tier": tool_def.risk_tier.value,
                "description": tool_def.description,
            }
            click.echo(json_mod.dumps(schema, indent=2))
        else:
            _render_tool_schema(tool_def)
        return

    cat_filter = ToolCategory(category) if category else None
    tool_defs = catalog.list_tools(category=cat_filter)

    if json_output:
        items = [
            {
                "name": t.name,
                "category": t.category.value,
                "risk_tier": t.risk_tier.value,
                "description": t.description,
            }
            for t in tool_defs
        ]
        click.echo(json_mod.dumps(items, indent=2))
    else:
        click.echo(f"\nAvailable tools ({len(tool_defs)}):")
        click.echo("-" * 60)
        for t in tool_defs:
            risk = t.risk_tier.value.upper()
            risk_color = _RISK_COLORS.get(risk, "white")
            click.echo(f"  {t.name:<30} [{click.style(risk, fg=risk_color)}] {t.description}")
        click.echo()


_RISK_COLORS = {"NONE": "green", "LOW": "blue", "MEDIUM": "yellow", "HIGH": "red"}


def _render_tool_schema(tool_def) -> None:
    """Render a tool's argument schema in human-readable format."""
    risk = tool_def.risk_tier.value.upper()
    risk_color = _RISK_COLORS.get(risk, "white")
    category = tool_def.category.value.upper()

    click.echo()
    click.echo(f"  {click.style(tool_def.name, bold=True)} ({category}, {click.style(risk, fg=risk_color)} risk)")
    click.echo(f"  {tool_def.description}")
    click.echo()

    schema = tool_def.input_json_schema()
    properties = schema.get("properties", {})
    required_fields = set(schema.get("required", []))
    defs = schema.get("$defs", {})

    if not properties:
        click.echo("  No arguments.")
        click.echo()
        return

    # Separate required and optional fields
    required_items = []
    optional_items = []
    for field_name, field_schema in properties.items():
        entry = (field_name, field_schema)
        if field_name in required_fields:
            required_items.append(entry)
        else:
            optional_items.append(entry)

    if required_items:
        click.echo(click.style("  Required:", bold=True))
        for field_name, field_schema in required_items:
            _render_field(field_name, field_schema, defs)

    if optional_items:
        if required_items:
            click.echo()
        click.echo(click.style("  Optional:", bold=True))
        for field_name, field_schema in optional_items:
            _render_field(field_name, field_schema, defs)

    click.echo()


def _render_field(field_name: str, field_schema: dict, defs: dict | None = None) -> None:
    """Render a single field in the schema."""
    type_str = _format_type(field_schema, defs or {})
    description = field_schema.get("description", "")
    has_default = "default" in field_schema
    default = field_schema.get("default")

    parts = [f"    {field_name:<22} ({type_str})"]
    if has_default:
        parts[0] += f"  [default: {default}]"
    if description:
        parts.append(f"      {description}")

    click.echo("\n".join(parts))


def _resolve_ref(schema: dict, defs: dict) -> dict | None:
    """Resolve a $ref pointer against $defs. Returns None if unresolvable."""
    ref = schema.get("$ref", "")
    if ref.startswith("#/$defs/"):
        name = ref[len("#/$defs/") :]
        return defs.get(name)
    return None


def _format_type(field_schema: dict, defs: dict | None = None) -> str:
    """Format a JSON Schema type into a readable string."""
    defs = defs or {}

    # Resolve top-level $ref
    if "$ref" in field_schema:
        resolved = _resolve_ref(field_schema, defs)
        return _format_type(resolved, defs) if resolved is not None else "any"

    # Handle anyOf (union types, e.g. str | None)
    if "anyOf" in field_schema:
        types = []
        for opt in field_schema["anyOf"]:
            fmt = _format_type(opt, defs)
            if fmt != "null":
                types.append(fmt)
        types = list(dict.fromkeys(types))
        return " | ".join(types) if types else "any"

    field_type = field_schema.get("type", "any")

    # Handle enum values
    if "enum" in field_schema:
        return f"{field_type} ({' | '.join(repr(v) for v in field_schema['enum'])})"

    # Handle arrays
    if field_type == "array":
        items = field_schema.get("items", {})
        item_type = _format_type(items, defs)
        return f"list[{item_type}]"

    return field_type


# ---------------------------------------------------------------------------
# almanak ax run <tool_name> [JSON_ARGS]
# ---------------------------------------------------------------------------


@ax.command("run")
@click.argument("tool_name")
@click.argument("args_json", default="{}")
@_action_options
@click.pass_context
def run_tool(ctx, tool_name, args_json, sub_yes, sub_dry_run, sub_json_output):
    """Run any tool from the catalog by name.

    Generic fallback for tools without a dedicated subcommand.
    Pass arguments as a JSON string.

    \b
    Examples:
        almanak ax run get_price '{"token": "ETH"}'
        almanak ax run get_balance '{"token": "USDC"}' --json
        almanak ax run compile_intent '{"intent_type": "swap", ...}'
        almanak ax --dry-run run swap_tokens '{"token_in": "USDC", "token_out": "ETH", "amount": "100"}'
    """
    import json as json_mod

    from almanak.framework.agent_tools.catalog import RiskTier, get_default_catalog
    from almanak.framework.cli.ax_render import (
        check_safety_gate,
        render_error,
        render_result,
        render_simulation,
    )

    yes, dry_run, json_output = _merge_flags(ctx, sub_yes, sub_dry_run, sub_json_output)

    # Validate tool exists
    catalog = get_default_catalog()
    tool_def = catalog.get(tool_name)
    if tool_def is None:
        available = [t.name for t in catalog.list_tools()]
        render_error(f"Unknown tool: '{tool_name}'. Available: {', '.join(available)}", json_output=json_output)
        sys.exit(1)

    # Parse args
    try:
        args = json_mod.loads(args_json)
    except json_mod.JSONDecodeError as e:
        render_error(f"Invalid JSON arguments: {e}", json_output=json_output)
        sys.exit(1)

    if not isinstance(args, dict):
        render_error("Arguments must be a JSON object (dict)", json_output=json_output)
        sys.exit(1)

    # Inject chain default if not provided
    if "chain" not in args:
        args["chain"] = ctx.obj["chain"]

    try:
        # For write actions (MEDIUM/HIGH risk), apply safety gate
        is_write = tool_def.risk_tier in (RiskTier.MEDIUM, RiskTier.HIGH)

        if is_write and dry_run:
            args["dry_run"] = True
            response = _run_tool(ctx, tool_name, args)
            render_simulation(response, json_output=json_output)
            if response.status == "error":
                sys.exit(1)
            return

        if is_write:
            proceed = check_safety_gate(
                dry_run=False,
                yes=yes,
                action_description=f"Execute {tool_name} with {args_json}",
            )
            if not proceed:
                click.echo("Cancelled.")
                return

        response = _run_tool(ctx, tool_name, args)
        render_result(response, json_output=json_output, title=tool_name)
        if response.status == "error":
            sys.exit(1)
    except click.ClickException:
        raise
    except Exception as e:
        render_error(str(e), json_output=json_output)
        sys.exit(1)


# =============================================================================
# `almanak ax positions` — control-plane reconciliation (T24 / VIB-4210)
# =============================================================================
#
# Reconciliation is a CONTROL-PLANE operation, not a strategy tool. It dispatches
# directly to ``PositionService.Reconcile`` via the gateway client (no ToolExecutor /
# PolicyEngine wrap — those are for LLM-driven strategy surfaces per CLAUDE.md
# "Agent-tools" rule; ``ax positions`` is invoked by an operator with full intent).
#
# v1 scope: UniV3 LP only (ADR §2.4). Per-primitive follow-ups land in T24+1.
# Closes user-facing bug: GH #2131.


@ax.group("positions")
@click.pass_context
def positions(ctx):
    """Position reconciliation commands (T24 / VIB-4210).

    \b
    Subcommands:
        reconcile   Reconcile position_registry against on-chain truth.
    """
    _ = ctx  # group is a thin namespace; subcommands carry the real logic


@positions.command("reconcile")
@click.option(
    "--deployment-id",
    required=True,
    help="Deployment id (ClassName:hash) whose registry to reconcile.",
)
@click.option(
    "--wallet-override",
    default=None,
    help="Override the wallet address (default: ax's configured wallet).",
)
@click.option(
    "--primitives",
    "primitives_csv",
    default="lp",
    help="Comma-separated primitive filter (default: 'lp'; v1 supports lp only).",
)
@click.option(
    "--physical-identity-hash",
    "physical_identity_hashes",
    multiple=True,
    help="Filter to specific registry rows by hash (repeatable).",
)
@click.option(
    "--apply",
    is_flag=True,
    default=False,
    help="Write phantom-missing rows to position_registry (default: dry-run).",
)
@click.option(
    "--max-age-blocks",
    type=int,
    default=0,
    help="Reject chain-head lag exceeding this many blocks (0 = no check).",
)
@click.option(
    "--operator-note",
    default="",
    help="Free-form audit note (capped at 256 bytes).",
)
@click.option(
    "--trigger",
    type=click.Choice(["operator_cli", "dashboard", "ci"]),
    default="operator_cli",
    help="Triggering surface for telemetry labelling.",
)
@click.pass_context
def positions_reconcile(
    ctx,
    deployment_id: str,
    wallet_override: str | None,
    primitives_csv: str,
    physical_identity_hashes: tuple[str, ...],
    apply: bool,
    max_age_blocks: int,
    operator_note: str,
    trigger: str,
):
    """Reconcile a deployment's position_registry against on-chain truth.

    Detects:
    \b
      * matched          - on-chain and registry agree.
      * phantom_missing  - on-chain has, registry doesn't (the GH #2131 case).
      * stranded         - registry status='open', chain absent.
      * rebuilt          - when --apply, the phantom_missing rows just written.

    Default is dry-run (--apply not set): displays the diff WITHOUT writing.
    Pass --apply to insert phantom_missing rows into position_registry
    (transaction_ledger is NOT touched on this path -- reconciliation
    re-derives registry from chain truth, never replays intent history).

    Stranded rows are NEVER auto-closed. After review, run a teardown for
    the specific position to close cleanly.

    \b
    Examples:
        almanak ax positions reconcile --deployment-id MyStrat:abc
        almanak ax --chain base positions reconcile --deployment-id MyStrat:abc --apply
        almanak ax positions reconcile --deployment-id MyStrat:abc --max-age-blocks 32
    """
    from almanak.framework.cli.ax_render import render_error
    from almanak.gateway.proto import gateway_pb2

    json_output = ctx.obj["json_output"]
    chain = ctx.obj["chain"]
    wallet = wallet_override or ctx.obj.get("wallet") or ""
    primitives_list = [p.strip() for p in primitives_csv.split(",") if p.strip()]

    try:
        _executor, client = _get_executor(ctx)
        request = gateway_pb2.ReconcileRequest(
            deployment_id=deployment_id,
            chain=chain,
            wallet_address=wallet,
            primitives=primitives_list,
            physical_identity_hashes=list(physical_identity_hashes),
            apply=apply,
            max_age_blocks=int(max_age_blocks),
            operator_note=operator_note,
            trigger=trigger,
        )
        response = client.position.Reconcile(request, timeout=120.0)
    except click.ClickException:
        raise
    except Exception as e:
        render_error(f"Reconcile RPC failed: {e}", json_output=json_output)
        sys.exit(1)

    _render_reconcile_response(response, json_output=json_output, apply=apply)


def _render_reconcile_response(response, *, json_output: bool, apply: bool) -> None:
    """Render a ReconcileResponse for the operator.

    Two surfaces:
    - ``--json``: full JSON dump of the response envelope. Forensic-grade,
      for piping into ``jq`` or dashboards.
    - default: human-readable summary line + per-bucket detail. The four
      counts are always shown so a one-line scan tells the operator
      immediately whether the registry is consistent.
    """
    import json as _json

    if json_output:
        envelope = {
            "reconciliation_id": response.reconciliation_id,
            "source_block_number": response.source_block_number,
            "matched_count": response.matched_count,
            "phantom_missing_count": response.phantom_missing_count,
            "stranded_count": response.stranded_count,
            "rebuilt_count": response.rebuilt_count,
            "oversize": response.oversize,
            "oversize_detail": response.oversize_detail,
            "duration_seconds": response.duration_seconds,
            "matched": [
                {
                    "physical_identity_hash": m.physical_identity_hash,
                    "primitive": m.primitive,
                    "accounting_category": m.accounting_category,
                    "confirmed_at_block": m.confirmed_at_block,
                }
                for m in response.matched
            ],
            "phantom_missing": [
                {
                    "physical_identity_hash": p.physical_identity_hash,
                    "primitive": p.primitive,
                    "accounting_category": p.accounting_category,
                    "semantic_grouping_key": p.semantic_grouping_key,
                    "payload": _safe_decode_json(p.payload_json),
                    "opened_at_block": p.opened_at_block,
                    "opened_tx": p.opened_tx,
                }
                for p in response.phantom_missing
            ],
            "stranded": [
                {
                    "physical_identity_hash": s.physical_identity_hash,
                    "primitive": s.primitive,
                    "accounting_category": s.accounting_category,
                    "handle": s.handle,
                    "registry_row": _safe_decode_json(s.registry_row_json),
                    "confirmed_absent_at_block": s.confirmed_absent_at_block,
                    "absent_reason": s.absent_reason,
                }
                for s in response.stranded
            ],
            "rebuilt": [
                {
                    "physical_identity_hash": r.physical_identity_hash,
                    "primitive": r.primitive,
                    "accounting_category": r.accounting_category,
                    "source": r.source,
                    "last_reconciled_at_block": r.last_reconciled_at_block,
                    "reconciliation_id": r.reconciliation_id,
                    "registry_row": _safe_decode_json(r.registry_row_json),
                }
                for r in response.rebuilt
            ],
            "primitive_errors": [
                {
                    "primitive": e.primitive,
                    "chain": e.chain,
                    "code": e.code,
                    "message": e.message,
                    "recoverable": e.recoverable,
                }
                for e in response.primitive_errors
            ],
        }
        click.echo(_json.dumps(envelope, indent=2))
        return

    apply_label = "apply=true (registry written)" if apply else "apply=false (dry-run)"
    click.echo(f"reconciliation_id: {response.reconciliation_id}")
    click.echo(f"source_block_number: {response.source_block_number}")
    click.echo(f"mode: {apply_label}")
    click.echo(f"duration: {response.duration_seconds:.3f}s")
    click.echo(
        f"matched: {response.matched_count} | "
        f"phantom_missing: {response.phantom_missing_count} | "
        f"stranded: {response.stranded_count} | "
        f"rebuilt: {response.rebuilt_count}"
    )
    if response.oversize:
        click.echo(click.style(f"OVERSIZE: {response.oversize_detail}", fg="yellow"))
    if response.phantom_missing_count > 0:
        click.echo("\nphantom_missing (on-chain has, registry doesn't):")
        for p in response.phantom_missing:
            click.echo(f"  - pih={p.physical_identity_hash[:16]}... primitive={p.primitive}")
    if response.stranded_count > 0:
        click.echo("\nstranded (registry open, chain absent -- NOT auto-closed):")
        for s in response.stranded:
            click.echo(f"  - pih={s.physical_identity_hash[:16]}... reason={s.absent_reason}")
    if response.rebuilt_count > 0:
        click.echo("\nrebuilt (registry rows written from chain truth):")
        for r in response.rebuilt:
            click.echo(
                f"  - pih={r.physical_identity_hash[:16]}... last_reconciled_at_block={r.last_reconciled_at_block}"
            )
    if response.primitive_errors:
        click.echo("\nprimitive_errors (partial failures, RPC still SUCCESS):")
        for e in response.primitive_errors:
            tag = "recoverable" if e.recoverable else "non-recoverable"
            click.echo(
                click.style(
                    f"  - [{e.code}] ({tag}) primitive={e.primitive}: {e.message}",
                    fg="red" if not e.recoverable else "yellow",
                )
            )


def _safe_decode_json(raw: bytes) -> object:
    """Decode a JSON-bytes proto field; return {} on any failure."""
    import json as _json

    if not raw:
        return {}
    try:
        return _json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError, _json.JSONDecodeError):
        return {}
