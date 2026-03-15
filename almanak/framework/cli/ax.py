"""``almanak ax`` -- direct DeFi action execution from the command line.

One-shot DeFi commands (swap, bridge, balance, price, LP) without writing strategy files.
Uses the existing ToolExecutor + PolicyEngine infrastructure with a thin CLI shim.

Auto-starts a gateway if none is running. Use ``--network anvil`` for local testing.
"""

from __future__ import annotations

import asyncio
import sys
from typing import TYPE_CHECKING

import click

if TYPE_CHECKING:
    from almanak.gateway.managed import ManagedGateway


@click.group(invoke_without_command=True)
@click.option(
    "--gateway-host",
    default="localhost",
    envvar="GATEWAY_HOST",
    help="Gateway hostname (default: localhost).",
)
@click.option(
    "--gateway-port",
    default=50051,
    type=int,
    envvar="GATEWAY_PORT",
    help="Gateway gRPC port (default: 50051).",
)
@click.option(
    "--chain",
    "-c",
    default="arbitrum",
    envvar="ALMANAK_CHAIN",
    help="Default chain (default: arbitrum).",
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
    # Also check .power-env (extended config). Neither overrides existing vars.
    from pathlib import Path

    from dotenv import load_dotenv

    for env_name in (".env", ".power-env"):
        env_file = Path.cwd() / env_name
        if env_file.exists():
            load_dotenv(env_file)

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
            "Set AGENT_LLM_API_KEY environment variable.\n\n"
            "  export AGENT_LLM_API_KEY=sk-...\n\n"
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

    from almanak.framework.agent_tools.cli_executor import create_cli_executor

    host = ctx.obj["gateway_host"]
    port = ctx.obj["gateway_port"]
    network = ctx.obj.get("network")

    # Try connecting to an existing gateway first
    try:
        executor, client = create_cli_executor(
            gateway_host=host,
            gateway_port=port,
            chain=ctx.obj["chain"],
            wallet_address=ctx.obj["wallet"],
            max_single_trade_usd=ctx.obj["max_trade_usd"],
            connect_timeout=2.0,  # quick probe
        )
        ctx.obj["executor"] = executor
        ctx.obj["client"] = client
        return executor, client
    except click.ClickException:
        pass  # No gateway running -- auto-start one below

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
    import os
    import uuid

    from almanak.gateway.core.settings import GatewaySettings
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

    # Forward private key so the gateway can sign transactions
    private_key = os.environ.get("ALMANAK_PRIVATE_KEY", "")
    if private_key:
        gateway_kwargs["private_key"] = private_key

    settings = GatewaySettings(**gateway_kwargs)
    anvil_chains = [chain] if resolved_network == "anvil" else []

    click.echo(click.style("Auto-starting gateway", bold=True) + f" ({resolved_network}) on {host}:{gw_port}...")

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

    click.echo(click.style("Gateway ready.", fg="green", bold=True))
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
@click.pass_context
def price(ctx, token):
    """Get the current USD price of a token.

    \b
    Examples:
        almanak ax price ETH
        almanak ax price USDC --chain base
        almanak ax price ETH --json
    """
    from almanak.framework.cli.ax_render import render_error, render_result

    json_output = ctx.obj["json_output"]
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
# almanak ax balance <token>
# ---------------------------------------------------------------------------


@ax.command()
@click.argument("token")
@click.pass_context
def balance(ctx, token):
    """Get the balance of a token in your wallet.

    \b
    Examples:
        almanak ax balance USDC
        almanak ax balance ETH --chain base
        almanak ax balance WETH --json
    """
    from almanak.framework.cli.ax_render import render_error, render_result

    json_output = ctx.obj["json_output"]
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
@click.pass_context
def swap(ctx, from_token, to_token, amount, slippage, protocol):
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

    json_output = ctx.obj["json_output"]
    dry_run = ctx.obj["dry_run"]
    yes = ctx.obj["yes"]

    action_desc = f"Swap {amount} {from_token.upper()} -> {to_token.upper()} on {ctx.obj['chain']}"

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
            if response.status == "error":
                sys.exit(1)
            return

        # Safety gate: confirm before executing
        proceed = check_safety_gate(dry_run=False, yes=yes, action_description=action_desc)
        if not proceed:
            click.echo("Cancelled.")
            return

        # Execute
        response = _run_tool(ctx, "swap_tokens", args)
        render_result(response, json_output=json_output, title="Swap")
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
@click.pass_context
def lp_close(ctx, position_id, protocol, no_collect_fees):
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

    json_output = ctx.obj["json_output"]
    dry_run = ctx.obj["dry_run"]
    yes = ctx.obj["yes"]

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
@click.pass_context
def lp_info(ctx, position_id, protocol):
    """Get details about an existing LP position.

    Shows range, liquidity, accrued fees, and in-range status.

    \b
    Examples:
        almanak ax lp-info 123456                      # View LP #123456
        almanak ax lp-info 123456 --json               # JSON output
        almanak ax lp-info 123456 --protocol uniswap_v3
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
# almanak ax pool <token_a> <token_b>
# ---------------------------------------------------------------------------


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
        render_result(
            response,
            json_output=json_output,
            title=f"Pool: {token_a.upper()}/{token_b.upper()} ({fee_tier / 10000:.2f}%)",
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
@click.pass_context
def bridge(ctx, token, amount, from_chain, to_chain, slippage, preferred_bridge):
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

    json_output = ctx.obj["json_output"]
    dry_run = ctx.obj["dry_run"]
    yes = ctx.obj["yes"]

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
@click.pass_context
def unwrap(ctx, token, amount):
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

    json_output = ctx.obj["json_output"]
    dry_run = ctx.obj["dry_run"]
    yes = ctx.obj["yes"]

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
@click.pass_context
def run_tool(ctx, tool_name, args_json):
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

    json_output = ctx.obj["json_output"]
    dry_run = ctx.obj["dry_run"]
    yes = ctx.obj["yes"]

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
