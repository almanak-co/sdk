import json
import os
import platform
import stat
import subprocess
import sys
from pathlib import Path

import click
import requests
from dotenv import load_dotenv

from almanak import __version__
from almanak.cli.agent import agent as agent_group
from almanak.core.redaction import install_redaction

# V2 Framework CLI commands
from almanak.framework.cli import backtest as framework_backtest_group
from almanak.framework.cli import new_strategy as framework_new_strategy_cmd
from almanak.framework.cli.demo import demo as framework_demo_cmd
from almanak.framework.cli.run import run as framework_run_cmd
from almanak.framework.cli.teardown import teardown as framework_teardown_group


def format_output(status: str | None = None, title=None, key_value_pairs=None, items=None, delimiter=True):
    """
    Formats and prints a title, a dictionary of key-value pairs, or a list of items with consistent formatting.

    :param status: Status of the response (success, error, info).
    :param title: The section title to display (e.g., "Available Strategies").
    :param key_value_pairs: A dictionary of key-value pairs to display in a formatted manner.
    :param items: A list of items to display with numbering.
    :param delimiter: Whether to print a delimiter (separator line) around the content.
    """
    output = []
    if delimiter:
        output.append("=========================================")

    if status:
        output.append(f"[{status.upper()}]")

    if title:
        output.append(title)
        output.append("-----------------------------------------")

    if key_value_pairs:
        for key, value in key_value_pairs.items():
            output.append(f"{key:<18}: {value}")

    if items:
        for i, item in enumerate(items, start=1):
            output.append(f"{i}. {item}")

    if delimiter:
        output.append("=========================================")

    click.echo("\n".join(output))


def _resolve_native_binary() -> Path:
    """Return the path to the platform-specific almanak-code binary."""
    os_name = platform.system().lower()  # "darwin", "linux", "windows"
    arch = platform.machine().lower()  # "arm64", "aarch64", "x86_64", "amd64"
    arch_map = {"aarch64": "arm64", "amd64": "x86_64"}
    arch = arch_map.get(arch, arch)
    ext = ".exe" if os_name == "windows" else ""
    binary_name = f"almanak-code-{os_name}-{arch}{ext}"
    return Path(__file__).resolve().parent.parent / "bin" / binary_name


@click.group(invoke_without_command=True)
@click.version_option(__version__)
@click.pass_context
def almanak(ctx):
    """Almanak CLI for managing strategies."""
    if ctx.invoked_subcommand is None:
        binary_path = _resolve_native_binary()
        if not binary_path.exists():
            click.echo(f"Error: no binary for this platform at {binary_path}", err=True)
            sys.exit(1)
        # Ensure the binary is executable (pip may strip permissions from wheel installs)
        if not os.access(binary_path, os.X_OK):
            binary_path.chmod(binary_path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
        result = subprocess.run([str(binary_path)])
        sys.exit(result.returncode)


@almanak.group()
def strat():
    """Commands for managing strategies."""
    pass


@almanak.group()
def copy():
    """Commands for copy-trading validation, replay, and reporting."""
    pass


# Add v2 backtest command group to strat
strat.add_command(framework_backtest_group, name="backtest")

# Note: strat run is registered via @strat.command("run") on strategy_run() below,
# which wraps framework_run_cmd with config discovery, .env loading, and error formatting.

# Add teardown command group to strat
strat.add_command(framework_teardown_group, name="teardown")

# Add demo command to strat
strat.add_command(framework_demo_cmd, name="demo")


def _load_cli_config(path: str) -> dict:
    config_path = Path(path)
    if config_path.suffix.lower() in [".yaml", ".yml"]:
        import yaml

        with open(config_path) as f:
            return yaml.safe_load(f) or {}

    with open(config_path) as f:
        return json.load(f)


@copy.command("validate")
@click.option(
    "--config",
    "-c",
    "config_file",
    type=click.Path(exists=True),
    required=True,
    help="Strategy config or copy_trading config file to validate.",
)
@click.option(
    "--strict",
    is_flag=True,
    default=False,
    help="Fail if copy_trading block is missing from strategy config.",
)
def copy_validate(config_file: str, strict: bool) -> None:
    """Validate copy-trading configuration schema."""
    from almanak.framework.services.copy_trading_models import CopyTradingConfigV2

    config = _load_cli_config(config_file)
    if "copy_trading" in config:
        config = config["copy_trading"]
    elif strict:
        raise click.ClickException("Config file does not contain a copy_trading block")

    parsed = CopyTradingConfigV2.from_config(config)
    summary = {
        "version": parsed.version,
        "leaders": len(parsed.leaders),
        "actions": ",".join(sorted(parsed.action_policies.keys())),
        "copy_mode": parsed.execution_policy.copy_mode,
        "submission_mode": parsed.execution_policy.submission_mode,
        "strict": parsed.execution_policy.strict,
    }
    format_output(status="success", title="Copy config valid", key_value_pairs=summary)


@copy.command("replay")
@click.option(
    "--config",
    "-c",
    "config_file",
    type=click.Path(exists=True),
    required=True,
    help="Strategy config or copy_trading config file.",
)
@click.option(
    "--replay-file",
    "-r",
    type=click.Path(exists=True),
    required=True,
    help="Replay signal fixture file (JSON array or JSONL).",
)
@click.option(
    "--ledger-db",
    type=click.Path(exists=False),
    default="./almanak_copy_ledger.db",
    help="Path to local copy ledger SQLite DB.",
)
@click.option(
    "--shadow/--no-shadow",
    default=True,
    help="Shadow mode performs decisioning/mapping without execution-side accounting updates.",
)
@click.option(
    "--json-output",
    is_flag=True,
    default=False,
    help="Print replay result as JSON.",
)
def copy_replay(config_file: str, replay_file: str, ledger_db: str, shadow: bool, json_output: bool) -> None:
    """Replay copy signals deterministically through policy + intent mapping."""
    from almanak.framework.services.copy_ledger import CopyLedger
    from almanak.framework.services.copy_trading_models import CopyTradingConfigV2
    from almanak.framework.testing.copy_replay import CopyReplayRunner

    config = _load_cli_config(config_file)
    if "copy_trading" in config:
        config = config["copy_trading"]

    parsed = CopyTradingConfigV2.from_config(config)
    ledger = CopyLedger(ledger_db)
    try:
        runner = CopyReplayRunner(config=parsed, ledger=ledger)
        result = runner.run(replay_file, shadow=shadow)
    finally:
        ledger.close()

    if json_output:
        click.echo(json.dumps(result, indent=2, default=str))
        return

    format_output(status="success", title="Copy replay complete", key_value_pairs=result)


@copy.command("report")
@click.option(
    "--ledger-db",
    type=click.Path(exists=True),
    default="./almanak_copy_ledger.db",
    help="Path to local copy ledger SQLite DB.",
)
@click.option(
    "--since-hours",
    type=int,
    default=None,
    help="Optional trailing report window in hours.",
)
@click.option(
    "--json-output",
    is_flag=True,
    default=False,
    help="Print report as JSON.",
)
def copy_report(ledger_db: str, since_hours: int | None, json_output: bool) -> None:
    """Generate copy-trading operational report and go-live gate verdicts."""
    from almanak.framework.services.copy_ledger import CopyLedger
    from almanak.framework.services.copy_reporting import CopyReportGenerator

    ledger = CopyLedger(ledger_db)
    try:
        generator = CopyReportGenerator(ledger)
        report = generator.generate(since_seconds=since_hours * 3600 if since_hours is not None else None)
    finally:
        ledger.close()

    if json_output:
        click.echo(json.dumps(report, indent=2, default=str))
        return

    format_output(
        status="info",
        title="Copy report",
        key_value_pairs={
            "all_gates_pass": report["all_gates_pass"],
            "signals": report["summary"]["signals"],
            "decisions": report["summary"]["decisions"],
            "execution_statuses": report["summary"]["executions"],
        },
    )


almanak.add_command(agent_group, name="agent")


@almanak.group()
def mcp():
    """MCP (Model Context Protocol) server commands."""
    pass


@mcp.command("serve")
@click.option(
    "--gateway-host",
    default="localhost",
    type=str,
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
    "--allowed-tokens",
    multiple=True,
    help="Restrict to specific tokens (can be repeated). Default: all tokens.",
)
@click.option(
    "--allowed-protocols",
    multiple=True,
    help="Restrict to specific protocols (can be repeated). Default: all protocols.",
)
@click.option(
    "--allowed-chains",
    multiple=True,
    default=("arbitrum",),
    help="Restrict to specific chains (can be repeated). Default: arbitrum.",
)
@click.option(
    "--max-single-trade-usd",
    type=click.FloatRange(min=0.0),
    default=10000,
    help="Max single trade size in USD (default: 10000).",
)
@click.option(
    "--max-daily-spend-usd",
    type=click.FloatRange(min=0.0),
    default=50000,
    help="Max daily spend in USD (default: 50000).",
)
@click.option(
    "--schema-only",
    is_flag=True,
    default=False,
    help="Serve tool schemas only (no gateway connection required).",
)
@click.option(
    "--log-level",
    default="warning",
    type=click.Choice(["debug", "info", "warning", "error"]),
    help="Log level (default: warning). Logs go to stderr, never stdout.",
)
def mcp_serve(
    gateway_host,
    gateway_port,
    allowed_tokens,
    allowed_protocols,
    allowed_chains,
    max_single_trade_usd,
    max_daily_spend_usd,
    schema_only,
    log_level,
):
    """Start Almanak as an MCP tool server (stdio transport).

    Serves all Almanak agent tools over the MCP protocol using stdio
    transport. Compatible with Claude Desktop, Cursor, and any MCP client.

    The server reads JSON-RPC messages from stdin and writes responses
    to stdout using content-length framing (LSP-style).

    Examples:

    \b
        # Start with gateway connection (full tool execution)
        almanak mcp serve

    \b
        # Schema-only mode (no gateway needed, tools/list only)
        almanak mcp serve --schema-only

    \b
        # With policy constraints
        almanak mcp serve --max-single-trade-usd 5000 --allowed-chains arbitrum --allowed-chains base

    \b
        # Claude Desktop config (~/.claude/claude_desktop_config.json):
        {
          "mcpServers": {
            "almanak": {
              "command": "almanak",
              "args": ["mcp", "serve"]
            }
          }
        }
    """
    import asyncio
    import logging

    # Configure logging to stderr only (stdout is reserved for MCP protocol)
    log_level_map = {
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "warning": logging.WARNING,
        "error": logging.ERROR,
    }
    logging.basicConfig(
        level=log_level_map.get(log_level, logging.WARNING),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        stream=sys.stderr,
    )
    install_redaction()

    from almanak.framework.agent_tools.adapters.mcp_server import AlmanakMCPStdioServer

    if schema_only:
        # No executor, just serve tool schemas
        server = AlmanakMCPStdioServer(executor=None)
        try:
            asyncio.run(server.run())
        except KeyboardInterrupt:
            pass
        return

    # Connect to gateway for full tool execution
    from decimal import Decimal

    from almanak.framework.agent_tools.executor import ToolExecutor
    from almanak.framework.agent_tools.policy import AgentPolicy
    from almanak.framework.gateway_client import GatewayClient, GatewayClientConfig

    config = GatewayClientConfig.from_env()
    config.host = gateway_host
    config.port = gateway_port
    client = GatewayClient(config)

    try:
        client.connect()
        if not client.wait_for_ready(timeout=10.0):
            click.echo(
                "Error: Cannot connect to gateway at "
                f"{gateway_host}:{gateway_port}. "
                "Start gateway first with: almanak gateway",
                err=True,
            )
            sys.exit(1)

        policy = AgentPolicy(
            max_single_trade_usd=Decimal(str(max_single_trade_usd)),
            max_daily_spend_usd=Decimal(str(max_daily_spend_usd)),
            allowed_chains=set(allowed_chains),
            allowed_tokens=set(allowed_tokens) if allowed_tokens else None,
            allowed_protocols=set(allowed_protocols) if allowed_protocols else None,
        )

        executor = ToolExecutor(
            gateway_client=client,
            policy=policy,
        )

        server = AlmanakMCPStdioServer(executor=executor)
        asyncio.run(server.run())
    except KeyboardInterrupt:
        pass
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)
    finally:
        client.disconnect()


almanak.add_command(mcp, name="mcp")


@almanak.group()
def docs():
    """Access bundled SDK documentation for LLM agents."""
    pass


@docs.command("path")
def docs_path():
    """Print the filesystem path to the bundled llms-full.txt.

    Use this to get a path that CLI agents can grep/read directly.

    Example:

    \b
        almanak docs path
        grep "SwapIntent" $(almanak docs path)
    """
    from almanak.llms import get_path

    try:
        click.echo(get_path())
    except FileNotFoundError as e:
        click.echo(str(e), err=True)
        sys.exit(1)


@docs.command("dump")
@click.option(
    "--output",
    "-o",
    type=click.Path(),
    default=None,
    help="Write to file instead of stdout.",
)
def docs_dump(output):
    """Dump the full llms-full.txt content.

    Prints to stdout by default. Use -o to write to a file.

    Example:

    \b
        almanak docs dump | grep "SwapIntent"
        almanak docs dump -o /tmp/almanak-docs.txt
    """
    from almanak.llms import get_text

    try:
        text = get_text()
    except FileNotFoundError as e:
        click.echo(str(e), err=True)
        sys.exit(1)

    if output:
        Path(output).write_text(text, encoding="utf-8")
        click.echo(f"Written to {output}", err=True)
    else:
        click.echo(text)


@docs.command("agent-skill")
@click.option(
    "--dump",
    is_flag=True,
    default=False,
    help="Dump the full SKILL.md content to stdout.",
)
def docs_agent_skill(dump):
    """Show path to bundled agent skill, or dump content.

    Examples:

    \b
        almanak docs agent-skill          # Print path
        almanak docs agent-skill --dump   # Print content
    """
    from almanak.skills import get_skill_content, get_skill_path

    try:
        if dump:
            click.echo(get_skill_content())
        else:
            click.echo(get_skill_path())
    except FileNotFoundError as e:
        click.echo(str(e), err=True)
        sys.exit(1)


@almanak.command()
@click.option(
    "--port",
    default=50051,
    type=int,
    envvar="GATEWAY_PORT",
    help="gRPC port number (default: 50051).",
)
@click.option(
    "--network",
    type=click.Choice(["mainnet", "anvil"], case_sensitive=False),
    default=None,
    envvar="ALMANAK_GATEWAY_NETWORK",
    help="Network environment: 'mainnet' for production RPC, 'anvil' for local fork.",
)
@click.option(
    "--metrics/--no-metrics",
    default=True,
    envvar="GATEWAY_METRICS_ENABLED",
    help="Enable Prometheus metrics endpoint (default: enabled).",
)
@click.option(
    "--metrics-port",
    default=9090,
    type=int,
    envvar="GATEWAY_METRICS_PORT",
    help="Prometheus metrics port (default: 9090).",
)
@click.option(
    "--log-level",
    default="info",
    type=click.Choice(["debug", "info", "warning", "error"]),
    help="Log level.",
)
@click.option(
    "--chains",
    default=None,
    type=str,
    help="Comma-separated chains to pre-initialize (e.g., 'arbitrum,base').",
)
def gateway(port, network, metrics, metrics_port, log_level, chains):
    """Start the Almanak Gateway gRPC server.

    The gateway is a sidecar service that mediates all external access for
    strategy containers. It provides gRPC services for:

    \b
    - Market data (prices, balances, indicators)
    - State persistence
    - Transaction execution
    - RPC proxy to blockchain nodes
    - External integrations (CoinGecko, TheGraph, etc.)

    The gateway holds all platform secrets (API keys, RPC credentials).
    Strategy containers connect to the gateway and have no direct external access.

    Examples:

    \b
        # Start gateway with defaults
        almanak gateway

    \b
        # Start gateway for Anvil testing
        almanak gateway --network anvil

    \b
        # Start gateway on custom port
        almanak gateway --port 50052
    """
    import asyncio
    import logging

    from almanak.gateway.core.settings import GatewaySettings
    from almanak.gateway.server import serve

    # Configure logging
    log_level_map = {
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "warning": logging.WARNING,
        "error": logging.ERROR,
    }
    logging.basicConfig(
        level=log_level_map.get(log_level, logging.INFO),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # Install centralized secret redaction on all logging channels
    install_redaction()

    # Parse chains list
    parsed_chains = [c.strip().lower() for c in chains.split(",") if c.strip()] if chains else []

    # Build settings
    effective_network = network if network else "mainnet"
    # allow_insecure: auto-enabled for anvil, otherwise respect env var
    env_allow_insecure = os.environ.get("ALMANAK_GATEWAY_ALLOW_INSECURE", "").lower() in ("true", "1", "yes")
    settings = GatewaySettings(
        grpc_port=port,
        metrics_enabled=metrics,
        metrics_port=metrics_port,
        network=effective_network,
        chains=parsed_chains,
        allow_insecure=(effective_network == "anvil") or env_allow_insecure,
    )

    if env_allow_insecure and effective_network not in ("anvil", "sepolia"):
        click.echo(
            click.style(
                f"SECURITY WARNING: ALMANAK_GATEWAY_ALLOW_INSECURE is set on network '{effective_network}'. "
                "Gateway authentication is DISABLED. Remove this env var for production use.",
                fg="red",
                bold=True,
            ),
            err=True,
        )

    format_output(
        status="info",
        title="Starting Almanak Gateway",
        key_value_pairs={
            "gRPC Port": port,
            "Network": settings.network,
            "Chains": ", ".join(parsed_chains) if parsed_chains else "(on-demand)",
            "Metrics": "enabled" if metrics else "disabled",
            "Metrics Port": metrics_port if metrics else "N/A",
            "Log Level": log_level,
        },
    )

    click.echo()
    click.echo("Press Ctrl+C to stop the gateway.")
    click.echo()

    try:
        asyncio.run(serve(settings))
    except KeyboardInterrupt:
        click.echo()
        click.echo("Gateway stopped.")


@almanak.command()
@click.option(
    "--port",
    default=8501,
    type=int,
    envvar="DASHBOARD_PORT",
    help="Streamlit port number (default: 8501).",
)
@click.option(
    "--gateway-host",
    default="localhost",
    type=str,
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
    "--no-browser",
    is_flag=True,
    default=False,
    help="Don't open browser automatically.",
)
def dashboard(port, gateway_host, gateway_port, no_browser):
    """Start the Almanak Operator Dashboard.

    The dashboard provides a web UI for monitoring and managing strategies.
    It connects to the gateway for all data access.

    IMPORTANT: The gateway must be running before starting the dashboard.
    Start the gateway first with: almanak gateway

    Examples:

    \b
        # Start dashboard (gateway must be running)
        almanak gateway &  # Terminal 1
        almanak dashboard  # Terminal 2

    \b
        # Start dashboard on custom port
        almanak dashboard --port 8502

    \b
        # Connect to remote gateway
        almanak dashboard --gateway-host 192.168.1.100 --gateway-port 50051
    """
    import subprocess

    from almanak.framework.gateway_client import GatewayClient, GatewayClientConfig

    # Check gateway connectivity first
    config = GatewayClientConfig(host=gateway_host, port=gateway_port)
    client = GatewayClient(config)

    format_output(
        status="info",
        title="Checking Gateway Connection",
        key_value_pairs={
            "Gateway": f"{gateway_host}:{gateway_port}",
        },
    )

    try:
        client.connect()
        if not client.wait_for_ready(timeout=5.0):
            format_output(
                status="error",
                title="Gateway Not Available",
                key_value_pairs={
                    "Error": "Cannot connect to gateway",
                    "Solution": "Start gateway first with: almanak gateway",
                },
            )
            sys.exit(1)
        client.disconnect()
    except Exception as e:
        format_output(
            status="error",
            title="Gateway Connection Failed",
            key_value_pairs={
                "Error": str(e),
                "Solution": "Start gateway first with: almanak gateway",
            },
        )
        sys.exit(1)

    format_output(
        status="success",
        title="Starting Almanak Dashboard",
        key_value_pairs={
            "Dashboard Port": port,
            "Gateway": f"{gateway_host}:{gateway_port}",
            "URL": f"http://localhost:{port}",
        },
    )

    # Set environment variables for the dashboard process
    env = os.environ.copy()
    env["GATEWAY_HOST"] = gateway_host
    env["GATEWAY_PORT"] = str(gateway_port)

    # Get path to dashboard app
    from almanak.framework.dashboard import app as dashboard_app

    app_path = dashboard_app.__file__

    # Build streamlit command
    cmd = [
        sys.executable,
        "-m",
        "streamlit",
        "run",
        app_path,
        "--server.port",
        str(port),
        "--server.headless",
        "true" if no_browser else "false",
        "--browser.gatherUsageStats",
        "false",
    ]

    click.echo()
    click.echo("Press Ctrl+C to stop the dashboard.")
    click.echo()

    try:
        subprocess.run(cmd, env=env, check=True)
    except KeyboardInterrupt:
        click.echo()
        click.echo("Dashboard stopped.")
    except subprocess.CalledProcessError as e:
        format_output(
            status="error",
            title="Dashboard Failed",
            key_value_pairs={"Error": str(e)},
        )
        sys.exit(1)


@strat.command()
@click.option(
    "--name",
    "-n",
    type=str,
    default=None,
    help="Name for the new strategy. If not provided, will prompt interactively.",
)
@click.option(
    "--working-dir",
    "-o",
    type=click.Path(exists=False),
    default=None,
    help="Output directory for the new strategy. Defaults to current directory.",
)
@click.option(
    "--template",
    "-t",
    type=click.Choice(
        ["blank", "dynamic_lp", "mean_reversion", "bollinger", "basis_trade", "lending_loop", "copy_trader"]
    ),
    default="blank",
    help="Strategy template to use (default: blank)",
)
@click.option(
    "--chain",
    "-c",
    type=click.Choice(["ethereum", "arbitrum", "optimism", "polygon", "base", "avalanche"]),
    default="arbitrum",
    help="Target blockchain network (default: arbitrum)",
)
@click.pass_context
def new(ctx, name, working_dir, template, chain):
    """Create a new v2 IntentStrategy from template.

    This creates a strategy using the v2 intent-based framework with:
    - strategy.py: Main strategy with decide() method
    - config.py: Configuration dataclass
    - tests/: Test scaffolding

    Templates:
    - blank: Minimal strategy for custom implementations
    - dynamic_lp: Volatility-based LP strategy
    - mean_reversion: RSI-based trading strategy
    - basis_trade: Spot+perp funding arbitrage
    - lending_loop: Aave/Morpho leverage looping
    - copy_trader: Copy trading from leader wallets
    """
    try:
        # Use provided name or prompt interactively
        strategy_name = name if name else click.prompt("Enter a name for your strategy")

        # Invoke the v2 framework's new-strategy command
        ctx.invoke(
            framework_new_strategy_cmd,
            name=strategy_name,
            template=template,
            chain=chain,
            output_dir=working_dir,
        )

    except click.Abort:
        sys.exit(1)
    except Exception as e:
        format_output(
            status="error",
            title="Failed to create a new strategy",
            key_value_pairs={"Error": str(e)},
        )
        sys.exit(1)


@strat.command("run")
@click.option(
    "--working-dir",
    "-d",
    type=click.Path(exists=True),
    default=".",
    help="Working directory containing the strategy files. Defaults to the current directory.",
)
@click.option(
    "--id",
    type=str,
    default=None,
    help="Strategy instance ID to resume a previous run.",
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
    "--once",
    is_flag=True,
    default=False,
    help="Run single iteration then exit.",
)
@click.option(
    "--interval",
    "-i",
    type=int,
    default=60,
    help="Loop interval in seconds (default: 60).",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Execute decide() but don't submit transactions.",
)
@click.option(
    "--fresh",
    is_flag=True,
    default=False,
    help="Clear strategy state before running (useful for fresh Anvil forks).",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    default=False,
    help="Enable verbose output.",
)
@click.option(
    "--network",
    "-n",
    type=click.Choice(["mainnet", "anvil"], case_sensitive=False),
    default=None,
    help="Network environment: 'mainnet' for production RPC, 'anvil' for local fork testing. "
    "For paper trading with PnL tracking, use 'almanak strat backtest paper'.",
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
    "--copy-mode",
    type=click.Choice(["live", "shadow", "replay"], case_sensitive=False),
    default=None,
    help="Copy-trading mode override for this run.",
)
@click.option(
    "--copy-shadow",
    is_flag=True,
    default=False,
    help="Enable copy-trading shadow mode (decisioning only, no submissions).",
)
@click.option(
    "--copy-replay-file",
    type=click.Path(exists=False),
    default=None,
    help="Replay file (JSON/JSONL CopySignal fixtures) for copy-trading replay mode.",
)
@click.option(
    "--copy-strict",
    is_flag=True,
    default=False,
    help="Enable strict copy-trading validation and fail-closed behavior.",
)
@click.option(
    "--dashboard",
    is_flag=True,
    default=False,
    help="Launch live dashboard alongside strategy execution.",
)
@click.option(
    "--dashboard-port",
    type=int,
    default=8501,
    help="Port to run the dashboard on (default: 8501).",
)
@click.option(
    "--wallet",
    type=click.Choice(["default", "isolated"], case_sensitive=False),
    default="default",
    help="Wallet mode for Anvil: 'isolated' derives a unique wallet per strategy for balance isolation.",
)
@click.option(
    "--log-file",
    type=click.Path(),
    default=None,
    help="Write JSON logs to this file (in addition to console output). Useful for AI agent analysis.",
)
@click.option(
    "--reset-fork",
    "reset_fork",
    is_flag=True,
    default=False,
    help="Reset Anvil fork to latest mainnet block before each iteration (requires --network anvil).",
)
@click.option(
    "--max-iterations",
    type=int,
    default=None,
    help="Maximum number of iterations to run before exiting cleanly. "
    "Without this flag, continuous mode runs indefinitely.",
)
@click.option(
    "--teardown-after",
    is_flag=True,
    default=False,
    help="After --once iteration, automatically teardown (close all positions). "
    "Useful for CI/testing to avoid accumulating stale positions on-chain.",
)
@click.pass_context
def strategy_run(
    ctx,
    working_dir,
    config_file,
    once,
    interval,
    dry_run,
    fresh,
    verbose,
    network,
    id,
    gateway_host,
    gateway_port,
    no_gateway,
    copy_mode,
    copy_shadow,
    copy_replay_file,
    copy_strict,
    dashboard,
    dashboard_port,
    wallet,
    log_file,
    reset_fork,
    max_iterations,
    teardown_after,
):
    """Run a strategy from its working directory.

    By default, a managed gateway is auto-started in the background.
    Use --no-gateway to connect to an existing gateway instead.

    Prerequisites:
        - Environment variables: ALMANAK_PRIVATE_KEY, RPC_URL (or ALCHEMY_API_KEY)
        - For anvil mode: Foundry installed (Anvil is auto-started by managed gateway)

    Examples:

        # Run from strategy directory (auto-starts gateway)
        cd strategies/demo/uniswap_rsi
        almanak strat run --once

        # Run with explicit working directory
        almanak strat run -d strategies/demo/uniswap_rsi --once

        # Connect to an existing gateway
        almanak strat run --no-gateway --once

        # Run continuously
        almanak strat run --interval 30

        # Dry run (no transactions)
        almanak strat run --dry-run --once

        # Resume a previous run
        almanak strat run --id abc123 --once

        # Fresh start (clear stale state, useful for Anvil forks)
        almanak strat run --fresh --once

        # Run with live dashboard
        almanak strat run -d strategies/demo/uniswap_lp --network anvil --dashboard
    """
    # Look for config.json or config.yaml in working directory if not specified
    if config_file is None:
        potential_configs = [
            Path(working_dir) / "config.json",
            Path(working_dir) / "config.yaml",
            Path(working_dir) / "config.yml",
        ]
        for potential_config in potential_configs:
            if potential_config.exists():
                config_file = str(potential_config)
                click.echo(f"Using config: {config_file}")
                break

    # Load environment from .env if present
    env_file = Path(working_dir) / ".env"
    if env_file.exists():
        load_dotenv(env_file)
        click.echo(f"Loaded environment from: {env_file}")

    # Install secret redaction after env is loaded so all secrets are registered.
    # (The managed gateway also calls install_redaction(), but strat run may log
    # before the gateway starts, so we install here too.)
    install_redaction()

    # Invoke the v2 framework's run command
    try:
        ctx.invoke(
            framework_run_cmd,
            config_file=config_file,
            once=once,
            interval=interval,
            dry_run=dry_run,
            fresh=fresh,
            list_all=False,
            verbose=verbose,
            debug=False,
            dashboard=dashboard,
            dashboard_port=dashboard_port,
            simulate_tx=None,
            network=network,
            gateway_host=gateway_host,
            gateway_port=gateway_port,
            no_gateway=no_gateway,
            copy_mode=copy_mode,
            copy_shadow=copy_shadow,
            copy_replay_file=copy_replay_file,
            copy_strict=copy_strict,
            wallet=wallet,
            log_file=log_file,
            reset_fork=reset_fork,
            max_iterations=max_iterations,
            teardown_after=teardown_after,
            working_dir=working_dir,
            strategy_id_override=id,
        )
    except click.Abort:
        sys.exit(1)
    except Exception as e:
        format_output(
            status="error",
            title="Strategy run failed",
            key_value_pairs={"Error": str(e)},
        )
        sys.exit(1)


def is_anvil_running(url="http://127.0.0.1:8545"):
    payload = {"jsonrpc": "2.0", "method": "web3_clientVersion", "params": [], "id": 1}
    headers = {"Content-Type": "application/json"}

    try:
        response = requests.post(url, headers=headers, data=json.dumps(payload))
        if response.status_code == 200:
            result = response.json()
            if "result" in result:
                print(f"Anvil is running. Client version: {result['result']}")
                return True
        print(f"Failed to validate Anvil. Status code: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"Error connecting to Anvil: {e}")

    return False


if __name__ == "__main__":
    almanak()
