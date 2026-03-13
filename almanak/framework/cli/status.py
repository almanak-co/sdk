"""CLI commands for strategy monitoring via gateway.

Provides `strat list`, `strat status`, and `strat logs` commands that query
the gateway's DashboardService for strategy state. No local file dependencies
— works identically on a laptop and GCP.

Usage:
    almanak strat list
    almanak strat list --chain arbitrum --status RUNNING
    almanak strat status -s my_strategy
    almanak strat status -s my_strategy --json
    almanak strat logs -s my_strategy
    almanak strat logs -s my_strategy --type TRADE --limit 20
"""

import json
import sys
from datetime import UTC, datetime
from typing import Any

import click

from ..gateway_client import GatewayClient, GatewayClientConfig


def _make_client(gateway_host: str, gateway_port: int) -> GatewayClient:
    """Create and connect a gateway client, exiting on failure."""
    effective_host = "127.0.0.1" if gateway_host == "localhost" else gateway_host
    config = GatewayClientConfig(host=effective_host, port=gateway_port)
    client = GatewayClient(config)
    try:
        client.connect()
        if not client.health_check():
            client.disconnect()
            click.secho(
                f"Cannot connect to gateway at {effective_host}:{gateway_port}. Start it with: almanak gateway",
                fg="red",
                err=True,
            )
            sys.exit(1)
    except Exception:
        try:
            client.disconnect()
        except Exception:
            pass
        click.secho(
            f"Cannot connect to gateway at {effective_host}:{gateway_port}. Start it with: almanak gateway",
            fg="red",
            err=True,
        )
        sys.exit(1)
    return client


def _format_timestamp(epoch_seconds: int) -> str:
    """Format epoch timestamp to human-readable string."""
    if not epoch_seconds:
        return "-"
    dt = datetime.fromtimestamp(epoch_seconds, tz=UTC)
    return dt.strftime("%Y-%m-%d %H:%M:%S UTC")


def _format_relative_time(epoch_seconds: int) -> str:
    """Format epoch timestamp as relative time (e.g., '5m ago')."""
    if not epoch_seconds:
        return "-"
    now = datetime.now(tz=UTC)
    dt = datetime.fromtimestamp(epoch_seconds, tz=UTC)
    delta = now - dt
    total_seconds = int(delta.total_seconds())
    if total_seconds < 0:
        return "just now"
    if total_seconds < 60:
        return f"{total_seconds}s ago"
    if total_seconds < 3600:
        return f"{total_seconds // 60}m ago"
    if total_seconds < 86400:
        return f"{total_seconds // 3600}h ago"
    return f"{total_seconds // 86400}d ago"


def _status_color(status: str) -> str:
    """Return colored status string."""
    colors = {
        "RUNNING": "green",
        "PAUSED": "yellow",
        "ERROR": "red",
        "STUCK": "red",
        "STALE": "yellow",
        "INACTIVE": "white",
        "ARCHIVED": "bright_black",
    }
    return click.style(status, fg=colors.get(status.upper(), "white"))


# Shared gateway options
_gateway_options = [
    click.option(
        "--gateway-host",
        default="localhost",
        envvar="GATEWAY_HOST",
        help="Gateway hostname (default: localhost).",
    ),
    click.option(
        "--gateway-port",
        default=50051,
        type=int,
        envvar="GATEWAY_PORT",
        help="Gateway gRPC port (default: 50051).",
    ),
]


def _add_gateway_options(func):
    """Apply shared gateway options to a click command."""
    for option in reversed(_gateway_options):
        func = option(func)
    return func


# =============================================================================
# strat list
# =============================================================================


@click.command("list")
@click.option(
    "--status",
    "-s",
    "status_filter",
    default=None,
    type=click.Choice(
        ["RUNNING", "PAUSED", "ERROR", "STUCK", "STALE", "INACTIVE", "ARCHIVED"],
        case_sensitive=False,
    ),
    help="Filter by status.",
)
@click.option("--chain", "-c", default=None, help="Filter by chain.")
@click.option("--json", "-j", "as_json", is_flag=True, help="Output as JSON.")
@_add_gateway_options
def list_strategies(status_filter, chain, as_json, gateway_host, gateway_port):
    """List all strategies registered with the gateway.

    Shows a summary table of all strategies with their status, chain,
    value, PnL, and last activity. Requires a running gateway.

    Examples:

    \b
        almanak strat list
        almanak strat list --status RUNNING
        almanak strat list --chain arbitrum --json
        almanak strat list --gateway-host 192.168.1.100
    """
    from almanak.gateway.proto import gateway_pb2

    client = _make_client(gateway_host, gateway_port)
    try:
        request = gateway_pb2.ListStrategiesRequest(
            status_filter=status_filter.upper() if status_filter else "REGISTRY",
            chain_filter=chain or "",
            include_position=False,
        )
        response = client.dashboard.ListStrategies(request)
    except Exception as e:
        click.secho(f"Failed to list strategies: {e}", fg="red", err=True)
        sys.exit(1)
    finally:
        client.disconnect()

    strategies = list(response.strategies)

    if not strategies:
        if as_json:
            click.echo("[]")
        else:
            click.echo("No strategies found.")
        return

    if as_json:
        rows = []
        for s in strategies:
            rows.append(
                {
                    "strategy_id": s.strategy_id,
                    "name": s.name,
                    "status": s.status,
                    "chain": s.chain,
                    "chains": list(s.chains) if s.is_multi_chain else [s.chain],
                    "protocol": s.protocol,
                    "total_value_usd": s.total_value_usd,
                    "pnl_24h_usd": s.pnl_24h_usd,
                    "last_action_at": s.last_action_at,
                    "attention_required": s.attention_required,
                    "attention_reason": s.attention_reason,
                }
            )
        click.echo(json.dumps(rows, indent=2))
        return

    # Table output
    click.echo()
    click.echo(click.style(f"Strategies ({len(strategies)})", bold=True, fg="cyan"))
    click.echo()

    # Column widths
    id_w = max(12, max((len(s.strategy_id) for s in strategies), default=12))
    id_w = min(id_w, 35)  # cap width

    header = f"{'ID':<{id_w}}  {'STATUS':<10}  {'CHAIN':<12}  {'VALUE (USD)':>12}  {'PnL 24h':>10}  {'LAST ACTIVE':<14}"
    click.echo(header)
    click.echo("-" * len(header))

    for s in strategies:
        sid = s.strategy_id[:id_w] if len(s.strategy_id) > id_w else s.strategy_id
        value = s.total_value_usd if s.total_value_usd is not None else "-"
        pnl = s.pnl_24h_usd if s.pnl_24h_usd is not None else "-"
        chain_display = ",".join(s.chains) if s.is_multi_chain else (s.chain or "-")

        line = (
            f"{sid:<{id_w}}  {_status_color(s.status):<21}  {chain_display:<12}  "
            f"{value:>12}  {pnl:>10}  {_format_relative_time(s.last_action_at):<14}"
        )
        click.echo(line)

        if s.attention_required and s.attention_reason:
            click.echo(f"  {click.style('!', fg='yellow', bold=True)} {s.attention_reason}")

    click.echo()
    click.echo(f"Total: {len(strategies)} strategies")


# =============================================================================
# strat status
# =============================================================================


@click.command("status")
@click.option(
    "--strategy-id",
    "-s",
    required=True,
    help="Strategy instance ID.",
)
@click.option(
    "--timeline/--no-timeline",
    default=True,
    help="Include recent timeline events (default: yes).",
)
@click.option(
    "--timeline-limit",
    default=10,
    type=int,
    help="Number of timeline events to show (default: 10).",
)
@click.option("--json", "-j", "as_json", is_flag=True, help="Output as JSON.")
@_add_gateway_options
def strategy_status(strategy_id, timeline, timeline_limit, as_json, gateway_host, gateway_port):
    """Get detailed status of a strategy.

    Shows strategy summary, position details, chain health, and recent
    timeline events. Requires a running gateway.

    Examples:

    \b
        almanak strat status -s my_strategy
        almanak strat status -s my_strategy --json
        almanak strat status -s my_strategy --no-timeline
        almanak strat status -s uniswap_lp:abc123 --timeline-limit 20
    """
    from almanak.gateway.proto import gateway_pb2

    client = _make_client(gateway_host, gateway_port)
    try:
        request = gateway_pb2.GetStrategyDetailsRequest(
            strategy_id=strategy_id,
            include_timeline=timeline,
            include_pnl_history=False,
            timeline_limit=timeline_limit,
        )
        details = client.dashboard.GetStrategyDetails(request)
    except Exception as e:
        click.secho(f"Failed to get strategy details: {e}", fg="red", err=True)
        sys.exit(1)
    finally:
        client.disconnect()

    s = details.summary

    if as_json:
        result = {
            "strategy_id": s.strategy_id,
            "name": s.name,
            "status": s.status,
            "chain": s.chain,
            "protocol": s.protocol,
            "total_value_usd": s.total_value_usd,
            "pnl_24h_usd": s.pnl_24h_usd,
            "last_action_at": s.last_action_at,
            "attention_required": s.attention_required,
            "attention_reason": s.attention_reason,
        }
        if details.position:
            pos_data: dict[str, Any] = {}
            if details.position.token_balances:
                pos_data["token_balances"] = [
                    {"symbol": t.symbol, "balance": t.balance, "value_usd": t.value_usd}
                    for t in details.position.token_balances
                ]
            if details.position.lp_positions:
                pos_data["lp_positions"] = [
                    {
                        "pool": lp.pool,
                        "token0": lp.token0,
                        "token1": lp.token1,
                        "liquidity_usd": lp.liquidity_usd,
                    }
                    for lp in details.position.lp_positions
                ]
            if details.position.health_factor is not None:
                pos_data["health_factor"] = float(details.position.health_factor)
            if pos_data:
                result["position"] = pos_data
        if details.timeline:
            result["timeline"] = [
                {
                    "timestamp": e.timestamp,
                    "event_type": e.event_type,
                    "description": e.description,
                    "tx_hash": e.tx_hash,
                    "chain": e.chain,
                }
                for e in details.timeline
            ]
        if details.operator_card and details.operator_card.strategy_id:
            result["operator_card"] = {
                "severity": details.operator_card.severity,
                "reason": details.operator_card.reason,
                "risk_description": details.operator_card.risk_description,
                "suggested_actions": list(details.operator_card.suggested_actions),
            }
        click.echo(json.dumps(result, indent=2))
        return

    # Pretty print
    click.echo()
    click.echo(click.style(f"Strategy: {s.name or s.strategy_id}", bold=True, fg="cyan"))
    click.echo()

    click.echo(f"  ID:          {s.strategy_id}")
    click.echo(f"  Status:      {_status_color(s.status)}")
    click.echo(f"  Chain:       {','.join(s.chains) if s.is_multi_chain else s.chain}")
    click.echo(f"  Protocol:    {s.protocol or '-'}")
    click.echo(f"  Value:       ${s.total_value_usd}" if s.total_value_usd is not None else "  Value:       -")
    click.echo(f"  PnL (24h):   ${s.pnl_24h_usd}" if s.pnl_24h_usd is not None else "  PnL (24h):   -")
    click.echo(f"  Last Active: {_format_timestamp(s.last_action_at)}")

    if s.attention_required:
        click.echo()
        click.echo(click.style(f"  ! {s.attention_reason}", fg="yellow", bold=True))

    # Operator card
    if details.operator_card and details.operator_card.strategy_id:
        oc = details.operator_card
        click.echo()
        severity_colors = {"LOW": "white", "MEDIUM": "yellow", "HIGH": "red", "CRITICAL": "red"}
        click.echo(
            click.style(
                f"  Operator Alert [{oc.severity}]: {oc.reason}",
                fg=severity_colors.get(oc.severity, "white"),
                bold=oc.severity in ("HIGH", "CRITICAL"),
            )
        )
        if oc.risk_description:
            click.echo(f"    Risk: {oc.risk_description}")
        if oc.suggested_actions:
            click.echo("    Suggested:")
            for action in oc.suggested_actions:
                click.echo(f"      - {action}")

    # Position
    pos = details.position
    if pos and (pos.token_balances or pos.lp_positions or pos.health_factor is not None):
        click.echo()
        click.echo(click.style("  Position:", bold=True))
        if pos.token_balances:
            for t in pos.token_balances:
                val = f" (${t.value_usd})" if t.value_usd is not None else ""
                click.echo(f"    {t.symbol}: {t.balance}{val}")
        if pos.lp_positions:
            for lp in pos.lp_positions:
                click.echo(f"    LP: {lp.pool} ({lp.token0}/{lp.token1}) ${lp.liquidity_usd}")
        if pos.health_factor is not None:
            click.echo(f"    Health Factor: {pos.health_factor}")

    # Chain health
    if details.chain_health:
        click.echo()
        click.echo(click.style("  Chain Health:", bold=True))
        for chain_name, health in details.chain_health.items():
            status_color = {"HEALTHY": "green", "DEGRADED": "yellow", "UNAVAILABLE": "red"}
            click.echo(
                f"    {chain_name}: "
                f"{click.style(health.status, fg=status_color.get(health.status, 'white'))} "
                f"(RPC: {health.rpc_latency_ms}ms, gas: {health.gas_price_gwei} gwei)"
            )

    # Timeline
    if timeline and details.timeline:
        click.echo()
        click.echo(click.style("  Recent Events:", bold=True))
        for evt in details.timeline:
            ts = _format_relative_time(evt.timestamp)
            type_colors = {"TRADE": "green", "REBALANCE": "cyan", "ERROR": "red", "STATE_CHANGE": "yellow"}
            etype = click.style(evt.event_type, fg=type_colors.get(evt.event_type, "white"))
            line = f"    {ts:<12} {etype:<20} {evt.description}"
            if evt.tx_hash:
                line += f"  tx:{evt.tx_hash[:10]}..."
            click.echo(line)

    click.echo()


# =============================================================================
# strat logs
# =============================================================================


@click.command("logs")
@click.option(
    "--strategy-id",
    "-s",
    required=True,
    help="Strategy instance ID.",
)
@click.option(
    "--limit",
    "-n",
    default=50,
    type=int,
    help="Number of events to show (default: 50).",
)
@click.option(
    "--type",
    "-t",
    "event_type",
    default=None,
    help="Filter by event type (e.g., TRADE, ERROR, REBALANCE, STATE_CHANGE).",
)
@click.option(
    "--since",
    default=None,
    help="Show events since timestamp (ISO 8601 or epoch seconds).",
)
@click.option("--json", "-j", "as_json", is_flag=True, help="Output as JSON.")
@_add_gateway_options
def strategy_logs(strategy_id, limit, event_type, since, as_json, gateway_host, gateway_port):
    """Show timeline events for a strategy.

    Displays the event log (trades, errors, state changes) from the
    gateway's timeline store. Requires a running gateway.

    Examples:

    \b
        almanak strat logs -s my_strategy
        almanak strat logs -s my_strategy --type TRADE --limit 20
        almanak strat logs -s my_strategy --since 2026-03-01T00:00:00Z
        almanak strat logs -s my_strategy --json
    """
    from almanak.gateway.proto import gateway_pb2

    # Parse --since
    since_ts = 0
    if since:
        try:
            since_ts = int(since)
        except ValueError:
            try:
                dt = datetime.fromisoformat(since.replace("Z", "+00:00"))
                since_ts = int(dt.timestamp())
            except ValueError:
                click.secho(f"Invalid --since value: {since}. Use ISO 8601 or epoch seconds.", fg="red", err=True)
                sys.exit(1)

    client = _make_client(gateway_host, gateway_port)
    try:
        request = gateway_pb2.GetTimelineRequest(
            strategy_id=strategy_id,
            limit=limit,
            event_type_filter=event_type or "",
            since_timestamp=since_ts,
        )
        response = client.dashboard.GetTimeline(request)
    except Exception as e:
        click.secho(f"Failed to get timeline: {e}", fg="red", err=True)
        sys.exit(1)
    finally:
        client.disconnect()

    events = list(response.events)

    if not events:
        if as_json:
            click.echo("[]")
        else:
            click.echo(f"No events found for strategy: {strategy_id}")
        return

    if as_json:
        rows = []
        for evt in events:
            row = {
                "timestamp": evt.timestamp,
                "time": _format_timestamp(evt.timestamp),
                "event_type": evt.event_type,
                "description": evt.description,
                "chain": evt.chain,
            }
            if evt.tx_hash:
                row["tx_hash"] = evt.tx_hash
            if evt.details_json:
                try:
                    row["details"] = json.loads(evt.details_json)
                except json.JSONDecodeError:
                    row["details_raw"] = evt.details_json
            rows.append(row)
        click.echo(json.dumps(rows, indent=2))
        return

    # Pretty output
    click.echo()
    click.echo(click.style(f"Timeline: {strategy_id}", bold=True, fg="cyan"))
    if event_type:
        click.echo(f"  Filter: {event_type}")
    click.echo()

    type_colors = {
        "TRADE": "green",
        "REBALANCE": "cyan",
        "ERROR": "red",
        "STATE_CHANGE": "yellow",
    }

    for evt in events:
        ts = _format_timestamp(evt.timestamp)
        etype = click.style(
            f"[{evt.event_type}]",
            fg=type_colors.get(evt.event_type, "white"),
        )
        click.echo(f"  {ts}  {etype}  {evt.description}")
        if evt.tx_hash:
            click.echo(f"    tx: {evt.tx_hash}")
        if evt.chain:
            click.echo(f"    chain: {evt.chain}")
        if evt.details_json:
            try:
                details = json.loads(evt.details_json)
                if isinstance(details, dict):
                    for k, v in details.items():
                        click.echo(f"    {k}: {v}")
                else:
                    click.echo(f"    {json.dumps(details, indent=2)}")
            except json.JSONDecodeError:
                pass
        click.echo()

    shown = len(events)
    more = " (more available)" if response.has_more else ""
    click.echo(f"Showing {shown} events{more}")
