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

import click

from almanak.config.cli_options import gateway_client_options

from ..gateway_client import GatewayClient, GatewayClientConfig
from .status_helpers import (
    _fetch_strategy_details,
    _render_details_as_json,
    _render_details_pretty,
    _validate_status_args,
)


def _make_client(gateway_host: str, gateway_port: int) -> GatewayClient:
    """Create and connect a gateway client, exiting on failure."""
    effective_host = "127.0.0.1" if gateway_host == "localhost" else gateway_host
    try:
        config = GatewayClientConfig.from_env()
    except (ValueError, TypeError) as e:
        click.secho(f"Invalid gateway configuration: {e}", fg="red", err=True)
        sys.exit(1)
    config.host = effective_host
    config.port = gateway_port
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


_STATUS_COLORS = {
    "RUNNING": "green",
    "PAUSED": "yellow",
    "ERROR": "red",
    "STUCK": "red",
    "STALE": "yellow",
    "INACTIVE": "white",
    "ARCHIVED": "bright_black",
}


def _status_color(status: str) -> str:
    """Return colored status string."""
    return click.style(status, fg=_STATUS_COLORS.get(status.upper(), "white"))


# =============================================================================
# strat list
# =============================================================================


# crap-allowlist: VIB-4722 mechanical deployment_id rename in existing high-CRAP function.
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
@gateway_client_options
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
                    "deployment_id": s.deployment_id,
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
                    "consecutive_errors": s.consecutive_errors,
                    "last_iteration_at": s.last_iteration_at,
                    "pnl_since_deploy_usd": s.pnl_since_deploy_usd or None,
                }
            )
        click.echo(json.dumps(rows, indent=2))
        return

    # Table output
    click.echo()
    click.echo(click.style(f"Strategies ({len(strategies)})", bold=True, fg="cyan"))
    click.echo()

    # Column widths
    id_w = max(12, max((len(s.deployment_id) for s in strategies), default=12))
    id_w = min(id_w, 35)  # cap width

    header = f"{'ID':<{id_w}}  {'STATUS':<10}  {'CHAIN':<12}  {'VALUE (USD)':>12}  {'PnL 24h':>10}  {'LAST ACTIVE':<14}"
    click.echo(header)
    click.echo("-" * len(header))

    for s in strategies:
        sid = s.deployment_id[:id_w] if len(s.deployment_id) > id_w else s.deployment_id
        value = s.total_value_usd if s.total_value_usd is not None else "-"
        pnl = s.pnl_24h_usd if s.pnl_24h_usd is not None else "-"
        chain_display = ",".join(s.chains) if s.is_multi_chain else (s.chain or "-")

        colored_status = click.style(f"{s.status:<10}", fg=_STATUS_COLORS.get(s.status.upper(), "white"))
        line = (
            f"{sid:<{id_w}}  {colored_status}  {chain_display:<12}  "
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
    "--deployment-id",
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
@gateway_client_options
def strategy_status(deployment_id, timeline, timeline_limit, as_json, gateway_host, gateway_port):
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
    _validate_status_args(timeline_limit)

    client = _make_client(gateway_host, gateway_port)
    details = _fetch_strategy_details(
        client,
        deployment_id,
        include_timeline=timeline,
        timeline_limit=timeline_limit,
    )

    if as_json:
        click.echo(_render_details_as_json(details))
        return

    _render_details_pretty(details, timeline_enabled=timeline)


# =============================================================================
# strat logs
# =============================================================================


_TIMELINE_TYPE_COLORS = {
    "TRADE": "green",
    "REBALANCE": "cyan",
    "ERROR": "red",
    "STATE_CHANGE": "yellow",
}


def _validate_logs_limit(limit: int) -> None:
    """Exit 1 when --limit is < 1, mirroring the original inline check."""
    if limit < 1:
        click.secho("--limit must be >= 1.", fg="red", err=True)
        sys.exit(1)


def _parse_since_value(since: str | None) -> int:
    """Parse --since as epoch seconds or ISO 8601, exiting 1 on bad input."""
    if not since:
        return 0
    try:
        return int(since)
    except ValueError:
        pass
    try:
        dt = datetime.fromisoformat(since.replace("Z", "+00:00"))
        return int(dt.timestamp())
    except ValueError:
        click.secho(f"Invalid --since value: {since}. Use ISO 8601 or epoch seconds.", fg="red", err=True)
        sys.exit(1)


def _fetch_strategy_timeline(client, deployment_id: str, limit: int, event_type: str | None, since_ts: int):
    """Fetch the timeline RPC, exit 1 on RPC error, disconnect in finally."""
    from almanak.gateway.proto import gateway_pb2

    try:
        request = gateway_pb2.GetTimelineRequest(
            deployment_id=deployment_id,
            limit=limit,
            event_type_filter=event_type.upper() if event_type else "",
            since_timestamp=since_ts,
        )
        return client.dashboard.GetTimeline(request)
    except Exception as e:
        click.secho(f"Failed to get timeline: {e}", fg="red", err=True)
        sys.exit(1)
    finally:
        client.disconnect()


def _build_log_json_row(evt) -> dict:
    """Build a single JSON row for a timeline event."""
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
    return row


def _render_logs_as_json(events) -> str:
    """Serialize events to the JSON string emitted by `strat logs --json`."""
    return json.dumps([_build_log_json_row(evt) for evt in events], indent=2)


def _print_log_event_details(details_json: str) -> None:
    """Emit the per-event details body, silently ignoring invalid JSON."""
    try:
        details = json.loads(details_json)
    except json.JSONDecodeError:
        return
    if isinstance(details, dict):
        for k, v in details.items():
            click.echo(f"    {k}: {v}")
    else:
        click.echo(f"    {json.dumps(details, indent=2)}")


def _print_log_event(evt) -> None:
    """Emit one timeline event in the pretty (human) layout."""
    ts = _format_timestamp(evt.timestamp)
    etype = click.style(f"[{evt.event_type}]", fg=_TIMELINE_TYPE_COLORS.get(evt.event_type, "white"))
    click.echo(f"  {ts}  {etype}  {evt.description}")
    if evt.tx_hash:
        click.echo(f"    tx: {evt.tx_hash}")
    if evt.chain:
        click.echo(f"    chain: {evt.chain}")
    if evt.details_json:
        _print_log_event_details(evt.details_json)
    click.echo()


def _render_logs_pretty(deployment_id: str, event_type: str | None, events, has_more: bool) -> None:
    """Emit the full pretty (human) timeline output."""
    click.echo()
    click.echo(click.style(f"Timeline: {deployment_id}", bold=True, fg="cyan"))
    if event_type:
        click.echo(f"  Filter: {event_type}")
    click.echo()

    for evt in events:
        _print_log_event(evt)

    more = " (more available)" if has_more else ""
    click.echo(f"Showing {len(events)} events{more}")


@click.command("logs")
@click.option(
    "--deployment-id",
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
@gateway_client_options
def strategy_logs(deployment_id, limit, event_type, since, as_json, gateway_host, gateway_port):
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
    _validate_logs_limit(limit)
    since_ts = _parse_since_value(since)

    client = _make_client(gateway_host, gateway_port)
    response = _fetch_strategy_timeline(client, deployment_id, limit, event_type, since_ts)
    events = list(response.events)

    if not events:
        click.echo("[]" if as_json else f"No events found for strategy: {deployment_id}")
        return

    if as_json:
        click.echo(_render_logs_as_json(events))
        return

    _render_logs_pretty(deployment_id, event_type, events, response.has_more)


# =============================================================================
# strat pause
# =============================================================================


# crap-allowlist: VIB-4722 mechanical deployment_id rename in existing high-CRAP function.
@click.command("pause")
@click.option("--deployment-id", "-s", required=True, help="Strategy instance ID.")
@click.option("--reason", required=True, help="Reason for pause (required for audit trail).")
@click.option(
    "--wait",
    is_flag=True,
    default=False,
    help="Wait until strategy confirms PAUSED status.",
)
@click.option("--timeout", default=60, type=int, help="Seconds to wait (default 60).")
@gateway_client_options
def strategy_pause(deployment_id, reason, wait, timeout, gateway_host, gateway_port):
    """Suspend a strategy's iteration loop without closing positions.

    The strategy completes its current iteration, then enters a suspended state.
    On-chain positions are not touched. Use 'strat resume' to restart the loop.

    Examples:

    \b
        almanak strat pause -s my_strategy --reason "manual review"
        almanak strat pause -s my_strategy --reason "market volatile" --wait
    """
    import time

    from almanak.gateway.proto import gateway_pb2

    client = _make_client(gateway_host, gateway_port)
    try:
        # Sample pre-pause status BEFORE issuing the command so --wait can detect
        # the transition and avoid false positives from filesystem strategies whose
        # default status is already "PAUSED".
        pre_status = ""
        if wait:
            try:
                pre_req = gateway_pb2.GetStrategyDetailsRequest(deployment_id=deployment_id)
                pre_details = client.dashboard.GetStrategyDetails(pre_req)
                pre_status = pre_details.summary.status
            except Exception:
                pass

        try:
            request = gateway_pb2.ExecuteActionRequest(
                deployment_id=deployment_id,
                action="PAUSE",
                reason=reason,
            )
            response = client.dashboard.ExecuteAction(request)
        except Exception as e:
            click.secho(f"Failed to pause strategy: {e}", fg="red", err=True)
            sys.exit(1)

        if not response.success:
            click.secho(f"Pause failed: {response.error}", fg="red", err=True)
            sys.exit(1)

        click.echo(f"Pause command issued for {deployment_id} (action_id: {response.action_id})")

        if wait:
            deadline = time.monotonic() + timeout
            while time.monotonic() < deadline:
                try:
                    det_req = gateway_pb2.GetStrategyDetailsRequest(deployment_id=deployment_id)
                    details = client.dashboard.GetStrategyDetails(det_req)
                    if details.summary.status == "PAUSED" and pre_status != "PAUSED":
                        click.secho(f"Strategy {deployment_id} is now PAUSED.", fg="yellow")
                        return
                except Exception as exc:
                    click.secho(f"Poll error: {exc}", fg="red", err=True)
                time.sleep(2)
            click.secho(f"Timed out waiting for {deployment_id} to reach PAUSED status.", fg="red", err=True)
            sys.exit(1)
    finally:
        client.disconnect()


# =============================================================================
# strat resume
# =============================================================================


@click.command("resume")
@click.option("--deployment-id", "-s", required=True, help="Strategy instance ID.")
@click.option("--reason", required=True, help="Reason for resume (required for audit trail).")
@gateway_client_options
def strategy_resume(deployment_id, reason, gateway_host, gateway_port):
    """Resume a previously paused strategy.

    Sends a RESUME command to the gateway, which the strategy runner picks up
    and uses to restart its iteration loop.

    Examples:

    \b
        almanak strat resume -s my_strategy --reason "review complete"
    """
    from almanak.gateway.proto import gateway_pb2

    client = _make_client(gateway_host, gateway_port)
    try:
        request = gateway_pb2.ExecuteActionRequest(
            deployment_id=deployment_id,
            action="RESUME",
            reason=reason,
        )
        response = client.dashboard.ExecuteAction(request)

        if not response.success:
            click.secho(f"Resume failed: {response.error}", fg="red", err=True)
            sys.exit(1)

        click.secho(f"Resume command issued for {deployment_id} (action_id: {response.action_id})", fg="green")
    except SystemExit:
        raise
    except Exception as e:
        click.secho(f"Failed to resume strategy: {e}", fg="red", err=True)
        sys.exit(1)
    finally:
        client.disconnect()
