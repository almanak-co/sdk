"""CLI command: ``almanak strat export``

Export strategy data (trades, timeline, PnL) to CSV or JSON.

Examples::

    almanak strat export --strategy-id my-strat --data trades --format csv -o trades.csv
    almanak strat export --strategy-id my-strat --data pnl --format json
"""

import sys

import click


@click.command("export")
@click.option("--strategy-id", "-s", required=True, help="Strategy ID to export data for.")
@click.option(
    "--data",
    "-d",
    type=click.Choice(["trades", "timeline", "pnl"], case_sensitive=False),
    default="trades",
    help="Data type to export (default: trades).",
)
@click.option(
    "--format",
    "-f",
    "fmt",
    type=click.Choice(["csv", "json"], case_sensitive=False),
    default="csv",
    help="Output format (default: csv).",
)
@click.option("--output", "-o", type=click.Path(), default=None, help="Output file path. Defaults to stdout.")
@click.option("--limit", type=int, default=10000, help="Maximum records to export.")
@click.option("--host", default="localhost", help="Gateway host.")
@click.option("--port", type=int, default=50051, help="Gateway port.")
def export(strategy_id: str, data: str, fmt: str, output: str | None, limit: int, host: str, port: int) -> None:
    """Export strategy data to CSV or JSON."""
    from almanak.framework.dashboard.data_client import DashboardDataClient
    from almanak.framework.dashboard.export import export_pnl, export_timeline, export_trades

    client = DashboardDataClient.for_gateway(host=host, port=port)
    try:
        client.connect()
    except Exception as e:
        click.echo(f"Failed to connect to gateway at {host}:{port}: {e}", err=True)
        sys.exit(1)

    try:
        if data == "trades":
            result = export_trades(client, strategy_id, limit=limit, fmt=fmt)
        elif data == "timeline":
            result = export_timeline(client, strategy_id, limit=limit, fmt=fmt)
        elif data == "pnl":
            result = export_pnl(client, strategy_id, fmt=fmt)
        else:
            click.echo(f"Unknown data type: {data}", err=True)
            sys.exit(1)

        if output:
            with open(output, "wb") as f:
                f.write(result)
            click.echo(f"Exported {data} to {output} ({len(result)} bytes)")
        else:
            sys.stdout.buffer.write(result)
    finally:
        client.disconnect()
