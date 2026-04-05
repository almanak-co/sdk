"""Export dashboard data to CSV and JSON.

Provides functions used by both Streamlit download buttons and the
``almanak strat export`` CLI command.

Usage (programmatic)::

    from almanak.framework.dashboard.export import export_trades, export_timeline, export_pnl

    csv_bytes = export_trades(client, "my-strategy", fmt="csv")
    json_bytes = export_trades(client, "my-strategy", fmt="json")

Usage (CLI)::

    almanak strat export --strategy-id my-strategy --data trades --format csv -o trades.csv
"""

import csv
import io
import json
import logging
from datetime import datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from almanak.framework.dashboard.data_client import DashboardDataClient

logger = logging.getLogger(__name__)

# Supported data types
EXPORT_TYPES = ("trades", "timeline", "pnl")
EXPORT_FORMATS = ("csv", "json")


def export_trades(
    client: "DashboardDataClient",
    strategy_id: str,
    since: datetime | None = None,
    limit: int = 10000,
    fmt: str = "csv",
) -> bytes:
    """Export transaction ledger trades.

    Args:
        client: Connected DashboardDataClient.
        strategy_id: Strategy to export.
        since: Only trades after this time.
        limit: Maximum records.
        fmt: Output format ("csv" or "json").

    Returns:
        Encoded bytes (UTF-8).
    """
    trades = client.get_trades(strategy_id, since=since, limit=limit)
    rows = [t.to_dict() for t in trades]
    return _format_rows(rows, fmt)


def export_timeline(
    client: "DashboardDataClient",
    strategy_id: str,
    limit: int = 10000,
    fmt: str = "csv",
) -> bytes:
    """Export timeline events.

    Args:
        client: Connected DashboardDataClient.
        strategy_id: Strategy to export.
        limit: Maximum records.
        fmt: Output format ("csv" or "json").

    Returns:
        Encoded bytes (UTF-8).
    """
    events = client.get_timeline(strategy_id, limit=limit)
    rows = []
    for e in events:
        rows.append(
            {
                "timestamp": e.timestamp.isoformat() if e.timestamp else "",
                "event_type": e.event_type,
                "description": e.description,
                "tx_hash": e.tx_hash or "",
                "chain": e.chain or "",
                "details": json.dumps(e.details) if e.details else "",
            }
        )
    return _format_rows(rows, fmt)


def export_pnl(
    client: "DashboardDataClient",
    strategy_id: str,
    since: datetime | None = None,
    fmt: str = "csv",
) -> bytes:
    """Export PnL / equity curve data.

    Args:
        client: Connected DashboardDataClient.
        strategy_id: Strategy to export.
        since: Only data points after this time.
        fmt: Output format ("csv" or "json").

    Returns:
        Encoded bytes (UTF-8).
    """
    points = client.get_pnl_history(strategy_id, since=since)
    rows = [p.to_dict() for p in points]
    return _format_rows(rows, fmt)


def _format_rows(rows: list[dict[str, Any]], fmt: str) -> bytes:
    """Serialize rows to the requested format."""
    if fmt == "json":
        return json.dumps(rows, indent=2, default=str).encode("utf-8")

    if fmt != "csv":
        raise ValueError(f"Unsupported export format: {fmt!r}. Must be 'csv' or 'json'.")

    # CSV
    if not rows:
        return b""
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue().encode("utf-8")
