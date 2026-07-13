"""Adapter: PositionEventData (proto) -> dict shape consumed by dashboard plots.

The state-service RPCs ``GetPositionHistory`` and ``GetPositionEventsFiltered``
return ``PositionEventData`` protobuf rows. Dashboard plotting code
(``plot_positions_over_time``) expects ``PositionData`` dataclasses or dicts with
``date_start`` / ``date_end`` / price-bound / active-flag shape.

This adapter sits between the two so dashboard templates do not unpack proto
fields inline. Keeps the conversion in one place, makes plot-shape changes
testable, and matches blueprint-22's rule that dashboards consume canonical
gateway state APIs (not ``timeline_events.details_json`` parsing).
"""

from __future__ import annotations

from datetime import UTC, datetime
from math import exp, log
from typing import Any

from ._token_decimals import resolve_token_decimals


def position_event_to_dict(event: Any) -> dict[str, Any]:
    """Convert a single ``PositionEventData`` proto row to a plain dict.

    Args:
        event: A ``gateway_pb2.PositionEventData`` row or any duck-typed object
            exposing the same field names. Numeric / decimal fields stay as
            strings (the proto encoding) — plot helpers handle the conversion.

    Returns:
        Dict with all fields the dashboard plots / templates expect. Always
        contains every key in the canonical PositionData shape; missing
        attributes resolve to a safe default.
    """
    return {
        "id": getattr(event, "id", "") or "",
        "deployment_id": getattr(event, "deployment_id", "") or "",
        "cycle_id": getattr(event, "cycle_id", "") or "",
        "execution_mode": getattr(event, "execution_mode", "") or "",
        "position_id": getattr(event, "position_id", "") or "",
        "position_type": getattr(event, "position_type", "") or "",
        "event_type": getattr(event, "event_type", "") or "",
        # ``timestamp`` is a Unix epoch second on the wire; convert to
        # ISO 8601 for renderer consumption (Streamlit tables / Plotly
        # axes prefer ISO strings, not epochs).
        "timestamp": _epoch_to_iso(getattr(event, "timestamp", 0) or 0),
        "protocol": getattr(event, "protocol", "") or "",
        "chain": getattr(event, "chain", "") or "",
        "token0": getattr(event, "token0", "") or "",
        "token1": getattr(event, "token1", "") or "",
        "amount0": getattr(event, "amount0", "") or "",
        "amount1": getattr(event, "amount1", "") or "",
        "value_usd": getattr(event, "value_usd", "") or "",
        # LP-specific
        "tick_lower": getattr(event, "tick_lower", None),
        "tick_upper": getattr(event, "tick_upper", None),
        "liquidity": getattr(event, "liquidity", "") or "",
        "in_range": getattr(event, "in_range", None),
        "fees_token0": getattr(event, "fees_token0", "") or "",
        "fees_token1": getattr(event, "fees_token1", "") or "",
        # Perp-specific
        "leverage": getattr(event, "leverage", "") or "",
        "entry_price": getattr(event, "entry_price", "") or "",
        "mark_price": getattr(event, "mark_price", "") or "",
        "unrealized_pnl": getattr(event, "unrealized_pnl", "") or "",
        "is_long": getattr(event, "is_long", None),
        # Execution
        "tx_hash": getattr(event, "tx_hash", "") or "",
        "gas_usd": getattr(event, "gas_usd", "") or "",
        "ledger_entry_id": getattr(event, "ledger_entry_id", "") or "",
        "protocol_fees_usd": getattr(event, "protocol_fees_usd", "") or "",
        # Attribution
        "attribution_json": getattr(event, "attribution_json", "") or "",
        "attribution_version": getattr(event, "attribution_version", 0) or 0,
    }


def position_events_to_position_data_dicts(
    events: list[Any],
    *,
    token0: str | None = None,
    token1: str | None = None,
) -> list[dict[str, Any]]:
    """Collapse a chronological list of position events into ``PositionData``-shaped dicts.

    Walks the events for one (or many) ``position_id`` and produces one dict
    per position with:

    - ``date_start`` set from the first OPEN row's timestamp,
    - ``date_end`` set from the matching CLOSE row's timestamp (or ``None``
      if the position is still open),
    - ``bound_price_lower`` / ``bound_price_upper`` lifted from the OPEN row
      when the row carries them (LP positions),
    - ``is_active`` derived from the presence of a CLOSE row.

    The output is consumed by ``plot_positions_over_time``, which already
    accepts both ``PositionData`` and dict shapes.

    Args:
        events: List of position events (dicts produced by
            :func:`position_event_to_dict` or proto rows). Ordering does not
            matter — the function sorts by ``(position_id, timestamp)``.

    Returns:
        One dict per position, in the order of first-seen ``position_id``.
    """
    # Normalize input — accept either dicts or raw protos.
    rows: list[dict[str, Any]] = [e if isinstance(e, dict) else position_event_to_dict(e) for e in events]
    # Group by position_id while preserving insertion order so output is
    # deterministic for a given input.
    # Group by position_id. Rows without a position_id are skipped so they
    # do not collapse into a single synthetic "" position that
    # ``plot_positions_over_time`` would then render as a merged ghost
    # position spanning unrelated events.
    by_position: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        pid = row.get("position_id")
        if not pid:
            continue
        by_position.setdefault(str(pid), []).append(row)

    positions: list[dict[str, Any]] = []
    for pid, group in by_position.items():
        # Sort within a position by timestamp ASC. Strings sort lexically
        # but ISO 8601 timestamps are ordered-correct, so this is safe.
        group_sorted = sorted(group, key=lambda r: r.get("timestamp") or "")
        open_row = next((r for r in group_sorted if r.get("event_type") == "OPEN"), None)
        close_row = next((r for r in group_sorted if r.get("event_type") == "CLOSE"), None)
        # If no OPEN was emitted (truncated history), fall back to the first row.
        anchor = open_row or (group_sorted[0] if group_sorted else None)
        if anchor is None:
            continue

        anchor_chain = anchor.get("chain") or None
        positions.append(
            {
                "position_id": pid,
                "date_start": _parse_iso(anchor.get("timestamp")),
                "date_end": _parse_iso(close_row.get("timestamp")) if close_row else None,
                "bound_tick_lower": anchor.get("tick_lower") or 0,
                "bound_tick_upper": anchor.get("tick_upper") or 0,
                "bound_price_lower": _tick_to_price(anchor.get("tick_lower"), token0, token1, anchor_chain),
                "bound_price_upper": _tick_to_price(anchor.get("tick_upper"), token0, token1, anchor_chain),
                "is_active": close_row is None,
                "position_type": anchor.get("position_type", "") or "",
                "protocol": anchor.get("protocol", "") or "",
                "chain": anchor.get("chain", "") or "",
            }
        )
    return positions


def _epoch_to_iso(epoch_seconds: int) -> str:
    """Convert Unix epoch seconds to ISO 8601 UTC string. ``0`` returns empty."""
    if not epoch_seconds:
        return ""
    try:
        return datetime.fromtimestamp(int(epoch_seconds), tz=UTC).isoformat()
    except (ValueError, OSError, OverflowError):
        return ""


def _parse_iso(value: Any) -> datetime | None:
    """Best-effort ISO 8601 -> datetime. Returns ``None`` on any failure."""
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _tick_to_price(tick: Any, token0: str | None, token1: str | None, chain: str | None = None) -> float:
    """Convert a Uniswap-style tick into token1-per-token0 display price."""
    if tick is None or token0 is None or token1 is None:
        return 0.0
    try:
        tick_int = int(tick)
    except (TypeError, ValueError):
        return 0.0

    # Registry-first, per-chain decimals — a static symbol map silently
    # mis-scales any token it doesn't list (VIB-5738), collapsing the
    # position-range band on the Positions-Over-Time chart to price 0.
    decimals0 = resolve_token_decimals(token0, chain)
    decimals1 = resolve_token_decimals(token1, chain)
    if decimals0 is None or decimals1 is None:
        return 0.0

    try:
        return exp(tick_int * log(1.0001)) * (10 ** (decimals0 - decimals1))
    except (OverflowError, ValueError):
        return 0.0


__all__ = [
    "position_event_to_dict",
    "position_events_to_position_data_dicts",
]
