"""Adapters for external consumers (Portfolio Manager, custom UIs).

Converts external data formats (PM's ``strategies.json`` entries) into SDK
``Strategy`` objects for offline rendering. Also provides rendering helpers
that accept an optional ``DashboardDataClient`` for live data injection.
"""

import logging
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from almanak.framework.dashboard.models import Strategy, StrategyStatus

if TYPE_CHECKING:
    from almanak.framework.dashboard.data_client import DashboardDataClient

logger = logging.getLogger(__name__)


def strategy_from_pm_dict(entry: dict[str, Any]) -> Strategy:
    """Convert a PM ``strategies.json`` entry to an SDK Strategy object.

    PM stores strategy metadata in a flat dict format. This adapter
    normalizes the fields so the SDK dashboard components can render
    the strategy without knowing about PM internals.

    Args:
        entry: A single strategy entry from PM's ``strategies.json``.

    Returns:
        A Strategy dataclass suitable for dashboard rendering.

    Example::

        with open("strategies.json") as f:
            pm_strategies = json.load(f)
        for raw in pm_strategies:
            strategy = strategy_from_pm_dict(raw)
            render_strategy_detail(strategy)
    """
    # Map PM status strings to StrategyStatus
    status_str = (entry.get("status") or "INACTIVE").upper()
    try:
        status = StrategyStatus(status_str)
    except ValueError:
        status = StrategyStatus.INACTIVE

    # Parse timestamps
    last_action_raw = entry.get("last_action_at") or entry.get("last_heartbeat")
    last_action_at = None
    if isinstance(last_action_raw, str):
        try:
            last_action_at = datetime.fromisoformat(last_action_raw)
        except ValueError:
            pass
    elif isinstance(last_action_raw, int | float):
        last_action_at = datetime.fromtimestamp(last_action_raw, tz=UTC)

    # Parse value fields
    def _dec(key: str) -> Decimal:
        val = entry.get(key)
        if val is None:
            return Decimal("0")
        try:
            return Decimal(str(val))
        except Exception:
            return Decimal("0")

    return Strategy(
        id=entry.get("strategy_id") or entry.get("id") or "",
        name=entry.get("name") or entry.get("strategy_name") or entry.get("strategy_id") or "",
        status=status,
        pnl_24h_usd=_dec("pnl_24h_usd"),
        total_value_usd=_dec("total_value_usd"),
        chain=entry.get("chain") or "",
        protocol=entry.get("protocol") or "",
        last_action_at=last_action_at,
        attention_required=bool(entry.get("attention_required")),
        attention_reason=entry.get("attention_reason"),
        is_multi_chain=bool(entry.get("is_multi_chain")),
        chains=entry.get("chains") or [],
        value_confidence=entry.get("value_confidence"),
    )


def render_strategy_detail(
    strategy: Strategy,
    client: "DashboardDataClient | None" = None,
) -> None:
    """Render strategy detail page in Streamlit.

    If ``client`` is provided, live data is fetched from the gateway.
    Otherwise renders with whatever data is already on the Strategy object
    (offline/cached mode).

    The caller's ``Strategy`` instance is **never mutated** (#1716). When
    live timeline data is fetched it is merged onto a shallow-copy built via
    ``dataclasses.replace``; the caller keeps its original object intact so
    re-rendering the same ``Strategy`` from multiple callers (PM caches, custom
    UIs) does not see timeline events from a previous render leak through.

    Args:
        strategy: Strategy to render. Not modified in place.
        client: Optional DashboardDataClient for live data.
    """

    from almanak.framework.dashboard.pages.detail import page

    render_strategy = strategy
    if client and client.is_connected:
        try:
            detail = client.get_strategy_detail(strategy.id, include_timeline=True, include_pnl_history=True)
            if detail.timeline:
                from almanak.framework.dashboard.models import TimelineEvent as ModelEvent
                from almanak.framework.dashboard.models import TimelineEventType

                fresh_events = [
                    ModelEvent(
                        timestamp=e.timestamp,
                        event_type=TimelineEventType(e.event_type)
                        if e.event_type in TimelineEventType.__members__
                        else TimelineEventType.TRADE,
                        description=e.description,
                        tx_hash=e.tx_hash,
                        chain=e.chain,
                        details=e.details,
                    )
                    for e in detail.timeline
                    if e.timestamp is not None
                ]
                # Copy-on-write: the caller's strategy object is untouched.
                render_strategy = replace(strategy, timeline_events=fresh_events)
        except Exception:
            logger.debug("Live data fetch failed for %s, using cached data", strategy.id, exc_info=True)

    # Render using the existing detail page with the strategy in a list
    page([render_strategy])


def render_strategy_timeline(
    strategy: Strategy,
    client: "DashboardDataClient | None" = None,
) -> None:
    """Render strategy timeline page in Streamlit.

    The caller's ``Strategy`` instance is never mutated (#1716) - a shallow
    copy via ``dataclasses.replace`` carries the fresh timeline events to the
    page renderer.

    Args:
        strategy: Strategy to render. Not modified in place.
        client: Optional DashboardDataClient for live data.
    """

    from almanak.framework.dashboard.pages.timeline import page

    render_strategy = strategy
    if client and client.is_connected:
        try:
            events = client.get_timeline(strategy.id, limit=200)
            from almanak.framework.dashboard.models import TimelineEvent as ModelEvent
            from almanak.framework.dashboard.models import TimelineEventType

            fresh_events = [
                ModelEvent(
                    timestamp=e.timestamp,
                    event_type=TimelineEventType(e.event_type)
                    if e.event_type in TimelineEventType.__members__
                    else TimelineEventType.TRADE,
                    description=e.description,
                    tx_hash=e.tx_hash,
                    chain=e.chain,
                    details=e.details,
                )
                for e in events
                if e.timestamp is not None
            ]
            render_strategy = replace(strategy, timeline_events=fresh_events)
        except Exception:
            logger.debug("Live timeline fetch failed for %s", strategy.id, exc_info=True)

    page([render_strategy])
