"""Forensic event emitter for StrategyRunner phase boundaries.

Emits structured forensic events as timeline events with cycle_id and phase
correlation. All events flow through the existing timeline dual-write path
(local file + gateway gRPC) with zero new infrastructure.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

from almanak.framework.api.timeline import TimelineEvent, TimelineEventType, add_event
from almanak.framework.observability.context import get_cycle_id
from almanak.framework.observability.events import StrategyPhase

logger = logging.getLogger(__name__)

# Pre-build lookup for str -> TimelineEventType resolution
_EVENT_TYPE_BY_NAME: dict[str, TimelineEventType] = {e.name: e for e in TimelineEventType}
_EVENT_TYPE_BY_VALUE: dict[str, TimelineEventType] = {e.value: e for e in TimelineEventType}


def _resolve_event_type(event_type: TimelineEventType | str) -> TimelineEventType:
    """Resolve a string event type to its enum member, falling back to CUSTOM."""
    if isinstance(event_type, TimelineEventType):
        return event_type
    return _EVENT_TYPE_BY_NAME.get(event_type) or _EVENT_TYPE_BY_VALUE.get(event_type) or TimelineEventType.CUSTOM


def emit_phase_event(
    *,
    strategy_id: str,
    phase: StrategyPhase,
    event_type: TimelineEventType | str,
    description: str,
    chain: str = "",
    tx_hash: str = "",
    details: dict[str, Any] | None = None,
) -> None:
    """Emit a forensic event at a strategy lifecycle phase boundary.

    Automatically attaches the current cycle_id from context. Events flow
    through the standard timeline path (local + gateway).

    Args:
        strategy_id: Strategy identifier.
        phase: Current lifecycle phase (DECIDE, COMPILE, etc.).
        event_type: Timeline event type.
        description: Human-readable description.
        chain: Optional chain name.
        tx_hash: Optional transaction hash.
        details: Optional detail payload.
    """
    cycle_id = get_cycle_id() or ""

    event = TimelineEvent(
        timestamp=datetime.now(UTC),
        event_type=_resolve_event_type(event_type),
        description=description,
        strategy_id=strategy_id,
        chain=chain,
        tx_hash=tx_hash or None,
        details=details or {},
        cycle_id=cycle_id,
        phase=phase.value,
    )
    add_event(event)
