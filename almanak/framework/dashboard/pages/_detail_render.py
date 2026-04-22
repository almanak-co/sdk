"""Timeline grouping + intent status derivation for the strategy detail page.

Extracted from ``detail.py:render_timeline_events`` (Phase 5d of the Dashboard
refactor plan) to isolate the pure correlation-ID grouping and intent-status
derivation logic from the surrounding Streamlit HTML emission. The HTML
rendering stays in ``render_timeline_events`` - only the pure data-shaping
helpers live here.

Public helpers:
    * ``IntentGroup`` - dataclass holding a correlation-ID cluster of
      ``TimelineEvent``s plus the derived intent description, status, tx
      count and latest timestamp.
    * ``group_events_by_intent`` - splits a flat list of ``TimelineEvent``s
      into ``(intent_groups, ungrouped_events)``. Groups are sorted by their
      most recent event timestamp, descending.
    * ``derive_intent_status`` - collapses an intent's execution events down
      to a single ``SUCCESS``/``FAILED``/``IN_PROGRESS`` literal used by the
      renderer to pick the status badge.

Behaviour is preserved verbatim from the pre-refactor ``render_timeline_events``
so the rendered HTML is byte-identical. The only semantic difference is that
the status values are now the normalized ``SUCCESS``/``FAILED``/``IN_PROGRESS``
literals instead of the raw ``EXECUTION_SUCCESS``/``EXECUTION_FAILED`` strings
that used to leak out of the grouping logic into the renderer.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal

from almanak.framework.dashboard.models import TimelineEvent

IntentStatus = Literal["SUCCESS", "FAILED", "IN_PROGRESS"]

# Execution-event value emitted by the executor when an intent finishes
# successfully. Pulled out as a constant so the grouping logic and the
# renderer stay in lock-step on the magic string.
EXEC_EVENT_SUCCESS = "EXECUTION_SUCCESS"
EXEC_EVENT_FAILED = "EXECUTION_FAILED"

# TX-level execution events recognised by the transaction-detail renderer.
# Any event whose ``details.execution_event`` falls outside this set is
# skipped by the expander loop in ``render_timeline_events``.
TX_EVENT_SENT = "TX_SENT"
TX_EVENT_CONFIRMED = "TX_CONFIRMED"
TX_EVENT_FAILED = "TX_FAILED"
TX_EVENT_REVERTED = "TX_REVERTED"
TX_EVENTS: frozenset[str] = frozenset({TX_EVENT_SENT, TX_EVENT_CONFIRMED, TX_EVENT_FAILED, TX_EVENT_REVERTED})


@dataclass(frozen=True)
class StatusBadge:
    """Display attributes for an intent-level status header.

    Used by ``render_timeline_events`` to render the coloured badge + text
    label at the top of each intent card. Kept here so the mapping from the
    derived ``IntentStatus`` literal to the user-visible badge stays
    adjacent to the derivation logic.
    """

    icon: str
    color: str
    text: str


_STATUS_BADGES: dict[IntentStatus, StatusBadge] = {
    "SUCCESS": StatusBadge(icon="✓", color="#00c853", text="Completed"),
    "FAILED": StatusBadge(icon="✗", color="#f44336", text="Failed"),
    "IN_PROGRESS": StatusBadge(icon="⏳", color="#ff9800", text="In Progress"),
}


def status_badge(status: IntentStatus) -> StatusBadge:
    """Return the display badge for a derived intent status.

    The mapping is exhaustive over ``IntentStatus`` and mirrors the
    if/elif/else ladder that used to live in ``render_timeline_events``.
    """
    return _STATUS_BADGES[status]


@dataclass(frozen=True)
class TxDisplay:
    """Per-TX display attributes derived from a single timeline event.

    The grouping logic is colour-agnostic; the renderer asks for an
    ``icon``/``color``/``detail`` triple via ``tx_display_fields`` so the
    branching mapping stays out of the Streamlit HTML block.
    """

    icon: str
    color: str
    detail: str


def tx_display_fields(event: TimelineEvent) -> TxDisplay | None:
    """Return the TX-level status badge + detail line for an event.

    Returns ``None`` when the event's ``details.execution_event`` is not a
    recognised TX-level event (``TX_SENT``/``TX_CONFIRMED``/``TX_FAILED``/
    ``TX_REVERTED``). Callers use that as the signal to skip the event in
    the expander loop - matching the pre-refactor ``if exec_event in (...)``
    guard exactly.

    Args:
        event: A single timeline event from an ``IntentGroup.events`` list.

    Returns:
        A ``TxDisplay`` with the icon, status colour and detail string to
        render, or ``None`` when the event is not a TX-level event.
    """
    details = event.details or {}
    exec_event = details.get("execution_event", "")
    if exec_event not in TX_EVENTS:
        return None

    if exec_event == TX_EVENT_CONFIRMED:
        block = details.get("block_number", "")
        gas = details.get("gas_used", "")
        detail = f"Block {block:,}" if block else ""
        if gas:
            detail += f" · Gas: {gas:,}"
        return TxDisplay(icon="✓", color="#00c853", detail=detail)
    if exec_event == TX_EVENT_SENT:
        return TxDisplay(icon="→", color="#2196f3", detail="Submitted to mempool")
    if exec_event == TX_EVENT_REVERTED:
        # Reverts carry a decoded ``revert_reason`` (e.g. "ERC20: insufficient
        # allowance") that is strictly more actionable than the generic
        # ``error`` field (often a raw exception string). Prefer it when
        # present (#1732); fall back to ``error`` then the generic literal.
        detail = details.get("revert_reason") or details.get("error") or "Transaction reverted"
        return TxDisplay(icon="✗", color="#f44336", detail=detail)
    if exec_event == TX_EVENT_FAILED:
        return TxDisplay(
            icon="✗",
            color="#f44336",
            detail=details.get("error", "Transaction failed"),
        )
    # Unreachable: ``exec_event`` is already constrained to ``TX_EVENTS`` by
    # the guard above. The ``•`` fallback mirrored the pre-refactor
    # ``else`` branch and is kept for defensive parity.
    return TxDisplay(icon="•", color="#888", detail="")


@dataclass
class IntentGroup:
    """A correlation-ID cluster of timeline events representing one intent.

    Attributes:
        correlation_id: The ``details.correlation_id`` shared by all events in
            this group. Used as the intent's stable identifier.
        intent_description: Human-readable intent description taken from the
            first event's ``details.intent_description`` (falls back to
            ``"Unknown Intent"`` when missing).
        status: Derived execution status of the intent - ``"SUCCESS"`` when
            any event carried ``execution_event == "EXECUTION_SUCCESS"``,
            ``"FAILED"`` when any event carried ``"EXECUTION_FAILED"`` (with
            failure taking precedence over success if both are observed),
            otherwise ``"IN_PROGRESS"``.
        events: All timeline events belonging to the intent, in insertion
            order. The renderer re-sorts these by timestamp before display.
        tx_count: Number of on-chain transactions attributed to this intent.
            Takes the first event's ``details.tx_count`` when present;
            otherwise falls back to the count of events in the group that
            carry a ``tx_hash``. This matches the pre-refactor fallback.
        latest_timestamp: The most recent timestamp observed across
            ``events``. Used by ``group_events_by_intent`` to sort groups
            newest-first for the renderer.
    """

    correlation_id: str
    intent_description: str
    status: IntentStatus
    events: list[TimelineEvent] = field(default_factory=list)
    tx_count: int = 0
    latest_timestamp: datetime | None = None


def derive_intent_status(events: list[TimelineEvent]) -> IntentStatus:
    """Collapse a group's execution events into a single status label.

    Matches the pre-refactor renderer semantics: the status is determined by
    the *last* event in iteration order that carried an
    ``EXECUTION_SUCCESS`` or ``EXECUTION_FAILED`` value in its
    ``details.execution_event`` field. If no such event exists the intent is
    still ``IN_PROGRESS``. This preserves byte-identical HTML output against
    the original ``render_timeline_events`` which used a last-write-wins
    mutable slot inside the grouping loop.

    * Last ``EXECUTION_SUCCESS`` observed -> ``"SUCCESS"``.
    * Last ``EXECUTION_FAILED`` observed -> ``"FAILED"``.
    * Neither observed -> ``"IN_PROGRESS"``.

    Args:
        events: The timeline events belonging to a single intent group,
            already in the order they were emitted.

    Returns:
        One of ``"SUCCESS"``, ``"FAILED"``, ``"IN_PROGRESS"``.
    """
    status: IntentStatus = "IN_PROGRESS"
    for event in events:
        if not event.details:
            continue
        exec_event = event.details.get("execution_event", "")
        if exec_event == "EXECUTION_SUCCESS":
            status = "SUCCESS"
        elif exec_event == "EXECUTION_FAILED":
            status = "FAILED"
    return status


def group_events_by_intent(
    events: list[TimelineEvent],
) -> tuple[list[IntentGroup], list[TimelineEvent]]:
    """Split timeline events into intent groups + ungrouped events.

    Events whose ``details.correlation_id`` is truthy are clustered by that
    ID into ``IntentGroup`` instances. Events without a ``correlation_id``
    - including legacy events that predate the correlation-ID field - are
    returned in a separate ``ungrouped`` list so the renderer can display
    them via the fallback path.

    The returned groups are sorted by ``latest_timestamp`` descending (newest
    intent first), matching the original renderer's ``sorted(..., reverse=True)``
    call. ``ungrouped`` preserves insertion order.

    Args:
        events: Flat timeline events as stored on ``Strategy.timeline_events``.

    Returns:
        A ``(groups, ungrouped)`` tuple. ``groups`` is the list of
        correlation-ID clusters; ``ungrouped`` is every event that lacked a
        correlation ID, in original order.
    """
    grouped: dict[str, IntentGroup] = {}
    ungrouped: list[TimelineEvent] = []

    for event in events:
        correlation_id = event.details.get("correlation_id") if event.details else None
        if not correlation_id:
            ungrouped.append(event)
            continue

        group = grouped.get(correlation_id)
        if group is None:
            group = IntentGroup(
                correlation_id=correlation_id,
                intent_description=event.details.get("intent_description", "Unknown Intent"),
                status="IN_PROGRESS",
                events=[],
                tx_count=int(event.details.get("tx_count", 0) or 0),
                latest_timestamp=event.timestamp,
            )
            grouped[correlation_id] = group

        group.events.append(event)
        if group.latest_timestamp is None or event.timestamp > group.latest_timestamp:
            group.latest_timestamp = event.timestamp

    # Finalize each group: derive status and fill in the tx_count fallback.
    for group in grouped.values():
        group.status = derive_intent_status(group.events)
        if not group.tx_count:
            group.tx_count = sum(1 for e in group.events if e.details and e.details.get("tx_hash"))

    ordered = sorted(
        grouped.values(),
        key=lambda g: g.latest_timestamp or datetime.min,
        reverse=True,
    )
    return ordered, ungrouped
