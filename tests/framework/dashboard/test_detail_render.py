"""Unit tests for the pure grouping + status-derivation helpers in
``almanak.framework.dashboard.pages._detail_render``.

Exercised under ``uv run pytest`` without Streamlit - these helpers are
deliberately framework-free so the grouping logic can be validated
independently of the ``render_timeline_events`` HTML emission path. Extracted
as part of Phase 5d of the Dashboard refactor plan.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from almanak.framework.dashboard.models import TimelineEvent, TimelineEventType
from almanak.framework.dashboard.pages._detail_render import (
    IntentGroup,
    derive_intent_status,
    group_events_by_intent,
    status_badge,
    tx_display_fields,
)


def _event(
    ts_seconds: int,
    details: dict[str, Any] | None = None,
    event_type: TimelineEventType = TimelineEventType.TRADE,
) -> TimelineEvent:
    """Build a minimal ``TimelineEvent`` for the grouping tests.

    ``ts_seconds`` is an offset from ``2025-01-01T00:00:00Z`` so test
    timestamps are easy to order without repeating the base date.
    """
    base = datetime(2025, 1, 1, tzinfo=UTC)
    return TimelineEvent(
        timestamp=base.replace(second=ts_seconds),
        event_type=event_type,
        description="test event",
        details=details or {},
    )


# ---------------------------------------------------------------------------
# group_events_by_intent
# ---------------------------------------------------------------------------


def test_group_events_by_intent_collapses_shared_correlation_id_into_one_group() -> None:
    events = [_event(i, {"correlation_id": "intent-A", "intent_description": "Swap USDC->ETH"}) for i in range(5)]

    groups, ungrouped = group_events_by_intent(events)

    assert ungrouped == []
    assert len(groups) == 1
    group = groups[0]
    assert isinstance(group, IntentGroup)
    assert group.correlation_id == "intent-A"
    assert group.intent_description == "Swap USDC->ETH"
    assert len(group.events) == 5
    assert group.status == "IN_PROGRESS"


def test_group_events_by_intent_events_without_correlation_id_go_to_ungrouped() -> None:
    events = [_event(i) for i in range(5)]  # no details.correlation_id

    groups, ungrouped = group_events_by_intent(events)

    assert groups == []
    assert len(ungrouped) == 5
    # Insertion order preserved.
    assert [e.timestamp.second for e in ungrouped] == [0, 1, 2, 3, 4]


def test_group_events_by_intent_mixed_inputs_split_correctly_and_sort_newest_first() -> None:
    events = [
        _event(0, {"correlation_id": "intent-A", "intent_description": "A"}),
        _event(1),  # ungrouped
        _event(2, {"correlation_id": "intent-B", "intent_description": "B"}),
        _event(3, {"correlation_id": "intent-A", "intent_description": "A"}),
        _event(4),  # ungrouped
        _event(5, {"correlation_id": "intent-B", "intent_description": "B"}),
    ]

    groups, ungrouped = group_events_by_intent(events)

    # Two distinct groups, sorted by latest_timestamp descending
    # (intent-B's latest is second=5, intent-A's is second=3).
    assert [g.correlation_id for g in groups] == ["intent-B", "intent-A"]
    assert [len(g.events) for g in groups] == [2, 2]
    assert [e.timestamp.second for e in ungrouped] == [1, 4]


def test_group_events_by_intent_empty_input_returns_empty_pair() -> None:
    groups, ungrouped = group_events_by_intent([])

    assert groups == []
    assert ungrouped == []


def test_group_events_by_intent_tx_count_counts_events_with_tx_hash() -> None:
    events = [
        _event(0, {"correlation_id": "intent-A", "intent_description": "A", "tx_hash": "0xaa"}),
        _event(1, {"correlation_id": "intent-A", "intent_description": "A", "tx_hash": "0xbb"}),
        _event(2, {"correlation_id": "intent-A", "intent_description": "A"}),  # no tx_hash
        _event(3, {"correlation_id": "intent-A", "intent_description": "A", "tx_hash": "0xcc"}),
    ]

    groups, _ = group_events_by_intent(events)

    assert len(groups) == 1
    # tx_count was not populated on any event -> falls back to count of
    # events that carry a tx_hash. Three of the four events have one.
    assert groups[0].tx_count == 3


def test_group_events_by_intent_honors_explicit_tx_count_on_first_event() -> None:
    events = [
        _event(0, {"correlation_id": "intent-A", "intent_description": "A", "tx_count": 7}),
        _event(1, {"correlation_id": "intent-A", "intent_description": "A", "tx_hash": "0xaa"}),
    ]

    groups, _ = group_events_by_intent(events)

    # Explicit tx_count from the first event wins over the tx_hash fallback.
    assert groups[0].tx_count == 7


# ---------------------------------------------------------------------------
# derive_intent_status
# ---------------------------------------------------------------------------


def test_derive_intent_status_all_success_returns_success() -> None:
    events = [
        _event(0, {"execution_event": "TX_SENT"}),
        _event(1, {"execution_event": "TX_CONFIRMED"}),
        _event(2, {"execution_event": "EXECUTION_SUCCESS"}),
    ]

    assert derive_intent_status(events) == "SUCCESS"


def test_derive_intent_status_any_fail_returns_failed() -> None:
    # Failure is the final execution event -> matches last-write-wins
    # semantics of the original renderer.
    events = [
        _event(0, {"execution_event": "TX_SENT"}),
        _event(1, {"execution_event": "EXECUTION_FAILED"}),
    ]

    assert derive_intent_status(events) == "FAILED"


def test_derive_intent_status_in_progress_when_no_terminal_event() -> None:
    events = [
        _event(0, {"execution_event": "TX_SENT"}),
        _event(1, {"execution_event": "TX_CONFIRMED"}),
    ]

    assert derive_intent_status(events) == "IN_PROGRESS"


def test_derive_intent_status_empty_events_returns_in_progress() -> None:
    assert derive_intent_status([]) == "IN_PROGRESS"


def test_group_events_by_intent_propagates_derived_status_to_group() -> None:
    events = [
        _event(0, {"correlation_id": "intent-ok", "intent_description": "ok", "execution_event": "EXECUTION_SUCCESS"}),
        _event(1, {"correlation_id": "intent-bad", "intent_description": "bad", "execution_event": "EXECUTION_FAILED"}),
        _event(2, {"correlation_id": "intent-wip", "intent_description": "wip", "execution_event": "TX_SENT"}),
    ]

    groups, _ = group_events_by_intent(events)
    by_id = {g.correlation_id: g.status for g in groups}

    assert by_id == {
        "intent-ok": "SUCCESS",
        "intent-bad": "FAILED",
        "intent-wip": "IN_PROGRESS",
    }


# ---------------------------------------------------------------------------
# tx_display_fields + status_badge (ancillary status-derivation helpers)
# ---------------------------------------------------------------------------


def test_tx_display_fields_returns_none_for_non_tx_event() -> None:
    # Summary events are filtered out by the renderer via the ``None`` sentinel.
    assert tx_display_fields(_event(0, {"execution_event": "EXECUTION_SUCCESS"})) is None


def test_tx_display_fields_confirmed_includes_block_and_gas_detail() -> None:
    fields = tx_display_fields(_event(0, {"execution_event": "TX_CONFIRMED", "block_number": 12345, "gas_used": 21000}))

    assert fields is not None
    assert fields.icon == "✓"
    assert fields.color == "#00c853"
    assert fields.detail == "Block 12,345 · Gas: 21,000"


def test_tx_display_fields_failed_uses_error_detail_or_default() -> None:
    failed_with_error = tx_display_fields(_event(0, {"execution_event": "TX_FAILED", "error": "insufficient funds"}))
    reverted_without_error = tx_display_fields(_event(0, {"execution_event": "TX_REVERTED"}))

    assert failed_with_error is not None
    assert failed_with_error.detail == "insufficient funds"
    assert reverted_without_error is not None
    assert reverted_without_error.detail == "Transaction failed"


def test_status_badge_returns_stable_mapping_for_every_literal() -> None:
    success = status_badge("SUCCESS")
    failed = status_badge("FAILED")
    in_progress = status_badge("IN_PROGRESS")

    assert (success.icon, success.text) == ("✓", "Completed")
    assert (failed.icon, failed.text) == ("✗", "Failed")
    assert (in_progress.icon, in_progress.text) == ("⏳", "In Progress")
    # Colours match the pre-refactor renderer exactly.
    assert success.color == "#00c853"
    assert failed.color == "#f44336"
    assert in_progress.color == "#ff9800"
