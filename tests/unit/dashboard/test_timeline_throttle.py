"""Tests for timeline event throttling and coalescing (VIB-2427).

Verifies:
1. add_event() throttles STRATEGY_STUCK events within the cooldown window
2. Non-throttled event types pass through unchanged
3. Suppressed count is annotated on the next emitted event
4. _coalesce_consecutive_events() collapses runs of the same event type
"""

from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import pytest

from almanak.framework.api.timeline import (
    TimelineEvent,
    TimelineEventType,
    _event_store,
    _throttle_last_emitted,
    _throttle_suppressed_counts,
    add_event,
    clear_events,
    set_event_gateway_client,
)
from almanak.framework.dashboard.pages.timeline import _coalesce_consecutive_events


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _clean_timeline_state():
    """Reset timeline module state between tests."""
    _event_store.clear()
    _throttle_last_emitted.clear()
    _throttle_suppressed_counts.clear()
    set_event_gateway_client(None)
    yield
    _event_store.clear()
    _throttle_last_emitted.clear()
    _throttle_suppressed_counts.clear()
    set_event_gateway_client(None)


def _make_event(
    event_type: TimelineEventType = TimelineEventType.STRATEGY_STUCK,
    deployment_id: str = "test-strat",
    description: str = "Circuit breaker open: too many failures",
    ts: datetime | None = None,
) -> TimelineEvent:
    return TimelineEvent(
        timestamp=ts or datetime.now(UTC),
        event_type=event_type,
        description=description,
        deployment_id=deployment_id,
    )


# ---------------------------------------------------------------------------
# Throttle tests (add_event level)
# ---------------------------------------------------------------------------


class TestTimelineEventThrottle:
    """Tests for add_event() throttling."""

    @patch("almanak.framework.api.timeline._persist_events_to_file")
    def test_first_event_is_not_throttled(self, _mock_persist):
        event = _make_event()
        add_event(event)
        assert len(_event_store.get("test-strat", [])) == 1

    @patch("almanak.framework.api.timeline._persist_events_to_file")
    def test_second_event_within_cooldown_is_throttled(self, _mock_persist):
        add_event(_make_event())
        add_event(_make_event())
        # Only the first should be stored
        assert len(_event_store["test-strat"]) == 1

    @patch("almanak.framework.api.timeline._persist_events_to_file")
    def test_event_after_cooldown_is_emitted(self, _mock_persist):
        add_event(_make_event())
        # Simulate cooldown expiry by backdating the throttle timestamp
        key = ("test-strat", "STRATEGY_STUCK")
        _throttle_last_emitted[key] = datetime.now(UTC) - timedelta(minutes=6)
        add_event(_make_event())
        assert len(_event_store["test-strat"]) == 2

    @patch("almanak.framework.api.timeline._persist_events_to_file")
    def test_suppressed_count_annotated_on_next_emission(self, _mock_persist):
        add_event(_make_event())
        # Suppress 3 events
        add_event(_make_event())
        add_event(_make_event())
        add_event(_make_event())
        assert len(_event_store["test-strat"]) == 1

        # Expire cooldown
        key = ("test-strat", "STRATEGY_STUCK")
        _throttle_last_emitted[key] = datetime.now(UTC) - timedelta(minutes=6)
        add_event(_make_event())
        assert len(_event_store["test-strat"]) == 2
        latest = _event_store["test-strat"][0]  # sorted newest-first
        assert "3 suppressed" in latest.description

    @patch("almanak.framework.api.timeline._persist_events_to_file")
    def test_non_throttled_event_type_passes_through(self, _mock_persist):
        """TRADE events should never be throttled."""
        for _ in range(5):
            add_event(_make_event(event_type=TimelineEventType.TRADE, description="swap"))
        assert len(_event_store["test-strat"]) == 5

    @patch("almanak.framework.api.timeline._persist_events_to_file")
    def test_different_strategies_throttled_independently(self, _mock_persist):
        add_event(_make_event(deployment_id="strat-a"))
        add_event(_make_event(deployment_id="strat-b"))
        # Both should be stored (different deployment IDs)
        assert len(_event_store.get("strat-a", [])) == 1
        assert len(_event_store.get("strat-b", [])) == 1

    @patch("almanak.framework.api.timeline._persist_events_to_file")
    def test_throttle_tracks_suppressed_count(self, _mock_persist):
        add_event(_make_event())
        add_event(_make_event())
        add_event(_make_event())
        key = ("test-strat", "STRATEGY_STUCK")
        assert _throttle_suppressed_counts[key] == 2

    @patch("almanak.framework.api.timeline._persist_events_to_file")
    def test_throttle_does_not_mutate_caller_event(self, _mock_persist):
        """The annotation should copy the event, not mutate the caller's."""
        add_event(_make_event())
        # Suppress some events
        add_event(_make_event())
        add_event(_make_event())
        # Expire cooldown
        key = ("test-strat", "STRATEGY_STUCK")
        _throttle_last_emitted[key] = datetime.now(UTC) - timedelta(minutes=6)
        original = _make_event(description="original desc")
        add_event(original)
        # The caller's event should not be mutated
        assert original.description == "original desc"

    @patch("almanak.framework.api.timeline._persist_events_to_file")
    def test_clear_events_resets_throttle_state(self, _mock_persist):
        """clear_events() must reset throttle state so next event isn't suppressed."""
        add_event(_make_event())
        add_event(_make_event())  # suppressed
        key = ("test-strat", "STRATEGY_STUCK")
        assert key in _throttle_last_emitted

        clear_events("test-strat")
        assert key not in _throttle_last_emitted
        assert key not in _throttle_suppressed_counts

        # Next event for same strategy should NOT be throttled
        add_event(_make_event())
        assert len(_event_store.get("test-strat", [])) == 1

    @patch("almanak.framework.api.timeline._persist_events_to_file")
    def test_clear_all_events_resets_all_throttle_state(self, _mock_persist):
        """clear_events() with no args resets all throttle state."""
        add_event(_make_event(deployment_id="a"))
        add_event(_make_event(deployment_id="b"))
        assert len(_throttle_last_emitted) == 2

        clear_events()
        assert len(_throttle_last_emitted) == 0
        assert len(_throttle_suppressed_counts) == 0


# ---------------------------------------------------------------------------
# Coalesce tests (dashboard page level)
# ---------------------------------------------------------------------------


class TestCoalesceConsecutiveEvents:
    """Tests for _coalesce_consecutive_events()."""

    def test_empty_list(self):
        assert _coalesce_consecutive_events([]) == []

    def test_single_event_unchanged(self):
        events = [_make_event()]
        result = _coalesce_consecutive_events(events)
        assert len(result) == 1

    def test_non_coalesceable_events_unchanged(self):
        """TRADE events should not be coalesced."""
        events = [
            _make_event(event_type=TimelineEventType.TRADE, description="swap1"),
            _make_event(event_type=TimelineEventType.TRADE, description="swap2"),
        ]
        result = _coalesce_consecutive_events(events)
        assert len(result) == 2

    def test_consecutive_stuck_events_coalesced(self):
        now = datetime.now(UTC)
        events = [
            _make_event(ts=now),
            _make_event(ts=now - timedelta(minutes=1)),
            _make_event(ts=now - timedelta(minutes=2)),
            _make_event(ts=now - timedelta(minutes=3)),
            _make_event(ts=now - timedelta(minutes=4)),
        ]
        result = _coalesce_consecutive_events(events)
        assert len(result) == 1
        assert "repeated 5x" in result[0].description
        assert "4min" in result[0].description

    def test_interleaved_events_not_coalesced(self):
        """Non-consecutive STRATEGY_STUCK events separated by another type."""
        now = datetime.now(UTC)
        events = [
            _make_event(ts=now),
            _make_event(event_type=TimelineEventType.TRADE, ts=now - timedelta(minutes=1)),
            _make_event(ts=now - timedelta(minutes=2)),
        ]
        result = _coalesce_consecutive_events(events)
        assert len(result) == 3  # Not coalesced because TRADE breaks the run

    def test_original_events_not_mutated(self):
        now = datetime.now(UTC)
        orig_desc = "Circuit breaker open: too many failures"
        events = [
            _make_event(ts=now, description=orig_desc),
            _make_event(ts=now - timedelta(minutes=1), description=orig_desc),
        ]
        _coalesce_consecutive_events(events)
        # Original events should not be modified
        assert events[0].description == orig_desc
        assert events[1].description == orig_desc

    def test_different_strategies_not_coalesced(self):
        now = datetime.now(UTC)
        events = [
            _make_event(deployment_id="a", ts=now),
            _make_event(deployment_id="b", ts=now - timedelta(minutes=1)),
        ]
        result = _coalesce_consecutive_events(events)
        assert len(result) == 2

    def test_gap_exceeding_max_breaks_run(self):
        """Events >10 min apart should NOT be coalesced into the same run."""
        now = datetime.now(UTC)
        events = [
            _make_event(ts=now),
            _make_event(ts=now - timedelta(minutes=15)),  # 15 min gap > 10 min default
        ]
        result = _coalesce_consecutive_events(events)
        assert len(result) == 2  # Not coalesced due to large gap
