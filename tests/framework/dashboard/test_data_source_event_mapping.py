"""Tests for dashboard data_source event type conversion."""

from almanak.framework.dashboard.data_source import _convert_event_type
from almanak.framework.dashboard.models import TimelineEventType


def test_convert_event_type_preserves_direct_swap_enum():
    assert _convert_event_type("SWAP") == TimelineEventType.SWAP


def test_convert_event_type_maps_legacy_execution_to_trade():
    assert _convert_event_type("EXECUTION") == TimelineEventType.TRADE


def test_convert_event_type_unknown_falls_back_to_trade():
    assert _convert_event_type("SOME_UNKNOWN_EVENT") == TimelineEventType.TRADE
