"""Tests for PM integration adapters (VIB-2406)."""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.dashboard.adapters import (
    render_strategy_detail,
    render_strategy_timeline,
    strategy_from_pm_dict,
)
from almanak.framework.dashboard.models import (
    Strategy,
    StrategyStatus,
    TimelineEvent,
    TimelineEventType,
)


class TestStrategyFromPmDict:
    def test_basic_conversion(self):
        entry = {
            "deployment_id": "s1",
            "name": "My Strategy",
            "status": "RUNNING",
            "chain": "arbitrum",
            "protocol": "uniswap_v3",
            "total_value_usd": "1000",
            "pnl_24h_usd": "50",
        }
        strategy = strategy_from_pm_dict(entry)
        assert strategy.id == "s1"
        assert strategy.name == "My Strategy"
        assert strategy.status == StrategyStatus.RUNNING
        assert strategy.chain == "arbitrum"
        assert strategy.total_value_usd == Decimal("1000")
        assert strategy.pnl_24h_usd == Decimal("50")

    def test_unknown_status_defaults_to_inactive(self):
        entry = {"deployment_id": "s1", "status": "WEIRD"}
        strategy = strategy_from_pm_dict(entry)
        assert strategy.status == StrategyStatus.INACTIVE

    def test_missing_status_defaults_to_inactive(self):
        entry = {"deployment_id": "s1"}
        strategy = strategy_from_pm_dict(entry)
        assert strategy.status == StrategyStatus.INACTIVE

    def test_timestamp_from_iso_string(self):
        entry = {
            "deployment_id": "s1",
            "last_action_at": "2026-04-05T12:00:00+00:00",
        }
        strategy = strategy_from_pm_dict(entry)
        assert strategy.last_action_at is not None
        assert strategy.last_action_at.year == 2026

    def test_timestamp_from_unix(self):
        entry = {
            "deployment_id": "s1",
            "last_action_at": 1775304000,  # ~2026-04-05
        }
        strategy = strategy_from_pm_dict(entry)
        assert strategy.last_action_at is not None

    def test_missing_values_use_defaults(self):
        entry = {}
        strategy = strategy_from_pm_dict(entry)
        assert strategy.id == ""
        assert strategy.name == ""
        assert strategy.total_value_usd == Decimal("0")
        assert strategy.pnl_24h_usd == Decimal("0")

    def test_multi_chain_flag(self):
        entry = {
            "deployment_id": "s1",
            "is_multi_chain": True,
            "chains": ["arbitrum", "base"],
        }
        strategy = strategy_from_pm_dict(entry)
        assert strategy.is_multi_chain is True
        assert strategy.chains == ["arbitrum", "base"]

    def test_id_field_fallback(self):
        entry = {"id": "fallback-id"}
        strategy = strategy_from_pm_dict(entry)
        assert strategy.id == "fallback-id"

    def test_name_field_fallback(self):
        entry = {"deployment_id": "s1", "strategy_name": "Fallback Name"}
        strategy = strategy_from_pm_dict(entry)
        assert strategy.name == "Fallback Name"

    def test_value_confidence_passed_through(self):
        entry = {"deployment_id": "s1", "value_confidence": "STALE"}
        strategy = strategy_from_pm_dict(entry)
        assert strategy.value_confidence == "STALE"


# ---------------------------------------------------------------------------
# Issue #1716: render_strategy_detail / render_strategy_timeline must not
# mutate the caller's Strategy.timeline_events.
#
# These tests construct a minimal ``DashboardDataClient``-shaped mock and
# patch the Streamlit render page out so the adapter's merge step is
# exercised without booting Streamlit. The caller's ``Strategy.timeline_events``
# identity is captured before and after the call and compared.
# ---------------------------------------------------------------------------


def _make_strategy() -> Strategy:
    """Build a minimal Strategy with a pre-existing timeline list."""
    preexisting = [
        TimelineEvent(
            timestamp=datetime(2025, 1, 1, tzinfo=UTC),
            event_type=TimelineEventType.TRADE,
            description="cached event",
        )
    ]
    return Strategy(
        id="strat-1",
        name="Test Strat",
        status=StrategyStatus.RUNNING,
        pnl_24h_usd=Decimal("0"),
        total_value_usd=Decimal("0"),
        chain="arbitrum",
        protocol="Uniswap V3",
        timeline_events=preexisting,
    )


def _make_gateway_timeline_event():
    """Build a gateway-side ``TimelineEvent`` (dashboard.gateway_client variant)."""
    from almanak.framework.dashboard.gateway_client import TimelineEvent as GwTimelineEvent

    return GwTimelineEvent(
        timestamp=datetime(2025, 2, 1, tzinfo=UTC),
        event_type="TRADE",
        description="fresh event from gateway",
    )


def test_render_strategy_detail_does_not_mutate_caller_timeline() -> None:
    """#1716: caller's ``Strategy.timeline_events`` list must not be replaced."""
    strategy = _make_strategy()
    original_events_id = id(strategy.timeline_events)
    original_events_copy = list(strategy.timeline_events)

    # Build a client mock that surfaces a fresh gateway-side timeline event.
    client = MagicMock()
    client.is_connected = True
    details = MagicMock()
    details.timeline = [_make_gateway_timeline_event()]
    client.get_strategy_detail.return_value = details

    captured: list[Strategy] = []

    def _capture_page(strategies: list[Strategy]) -> None:
        # The adapter delegates to ``detail.page``; capture the strategy it
        # actually rendered so we can verify the fresh data reached Streamlit.
        captured.append(strategies[0])

    with patch("almanak.framework.dashboard.pages.detail.page", side_effect=_capture_page):
        render_strategy_detail(strategy, client=client)

    # 1) Caller's Strategy is untouched - same list identity, same contents.
    assert id(strategy.timeline_events) == original_events_id
    assert strategy.timeline_events == original_events_copy

    # 2) The strategy passed to the page renderer got the FRESH events.
    assert len(captured) == 1
    assert len(captured[0].timeline_events) == 1
    assert captured[0].timeline_events[0].description == "fresh event from gateway"
    # The rendered Strategy is a distinct object.
    assert captured[0] is not strategy


def test_render_strategy_detail_without_live_data_passes_original_through() -> None:
    """When no client is given the caller's strategy is passed to page() as-is."""
    strategy = _make_strategy()
    captured: list[Strategy] = []

    def _capture_page(strategies: list[Strategy]) -> None:
        captured.append(strategies[0])

    with patch("almanak.framework.dashboard.pages.detail.page", side_effect=_capture_page):
        render_strategy_detail(strategy, client=None)

    assert captured[0] is strategy  # identity preserved in offline mode


def test_render_strategy_timeline_does_not_mutate_caller_timeline() -> None:
    """#1716: the timeline adapter must also avoid mutating the caller."""
    strategy = _make_strategy()
    original_events_id = id(strategy.timeline_events)
    original_events_copy = list(strategy.timeline_events)

    client = MagicMock()
    client.is_connected = True
    client.get_timeline.return_value = [_make_gateway_timeline_event()]

    captured: list[Strategy] = []

    def _capture_page(strategies: list[Strategy]) -> None:
        captured.append(strategies[0])

    with patch("almanak.framework.dashboard.pages.timeline.page", side_effect=_capture_page):
        render_strategy_timeline(strategy, client=client)

    assert id(strategy.timeline_events) == original_events_id
    assert strategy.timeline_events == original_events_copy
    assert captured[0].timeline_events[0].description == "fresh event from gateway"
    assert captured[0] is not strategy
