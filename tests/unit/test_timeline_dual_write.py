"""Tests for the timeline dual-write feature.

Tests verify that add_event() writes to both:
1. Local .dashboard_events.json (existing behavior)
2. Gateway's RecordTimelineEvent RPC (new dual-write)

Uses mock gateway client to verify RPC calls without requiring a running gateway.
"""

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.api.timeline import (
    TimelineEvent,
    TimelineEventType,
    add_event,
    get_event_gateway_client,
    get_events,
    set_event_gateway_client,
)


class TestDualWriteRegistration:
    """Tests for gateway client registration."""

    def setup_method(self):
        """Reset gateway client before each test."""
        set_event_gateway_client(None)

    def teardown_method(self):
        """Reset gateway client after each test."""
        set_event_gateway_client(None)

    def test_set_and_get_gateway_client(self):
        """Test registering and retrieving a gateway client."""
        mock_client = MagicMock()
        set_event_gateway_client(mock_client)
        assert get_event_gateway_client() is mock_client

    def test_default_no_gateway_client(self):
        """Test that no gateway client is set by default."""
        assert get_event_gateway_client() is None


class TestDualWriteBehavior:
    """Tests for dual-write add_event() behavior."""

    def setup_method(self):
        """Reset gateway client before each test."""
        set_event_gateway_client(None)

    def teardown_method(self):
        """Reset gateway client after each test."""
        set_event_gateway_client(None)

    @patch("almanak.framework.api.timeline._persist_events_to_file")
    def test_add_event_without_gateway_client(self, mock_persist):
        """Test that add_event works without gateway client (local-only)."""
        event = TimelineEvent(
            timestamp=datetime.now(UTC),
            event_type=TimelineEventType.STRATEGY_STARTED,
            description="Test event",
            strategy_id="test_strategy:abc123",
        )

        add_event(event)

        # Event should be in local store
        response = get_events("test_strategy:abc123")
        assert response.total_count == 1
        assert response.events[0].description == "Test event"

        # Local file persistence should be called
        mock_persist.assert_called()

    @patch("almanak.framework.api.timeline._persist_events_to_file")
    def test_add_event_with_gateway_client_sends_rpc(self, mock_persist):
        """Test that add_event sends RPC when gateway client is registered."""
        mock_client = MagicMock()
        mock_observe = MagicMock()
        mock_client.observe = mock_observe
        set_event_gateway_client(mock_client)

        event = TimelineEvent(
            timestamp=datetime.now(UTC),
            event_type=TimelineEventType.SWAP,
            description="Swapped 100 USDC for ETH",
            strategy_id="test_dual:xyz789",
            chain="arbitrum",
            tx_hash="0xabc123",
            details={"amount": "100"},
        )

        add_event(event)

        # Local store should have the event
        response = get_events("test_dual:xyz789")
        assert response.total_count >= 1

        # Gateway RPC should have been called
        mock_observe.RecordTimelineEvent.assert_called_once()

        # Verify the RPC request has correct fields
        call_args = mock_observe.RecordTimelineEvent.call_args
        request = call_args[0][0]  # First positional arg
        assert request.strategy_id == "test_dual:xyz789"
        assert request.event_type == "SWAP"
        assert request.description == "Swapped 100 USDC for ETH"
        assert request.tx_hash == "0xabc123"
        assert request.chain == "arbitrum"

    @patch("almanak.framework.api.timeline._persist_events_to_file")
    def test_add_event_gateway_failure_is_nonfatal(self, mock_persist):
        """Test that gateway RPC failure doesn't prevent local storage."""
        mock_client = MagicMock()
        mock_observe = MagicMock()
        mock_observe.RecordTimelineEvent.side_effect = Exception("Connection refused")
        mock_client.observe = mock_observe
        set_event_gateway_client(mock_client)

        event = TimelineEvent(
            timestamp=datetime.now(UTC),
            event_type=TimelineEventType.ERROR_OCCURRED,
            description="Something failed",
            strategy_id="test_fail:err001",
        )

        # Should not raise despite gateway failure
        add_event(event)

        # Local store should still have the event
        response = get_events("test_fail:err001")
        assert response.total_count >= 1

    @patch("almanak.framework.api.timeline._persist_events_to_file")
    def test_add_event_handles_none_optional_fields(self, mock_persist):
        """Test that None optional fields are handled correctly in RPC."""
        mock_client = MagicMock()
        mock_observe = MagicMock()
        mock_client.observe = mock_observe
        set_event_gateway_client(mock_client)

        event = TimelineEvent(
            timestamp=datetime.now(UTC),
            event_type=TimelineEventType.STRATEGY_STOPPED,
            description="Strategy stopped",
            strategy_id="test_none:stop01",
            # tx_hash, chain, details left as None/empty
        )

        add_event(event)

        # Gateway RPC should have been called with empty strings for None fields
        mock_observe.RecordTimelineEvent.assert_called_once()
        call_args = mock_observe.RecordTimelineEvent.call_args
        request = call_args[0][0]
        assert request.tx_hash == ""
        assert request.chain == ""

    @patch("almanak.framework.api.timeline._persist_events_to_file")
    def test_multiple_events_all_sent_to_gateway(self, mock_persist):
        """Test that multiple events all get sent to gateway."""
        mock_client = MagicMock()
        mock_observe = MagicMock()
        mock_client.observe = mock_observe
        set_event_gateway_client(mock_client)

        for i in range(5):
            event = TimelineEvent(
                timestamp=datetime.now(UTC),
                event_type=TimelineEventType.CUSTOM,
                description=f"Event {i}",
                strategy_id="test_multi:batch01",
            )
            add_event(event)

        assert mock_observe.RecordTimelineEvent.call_count == 5
