"""Tests for CLOB Action Handler.

This module tests the ClobActionHandler class which handles off-chain
CLOB order execution for Polymarket.

Tests cover:
- can_handle() detection logic
- execute() order submission
- get_status() order status retrieval
- cancel() order cancellation
- Error handling and edge cases
"""

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from almanak.framework.execution.clob_handler import (
    ClobActionHandler,
    ClobExecutionResult,
    ClobFill,
    ClobOrderState,
    ClobOrderStatus,
)
from almanak.framework.models.reproduction_bundle import ActionBundle

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def mock_clob_client():
    """Create a mock ClobClient."""
    client = MagicMock()
    return client


@pytest.fixture
def handler(mock_clob_client):
    """Create a ClobActionHandler with mocked client."""
    return ClobActionHandler(clob_client=mock_clob_client)


@pytest.fixture
def handler_no_client():
    """Create a ClobActionHandler without a client."""
    return ClobActionHandler(clob_client=None)


@pytest.fixture
def valid_clob_bundle():
    """Create a valid CLOB order bundle."""
    return ActionBundle(
        intent_type="PREDICTION_BUY",
        transactions=[],  # CLOB orders have no on-chain transactions
        metadata={
            "protocol": "polymarket",
            "order_payload": {
                "order": {
                    "salt": 12345,
                    "maker": "0x1234...",
                    "signer": "0x1234...",
                    "taker": "0x0000...",
                    "tokenId": "12345...",
                    "makerAmount": "1000000000",
                    "takerAmount": "500000000",
                    "expiration": "0",
                    "nonce": "0",
                    "feeRateBps": "0",
                    "side": 0,
                    "signatureType": 0,
                },
                "signature": "0xabcdef...",
                "orderType": "GTC",
            },
            "side": "BUY",
            "size": "100",
            "price": "0.50",
            "intent_id": "test-intent-123",
        },
    )


@pytest.fixture
def on_chain_bundle():
    """Create an on-chain transaction bundle (not CLOB)."""
    return ActionBundle(
        intent_type="SWAP",
        transactions=[{"to": "0x1234...", "data": "0xabcd..."}],
        metadata={"protocol": "uniswap"},
    )


@pytest.fixture
def mock_order_response():
    """Create a mock OrderResponse."""
    response = MagicMock()
    response.order_id = "order-123"
    response.status = MagicMock()
    response.status.value = "LIVE"
    response.market = "market-456"
    response.side = "BUY"
    response.price = Decimal("0.50")
    response.size = Decimal("100")
    response.filled_size = Decimal("0")
    return response


@pytest.fixture
def mock_open_order():
    """Create a mock OpenOrder."""
    order = MagicMock()
    order.order_id = "order-123"
    order.market = "token-789"
    order.side = "BUY"
    order.price = Decimal("0.50")
    order.size = Decimal("100")
    order.filled_size = Decimal("25")
    order.created_at = datetime(2026, 1, 25, 12, 0, 0, tzinfo=UTC)
    return order


# =============================================================================
# can_handle() Tests
# =============================================================================


class TestCanHandle:
    """Tests for can_handle() method."""

    def test_handles_valid_clob_bundle(self, handler, valid_clob_bundle):
        """Test that handler accepts valid CLOB bundles."""
        assert handler.can_handle(valid_clob_bundle) is True

    def test_rejects_non_polymarket_bundle(self, handler, on_chain_bundle):
        """Test that handler rejects non-Polymarket bundles."""
        assert handler.can_handle(on_chain_bundle) is False

    def test_rejects_bundle_with_transactions(self, handler, valid_clob_bundle):
        """Test that handler rejects bundles with on-chain transactions."""
        valid_clob_bundle.transactions = [{"to": "0x123", "data": "0xabc"}]
        assert handler.can_handle(valid_clob_bundle) is False

    def test_rejects_bundle_without_order_payload(self, handler, valid_clob_bundle):
        """Test that handler rejects bundles without order_payload."""
        del valid_clob_bundle.metadata["order_payload"]
        assert handler.can_handle(valid_clob_bundle) is False

    def test_rejects_wrong_protocol(self, handler, valid_clob_bundle):
        """Test that handler rejects bundles with wrong protocol."""
        valid_clob_bundle.metadata["protocol"] = "uniswap"
        assert handler.can_handle(valid_clob_bundle) is False

    def test_rejects_missing_protocol(self, handler, valid_clob_bundle):
        """Test that handler rejects bundles without protocol."""
        del valid_clob_bundle.metadata["protocol"]
        assert handler.can_handle(valid_clob_bundle) is False


# =============================================================================
# execute() Tests
# =============================================================================


class TestExecute:
    """Tests for execute() method."""

    def test_execute_success(self, handler, mock_clob_client, valid_clob_bundle, mock_order_response):
        """Test successful order submission."""
        mock_clob_client.submit_order_payload.return_value = mock_order_response

        result = asyncio.run(handler.execute(valid_clob_bundle))

        assert result.success is True
        assert result.order_id == "order-123"
        assert result.status == ClobOrderStatus.LIVE
        assert result.error is None
        mock_clob_client.submit_order_payload.assert_called_once_with(valid_clob_bundle.metadata["order_payload"])

    def test_execute_without_client(self, handler_no_client, valid_clob_bundle):
        """Test that execute fails gracefully without client."""
        result = asyncio.run(handler_no_client.execute(valid_clob_bundle))

        assert result.success is False
        assert result.error == "CLOB client not configured"
        assert result.status == ClobOrderStatus.PENDING

    def test_execute_invalid_bundle(self, handler, on_chain_bundle):
        """Test that execute rejects invalid bundles."""
        result = asyncio.run(handler.execute(on_chain_bundle))

        assert result.success is False
        assert result.error == "Bundle is not a CLOB order"

    def test_execute_api_error(self, handler, mock_clob_client, valid_clob_bundle):
        """Test handling of API errors."""
        mock_clob_client.submit_order_payload.side_effect = Exception("API rate limit exceeded")

        result = asyncio.run(handler.execute(valid_clob_bundle))

        assert result.success is False
        assert result.status == ClobOrderStatus.FAILED
        assert "API rate limit exceeded" in result.error

    def test_execute_maps_matched_status(self, handler, mock_clob_client, valid_clob_bundle):
        """Test that MATCHED status is properly mapped."""
        mock_response = MagicMock()
        mock_response.order_id = "order-456"
        mock_response.status = MagicMock()
        mock_response.status.value = "MATCHED"
        mock_clob_client.submit_order_payload.return_value = mock_response

        result = asyncio.run(handler.execute(valid_clob_bundle))

        assert result.success is True
        assert result.status == ClobOrderStatus.MATCHED

    def test_execute_includes_submitted_at(self, handler, mock_clob_client, valid_clob_bundle, mock_order_response):
        """Test that result includes submission timestamp."""
        mock_clob_client.submit_order_payload.return_value = mock_order_response

        result = asyncio.run(handler.execute(valid_clob_bundle))

        assert result.submitted_at is not None
        assert isinstance(result.submitted_at, datetime)


# =============================================================================
# get_status() Tests
# =============================================================================


class TestGetStatus:
    """Tests for get_status() method."""

    def test_get_status_success(self, handler, mock_clob_client, mock_open_order):
        """Test successful status retrieval."""
        mock_clob_client.get_order.return_value = mock_open_order

        result = asyncio.run(handler.get_status("order-123"))

        assert result is not None
        assert result.order_id == "order-123"
        assert result.market_id == "token-789"
        assert result.side == "BUY"
        assert result.price == Decimal("0.50")
        assert result.size == Decimal("100")
        assert result.filled_size == Decimal("25")
        assert result.status == ClobOrderStatus.PARTIALLY_FILLED
        mock_clob_client.get_order.assert_called_once_with("order-123")

    def test_get_status_order_not_found(self, handler, mock_clob_client):
        """Test status when order not found."""
        mock_clob_client.get_order.return_value = None

        result = asyncio.run(handler.get_status("nonexistent-order"))

        assert result is None

    def test_get_status_without_client(self, handler_no_client):
        """Test status retrieval without client."""
        result = asyncio.run(handler_no_client.get_status("order-123"))

        assert result is None

    def test_get_status_api_error(self, handler, mock_clob_client):
        """Test status retrieval on API error."""
        mock_clob_client.get_order.side_effect = Exception("Connection timeout")

        result = asyncio.run(handler.get_status("order-123"))

        assert result is None

    def test_get_status_fully_filled(self, handler, mock_clob_client, mock_open_order):
        """Test status for fully filled order."""
        mock_open_order.filled_size = Decimal("100")  # Same as size
        mock_clob_client.get_order.return_value = mock_open_order

        result = asyncio.run(handler.get_status("order-123"))

        assert result.status == ClobOrderStatus.MATCHED

    def test_get_status_unfilled(self, handler, mock_clob_client, mock_open_order):
        """Test status for unfilled order."""
        mock_open_order.filled_size = Decimal("0")
        mock_clob_client.get_order.return_value = mock_open_order

        result = asyncio.run(handler.get_status("order-123"))

        assert result.status == ClobOrderStatus.LIVE


# =============================================================================
# cancel() Tests
# =============================================================================


class TestCancel:
    """Tests for cancel() method."""

    def test_cancel_success(self, handler, mock_clob_client):
        """Test successful order cancellation."""
        mock_clob_client.cancel_order.return_value = True

        result = asyncio.run(handler.cancel("order-123"))

        assert result is True
        mock_clob_client.cancel_order.assert_called_once_with("order-123")

    def test_cancel_failure(self, handler, mock_clob_client):
        """Test failed order cancellation."""
        mock_clob_client.cancel_order.return_value = False

        result = asyncio.run(handler.cancel("order-123"))

        assert result is False

    def test_cancel_without_client(self, handler_no_client):
        """Test cancellation without client."""
        result = asyncio.run(handler_no_client.cancel("order-123"))

        assert result is False

    def test_cancel_api_error(self, handler, mock_clob_client):
        """Test cancellation on API error."""
        mock_clob_client.cancel_order.side_effect = Exception("Order not found")

        result = asyncio.run(handler.cancel("order-123"))

        assert result is False


# =============================================================================
# Data Class Tests
# =============================================================================


class TestClobOrderState:
    """Tests for ClobOrderState dataclass."""

    def test_is_open_for_live_order(self):
        """Test is_open returns True for live orders."""
        state = ClobOrderState(
            order_id="order-1",
            market_id="market-1",
            token_id="token-1",
            side="BUY",
            status=ClobOrderStatus.LIVE,
            price=Decimal("0.50"),
            size=Decimal("100"),
        )
        assert state.is_open is True

    def test_is_open_for_matched_order(self):
        """Test is_open returns False for matched orders."""
        state = ClobOrderState(
            order_id="order-1",
            market_id="market-1",
            token_id="token-1",
            side="BUY",
            status=ClobOrderStatus.MATCHED,
            price=Decimal("0.50"),
            size=Decimal("100"),
        )
        assert state.is_open is False

    def test_is_terminal_for_cancelled_order(self):
        """Test is_terminal returns True for cancelled orders."""
        state = ClobOrderState(
            order_id="order-1",
            market_id="market-1",
            token_id="token-1",
            side="BUY",
            status=ClobOrderStatus.CANCELLED,
            price=Decimal("0.50"),
            size=Decimal("100"),
        )
        assert state.is_terminal is True

    def test_fill_percentage_calculation(self):
        """Test fill percentage calculation."""
        state = ClobOrderState(
            order_id="order-1",
            market_id="market-1",
            token_id="token-1",
            side="BUY",
            status=ClobOrderStatus.PARTIALLY_FILLED,
            price=Decimal("0.50"),
            size=Decimal("100"),
            filled_size=Decimal("25"),
        )
        assert state.fill_percentage == 25.0

    def test_to_dict_serialization(self):
        """Test to_dict produces valid dictionary."""
        state = ClobOrderState(
            order_id="order-1",
            market_id="market-1",
            token_id="token-1",
            side="BUY",
            status=ClobOrderStatus.LIVE,
            price=Decimal("0.50"),
            size=Decimal("100"),
        )
        result = state.to_dict()

        assert result["order_id"] == "order-1"
        assert result["status"] == "live"
        assert result["price"] == "0.50"

    def test_from_dict_deserialization(self):
        """Test from_dict creates valid state."""
        data = {
            "order_id": "order-1",
            "market_id": "market-1",
            "token_id": "token-1",
            "side": "BUY",
            "status": "live",
            "price": "0.50",
            "size": "100",
            "filled_size": "0",
            "fills": [],
            "submitted_at": "2026-01-25T12:00:00+00:00",
            "updated_at": "2026-01-25T12:00:00+00:00",
        }
        state = ClobOrderState.from_dict(data)

        assert state.order_id == "order-1"
        assert state.status == ClobOrderStatus.LIVE
        assert state.price == Decimal("0.50")


class TestClobFill:
    """Tests for ClobFill dataclass."""

    def test_to_dict_serialization(self):
        """Test to_dict produces valid dictionary."""
        fill = ClobFill(
            fill_id="fill-1",
            price=Decimal("0.50"),
            size=Decimal("25"),
            fee=Decimal("0.01"),
            timestamp=datetime(2026, 1, 25, 12, 0, 0, tzinfo=UTC),
        )
        result = fill.to_dict()

        assert result["fill_id"] == "fill-1"
        assert result["price"] == "0.50"
        assert result["size"] == "25"
        assert result["fee"] == "0.01"


class TestClobExecutionResult:
    """Tests for ClobExecutionResult dataclass."""

    def test_success_result(self):
        """Test creating a success result."""
        result = ClobExecutionResult(
            success=True,
            order_id="order-123",
            status=ClobOrderStatus.LIVE,
        )
        assert result.success is True
        assert result.order_id == "order-123"
        assert result.error is None

    def test_failure_result(self):
        """Test creating a failure result."""
        result = ClobExecutionResult(
            success=False,
            status=ClobOrderStatus.FAILED,
            error="Insufficient balance",
        )
        assert result.success is False
        assert result.error == "Insufficient balance"

    def test_to_dict_serialization(self):
        """Test to_dict produces valid dictionary."""
        result = ClobExecutionResult(
            success=True,
            order_id="order-123",
            status=ClobOrderStatus.LIVE,
        )
        data = result.to_dict()

        assert data["success"] is True
        assert data["order_id"] == "order-123"
        assert data["status"] == "live"


# =============================================================================
# Status Mapping Tests
# =============================================================================


class TestStatusMapping:
    """Tests for API status mapping."""

    def test_map_api_status_live(self, handler):
        """Test mapping LIVE status."""
        assert handler._map_api_status("LIVE") == ClobOrderStatus.LIVE

    def test_map_api_status_open(self, handler):
        """Test mapping OPEN status (alias for LIVE)."""
        assert handler._map_api_status("OPEN") == ClobOrderStatus.LIVE

    def test_map_api_status_matched(self, handler):
        """Test mapping MATCHED status."""
        assert handler._map_api_status("MATCHED") == ClobOrderStatus.MATCHED

    def test_map_api_status_filled(self, handler):
        """Test mapping FILLED status (alias for MATCHED)."""
        assert handler._map_api_status("FILLED") == ClobOrderStatus.MATCHED

    def test_map_api_status_cancelled(self, handler):
        """Test mapping CANCELLED status."""
        assert handler._map_api_status("CANCELLED") == ClobOrderStatus.CANCELLED

    def test_map_api_status_canceled_us_spelling(self, handler):
        """Test mapping CANCELED status (US spelling)."""
        assert handler._map_api_status("CANCELED") == ClobOrderStatus.CANCELLED

    def test_map_api_status_unknown(self, handler):
        """Test mapping unknown status defaults to PENDING."""
        assert handler._map_api_status("UNKNOWN_STATUS") == ClobOrderStatus.PENDING

    def test_map_api_status_case_insensitive(self, handler):
        """Test mapping is case insensitive."""
        assert handler._map_api_status("live") == ClobOrderStatus.LIVE
        assert handler._map_api_status("Live") == ClobOrderStatus.LIVE
