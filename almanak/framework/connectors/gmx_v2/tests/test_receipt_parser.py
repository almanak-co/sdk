"""Tests for GMX v2 Receipt Parser.

This test suite covers:
- Event parsing from receipts
- Position increase/decrease event parsing
- Order event parsing
- Event type detection
"""

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from ..receipt_parser import (
    EVENT_NAME_TO_TYPE,
    EVENT_TOPICS,
    TOPIC_TO_EVENT,
    GMXv2Event,
    GMXv2EventType,
    GMXv2ReceiptParser,
    OrderEventData,
    ParseResult,
    PositionDecreaseData,
    PositionIncreaseData,
)

# =============================================================================
# GMXv2Event Tests
# =============================================================================


class TestGMXv2Event:
    """Tests for GMXv2Event dataclass."""

    def test_event_creation(self) -> None:
        """Test event creation."""
        event = GMXv2Event(
            event_type=GMXv2EventType.POSITION_INCREASE,
            event_name="PositionIncrease",
            log_index=0,
            transaction_hash="0x1234",
            block_number=12345678,
            contract_address="0x5678",
            data={"key": "value"},
        )

        assert event.event_type == GMXv2EventType.POSITION_INCREASE
        assert event.event_name == "PositionIncrease"
        assert event.log_index == 0
        assert event.transaction_hash == "0x1234"
        assert event.block_number == 12345678

    def test_event_to_dict(self) -> None:
        """Test event serialization."""
        event = GMXv2Event(
            event_type=GMXv2EventType.POSITION_INCREASE,
            event_name="PositionIncrease",
            log_index=0,
            transaction_hash="0x1234",
            block_number=12345678,
            contract_address="0x5678",
            data={"key": "value"},
        )

        event_dict = event.to_dict()

        assert event_dict["event_type"] == "POSITION_INCREASE"
        assert event_dict["event_name"] == "PositionIncrease"
        assert event_dict["data"] == {"key": "value"}

    def test_event_from_dict(self) -> None:
        """Test event deserialization."""
        data = {
            "event_type": "POSITION_INCREASE",
            "event_name": "PositionIncrease",
            "log_index": 0,
            "transaction_hash": "0x1234",
            "block_number": 12345678,
            "contract_address": "0x5678",
            "data": {"key": "value"},
            "timestamp": datetime.now(UTC).isoformat(),
        }

        event = GMXv2Event.from_dict(data)

        assert event.event_type == GMXv2EventType.POSITION_INCREASE
        assert event.event_name == "PositionIncrease"
        assert event.data == {"key": "value"}


# =============================================================================
# PositionIncreaseData Tests
# =============================================================================


class TestPositionIncreaseData:
    """Tests for PositionIncreaseData dataclass."""

    def test_position_increase_creation(self) -> None:
        """Test position increase data creation."""
        data = PositionIncreaseData(
            key="0x1234",
            account="0x5678",
            market="0xabcd",
            collateral_token="0xef01",
            is_long=True,
            size_in_usd=Decimal("5000"),
            size_in_tokens=Decimal("2.5"),
            collateral_amount=Decimal("1000"),
            execution_price=Decimal("2000"),
        )

        assert data.key == "0x1234"
        assert data.is_long is True
        assert data.size_in_usd == Decimal("5000")
        assert data.execution_price == Decimal("2000")

    def test_position_increase_to_dict(self) -> None:
        """Test position increase data serialization."""
        data = PositionIncreaseData(
            key="0x1234",
            account="0x5678",
            market="0xabcd",
            collateral_token="0xef01",
            is_long=True,
            size_in_usd=Decimal("5000"),
            size_in_tokens=Decimal("2.5"),
            collateral_amount=Decimal("1000"),
        )

        data_dict = data.to_dict()

        assert data_dict["key"] == "0x1234"
        assert data_dict["size_in_usd"] == "5000"
        assert data_dict["is_long"] is True


# =============================================================================
# PositionDecreaseData Tests
# =============================================================================


class TestPositionDecreaseData:
    """Tests for PositionDecreaseData dataclass."""

    def test_position_decrease_creation(self) -> None:
        """Test position decrease data creation."""
        data = PositionDecreaseData(
            key="0x1234",
            account="0x5678",
            market="0xabcd",
            collateral_token="0xef01",
            is_long=True,
            size_in_usd=Decimal("3000"),
            size_in_tokens=Decimal("1.5"),
            collateral_amount=Decimal("800"),
            execution_price=Decimal("2100"),
            realized_pnl=Decimal("150"),
        )

        assert data.key == "0x1234"
        assert data.size_in_usd == Decimal("3000")
        assert data.realized_pnl == Decimal("150")

    def test_position_decrease_to_dict(self) -> None:
        """Test position decrease data serialization."""
        data = PositionDecreaseData(
            key="0x1234",
            account="0x5678",
            market="0xabcd",
            collateral_token="0xef01",
            is_long=True,
            size_in_usd=Decimal("3000"),
            size_in_tokens=Decimal("1.5"),
            collateral_amount=Decimal("800"),
            realized_pnl=Decimal("150"),
        )

        data_dict = data.to_dict()

        assert data_dict["key"] == "0x1234"
        assert data_dict["realized_pnl"] == "150"


# =============================================================================
# OrderEventData Tests
# =============================================================================


class TestOrderEventData:
    """Tests for OrderEventData dataclass."""

    def test_order_event_creation(self) -> None:
        """Test order event data creation."""
        data = OrderEventData(
            key="0x1234",
            account="0x5678",
            receiver="0x5678",
            market="0xabcd",
            initial_collateral_token="0xef01",
            order_type=0,
            is_long=True,
            size_delta_usd=Decimal("5000"),
        )

        assert data.key == "0x1234"
        assert data.order_type == 0
        assert data.is_long is True

    def test_order_event_cancelled(self) -> None:
        """Test cancelled order event data."""
        data = OrderEventData(
            key="0x1234",
            account="0x5678",
            receiver="0x5678",
            market="0xabcd",
            initial_collateral_token="0xef01",
            order_type=0,
            cancelled_reason="User cancelled",
        )

        assert data.cancelled_reason == "User cancelled"

    def test_order_event_frozen(self) -> None:
        """Test frozen order event data."""
        data = OrderEventData(
            key="0x1234",
            account="0x5678",
            receiver="0x5678",
            market="0xabcd",
            initial_collateral_token="0xef01",
            order_type=0,
            is_frozen=True,
            frozen_reason="Execution failed",
        )

        assert data.is_frozen is True
        assert data.frozen_reason == "Execution failed"

    def test_order_event_to_dict(self) -> None:
        """Test order event data serialization."""
        data = OrderEventData(
            key="0x1234",
            account="0x5678",
            receiver="0x5678",
            market="0xabcd",
            initial_collateral_token="0xef01",
            order_type=0,
            is_long=True,
            size_delta_usd=Decimal("5000"),
        )

        data_dict = data.to_dict()

        assert data_dict["key"] == "0x1234"
        assert data_dict["size_delta_usd"] == "5000"


# =============================================================================
# ParseResult Tests
# =============================================================================


class TestParseResult:
    """Tests for ParseResult dataclass."""

    def test_parse_result_success(self) -> None:
        """Test successful parse result."""
        result = ParseResult(
            success=True,
            transaction_hash="0x1234",
            block_number=12345678,
        )

        assert result.success is True
        assert result.error is None
        assert len(result.events) == 0

    def test_parse_result_failure(self) -> None:
        """Test failed parse result."""
        result = ParseResult(
            success=False,
            error="Parse error",
        )

        assert result.success is False
        assert result.error == "Parse error"

    def test_parse_result_with_events(self) -> None:
        """Test parse result with events."""
        event = GMXv2Event(
            event_type=GMXv2EventType.POSITION_INCREASE,
            event_name="PositionIncrease",
            log_index=0,
            transaction_hash="0x1234",
            block_number=12345678,
            contract_address="0x5678",
            data={},
        )

        result = ParseResult(
            success=True,
            events=[event],
            transaction_hash="0x1234",
            block_number=12345678,
        )

        assert len(result.events) == 1
        assert result.events[0].event_type == GMXv2EventType.POSITION_INCREASE

    def test_parse_result_to_dict(self) -> None:
        """Test parse result serialization."""
        result = ParseResult(
            success=True,
            transaction_hash="0x1234",
            block_number=12345678,
        )

        result_dict = result.to_dict()

        assert result_dict["success"] is True
        assert result_dict["transaction_hash"] == "0x1234"


# =============================================================================
# GMXv2ReceiptParser Tests
# =============================================================================


class TestGMXv2ReceiptParser:
    """Tests for GMXv2ReceiptParser."""

    @pytest.fixture
    def parser(self) -> GMXv2ReceiptParser:
        """Create parser for testing."""
        return GMXv2ReceiptParser()

    def test_parser_creation(self, parser: GMXv2ReceiptParser) -> None:
        """Test parser creation."""
        assert parser is not None
        assert len(parser._known_topics) > 0

    def test_parse_empty_receipt(self, parser: GMXv2ReceiptParser) -> None:
        """Test parsing empty receipt."""
        receipt = {
            "transactionHash": "0x1234",
            "blockNumber": 12345678,
            "logs": [],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.events) == 0

    def test_parse_receipt_with_position_increase(self, parser: GMXv2ReceiptParser) -> None:
        """Test parsing receipt with PositionIncrease event."""
        # Create mock log with PositionIncrease topic
        log = {
            "topics": [EVENT_TOPICS["PositionIncrease"]],
            "data": "0x" + "00" * 320,  # Mock data
            "address": "0x1234567890123456789012345678901234567890",
            "logIndex": 0,
        }

        receipt = {
            "transactionHash": "0x1234",
            "blockNumber": 12345678,
            "logs": [log],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.events) == 1
        assert result.events[0].event_type == GMXv2EventType.POSITION_INCREASE
        assert result.events[0].event_name == "PositionIncrease"

    def test_parse_receipt_with_position_decrease(self, parser: GMXv2ReceiptParser) -> None:
        """Test parsing receipt with PositionDecrease event."""
        log = {
            "topics": [EVENT_TOPICS["PositionDecrease"]],
            "data": "0x" + "00" * 320,
            "address": "0x1234567890123456789012345678901234567890",
            "logIndex": 0,
        }

        receipt = {
            "transactionHash": "0x1234",
            "blockNumber": 12345678,
            "logs": [log],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.events) == 1
        assert result.events[0].event_type == GMXv2EventType.POSITION_DECREASE

    def test_parse_receipt_with_order_created(self, parser: GMXv2ReceiptParser) -> None:
        """Test parsing receipt with OrderCreated event."""
        log = {
            "topics": [EVENT_TOPICS["OrderCreated"]],
            "data": "0x" + "00" * 256,
            "address": "0x1234567890123456789012345678901234567890",
            "logIndex": 0,
        }

        receipt = {
            "transactionHash": "0x1234",
            "blockNumber": 12345678,
            "logs": [log],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.events) == 1
        assert result.events[0].event_type == GMXv2EventType.ORDER_CREATED

    def test_parse_receipt_with_order_executed(self, parser: GMXv2ReceiptParser) -> None:
        """Test parsing receipt with OrderExecuted event."""
        log = {
            "topics": [EVENT_TOPICS["OrderExecuted"]],
            "data": "0x" + "00" * 256,
            "address": "0x1234567890123456789012345678901234567890",
            "logIndex": 0,
        }

        receipt = {
            "transactionHash": "0x1234",
            "blockNumber": 12345678,
            "logs": [log],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.events) == 1
        assert result.events[0].event_type == GMXv2EventType.ORDER_EXECUTED

    def test_parse_receipt_with_order_cancelled(self, parser: GMXv2ReceiptParser) -> None:
        """Test parsing receipt with OrderCancelled event."""
        log = {
            "topics": [EVENT_TOPICS["OrderCancelled"]],
            "data": "0x" + "00" * 256,
            "address": "0x1234567890123456789012345678901234567890",
            "logIndex": 0,
        }

        receipt = {
            "transactionHash": "0x1234",
            "blockNumber": 12345678,
            "logs": [log],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.events) == 1
        assert result.events[0].event_type == GMXv2EventType.ORDER_CANCELLED

    def test_parse_receipt_with_unknown_event(self, parser: GMXv2ReceiptParser) -> None:
        """Test parsing receipt with unknown event."""
        log = {
            "topics": ["0xunknowntopic000000000000000000000000000000000000000000000000"],
            "data": "0x00",
            "address": "0x1234567890123456789012345678901234567890",
            "logIndex": 0,
        }

        receipt = {
            "transactionHash": "0x1234",
            "blockNumber": 12345678,
            "logs": [log],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.events) == 0  # Unknown events are skipped

    def test_parse_receipt_with_multiple_events(self, parser: GMXv2ReceiptParser) -> None:
        """Test parsing receipt with multiple events."""
        logs = [
            {
                "topics": [EVENT_TOPICS["OrderCreated"]],
                "data": "0x" + "00" * 256,
                "address": "0x1234567890123456789012345678901234567890",
                "logIndex": 0,
            },
            {
                "topics": [EVENT_TOPICS["PositionIncrease"]],
                "data": "0x" + "00" * 320,
                "address": "0x1234567890123456789012345678901234567890",
                "logIndex": 1,
            },
            {
                "topics": [EVENT_TOPICS["OrderExecuted"]],
                "data": "0x" + "00" * 256,
                "address": "0x1234567890123456789012345678901234567890",
                "logIndex": 2,
            },
        ]

        receipt = {
            "transactionHash": "0x1234",
            "blockNumber": 12345678,
            "logs": logs,
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.events) == 3
        assert result.events[0].event_type == GMXv2EventType.ORDER_CREATED
        assert result.events[1].event_type == GMXv2EventType.POSITION_INCREASE
        assert result.events[2].event_type == GMXv2EventType.ORDER_EXECUTED

    def test_parse_receipt_bytes_transaction_hash(self, parser: GMXv2ReceiptParser) -> None:
        """Test parsing receipt with bytes transaction hash."""
        receipt = {
            "transactionHash": bytes.fromhex("1234567890abcdef" * 4),
            "blockNumber": 12345678,
            "logs": [],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert result.transaction_hash.startswith("0x")

    def test_parse_logs(self, parser: GMXv2ReceiptParser) -> None:
        """Test parsing logs directly."""
        logs = [
            {
                "topics": [EVENT_TOPICS["PositionIncrease"]],
                "data": "0x" + "00" * 320,
                "address": "0x1234567890123456789012345678901234567890",
                "logIndex": 0,
            },
        ]

        events = parser.parse_logs(logs)

        assert len(events) == 1
        assert events[0].event_type == GMXv2EventType.POSITION_INCREASE

    def test_is_gmx_event(self, parser: GMXv2ReceiptParser) -> None:
        """Test checking if topic is GMX event."""
        assert parser.is_gmx_event(EVENT_TOPICS["PositionIncrease"]) is True
        assert parser.is_gmx_event(EVENT_TOPICS["OrderCreated"]) is True
        assert parser.is_gmx_event("0xunknown") is False

    def test_get_event_type(self, parser: GMXv2ReceiptParser) -> None:
        """Test getting event type from topic."""
        assert parser.get_event_type(EVENT_TOPICS["PositionIncrease"]) == GMXv2EventType.POSITION_INCREASE
        assert parser.get_event_type(EVENT_TOPICS["OrderCreated"]) == GMXv2EventType.ORDER_CREATED
        assert parser.get_event_type("0xunknown") == GMXv2EventType.UNKNOWN


# =============================================================================
# Event Type Mapping Tests
# =============================================================================


class TestEventTypeMappings:
    """Tests for event type mappings."""

    def test_event_topics_exist(self) -> None:
        """Test that all expected event topics exist."""
        expected_events = [
            "OrderCreated",
            "OrderExecuted",
            "OrderCancelled",
            "OrderFrozen",
            "PositionIncrease",
            "PositionDecrease",
            "DepositCreated",
            "DepositExecuted",
            "WithdrawalCreated",
            "WithdrawalExecuted",
        ]

        for event in expected_events:
            assert event in EVENT_TOPICS, f"Missing event topic: {event}"

    def test_topic_to_event_reverse_mapping(self) -> None:
        """Test reverse mapping from topic to event name."""
        for event_name, topic in EVENT_TOPICS.items():
            assert TOPIC_TO_EVENT.get(topic) == event_name

    def test_event_name_to_type_mapping(self) -> None:
        """Test mapping from event name to event type."""
        assert EVENT_NAME_TO_TYPE["PositionIncrease"] == GMXv2EventType.POSITION_INCREASE
        assert EVENT_NAME_TO_TYPE["PositionDecrease"] == GMXv2EventType.POSITION_DECREASE
        assert EVENT_NAME_TO_TYPE["OrderCreated"] == GMXv2EventType.ORDER_CREATED
        assert EVENT_NAME_TO_TYPE["OrderExecuted"] == GMXv2EventType.ORDER_EXECUTED
        assert EVENT_NAME_TO_TYPE["OrderCancelled"] == GMXv2EventType.ORDER_CANCELLED
        assert EVENT_NAME_TO_TYPE["OrderFrozen"] == GMXv2EventType.ORDER_FROZEN


# =============================================================================
# Event Type Enum Tests
# =============================================================================


class TestGMXv2EventType:
    """Tests for GMXv2EventType enum."""

    def test_all_event_types_exist(self) -> None:
        """Test that all expected event types exist."""
        expected_types = [
            "ORDER_CREATED",
            "ORDER_EXECUTED",
            "ORDER_CANCELLED",
            "ORDER_FROZEN",
            "POSITION_INCREASE",
            "POSITION_DECREASE",
            "DEPOSIT_CREATED",
            "DEPOSIT_EXECUTED",
            "WITHDRAWAL_CREATED",
            "WITHDRAWAL_EXECUTED",
            "UNKNOWN",
        ]

        for type_name in expected_types:
            assert hasattr(GMXv2EventType, type_name), f"Missing event type: {type_name}"

    def test_event_type_values(self) -> None:
        """Test event type enum values."""
        assert GMXv2EventType.ORDER_CREATED.value == "ORDER_CREATED"
        assert GMXv2EventType.POSITION_INCREASE.value == "POSITION_INCREASE"
        assert GMXv2EventType.UNKNOWN.value == "UNKNOWN"
