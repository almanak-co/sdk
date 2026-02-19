"""Tests for BaseEvent generic event wrapper."""

from datetime import UTC, datetime
from enum import Enum

from almanak.framework.connectors.base.event import BaseEvent


class MockEventType(Enum):
    """Mock event type enum for testing."""

    SWAP = "SWAP"
    MINT = "MINT"
    BURN = "BURN"
    UNKNOWN = "UNKNOWN"


class TestBaseEventCreation:
    """Tests for creating BaseEvent instances."""

    def test_create_basic_event(self):
        """Test creating a basic event."""
        event = BaseEvent[MockEventType](
            event_type=MockEventType.SWAP,
            event_name="Swap",
            log_index=5,
            transaction_hash="0xabc123",
            block_number=12345678,
            contract_address="0xdef456",
            data={"amount": 1000},
        )

        assert event.event_type == MockEventType.SWAP
        assert event.event_name == "Swap"
        assert event.log_index == 5
        assert event.transaction_hash == "0xabc123"
        assert event.block_number == 12345678
        assert event.contract_address == "0xdef456"
        assert event.data == {"amount": 1000}

    def test_create_event_with_raw_data(self):
        """Test creating event with raw topics and data."""
        topics = ["0x123", "0x456", "0x789"]
        raw_data = "0xabcdef"

        event = BaseEvent[MockEventType](
            event_type=MockEventType.MINT,
            event_name="Mint",
            log_index=10,
            transaction_hash="0xfff",
            block_number=999,
            contract_address="0xaaa",
            data={},
            raw_topics=topics,
            raw_data=raw_data,
        )

        assert event.raw_topics == topics
        assert event.raw_data == raw_data

    def test_create_event_with_timestamp(self):
        """Test creating event with custom timestamp."""
        ts = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)

        event = BaseEvent[MockEventType](
            event_type=MockEventType.BURN,
            event_name="Burn",
            log_index=0,
            transaction_hash="0x000",
            block_number=1,
            contract_address="0x111",
            data={},
            timestamp=ts,
        )

        assert event.timestamp == ts

    def test_create_event_default_timestamp(self):
        """Test that default timestamp is set to current time."""
        before = datetime.now(UTC)

        event = BaseEvent[MockEventType](
            event_type=MockEventType.SWAP,
            event_name="Swap",
            log_index=0,
            transaction_hash="0x000",
            block_number=1,
            contract_address="0x111",
            data={},
        )

        after = datetime.now(UTC)

        # Timestamp should be between before and after
        assert before <= event.timestamp <= after

    def test_create_event_empty_defaults(self):
        """Test creating event with empty default fields."""
        event = BaseEvent[MockEventType](
            event_type=MockEventType.SWAP,
            event_name="Swap",
            log_index=0,
            transaction_hash="",
            block_number=0,
            contract_address="",
            data={},
        )

        assert event.raw_topics == []
        assert event.raw_data == ""
        assert isinstance(event.timestamp, datetime)


class TestBaseEventSerialization:
    """Tests for serializing BaseEvent to dict."""

    def test_to_dict_basic(self):
        """Test converting basic event to dict."""
        event = BaseEvent[MockEventType](
            event_type=MockEventType.SWAP,
            event_name="Swap",
            log_index=5,
            transaction_hash="0xabc",
            block_number=123,
            contract_address="0xdef",
            data={"amount": 1000},
        )

        result = event.to_dict()

        assert result["event_type"] == "SWAP"
        assert result["event_name"] == "Swap"
        assert result["log_index"] == 5
        assert result["transaction_hash"] == "0xabc"
        assert result["block_number"] == 123
        assert result["contract_address"] == "0xdef"
        assert result["data"] == {"amount": 1000}
        assert "timestamp" in result

    def test_to_dict_with_raw_data(self):
        """Test converting event with raw data to dict."""
        topics = ["0x123", "0x456"]
        raw_data = "0xabcdef"

        event = BaseEvent[MockEventType](
            event_type=MockEventType.MINT,
            event_name="Mint",
            log_index=10,
            transaction_hash="0xfff",
            block_number=999,
            contract_address="0xaaa",
            data={"value": 500},
            raw_topics=topics,
            raw_data=raw_data,
        )

        result = event.to_dict()

        assert result["raw_topics"] == topics
        assert result["raw_data"] == raw_data

    def test_to_dict_timestamp_format(self):
        """Test that timestamp is serialized as ISO format."""
        ts = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)

        event = BaseEvent[MockEventType](
            event_type=MockEventType.BURN,
            event_name="Burn",
            log_index=0,
            transaction_hash="0x000",
            block_number=1,
            contract_address="0x111",
            data={},
            timestamp=ts,
        )

        result = event.to_dict()

        assert result["timestamp"] == "2024-01-01T12:00:00+00:00"
        assert isinstance(result["timestamp"], str)

    def test_to_dict_nested_data(self):
        """Test converting event with nested data structure."""
        complex_data = {
            "sender": "0xaaa",
            "recipient": "0xbbb",
            "amounts": [100, 200, 300],
            "metadata": {"fee": 30, "slippage": 0.5},
        }

        event = BaseEvent[MockEventType](
            event_type=MockEventType.SWAP,
            event_name="Swap",
            log_index=0,
            transaction_hash="0x000",
            block_number=1,
            contract_address="0x111",
            data=complex_data,
        )

        result = event.to_dict()

        assert result["data"] == complex_data
        assert result["data"]["amounts"] == [100, 200, 300]


class TestBaseEventRepresentation:
    """Tests for string representation of BaseEvent."""

    def test_repr_basic(self):
        """Test string representation."""
        event = BaseEvent[MockEventType](
            event_type=MockEventType.SWAP,
            event_name="Swap",
            log_index=5,
            transaction_hash="0xabcdef1234567890",
            block_number=12345678,
            contract_address="0xdef456",
            data={},
        )

        repr_str = repr(event)

        assert "BaseEvent" in repr_str
        assert "type=SWAP" in repr_str
        assert "name=Swap" in repr_str
        assert "log_index=5" in repr_str
        assert "0xabcdef12" in repr_str  # Truncated tx hash
        assert "block=12345678" in repr_str

    def test_repr_different_event_types(self):
        """Test repr for different event types."""
        mint_event = BaseEvent[MockEventType](
            event_type=MockEventType.MINT,
            event_name="Mint",
            log_index=0,
            transaction_hash="0x000",
            block_number=1,
            contract_address="0x111",
            data={},
        )

        burn_event = BaseEvent[MockEventType](
            event_type=MockEventType.BURN,
            event_name="Burn",
            log_index=1,
            transaction_hash="0x111",
            block_number=2,
            contract_address="0x222",
            data={},
        )

        assert "type=MINT" in repr(mint_event)
        assert "type=BURN" in repr(burn_event)


class TestBaseEventTypeChecking:
    """Tests for event type checking and validation."""

    def test_event_type_is_enum(self):
        """Test that event_type is an Enum."""
        event = BaseEvent[MockEventType](
            event_type=MockEventType.SWAP,
            event_name="Swap",
            log_index=0,
            transaction_hash="0x000",
            block_number=1,
            contract_address="0x111",
            data={},
        )

        assert isinstance(event.event_type, Enum)
        assert isinstance(event.event_type, MockEventType)

    def test_event_type_comparison(self):
        """Test comparing event types."""
        swap_event = BaseEvent[MockEventType](
            event_type=MockEventType.SWAP,
            event_name="Swap",
            log_index=0,
            transaction_hash="0x000",
            block_number=1,
            contract_address="0x111",
            data={},
        )

        mint_event = BaseEvent[MockEventType](
            event_type=MockEventType.MINT,
            event_name="Mint",
            log_index=1,
            transaction_hash="0x111",
            block_number=2,
            contract_address="0x222",
            data={},
        )

        assert swap_event.event_type == MockEventType.SWAP
        assert mint_event.event_type == MockEventType.MINT
        assert swap_event.event_type != mint_event.event_type


class TestBaseEventWithDifferentEnums:
    """Tests for using BaseEvent with different enum types."""

    def test_multiple_enum_types(self):
        """Test that BaseEvent works with different enum types."""

        class ProtocolAEventType(Enum):
            SWAP = "SWAP"
            ADD_LIQUIDITY = "ADD_LIQUIDITY"

        class ProtocolBEventType(Enum):
            DEPOSIT = "DEPOSIT"
            WITHDRAW = "WITHDRAW"

        event_a = BaseEvent[ProtocolAEventType](
            event_type=ProtocolAEventType.SWAP,
            event_name="Swap",
            log_index=0,
            transaction_hash="0x000",
            block_number=1,
            contract_address="0x111",
            data={},
        )

        event_b = BaseEvent[ProtocolBEventType](
            event_type=ProtocolBEventType.DEPOSIT,
            event_name="Deposit",
            log_index=1,
            transaction_hash="0x111",
            block_number=2,
            contract_address="0x222",
            data={},
        )

        assert event_a.event_type == ProtocolAEventType.SWAP
        assert event_b.event_type == ProtocolBEventType.DEPOSIT


class TestBaseEventDataStructures:
    """Tests for different data structures in event data."""

    def test_event_with_list_data(self):
        """Test event with list in data."""
        event = BaseEvent[MockEventType](
            event_type=MockEventType.SWAP,
            event_name="Swap",
            log_index=0,
            transaction_hash="0x000",
            block_number=1,
            contract_address="0x111",
            data={"amounts": [100, 200, 300]},
        )

        assert event.data["amounts"] == [100, 200, 300]
        dict_result = event.to_dict()
        assert dict_result["data"]["amounts"] == [100, 200, 300]

    def test_event_with_nested_dict(self):
        """Test event with nested dictionary in data."""
        event = BaseEvent[MockEventType](
            event_type=MockEventType.MINT,
            event_name="Mint",
            log_index=0,
            transaction_hash="0x000",
            block_number=1,
            contract_address="0x111",
            data={"pool": {"token0": "0xaaa", "token1": "0xbbb", "fee": 3000}},
        )

        assert event.data["pool"]["token0"] == "0xaaa"
        assert event.data["pool"]["fee"] == 3000

    def test_event_with_empty_data(self):
        """Test event with empty data dict."""
        event = BaseEvent[MockEventType](
            event_type=MockEventType.BURN,
            event_name="Burn",
            log_index=0,
            transaction_hash="0x000",
            block_number=1,
            contract_address="0x111",
            data={},
        )

        assert event.data == {}
        assert event.to_dict()["data"] == {}


class TestBaseEventImmutability:
    """Tests for event field modifications."""

    def test_modify_data_dict(self):
        """Test that we can modify the data dict (dataclass is mutable by default)."""
        event = BaseEvent[MockEventType](
            event_type=MockEventType.SWAP,
            event_name="Swap",
            log_index=0,
            transaction_hash="0x000",
            block_number=1,
            contract_address="0x111",
            data={"amount": 100},
        )

        # Dataclasses are mutable by default
        event.data["amount"] = 200
        assert event.data["amount"] == 200

    def test_modify_raw_topics(self):
        """Test modifying raw_topics list."""
        event = BaseEvent[MockEventType](
            event_type=MockEventType.SWAP,
            event_name="Swap",
            log_index=0,
            transaction_hash="0x000",
            block_number=1,
            contract_address="0x111",
            data={},
            raw_topics=["0x123"],
        )

        event.raw_topics.append("0x456")
        assert len(event.raw_topics) == 2
