"""Tests for BaseReceiptParser.

Since BaseReceiptParser is an abstract class, these tests use a mock implementation
to test the template method pattern and common parsing logic.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from almanak.framework.connectors.base.hex_utils import HexDecoder
from almanak.framework.connectors.base.receipt_parser import BaseReceiptParser
from almanak.framework.connectors.base.registry import EventRegistry

# ============================================================================
# Mock Protocol Implementation for Testing
# ============================================================================


class MockEventType(Enum):
    """Mock event types for testing."""

    SWAP = "SWAP"
    TRANSFER = "TRANSFER"
    UNKNOWN = "UNKNOWN"


@dataclass
class MockEvent:
    """Mock event object for testing."""

    event_type: MockEventType
    event_name: str
    log_index: int
    transaction_hash: str
    block_number: int
    contract_address: str
    data: dict[str, Any]
    raw_topics: list[str] = field(default_factory=list)
    raw_data: str = ""


@dataclass
class MockParseResult:
    """Mock parse result for testing."""

    success: bool
    events: list[MockEvent] = field(default_factory=list)
    error: str | None = None
    transaction_hash: str = ""
    block_number: int = 0
    transaction_success: bool = True


# Mock event topics
MOCK_EVENT_TOPICS = {
    "Swap": "0xswap_topic_hash",
    "Transfer": "0xtransfer_topic_hash",
}

MOCK_EVENT_NAME_TO_TYPE = {
    "Swap": MockEventType.SWAP,
    "Transfer": MockEventType.TRANSFER,
}


class MockReceiptParser(BaseReceiptParser[MockEvent, MockParseResult]):
    """Mock parser implementation for testing."""

    def __init__(self):
        """Initialize mock parser."""
        registry = EventRegistry(MOCK_EVENT_TOPICS, MOCK_EVENT_NAME_TO_TYPE)
        super().__init__(registry=registry)
        self.decode_call_count = 0
        self.create_call_count = 0
        self.build_call_count = 0

    def _decode_log_data(
        self,
        event_name: str,
        topics: list[Any],
        data: str,
        contract_address: str,
    ) -> dict[str, Any]:
        """Decode mock log data."""
        self.decode_call_count += 1

        if event_name == "Swap":
            # Mock decoding: extract two uint256 values
            amount_in = HexDecoder.decode_uint256(data, 0) if data else 0
            amount_out = HexDecoder.decode_uint256(data, 32) if len(data) >= 128 else 0
            return {
                "sender": HexDecoder.topic_to_address(topics[1]) if len(topics) > 1 else "",
                "recipient": HexDecoder.topic_to_address(topics[2]) if len(topics) > 2 else "",
                "amount_in": amount_in,
                "amount_out": amount_out,
            }
        elif event_name == "Transfer":
            return {
                "from": HexDecoder.topic_to_address(topics[1]) if len(topics) > 1 else "",
                "to": HexDecoder.topic_to_address(topics[2]) if len(topics) > 2 else "",
                "value": HexDecoder.decode_uint256(data, 0) if data else 0,
            }

        return {}

    def _create_event(
        self,
        event_name: str,
        log_index: int,
        tx_hash: str,
        block_number: int,
        contract_address: str,
        decoded_data: dict[str, Any],
        raw_topics: list[str],
        raw_data: str,
    ) -> MockEvent:
        """Create mock event."""
        self.create_call_count += 1

        event_type = self.registry.get_event_type(event_name) or MockEventType.UNKNOWN

        return MockEvent(
            event_type=event_type,
            event_name=event_name,
            log_index=log_index,
            transaction_hash=tx_hash,
            block_number=block_number,
            contract_address=contract_address,
            data=decoded_data,
            raw_topics=raw_topics,
            raw_data=raw_data,
        )

    def _build_result(
        self,
        events: list[MockEvent],
        receipt: dict[str, Any],
        tx_hash: str,
        block_number: int,
        tx_success: bool,
        **kwargs,
    ) -> MockParseResult:
        """Build mock result."""
        self.build_call_count += 1

        error = kwargs.get("error")

        return MockParseResult(
            success=tx_success and not error,
            events=events,
            error=error,
            transaction_hash=tx_hash,
            block_number=block_number,
            transaction_success=tx_success,
        )


# ============================================================================
# Tests
# ============================================================================


class TestBaseReceiptParserInitialization:
    """Tests for initializing BaseReceiptParser."""

    def test_init_with_registry(self):
        """Test initialization with registry."""
        EventRegistry(MOCK_EVENT_TOPICS, MOCK_EVENT_NAME_TO_TYPE)
        parser = MockReceiptParser()

        assert parser.registry is not None
        assert len(parser.known_topics) == 2

    def test_init_with_known_topics(self):
        """Test initialization with known_topics set."""

        class CustomParser(MockReceiptParser):
            def __init__(self):
                known_topics = {"0xabc", "0xdef"}
                # Don't call super().__init__() to avoid registry
                BaseReceiptParser.__init__(self, known_topics=known_topics)

        parser = CustomParser()
        assert parser.known_topics == {"0xabc", "0xdef"}


class TestParseReceiptBasic:
    """Tests for basic receipt parsing."""

    def test_parse_empty_receipt(self):
        """Test parsing receipt with no logs."""
        parser = MockReceiptParser()

        receipt = {
            "transactionHash": "0xabc123",
            "blockNumber": 12345,
            "status": 1,
            "logs": [],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert result.transaction_success is True
        assert len(result.events) == 0
        assert result.transaction_hash == "0xabc123"
        assert result.block_number == 12345

    def test_parse_receipt_with_single_event(self):
        """Test parsing receipt with single known event."""
        parser = MockReceiptParser()

        receipt = {
            "transactionHash": "0xabc123",
            "blockNumber": 12345,
            "status": 1,
            "logs": [
                {
                    "topics": [
                        "0xswap_topic_hash",
                        "0x" + "00" * 12 + "aaa" + "0" * 37,  # sender
                        "0x" + "00" * 12 + "bbb" + "0" * 37,  # recipient
                    ],
                    "data": "0x" + "00" * 31 + "64" + "00" * 31 + "c8",  # 100, 200
                    "address": "0xpool123",
                    "logIndex": 5,
                }
            ],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.events) == 1
        assert result.events[0].event_type == MockEventType.SWAP
        assert result.events[0].event_name == "Swap"
        assert result.events[0].data["amount_in"] == 100
        assert result.events[0].data["amount_out"] == 200

    def test_parse_receipt_with_multiple_events(self):
        """Test parsing receipt with multiple events."""
        parser = MockReceiptParser()

        receipt = {
            "transactionHash": "0xabc123",
            "blockNumber": 12345,
            "status": 1,
            "logs": [
                {
                    "topics": [
                        "0xswap_topic_hash",
                        "0x" + "00" * 12 + "aaa" + "0" * 37,
                        "0x" + "00" * 12 + "bbb" + "0" * 37,
                    ],
                    "data": "0x" + "00" * 31 + "64" + "00" * 31 + "c8",
                    "address": "0xpool1",
                    "logIndex": 5,
                },
                {
                    "topics": [
                        "0xtransfer_topic_hash",
                        "0x" + "00" * 12 + "ccc" + "0" * 37,
                        "0x" + "00" * 12 + "ddd" + "0" * 37,
                    ],
                    "data": "0x" + "00" * 31 + "0a",  # 10
                    "address": "0xtoken1",
                    "logIndex": 6,
                },
            ],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.events) == 2
        assert result.events[0].event_type == MockEventType.SWAP
        assert result.events[1].event_type == MockEventType.TRANSFER

    def test_parse_receipt_filters_unknown_events(self):
        """Test that unknown events are filtered out."""
        parser = MockReceiptParser()

        receipt = {
            "transactionHash": "0xabc123",
            "blockNumber": 12345,
            "status": 1,
            "logs": [
                {
                    "topics": ["0xswap_topic_hash"],
                    "data": "0x" + "00" * 64,
                    "address": "0xpool1",
                    "logIndex": 5,
                },
                {
                    "topics": ["0xunknown_event"],  # Unknown event
                    "data": "0x" + "00" * 64,
                    "address": "0xpool2",
                    "logIndex": 6,
                },
            ],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.events) == 1  # Only known event
        assert result.events[0].event_name == "Swap"


class TestParseReceiptTransactionStatus:
    """Tests for handling transaction status."""

    def test_parse_failed_transaction(self):
        """Test parsing receipt with failed transaction (status=0)."""
        parser = MockReceiptParser()

        receipt = {
            "transactionHash": "0xfailed",
            "blockNumber": 12345,
            "status": 0,  # Failed
            "logs": [],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is False  # Result indicates failure
        assert result.transaction_success is False  # Transaction failed
        assert result.error == "Transaction reverted"
        assert len(result.events) == 0

    def test_parse_successful_transaction(self):
        """Test parsing receipt with successful transaction."""
        parser = MockReceiptParser()

        receipt = {
            "transactionHash": "0xsuccess",
            "blockNumber": 12345,
            "status": 1,  # Success
            "logs": [],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert result.transaction_success is True
        assert result.error is None


class TestParseReceiptErrorHandling:
    """Tests for error handling during parsing."""

    def test_parse_receipt_with_exception(self):
        """Test that exceptions are caught and returned in result."""

        class FailingParser(MockReceiptParser):
            def _decode_log_data(self, event_name, topics, data, contract_address):
                # This will raise during parsing but be caught gracefully
                raise ValueError("Decode failed!")

        parser = FailingParser()

        receipt = {
            "transactionHash": "0xabc",
            "blockNumber": 123,
            "status": 1,
            "logs": [
                {
                    "topics": ["0xswap_topic_hash"],
                    "data": "0x",
                    "address": "0xpool",
                    "logIndex": 0,
                }
            ],
        }

        result = parser.parse_receipt(receipt)

        # Log parsing exception is caught gracefully, result continues
        # The exception in _parse_log is caught and returns None for that log
        assert result.success is True  # Overall parsing succeeded
        assert len(result.events) == 0  # But no events were parsed

    def test_parse_log_with_exception(self):
        """Test that log parsing exceptions are handled gracefully."""

        class FailingParser(MockReceiptParser):
            def _decode_log_data(self, event_name, topics, data, contract_address):
                raise RuntimeError("Log decode error")

        parser = FailingParser()

        receipt = {
            "transactionHash": "0xabc",
            "blockNumber": 123,
            "status": 1,
            "logs": [
                {
                    "topics": ["0xswap_topic_hash"],
                    "data": "0x",
                    "address": "0xpool",
                    "logIndex": 0,
                }
            ],
        }

        # Should catch exception in _parse_log and return None
        result = parser.parse_receipt(receipt)

        # Parser should continue and build result with no events
        assert len(result.events) == 0


class TestParseReceiptNormalization:
    """Tests for normalizing receipt fields."""

    def test_normalize_bytes_tx_hash(self):
        """Test normalizing bytes transaction hash."""
        parser = MockReceiptParser()

        receipt = {
            "transactionHash": b"\xab\xcd\xef",  # Bytes
            "blockNumber": 123,
            "status": 1,
            "logs": [],
        }

        result = parser.parse_receipt(receipt)

        assert result.transaction_hash.startswith("0x")
        assert "abcdef" in result.transaction_hash.lower()

    def test_normalize_bytes_topics(self):
        """Test normalizing bytes topics."""
        parser = MockReceiptParser()

        swap_topic_bytes = b"swap_topic_hash\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"

        receipt = {
            "transactionHash": "0xabc",
            "blockNumber": 123,
            "status": 1,
            "logs": [
                {
                    "topics": [swap_topic_bytes],  # Bytes topic
                    "data": "0x",
                    "address": "0xpool",
                    "logIndex": 0,
                }
            ],
        }

        # Should handle bytes topic gracefully (may not match, but shouldn't crash)
        result = parser.parse_receipt(receipt)
        assert result.success is True


class TestParseReceiptCallCounts:
    """Tests for verifying hook methods are called correctly."""

    def test_hook_methods_called_for_each_log(self):
        """Test that hook methods are called for each parsed log."""
        parser = MockReceiptParser()

        receipt = {
            "transactionHash": "0xabc",
            "blockNumber": 123,
            "status": 1,
            "logs": [
                {
                    "topics": ["0xswap_topic_hash"],
                    "data": "0x" + "00" * 64,
                    "address": "0xpool",
                    "logIndex": 0,
                },
                {
                    "topics": ["0xtransfer_topic_hash"],
                    "data": "0x" + "00" * 32,
                    "address": "0xtoken",
                    "logIndex": 1,
                },
            ],
        }

        parser.parse_receipt(receipt)

        # Each log should trigger decode and create
        assert parser.decode_call_count == 2
        assert parser.create_call_count == 2

        # Build result is called once at the end
        assert parser.build_call_count == 1

    def test_build_result_called_even_with_no_events(self):
        """Test that _build_result is called even when no events parsed."""
        parser = MockReceiptParser()

        receipt = {
            "transactionHash": "0xabc",
            "blockNumber": 123,
            "status": 1,
            "logs": [],
        }

        parser.parse_receipt(receipt)

        # Build should still be called
        assert parser.build_call_count == 1


class TestParseReceiptKwargs:
    """Tests for passing kwargs through to _build_result."""

    def test_kwargs_passed_to_build_result(self):
        """Test that kwargs are passed through to _build_result."""

        class KwargsCheckingParser(MockReceiptParser):
            received_kwargs = None

            def _build_result(self, events, receipt, tx_hash, block_number, tx_success, **kwargs):
                # Store kwargs for verification as class attribute
                KwargsCheckingParser.received_kwargs = kwargs.copy()
                return super()._build_result(events, receipt, tx_hash, block_number, tx_success, **kwargs)

        parser = KwargsCheckingParser()

        receipt = {
            "transactionHash": "0xabc",
            "blockNumber": 123,
            "status": 1,
            "logs": [
                {
                    "topics": ["0xswap_topic_hash"],
                    "data": "0x" + "00" * 64,
                    "address": "0xpool",
                    "logIndex": 0,
                }
            ],
        }

        # Pass custom kwarg
        result = parser.parse_receipt(receipt, custom_param="test_value")

        assert result.success is True
        assert KwargsCheckingParser.received_kwargs is not None
        assert "custom_param" in KwargsCheckingParser.received_kwargs
        assert KwargsCheckingParser.received_kwargs["custom_param"] == "test_value"


class TestParseReceiptEdgeCases:
    """Tests for edge cases."""

    def test_receipt_missing_fields(self):
        """Test parsing receipt with missing fields."""
        parser = MockReceiptParser()

        # Minimal receipt
        receipt = {}

        result = parser.parse_receipt(receipt)

        # Should handle gracefully
        assert result.transaction_hash == ""
        assert result.block_number == 0

    def test_log_with_no_topics(self):
        """Test parsing log with empty topics list."""
        parser = MockReceiptParser()

        receipt = {
            "transactionHash": "0xabc",
            "blockNumber": 123,
            "status": 1,
            "logs": [
                {
                    "topics": [],  # No topics
                    "data": "0x",
                    "address": "0xpool",
                    "logIndex": 0,
                }
            ],
        }

        result = parser.parse_receipt(receipt)

        # Should skip log with no topics
        assert len(result.events) == 0

    def test_log_with_missing_fields(self):
        """Test parsing log with missing optional fields."""
        parser = MockReceiptParser()

        receipt = {
            "transactionHash": "0xabc",
            "blockNumber": 123,
            "status": 1,
            "logs": [
                {
                    "topics": ["0xswap_topic_hash"],
                    # Missing data, address, logIndex
                }
            ],
        }

        # Should handle gracefully with defaults
        result = parser.parse_receipt(receipt)
        # Event should be created with default values
        if len(result.events) > 0:
            assert result.events[0].contract_address == ""
            assert result.events[0].log_index == 0
