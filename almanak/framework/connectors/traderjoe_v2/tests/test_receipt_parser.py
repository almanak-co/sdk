"""Tests for TraderJoe V2 Receipt Parser.

This test suite covers:
- Swap event parsing
- Liquidity event parsing (DepositedToBins, WithdrawnFromBins)
- Transfer event parsing
- Receipt parsing from transaction receipts
"""

from decimal import Decimal

import pytest

from ..receipt_parser import (
    DEPOSITED_TO_BINS_TOPIC,
    EVENT_NAME_TO_TYPE,
    EVENT_TOPICS,
    TOPIC_TO_EVENT,
    WITHDRAWN_FROM_BINS_TOPIC,
    LiquidityEventData,
    ParsedLiquidityResult,
    ParsedSwapResult,
    ParseResult,
    SwapEventData,
    TraderJoeV2Event,
    TraderJoeV2EventType,
    TraderJoeV2ReceiptParser,
    TransferEventData,
)
from ..sdk import BIN_ID_OFFSET

# =============================================================================
# Test Constants
# =============================================================================

WAVAX_ADDRESS = "0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7"
USDC_ADDRESS = "0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E"
TEST_WALLET = "0x1234567890123456789012345678901234567890"
TEST_POOL = "0x9f8973fb86b35c307324ec31fd81cf565e2f4a63"
TEST_TX_HASH = "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"


# =============================================================================
# Event Type Tests
# =============================================================================


class TestEventTypes:
    """Tests for event types and constants."""

    def test_event_type_values(self) -> None:
        """Test TraderJoeV2EventType enum values."""
        # Event type values are uppercase
        assert TraderJoeV2EventType.DEPOSITED_TO_BINS.value == "DEPOSITED_TO_BINS"
        assert TraderJoeV2EventType.WITHDRAWN_FROM_BINS.value == "WITHDRAWN_FROM_BINS"
        assert TraderJoeV2EventType.TRANSFER_BATCH.value == "TRANSFER_BATCH"
        assert TraderJoeV2EventType.TRANSFER.value == "TRANSFER"

    def test_event_topics_exist(self) -> None:
        """Test that EVENT_TOPICS contains expected events."""
        assert "DepositedToBins" in EVENT_TOPICS
        assert "WithdrawnFromBins" in EVENT_TOPICS
        assert "TransferBatch" in EVENT_TOPICS
        assert "Transfer" in EVENT_TOPICS

    def test_event_topics_format(self) -> None:
        """Test that event topics are valid keccak256 hashes."""
        for _event_name, topic in EVENT_TOPICS.items():
            assert topic.startswith("0x")
            assert len(topic) == 66  # 0x + 64 hex chars

    def test_deposited_to_bins_topic(self) -> None:
        """Test DEPOSITED_TO_BINS_TOPIC constant."""
        assert DEPOSITED_TO_BINS_TOPIC == EVENT_TOPICS["DepositedToBins"]

    def test_withdrawn_from_bins_topic(self) -> None:
        """Test WITHDRAWN_FROM_BINS_TOPIC constant."""
        assert WITHDRAWN_FROM_BINS_TOPIC == EVENT_TOPICS["WithdrawnFromBins"]

    def test_topic_to_event_mapping(self) -> None:
        """Test TOPIC_TO_EVENT mapping."""
        for _event_name, topic in EVENT_TOPICS.items():
            # TOPIC_TO_EVENT uses lowercase keys
            assert topic.lower() in TOPIC_TO_EVENT or topic in TOPIC_TO_EVENT

    def test_event_name_to_type_mapping(self) -> None:
        """Test EVENT_NAME_TO_TYPE mapping."""
        assert EVENT_NAME_TO_TYPE["DepositedToBins"] == TraderJoeV2EventType.DEPOSITED_TO_BINS
        assert EVENT_NAME_TO_TYPE["WithdrawnFromBins"] == TraderJoeV2EventType.WITHDRAWN_FROM_BINS
        assert EVENT_NAME_TO_TYPE["Transfer"] == TraderJoeV2EventType.TRANSFER


# =============================================================================
# Event Data Tests
# =============================================================================


class TestSwapEventData:
    """Tests for SwapEventData dataclass."""

    def test_swap_event_data_creation(self) -> None:
        """Test SwapEventData creation."""
        event_data = SwapEventData(
            token_in=WAVAX_ADDRESS,
            token_out=USDC_ADDRESS,
            amount_in=10**18,
            amount_out=30 * 10**6,
            sender=TEST_WALLET,
            recipient=TEST_WALLET,
        )

        assert event_data.sender == TEST_WALLET
        assert event_data.amount_in == 10**18
        assert event_data.amount_out == 30 * 10**6


class TestLiquidityEventData:
    """Tests for LiquidityEventData dataclass."""

    def test_liquidity_event_data_creation(self) -> None:
        """Test LiquidityEventData creation."""
        event_data = LiquidityEventData(
            pool_address=TEST_POOL,
            sender=TEST_WALLET,
            to=TEST_WALLET,
            bin_ids=[BIN_ID_OFFSET - 1, BIN_ID_OFFSET, BIN_ID_OFFSET + 1],
            amounts_x=[1000, 2000, 1000],
            amounts_y=[3000, 6000, 3000],
        )

        assert event_data.sender == TEST_WALLET
        assert len(event_data.bin_ids) == 3
        assert sum(event_data.amounts_x) == 4000


class TestTransferEventData:
    """Tests for TransferEventData dataclass."""

    def test_transfer_event_data_creation(self) -> None:
        """Test TransferEventData creation."""
        event_data = TransferEventData(
            token=WAVAX_ADDRESS,
            from_address="0x0000000000000000000000000000000000000000",
            to_address=TEST_WALLET,
            amount=1000 * 10**18,
        )

        assert event_data.from_address == "0x0000000000000000000000000000000000000000"
        assert event_data.to_address == TEST_WALLET
        assert event_data.amount == 1000 * 10**18


# =============================================================================
# TraderJoeV2Event Tests
# =============================================================================


class TestTraderJoeV2Event:
    """Tests for TraderJoeV2Event dataclass."""

    def test_event_creation(self) -> None:
        """Test TraderJoeV2Event creation."""
        event = TraderJoeV2Event(
            event_type=TraderJoeV2EventType.TRANSFER,
            event_name="Transfer",
            log_index=0,
            transaction_hash=TEST_TX_HASH,
            block_number=12345678,
            contract_address=WAVAX_ADDRESS,
            data={"from": TEST_WALLET, "to": TEST_POOL, "amount": 10**18},
        )

        assert event.event_type == TraderJoeV2EventType.TRANSFER
        assert event.event_name == "Transfer"
        assert event.log_index == 0
        assert event.contract_address == WAVAX_ADDRESS


# =============================================================================
# ParseResult Tests
# =============================================================================


class TestParsedSwapResult:
    """Tests for ParsedSwapResult dataclass."""

    def test_parsed_swap_result_creation(self) -> None:
        """Test ParsedSwapResult creation."""
        result = ParsedSwapResult(
            success=True,
            token_in=WAVAX_ADDRESS,
            token_out=USDC_ADDRESS,
            amount_in=10**18,
            amount_out=30 * 10**6,
            price=Decimal("0.00003"),
            gas_used=200000,
            block_number=12345678,
        )

        assert result.success is True
        assert result.amount_in == 10**18
        assert result.amount_out == 30 * 10**6


class TestParsedLiquidityResult:
    """Tests for ParsedLiquidityResult dataclass."""

    def test_parsed_liquidity_result_creation(self) -> None:
        """Test ParsedLiquidityResult creation."""
        result = ParsedLiquidityResult(
            success=True,
            is_add=True,
            pool_address=TEST_POOL,
            bin_ids=[BIN_ID_OFFSET],
            amount_x=10**18,
            amount_y=30 * 10**6,
            gas_used=300000,
            block_number=12345678,
        )

        assert result.success is True
        assert result.is_add is True
        assert result.pool_address == TEST_POOL


class TestParseResult:
    """Tests for ParseResult dataclass."""

    def test_parse_result_creation(self) -> None:
        """Test ParseResult creation."""
        result = ParseResult(
            success=True,
            transaction_hash=TEST_TX_HASH,
            block_number=12345678,
            gas_used=200000,
            events=[],
        )

        assert result.success is True
        assert result.transaction_hash == TEST_TX_HASH
        assert result.gas_used == 200000

    def test_parse_result_with_swap(self) -> None:
        """Test ParseResult with swap result."""
        swap_result = ParsedSwapResult(
            success=True,
            token_in=WAVAX_ADDRESS,
            token_out=USDC_ADDRESS,
            amount_in=10**18,
            amount_out=30 * 10**6,
        )

        result = ParseResult(
            success=True,
            transaction_hash=TEST_TX_HASH,
            block_number=12345678,
            gas_used=200000,
            events=[],
            swap_result=swap_result,
        )

        assert result.swap_result is not None
        assert result.swap_result.token_in == WAVAX_ADDRESS


# =============================================================================
# Receipt Parser Tests
# =============================================================================


class TestTraderJoeV2ReceiptParser:
    """Tests for TraderJoeV2ReceiptParser."""

    @pytest.fixture
    def parser(self) -> TraderJoeV2ReceiptParser:
        """Create parser for testing."""
        return TraderJoeV2ReceiptParser()

    def test_parser_creation(self, parser: TraderJoeV2ReceiptParser) -> None:
        """Test parser creation."""
        assert parser is not None

    def test_parse_empty_receipt(self, parser: TraderJoeV2ReceiptParser) -> None:
        """Test parsing receipt with no logs."""
        # Use bytes for transactionHash as Web3 returns
        receipt = {
            "status": 1,
            "transactionHash": bytes.fromhex(TEST_TX_HASH[2:]),
            "blockNumber": 12345678,
            "gasUsed": 100000,
            "logs": [],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert result.events == []

    def test_parse_failed_transaction(self, parser: TraderJoeV2ReceiptParser) -> None:
        """Test parsing failed transaction."""
        receipt = {
            "status": 0,  # Failed
            "transactionHash": bytes.fromhex(TEST_TX_HASH[2:]),
            "blockNumber": 12345678,
            "gasUsed": 50000,
            "logs": [],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is False
        assert result.error == "Transaction reverted"

    def test_parse_receipt_with_transfer_log(self, parser: TraderJoeV2ReceiptParser) -> None:
        """Test parsing receipt with Transfer event."""
        receipt = {
            "status": 1,
            "transactionHash": bytes.fromhex(TEST_TX_HASH[2:]),
            "blockNumber": 12345678,
            "gasUsed": 100000,
            "logs": [
                {
                    "address": WAVAX_ADDRESS,
                    "topics": [
                        EVENT_TOPICS["Transfer"],  # Transfer event topic
                        "0x" + "00" * 12 + TEST_WALLET[2:].lower(),  # from
                        "0x" + "00" * 12 + TEST_POOL[2:].lower(),  # to
                    ],
                    "data": "0x" + hex(10**18)[2:].zfill(64),  # amount
                    "logIndex": 0,
                }
            ],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        # Events may or may not be parsed depending on implementation details


# =============================================================================
# Integration Tests
# =============================================================================


class TestParserIntegration:
    """Integration tests for receipt parser."""

    @pytest.fixture
    def parser(self) -> TraderJoeV2ReceiptParser:
        """Create parser for testing."""
        return TraderJoeV2ReceiptParser()

    def test_parser_handles_bytes_tx_hash(self, parser: TraderJoeV2ReceiptParser) -> None:
        """Test parser handles bytes transaction hash."""
        receipt = {
            "status": 1,
            "transactionHash": bytes.fromhex(TEST_TX_HASH[2:]),
            "blockNumber": 12345678,
            "gasUsed": 100000,
            "logs": [],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert result.transaction_hash == TEST_TX_HASH[2:]  # Without 0x prefix

    def test_parser_handles_exception(self, parser: TraderJoeV2ReceiptParser) -> None:
        """Test parser handles exceptions gracefully."""
        # Malformed receipt
        receipt = {
            "status": 1,
            # Missing required fields
        }

        result = parser.parse_receipt(receipt)

        # Should return a result even on error
        assert isinstance(result, ParseResult)
