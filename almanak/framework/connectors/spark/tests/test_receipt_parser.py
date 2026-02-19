"""Tests for Spark Receipt Parser.

This module contains unit tests for the SparkReceiptParser class,
covering event parsing and contract address filtering functionality.
"""

import pytest

from ..receipt_parser import (
    EVENT_TOPICS,
    SPARK_POOL_ADDRESSES,
    SparkEventType,
    SparkReceiptParser,
)

# =============================================================================
# Constants for Testing
# =============================================================================

# Known Spark pool address (Ethereum mainnet)
SPARK_POOL_ADDRESS = "0xC13e21B648A5Ee794902342038FF3aDAB66BE987"

# Known Aave V3 pool address (Ethereum mainnet) - should be filtered out
AAVE_V3_POOL_ADDRESS = "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2"

# Test wallet address
TEST_WALLET = "0x1234567890123456789012345678901234567890"


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def parser() -> SparkReceiptParser:
    """Create a test parser instance."""
    return SparkReceiptParser()


@pytest.fixture
def custom_parser() -> SparkReceiptParser:
    """Create a parser with custom pool addresses."""
    return SparkReceiptParser(pool_addresses={"0xcustom1234567890123456789012345678901234"})


# =============================================================================
# Parser Initialization Tests
# =============================================================================


class TestSparkReceiptParserInit:
    """Tests for SparkReceiptParser initialization."""

    def test_default_pool_addresses(self, parser: SparkReceiptParser) -> None:
        """Test that default pool addresses are loaded."""
        assert parser._pool_addresses == SPARK_POOL_ADDRESSES

    def test_custom_pool_addresses(self, custom_parser: SparkReceiptParser) -> None:
        """Test that custom pool addresses can be provided."""
        assert custom_parser._pool_addresses == {"0xcustom1234567890123456789012345678901234"}

    def test_known_topics_loaded(self, parser: SparkReceiptParser) -> None:
        """Test that known event topics are loaded."""
        assert len(parser._known_topics) > 0
        assert EVENT_TOPICS["Supply"] in parser._known_topics
        assert EVENT_TOPICS["Withdraw"] in parser._known_topics


# =============================================================================
# Contract Address Filtering Tests
# =============================================================================


class TestContractAddressFiltering:
    """Tests for contract address filtering."""

    def test_ignores_non_spark_logs(self, parser: SparkReceiptParser) -> None:
        """Test that parser ignores logs from non-Spark contracts (e.g., Aave V3).

        This is critical because Spark and Aave V3 use the same event signatures.
        """
        # Create a receipt with a Supply event from Aave V3 pool address
        receipt = {
            "transactionHash": "0xabc123",
            "blockNumber": 12345678,
            "logs": [
                {
                    # Supply event from Aave V3 - should be ignored
                    "address": AAVE_V3_POOL_ADDRESS,
                    "topics": [
                        EVENT_TOPICS["Supply"],
                        # reserve (indexed) - DAI
                        "0x0000000000000000000000006B175474E89094C44Da98b954EedeAC495271d0F",
                        # onBehalfOf (indexed)
                        "0x0000000000000000000000001234567890123456789012345678901234567890",
                    ],
                    "data": (
                        # user (non-indexed address)
                        "0x0000000000000000000000001234567890123456789012345678901234567890"
                        # amount (1000 DAI = 1000 * 10^18)
                        "00000000000000000000000000000000000000000000003635c9adc5dea00000"
                        # referralCode
                        "0000000000000000000000000000000000000000000000000000000000000000"
                    ),
                }
            ],
        }

        result = parser.parse_receipt(receipt)

        # Should succeed but find no events (Aave V3 logs filtered out)
        assert result.success is True
        assert len(result.supplies) == 0
        assert len(result.withdraws) == 0
        assert len(result.borrows) == 0
        assert len(result.repays) == 0

    def test_parses_spark_logs(self, parser: SparkReceiptParser) -> None:
        """Test that parser correctly parses logs from Spark pool."""
        # Create a receipt with a Supply event from Spark pool address
        receipt = {
            "transactionHash": "0xabc123",
            "blockNumber": 12345678,
            "logs": [
                {
                    # Supply event from Spark - should be parsed
                    "address": SPARK_POOL_ADDRESS,
                    "topics": [
                        EVENT_TOPICS["Supply"],
                        # reserve (indexed) - DAI
                        "0x0000000000000000000000006b175474e89094c44da98b954eedeac495271d0f",
                        # onBehalfOf (indexed)
                        "0x0000000000000000000000001234567890123456789012345678901234567890",
                    ],
                    "data": (
                        # user (non-indexed address)
                        "0x0000000000000000000000001234567890123456789012345678901234567890"
                        # amount (1000 * 10^18)
                        "00000000000000000000000000000000000000000000003635c9adc5dea00000"
                        # referralCode
                        "0000000000000000000000000000000000000000000000000000000000000000"
                    ),
                }
            ],
        }

        result = parser.parse_receipt(receipt)

        # Should succeed and find the supply event
        assert result.success is True
        assert len(result.supplies) == 1
        assert result.supplies[0].reserve.lower() == "0x6b175474e89094c44da98b954eedeac495271d0f"

    def test_parses_spark_logs_case_insensitive(self, parser: SparkReceiptParser) -> None:
        """Test that address matching is case-insensitive."""
        # Use lowercase version of Spark pool address
        receipt = {
            "transactionHash": "0xabc123",
            "blockNumber": 12345678,
            "logs": [
                {
                    "address": SPARK_POOL_ADDRESS.lower(),
                    "topics": [
                        EVENT_TOPICS["Supply"],
                        "0x0000000000000000000000006b175474e89094c44da98b954eedeac495271d0f",
                        "0x0000000000000000000000001234567890123456789012345678901234567890",
                    ],
                    "data": (
                        "0x0000000000000000000000001234567890123456789012345678901234567890"
                        "00000000000000000000000000000000000000000000003635c9adc5dea00000"
                        "0000000000000000000000000000000000000000000000000000000000000000"
                    ),
                }
            ],
        }

        result = parser.parse_receipt(receipt)
        assert result.success is True
        assert len(result.supplies) == 1

    def test_mixed_logs_filters_correctly(self, parser: SparkReceiptParser) -> None:
        """Test that parser filters correctly when receipt has mixed logs."""
        receipt = {
            "transactionHash": "0xabc123",
            "blockNumber": 12345678,
            "logs": [
                # First log: Supply from Aave V3 (should be ignored)
                {
                    "address": AAVE_V3_POOL_ADDRESS,
                    "topics": [
                        EVENT_TOPICS["Supply"],
                        "0x0000000000000000000000006b175474e89094c44da98b954eedeac495271d0f",
                        "0x0000000000000000000000001234567890123456789012345678901234567890",
                    ],
                    "data": (
                        "0x0000000000000000000000001234567890123456789012345678901234567890"
                        "00000000000000000000000000000000000000000000003635c9adc5dea00000"
                        "0000000000000000000000000000000000000000000000000000000000000000"
                    ),
                },
                # Second log: Supply from Spark (should be parsed)
                {
                    "address": SPARK_POOL_ADDRESS,
                    "topics": [
                        EVENT_TOPICS["Supply"],
                        "0x000000000000000000000000a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
                        "0x0000000000000000000000001234567890123456789012345678901234567890",
                    ],
                    "data": (
                        "0x0000000000000000000000001234567890123456789012345678901234567890"
                        "000000000000000000000000000000000000000000000000000000003b9aca00"
                        "0000000000000000000000000000000000000000000000000000000000000000"
                    ),
                },
                # Third log: Some random contract (should be ignored)
                {
                    "address": "0x0000000000000000000000000000000000000001",
                    "topics": [
                        EVENT_TOPICS["Supply"],
                        "0x0000000000000000000000006b175474e89094c44da98b954eedeac495271d0f",
                        "0x0000000000000000000000001234567890123456789012345678901234567890",
                    ],
                    "data": (
                        "0x0000000000000000000000001234567890123456789012345678901234567890"
                        "00000000000000000000000000000000000000000000003635c9adc5dea00000"
                        "0000000000000000000000000000000000000000000000000000000000000000"
                    ),
                },
            ],
        }

        result = parser.parse_receipt(receipt)

        # Should only have 1 supply (from Spark pool)
        assert result.success is True
        assert len(result.supplies) == 1
        # Verify it's the USDC supply from Spark (not the DAI from Aave)
        assert result.supplies[0].reserve.lower() == "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"


# =============================================================================
# is_spark_pool Method Tests
# =============================================================================


class TestIsSparkPool:
    """Tests for is_spark_pool method."""

    def test_is_spark_pool_true(self, parser: SparkReceiptParser) -> None:
        """Test that known Spark pool returns True."""
        assert parser.is_spark_pool(SPARK_POOL_ADDRESS) is True
        assert parser.is_spark_pool(SPARK_POOL_ADDRESS.lower()) is True

    def test_is_spark_pool_false(self, parser: SparkReceiptParser) -> None:
        """Test that unknown address returns False."""
        assert parser.is_spark_pool(AAVE_V3_POOL_ADDRESS) is False
        assert parser.is_spark_pool(TEST_WALLET) is False


# =============================================================================
# Empty Receipt Tests
# =============================================================================


class TestEmptyReceipt:
    """Tests for empty or minimal receipts."""

    def test_empty_logs(self, parser: SparkReceiptParser) -> None:
        """Test parsing receipt with no logs."""
        receipt = {
            "transactionHash": "0xabc123",
            "blockNumber": 12345678,
            "logs": [],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.supplies) == 0
        assert len(result.withdraws) == 0

    def test_missing_logs(self, parser: SparkReceiptParser) -> None:
        """Test parsing receipt with missing logs key."""
        receipt = {
            "transactionHash": "0xabc123",
            "blockNumber": 12345678,
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True


# =============================================================================
# Event Type Tests
# =============================================================================


class TestEventTypes:
    """Tests for event type detection."""

    def test_is_spark_event(self, parser: SparkReceiptParser) -> None:
        """Test is_spark_event method."""
        assert parser.is_spark_event(EVENT_TOPICS["Supply"]) is True
        assert parser.is_spark_event(EVENT_TOPICS["Withdraw"]) is True
        assert parser.is_spark_event(EVENT_TOPICS["Borrow"]) is True
        assert parser.is_spark_event(EVENT_TOPICS["Repay"]) is True
        assert parser.is_spark_event("0xinvalid") is False

    def test_get_event_type(self, parser: SparkReceiptParser) -> None:
        """Test get_event_type method."""
        assert parser.get_event_type(EVENT_TOPICS["Supply"]) == SparkEventType.SUPPLY
        assert parser.get_event_type(EVENT_TOPICS["Withdraw"]) == SparkEventType.WITHDRAW
        assert parser.get_event_type(EVENT_TOPICS["Borrow"]) == SparkEventType.BORROW
        assert parser.get_event_type(EVENT_TOPICS["Repay"]) == SparkEventType.REPAY
        assert parser.get_event_type("0xinvalid") == SparkEventType.UNKNOWN


# =============================================================================
# Bytes Address Handling Tests
# =============================================================================


class TestBytesAddressHandling:
    """Tests for handling addresses as bytes (as returned by web3.py)."""

    def test_bytes_log_address(self, parser: SparkReceiptParser) -> None:
        """Test that parser handles log address as bytes."""
        # Spark pool address as bytes
        spark_pool_bytes = bytes.fromhex(
            SPARK_POOL_ADDRESS[2:]  # Remove 0x prefix
        )

        receipt = {
            "transactionHash": b"\xab\xc1\x23",
            "blockNumber": 12345678,
            "logs": [
                {
                    "address": spark_pool_bytes,
                    "topics": [
                        bytes.fromhex(EVENT_TOPICS["Supply"][2:]),
                        bytes.fromhex("0000000000000000000000006b175474e89094c44da98b954eedeac495271d0f"),
                        bytes.fromhex("0000000000000000000000001234567890123456789012345678901234567890"),
                    ],
                    "data": bytes.fromhex(
                        "0000000000000000000000001234567890123456789012345678901234567890"
                        "00000000000000000000000000000000000000000000003635c9adc5dea00000"
                        "0000000000000000000000000000000000000000000000000000000000000000"
                    ),
                }
            ],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.supplies) == 1
