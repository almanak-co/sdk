"""Tests for Compound V3 Receipt Parser.

These tests verify the CompoundV3ReceiptParser for:
- Event parsing (Supply, Withdraw, SupplyCollateral, etc.)
- Receipt aggregation
- Error handling
"""

from decimal import Decimal

import pytest

from ..receipt_parser import (
    EVENT_NAME_TO_TYPE,
    EVENT_TOPICS,
    TOPIC_TO_EVENT,
    CompoundV3Event,
    CompoundV3EventType,
    CompoundV3ReceiptParser,
)

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def parser():
    """Create a receipt parser."""
    return CompoundV3ReceiptParser()


@pytest.fixture
def supply_log():
    """Create a Supply event log."""
    return {
        "address": "0xc3d688B66703497DAA19211EEdff47f25384cdc3",
        "topics": [
            EVENT_TOPICS["Supply"],
            "0x0000000000000000000000001234567890123456789012345678901234567890",  # from
            "0x000000000000000000000000abcdefabcdefabcdefabcdefabcdefabcdefabcd",  # dst
        ],
        "data": "0x0000000000000000000000000000000000000000000000000000000005f5e100",  # 100 USDC (6 decimals)
        "logIndex": 0,
    }


@pytest.fixture
def withdraw_log():
    """Create a Withdraw event log."""
    return {
        "address": "0xc3d688B66703497DAA19211EEdff47f25384cdc3",
        "topics": [
            EVENT_TOPICS["Withdraw"],
            "0x0000000000000000000000001234567890123456789012345678901234567890",  # src
            "0x000000000000000000000000abcdefabcdefabcdefabcdefabcdefabcdefabcd",  # to
        ],
        "data": "0x0000000000000000000000000000000000000000000000000000000002faf080",  # 50 USDC
        "logIndex": 1,
    }


@pytest.fixture
def supply_collateral_log():
    """Create a SupplyCollateral event log."""
    return {
        "address": "0xc3d688B66703497DAA19211EEdff47f25384cdc3",
        "topics": [
            EVENT_TOPICS["SupplyCollateral"],
            "0x0000000000000000000000001234567890123456789012345678901234567890",  # from
            "0x000000000000000000000000abcdefabcdefabcdefabcdefabcdefabcdefabcd",  # dst
            "0x000000000000000000000000c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",  # WETH
        ],
        "data": "0x0000000000000000000000000000000000000000000000000de0b6b3a7640000",  # 1 ETH
        "logIndex": 0,
    }


@pytest.fixture
def withdraw_collateral_log():
    """Create a WithdrawCollateral event log."""
    return {
        "address": "0xc3d688B66703497DAA19211EEdff47f25384cdc3",
        "topics": [
            EVENT_TOPICS["WithdrawCollateral"],
            "0x0000000000000000000000001234567890123456789012345678901234567890",  # src
            "0x000000000000000000000000abcdefabcdefabcdefabcdefabcdefabcdefabcd",  # to
            "0x000000000000000000000000c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",  # WETH
        ],
        "data": "0x0000000000000000000000000000000000000000000000000b1a2bc2ec500000",  # 0.8 ETH
        "logIndex": 0,
    }


@pytest.fixture
def transfer_log():
    """Create a Transfer event log."""
    return {
        "address": "0xc3d688B66703497DAA19211EEdff47f25384cdc3",
        "topics": [
            EVENT_TOPICS["Transfer"],
            "0x0000000000000000000000001234567890123456789012345678901234567890",  # from
            "0x000000000000000000000000abcdefabcdefabcdefabcdefabcdefabcdefabcd",  # to
        ],
        "data": "0x0000000000000000000000000000000000000000000000000000000005f5e100",  # 100 USDC
        "logIndex": 0,
    }


@pytest.fixture
def absorb_debt_log():
    """Create an AbsorbDebt event log."""
    return {
        "address": "0xc3d688B66703497DAA19211EEdff47f25384cdc3",
        "topics": [
            EVENT_TOPICS["AbsorbDebt"],
            "0x0000000000000000000000001111111111111111111111111111111111111111",  # absorber
            "0x0000000000000000000000002222222222222222222222222222222222222222",  # borrower
        ],
        "data": "0x" + "0" * 62 + "64" + "0" * 62 + "c8",  # 100 basePaidOut, 200 usdValue
        "logIndex": 0,
    }


@pytest.fixture
def buy_collateral_log():
    """Create a BuyCollateral event log."""
    return {
        "address": "0xc3d688B66703497DAA19211EEdff47f25384cdc3",
        "topics": [
            EVENT_TOPICS["BuyCollateral"],
            "0x0000000000000000000000001111111111111111111111111111111111111111",  # buyer
            "0x000000000000000000000000c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",  # asset (WETH)
        ],
        "data": "0x" + "0" * 62 + "64" + "0" * 62 + "c8",  # 100 baseAmount, 200 collateralAmount
        "logIndex": 0,
    }


# =============================================================================
# Event Topic Tests
# =============================================================================


class TestEventTopics:
    """Test event topic constants."""

    def test_supply_topic_defined(self):
        """Test Supply event topic is defined."""
        assert "Supply" in EVENT_TOPICS
        assert EVENT_TOPICS["Supply"].startswith("0x")

    def test_withdraw_topic_defined(self):
        """Test Withdraw event topic is defined."""
        assert "Withdraw" in EVENT_TOPICS
        assert EVENT_TOPICS["Withdraw"].startswith("0x")

    def test_supply_collateral_topic_defined(self):
        """Test SupplyCollateral event topic is defined."""
        assert "SupplyCollateral" in EVENT_TOPICS
        assert EVENT_TOPICS["SupplyCollateral"].startswith("0x")

    def test_topic_to_event_reverse_mapping(self):
        """Test topic to event name reverse mapping."""
        for event_name, topic in EVENT_TOPICS.items():
            assert TOPIC_TO_EVENT[topic] == event_name

    def test_event_name_to_type_mapping(self):
        """Test event name to type mapping."""
        assert EVENT_NAME_TO_TYPE["Supply"] == CompoundV3EventType.SUPPLY
        assert EVENT_NAME_TO_TYPE["Withdraw"] == CompoundV3EventType.WITHDRAW
        assert EVENT_NAME_TO_TYPE["SupplyCollateral"] == CompoundV3EventType.SUPPLY_COLLATERAL


# =============================================================================
# Supply Event Tests
# =============================================================================


class TestSupplyEventParsing:
    """Test Supply event parsing."""

    def test_parse_supply_event(self, parser, supply_log):
        """Test parsing a Supply event."""
        events = parser.parse_logs([supply_log], tx_hash="0x123", block_number=12345)

        assert len(events) == 1
        event = events[0]
        assert event.event_type == CompoundV3EventType.SUPPLY
        assert event.event_name == "Supply"
        assert event.data["from_address"] == "0x1234567890123456789012345678901234567890"
        assert event.data["dst"] == "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"
        assert event.data["amount"] == Decimal("100000000")  # 100 USDC in wei

    def test_supply_event_in_receipt(self, parser, supply_log):
        """Test parsing Supply event in receipt."""
        receipt = {
            "transactionHash": "0x1234",
            "blockNumber": 12345,
            "logs": [supply_log],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert result.supply_amount == Decimal("100000000")
        assert len(result.events) == 1


# =============================================================================
# Withdraw Event Tests
# =============================================================================


class TestWithdrawEventParsing:
    """Test Withdraw event parsing."""

    def test_parse_withdraw_event(self, parser, withdraw_log):
        """Test parsing a Withdraw event."""
        events = parser.parse_logs([withdraw_log], tx_hash="0x123", block_number=12345)

        assert len(events) == 1
        event = events[0]
        assert event.event_type == CompoundV3EventType.WITHDRAW
        assert event.event_name == "Withdraw"
        assert event.data["src"] == "0x1234567890123456789012345678901234567890"
        assert event.data["to"] == "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"
        assert event.data["amount"] == Decimal("50000000")  # 50 USDC in wei

    def test_withdraw_event_in_receipt(self, parser, withdraw_log):
        """Test parsing Withdraw event in receipt."""
        receipt = {
            "transactionHash": "0x1234",
            "blockNumber": 12345,
            "logs": [withdraw_log],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert result.withdraw_amount == Decimal("50000000")


# =============================================================================
# Collateral Event Tests
# =============================================================================


class TestCollateralEventParsing:
    """Test collateral event parsing."""

    def test_parse_supply_collateral_event(self, parser, supply_collateral_log):
        """Test parsing a SupplyCollateral event."""
        events = parser.parse_logs([supply_collateral_log], tx_hash="0x123", block_number=12345)

        assert len(events) == 1
        event = events[0]
        assert event.event_type == CompoundV3EventType.SUPPLY_COLLATERAL
        assert event.event_name == "SupplyCollateral"
        assert event.data["from_address"] == "0x1234567890123456789012345678901234567890"
        assert event.data["asset"] == "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
        assert event.data["amount"] == Decimal("1000000000000000000")  # 1 ETH

    def test_supply_collateral_aggregation(self, parser, supply_collateral_log):
        """Test SupplyCollateral aggregation in receipt."""
        receipt = {
            "transactionHash": "0x1234",
            "blockNumber": 12345,
            "logs": [supply_collateral_log],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2" in result.collateral_supplied
        assert result.collateral_supplied["0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"] == Decimal(
            "1000000000000000000"
        )

    def test_parse_withdraw_collateral_event(self, parser, withdraw_collateral_log):
        """Test parsing a WithdrawCollateral event."""
        events = parser.parse_logs([withdraw_collateral_log], tx_hash="0x123", block_number=12345)

        assert len(events) == 1
        event = events[0]
        assert event.event_type == CompoundV3EventType.WITHDRAW_COLLATERAL
        assert event.event_name == "WithdrawCollateral"
        assert event.data["asset"] == "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"

    def test_withdraw_collateral_aggregation(self, parser, withdraw_collateral_log):
        """Test WithdrawCollateral aggregation in receipt."""
        receipt = {
            "transactionHash": "0x1234",
            "blockNumber": 12345,
            "logs": [withdraw_collateral_log],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2" in result.collateral_withdrawn


# =============================================================================
# Transfer Event Tests
# =============================================================================


class TestTransferEventParsing:
    """Test Transfer event parsing."""

    def test_parse_transfer_event(self, parser, transfer_log):
        """Test parsing a Transfer event."""
        events = parser.parse_logs([transfer_log], tx_hash="0x123", block_number=12345)

        assert len(events) == 1
        event = events[0]
        assert event.event_type == CompoundV3EventType.TRANSFER
        assert event.event_name == "Transfer"
        assert event.data["from_address"] == "0x1234567890123456789012345678901234567890"
        assert event.data["to"] == "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"


# =============================================================================
# Liquidation Event Tests
# =============================================================================


class TestLiquidationEventParsing:
    """Test liquidation event parsing."""

    def test_parse_absorb_debt_event(self, parser, absorb_debt_log):
        """Test parsing an AbsorbDebt event."""
        events = parser.parse_logs([absorb_debt_log], tx_hash="0x123", block_number=12345)

        assert len(events) == 1
        event = events[0]
        assert event.event_type == CompoundV3EventType.ABSORB_DEBT
        assert event.event_name == "AbsorbDebt"
        assert event.data["absorber"] == "0x1111111111111111111111111111111111111111"
        assert event.data["borrower"] == "0x2222222222222222222222222222222222222222"
        assert event.data["base_paid_out"] == Decimal("100")

    def test_parse_buy_collateral_event(self, parser, buy_collateral_log):
        """Test parsing a BuyCollateral event."""
        events = parser.parse_logs([buy_collateral_log], tx_hash="0x123", block_number=12345)

        assert len(events) == 1
        event = events[0]
        assert event.event_type == CompoundV3EventType.BUY_COLLATERAL
        assert event.event_name == "BuyCollateral"
        assert event.data["buyer"] == "0x1111111111111111111111111111111111111111"
        assert event.data["asset"] == "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"


# =============================================================================
# Receipt Parsing Tests
# =============================================================================


class TestReceiptParsing:
    """Test receipt parsing."""

    def test_parse_empty_receipt(self, parser):
        """Test parsing receipt with no logs."""
        receipt = {
            "transactionHash": "0x1234",
            "blockNumber": 12345,
            "logs": [],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.events) == 0
        assert result.supply_amount == Decimal("0")
        assert result.withdraw_amount == Decimal("0")

    def test_parse_receipt_with_multiple_events(self, parser, supply_log, supply_collateral_log):
        """Test parsing receipt with multiple events."""
        receipt = {
            "transactionHash": "0x1234",
            "blockNumber": 12345,
            "logs": [supply_log, supply_collateral_log],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.events) == 2
        assert result.supply_amount > Decimal("0")
        assert len(result.collateral_supplied) > 0

    def test_parse_receipt_filters_by_address(self, parser, supply_log):
        """Test parsing receipt with address filter."""
        receipt = {
            "transactionHash": "0x1234",
            "blockNumber": 12345,
            "logs": [supply_log],
        }

        # Filter by different address should return no events
        result = parser.parse_receipt(
            receipt,
            comet_address="0x0000000000000000000000000000000000000000",
        )

        assert result.success is True
        assert len(result.events) == 0

    def test_parse_receipt_with_matching_address(self, parser, supply_log):
        """Test parsing receipt with matching address filter."""
        receipt = {
            "transactionHash": "0x1234",
            "blockNumber": 12345,
            "logs": [supply_log],
        }

        # Filter by correct address should return events
        result = parser.parse_receipt(
            receipt,
            comet_address="0xc3d688B66703497DAA19211EEdff47f25384cdc3",
        )

        assert result.success is True
        assert len(result.events) == 1

    def test_parse_receipt_with_hash_field(self, parser, supply_log):
        """Test parsing receipt with 'hash' field instead of 'transactionHash'."""
        receipt = {
            "hash": "0x1234",
            "blockNumber": 12345,
            "logs": [supply_log],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert result.events[0].transaction_hash == "0x1234"


# =============================================================================
# Event Serialization Tests
# =============================================================================


class TestEventSerialization:
    """Test event serialization."""

    def test_event_to_dict(self, parser, supply_log):
        """Test event to_dict method."""
        events = parser.parse_logs([supply_log], tx_hash="0x123", block_number=12345)
        event = events[0]

        data = event.to_dict()

        assert data["event_type"] == "SUPPLY"
        assert data["event_name"] == "Supply"
        assert data["transaction_hash"] == "0x123"
        assert data["block_number"] == 12345
        assert "data" in data
        assert "raw_topics" in data

    def test_event_from_dict(self, parser, supply_log):
        """Test event from_dict method."""
        events = parser.parse_logs([supply_log], tx_hash="0x123", block_number=12345)
        original = events[0]
        data = original.to_dict()

        restored = CompoundV3Event.from_dict(data)

        assert restored.event_type == original.event_type
        assert restored.event_name == original.event_name
        assert restored.transaction_hash == original.transaction_hash

    def test_parse_result_to_dict(self, parser, supply_log):
        """Test ParseResult to_dict method."""
        receipt = {
            "transactionHash": "0x1234",
            "blockNumber": 12345,
            "logs": [supply_log],
        }

        result = parser.parse_receipt(receipt)
        data = result.to_dict()

        assert data["success"] is True
        assert len(data["events"]) == 1
        assert data["supply_amount"] == str(Decimal("100000000"))


# =============================================================================
# Error Handling Tests
# =============================================================================


class TestErrorHandling:
    """Test error handling."""

    def test_parse_log_with_no_topics(self, parser):
        """Test parsing log with no topics."""
        log = {
            "address": "0xc3d688B66703497DAA19211EEdff47f25384cdc3",
            "topics": [],
            "data": "0x00",
            "logIndex": 0,
        }

        events = parser.parse_logs([log], tx_hash="0x123", block_number=12345)

        assert len(events) == 0

    def test_parse_log_with_unknown_topic(self, parser):
        """Test parsing log with unknown topic."""
        log = {
            "address": "0xc3d688B66703497DAA19211EEdff47f25384cdc3",
            "topics": ["0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"],
            "data": "0x00",
            "logIndex": 0,
        }

        events = parser.parse_logs([log], tx_hash="0x123", block_number=12345)

        assert len(events) == 0

    def test_parse_log_with_missing_data(self, parser):
        """Test parsing log with missing data."""
        log = {
            "address": "0xc3d688B66703497DAA19211EEdff47f25384cdc3",
            "topics": [
                EVENT_TOPICS["Supply"],
                "0x0000000000000000000000001234567890123456789012345678901234567890",
                "0x000000000000000000000000abcdefabcdefabcdefabcdefabcdefabcdefabcd",
            ],
            "data": "",
            "logIndex": 0,
        }

        events = parser.parse_logs([log], tx_hash="0x123", block_number=12345)

        assert len(events) == 1
        assert events[0].data["amount"] == Decimal("0")

    def test_parse_receipt_handles_exception(self, parser):
        """Test parse_receipt handles exceptions gracefully."""
        # Pass invalid receipt that will cause an exception
        receipt = None

        # Should handle None gracefully and return error result
        result = parser.parse_receipt(receipt)
        assert result.success is False
        assert result.error is not None


# =============================================================================
# Helper Method Tests
# =============================================================================


class TestHelperMethods:
    """Test helper methods."""

    def test_topic_to_address(self, parser):
        """Test _topic_to_address helper."""
        topic = "0x0000000000000000000000001234567890123456789012345678901234567890"
        address = parser._topic_to_address(topic)

        assert address == "0x1234567890123456789012345678901234567890"

    def test_topic_to_address_without_prefix(self, parser):
        """Test _topic_to_address helper without 0x prefix."""
        topic = "0000000000000000000000001234567890123456789012345678901234567890"
        address = parser._topic_to_address(topic)

        assert address == "0x1234567890123456789012345678901234567890"

    def test_decode_uint256(self, parser):
        """Test _decode_uint256 helper."""
        # 100 in hex (64 chars)
        data = "0x0000000000000000000000000000000000000000000000000000000000000064"
        value = parser._decode_uint256(data)

        assert value == Decimal("100")

    def test_decode_uint256_empty(self, parser):
        """Test _decode_uint256 with empty data."""
        value = parser._decode_uint256("")

        assert value == Decimal("0")

    def test_hex_to_decimal(self, parser):
        """Test _hex_to_decimal helper."""
        value = parser._hex_to_decimal("ff")

        assert value == Decimal("255")

    def test_hex_to_decimal_empty(self, parser):
        """Test _hex_to_decimal with empty string."""
        value = parser._hex_to_decimal("")

        assert value == Decimal("0")


# =============================================================================
# Multiple Collateral Tests
# =============================================================================


class TestMultipleCollaterals:
    """Test multiple collateral handling."""

    def test_multiple_collateral_supplies(self, parser):
        """Test aggregating multiple collateral supplies."""
        weth_log = {
            "address": "0xc3d688B66703497DAA19211EEdff47f25384cdc3",
            "topics": [
                EVENT_TOPICS["SupplyCollateral"],
                "0x0000000000000000000000001234567890123456789012345678901234567890",
                "0x000000000000000000000000abcdefabcdefabcdefabcdefabcdefabcdefabcd",
                "0x000000000000000000000000c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",  # WETH
            ],
            "data": "0x0000000000000000000000000000000000000000000000000de0b6b3a7640000",  # 1 ETH
            "logIndex": 0,
        }

        wbtc_log = {
            "address": "0xc3d688B66703497DAA19211EEdff47f25384cdc3",
            "topics": [
                EVENT_TOPICS["SupplyCollateral"],
                "0x0000000000000000000000001234567890123456789012345678901234567890",
                "0x000000000000000000000000abcdefabcdefabcdefabcdefabcdefabcdefabcd",
                "0x0000000000000000000000002260fac5e5542a773aa44fbcfedf7c193bc2c599",  # WBTC
            ],
            "data": "0x0000000000000000000000000000000000000000000000000000000005f5e100",  # 1 WBTC (8 decimals)
            "logIndex": 1,
        }

        receipt = {
            "transactionHash": "0x1234",
            "blockNumber": 12345,
            "logs": [weth_log, wbtc_log],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.collateral_supplied) == 2
        assert "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2" in result.collateral_supplied
        assert "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599" in result.collateral_supplied

    def test_same_collateral_multiple_supplies(self, parser):
        """Test aggregating multiple supplies of same collateral."""
        log1 = {
            "address": "0xc3d688B66703497DAA19211EEdff47f25384cdc3",
            "topics": [
                EVENT_TOPICS["SupplyCollateral"],
                "0x0000000000000000000000001234567890123456789012345678901234567890",
                "0x000000000000000000000000abcdefabcdefabcdefabcdefabcdefabcdefabcd",
                "0x000000000000000000000000c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",  # WETH
            ],
            "data": "0x0000000000000000000000000000000000000000000000000de0b6b3a7640000",  # 1 ETH
            "logIndex": 0,
        }

        log2 = {
            "address": "0xc3d688B66703497DAA19211EEdff47f25384cdc3",
            "topics": [
                EVENT_TOPICS["SupplyCollateral"],
                "0x0000000000000000000000001234567890123456789012345678901234567890",
                "0x000000000000000000000000abcdefabcdefabcdefabcdefabcdefabcdefabcd",
                "0x000000000000000000000000c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",  # WETH
            ],
            "data": "0x0000000000000000000000000000000000000000000000000de0b6b3a7640000",  # 1 ETH
            "logIndex": 1,
        }

        receipt = {
            "transactionHash": "0x1234",
            "blockNumber": 12345,
            "logs": [log1, log2],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        weth_addr = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
        # Should be 2 ETH total
        assert result.collateral_supplied[weth_addr] == Decimal("2000000000000000000")
