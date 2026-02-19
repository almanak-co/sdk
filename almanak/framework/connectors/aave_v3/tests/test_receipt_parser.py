"""Tests for Aave V3 Receipt Parser.

This module contains comprehensive tests for the AaveV3ReceiptParser class,
covering all event types including Supply, Withdraw, Borrow, Repay,
FlashLoan, LiquidationCall, and more.
"""

from decimal import Decimal

import pytest

from ..receipt_parser import (
    EVENT_NAME_TO_TYPE,
    EVENT_TOPICS,
    TOPIC_TO_EVENT,
    AaveV3Event,
    AaveV3EventType,
    AaveV3ReceiptParser,
    BorrowEventData,
    FlashLoanEventData,
    IsolationModeDebtUpdatedEventData,
    LiquidationCallEventData,
    ParseResult,
    RepayEventData,
    ReserveDataUpdatedEventData,
    SupplyEventData,
    UserEModeSetEventData,
)

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def parser() -> AaveV3ReceiptParser:
    """Create a parser instance."""
    return AaveV3ReceiptParser()


@pytest.fixture
def sample_supply_log() -> dict:
    """Create a sample Supply event log."""
    return {
        "address": "0x794a61358D6845594F94dc1DB02A252b5b4814aD",
        "topics": [
            EVENT_TOPICS["Supply"],  # Event signature
            "0x000000000000000000000000af88d065e77c8cc2239327c5edb3a432268e5831",  # reserve (indexed)
            "0x0000000000000000000000001234567890123456789012345678901234567890",  # onBehalfOf (indexed)
        ],
        "data": (
            "0x"
            # user (address)
            "0000000000000000000000001234567890123456789012345678901234567890"
            # amount (uint256) - 1000 USDC = 1000000000 (6 decimals)
            "000000000000000000000000000000000000000000000000000000003b9aca00"
            # referralCode (uint16)
            "0000000000000000000000000000000000000000000000000000000000000000"
        ),
        "logIndex": 5,
    }


@pytest.fixture
def sample_borrow_log() -> dict:
    """Create a sample Borrow event log."""
    return {
        "address": "0x794a61358D6845594F94dc1DB02A252b5b4814aD",
        "topics": [
            EVENT_TOPICS["Borrow"],  # Event signature
            "0x00000000000000000000000082af49447d8a07e3bd95bd0d56f35241523fbab1",  # reserve (indexed)
            "0x0000000000000000000000001234567890123456789012345678901234567890",  # onBehalfOf (indexed)
        ],
        "data": (
            "0x"
            # user (address)
            "0000000000000000000000001234567890123456789012345678901234567890"
            # amount (uint256) - 1 WETH = 1e18
            "0000000000000000000000000000000000000000000000000de0b6b3a7640000"
            # interestRateMode (uint256) - 2 = variable
            "0000000000000000000000000000000000000000000000000000000000000002"
            # borrowRate (uint256) - 5% APY in ray
            "000000000000000000000000000000000000000001743b34e18439b502000000"
            # referralCode (uint16)
            "0000000000000000000000000000000000000000000000000000000000000000"
        ),
        "logIndex": 10,
    }


@pytest.fixture
def sample_receipt() -> dict:
    """Create a sample transaction receipt with multiple events."""
    return {
        "transactionHash": "0xabc123def456789012345678901234567890123456789012345678901234567890",
        "blockNumber": 12345678,
        "logs": [
            {
                "address": "0x794a61358D6845594F94dc1DB02A252b5b4814aD",
                "topics": [
                    EVENT_TOPICS["Supply"],
                    "0x000000000000000000000000af88d065e77c8cc2239327c5edb3a432268e5831",
                    "0x0000000000000000000000001234567890123456789012345678901234567890",
                ],
                "data": (
                    "0x"
                    "0000000000000000000000001234567890123456789012345678901234567890"
                    "000000000000000000000000000000000000000000000000000000003b9aca00"
                    "0000000000000000000000000000000000000000000000000000000000000000"
                ),
                "logIndex": 0,
            },
            {
                "address": "0x794a61358D6845594F94dc1DB02A252b5b4814aD",
                "topics": [
                    EVENT_TOPICS["Borrow"],
                    "0x00000000000000000000000082af49447d8a07e3bd95bd0d56f35241523fbab1",
                    "0x0000000000000000000000001234567890123456789012345678901234567890",
                ],
                "data": (
                    "0x"
                    "0000000000000000000000001234567890123456789012345678901234567890"
                    "0000000000000000000000000000000000000000000000000de0b6b3a7640000"
                    "0000000000000000000000000000000000000000000000000000000000000002"
                    "000000000000000000000000000000000000000001743b34e18439b502000000"
                    "0000000000000000000000000000000000000000000000000000000000000000"
                ),
                "logIndex": 1,
            },
        ],
    }


# =============================================================================
# Parser Initialization Tests
# =============================================================================


class TestParserInitialization:
    """Tests for parser initialization."""

    def test_init(self, parser: AaveV3ReceiptParser) -> None:
        """Test parser initialization."""
        assert parser is not None
        assert len(parser._known_topics) > 0

    def test_is_aave_event(self, parser: AaveV3ReceiptParser) -> None:
        """Test checking if a topic is an Aave event."""
        assert parser.is_aave_event(EVENT_TOPICS["Supply"]) is True
        assert parser.is_aave_event(EVENT_TOPICS["Borrow"]) is True
        assert parser.is_aave_event("0x0000000000000000000000000000000000000000") is False

    def test_get_event_type(self, parser: AaveV3ReceiptParser) -> None:
        """Test getting event type from topic."""
        assert parser.get_event_type(EVENT_TOPICS["Supply"]) == AaveV3EventType.SUPPLY
        assert parser.get_event_type(EVENT_TOPICS["Borrow"]) == AaveV3EventType.BORROW
        assert parser.get_event_type(EVENT_TOPICS["FlashLoan"]) == AaveV3EventType.FLASH_LOAN
        assert parser.get_event_type("0x0000") == AaveV3EventType.UNKNOWN


# =============================================================================
# Event Topic Tests
# =============================================================================


class TestEventTopics:
    """Tests for event topic constants."""

    def test_event_topics_defined(self) -> None:
        """Test that all event topics are defined."""
        required_events = [
            "Supply",
            "Withdraw",
            "Borrow",
            "Repay",
            "FlashLoan",
            "LiquidationCall",
            "ReserveDataUpdated",
            "UserEModeSet",
            "IsolationModeTotalDebtUpdated",
        ]
        for event in required_events:
            assert event in EVENT_TOPICS

    def test_topic_to_event_reverse_mapping(self) -> None:
        """Test that reverse mapping is correct."""
        for event_name, topic in EVENT_TOPICS.items():
            assert TOPIC_TO_EVENT[topic] == event_name

    def test_event_name_to_type_mapping(self) -> None:
        """Test that event name to type mapping is correct."""
        assert EVENT_NAME_TO_TYPE["Supply"] == AaveV3EventType.SUPPLY
        assert EVENT_NAME_TO_TYPE["Borrow"] == AaveV3EventType.BORROW
        assert EVENT_NAME_TO_TYPE["FlashLoan"] == AaveV3EventType.FLASH_LOAN


# =============================================================================
# Receipt Parsing Tests
# =============================================================================


class TestReceiptParsing:
    """Tests for parsing complete receipts."""

    def test_parse_empty_receipt(self, parser: AaveV3ReceiptParser) -> None:
        """Test parsing an empty receipt."""
        receipt = {
            "transactionHash": "0x123",
            "blockNumber": 100,
            "logs": [],
        }
        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.events) == 0

    def test_parse_receipt_with_events(self, parser: AaveV3ReceiptParser, sample_receipt: dict) -> None:
        """Test parsing a receipt with multiple events."""
        result = parser.parse_receipt(sample_receipt)

        assert result.success is True
        assert len(result.events) == 2
        assert len(result.supplies) == 1
        assert len(result.borrows) == 1

    def test_parse_receipt_preserves_tx_hash(self, parser: AaveV3ReceiptParser, sample_receipt: dict) -> None:
        """Test that transaction hash is preserved."""
        result = parser.parse_receipt(sample_receipt)

        assert result.transaction_hash == sample_receipt["transactionHash"]
        assert result.block_number == sample_receipt["blockNumber"]

    def test_parse_receipt_bytes_tx_hash(self, parser: AaveV3ReceiptParser) -> None:
        """Test parsing receipt with bytes transaction hash."""
        receipt = {
            "transactionHash": bytes.fromhex("abc123def456789012345678901234567890123456789012345678901234567890"),
            "blockNumber": 100,
            "logs": [],
        }
        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert result.transaction_hash.startswith("0x")


# =============================================================================
# Supply Event Tests
# =============================================================================


class TestSupplyEventParsing:
    """Tests for Supply event parsing."""

    def test_parse_supply_log(self, parser: AaveV3ReceiptParser, sample_supply_log: dict) -> None:
        """Test parsing a Supply event log."""
        events = parser.parse_logs([sample_supply_log])

        assert len(events) == 1
        event = events[0]
        assert event.event_type == AaveV3EventType.SUPPLY
        assert event.event_name == "Supply"

    def test_supply_event_data(self, parser: AaveV3ReceiptParser, sample_supply_log: dict) -> None:
        """Test that Supply event data is correctly parsed."""
        events = parser.parse_logs([sample_supply_log])

        event = events[0]
        assert "reserve" in event.data
        assert "user" in event.data
        assert "amount" in event.data

    def test_supply_event_data_class(self) -> None:
        """Test SupplyEventData class."""
        supply = SupplyEventData(
            reserve="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            user="0x1234567890123456789012345678901234567890",
            on_behalf_of="0x1234567890123456789012345678901234567890",
            amount=Decimal("1000000000"),
            referral_code=0,
        )

        data = supply.to_dict()
        assert data["reserve"] == "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
        assert data["amount"] == "1000000000"


# =============================================================================
# Borrow Event Tests
# =============================================================================


class TestBorrowEventParsing:
    """Tests for Borrow event parsing."""

    def test_parse_borrow_log(self, parser: AaveV3ReceiptParser, sample_borrow_log: dict) -> None:
        """Test parsing a Borrow event log."""
        events = parser.parse_logs([sample_borrow_log])

        assert len(events) == 1
        event = events[0]
        assert event.event_type == AaveV3EventType.BORROW
        assert event.event_name == "Borrow"

    def test_borrow_event_data(self, parser: AaveV3ReceiptParser, sample_borrow_log: dict) -> None:
        """Test that Borrow event data is correctly parsed."""
        events = parser.parse_logs([sample_borrow_log])

        event = events[0]
        assert "reserve" in event.data
        assert "user" in event.data
        assert "amount" in event.data
        assert "interest_rate_mode" in event.data

    def test_borrow_event_data_class(self) -> None:
        """Test BorrowEventData class."""
        borrow = BorrowEventData(
            reserve="0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
            user="0x1234567890123456789012345678901234567890",
            on_behalf_of="0x1234567890123456789012345678901234567890",
            amount=Decimal("1000000000000000000"),
            interest_rate_mode=2,
            borrow_rate=Decimal("0.05"),
        )

        assert borrow.is_variable_rate is True
        data = borrow.to_dict()
        assert data["is_variable_rate"] is True


# =============================================================================
# Repay Event Tests
# =============================================================================


class TestRepayEventParsing:
    """Tests for Repay event parsing."""

    def test_repay_event_data_class(self) -> None:
        """Test RepayEventData class."""
        repay = RepayEventData(
            reserve="0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
            user="0x1234567890123456789012345678901234567890",
            repayer="0x1234567890123456789012345678901234567890",
            amount=Decimal("500000000000000000"),
            use_atokens=False,
        )

        data = repay.to_dict()
        assert data["amount"] == "500000000000000000"
        assert data["use_atokens"] is False


# =============================================================================
# Flash Loan Event Tests
# =============================================================================


class TestFlashLoanEventParsing:
    """Tests for FlashLoan event parsing."""

    def test_flash_loan_event_data_class(self) -> None:
        """Test FlashLoanEventData class."""
        flash = FlashLoanEventData(
            target="0x9876543210987654321098765432109876543210",
            initiator="0x1234567890123456789012345678901234567890",
            asset="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            amount=Decimal("1000000000000"),
            interest_rate_mode=0,  # No debt
            premium=Decimal("900000000"),  # 0.09% premium
        )

        assert flash.opened_debt is False
        data = flash.to_dict()
        assert data["opened_debt"] is False

    def test_flash_loan_with_debt(self) -> None:
        """Test FlashLoan that opens debt."""
        flash = FlashLoanEventData(
            target="0x9876543210987654321098765432109876543210",
            initiator="0x1234567890123456789012345678901234567890",
            asset="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            amount=Decimal("1000000000000"),
            interest_rate_mode=2,  # Variable debt
            premium=Decimal("0"),
        )

        assert flash.opened_debt is True


# =============================================================================
# Liquidation Event Tests
# =============================================================================


class TestLiquidationEventParsing:
    """Tests for LiquidationCall event parsing."""

    def test_liquidation_event_data_class(self) -> None:
        """Test LiquidationCallEventData class."""
        liquidation = LiquidationCallEventData(
            collateral_asset="0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
            debt_asset="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            user="0x1234567890123456789012345678901234567890",
            debt_to_cover=Decimal("1000000000"),
            liquidated_collateral_amount=Decimal("550000000000000000"),
            liquidator="0x9876543210987654321098765432109876543210",
            receive_atoken=False,
        )

        data = liquidation.to_dict()
        assert data["debt_to_cover"] == "1000000000"
        assert data["liquidated_collateral_amount"] == "550000000000000000"


# =============================================================================
# Reserve Data Updated Event Tests
# =============================================================================


class TestReserveDataUpdatedParsing:
    """Tests for ReserveDataUpdated event parsing."""

    def test_reserve_data_updated_event_data_class(self) -> None:
        """Test ReserveDataUpdatedEventData class."""
        reserve_update = ReserveDataUpdatedEventData(
            reserve="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            liquidity_rate=Decimal("0.03"),  # 3% APY
            stable_borrow_rate=Decimal("0.05"),  # 5% APY
            variable_borrow_rate=Decimal("0.04"),  # 4% APY
            liquidity_index=Decimal("1.05"),
            variable_borrow_index=Decimal("1.04"),
        )

        data = reserve_update.to_dict()
        assert data["liquidity_rate"] == "0.03"
        assert data["variable_borrow_rate"] == "0.04"


# =============================================================================
# E-Mode Event Tests
# =============================================================================


class TestEModeEventParsing:
    """Tests for UserEModeSet event parsing."""

    def test_user_emode_set_event_data_class(self) -> None:
        """Test UserEModeSetEventData class."""
        emode = UserEModeSetEventData(
            user="0x1234567890123456789012345678901234567890",
            category_id=1,
        )

        assert emode.category_name == "ETH Correlated"
        data = emode.to_dict()
        assert data["category_id"] == 1
        assert data["category_name"] == "ETH Correlated"

    def test_user_emode_stablecoins(self) -> None:
        """Test E-Mode for stablecoins category."""
        emode = UserEModeSetEventData(
            user="0x1234567890123456789012345678901234567890",
            category_id=2,
        )

        assert emode.category_name == "Stablecoins"

    def test_user_emode_none(self) -> None:
        """Test E-Mode disabled."""
        emode = UserEModeSetEventData(
            user="0x1234567890123456789012345678901234567890",
            category_id=0,
        )

        assert emode.category_name == "None"


# =============================================================================
# Isolation Mode Event Tests
# =============================================================================


class TestIsolationModeEventParsing:
    """Tests for IsolationModeTotalDebtUpdated event parsing."""

    def test_isolation_mode_debt_updated_event_data_class(self) -> None:
        """Test IsolationModeDebtUpdatedEventData class."""
        isolation = IsolationModeDebtUpdatedEventData(
            asset="0x1234567890123456789012345678901234567890",
            total_debt=Decimal("500000"),  # $500,000
        )

        data = isolation.to_dict()
        assert data["total_debt"] == "500000"


# =============================================================================
# Event Class Tests
# =============================================================================


class TestAaveV3Event:
    """Tests for AaveV3Event class."""

    def test_event_to_dict(self) -> None:
        """Test event serialization."""
        event = AaveV3Event(
            event_type=AaveV3EventType.SUPPLY,
            event_name="Supply",
            log_index=5,
            transaction_hash="0xabc123",
            block_number=12345,
            contract_address="0x794a61358D6845594F94dc1DB02A252b5b4814aD",
            data={"amount": "1000"},
            raw_topics=["0x123"],
            raw_data="0x456",
        )

        data = event.to_dict()
        assert data["event_type"] == "SUPPLY"
        assert data["event_name"] == "Supply"
        assert data["log_index"] == 5

    def test_event_from_dict(self) -> None:
        """Test event deserialization."""
        data = {
            "event_type": "SUPPLY",
            "event_name": "Supply",
            "log_index": 5,
            "transaction_hash": "0xabc123",
            "block_number": 12345,
            "contract_address": "0x794a61358D6845594F94dc1DB02A252b5b4814aD",
            "data": {"amount": "1000"},
            "raw_topics": ["0x123"],
            "raw_data": "0x456",
            "timestamp": "2024-01-01T00:00:00",
        }

        event = AaveV3Event.from_dict(data)
        assert event.event_type == AaveV3EventType.SUPPLY
        assert event.log_index == 5


# =============================================================================
# Parse Result Tests
# =============================================================================


class TestParseResult:
    """Tests for ParseResult class."""

    def test_parse_result_to_dict(self) -> None:
        """Test ParseResult serialization."""
        result = ParseResult(
            success=True,
            events=[],
            transaction_hash="0xabc123",
            block_number=12345,
        )

        data = result.to_dict()
        assert data["success"] is True
        assert data["transaction_hash"] == "0xabc123"

    def test_parse_result_with_supplies(self) -> None:
        """Test ParseResult with supply events."""
        supply = SupplyEventData(
            reserve="0x123",
            user="0x456",
            on_behalf_of="0x456",
            amount=Decimal("1000"),
        )

        result = ParseResult(
            success=True,
            supplies=[supply],
            transaction_hash="0xabc123",
            block_number=12345,
        )

        data = result.to_dict()
        assert len(data["supplies"]) == 1


# =============================================================================
# Edge Case Tests
# =============================================================================


class TestEdgeCases:
    """Tests for edge cases."""

    def test_parse_unknown_event(self, parser: AaveV3ReceiptParser) -> None:
        """Test parsing an unknown event."""
        log = {
            "address": "0x794a61358D6845594F94dc1DB02A252b5b4814aD",
            "topics": [
                "0x0000000000000000000000000000000000000000000000000000000000000000",
            ],
            "data": "0x",
            "logIndex": 0,
        }

        events = parser.parse_logs([log])
        assert len(events) == 0  # Unknown events are skipped

    def test_parse_malformed_log(self, parser: AaveV3ReceiptParser) -> None:
        """Test parsing a malformed log."""
        log = {
            "address": "0x794a61358D6845594F94dc1DB02A252b5b4814aD",
            "topics": [],  # Empty topics
            "data": "0x",
            "logIndex": 0,
        }

        events = parser.parse_logs([log])
        assert len(events) == 0

    def test_parse_bytes_data(self, parser: AaveV3ReceiptParser) -> None:
        """Test parsing log with bytes data."""
        log = {
            "address": bytes.fromhex("794a61358D6845594F94dc1DB02A252b5b4814aD"),
            "topics": [
                bytes.fromhex(EVENT_TOPICS["Supply"][2:]),
                bytes.fromhex("000000000000000000000000af88d065e77c8cc2239327c5edb3a432268e5831"),
                bytes.fromhex("0000000000000000000000001234567890123456789012345678901234567890"),
            ],
            "data": bytes.fromhex(
                "0000000000000000000000001234567890123456789012345678901234567890"
                "000000000000000000000000000000000000000000000000000000003b9aca00"
                "0000000000000000000000000000000000000000000000000000000000000000"
            ),
            "logIndex": 0,
        }

        events = parser.parse_logs([log])
        assert len(events) == 1
        assert events[0].event_type == AaveV3EventType.SUPPLY

    def test_parse_receipt_error_handling(self, parser: AaveV3ReceiptParser) -> None:
        """Test error handling in parse_receipt."""
        # This should not raise, even with invalid data
        result = parser.parse_receipt({})

        assert result.success is True
        assert len(result.events) == 0
