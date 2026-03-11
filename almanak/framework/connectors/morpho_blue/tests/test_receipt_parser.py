"""Unit tests for Morpho Blue Receipt Parser."""

from datetime import datetime
from decimal import Decimal

import pytest

from ..receipt_parser import (
    EVENT_NAME_TO_TYPE,
    EVENT_TOPICS,
    TOPIC_TO_EVENT,
    AccrueInterestEventData,
    BorrowEventData,
    CreateMarketEventData,
    FlashLoanEventData,
    LiquidateEventData,
    MorphoBlueEvent,
    MorphoBlueEventType,
    MorphoBlueReceiptParser,
    ParseResult,
    RepayEventData,
    SetAuthorizationEventData,
    SupplyCollateralEventData,
    SupplyEventData,
    TransferEventData,
    WithdrawCollateralEventData,
    WithdrawEventData,
)

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def parser() -> MorphoBlueReceiptParser:
    """Create parser fixture."""
    return MorphoBlueReceiptParser()


# Sample market ID (bytes32)
SAMPLE_MARKET_ID = "0xb323495f7e4148be5643a4ea4a8221eef163e4bccfdedc2a6f4696baacbc86cc"

# Sample addresses
CALLER_ADDRESS = "0x1234567890123456789012345678901234567890"
ON_BEHALF_OF_ADDRESS = "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"
RECEIVER_ADDRESS = "0x9876543210987654321098765432109876543210"
MORPHO_ADDRESS = "0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb"


def pad_address(address: str) -> str:
    """Pad address to 32 bytes for topic."""
    addr = address.lower().replace("0x", "")
    return "0x" + addr.zfill(64)


def encode_uint256(value: int) -> str:
    """Encode uint256 to hex string."""
    return hex(value)[2:].zfill(64)


def encode_address(address: str) -> str:
    """Encode address to hex string (32 bytes)."""
    addr = address.lower().replace("0x", "")
    return addr.zfill(64)


# =============================================================================
# Parser Initialization Tests
# =============================================================================


class TestParserInit:
    """Tests for parser initialization."""

    def test_parser_init(self, parser: MorphoBlueReceiptParser) -> None:
        """Test parser initializes correctly."""
        assert parser is not None


# =============================================================================
# Event Topics Tests
# =============================================================================


class TestEventTopics:
    """Tests for event topics."""

    def test_event_topics_format(self) -> None:
        """Test all event topics are valid hex strings."""
        for event_name, topic in EVENT_TOPICS.items():
            assert topic.startswith("0x"), f"Topic for {event_name} should start with 0x"
            assert len(topic) == 66, f"Topic for {event_name} should be 66 chars"

    def test_topic_to_event_inverse(self) -> None:
        """Test TOPIC_TO_EVENT is inverse of EVENT_TOPICS."""
        for event_name, topic in EVENT_TOPICS.items():
            assert TOPIC_TO_EVENT[topic] == event_name

    def test_event_name_to_type_coverage(self) -> None:
        """Test all main events have type mappings."""
        main_events = ["Supply", "Withdraw", "Borrow", "Repay", "SupplyCollateral", "WithdrawCollateral"]
        for event_name in main_events:
            assert event_name in EVENT_NAME_TO_TYPE


# =============================================================================
# Parse Receipt Tests
# =============================================================================


class TestParseReceipt:
    """Tests for parse_receipt method."""

    def test_parse_empty_receipt(self, parser: MorphoBlueReceiptParser) -> None:
        """Test parsing empty receipt."""
        receipt = {
            "transactionHash": "0x" + "1" * 64,
            "blockNumber": 12345,
            "logs": [],
        }
        result = parser.parse_receipt(receipt)
        assert result.success is True
        assert len(result.events) == 0
        assert result.transaction_hash == "0x" + "1" * 64
        assert result.block_number == 12345

    def test_parse_receipt_with_bytes_hash(self, parser: MorphoBlueReceiptParser) -> None:
        """Test parsing receipt with bytes transaction hash."""
        receipt = {
            "transactionHash": bytes.fromhex("1" * 64),
            "blockNumber": 12345,
            "logs": [],
        }
        result = parser.parse_receipt(receipt)
        assert result.success is True
        assert result.transaction_hash == "0x" + "1" * 64

    def test_parse_receipt_with_hex_block_number(self, parser: MorphoBlueReceiptParser) -> None:
        """Test parsing receipt with hex block number."""
        receipt = {
            "transactionHash": "0x" + "1" * 64,
            "blockNumber": "0x3039",  # 12345 in hex
            "logs": [],
        }
        result = parser.parse_receipt(receipt)
        assert result.block_number == 12345

    def test_parse_unknown_event(self, parser: MorphoBlueReceiptParser) -> None:
        """Test parsing receipt with unknown event."""
        receipt = {
            "transactionHash": "0x" + "1" * 64,
            "blockNumber": 12345,
            "logs": [
                {
                    "topics": ["0x" + "9" * 64],  # Unknown topic
                    "data": "0x" + "0" * 64,
                    "address": MORPHO_ADDRESS,
                }
            ],
        }
        result = parser.parse_receipt(receipt)
        assert result.success is True
        assert len(result.events) == 0  # Unknown events are skipped


# =============================================================================
# Supply Event Tests
# =============================================================================


class TestSupplyEvent:
    """Tests for Supply event parsing."""

    def test_parse_supply_event(self, parser: MorphoBlueReceiptParser) -> None:
        """Test parsing Supply event."""
        # Supply(Id indexed id, address caller, address indexed onBehalfOf, uint256 assets, uint256 shares)
        assets = 1000 * 10**6  # 1000 USDC
        shares = 1000 * 10**18  # 1000 shares

        receipt = {
            "transactionHash": "0x" + "1" * 64,
            "blockNumber": 12345,
            "logs": [
                {
                    "topics": [
                        EVENT_TOPICS["Supply"],
                        SAMPLE_MARKET_ID,
                        pad_address(ON_BEHALF_OF_ADDRESS),
                    ],
                    "data": "0x" + encode_address(CALLER_ADDRESS) + encode_uint256(assets) + encode_uint256(shares),
                    "address": MORPHO_ADDRESS,
                }
            ],
        }
        result = parser.parse_receipt(receipt)
        assert result.success is True
        assert len(result.events) == 1

        event = result.events[0]
        assert event.event_type == MorphoBlueEventType.SUPPLY
        assert event.event_name == "Supply"
        assert event.data["market_id"] == SAMPLE_MARKET_ID
        assert event.data["caller"].lower() == CALLER_ADDRESS.lower()
        assert event.data["on_behalf_of"].lower() == ON_BEHALF_OF_ADDRESS.lower()
        assert event.data["assets"] == str(assets)
        assert event.data["shares"] == str(shares)


# =============================================================================
# Withdraw Event Tests
# =============================================================================


class TestWithdrawEvent:
    """Tests for Withdraw event parsing."""

    def test_parse_withdraw_event(self, parser: MorphoBlueReceiptParser) -> None:
        """Test parsing Withdraw event."""
        # Withdraw(Id indexed id, address caller, address indexed onBehalfOf, address indexed receiver, uint256 assets, uint256 shares)
        assets = 500 * 10**6
        shares = 500 * 10**18

        receipt = {
            "transactionHash": "0x" + "1" * 64,
            "blockNumber": 12345,
            "logs": [
                {
                    "topics": [
                        EVENT_TOPICS["Withdraw"],
                        SAMPLE_MARKET_ID,
                        pad_address(ON_BEHALF_OF_ADDRESS),
                        pad_address(RECEIVER_ADDRESS),
                    ],
                    "data": "0x" + encode_address(CALLER_ADDRESS) + encode_uint256(assets) + encode_uint256(shares),
                    "address": MORPHO_ADDRESS,
                }
            ],
        }
        result = parser.parse_receipt(receipt)
        assert result.success is True
        assert len(result.events) == 1

        event = result.events[0]
        assert event.event_type == MorphoBlueEventType.WITHDRAW
        assert event.data["receiver"].lower() == RECEIVER_ADDRESS.lower()


# =============================================================================
# Borrow Event Tests
# =============================================================================


class TestBorrowEvent:
    """Tests for Borrow event parsing."""

    def test_parse_borrow_event(self, parser: MorphoBlueReceiptParser) -> None:
        """Test parsing Borrow event."""
        # Borrow(Id indexed id, address caller, address indexed onBehalfOf, address indexed receiver, uint256 assets, uint256 shares)
        assets = 1000 * 10**6
        shares = 1000 * 10**18

        receipt = {
            "transactionHash": "0x" + "1" * 64,
            "blockNumber": 12345,
            "logs": [
                {
                    "topics": [
                        EVENT_TOPICS["Borrow"],
                        SAMPLE_MARKET_ID,
                        pad_address(ON_BEHALF_OF_ADDRESS),
                        pad_address(RECEIVER_ADDRESS),
                    ],
                    "data": "0x" + encode_address(CALLER_ADDRESS) + encode_uint256(assets) + encode_uint256(shares),
                    "address": MORPHO_ADDRESS,
                }
            ],
        }
        result = parser.parse_receipt(receipt)
        assert result.success is True
        assert len(result.events) == 1

        event = result.events[0]
        assert event.event_type == MorphoBlueEventType.BORROW
        assert event.data["assets"] == str(assets)


# =============================================================================
# Repay Event Tests
# =============================================================================


class TestRepayEvent:
    """Tests for Repay event parsing."""

    def test_parse_repay_event(self, parser: MorphoBlueReceiptParser) -> None:
        """Test parsing Repay event."""
        # Repay(Id indexed id, address caller, address indexed onBehalfOf, uint256 assets, uint256 shares)
        assets = 500 * 10**6
        shares = 500 * 10**18

        receipt = {
            "transactionHash": "0x" + "1" * 64,
            "blockNumber": 12345,
            "logs": [
                {
                    "topics": [
                        EVENT_TOPICS["Repay"],
                        SAMPLE_MARKET_ID,
                        pad_address(ON_BEHALF_OF_ADDRESS),
                    ],
                    "data": "0x" + encode_address(CALLER_ADDRESS) + encode_uint256(assets) + encode_uint256(shares),
                    "address": MORPHO_ADDRESS,
                }
            ],
        }
        result = parser.parse_receipt(receipt)
        assert result.success is True
        assert len(result.events) == 1

        event = result.events[0]
        assert event.event_type == MorphoBlueEventType.REPAY
        assert event.data["assets"] == str(assets)


# =============================================================================
# Supply Collateral Event Tests
# =============================================================================


class TestSupplyCollateralEvent:
    """Tests for SupplyCollateral event parsing."""

    def test_parse_supply_collateral_event(self, parser: MorphoBlueReceiptParser) -> None:
        """Test parsing SupplyCollateral event."""
        # SupplyCollateral(Id indexed id, address caller, address indexed onBehalfOf, uint256 assets)
        assets = 10 * 10**18  # 10 wstETH

        receipt = {
            "transactionHash": "0x" + "1" * 64,
            "blockNumber": 12345,
            "logs": [
                {
                    "topics": [
                        EVENT_TOPICS["SupplyCollateral"],
                        SAMPLE_MARKET_ID,
                        pad_address(ON_BEHALF_OF_ADDRESS),
                    ],
                    "data": "0x" + encode_address(CALLER_ADDRESS) + encode_uint256(assets),
                    "address": MORPHO_ADDRESS,
                }
            ],
        }
        result = parser.parse_receipt(receipt)
        assert result.success is True
        assert len(result.events) == 1

        event = result.events[0]
        assert event.event_type == MorphoBlueEventType.SUPPLY_COLLATERAL
        assert event.data["assets"] == str(assets)


# =============================================================================
# Withdraw Collateral Event Tests
# =============================================================================


class TestWithdrawCollateralEvent:
    """Tests for WithdrawCollateral event parsing."""

    def test_parse_withdraw_collateral_event(self, parser: MorphoBlueReceiptParser) -> None:
        """Test parsing WithdrawCollateral event."""
        # WithdrawCollateral(Id indexed id, address caller, address indexed onBehalfOf, address indexed receiver, uint256 assets)
        assets = 5 * 10**18

        receipt = {
            "transactionHash": "0x" + "1" * 64,
            "blockNumber": 12345,
            "logs": [
                {
                    "topics": [
                        EVENT_TOPICS["WithdrawCollateral"],
                        SAMPLE_MARKET_ID,
                        pad_address(ON_BEHALF_OF_ADDRESS),
                        pad_address(RECEIVER_ADDRESS),
                    ],
                    "data": "0x" + encode_address(CALLER_ADDRESS) + encode_uint256(assets),
                    "address": MORPHO_ADDRESS,
                }
            ],
        }
        result = parser.parse_receipt(receipt)
        assert result.success is True
        assert len(result.events) == 1

        event = result.events[0]
        assert event.event_type == MorphoBlueEventType.WITHDRAW_COLLATERAL
        assert event.data["receiver"].lower() == RECEIVER_ADDRESS.lower()


# =============================================================================
# Liquidate Event Tests
# =============================================================================


class TestLiquidateEvent:
    """Tests for Liquidate event parsing."""

    def test_parse_liquidate_event(self, parser: MorphoBlueReceiptParser) -> None:
        """Test parsing Liquidate event."""
        repaid_assets = 1000 * 10**6
        repaid_shares = 1000 * 10**18
        seized_assets = 5 * 10**18
        bad_debt_assets = 0
        bad_debt_shares = 0

        receipt = {
            "transactionHash": "0x" + "1" * 64,
            "blockNumber": 12345,
            "logs": [
                {
                    "topics": [
                        EVENT_TOPICS["Liquidate"],
                        SAMPLE_MARKET_ID,
                        pad_address(ON_BEHALF_OF_ADDRESS),  # borrower
                    ],
                    "data": "0x"
                    + encode_address(CALLER_ADDRESS)
                    + encode_uint256(repaid_assets)
                    + encode_uint256(repaid_shares)
                    + encode_uint256(seized_assets)
                    + encode_uint256(bad_debt_assets)
                    + encode_uint256(bad_debt_shares),
                    "address": MORPHO_ADDRESS,
                }
            ],
        }
        result = parser.parse_receipt(receipt)
        assert result.success is True
        assert len(result.events) == 1

        event = result.events[0]
        assert event.event_type == MorphoBlueEventType.LIQUIDATE
        assert event.data["repaid_assets"] == str(repaid_assets)
        assert event.data["seized_assets"] == str(seized_assets)


# =============================================================================
# Transfer Event Tests
# =============================================================================


class TestTransferEvent:
    """Tests for ERC20 Transfer event parsing."""

    def test_parse_transfer_event(self, parser: MorphoBlueReceiptParser) -> None:
        """Test parsing Transfer event."""
        amount = 1000 * 10**6

        receipt = {
            "transactionHash": "0x" + "1" * 64,
            "blockNumber": 12345,
            "logs": [
                {
                    "topics": [
                        EVENT_TOPICS["Transfer"],
                        pad_address(CALLER_ADDRESS),  # from
                        pad_address(RECEIVER_ADDRESS),  # to
                    ],
                    "data": "0x" + encode_uint256(amount),
                    "address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",  # USDC
                }
            ],
        }
        result = parser.parse_receipt(receipt)
        assert result.success is True
        assert len(result.events) == 1

        event = result.events[0]
        assert event.event_type == MorphoBlueEventType.TRANSFER
        assert event.data["from"].lower() == CALLER_ADDRESS.lower()
        assert event.data["to"].lower() == RECEIVER_ADDRESS.lower()
        assert event.data["amount"] == str(amount)


# =============================================================================
# Event Data Class Tests
# =============================================================================


class TestEventDataClasses:
    """Tests for event data classes."""

    def test_supply_event_data_to_dict(self) -> None:
        """Test SupplyEventData.to_dict()."""
        data = SupplyEventData(
            market_id=SAMPLE_MARKET_ID,
            caller=CALLER_ADDRESS,
            on_behalf_of=ON_BEHALF_OF_ADDRESS,
            assets=Decimal("1000"),
            shares=Decimal("1000"),
        )
        d = data.to_dict()
        assert d["market_id"] == SAMPLE_MARKET_ID
        assert d["assets"] == "1000"

    def test_withdraw_event_data_to_dict(self) -> None:
        """Test WithdrawEventData.to_dict()."""
        data = WithdrawEventData(
            market_id=SAMPLE_MARKET_ID,
            caller=CALLER_ADDRESS,
            on_behalf_of=ON_BEHALF_OF_ADDRESS,
            receiver=RECEIVER_ADDRESS,
            assets=Decimal("500"),
            shares=Decimal("500"),
        )
        d = data.to_dict()
        assert d["receiver"] == RECEIVER_ADDRESS

    def test_borrow_event_data_to_dict(self) -> None:
        """Test BorrowEventData.to_dict()."""
        data = BorrowEventData(
            market_id=SAMPLE_MARKET_ID,
            caller=CALLER_ADDRESS,
            on_behalf_of=ON_BEHALF_OF_ADDRESS,
            receiver=RECEIVER_ADDRESS,
            assets=Decimal("1000"),
            shares=Decimal("1000"),
        )
        d = data.to_dict()
        assert d["assets"] == "1000"

    def test_repay_event_data_to_dict(self) -> None:
        """Test RepayEventData.to_dict()."""
        data = RepayEventData(
            market_id=SAMPLE_MARKET_ID,
            caller=CALLER_ADDRESS,
            on_behalf_of=ON_BEHALF_OF_ADDRESS,
            assets=Decimal("500"),
            shares=Decimal("500"),
        )
        d = data.to_dict()
        assert d["assets"] == "500"

    def test_supply_collateral_event_data_to_dict(self) -> None:
        """Test SupplyCollateralEventData.to_dict()."""
        data = SupplyCollateralEventData(
            market_id=SAMPLE_MARKET_ID,
            caller=CALLER_ADDRESS,
            on_behalf_of=ON_BEHALF_OF_ADDRESS,
            assets=Decimal("10"),
        )
        d = data.to_dict()
        assert d["assets"] == "10"

    def test_withdraw_collateral_event_data_to_dict(self) -> None:
        """Test WithdrawCollateralEventData.to_dict()."""
        data = WithdrawCollateralEventData(
            market_id=SAMPLE_MARKET_ID,
            caller=CALLER_ADDRESS,
            on_behalf_of=ON_BEHALF_OF_ADDRESS,
            receiver=RECEIVER_ADDRESS,
            assets=Decimal("5"),
        )
        d = data.to_dict()
        assert d["receiver"] == RECEIVER_ADDRESS

    def test_liquidate_event_data_to_dict(self) -> None:
        """Test LiquidateEventData.to_dict()."""
        data = LiquidateEventData(
            market_id=SAMPLE_MARKET_ID,
            caller=CALLER_ADDRESS,
            borrower=ON_BEHALF_OF_ADDRESS,
            repaid_assets=Decimal("1000"),
            repaid_shares=Decimal("1000"),
            seized_assets=Decimal("5"),
        )
        d = data.to_dict()
        assert d["seized_assets"] == "5"

    def test_flash_loan_event_data_to_dict(self) -> None:
        """Test FlashLoanEventData.to_dict()."""
        data = FlashLoanEventData(
            caller=CALLER_ADDRESS,
            token="0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            assets=Decimal("1000000"),
        )
        d = data.to_dict()
        assert d["caller"] == CALLER_ADDRESS

    def test_create_market_event_data_to_dict(self) -> None:
        """Test CreateMarketEventData.to_dict()."""
        data = CreateMarketEventData(
            market_id=SAMPLE_MARKET_ID,
            loan_token="0x1111111111111111111111111111111111111111",
            collateral_token="0x2222222222222222222222222222222222222222",
            oracle="0x3333333333333333333333333333333333333333",
            irm="0x4444444444444444444444444444444444444444",
            lltv=860000000000000000,
        )
        d = data.to_dict()
        assert d["lltv_percent"] == 86.0

    def test_set_authorization_event_data_to_dict(self) -> None:
        """Test SetAuthorizationEventData.to_dict()."""
        data = SetAuthorizationEventData(
            caller=CALLER_ADDRESS,
            authorized=RECEIVER_ADDRESS,
            is_authorized=True,
        )
        d = data.to_dict()
        assert d["is_authorized"] is True

    def test_accrue_interest_event_data_to_dict(self) -> None:
        """Test AccrueInterestEventData.to_dict()."""
        data = AccrueInterestEventData(
            market_id=SAMPLE_MARKET_ID,
            prev_borrow_rate=Decimal("1000000000000000"),
            interest=Decimal("100000"),
            fee_shares=Decimal("1000"),
        )
        d = data.to_dict()
        assert d["market_id"] == SAMPLE_MARKET_ID

    def test_transfer_event_data_to_dict(self) -> None:
        """Test TransferEventData.to_dict()."""
        data = TransferEventData(
            from_address=CALLER_ADDRESS,
            to_address=RECEIVER_ADDRESS,
            amount=Decimal("1000"),
        )
        d = data.to_dict()
        assert d["from"] == CALLER_ADDRESS
        assert d["to"] == RECEIVER_ADDRESS


# =============================================================================
# MorphoBlueEvent Tests
# =============================================================================


class TestMorphoBlueEvent:
    """Tests for MorphoBlueEvent class."""

    def test_event_to_dict(self) -> None:
        """Test MorphoBlueEvent.to_dict()."""
        event = MorphoBlueEvent(
            event_type=MorphoBlueEventType.SUPPLY,
            event_name="Supply",
            log_index=0,
            transaction_hash="0x" + "1" * 64,
            block_number=12345,
            contract_address=MORPHO_ADDRESS,
            data={"assets": "1000"},
        )
        d = event.to_dict()
        assert d["event_type"] == "SUPPLY"
        assert d["event_name"] == "Supply"
        assert d["block_number"] == 12345

    def test_event_from_dict(self) -> None:
        """Test MorphoBlueEvent.from_dict()."""
        timestamp = datetime.now()
        d = {
            "event_type": "SUPPLY",
            "event_name": "Supply",
            "log_index": 0,
            "transaction_hash": "0x" + "1" * 64,
            "block_number": 12345,
            "contract_address": MORPHO_ADDRESS,
            "data": {"assets": "1000"},
            "raw_topics": [],
            "raw_data": "",
            "timestamp": timestamp.isoformat(),
        }
        event = MorphoBlueEvent.from_dict(d)
        assert event.event_type == MorphoBlueEventType.SUPPLY
        assert event.data["assets"] == "1000"


# =============================================================================
# ParseResult Tests
# =============================================================================


class TestParseResult:
    """Tests for ParseResult class."""

    def test_parse_result_success_to_dict(self) -> None:
        """Test ParseResult.to_dict() for success."""
        event = MorphoBlueEvent(
            event_type=MorphoBlueEventType.SUPPLY,
            event_name="Supply",
            log_index=0,
            transaction_hash="0x" + "1" * 64,
            block_number=12345,
            contract_address=MORPHO_ADDRESS,
            data={},
        )
        result = ParseResult(
            success=True,
            events=[event],
            transaction_hash="0x" + "1" * 64,
            block_number=12345,
        )
        d = result.to_dict()
        assert d["success"] is True
        assert len(d["events"]) == 1

    def test_parse_result_failure_to_dict(self) -> None:
        """Test ParseResult.to_dict() for failure."""
        result = ParseResult(
            success=False,
            error="Test error",
        )
        d = result.to_dict()
        assert d["success"] is False
        assert d["error"] == "Test error"


# =============================================================================
# Multi-Event Tests
# =============================================================================


class TestMultipleEvents:
    """Tests for parsing multiple events."""

    def test_parse_supply_and_transfer(self, parser: MorphoBlueReceiptParser) -> None:
        """Test parsing receipt with multiple events."""
        assets = 1000 * 10**6
        shares = 1000 * 10**18

        receipt = {
            "transactionHash": "0x" + "1" * 64,
            "blockNumber": 12345,
            "logs": [
                # Transfer event
                {
                    "topics": [
                        EVENT_TOPICS["Transfer"],
                        pad_address(CALLER_ADDRESS),
                        pad_address(MORPHO_ADDRESS),
                    ],
                    "data": "0x" + encode_uint256(assets),
                    "address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
                },
                # Supply event
                {
                    "topics": [
                        EVENT_TOPICS["Supply"],
                        SAMPLE_MARKET_ID,
                        pad_address(CALLER_ADDRESS),
                    ],
                    "data": "0x" + encode_address(CALLER_ADDRESS) + encode_uint256(assets) + encode_uint256(shares),
                    "address": MORPHO_ADDRESS,
                },
            ],
        }
        result = parser.parse_receipt(receipt)
        assert result.success is True
        assert len(result.events) == 2

        # Check event order
        assert result.events[0].event_type == MorphoBlueEventType.TRANSFER
        assert result.events[1].event_type == MorphoBlueEventType.SUPPLY


# =============================================================================
# Edge Cases Tests
# =============================================================================


class TestEdgeCases:
    """Tests for edge cases."""

    def test_parse_log_with_bytes_topics(self, parser: MorphoBlueReceiptParser) -> None:
        """Test parsing log with bytes topics."""
        assets = 1000 * 10**6
        shares = 1000 * 10**18

        receipt = {
            "transactionHash": "0x" + "1" * 64,
            "blockNumber": 12345,
            "logs": [
                {
                    "topics": [
                        bytes.fromhex(EVENT_TOPICS["Supply"][2:]),  # bytes instead of hex
                        bytes.fromhex(SAMPLE_MARKET_ID[2:]),
                        bytes.fromhex(pad_address(ON_BEHALF_OF_ADDRESS)[2:]),
                    ],
                    "data": bytes.fromhex(
                        encode_address(CALLER_ADDRESS) + encode_uint256(assets) + encode_uint256(shares)
                    ),
                    "address": bytes.fromhex(MORPHO_ADDRESS[2:]),
                }
            ],
        }
        result = parser.parse_receipt(receipt)
        assert result.success is True
        assert len(result.events) == 1

    def test_parse_log_no_topics(self, parser: MorphoBlueReceiptParser) -> None:
        """Test parsing log with no topics."""
        receipt = {
            "transactionHash": "0x" + "1" * 64,
            "blockNumber": 12345,
            "logs": [
                {
                    "topics": [],
                    "data": "0x" + "0" * 64,
                    "address": MORPHO_ADDRESS,
                }
            ],
        }
        result = parser.parse_receipt(receipt)
        assert result.success is True
        assert len(result.events) == 0  # No valid events

    def test_parse_with_timestamp(self, parser: MorphoBlueReceiptParser) -> None:
        """Test parsing with custom timestamp."""
        timestamp = datetime(2024, 1, 1, 12, 0, 0)
        receipt = {
            "transactionHash": "0x" + "1" * 64,
            "blockNumber": 12345,
            "logs": [
                {
                    "topics": [
                        EVENT_TOPICS["Supply"],
                        SAMPLE_MARKET_ID,
                        pad_address(ON_BEHALF_OF_ADDRESS),
                    ],
                    "data": "0x" + encode_address(CALLER_ADDRESS) + encode_uint256(1000) + encode_uint256(1000),
                    "address": MORPHO_ADDRESS,
                }
            ],
        }
        result = parser.parse_receipt(receipt, timestamp=timestamp)
        assert result.success is True
        assert result.events[0].timestamp == timestamp
