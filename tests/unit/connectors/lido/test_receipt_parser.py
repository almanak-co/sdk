"""Tests for Lido Receipt Parser.

This module contains unit tests for the LidoReceiptParser class,
covering all event types including staking, wrapping, unwrapping,
withdrawal requests, and withdrawal claims.
"""

from decimal import Decimal

import pytest

from almanak.connectors.lido.receipt_parser import (
    EVENT_TOPICS,
    TOPIC_TO_EVENT,
    LidoEventType,
    LidoReceiptParser,
    ParseResult,
    StakeEventData,
    UnwrapEventData,
    WithdrawalClaimedEventData,
    WithdrawalRequestedEventData,
    WrapEventData,
)

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def parser() -> LidoReceiptParser:
    """Create a test parser instance for Ethereum."""
    return LidoReceiptParser(chain="ethereum")


@pytest.fixture
def arbitrum_parser() -> LidoReceiptParser:
    """Create a test parser instance for Arbitrum."""
    return LidoReceiptParser(chain="arbitrum")


# =============================================================================
# Constants Tests
# =============================================================================


class TestEventTopics:
    """Tests for event topic signatures."""

    def test_submitted_topic_exists(self) -> None:
        """Test that Submitted event topic is defined."""
        assert "Submitted" in EVENT_TOPICS
        assert EVENT_TOPICS["Submitted"].startswith("0x")

    def test_transfer_topic_exists(self) -> None:
        """Test that Transfer event topic is defined."""
        assert "Transfer" in EVENT_TOPICS
        assert EVENT_TOPICS["Transfer"].startswith("0x")

    def test_withdrawal_requested_topic_exists(self) -> None:
        """Test that WithdrawalRequested event topic is defined."""
        assert "WithdrawalRequested" in EVENT_TOPICS
        assert EVENT_TOPICS["WithdrawalRequested"].startswith("0x")
        assert (
            EVENT_TOPICS["WithdrawalRequested"] == "0xf0cb471f23fb74ea44b8252eb1881a2dca546288d9f6e90d1a0e82fe0ed342ab"
        )

    def test_withdrawal_claimed_topic_exists(self) -> None:
        """Test that WithdrawalClaimed event topic is defined."""
        assert "WithdrawalClaimed" in EVENT_TOPICS
        assert EVENT_TOPICS["WithdrawalClaimed"].startswith("0x")
        assert EVENT_TOPICS["WithdrawalClaimed"] == "0x6ad26c5e238e7d002799f9a5db07e81ef14e37386ae03496d7a7ef04713e145b"

    def test_topic_to_event_reverse_lookup(self) -> None:
        """Test that TOPIC_TO_EVENT is correctly populated."""
        for event_name, topic in EVENT_TOPICS.items():
            assert TOPIC_TO_EVENT[topic] == event_name


# =============================================================================
# Parser Initialization Tests
# =============================================================================


class TestLidoReceiptParserInit:
    """Tests for LidoReceiptParser initialization."""

    def test_init_ethereum(self, parser: LidoReceiptParser) -> None:
        """Test parser initialization for Ethereum."""
        assert parser.chain == "ethereum"
        assert parser.steth_address != ""
        assert parser.wsteth_address != ""
        assert parser.withdrawal_queue_address != ""

    def test_init_arbitrum(self, arbitrum_parser: LidoReceiptParser) -> None:
        """Test parser initialization for Arbitrum."""
        assert arbitrum_parser.chain == "arbitrum"
        assert arbitrum_parser.steth_address == ""  # No stETH on L2s
        assert arbitrum_parser.wsteth_address != ""
        assert arbitrum_parser.withdrawal_queue_address == ""  # No withdrawal queue on L2s

    def test_known_topics_includes_all_events(self, parser: LidoReceiptParser) -> None:
        """Test that all event topics are in the known topics set."""
        for topic in EVENT_TOPICS.values():
            assert parser.is_lido_event(topic)


# =============================================================================
# Event Type Detection Tests
# =============================================================================


class TestGetEventType:
    """Tests for event type detection."""

    def test_get_event_type_stake(self, parser: LidoReceiptParser) -> None:
        """Test getting event type for Submitted event."""
        topic = EVENT_TOPICS["Submitted"]
        assert parser.get_event_type(topic) == LidoEventType.STAKE

    def test_get_event_type_withdrawal_requested(self, parser: LidoReceiptParser) -> None:
        """Test getting event type for WithdrawalRequested event."""
        topic = EVENT_TOPICS["WithdrawalRequested"]
        assert parser.get_event_type(topic) == LidoEventType.WITHDRAWAL_REQUESTED

    def test_get_event_type_withdrawal_claimed(self, parser: LidoReceiptParser) -> None:
        """Test getting event type for WithdrawalClaimed event."""
        topic = EVENT_TOPICS["WithdrawalClaimed"]
        assert parser.get_event_type(topic) == LidoEventType.WITHDRAWAL_CLAIMED

    def test_get_event_type_unknown(self, parser: LidoReceiptParser) -> None:
        """Test getting event type for unknown topic."""
        topic = "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
        assert parser.get_event_type(topic) == LidoEventType.UNKNOWN


# =============================================================================
# Withdrawal Requested Parsing Tests
# =============================================================================


class TestParseWithdrawalRequested:
    """Tests for parsing WithdrawalRequested events."""

    def test_parse_withdrawal_requested_log(self, parser: LidoReceiptParser) -> None:
        """Test parsing a single WithdrawalRequested log entry."""
        # WithdrawalRequested(uint256 indexed requestId, address indexed requestor,
        #                    address indexed owner, uint256 amountOfStETH, uint256 amountOfShares)
        log = {
            "topics": [
                EVENT_TOPICS["WithdrawalRequested"],
                # requestId = 12345 (padded to 32 bytes)
                "0x0000000000000000000000000000000000000000000000000000000000003039",
                # requestor (padded address)
                "0x000000000000000000000000abcdefabcdefabcdefabcdefabcdefabcdefabcd",
                # owner (padded address)
                "0x0000000000000000000000001234567890123456789012345678901234567890",
            ],
            # data: amountOfStETH (1e18), amountOfShares (0.5e18)
            "data": "0x"
            + "0000000000000000000000000000000000000000000000000de0b6b3a7640000"  # 1 ETH
            + "0000000000000000000000000000000000000000000000000707e5e5e5e5e5e5",  # ~0.505 shares
        }

        result = parser.parse_withdrawal_requested(log)

        assert result is not None
        assert result.request_id == 12345
        assert result.requestor == "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"
        assert result.owner == "0x1234567890123456789012345678901234567890"
        assert result.amount_of_steth == Decimal("1")
        assert result.amount_of_shares > Decimal("0")

    def test_parse_withdrawal_requested_in_receipt(self, parser: LidoReceiptParser) -> None:
        """Test parsing WithdrawalRequested from full receipt."""
        withdrawal_queue_address = "0x889edC2eDab5f40e902b864aD4d7AdE8E412F9B1"
        receipt = {
            "transactionHash": "0xabc123",
            "blockNumber": 12345678,
            "logs": [
                {
                    "address": withdrawal_queue_address,
                    "topics": [
                        EVENT_TOPICS["WithdrawalRequested"],
                        "0x0000000000000000000000000000000000000000000000000000000000000064",  # requestId=100
                        "0x000000000000000000000000abcdefabcdefabcdefabcdefabcdefabcdefabcd",
                        "0x000000000000000000000000abcdefabcdefabcdefabcdefabcdefabcdefabcd",
                    ],
                    "data": "0x"
                    + "0000000000000000000000000000000000000000000000001bc16d674ec80000"  # 2 ETH
                    + "0000000000000000000000000000000000000000000000001bc16d674ec80000",
                }
            ],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.withdrawal_requests) == 1
        assert result.withdrawal_requests[0].request_id == 100
        assert result.withdrawal_requests[0].amount_of_steth == Decimal("2")

    def test_parse_withdrawal_requested_bytes_topics(self, parser: LidoReceiptParser) -> None:
        """Test parsing WithdrawalRequested with bytes topics (web3.py format)."""
        log = {
            "topics": [
                bytes.fromhex(EVENT_TOPICS["WithdrawalRequested"][2:]),
                bytes.fromhex("0000000000000000000000000000000000000000000000000000000000000001"),
                bytes.fromhex("000000000000000000000000abcdefabcdefabcdefabcdefabcdefabcdefabcd"),
                bytes.fromhex("0000000000000000000000001234567890123456789012345678901234567890"),
            ],
            "data": bytes.fromhex(
                "0000000000000000000000000000000000000000000000000de0b6b3a7640000"
                + "0000000000000000000000000000000000000000000000000de0b6b3a7640000"
            ),
        }

        result = parser.parse_withdrawal_requested(log)

        assert result is not None
        assert result.request_id == 1

    def test_parse_withdrawal_requested_to_dict(self, parser: LidoReceiptParser) -> None:
        """Test WithdrawalRequestedEventData to_dict method."""
        data = WithdrawalRequestedEventData(
            request_id=100,
            requestor="0xabcd",
            owner="0x1234",
            amount_of_steth=Decimal("1.5"),
            amount_of_shares=Decimal("1.2"),
        )
        d = data.to_dict()

        assert d["request_id"] == 100
        assert d["requestor"] == "0xabcd"
        assert d["owner"] == "0x1234"
        assert d["amount_of_steth"] == "1.5"
        assert d["amount_of_shares"] == "1.2"


# =============================================================================
# Withdrawal Claimed Parsing Tests
# =============================================================================


class TestParseWithdrawalsClaimed:
    """Tests for parsing WithdrawalClaimed events."""

    def test_parse_withdrawals_claimed_log(self, parser: LidoReceiptParser) -> None:
        """Test parsing a single WithdrawalClaimed log entry."""
        # WithdrawalClaimed(uint256 indexed requestId, address indexed owner,
        #                  address indexed receiver, uint256 amountOfETH)
        log = {
            "topics": [
                EVENT_TOPICS["WithdrawalClaimed"],
                # requestId = 100 (padded to 32 bytes)
                "0x0000000000000000000000000000000000000000000000000000000000000064",
                # owner (padded address)
                "0x000000000000000000000000abcdefabcdefabcdefabcdefabcdefabcdefabcd",
                # receiver (padded address)
                "0x0000000000000000000000001234567890123456789012345678901234567890",
            ],
            # data: amountOfETH (1e18)
            "data": "0x0000000000000000000000000000000000000000000000000de0b6b3a7640000",
        }

        result = parser.parse_withdrawals_claimed(log)

        assert result is not None
        assert result.request_id == 100
        assert result.owner == "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"
        assert result.receiver == "0x1234567890123456789012345678901234567890"
        assert result.amount_of_eth == Decimal("1")

    def test_parse_withdrawals_claimed_in_receipt(self, parser: LidoReceiptParser) -> None:
        """Test parsing WithdrawalClaimed from full receipt."""
        withdrawal_queue_address = "0x889edC2eDab5f40e902b864aD4d7AdE8E412F9B1"
        receipt = {
            "transactionHash": "0xdef456",
            "blockNumber": 12345679,
            "logs": [
                {
                    "address": withdrawal_queue_address,
                    "topics": [
                        EVENT_TOPICS["WithdrawalClaimed"],
                        "0x00000000000000000000000000000000000000000000000000000000000000c8",  # requestId=200
                        "0x000000000000000000000000abcdefabcdefabcdefabcdefabcdefabcdefabcd",
                        "0x000000000000000000000000abcdefabcdefabcdefabcdefabcdefabcdefabcd",
                    ],
                    "data": "0x000000000000000000000000000000000000000000000002b5e3af16b1880000",  # 50 ETH
                }
            ],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.withdrawal_claims) == 1
        assert result.withdrawal_claims[0].request_id == 200
        assert result.withdrawal_claims[0].amount_of_eth == Decimal("50")

    def test_parse_withdrawals_claimed_bytes_topics(self, parser: LidoReceiptParser) -> None:
        """Test parsing WithdrawalClaimed with bytes topics (web3.py format)."""
        log = {
            "topics": [
                bytes.fromhex(EVENT_TOPICS["WithdrawalClaimed"][2:]),
                bytes.fromhex("0000000000000000000000000000000000000000000000000000000000000005"),
                bytes.fromhex("000000000000000000000000abcdefabcdefabcdefabcdefabcdefabcdefabcd"),
                bytes.fromhex("0000000000000000000000001234567890123456789012345678901234567890"),
            ],
            "data": bytes.fromhex("0000000000000000000000000000000000000000000000000de0b6b3a7640000"),
        }

        result = parser.parse_withdrawals_claimed(log)

        assert result is not None
        assert result.request_id == 5
        assert result.amount_of_eth == Decimal("1")

    def test_parse_withdrawals_claimed_to_dict(self, parser: LidoReceiptParser) -> None:
        """Test WithdrawalClaimedEventData to_dict method."""
        data = WithdrawalClaimedEventData(
            request_id=200,
            owner="0xabcd",
            receiver="0x1234",
            amount_of_eth=Decimal("3.5"),
        )
        d = data.to_dict()

        assert d["request_id"] == 200
        assert d["owner"] == "0xabcd"
        assert d["receiver"] == "0x1234"
        assert d["amount_of_eth"] == "3.5"


# =============================================================================
# Receipt Parsing with Multiple Events
# =============================================================================


class TestParseReceiptWithMultipleEvents:
    """Tests for parsing receipts with multiple events."""

    def test_parse_receipt_with_all_event_types(self, parser: LidoReceiptParser) -> None:
        """Test parsing a receipt that contains all event types."""
        steth = "0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84"
        wsteth = "0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0"
        withdrawal_queue = "0x889edC2eDab5f40e902b864aD4d7AdE8E412F9B1"

        receipt = {
            "transactionHash": "0xmulti123",
            "blockNumber": 12345680,
            "logs": [
                # Stake event
                {
                    "address": steth,
                    "topics": [
                        EVENT_TOPICS["Submitted"],
                        "0x000000000000000000000000abcdefabcdefabcdefabcdefabcdefabcdefabcd",
                    ],
                    "data": "0x"
                    + "0000000000000000000000000000000000000000000000000de0b6b3a7640000"  # 1 ETH
                    + "0000000000000000000000000000000000000000000000000000000000000000",  # referral
                },
                # Wrap event (Transfer from zero address)
                {
                    "address": wsteth,
                    "topics": [
                        EVENT_TOPICS["Transfer"],
                        "0x0000000000000000000000000000000000000000000000000000000000000000",  # from=zero
                        "0x000000000000000000000000abcdefabcdefabcdefabcdefabcdefabcdefabcd",  # to=user
                    ],
                    "data": "0x0000000000000000000000000000000000000000000000000de0b6b3a7640000",
                },
                # WithdrawalRequested event
                {
                    "address": withdrawal_queue,
                    "topics": [
                        EVENT_TOPICS["WithdrawalRequested"],
                        "0x0000000000000000000000000000000000000000000000000000000000000001",
                        "0x000000000000000000000000abcdefabcdefabcdefabcdefabcdefabcdefabcd",
                        "0x000000000000000000000000abcdefabcdefabcdefabcdefabcdefabcdefabcd",
                    ],
                    "data": "0x"
                    + "0000000000000000000000000000000000000000000000000de0b6b3a7640000"
                    + "0000000000000000000000000000000000000000000000000de0b6b3a7640000",
                },
                # WithdrawalClaimed event
                {
                    "address": withdrawal_queue,
                    "topics": [
                        EVENT_TOPICS["WithdrawalClaimed"],
                        "0x0000000000000000000000000000000000000000000000000000000000000002",
                        "0x000000000000000000000000abcdefabcdefabcdefabcdefabcdefabcdefabcd",
                        "0x000000000000000000000000abcdefabcdefabcdefabcdefabcdefabcdefabcd",
                    ],
                    "data": "0x0000000000000000000000000000000000000000000000000de0b6b3a7640000",
                },
            ],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.stakes) == 1
        assert len(result.wraps) == 1
        assert len(result.withdrawal_requests) == 1
        assert len(result.withdrawal_claims) == 1
        assert result.withdrawal_requests[0].request_id == 1
        assert result.withdrawal_claims[0].request_id == 2

    def test_parse_receipt_ignores_non_lido_events(self, parser: LidoReceiptParser) -> None:
        """Test that receipt parsing ignores events from non-Lido contracts."""
        random_contract = "0x1111111111111111111111111111111111111111"
        withdrawal_queue = "0x889edC2eDab5f40e902b864aD4d7AdE8E412F9B1"

        receipt = {
            "transactionHash": "0xignore123",
            "blockNumber": 12345681,
            "logs": [
                # WithdrawalRequested from wrong contract - should be ignored
                {
                    "address": random_contract,
                    "topics": [
                        EVENT_TOPICS["WithdrawalRequested"],
                        "0x0000000000000000000000000000000000000000000000000000000000000001",
                        "0x000000000000000000000000abcdefabcdefabcdefabcdefabcdefabcdefabcd",
                        "0x000000000000000000000000abcdefabcdefabcdefabcdefabcdefabcdefabcd",
                    ],
                    "data": "0x"
                    + "0000000000000000000000000000000000000000000000000de0b6b3a7640000"
                    + "0000000000000000000000000000000000000000000000000de0b6b3a7640000",
                },
                # WithdrawalRequested from correct contract - should be parsed
                {
                    "address": withdrawal_queue,
                    "topics": [
                        EVENT_TOPICS["WithdrawalRequested"],
                        "0x0000000000000000000000000000000000000000000000000000000000000002",
                        "0x000000000000000000000000abcdefabcdefabcdefabcdefabcdefabcdefabcd",
                        "0x000000000000000000000000abcdefabcdefabcdefabcdefabcdefabcdefabcd",
                    ],
                    "data": "0x"
                    + "0000000000000000000000000000000000000000000000000de0b6b3a7640000"
                    + "0000000000000000000000000000000000000000000000000de0b6b3a7640000",
                },
            ],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.withdrawal_requests) == 1
        assert result.withdrawal_requests[0].request_id == 2

    def test_parse_receipt_empty_logs(self, parser: LidoReceiptParser) -> None:
        """Test parsing receipt with no logs."""
        receipt = {
            "transactionHash": "0xempty123",
            "blockNumber": 12345682,
            "logs": [],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.stakes) == 0
        assert len(result.wraps) == 0
        assert len(result.unwraps) == 0
        assert len(result.withdrawal_requests) == 0
        assert len(result.withdrawal_claims) == 0


# =============================================================================
# ParseResult Tests
# =============================================================================


class TestParseResult:
    """Tests for ParseResult data class."""

    def test_parse_result_to_dict_with_all_events(self) -> None:
        """Test ParseResult to_dict with all event types populated."""
        result = ParseResult(
            success=True,
            stakes=[StakeEventData(sender="0xsender", amount=Decimal("1"), referral="0x0")],
            wraps=[WrapEventData(from_address="0x0", to_address="0xuser", amount=Decimal("1"))],
            unwraps=[UnwrapEventData(from_address="0xuser", to_address="0x0", amount=Decimal("1"))],
            withdrawal_requests=[
                WithdrawalRequestedEventData(
                    request_id=1,
                    requestor="0xreq",
                    owner="0xowner",
                    amount_of_steth=Decimal("1"),
                    amount_of_shares=Decimal("1"),
                )
            ],
            withdrawal_claims=[
                WithdrawalClaimedEventData(
                    request_id=1,
                    owner="0xowner",
                    receiver="0xreceiver",
                    amount_of_eth=Decimal("1"),
                )
            ],
            transaction_hash="0xtest",
            block_number=12345,
        )

        d = result.to_dict()

        assert d["success"] is True
        assert len(d["stakes"]) == 1
        assert len(d["wraps"]) == 1
        assert len(d["unwraps"]) == 1
        assert len(d["withdrawal_requests"]) == 1
        assert len(d["withdrawal_claims"]) == 1
        assert d["transaction_hash"] == "0xtest"
        assert d["block_number"] == 12345


# =============================================================================
# Edge Cases
# =============================================================================


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    def test_parse_receipt_with_bytes_tx_hash(self, parser: LidoReceiptParser) -> None:
        """Test parsing receipt when transactionHash is bytes."""
        receipt = {
            "transactionHash": bytes.fromhex("abc123def456abc123def456abc123def456abc123def456abc123def456abc1"),
            "blockNumber": 12345,
            "logs": [],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert result.transaction_hash.startswith("0x")

    def test_parse_receipt_with_bytes_address(self, parser: LidoReceiptParser) -> None:
        """Test parsing receipt when contract address is bytes."""
        withdrawal_queue_bytes = bytes.fromhex("889edC2eDab5f40e902b864aD4d7AdE8E412F9B1")
        receipt = {
            "transactionHash": "0xtest",
            "blockNumber": 12345,
            "logs": [
                {
                    "address": withdrawal_queue_bytes,
                    "topics": [
                        EVENT_TOPICS["WithdrawalClaimed"],
                        "0x0000000000000000000000000000000000000000000000000000000000000001",
                        "0x000000000000000000000000abcdefabcdefabcdefabcdefabcdefabcdefabcd",
                        "0x000000000000000000000000abcdefabcdefabcdefabcdefabcdefabcdefabcd",
                    ],
                    "data": "0x0000000000000000000000000000000000000000000000000de0b6b3a7640000",
                }
            ],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.withdrawal_claims) == 1

    def test_parse_receipt_with_log_without_topics(self, parser: LidoReceiptParser) -> None:
        """Test parsing receipt with log entry that has no topics."""
        receipt = {
            "transactionHash": "0xtest",
            "blockNumber": 12345,
            "logs": [
                {
                    "address": "0x889edC2eDab5f40e902b864aD4d7AdE8E412F9B1",
                    "topics": [],
                    "data": "0x",
                }
            ],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.withdrawal_requests) == 0
        assert len(result.withdrawal_claims) == 0

    def test_is_lido_event_true(self, parser: LidoReceiptParser) -> None:
        """Test is_lido_event returns True for known topics."""
        assert parser.is_lido_event(EVENT_TOPICS["WithdrawalRequested"]) is True
        assert parser.is_lido_event(EVENT_TOPICS["WithdrawalClaimed"]) is True

    def test_is_lido_event_false(self, parser: LidoReceiptParser) -> None:
        """Test is_lido_event returns False for unknown topics."""
        unknown = "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
        assert parser.is_lido_event(unknown) is False


# =============================================================================
# extract_wsteth_received Tests
# =============================================================================


class TestExtractWstethReceived:
    """Tests for extract_wsteth_received enricher method."""

    def test_extract_wsteth_from_stake_with_wrap(self, parser: LidoReceiptParser) -> None:
        """Test extracting wstETH amount from a stake+wrap receipt (receive_wrapped=True)."""
        steth = "0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84"
        wsteth = "0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0"

        receipt = {
            "transactionHash": "0xstakewrap123",
            "blockNumber": 19000000,
            "logs": [
                # Submitted event (1 ETH staked)
                {
                    "address": steth,
                    "topics": [
                        EVENT_TOPICS["Submitted"],
                        "0x000000000000000000000000abcdefabcdefabcdefabcdefabcdefabcdefabcd",
                    ],
                    "data": "0x"
                    + "0000000000000000000000000000000000000000000000000de0b6b3a7640000"  # 1 ETH
                    + "0000000000000000000000000000000000000000000000000000000000000000",
                },
                # Transfer (mint) event on wstETH: 0 -> user, 0.85 wstETH
                {
                    "address": wsteth,
                    "topics": [
                        EVENT_TOPICS["Transfer"],
                        "0x0000000000000000000000000000000000000000000000000000000000000000",  # from=zero (mint)
                        "0x000000000000000000000000abcdefabcdefabcdefabcdefabcdefabcdefabcd",  # to=user
                    ],
                    # 0.85 wstETH = 850000000000000000 wei
                    "data": "0x0000000000000000000000000000000000000000000000000bcbce7f1b150000",
                },
            ],
        }

        wsteth_wei = parser.extract_wsteth_received(receipt)
        assert wsteth_wei is not None
        assert wsteth_wei == 850000000000000000  # 0.85 wstETH in wei

    def test_extract_wsteth_returns_none_for_plain_stake(self, parser: LidoReceiptParser) -> None:
        """Test that extract_wsteth_received returns None when no wrap event exists."""
        steth = "0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84"

        receipt = {
            "transactionHash": "0xplainstake",
            "blockNumber": 19000001,
            "logs": [
                # Only Submitted event, no wrap
                {
                    "address": steth,
                    "topics": [
                        EVENT_TOPICS["Submitted"],
                        "0x000000000000000000000000abcdefabcdefabcdefabcdefabcdefabcdefabcd",
                    ],
                    "data": "0x"
                    + "0000000000000000000000000000000000000000000000000de0b6b3a7640000"
                    + "0000000000000000000000000000000000000000000000000000000000000000",
                },
            ],
        }

        wsteth_wei = parser.extract_wsteth_received(receipt)
        assert wsteth_wei is None

    def test_extract_wsteth_returns_none_for_empty_receipt(self, parser: LidoReceiptParser) -> None:
        """Test that extract_wsteth_received returns None for receipt with no logs."""
        receipt = {
            "transactionHash": "0xempty",
            "blockNumber": 19000002,
            "logs": [],
        }

        assert parser.extract_wsteth_received(receipt) is None

    def test_extract_wsteth_and_shares_both_populated(self, parser: LidoReceiptParser) -> None:
        """Test that both shares_received and wsteth_received can be extracted from same receipt."""
        steth = "0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84"
        wsteth = "0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0"

        receipt = {
            "transactionHash": "0xboth",
            "blockNumber": 19000003,
            "logs": [
                {
                    "address": steth,
                    "topics": [
                        EVENT_TOPICS["Submitted"],
                        "0x000000000000000000000000abcdefabcdefabcdefabcdefabcdefabcdefabcd",
                    ],
                    "data": "0x"
                    + "0000000000000000000000000000000000000000000000000de0b6b3a7640000"
                    + "0000000000000000000000000000000000000000000000000000000000000000",
                },
                {
                    "address": wsteth,
                    "topics": [
                        EVENT_TOPICS["Transfer"],
                        "0x0000000000000000000000000000000000000000000000000000000000000000",
                        "0x000000000000000000000000abcdefabcdefabcdefabcdefabcdefabcdefabcd",
                    ],
                    "data": "0x0000000000000000000000000000000000000000000000000bcbce7f1b150000",
                },
            ],
        }

        # Both should be available
        shares = parser.extract_shares_received(receipt)
        wsteth_amt = parser.extract_wsteth_received(receipt)

        assert shares is not None
        assert wsteth_amt is not None
        # shares_received returns wstETH amount when wraps exist (existing behavior)
        assert shares == wsteth_amt


# =============================================================================
# extract_primitive_money_legs Tests (VIB-5220)
# =============================================================================

_STETH = "0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84"
_WSTETH = "0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0"
_USER = "0x000000000000000000000000abcdefabcdefabcdefabcdefabcdefabcdefabcd"
_ZERO_TOPIC = "0x" + "0" * 64
_ONE_ETH_DATA = "0x" + "0000000000000000000000000000000000000000000000000de0b6b3a7640000" + "0" * 64
_ONE_ETH_VALUE = "0x0000000000000000000000000000000000000000000000000de0b6b3a7640000"  # 1.0 (18dp)
_085_VALUE = "0x0000000000000000000000000000000000000000000000000bcbce7f1b150000"  # 0.85 (18dp)


def _submitted_log() -> dict:
    """A Lido ``Submitted`` (1 ETH staked) log on the stETH contract."""
    return {"address": _STETH, "topics": [EVENT_TOPICS["Submitted"], _USER], "data": _ONE_ETH_DATA}


def _steth_mint_log(value_hex: str = _ONE_ETH_VALUE) -> dict:
    """A stETH mint ``Transfer(0x0 -> staker)`` log — the plain-stake output leg."""
    return {"address": _STETH, "topics": [EVENT_TOPICS["Transfer"], _ZERO_TOPIC, _USER], "data": value_hex}


def _wsteth_mint_log(value_hex: str = _085_VALUE) -> dict:
    """A wstETH wrap-mint ``Transfer(0x0 -> staker)`` log — the wrapped-stake output."""
    return {"address": _WSTETH, "topics": [EVENT_TOPICS["Transfer"], _ZERO_TOPIC, _USER], "data": value_hex}


class TestParseStethMint:
    """The stETH mint Transfer(0x0 -> staker) is captured as a measured output leg."""

    def test_steth_mint_captured_as_stake_output(self, parser: LidoReceiptParser) -> None:
        receipt = {"transactionHash": "0xmint", "blockNumber": 1, "logs": [_submitted_log(), _steth_mint_log()]}
        result = parser.parse_receipt(receipt)
        assert len(result.stake_outputs) == 1
        assert result.stake_outputs[0].amount == Decimal("1")
        assert result.stake_outputs[0].recipient.lower().endswith("abcd")
        assert result.stake_outputs[0].token == _STETH.lower()

    def test_no_steth_mint_for_plain_submit_only(self, parser: LidoReceiptParser) -> None:
        receipt = {"transactionHash": "0xsub", "blockNumber": 1, "logs": [_submitted_log()]}
        result = parser.parse_receipt(receipt)
        assert result.stake_outputs == []


class TestExtractPrimitiveMoneyLegs:
    """VIB-5220 — Lido declares STAKE money legs (INPUT=ETH, OUTPUT=stETH/wstETH)
    as a typed ``PrimitiveMoneyLegs`` for the US-009 ledger dispatcher."""

    def _roles(self, legs):
        return [(leg.role.value, leg.token) for leg in legs.legs]

    def test_plain_stake_declares_eth_in_steth_out(self, parser: LidoReceiptParser) -> None:
        """Repro of the #2897 case: stETH mint Transfer(0x0 -> staker) books as the
        measured OUTPUT leg; INPUT is the submitted ETH."""
        from almanak.framework.accounting.measured import MeasuredMoney

        receipt = {"transactionHash": "0xplain", "blockNumber": 1, "logs": [_submitted_log(), _steth_mint_log()]}
        legs = parser.extract_primitive_money_legs(receipt)
        assert legs is not None
        assert self._roles(legs) == [("input", "ETH"), ("output", "stETH")]
        assert legs.total_input() == MeasuredMoney.measured(Decimal("1"))
        assert legs.total_output() == MeasuredMoney.measured(Decimal("1"))

    def test_wrapped_stake_declares_eth_in_wsteth_out(self, parser: LidoReceiptParser) -> None:
        """receive_wrapped=True: the OUTPUT leg is the measured wstETH wrap-mint."""
        from almanak.framework.accounting.measured import MeasuredMoney

        receipt = {
            "transactionHash": "0xwrap",
            "blockNumber": 1,
            "logs": [_submitted_log(), _steth_mint_log(), _wsteth_mint_log()],
        }
        legs = parser.extract_primitive_money_legs(receipt)
        assert legs is not None
        assert self._roles(legs) == [("input", "ETH"), ("output", "wstETH")]
        assert legs.total_input() == MeasuredMoney.measured(Decimal("1"))
        assert legs.total_output() == MeasuredMoney.measured(Decimal("0.85"))

    def test_missing_mint_output_is_unmeasured_not_zero(self, parser: LidoReceiptParser) -> None:
        """No mint Transfer in the receipt → OUTPUT token known (stETH) but amount
        UNMEASURED — never an ETH-input proxy, never a fabricated measured zero."""
        receipt = {"transactionHash": "0xsubonly", "blockNumber": 1, "logs": [_submitted_log()]}
        legs = parser.extract_primitive_money_legs(receipt)
        assert legs is not None
        out = legs.output_legs[0]
        assert out.token == "stETH"
        assert out.amount.is_unmeasured
        # INPUT still measured from the Submitted event.
        assert legs.input_legs[0].amount.is_measured

    def test_non_stake_receipt_returns_none(self, parser: LidoReceiptParser) -> None:
        """No Submitted event → not a stake → None (dispatcher falls back)."""
        receipt = {"transactionHash": "0xnone", "blockNumber": 1, "logs": []}
        assert parser.extract_primitive_money_legs(receipt) is None
