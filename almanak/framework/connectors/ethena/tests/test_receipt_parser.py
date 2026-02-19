"""Tests for Ethena Receipt Parser.

This module contains unit tests for the EthenaReceiptParser class,
covering stake (Deposit) and withdraw event parsing.
"""

from decimal import Decimal

import pytest

from ..receipt_parser import (
    ETHENA_EVENT_SIGNATURES,
    EVENT_TOPICS,
    TOPIC_TO_EVENT,
    EthenaEventType,
    EthenaReceiptParser,
    ParseResult,
    StakeEventData,
    WithdrawEventData,
)

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def parser() -> EthenaReceiptParser:
    """Create a test parser instance for Ethereum."""
    return EthenaReceiptParser(chain="ethereum")


# =============================================================================
# Constants Tests
# =============================================================================


class TestEventTopics:
    """Tests for event topic signatures."""

    def test_deposit_topic_exists(self) -> None:
        """Test that Deposit event topic is defined."""
        assert "Deposit" in EVENT_TOPICS
        assert EVENT_TOPICS["Deposit"].startswith("0x")
        assert EVENT_TOPICS["Deposit"] == "0xdcbc1c05240f31ff3ad067ef1ee35ce4997762752e3a095284754544f4c709d7"

    def test_withdraw_topic_exists(self) -> None:
        """Test that Withdraw event topic is defined."""
        assert "Withdraw" in EVENT_TOPICS
        assert EVENT_TOPICS["Withdraw"].startswith("0x")
        assert EVENT_TOPICS["Withdraw"] == "0xfbde797d201c681b91056529119e0b02407c7bb96a4a2c75c01fc9667232c8db"

    def test_ethena_event_signatures_is_alias(self) -> None:
        """Test that ETHENA_EVENT_SIGNATURES is an alias for EVENT_TOPICS."""
        assert ETHENA_EVENT_SIGNATURES is EVENT_TOPICS
        assert "Deposit" in ETHENA_EVENT_SIGNATURES
        assert "Withdraw" in ETHENA_EVENT_SIGNATURES

    def test_topic_to_event_reverse_lookup(self) -> None:
        """Test that TOPIC_TO_EVENT is correctly populated."""
        for event_name, topic in EVENT_TOPICS.items():
            assert TOPIC_TO_EVENT[topic] == event_name


# =============================================================================
# Parser Initialization Tests
# =============================================================================


class TestEthenaReceiptParserInit:
    """Tests for EthenaReceiptParser initialization."""

    def test_init_ethereum(self, parser: EthenaReceiptParser) -> None:
        """Test parser initialization for Ethereum."""
        assert parser.chain == "ethereum"
        assert parser.usde_address != ""
        assert parser.susde_address != ""

    def test_init_unknown_chain(self) -> None:
        """Test parser initialization for unknown chain."""
        parser = EthenaReceiptParser(chain="unknown")
        assert parser.chain == "unknown"
        assert parser.usde_address == ""
        assert parser.susde_address == ""

    def test_known_topics_includes_all_events(self, parser: EthenaReceiptParser) -> None:
        """Test that all event topics are in the known topics set."""
        for topic in EVENT_TOPICS.values():
            assert parser.is_ethena_event(topic)


# =============================================================================
# Event Type Detection Tests
# =============================================================================


class TestGetEventType:
    """Tests for event type detection."""

    def test_get_event_type_stake(self, parser: EthenaReceiptParser) -> None:
        """Test getting event type for Deposit event."""
        topic = EVENT_TOPICS["Deposit"]
        assert parser.get_event_type(topic) == EthenaEventType.STAKE

    def test_get_event_type_withdraw(self, parser: EthenaReceiptParser) -> None:
        """Test getting event type for Withdraw event."""
        topic = EVENT_TOPICS["Withdraw"]
        assert parser.get_event_type(topic) == EthenaEventType.WITHDRAW

    def test_get_event_type_unknown(self, parser: EthenaReceiptParser) -> None:
        """Test getting event type for unknown topic."""
        topic = "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
        assert parser.get_event_type(topic) == EthenaEventType.UNKNOWN


# =============================================================================
# Withdraw Event Parsing Tests
# =============================================================================


class TestParseWithdraw:
    """Tests for parsing Withdraw events (ERC4626 standard)."""

    def test_parse_withdraw_log(self, parser: EthenaReceiptParser) -> None:
        """Test parsing a single Withdraw log entry."""
        # Withdraw(address indexed sender, address indexed receiver, address indexed owner,
        #          uint256 assets, uint256 shares)
        log = {
            "topics": [
                EVENT_TOPICS["Withdraw"],
                # sender (padded address)
                "0x000000000000000000000000abcdefabcdefabcdefabcdefabcdefabcdefabcd",
                # receiver (padded address)
                "0x0000000000000000000000001234567890123456789012345678901234567890",
                # owner (padded address)
                "0x000000000000000000000000fedcba9876543210fedcba9876543210fedcba98",
            ],
            # data: assets (1e18), shares (0.95e18)
            "data": "0x"
            + "0000000000000000000000000000000000000000000000000de0b6b3a7640000"  # 1 USDe
            + "0000000000000000000000000000000000000000000000000d2f13f7789f0000",  # 0.95 sUSDe
        }

        result = parser.parse_withdraw(log)

        assert result is not None
        assert result.sender == "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"
        assert result.receiver == "0x1234567890123456789012345678901234567890"
        assert result.owner == "0xfedcba9876543210fedcba9876543210fedcba98"
        assert result.assets == Decimal("1")
        assert result.shares == Decimal("0.95")

    def test_parse_withdraw_extracts_assets_and_shares(self, parser: EthenaReceiptParser) -> None:
        """Test that parse_withdraw correctly extracts assets and shares."""
        log = {
            "topics": [
                EVENT_TOPICS["Withdraw"],
                "0x000000000000000000000000abcdefabcdefabcdefabcdefabcdefabcdefabcd",
                "0x0000000000000000000000001234567890123456789012345678901234567890",
                "0x000000000000000000000000abcdefabcdefabcdefabcdefabcdefabcdefabcd",
            ],
            # 2500.5 assets, 2400.25 shares
            "data": "0x"
            + "0000000000000000000000000000000000000000000000878d688dc880420000"  # 2500.5e18
            + "0000000000000000000000000000000000000000000000821e2901ee33590000",  # 2400.25e18
        }

        result = parser.parse_withdraw(log)

        assert result is not None
        assert result.assets == Decimal("2500.5")
        assert result.shares == Decimal("2400.25")

    def test_parse_withdraw_extracts_receiver(self, parser: EthenaReceiptParser) -> None:
        """Test that parse_withdraw correctly extracts receiver address."""
        receiver_addr = "0x9999999999999999999999999999999999999999"
        log = {
            "topics": [
                EVENT_TOPICS["Withdraw"],
                "0x000000000000000000000000abcdefabcdefabcdefabcdefabcdefabcdefabcd",
                "0x0000000000000000000000009999999999999999999999999999999999999999",  # receiver
                "0x000000000000000000000000abcdefabcdefabcdefabcdefabcdefabcdefabcd",
            ],
            "data": "0x"
            + "0000000000000000000000000000000000000000000000000de0b6b3a7640000"
            + "0000000000000000000000000000000000000000000000000de0b6b3a7640000",
        }

        result = parser.parse_withdraw(log)

        assert result is not None
        assert result.receiver == receiver_addr

    def test_parse_withdraw_in_receipt(self, parser: EthenaReceiptParser) -> None:
        """Test parsing Withdraw from full receipt."""
        susde_address = "0x9D39A5DE30e57443BfF2A8307A4256c8797A3497"
        receipt = {
            "transactionHash": "0xabc123",
            "blockNumber": 12345678,
            "logs": [
                {
                    "address": susde_address,
                    "topics": [
                        EVENT_TOPICS["Withdraw"],
                        "0x000000000000000000000000abcdefabcdefabcdefabcdefabcdefabcdefabcd",
                        "0x0000000000000000000000001234567890123456789012345678901234567890",
                        "0x000000000000000000000000abcdefabcdefabcdefabcdefabcdefabcdefabcd",
                    ],
                    "data": "0x"
                    + "0000000000000000000000000000000000000000000000001bc16d674ec80000"  # 2 USDe
                    + "0000000000000000000000000000000000000000000000001bc16d674ec80000",
                }
            ],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.withdraws) == 1
        assert result.withdraws[0].assets == Decimal("2")
        assert result.withdraws[0].receiver == "0x1234567890123456789012345678901234567890"

    def test_parse_withdraw_bytes_topics(self, parser: EthenaReceiptParser) -> None:
        """Test parsing Withdraw with bytes topics (web3.py format)."""
        log = {
            "topics": [
                bytes.fromhex(EVENT_TOPICS["Withdraw"][2:]),
                bytes.fromhex("000000000000000000000000abcdefabcdefabcdefabcdefabcdefabcdefabcd"),
                bytes.fromhex("0000000000000000000000001234567890123456789012345678901234567890"),
                bytes.fromhex("000000000000000000000000fedcba9876543210fedcba9876543210fedcba98"),
            ],
            "data": bytes.fromhex(
                "0000000000000000000000000000000000000000000000000de0b6b3a7640000"
                + "0000000000000000000000000000000000000000000000000de0b6b3a7640000"
            ),
        }

        result = parser.parse_withdraw(log)

        assert result is not None
        assert result.sender == "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"
        assert result.assets == Decimal("1")

    def test_parse_withdraw_to_dict(self) -> None:
        """Test WithdrawEventData to_dict method."""
        data = WithdrawEventData(
            sender="0xsender",
            receiver="0xreceiver",
            owner="0xowner",
            assets=Decimal("1.5"),
            shares=Decimal("1.2"),
        )
        d = data.to_dict()

        assert d["sender"] == "0xsender"
        assert d["receiver"] == "0xreceiver"
        assert d["owner"] == "0xowner"
        assert d["assets"] == "1.5"
        assert d["shares"] == "1.2"

    def test_unstake_alias_for_withdraw(self, parser: EthenaReceiptParser) -> None:
        """Test that parse_unstake is an alias for parse_withdraw."""
        log = {
            "topics": [
                EVENT_TOPICS["Withdraw"],
                "0x000000000000000000000000abcdefabcdefabcdefabcdefabcdefabcdefabcd",
                "0x0000000000000000000000001234567890123456789012345678901234567890",
                "0x000000000000000000000000abcdefabcdefabcdefabcdefabcdefabcdefabcd",
            ],
            "data": "0x"
            + "0000000000000000000000000000000000000000000000000de0b6b3a7640000"
            + "0000000000000000000000000000000000000000000000000de0b6b3a7640000",
        }

        result1 = parser.parse_withdraw(log)
        result2 = parser.parse_unstake(log)

        assert result1 is not None
        assert result2 is not None
        assert result1.sender == result2.sender
        assert result1.receiver == result2.receiver
        assert result1.assets == result2.assets


# =============================================================================
# Stake Event Parsing Tests
# =============================================================================


class TestParseStake:
    """Tests for parsing Deposit (stake) events."""

    def test_parse_stake_log(self, parser: EthenaReceiptParser) -> None:
        """Test parsing a single Deposit (stake) log entry."""
        # Deposit(address indexed sender, address indexed owner, uint256 assets, uint256 shares)
        log = {
            "topics": [
                EVENT_TOPICS["Deposit"],
                # sender (padded address)
                "0x000000000000000000000000abcdefabcdefabcdefabcdefabcdefabcdefabcd",
                # owner (padded address)
                "0x0000000000000000000000001234567890123456789012345678901234567890",
            ],
            # data: assets (1000e18), shares (950e18)
            "data": "0x"
            + "00000000000000000000000000000000000000000000003635c9adc5dea00000"  # 1000 USDe
            + "00000000000000000000000000000000000000000000003382c6fc5df2600000",  # ~950 sUSDe
        }

        result = parser.parse_stake(log)

        assert result is not None
        assert result.sender == "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"
        assert result.owner == "0x1234567890123456789012345678901234567890"
        assert result.assets == Decimal("1000")

    def test_parse_stake_in_receipt(self, parser: EthenaReceiptParser) -> None:
        """Test parsing Deposit (stake) from full receipt."""
        susde_address = "0x9D39A5DE30e57443BfF2A8307A4256c8797A3497"
        receipt = {
            "transactionHash": "0xstake123",
            "blockNumber": 12345678,
            "logs": [
                {
                    "address": susde_address,
                    "topics": [
                        EVENT_TOPICS["Deposit"],
                        "0x000000000000000000000000abcdefabcdefabcdefabcdefabcdefabcdefabcd",
                        "0x000000000000000000000000abcdefabcdefabcdefabcdefabcdefabcdefabcd",
                    ],
                    "data": "0x"
                    + "0000000000000000000000000000000000000000000000000de0b6b3a7640000"  # 1 USDe
                    + "0000000000000000000000000000000000000000000000000de0b6b3a7640000",
                }
            ],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.stakes) == 1
        assert result.stakes[0].assets == Decimal("1")

    def test_parse_stake_to_dict(self) -> None:
        """Test StakeEventData to_dict method."""
        data = StakeEventData(
            sender="0xsender",
            owner="0xowner",
            assets=Decimal("1000"),
            shares=Decimal("950"),
        )
        d = data.to_dict()

        assert d["sender"] == "0xsender"
        assert d["owner"] == "0xowner"
        assert d["assets"] == "1000"
        assert d["shares"] == "950"


# =============================================================================
# Receipt Parsing with Multiple Events
# =============================================================================


class TestParseReceiptWithMultipleEvents:
    """Tests for parsing receipts with multiple events."""

    def test_parse_receipt_with_stake_and_withdraw(self, parser: EthenaReceiptParser) -> None:
        """Test parsing a receipt that contains both stake and withdraw events."""
        susde_address = "0x9D39A5DE30e57443BfF2A8307A4256c8797A3497"

        receipt = {
            "transactionHash": "0xmulti123",
            "blockNumber": 12345680,
            "logs": [
                # Stake event
                {
                    "address": susde_address,
                    "topics": [
                        EVENT_TOPICS["Deposit"],
                        "0x000000000000000000000000abcdefabcdefabcdefabcdefabcdefabcdefabcd",
                        "0x000000000000000000000000abcdefabcdefabcdefabcdefabcdefabcdefabcd",
                    ],
                    "data": "0x"
                    + "0000000000000000000000000000000000000000000000000de0b6b3a7640000"
                    + "0000000000000000000000000000000000000000000000000de0b6b3a7640000",
                },
                # Withdraw event
                {
                    "address": susde_address,
                    "topics": [
                        EVENT_TOPICS["Withdraw"],
                        "0x000000000000000000000000abcdefabcdefabcdefabcdefabcdefabcdefabcd",
                        "0x0000000000000000000000001234567890123456789012345678901234567890",
                        "0x000000000000000000000000abcdefabcdefabcdefabcdefabcdefabcdefabcd",
                    ],
                    "data": "0x"
                    + "0000000000000000000000000000000000000000000000001bc16d674ec80000"
                    + "0000000000000000000000000000000000000000000000001bc16d674ec80000",
                },
            ],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.stakes) == 1
        assert len(result.withdraws) == 1
        assert result.stakes[0].assets == Decimal("1")
        assert result.withdraws[0].assets == Decimal("2")

    def test_parse_receipt_ignores_non_susde_events(self, parser: EthenaReceiptParser) -> None:
        """Test that receipt parsing ignores events from non-sUSDe contracts."""
        random_contract = "0x1111111111111111111111111111111111111111"
        susde_address = "0x9D39A5DE30e57443BfF2A8307A4256c8797A3497"

        receipt = {
            "transactionHash": "0xignore123",
            "blockNumber": 12345681,
            "logs": [
                # Withdraw from wrong contract - should be ignored
                {
                    "address": random_contract,
                    "topics": [
                        EVENT_TOPICS["Withdraw"],
                        "0x000000000000000000000000abcdefabcdefabcdefabcdefabcdefabcdefabcd",
                        "0x0000000000000000000000001234567890123456789012345678901234567890",
                        "0x000000000000000000000000abcdefabcdefabcdefabcdefabcdefabcdefabcd",
                    ],
                    "data": "0x"
                    + "0000000000000000000000000000000000000000000000000de0b6b3a7640000"
                    + "0000000000000000000000000000000000000000000000000de0b6b3a7640000",
                },
                # Withdraw from correct contract - should be parsed
                {
                    "address": susde_address,
                    "topics": [
                        EVENT_TOPICS["Withdraw"],
                        "0x000000000000000000000000abcdefabcdefabcdefabcdefabcdefabcdefabcd",
                        "0x0000000000000000000000001234567890123456789012345678901234567890",
                        "0x000000000000000000000000abcdefabcdefabcdefabcdefabcdefabcdefabcd",
                    ],
                    "data": "0x"
                    + "0000000000000000000000000000000000000000000000001bc16d674ec80000"
                    + "0000000000000000000000000000000000000000000000001bc16d674ec80000",
                },
            ],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.withdraws) == 1
        assert result.withdraws[0].assets == Decimal("2")

    def test_parse_receipt_empty_logs(self, parser: EthenaReceiptParser) -> None:
        """Test parsing receipt with no logs."""
        receipt = {
            "transactionHash": "0xempty123",
            "blockNumber": 12345682,
            "logs": [],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.stakes) == 0
        assert len(result.withdraws) == 0


# =============================================================================
# ParseResult Tests
# =============================================================================


class TestParseResult:
    """Tests for ParseResult data class."""

    def test_parse_result_to_dict(self) -> None:
        """Test ParseResult to_dict method."""
        result = ParseResult(
            success=True,
            stakes=[
                StakeEventData(
                    sender="0xsender",
                    owner="0xowner",
                    assets=Decimal("1000"),
                    shares=Decimal("950"),
                )
            ],
            withdraws=[
                WithdrawEventData(
                    sender="0xsender",
                    receiver="0xreceiver",
                    owner="0xowner",
                    assets=Decimal("500"),
                    shares=Decimal("475"),
                )
            ],
            transaction_hash="0xtest",
            block_number=12345,
        )

        d = result.to_dict()

        assert d["success"] is True
        assert len(d["stakes"]) == 1
        assert len(d["withdraws"]) == 1
        assert d["transaction_hash"] == "0xtest"
        assert d["block_number"] == 12345

    def test_parse_result_unstakes_alias(self) -> None:
        """Test that unstakes is an alias for withdraws."""
        result = ParseResult(
            success=True,
            withdraws=[
                WithdrawEventData(
                    sender="0xsender",
                    receiver="0xreceiver",
                    owner="0xowner",
                    assets=Decimal("100"),
                    shares=Decimal("95"),
                )
            ],
        )

        assert result.unstakes is result.withdraws
        assert len(result.unstakes) == 1


# =============================================================================
# Edge Cases
# =============================================================================


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    def test_parse_receipt_with_bytes_tx_hash(self, parser: EthenaReceiptParser) -> None:
        """Test parsing receipt when transactionHash is bytes."""
        receipt = {
            "transactionHash": bytes.fromhex("abc123def456abc123def456abc123def456abc123def456abc123def456abc1"),
            "blockNumber": 12345,
            "logs": [],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert result.transaction_hash.startswith("0x")

    def test_parse_receipt_with_bytes_address(self, parser: EthenaReceiptParser) -> None:
        """Test parsing receipt when contract address is bytes."""
        susde_address_bytes = bytes.fromhex("9D39A5DE30e57443BfF2A8307A4256c8797A3497")
        receipt = {
            "transactionHash": "0xtest",
            "blockNumber": 12345,
            "logs": [
                {
                    "address": susde_address_bytes,
                    "topics": [
                        EVENT_TOPICS["Withdraw"],
                        "0x000000000000000000000000abcdefabcdefabcdefabcdefabcdefabcdefabcd",
                        "0x0000000000000000000000001234567890123456789012345678901234567890",
                        "0x000000000000000000000000abcdefabcdefabcdefabcdefabcdefabcdefabcd",
                    ],
                    "data": "0x"
                    + "0000000000000000000000000000000000000000000000000de0b6b3a7640000"
                    + "0000000000000000000000000000000000000000000000000de0b6b3a7640000",
                }
            ],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.withdraws) == 1

    def test_parse_receipt_with_log_without_topics(self, parser: EthenaReceiptParser) -> None:
        """Test parsing receipt with log entry that has no topics."""
        receipt = {
            "transactionHash": "0xtest",
            "blockNumber": 12345,
            "logs": [
                {
                    "address": "0x9D39A5DE30e57443BfF2A8307A4256c8797A3497",
                    "topics": [],
                    "data": "0x",
                }
            ],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.stakes) == 0
        assert len(result.withdraws) == 0

    def test_is_ethena_event_true(self, parser: EthenaReceiptParser) -> None:
        """Test is_ethena_event returns True for known topics."""
        assert parser.is_ethena_event(EVENT_TOPICS["Deposit"]) is True
        assert parser.is_ethena_event(EVENT_TOPICS["Withdraw"]) is True

    def test_is_ethena_event_false(self, parser: EthenaReceiptParser) -> None:
        """Test is_ethena_event returns False for unknown topics."""
        unknown = "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
        assert parser.is_ethena_event(unknown) is False

    def test_parse_withdraw_with_zero_values(self, parser: EthenaReceiptParser) -> None:
        """Test parsing Withdraw event with zero assets and shares."""
        log = {
            "topics": [
                EVENT_TOPICS["Withdraw"],
                "0x000000000000000000000000abcdefabcdefabcdefabcdefabcdefabcdefabcd",
                "0x0000000000000000000000001234567890123456789012345678901234567890",
                "0x000000000000000000000000abcdefabcdefabcdefabcdefabcdefabcdefabcd",
            ],
            "data": "0x"
            + "0000000000000000000000000000000000000000000000000000000000000000"  # 0 assets
            + "0000000000000000000000000000000000000000000000000000000000000000",  # 0 shares
        }

        result = parser.parse_withdraw(log)

        assert result is not None
        assert result.assets == Decimal("0")
        assert result.shares == Decimal("0")

    def test_parse_withdraw_with_large_values(self, parser: EthenaReceiptParser) -> None:
        """Test parsing Withdraw event with very large values."""
        # 1 million USDe
        log = {
            "topics": [
                EVENT_TOPICS["Withdraw"],
                "0x000000000000000000000000abcdefabcdefabcdefabcdefabcdefabcdefabcd",
                "0x0000000000000000000000001234567890123456789012345678901234567890",
                "0x000000000000000000000000abcdefabcdefabcdefabcdefabcdefabcdefabcd",
            ],
            # 1,000,000e18 = 0xd3c21bcecceda1000000
            "data": "0x"
            + "00000000000000000000000000000000000000000000d3c21bcecceda1000000"
            + "00000000000000000000000000000000000000000000d3c21bcecceda1000000",
        }

        result = parser.parse_withdraw(log)

        assert result is not None
        assert result.assets == Decimal("1000000")
        assert result.shares == Decimal("1000000")
