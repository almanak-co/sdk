"""Tests for Ethena receipt parser (refactored version)."""

from decimal import Decimal

from almanak.framework.connectors.ethena.receipt_parser import (
    EVENT_TOPICS,
    EthenaEventType,
    EthenaReceiptParser,
    ParseResult,
    StakeEventData,
    WithdrawEventData,
)


class TestEthenaReceiptParserBasic:
    """Basic tests for EthenaReceiptParser."""

    def test_parse_receipt_with_deposit(self):
        """Test parsing receipt with Deposit (stake) event."""
        parser = EthenaReceiptParser(chain="ethereum")

        receipt = {
            "transactionHash": "0xstake123",
            "blockNumber": 12345,
            "status": 1,
            "logs": [
                {
                    "address": parser.susde_address,
                    "topics": [
                        EVENT_TOPICS["Deposit"],
                        "0x000000000000000000000000" + "a" * 40,  # sender
                        "0x000000000000000000000000" + "b" * 40,  # owner
                    ],
                    "data": (
                        "0x"
                        + "0000000000000000000000000000000000000000000000000de0b6b3a7640000"  # assets = 1e18 (1 token) - 64 hex chars
                        + "0000000000000000000000000000000000000000000000000de0b6b3a7640000"  # shares = 1e18 (1 token) - 64 hex chars
                    ),
                }
            ],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.stakes) == 1
        assert result.stakes[0].assets == Decimal("1")
        assert result.stakes[0].shares == Decimal("1")
        assert result.transaction_hash == "0xstake123"

    def test_parse_receipt_with_withdraw(self):
        """Test parsing receipt with Withdraw event."""
        parser = EthenaReceiptParser(chain="ethereum")

        receipt = {
            "transactionHash": "0xwithdraw123",
            "blockNumber": 12345,
            "status": 1,
            "logs": [
                {
                    "address": parser.susde_address,
                    "topics": [
                        EVENT_TOPICS["Withdraw"],
                        "0x000000000000000000000000" + "a" * 40,  # sender
                        "0x000000000000000000000000" + "b" * 40,  # receiver
                        "0x000000000000000000000000" + "c" * 40,  # owner
                    ],
                    "data": (
                        "0x"
                        + "0000000000000000000000000000000000000000000000000de0b6b3a7640000"  # assets = 1e18 - 64 hex chars
                        + "0000000000000000000000000000000000000000000000000de0b6b3a7640000"  # shares = 1e18 - 64 hex chars
                    ),
                }
            ],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.withdraws) == 1
        assert result.withdraws[0].assets == Decimal("1")
        assert result.withdraws[0].shares == Decimal("1")
        assert result.withdraws[0].receiver == "0x" + "b" * 40

    def test_parse_receipt_empty_logs(self):
        """Test parsing receipt with no logs."""
        parser = EthenaReceiptParser(chain="ethereum")

        receipt = {
            "transactionHash": "0xempty",
            "blockNumber": 123,
            "logs": [],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.stakes) == 0
        assert len(result.withdraws) == 0

    def test_parse_receipt_filters_wrong_contract(self):
        """Test that parser filters events from wrong contract."""
        parser = EthenaReceiptParser(chain="ethereum")

        receipt = {
            "transactionHash": "0xfilter",
            "blockNumber": 123,
            "logs": [
                {
                    "address": "0xwrong_contract",  # Not sUSDe
                    "topics": [
                        EVENT_TOPICS["Deposit"],
                        "0x" + "00" * 12 + "a" * 40,
                        "0x" + "00" * 12 + "b" * 40,
                    ],
                    "data": "0x" + "00" * 64,
                }
            ],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.stakes) == 0  # Filtered out

    def test_parse_receipt_filters_unknown_events(self):
        """Test that unknown events are filtered out."""
        parser = EthenaReceiptParser(chain="ethereum")

        receipt = {
            "transactionHash": "0xunknown",
            "blockNumber": 123,
            "logs": [
                {
                    "address": parser.susde_address,
                    "topics": ["0xunknown_event"],
                    "data": "0x" + "00" * 64,
                }
            ],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.stakes) == 0
        assert len(result.withdraws) == 0


class TestEthenaStakeEventData:
    """Tests for StakeEventData."""

    def test_to_dict(self):
        """Test converting to dictionary."""
        stake = StakeEventData(
            sender="0xsender",
            owner="0xowner",
            assets=Decimal("100.5"),
            shares=Decimal("95.3"),
        )

        result = stake.to_dict()

        assert result["sender"] == "0xsender"
        assert result["owner"] == "0xowner"
        assert result["assets"] == "100.5"
        assert result["shares"] == "95.3"


class TestEthenaWithdrawEventData:
    """Tests for WithdrawEventData."""

    def test_to_dict(self):
        """Test converting to dictionary."""
        withdraw = WithdrawEventData(
            sender="0xsender",
            receiver="0xreceiver",
            owner="0xowner",
            assets=Decimal("50.25"),
            shares=Decimal("48.1"),
        )

        result = withdraw.to_dict()

        assert result["sender"] == "0xsender"
        assert result["receiver"] == "0xreceiver"
        assert result["owner"] == "0xowner"
        assert result["assets"] == "50.25"
        assert result["shares"] == "48.1"


class TestEthenaParseResult:
    """Tests for ParseResult."""

    def test_unstakes_alias(self):
        """Test backward compatibility unstakes property."""
        withdraw = WithdrawEventData(
            sender="0xa",
            receiver="0xb",
            owner="0xc",
            assets=Decimal("10"),
            shares=Decimal("9"),
        )

        result = ParseResult(
            success=True,
            withdraws=[withdraw],
        )

        # unstakes should alias to withdraws
        assert result.unstakes == result.withdraws
        assert len(result.unstakes) == 1

    def test_to_dict(self):
        """Test converting ParseResult to dictionary."""
        stake = StakeEventData("0xa", "0xb", Decimal("1"), Decimal("1"))
        withdraw = WithdrawEventData("0xc", "0xd", "0xe", Decimal("2"), Decimal("2"))

        result = ParseResult(
            success=True,
            stakes=[stake],
            withdraws=[withdraw],
            transaction_hash="0xhash",
            block_number=12345,
        )

        dict_result = result.to_dict()

        assert dict_result["success"] is True
        assert len(dict_result["stakes"]) == 1
        assert len(dict_result["withdraws"]) == 1
        assert dict_result["transaction_hash"] == "0xhash"


class TestEthenaBackwardCompatibility:
    """Tests for backward compatibility methods."""

    def test_parse_stake_method(self):
        """Test backward compatible parse_stake method."""
        parser = EthenaReceiptParser(chain="ethereum")

        log = {
            "topics": [
                EVENT_TOPICS["Deposit"],
                "0x" + "00" * 12 + "a" * 40,
                "0x" + "00" * 12 + "b" * 40,
            ],
            "data": (
                "0x"
                + "0000000000000000000000000000000000000000000000000de0b6b3a7640000"  # 1e18 - 64 hex chars
                + "0000000000000000000000000000000000000000000000000de0b6b3a7640000"  # 1e18 - 64 hex chars
            ),
        }

        result = parser.parse_stake(log)

        assert result is not None
        assert result.assets == Decimal("1")

    def test_parse_withdraw_method(self):
        """Test backward compatible parse_withdraw method."""
        parser = EthenaReceiptParser(chain="ethereum")

        log = {
            "topics": [
                EVENT_TOPICS["Withdraw"],
                "0x" + "00" * 12 + "a" * 40,
                "0x" + "00" * 12 + "b" * 40,
                "0x" + "00" * 12 + "c" * 40,
            ],
            "data": (
                "0x"
                + "0000000000000000000000000000000000000000000000000de0b6b3a7640000"  # 1e18 - 64 hex chars
                + "0000000000000000000000000000000000000000000000000de0b6b3a7640000"  # 1e18 - 64 hex chars
            ),
        }

        result = parser.parse_withdraw(log)

        assert result is not None
        assert result.assets == Decimal("1")

    def test_parse_unstake_alias(self):
        """Test parse_unstake as alias for parse_withdraw."""
        parser = EthenaReceiptParser(chain="ethereum")

        log = {
            "topics": [
                EVENT_TOPICS["Withdraw"],
                "0x" + "00" * 12 + "a" * 40,
                "0x" + "00" * 12 + "b" * 40,
                "0x" + "00" * 12 + "c" * 40,
            ],
            "data": "0x" + "00" * 64,
        }

        result = parser.parse_unstake(log)
        assert result is not None

    def test_is_ethena_event(self):
        """Test is_ethena_event method."""
        parser = EthenaReceiptParser(chain="ethereum")

        assert parser.is_ethena_event(EVENT_TOPICS["Deposit"]) is True
        assert parser.is_ethena_event(EVENT_TOPICS["Withdraw"]) is True
        assert parser.is_ethena_event("0xunknown") is False

    def test_get_event_type(self):
        """Test get_event_type method."""
        parser = EthenaReceiptParser(chain="ethereum")

        deposit_type = parser.get_event_type(EVENT_TOPICS["Deposit"])
        assert deposit_type == EthenaEventType.STAKE

        withdraw_type = parser.get_event_type(EVENT_TOPICS["Withdraw"])
        assert withdraw_type == EthenaEventType.WITHDRAW

        unknown_type = parser.get_event_type("0xunknown")
        assert unknown_type == EthenaEventType.UNKNOWN


class TestEthenaDecimalConversion:
    """Tests for wei to decimal conversion."""

    def test_small_amounts(self):
        """Test parsing small token amounts."""
        parser = EthenaReceiptParser(chain="ethereum")

        log = {
            "topics": [
                EVENT_TOPICS["Deposit"],
                "0x" + "00" * 12 + "a" * 40,
                "0x" + "00" * 12 + "b" * 40,
            ],
            "data": (
                "0x"
                + "00" * 31
                + "01"  # 1 wei
                + "00" * 31
                + "01"  # 1 wei
            ),
        }

        result = parser.parse_stake(log)

        assert result is not None
        # 1 wei = 1e-18 tokens
        assert result.assets == Decimal("1") / Decimal("10") ** 18

    def test_large_amounts(self):
        """Test parsing large token amounts."""
        parser = EthenaReceiptParser(chain="ethereum")

        # 1000 tokens = 1000e18 wei
        large_amount_hex = format(1000 * 10**18, "064x")

        log = {
            "topics": [
                EVENT_TOPICS["Deposit"],
                "0x" + "00" * 12 + "a" * 40,
                "0x" + "00" * 12 + "b" * 40,
            ],
            "data": f"0x{large_amount_hex}{large_amount_hex}",
        }

        result = parser.parse_stake(log)

        assert result is not None
        assert result.assets == Decimal("1000")
