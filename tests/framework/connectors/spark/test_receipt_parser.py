"""Tests for Spark receipt parser (refactored version)."""

import importlib.util
from decimal import Decimal

# Load the v2 module for testing
spec = importlib.util.spec_from_file_location("spark_parser_v2", "almanak/framework/connectors/spark/receipt_parser.py")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

SparkReceiptParser = module.SparkReceiptParser
SupplyEventData = module.SupplyEventData
WithdrawEventData = module.WithdrawEventData
BorrowEventData = module.BorrowEventData
RepayEventData = module.RepayEventData
ParseResult = module.ParseResult
SparkEventType = module.SparkEventType
EVENT_TOPICS = module.EVENT_TOPICS
SPARK_POOL_ADDRESSES = module.SPARK_POOL_ADDRESSES


class TestSparkReceiptParserBasic:
    """Basic tests for SparkReceiptParser."""

    def test_parse_receipt_with_supply(self):
        """Test parsing receipt with Supply event."""
        parser = SparkReceiptParser()
        pool_address = list(SPARK_POOL_ADDRESSES)[0]

        receipt = {
            "transactionHash": "0xsupply123",
            "blockNumber": 12345,
            "logs": [
                {
                    "address": pool_address,
                    "topics": [
                        EVENT_TOPICS["Supply"],
                        "0x000000000000000000000000" + "a" * 40,  # reserve
                        "0x000000000000000000000000" + "b" * 40,  # onBehalfOf
                    ],
                    "data": (
                        "0x"
                        + "000000000000000000000000"
                        + "c" * 40  # user
                        + "0000000000000000000000000000000000000000000000000de0b6b3a7640000"  # amount = 1e18
                        + "0000000000000000000000000000000000000000000000000000000000000001"  # referralCode = 1
                    ),
                }
            ],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.supplies) == 1
        assert result.supplies[0].reserve == "0x" + "a" * 40
        assert result.supplies[0].user == "0x" + "c" * 40
        assert result.supplies[0].on_behalf_of == "0x" + "b" * 40
        assert result.supplies[0].amount == Decimal("1000000000000000000")
        assert result.supplies[0].referral_code == 1

    def test_parse_receipt_with_withdraw(self):
        """Test parsing receipt with Withdraw event."""
        parser = SparkReceiptParser()
        pool_address = list(SPARK_POOL_ADDRESSES)[0]

        receipt = {
            "transactionHash": "0xwithdraw123",
            "blockNumber": 12345,
            "logs": [
                {
                    "address": pool_address,
                    "topics": [
                        EVENT_TOPICS["Withdraw"],
                        "0x000000000000000000000000" + "a" * 40,  # reserve
                        "0x000000000000000000000000" + "b" * 40,  # user
                        "0x000000000000000000000000" + "c" * 40,  # to
                    ],
                    "data": (
                        "0x" + "0000000000000000000000000000000000000000000000000de0b6b3a7640000"  # amount = 1e18
                    ),
                }
            ],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.withdraws) == 1
        assert result.withdraws[0].reserve == "0x" + "a" * 40
        assert result.withdraws[0].user == "0x" + "b" * 40
        assert result.withdraws[0].to == "0x" + "c" * 40
        assert result.withdraws[0].amount == Decimal("1000000000000000000")

    def test_parse_receipt_with_borrow(self):
        """Test parsing receipt with Borrow event."""
        parser = SparkReceiptParser()
        pool_address = list(SPARK_POOL_ADDRESSES)[0]

        # Borrow rate of 5% APR in ray format (5% * 1e27 / 100 = 0.05 * 1e27)
        borrow_rate_ray = 50000000000000000000000000  # 0.05 * 1e27

        receipt = {
            "transactionHash": "0xborrow123",
            "blockNumber": 12345,
            "logs": [
                {
                    "address": pool_address,
                    "topics": [
                        EVENT_TOPICS["Borrow"],
                        "0x000000000000000000000000" + "a" * 40,  # reserve
                        "0x000000000000000000000000" + "b" * 40,  # onBehalfOf
                    ],
                    "data": (
                        "0x"
                        + "000000000000000000000000"
                        + "c" * 40  # user
                        + "0000000000000000000000000000000000000000000000000de0b6b3a7640000"  # amount = 1e18
                        + "0000000000000000000000000000000000000000000000000000000000000002"  # interestRateMode = 2 (variable)
                        + format(borrow_rate_ray, "064x")  # borrowRate
                        + "0000000000000000000000000000000000000000000000000000000000000005"  # referralCode = 5
                    ),
                }
            ],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.borrows) == 1
        assert result.borrows[0].reserve == "0x" + "a" * 40
        assert result.borrows[0].user == "0x" + "c" * 40
        assert result.borrows[0].on_behalf_of == "0x" + "b" * 40
        assert result.borrows[0].amount == Decimal("1000000000000000000")
        assert result.borrows[0].interest_rate_mode == 2
        assert result.borrows[0].is_variable_rate is True
        assert result.borrows[0].borrow_rate == Decimal("0.05")
        assert result.borrows[0].referral_code == 5

    def test_parse_receipt_with_repay(self):
        """Test parsing receipt with Repay event."""
        parser = SparkReceiptParser()
        pool_address = list(SPARK_POOL_ADDRESSES)[0]

        receipt = {
            "transactionHash": "0xrepay123",
            "blockNumber": 12345,
            "logs": [
                {
                    "address": pool_address,
                    "topics": [
                        EVENT_TOPICS["Repay"],
                        "0x000000000000000000000000" + "a" * 40,  # reserve
                        "0x000000000000000000000000" + "b" * 40,  # user
                        "0x000000000000000000000000" + "c" * 40,  # repayer
                    ],
                    "data": (
                        "0x"
                        + "0000000000000000000000000000000000000000000000000de0b6b3a7640000"  # amount = 1e18
                        + "0000000000000000000000000000000000000000000000000000000000000001"  # useATokens = true
                    ),
                }
            ],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.repays) == 1
        assert result.repays[0].reserve == "0x" + "a" * 40
        assert result.repays[0].user == "0x" + "b" * 40
        assert result.repays[0].repayer == "0x" + "c" * 40
        assert result.repays[0].amount == Decimal("1000000000000000000")
        assert result.repays[0].use_atokens is True

    def test_parse_receipt_empty_logs(self):
        """Test parsing receipt with no logs."""
        parser = SparkReceiptParser()

        receipt = {
            "transactionHash": "0xempty",
            "blockNumber": 123,
            "logs": [],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.supplies) == 0
        assert len(result.withdraws) == 0
        assert len(result.borrows) == 0
        assert len(result.repays) == 0

    def test_parse_receipt_filters_wrong_pool(self):
        """Test that parser filters events from wrong pool address."""
        parser = SparkReceiptParser()

        receipt = {
            "transactionHash": "0xfilter",
            "blockNumber": 123,
            "logs": [
                {
                    "address": "0xwrong_pool",  # Not a Spark pool
                    "topics": [
                        EVENT_TOPICS["Supply"],
                        "0x" + "00" * 12 + "a" * 40,
                        "0x" + "00" * 12 + "b" * 40,
                    ],
                    "data": "0x" + "00" * 96,
                }
            ],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.supplies) == 0  # Filtered out

    def test_parse_receipt_filters_unknown_events(self):
        """Test that unknown events are filtered out."""
        parser = SparkReceiptParser()
        pool_address = list(SPARK_POOL_ADDRESSES)[0]

        receipt = {
            "transactionHash": "0xunknown",
            "blockNumber": 123,
            "logs": [
                {
                    "address": pool_address,
                    "topics": ["0xunknown_event"],
                    "data": "0x" + "00" * 64,
                }
            ],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.supplies) == 0

    def test_parse_receipt_custom_pool_addresses(self):
        """Test parser with custom pool addresses."""
        custom_pool = "0xcustom_pool_address"
        parser = SparkReceiptParser(pool_addresses={custom_pool})

        receipt = {
            "transactionHash": "0xcustom",
            "blockNumber": 123,
            "logs": [
                {
                    "address": custom_pool,
                    "topics": [
                        EVENT_TOPICS["Supply"],
                        "0x" + "00" * 12 + "a" * 40,
                        "0x" + "00" * 12 + "b" * 40,
                    ],
                    "data": (
                        "0x"
                        + "000000000000000000000000"
                        + "c" * 40
                        + "0000000000000000000000000000000000000000000000000000000000000064"  # 100
                        + "00" * 32
                    ),
                }
            ],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.supplies) == 1


class TestSparkSupplyEventData:
    """Tests for SupplyEventData."""

    def test_to_dict(self):
        """Test converting to dictionary."""
        supply = SupplyEventData(
            reserve="0xreserve",
            user="0xuser",
            on_behalf_of="0xbeneficiary",
            amount=Decimal("1000"),
            referral_code=5,
        )

        result = supply.to_dict()

        assert result["reserve"] == "0xreserve"
        assert result["user"] == "0xuser"
        assert result["on_behalf_of"] == "0xbeneficiary"
        assert result["amount"] == "1000"
        assert result["referral_code"] == 5


class TestSparkWithdrawEventData:
    """Tests for WithdrawEventData."""

    def test_to_dict(self):
        """Test converting to dictionary."""
        withdraw = WithdrawEventData(
            reserve="0xreserve",
            user="0xuser",
            to="0xrecipient",
            amount=Decimal("500"),
        )

        result = withdraw.to_dict()

        assert result["reserve"] == "0xreserve"
        assert result["user"] == "0xuser"
        assert result["to"] == "0xrecipient"
        assert result["amount"] == "500"


class TestSparkBorrowEventData:
    """Tests for BorrowEventData."""

    def test_is_variable_rate(self):
        """Test is_variable_rate property."""
        borrow_variable = BorrowEventData(
            reserve="0xreserve",
            user="0xuser",
            on_behalf_of="0xbeneficiary",
            amount=Decimal("1000"),
            interest_rate_mode=2,  # Variable
        )

        borrow_stable = BorrowEventData(
            reserve="0xreserve",
            user="0xuser",
            on_behalf_of="0xbeneficiary",
            amount=Decimal("1000"),
            interest_rate_mode=1,  # Stable
        )

        assert borrow_variable.is_variable_rate is True
        assert borrow_stable.is_variable_rate is False

    def test_to_dict(self):
        """Test converting to dictionary."""
        borrow = BorrowEventData(
            reserve="0xreserve",
            user="0xuser",
            on_behalf_of="0xbeneficiary",
            amount=Decimal("1000"),
            interest_rate_mode=2,
            borrow_rate=Decimal("0.05"),
            referral_code=10,
        )

        result = borrow.to_dict()

        assert result["reserve"] == "0xreserve"
        assert result["user"] == "0xuser"
        assert result["on_behalf_of"] == "0xbeneficiary"
        assert result["amount"] == "1000"
        assert result["interest_rate_mode"] == 2
        assert result["is_variable_rate"] is True
        assert result["borrow_rate"] == "0.05"
        assert result["referral_code"] == 10


class TestSparkRepayEventData:
    """Tests for RepayEventData."""

    def test_to_dict(self):
        """Test converting to dictionary."""
        repay = RepayEventData(
            reserve="0xreserve",
            user="0xuser",
            repayer="0xrepayer",
            amount=Decimal("250"),
            use_atokens=True,
        )

        result = repay.to_dict()

        assert result["reserve"] == "0xreserve"
        assert result["user"] == "0xuser"
        assert result["repayer"] == "0xrepayer"
        assert result["amount"] == "250"
        assert result["use_atokens"] is True


class TestSparkParseResult:
    """Tests for ParseResult."""

    def test_to_dict(self):
        """Test converting ParseResult to dictionary."""
        supply = SupplyEventData("0xa", "0xb", "0xc", Decimal("1"), 0)
        withdraw = WithdrawEventData("0xd", "0xe", "0xf", Decimal("2"))
        borrow = BorrowEventData("0xg", "0xh", "0xi", Decimal("3"), 2)
        repay = RepayEventData("0xj", "0xk", "0xl", Decimal("4"))

        result = ParseResult(
            success=True,
            supplies=[supply],
            withdraws=[withdraw],
            borrows=[borrow],
            repays=[repay],
            transaction_hash="0xhash",
            block_number=12345,
        )

        dict_result = result.to_dict()

        assert dict_result["success"] is True
        assert len(dict_result["supplies"]) == 1
        assert len(dict_result["withdraws"]) == 1
        assert len(dict_result["borrows"]) == 1
        assert len(dict_result["repays"]) == 1
        assert dict_result["transaction_hash"] == "0xhash"
        assert dict_result["block_number"] == 12345


class TestSparkBackwardCompatibility:
    """Tests for backward compatibility methods."""

    def test_parse_supply_method(self):
        """Test backward compatible parse_supply method."""
        parser = SparkReceiptParser()

        log = {
            "topics": [
                EVENT_TOPICS["Supply"],
                "0x000000000000000000000000" + "a" * 40,
                "0x000000000000000000000000" + "b" * 40,
            ],
            "data": (
                "0x"
                + "000000000000000000000000"
                + "c" * 40
                + "0000000000000000000000000000000000000000000000000de0b6b3a7640000"
                + "00" * 32
            ),
        }

        result = parser.parse_supply(log)

        assert result is not None
        assert result.amount == Decimal("1000000000000000000")

    def test_parse_borrow_method(self):
        """Test backward compatible parse_borrow method."""
        parser = SparkReceiptParser()

        log = {
            "topics": [
                EVENT_TOPICS["Borrow"],
                "0x000000000000000000000000" + "a" * 40,
                "0x000000000000000000000000" + "b" * 40,
            ],
            "data": (
                "0x"
                + "000000000000000000000000"
                + "c" * 40
                + "0000000000000000000000000000000000000000000000000de0b6b3a7640000"
                + "00" * 32 * 3
            ),
        }

        result = parser.parse_borrow(log)

        assert result is not None
        assert result.amount == Decimal("1000000000000000000")

    def test_is_spark_event(self):
        """Test is_spark_event method."""
        parser = SparkReceiptParser()

        assert parser.is_spark_event(EVENT_TOPICS["Supply"]) is True
        assert parser.is_spark_event(EVENT_TOPICS["Borrow"]) is True
        assert parser.is_spark_event("0xunknown") is False

    def test_get_event_type(self):
        """Test get_event_type method."""
        parser = SparkReceiptParser()

        assert parser.get_event_type(EVENT_TOPICS["Supply"]) == SparkEventType.SUPPLY
        assert parser.get_event_type(EVENT_TOPICS["Withdraw"]) == SparkEventType.WITHDRAW
        assert parser.get_event_type(EVENT_TOPICS["Borrow"]) == SparkEventType.BORROW
        assert parser.get_event_type(EVENT_TOPICS["Repay"]) == SparkEventType.REPAY
        assert parser.get_event_type("0xunknown") == SparkEventType.UNKNOWN

    def test_is_spark_pool(self):
        """Test is_spark_pool method."""
        parser = SparkReceiptParser()
        pool_address = list(SPARK_POOL_ADDRESSES)[0]

        assert parser.is_spark_pool(pool_address) is True
        assert parser.is_spark_pool(pool_address.upper()) is True  # Case insensitive
        assert parser.is_spark_pool("0xrandom_address") is False
