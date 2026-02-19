"""Tests for Lido receipt parser (refactored version)."""

import importlib.util
from decimal import Decimal

# Load the v2 module for testing
spec = importlib.util.spec_from_file_location("lido_parser_v2", "almanak/framework/connectors/lido/receipt_parser.py")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

LidoReceiptParser = module.LidoReceiptParser
StakeEventData = module.StakeEventData
WrapEventData = module.WrapEventData
UnwrapEventData = module.UnwrapEventData
WithdrawalRequestedEventData = module.WithdrawalRequestedEventData
WithdrawalClaimedEventData = module.WithdrawalClaimedEventData
ParseResult = module.ParseResult
LidoEventType = module.LidoEventType
EVENT_TOPICS = module.EVENT_TOPICS


class TestLidoReceiptParserBasic:
    """Basic tests for LidoReceiptParser."""

    def test_parse_receipt_with_stake(self):
        """Test parsing receipt with Submitted (stake) event."""
        parser = LidoReceiptParser(chain="ethereum")

        receipt = {
            "transactionHash": "0xstake123",
            "blockNumber": 12345,
            "logs": [
                {
                    "address": parser.steth_address,
                    "topics": [
                        EVENT_TOPICS["Submitted"],
                        "0x000000000000000000000000" + "a" * 40,  # sender
                    ],
                    "data": (
                        "0x"
                        + "0000000000000000000000000000000000000000000000000de0b6b3a7640000"  # amount = 1 ETH
                        + "000000000000000000000000"
                        + "b" * 40  # referral address
                    ),
                }
            ],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.stakes) == 1
        assert result.stakes[0].amount == Decimal("1")
        assert result.stakes[0].sender == "0x" + "a" * 40
        assert result.stakes[0].referral == "0x" + "b" * 40

    def test_parse_receipt_with_wrap(self):
        """Test parsing receipt with wrap event (Transfer from zero address)."""
        parser = LidoReceiptParser(chain="ethereum")

        receipt = {
            "transactionHash": "0xwrap123",
            "blockNumber": 12345,
            "logs": [
                {
                    "address": parser.wsteth_address,
                    "topics": [
                        EVENT_TOPICS["Transfer"],
                        "0x" + "0" * 64,  # from = zero address (mint)
                        "0x000000000000000000000000" + "a" * 40,  # to = user
                    ],
                    "data": (
                        "0x" + "0000000000000000000000000000000000000000000000000de0b6b3a7640000"  # amount = 1 token
                    ),
                }
            ],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.wraps) == 1
        assert result.wraps[0].amount == Decimal("1")
        assert result.wraps[0].from_address == "0x" + "0" * 40
        assert result.wraps[0].to_address == "0x" + "a" * 40
        assert result.wraps[0].token == parser.wsteth_address

    def test_parse_receipt_with_unwrap(self):
        """Test parsing receipt with unwrap event (Transfer to zero address)."""
        parser = LidoReceiptParser(chain="ethereum")

        receipt = {
            "transactionHash": "0xunwrap123",
            "blockNumber": 12345,
            "logs": [
                {
                    "address": parser.wsteth_address,
                    "topics": [
                        EVENT_TOPICS["Transfer"],
                        "0x000000000000000000000000" + "a" * 40,  # from = user
                        "0x" + "0" * 64,  # to = zero address (burn)
                    ],
                    "data": (
                        "0x" + "0000000000000000000000000000000000000000000000000de0b6b3a7640000"  # amount = 1 token
                    ),
                }
            ],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.unwraps) == 1
        assert result.unwraps[0].amount == Decimal("1")
        assert result.unwraps[0].from_address == "0x" + "a" * 40
        assert result.unwraps[0].to_address == "0x" + "0" * 40
        assert result.unwraps[0].token == parser.wsteth_address

    def test_parse_receipt_with_withdrawal_requested(self):
        """Test parsing receipt with WithdrawalRequested event."""
        parser = LidoReceiptParser(chain="ethereum")

        receipt = {
            "transactionHash": "0xwithdrawal_req123",
            "blockNumber": 12345,
            "logs": [
                {
                    "address": parser.withdrawal_queue_address,
                    "topics": [
                        EVENT_TOPICS["WithdrawalRequested"],
                        "0x" + "00" * 31 + "01",  # requestId = 1
                        "0x000000000000000000000000" + "a" * 40,  # requestor
                        "0x000000000000000000000000" + "b" * 40,  # owner
                    ],
                    "data": (
                        "0x"
                        + "0000000000000000000000000000000000000000000000000de0b6b3a7640000"  # amountOfStETH = 1 ETH
                        + "0000000000000000000000000000000000000000000000000de0b6b3a7640000"  # amountOfShares = 1 share
                    ),
                }
            ],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.withdrawal_requests) == 1
        assert result.withdrawal_requests[0].request_id == 1
        assert result.withdrawal_requests[0].requestor == "0x" + "a" * 40
        assert result.withdrawal_requests[0].owner == "0x" + "b" * 40
        assert result.withdrawal_requests[0].amount_of_steth == Decimal("1")
        assert result.withdrawal_requests[0].amount_of_shares == Decimal("1")

    def test_parse_receipt_with_withdrawal_claimed(self):
        """Test parsing receipt with WithdrawalClaimed event."""
        parser = LidoReceiptParser(chain="ethereum")

        receipt = {
            "transactionHash": "0xwithdrawal_claim123",
            "blockNumber": 12345,
            "logs": [
                {
                    "address": parser.withdrawal_queue_address,
                    "topics": [
                        EVENT_TOPICS["WithdrawalClaimed"],
                        "0x" + "00" * 31 + "01",  # requestId = 1
                        "0x000000000000000000000000" + "a" * 40,  # owner
                        "0x000000000000000000000000" + "b" * 40,  # receiver
                    ],
                    "data": (
                        "0x" + "0000000000000000000000000000000000000000000000000de0b6b3a7640000"  # amountOfETH = 1 ETH
                    ),
                }
            ],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.withdrawal_claims) == 1
        assert result.withdrawal_claims[0].request_id == 1
        assert result.withdrawal_claims[0].owner == "0x" + "a" * 40
        assert result.withdrawal_claims[0].receiver == "0x" + "b" * 40
        assert result.withdrawal_claims[0].amount_of_eth == Decimal("1")

    def test_parse_receipt_empty_logs(self):
        """Test parsing receipt with no logs."""
        parser = LidoReceiptParser(chain="ethereum")

        receipt = {
            "transactionHash": "0xempty",
            "blockNumber": 123,
            "logs": [],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.stakes) == 0
        assert len(result.wraps) == 0
        assert len(result.unwraps) == 0
        assert len(result.withdrawal_requests) == 0
        assert len(result.withdrawal_claims) == 0

    def test_parse_receipt_filters_wrong_contract(self):
        """Test that parser filters events from wrong contract."""
        parser = LidoReceiptParser(chain="ethereum")

        receipt = {
            "transactionHash": "0xfilter",
            "blockNumber": 123,
            "logs": [
                {
                    "address": "0xwrong_contract",  # Not stETH
                    "topics": [
                        EVENT_TOPICS["Submitted"],
                        "0x" + "00" * 12 + "a" * 40,
                    ],
                    "data": "0x" + "00" * 128,
                }
            ],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.stakes) == 0  # Filtered out

    def test_parse_receipt_filters_unknown_events(self):
        """Test that unknown events are filtered out."""
        parser = LidoReceiptParser(chain="ethereum")

        receipt = {
            "transactionHash": "0xunknown",
            "blockNumber": 123,
            "logs": [
                {
                    "address": parser.steth_address,
                    "topics": ["0xunknown_event"],
                    "data": "0x" + "00" * 64,
                }
            ],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.stakes) == 0
        assert len(result.wraps) == 0

    def test_parse_receipt_filters_transfer_from_other_contract(self):
        """Test that Transfer events from non-wstETH contracts are filtered."""
        parser = LidoReceiptParser(chain="ethereum")

        receipt = {
            "transactionHash": "0xfilter_transfer",
            "blockNumber": 123,
            "logs": [
                {
                    "address": "0xother_token",  # Not wstETH
                    "topics": [
                        EVENT_TOPICS["Transfer"],
                        "0x" + "0" * 64,  # from = zero
                        "0x" + "00" * 12 + "a" * 40,  # to = user
                    ],
                    "data": "0x" + "00" * 64,
                }
            ],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.wraps) == 0
        assert len(result.unwraps) == 0


class TestLidoStakeEventData:
    """Tests for StakeEventData."""

    def test_to_dict(self):
        """Test converting to dictionary."""
        stake = StakeEventData(
            sender="0xsender",
            amount=Decimal("1.5"),
            referral="0xreferral",
        )

        result = stake.to_dict()

        assert result["sender"] == "0xsender"
        assert result["amount"] == "1.5"
        assert result["referral"] == "0xreferral"


class TestLidoWrapEventData:
    """Tests for WrapEventData."""

    def test_to_dict(self):
        """Test converting to dictionary."""
        wrap = WrapEventData(
            from_address="0xzero",
            to_address="0xuser",
            amount=Decimal("2.5"),
            token="0xwsteth",
        )

        result = wrap.to_dict()

        assert result["from_address"] == "0xzero"
        assert result["to_address"] == "0xuser"
        assert result["amount"] == "2.5"
        assert result["token"] == "0xwsteth"


class TestLidoUnwrapEventData:
    """Tests for UnwrapEventData."""

    def test_to_dict(self):
        """Test converting to dictionary."""
        unwrap = UnwrapEventData(
            from_address="0xuser",
            to_address="0xzero",
            amount=Decimal("0.5"),
            token="0xwsteth",
        )

        result = unwrap.to_dict()

        assert result["from_address"] == "0xuser"
        assert result["to_address"] == "0xzero"
        assert result["amount"] == "0.5"
        assert result["token"] == "0xwsteth"


class TestLidoWithdrawalRequestedEventData:
    """Tests for WithdrawalRequestedEventData."""

    def test_to_dict(self):
        """Test converting to dictionary."""
        withdrawal = WithdrawalRequestedEventData(
            request_id=42,
            requestor="0xrequestor",
            owner="0xowner",
            amount_of_steth=Decimal("10.5"),
            amount_of_shares=Decimal("9.8"),
        )

        result = withdrawal.to_dict()

        assert result["request_id"] == 42
        assert result["requestor"] == "0xrequestor"
        assert result["owner"] == "0xowner"
        assert result["amount_of_steth"] == "10.5"
        assert result["amount_of_shares"] == "9.8"


class TestLidoWithdrawalClaimedEventData:
    """Tests for WithdrawalClaimedEventData."""

    def test_to_dict(self):
        """Test converting to dictionary."""
        claim = WithdrawalClaimedEventData(
            request_id=42,
            owner="0xowner",
            receiver="0xreceiver",
            amount_of_eth=Decimal("9.5"),
        )

        result = claim.to_dict()

        assert result["request_id"] == 42
        assert result["owner"] == "0xowner"
        assert result["receiver"] == "0xreceiver"
        assert result["amount_of_eth"] == "9.5"


class TestLidoParseResult:
    """Tests for ParseResult."""

    def test_to_dict(self):
        """Test converting ParseResult to dictionary."""
        stake = StakeEventData("0xa", Decimal("1"), "0xb")
        wrap = WrapEventData("0xc", "0xd", Decimal("2"), "0xe")
        unwrap = UnwrapEventData("0xf", "0xg", Decimal("3"), "0xh")
        wr = WithdrawalRequestedEventData(1, "0xi", "0xj", Decimal("4"), Decimal("5"))
        wc = WithdrawalClaimedEventData(1, "0xk", "0xl", Decimal("6"))

        result = ParseResult(
            success=True,
            stakes=[stake],
            wraps=[wrap],
            unwraps=[unwrap],
            withdrawal_requests=[wr],
            withdrawal_claims=[wc],
            transaction_hash="0xhash",
            block_number=12345,
        )

        dict_result = result.to_dict()

        assert dict_result["success"] is True
        assert len(dict_result["stakes"]) == 1
        assert len(dict_result["wraps"]) == 1
        assert len(dict_result["unwraps"]) == 1
        assert len(dict_result["withdrawal_requests"]) == 1
        assert len(dict_result["withdrawal_claims"]) == 1
        assert dict_result["transaction_hash"] == "0xhash"
        assert dict_result["block_number"] == 12345


class TestLidoBackwardCompatibility:
    """Tests for backward compatibility methods."""

    def test_parse_stake_method(self):
        """Test backward compatible parse_stake method."""
        parser = LidoReceiptParser(chain="ethereum")

        log = {
            "topics": [
                EVENT_TOPICS["Submitted"],
                "0x000000000000000000000000" + "a" * 40,
            ],
            "data": (
                "0x"
                + "0000000000000000000000000000000000000000000000000de0b6b3a7640000"  # 1 ETH
                + "000000000000000000000000"
                + "b" * 40  # referral
            ),
        }

        result = parser.parse_stake(log)

        assert result is not None
        assert result.amount == Decimal("1")
        assert result.sender == "0x" + "a" * 40

    def test_parse_wrap_method(self):
        """Test backward compatible parse_wrap method."""
        parser = LidoReceiptParser(chain="ethereum")

        log = {
            "address": parser.wsteth_address,
            "topics": [
                EVENT_TOPICS["Transfer"],
                "0x" + "0" * 64,  # from = zero
                "0x000000000000000000000000" + "a" * 40,  # to = user
            ],
            "data": "0x" + "0000000000000000000000000000000000000000000000000de0b6b3a7640000",
        }

        result = parser.parse_wrap(log)

        assert result is not None
        assert result.amount == Decimal("1")
        assert result.from_address == "0x" + "0" * 40

    def test_parse_unwrap_method(self):
        """Test backward compatible parse_unwrap method."""
        parser = LidoReceiptParser(chain="ethereum")

        log = {
            "address": parser.wsteth_address,
            "topics": [
                EVENT_TOPICS["Transfer"],
                "0x000000000000000000000000" + "a" * 40,  # from = user
                "0x" + "0" * 64,  # to = zero
            ],
            "data": "0x" + "0000000000000000000000000000000000000000000000000de0b6b3a7640000",
        }

        result = parser.parse_unwrap(log)

        assert result is not None
        assert result.amount == Decimal("1")
        assert result.to_address == "0x" + "0" * 40

    def test_is_lido_event(self):
        """Test is_lido_event method."""
        parser = LidoReceiptParser(chain="ethereum")

        assert parser.is_lido_event(EVENT_TOPICS["Submitted"]) is True
        assert parser.is_lido_event(EVENT_TOPICS["Transfer"]) is True
        assert parser.is_lido_event(EVENT_TOPICS["WithdrawalRequested"]) is True
        assert parser.is_lido_event(EVENT_TOPICS["WithdrawalClaimed"]) is True
        assert parser.is_lido_event("0xunknown") is False

    def test_get_event_type(self):
        """Test get_event_type method."""
        parser = LidoReceiptParser(chain="ethereum")

        assert parser.get_event_type(EVENT_TOPICS["Submitted"]) == LidoEventType.STAKE
        assert parser.get_event_type(EVENT_TOPICS["WithdrawalRequested"]) == LidoEventType.WITHDRAWAL_REQUESTED
        assert parser.get_event_type(EVENT_TOPICS["WithdrawalClaimed"]) == LidoEventType.WITHDRAWAL_CLAIMED

        # Transfer needs log for disambiguation
        wrap_log = {
            "topics": [
                EVENT_TOPICS["Transfer"],
                "0x" + "0" * 64,  # from = zero
                "0x" + "00" * 12 + "a" * 40,  # to = user
            ],
        }
        assert parser.get_event_type(EVENT_TOPICS["Transfer"], wrap_log) == LidoEventType.WRAP

        unwrap_log = {
            "topics": [
                EVENT_TOPICS["Transfer"],
                "0x" + "00" * 12 + "a" * 40,  # from = user
                "0x" + "0" * 64,  # to = zero
            ],
        }
        assert parser.get_event_type(EVENT_TOPICS["Transfer"], unwrap_log) == LidoEventType.UNWRAP

        assert parser.get_event_type("0xunknown") == LidoEventType.UNKNOWN


class TestLidoDecimalConversion:
    """Tests for wei to decimal conversion."""

    def test_small_amounts(self):
        """Test parsing small token amounts."""
        parser = LidoReceiptParser(chain="ethereum")

        log = {
            "topics": [
                EVENT_TOPICS["Submitted"],
                "0x" + "00" * 12 + "a" * 40,
            ],
            "data": (
                "0x"
                + "00" * 31
                + "01"  # 1 wei
                + "00" * 32  # no referral
            ),
        }

        result = parser.parse_stake(log)

        assert result is not None
        # 1 wei = 1e-18 ETH
        assert result.amount == Decimal("1") / Decimal("10") ** 18

    def test_large_amounts(self):
        """Test parsing large token amounts."""
        parser = LidoReceiptParser(chain="ethereum")

        # 1000 ETH = 1000e18 wei
        large_amount_hex = format(1000 * 10**18, "064x")

        log = {
            "topics": [
                EVENT_TOPICS["Submitted"],
                "0x" + "00" * 12 + "a" * 40,
            ],
            "data": f"0x{large_amount_hex}" + "00" * 64,
        }

        result = parser.parse_stake(log)

        assert result is not None
        assert result.amount == Decimal("1000")
