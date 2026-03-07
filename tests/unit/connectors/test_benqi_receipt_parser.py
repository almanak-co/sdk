"""Unit tests for BENQI receipt parser."""

from decimal import Decimal

import pytest

from almanak.framework.connectors.benqi.receipt_parser import (
    EVENT_TOPICS,
    BenqiEventType,
    BenqiReceiptParser,
)


@pytest.fixture
def parser():
    """Create a BENQI receipt parser with USDC decimals."""
    return BenqiReceiptParser(underlying_decimals=6)


@pytest.fixture
def parser_18():
    """Create a BENQI receipt parser with 18 decimals (AVAX)."""
    return BenqiReceiptParser(underlying_decimals=18)


def _make_receipt(logs, tx_hash="0xabc123"):
    """Helper to create a receipt dict."""
    return {
        "transactionHash": tx_hash,
        "blockNumber": 12345678,
        "logs": logs,
    }


def _make_log(event_name, data, address="0xBEb5d47A3f720Ec0a390d04b4d41ED7d9688bC7F", topics=None):
    """Helper to create a log entry."""
    if topics is None:
        topics = [EVENT_TOPICS[event_name]]
    return {
        "address": address,
        "topics": topics,
        "data": data,
        "logIndex": 0,
    }


class TestBenqiReceiptParserMint:
    """Test parsing Mint events (supply)."""

    def test_parse_mint_event(self, parser):
        """Test parsing a Mint(address, uint256 mintAmount, uint256 mintTokens) event."""
        # Mint event: minter=0x..., mintAmount=1000000000 (1000 USDC), mintTokens=50000000000 (500 qiTokens)
        minter = "0000000000000000000000001234567890123456789012345678901234567890"
        mint_amount = f"{1000_000_000:064x}"  # 1000 USDC (6 decimals)
        mint_tokens = f"{50_000_000_000:064x}"  # 500 qiTokens (8 decimals)
        data = "0x" + minter + mint_amount + mint_tokens

        receipt = _make_receipt([_make_log("Mint", data)])
        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.events) == 1
        assert result.events[0].event_type == BenqiEventType.MINT
        assert result.supply_amount == Decimal("1000")
        assert result.qi_tokens_minted == Decimal("500")

    def test_parse_empty_receipt(self, parser):
        """Test parsing receipt with no logs."""
        receipt = _make_receipt([])
        result = parser.parse_receipt(receipt)
        assert result.success is True
        assert len(result.events) == 0


class TestBenqiReceiptParserRedeem:
    """Test parsing Redeem events (withdraw)."""

    def test_parse_redeem_event(self, parser):
        """Test parsing a Redeem(address, uint256 redeemAmount, uint256 redeemTokens) event."""
        redeemer = "0000000000000000000000001234567890123456789012345678901234567890"
        redeem_amount = f"{500_000_000:064x}"  # 500 USDC
        redeem_tokens = f"{25_000_000_000:064x}"  # 250 qiTokens
        data = "0x" + redeemer + redeem_amount + redeem_tokens

        receipt = _make_receipt([_make_log("Redeem", data)])
        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.events) == 1
        assert result.events[0].event_type == BenqiEventType.REDEEM
        assert result.withdraw_amount == Decimal("500")
        assert result.qi_tokens_redeemed == Decimal("250")


class TestBenqiReceiptParserBorrow:
    """Test parsing Borrow events."""

    def test_parse_borrow_event(self, parser):
        """Test parsing Borrow(address, uint256 borrowAmount, uint256 accountBorrows, uint256 totalBorrows)."""
        borrower = "0000000000000000000000001234567890123456789012345678901234567890"
        borrow_amount = f"{200_000_000:064x}"  # 200 USDC
        account_borrows = f"{200_000_000:064x}"
        total_borrows = f"{1_000_000_000:064x}"
        data = "0x" + borrower + borrow_amount + account_borrows + total_borrows

        receipt = _make_receipt([_make_log("Borrow", data)])
        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.events) == 1
        assert result.events[0].event_type == BenqiEventType.BORROW
        assert result.borrow_amount == Decimal("200")


class TestBenqiReceiptParserRepayBorrow:
    """Test parsing RepayBorrow events."""

    def test_parse_repay_event(self, parser):
        """Test parsing RepayBorrow(address payer, address borrower, uint256 repayAmount, ...)."""
        payer = "0000000000000000000000001234567890123456789012345678901234567890"
        borrower = "0000000000000000000000001234567890123456789012345678901234567890"
        repay_amount = f"{100_000_000:064x}"  # 100 USDC
        account_borrows = f"{100_000_000:064x}"
        total_borrows = f"{900_000_000:064x}"
        data = "0x" + payer + borrower + repay_amount + account_borrows + total_borrows

        receipt = _make_receipt([_make_log("RepayBorrow", data)])
        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.events) == 1
        assert result.events[0].event_type == BenqiEventType.REPAY_BORROW
        assert result.repay_amount == Decimal("100")


class TestBenqiReceiptParserTransfer:
    """Test parsing Transfer events (ERC20)."""

    def test_parse_transfer_event(self, parser):
        """Test parsing Transfer(address indexed from, address indexed to, uint256 value)."""
        from_addr = "0x" + "0" * 24 + "1234567890123456789012345678901234567890"
        to_addr = "0x" + "0" * 24 + "abcdefabcdefabcdefabcdefabcdefabcdefabcd"
        value_data = "0x" + f"{1000:064x}"

        topics = [
            EVENT_TOPICS["Transfer"],
            from_addr,
            to_addr,
        ]

        receipt = _make_receipt([_make_log("Transfer", value_data, topics=topics)])
        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.events) == 1
        assert result.events[0].event_type == BenqiEventType.TRANSFER


class TestBenqiReceiptParserFiltering:
    """Test qi_token_address filtering."""

    def test_filter_by_qi_token(self, parser):
        """Test that we can filter events to a specific qiToken."""
        minter = "0000000000000000000000001234567890123456789012345678901234567890"
        mint_amount = f"{1000_000_000:064x}"
        mint_tokens = f"{50_000_000_000:064x}"
        data = "0x" + minter + mint_amount + mint_tokens

        qi_usdc = "0xBEb5d47A3f720Ec0a390d04b4d41ED7d9688bC7F"
        qi_avax = "0x5C0401e81Bc07Ca70fAD469b451682c0d747Ef1c"

        logs = [
            _make_log("Mint", data, address=qi_usdc),
            _make_log("Mint", data, address=qi_avax),
        ]

        receipt = _make_receipt(logs)

        # Parse all events
        result_all = parser.parse_receipt(receipt)
        assert len(result_all.events) == 2

        # Parse filtered to qiUSDC
        result_filtered = parser.parse_receipt(receipt, qi_token_address=qi_usdc)
        assert len(result_filtered.events) == 1
        assert result_filtered.events[0].contract_address == qi_usdc


class TestBenqiReceiptParserSerialization:
    """Test event serialization."""

    def test_event_to_dict_roundtrip(self, parser):
        """Test BenqiEvent serialization and deserialization."""
        from almanak.framework.connectors.benqi.receipt_parser import BenqiEvent

        minter = "0000000000000000000000001234567890123456789012345678901234567890"
        data = "0x" + minter + f"{1000:064x}" + f"{500:064x}"

        receipt = _make_receipt([_make_log("Mint", data)])
        result = parser.parse_receipt(receipt)

        event = result.events[0]
        event_dict = event.to_dict()
        restored = BenqiEvent.from_dict(event_dict)

        assert restored.event_type == event.event_type
        assert restored.event_name == event.event_name
        assert restored.contract_address == event.contract_address

    def test_parse_result_to_dict(self, parser):
        """Test ParseResult serialization."""
        minter = "0000000000000000000000001234567890123456789012345678901234567890"
        data = "0x" + minter + f"{1000_000_000:064x}" + f"{50_000_000_000:064x}"

        receipt = _make_receipt([_make_log("Mint", data)])
        result = parser.parse_receipt(receipt)
        result_dict = result.to_dict()

        assert result_dict["success"] is True
        assert result_dict["supply_amount"] == "1000"
        assert len(result_dict["events"]) == 1


class TestBenqiReceiptParserMalformedData:
    """VIB-651: Malformed event data should not abort parsing of entire receipt."""

    def test_malformed_data_does_not_abort_parsing(self, parser):
        """One bad log entry should not prevent parsing of subsequent valid events."""
        minter = "0000000000000000000000001234567890123456789012345678901234567890"
        valid_data = "0x" + minter + f"{1000_000_000:064x}" + f"{50_000_000_000:064x}"
        # Data long enough to enter decoding but with invalid hex chars -> ValueError
        malformed_data = "0x" + "G" * 192

        logs = [
            _make_log("Mint", malformed_data),  # malformed: invalid hex triggers exception
            _make_log("Mint", valid_data),  # valid: should still be parsed
        ]
        receipt = _make_receipt(logs)
        result = parser.parse_receipt(receipt)

        # Both events should be present (malformed one with empty data)
        assert len(result.events) == 2
        assert result.events[0].data == {}
        # Second event should have valid parsed data
        assert result.events[1].data.get("mint_amount") is not None

    def test_short_data_field(self, parser):
        """Data too short for decoding should produce empty data dict."""
        logs = [_make_log("Mint", "0xBADDATA")]
        receipt = _make_receipt(logs)
        result = parser.parse_receipt(receipt)
        assert len(result.events) == 1
        assert result.events[0].data == {}

    def test_empty_data_field(self, parser):
        """Empty data field should not crash the parser."""
        logs = [_make_log("Mint", "0x")]
        receipt = _make_receipt(logs)
        result = parser.parse_receipt(receipt)
        assert len(result.events) == 1
        assert result.events[0].data == {}  # fallback empty dict
