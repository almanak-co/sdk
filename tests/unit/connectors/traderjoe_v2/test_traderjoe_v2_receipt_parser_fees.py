"""Tests for TraderJoeV2ReceiptParser fee collection extraction methods."""

from almanak.framework.connectors.traderjoe_v2.receipt_parser import (
    EVENT_TOPICS,
    TraderJoeV2ReceiptParser,
)


def _make_log(topic0: str, contract: str, topics: list[str] | None = None, data: str = "0x") -> dict:
    """Helper to construct a log entry."""
    all_topics = [topic0]
    if topics:
        all_topics.extend(topics)
    return {
        "topics": all_topics,
        "address": contract,
        "data": data,
        "logIndex": 0,
        "transactionHash": "0x" + "ab" * 32,
        "blockNumber": 12345,
    }


POOL_ADDRESS = "0x1234567890abcdef1234567890abcdef12345678"
WALLET_ADDRESS = "0x" + "11" * 20
TOKEN_X = "0x" + "aa" * 20
TOKEN_Y = "0x" + "bb" * 20


class TestExtractCollectedFees:
    """Tests for extract_collected_fees method."""

    def setup_method(self):
        self.parser = TraderJoeV2ReceiptParser()

    def test_returns_none_for_empty_receipt(self):
        receipt = {"logs": [], "gasUsed": 0, "blockNumber": 0}
        result = self.parser.extract_collected_fees(receipt)
        assert result is None

    def test_returns_none_for_no_logs(self):
        receipt = {"logs": [], "gasUsed": 100, "blockNumber": 100}
        result = self.parser.extract_collected_fees(receipt)
        assert result is None

    def test_returns_none_for_unrelated_logs(self):
        """Receipt with no ClaimedFees event should return None."""
        receipt = {
            "logs": [
                _make_log(
                    EVENT_TOPICS["Transfer"],
                    TOKEN_X,
                    topics=["0x" + "00" * 12 + "11" * 20, "0x" + "00" * 12 + "22" * 20],
                    data="0x" + "00" * 31 + "0a",  # value = 10
                ),
            ],
            "gasUsed": 50000,
            "blockNumber": 12345,
        }
        result = self.parser.extract_collected_fees(receipt)
        assert result is None

    def test_parses_claimed_fees_event(self):
        """Receipt with ClaimedFees event should be parsed."""
        # ClaimedFees(address indexed sender, address indexed to, uint256[] ids, bytes32[] amounts)
        sender_topic = "0x" + "00" * 12 + WALLET_ADDRESS[2:]
        to_topic = "0x" + "00" * 12 + WALLET_ADDRESS[2:]

        receipt = {
            "logs": [
                # ClaimedFees event
                _make_log(
                    EVENT_TOPICS["ClaimedFees"],
                    POOL_ADDRESS,
                    topics=[sender_topic, to_topic],
                    data="0x" + "00" * 64,  # minimal data
                ),
            ],
            "gasUsed": 150000,
            "blockNumber": 99999,
        }
        result = self.parser.extract_collected_fees(receipt)
        assert result is not None
        assert result.success is True
        assert result.pool_address == POOL_ADDRESS
        assert result.gas_used == 150000
        assert result.block_number == 99999

    def test_parses_fees_from_transfers(self):
        """ClaimedFees + Transfer events should extract fee amounts."""
        sender_topic = "0x" + "00" * 12 + WALLET_ADDRESS[2:]
        to_topic = "0x" + "00" * 12 + WALLET_ADDRESS[2:]

        # Transfer value = 1000 (0x3e8)
        transfer_data_x = "0x" + "00" * 30 + "03e8"
        # Transfer value = 2000 (0x7d0)
        transfer_data_y = "0x" + "00" * 30 + "07d0"

        receipt = {
            "logs": [
                # Transfer for token X fees
                _make_log(
                    EVENT_TOPICS["Transfer"],
                    TOKEN_X,
                    topics=["0x" + "00" * 12 + POOL_ADDRESS[2:], "0x" + "00" * 12 + WALLET_ADDRESS[2:]],
                    data=transfer_data_x,
                ),
                # Transfer for token Y fees
                _make_log(
                    EVENT_TOPICS["Transfer"],
                    TOKEN_Y,
                    topics=["0x" + "00" * 12 + POOL_ADDRESS[2:], "0x" + "00" * 12 + WALLET_ADDRESS[2:]],
                    data=transfer_data_y,
                ),
                # ClaimedFees event
                _make_log(
                    EVENT_TOPICS["ClaimedFees"],
                    POOL_ADDRESS,
                    topics=[sender_topic, to_topic],
                    data="0x" + "00" * 64,
                ),
            ],
            "gasUsed": 200000,
            "blockNumber": 100000,
        }
        result = self.parser.extract_collected_fees(receipt)
        assert result is not None
        assert result.success is True
        assert result.fees_x == 1000
        assert result.fees_y == 2000

    def test_handles_malformed_receipt_gracefully(self):
        """Malformed receipts should not raise, returns None."""
        receipt = {"not_logs": []}
        result = self.parser.extract_collected_fees(receipt)
        assert result is None


class TestExtractFees0:
    """Tests for extract_fees0 method."""

    def setup_method(self):
        self.parser = TraderJoeV2ReceiptParser()

    def test_returns_none_for_empty_receipt(self):
        receipt = {"logs": [], "gasUsed": 0, "blockNumber": 0}
        assert self.parser.extract_fees0(receipt) is None

    def test_returns_fees_x(self):
        """Should return fees_x from the ParsedFeeCollectionResult."""
        sender_topic = "0x" + "00" * 12 + WALLET_ADDRESS[2:]
        to_topic = "0x" + "00" * 12 + WALLET_ADDRESS[2:]

        transfer_data = "0x" + "00" * 30 + "03e8"  # 1000

        receipt = {
            "logs": [
                _make_log(
                    EVENT_TOPICS["Transfer"],
                    TOKEN_X,
                    topics=["0x" + "00" * 12 + POOL_ADDRESS[2:], "0x" + "00" * 12 + WALLET_ADDRESS[2:]],
                    data=transfer_data,
                ),
                _make_log(
                    EVENT_TOPICS["ClaimedFees"],
                    POOL_ADDRESS,
                    topics=[sender_topic, to_topic],
                    data="0x" + "00" * 64,
                ),
            ],
            "gasUsed": 100000,
            "blockNumber": 50000,
        }
        result = self.parser.extract_fees0(receipt)
        assert result == 1000


class TestExtractFees1:
    """Tests for extract_fees1 method."""

    def setup_method(self):
        self.parser = TraderJoeV2ReceiptParser()

    def test_returns_none_for_empty_receipt(self):
        receipt = {"logs": [], "gasUsed": 0, "blockNumber": 0}
        assert self.parser.extract_fees1(receipt) is None


class TestExtractBinIds:
    """Tests for extract_bin_ids method."""

    def setup_method(self):
        self.parser = TraderJoeV2ReceiptParser()

    def test_returns_none_for_empty_receipt(self):
        receipt = {"logs": [], "gasUsed": 0, "blockNumber": 0}
        result = self.parser.extract_bin_ids(receipt)
        assert result is None
