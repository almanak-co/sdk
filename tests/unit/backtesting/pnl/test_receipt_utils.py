"""Tests for receipt_utils module."""

import logging

import pytest

from almanak.framework.backtesting.pnl.receipt_utils import (
    DEFAULT_DISCREPANCY_THRESHOLD,
    TRANSFER_EVENT_TOPIC,
    DiscrepancyResult,
    calculate_discrepancy,
    extract_token_flows,
    parse_transfer_events,
)

WALLET = "0x" + "11" * 20
COUNTERPARTY = "0x" + "22" * 20
TOKEN = "0x" + "aa" * 20


def _address_topic(address: str) -> str:
    return "0x" + ("0" * 24) + address[2:].lower()


def _uint256_data(value: int) -> str:
    return "0x" + f"{value:064x}"


def _transfer_log(
    *,
    token: str = TOKEN,
    from_addr: str = COUNTERPARTY,
    to_addr: str = WALLET,
    value: int = 1000,
    data: str | None = None,
    topic: str = TRANSFER_EVENT_TOPIC,
    log_index: int | str = 0,
    extra_topics: tuple[str, ...] = (),
) -> dict:
    return {
        "address": token,
        "topics": [topic, _address_topic(from_addr), _address_topic(to_addr), *extra_topics],
        "data": _uint256_data(value) if data is None else data,
        "logIndex": log_index,
    }


class TestTransferEventParsing:
    """Tests for ERC-20 Transfer receipt parsing."""

    def test_parse_transfer_events_accepts_hex_success_status(self):
        """JSON-RPC hex status 0x1 should be treated as success."""
        receipt = {"status": "0x1", "logs": [_transfer_log(value=123)]}

        transfers = parse_transfer_events(receipt)

        assert len(transfers) == 1
        assert transfers[0].value == 123
        assert transfers[0].token_address == TOKEN
        assert transfers[0].from_addr == COUNTERPARTY
        assert transfers[0].to_addr == WALLET

    def test_extract_token_flows_accepts_hex_success_status(self):
        """Receipt-backed token flow extraction must not drop JSON-RPC status receipts."""
        receipt = {
            "status": "0x1",
            "logs": [
                _transfer_log(from_addr=WALLET, to_addr=COUNTERPARTY, value=250),
                _transfer_log(from_addr=COUNTERPARTY, to_addr=WALLET, value=100),
            ],
        }

        flows = extract_token_flows(receipt, wallet_address=WALLET.upper())

        assert flows.tokens_out == {TOKEN: 250}
        assert flows.tokens_in == {TOKEN: 100}
        assert flows.flows[TOKEN].net_amount == -150

    def test_empty_transfer_data_is_not_measured_zero(self):
        """Missing Transfer data is malformed/unmeasured, not a zero-value transfer."""
        receipt = {"status": 1, "logs": [_transfer_log(data="0x")]}

        assert parse_transfer_events(receipt) == []

    def test_zero_value_transfer_requires_explicit_zero_word(self):
        """A measured zero transfer has an explicit uint256 zero word in data."""
        receipt = {"status": 1, "logs": [_transfer_log(value=0)]}

        transfers = parse_transfer_events(receipt)

        assert len(transfers) == 1
        assert transfers[0].value == 0

    def test_erc721_transfer_topic_shape_is_skipped(self):
        """ERC-721 Transfer has the same signature but four topics and no ERC-20 value data."""
        token_id_topic = "0x" + f"{42:064x}"
        receipt = {"status": 1, "logs": [_transfer_log(data="0x", extra_topics=(token_id_topic,))]}

        assert parse_transfer_events(receipt) == []

    def test_topic_matching_is_case_insensitive(self):
        """Event topics are hex data and should not depend on letter casing."""
        receipt = {"status": 1, "logs": [_transfer_log(topic=TRANSFER_EVENT_TOPIC.upper())]}

        assert len(parse_transfer_events(receipt)) == 1

    def test_hex_log_index_is_normalized_to_int(self):
        """JSON-RPC logIndex hex strings should be normalized for ordering consumers."""
        receipt = {"status": 1, "logs": [_transfer_log(log_index="0x2")]}

        transfers = parse_transfer_events(receipt)

        assert transfers[0].log_index == 2


class TestCalculateDiscrepancy:
    """Tests for calculate_discrepancy function."""

    def test_no_discrepancy(self):
        """Test when expected equals actual."""
        result = calculate_discrepancy(expected=1000, actual=1000, log_warning=False)

        assert result.expected == 1000
        assert result.actual == 1000
        assert result.difference == 0
        assert result.percentage == 0.0
        assert result.exceeds_threshold is False
        assert result.threshold == DEFAULT_DISCREPANCY_THRESHOLD

    def test_discrepancy_below_threshold(self):
        """Test discrepancy below the 1% default threshold."""
        # 0.5% discrepancy (995 vs 1000)
        result = calculate_discrepancy(expected=1000, actual=995, log_warning=False)

        assert result.expected == 1000
        assert result.actual == 995
        assert result.difference == -5
        assert result.percentage == 0.005
        assert result.exceeds_threshold is False

    def test_discrepancy_above_threshold(self):
        """Test discrepancy above the 1% default threshold."""
        # 2% discrepancy (980 vs 1000)
        result = calculate_discrepancy(expected=1000, actual=980, log_warning=False)

        assert result.expected == 1000
        assert result.actual == 980
        assert result.difference == -20
        assert result.percentage == 0.02
        assert result.exceeds_threshold is True

    def test_discrepancy_exactly_at_threshold(self):
        """Test discrepancy exactly at threshold (should not exceed)."""
        # Exactly 1% discrepancy
        result = calculate_discrepancy(expected=1000, actual=990, log_warning=False)

        assert result.percentage == 0.01
        assert result.exceeds_threshold is False  # 0.01 is not > 0.01

    def test_positive_discrepancy(self):
        """Test when actual is greater than expected."""
        result = calculate_discrepancy(expected=1000, actual=1050, log_warning=False)

        assert result.difference == 50
        assert result.percentage == 0.05
        assert result.exceeds_threshold is True

    def test_custom_threshold(self):
        """Test with a custom threshold."""
        # 2% discrepancy with 5% threshold
        result = calculate_discrepancy(
            expected=1000, actual=980, threshold=0.05, log_warning=False
        )

        assert result.percentage == 0.02
        assert result.threshold == 0.05
        assert result.exceeds_threshold is False

    def test_zero_expected(self):
        """Test handling of zero expected value."""
        # When expected is 0 and actual is not, that's 100% discrepancy
        result = calculate_discrepancy(expected=0, actual=100, log_warning=False)

        assert result.percentage == 1.0
        assert result.exceeds_threshold is True

    def test_both_zero(self):
        """Test when both expected and actual are zero."""
        result = calculate_discrepancy(expected=0, actual=0, log_warning=False)

        assert result.percentage == 0.0
        assert result.exceeds_threshold is False

    def test_float_values(self):
        """Test with float values."""
        result = calculate_discrepancy(
            expected=100.5, actual=99.5, log_warning=False
        )

        assert result.expected == 100.5
        assert result.actual == 99.5
        assert result.difference == -1.0
        assert pytest.approx(result.percentage, abs=0.0001) == 0.00995

    def test_warning_logged_when_threshold_exceeded(self, caplog):
        """Test that a warning is logged when threshold is exceeded."""
        with caplog.at_level(logging.WARNING):
            calculate_discrepancy(
                expected=1000,
                actual=900,  # 10% discrepancy
                log_warning=True,
            )

        assert len(caplog.records) == 1
        assert "Execution discrepancy" in caplog.records[0].message
        assert "expected=1000" in caplog.records[0].message
        assert "actual=900" in caplog.records[0].message
        assert "10.00%" in caplog.records[0].message

    def test_no_warning_logged_below_threshold(self, caplog):
        """Test that no warning is logged when below threshold."""
        with caplog.at_level(logging.WARNING):
            calculate_discrepancy(
                expected=1000,
                actual=995,  # 0.5% discrepancy
                log_warning=True,
            )

        assert len(caplog.records) == 0

    def test_warning_with_context(self, caplog):
        """Test that context is included in warning message."""
        with caplog.at_level(logging.WARNING):
            calculate_discrepancy(
                expected=1000,
                actual=900,
                log_warning=True,
                context="USDC swap",
            )

        assert "[USDC swap]" in caplog.records[0].message

    def test_log_warning_disabled(self, caplog):
        """Test that log_warning=False suppresses warning."""
        with caplog.at_level(logging.WARNING):
            result = calculate_discrepancy(
                expected=1000,
                actual=900,  # 10% discrepancy
                log_warning=False,
            )

        assert result.exceeds_threshold is True
        assert len(caplog.records) == 0


class TestDiscrepancyResultToDict:
    """Tests for DiscrepancyResult.to_dict() method."""

    def test_to_dict(self):
        """Test serialization to dictionary."""
        result = DiscrepancyResult(
            expected=1000,
            actual=980,
            difference=-20,
            percentage=0.02,
            exceeds_threshold=True,
            threshold=0.01,
        )

        data = result.to_dict()

        assert data["expected"] == "1000"
        assert data["actual"] == "980"
        assert data["difference"] == "-20"
        assert data["percentage"] == 0.02
        assert data["exceeds_threshold"] is True
        assert data["threshold"] == 0.01

    def test_to_dict_with_floats(self):
        """Test serialization with float values."""
        result = DiscrepancyResult(
            expected=100.5,
            actual=99.5,
            difference=-1.0,
            percentage=0.00995,
            exceeds_threshold=False,
            threshold=0.01,
        )

        data = result.to_dict()

        assert data["expected"] == "100.5"
        assert data["actual"] == "99.5"
        assert data["difference"] == "-1.0"
