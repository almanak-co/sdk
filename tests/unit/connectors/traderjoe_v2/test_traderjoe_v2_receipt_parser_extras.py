"""Tests for TraderJoeV2ReceiptParser uncovered extraction paths.

Targets uncovered branches in `receipt_parser.py`:
- _parse_event_data: TRANSFER / APPROVAL / DEPOSITED_TO_BINS / WITHDRAWN_FROM_BINS / DEPOSIT / WITHDRAWAL paths
- _extract_swap_result: zero-amount short-circuit + happy path
- _extract_liquidity_result: deposit + withdraw branches with bin IDs
- _parse_bin_ids_from_data: malformed / sanity-bound / valid layouts
- extract_protocol_fees: unavailable_reason path
- extract_lp_close_data: amount extraction from Transfer events
- extract_bin_ids: graceful return paths
- extract_swap_amounts with realized slippage
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.connectors.traderjoe_v2.receipt_parser import (
    EVENT_TOPICS,
    ParsedLiquidityResult,
    ParseResult,
    TraderJoeV2EventType,
    TraderJoeV2ReceiptParser,
)

WALLET = "0x" + "11" * 20
WALLET_WITH_PREFIX = WALLET
POOL = "0x" + "22" * 20
TOKEN_X = "0x" + "aa" * 20
TOKEN_Y = "0x" + "bb" * 20


def _topic_addr(addr: str) -> str:
    """Pad an address to a 32-byte topic."""
    return "0x" + "00" * 12 + addr[2:].lower()


def _uint256_hex(value: int) -> str:
    """Encode value as a 32-byte hex (no 0x)."""
    return f"{value:064x}"


def _bins_data(bin_ids: list[int]) -> str:
    """Build event data with the layout expected by _parse_bin_ids_from_data:
    [offset_to_ids][offset_to_amounts][ids_array_len][ids...][amounts_array_len][amounts...].
    Offset is in bytes; first slot points to the second slot end (0x40).
    """
    # offset to ids = 0x40 (64 bytes = 2 slots)
    ids_offset_hex = _uint256_hex(0x40)
    # offset to amounts: 0x40 + 32 (length) + len*32
    amounts_offset = 0x40 + 32 + len(bin_ids) * 32
    amounts_offset_hex = _uint256_hex(amounts_offset)
    ids_len_hex = _uint256_hex(len(bin_ids))
    ids_elements = "".join(_uint256_hex(b) for b in bin_ids)
    amounts_len_hex = _uint256_hex(0)  # zero amounts to keep simple
    return "0x" + ids_offset_hex + amounts_offset_hex + ids_len_hex + ids_elements + amounts_len_hex


def _make_log(topic0: str, contract: str, topics: list[str] | None = None, data: str = "0x") -> dict:
    all_topics = [topic0]
    if topics:
        all_topics.extend(topics)
    return {
        "topics": all_topics,
        "address": contract,
        "data": data,
        "logIndex": 0,
    }


# =============================================================================
# _parse_event_data branches
# =============================================================================


class TestParseEventDataBranches:
    @pytest.fixture
    def parser(self) -> TraderJoeV2ReceiptParser:
        return TraderJoeV2ReceiptParser()

    def test_approval_event_parsed(self, parser: TraderJoeV2ReceiptParser) -> None:
        receipt = {
            "status": 1,
            "transactionHash": "0x" + "ab" * 32,
            "blockNumber": 1,
            "gasUsed": 100,
            "logs": [
                _make_log(
                    EVENT_TOPICS["Approval"],
                    TOKEN_X,
                    topics=[_topic_addr(WALLET), _topic_addr(POOL)],
                    data="0x" + _uint256_hex(123),
                ),
            ],
        }
        result = parser.parse_receipt(receipt)
        approval_events = [e for e in result.events if e.event_type == TraderJoeV2EventType.APPROVAL]
        assert len(approval_events) == 1
        assert approval_events[0].data["value"] == 123
        assert approval_events[0].data["spender"].lower() == POOL.lower()

    def test_deposit_event_parsed(self, parser: TraderJoeV2ReceiptParser) -> None:
        """WAVAX wrap event: Deposit(address indexed dst, uint256 wad)."""
        receipt = {
            "status": 1,
            "transactionHash": "0x" + "cd" * 32,
            "blockNumber": 2,
            "gasUsed": 1000,
            "logs": [
                _make_log(
                    EVENT_TOPICS["Deposit"],
                    TOKEN_X,
                    topics=[_topic_addr(WALLET)],
                    data="0x" + _uint256_hex(10**18),
                ),
            ],
        }
        result = parser.parse_receipt(receipt)
        deposits = [e for e in result.events if e.event_type == TraderJoeV2EventType.DEPOSIT]
        assert len(deposits) == 1
        assert deposits[0].data["wad"] == 10**18

    def test_withdrawal_event_parsed(self, parser: TraderJoeV2ReceiptParser) -> None:
        """WAVAX unwrap event."""
        receipt = {
            "status": 1,
            "transactionHash": "0x" + "ef" * 32,
            "blockNumber": 3,
            "gasUsed": 2000,
            "logs": [
                _make_log(
                    EVENT_TOPICS["Withdrawal"],
                    TOKEN_X,
                    topics=[_topic_addr(WALLET)],
                    data="0x" + _uint256_hex(5 * 10**17),
                ),
            ],
        }
        result = parser.parse_receipt(receipt)
        withdrawals = [e for e in result.events if e.event_type == TraderJoeV2EventType.WITHDRAWAL]
        assert len(withdrawals) == 1
        assert withdrawals[0].data["wad"] == 5 * 10**17

    def test_unknown_topic_skipped(self, parser: TraderJoeV2ReceiptParser) -> None:
        """Logs with unknown topics are silently skipped."""
        receipt = {
            "status": 1,
            "transactionHash": "0x" + "01" * 32,
            "blockNumber": 4,
            "gasUsed": 100,
            "logs": [
                {"topics": ["0x" + "ff" * 32], "address": TOKEN_X, "data": "0x"},
            ],
        }
        result = parser.parse_receipt(receipt)
        assert result.events == []

    def test_log_with_no_topics_skipped(self, parser: TraderJoeV2ReceiptParser) -> None:
        receipt = {
            "status": 1,
            "transactionHash": "0x" + "02" * 32,
            "blockNumber": 5,
            "gasUsed": 100,
            "logs": [
                {"topics": [], "address": TOKEN_X, "data": "0x"},
            ],
        }
        result = parser.parse_receipt(receipt)
        assert result.events == []

    def test_topic_without_0x_prefix_normalized(self, parser: TraderJoeV2ReceiptParser) -> None:
        """Hex topics passed without the 0x prefix should be normalized."""
        topic_no_prefix = EVENT_TOPICS["Transfer"][2:]  # strip 0x
        receipt = {
            "status": 1,
            "transactionHash": "0x" + "ee" * 32,
            "blockNumber": 99,
            "gasUsed": 100,
            "logs": [
                {
                    "topics": [topic_no_prefix, _topic_addr(WALLET), _topic_addr(POOL)],
                    "address": TOKEN_X,
                    "data": "0x" + _uint256_hex(7),
                }
            ],
        }
        result = parser.parse_receipt(receipt)
        # Transfer event should still be recognized after prefix normalization.
        transfers = [e for e in result.events if e.event_type == TraderJoeV2EventType.TRANSFER]
        assert len(transfers) == 1
        assert transfers[0].data["value"] == 7

    def test_tx_hash_non_bytes_non_string_falls_back(self, parser: TraderJoeV2ReceiptParser) -> None:
        """transactionHash that is neither bytes nor str → tx_hash="""
        receipt = {
            "status": 1,
            "transactionHash": 12345,  # int, neither bytes nor str
            "blockNumber": 1,
            "gasUsed": 100,
            "logs": [],
        }
        result = parser.parse_receipt(receipt)
        # tx_hash falls back to "" but parse still succeeds.
        assert result.success is True
        assert result.transaction_hash == ""

    def test_topic_as_bytes_normalized(self, parser: TraderJoeV2ReceiptParser) -> None:
        """First topic supplied as bytes should be normalized to '0x...' hex."""
        topic_hex = EVENT_TOPICS["Transfer"]
        topic_bytes = bytes.fromhex(topic_hex[2:])
        from_topic_bytes = bytes.fromhex(_topic_addr(WALLET)[2:])
        to_topic_bytes = bytes.fromhex(_topic_addr(POOL)[2:])
        receipt = {
            "status": 1,
            "transactionHash": "0x" + "03" * 32,
            "blockNumber": 6,
            "gasUsed": 100,
            "logs": [
                {
                    "topics": [topic_bytes, from_topic_bytes, to_topic_bytes],
                    "address": bytes.fromhex(TOKEN_X[2:]),
                    "data": bytes.fromhex(_uint256_hex(42)),
                },
            ],
        }
        result = parser.parse_receipt(receipt)
        transfers = [e for e in result.events if e.event_type == TraderJoeV2EventType.TRANSFER]
        assert len(transfers) == 1
        # raw_topics should all be hex strings
        for raw_topic in transfers[0].raw_topics:
            assert raw_topic.startswith("0x")


# =============================================================================
# _extract_swap_result branches
# =============================================================================


class TestExtractSwapResult:
    @pytest.fixture
    def parser(self) -> TraderJoeV2ReceiptParser:
        return TraderJoeV2ReceiptParser()

    def test_returns_none_with_single_transfer(self, parser: TraderJoeV2ReceiptParser) -> None:
        """A single Transfer event isn't enough to derive swap (need >=2)."""
        receipt = {
            "status": 1,
            "transactionHash": "0x" + "04" * 32,
            "blockNumber": 7,
            "gasUsed": 100,
            "logs": [
                _make_log(
                    EVENT_TOPICS["Transfer"],
                    TOKEN_X,
                    topics=[_topic_addr(WALLET), _topic_addr(POOL)],
                    data="0x" + _uint256_hex(1000),
                ),
            ],
        }
        result = parser.parse_receipt(receipt)
        assert result.swap_result is None

    def test_zero_amount_returns_none(self, parser: TraderJoeV2ReceiptParser) -> None:
        receipt = {
            "status": 1,
            "transactionHash": "0x" + "05" * 32,
            "blockNumber": 8,
            "gasUsed": 100,
            "logs": [
                _make_log(
                    EVENT_TOPICS["Transfer"],
                    TOKEN_X,
                    topics=[_topic_addr(WALLET), _topic_addr(POOL)],
                    data="0x" + _uint256_hex(0),  # amount in = 0
                ),
                _make_log(
                    EVENT_TOPICS["Transfer"],
                    TOKEN_Y,
                    topics=[_topic_addr(POOL), _topic_addr(WALLET)],
                    data="0x" + _uint256_hex(100),
                ),
            ],
        }
        result = parser.parse_receipt(receipt)
        assert result.swap_result is None

    def test_swap_result_extracted_with_two_transfers(self, parser: TraderJoeV2ReceiptParser) -> None:
        receipt = {
            "status": 1,
            "transactionHash": "0x" + "06" * 32,
            "blockNumber": 9,
            "gasUsed": 200,
            "logs": [
                _make_log(
                    EVENT_TOPICS["Transfer"],
                    TOKEN_X,
                    topics=[_topic_addr(WALLET), _topic_addr(POOL)],
                    data="0x" + _uint256_hex(1000),
                ),
                _make_log(
                    EVENT_TOPICS["Transfer"],
                    TOKEN_Y,
                    topics=[_topic_addr(POOL), _topic_addr(WALLET)],
                    data="0x" + _uint256_hex(2000),
                ),
            ],
        }
        result = parser.parse_receipt(receipt)
        assert result.swap_result is not None
        assert result.swap_result.amount_in == 1000
        assert result.swap_result.amount_out == 2000
        assert result.swap_result.price == Decimal(2)


# =============================================================================
# _extract_liquidity_result branches
# =============================================================================


class TestExtractLiquidityResult:
    @pytest.fixture
    def parser(self) -> TraderJoeV2ReceiptParser:
        return TraderJoeV2ReceiptParser()

    def test_deposit_event_extracts_bin_ids(self, parser: TraderJoeV2ReceiptParser) -> None:
        bin_ids = [8388607, 8388608, 8388609]
        receipt = {
            "status": 1,
            "transactionHash": "0x" + "07" * 32,
            "blockNumber": 10,
            "gasUsed": 500,
            "logs": [
                _make_log(
                    EVENT_TOPICS["DepositedToBins"],
                    POOL,
                    topics=[_topic_addr(WALLET), _topic_addr(WALLET)],
                    data=_bins_data(bin_ids),
                ),
            ],
        }
        result = parser.parse_receipt(receipt)
        assert result.liquidity_result is not None
        assert result.liquidity_result.is_add is True
        assert result.liquidity_result.bin_ids == bin_ids
        assert result.liquidity_result.pool_address.lower() == POOL.lower()

    def test_withdraw_event_extracts_bin_ids(self, parser: TraderJoeV2ReceiptParser) -> None:
        bin_ids = [8388607, 8388608]
        receipt = {
            "status": 1,
            "transactionHash": "0x" + "08" * 32,
            "blockNumber": 11,
            "gasUsed": 500,
            "logs": [
                _make_log(
                    EVENT_TOPICS["WithdrawnFromBins"],
                    POOL,
                    topics=[_topic_addr(WALLET), _topic_addr(WALLET)],
                    data=_bins_data(bin_ids),
                ),
            ],
        }
        result = parser.parse_receipt(receipt)
        assert result.liquidity_result is not None
        assert result.liquidity_result.is_add is False
        assert result.liquidity_result.bin_ids == bin_ids


# =============================================================================
# _parse_bin_ids_from_data sanity branches
# =============================================================================


class TestParseBinIdsFromData:
    @pytest.fixture
    def parser(self) -> TraderJoeV2ReceiptParser:
        return TraderJoeV2ReceiptParser()

    def test_too_short_returns_none(self, parser: TraderJoeV2ReceiptParser) -> None:
        # Less than 64 bytes worth of hex (128 chars).
        result = parser._parse_bin_ids_from_data("0x" + "00" * 8)
        assert result is None

    def test_zero_length_returns_none(self, parser: TraderJoeV2ReceiptParser) -> None:
        # offset_to_ids=0x40, ids length=0
        data = "0x" + _uint256_hex(0x40) + _uint256_hex(0x40) + _uint256_hex(0)
        # Pad to be at least 128 hex chars.
        result = parser._parse_bin_ids_from_data(data)
        assert result is None

    def test_excessive_length_returns_none(self, parser: TraderJoeV2ReceiptParser) -> None:
        # ids_length = 1001 → exceeds sanity bound of 1000.
        data = "0x" + _uint256_hex(0x40) + _uint256_hex(0x40) + _uint256_hex(1001)
        result = parser._parse_bin_ids_from_data(data)
        assert result is None


# =============================================================================
# extract_swap_amounts branches
# =============================================================================


class TestExtractSwapAmountsSlippage:
    @pytest.fixture
    def parser(self) -> TraderJoeV2ReceiptParser:
        return TraderJoeV2ReceiptParser(chain="avalanche")

    def test_slippage_bps_computed_when_resolver_supplies_decimals(
        self, parser: TraderJoeV2ReceiptParser
    ) -> None:
        """expected_out=100, realized=99 → slippage = 1/100 = 100bps."""
        receipt = {
            "status": 1,
            "transactionHash": "0x" + "0a" * 32,
            "blockNumber": 12,
            "gasUsed": 200,
            "logs": [
                _make_log(
                    EVENT_TOPICS["Transfer"],
                    TOKEN_X,
                    topics=[_topic_addr(WALLET), _topic_addr(POOL)],
                    data="0x" + _uint256_hex(10**18),  # 1 token (18d)
                ),
                _make_log(
                    EVENT_TOPICS["Transfer"],
                    TOKEN_Y,
                    topics=[_topic_addr(POOL), _topic_addr(WALLET)],
                    data="0x" + _uint256_hex(99 * 10**18),  # 99 tokens (18d)
                ),
            ],
        }
        mock_resolver = MagicMock()
        mock_resolver.get_decimals.return_value = 18
        with patch(
            "almanak.framework.connectors.traderjoe_v2.receipt_parser.get_token_resolver",
            return_value=mock_resolver,
        ):
            result = parser.extract_swap_amounts(receipt, expected_out=Decimal("100"))

        assert result is not None
        assert result.slippage_bps == 100
        assert result.expected_out_decimal == Decimal("100")

    def test_slippage_suppressed_when_decimals_resolver_fails(
        self, parser: TraderJoeV2ReceiptParser
    ) -> None:
        """If decimals lookup raises, slippage_bps stays None even with expected_out."""
        receipt = {
            "status": 1,
            "transactionHash": "0x" + "0b" * 32,
            "blockNumber": 13,
            "gasUsed": 200,
            "logs": [
                _make_log(
                    EVENT_TOPICS["Transfer"],
                    TOKEN_X,
                    topics=[_topic_addr(WALLET), _topic_addr(POOL)],
                    data="0x" + _uint256_hex(10**18),
                ),
                _make_log(
                    EVENT_TOPICS["Transfer"],
                    TOKEN_Y,
                    topics=[_topic_addr(POOL), _topic_addr(WALLET)],
                    data="0x" + _uint256_hex(99 * 10**18),
                ),
            ],
        }
        mock_resolver = MagicMock()
        # Decimals lookup for slippage gate raises, but the earlier
        # _resolve_token_decimals call returns 18 (fallback). The strict
        # gate then catches the exception and leaves slippage_bps as None.
        call_count = [0]

        def get_decimals(chain: str, addr: str) -> int:
            call_count[0] += 1
            # Fail the strict slippage gate (last call); succeed earlier.
            if call_count[0] >= 3:
                raise RuntimeError("registry down")
            return 18

        mock_resolver.get_decimals.side_effect = get_decimals
        with patch(
            "almanak.framework.connectors.traderjoe_v2.receipt_parser.get_token_resolver",
            return_value=mock_resolver,
        ):
            result = parser.extract_swap_amounts(receipt, expected_out=Decimal("100"))

        assert result is not None
        assert result.slippage_bps is None

    def test_no_swap_returns_none(self, parser: TraderJoeV2ReceiptParser) -> None:
        receipt = {
            "status": 1,
            "transactionHash": "0x" + "0c" * 32,
            "blockNumber": 14,
            "gasUsed": 50,
            "logs": [],
        }
        result = parser.extract_swap_amounts(receipt)
        assert result is None


# =============================================================================
# extract_protocol_fees + extract_lp_close_data
# =============================================================================


class TestExtractProtocolFees:
    def test_returns_unavailable_reason(self) -> None:
        parser = TraderJoeV2ReceiptParser()
        result = parser.extract_protocol_fees({})
        assert result.total_usd is None
        assert result.unavailable_reason == "protocol_fee_not_emitted_in_receipt"


class TestExtractLPCloseData:
    @pytest.fixture
    def parser(self) -> TraderJoeV2ReceiptParser:
        return TraderJoeV2ReceiptParser()

    def test_extracts_amounts_from_two_transfers(self, parser: TraderJoeV2ReceiptParser) -> None:
        bin_ids = [8388608]
        receipt = {
            "status": 1,
            "transactionHash": "0x" + "0d" * 32,
            "blockNumber": 15,
            "gasUsed": 500,
            "logs": [
                # WithdrawnFromBins triggers liquidity_result.is_add=False
                _make_log(
                    EVENT_TOPICS["WithdrawnFromBins"],
                    POOL,
                    topics=[_topic_addr(WALLET), _topic_addr(WALLET)],
                    data=_bins_data(bin_ids),
                ),
                # Two ERC-20 Transfer events for the withdrawn amounts.
                _make_log(
                    EVENT_TOPICS["Transfer"],
                    TOKEN_X,
                    topics=[_topic_addr(POOL), _topic_addr(WALLET)],
                    data="0x" + _uint256_hex(10**18),
                ),
                _make_log(
                    EVENT_TOPICS["Transfer"],
                    TOKEN_Y,
                    topics=[_topic_addr(POOL), _topic_addr(WALLET)],
                    data="0x" + _uint256_hex(2 * 10**18),
                ),
            ],
        }
        result = parser.extract_lp_close_data(receipt)
        assert result is not None
        assert result.amount0_collected == 10**18
        assert result.amount1_collected == 2 * 10**18
        assert result.fees0 == 0
        assert result.fees1 == 0

    def test_returns_none_when_event_is_add(self, parser: TraderJoeV2ReceiptParser) -> None:
        bin_ids = [8388608]
        receipt = {
            "status": 1,
            "transactionHash": "0x" + "0e" * 32,
            "blockNumber": 16,
            "gasUsed": 500,
            "logs": [
                _make_log(
                    EVENT_TOPICS["DepositedToBins"],
                    POOL,
                    topics=[_topic_addr(WALLET), _topic_addr(WALLET)],
                    data=_bins_data(bin_ids),
                ),
            ],
        }
        result = parser.extract_lp_close_data(receipt)
        assert result is None

    def test_returns_none_when_no_liquidity_event(self, parser: TraderJoeV2ReceiptParser) -> None:
        receipt = {
            "status": 1,
            "transactionHash": "0x" + "0f" * 32,
            "blockNumber": 17,
            "gasUsed": 50,
            "logs": [],
        }
        result = parser.extract_lp_close_data(receipt)
        assert result is None


class TestExtractBinIds:
    """Cover extract_bin_ids happy path + degraded paths."""

    @pytest.fixture
    def parser(self) -> TraderJoeV2ReceiptParser:
        return TraderJoeV2ReceiptParser()

    def test_extracts_from_deposit_event(self, parser: TraderJoeV2ReceiptParser) -> None:
        bin_ids = [8388607, 8388608, 8388609]
        receipt = {
            "status": 1,
            "transactionHash": "0x" + "10" * 32,
            "blockNumber": 18,
            "gasUsed": 500,
            "logs": [
                _make_log(
                    EVENT_TOPICS["DepositedToBins"],
                    POOL,
                    topics=[_topic_addr(WALLET), _topic_addr(WALLET)],
                    data=_bins_data(bin_ids),
                ),
            ],
        }
        result = parser.extract_bin_ids(receipt)
        assert result == bin_ids


class TestExtractLiquidity:
    """Cover extract_liquidity (returns None for now, but exercises the path)."""

    def test_extract_liquidity_returns_none_for_lp_event(self) -> None:
        parser = TraderJoeV2ReceiptParser()
        bin_ids = [8388608]
        receipt = {
            "status": 1,
            "transactionHash": "0x" + "11" * 32,
            "blockNumber": 19,
            "gasUsed": 500,
            "logs": [
                _make_log(
                    EVENT_TOPICS["DepositedToBins"],
                    POOL,
                    topics=[_topic_addr(WALLET), _topic_addr(WALLET)],
                    data=_bins_data(bin_ids),
                ),
            ],
        }
        # extract_liquidity returns None by design (amount decoding not implemented).
        result = parser.extract_liquidity(receipt)
        assert result is None

    def test_extract_liquidity_returns_none_when_no_lp_event(self) -> None:
        parser = TraderJoeV2ReceiptParser()
        receipt = {"status": 1, "logs": [], "gasUsed": 0, "blockNumber": 0}
        result = parser.extract_liquidity(receipt)
        assert result is None


class TestParseSwapEvents:
    """parse_swap_events convenience method."""

    def test_returns_swap_event_data_for_swap_receipt(self) -> None:
        parser = TraderJoeV2ReceiptParser()
        receipt = {
            "status": 1,
            "transactionHash": "0x" + "12" * 32,
            "blockNumber": 20,
            "gasUsed": 200,
            "logs": [
                _make_log(
                    EVENT_TOPICS["Transfer"],
                    TOKEN_X,
                    topics=[_topic_addr(WALLET), _topic_addr(POOL)],
                    data="0x" + _uint256_hex(10**18),
                ),
                _make_log(
                    EVENT_TOPICS["Transfer"],
                    TOKEN_Y,
                    topics=[_topic_addr(POOL), _topic_addr(WALLET)],
                    data="0x" + _uint256_hex(2 * 10**18),
                ),
            ],
        }
        result = parser.parse_swap_events(receipt)
        assert len(result) == 1
        assert result[0].amount_in == 10**18
        assert result[0].amount_out == 2 * 10**18

    def test_returns_empty_list_when_no_swap(self) -> None:
        parser = TraderJoeV2ReceiptParser()
        receipt = {"status": 1, "logs": [], "gasUsed": 0, "blockNumber": 0}
        result = parser.parse_swap_events(receipt)
        assert result == []


# =============================================================================
# Top-level parse_receipt sanity
# =============================================================================


class TestParseReceiptSanity:
    def test_parse_receipt_returns_parse_result_on_complete_failure(self) -> None:
        """Even malformed input should not raise — returns a failed ParseResult."""
        parser = TraderJoeV2ReceiptParser()
        # logs as a non-iterable will explode in _parse_logs.
        receipt = {"status": 1, "logs": object(), "gasUsed": 0, "blockNumber": 0}
        result = parser.parse_receipt(receipt)
        assert isinstance(result, ParseResult)
        assert result.success is False

    def test_parse_receipt_with_no_logs_succeeds(self) -> None:
        parser = TraderJoeV2ReceiptParser()
        receipt = {"status": 1, "logs": [], "gasUsed": 100, "blockNumber": 1}
        result = parser.parse_receipt(receipt)
        assert result.success is True
        assert result.events == []
        assert result.swap_result is None
        assert result.liquidity_result is None

    def test_parse_receipt_fails_for_failed_tx(self) -> None:
        parser = TraderJoeV2ReceiptParser()
        receipt = {"status": 0, "logs": [], "gasUsed": 50, "blockNumber": 1}
        result = parser.parse_receipt(receipt)
        assert result.success is False
        assert result.error == "Transaction reverted"

    def test_logs_parsed_liquidity_action(self, caplog: pytest.LogCaptureFixture) -> None:
        """Liquidity result triggers ADD/REMOVE log line — exercises the ParsedLiquidityResult log branch."""
        parser = TraderJoeV2ReceiptParser()
        bin_ids = [8388608]
        receipt = {
            "status": 1,
            "transactionHash": "0x" + "13" * 32,
            "blockNumber": 21,
            "gasUsed": 500,
            "logs": [
                _make_log(
                    EVENT_TOPICS["DepositedToBins"],
                    POOL,
                    topics=[_topic_addr(WALLET), _topic_addr(WALLET)],
                    data=_bins_data(bin_ids),
                ),
            ],
        }
        with caplog.at_level("INFO", logger="almanak.framework.connectors.traderjoe_v2.receipt_parser"):
            result = parser.parse_receipt(receipt)
        assert result.liquidity_result is not None
        assert isinstance(result.liquidity_result, ParsedLiquidityResult)
