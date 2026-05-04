"""Coverage tests for PancakeSwap V3 receipt parser branches.

These tests target the uncovered paths in receipt_parser.py:
- Dataclass helpers (``SwapEventData.token0_in / token1_in / to_dict``,
  ``ParseResult.to_dict``)
- Internal hooks ``_decode_log_data``, ``_create_event``, ``_build_result``
- ``extract_swap_amounts`` exception path + ``_parse_transfer_log`` malformed-data
- Bytes-as-topic / bytes-as-address handling across ``extract_position_id``,
  ``extract_tick_lower``, ``extract_tick_upper``, ``extract_liquidity``,
  ``extract_lp_close_data``
- Backward-compat methods ``parse_swap``, ``is_pancakeswap_event``,
  ``get_event_type`` (incl. None-registry guards)
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock, patch

from almanak.framework.connectors.pancakeswap_v3.receipt_parser import (
    EVENT_TOPICS,
    POSITION_MANAGER_ADDRESSES,
    ZERO_ADDRESS_PADDED,
    PancakeSwapV3EventType,
    PancakeSwapV3ReceiptParser,
    ParseResult,
    SwapEventData,
)

POSITION_MANAGER = POSITION_MANAGER_ADDRESSES["bsc"].lower()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _pad32(val: int, signed: bool = False) -> str:
    if signed and val < 0:
        val += 1 << 256
    return f"{val:064x}"


def _addr_topic(addr: str) -> str:
    return "0x" + addr.replace("0x", "").lower().zfill(64)


# ---------------------------------------------------------------------------
# SwapEventData / ParseResult dataclasses
# ---------------------------------------------------------------------------


class TestSwapEventDataDataclass:
    def test_token0_in_when_amount0_positive(self):
        ev = SwapEventData(
            pool="0x" + "11" * 20,
            sender="0x" + "22" * 20,
            recipient="0x" + "33" * 20,
            amount0=Decimal(100),
            amount1=Decimal(-50),
        )
        assert ev.token0_in is True
        assert ev.token1_in is False

    def test_token1_in_when_amount1_positive(self):
        ev = SwapEventData(
            pool="0x" + "11" * 20,
            sender="0x" + "22" * 20,
            recipient="0x" + "33" * 20,
            amount0=Decimal(-100),
            amount1=Decimal(50),
        )
        assert ev.token0_in is False
        assert ev.token1_in is True

    def test_to_dict_roundtrip(self):
        ev = SwapEventData(
            pool="0x" + "aa" * 20,
            sender="0x" + "bb" * 20,
            recipient="0x" + "cc" * 20,
            amount0=Decimal(10),
            amount1=Decimal(-20),
            sqrt_price_x96=2**96,
            liquidity=10**18,
            tick=42,
            protocol_fees_token0=1,
            protocol_fees_token1=2,
        )
        d = ev.to_dict()
        assert d["pool"] == "0x" + "aa" * 20
        assert d["amount0"] == "10"
        assert d["amount1"] == "-20"
        assert d["sqrt_price_x96"] == str(2**96)
        assert d["liquidity"] == str(10**18)
        assert d["tick"] == 42
        assert d["token0_in"] is True
        assert d["token1_in"] is False
        assert d["protocol_fees_token0"] == "1"
        assert d["protocol_fees_token1"] == "2"


class TestParseResultDataclass:
    def test_to_dict_empty_swaps(self):
        r = ParseResult(success=True, transaction_hash="0xabc", block_number=10)
        d = r.to_dict()
        assert d["success"] is True
        assert d["swaps"] == []
        assert d["transaction_hash"] == "0xabc"
        assert d["block_number"] == 10
        assert d["error"] is None

    def test_to_dict_with_swaps(self):
        ev = SwapEventData(
            pool="0x" + "aa" * 20,
            sender="0x" + "bb" * 20,
            recipient="0x" + "cc" * 20,
            amount0=Decimal(1),
            amount1=Decimal(-2),
        )
        r = ParseResult(success=True, swaps=[ev])
        d = r.to_dict()
        assert len(d["swaps"]) == 1
        assert d["swaps"][0]["pool"] == "0x" + "aa" * 20


# ---------------------------------------------------------------------------
# _decode_log_data / _create_event / _build_result hooks
# ---------------------------------------------------------------------------


class TestDecodeLogData:
    def test_returns_empty_for_non_swap_event(self):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        result = parser._decode_log_data(
            "Transfer", ["0x" + "00" * 32], "0x" + "00" * 32, "0x" + "11" * 20
        )
        assert result == {}

    def test_decodes_swap_event_data(self):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        sender = "0x" + "aa" * 20
        recipient = "0x" + "bb" * 20
        # 9 fields × 32 bytes
        data = "0x" + (
            _pad32(123, signed=True)  # amount0
            + _pad32(-456, signed=True)  # amount1
            + _pad32(2**96)  # sqrtPriceX96
            + _pad32(10**18)  # liquidity
            + _pad32(7, signed=True)  # tick
            + _pad32(11)  # protocol_fees_token0
            + _pad32(22)  # protocol_fees_token1
        )
        topics = [
            EVENT_TOPICS["Swap"],
            _addr_topic(sender),
            _addr_topic(recipient),
        ]
        result = parser._decode_log_data("Swap", topics, data, "0x" + "CC" * 20)
        assert result["amount0"] == 123
        assert result["amount1"] == -456
        assert result["liquidity"] == 10**18
        assert result["tick"] == 7
        assert result["protocol_fees_token0"] == 11
        assert result["protocol_fees_token1"] == 22
        # Address normalized to lowercase
        assert result["pool_address"] == "0x" + "cc" * 20


class TestCreateEvent:
    def test_returns_none_for_non_swap_event(self):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        out = parser._create_event(
            event_name="Transfer",
            log_index=0,
            tx_hash="0xabc",
            block_number=1,
            contract_address="0x" + "11" * 20,
            decoded_data={"sender": "0x"},
            raw_topics=[],
            raw_data="",
        )
        assert out is None

    def test_returns_none_for_empty_decoded_data(self):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        out = parser._create_event(
            event_name="Swap",
            log_index=0,
            tx_hash="0xabc",
            block_number=1,
            contract_address="0x" + "11" * 20,
            decoded_data={},
            raw_topics=[],
            raw_data="",
        )
        assert out is None

    def test_creates_swap_event_data(self):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        decoded = {
            "pool_address": "0x" + "ab" * 20,
            "sender": "0x" + "01" * 20,
            "recipient": "0x" + "02" * 20,
            "amount0": 100,
            "amount1": -50,
            "sqrt_price_x96": 2**96,
            "liquidity": 10**18,
            "tick": 7,
            "protocol_fees_token0": 1,
            "protocol_fees_token1": 2,
        }
        out = parser._create_event(
            event_name="Swap",
            log_index=0,
            tx_hash="0xabc",
            block_number=1,
            contract_address="0x" + "ff" * 20,
            decoded_data=decoded,
            raw_topics=[],
            raw_data="",
        )
        assert out is not None
        assert out.pool == "0x" + "ab" * 20
        assert out.amount0 == Decimal(100)
        assert out.amount1 == Decimal(-50)
        assert out.protocol_fees_token0 == 1


class TestBuildResult:
    def test_returns_failure_when_tx_failed(self):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        result = parser._build_result(
            events=[],
            receipt={},
            tx_hash="0xabc",
            block_number=10,
            tx_success=False,
        )
        assert result.success is False
        assert result.error == "Transaction failed"
        assert result.transaction_hash == "0xabc"

    def test_returns_failure_when_kwargs_error_provided(self):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        result = parser._build_result(
            events=[],
            receipt={},
            tx_hash="0xabc",
            block_number=10,
            tx_success=True,
            error="bad parse",
        )
        assert result.success is False
        assert result.error == "bad parse"

    def test_returns_success_with_events(self):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        ev = SwapEventData(
            pool="0x" + "11" * 20,
            sender="0x" + "22" * 20,
            recipient="0x" + "33" * 20,
            amount0=Decimal(0),
            amount1=Decimal(0),
        )
        result = parser._build_result(
            events=[ev],
            receipt={},
            tx_hash="0x" + "ab" * 32,
            block_number=42,
            tx_success=True,
        )
        assert result.success is True
        assert result.swaps == [ev]
        assert result.block_number == 42


# ---------------------------------------------------------------------------
# extract_swap_amounts exception path & helpers
# ---------------------------------------------------------------------------


class TestExtractSwapAmountsExceptionPath:
    def test_exception_in_pipeline_returns_none(self):
        """If something raises mid-pipeline, the outer try/except catches and
        returns None (does NOT crash)."""
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        # Receipt with a successful status flag that triggers later code paths,
        # but we'll make _has_pcs_swap_log raise.
        with patch.object(parser, "_has_pcs_swap_log", side_effect=RuntimeError("boom")):
            assert parser.extract_swap_amounts({"status": 1, "logs": []}) is None


class TestParseTransferLog:
    def test_returns_none_on_too_few_topics(self):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        log = {"topics": [EVENT_TOPICS["Transfer"]], "data": "0x" + "00" * 32}
        assert parser._parse_transfer_log(log, EVENT_TOPICS["Transfer"].lower()) is None

    def test_returns_none_when_topic_is_not_transfer(self):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        log = {
            "topics": ["0x" + "11" * 32, _addr_topic("0x" + "01" * 20), _addr_topic("0x" + "02" * 20)],
            "data": "0x" + "00" * 32,
        }
        assert parser._parse_transfer_log(log, EVENT_TOPICS["Transfer"].lower()) is None

    def test_returns_none_when_data_is_empty(self):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        log = {
            "topics": [
                EVENT_TOPICS["Transfer"],
                _addr_topic("0x" + "01" * 20),
                _addr_topic("0x" + "02" * 20),
            ],
            "data": "",
        }
        assert parser._parse_transfer_log(log, EVENT_TOPICS["Transfer"].lower()) is None

    def test_returns_none_on_bad_amount_decode(self):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        # 'data' too short / non-hex causes ValueError or IndexError -> None
        log = {
            "topics": [
                EVENT_TOPICS["Transfer"],
                _addr_topic("0x" + "01" * 20),
                _addr_topic("0x" + "02" * 20),
            ],
            "data": "0xZZZ",  # non-hex
        }
        # The parser catches (ValueError, IndexError) -> returns None
        assert parser._parse_transfer_log(log, EVENT_TOPICS["Transfer"].lower()) is None


class TestResolveDecimalsBranches:
    def test_empty_token_returns_none(self):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        assert parser._resolve_decimals("") is None

    def test_resolver_exception_returns_none(self):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        with patch(
            "almanak.framework.data.tokens.get_token_resolver",
            side_effect=RuntimeError("no resolver"),
        ):
            assert parser._resolve_decimals("0x" + "11" * 20) is None

    def test_resolver_resolves_decimals(self):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        mock_resolver = MagicMock()
        mock_token = MagicMock()
        mock_token.decimals = 6
        mock_resolver.resolve.return_value = mock_token
        with patch(
            "almanak.framework.data.tokens.get_token_resolver",
            return_value=mock_resolver,
        ):
            assert parser._resolve_decimals("0x" + "11" * 20) == 6


# ---------------------------------------------------------------------------
# Bytes-as-topic / bytes-as-address handling across extract_* methods
# ---------------------------------------------------------------------------


class TestExtractPositionIdBranches:
    def test_handles_address_as_bytes(self):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        token_id = 999
        log = {
            "address": bytes.fromhex(POSITION_MANAGER.replace("0x", "")),
            "topics": [
                bytes.fromhex(EVENT_TOPICS["Transfer"].replace("0x", "")),
                bytes.fromhex(ZERO_ADDRESS_PADDED.replace("0x", "")),
                bytes.fromhex("00" * 12 + "01" * 20),  # to (32 bytes)
                bytes.fromhex(_pad32(token_id)),  # tokenId topic
            ],
            "data": "0x",
        }
        assert parser.extract_position_id({"logs": [log]}) == token_id

    def test_skips_log_with_wrong_address(self):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        log = {
            "address": "0x" + "01" * 20,  # Not position manager
            "topics": [
                EVENT_TOPICS["Transfer"],
                ZERO_ADDRESS_PADDED,
                _addr_topic("0x" + "01" * 20),
                _addr_topic("0x" + "02" * 20),
            ],
            "data": "0x",
        }
        assert parser.extract_position_id({"logs": [log]}) is None

    def test_skips_log_with_too_few_topics(self):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        log = {
            "address": POSITION_MANAGER,
            "topics": [EVENT_TOPICS["Transfer"]],  # < 4
            "data": "0x",
        }
        assert parser.extract_position_id({"logs": [log]}) is None

    def test_skips_log_with_wrong_first_topic(self):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        log = {
            "address": POSITION_MANAGER,
            "topics": [
                "0x" + "ff" * 32,  # not Transfer
                ZERO_ADDRESS_PADDED,
                _addr_topic("0x" + "01" * 20),
                _addr_topic("0x" + "02" * 20),
            ],
            "data": "0x",
        }
        assert parser.extract_position_id({"logs": [log]}) is None

    def test_skips_log_with_non_zero_from_topic(self):
        """Transfers from a non-zero address are not mints — should be ignored."""
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        log = {
            "address": POSITION_MANAGER,
            "topics": [
                EVENT_TOPICS["Transfer"],
                _addr_topic("0x" + "ff" * 20),  # NOT zero
                _addr_topic("0x" + "01" * 20),
                "0x" + _pad32(123),
            ],
            "data": "0x",
        }
        assert parser.extract_position_id({"logs": [log]}) is None

    def test_returns_none_on_invalid_token_id(self):
        """Non-hex tokenId topic should be skipped (continue), not crash."""
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        log = {
            "address": POSITION_MANAGER,
            "topics": [
                EVENT_TOPICS["Transfer"],
                ZERO_ADDRESS_PADDED,
                _addr_topic("0x" + "01" * 20),
                "0xnot-hex-at-all",
            ],
            "data": "0x",
        }
        assert parser.extract_position_id({"logs": [log]}) is None

    def test_unknown_chain_falls_back_to_bsc_position_manager(self):
        """When chain is not in POSITION_MANAGER_ADDRESSES, the parser falls
        back to the canonical hardcoded BSC position manager."""
        parser = PancakeSwapV3ReceiptParser(chain="some-fictional-chain")
        token_id = 42
        log = {
            "address": "0x46A15B0b27311cedF172AB29E4f4766fbE7F4364".lower(),
            "topics": [
                EVENT_TOPICS["Transfer"],
                ZERO_ADDRESS_PADDED,
                _addr_topic("0x" + "01" * 20),
                "0x" + _pad32(token_id),
            ],
            "data": "0x",
        }
        assert parser.extract_position_id({"logs": [log]}) == token_id

    def test_outer_exception_returns_none(self):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        # Receipt is itself bad (raises on .get) -> outer try/except returns None
        bad = MagicMock()
        bad.get.side_effect = RuntimeError("boom")
        assert parser.extract_position_id(bad) is None


class TestExtractTickLowerBranches:
    def test_skips_log_with_too_few_topics(self):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        log = {
            "topics": [EVENT_TOPICS["Mint"]],
            "data": "0x",
        }
        assert parser.extract_tick_lower({"logs": [log]}) is None

    def test_handles_first_topic_as_bytes(self):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        log = {
            "address": "0x" + "11" * 20,
            "topics": [
                bytes.fromhex(EVENT_TOPICS["Mint"].replace("0x", "")),
                _addr_topic("0x" + "01" * 20),
                "0x" + _pad32(-100, signed=True),
                "0x" + _pad32(200, signed=True),
            ],
            "data": "0x",
        }
        assert parser.extract_tick_lower({"logs": [log]}) == -100

    def test_handles_tick_lower_topic_as_bytes(self):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        log = {
            "address": "0x" + "11" * 20,
            "topics": [
                EVENT_TOPICS["Mint"],
                _addr_topic("0x" + "01" * 20),
                bytes.fromhex(_pad32(50, signed=False)),
                "0x" + _pad32(200, signed=True),
            ],
            "data": "0x",
        }
        assert parser.extract_tick_lower({"logs": [log]}) == 50

    def test_outer_exception_returns_none(self):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        bad = MagicMock()
        bad.get.side_effect = RuntimeError("boom")
        assert parser.extract_tick_lower(bad) is None


class TestExtractTickUpperBranches:
    def test_skips_log_with_too_few_topics(self):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        log = {"topics": [EVENT_TOPICS["Mint"]], "data": "0x"}
        assert parser.extract_tick_upper({"logs": [log]}) is None

    def test_handles_first_topic_as_bytes(self):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        log = {
            "topics": [
                bytes.fromhex(EVENT_TOPICS["Mint"].replace("0x", "")),
                _addr_topic("0x" + "01" * 20),
                "0x" + _pad32(-100, signed=True),
                "0x" + _pad32(300, signed=True),
            ],
            "data": "0x",
        }
        assert parser.extract_tick_upper({"logs": [log]}) == 300

    def test_handles_tick_upper_topic_as_bytes(self):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        log = {
            "topics": [
                EVENT_TOPICS["Mint"],
                _addr_topic("0x" + "01" * 20),
                "0x" + _pad32(-100, signed=True),
                bytes.fromhex(_pad32(400, signed=False)),
            ],
            "data": "0x",
        }
        assert parser.extract_tick_upper({"logs": [log]}) == 400

    def test_outer_exception_returns_none(self):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        bad = MagicMock()
        bad.get.side_effect = RuntimeError("boom")
        assert parser.extract_tick_upper(bad) is None


class TestExtractLiquidityBranches:
    def test_handles_first_topic_as_bytes(self):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        sender = "0x" + "01" * 20
        sender_padded = sender.replace("0x", "").lower().zfill(64)
        liquidity = 10**18
        amount0 = 100
        amount1 = 200
        data = "0x" + sender_padded + _pad32(liquidity) + _pad32(amount0) + _pad32(amount1)
        log = {
            "topics": [
                bytes.fromhex(EVENT_TOPICS["Mint"].replace("0x", "")),
                _addr_topic(sender),
                "0x" + _pad32(-100, signed=True),
                "0x" + _pad32(300, signed=True),
            ],
            "data": data,
        }
        assert parser.extract_liquidity({"logs": [log]}) == liquidity

    def test_skips_log_with_empty_data(self):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        log = {
            "topics": [
                EVENT_TOPICS["Mint"],
                _addr_topic("0x" + "01" * 20),
                "0x" + _pad32(-100, signed=True),
                "0x" + _pad32(300, signed=True),
            ],
            "data": "",
        }
        # Empty data -> normalized to "" by HexDecoder.normalize_hex -> continue
        assert parser.extract_liquidity({"logs": [log]}) is None

    def test_outer_exception_returns_none(self):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        bad = MagicMock()
        bad.get.side_effect = RuntimeError("boom")
        assert parser.extract_liquidity(bad) is None


class TestExtractLPCloseDataBranches:
    def test_handles_first_topic_as_bytes(self):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        amount0_collected = 5000
        amount1_collected = 10000
        # Collect data layout: recipient (32) + amount0 (uint128) + amount1 (uint128)
        # Decoder reads at offsets 32 and 64 (interprets as uint128 right-aligned in 32-byte words)
        data = "0x" + _pad32(0) + _pad32(amount0_collected) + _pad32(amount1_collected)
        log = {
            "topics": [
                bytes.fromhex(EVENT_TOPICS["Collect"].replace("0x", "")),
                _addr_topic("0x" + "01" * 20),
                "0x" + _pad32(-100, signed=True),
                "0x" + _pad32(300, signed=True),
            ],
            "data": data,
        }
        result = parser.extract_lp_close_data({"logs": [log]})
        assert result is not None
        assert result.amount0_collected == amount0_collected
        assert result.amount1_collected == amount1_collected

    def test_outer_exception_returns_none(self):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        bad = MagicMock()
        bad.get.side_effect = RuntimeError("boom")
        assert parser.extract_lp_close_data(bad) is None

    def test_skips_log_with_empty_topics(self):
        """Log with empty topics list should be skipped (continue) without
        raising."""
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        log = {"topics": [], "data": "0x"}
        assert parser.extract_lp_close_data({"logs": [log]}) is None


class TestHasPcsSwapLogBranches:
    """Targeted coverage for the empty-topics continue branch of
    ``_has_pcs_swap_log`` (line 442)."""

    def test_skips_log_with_empty_topics(self):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        receipt = {"logs": [{"address": "0x" + "11" * 20, "topics": [], "data": "0x"}]}
        assert parser._has_pcs_swap_log(receipt) is False


# ---------------------------------------------------------------------------
# extract_protocol_fees additional coverage
# ---------------------------------------------------------------------------


class TestExtractProtocolFeesBranches:
    def test_outer_exception_returns_none(self):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        bad = MagicMock()
        bad.get.side_effect = RuntimeError("boom")
        assert parser.extract_protocol_fees(bad, fee_tier_bps=500) is None

    def test_log_with_empty_topics_continues(self):
        """Logs with empty topics list should be skipped without raising."""
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        receipt = {
            "status": 1,
            "from": "0x" + "bb" * 20,
            "logs": [{"address": "0x" + "11" * 20, "topics": [], "data": "0x"}],
        }
        assert parser.extract_protocol_fees(receipt, fee_tier_bps=500) is None

    def test_status_as_hex_string(self):
        """Hex string status (e.g. '0x1') is accepted."""
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        receipt = {
            "status": "0x1",
            "from": "0x" + "bb" * 20,
            "logs": [],
        }
        assert parser.extract_protocol_fees(receipt, fee_tier_bps=500) is None

    def test_status_as_decimal_string(self):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        receipt = {
            "status": "1",
            "from": "0x" + "bb" * 20,
            "logs": [],
        }
        assert parser.extract_protocol_fees(receipt, fee_tier_bps=500) is None


# ---------------------------------------------------------------------------
# Backward-compat helpers: parse_swap, is_pancakeswap_event, get_event_type
# ---------------------------------------------------------------------------


class TestParseSwapBackcompat:
    def test_parse_swap_returns_swap_event_data(self):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        sender = "0x" + "01" * 20
        recipient = "0x" + "02" * 20
        data = "0x" + (
            _pad32(100, signed=True)  # amount0
            + _pad32(-50, signed=True)  # amount1
            + _pad32(2**96)
            + _pad32(10**18)
            + _pad32(0, signed=True)
            + _pad32(0)
            + _pad32(0)
        )
        log = {
            "address": "0x" + "AA" * 20,
            "topics": [
                EVENT_TOPICS["Swap"],
                _addr_topic(sender),
                _addr_topic(recipient),
            ],
            "data": data,
            "logIndex": 7,
        }
        ev = parser.parse_swap(log)
        assert ev is not None
        assert ev.amount0 == Decimal(100)
        assert ev.amount1 == Decimal(-50)
        assert ev.pool == "0x" + "aa" * 20  # lowercased

    def test_parse_swap_handles_data_as_bytes(self):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        sender = "0x" + "01" * 20
        recipient = "0x" + "02" * 20
        data_hex = (
            _pad32(1, signed=True)
            + _pad32(-2, signed=True)
            + _pad32(2**96)
            + _pad32(10**18)
            + _pad32(0, signed=True)
            + _pad32(0)
            + _pad32(0)
        )
        log = {
            "address": bytes.fromhex("aa" * 20),
            "topics": [
                EVENT_TOPICS["Swap"],
                _addr_topic(sender),
                _addr_topic(recipient),
            ],
            "data": bytes.fromhex(data_hex),
            "logIndex": 0,
        }
        ev = parser.parse_swap(log)
        assert ev is not None
        assert ev.amount0 == Decimal(1)
        assert ev.amount1 == Decimal(-2)

    def test_parse_swap_returns_none_when_decode_returns_empty(self):
        """Forcing _decode_log_data to return {} -> parse_swap should return None."""
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        with patch.object(parser, "_decode_log_data", return_value={}):
            log = {
                "address": "0x" + "aa" * 20,
                "topics": [EVENT_TOPICS["Swap"]],
                "data": "0x",
            }
            assert parser.parse_swap(log) is None


class TestIsPancakeswapEvent:
    def test_known_topic_returns_true(self):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        assert parser.is_pancakeswap_event(EVENT_TOPICS["Swap"]) is True

    def test_topic_as_bytes(self):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        topic_bytes = bytes.fromhex(EVENT_TOPICS["Swap"].replace("0x", ""))
        assert parser.is_pancakeswap_event(topic_bytes) is True

    def test_topic_without_0x_prefix(self):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        topic_no_prefix = EVENT_TOPICS["Swap"].replace("0x", "")
        assert parser.is_pancakeswap_event(topic_no_prefix) is True

    def test_unknown_topic_returns_false(self):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        assert parser.is_pancakeswap_event("0x" + "ff" * 32) is False

    def test_returns_false_when_registry_is_none(self):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        parser.registry = None
        assert parser.is_pancakeswap_event(EVENT_TOPICS["Swap"]) is False


class TestGetEventType:
    def test_known_topic_returns_correct_type(self):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        assert parser.get_event_type(EVENT_TOPICS["Swap"]) == PancakeSwapV3EventType.SWAP
        assert parser.get_event_type(EVENT_TOPICS["Mint"]) == PancakeSwapV3EventType.MINT
        assert parser.get_event_type(EVENT_TOPICS["Burn"]) == PancakeSwapV3EventType.BURN
        assert parser.get_event_type(EVENT_TOPICS["Collect"]) == PancakeSwapV3EventType.COLLECT
        assert parser.get_event_type(EVENT_TOPICS["Transfer"]) == PancakeSwapV3EventType.TRANSFER

    def test_topic_as_bytes(self):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        topic_bytes = bytes.fromhex(EVENT_TOPICS["Swap"].replace("0x", ""))
        assert parser.get_event_type(topic_bytes) == PancakeSwapV3EventType.SWAP

    def test_topic_without_0x_prefix(self):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        no_prefix = EVENT_TOPICS["Mint"].replace("0x", "")
        assert parser.get_event_type(no_prefix) == PancakeSwapV3EventType.MINT

    def test_unknown_topic_returns_unknown(self):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        assert parser.get_event_type("0x" + "ff" * 32) == PancakeSwapV3EventType.UNKNOWN

    def test_returns_unknown_when_registry_is_none(self):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        parser.registry = None
        assert parser.get_event_type(EVENT_TOPICS["Swap"]) == PancakeSwapV3EventType.UNKNOWN


# ---------------------------------------------------------------------------
# Permission hints module — coverage for the import statement
# ---------------------------------------------------------------------------


def test_permission_hints_module_loads():
    """Importing the module exercises the PERMISSION_HINTS constant assignment."""
    from almanak.framework.connectors.pancakeswap_v3 import permission_hints

    assert permission_hints.PERMISSION_HINTS is not None
    assert "bsc" in permission_hints.PERMISSION_HINTS.synthetic_lp_pair
    bsc_pair = permission_hints.PERMISSION_HINTS.synthetic_lp_pair["bsc"]
    assert len(bsc_pair) == 2
    # USDT + WBNB on BSC
    assert bsc_pair[0].lower() == "0x55d398326f99059fF775485246999027B3197955".lower()
    assert bsc_pair[1].lower() == "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c".lower()
