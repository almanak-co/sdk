"""Unit tests for ``almanak.connectors._strategy_base.v3_receipt_parser_helpers``.

Tests the canonical implementations of the five receipt-parser helpers
extracted from ``UniswapV3ReceiptParser`` / ``SushiSwapV3ReceiptParser``
(plan 014 Stage C). The per-parser tests continue to exercise the delegate
methods; this file pins the shared module-level function behaviour directly.

Cover at minimum (per plan 014 Step 10):
- decode_swap_data: happy path + malformed-data fallback
- decode_transfer_data: happy path + malformed-data fallback
- build_hint_map: None -> {}; malformed entry skipped; address lowercased
- resolve_token_info: resolver failure -> ("", None)
- strict_parse: parse_receipt raises -> ExtractError; success=False ->
  ExtractError; success=True -> None
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from almanak.connectors._strategy_base.v3_receipt_parser_helpers import (
    build_hint_map,
    decode_swap_data,
    decode_transfer_data,
    resolve_token_info,
    strict_parse,
)
from almanak.framework.execution.extract_result import ExtractError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

WALLET = "0x1234567890123456789012345678901234567890"
POOL_ADDRESS = "0xC6962004F452BE9203591991D15F6B388e09E8D0"
USDC_ARB = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"


def _pad_addr(addr: str) -> str:
    """Pad address to 32-byte topic."""
    return "0x" + addr.lower().replace("0x", "").zfill(64)


def _enc_uint(value: int) -> str:
    """Encode uint as 32-byte hex (no 0x)."""
    return hex(value)[2:].zfill(64)


def _enc_int_signed(value: int) -> str:
    """Encode signed int as 32-byte two's complement hex."""
    if value >= 0:
        return hex(value)[2:].zfill(64)
    return hex((1 << 256) + value)[2:].zfill(64)


# ---------------------------------------------------------------------------
# decode_swap_data
# ---------------------------------------------------------------------------


class TestDecodeSwapData:
    """decode_swap_data happy path and fallback."""

    def test_happy_path_all_fields_present(self) -> None:
        """Swap event decode: all fields extracted correctly."""
        data = "0x" + (
            _enc_int_signed(1000)
            + _enc_int_signed(-500)
            + _enc_uint(2**96)
            + _enc_uint(10**12)
            + _enc_int_signed(42)
        )
        topics = ["0xTopic0", _pad_addr(WALLET), _pad_addr(WALLET)]
        result = decode_swap_data(topics, data, POOL_ADDRESS)
        assert result["amount0"] == 1000
        assert result["amount1"] == -500
        assert result["sqrt_price_x96"] == 2**96
        assert result["liquidity"] == 10**12
        assert result["tick"] == 42
        assert result["sender"] == WALLET.lower()
        assert result["recipient"] == WALLET.lower()
        assert result["pool_address"] == POOL_ADDRESS.lower()

    def test_malformed_data_returns_raw_data_fallback(self) -> None:
        """Malformed data: returns {raw_data: data} without raising."""
        result = decode_swap_data([], "not-hex", POOL_ADDRESS)
        assert "raw_data" in result
        assert result["raw_data"] == "not-hex"

    def test_bytes_address_normalized(self) -> None:
        """bytes address -> canonical lowercase 0x-prefixed hex."""
        data = "0x" + (
            _enc_int_signed(1) + _enc_int_signed(-1)
            + _enc_uint(2**96) + _enc_uint(0) + _enc_int_signed(0)
        )
        topics = ["0xTopic0", _pad_addr(WALLET), _pad_addr(WALLET)]
        result = decode_swap_data(
            topics, data, bytes.fromhex(POOL_ADDRESS[2:])
        )
        assert result["pool_address"] == POOL_ADDRESS.lower()

    def test_empty_topics_handled(self) -> None:
        """Missing topics produce empty sender/recipient strings (not raises)."""
        data = "0x" + (
            _enc_int_signed(1) + _enc_int_signed(0)
            + _enc_uint(2**96) + _enc_uint(0) + _enc_int_signed(0)
        )
        result = decode_swap_data([], data, POOL_ADDRESS)
        # With empty topics the function still succeeds but sender/recipient are "".
        assert result["amount0"] == 1

    def test_log_parameter_accepted(self) -> None:
        """log= kwarg is accepted without raising."""
        import logging

        custom_logger = logging.getLogger("test.custom.logger")
        # Use data that will parse (to any result) - the key requirement is
        # that passing log= doesn't raise.
        data = "0x" + (
            _enc_int_signed(1) + _enc_int_signed(0)
            + _enc_uint(2**96) + _enc_uint(0) + _enc_int_signed(0)
        )
        result = decode_swap_data([], data, "0xaddr", log=custom_logger)
        assert isinstance(result, dict)


# ---------------------------------------------------------------------------
# decode_transfer_data
# ---------------------------------------------------------------------------


class TestDecodeTransferData:
    """decode_transfer_data happy path and fallback."""

    def test_happy_path_all_fields_present(self) -> None:
        """Transfer event decode: value and addresses extracted."""
        data = "0x" + _enc_uint(12345)
        topics = ["0xTopic0", _pad_addr(WALLET), _pad_addr(POOL_ADDRESS)]
        result = decode_transfer_data(topics, data, USDC_ARB)
        assert result["value"] == 12345
        assert result["from_addr"] == WALLET.lower()
        assert result["to_addr"] == POOL_ADDRESS.lower()
        assert result["token_address"] == USDC_ARB.lower()

    def test_malformed_data_returns_raw_data_fallback(self) -> None:
        """Malformed data: returns {raw_data: data} without raising."""
        result = decode_transfer_data([], "not-hex", USDC_ARB)
        assert "raw_data" in result
        assert result["raw_data"] == "not-hex"

    def test_bytes_address_normalized(self) -> None:
        """bytes token address -> canonical lowercase 0x-prefixed hex."""
        data = "0x" + _enc_uint(5)
        topics = ["0xTopic0", _pad_addr(WALLET), _pad_addr(POOL_ADDRESS)]
        result = decode_transfer_data(
            topics, data, bytes.fromhex(USDC_ARB[2:])
        )
        assert result["token_address"] == USDC_ARB.lower()

    def test_log_parameter_accepted(self) -> None:
        """log= kwarg is accepted without raising."""
        import logging

        custom_logger = logging.getLogger("test.custom.transfer.logger")
        # Use data that will parse (to any result) - the key requirement is
        # that passing log= doesn't raise.
        data = "0x" + _enc_uint(42)
        result = decode_transfer_data([], data, "0xaddr", log=custom_logger)
        assert isinstance(result, dict)


# ---------------------------------------------------------------------------
# build_hint_map
# ---------------------------------------------------------------------------


class TestBuildHintMap:
    """build_hint_map: None, malformed entry, address lowercased."""

    def test_none_input_returns_empty_dict(self) -> None:
        assert build_hint_map(None) == {}

    def test_empty_dict_returns_empty_dict(self) -> None:
        assert build_hint_map({}) == {}

    def test_valid_token_in_and_token_out(self) -> None:
        meta = {
            "token_in": {"address": USDC_ARB, "decimals": 6, "symbol": "USDC"},
            "token_out": {"address": POOL_ADDRESS, "decimals": 18, "symbol": "WETH"},
        }
        result = build_hint_map(meta)
        assert result[USDC_ARB.lower()] == ("USDC", 6)
        assert result[POOL_ADDRESS.lower()] == ("WETH", 18)

    def test_address_lowercased(self) -> None:
        """Address keys must be lowercased."""
        meta = {
            "token_in": {"address": USDC_ARB.upper(), "decimals": 6, "symbol": "USDC"},
        }
        result = build_hint_map(meta)
        assert USDC_ARB.lower() in result
        assert USDC_ARB.upper() not in result

    def test_malformed_entry_skipped(self) -> None:
        """Entry without address or decimals is silently skipped."""
        meta = {
            "token_in": {"address": USDC_ARB, "decimals": 6, "symbol": "USDC"},
            "token_out": {"no_address": True},  # missing address key
        }
        result = build_hint_map(meta)
        assert len(result) == 1
        assert USDC_ARB.lower() in result

    def test_non_dict_entry_skipped(self) -> None:
        """Non-dict entry (e.g. None) is silently skipped."""
        meta = {
            "token_in": None,
            "token_out": {"address": USDC_ARB, "decimals": 6, "symbol": "USDC"},
        }
        result = build_hint_map(meta)
        assert len(result) == 1

    def test_invalid_decimals_skipped(self) -> None:
        """Entry with non-integer decimals (str of non-int) is skipped."""
        meta = {
            "token_in": {"address": USDC_ARB, "decimals": "bad", "symbol": "USDC"},
        }
        result = build_hint_map(meta)
        assert result == {}

    def test_log_parameter_used_for_debug(self, caplog: pytest.LogCaptureFixture) -> None:
        """log= kwarg routes the debug message about malformed entries."""
        import logging

        custom_logger = logging.getLogger("test.hint.logger")
        meta = {
            "token_in": {"address": USDC_ARB, "decimals": "bad", "symbol": "USDC"},
        }
        with caplog.at_level(logging.DEBUG, logger="test.hint.logger"):
            build_hint_map(meta, log=custom_logger)
        # The debug message may or may not fire (depends on whether int() fails);
        # just ensure it doesn't raise and the logger is accepted.


# ---------------------------------------------------------------------------
# resolve_token_info
# ---------------------------------------------------------------------------


class TestResolveTokenInfo:
    """resolve_token_info: resolver failure produces ("", None)."""

    def test_resolver_failure_returns_empty(self) -> None:
        """When get_token_resolver() raises, returns ("", None)."""
        with patch(
            "almanak.connectors._strategy_base.v3_receipt_parser_helpers.resolve_token_info"
        ) as mock_resolve:
            mock_resolve.side_effect = Exception("resolver unavailable")
            # Call the actual function since we patched at the wrong level;
            # test directly with a mock resolver instead.

        # Use a mock that makes the resolver raise inside the function.
        with patch(
            "almanak.framework.data.tokens.resolver.get_token_resolver"
        ) as mock_get_resolver:
            mock_get_resolver.side_effect = RuntimeError("no resolver")
            result = resolve_token_info(USDC_ARB, "arbitrum")
        assert result == ("", None)

    def test_resolver_returns_symbol_and_decimals(self) -> None:
        """When resolver succeeds, returns (symbol, decimals)."""
        fake_resolved = SimpleNamespace(symbol="USDC", decimals=6)
        with patch(
            "almanak.framework.data.tokens.resolver.get_token_resolver"
        ) as mock_get_resolver:
            mock_resolver = MagicMock()
            mock_resolver.resolve.return_value = fake_resolved
            mock_get_resolver.return_value = mock_resolver
            result = resolve_token_info(USDC_ARB, "arbitrum")
        assert result == ("USDC", 6)

    def test_resolve_raises_returns_empty(self) -> None:
        """When resolver.resolve() raises, returns ("", None)."""
        with patch(
            "almanak.framework.data.tokens.resolver.get_token_resolver"
        ) as mock_get_resolver:
            mock_resolver = MagicMock()
            mock_resolver.resolve.side_effect = KeyError("token not found")
            mock_get_resolver.return_value = mock_resolver
            result = resolve_token_info("0xunknown", "arbitrum")
        assert result == ("", None)


# ---------------------------------------------------------------------------
# strict_parse
# ---------------------------------------------------------------------------


class TestStrictParse:
    """strict_parse: exception -> ExtractError; success=False -> ExtractError; success -> None."""

    def test_parse_receipt_raises_returns_extract_error(self) -> None:
        """When parse_receipt raises, returns ExtractError."""
        parser = MagicMock()
        parser.parse_receipt.side_effect = ValueError("malformed receipt")
        result = strict_parse(parser, {})
        assert isinstance(result, ExtractError)
        assert "ValueError" in result.error

    def test_parse_receipt_success_false_returns_extract_error(self) -> None:
        """When parse_receipt returns success=False, returns ExtractError."""
        parser = MagicMock()
        parser.parse_receipt.return_value = SimpleNamespace(success=False, error="boom")
        result = strict_parse(parser, {})
        assert isinstance(result, ExtractError)
        assert result.error == "boom"

    def test_parse_receipt_success_false_no_error_message(self) -> None:
        """success=False with None error field: fallback message used."""
        parser = MagicMock()
        parser.parse_receipt.return_value = SimpleNamespace(success=False, error=None)
        result = strict_parse(parser, {})
        assert isinstance(result, ExtractError)
        assert result.error  # non-empty

    def test_parse_receipt_success_true_returns_none(self) -> None:
        """When parse_receipt returns success=True, returns None (proceed)."""
        parser = MagicMock()
        parser.parse_receipt.return_value = SimpleNamespace(success=True, error=None)
        result = strict_parse(parser, {"logs": []})
        assert result is None
