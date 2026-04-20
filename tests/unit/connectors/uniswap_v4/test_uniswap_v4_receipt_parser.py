"""Tests for Uniswap V4 Receipt Parser."""

from __future__ import annotations

from decimal import Decimal

import pytest

from almanak.framework.connectors.uniswap_v4.receipt_parser import (
    EVENT_TOPICS,
    ParseResult,
    SwapEventData,
    UniswapV4EventType,
    UniswapV4ReceiptParser,
)


# =============================================================================
# Helper: build mock receipts
# =============================================================================


def _encode_int128(value: int) -> str:
    """Encode int128 as 32-byte hex (two's complement for negative)."""
    if value < 0:
        value = (1 << 256) + value
    return hex(value)[2:].zfill(64)


def _encode_uint(value: int) -> str:
    """Encode uint256 as 32-byte hex."""
    return hex(value)[2:].zfill(64)


def _build_swap_log(
    amount0: int,
    amount1: int,
    sqrt_price_x96: int = 79228162514264337593543950336,
    liquidity: int = 10**18,
    tick: int = 0,
    fee: int = 3000,
    pool_id: str = "0x" + "ab" * 32,
    sender: str = "0x" + "00" * 12 + "1234567890123456789012345678901234567890",
) -> dict:
    """Build a mock V4 Swap event log."""
    data = (
        "0x"
        + _encode_int128(amount0)
        + _encode_int128(amount1)
        + _encode_uint(sqrt_price_x96)
        + _encode_uint(liquidity)
        + _encode_int128(tick)  # int24 but padded to 32 bytes
        + _encode_uint(fee)
    )
    return {
        "address": "0x000000000004444c5dc75cb358380d2e3de08a90",
        "topics": [
            EVENT_TOPICS["Swap"],
            pool_id,
            sender,
        ],
        "data": data,
    }


def _build_transfer_log(
    token: str,
    from_addr: str,
    to_addr: str,
    amount: int,
) -> dict:
    """Build a mock ERC-20 Transfer event log."""
    return {
        "address": token,
        "topics": [
            EVENT_TOPICS["Transfer"],
            "0x" + from_addr.replace("0x", "").zfill(64),
            "0x" + to_addr.replace("0x", "").zfill(64),
        ],
        "data": "0x" + _encode_uint(amount),
    }


# =============================================================================
# Event topics tests
# =============================================================================


class TestEventTopics:
    def test_swap_topic_exists(self):
        assert "Swap" in EVENT_TOPICS
        assert EVENT_TOPICS["Swap"].startswith("0x")

    def test_transfer_topic(self):
        assert EVENT_TOPICS["Transfer"] == "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"


class TestEventTypes:
    def test_swap_type(self):
        assert UniswapV4EventType.SWAP.value == "SWAP"

    def test_modify_liquidity_type(self):
        assert UniswapV4EventType.MODIFY_LIQUIDITY.value == "MODIFY_LIQUIDITY"


# =============================================================================
# Parser initialization tests
# =============================================================================


class TestParserInit:
    def test_init_default(self):
        parser = UniswapV4ReceiptParser()
        assert parser.chain == "ethereum"

    def test_init_with_chain(self):
        parser = UniswapV4ReceiptParser(chain="arbitrum")
        assert parser.chain == "arbitrum"
        assert parser.pool_manager == "0x000000000004444c5dc75cb358380d2e3de08a90"


# =============================================================================
# Receipt parsing tests
# =============================================================================


class TestParseReceipt:
    def test_parse_swap_receipt(self):
        parser = UniswapV4ReceiptParser(chain="arbitrum")

        # token0 in (+1000 USDC), token1 out (-0.5 WETH)
        swap_log = _build_swap_log(
            amount0=1000 * 10**6,
            amount1=-(5 * 10**17),
        )
        receipt = {"logs": [swap_log]}

        result = parser.parse_receipt(receipt)

        assert len(result.swap_events) == 1
        assert result.swap_result is not None
        assert result.swap_result.amount_in == 1000 * 10**6
        assert result.swap_result.amount_out == 5 * 10**17

    def test_parse_reverse_direction(self):
        """Test swap where token1 is input."""
        parser = UniswapV4ReceiptParser(chain="arbitrum")

        swap_log = _build_swap_log(
            amount0=-(1000 * 10**6),
            amount1=5 * 10**17,
        )
        receipt = {"logs": [swap_log]}

        result = parser.parse_receipt(receipt)

        assert result.swap_result is not None
        assert result.swap_result.amount_in == 5 * 10**17
        assert result.swap_result.amount_out == 1000 * 10**6

    def test_parse_with_transfer_events(self):
        parser = UniswapV4ReceiptParser(chain="arbitrum")

        swap_log = _build_swap_log(amount0=1000 * 10**6, amount1=-(5 * 10**17))
        transfer_log = _build_transfer_log(
            token="0xaf88d065e77c8cc2239327c5edb3a432268e5831",
            from_addr="0x1111111111111111111111111111111111111111",
            to_addr="0x000000000004444c5dc75cb358380d2e3de08a90",
            amount=1000 * 10**6,
        )

        receipt = {"logs": [swap_log, transfer_log]}
        result = parser.parse_receipt(receipt)

        assert len(result.swap_events) == 1
        assert len(result.transfer_events) == 1

    def test_parse_empty_receipt(self):
        parser = UniswapV4ReceiptParser(chain="arbitrum")
        result = parser.parse_receipt({"logs": []})

        assert len(result.swap_events) == 0
        assert result.swap_result is None

    def test_slippage_calculation(self):
        parser = UniswapV4ReceiptParser(chain="arbitrum")

        swap_log = _build_swap_log(
            amount0=1000 * 10**6,
            amount1=-(5 * 10**17),
        )
        receipt = {"logs": [swap_log]}

        # Quote was 0.51 WETH but got 0.5 WETH
        result = parser.parse_receipt(receipt, quoted_amount_out=51 * 10**16)

        assert result.swap_result is not None
        assert result.swap_result.slippage_bps is not None
        assert result.swap_result.slippage_bps > 0

    def test_effective_price_without_transfers(self):
        """Without Transfer events, token decimals can't be resolved so effective_price is None."""
        parser = UniswapV4ReceiptParser(chain="arbitrum")

        swap_log = _build_swap_log(
            amount0=2000 * 10**6,
            amount1=-(1 * 10**18),
        )
        receipt = {"logs": [swap_log]}

        result = parser.parse_receipt(receipt)

        assert result.swap_result is not None
        # effective_price requires Transfer events to identify token addresses for decimal resolution
        assert result.swap_result.effective_price is None
        # Raw amounts are still populated
        assert result.swap_result.amount_in == 2000 * 10**6
        assert result.swap_result.amount_out == 1 * 10**18


class TestExtractSwapAmounts:
    def test_extract_swap_amounts(self):
        parser = UniswapV4ReceiptParser(chain="arbitrum")

        swap_log = _build_swap_log(
            amount0=1000 * 10**6,
            amount1=-(5 * 10**17),
        )
        receipt = {"logs": [swap_log]}

        amounts = parser.extract_swap_amounts(receipt)

        assert amounts is not None
        assert amounts.amount_in == 1000 * 10**6
        assert amounts.amount_out == 5 * 10**17
        # Without Transfer events, decimals can't be resolved so effective_price falls back to 0
        assert amounts.effective_price == Decimal(0)

    def test_extract_no_swap(self):
        parser = UniswapV4ReceiptParser(chain="arbitrum")
        amounts = parser.extract_swap_amounts({"logs": []})
        assert amounts is None
