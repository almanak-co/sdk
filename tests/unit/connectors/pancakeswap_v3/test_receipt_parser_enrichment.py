"""Tests for PancakeSwap V3 receipt parser LP enrichment methods.

Validates that SUPPORTED_EXTRACTIONS is declared and all LP extraction
methods work correctly with realistic receipt data.
"""

from __future__ import annotations

import pytest

from almanak.framework.connectors.pancakeswap_v3.receipt_parser import (
    EVENT_TOPICS,
    POSITION_MANAGER_ADDRESSES,
    ZERO_ADDRESS_PADDED,
    PancakeSwapV3ReceiptParser,
)


# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------

MINT_TOPIC = EVENT_TOPICS["Mint"].lower()
TRANSFER_TOPIC = EVENT_TOPICS["Transfer"].lower()
COLLECT_TOPIC = EVENT_TOPICS["Collect"].lower()
BURN_TOPIC = EVENT_TOPICS["Burn"].lower()
POSITION_MANAGER = POSITION_MANAGER_ADDRESSES["bsc"].lower()


def _hex_pad(value: int, signed: bool = False) -> str:
    """Pad an integer to a 32-byte hex word."""
    if signed and value < 0:
        value = (1 << 256) + value
    return f"0x{value:064x}"


def _make_mint_receipt(
    tick_lower: int = -887220,
    tick_upper: int = 887220,
    liquidity: int = 123456789,
    amount0: int = 1000000,
    amount1: int = 2000000,
    sender: str = "0x" + "ab" * 20,
    token_id: int = 42,
) -> dict:
    """Create a realistic LP_OPEN receipt with Transfer + Mint events."""
    # Mint data: sender (address, 32 bytes) + amount (uint128, 32 bytes) + amount0 + amount1
    sender_padded = sender.replace("0x", "").lower().zfill(64)
    liquidity_hex = f"{liquidity:064x}"
    amount0_hex = f"{amount0:064x}"
    amount1_hex = f"{amount1:064x}"
    mint_data = "0x" + sender_padded + liquidity_hex + amount0_hex + amount1_hex

    # ERC-721 Transfer: from zero address (mint) with tokenId
    transfer_log = {
        "address": POSITION_MANAGER,
        "topics": [
            TRANSFER_TOPIC,
            ZERO_ADDRESS_PADDED,  # from = zero (mint)
            _hex_pad(int(sender.replace("0x", ""), 16)),  # to
            _hex_pad(token_id),  # tokenId
        ],
        "data": "0x",
    }

    # Pool address for Mint event
    pool_address = "0x" + "cc" * 20

    # Mint event: tickLower and tickUpper are indexed (topics[2], topics[3])
    mint_log = {
        "address": pool_address,
        "topics": [
            MINT_TOPIC,
            _hex_pad(int(sender.replace("0x", ""), 16)),  # owner (indexed)
            _hex_pad(tick_lower, signed=True),  # tickLower (indexed int24)
            _hex_pad(tick_upper, signed=True),  # tickUpper (indexed int24)
        ],
        "data": mint_data,
    }

    return {
        "transactionHash": "0x" + "ff" * 32,
        "blockNumber": 100,
        "status": 1,
        "logs": [transfer_log, mint_log],
    }


def _make_close_receipt(
    amount0_collected: int = 5000,
    amount1_collected: int = 10000,
    liquidity_removed: int = 123456789,
    tick_lower: int = -887220,
    tick_upper: int = 887220,
) -> dict:
    """Create a realistic LP_CLOSE receipt with Burn + Collect events."""
    # Burn data: amount (uint128) + amount0 (uint256) + amount1 (uint256)
    burn_data = "0x" + f"{liquidity_removed:064x}" + f"{amount0_collected:064x}" + f"{amount1_collected:064x}"

    burn_log = {
        "address": "0x" + "cc" * 20,
        "topics": [
            BURN_TOPIC,
            _hex_pad(int("ab" * 20, 16)),  # owner
            _hex_pad(tick_lower, signed=True),  # tickLower
            _hex_pad(tick_upper, signed=True),  # tickUpper
        ],
        "data": burn_data,
    }

    # Collect data: recipient (address) + amount0 (uint128) + amount1 (uint128)
    recipient = "ab" * 20
    collect_data = "0x" + recipient.zfill(64) + f"{amount0_collected:064x}" + f"{amount1_collected:064x}"

    collect_log = {
        "address": "0x" + "cc" * 20,
        "topics": [
            COLLECT_TOPIC,
            _hex_pad(int("ab" * 20, 16)),  # owner
            _hex_pad(tick_lower, signed=True),  # tickLower
            _hex_pad(tick_upper, signed=True),  # tickUpper
        ],
        "data": collect_data,
    }

    return {
        "transactionHash": "0x" + "ee" * 32,
        "blockNumber": 200,
        "status": 1,
        "logs": [burn_log, collect_log],
    }


# ---------------------------------------------------------------------------
# SUPPORTED_EXTRACTIONS declaration tests
# ---------------------------------------------------------------------------


class TestSupportedExtractions:
    def test_declares_supported_extractions(self):
        """Parser must declare SUPPORTED_EXTRACTIONS to avoid ResultEnricher warnings."""
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        assert hasattr(parser, "SUPPORTED_EXTRACTIONS")
        assert isinstance(parser.SUPPORTED_EXTRACTIONS, frozenset)

    def test_supports_position_id(self):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        assert "position_id" in parser.SUPPORTED_EXTRACTIONS

    def test_supports_swap_amounts(self):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        assert "swap_amounts" in parser.SUPPORTED_EXTRACTIONS

    def test_supports_tick_lower(self):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        assert "tick_lower" in parser.SUPPORTED_EXTRACTIONS

    def test_supports_tick_upper(self):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        assert "tick_upper" in parser.SUPPORTED_EXTRACTIONS

    def test_supports_liquidity(self):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        assert "liquidity" in parser.SUPPORTED_EXTRACTIONS

    def test_supports_lp_close_data(self):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        assert "lp_close_data" in parser.SUPPORTED_EXTRACTIONS

    def test_matches_uniswap_v3_support(self):
        """PancakeSwap V3 should support the same extractions as Uniswap V3."""
        expected = {"position_id", "swap_amounts", "tick_lower", "tick_upper", "liquidity", "lp_close_data"}
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        assert parser.SUPPORTED_EXTRACTIONS == expected


# ---------------------------------------------------------------------------
# extract_position_id tests
# ---------------------------------------------------------------------------


class TestExtractPositionId:
    def test_extracts_token_id_from_mint(self):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        receipt = _make_mint_receipt(token_id=42)
        assert parser.extract_position_id(receipt) == 42

    def test_extracts_large_token_id(self):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        receipt = _make_mint_receipt(token_id=999999)
        assert parser.extract_position_id(receipt) == 999999

    def test_returns_none_for_empty_logs(self):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        assert parser.extract_position_id({"logs": []}) is None

    def test_returns_none_for_no_logs(self):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        assert parser.extract_position_id({}) is None

    def test_ignores_non_mint_transfers(self):
        """Should only match Transfer events from zero address (mints)."""
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        receipt = {
            "logs": [{
                "address": POSITION_MANAGER,
                "topics": [
                    TRANSFER_TOPIC,
                    _hex_pad(123),  # from = non-zero (not a mint)
                    _hex_pad(456),  # to
                    _hex_pad(789),  # tokenId
                ],
                "data": "0x",
            }]
        }
        assert parser.extract_position_id(receipt) is None

    def test_different_chains(self):
        """Position manager address varies by chain."""
        for chain in ["bsc", "ethereum", "arbitrum", "base"]:
            parser = PancakeSwapV3ReceiptParser(chain=chain)
            receipt = _make_mint_receipt(token_id=100)
            assert parser.extract_position_id(receipt) == 100


# ---------------------------------------------------------------------------
# extract_tick_lower / extract_tick_upper tests
# ---------------------------------------------------------------------------


class TestExtractTicks:
    def test_extracts_tick_lower(self):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        receipt = _make_mint_receipt(tick_lower=-887220)
        result = parser.extract_tick_lower(receipt)
        assert result == -887220

    def test_extracts_tick_upper(self):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        receipt = _make_mint_receipt(tick_upper=887220)
        result = parser.extract_tick_upper(receipt)
        assert result == 887220

    def test_negative_ticks(self):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        receipt = _make_mint_receipt(tick_lower=-100, tick_upper=-50)
        assert parser.extract_tick_lower(receipt) == -100
        assert parser.extract_tick_upper(receipt) == -50

    def test_zero_ticks(self):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        receipt = _make_mint_receipt(tick_lower=0, tick_upper=0)
        assert parser.extract_tick_lower(receipt) == 0
        assert parser.extract_tick_upper(receipt) == 0

    def test_returns_none_for_empty_logs(self):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        assert parser.extract_tick_lower({"logs": []}) is None
        assert parser.extract_tick_upper({"logs": []}) is None


# ---------------------------------------------------------------------------
# extract_liquidity tests
# ---------------------------------------------------------------------------


class TestExtractLiquidity:
    def test_extracts_liquidity(self):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        receipt = _make_mint_receipt(liquidity=123456789)
        result = parser.extract_liquidity(receipt)
        assert result == 123456789

    def test_large_liquidity(self):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        big_liq = 2**127 - 1  # max uint128
        receipt = _make_mint_receipt(liquidity=big_liq)
        result = parser.extract_liquidity(receipt)
        assert result == big_liq

    def test_returns_none_for_empty_logs(self):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        assert parser.extract_liquidity({"logs": []}) is None

    def test_returns_none_for_no_mint_event(self):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        # Receipt with only a transfer event, no Mint
        receipt = {
            "logs": [{
                "address": POSITION_MANAGER,
                "topics": [TRANSFER_TOPIC, ZERO_ADDRESS_PADDED, _hex_pad(1), _hex_pad(42)],
                "data": "0x",
            }]
        }
        assert parser.extract_liquidity(receipt) is None


# ---------------------------------------------------------------------------
# extract_lp_close_data tests
# ---------------------------------------------------------------------------


class TestExtractLPCloseData:
    def test_extracts_close_data(self):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        receipt = _make_close_receipt(
            amount0_collected=5000,
            amount1_collected=10000,
            liquidity_removed=123456789,
        )
        result = parser.extract_lp_close_data(receipt)
        assert result is not None
        assert result.amount0_collected == 5000
        assert result.amount1_collected == 10000
        assert result.liquidity_removed == 123456789

    def test_returns_none_for_empty_logs(self):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        assert parser.extract_lp_close_data({"logs": []}) is None

    def test_returns_none_when_no_amounts(self):
        """If both amounts are 0, should return None."""
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        receipt = _make_close_receipt(amount0_collected=0, amount1_collected=0)
        assert parser.extract_lp_close_data(receipt) is None


# ---------------------------------------------------------------------------
# Edge case / robustness tests
# ---------------------------------------------------------------------------


class TestEdgeCases:
    def test_handles_missing_data_field(self):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        receipt = {
            "logs": [{
                "address": "0x" + "cc" * 20,
                "topics": [MINT_TOPIC, _hex_pad(1), _hex_pad(-100, signed=True), _hex_pad(100, signed=True)],
                # Missing 'data' field
            }]
        }
        # Should not crash
        assert parser.extract_liquidity(receipt) is None

    def test_handles_empty_data(self):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        receipt = {
            "logs": [{
                "address": "0x" + "cc" * 20,
                "topics": [MINT_TOPIC, _hex_pad(1), _hex_pad(-100, signed=True), _hex_pad(100, signed=True)],
                "data": "0x",
            }]
        }
        assert parser.extract_liquidity(receipt) is None

    def test_handles_too_few_topics(self):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        receipt = {
            "logs": [{
                "address": "0x" + "cc" * 20,
                "topics": [MINT_TOPIC],  # Only 1 topic, need 4
                "data": "0x" + "00" * 128,
            }]
        }
        assert parser.extract_tick_lower(receipt) is None
        assert parser.extract_tick_upper(receipt) is None
        assert parser.extract_liquidity(receipt) is None
