"""Characterization tests for AsterPerpsReceiptParser.

These tests pin CURRENT behavior. They are the regression contract for any future
refactor of the parser. Do not change parser source in this PR.

Also covers the pancakeswap_perps shim, which re-exports from aster_perps.
"""

from decimal import Decimal

import pytest

from almanak.connectors.aster_perps.receipt_parser import AsterPerpsReceiptParser
from almanak.connectors.aster_perps.sdk import (
    EVENT_CLOSE_TRADE_RECEIVED,
    EVENT_CLOSE_TRADE_SUCCESSFUL,
    EVENT_MARKET_PENDING_TRADE,
    EVENT_OPEN_MARKET_TRADE,
    EVENT_PENDING_TRADE_REFUND,
    PRICE_DECIMALS,
    QTY_DECIMALS,
)


# ---------------------------------------------------------------------------
# Hex helpers
# ---------------------------------------------------------------------------


def word(v: int) -> str:
    """One 32-byte ABI word as 64 hex chars (no 0x)."""
    return f"{v:064x}"


def signed_word(v: int) -> str:
    """Two's-complement int256 word."""
    return f"{v & ((1 << 256) - 1):064x}"


def addr_word(a: str) -> str:
    """Address left-padded to a 32-byte word (no 0x)."""
    return a.lower().replace("0x", "").zfill(64)


def addr_topic(a: str) -> str:
    """Address as an indexed topic (0x-prefixed 32-byte word)."""
    return "0x" + a.lower().replace("0x", "").zfill(64)


def bytes32_topic(h: str) -> str:
    """bytes32 value as 0x-prefixed 64-char hex topic."""
    return "0x" + h.lower().replace("0x", "").zfill(64)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

USER = "0x1111111111111111111111111111111111111111"
TRADE_HASH = "0xdeadbeef" + "00" * 28
PAIR_BASE = "0x2222222222222222222222222222222222222222"
TOKEN_IN = "0x3333333333333333333333333333333333333333"
TOKEN_OUT = "0x4444444444444444444444444444444444444444"
TX_HASH = "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def parser():
    return AsterPerpsReceiptParser(chain="bsc")


def make_receipt(logs: list) -> dict:
    return {"transactionHash": TX_HASH, "blockNumber": 12345, "logs": logs}


def make_market_pending_trade_log() -> dict:
    """MarketPendingTrade with 9 data words."""
    data = (
        addr_word(PAIR_BASE)    # word 0: pairBase
        + word(1)               # word 1: isLong = True
        + addr_word(TOKEN_IN)   # word 2: tokenIn
        + word(500_000_000)     # word 3: amountIn (500 USDC at 6 decimals)
        + word(25_000_000_000)  # word 4: qty (2.5 at QTY_DECIMALS=10)
        + word(50000 * 10**8)   # word 5: price (50000 USD at PRICE_DECIMALS=8)
        + word(0)               # word 6: stopLoss
        + word(0)               # word 7: takeProfit
        + word(2)               # word 8: broker
    )
    return {
        "address": "0xabcd",
        "topics": [EVENT_MARKET_PENDING_TRADE, addr_topic(USER), bytes32_topic(TRADE_HASH)],
        "data": "0x" + data,
        "logIndex": 0,
    }


def make_open_market_trade_log() -> dict:
    """OpenMarketTrade with 17 data words (fields at specific word indices)."""
    words = ["0" * 64] * 17
    words[0] = addr_word(USER)         # word 0: user
    # word 1: userOpenTradeIndex (0)
    words[2] = word(50000 * 10**8)     # word 2: entryPrice
    words[3] = addr_word(PAIR_BASE)    # word 3: pairBase
    words[4] = addr_word(TOKEN_IN)     # word 4: tokenIn
    words[5] = word(500_000_000)       # word 5: margin
    # words 6,7,8: stopLoss, takeProfit, broker (zeros)
    words[9] = word(1)                 # word 9: isLong = True
    words[10] = word(100_000)          # word 10: openFee
    # word 11: longAccFundingFeePerShare (0)
    words[12] = word(50_000)           # word 12: executionFee
    words[13] = word(1_700_000_000)    # word 13: timestamp
    words[14] = word(25_000_000_000)   # word 14: qty (2.5 base tokens)
    # words 15,16: holdingFeeRate, openBlock (zeros)
    return {
        "address": "0xabcd",
        "topics": [EVENT_OPEN_MARKET_TRADE, addr_topic(USER), bytes32_topic(TRADE_HASH)],
        "data": "0x" + "".join(words),
        "logIndex": 1,
    }


def make_close_trade_successful_log(
    close_price: int = 49000 * 10**8,
    funding_fee: int = -7,
    close_fee: int = 50_000,
    pnl: int = -25,
    holding_fee: int = 10_000,
) -> dict:
    """CloseTradeSuccessful with 5 data words (includes signed int96 values)."""
    data = (
        word(close_price)
        + signed_word(funding_fee)
        + word(close_fee)
        + signed_word(pnl)
        + word(holding_fee)
    )
    return {
        "address": "0xabcd",
        "topics": [EVENT_CLOSE_TRADE_SUCCESSFUL, addr_topic(USER), bytes32_topic(TRADE_HASH)],
        "data": "0x" + data,
        "logIndex": 2,
    }


def make_close_trade_received_log(amount: int, token: str = TOKEN_OUT, log_index: int = 3) -> dict:
    """CloseTradeReceived with 4 topics + 1 data word."""
    return {
        "address": "0xabcd",
        "topics": [
            EVENT_CLOSE_TRADE_RECEIVED,
            addr_topic(USER),
            bytes32_topic(TRADE_HASH),
            addr_topic(token),
        ],
        "data": "0x" + word(amount),
        "logIndex": log_index,
    }


def make_pending_trade_refund_log(refund_code: int = 3) -> dict:
    """PendingTradeRefund with 3 topics + 1 data word."""
    return {
        "address": "0xabcd",
        "topics": [EVENT_PENDING_TRADE_REFUND, addr_topic(USER), bytes32_topic(TRADE_HASH)],
        "data": "0x" + word(refund_code),
        "logIndex": 0,
    }


# ---------------------------------------------------------------------------
# MarketPendingTrade
# ---------------------------------------------------------------------------


class TestMarketPendingTrade:
    def test_decodes_all_fields(self, parser):
        receipt = make_receipt([make_market_pending_trade_log()])
        parsed = parser.parse_receipt(receipt)

        assert len(parsed.market_pending_trades) == 1
        ev = parsed.market_pending_trades[0]
        assert ev.user.lower() == USER.lower()
        assert ev.trade_hash == TRADE_HASH.lower()
        assert ev.is_long is True
        assert ev.amount_in == 500_000_000
        assert ev.qty == 25_000_000_000
        assert ev.price == 50000 * 10**8
        assert ev.broker == 2


# ---------------------------------------------------------------------------
# OpenMarketTrade
# ---------------------------------------------------------------------------


class TestOpenMarketTrade:
    def test_decodes_correct_word_indices(self, parser):
        receipt = make_receipt([make_open_market_trade_log()])
        parsed = parser.parse_receipt(receipt)

        assert len(parsed.open_market_trades) == 1
        ev = parsed.open_market_trades[0]
        assert ev.entry_price == 50000 * 10**8
        assert ev.margin == 500_000_000
        assert ev.is_long is True
        assert ev.qty == 25_000_000_000


# ---------------------------------------------------------------------------
# CloseTradeSuccessful (signed pnl / funding_fee)
# ---------------------------------------------------------------------------


class TestCloseTradeSuccessful:
    def test_decodes_negative_pnl_and_funding_fee(self, parser):
        receipt = make_receipt([make_close_trade_successful_log(funding_fee=-7, pnl=-25)])
        parsed = parser.parse_receipt(receipt)

        assert len(parsed.close_trade_successful) == 1
        ev = parsed.close_trade_successful[0]
        assert ev.pnl == -25
        assert ev.funding_fee == -7
        assert ev.close_fee == 50_000
        assert ev.holding_fee == 10_000


# ---------------------------------------------------------------------------
# CloseTradeReceived
# ---------------------------------------------------------------------------


class TestCloseTradeReceived:
    def test_decodes_token_and_amount(self, parser):
        receipt = make_receipt([make_close_trade_received_log(amount=123_456_789)])
        parsed = parser.parse_receipt(receipt)

        assert len(parsed.close_trade_received) == 1
        ev = parsed.close_trade_received[0]
        assert ev.token.lower() == TOKEN_OUT.lower()
        assert ev.amount == 123_456_789


# ---------------------------------------------------------------------------
# PendingTradeRefund
# ---------------------------------------------------------------------------


class TestPendingTradeRefund:
    def test_decodes_refund_code(self, parser):
        receipt = make_receipt([make_pending_trade_refund_log(refund_code=5)])
        parsed = parser.parse_receipt(receipt)

        assert len(parsed.pending_trade_refunds) == 1
        ev = parsed.pending_trade_refunds[0]
        assert ev.refund_code == 5
        assert ev.trade_hash == TRADE_HASH.lower()


# ---------------------------------------------------------------------------
# Empty receipts
# ---------------------------------------------------------------------------


class TestEmptyReceipt:
    def test_empty_dict_returns_empty_parsed_receipt(self, parser):
        parsed = parser.parse_receipt({})
        assert parsed.market_pending_trades == []
        assert parsed.open_market_trades == []
        assert parsed.pending_trade_refunds == []
        assert parsed.close_trade_successful == []
        assert parsed.close_trade_received == []

    def test_empty_logs_list_returns_empty_parsed_receipt(self, parser):
        parsed = parser.parse_receipt({"logs": []})
        assert parsed.open_market_trades == []
        assert parsed.market_pending_trades == []


# ---------------------------------------------------------------------------
# Truncated data is skipped gracefully
# ---------------------------------------------------------------------------


class TestTruncated:
    def test_truncated_market_pending_trade_8_words_skipped(self, parser):
        """MarketPendingTrade requires >= 9 data words (2 + 9*64 hex chars).
        With only 8 words the decoder returns None and no event is appended."""
        # 8 words = 512 hex chars (data without 0x prefix)
        data_8w = (
            addr_word(PAIR_BASE)
            + word(1)
            + addr_word(TOKEN_IN)
            + word(500_000_000)
            + word(25_000_000_000)
            + word(50000 * 10**8)
            + word(0)
            + word(0)
            # missing 9th word (broker)
        )
        log = {
            "address": "0xabcd",
            "topics": [EVENT_MARKET_PENDING_TRADE, addr_topic(USER), bytes32_topic(TRADE_HASH)],
            "data": "0x" + data_8w,
            "logIndex": 0,
        }
        parsed = parser.parse_receipt(make_receipt([log]))
        assert parsed.market_pending_trades == []


# ---------------------------------------------------------------------------
# Extraction method scaling
# ---------------------------------------------------------------------------


class TestExtractionScaling:
    def test_extract_position_id_from_open_market_trade(self, parser):
        """Prefers open_market_trades[0] over market_pending_trades[0]."""
        receipt = make_receipt([make_open_market_trade_log()])
        assert parser.extract_position_id(receipt) == TRADE_HASH.lower()

    def test_extract_position_id_from_market_pending_trade(self, parser):
        """Falls back to market_pending_trades[0] when no open trade."""
        receipt = make_receipt([make_market_pending_trade_log()])
        assert parser.extract_position_id(receipt) == TRADE_HASH.lower()

    def test_extract_size_delta_scales_by_qty_decimals(self, parser):
        """qty=25_000_000_000 / 10^QTY_DECIMALS = 2.5"""
        receipt = make_receipt([make_market_pending_trade_log()])
        assert parser.extract_size_delta(receipt) == Decimal("2.5")

    def test_extract_collateral_raw_margin_from_open_trade(self, parser):
        receipt = make_receipt([make_open_market_trade_log()])
        assert parser.extract_collateral(receipt) == Decimal("500000000")

    def test_extract_collateral_raw_amount_in_from_pending_trade(self, parser):
        receipt = make_receipt([make_market_pending_trade_log()])
        assert parser.extract_collateral(receipt) == Decimal("500000000")

    def test_extract_entry_price_scales_by_price_decimals(self, parser):
        """entry_price = 50000 * 10^8 / 10^PRICE_DECIMALS = 50000"""
        receipt = make_receipt([make_open_market_trade_log()])
        assert parser.extract_entry_price(receipt) == Decimal("50000")

    def test_extract_exit_price_scales_by_price_decimals(self, parser):
        """close_price = 49000 * 10^8 / 10^PRICE_DECIMALS = 49000"""
        receipt = make_receipt([make_close_trade_successful_log()])
        assert parser.extract_exit_price(receipt) == Decimal("49000")

    def test_extract_realized_pnl_raw_signed_decimal(self, parser):
        receipt = make_receipt([make_close_trade_successful_log(pnl=-25)])
        assert parser.extract_realized_pnl(receipt) == Decimal("-25")

    def test_extract_fees_paid_close_fee_plus_holding_fee(self, parser):
        """close_fee=50_000 + holding_fee=10_000 = 60_000"""
        receipt = make_receipt([make_close_trade_successful_log(close_fee=50_000, holding_fee=10_000)])
        assert parser.extract_fees_paid(receipt) == Decimal("60000")

    def test_extract_collateral_returned_sums_two_received_logs(self, parser):
        """Sum all CloseTradeReceived amounts across the receipt."""
        receipt = make_receipt([
            make_close_trade_received_log(amount=100_000_000, log_index=0),
            make_close_trade_received_log(amount=50_000_000, log_index=1),
        ])
        assert parser.extract_collateral_returned(receipt) == Decimal("150000000")

    def test_extract_protocol_fees_returns_none(self, parser):
        """VIB-3204 stub — always returns None."""
        receipt = make_receipt([make_close_trade_successful_log()])
        assert parser.extract_protocol_fees(receipt) is None

    def test_extract_funding_fee_usd_returns_none(self, parser):
        """VIB-3520 stub — always returns None."""
        receipt = make_receipt([make_close_trade_successful_log()])
        assert parser.extract_funding_fee_usd(receipt) is None
