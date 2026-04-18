"""Regression test for hex-encoded logIndex handling in AsterPerpsReceiptParser.

Web3.py returns receipts with ``logIndex`` as int, but raw JSON-RPC responses
and many test fixtures return it hex-encoded (e.g. ``"0x0"``). Naive
``int("0x0")`` raises ``ValueError`` which would be silently swallowed by the
outer try/except in parse_receipt, dropping the entire event.

See CodeRabbit review on PR #1547 (VIB-3044).
"""

from __future__ import annotations

import pytest

from almanak.framework.connectors.aster_perps.receipt_parser import (
    AsterPerpsReceiptParser,
)
from almanak.framework.connectors.aster_perps.sdk import (
    EVENT_MARKET_PENDING_TRADE,
)


@pytest.mark.parametrize(
    "raw,expected",
    [
        (0, 0),
        (1, 1),
        (42, 42),
        ("0", 0),
        ("1", 1),
        ("42", 42),
        ("0x0", 0),
        ("0x1", 1),
        ("0x2a", 42),
        ("0X2A", 42),
        (None, 0),
        ("", 0),
    ],
)
def test_decode_log_index_accepts_int_and_hex(raw, expected):
    assert AsterPerpsReceiptParser._decode_log_index(raw) == expected


def test_parse_receipt_preserves_event_when_logindex_is_hex_string():
    """Hex-encoded logIndex MUST NOT cause the event to be silently dropped."""
    parser = AsterPerpsReceiptParser(chain="bsc")
    # Minimal receipt mimicking a MarketPendingTrade event with hex logIndex.
    # 9 data words (pairBase, isLong, tokenIn, amountIn, qty, price, stopLoss,
    # takeProfit, broker) — all zeros except broker=0 which is fine.
    data = "0x" + ("00" * 32) * 9
    receipt = {
        "logs": [
            {
                "address": "0x1b81d678ffb9c0263b24a97847620c99d213eb14",
                "topics": [
                    EVENT_MARKET_PENDING_TRADE,
                    "0x000000000000000000000000abababababababababababababababababababab",  # user
                    "0x" + "cd" * 32,  # tradeHash
                ],
                "data": data,
                "logIndex": "0x5",  # hex-encoded — this is the regression case
            }
        ]
    }
    parsed = parser.parse_receipt(receipt)
    assert len(parsed.market_pending_trades) == 1
    assert parsed.market_pending_trades[0].log_index == 5
