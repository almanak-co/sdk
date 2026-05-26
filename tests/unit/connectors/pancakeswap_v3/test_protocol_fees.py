"""Tests for PancakeSwap V3 ``extract_protocol_fees`` (VIB-3204)."""

from __future__ import annotations

from almanak.connectors.pancakeswap_v3.receipt_parser import (
    EVENT_TOPICS,
    PancakeSwapV3ReceiptParser,
)


SWAP_TOPIC = EVENT_TOPICS["Swap"]


def _receipt_with_swap() -> dict:
    pool = "0x" + "aa" * 20
    wallet = "0x" + "bb" * 20
    # PancakeSwap V3 Swap event has more params than Uniswap V3 but the parser
    # only checks topic[0] to detect presence — a minimal swap-topic log is
    # enough for the fee extractor (which doesn't decode the data).
    return {
        "status": 1,
        "from": wallet,
        "logs": [
            {
                "address": pool,
                "topics": [
                    SWAP_TOPIC,
                    "0x" + wallet[2:].zfill(64),
                    "0x" + wallet[2:].zfill(64),
                ],
                "data": "0x" + "0" * 64 * 9,
            },
        ],
    }


class TestPancakeSwapV3ProtocolFees:
    def test_returns_none_on_empty_receipt(self):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        assert parser.extract_protocol_fees({"logs": []}) is None

    def test_returns_none_when_fee_tier_missing(self):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        assert parser.extract_protocol_fees(_receipt_with_swap()) is None

    def test_returns_none_when_no_swap_event(self):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        receipt = {"status": 1, "from": "0x" + "bb" * 20, "logs": []}
        assert parser.extract_protocol_fees(receipt, fee_tier_bps=500) is None

    def test_returns_none_when_swap_present_but_no_price_oracle(self):
        """VIB-3204 audit fix (Codex P1, pr-auditor Blocker #2): the parser
        has no price oracle at this layer so it cannot convert fee tier
        to USD. Returning ``ProtocolFees(total_usd=0)`` would falsely
        claim "$0 fee" on a charged swap — systematically under-attributing
        swap costs in PnL. Until a price oracle is plumbed through, the
        parser returns ``None`` (unknown) even when all inputs are valid.
        """
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        result = parser.extract_protocol_fees(_receipt_with_swap(), fee_tier_bps=500)
        assert result is None

    def test_returns_none_on_failed_tx_status(self):
        """A failed transaction (status=0) should not report fees even if
        a Swap event is present — the pool reverted."""
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        receipt = _receipt_with_swap()
        receipt["status"] = 0
        assert parser.extract_protocol_fees(receipt, fee_tier_bps=500) is None

    def test_zero_or_negative_fee_tier_returns_none(self):
        parser = PancakeSwapV3ReceiptParser(chain="bsc")
        receipt = _receipt_with_swap()
        assert parser.extract_protocol_fees(receipt, fee_tier_bps=0) is None
        assert parser.extract_protocol_fees(receipt, fee_tier_bps=-1) is None
