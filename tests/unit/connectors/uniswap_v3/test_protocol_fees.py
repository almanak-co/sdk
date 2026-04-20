"""Tests for Uniswap V3 ``extract_protocol_fees`` (VIB-3204).

Verifies (post-audit contract — CodeRabbit + pr-auditor + Codex on #1602):
- Returns None when no Swap event is present on the receipt.
- Returns None when ``fee_tier_bps`` is missing (cannot compute).
- Returns None when a swap IS present but USD conversion is unavailable
  at this layer (the parser has no price oracle). Returning a
  ``ProtocolFees(total_usd=Decimal(0))`` would falsely advertise
  "measured zero fee" on every charged swap — see audit-pr-1602.md.
- Returns None for zero / negative ``fee_tier_bps`` inputs.

A future iteration with price-oracle access will emit a populated
``ProtocolFees``.
"""

from __future__ import annotations

from almanak.framework.connectors.uniswap_v3.receipt_parser import (
    EVENT_TOPICS,
    UniswapV3ReceiptParser,
)


SWAP_TOPIC = EVENT_TOPICS["Swap"]
TRANSFER_TOPIC = EVENT_TOPICS["Transfer"]


def _swap_receipt() -> dict:
    """Build a minimal receipt with a Swap event + a Transfer to the wallet
    so parse_receipt() yields a swap_result (required for fee extraction)."""
    wallet = "0x" + "11" * 20
    pool = "0x" + "22" * 20
    token0 = "0x" + "33" * 20
    token1 = "0x" + "44" * 20

    # Swap event: topics = [topic0, sender, recipient]; data = [amount0, amount1,
    # sqrtPriceX96, liquidity, tick]
    # amount0 = +1000 (input), amount1 = -950 (output) — token0 -> token1 swap
    #
    # CodeRabbit audit fix: the prior encoding built a 127-hex-char word
    # for amount1 which isn't a valid 32-byte ABI slot — if the parser
    # rejected it on decode-fail the test would pass for the wrong
    # reason. Use proper two's-complement int256 encoding.
    amount0_word = f"{1000:064x}"
    amount1_word = f"{((1 << 256) - 950):064x}"  # -950 in int256 two's complement
    swap_data = (
        "0x"
        + amount0_word
        + amount1_word
        + ("0" * 64)  # sqrtPriceX96
        + ("0" * 64)  # liquidity
        + ("0" * 64)  # tick
    )

    return {
        "status": 1,
        "from": wallet,
        "logs": [
            # Transfer token0 from wallet -> pool (input)
            {
                "address": token0,
                "topics": [
                    TRANSFER_TOPIC,
                    "0x" + wallet[2:].zfill(64),
                    "0x" + pool[2:].zfill(64),
                ],
                "data": "0x" + hex(1000)[2:].zfill(64),
            },
            # Transfer token1 from pool -> wallet (output)
            {
                "address": token1,
                "topics": [
                    TRANSFER_TOPIC,
                    "0x" + pool[2:].zfill(64),
                    "0x" + wallet[2:].zfill(64),
                ],
                "data": "0x" + hex(950)[2:].zfill(64),
            },
            # Swap event
            {
                "address": pool,
                "topics": [
                    SWAP_TOPIC,
                    "0x" + wallet[2:].zfill(64),
                    "0x" + wallet[2:].zfill(64),
                ],
                "data": swap_data,
            },
        ],
    }


class TestUniswapV3ProtocolFees:
    def test_returns_none_on_empty_receipt(self):
        parser = UniswapV3ReceiptParser(chain="arbitrum")
        assert parser.extract_protocol_fees({"logs": []}) is None

    def test_returns_none_when_fee_tier_missing(self):
        """Without the compile-time fee tier the parser cannot compute the
        fee and MUST return None rather than guessing."""
        parser = UniswapV3ReceiptParser(chain="arbitrum")
        assert parser.extract_protocol_fees(_swap_receipt()) is None

    def test_returns_none_when_no_swap_event(self):
        parser = UniswapV3ReceiptParser(chain="arbitrum")
        receipt = {"logs": [], "from": "0x" + "11" * 20, "status": 1}
        assert parser.extract_protocol_fees(receipt, fee_tier_bps=500) is None

    def test_returns_none_when_swap_present_but_no_price_oracle(self):
        """VIB-3204 audit fix (Codex P1, pr-auditor Blocker #2): a charged
        Uniswap V3 swap must NOT be reported as zero-fee just because the
        parser lacks a USD price source. Returning
        ``ProtocolFees(total_usd=0)`` was misleading and caused PnL
        attribution to under-attribute swap costs. The parser now returns
        ``None`` (unknown) when a swap is present but USD can't be
        computed at this layer.
        """
        parser = UniswapV3ReceiptParser(chain="arbitrum")
        receipt = _swap_receipt()
        result = parser.extract_protocol_fees(receipt, fee_tier_bps=500)
        assert result is None

    def test_zero_or_negative_fee_tier_returns_none(self):
        """Defensive: callers passing 0 or negative should not yield
        misleading zero fees."""
        parser = UniswapV3ReceiptParser(chain="arbitrum")
        receipt = _swap_receipt()
        assert parser.extract_protocol_fees(receipt, fee_tier_bps=0) is None
        assert parser.extract_protocol_fees(receipt, fee_tier_bps=-1) is None
