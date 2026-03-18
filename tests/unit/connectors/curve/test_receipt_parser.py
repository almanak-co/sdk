"""Tests for CurveReceiptParser.extract_swap_amounts() decimal handling.

VIB-441: Verifies that extract_swap_amounts() uses actual token decimals
from TokenResolver instead of hardcoding 18.
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.connectors.curve.receipt_parser import (
    EVENT_TOPICS,
    CurveReceiptParser,
)


def _make_topic(hex_str: str) -> str:
    """Ensure topic is lowercase 0x-prefixed."""
    return hex_str.lower()


def _pad_hex(value: int, signed: bool = False) -> str:
    """Encode an integer as a 32-byte hex word (no 0x prefix)."""
    if signed and value < 0:
        value = (1 << 256) + value
    return f"{value:064x}"


def _build_swap_receipt(
    wallet: str = "0xaabbccddee1122334455667788990011aabbccdd",
    pool: str = "0xbebc44782c7db0a1a60cb6fe97d0b483032ff1c7",
    token_in: str = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",  # USDC
    token_out: str = "0x6b175474e89094c44da98b954eedeac495271d0f",  # DAI
    sold_id: int = 1,
    bought_id: int = 0,
    tokens_sold: int = 100_000_000,  # 100 USDC (6 decimals)
    tokens_bought: int = 99_984_871_483_550_784_213,  # ~99.98 DAI (18 decimals)
) -> dict:
    """Build a synthetic Curve swap receipt with TokenExchange + Transfer events."""
    # TokenExchange event: buyer (indexed), sold_id, tokens_sold, bought_id, tokens_bought
    buyer_topic = "0x" + "00" * 12 + wallet[2:]
    exchange_data = "0x" + _pad_hex(sold_id, signed=True) + _pad_hex(tokens_sold) + _pad_hex(bought_id, signed=True) + _pad_hex(tokens_bought)

    # ERC-20 Transfer: from wallet to pool (token_in)
    transfer_topic = _make_topic(EVENT_TOPICS["Transfer"])
    wallet_topic = "0x" + "00" * 12 + wallet[2:]
    pool_topic = "0x" + "00" * 12 + pool[2:]
    transfer_in_data = "0x" + _pad_hex(tokens_sold)
    transfer_out_data = "0x" + _pad_hex(tokens_bought)

    return {
        "status": 1,
        "from": wallet,
        "transactionHash": "0x" + "ab" * 32,
        "blockNumber": 19_000_000,
        "gasUsed": 150_000,
        "logs": [
            # Transfer: wallet -> pool (token_in = USDC)
            {
                "address": token_in,
                "topics": [transfer_topic, wallet_topic, pool_topic],
                "data": transfer_in_data,
                "logIndex": 0,
            },
            # TokenExchange event from pool
            {
                "address": pool,
                "topics": [_make_topic(EVENT_TOPICS["TokenExchange"]), buyer_topic],
                "data": exchange_data,
                "logIndex": 1,
            },
            # Transfer: pool -> wallet (token_out = DAI)
            {
                "address": token_out,
                "topics": [transfer_topic, pool_topic, wallet_topic],
                "data": transfer_out_data,
                "logIndex": 2,
            },
        ],
    }


def _mock_resolver(decimals_map: dict[str, int]):
    """Create a mock token resolver that returns decimals from a map."""
    mock_resolver = MagicMock()

    def resolve(address, chain):
        addr = address.lower()
        if addr in decimals_map:
            token = MagicMock()
            token.decimals = decimals_map[addr]
            return token
        raise ValueError(f"Unknown token: {addr}")

    mock_resolver.resolve = resolve
    return mock_resolver


class TestExtractSwapAmountsDecimals:
    """Test that extract_swap_amounts uses actual token decimals."""

    USDC = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
    DAI = "0x6b175474e89094c44da98b954eedeac495271d0f"
    USDT = "0xdac17f958d2ee523a2206206994597c13d831ec7"
    WBTC = "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599"

    def test_usdc_to_dai_correct_decimals(self):
        """USDC (6 dec) -> DAI (18 dec) should give effective_price ~1.0."""
        receipt = _build_swap_receipt(
            token_in=self.USDC,
            token_out=self.DAI,
            tokens_sold=100_000_000,  # 100 USDC
            tokens_bought=99_984_871_483_550_784_213,  # ~99.98 DAI
        )
        resolver = _mock_resolver({
            self.USDC: 6,
            self.DAI: 18,
        })

        parser = CurveReceiptParser(chain="ethereum")
        with patch("almanak.framework.data.tokens.get_token_resolver", return_value=resolver):
            result = parser.extract_swap_amounts(receipt)

        assert result is not None
        assert result.amount_in == 100_000_000
        assert result.amount_out == 99_984_871_483_550_784_213
        assert result.amount_in_decimal == Decimal("100")
        # ~99.98 DAI
        assert Decimal("99") < result.amount_out_decimal < Decimal("100")
        # effective_price should be ~1.0 for stablecoin pair
        assert Decimal("0.9") < result.effective_price < Decimal("1.1")
        assert result.token_in == self.USDC
        assert result.token_out == self.DAI

    def test_dai_to_usdt_correct_decimals(self):
        """DAI (18 dec) -> USDT (6 dec) should also give ~1.0."""
        receipt = _build_swap_receipt(
            token_in=self.DAI,
            token_out=self.USDT,
            sold_id=0,
            bought_id=2,
            tokens_sold=500_000_000_000_000_000_000,  # 500 DAI
            tokens_bought=499_750_000,  # 499.75 USDT
        )
        resolver = _mock_resolver({
            self.DAI: 18,
            self.USDT: 6,
        })

        parser = CurveReceiptParser(chain="ethereum")
        with patch("almanak.framework.data.tokens.get_token_resolver", return_value=resolver):
            result = parser.extract_swap_amounts(receipt)

        assert result is not None
        assert result.amount_in_decimal == Decimal("500")
        assert result.amount_out_decimal == Decimal("499.75")
        assert Decimal("0.9") < result.effective_price < Decimal("1.1")

    def test_wbtc_8_decimals(self):
        """WBTC (8 dec) -> DAI (18 dec) should handle 8-decimal token."""
        receipt = _build_swap_receipt(
            token_in=self.WBTC,
            token_out=self.DAI,
            sold_id=0,
            bought_id=1,
            tokens_sold=100_000_000,  # 1 WBTC (8 decimals)
            tokens_bought=60_000_000_000_000_000_000_000,  # 60000 DAI
        )
        resolver = _mock_resolver({
            self.WBTC: 8,
            self.DAI: 18,
        })

        parser = CurveReceiptParser(chain="ethereum")
        with patch("almanak.framework.data.tokens.get_token_resolver", return_value=resolver):
            result = parser.extract_swap_amounts(receipt)

        assert result is not None
        assert result.amount_in_decimal == Decimal("1")
        assert result.amount_out_decimal == Decimal("60000")
        assert result.effective_price == Decimal("60000")

    def test_returns_none_when_resolver_unavailable(self):
        """Should return None (not wrong data) when resolver fails."""
        receipt = _build_swap_receipt()

        parser = CurveReceiptParser(chain="ethereum")
        with patch(
            "almanak.framework.data.tokens.get_token_resolver",
            side_effect=Exception("No resolver"),
        ):
            result = parser.extract_swap_amounts(receipt)

        assert result is None

    def test_returns_none_for_empty_receipt(self):
        """Should return None for receipt with no swap events."""
        parser = CurveReceiptParser(chain="ethereum")
        result = parser.extract_swap_amounts({"status": 1, "logs": [], "from": "0x1234"})
        assert result is None

    def test_18_to_18_still_works(self):
        """Both 18-decimal tokens should still produce correct results."""
        receipt = _build_swap_receipt(
            token_in=self.DAI,
            token_out="0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",  # WETH
            sold_id=0,
            bought_id=1,
            tokens_sold=2000_000_000_000_000_000_000,  # 2000 DAI
            tokens_bought=1_000_000_000_000_000_000,  # 1 WETH
        )
        weth = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
        resolver = _mock_resolver({
            self.DAI: 18,
            weth: 18,
        })

        parser = CurveReceiptParser(chain="ethereum")
        with patch("almanak.framework.data.tokens.get_token_resolver", return_value=resolver):
            result = parser.extract_swap_amounts(receipt)

        assert result is not None
        assert result.amount_in_decimal == Decimal("2000")
        assert result.amount_out_decimal == Decimal("1")
        assert result.effective_price == Decimal("0.0005")


def _build_cryptoswap_receipt(
    wallet: str = "0xaabbccddee1122334455667788990011aabbccdd",
    pool: str = "0xd51a44d3fae010294c616388b506acda1bfaae46",  # tricrypto2
    token_in: str = "0xdac17f958d2ee523a2206206994597c13d831ec7",  # USDT
    token_out: str = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",  # WETH
    sold_id: int = 0,
    bought_id: int = 2,
    tokens_sold: int = 500_000_000,  # 500 USDT (6 decimals)
    tokens_bought: int = 215_700_000_000_000_000,  # ~0.2157 WETH (18 decimals)
) -> dict:
    """Build a synthetic Curve CryptoSwap receipt with TokenExchangeCrypto + Transfer events.

    CryptoSwap uses uint256 indices and a different keccak256 topic than StableSwap.
    """
    buyer_topic = "0x" + "00" * 12 + wallet[2:]
    # CryptoSwap encodes indices as uint256 (same byte layout as int128 for small values)
    exchange_data = (
        "0x"
        + _pad_hex(sold_id)  # uint256 sold_id
        + _pad_hex(tokens_sold)  # uint256 tokens_sold
        + _pad_hex(bought_id)  # uint256 bought_id
        + _pad_hex(tokens_bought)  # uint256 tokens_bought
    )

    transfer_topic = _make_topic(EVENT_TOPICS["Transfer"])
    wallet_topic = "0x" + "00" * 12 + wallet[2:]
    pool_topic = "0x" + "00" * 12 + pool[2:]

    return {
        "status": 1,
        "from": wallet,
        "transactionHash": "0x" + "cd" * 32,
        "blockNumber": 19_500_000,
        "gasUsed": 320_000,
        "logs": [
            # Transfer: wallet -> pool (token_in = USDT)
            {
                "address": token_in,
                "topics": [transfer_topic, wallet_topic, pool_topic],
                "data": "0x" + _pad_hex(tokens_sold),
                "logIndex": 0,
            },
            # TokenExchange event from CryptoSwap pool (different topic than StableSwap)
            {
                "address": pool,
                "topics": [_make_topic(EVENT_TOPICS["TokenExchangeCrypto"]), buyer_topic],
                "data": exchange_data,
                "logIndex": 1,
            },
            # Transfer: pool -> wallet (token_out = WETH)
            {
                "address": token_out,
                "topics": [transfer_topic, pool_topic, wallet_topic],
                "data": "0x" + _pad_hex(tokens_bought),
                "logIndex": 2,
            },
        ],
    }


class TestCryptoSwapReceiptParsing:
    """Test that CryptoSwap (TokenExchangeCrypto) receipts are parsed correctly.

    CryptoSwap pools use a different keccak256 topic than StableSwap pools.
    This was added in iter-95 to fix missing swap_amounts enrichment for tricrypto pools.
    """

    USDT = "0xdac17f958d2ee523a2206206994597c13d831ec7"
    WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
    WBTC = "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599"

    def test_cryptoswap_usdt_to_weth(self):
        """USDT (6 dec) -> WETH (18 dec) via CryptoSwap should produce correct swap_amounts."""
        receipt = _build_cryptoswap_receipt(
            token_in=self.USDT,
            token_out=self.WETH,
            tokens_sold=500_000_000,  # 500 USDT
            tokens_bought=215_700_000_000_000_000,  # ~0.2157 WETH
        )
        resolver = _mock_resolver({
            self.USDT: 6,
            self.WETH: 18,
        })

        parser = CurveReceiptParser(chain="ethereum")
        with patch("almanak.framework.data.tokens.get_token_resolver", return_value=resolver):
            result = parser.extract_swap_amounts(receipt)

        assert result is not None, "CryptoSwap receipt must be parsed (TokenExchangeCrypto topic)"
        assert result.amount_in == 500_000_000
        assert result.amount_out == 215_700_000_000_000_000
        assert result.amount_in_decimal == Decimal("500")
        assert Decimal("0.21") < result.amount_out_decimal < Decimal("0.22")
        # effective_price = amount_out / amount_in ~= 0.000431 WETH per USDT
        assert result.effective_price > Decimal("0")
        assert result.token_in == self.USDT
        assert result.token_out == self.WETH

    def test_cryptoswap_topic_distinct_from_stableswap(self):
        """Verify the two event topics are different (regression guard for the fix)."""
        assert EVENT_TOPICS["TokenExchange"] != EVENT_TOPICS["TokenExchangeCrypto"], (
            "StableSwap and CryptoSwap TokenExchange events have different keccak256 topics"
        )
        assert EVENT_TOPICS["TokenExchange"] == "0x8b3e96f2b889fa771c53c981b40daf005f63f637f1869f707052d15a3dd97140"
        assert EVENT_TOPICS["TokenExchangeCrypto"] == "0xb2e76ae99761dc136e598d4a629bb347eccb9532a5f8bbd72e18467c3c34cc98"

    def test_stableswap_receipt_still_works_after_cryptoswap_addition(self):
        """Adding TokenExchangeCrypto must not break StableSwap parsing."""
        receipt = _build_swap_receipt(
            token_in="0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",  # USDC
            token_out="0x6b175474e89094c44da98b954eedeac495271d0f",  # DAI
            tokens_sold=100_000_000,
            tokens_bought=99_984_871_483_550_784_213,
        )
        usdc = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
        dai = "0x6b175474e89094c44da98b954eedeac495271d0f"
        resolver = _mock_resolver({usdc: 6, dai: 18})

        parser = CurveReceiptParser(chain="ethereum")
        with patch("almanak.framework.data.tokens.get_token_resolver", return_value=resolver):
            result = parser.extract_swap_amounts(receipt)

        assert result is not None
        assert result.amount_in_decimal == Decimal("100")
        assert Decimal("99") < result.amount_out_decimal < Decimal("100")
