"""Tests for DexScreener data models."""

import time

from almanak.framework.data.dexscreener.models import (
    BoostedToken,
    DexLiquidity,
    DexPair,
    DexPriceChange,
    DexToken,
    DexTxnCounts,
    DexTxns,
    DexVolume,
    parse_boosted_token,
    parse_pair,
)


# ---------------------------------------------------------------------------
# DexTxnCounts
# ---------------------------------------------------------------------------


class TestDexTxnCounts:
    def test_total(self):
        tc = DexTxnCounts(buys=10, sells=5)
        assert tc.total == 15

    def test_buy_ratio_normal(self):
        tc = DexTxnCounts(buys=60, sells=40)
        assert abs(tc.buy_ratio - 0.6) < 0.001

    def test_buy_ratio_zero_txns(self):
        tc = DexTxnCounts(buys=0, sells=0)
        assert tc.buy_ratio == 0.5

    def test_buy_ratio_all_buys(self):
        tc = DexTxnCounts(buys=100, sells=0)
        assert tc.buy_ratio == 1.0

    def test_buy_ratio_all_sells(self):
        tc = DexTxnCounts(buys=0, sells=100)
        assert tc.buy_ratio == 0.0


# ---------------------------------------------------------------------------
# DexPair
# ---------------------------------------------------------------------------


class TestDexPair:
    def test_price_usd_float(self):
        pair = DexPair(price_usd="0.00001234")
        assert abs(pair.price_usd_float - 0.00001234) < 1e-10

    def test_price_usd_float_empty(self):
        pair = DexPair(price_usd="")
        assert pair.price_usd_float == 0.0

    def test_price_usd_float_invalid(self):
        pair = DexPair(price_usd="not-a-number")
        assert pair.price_usd_float == 0.0

    def test_age_hours(self):
        # Created 2 hours ago
        created_ms = int((time.time() - 7200) * 1000)
        pair = DexPair(pair_created_at=created_ms)
        age = pair.age_hours
        assert age is not None
        assert abs(age - 2.0) < 0.1

    def test_age_hours_none(self):
        pair = DexPair(pair_created_at=None)
        assert pair.age_hours is None


# ---------------------------------------------------------------------------
# parse_pair
# ---------------------------------------------------------------------------


class TestParsePair:
    def test_parse_full_pair(self):
        raw = {
            "chainId": "solana",
            "dexId": "raydium",
            "pairAddress": "abc123",
            "url": "https://dexscreener.com/solana/abc123",
            "baseToken": {"address": "mint1", "name": "Bonk", "symbol": "BONK"},
            "quoteToken": {"address": "mint2", "name": "USD Coin", "symbol": "USDC"},
            "priceNative": "0.0000001",
            "priceUsd": "0.00001234",
            "txns": {
                "m5": {"buys": 10, "sells": 5},
                "h1": {"buys": 100, "sells": 80},
                "h6": {"buys": 500, "sells": 400},
                "h24": {"buys": 2000, "sells": 1800},
            },
            "volume": {"m5": 1000, "h1": 15000, "h6": 80000, "h24": 300000},
            "priceChange": {"m5": 0.5, "h1": 3.2, "h6": -1.1, "h24": 8.5},
            "liquidity": {"usd": 500000, "base": 10000000, "quote": 250000},
            "fdv": 5000000,
            "marketCap": 3000000,
            "pairCreatedAt": 1709000000000,
            "labels": ["CLMM"],
            "boosts": {"active": 10},
        }

        pair = parse_pair(raw)
        assert pair.chain_id == "solana"
        assert pair.dex_id == "raydium"
        assert pair.base_token.symbol == "BONK"
        assert pair.quote_token.symbol == "USDC"
        assert pair.price_usd == "0.00001234"
        assert pair.txns.h1.buys == 100
        assert pair.txns.h1.sells == 80
        assert pair.volume.h24 == 300000
        assert pair.price_change.h1 == 3.2
        assert pair.liquidity.usd == 500000
        assert pair.fdv == 5000000
        assert pair.market_cap == 3000000
        assert pair.pair_created_at == 1709000000000
        assert pair.labels == ["CLMM"]
        assert pair.boost_active == 10

    def test_parse_minimal_pair(self):
        raw = {"chainId": "solana", "priceUsd": "1.0"}
        pair = parse_pair(raw)
        assert pair.chain_id == "solana"
        assert pair.price_usd_float == 1.0
        assert pair.txns.h1.total == 0
        assert pair.volume.h24 == 0
        assert pair.liquidity.usd == 0
        assert pair.fdv is None

    def test_parse_pair_null_liquidity(self):
        raw = {"chainId": "solana", "liquidity": None}
        pair = parse_pair(raw)
        assert pair.liquidity.usd == 0

    def test_parse_pair_missing_fields(self):
        raw = {}
        pair = parse_pair(raw)
        assert pair.chain_id == ""
        assert pair.price_usd_float == 0.0


# ---------------------------------------------------------------------------
# parse_boosted_token
# ---------------------------------------------------------------------------


class TestParseBoostedToken:
    def test_parse_boosted(self):
        raw = {
            "chainId": "solana",
            "tokenAddress": "abc123",
            "url": "https://dexscreener.com/solana/abc123",
            "icon": "https://cdn.dexscreener.com/icon.png",
            "description": "A meme coin",
            "amount": 50,
            "totalAmount": 200,
        }
        token = parse_boosted_token(raw)
        assert isinstance(token, BoostedToken)
        assert token.chain_id == "solana"
        assert token.token_address == "abc123"
        assert token.amount == 50
        assert token.total_amount == 200

    def test_parse_boosted_minimal(self):
        raw = {}
        token = parse_boosted_token(raw)
        assert token.chain_id == ""
        assert token.amount == 0
