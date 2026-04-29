"""Unit tests for Linea Chainlink price feed configuration (VIB-3718 / BUG-44).

`uniswap_v3_swap_linea` and other Linea strategies timed out waiting for
ETH/USD prices because Linea was not registered in the Chainlink feed
registry — the gateway oracle had no on-chain source and fell through to
slower web fallbacks (CoinGecko/DexScreener), exceeding the 5s deadline.

Feed addresses verified on-chain via `description()` and `latestAnswer()`
against `https://rpc.linea.build` on 2026-04-29.

Verifies:
1. LINEA_PRICE_FEEDS dict contains the 5 major USD feeds
2. 'linea' is registered in CHAINLINK_PRICE_FEEDS
3. Linea chain ID (59144) is in CHAINLINK_CHAIN_IDS
"""

import pytest

from almanak.core.chainlink import (
    CHAINLINK_CHAIN_IDS,
    CHAINLINK_PRICE_FEEDS,
    LINEA_PRICE_FEEDS,
    TOKEN_TO_PAIR,
)

# Linea Chainlink feed addresses verified on-chain 2026-04-29.
# Each address responded to description() with the expected pair name and
# returned a sensible latestAnswer() (e.g. ETH/USD ≈ $2283 at verification).
EXPECTED_LINEA_FEEDS = {
    "ETH/USD": "0x3c6Cd9Cc7c7a4c2Cf5a82734CD249D7D593354dA",
    "BTC/USD": "0x7A99092816C8BD5ec8ba229e3a6E6Da1E628E1F9",
    "USDC/USD": "0xAADAa473C1bDF7317ec07c915680Af29DeBfdCb5",
    "USDT/USD": "0xefCA2bbe0EdD0E22b2e0d2F8248E99F4bEf4A7dB",
    "DAI/USD": "0x5133D67c38AFbdd02997c14Abd8d83676B4e309A",
}

LINEA_CHAIN_ID = 59144


class TestLineaPriceFeedsConfig:
    """LINEA_PRICE_FEEDS dict structure and content."""

    def test_linea_price_feeds_exists(self):
        assert isinstance(LINEA_PRICE_FEEDS, dict)
        assert len(LINEA_PRICE_FEEDS) >= 5

    @pytest.mark.parametrize("pair", sorted(EXPECTED_LINEA_FEEDS))
    def test_required_feed_present(self, pair):
        assert pair in LINEA_PRICE_FEEDS

    @pytest.mark.parametrize("pair,expected_address", EXPECTED_LINEA_FEEDS.items())
    def test_feed_addresses_match_onchain_verification(self, pair, expected_address):
        """Feed addresses must match the on-chain-verified canonical Chainlink proxies."""
        assert LINEA_PRICE_FEEDS[pair] == expected_address, (
            f"{pair} address mismatch: expected {expected_address}, "
            f"got {LINEA_PRICE_FEEDS[pair]}"
        )

    def test_all_addresses_are_valid_evm_format(self):
        for pair, address in LINEA_PRICE_FEEDS.items():
            assert address.startswith("0x"), f"{pair}: address must start with 0x"
            assert len(address) == 42, f"{pair}: address must be 42 chars (0x + 40 hex)"


class TestLineaInCombinedRegistry:
    """'linea' entry in CHAINLINK_PRICE_FEEDS registry."""

    def test_linea_in_chainlink_price_feeds(self):
        assert "linea" in CHAINLINK_PRICE_FEEDS

    def test_linea_feeds_alias_dict(self):
        assert CHAINLINK_PRICE_FEEDS["linea"] is LINEA_PRICE_FEEDS

    def test_linea_chain_id_configured(self):
        assert "linea" in CHAINLINK_CHAIN_IDS
        assert CHAINLINK_CHAIN_IDS["linea"] == LINEA_CHAIN_ID


class TestLineaPairResolution:
    """Symbol→pair mapping must resolve the tokens Linea strategies need."""

    @pytest.mark.parametrize(
        "symbol,expected_pair",
        [
            ("ETH", "ETH/USD"),
            ("WETH", "ETH/USD"),
            ("USDC", "USDC/USD"),
            ("USDT", "USDT/USD"),
            ("DAI", "DAI/USD"),
        ],
    )
    def test_token_resolves_via_linea_feeds(self, symbol, expected_pair):
        """The token resolver expects symbol→pair, and the pair must exist in LINEA_PRICE_FEEDS."""
        pair = TOKEN_TO_PAIR[symbol]
        assert pair == expected_pair
        assert pair in LINEA_PRICE_FEEDS, (
            f"{symbol} resolves to {pair} but Linea has no feed for it — "
            "would force fallback to slower web sources"
        )
