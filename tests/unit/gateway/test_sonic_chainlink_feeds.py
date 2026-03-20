"""Unit tests for Sonic Chainlink price feed configuration (VIB-1535).

Verifies that Sonic chain is correctly configured in the Chainlink registry:
1. SONIC_PRICE_FEEDS dict exists and contains required token feeds
2. 'sonic' entry is present in CHAINLINK_PRICE_FEEDS
3. Sonic chain ID (146) is in CHAINLINK_CHAIN_IDS
4. S/WS tokens are mapped in TOKEN_TO_PAIR
"""

import pytest

from almanak.core.chainlink import (
    CHAINLINK_CHAIN_IDS,
    CHAINLINK_PRICE_FEEDS,
    SONIC_PRICE_FEEDS,
    TOKEN_TO_PAIR,
)

# Expected Sonic Chainlink feed addresses (sourced from bgd-labs/aave-address-book AaveV3Sonic.sol)
EXPECTED_SONIC_FEEDS = {
    "ETH/USD": "0x824364077993847f71293B24ccA8567c00c2de11",
    "USDC/USD": "0x7A8443a2a5D772db7f1E40DeFe32db485108F128",
    "S/USD": "0xc76dFb89fF298145b417d221B2c747d84952e01d",
}

SONIC_CHAIN_ID = 146


class TestSonicPriceFeedsConfig:
    """SONIC_PRICE_FEEDS dict structure and content."""

    def test_sonic_price_feeds_exists(self):
        """SONIC_PRICE_FEEDS must be a non-empty dict."""
        assert isinstance(SONIC_PRICE_FEEDS, dict)
        assert len(SONIC_PRICE_FEEDS) > 0

    def test_eth_usd_feed_present(self):
        """ETH/USD feed must be configured for Sonic."""
        assert "ETH/USD" in SONIC_PRICE_FEEDS

    def test_usdc_usd_feed_present(self):
        """USDC/USD feed must be configured for Sonic."""
        assert "USDC/USD" in SONIC_PRICE_FEEDS

    def test_s_usd_feed_present(self):
        """S/USD (native Sonic token) feed must be configured."""
        assert "S/USD" in SONIC_PRICE_FEEDS

    @pytest.mark.parametrize("pair,expected_address", EXPECTED_SONIC_FEEDS.items())
    def test_feed_addresses_match_aave_oracle(self, pair, expected_address):
        """Feed addresses must match Aave V3 Sonic oracle deployment (authoritative source)."""
        assert SONIC_PRICE_FEEDS[pair] == expected_address, (
            f"{pair} address mismatch: expected {expected_address}, got {SONIC_PRICE_FEEDS[pair]}"
        )

    def test_all_addresses_are_valid_evm_format(self):
        """All feed addresses must be valid EVM hex addresses."""
        for pair, address in SONIC_PRICE_FEEDS.items():
            assert address.startswith("0x"), f"{pair}: address must start with 0x"
            assert len(address) == 42, f"{pair}: address must be 42 chars (0x + 40 hex)"


class TestSonicInCombinedRegistry:
    """'sonic' entry in CHAINLINK_PRICE_FEEDS registry."""

    def test_sonic_in_chainlink_price_feeds(self):
        """'sonic' must be present in the combined CHAINLINK_PRICE_FEEDS dict."""
        assert "sonic" in CHAINLINK_PRICE_FEEDS

    def test_sonic_feeds_match_sonic_price_feeds(self):
        """CHAINLINK_PRICE_FEEDS['sonic'] must reference the SONIC_PRICE_FEEDS dict."""
        assert CHAINLINK_PRICE_FEEDS["sonic"] is SONIC_PRICE_FEEDS

    def test_sonic_chain_id_configured(self):
        """Sonic chain ID (146) must be in CHAINLINK_CHAIN_IDS for RPC validation."""
        assert "sonic" in CHAINLINK_CHAIN_IDS
        assert CHAINLINK_CHAIN_IDS["sonic"] == SONIC_CHAIN_ID


class TestSonicTokenMapping:
    """S/wS tokens in TOKEN_TO_PAIR mapping."""

    def test_s_token_maps_to_s_usd(self):
        """Native Sonic 'S' token maps to S/USD pair."""
        assert "S" in TOKEN_TO_PAIR
        assert TOKEN_TO_PAIR["S"] == "S/USD"

    def test_ws_token_maps_to_s_usd(self):
        """Wrapped Sonic 'WS' maps to S/USD (same feed as native S)."""
        assert "WS" in TOKEN_TO_PAIR
        assert TOKEN_TO_PAIR["WS"] == "S/USD"
