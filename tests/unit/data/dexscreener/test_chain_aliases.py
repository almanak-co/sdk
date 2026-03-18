"""Tests for DexScreener chain alias mappings."""

import pytest

from almanak.gateway.data.price.dexscreener import CHAIN_TO_DEXSCREENER_PLATFORM


class TestDexScreenerChainAliases:
    """Verify that all known chain name variants map correctly."""

    @pytest.mark.parametrize(
        "chain_name,expected_platform",
        [
            ("bsc", "bsc"),
            ("bnb", "bsc"),  # VIB-1441: bnb alias was missing
            ("ethereum", "ethereum"),
            ("arbitrum", "arbitrum"),
            ("base", "base"),
            ("optimism", "optimism"),
            ("polygon", "polygon"),
            ("avalanche", "avalanche"),
            ("sonic", "sonic"),
            ("solana", "solana"),
        ],
    )
    def test_chain_alias_maps_correctly(self, chain_name, expected_platform):
        assert chain_name in CHAIN_TO_DEXSCREENER_PLATFORM, (
            f"Chain '{chain_name}' missing from CHAIN_TO_DEXSCREENER_PLATFORM"
        )
        assert CHAIN_TO_DEXSCREENER_PLATFORM[chain_name] == expected_platform
