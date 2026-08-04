"""Tests for DexScreener chain alias mappings."""

import pytest

from almanak.integrations.chains import integration_chain_id


class TestDexScreenerChainAliases:
    """Verify that all known chain name variants map correctly."""

    @pytest.mark.parametrize(
        "chain_name,expected_platform",
        [
            ("bsc", "bsc"),
            ("bnb", "bsc"),  # VIB-1441: bnb alias resolves via the registry
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
        # Provider IDs derive from the integration manifest. The platform map is
        # canonical-only, so the "bnb" alias resolves through
        # the integration registry rather than living as a map key.
        assert integration_chain_id(chain_name, "dexscreener") == expected_platform
