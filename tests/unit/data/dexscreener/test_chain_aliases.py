"""Tests for DexScreener chain alias mappings."""

import pytest

from almanak.core.chains._helpers import external_id_for


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
        # B1 (VIB-4851): vendor ids now derive from ChainDescriptor.external_ids;
        # the platform map is canonical-only, so the "bnb" alias resolves through
        # the registry via external_id_for rather than living as a map key.
        assert external_id_for(chain_name, "dexscreener") == expected_platform
