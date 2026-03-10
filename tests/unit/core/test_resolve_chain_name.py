"""Tests for resolve_chain_name in almanak.core.constants."""

import pytest

from almanak.core.constants import resolve_chain_name


class TestResolveChainName:
    """Tests for the central chain name resolver."""

    def test_canonical_names_unchanged(self):
        """Canonical names should pass through unchanged."""
        assert resolve_chain_name("ethereum") == "ethereum"
        assert resolve_chain_name("arbitrum") == "arbitrum"
        assert resolve_chain_name("bsc") == "bsc"
        assert resolve_chain_name("base") == "base"
        assert resolve_chain_name("avalanche") == "avalanche"

    def test_bsc_aliases_normalize_to_bsc(self):
        """All BSC aliases should resolve to 'bsc'."""
        assert resolve_chain_name("bnb") == "bsc"
        assert resolve_chain_name("binance") == "bsc"
        assert resolve_chain_name("bsc") == "bsc"

    def test_common_aliases(self):
        """Common aliases should resolve to canonical names."""
        assert resolve_chain_name("eth") == "ethereum"
        assert resolve_chain_name("mainnet") == "ethereum"
        assert resolve_chain_name("arb") == "arbitrum"
        assert resolve_chain_name("op") == "optimism"
        assert resolve_chain_name("avax") == "avalanche"
        assert resolve_chain_name("matic") == "polygon"

    def test_case_insensitive(self):
        """Should be case-insensitive."""
        assert resolve_chain_name("BSC") == "bsc"
        assert resolve_chain_name("BNB") == "bsc"
        assert resolve_chain_name("Ethereum") == "ethereum"
        assert resolve_chain_name("ARBITRUM") == "arbitrum"

    def test_strips_whitespace(self):
        """Should strip leading/trailing whitespace."""
        assert resolve_chain_name(" bsc ") == "bsc"
        assert resolve_chain_name("  bnb  ") == "bsc"

    def test_unknown_chain_raises(self):
        """Unknown chain names should raise ValueError."""
        with pytest.raises(ValueError, match="Unknown chain"):
            resolve_chain_name("fakenet")
        with pytest.raises(ValueError, match="Unknown chain"):
            resolve_chain_name("")
