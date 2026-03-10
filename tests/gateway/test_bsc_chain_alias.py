"""Tests for BSC chain alias resolution (VIB-180, VIB-708).

After VIB-708, "bsc" is the canonical name. The alias "bnb" is resolved
to "bsc" by resolve_chain_name() at entry points, so lookup dicts only
need the "bsc" key.
"""

from almanak.core.constants import resolve_chain_name
from almanak.gateway.utils.rpc_provider import (
    ALCHEMY_CHAIN_KEYS,
    ANVIL_CHAIN_PORTS,
    POA_CHAINS,
)


class TestBscChainAlias:
    """Ensure BSC is consistently available under canonical name 'bsc'."""

    def test_bsc_in_alchemy_chain_keys(self):
        assert "bsc" in ALCHEMY_CHAIN_KEYS

    def test_bsc_in_anvil_chain_ports(self):
        assert "bsc" in ANVIL_CHAIN_PORTS

    def test_bsc_in_poa_chains(self):
        assert "bsc" in POA_CHAINS

    def test_bnb_resolves_to_bsc(self):
        """The alias 'bnb' should resolve to 'bsc' via resolve_chain_name."""
        assert resolve_chain_name("bnb") == "bsc"

    def test_bnb_alchemy_lookup_via_resolver(self):
        """After resolving 'bnb' -> 'bsc', Alchemy key lookup works."""
        canonical = resolve_chain_name("bnb")
        assert canonical in ALCHEMY_CHAIN_KEYS
