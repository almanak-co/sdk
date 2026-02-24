"""Tests for BSC chain alias in RPC provider (VIB-180)."""

from almanak.gateway.utils.rpc_provider import (
    ALCHEMY_CHAIN_KEYS,
    ANVIL_CHAIN_PORTS,
    POA_CHAINS,
)


class TestBscChainAlias:
    """Ensure 'bsc' and 'bnb' both resolve consistently."""

    def test_bsc_in_alchemy_chain_keys(self):
        assert "bsc" in ALCHEMY_CHAIN_KEYS

    def test_bnb_in_alchemy_chain_keys(self):
        assert "bnb" in ALCHEMY_CHAIN_KEYS

    def test_bsc_and_bnb_resolve_to_same_alchemy_key(self):
        assert ALCHEMY_CHAIN_KEYS["bsc"] == ALCHEMY_CHAIN_KEYS["bnb"]

    def test_bsc_in_anvil_chain_ports(self):
        assert "bsc" in ANVIL_CHAIN_PORTS

    def test_bsc_and_bnb_same_anvil_port(self):
        assert ANVIL_CHAIN_PORTS["bsc"] == ANVIL_CHAIN_PORTS["bnb"]

    def test_bsc_in_poa_chains(self):
        assert "bsc" in POA_CHAINS

    def test_bnb_in_poa_chains(self):
        assert "bnb" in POA_CHAINS
