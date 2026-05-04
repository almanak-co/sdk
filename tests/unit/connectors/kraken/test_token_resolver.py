"""Tests for Kraken token and chain resolution."""

import pytest

from almanak.framework.connectors.kraken.exceptions import KrakenChainNotSupportedError
from almanak.framework.connectors.kraken.token_resolver import KrakenChainMapper, KrakenTokenResolver


class TestKrakenTokenResolver:
    """Tests for KrakenTokenResolver."""

    def setup_method(self):
        """Setup test instance."""
        self.resolver = KrakenTokenResolver()

    def test_usdc_e_maps_to_usdc(self):
        """USDC.e on Arbitrum should map to USDC on Kraken."""
        result = self.resolver.to_kraken_symbol("arbitrum", "USDC.e")
        assert result == "USDC"

    def test_usdc_maps_to_usdc(self):
        """Native USDC should map to USDC on Kraken."""
        result = self.resolver.to_kraken_symbol("arbitrum", "USDC")
        assert result == "USDC"

    def test_eth_maps_to_eth(self):
        """ETH should map to ETH on Kraken."""
        result = self.resolver.to_kraken_symbol("ethereum", "ETH")
        assert result == "ETH"

    def test_weth_maps_to_eth(self):
        """WETH should map to ETH on Kraken."""
        result = self.resolver.to_kraken_symbol("arbitrum", "WETH")
        assert result == "ETH"

    def test_btc_maps_to_xbt(self):
        """BTC should map to XBT on Kraken."""
        result = self.resolver.to_kraken_symbol("ethereum", "BTC")
        assert result == "XBT"

    def test_unknown_token_returns_uppercase(self):
        """Unknown tokens should be returned as uppercase."""
        result = self.resolver.to_kraken_symbol("ethereum", "random_token")
        assert result == "RANDOM_TOKEN"

    def test_from_kraken_symbol_xeth(self):
        """XETH should convert to ETH."""
        result = self.resolver.from_kraken_symbol("XETH")
        assert result == "ETH"

    def test_from_kraken_symbol_xxbt(self):
        """XXBT should convert to BTC."""
        result = self.resolver.from_kraken_symbol("XXBT")
        assert result == "BTC"

    def test_from_kraken_symbol_xbt(self):
        """XBT should convert to BTC."""
        result = self.resolver.from_kraken_symbol("XBT")
        assert result == "BTC"

    def test_from_kraken_symbol_unknown(self):
        """Unknown Kraken symbols should be returned as-is."""
        result = self.resolver.from_kraken_symbol("USDC")
        assert result == "USDC"

    def test_get_trading_pair(self):
        """Should create proper trading pair string."""
        pair = self.resolver.get_trading_pair("ETH", "USD", "ethereum")
        assert pair == "ETHUSD"

    def test_get_trading_pair_with_mapping(self):
        """Should use proper symbols for trading pair."""
        pair = self.resolver.get_trading_pair("BTC", "USD", "ethereum")
        assert pair == "XBTUSD"

    def test_chain_agnostic_for_unknown(self):
        """Unknown tokens should work regardless of chain."""
        result1 = self.resolver.to_kraken_symbol("ethereum", "AAVE")
        result2 = self.resolver.to_kraken_symbol("arbitrum", "AAVE")
        assert result1 == "AAVE"
        assert result2 == "AAVE"


class TestKrakenChainMapper:
    """Tests for KrakenChainMapper."""

    def setup_method(self):
        """Setup test instance."""
        self.mapper = KrakenChainMapper()

    def test_get_deposit_method_arbitrum(self):
        """Should return correct deposit method for Arbitrum."""
        method = self.mapper.get_deposit_method("arbitrum", "ETH")
        assert "Arbitrum One" in method
        assert "ETH" in method

    def test_get_deposit_method_ethereum(self):
        """Ethereum should return special format."""
        method = self.mapper.get_deposit_method("ethereum", "ETH")
        assert method == "Ether (Hex)"

    def test_get_deposit_method_optimism(self):
        """Should return correct deposit method for Optimism."""
        method = self.mapper.get_deposit_method("optimism", "USDC")
        assert "Optimism" in method
        assert "USDC" in method

    def test_get_deposit_method_unsupported_chain(self):
        """Should raise error for unsupported chain."""
        with pytest.raises(KrakenChainNotSupportedError):
            self.mapper.get_deposit_method("solana", "SOL")

    def test_get_withdraw_method_arbitrum(self):
        """Should return correct withdrawal method for Arbitrum."""
        method = self.mapper.get_withdraw_method("arbitrum")
        assert method == "Arbitrum One"

    def test_get_withdraw_method_optimism(self):
        """Should return correct withdrawal method for Optimism."""
        method = self.mapper.get_withdraw_method("optimism")
        assert method == "Optimism"

    def test_get_withdraw_method_ethereum(self):
        """Should return correct withdrawal method for Ethereum."""
        method = self.mapper.get_withdraw_method("ethereum")
        assert method == "Ether"

    def test_get_withdraw_method_unsupported(self):
        """Should raise error for unsupported chain."""
        with pytest.raises(KrakenChainNotSupportedError):
            self.mapper.get_withdraw_method("fantom")

    def test_get_supported_chains(self):
        """Should return list of supported chains."""
        chains = self.mapper.get_supported_chains()
        assert "arbitrum" in chains
        assert "optimism" in chains
        assert "ethereum" in chains

    def test_chain_from_network_arbitrum(self):
        """Should parse Arbitrum network string."""
        chain = self.mapper.chain_from_network("Arbitrum One")
        assert chain == "arbitrum"

    def test_chain_from_network_optimism(self):
        """Should parse Optimism network string."""
        chain = self.mapper.chain_from_network("Optimism")
        assert chain == "optimism"

    def test_chain_from_network_ethereum(self):
        """Should parse Ethereum network string."""
        chain = self.mapper.chain_from_network("Ethereum")
        assert chain == "ethereum"

    def test_chain_from_network_unified(self):
        """Should handle unified network strings."""
        chain = self.mapper.chain_from_network("Arbitrum One (Unified)")
        assert chain == "arbitrum"

    def test_chain_from_network_unknown(self):
        """Unknown networks should return None."""
        chain = self.mapper.chain_from_network("Unknown Network")
        assert chain is None

    def test_parse_deposit_method(self):
        """Should parse deposit method to chain."""
        chain = self.mapper.parse_deposit_method("ETH - Arbitrum One (Unified)")
        assert chain == "arbitrum"

    def test_parse_deposit_method_with_asset_verification(self):
        """Should verify asset when provided."""
        chain = self.mapper.parse_deposit_method(
            "ETH - Arbitrum One (Unified)",
            expected_asset="ETH",
        )
        assert chain == "arbitrum"

        # Wrong asset should return None
        chain = self.mapper.parse_deposit_method(
            "ETH - Arbitrum One (Unified)",
            expected_asset="USDC",
        )
        assert chain is None

    def test_parse_deposit_method_ethereum(self):
        """Should handle Ethereum special case."""
        chain = self.mapper.parse_deposit_method("Ether (Hex)")
        assert chain == "ethereum"

    def test_case_insensitive_chain_lookup(self):
        """Chain names should be case-insensitive."""
        method1 = self.mapper.get_withdraw_method("ARBITRUM")
        method2 = self.mapper.get_withdraw_method("arbitrum")
        method3 = self.mapper.get_withdraw_method("Arbitrum")
        assert method1 == method2 == method3
