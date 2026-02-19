"""Tests for UniswapV3Adapter TokenResolver integration.

These tests verify that the UniswapV3Adapter correctly uses the TokenResolver
for token resolution.
"""

from unittest.mock import MagicMock

import pytest

from almanak.framework.connectors.uniswap_v3.adapter import (
    UniswapV3Adapter,
    UniswapV3Config,
)
from almanak.framework.data.tokens.exceptions import TokenResolutionError
from almanak.framework.data.tokens.models import ResolvedToken


TEST_WALLET = "0x1234567890123456789012345678901234567890"


@pytest.fixture
def config():
    """Create a UniswapV3Config for testing."""
    return UniswapV3Config(
        chain="arbitrum",
        wallet_address=TEST_WALLET,
        allow_placeholder_prices=True,
    )


@pytest.fixture
def eth_config():
    """Create a UniswapV3Config for Ethereum testing."""
    return UniswapV3Config(
        chain="ethereum",
        wallet_address=TEST_WALLET,
        allow_placeholder_prices=True,
    )


@pytest.fixture
def mock_resolver():
    """Create a mock TokenResolver."""
    return MagicMock()


@pytest.fixture
def adapter(config, mock_resolver):
    """Create a UniswapV3Adapter with mock resolver."""
    return UniswapV3Adapter(config, token_resolver=mock_resolver)


class TestUniswapV3AdapterResolverInit:
    """Test UniswapV3Adapter initializes with TokenResolver."""

    def test_custom_resolver_injected(self, config, mock_resolver):
        """Test custom resolver is used when provided."""
        adapter = UniswapV3Adapter(config, token_resolver=mock_resolver)
        assert adapter._token_resolver is mock_resolver

    def test_default_resolver_initialized(self, config):
        """Test default resolver is initialized when not provided."""
        adapter = UniswapV3Adapter(config)
        assert adapter._token_resolver is not None

    def test_resolver_none_raises_error(self, config):
        """Test adapter raises error when resolver is None."""
        adapter = UniswapV3Adapter(config, token_resolver=MagicMock())
        adapter._token_resolver = None
        with pytest.raises(AttributeError):
            adapter._resolve_token("USDC")


class TestUniswapV3AdapterResolveToken:
    """Test _resolve_token uses TokenResolver."""

    def test_resolve_symbol_via_resolver(self, adapter, mock_resolver):
        """Test symbol resolution via TokenResolver."""
        mock_resolver.resolve.return_value = ResolvedToken(
            symbol="USDC",
            address="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            decimals=6,
            chain="arbitrum",
            chain_id=42161,
        )
        result = adapter._resolve_token("USDC")
        assert result == "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
        mock_resolver.resolve.assert_called_once_with("USDC", "arbitrum")

    def test_resolve_address_passthrough(self, adapter, mock_resolver):
        """Test address passthrough (no resolver call)."""
        addr = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
        result = adapter._resolve_token(addr)
        assert result == addr
        mock_resolver.resolve.assert_not_called()

    def test_resolve_unknown_raises_error(self, adapter, mock_resolver):
        """Test unknown token raises TokenResolutionError."""
        mock_resolver.resolve.side_effect = TokenResolutionError(
            token="NONEXISTENT_TOKEN_XYZ", chain="arbitrum", reason="Not found"
        )
        with pytest.raises(TokenResolutionError):
            adapter._resolve_token("NONEXISTENT_TOKEN_XYZ")


class TestUniswapV3AdapterGetDecimals:
    """Test _get_token_decimals uses TokenResolver."""

    def test_decimals_via_resolver(self, adapter, mock_resolver):
        """Test decimals resolution via TokenResolver."""
        mock_resolver.resolve.return_value = ResolvedToken(
            symbol="USDC",
            address="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            decimals=6,
            chain="arbitrum",
            chain_id=42161,
        )
        result = adapter._get_token_decimals("USDC")
        assert result == 6

    def test_wbtc_decimals(self, adapter, mock_resolver):
        """Test WBTC has 8 decimals."""
        mock_resolver.resolve.return_value = ResolvedToken(
            symbol="WBTC",
            address="0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f",
            decimals=8,
            chain="arbitrum",
            chain_id=42161,
        )
        result = adapter._get_token_decimals("WBTC")
        assert result == 8

    def test_weth_decimals(self, adapter, mock_resolver):
        """Test WETH has 18 decimals."""
        mock_resolver.resolve.return_value = ResolvedToken(
            symbol="WETH",
            address="0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
            decimals=18,
            chain="arbitrum",
            chain_id=42161,
        )
        result = adapter._get_token_decimals("WETH")
        assert result == 18

    def test_unknown_raises_error(self, adapter, mock_resolver):
        """Test unknown token raises TokenResolutionError instead of defaulting to 18."""
        mock_resolver.resolve.side_effect = TokenResolutionError(
            token="UNKNOWN_TOKEN", chain="arbitrum", reason="Not found"
        )
        with pytest.raises(TokenResolutionError):
            adapter._get_token_decimals("UNKNOWN_TOKEN")


class TestUniswapV3MultiChain:
    """Test multi-chain resolution."""

    def test_ethereum_chain(self, eth_config, mock_resolver):
        """Test Ethereum chain resolution."""
        adapter = UniswapV3Adapter(eth_config, token_resolver=mock_resolver)
        mock_resolver.resolve.return_value = ResolvedToken(
            symbol="USDC",
            address="0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            decimals=6,
            chain="ethereum",
            chain_id=1,
        )
        result = adapter._resolve_token("USDC")
        assert result == "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
        mock_resolver.resolve.assert_called_once_with("USDC", "ethereum")


class TestDeprecatedDictsRemoved:
    """Verify deprecated token dicts have been removed (US-028)."""

    def test_deprecated_dicts_removed(self):
        """Verify deprecated token dicts have been removed (US-028)."""
        import almanak.framework.connectors.uniswap_v3.adapter as adapter_module

        assert not hasattr(adapter_module, "TOKEN_DECIMALS")
        assert not hasattr(adapter_module, "UNISWAP_V3_TOKENS")
