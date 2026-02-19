"""Tests for GMXv2Adapter TokenResolver integration.

These tests verify that the GMXv2Adapter correctly uses the TokenResolver
for token resolution.
"""

from unittest.mock import MagicMock

import pytest

from almanak.framework.connectors.gmx_v2.adapter import (
    GMXv2Adapter,
    GMXv2Config,
)
from almanak.framework.data.tokens.exceptions import TokenResolutionError
from almanak.framework.data.tokens.models import ResolvedToken


TEST_WALLET = "0x1234567890123456789012345678901234567890"


@pytest.fixture
def config():
    """Create a GMXv2Config for testing."""
    return GMXv2Config(
        chain="arbitrum",
        wallet_address=TEST_WALLET,
    )


@pytest.fixture
def mock_resolver():
    """Create a mock TokenResolver."""
    return MagicMock()


@pytest.fixture
def adapter(config, mock_resolver):
    """Create a GMXv2Adapter with mock resolver."""
    return GMXv2Adapter(config, token_resolver=mock_resolver)


class TestGMXv2AdapterResolverInit:
    """Test GMXv2Adapter initializes with TokenResolver."""

    def test_custom_resolver_injected(self, config, mock_resolver):
        """Test custom resolver is used when provided."""
        adapter = GMXv2Adapter(config, token_resolver=mock_resolver)
        assert adapter._token_resolver is mock_resolver

    def test_default_resolver_initialized(self, config):
        """Test default resolver is initialized when not provided."""
        adapter = GMXv2Adapter(config)
        assert adapter._token_resolver is not None


class TestGMXv2AdapterResolveToken:
    """Test _resolve_token uses TokenResolver."""

    def test_resolve_symbol_via_resolver(self, adapter, mock_resolver):
        """Test symbol resolution via TokenResolver."""
        mock_resolver.resolve.return_value = ResolvedToken(
            symbol="WETH",
            address="0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
            decimals=18,
            chain="arbitrum",
            chain_id=42161,
        )
        result = adapter._resolve_token("WETH")
        assert result == "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"

    def test_resolve_address_passthrough(self, adapter, mock_resolver):
        """Test address passthrough."""
        addr = "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"
        result = adapter._resolve_token(addr)
        assert result == addr
        mock_resolver.resolve.assert_not_called()


class TestGMXv2AdapterGetDecimals:
    """Test _get_token_decimals uses TokenResolver."""

    def test_usdc_decimals_via_resolver(self, adapter, mock_resolver):
        """Test USDC decimals (6) via TokenResolver."""
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


class TestGMXv2AdapterResolverErrors:
    """Test that adapter methods raise TokenResolutionError when resolver fails."""

    def test_resolve_token_raises_on_resolver_failure(self, adapter, mock_resolver):
        """Test _resolve_token raises TokenResolutionError when resolver fails."""
        mock_resolver.resolve.side_effect = TokenResolutionError(
            token="UNKNOWN", chain="arbitrum", reason="Not found"
        )
        with pytest.raises(TokenResolutionError):
            adapter._resolve_token("UNKNOWN")

    def test_get_decimals_raises_on_resolver_failure(self, adapter, mock_resolver):
        """Test _get_token_decimals raises TokenResolutionError when resolver fails."""
        mock_resolver.resolve.side_effect = TokenResolutionError(
            token="UNKNOWN", chain="arbitrum", reason="Not found"
        )
        with pytest.raises(TokenResolutionError):
            adapter._get_token_decimals("UNKNOWN")

    def test_resolve_token_raises_attribute_error_when_no_resolver(self, config):
        """Test _resolve_token raises AttributeError when resolver is None."""
        adapter = GMXv2Adapter(config, token_resolver=None)
        adapter._token_resolver = None
        with pytest.raises(AttributeError):
            adapter._resolve_token("WETH")

    def test_get_decimals_raises_attribute_error_when_no_resolver(self, config):
        """Test _get_token_decimals raises AttributeError when resolver is None."""
        adapter = GMXv2Adapter(config, token_resolver=None)
        adapter._token_resolver = None
        with pytest.raises(AttributeError):
            adapter._get_token_decimals("WETH")


class TestDeprecatedDictsRemoved:
    """Verify deprecated token dicts have been removed (US-028)."""

    def test_deprecated_dicts_removed(self):
        """Verify deprecated token dicts have been removed (US-028)."""
        import almanak.framework.connectors.gmx_v2.adapter as adapter_module

        assert not hasattr(adapter_module, "GMX_V2_TOKENS")
