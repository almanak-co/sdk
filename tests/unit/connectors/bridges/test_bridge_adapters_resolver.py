"""Tests for bridge adapter TokenResolver integration.

Tests AcrossBridgeAdapter and StargateBridgeAdapter TokenResolver usage.
"""

from unittest.mock import MagicMock

import pytest

from almanak.framework.connectors.bridges.across.adapter import (
    AcrossBridgeAdapter,
    AcrossConfig,
)
from almanak.framework.connectors.bridges.stargate.adapter import (
    StargateBridgeAdapter,
    StargateConfig,
)
from almanak.framework.data.tokens.exceptions import TokenResolutionError
from almanak.framework.data.tokens.models import ResolvedToken


@pytest.fixture
def mock_resolver():
    """Create a mock TokenResolver."""
    return MagicMock()


class TestAcrossBridgeAdapterResolver:
    """Test AcrossBridgeAdapter TokenResolver integration."""

    def test_custom_resolver_injected(self, mock_resolver):
        """Test custom resolver is used when provided."""
        adapter = AcrossBridgeAdapter(token_resolver=mock_resolver)
        assert adapter._token_resolver is mock_resolver

    def test_get_token_address_via_resolver(self, mock_resolver):
        """Test token address resolution via TokenResolver."""
        adapter = AcrossBridgeAdapter(token_resolver=mock_resolver)
        mock_resolver.resolve.return_value = ResolvedToken(
            symbol="WETH",
            address="0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
            decimals=18,
            chain="arbitrum",
            chain_id=42161,
        )
        result = adapter._get_token_address("ETH", 42161)
        assert result == "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"
        # ETH gets mapped to WETH
        mock_resolver.resolve.assert_called_once_with("WETH", "arbitrum")

    def test_get_token_decimals_via_resolver(self, mock_resolver):
        """Test token decimals via TokenResolver."""
        adapter = AcrossBridgeAdapter(token_resolver=mock_resolver)
        mock_resolver.resolve.return_value = ResolvedToken(
            symbol="USDC",
            address="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            decimals=6,
            chain="arbitrum",
            chain_id=42161,
        )
        result = adapter._get_token_decimals("USDC", chain_id=42161)
        assert result == 6

    def test_get_token_address_raises_on_unknown(self, mock_resolver):
        """Test unknown token raises TokenResolutionError."""
        adapter = AcrossBridgeAdapter(token_resolver=mock_resolver)
        mock_resolver.resolve.side_effect = TokenResolutionError(
            token="UNKNOWN", chain="arbitrum", reason="Not found"
        )
        with pytest.raises(TokenResolutionError):
            adapter._get_token_address("UNKNOWN", 42161)

    def test_get_token_decimals_raises_on_unknown(self, mock_resolver):
        """Test unknown token decimals raises TokenResolutionError."""
        adapter = AcrossBridgeAdapter(token_resolver=mock_resolver)
        mock_resolver.resolve.side_effect = TokenResolutionError(
            token="UNKNOWN", chain="arbitrum", reason="Not found"
        )
        with pytest.raises(TokenResolutionError):
            adapter._get_token_decimals("UNKNOWN", chain_id=42161)


class TestStargateBridgeAdapterResolver:
    """Test StargateBridgeAdapter TokenResolver integration."""

    def test_custom_resolver_injected(self, mock_resolver):
        """Test custom resolver is used when provided."""
        adapter = StargateBridgeAdapter(token_resolver=mock_resolver)
        assert adapter._token_resolver is mock_resolver

    def test_get_token_address_via_resolver(self, mock_resolver):
        """Test token address resolution via TokenResolver."""
        adapter = StargateBridgeAdapter(token_resolver=mock_resolver)
        mock_resolver.resolve.return_value = ResolvedToken(
            symbol="USDC",
            address="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            decimals=6,
            chain="arbitrum",
            chain_id=42161,
        )
        result = adapter._get_token_address("USDC", 42161)
        assert result == "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"

    def test_get_token_decimals_via_resolver(self, mock_resolver):
        """Test token decimals via TokenResolver."""
        adapter = StargateBridgeAdapter(token_resolver=mock_resolver)
        mock_resolver.resolve.return_value = ResolvedToken(
            symbol="USDC",
            address="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            decimals=6,
            chain="arbitrum",
            chain_id=42161,
        )
        result = adapter._get_token_decimals("USDC", chain_id=42161)
        assert result == 6

    def test_get_token_address_raises_on_unknown(self, mock_resolver):
        """Test unknown token raises TokenResolutionError."""
        adapter = StargateBridgeAdapter(token_resolver=mock_resolver)
        mock_resolver.resolve.side_effect = TokenResolutionError(
            token="UNKNOWN", chain="arbitrum", reason="Not found"
        )
        with pytest.raises(TokenResolutionError):
            adapter._get_token_address("UNKNOWN", 42161)

    def test_get_token_decimals_raises_on_unknown(self, mock_resolver):
        """Test unknown token decimals raises TokenResolutionError."""
        adapter = StargateBridgeAdapter(token_resolver=mock_resolver)
        mock_resolver.resolve.side_effect = TokenResolutionError(
            token="UNKNOWN", chain="arbitrum", reason="Not found"
        )
        with pytest.raises(TokenResolutionError):
            adapter._get_token_decimals("UNKNOWN", chain_id=42161)


class TestDeprecatedDictsRemoved:
    """Verify deprecated token dicts have been removed (US-028)."""

    def test_deprecated_dicts_removed(self):
        """Verify deprecated token dicts have been removed (US-028)."""
        import almanak.framework.connectors.bridges.across.adapter as across_module
        import almanak.framework.connectors.bridges.stargate.adapter as stargate_module

        assert not hasattr(across_module, "ACROSS_TOKEN_ADDRESSES")
        assert not hasattr(stargate_module, "STARGATE_TOKEN_ADDRESSES")
