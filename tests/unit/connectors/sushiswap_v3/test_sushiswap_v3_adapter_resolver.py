"""Tests for SushiSwapV3Adapter TokenResolver integration.

These tests verify that the SushiSwapV3Adapter correctly uses the TokenResolver
for token resolution.
"""

from unittest.mock import MagicMock

import pytest

from almanak.framework.connectors.sushiswap_v3.adapter import (
    SushiSwapV3Adapter,
    SushiSwapV3Config,
)
from almanak.framework.data.tokens.exceptions import TokenResolutionError
from almanak.framework.data.tokens.models import ResolvedToken


TEST_WALLET = "0x1234567890123456789012345678901234567890"


@pytest.fixture
def config():
    """Create a SushiSwapV3Config for testing."""
    return SushiSwapV3Config(
        chain="arbitrum",
        wallet_address=TEST_WALLET,
        allow_placeholder_prices=True,
    )


@pytest.fixture
def mock_resolver():
    """Create a mock TokenResolver."""
    return MagicMock()


@pytest.fixture
def adapter(config, mock_resolver):
    """Create a SushiSwapV3Adapter with mock resolver."""
    return SushiSwapV3Adapter(config, token_resolver=mock_resolver)


class TestSushiSwapV3AdapterResolverInit:
    """Test SushiSwapV3Adapter initializes with TokenResolver."""

    def test_custom_resolver_injected(self, config, mock_resolver):
        """Test custom resolver is used when provided."""
        adapter = SushiSwapV3Adapter(config, token_resolver=mock_resolver)
        assert adapter._token_resolver is mock_resolver

    def test_default_resolver_initialized(self, config):
        """Test default resolver is initialized when not provided."""
        adapter = SushiSwapV3Adapter(config)
        assert adapter._token_resolver is not None


class TestSushiSwapV3AdapterResolveToken:
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

    def test_resolve_address_passthrough(self, adapter, mock_resolver):
        """Test address passthrough."""
        addr = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
        result = adapter._resolve_token(addr)
        assert result == addr
        mock_resolver.resolve.assert_not_called()


class TestSushiSwapV3AdapterGetDecimals:
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

    def test_bridged_token_decimals(self, adapter, mock_resolver):
        """Test USDC.e decimals (6) - case-sensitive."""
        mock_resolver.resolve.return_value = ResolvedToken(
            symbol="USDC.e",
            address="0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8",
            decimals=6,
            chain="arbitrum",
            chain_id=42161,
        )
        result = adapter._get_token_decimals("USDC.e")
        assert result == 6


class TestSushiSwapV3AdapterResolveTokenErrors:
    """Test _resolve_token raises TokenResolutionError on failure."""

    def test_resolve_unknown_symbol_raises(self, adapter, mock_resolver):
        """Test that resolving an unknown symbol raises TokenResolutionError."""
        mock_resolver.resolve.side_effect = TokenResolutionError(
            token="UNKNOWN", chain="arbitrum", reason="Not found"
        )
        with pytest.raises(TokenResolutionError):
            adapter._resolve_token("UNKNOWN")

    def test_resolve_invalid_symbol_raises(self, adapter, mock_resolver):
        """Test that resolving an invalid symbol raises TokenResolutionError."""
        mock_resolver.resolve.side_effect = TokenResolutionError(
            token="NOTAREAL", chain="arbitrum", reason="Not found"
        )
        with pytest.raises(TokenResolutionError):
            adapter._resolve_token("NOTAREAL")


class TestSushiSwapV3AdapterGetDecimalsErrors:
    """Test _get_token_decimals raises TokenResolutionError on failure."""

    def test_unknown_token_decimals_raises(self, adapter, mock_resolver):
        """Test that getting decimals for unknown token raises TokenResolutionError."""
        mock_resolver.resolve.side_effect = TokenResolutionError(
            token="UNKNOWN", chain="arbitrum", reason="Not found"
        )
        with pytest.raises(TokenResolutionError):
            adapter._get_token_decimals("UNKNOWN")

    def test_unresolvable_address_decimals_raises(self, adapter, mock_resolver):
        """Test that getting decimals for unresolvable address raises TokenResolutionError."""
        mock_resolver.resolve.side_effect = TokenResolutionError(
            token="0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
            chain="arbitrum",
            reason="Not found",
        )
        with pytest.raises(TokenResolutionError):
            adapter._get_token_decimals("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef")


class TestSushiSwapV3AdapterNullResolver:
    """Test adapter methods raise AttributeError when resolver is None."""

    def test_resolve_token_no_resolver_raises(self, config):
        """Test _resolve_token raises AttributeError when resolver is None."""
        adapter = SushiSwapV3Adapter(config, token_resolver=MagicMock())
        adapter._token_resolver = None
        with pytest.raises(AttributeError):
            adapter._resolve_token("USDC")

    def test_get_decimals_no_resolver_raises(self, config):
        """Test _get_token_decimals raises AttributeError when resolver is None."""
        adapter = SushiSwapV3Adapter(config, token_resolver=MagicMock())
        adapter._token_resolver = None
        with pytest.raises(AttributeError):
            adapter._get_token_decimals("USDC")


class TestDeprecatedDictsRemoved:
    """Verify deprecated token dicts have been removed (US-028)."""

    def test_deprecated_dicts_removed(self):
        """Verify deprecated token dicts have been removed (US-028)."""
        import almanak.framework.connectors.sushiswap_v3.adapter as adapter_module

        assert not hasattr(adapter_module, "SUSHISWAP_V3_TOKENS")
        assert not hasattr(adapter_module, "TOKEN_DECIMALS")
