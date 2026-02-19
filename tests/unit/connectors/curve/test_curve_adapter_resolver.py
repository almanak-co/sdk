"""Tests for CurveAdapter TokenResolver integration.

These tests verify that the CurveAdapter correctly uses the TokenResolver
for token resolution.
"""

from unittest.mock import MagicMock

import pytest

from almanak.framework.connectors.curve.adapter import (
    CurveAdapter,
    CurveConfig,
)
from almanak.framework.data.tokens.exceptions import TokenResolutionError
from almanak.framework.data.tokens.models import ResolvedToken


TEST_WALLET = "0x1234567890123456789012345678901234567890"


@pytest.fixture
def config():
    """Create a CurveConfig for testing."""
    return CurveConfig(
        chain="ethereum",
        wallet_address=TEST_WALLET,
    )


@pytest.fixture
def arb_config():
    """Create a CurveConfig for Arbitrum testing."""
    return CurveConfig(
        chain="arbitrum",
        wallet_address=TEST_WALLET,
    )


@pytest.fixture
def mock_resolver():
    """Create a mock TokenResolver."""
    return MagicMock()


@pytest.fixture
def adapter(config, mock_resolver):
    """Create a CurveAdapter with mock resolver."""
    return CurveAdapter(config, token_resolver=mock_resolver)


class TestCurveAdapterResolverInit:
    """Test CurveAdapter initializes with TokenResolver."""

    def test_custom_resolver_injected(self, config, mock_resolver):
        """Test custom resolver is used when provided."""
        adapter = CurveAdapter(config, token_resolver=mock_resolver)
        assert adapter._token_resolver is mock_resolver

    def test_default_resolver_initialized(self, config):
        """Test default resolver is initialized when not provided."""
        adapter = CurveAdapter(config)
        assert adapter._token_resolver is not None

    def test_resolver_none_raises_error(self, config):
        """Test adapter raises error when resolver is None."""
        adapter = CurveAdapter(config, token_resolver=MagicMock())
        adapter._token_resolver = None
        with pytest.raises(AttributeError):
            adapter._resolve_token("USDC")


class TestCurveAdapterResolveToken:
    """Test _resolve_token uses TokenResolver."""

    def test_resolve_symbol_via_resolver(self, adapter, mock_resolver):
        """Test symbol resolution via TokenResolver."""
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

    def test_resolve_address_passthrough(self, adapter, mock_resolver):
        """Test address passthrough (no resolver call)."""
        addr = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
        result = adapter._resolve_token(addr)
        assert result == addr
        mock_resolver.resolve.assert_not_called()

    def test_resolve_unknown_raises_error(self, adapter, mock_resolver):
        """Test unknown token raises TokenResolutionError."""
        mock_resolver.resolve.side_effect = TokenResolutionError(
            token="NONEXISTENT_TOKEN_XYZ", chain="ethereum", reason="Not found"
        )
        with pytest.raises(TokenResolutionError):
            adapter._resolve_token("NONEXISTENT_TOKEN_XYZ")


class TestCurveAdapterGetDecimals:
    """Test _get_token_decimals uses TokenResolver."""

    def test_decimals_via_resolver(self, adapter, mock_resolver):
        """Test decimals resolution via TokenResolver."""
        mock_resolver.resolve.return_value = ResolvedToken(
            symbol="USDC",
            address="0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            decimals=6,
            chain="ethereum",
            chain_id=1,
        )
        result = adapter._get_token_decimals("USDC")
        assert result == 6

    def test_wbtc_decimals(self, adapter, mock_resolver):
        """Test WBTC has 8 decimals."""
        mock_resolver.resolve.return_value = ResolvedToken(
            symbol="WBTC",
            address="0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599",
            decimals=8,
            chain="ethereum",
            chain_id=1,
        )
        result = adapter._get_token_decimals("WBTC")
        assert result == 8

    def test_steth_decimals(self, adapter, mock_resolver):
        """Test stETH has 18 decimals."""
        mock_resolver.resolve.return_value = ResolvedToken(
            symbol="stETH",
            address="0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84",
            decimals=18,
            chain="ethereum",
            chain_id=1,
        )
        result = adapter._get_token_decimals("stETH")
        assert result == 18

    def test_unknown_raises_error(self, adapter, mock_resolver):
        """Test unknown token raises TokenResolutionError instead of defaulting to 18."""
        mock_resolver.resolve.side_effect = TokenResolutionError(
            token="UNKNOWN_TOKEN", chain="ethereum", reason="Not found"
        )
        with pytest.raises(TokenResolutionError):
            adapter._get_token_decimals("UNKNOWN_TOKEN")


class TestCurveMultiChain:
    """Test multi-chain resolution."""

    def test_arbitrum_chain(self, arb_config, mock_resolver):
        """Test Arbitrum chain resolution."""
        adapter = CurveAdapter(arb_config, token_resolver=mock_resolver)
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


class TestDeprecatedDictsRemoved:
    """Verify deprecated token dicts have been removed (US-028)."""

    def test_deprecated_dicts_removed(self):
        """Verify deprecated token dicts have been removed (US-028)."""
        import almanak.framework.connectors.curve.adapter as adapter_module

        assert not hasattr(adapter_module, "TOKEN_DECIMALS")
        assert not hasattr(adapter_module, "CURVE_TOKENS")
