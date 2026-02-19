"""Tests for AaveV3Adapter TokenResolver integration.

These tests verify that the AaveV3Adapter correctly uses the TokenResolver
for token resolution.
"""

from unittest.mock import MagicMock

import pytest

from almanak.framework.connectors.aave_v3.adapter import (
    AaveV3Adapter,
    AaveV3Config,
)
from almanak.framework.data.tokens.exceptions import TokenResolutionError
from almanak.framework.data.tokens.models import ResolvedToken


TEST_WALLET = "0x1234567890123456789012345678901234567890"


@pytest.fixture
def config():
    """Create an AaveV3Config for testing."""
    return AaveV3Config(
        chain="ethereum",
        wallet_address=TEST_WALLET,
        allow_placeholder_prices=True,
    )


@pytest.fixture
def arb_config():
    """Create an AaveV3Config for Arbitrum testing."""
    return AaveV3Config(
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
    """Create an AaveV3Adapter with mock resolver."""
    return AaveV3Adapter(config, token_resolver=mock_resolver)


class TestAaveV3AdapterResolverInit:
    """Test AaveV3Adapter initializes with TokenResolver."""

    def test_custom_resolver_injected(self, config, mock_resolver):
        """Test custom resolver is used when provided."""
        adapter = AaveV3Adapter(config, token_resolver=mock_resolver)
        assert adapter._token_resolver is mock_resolver

    def test_default_resolver_initialized(self, config):
        """Test default resolver is initialized when not provided."""
        adapter = AaveV3Adapter(config)
        assert adapter._token_resolver is not None


class TestAaveV3AdapterResolveAsset:
    """Test _resolve_asset uses TokenResolver."""

    def test_resolve_symbol_via_resolver(self, adapter, mock_resolver):
        """Test symbol resolution via TokenResolver."""
        mock_resolver.resolve.return_value = ResolvedToken(
            symbol="USDC",
            address="0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            decimals=6,
            chain="ethereum",
            chain_id=1,
        )
        result = adapter._resolve_asset("USDC")
        assert result == "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
        mock_resolver.resolve.assert_called_once_with("USDC", "ethereum")

    def test_resolve_address_passthrough(self, adapter, mock_resolver):
        """Test address passthrough (no resolver call)."""
        addr = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
        result = adapter._resolve_asset(addr)
        assert result == addr
        mock_resolver.resolve.assert_not_called()

    def test_resolve_wsteth(self, adapter, mock_resolver):
        """Test wstETH resolution via TokenResolver."""
        mock_resolver.resolve.return_value = ResolvedToken(
            symbol="wstETH",
            address="0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0",
            decimals=18,
            chain="ethereum",
            chain_id=1,
        )
        result = adapter._resolve_asset("wstETH")
        assert result == "0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0"


class TestAaveV3AdapterGetDecimals:
    """Test _get_decimals uses TokenResolver."""

    def test_usdc_decimals_via_resolver(self, adapter, mock_resolver):
        """Test USDC decimals (6) via TokenResolver."""
        mock_resolver.resolve.return_value = ResolvedToken(
            symbol="USDC",
            address="0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            decimals=6,
            chain="ethereum",
            chain_id=1,
        )
        result = adapter._get_decimals("USDC")
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
        result = adapter._get_decimals("WBTC")
        assert result == 8


class TestAaveV3AdapterResolveAssetErrors:
    """Test _resolve_asset raises TokenResolutionError for unknown tokens."""

    def test_resolve_unknown_symbol_raises(self, adapter, mock_resolver):
        """Test that _resolve_asset raises TokenResolutionError for unknown symbol."""
        mock_resolver.resolve.side_effect = TokenResolutionError(
            token="UNKNOWN", chain="ethereum", reason="Not found"
        )
        with pytest.raises(TokenResolutionError):
            adapter._resolve_asset("UNKNOWN")

    def test_resolve_asset_error_preserves_token(self, adapter, mock_resolver):
        """Test that the raised error includes the correct token identifier."""
        mock_resolver.resolve.side_effect = TokenResolutionError(
            token="FAKECOIN", chain="ethereum", reason="Not found"
        )
        with pytest.raises(TokenResolutionError, match="FAKECOIN"):
            adapter._resolve_asset("FAKECOIN")

    def test_resolve_asset_no_resolver_raises(self, config):
        """Test that calling _resolve_asset with no resolver raises AttributeError."""
        adapter = AaveV3Adapter(config)
        adapter._token_resolver = None
        with pytest.raises(AttributeError):
            adapter._resolve_asset("USDC")


class TestAaveV3AdapterGetDecimalsErrors:
    """Test _get_decimals raises TokenResolutionError for unknown tokens."""

    def test_get_decimals_unknown_token_raises(self, adapter, mock_resolver):
        """Test that _get_decimals raises TokenResolutionError for unknown token."""
        mock_resolver.resolve.side_effect = TokenResolutionError(
            token="UNKNOWN", chain="ethereum", reason="Not found"
        )
        with pytest.raises(TokenResolutionError):
            adapter._get_decimals("UNKNOWN")

    def test_get_decimals_error_preserves_token(self, adapter, mock_resolver):
        """Test that the raised error includes the correct token identifier."""
        mock_resolver.resolve.side_effect = TokenResolutionError(
            token="FAKECOIN", chain="ethereum", reason="Not found"
        )
        with pytest.raises(TokenResolutionError, match="FAKECOIN"):
            adapter._get_decimals("FAKECOIN")

    def test_get_decimals_no_resolver_raises(self, config):
        """Test that calling _get_decimals with no resolver raises AttributeError."""
        adapter = AaveV3Adapter(config)
        adapter._token_resolver = None
        with pytest.raises(AttributeError):
            adapter._get_decimals("USDC")


class TestAaveV3MultiChain:
    """Test multi-chain resolution."""

    def test_arbitrum_chain(self, arb_config, mock_resolver):
        """Test Arbitrum chain resolution."""
        adapter = AaveV3Adapter(arb_config, token_resolver=mock_resolver)
        mock_resolver.resolve.return_value = ResolvedToken(
            symbol="USDC",
            address="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            decimals=6,
            chain="arbitrum",
            chain_id=42161,
        )
        result = adapter._resolve_asset("USDC")
        assert result == "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
        mock_resolver.resolve.assert_called_once_with("USDC", "arbitrum")


class TestDeprecatedDictsRemoved:
    """Verify deprecated token dicts have been removed (US-028)."""

    def test_deprecated_dicts_removed(self):
        """Verify deprecated token dicts have been removed (US-028)."""
        import almanak.framework.connectors.aave_v3.adapter as adapter_module

        assert not hasattr(adapter_module, "AAVE_V3_TOKEN_ADDRESSES")
        assert not hasattr(adapter_module, "TOKEN_DECIMALS")
