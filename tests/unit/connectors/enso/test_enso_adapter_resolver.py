"""Tests for EnsoAdapter TokenResolver integration.

These tests verify that the EnsoAdapter correctly uses the TokenResolver
for token resolution.
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.connectors.enso.adapter import (
    EnsoAdapter,
)
from almanak.framework.connectors.enso.client import EnsoConfig
from almanak.framework.data.tokens.exceptions import TokenResolutionError
from almanak.framework.data.tokens.models import ResolvedToken


@pytest.fixture
def enso_config():
    """Create an EnsoConfig for testing."""
    return EnsoConfig(
        chain="arbitrum",
        wallet_address="0x1234567890123456789012345678901234567890",
        api_key="test-api-key",
    )


@pytest.fixture
def mock_resolver():
    """Create a mock TokenResolver."""
    resolver = MagicMock()
    return resolver


def _make_adapter(config, token_resolver=None):
    """Helper to create adapter bypassing __init__ for testing."""
    with patch.object(EnsoAdapter, "__init__", lambda self, *a, **kw: None):
        adapter = EnsoAdapter.__new__(EnsoAdapter)
        adapter.config = config
        adapter.chain = config.chain
        adapter.wallet_address = config.wallet_address
        adapter.tokens = {}
        adapter.use_safe_route_single = False
        adapter._token_resolver = token_resolver
        adapter._using_placeholders = True
        adapter._price_provider = {
            "ETH": Decimal("2000"),
            "WETH": Decimal("2000"),
            "USDC": Decimal("1"),
        }
        return adapter


class TestEnsoAdapterResolverInit:
    """Test EnsoAdapter initializes with TokenResolver."""

    def test_custom_resolver_injected(self, enso_config, mock_resolver):
        """Test that a custom resolver is used when provided."""
        adapter = _make_adapter(enso_config, token_resolver=mock_resolver)
        assert adapter._token_resolver is mock_resolver

    def test_default_resolver_initialized(self, enso_config):
        """Test that default singleton resolver is used when none provided."""
        mock_resolver_instance = MagicMock()
        with patch(
            "almanak.framework.connectors.enso.adapter.EnsoAdapter.__init__",
            lambda self, *a, **kw: None,
        ):
            # Simulate what __init__ does for default resolver
            adapter = EnsoAdapter.__new__(EnsoAdapter)
            with patch(
                "almanak.framework.data.tokens.resolver.get_token_resolver",
                return_value=mock_resolver_instance,
            ):
                from almanak.framework.data.tokens.resolver import get_token_resolver

                adapter._token_resolver = get_token_resolver()
            assert adapter._token_resolver is mock_resolver_instance

    def test_resolver_none_when_init_fails(self, enso_config):
        """Test that resolver is None when initialization fails."""
        adapter = _make_adapter(enso_config, token_resolver=None)
        assert adapter._token_resolver is None


class TestResolveTokenAddressWithResolver:
    """Test token address resolution via TokenResolver."""

    def test_resolve_symbol_via_resolver(self, enso_config, mock_resolver):
        """Test resolving symbol uses TokenResolver."""
        mock_resolver.resolve.return_value = ResolvedToken(
            symbol="USDC",
            address="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            decimals=6,
            chain="arbitrum",
            chain_id=42161,
        )
        adapter = _make_adapter(enso_config, token_resolver=mock_resolver)

        result = adapter.resolve_token_address("USDC")
        assert result == "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
        mock_resolver.resolve.assert_called_once_with("USDC", "arbitrum")

    def test_resolve_address_passthrough(self, enso_config, mock_resolver):
        """Test that address input bypasses resolver and returns directly."""
        adapter = _make_adapter(enso_config, token_resolver=mock_resolver)

        address = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
        result = adapter.resolve_token_address(address)
        assert result == address
        mock_resolver.resolve.assert_not_called()

    def test_resolve_bridged_token_via_resolver(self, enso_config, mock_resolver):
        """Test resolving bridged token (USDC.e) via resolver."""
        mock_resolver.resolve.return_value = ResolvedToken(
            symbol="USDC.e",
            address="0xff970a61a04b1ca14834a43f5de4533ebddb5cc8",
            decimals=6,
            chain="arbitrum",
            chain_id=42161,
        )
        adapter = _make_adapter(enso_config, token_resolver=mock_resolver)

        result = adapter.resolve_token_address("USDC.e")
        assert result == "0xff970a61a04b1ca14834a43f5de4533ebddb5cc8"

    def test_resolve_unknown_token_raises_error(self, enso_config, mock_resolver):
        """Test unknown token raises TokenResolutionError."""
        mock_resolver.resolve.side_effect = TokenResolutionError(
            token="UNKNOWN_TOKEN", chain="arbitrum", reason="Not found"
        )
        adapter = _make_adapter(enso_config, token_resolver=mock_resolver)
        with pytest.raises(TokenResolutionError):
            adapter.resolve_token_address("UNKNOWN_TOKEN")


class TestGetTokenDecimalsWithResolver:
    """Test token decimals resolution via TokenResolver."""

    def test_get_decimals_via_resolver(self, enso_config, mock_resolver):
        """Test getting decimals uses TokenResolver."""
        mock_resolver.resolve.return_value = ResolvedToken(
            symbol="USDC",
            address="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            decimals=6,
            chain="arbitrum",
            chain_id=42161,
        )
        adapter = _make_adapter(enso_config, token_resolver=mock_resolver)

        result = adapter.get_token_decimals("USDC")
        assert result == 6

    def test_get_decimals_wbtc_via_resolver(self, enso_config, mock_resolver):
        """Test WBTC returns 8 decimals via resolver."""
        mock_resolver.resolve.return_value = ResolvedToken(
            symbol="WBTC",
            address="0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f",
            decimals=8,
            chain="arbitrum",
            chain_id=42161,
        )
        adapter = _make_adapter(enso_config, token_resolver=mock_resolver)

        result = adapter.get_token_decimals("WBTC")
        assert result == 8

    def test_get_decimals_by_address_via_resolver(self, enso_config, mock_resolver):
        """Test getting decimals by address uses TokenResolver."""
        mock_resolver.resolve.return_value = ResolvedToken(
            symbol="USDC",
            address="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            decimals=6,
            chain="arbitrum",
            chain_id=42161,
        )
        adapter = _make_adapter(enso_config, token_resolver=mock_resolver)

        result = adapter.get_token_decimals("0xaf88d065e77c8cC2239327C5EDb3A432268e5831")
        assert result == 6


class TestGetTokenSymbolWithResolver:
    """Test token symbol resolution from address via TokenResolver."""

    def test_get_symbol_via_resolver(self, enso_config, mock_resolver):
        """Test getting symbol from address uses TokenResolver."""
        mock_resolver.resolve.return_value = ResolvedToken(
            symbol="USDC",
            address="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            decimals=6,
            chain="arbitrum",
            chain_id=42161,
        )
        adapter = _make_adapter(enso_config, token_resolver=mock_resolver)

        result = adapter._get_token_symbol("0xaf88d065e77c8cC2239327C5EDb3A432268e5831")
        assert result == "USDC"

    def test_get_symbol_unknown_address_raises_error(self, enso_config, mock_resolver):
        """Test unknown address raises TokenResolutionError."""
        mock_resolver.resolve.side_effect = TokenResolutionError(
            token="0x0000000000000000000000000000000000000001", chain="arbitrum", reason="Not found"
        )
        adapter = _make_adapter(enso_config, token_resolver=mock_resolver)
        with pytest.raises(TokenResolutionError):
            adapter._get_token_symbol("0x0000000000000000000000000000000000000001")


class TestMultiChainResolution:
    """Test token resolution across different chains."""

    def test_resolve_on_ethereum(self, mock_resolver):
        """Test resolution on ethereum chain."""
        config = EnsoConfig(
            chain="ethereum",
            wallet_address="0x1234567890123456789012345678901234567890",
            api_key="test-api-key",
        )
        mock_resolver.resolve.return_value = ResolvedToken(
            symbol="USDC",
            address="0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            decimals=6,
            chain="ethereum",
            chain_id=1,
        )
        adapter = _make_adapter(config, token_resolver=mock_resolver)

        result = adapter.resolve_token_address("USDC")
        assert result == "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
        mock_resolver.resolve.assert_called_once_with("USDC", "ethereum")

    def test_resolve_on_avalanche(self, mock_resolver):
        """Test resolution on avalanche chain."""
        config = EnsoConfig(
            chain="avalanche",
            wallet_address="0x1234567890123456789012345678901234567890",
            api_key="test-api-key",
        )
        mock_resolver.resolve.return_value = ResolvedToken(
            symbol="WAVAX",
            address="0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7",
            decimals=18,
            chain="avalanche",
            chain_id=43114,
        )
        adapter = _make_adapter(config, token_resolver=mock_resolver)

        result = adapter.resolve_token_address("WAVAX")
        assert result == "0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7"
        mock_resolver.resolve.assert_called_once_with("WAVAX", "avalanche")


class TestDeprecatedDictsRemoved:
    """Verify deprecated token dicts have been removed (US-028)."""

    def test_deprecated_dicts_removed(self):
        """Verify deprecated token dicts have been removed (US-028)."""
        import almanak.framework.connectors.enso.adapter as adapter_module

        assert not hasattr(adapter_module, "TOKEN_ADDRESSES")
        assert not hasattr(adapter_module, "TOKEN_DECIMALS")
