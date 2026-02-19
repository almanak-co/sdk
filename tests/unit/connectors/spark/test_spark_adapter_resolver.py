"""Tests for SparkAdapter TokenResolver integration.

These tests verify that the SparkAdapter correctly uses the TokenResolver
for token resolution.
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.connectors.spark.adapter import (
    SparkAdapter,
    SparkConfig,
)
from almanak.framework.data.tokens.exceptions import TokenResolutionError
from almanak.framework.data.tokens.models import ResolvedToken


TEST_WALLET = "0x1234567890123456789012345678901234567890"


@pytest.fixture
def config():
    """Create a SparkConfig for testing."""
    return SparkConfig(
        chain="ethereum",
        wallet_address=TEST_WALLET,
    )


@pytest.fixture
def mock_resolver():
    """Create a mock TokenResolver."""
    return MagicMock()


@pytest.fixture
def adapter(config, mock_resolver):
    """Create a SparkAdapter with mock resolver."""
    return SparkAdapter(config, token_resolver=mock_resolver)


class TestSparkAdapterResolverInit:
    """Test SparkAdapter initializes with TokenResolver."""

    def test_custom_resolver_injected(self, config, mock_resolver):
        """Test that a custom resolver is used when provided."""
        adapter = SparkAdapter(config, token_resolver=mock_resolver)
        assert adapter._token_resolver is mock_resolver

    def test_default_resolver_initialized(self, config):
        """Test that default singleton resolver is used when none provided."""
        mock_resolver_instance = MagicMock()
        with patch(
            "almanak.framework.data.tokens.resolver.get_token_resolver",
            return_value=mock_resolver_instance,
        ):
            adapter = SparkAdapter(config)
            assert adapter._token_resolver is mock_resolver_instance

    def test_resolver_none_when_init_fails(self, config):
        """Test that adapter construction fails when resolver init fails."""
        with patch(
            "almanak.framework.data.tokens.resolver.get_token_resolver",
            side_effect=Exception("import failed"),
        ):
            with pytest.raises(Exception, match="import failed"):
                SparkAdapter(config)


class TestResolveAssetWithResolver:
    """Test asset address resolution via TokenResolver."""

    def test_resolve_symbol_via_resolver(self, adapter, mock_resolver):
        """Test resolving symbol uses TokenResolver."""
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
        """Test that address input bypasses resolver and returns as-is."""
        address = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
        result = adapter._resolve_asset(address)
        assert result == address
        mock_resolver.resolve.assert_not_called()

    def test_resolve_unknown_token_raises_error(self, adapter, mock_resolver):
        """Test unknown token raises TokenResolutionError."""
        mock_resolver.resolve.side_effect = TokenResolutionError(
            token="UNKNOWN_TOKEN", chain="ethereum", reason="Not found"
        )
        with pytest.raises(TokenResolutionError):
            adapter._resolve_asset("UNKNOWN_TOKEN")

    def test_resolve_dai_via_resolver(self, adapter, mock_resolver):
        """Test resolving DAI via TokenResolver."""
        mock_resolver.resolve.return_value = ResolvedToken(
            symbol="DAI",
            address="0x6B175474E89094C44Da98b954EedeAC495271d0F",
            decimals=18,
            chain="ethereum",
            chain_id=1,
        )

        result = adapter._resolve_asset("DAI")
        assert result == "0x6B175474E89094C44Da98b954EedeAC495271d0F"

    def test_resolve_wsteth_via_resolver(self, adapter, mock_resolver):
        """Test resolving wstETH via TokenResolver."""
        mock_resolver.resolve.return_value = ResolvedToken(
            symbol="wstETH",
            address="0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0",
            decimals=18,
            chain="ethereum",
            chain_id=1,
        )

        result = adapter._resolve_asset("wstETH")
        assert result == "0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0"


class TestGetDecimalsWithResolver:
    """Test token decimals resolution via TokenResolver."""

    def test_get_decimals_usdc_6(self, adapter, mock_resolver):
        """Test USDC returns 6 decimals via resolver."""
        mock_resolver.resolve.return_value = ResolvedToken(
            symbol="USDC",
            address="0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            decimals=6,
            chain="ethereum",
            chain_id=1,
        )

        result = adapter._get_decimals("USDC")
        assert result == 6

    def test_get_decimals_dai_18(self, adapter, mock_resolver):
        """Test DAI returns 18 decimals via resolver."""
        mock_resolver.resolve.return_value = ResolvedToken(
            symbol="DAI",
            address="0x6B175474E89094C44Da98b954EedeAC495271d0F",
            decimals=18,
            chain="ethereum",
            chain_id=1,
        )

        result = adapter._get_decimals("DAI")
        assert result == 18

    def test_get_decimals_wbtc_8(self, adapter, mock_resolver):
        """Test WBTC returns 8 decimals via resolver."""
        mock_resolver.resolve.return_value = ResolvedToken(
            symbol="WBTC",
            address="0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599",
            decimals=8,
            chain="ethereum",
            chain_id=1,
        )

        result = adapter._get_decimals("WBTC")
        assert result == 8

    def test_get_decimals_usdt_6(self, adapter, mock_resolver):
        """Test USDT returns 6 decimals via resolver."""
        mock_resolver.resolve.return_value = ResolvedToken(
            symbol="USDT",
            address="0xdAC17F958D2ee523a2206206994597C13D831ec7",
            decimals=6,
            chain="ethereum",
            chain_id=1,
        )

        result = adapter._get_decimals("USDT")
        assert result == 6

    def test_get_decimals_unknown_raises_error(self, adapter, mock_resolver):
        """Test unknown token raises TokenResolutionError instead of defaulting to 18."""
        mock_resolver.resolve.side_effect = TokenResolutionError(
            token="UNKNOWN_TOKEN", chain="ethereum", reason="Not found"
        )
        with pytest.raises(TokenResolutionError):
            adapter._get_decimals("UNKNOWN_TOKEN")


class TestSupplyWithResolver:
    """Test supply operations use TokenResolver for token resolution."""

    def test_supply_uses_resolver_for_address(self, adapter, mock_resolver):
        """Test supply resolves token address via TokenResolver."""
        mock_resolver.resolve.return_value = ResolvedToken(
            symbol="USDC",
            address="0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            decimals=6,
            chain="ethereum",
            chain_id=1,
        )

        result = adapter.supply("USDC", Decimal("1000"))
        assert result.success is True
        assert result.tx_data is not None
        # Verify resolver was called for both _resolve_asset and _get_decimals
        assert mock_resolver.resolve.call_count == 2


class TestDeprecatedDictsRemoved:
    """Verify deprecated token dicts have been removed (US-028)."""

    def test_deprecated_dicts_removed(self):
        """Verify deprecated token dicts have been removed (US-028)."""
        import almanak.framework.connectors.spark.adapter as adapter_module

        assert not hasattr(adapter_module, "SPARK_TOKEN_ADDRESSES")
        assert not hasattr(adapter_module, "TOKEN_DECIMALS")
