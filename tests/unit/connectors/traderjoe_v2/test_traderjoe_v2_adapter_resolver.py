"""Tests for TraderJoeV2Adapter TokenResolver integration.

These tests verify that the TraderJoeV2Adapter correctly uses the TokenResolver
for token resolution.
"""

from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.connectors.traderjoe_v2.adapter import (
    TraderJoeV2Adapter,
    TraderJoeV2Config,
)
from almanak.framework.data.tokens.exceptions import TokenResolutionError
from almanak.framework.data.tokens.models import ResolvedToken


TEST_WALLET = "0x1234567890123456789012345678901234567890"
MOCK_RPC_URL = "https://api.avax.network/ext/bc/C/rpc"


@pytest.fixture
def config():
    """Create a TraderJoeV2Config for testing."""
    return TraderJoeV2Config(
        chain="avalanche",
        wallet_address=TEST_WALLET,
        rpc_url=MOCK_RPC_URL,
    )


@pytest.fixture
def mock_resolver():
    """Create a mock TokenResolver."""
    return MagicMock()


@pytest.fixture
def adapter(config, mock_resolver):
    """Create a TraderJoeV2Adapter with mocked SDK and resolver."""
    with patch("almanak.framework.connectors.traderjoe_v2.adapter.TraderJoeV2SDK"):
        return TraderJoeV2Adapter(config, token_resolver=mock_resolver)


class TestTraderJoeAdapterResolverInit:
    """Test TraderJoeV2Adapter initializes with TokenResolver."""

    @patch("almanak.framework.connectors.traderjoe_v2.adapter.TraderJoeV2SDK")
    def test_custom_resolver_injected(self, mock_sdk_class, config, mock_resolver):
        """Test that a custom resolver is used when provided."""
        adapter = TraderJoeV2Adapter(config, token_resolver=mock_resolver)
        assert adapter._token_resolver is mock_resolver

    @patch("almanak.framework.connectors.traderjoe_v2.adapter.TraderJoeV2SDK")
    def test_default_resolver_initialized(self, mock_sdk_class, config):
        """Test that default singleton resolver is used when none provided."""
        mock_resolver_instance = MagicMock()
        with patch(
            "almanak.framework.data.tokens.resolver.get_token_resolver",
            return_value=mock_resolver_instance,
        ):
            adapter = TraderJoeV2Adapter(config)
            assert adapter._token_resolver is mock_resolver_instance

    @patch("almanak.framework.connectors.traderjoe_v2.adapter.TraderJoeV2SDK")
    def test_resolver_none_when_init_fails(self, mock_sdk_class, config):
        """Test that adapter construction fails when resolver init fails."""
        with patch(
            "almanak.framework.data.tokens.resolver.get_token_resolver",
            side_effect=Exception("import failed"),
        ):
            with pytest.raises(Exception, match="import failed"):
                TraderJoeV2Adapter(config)


class TestResolveTokenAddressWithResolver:
    """Test token address resolution via TokenResolver."""

    def test_resolve_symbol_via_resolver(self, adapter, mock_resolver):
        """Test resolving symbol uses TokenResolver."""
        mock_resolver.resolve.return_value = ResolvedToken(
            symbol="WAVAX",
            address="0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7",
            decimals=18,
            chain="avalanche",
            chain_id=43114,
        )

        result = adapter.resolve_token_address("WAVAX")
        assert result == "0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7"
        mock_resolver.resolve.assert_called_once_with("WAVAX", "avalanche")

    def test_resolve_address_passthrough(self, adapter, mock_resolver):
        """Test that address input bypasses resolver and returns checksummed."""
        address = "0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7"
        result = adapter.resolve_token_address(address)
        assert result == "0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7"
        mock_resolver.resolve.assert_not_called()

    def test_resolve_unknown_token_raises(self, adapter, mock_resolver):
        """Test unknown token raises TokenResolutionError."""
        mock_resolver.resolve.side_effect = TokenResolutionError(
            token="UNKNOWN_TOKEN", chain="avalanche", reason="Not found"
        )

        with pytest.raises(TokenResolutionError):
            adapter.resolve_token_address("UNKNOWN_TOKEN")


class TestGetDecimalsWithResolver:
    """Test token decimals resolution via TokenResolver."""

    def test_get_decimals_via_resolver(self, adapter, mock_resolver):
        """Test getting decimals uses TokenResolver."""
        mock_resolver.resolve.return_value = ResolvedToken(
            symbol="USDC",
            address="0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E",
            decimals=6,
            chain="avalanche",
            chain_id=43114,
        )

        result = adapter.get_token_decimals("USDC")
        assert result == 6

    def test_get_decimals_wavax_18(self, adapter, mock_resolver):
        """Test WAVAX returns 18 decimals via resolver."""
        mock_resolver.resolve.return_value = ResolvedToken(
            symbol="WAVAX",
            address="0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7",
            decimals=18,
            chain="avalanche",
            chain_id=43114,
        )

        result = adapter.get_token_decimals("WAVAX")
        assert result == 18

    def test_get_decimals_btcb_8(self, adapter, mock_resolver):
        """Test BTC.b returns 8 decimals via resolver."""
        mock_resolver.resolve.return_value = ResolvedToken(
            symbol="BTC.b",
            address="0x152b9d0FdC40C096757F570A51E494bd4b943E50",
            decimals=8,
            chain="avalanche",
            chain_id=43114,
        )

        result = adapter.get_token_decimals("BTC.b")
        assert result == 8

    def test_get_decimals_unknown_raises_error(self, adapter, mock_resolver):
        """Test unknown token raises TokenResolutionError instead of defaulting to 18."""
        mock_resolver.resolve.side_effect = TokenResolutionError(
            token="UNKNOWN_TOKEN", chain="avalanche", reason="Not found"
        )
        with pytest.raises(TokenResolutionError):
            adapter.get_token_decimals("UNKNOWN_TOKEN")


class TestBridgedTokenResolution:
    """Test bridged token resolution (WETH.e, BTC.b)."""

    def test_resolve_weth_e_via_resolver(self, adapter, mock_resolver):
        """Test WETH.e resolves via TokenResolver."""
        mock_resolver.resolve.return_value = ResolvedToken(
            symbol="WETH.e",
            address="0x49D5c2BdFfac6CE2BFdB6640F4F80f226bc10bAB",
            decimals=18,
            chain="avalanche",
            chain_id=43114,
        )

        result = adapter.resolve_token_address("WETH.e")
        assert result == "0x49D5c2BdFfac6CE2BFdB6640F4F80f226bc10bAB"

    def test_resolve_btcb_via_resolver(self, adapter, mock_resolver):
        """Test BTC.b resolves via TokenResolver."""
        mock_resolver.resolve.return_value = ResolvedToken(
            symbol="BTC.b",
            address="0x152b9d0FdC40C096757F570A51E494bd4b943E50",
            decimals=8,
            chain="avalanche",
            chain_id=43114,
        )

        result = adapter.resolve_token_address("BTC.b")
        assert result == "0x152b9d0FdC40C096757F570A51E494bd4b943E50"

    def test_resolve_joe_via_resolver(self, adapter, mock_resolver):
        """Test JOE token resolves via TokenResolver."""
        mock_resolver.resolve.return_value = ResolvedToken(
            symbol="JOE",
            address="0x6e84a6216eA6dACC71eE8E6b0a5B7322EEbC0fDd",
            decimals=18,
            chain="avalanche",
            chain_id=43114,
        )

        result = adapter.resolve_token_address("JOE")
        assert result == "0x6e84a6216eA6dACC71eE8E6b0a5B7322EEbC0fDd"


class TestDeprecatedDictsRemoved:
    """Verify deprecated token dicts have been removed (US-028)."""

    def test_deprecated_dicts_removed(self):
        """Verify deprecated token dicts have been removed (US-028)."""
        import almanak.framework.connectors.traderjoe_v2.adapter as adapter_module

        assert not hasattr(adapter_module, "TOKEN_DECIMALS")
        assert not hasattr(adapter_module, "TRADERJOE_V2_TOKENS")
