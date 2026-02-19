"""Tests for CompoundV3Adapter TokenResolver integration.

These tests verify that the CompoundV3Adapter correctly uses the TokenResolver
for token resolution.
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.data.tokens.exceptions import TokenResolutionError
from almanak.framework.connectors.compound_v3.adapter import (
    CompoundV3Adapter,
    CompoundV3Config,
)
from almanak.framework.data.tokens.models import ResolvedToken


TEST_WALLET = "0x1234567890123456789012345678901234567890"


@pytest.fixture
def config():
    """Create a CompoundV3Config for Ethereum USDC market testing."""
    return CompoundV3Config(
        chain="ethereum",
        wallet_address=TEST_WALLET,
        market="usdc",
    )


@pytest.fixture
def arbitrum_config():
    """Create a CompoundV3Config for Arbitrum USDC market testing."""
    return CompoundV3Config(
        chain="arbitrum",
        wallet_address=TEST_WALLET,
        market="usdc",
    )


@pytest.fixture
def mock_resolver():
    """Create a mock TokenResolver."""
    return MagicMock()


@pytest.fixture
def adapter(config, mock_resolver):
    """Create a CompoundV3Adapter with mock resolver."""
    return CompoundV3Adapter(config, token_resolver=mock_resolver)


@pytest.fixture
def arbitrum_adapter(arbitrum_config, mock_resolver):
    """Create a CompoundV3Adapter for Arbitrum with mock resolver."""
    return CompoundV3Adapter(arbitrum_config, token_resolver=mock_resolver)


class TestCompoundV3AdapterResolverInit:
    """Test CompoundV3Adapter initializes with TokenResolver."""

    def test_custom_resolver_injected(self, config, mock_resolver):
        """Test that a custom resolver is used when provided."""
        adapter = CompoundV3Adapter(config, token_resolver=mock_resolver)
        assert adapter._token_resolver is mock_resolver

    def test_default_resolver_initialized(self, config):
        """Test that default singleton resolver is used when none provided."""
        mock_resolver_instance = MagicMock()
        with patch(
            "almanak.framework.data.tokens.resolver.get_token_resolver",
            return_value=mock_resolver_instance,
        ):
            adapter = CompoundV3Adapter(config)
            assert adapter._token_resolver is mock_resolver_instance

    def test_resolver_none_when_init_fails(self, config):
        """Test that adapter construction fails when resolver init fails."""
        with patch(
            "almanak.framework.data.tokens.resolver.get_token_resolver",
            side_effect=Exception("import failed"),
        ):
            with pytest.raises(Exception, match="import failed"):
                CompoundV3Adapter(config)


class TestResolveTokenAddressWithResolver:
    """Test token address resolution via TokenResolver."""

    def test_resolve_symbol_via_resolver(self, adapter, mock_resolver):
        """Test resolving symbol uses TokenResolver."""
        mock_resolver.resolve.return_value = ResolvedToken(
            symbol="USDC",
            address="0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            decimals=6,
            chain="ethereum",
            chain_id=1,
        )

        result = adapter._resolve_token_address("USDC")
        assert result == "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
        mock_resolver.resolve.assert_called_once_with("USDC", "ethereum")

    def test_resolve_unknown_token_raises_error(self, adapter, mock_resolver):
        """Test unknown token raises TokenResolutionError."""
        mock_resolver.resolve.side_effect = TokenResolutionError(
            token="UNKNOWN_TOKEN", chain="ethereum", reason="Not found"
        )
        with pytest.raises(TokenResolutionError):
            adapter._resolve_token_address("UNKNOWN_TOKEN")

    def test_resolve_wsteth_via_resolver(self, adapter, mock_resolver):
        """Test resolving wstETH via TokenResolver."""
        mock_resolver.resolve.return_value = ResolvedToken(
            symbol="wstETH",
            address="0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0",
            decimals=18,
            chain="ethereum",
            chain_id=1,
        )

        result = adapter._resolve_token_address("wstETH")
        assert result == "0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0"

    def test_resolve_arbitrum_usdc(self, arbitrum_adapter, mock_resolver):
        """Test resolving USDC on Arbitrum via TokenResolver."""
        mock_resolver.resolve.return_value = ResolvedToken(
            symbol="USDC",
            address="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            decimals=6,
            chain="arbitrum",
            chain_id=42161,
        )

        result = arbitrum_adapter._resolve_token_address("USDC")
        assert result == "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
        mock_resolver.resolve.assert_called_once_with("USDC", "arbitrum")


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

    def test_get_decimals_weth_18(self, adapter, mock_resolver):
        """Test WETH returns 18 decimals via resolver."""
        mock_resolver.resolve.return_value = ResolvedToken(
            symbol="WETH",
            address="0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
            decimals=18,
            chain="ethereum",
            chain_id=1,
        )

        result = adapter._get_decimals("WETH")
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


class TestApproveWithResolver:
    """Test approve operations use TokenResolver for token resolution."""

    def test_approve_uses_resolver_for_unknown_token(self, adapter, mock_resolver):
        """Test build_approve_transaction resolves token via TokenResolver when not in market config."""
        mock_resolver.resolve.return_value = ResolvedToken(
            symbol="COMP",
            address="0xc00e94Cb662C3520282E6f5717214004A7f26888",
            decimals=18,
            chain="ethereum",
            chain_id=1,
        )

        # COMP is in COMPOUND_V3_TOKEN_ADDRESSES but NOT in USDC market collaterals
        # (it is in the collaterals dict for USDC market, so this will hit the collateral path)
        # Use a token that is NOT a collateral for the USDC market
        # Actually COMP IS in USDC market collaterals on ethereum, so let's use sUSDe which is
        # not a collateral for the USDC market
        mock_resolver.resolve.return_value = ResolvedToken(
            symbol="sUSDe",
            address="0x9D39A5DE30e57443BfF2A8307A4256c8797A3497",
            decimals=18,
            chain="ethereum",
            chain_id=1,
        )

        result = adapter.build_approve_transaction(token="sUSDe")
        assert result.success is True
        assert result.tx_data is not None
        # Verify resolver was called for _resolve_token_address
        mock_resolver.resolve.assert_called()

    def test_approve_base_token_does_not_use_resolver(self, adapter, mock_resolver):
        """Test that approving the base token uses market config directly, not resolver."""
        result = adapter.build_approve_transaction(token="USDC")
        assert result.success is True
        # Resolver should not be called for _resolve_token_address (base token path)
        # But _get_decimals may call it if amount is specified
        mock_resolver.resolve.assert_not_called()


class TestSupplyWithResolver:
    """Test supply operations use TokenResolver for decimals."""

    def test_supply_uses_resolver_for_decimals(self, adapter, mock_resolver):
        """Test supply resolves token decimals via TokenResolver."""
        mock_resolver.resolve.return_value = ResolvedToken(
            symbol="USDC",
            address="0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            decimals=6,
            chain="ethereum",
            chain_id=1,
        )

        result = adapter.supply(amount=Decimal("1000"))
        assert result.success is True
        assert result.tx_data is not None
        mock_resolver.resolve.assert_called()


class TestDeprecatedDictsRemoved:
    """Verify deprecated token dicts have been removed (US-028)."""

    def test_deprecated_dicts_removed(self):
        """Verify deprecated token dicts have been removed (US-028)."""
        import almanak.framework.connectors.compound_v3.adapter as adapter_module

        assert not hasattr(adapter_module, "COMPOUND_V3_TOKEN_ADDRESSES")
        assert not hasattr(adapter_module, "TOKEN_DECIMALS")
