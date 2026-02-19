"""Tests for PancakeSwapV3Adapter TokenResolver integration.

These tests verify that the PancakeSwapV3Adapter correctly uses the TokenResolver
for token resolution.
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.connectors.pancakeswap_v3.adapter import (
    PancakeSwapV3Adapter,
    PancakeSwapV3Config,
)
from almanak.framework.data.tokens.exceptions import TokenResolutionError
from almanak.framework.data.tokens.models import ResolvedToken


@pytest.fixture
def config_bnb():
    """Create a PancakeSwapV3Config for BNB chain testing."""
    return PancakeSwapV3Config(
        chain="bnb",
        wallet_address="0x1234567890123456789012345678901234567890",
        allow_placeholder_prices=True,
    )


@pytest.fixture
def config_ethereum():
    """Create a PancakeSwapV3Config for Ethereum chain testing."""
    return PancakeSwapV3Config(
        chain="ethereum",
        wallet_address="0x1234567890123456789012345678901234567890",
        allow_placeholder_prices=True,
    )


@pytest.fixture
def config_arbitrum():
    """Create a PancakeSwapV3Config for Arbitrum chain testing."""
    return PancakeSwapV3Config(
        chain="arbitrum",
        wallet_address="0x1234567890123456789012345678901234567890",
        allow_placeholder_prices=True,
    )


@pytest.fixture
def mock_resolver():
    """Create a mock TokenResolver."""
    return MagicMock()


class TestPancakeSwapAdapterResolverInit:
    """Test PancakeSwapV3Adapter initializes with TokenResolver."""

    def test_custom_resolver_injected(self, config_bnb, mock_resolver):
        """Test that a custom resolver is used when provided."""
        adapter = PancakeSwapV3Adapter(config_bnb, token_resolver=mock_resolver)
        assert adapter._token_resolver is mock_resolver

    def test_default_resolver_initialized(self, config_bnb):
        """Test that default singleton resolver is used when none provided."""
        mock_resolver_instance = MagicMock()
        with patch(
            "almanak.framework.data.tokens.resolver.get_token_resolver",
            return_value=mock_resolver_instance,
        ):
            adapter = PancakeSwapV3Adapter(config_bnb)
            assert adapter._token_resolver is mock_resolver_instance

    def test_resolver_none_when_init_fails(self, config_bnb):
        """Test that adapter construction fails when resolver init fails."""
        with patch(
            "almanak.framework.data.tokens.resolver.get_token_resolver",
            side_effect=Exception("import failed"),
        ):
            with pytest.raises(Exception, match="import failed"):
                PancakeSwapV3Adapter(config_bnb)


class TestResolveTokenWithResolver:
    """Test token address resolution via TokenResolver."""

    def test_resolve_symbol_via_resolver(self, config_bnb, mock_resolver):
        """Test resolving symbol uses TokenResolver."""
        mock_resolver.resolve.return_value = ResolvedToken(
            symbol="USDT",
            address="0x55d398326f99059fF775485246999027B3197955",
            decimals=18,
            chain="bnb",
            chain_id=56,
        )
        adapter = PancakeSwapV3Adapter(config_bnb, token_resolver=mock_resolver)

        result = adapter._resolve_token("USDT")
        assert result == "0x55d398326f99059fF775485246999027B3197955"
        mock_resolver.resolve.assert_called_once_with("USDT", "bnb")

    def test_resolve_address_passthrough(self, config_bnb, mock_resolver):
        """Test that address input bypasses resolver and returns directly."""
        adapter = PancakeSwapV3Adapter(config_bnb, token_resolver=mock_resolver)

        address = "0x55d398326f99059fF775485246999027B3197955"
        result = adapter._resolve_token(address)
        assert result == address
        mock_resolver.resolve.assert_not_called()

    def test_resolve_unknown_token_raises_error(self, config_bnb, mock_resolver):
        """Test unknown token raises TokenResolutionError."""
        mock_resolver.resolve.side_effect = TokenResolutionError(
            token="UNKNOWN_TOKEN", chain="bnb", reason="Not found"
        )
        adapter = PancakeSwapV3Adapter(config_bnb, token_resolver=mock_resolver)
        with pytest.raises(TokenResolutionError):
            adapter._resolve_token("UNKNOWN_TOKEN")


class TestGetDecimalsWithResolver:
    """Test token decimals resolution via TokenResolver."""

    def test_get_decimals_via_resolver(self, config_bnb, mock_resolver):
        """Test getting decimals uses TokenResolver."""
        mock_resolver.resolve.return_value = ResolvedToken(
            symbol="USDT",
            address="0x55d398326f99059fF775485246999027B3197955",
            decimals=18,
            chain="bnb",
            chain_id=56,
        )
        adapter = PancakeSwapV3Adapter(config_bnb, token_resolver=mock_resolver)

        result = adapter._get_decimals("USDT")
        assert result == 18

    def test_get_decimals_usdc_6_via_resolver(self, config_ethereum, mock_resolver):
        """Test USDC returns 6 decimals via resolver on Ethereum."""
        mock_resolver.resolve.return_value = ResolvedToken(
            symbol="USDC",
            address="0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            decimals=6,
            chain="ethereum",
            chain_id=1,
        )
        adapter = PancakeSwapV3Adapter(config_ethereum, token_resolver=mock_resolver)

        result = adapter._get_decimals("USDC")
        assert result == 6

    def test_get_decimals_wbtc_8_via_resolver(self, config_ethereum, mock_resolver):
        """Test WBTC returns 8 decimals via resolver."""
        mock_resolver.resolve.return_value = ResolvedToken(
            symbol="WBTC",
            address="0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599",
            decimals=8,
            chain="ethereum",
            chain_id=1,
        )
        adapter = PancakeSwapV3Adapter(config_ethereum, token_resolver=mock_resolver)

        result = adapter._get_decimals("WBTC")
        assert result == 8

    def test_get_decimals_unknown_raises_error(self, config_bnb, mock_resolver):
        """Test unknown token raises TokenResolutionError instead of defaulting to 18."""
        mock_resolver.resolve.side_effect = TokenResolutionError(
            token="UNKNOWN_TOKEN", chain="bnb", reason="Not found"
        )
        adapter = PancakeSwapV3Adapter(config_bnb, token_resolver=mock_resolver)
        with pytest.raises(TokenResolutionError):
            adapter._get_decimals("UNKNOWN_TOKEN")


class TestMultiChainResolution:
    """Test token resolution across different chains."""

    def test_resolve_on_ethereum(self, config_ethereum, mock_resolver):
        """Test resolution on ethereum chain."""
        mock_resolver.resolve.return_value = ResolvedToken(
            symbol="WETH",
            address="0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
            decimals=18,
            chain="ethereum",
            chain_id=1,
        )
        adapter = PancakeSwapV3Adapter(config_ethereum, token_resolver=mock_resolver)

        result = adapter._resolve_token("WETH")
        assert result == "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
        mock_resolver.resolve.assert_called_once_with("WETH", "ethereum")

    def test_resolve_on_arbitrum(self, config_arbitrum, mock_resolver):
        """Test resolution on arbitrum chain."""
        mock_resolver.resolve.return_value = ResolvedToken(
            symbol="USDC",
            address="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            decimals=6,
            chain="arbitrum",
            chain_id=42161,
        )
        adapter = PancakeSwapV3Adapter(config_arbitrum, token_resolver=mock_resolver)

        result = adapter._resolve_token("USDC")
        assert result == "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
        mock_resolver.resolve.assert_called_once_with("USDC", "arbitrum")

    def test_decimals_on_bnb_usdt_18(self, config_bnb, mock_resolver):
        """Test BSC USDT is 18 decimals (unique to BSC)."""
        mock_resolver.resolve.return_value = ResolvedToken(
            symbol="USDT",
            address="0x55d398326f99059fF775485246999027B3197955",
            decimals=18,
            chain="bnb",
            chain_id=56,
        )
        adapter = PancakeSwapV3Adapter(config_bnb, token_resolver=mock_resolver)

        result = adapter._get_decimals("USDT")
        assert result == 18

    def test_decimals_on_arbitrum_usdt_6(self, config_arbitrum, mock_resolver):
        """Test Arbitrum USDT is 6 decimals."""
        mock_resolver.resolve.return_value = ResolvedToken(
            symbol="USDT",
            address="0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9",
            decimals=6,
            chain="arbitrum",
            chain_id=42161,
        )
        adapter = PancakeSwapV3Adapter(config_arbitrum, token_resolver=mock_resolver)

        result = adapter._get_decimals("USDT")
        assert result == 6


class TestSwapWithResolver:
    """Test that swap operations use resolver for token resolution."""

    def test_swap_exact_input_uses_resolver(self, config_bnb, mock_resolver):
        """Test swap_exact_input resolves tokens via TokenResolver."""
        mock_resolver.resolve.side_effect = [
            # First call: token_in (USDT) in _resolve_token
            ResolvedToken(
                symbol="USDT",
                address="0x55d398326f99059fF775485246999027B3197955",
                decimals=18,
                chain="bnb",
                chain_id=56,
            ),
            # Second call: token_out (WBNB) in _resolve_token
            ResolvedToken(
                symbol="WBNB",
                address="0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c",
                decimals=18,
                chain="bnb",
                chain_id=56,
            ),
            # Third call: token_in decimals in _get_decimals
            ResolvedToken(
                symbol="USDT",
                address="0x55d398326f99059fF775485246999027B3197955",
                decimals=18,
                chain="bnb",
                chain_id=56,
            ),
            # Fourth+ calls: for slippage calculation decimals
            ResolvedToken(
                symbol="WBNB",
                address="0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c",
                decimals=18,
                chain="bnb",
                chain_id=56,
            ),
        ]
        adapter = PancakeSwapV3Adapter(config_bnb, token_resolver=mock_resolver)

        result = adapter.swap_exact_input(
            token_in="USDT",
            token_out="WBNB",
            amount_in=Decimal("100"),
            amount_out_min=Decimal("0.1"),
        )

        assert result.success is True
        assert result.tx_data is not None
        # Verify resolver was called for token_in and token_out
        assert mock_resolver.resolve.call_count >= 2


class TestDeprecatedDictsRemoved:
    """Verify deprecated token dicts have been removed (US-028)."""

    def test_deprecated_dicts_removed(self):
        """Verify deprecated token dicts have been removed (US-028)."""
        import almanak.framework.connectors.pancakeswap_v3.adapter as adapter_module

        assert not hasattr(adapter_module, "PANCAKESWAP_V3_TOKENS")
        assert not hasattr(adapter_module, "TOKEN_DECIMALS")
