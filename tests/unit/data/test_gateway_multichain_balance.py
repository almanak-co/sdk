"""Tests for MultiChainGatewayBalanceProvider.

Tests the gateway-backed multi-chain balance provider that routes
balance queries through the gateway's MarketService.
"""

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from almanak.framework.data.balance.gateway_multichain import MultiChainGatewayBalanceProvider
from almanak.framework.strategies.intent_strategy import TokenBalance


@pytest.fixture
def mock_gateway_client():
    """Create a mock GatewayClient."""
    client = MagicMock()
    client.market = MagicMock()
    return client


@pytest.fixture
def balance_provider(mock_gateway_client):
    """Create a MultiChainGatewayBalanceProvider."""
    return MultiChainGatewayBalanceProvider(
        client=mock_gateway_client,
        wallet_address="0x1234567890abcdef1234567890abcdef12345678",
        chains=["arbitrum", "base"],
    )


class TestMultiChainGatewayBalanceProvider:
    """Test MultiChainGatewayBalanceProvider."""

    def test_chains_property(self, balance_provider):
        assert balance_provider.chains == ["arbitrum", "base"]

    def test_chains_normalized_to_lowercase(self, mock_gateway_client):
        provider = MultiChainGatewayBalanceProvider(
            client=mock_gateway_client,
            wallet_address="0x1234567890abcdef1234567890abcdef12345678",
            chains=["Arbitrum", "BASE"],
        )
        assert provider.chains == ["arbitrum", "base"]

    def test_get_balance_returns_token_balance(self, balance_provider, mock_gateway_client):
        """get_balance returns a TokenBalance with correct values."""
        response = MagicMock()
        response.balance = "1000.5"
        response.balance_usd = "1000.50"
        mock_gateway_client.market.GetBalance = MagicMock(return_value=response)

        result = balance_provider.get_balance("USDC", "arbitrum")
        assert isinstance(result, TokenBalance)
        assert result.symbol == "USDC"
        assert result.balance == Decimal("1000.5")
        assert result.balance_usd == Decimal("1000.50")

    def test_callable_interface(self, balance_provider, mock_gateway_client):
        """Provider is callable matching MultiChainBalanceProvider type."""
        response = MagicMock()
        response.balance = "2.5"
        response.balance_usd = "5000.00"
        mock_gateway_client.market.GetBalance = MagicMock(return_value=response)

        result = balance_provider("WETH", "base")
        assert isinstance(result, TokenBalance)
        assert result.symbol == "WETH"
        assert result.balance == Decimal("2.5")

    def test_unconfigured_chain_returns_zero(self, balance_provider):
        """Querying an unconfigured chain returns zero balance."""
        result = balance_provider.get_balance("USDC", "ethereum")
        assert result.balance == Decimal("0")
        assert result.balance_usd == Decimal("0")

    def test_gateway_error_returns_zero(self, balance_provider, mock_gateway_client):
        """Gateway errors return zero balance gracefully."""
        mock_gateway_client.market.GetBalance = MagicMock(
            side_effect=Exception("gRPC unavailable")
        )

        result = balance_provider.get_balance("USDC", "arbitrum")
        assert result.balance == Decimal("0")
        assert result.balance_usd == Decimal("0")

    def test_empty_balance_response(self, balance_provider, mock_gateway_client):
        """Empty response fields default to zero."""
        response = MagicMock()
        response.balance = ""
        response.balance_usd = ""
        mock_gateway_client.market.GetBalance = MagicMock(return_value=response)

        result = balance_provider.get_balance("USDC", "arbitrum")
        assert result.balance == Decimal("0")
        assert result.balance_usd == Decimal("0")
