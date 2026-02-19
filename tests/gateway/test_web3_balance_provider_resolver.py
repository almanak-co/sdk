"""Tests for Web3BalanceProvider integration with TokenResolver.

Verifies that the provider uses TokenResolver as the sole source of truth
for token resolution (no local fallback registry).
"""

from unittest.mock import MagicMock, patch

import pytest

from almanak.gateway.data.balance.web3_provider import (
    TokenMetadata,
    Web3BalanceProvider,
)


class TestWeb3BalanceProviderTokenResolver:
    """Tests for TokenResolver integration in Web3BalanceProvider."""

    @pytest.fixture
    def mock_resolver(self):
        """Create a mock TokenResolver."""
        from almanak.framework.data.tokens.models import ResolvedToken

        resolver = MagicMock()

        # Default: resolve WETH on arbitrum
        weth_resolved = ResolvedToken(
            symbol="WETH",
            address="0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
            decimals=18,
            chain="arbitrum",
            chain_id=42161,
            is_native=False,
            is_wrapped_native=True,
            source="static",
        )
        resolver.resolve.return_value = weth_resolved
        return resolver

    @pytest.fixture
    def provider(self, mock_resolver):
        """Create provider with mock resolver."""
        return Web3BalanceProvider(
            rpc_url="http://localhost:8545",
            wallet_address="0x0000000000000000000000000000000000000001",
            chain="arbitrum",
            token_resolver=mock_resolver,
        )

    def test_init_with_custom_resolver(self, mock_resolver):
        """Provider accepts custom token_resolver parameter."""
        provider = Web3BalanceProvider(
            rpc_url="http://localhost:8545",
            wallet_address="0x0000000000000000000000000000000000000001",
            chain="arbitrum",
            token_resolver=mock_resolver,
        )
        assert provider._token_resolver is mock_resolver

    def test_init_with_default_resolver(self):
        """Provider uses get_token_resolver() when no resolver provided."""
        mock_resolver = MagicMock()
        with patch(
            "almanak.framework.data.tokens.resolver.get_token_resolver",
            return_value=mock_resolver,
        ):
            provider = Web3BalanceProvider(
                rpc_url="http://localhost:8545",
                wallet_address="0x0000000000000000000000000000000000000001",
                chain="arbitrum",
            )
            assert provider._token_resolver is mock_resolver

    def test_resolve_token_uses_resolver(self, provider, mock_resolver):
        """_resolve_token delegates to TokenResolver.resolve()."""
        result = provider._resolve_token("WETH")

        mock_resolver.resolve.assert_called_once_with("WETH", "arbitrum")
        assert result is not None
        assert result.symbol == "WETH"
        assert result.decimals == 18
        assert result.address == "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"

    def test_resolve_token_by_address(self, provider, mock_resolver):
        """_resolve_token resolves addresses via TokenResolver."""
        from almanak.framework.data.tokens.models import ResolvedToken

        usdc_resolved = ResolvedToken(
            symbol="USDC",
            address="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            decimals=6,
            chain="arbitrum",
            chain_id=42161,
            source="static",
        )
        mock_resolver.resolve.return_value = usdc_resolved

        result = provider._resolve_token("0xaf88d065e77c8cC2239327C5EDb3A432268e5831")

        assert result is not None
        assert result.symbol == "USDC"
        assert result.decimals == 6

    def test_resolve_token_returns_token_metadata(self, provider, mock_resolver):
        """_resolve_token converts ResolvedToken to TokenMetadata."""
        result = provider._resolve_token("WETH")

        assert isinstance(result, TokenMetadata)
        assert result.symbol == "WETH"
        assert result.decimals == 18
        assert result.is_native is False

    def test_resolve_native_token(self, provider, mock_resolver):
        """_resolve_token correctly handles native tokens."""
        from almanak.framework.data.tokens.models import ResolvedToken

        eth_resolved = ResolvedToken(
            symbol="ETH",
            address="0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE",
            decimals=18,
            chain="arbitrum",
            chain_id=42161,
            is_native=True,
            source="static",
        )
        mock_resolver.resolve.return_value = eth_resolved

        result = provider._resolve_token("ETH")

        assert result is not None
        assert result.is_native is True
        assert result.symbol == "ETH"

    def test_resolve_usdc_correct_decimals(self, provider, mock_resolver):
        """_resolve_token returns correct decimals for USDC (6, not 18)."""
        from almanak.framework.data.tokens.models import ResolvedToken

        usdc_resolved = ResolvedToken(
            symbol="USDC",
            address="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            decimals=6,
            chain="arbitrum",
            chain_id=42161,
            source="static",
        )
        mock_resolver.resolve.return_value = usdc_resolved

        result = provider._resolve_token("USDC")

        assert result is not None
        assert result.decimals == 6  # NEVER default to 18

    def test_resolve_unknown_token_returns_none(self, provider, mock_resolver):
        """_resolve_token returns None for unknown tokens."""
        from almanak.framework.data.tokens.exceptions import TokenNotFoundError

        mock_resolver.resolve.side_effect = TokenNotFoundError("UNKNOWN_TOKEN", "arbitrum")

        result = provider._resolve_token("UNKNOWN_TOKEN")

        assert result is None

    def test_resolve_unknown_address_returns_none(self, provider, mock_resolver):
        """_resolve_token returns None for unknown addresses."""
        from almanak.framework.data.tokens.exceptions import TokenNotFoundError

        unknown_addr = "0x1111111111111111111111111111111111111111"
        mock_resolver.resolve.side_effect = TokenNotFoundError(unknown_addr, "arbitrum")

        result = provider._resolve_token(unknown_addr)

        assert result is None

    def test_resolver_failure_returns_none(self, provider, mock_resolver):
        """_resolve_token returns None if resolver raises (no fallback registry)."""
        mock_resolver.resolve.side_effect = Exception("resolver unavailable")

        result = provider._resolve_token("WETH")

        # No fallback - returns None when resolver fails
        assert result is None

    def test_no_local_token_registry(self, provider):
        """Provider no longer has a local _token_registry attribute."""
        assert not hasattr(provider, "_token_registry")

    def test_token_registry_removed_from_module(self):
        """TOKEN_REGISTRY is no longer exported from web3_provider module."""
        import almanak.gateway.data.balance.web3_provider as mod

        assert not hasattr(mod, "TOKEN_REGISTRY")

    def test_resolve_bridged_token(self, provider, mock_resolver):
        """_resolve_token handles bridged tokens like USDC.e."""
        from almanak.framework.data.tokens.models import ResolvedToken

        usdc_e_resolved = ResolvedToken(
            symbol="USDC.e",
            address="0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8",
            decimals=6,
            chain="arbitrum",
            chain_id=42161,
            source="static",
        )
        mock_resolver.resolve.return_value = usdc_e_resolved

        result = provider._resolve_token("USDC.e")

        assert result is not None
        assert result.symbol == "USDC.e"
        assert result.decimals == 6

    def test_resolve_case_insensitive(self, provider, mock_resolver):
        """_resolve_token works with different cases."""
        result = provider._resolve_token("weth")

        # TokenResolver.resolve should be called with the original token
        mock_resolver.resolve.assert_called_once_with("weth", "arbitrum")

    def test_add_token_registers_with_resolver(self, provider, mock_resolver):
        """add_token() registers tokens with the TokenResolver."""
        provider.add_token(
            symbol="CUSTOM",
            address="0x0000000000000000000000000000000000000042",
            decimals=18,
        )

        mock_resolver.register.assert_called_once()
        registered = mock_resolver.register.call_args[0][0]
        assert registered.symbol == "CUSTOM"
        assert registered.decimals == 18
        from almanak.core.enums import Chain

        assert registered.chain == Chain.ARBITRUM


class TestWeb3BalanceProviderMultiChain:
    """Test resolver integration across multiple chains."""

    def test_ethereum_chain(self):
        """Provider works with ethereum chain."""
        mock_resolver = MagicMock()
        from almanak.framework.data.tokens.models import ResolvedToken

        mock_resolver.resolve.return_value = ResolvedToken(
            symbol="WETH",
            address="0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
            decimals=18,
            chain="ethereum",
            chain_id=1,
            source="static",
        )

        provider = Web3BalanceProvider(
            rpc_url="http://localhost:8545",
            wallet_address="0x0000000000000000000000000000000000000001",
            chain="ethereum",
            token_resolver=mock_resolver,
        )

        result = provider._resolve_token("WETH")
        mock_resolver.resolve.assert_called_once_with("WETH", "ethereum")
        assert result is not None
        assert result.address == "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"

    def test_avalanche_chain(self):
        """Provider works with avalanche chain."""
        mock_resolver = MagicMock()
        from almanak.framework.data.tokens.models import ResolvedToken

        mock_resolver.resolve.return_value = ResolvedToken(
            symbol="WAVAX",
            address="0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7",
            decimals=18,
            chain="avalanche",
            chain_id=43114,
            source="static",
        )

        provider = Web3BalanceProvider(
            rpc_url="http://localhost:8545",
            wallet_address="0x0000000000000000000000000000000000000001",
            chain="avalanche",
            token_resolver=mock_resolver,
        )

        result = provider._resolve_token("WAVAX")
        mock_resolver.resolve.assert_called_once_with("WAVAX", "avalanche")
        assert result is not None
        assert result.address == "0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7"
