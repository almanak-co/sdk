"""Tests for MultiDexPriceService integration with TokenResolver.

Verifies that the service uses TokenResolver as the sole source of truth
for token address and decimals resolution (no local fallback registries).
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.gateway.data.price.multi_dex import (
    MultiDexPriceService,
    TokenNotSupportedError,
)


class TestMultiDexPriceServiceTokenResolver:
    """Tests for TokenResolver integration in MultiDexPriceService."""

    @pytest.fixture
    def mock_resolver(self):
        """Create a mock TokenResolver."""
        from almanak.framework.data.tokens.models import ResolvedToken

        resolver = MagicMock()

        # Default: resolve WETH on ethereum
        weth_resolved = ResolvedToken(
            symbol="WETH",
            address="0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
            decimals=18,
            chain="ethereum",
            chain_id=1,
            is_native=False,
            is_wrapped_native=True,
            source="static",
        )
        resolver.resolve.return_value = weth_resolved
        return resolver

    @pytest.fixture
    def service(self, mock_resolver):
        """Create service with mock resolver."""
        return MultiDexPriceService(
            chain="ethereum",
            token_resolver=mock_resolver,
        )

    def test_init_with_custom_resolver(self, mock_resolver):
        """Service accepts custom token_resolver parameter."""
        service = MultiDexPriceService(
            chain="ethereum",
            token_resolver=mock_resolver,
        )
        assert service._token_resolver is mock_resolver

    def test_init_with_default_resolver(self):
        """Service uses get_token_resolver() when no resolver provided."""
        mock_resolver = MagicMock()
        with patch(
            "almanak.framework.data.tokens.resolver.get_token_resolver",
            return_value=mock_resolver,
        ):
            service = MultiDexPriceService(chain="ethereum")
            assert service._token_resolver is mock_resolver

    def test_resolve_token_address_uses_resolver(self, service, mock_resolver):
        """_resolve_token_address delegates to TokenResolver.resolve()."""
        result = service._resolve_token_address("WETH")

        mock_resolver.resolve.assert_called_once_with("WETH", "ethereum")
        assert result == "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"

    def test_resolve_token_address_by_symbol(self, service, mock_resolver):
        """_resolve_token_address resolves symbols via TokenResolver."""
        from almanak.framework.data.tokens.models import ResolvedToken

        usdc_resolved = ResolvedToken(
            symbol="USDC",
            address="0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            decimals=6,
            chain="ethereum",
            chain_id=1,
            source="static",
        )
        mock_resolver.resolve.return_value = usdc_resolved

        result = service._resolve_token_address("USDC")

        assert result == "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"

    def test_get_token_decimals_uses_resolver(self, service, mock_resolver):
        """_get_token_decimals delegates to TokenResolver.resolve()."""
        result = service._get_token_decimals("WETH")

        mock_resolver.resolve.assert_called_once_with("WETH", "ethereum")
        assert result == 18

    def test_get_token_decimals_usdc(self, service, mock_resolver):
        """_get_token_decimals returns correct decimals for USDC (6, not 18)."""
        from almanak.framework.data.tokens.models import ResolvedToken

        usdc_resolved = ResolvedToken(
            symbol="USDC",
            address="0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            decimals=6,
            chain="ethereum",
            chain_id=1,
            source="static",
        )
        mock_resolver.resolve.return_value = usdc_resolved

        result = service._get_token_decimals("USDC")

        assert result == 6  # NEVER default to 18

    def test_resolve_token_address_resolver_failure_passthrough_address(self, service, mock_resolver):
        """_resolve_token_address passes through raw addresses when resolver fails."""
        from almanak.framework.data.tokens.exceptions import TokenNotFoundError

        unknown_addr = "0x1111111111111111111111111111111111111111"
        mock_resolver.resolve.side_effect = TokenNotFoundError(unknown_addr, "ethereum")

        result = service._resolve_token_address(unknown_addr)

        assert result == unknown_addr

    def test_resolve_token_address_unknown_symbol_raises(self, service, mock_resolver):
        """_resolve_token_address raises TokenNotSupportedError for unknown symbols."""
        from almanak.framework.data.tokens.exceptions import TokenNotFoundError

        mock_resolver.resolve.side_effect = TokenNotFoundError("UNKNOWN_TOKEN", "ethereum")

        with pytest.raises(TokenNotSupportedError):
            service._resolve_token_address("UNKNOWN_TOKEN")

    def test_get_token_decimals_resolver_failure_raises(self, service, mock_resolver):
        """_get_token_decimals raises TokenNotSupportedError when resolver fails."""
        mock_resolver.resolve.side_effect = Exception("resolver unavailable")

        with pytest.raises(TokenNotSupportedError):
            service._get_token_decimals("UNKNOWN_TOKEN")

    def test_token_addresses_removed_from_module(self):
        """TOKEN_ADDRESSES is no longer defined in multi_dex module."""
        import almanak.gateway.data.price.multi_dex as mod

        assert not hasattr(mod, "TOKEN_ADDRESSES")

    def test_token_decimals_removed_from_module(self):
        """TOKEN_DECIMALS is no longer defined in multi_dex module."""
        import almanak.gateway.data.price.multi_dex as mod

        assert not hasattr(mod, "TOKEN_DECIMALS")

    def test_resolve_bridged_token(self, service, mock_resolver):
        """_resolve_token_address handles bridged tokens like USDC.e."""
        from almanak.framework.data.tokens.models import ResolvedToken

        mock_resolver_arb = MagicMock()
        usdc_e_resolved = ResolvedToken(
            symbol="USDC.e",
            address="0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8",
            decimals=6,
            chain="arbitrum",
            chain_id=42161,
            source="static",
        )
        mock_resolver_arb.resolve.return_value = usdc_e_resolved

        service_arb = MultiDexPriceService(
            chain="arbitrum",
            token_resolver=mock_resolver_arb,
        )

        result = service_arb._resolve_token_address("USDC.e")

        assert result == "0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8"

    def test_amount_to_wei_uses_resolver_decimals(self, service, mock_resolver):
        """_amount_to_wei uses decimals from TokenResolver."""
        from almanak.framework.data.tokens.models import ResolvedToken

        usdc_resolved = ResolvedToken(
            symbol="USDC",
            address="0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            decimals=6,
            chain="ethereum",
            chain_id=1,
            source="static",
        )
        mock_resolver.resolve.return_value = usdc_resolved

        result = service._amount_to_wei(Decimal("100"), "USDC")

        assert result == 100_000_000  # 100 * 10^6

    def test_wei_to_amount_uses_resolver_decimals(self, service, mock_resolver):
        """_wei_to_amount uses decimals from TokenResolver."""
        from almanak.framework.data.tokens.models import ResolvedToken

        usdc_resolved = ResolvedToken(
            symbol="USDC",
            address="0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            decimals=6,
            chain="ethereum",
            chain_id=1,
            source="static",
        )
        mock_resolver.resolve.return_value = usdc_resolved

        result = service._wei_to_amount(100_000_000, "USDC")

        assert result == Decimal("100")


class TestMultiDexPriceServiceMultiChain:
    """Test resolver integration across multiple chains."""

    def test_arbitrum_chain(self):
        """Service works with arbitrum chain and resolver."""
        mock_resolver = MagicMock()
        from almanak.framework.data.tokens.models import ResolvedToken

        mock_resolver.resolve.return_value = ResolvedToken(
            symbol="WETH",
            address="0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
            decimals=18,
            chain="arbitrum",
            chain_id=42161,
            source="static",
        )

        service = MultiDexPriceService(
            chain="arbitrum",
            token_resolver=mock_resolver,
        )

        result = service._resolve_token_address("WETH")
        mock_resolver.resolve.assert_called_once_with("WETH", "arbitrum")
        assert result == "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"

    def test_base_chain(self):
        """Service works with base chain and resolver."""
        mock_resolver = MagicMock()
        from almanak.framework.data.tokens.models import ResolvedToken

        mock_resolver.resolve.return_value = ResolvedToken(
            symbol="USDC",
            address="0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
            decimals=6,
            chain="base",
            chain_id=8453,
            source="static",
        )

        service = MultiDexPriceService(
            chain="base",
            token_resolver=mock_resolver,
        )

        result = service._resolve_token_address("USDC")
        assert result == "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"

        decimals = service._get_token_decimals("USDC")
        assert decimals == 6
