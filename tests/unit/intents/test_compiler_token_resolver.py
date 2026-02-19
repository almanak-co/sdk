"""Unit tests for IntentCompiler TokenResolver integration.

Tests verify that IntentCompiler uses TokenResolver for token resolution.
TOKEN_ADDRESSES dict has been removed after deprecation period.
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.data.tokens import TokenResolver, get_token_resolver
from almanak.framework.data.tokens.exceptions import TokenNotFoundError
from almanak.framework.data.tokens.models import ResolvedToken
from almanak.framework.intents import Intent
from almanak.framework.intents.compiler import (
    IntentCompiler,
    IntentCompilerConfig,
    TokenInfo,
)


class TestIntentCompilerWithTokenResolver:
    """Test IntentCompiler uses TokenResolver for token resolution."""

    def setup_method(self) -> None:
        """Reset TokenResolver singleton before each test."""
        TokenResolver.reset_instance()

    def teardown_method(self) -> None:
        """Reset TokenResolver singleton after each test."""
        TokenResolver.reset_instance()

    def test_compiler_uses_token_resolver_by_default(self) -> None:
        """Verify compiler creates default token resolver if none provided."""
        config = IntentCompilerConfig(allow_placeholder_prices=True)
        compiler = IntentCompiler(chain="arbitrum", config=config)

        assert compiler._token_resolver is not None
        assert isinstance(compiler._token_resolver, TokenResolver)

    def test_compiler_accepts_custom_token_resolver(self) -> None:
        """Verify compiler can use a custom token resolver."""
        config = IntentCompilerConfig(allow_placeholder_prices=True)
        custom_resolver = TokenResolver.get_instance()

        compiler = IntentCompiler(
            chain="arbitrum",
            config=config,
            token_resolver=custom_resolver,
        )

        assert compiler._token_resolver is custom_resolver

    def test_resolve_token_uses_resolver(self) -> None:
        """Verify _resolve_token uses TokenResolver internally."""
        config = IntentCompilerConfig(allow_placeholder_prices=True)
        compiler = IntentCompiler(chain="arbitrum", config=config)

        # Resolve USDC on Arbitrum
        token_info = compiler._resolve_token("USDC")

        assert token_info is not None
        assert token_info.symbol == "USDC"
        assert token_info.decimals == 6
        # Verify correct Arbitrum USDC address
        assert token_info.address.lower() == "0xaf88d065e77c8cc2239327c5edb3a432268e5831"

    def test_resolve_token_by_address(self) -> None:
        """Verify _resolve_token resolves by address correctly."""
        config = IntentCompilerConfig(allow_placeholder_prices=True)
        compiler = IntentCompiler(chain="arbitrum", config=config)

        # Resolve Arbitrum USDC by address
        usdc_address = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
        token_info = compiler._resolve_token(usdc_address)

        assert token_info is not None
        assert token_info.symbol == "USDC"
        assert token_info.decimals == 6

    def test_resolve_token_returns_none_for_unknown(self) -> None:
        """Verify _resolve_token returns None for unknown tokens."""
        config = IntentCompilerConfig(allow_placeholder_prices=True)
        compiler = IntentCompiler(chain="arbitrum", config=config)

        # Unknown token should return None (backward compatibility)
        token_info = compiler._resolve_token("UNKNOWN_TOKEN_XYZ")

        assert token_info is None

    def test_get_token_decimals_uses_resolver(self) -> None:
        """Verify _get_token_decimals uses TokenResolver."""
        config = IntentCompilerConfig(allow_placeholder_prices=True)
        compiler = IntentCompiler(chain="arbitrum", config=config)

        # USDC has 6 decimals
        decimals = compiler._get_token_decimals("USDC")
        assert decimals == 6

        # WETH has 18 decimals
        decimals = compiler._get_token_decimals("WETH")
        assert decimals == 18

        # WBTC has 8 decimals
        decimals = compiler._get_token_decimals("WBTC")
        assert decimals == 8

    def test_get_token_decimals_raises_for_unknown(self) -> None:
        """Verify _get_token_decimals raises TokenNotFoundError for unknown tokens."""
        config = IntentCompilerConfig(allow_placeholder_prices=True)
        compiler = IntentCompiler(chain="arbitrum", config=config)

        with pytest.raises(TokenNotFoundError):
            compiler._get_token_decimals("UNKNOWN_TOKEN_XYZ")

    def test_resolve_token_different_chains(self) -> None:
        """Verify _resolve_token works for different chains."""
        config = IntentCompilerConfig(allow_placeholder_prices=True)

        # Test Ethereum
        eth_compiler = IntentCompiler(chain="ethereum", config=config)
        TokenResolver.reset_instance()
        token = eth_compiler._resolve_token("USDC")
        assert token is not None
        assert token.address.lower() == "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"

        # Test Base
        TokenResolver.reset_instance()
        base_compiler = IntentCompiler(chain="base", config=config)
        token = base_compiler._resolve_token("USDC")
        assert token is not None
        assert token.address.lower() == "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"

    def test_resolve_token_with_explicit_chain(self) -> None:
        """Verify _resolve_token can resolve on a different chain than self.chain."""
        config = IntentCompilerConfig(allow_placeholder_prices=True)
        compiler = IntentCompiler(chain="arbitrum", config=config)

        # Resolve USDC on Ethereum (different from compiler's chain)
        token_info = compiler._resolve_token("USDC", chain="ethereum")

        assert token_info is not None
        assert token_info.symbol == "USDC"
        # Should be Ethereum USDC address, not Arbitrum
        assert token_info.address.lower() == "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"


class TestTokenAddressesRemoved:
    """Verify TOKEN_ADDRESSES has been removed from compiler.py after deprecation period."""

    def test_token_addresses_not_importable(self) -> None:
        """Verify TOKEN_ADDRESSES is no longer in compiler.py."""
        with pytest.raises(ImportError):
            from almanak.framework.intents.compiler import TOKEN_ADDRESSES  # noqa: F401

    def test_deprecated_dict_not_importable(self) -> None:
        """Verify _DeprecatedDict is no longer in compiler.py."""
        with pytest.raises(ImportError):
            from almanak.framework.intents.compiler import _DeprecatedDict  # noqa: F401

    def test_compiler_has_no_token_addresses_attribute(self) -> None:
        """Verify IntentCompiler instances no longer have token_addresses attribute."""
        config = IntentCompilerConfig(allow_placeholder_prices=True)
        compiler = IntentCompiler(chain="arbitrum", config=config)
        assert not hasattr(compiler, "token_addresses")


class TestResolverIntegrationWithMock:
    """Test IntentCompiler with mocked TokenResolver."""

    def test_compiler_with_mocked_resolver(self) -> None:
        """Verify compiler uses injected resolver correctly."""
        # Create a mock resolver
        mock_resolver = MagicMock(spec=TokenResolver)
        mock_resolved_token = ResolvedToken(
            symbol="MOCK",
            address="0x1234567890123456789012345678901234567890",
            decimals=18,
            chain="arbitrum",
            chain_id=42161,
            name="Mock Token",
            coingecko_id=None,
            is_stablecoin=False,
            is_native=False,
            is_wrapped_native=False,
            canonical_symbol="MOCK",
            bridge_type=None,
            source="mock",
            is_verified=True,
        )
        mock_resolver.resolve.return_value = mock_resolved_token
        mock_resolver.get_decimals.return_value = 18

        config = IntentCompilerConfig(allow_placeholder_prices=True)
        compiler = IntentCompiler(
            chain="arbitrum",
            config=config,
            token_resolver=mock_resolver,
        )

        # Call _resolve_token
        token_info = compiler._resolve_token("MOCK")

        # Verify mock was called
        mock_resolver.resolve.assert_called_once_with("MOCK", "arbitrum")
        assert token_info is not None
        assert token_info.symbol == "MOCK"
        assert token_info.decimals == 18

    def test_compiler_with_resolver_returning_native_token(self) -> None:
        """Verify compiler handles native tokens correctly."""
        config = IntentCompilerConfig(allow_placeholder_prices=True)
        compiler = IntentCompiler(chain="arbitrum", config=config)

        # Resolve ETH (native token)
        token_info = compiler._resolve_token("ETH")

        assert token_info is not None
        assert token_info.symbol == "ETH"
        assert token_info.is_native is True


class TestBackwardCompatibility:
    """Test backward compatibility of the token resolution changes."""

    def setup_method(self) -> None:
        """Reset TokenResolver singleton before each test."""
        TokenResolver.reset_instance()

    def teardown_method(self) -> None:
        """Reset TokenResolver singleton after each test."""
        TokenResolver.reset_instance()

    def test_token_info_structure_unchanged(self) -> None:
        """Verify TokenInfo returned by _resolve_token has expected structure."""
        config = IntentCompilerConfig(allow_placeholder_prices=True)
        compiler = IntentCompiler(chain="arbitrum", config=config)

        token_info = compiler._resolve_token("USDC")

        assert token_info is not None
        # Verify TokenInfo has all expected attributes
        assert hasattr(token_info, "symbol")
        assert hasattr(token_info, "address")
        assert hasattr(token_info, "decimals")
        assert hasattr(token_info, "is_native")

        # Verify types
        assert isinstance(token_info.symbol, str)
        assert isinstance(token_info.address, str)
        assert isinstance(token_info.decimals, int)
        assert isinstance(token_info.is_native, bool)

    def test_bridged_tokens_resolve_correctly(self) -> None:
        """Verify bridged tokens like USDC.e resolve correctly."""
        config = IntentCompilerConfig(allow_placeholder_prices=True)
        compiler = IntentCompiler(chain="arbitrum", config=config)

        # USDC.e is bridged USDC on Arbitrum
        token_info = compiler._resolve_token("USDC.e")

        assert token_info is not None
        assert token_info.decimals == 6
        # Should be the bridged USDC address
        assert token_info.address.lower() == "0xff970a61a04b1ca14834a43f5de4533ebddb5cc8"

    def test_case_insensitive_resolution(self) -> None:
        """Verify token resolution is case-insensitive."""
        config = IntentCompilerConfig(allow_placeholder_prices=True)
        compiler = IntentCompiler(chain="arbitrum", config=config)

        # Try different cases
        token_upper = compiler._resolve_token("USDC")
        token_lower = compiler._resolve_token("usdc")
        token_mixed = compiler._resolve_token("UsDc")

        assert token_upper is not None
        assert token_lower is not None
        assert token_mixed is not None

        # All should resolve to the same token
        assert token_upper.address == token_lower.address == token_mixed.address
        assert token_upper.decimals == token_lower.decimals == token_mixed.decimals


class TestSwapCompilationFailsOnMissingPrice:
    """Verify swap compilation fails closed when price oracle lacks a token price.

    This is a safety-critical behavior: without a price, slippage protection
    (min_output) cannot be calculated. Compiling with min_output=0 would leave
    the swap vulnerable to sandwich attacks / MEV extraction.
    """

    def setup_method(self) -> None:
        TokenResolver.reset_instance()

    def teardown_method(self) -> None:
        TokenResolver.reset_instance()

    def test_swap_compilation_fails_when_price_missing(self) -> None:
        """Swap compile should return FAILED when token price is missing."""
        from almanak.framework.intents.compiler import CompilationStatus

        # Price oracle has USDC but not WETH
        price_oracle = {"USDC": Decimal("1.0")}

        config = IntentCompilerConfig(allow_placeholder_prices=False)
        compiler = IntentCompiler(
            chain="arbitrum",
            config=config,
            price_oracle=price_oracle,
        )

        intent = Intent.swap(
            from_token="USDC",
            to_token="WETH",
            amount=Decimal("100"),
            max_slippage=Decimal("0.005"),
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "slippage protection" in result.error.lower()

    def test_swap_compilation_fails_when_price_zero(self) -> None:
        """Swap compile should return FAILED when token price is zero."""
        from almanak.framework.intents.compiler import CompilationStatus

        # Price oracle has both tokens but WETH price is zero
        price_oracle = {"USDC": Decimal("1.0"), "WETH": Decimal("0")}

        config = IntentCompilerConfig(allow_placeholder_prices=False)
        compiler = IntentCompiler(
            chain="arbitrum",
            config=config,
            price_oracle=price_oracle,
        )

        intent = Intent.swap(
            from_token="USDC",
            to_token="WETH",
            amount=Decimal("100"),
            max_slippage=Decimal("0.005"),
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "price" in result.error.lower()

    def test_swap_compilation_succeeds_with_placeholder_prices(self) -> None:
        """Swap compile should succeed when placeholder prices are enabled."""
        from almanak.framework.intents.compiler import CompilationStatus

        config = IntentCompilerConfig(allow_placeholder_prices=True)
        compiler = IntentCompiler(
            chain="arbitrum",
            config=config,
        )

        intent = Intent.swap(
            from_token="USDC",
            to_token="WETH",
            amount=Decimal("100"),
            max_slippage=Decimal("0.005"),
        )

        result = compiler.compile(intent)

        # With placeholder prices, compilation should succeed
        assert result.status == CompilationStatus.SUCCESS
