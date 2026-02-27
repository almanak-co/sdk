"""Tests for AerodromeAdapter and AerodromeSDK TokenResolver integration.

These tests verify that both the adapter and SDK correctly use the TokenResolver
for token resolution.
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.connectors.aerodrome.adapter import (
    AerodromeAdapter,
    AerodromeConfig,
)
from almanak.framework.connectors.aerodrome.sdk import (
    AerodromeSDK,
)
from almanak.framework.data.tokens.exceptions import TokenResolutionError
from almanak.framework.data.tokens.models import ResolvedToken

TEST_WALLET = "0x1234567890123456789012345678901234567890"


@pytest.fixture
def config():
    """Create an AerodromeConfig for testing."""
    return AerodromeConfig(
        chain="base",
        wallet_address=TEST_WALLET,
        allow_placeholder_prices=True,
    )


@pytest.fixture
def mock_resolver():
    """Create a mock TokenResolver."""
    return MagicMock()


@pytest.fixture
def adapter(config, mock_resolver):
    """Create an AerodromeAdapter with mock resolver."""
    return AerodromeAdapter(config, token_resolver=mock_resolver)


@pytest.fixture
def sdk(mock_resolver):
    """Create an AerodromeSDK with mock resolver."""
    return AerodromeSDK(chain="base", token_resolver=mock_resolver)


class TestAerodromeAdapterResolverInit:
    """Test AerodromeAdapter initializes with TokenResolver."""

    def test_custom_resolver_injected(self, config, mock_resolver):
        """Test custom resolver is used when provided."""
        adapter = AerodromeAdapter(config, token_resolver=mock_resolver)
        assert adapter._token_resolver is mock_resolver

    def test_default_resolver_initialized(self, config):
        """Test default resolver is initialized when not provided."""
        adapter = AerodromeAdapter(config)
        assert adapter._token_resolver is not None

    def test_rpc_url_passed_to_sdk(self, config):
        """Test AerodromeAdapter passes optional RPC URL to SDK."""
        with patch("almanak.framework.connectors.aerodrome.adapter.AerodromeSDK") as mock_sdk:
            config.rpc_url = "https://base-mainnet.example"
            AerodromeAdapter(config)
            mock_sdk.assert_called_once_with(chain="base", rpc_url="https://base-mainnet.example")


class TestAerodromeAdapterResolveToken:
    """Test _resolve_token uses TokenResolver."""

    def test_resolve_symbol_via_resolver(self, adapter, mock_resolver):
        """Test symbol resolution via TokenResolver."""
        mock_resolver.resolve.return_value = ResolvedToken(
            symbol="USDC",
            address="0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
            decimals=6,
            chain="base",
            chain_id=8453,
        )
        result = adapter._resolve_token("USDC")
        assert result == "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"

    def test_resolve_address_passthrough(self, adapter, mock_resolver):
        """Test address passthrough."""
        addr = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"
        result = adapter._resolve_token(addr)
        assert result == addr
        mock_resolver.resolve.assert_not_called()

    def test_resolve_unknown_symbol_raises_error(self, adapter, mock_resolver):
        """Test that resolving an unknown symbol raises TokenResolutionError."""
        mock_resolver.resolve.side_effect = TokenResolutionError(
            token="UNKNOWN", chain="base", reason="Not found"
        )
        with pytest.raises(TokenResolutionError):
            adapter._resolve_token("UNKNOWN")

    def test_resolver_none_raises_attribute_error(self, config):
        """Test that calling _resolve_token with no resolver raises AttributeError."""
        adapter = AerodromeAdapter(config, token_resolver=None)
        adapter._token_resolver = None
        with pytest.raises(AttributeError):
            adapter._resolve_token("USDC")


class TestAerodromeAdapterGetDecimals:
    """Test _get_token_decimals uses TokenResolver."""

    def test_usdc_decimals_via_resolver(self, adapter, mock_resolver):
        """Test USDC decimals (6) via TokenResolver."""
        mock_resolver.resolve.return_value = ResolvedToken(
            symbol="USDC",
            address="0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
            decimals=6,
            chain="base",
            chain_id=8453,
        )
        result = adapter._get_token_decimals("USDC")
        assert result == 6

    def test_unknown_token_decimals_raises_error(self, adapter, mock_resolver):
        """Test that getting decimals for unknown token raises TokenResolutionError."""
        mock_resolver.resolve.side_effect = TokenResolutionError(
            token="UNKNOWN", chain="base", reason="Not found"
        )
        with pytest.raises(TokenResolutionError):
            adapter._get_token_decimals("UNKNOWN")

    def test_decimals_resolver_none_raises_attribute_error(self, config):
        """Test that calling _get_token_decimals with no resolver raises AttributeError."""
        adapter = AerodromeAdapter(config, token_resolver=None)
        adapter._token_resolver = None
        with pytest.raises(AttributeError):
            adapter._get_token_decimals("USDC")


class TestAerodromeSDKResolverInit:
    """Test AerodromeSDK initializes with TokenResolver."""

    def test_custom_resolver_injected(self, mock_resolver):
        """Test custom resolver is used when provided."""
        sdk = AerodromeSDK(chain="base", token_resolver=mock_resolver)
        assert sdk._token_resolver is mock_resolver


class TestAerodromeSDKResolveToken:
    """Test SDK resolve_token uses TokenResolver."""

    def test_resolve_symbol_via_resolver(self, sdk, mock_resolver):
        """Test symbol resolution via TokenResolver."""
        mock_resolver.resolve.return_value = ResolvedToken(
            symbol="WETH",
            address="0x4200000000000000000000000000000000000006",
            decimals=18,
            chain="base",
            chain_id=8453,
        )
        result = sdk.resolve_token("WETH")
        assert result == "0x4200000000000000000000000000000000000006"


class TestAerodromeSDKGetDecimals:
    """Test SDK get_token_decimals uses TokenResolver."""

    def test_usdc_decimals_via_resolver(self, sdk, mock_resolver):
        """Test USDC decimals (6) via TokenResolver."""
        mock_resolver.resolve.return_value = ResolvedToken(
            symbol="USDC",
            address="0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
            decimals=6,
            chain="base",
            chain_id=8453,
        )
        result = sdk.get_token_decimals("USDC")
        assert result == 6


class TestDeprecatedDictsRemoved:
    """Verify deprecated token dicts have been removed (US-028)."""

    def test_deprecated_dicts_removed(self):
        """Verify deprecated token dicts have been removed (US-028)."""
        import almanak.framework.connectors.aerodrome.sdk as sdk_module

        assert not hasattr(sdk_module, "AERODROME_TOKENS")
        assert not hasattr(sdk_module, "TOKEN_DECIMALS")


class TestAerodromeRemoveLiquidityLPApproval:
    """Regression tests for remove_liquidity LP approval behavior."""

    def test_remove_liquidity_skips_lp_symbol_resolution_when_label_provided(self, adapter, mock_resolver):
        """Unknown LP pool token should not block remove_liquidity build."""
        weth_addr = "0x4200000000000000000000000000000000000006"
        usdc_addr = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"
        pool_lp_addr = "0x" + "cd" * 20

        def resolve_side_effect(token, chain):
            if chain != "base":
                raise AssertionError(f"Unexpected chain: {chain}")
            token_map = {
                "WETH": ResolvedToken("WETH", weth_addr, 18, "base", 8453),
                weth_addr: ResolvedToken("WETH", weth_addr, 18, "base", 8453),
                "USDC": ResolvedToken("USDC", usdc_addr, 6, "base", 8453),
                usdc_addr: ResolvedToken("USDC", usdc_addr, 6, "base", 8453),
            }
            if token in token_map:
                return token_map[token]
            raise TokenResolutionError(token=token, chain="base", reason="Not found")

        mock_resolver.resolve.side_effect = resolve_side_effect
        adapter.sdk.get_pool_address = MagicMock(return_value=pool_lp_addr)

        result = adapter.remove_liquidity(
            token_a="WETH",
            token_b="USDC",
            liquidity=Decimal("1"),
            stable=False,
        )

        assert result.success is True
        assert len(result.transactions) == 2
        assert result.transactions[0].tx_type == "approve"
        assert "LP(WETH/USDC)" in result.transactions[0].description
        resolved_tokens = [call.args[0] for call in mock_resolver.resolve.call_args_list]
        assert pool_lp_addr not in resolved_tokens

    def test_build_approve_tx_uses_explicit_label_without_resolution(self, adapter, mock_resolver):
        """Explicit token labels should bypass resolver symbol lookup."""
        mock_resolver.resolve.side_effect = TokenResolutionError(token="x", chain="base", reason="Not found")

        tx = adapter._build_approve_tx(
            token_address="0x" + "ab" * 20,
            spender="0x" + "cd" * 20,
            amount=1,
            token_label="LP(TEST)",
        )

        assert tx is not None
        assert "LP(TEST)" in tx.description
        mock_resolver.resolve.assert_not_called()
