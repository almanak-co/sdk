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


class TestTraderJoeV2SwapGuard:
    """Verify TraderJoe V2 swap routes to dedicated compilation path (VIB-1928).

    LBRouter2 uses a bin-based AMM interface incompatible with DefaultSwapAdapter's
    Uniswap V3 exactInputSingle calldata. Swaps route to _compile_swap_traderjoe_v2().
    """

    def test_traderjoe_v2_swap_not_blocked(self):
        """SwapIntent(protocol='traderjoe_v2') no longer returns VIB-1406 error (VIB-1928)."""
        from decimal import Decimal

        from almanak.framework.intents import SwapIntent
        from almanak.framework.intents.compiler import IntentCompiler

        compiler = IntentCompiler(
            chain="avalanche",
            wallet_address=TEST_WALLET,
            price_oracle={"USDC": Decimal("1.0"), "WAVAX": Decimal("25.0")},
        )
        intent = SwapIntent(
            from_token="USDC",
            to_token="WAVAX",
            amount=Decimal("100"),
            max_slippage=Decimal("0.01"),
            protocol="traderjoe_v2",
            chain="avalanche",
        )

        result = compiler.compile(intent)
        # VIB-1928: TJ V2 swaps now route to dedicated compilation path.
        # May fail for RPC/pool reasons in unit test, but NOT with VIB-1406 error.
        if result.status.value == "FAILED":
            assert "VIB-1406" not in (result.error or ""), (
                "TraderJoe V2 swap still blocked by VIB-1406 guard!"
            )
            assert "not yet supported" not in (result.error or ""), (
                "TraderJoe V2 swap still returns 'not yet supported' error!"
            )
        elif result.status.value == "SUCCESS":
            assert result.action_bundle is not None
            assert result.action_bundle.metadata["protocol"] == "traderjoe_v2"

    def test_traderjoe_v2_removed_from_protocol_routers(self):
        """Verify traderjoe_v2 is not in PROTOCOL_ROUTERS (swap routing)."""
        from almanak.framework.intents.compiler import PROTOCOL_ROUTERS

        for chain, routers in PROTOCOL_ROUTERS.items():
            assert "traderjoe_v2" not in routers, (
                f"traderjoe_v2 still in PROTOCOL_ROUTERS['{chain}'] — "
                f"DefaultSwapAdapter generates incompatible Uniswap V3 calldata"
            )

    def test_traderjoe_v2_still_in_lp_position_managers(self):
        """Verify traderjoe_v2 LP operations still work (not removed from LP routing)."""
        from almanak.framework.intents.compiler import LP_POSITION_MANAGERS

        # TraderJoe V2 LP should still be available on all supported chains
        expected_chains = {"ethereum", "arbitrum", "avalanche", "bsc"}
        for chain in expected_chains:
            assert "traderjoe_v2" in LP_POSITION_MANAGERS.get(chain, {}), (
                f"traderjoe_v2 should be present in LP_POSITION_MANAGERS for '{chain}'"
            )
