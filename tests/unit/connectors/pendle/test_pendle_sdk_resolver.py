"""Tests for PendleSDK TokenResolver integration.

These tests verify that the PendleSDK correctly initializes with
the TokenResolver. Pendle defines TOKEN_DECIMALS but doesn't actively
use them in SDK methods (all operations use specialized PT_TOKEN_INFO
and other protocol-specific dicts). The resolver is available for future use.
"""

from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.connectors.pendle.sdk import MARKET_BY_PT_TOKEN, PT_TOKEN_INFO, PendleSDK


@pytest.fixture
def mock_resolver():
    """Create a mock TokenResolver."""
    return MagicMock()


class TestPendleSDKResolverInit:
    """Test PendleSDK initializes with TokenResolver."""

    @patch("almanak.framework.connectors.pendle.sdk.Web3")
    def test_custom_resolver_injected(self, mock_web3, mock_resolver):
        """Test custom resolver is used when provided."""
        sdk = PendleSDK(rpc_url="http://localhost:8545", chain="arbitrum", token_resolver=mock_resolver)
        assert sdk._token_resolver is mock_resolver

    @patch("almanak.framework.connectors.pendle.sdk.Web3")
    def test_default_resolver_initialized(self, mock_web3):
        """Test default resolver is initialized when not provided."""
        sdk = PendleSDK(rpc_url="http://localhost:8545", chain="arbitrum")
        assert sdk._token_resolver is not None

    @patch("almanak.framework.connectors.pendle.sdk.Web3")
    def test_resolver_init_failure_graceful(self, mock_web3, mock_resolver):
        """Test SDK works even if resolver init is set to None."""
        sdk = PendleSDK(rpc_url="http://localhost:8545", chain="arbitrum", token_resolver=mock_resolver)
        sdk._token_resolver = None
        # SDK should still be functional
        assert sdk.chain == "arbitrum"
        assert sdk.router_address is not None


# Expected addresses for Arbitrum PT-wstETH
ARB_WSTETH_MARKET = "0xf78452e0f5C0B95fc5dC8353B8CD1e06E53fa25B"
ARB_PT_WSTETH_ADDRESS = "0x71fBF40651E9D4278a74586AfC99F307f369Ce9A"
ARB_PT_WSTETH_DECIMALS = 18


class TestArbitrumPTDateSuffix:
    """Test that Arbitrum PT tokens are registered with date-suffixed names (VIB-115)."""

    @pytest.mark.parametrize(
        "name",
        [
            "PT-WSTETH-25JUN2026",
            "PT-wstETH-25JUN2026",
            "PT-WSTETH",
            "PT-wstETH",
        ],
    )
    def test_market_by_pt_token_resolves(self, name):
        """All name variants should resolve to the same market address."""
        market = MARKET_BY_PT_TOKEN["arbitrum"].get(name)
        assert market == ARB_WSTETH_MARKET, f"MARKET_BY_PT_TOKEN['arbitrum']['{name}'] not found or wrong"

    @pytest.mark.parametrize(
        "name",
        [
            "PT-WSTETH-25JUN2026",
            "PT-wstETH-25JUN2026",
            "PT-WSTETH",
            "PT-wstETH",
        ],
    )
    def test_pt_token_info_resolves(self, name):
        """All name variants should resolve to the same PT token address and decimals."""
        info = PT_TOKEN_INFO["arbitrum"].get(name)
        assert info is not None, f"PT_TOKEN_INFO['arbitrum']['{name}'] not found"
        address, decimals = info
        assert address == ARB_PT_WSTETH_ADDRESS
        assert decimals == ARB_PT_WSTETH_DECIMALS

    def test_all_arbitrum_market_entries_consistent(self):
        """All MARKET_BY_PT_TOKEN entries for arbitrum should point to the same market."""
        markets = MARKET_BY_PT_TOKEN["arbitrum"]
        unique_values = set(markets.values())
        assert len(unique_values) == 1, f"Inconsistent market addresses: {unique_values}"
        assert unique_values.pop() == ARB_WSTETH_MARKET

    def test_all_arbitrum_pt_info_entries_consistent(self):
        """All PT_TOKEN_INFO entries for arbitrum should point to the same token."""
        infos = PT_TOKEN_INFO["arbitrum"]
        unique_addresses = {addr for addr, _ in infos.values()}
        unique_decimals = {dec for _, dec in infos.values()}
        assert len(unique_addresses) == 1, f"Inconsistent PT addresses: {unique_addresses}"
        assert len(unique_decimals) == 1, f"Inconsistent decimals: {unique_decimals}"
