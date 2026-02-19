"""Unit tests for PendleOnChainReader with mocked RPC responses."""

import time
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.data.pendle.on_chain_reader import (
    ROUTER_STATIC_ADDRESSES,
    PendleOnChainError,
    PendleOnChainReader,
)


# =========================================================================
# Fixtures
# =========================================================================


@pytest.fixture
def mock_web3():
    """Create a mock Web3 instance."""
    mock = MagicMock()
    mock.to_checksum_address = lambda addr: addr
    mock.eth.contract.return_value = MagicMock()
    return mock


@pytest.fixture
def reader(mock_web3):
    """Create a PendleOnChainReader with mocked Web3."""
    with patch("almanak.framework.data.pendle.on_chain_reader.Web3") as MockWeb3:
        MockWeb3.return_value = mock_web3
        MockWeb3.HTTPProvider = MagicMock()
        r = PendleOnChainReader(rpc_url="http://localhost:8545", chain="ethereum")
        r._cache.clear()
        return r


# =========================================================================
# Initialization Tests
# =========================================================================


class TestOnChainReaderInit:
    """Test reader initialization."""

    def test_valid_chain(self):
        with patch("almanak.framework.data.pendle.on_chain_reader.Web3"):
            reader = PendleOnChainReader(rpc_url="http://localhost:8545", chain="ethereum")
        assert reader.chain == "ethereum"
        assert reader.router_static_address == ROUTER_STATIC_ADDRESSES["ethereum"]

    def test_arbitrum_chain(self):
        with patch("almanak.framework.data.pendle.on_chain_reader.Web3"):
            reader = PendleOnChainReader(rpc_url="http://localhost:8545", chain="arbitrum")
        assert reader.chain == "arbitrum"

    def test_unsupported_chain_raises(self):
        with pytest.raises(ValueError, match="Unsupported chain"):
            with patch("almanak.framework.data.pendle.on_chain_reader.Web3"):
                PendleOnChainReader(rpc_url="http://localhost:8545", chain="polygon")


# =========================================================================
# PT Rate Tests
# =========================================================================


class TestGetPtToAssetRate:
    """Test get_pt_to_asset_rate method."""

    def test_returns_normalized_rate(self, reader):
        # Rate in 1e18 scale: 0.97 = 970000000000000000
        reader.router_static.functions.getPtToAssetRate.return_value.call.return_value = 970000000000000000
        rate = reader.get_pt_to_asset_rate("0xmarket")
        assert rate == Decimal("0.97")

    def test_rate_of_one(self, reader):
        reader.router_static.functions.getPtToAssetRate.return_value.call.return_value = 10**18
        rate = reader.get_pt_to_asset_rate("0xmarket")
        assert rate == Decimal("1")

    def test_caches_result(self, reader):
        reader.router_static.functions.getPtToAssetRate.return_value.call.return_value = 970000000000000000
        rate1 = reader.get_pt_to_asset_rate("0xmarket")
        rate2 = reader.get_pt_to_asset_rate("0xmarket")
        assert rate1 == rate2
        # Should only call once due to caching
        assert reader.router_static.functions.getPtToAssetRate.return_value.call.call_count == 1

    def test_rpc_error_raises(self, reader):
        reader.router_static.functions.getPtToAssetRate.return_value.call.side_effect = Exception("RPC error")
        with pytest.raises(PendleOnChainError, match="getPtToAssetRate failed"):
            reader.get_pt_to_asset_rate("0xmarket")


# =========================================================================
# Implied APY Tests
# =========================================================================


class TestGetImpliedApy:
    """Test get_implied_apy method."""

    def test_returns_normalized_apy(self, reader):
        # 5% APY in 1e18 scale
        reader.router_static.functions.getImpliedApy.return_value.call.return_value = 50000000000000000
        apy = reader.get_implied_apy("0xmarket")
        assert apy == Decimal("0.05")

    def test_rpc_error_raises(self, reader):
        reader.router_static.functions.getImpliedApy.return_value.call.side_effect = Exception("timeout")
        with pytest.raises(PendleOnChainError, match="getImpliedApy failed"):
            reader.get_implied_apy("0xmarket")


# =========================================================================
# Market Expiry Tests
# =========================================================================


class TestIsMarketExpired:
    """Test is_market_expired method."""

    def test_expired_market(self, reader):
        past_expiry = int(time.time()) - 86400  # Yesterday
        mock_contract = MagicMock()
        mock_contract.functions.expiry.return_value.call.return_value = past_expiry
        reader.web3.eth.contract.return_value = mock_contract

        assert reader.is_market_expired("0xmarket") is True

    def test_active_market(self, reader):
        future_expiry = int(time.time()) + 86400 * 365  # 1 year from now
        mock_contract = MagicMock()
        mock_contract.functions.expiry.return_value.call.return_value = future_expiry
        reader.web3.eth.contract.return_value = mock_contract

        assert reader.is_market_expired("0xmarket") is False

    def test_rpc_error_raises(self, reader):
        mock_contract = MagicMock()
        mock_contract.functions.expiry.return_value.call.side_effect = Exception("error")
        reader.web3.eth.contract.return_value = mock_contract

        with pytest.raises(PendleOnChainError, match="expiry\\(\\) failed"):
            reader.is_market_expired("0xmarket")


# =========================================================================
# Market Tokens Tests
# =========================================================================


class TestGetMarketTokens:
    """Test get_market_tokens method."""

    def test_returns_token_addresses(self, reader):
        reader.router_static.functions.readTokens.return_value.call.return_value = (
            "0xSY_ADDR",
            "0xPT_ADDR",
            "0xYT_ADDR",
        )
        tokens = reader.get_market_tokens("0xmarket")
        assert tokens["sy"] == "0xsy_addr"
        assert tokens["pt"] == "0xpt_addr"
        assert tokens["yt"] == "0xyt_addr"


# =========================================================================
# PT Output Estimation Tests
# =========================================================================


class TestEstimatePtOutput:
    """Test estimate_pt_output method."""

    def test_basic_estimate(self, reader):
        # Rate = 0.95 (PT is at 5% discount)
        reader.router_static.functions.getPtToAssetRate.return_value.call.return_value = 950000000000000000
        # For 1000 USDC (1e6 wei), PT output = 1000 / 0.95 ≈ 1052
        output = reader.estimate_pt_output("0xmarket", 1000000000000000000)
        assert output > 1000000000000000000  # More PT than input (discount)

    def test_invalid_rate_raises(self, reader):
        reader.router_static.functions.getPtToAssetRate.return_value.call.return_value = 0
        with pytest.raises(PendleOnChainError, match="Invalid PT rate"):
            reader.estimate_pt_output("0xmarket", 1000000)


# =========================================================================
# Cache Tests
# =========================================================================


class TestOnChainCache:
    """Test cache behavior."""

    def test_clear_cache(self, reader):
        reader.router_static.functions.getPtToAssetRate.return_value.call.return_value = 970000000000000000
        reader.get_pt_to_asset_rate("0xmarket")
        reader.clear_cache()
        reader.get_pt_to_asset_rate("0xmarket")
        assert reader.router_static.functions.getPtToAssetRate.return_value.call.call_count == 2
