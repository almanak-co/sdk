"""Unit tests for PendleOnChainReader with mocked RPC responses."""

import json
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
    with patch("web3.Web3") as MockWeb3:
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
        with patch("web3.Web3"):
            reader = PendleOnChainReader(rpc_url="http://localhost:8545", chain="ethereum")
        assert reader.chain == "ethereum"
        assert reader.router_static_address == ROUTER_STATIC_ADDRESSES["ethereum"]

    def test_arbitrum_chain(self):
        with patch("web3.Web3"):
            reader = PendleOnChainReader(rpc_url="http://localhost:8545", chain="arbitrum")
        assert reader.chain == "arbitrum"

    def test_unsupported_chain_raises(self):
        with pytest.raises(ValueError, match="Unsupported chain"):
            with patch("web3.Web3"):
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


# =========================================================================
# Gateway Mode Helpers
# =========================================================================


def _mock_rpc_response(result_hex: str, success: bool = True, error: str = ""):
    """Create a mock gateway RPC response."""
    resp = MagicMock()
    resp.success = success
    resp.result = json.dumps(result_hex)
    resp.error = error
    return resp


@pytest.fixture
def gateway_client():
    """Create a mock GatewayClient for gateway mode tests."""
    client = MagicMock()
    client.is_connected = True
    return client


@pytest.fixture
def gw_reader(gateway_client):
    """Create a PendleOnChainReader in gateway mode."""
    r = PendleOnChainReader(gateway_client=gateway_client, chain="ethereum")
    r.clear_cache()
    return r


# =========================================================================
# Gateway Mode Init Tests
# =========================================================================


class TestGatewayModeInit:
    """Test reader initialization in gateway mode."""

    def test_gateway_mode_init(self, gateway_client):
        reader = PendleOnChainReader(gateway_client=gateway_client, chain="ethereum")
        assert reader.chain == "ethereum"
        assert reader.web3 is None
        assert reader.router_static is None

    def test_gateway_mode_arbitrum(self, gateway_client):
        reader = PendleOnChainReader(gateway_client=gateway_client, chain="arbitrum")
        assert reader.chain == "arbitrum"
        assert reader.router_static_address == ROUTER_STATIC_ADDRESSES["arbitrum"]

    def test_no_client_no_url_raises(self):
        with pytest.raises(ValueError, match="Either rpc_url or gateway_client"):
            PendleOnChainReader(chain="ethereum")

    def test_unsupported_chain_gateway_raises(self, gateway_client):
        with pytest.raises(ValueError, match="Unsupported chain"):
            PendleOnChainReader(gateway_client=gateway_client, chain="polygon")


# =========================================================================
# Gateway Mode PT Rate Tests
# =========================================================================


class TestGatewayPtRate:
    """Test get_pt_to_asset_rate via gateway mode."""

    def test_returns_normalized_rate(self, gw_reader, gateway_client):
        # 0.97 in 1e18 = 0x0D7621DC58210000 (970000000000000000)
        rate_hex = hex(970000000000000000)
        gateway_client.rpc.Call.return_value = _mock_rpc_response(rate_hex)

        rate = gw_reader.get_pt_to_asset_rate("0x1234567890abcdef1234567890abcdef12345678")
        assert rate == Decimal("0.97")

    def test_rate_of_one(self, gw_reader, gateway_client):
        rate_hex = hex(10**18)
        gateway_client.rpc.Call.return_value = _mock_rpc_response(rate_hex)

        rate = gw_reader.get_pt_to_asset_rate("0x1234567890abcdef1234567890abcdef12345678")
        assert rate == Decimal("1")

    def test_caches_result(self, gw_reader, gateway_client):
        rate_hex = hex(970000000000000000)
        gateway_client.rpc.Call.return_value = _mock_rpc_response(rate_hex)

        gw_reader.get_pt_to_asset_rate("0xmarket_addr_padded_to_40_hex_chars_00")
        gw_reader.get_pt_to_asset_rate("0xmarket_addr_padded_to_40_hex_chars_00")
        assert gateway_client.rpc.Call.call_count == 1

    def test_rpc_failure_raises(self, gw_reader, gateway_client):
        gateway_client.rpc.Call.return_value = _mock_rpc_response("", success=False, error="rpc error")

        with pytest.raises(PendleOnChainError, match="Gateway RPC call error"):
            gw_reader.get_pt_to_asset_rate("0x1234567890abcdef1234567890abcdef12345678")

    def test_exception_raises(self, gw_reader, gateway_client):
        gateway_client.rpc.Call.side_effect = Exception("connection refused")

        with pytest.raises(PendleOnChainError, match="Gateway RPC call failed"):
            gw_reader.get_pt_to_asset_rate("0x1234567890abcdef1234567890abcdef12345678")


# =========================================================================
# Gateway Mode Implied APY Tests
# =========================================================================


class TestGatewayImpliedApy:
    """Test get_implied_apy via gateway mode."""

    def test_returns_normalized_apy(self, gw_reader, gateway_client):
        # 5% APY in 1e18 scale = 50000000000000000
        apy_hex = hex(50000000000000000)
        gateway_client.rpc.Call.return_value = _mock_rpc_response(apy_hex)

        apy = gw_reader.get_implied_apy("0x1234567890abcdef1234567890abcdef12345678")
        assert apy == Decimal("0.05")


# =========================================================================
# Gateway Mode Market Expiry Tests
# =========================================================================


class TestGatewayMarketExpiry:
    """Test is_market_expired via gateway mode."""

    def test_expired_market(self, gw_reader, gateway_client):
        past_expiry = int(time.time()) - 86400
        gateway_client.rpc.Call.return_value = _mock_rpc_response(hex(past_expiry))

        assert gw_reader.is_market_expired("0x1234567890abcdef1234567890abcdef12345678") is True

    def test_active_market(self, gw_reader, gateway_client):
        future_expiry = int(time.time()) + 86400 * 365
        gateway_client.rpc.Call.return_value = _mock_rpc_response(hex(future_expiry))

        assert gw_reader.is_market_expired("0x1234567890abcdef1234567890abcdef12345678") is False


# =========================================================================
# Gateway Mode Market Tokens Tests
# =========================================================================


class TestGatewayMarketTokens:
    """Test get_market_tokens via gateway mode."""

    def test_returns_token_addresses(self, gw_reader, gateway_client):
        # Encode 3 addresses as 3 x 32-byte hex slots
        sy_addr = "0000000000000000000000001111111111111111111111111111111111111111"
        pt_addr = "0000000000000000000000002222222222222222222222222222222222222222"
        yt_addr = "0000000000000000000000003333333333333333333333333333333333333333"
        result_hex = "0x" + sy_addr + pt_addr + yt_addr
        gateway_client.rpc.Call.return_value = _mock_rpc_response(result_hex)

        tokens = gw_reader.get_market_tokens("0x1234567890abcdef1234567890abcdef12345678")
        assert tokens["sy"] == "0x1111111111111111111111111111111111111111"
        assert tokens["pt"] == "0x2222222222222222222222222222222222222222"
        assert tokens["yt"] == "0x3333333333333333333333333333333333333333"

    def test_short_response_raises(self, gw_reader, gateway_client):
        gateway_client.rpc.Call.return_value = _mock_rpc_response("0x" + "00" * 32)

        with pytest.raises(PendleOnChainError, match="unexpected data length"):
            gw_reader.get_market_tokens("0x1234567890abcdef1234567890abcdef12345678")
