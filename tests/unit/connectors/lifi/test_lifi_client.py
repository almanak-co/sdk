"""Unit tests for LiFi Client."""

from unittest.mock import MagicMock, patch

import pytest
import requests

from almanak.framework.connectors.lifi.client import (
    CHAIN_MAPPING,
    LiFiClient,
    LiFiConfig,
)
from almanak.framework.connectors.lifi.exceptions import (
    LiFiAPIError,
    LiFiConfigError,
    LiFiRouteNotFoundError,
)
from almanak.framework.connectors.lifi.models import (
    LiFiOrderStrategy,
    LiFiStep,
    LiFiStatusResponse,
)


# ============================================================================
# Fixtures
# ============================================================================


WALLET_ADDRESS = "0x1234567890abcdef1234567890abcdef12345678"


@pytest.fixture
def lifi_config():
    """Create a LiFi config for testing."""
    return LiFiConfig(
        chain_id=42161,
        wallet_address=WALLET_ADDRESS,
        api_key="test-api-key",
    )


@pytest.fixture
def lifi_client(lifi_config):
    """Create a LiFi client for testing."""
    return LiFiClient(lifi_config)


def _make_quote_response(
    from_chain_id=42161,
    to_chain_id=8453,
    tool="across",
    step_type="cross",
    to_amount="995000000",
    to_amount_min="990000000",
):
    """Create a mock LiFi quote response."""
    return {
        "id": "test-step-id",
        "type": step_type,
        "tool": tool,
        "action": {
            "fromChainId": from_chain_id,
            "toChainId": to_chain_id,
            "fromToken": {
                "address": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
                "chainId": from_chain_id,
                "symbol": "USDC",
                "decimals": 6,
                "name": "USD Coin",
            },
            "toToken": {
                "address": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
                "chainId": to_chain_id,
                "symbol": "USDC",
                "decimals": 6,
                "name": "USD Coin",
            },
            "fromAmount": "1000000000",
            "fromAddress": WALLET_ADDRESS,
            "toAddress": WALLET_ADDRESS,
            "slippage": 0.005,
        },
        "estimate": {
            "fromAmount": "1000000000",
            "toAmount": to_amount,
            "toAmountMin": to_amount_min,
            "approvalAddress": "0x1231DEB6f5749EF6cE6943a275A1D3E7486F4EaE",
            "executionDuration": 120,
            "feeCosts": [
                {
                    "name": "Bridge Fee",
                    "amount": "5000000",
                    "amountUSD": "5.00",
                    "percentage": "0.005",
                    "included": True,
                }
            ],
            "gasCosts": [
                {
                    "type": "SUM",
                    "estimate": "250000",
                    "limit": "350000",
                    "amount": "2500000000000000",
                    "amountUSD": "0.50",
                }
            ],
        },
        "transactionRequest": {
            "from": WALLET_ADDRESS,
            "to": "0x1231DEB6f5749EF6cE6943a275A1D3E7486F4EaE",
            "chainId": from_chain_id,
            "data": "0xabcdef1234567890",
            "value": "0",
            "gasPrice": "100000000",
            "gasLimit": "350000",
        },
        "includedSteps": [],
        "integrator": "almanak",
    }


def _make_status_response(status="DONE", substatus="COMPLETED"):
    """Create a mock LiFi status response."""
    return {
        "transactionId": "test-tx-id",
        "sending": {
            "txHash": "0xabc123",
            "chainId": 42161,
        },
        "receiving": {
            "txHash": "0xdef456",
            "chainId": 8453,
        },
        "bridge": "across",
        "fromChainId": 42161,
        "toChainId": 8453,
        "status": status,
        "substatus": substatus,
        "substatusMessage": "Transfer completed",
    }


# ============================================================================
# Config Tests
# ============================================================================


class TestLiFiConfig:
    """Tests for LiFi configuration."""

    def test_config_with_api_key(self):
        """Config with explicit API key."""
        config = LiFiConfig(
            chain_id=42161,
            wallet_address=WALLET_ADDRESS,
            api_key="test-key",
        )
        assert config.api_key == "test-key"
        assert config.chain_id == 42161

    def test_config_without_api_key(self):
        """Config without API key is valid (LiFi key is optional)."""
        config = LiFiConfig(
            chain_id=42161,
            wallet_address=WALLET_ADDRESS,
        )
        assert config.api_key is None

    def test_config_api_key_from_env(self):
        """Config reads API key from environment."""
        with patch.dict("os.environ", {"LIFI_API_KEY": "env-key"}):
            config = LiFiConfig(
                chain_id=42161,
                wallet_address=WALLET_ADDRESS,
            )
            assert config.api_key == "env-key"

    def test_config_missing_wallet_raises(self):
        """Config without wallet address raises error."""
        with pytest.raises(LiFiConfigError):
            LiFiConfig(chain_id=42161, wallet_address="")

    def test_config_defaults(self):
        """Config has sensible defaults."""
        config = LiFiConfig(chain_id=42161, wallet_address=WALLET_ADDRESS)
        assert config.base_url == "https://li.quest/v1"
        assert config.integrator == "almanak"
        assert config.timeout == 30
        assert config.order == LiFiOrderStrategy.RECOMMENDED


# ============================================================================
# Client Tests
# ============================================================================


class TestLiFiClient:
    """Tests for LiFi API client."""

    def test_client_initialization(self, lifi_client):
        """Client initializes correctly."""
        assert lifi_client.config.chain_id == 42161

    def test_session_headers_with_api_key(self, lifi_client):
        """Session includes API key header."""
        assert "x-lifi-api-key" in lifi_client.session.headers
        assert lifi_client.session.headers["x-lifi-api-key"] == "test-api-key"

    def test_session_headers_without_api_key(self):
        """Session omits API key header when not provided."""
        config = LiFiConfig(chain_id=42161, wallet_address=WALLET_ADDRESS)
        client = LiFiClient(config)
        assert "x-lifi-api-key" not in client.session.headers

    @patch.object(LiFiClient, "_make_request")
    def test_get_quote_cross_chain(self, mock_request, lifi_client):
        """Get cross-chain quote returns valid step."""
        mock_request.return_value = _make_quote_response()

        quote = lifi_client.get_quote(
            from_chain_id=42161,
            to_chain_id=8453,
            from_token="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            to_token="0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
            from_amount="1000000000",
            from_address=WALLET_ADDRESS,
        )

        assert isinstance(quote, LiFiStep)
        assert quote.tool == "across"
        assert quote.type == "cross"
        assert quote.is_cross_chain
        assert quote.get_to_amount() == 995000000
        assert quote.get_to_amount_min() == 990000000
        assert quote.transaction_request is not None
        assert quote.transaction_request.data == "0xabcdef1234567890"

    @patch.object(LiFiClient, "_make_request")
    def test_get_quote_same_chain(self, mock_request, lifi_client):
        """Get same-chain quote returns valid step."""
        mock_request.return_value = _make_quote_response(
            from_chain_id=42161,
            to_chain_id=42161,
            tool="1inch",
            step_type="swap",
        )

        quote = lifi_client.get_quote(
            from_chain_id=42161,
            to_chain_id=42161,
            from_token="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            to_token="0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
            from_amount="1000000000",
            from_address=WALLET_ADDRESS,
        )

        assert quote.tool == "1inch"
        assert quote.type == "swap"
        assert not quote.is_cross_chain

    @patch.object(LiFiClient, "_make_request")
    def test_get_quote_with_bridges_filter(self, mock_request, lifi_client):
        """Get quote with bridge filters passes params correctly."""
        mock_request.return_value = _make_quote_response()

        lifi_client.get_quote(
            from_chain_id=42161,
            to_chain_id=8453,
            from_token="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            to_token="0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
            from_amount="1000000000",
            from_address=WALLET_ADDRESS,
            allow_bridges=["across", "stargate"],
            deny_bridges=["hop"],
        )

        call_args = mock_request.call_args
        params = call_args[1].get("params") or call_args[0][2]
        assert params["allowBridges"] == "across,stargate"
        assert params["denyBridges"] == "hop"

    @patch.object(LiFiClient, "_make_request")
    def test_get_quote_no_route_raises(self, mock_request, lifi_client):
        """Get quote raises when no route found."""
        mock_request.side_effect = LiFiAPIError(
            message="No route found",
            status_code=404,
            endpoint="/quote",
        )

        with pytest.raises(LiFiRouteNotFoundError):
            lifi_client.get_quote(
                from_chain_id=42161,
                to_chain_id=8453,
                from_token="0x0000000000000000000000000000000000000001",
                to_token="0x0000000000000000000000000000000000000002",
                from_amount="1",
                from_address=WALLET_ADDRESS,
            )

    @patch.object(LiFiClient, "_make_request")
    def test_get_quote_no_tx_data_raises(self, mock_request, lifi_client):
        """Get quote raises when response has no transaction data."""
        response = _make_quote_response()
        response["transactionRequest"] = {"data": "", "to": "", "from": ""}
        mock_request.return_value = response

        with pytest.raises(LiFiRouteNotFoundError):
            lifi_client.get_quote(
                from_chain_id=42161,
                to_chain_id=8453,
                from_token="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
                to_token="0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
                from_amount="1000000000",
                from_address=WALLET_ADDRESS,
            )

    @patch.object(LiFiClient, "_make_request")
    def test_get_quote_api_error_propagates(self, mock_request, lifi_client):
        """API errors (non-404) propagate as LiFiAPIError."""
        mock_request.side_effect = LiFiAPIError(
            message="Server error",
            status_code=500,
            endpoint="/quote",
        )

        with pytest.raises(LiFiAPIError) as exc_info:
            lifi_client.get_quote(
                from_chain_id=42161,
                to_chain_id=8453,
                from_token="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
                to_token="0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
                from_amount="1000000000",
                from_address=WALLET_ADDRESS,
            )
        assert exc_info.value.status_code == 500

    @patch.object(LiFiClient, "_make_request")
    def test_get_status_done(self, mock_request, lifi_client):
        """Get status returns completed transfer."""
        mock_request.return_value = _make_status_response()

        status = lifi_client.get_status(
            tx_hash="0xabc123",
            from_chain=42161,
            to_chain=8453,
        )

        assert isinstance(status, LiFiStatusResponse)
        assert status.is_complete
        assert not status.is_failed
        assert status.sending_tx_hash == "0xabc123"
        assert status.receiving_tx_hash == "0xdef456"

    @patch.object(LiFiClient, "_make_request")
    def test_get_status_pending(self, mock_request, lifi_client):
        """Get status returns pending transfer."""
        mock_request.return_value = _make_status_response(status="PENDING", substatus="")

        status = lifi_client.get_status(
            tx_hash="0xabc123",
            from_chain=42161,
            to_chain=8453,
        )

        assert status.is_pending
        assert not status.is_complete

    @patch.object(LiFiClient, "_make_request")
    def test_get_status_failed(self, mock_request, lifi_client):
        """Get status returns failed transfer."""
        mock_request.return_value = _make_status_response(status="FAILED", substatus="REFUNDED")

        status = lifi_client.get_status(
            tx_hash="0xabc123",
            from_chain=42161,
            to_chain=8453,
        )

        assert status.is_failed
        assert status.substatus == "REFUNDED"

    @patch.object(LiFiClient, "_make_request")
    def test_get_tokens(self, mock_request, lifi_client):
        """Get tokens returns token data."""
        mock_request.return_value = {"tokens": {"42161": [{"symbol": "USDC"}]}}

        result = lifi_client.get_tokens(chain_id=42161)
        assert "tokens" in result

    @patch.object(LiFiClient, "_make_request")
    def test_get_chains(self, mock_request, lifi_client):
        """Get chains returns chain data."""
        mock_request.return_value = {"chains": [{"id": 42161, "name": "Arbitrum"}]}

        result = lifi_client.get_chains()
        assert isinstance(result, list)
        assert result[0]["name"] == "Arbitrum"


# ============================================================================
# Chain Resolution Tests
# ============================================================================


class TestChainResolution:
    """Tests for chain name/ID resolution."""

    def test_resolve_chain_name(self):
        """Resolve chain name to ID."""
        assert LiFiClient.resolve_chain_id("arbitrum") == 42161
        assert LiFiClient.resolve_chain_id("base") == 8453
        assert LiFiClient.resolve_chain_id("ethereum") == 1

    def test_resolve_chain_id_passthrough(self):
        """Chain IDs pass through unchanged."""
        assert LiFiClient.resolve_chain_id(42161) == 42161

    def test_resolve_unknown_chain_raises(self):
        """Unknown chain name raises error."""
        with pytest.raises(LiFiConfigError):
            LiFiClient.resolve_chain_id("unknown_chain")

    def test_chain_mapping_completeness(self):
        """All expected chains are in the mapping."""
        expected = ["ethereum", "arbitrum", "base", "optimism", "polygon", "avalanche", "bsc"]
        for chain in expected:
            assert chain in CHAIN_MAPPING, f"Missing chain: {chain}"


# ============================================================================
# Model Tests
# ============================================================================


class TestLiFiStep:
    """Tests for LiFi step model parsing."""

    def test_parse_cross_chain_step(self):
        """Parse a cross-chain step from API response."""
        step = LiFiStep.from_api_response(_make_quote_response())

        assert step.id == "test-step-id"
        assert step.type == "cross"
        assert step.tool == "across"
        assert step.is_cross_chain
        assert step.action is not None
        assert step.action.from_chain_id == 42161
        assert step.action.to_chain_id == 8453
        assert step.estimate is not None
        assert step.estimate.to_amount == "995000000"
        assert step.estimate.approval_address == "0x1231DEB6f5749EF6cE6943a275A1D3E7486F4EaE"
        assert step.transaction_request is not None

    def test_parse_same_chain_step(self):
        """Parse a same-chain step."""
        response = _make_quote_response(
            from_chain_id=42161,
            to_chain_id=42161,
            tool="1inch",
            step_type="swap",
        )
        step = LiFiStep.from_api_response(response)

        assert not step.is_cross_chain
        assert step.type == "swap"

    def test_estimate_gas_calculation(self):
        """Estimate total gas calculation works."""
        step = LiFiStep.from_api_response(_make_quote_response())
        assert step.estimate.total_gas_estimate == 250000

    def test_estimate_fee_calculation(self):
        """Estimate total fee USD calculation works."""
        step = LiFiStep.from_api_response(_make_quote_response())
        assert step.estimate.total_fee_usd == 5.0

    def test_status_response_parsing(self):
        """Parse status response from API."""
        status = LiFiStatusResponse.from_api_response(_make_status_response())
        assert status.is_complete
        assert status.bridge_name == "across"
        assert status.from_chain_id == 42161
        assert status.to_chain_id == 8453
