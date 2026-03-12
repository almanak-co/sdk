"""Tests for Jupiter HTTP client."""

from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.connectors.jupiter.client import JupiterClient, JupiterConfig
from almanak.framework.connectors.jupiter.exceptions import (
    JupiterAPIError,
    JupiterConfigError,
    JupiterPriceImpactError,
)
from almanak.framework.connectors.jupiter.models import JupiterQuote, JupiterSwapTransaction


# ---------------------------------------------------------------------------
# JupiterConfig tests
# ---------------------------------------------------------------------------


class TestJupiterConfig:
    def test_valid_config(self, monkeypatch):
        monkeypatch.delenv("JUPITER_API_KEY", raising=False)
        config = JupiterConfig(wallet_address="MyWallet123456789abcdefg")
        assert config.wallet_address == "MyWallet123456789abcdefg"
        assert config.base_url == "https://lite-api.jup.ag"
        assert config.timeout == 30
        assert config.max_accounts is None

    def test_paid_url_with_api_key(self):
        config = JupiterConfig(wallet_address="wallet123", api_key="my-key")
        assert config.base_url == "https://api.jup.ag"

    def test_empty_wallet_raises(self):
        with pytest.raises(JupiterConfigError, match="wallet_address is required"):
            JupiterConfig(wallet_address="")

    def test_custom_config(self):
        config = JupiterConfig(
            wallet_address="wallet123",
            api_key="test-key",
            base_url="https://custom.jup.ag",
            timeout=60,
            max_accounts=20,
        )
        assert config.base_url == "https://custom.jup.ag"
        assert config.timeout == 60
        assert config.max_accounts == 20
        assert config.api_key == "test-key"

    def test_api_key_from_env(self, monkeypatch):
        monkeypatch.setenv("JUPITER_API_KEY", "env-key-123")
        config = JupiterConfig(wallet_address="wallet123")
        assert config.api_key == "env-key-123"

    def test_api_key_explicit_overrides_env(self, monkeypatch):
        monkeypatch.setenv("JUPITER_API_KEY", "env-key")
        config = JupiterConfig(wallet_address="wallet123", api_key="explicit-key")
        assert config.api_key == "explicit-key"


# ---------------------------------------------------------------------------
# JupiterClient tests
# ---------------------------------------------------------------------------

USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
WSOL_MINT = "So11111111111111111111111111111111111111112"


@pytest.fixture
def jupiter_config():
    return JupiterConfig(wallet_address="TestWallet123456789abcdef")


@pytest.fixture
def jupiter_client(jupiter_config):
    return JupiterClient(jupiter_config)


MOCK_QUOTE_RESPONSE = {
    "inputMint": USDC_MINT,
    "outputMint": WSOL_MINT,
    "inAmount": "1000000000",
    "outAmount": "6666666",
    "otherAmountThreshold": "6633333",
    "priceImpactPct": "0.12",
    "slippageBps": 50,
    "routePlan": [],
}

MOCK_SWAP_RESPONSE = {
    "swapTransaction": "base64tx_data_here",
    "lastValidBlockHeight": 280000000,
    "prioritizationFeeLamports": 5000,
}


class TestJupiterClientGetQuote:
    @patch.object(JupiterClient, "_make_request")
    def test_get_quote_success(self, mock_request, jupiter_client):
        mock_request.return_value = MOCK_QUOTE_RESPONSE

        quote = jupiter_client.get_quote(
            input_mint=USDC_MINT,
            output_mint=WSOL_MINT,
            amount=1000000000,
            slippage_bps=50,
        )

        assert isinstance(quote, JupiterQuote)
        assert quote.input_mint == USDC_MINT
        assert quote.output_mint == WSOL_MINT
        assert quote.out_amount == "6666666"

        mock_request.assert_called_once_with(
            "GET",
            "/swap/v1/quote",
            params={
                "inputMint": USDC_MINT,
                "outputMint": WSOL_MINT,
                "amount": "1000000000",
                "slippageBps": 50,
            },
        )

    @patch.object(JupiterClient, "_make_request")
    def test_get_quote_with_max_accounts(self, mock_request, jupiter_config):
        jupiter_config.max_accounts = 20
        client = JupiterClient(jupiter_config)
        mock_request.return_value = MOCK_QUOTE_RESPONSE

        client.get_quote(
            input_mint=USDC_MINT,
            output_mint=WSOL_MINT,
            amount=1000000000,
        )

        call_params = mock_request.call_args[1]["params"]
        assert call_params["maxAccounts"] == 20

    @patch.object(JupiterClient, "_make_request")
    def test_get_quote_price_impact_exceeded(self, mock_request, jupiter_client):
        """Test that price impact validation raises JupiterPriceImpactError."""
        mock_request.return_value = {
            **MOCK_QUOTE_RESPONSE,
            "priceImpactPct": "5.0",
        }

        with pytest.raises(JupiterPriceImpactError) as exc_info:
            jupiter_client.get_quote(
                input_mint=USDC_MINT,
                output_mint=WSOL_MINT,
                amount=1000000000,
                max_price_impact_pct=1.0,
            )

        assert exc_info.value.price_impact_pct == pytest.approx(5.0)
        assert exc_info.value.threshold_pct == pytest.approx(1.0)

    @patch.object(JupiterClient, "_make_request")
    def test_get_quote_price_impact_within_threshold(self, mock_request, jupiter_client):
        """Test that price impact within threshold passes."""
        mock_request.return_value = MOCK_QUOTE_RESPONSE  # 0.12% impact

        quote = jupiter_client.get_quote(
            input_mint=USDC_MINT,
            output_mint=WSOL_MINT,
            amount=1000000000,
            max_price_impact_pct=1.0,
        )

        assert quote is not None


class TestJupiterClientGetSwapTransaction:
    @patch.object(JupiterClient, "_make_request")
    def test_get_swap_transaction_success(self, mock_request, jupiter_client):
        mock_request.return_value = MOCK_SWAP_RESPONSE
        quote = JupiterQuote.from_api_response(MOCK_QUOTE_RESPONSE)

        swap_tx = jupiter_client.get_swap_transaction(quote)

        assert isinstance(swap_tx, JupiterSwapTransaction)
        assert swap_tx.swap_transaction == "base64tx_data_here"
        assert swap_tx.last_valid_block_height == 280000000
        assert swap_tx.quote is quote

        # Verify the POST payload
        call_args = mock_request.call_args
        assert call_args[0] == ("POST", "/swap/v1/swap")
        payload = call_args[1]["json_data"]
        assert payload["userPublicKey"] == "TestWallet123456789abcdef"
        assert payload["wrapAndUnwrapSol"] is True
        assert payload["dynamicComputeUnitLimit"] is True
        assert payload["dynamicSlippage"] is True
        assert "prioritizationFeeLamports" in payload

    @patch.object(JupiterClient, "_make_request")
    def test_get_swap_transaction_custom_pubkey(self, mock_request, jupiter_client):
        mock_request.return_value = MOCK_SWAP_RESPONSE
        quote = JupiterQuote.from_api_response(MOCK_QUOTE_RESPONSE)

        jupiter_client.get_swap_transaction(quote, user_public_key="CustomPubKey")

        call_args = mock_request.call_args
        payload = call_args[1]["json_data"]
        assert payload["userPublicKey"] == "CustomPubKey"


class TestJupiterClientPriorityFee:
    @patch.object(JupiterClient, "_make_request")
    def test_default_priority_fee(self, mock_request, jupiter_client):
        """Default priority fee should be veryHigh with 1M lamports."""
        mock_request.return_value = MOCK_SWAP_RESPONSE
        quote = JupiterQuote.from_api_response(MOCK_QUOTE_RESPONSE)

        jupiter_client.get_swap_transaction(quote)

        payload = mock_request.call_args[1]["json_data"]
        fee_config = payload["prioritizationFeeLamports"]["priorityLevelWithMaxLamports"]
        assert fee_config["priorityLevel"] == "veryHigh"
        assert fee_config["maxLamports"] == 1_000_000

    @patch.object(JupiterClient, "_make_request")
    def test_custom_priority_fee_level(self, mock_request, jupiter_client):
        """Custom priority fee level should be respected."""
        mock_request.return_value = MOCK_SWAP_RESPONSE
        quote = JupiterQuote.from_api_response(MOCK_QUOTE_RESPONSE)

        jupiter_client.get_swap_transaction(quote, priority_fee_level="low")

        payload = mock_request.call_args[1]["json_data"]
        fee_config = payload["prioritizationFeeLamports"]["priorityLevelWithMaxLamports"]
        assert fee_config["priorityLevel"] == "low"
        assert fee_config["maxLamports"] == 1_000_000  # default max

    @patch.object(JupiterClient, "_make_request")
    def test_custom_max_lamports(self, mock_request, jupiter_client):
        """Custom max lamports should be respected."""
        mock_request.return_value = MOCK_SWAP_RESPONSE
        quote = JupiterQuote.from_api_response(MOCK_QUOTE_RESPONSE)

        jupiter_client.get_swap_transaction(
            quote, priority_fee_level="medium", priority_fee_max_lamports=500_000
        )

        payload = mock_request.call_args[1]["json_data"]
        fee_config = payload["prioritizationFeeLamports"]["priorityLevelWithMaxLamports"]
        assert fee_config["priorityLevel"] == "medium"
        assert fee_config["maxLamports"] == 500_000

    def test_invalid_priority_fee_level_raises(self, jupiter_client):
        """Invalid priority fee level should raise ValueError."""
        quote = JupiterQuote.from_api_response(MOCK_QUOTE_RESPONSE)

        with pytest.raises(ValueError, match="Invalid priority_fee_level"):
            jupiter_client.get_swap_transaction(quote, priority_fee_level="ultra")


class TestJupiterClientErrors:
    @patch.object(JupiterClient, "_make_request")
    def test_api_error_propagates(self, mock_request, jupiter_client):
        mock_request.side_effect = JupiterAPIError(
            message="Bad request",
            status_code=400,
            endpoint="/swap/v1/quote",
            error_data={"error": "Invalid mint"},
        )

        with pytest.raises(JupiterAPIError) as exc_info:
            jupiter_client.get_quote(
                input_mint="invalid",
                output_mint=WSOL_MINT,
                amount=100,
            )

        assert exc_info.value.status_code == 400

    def test_wallet_address_property(self, jupiter_client, jupiter_config):
        assert jupiter_client.wallet_address == jupiter_config.wallet_address


class TestJupiterClientApiKey:
    def test_api_key_sent_in_header(self):
        config = JupiterConfig(wallet_address="wallet123", api_key="test-api-key")
        client = JupiterClient(config)
        assert client.session.headers.get("x-api-key") == "test-api-key"

    def test_no_api_key_header_when_missing(self, monkeypatch):
        monkeypatch.delenv("JUPITER_API_KEY", raising=False)
        config = JupiterConfig(wallet_address="wallet123")
        client = JupiterClient(config)
        assert "x-api-key" not in client.session.headers
