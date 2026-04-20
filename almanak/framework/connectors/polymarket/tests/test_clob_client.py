"""Tests for Polymarket CLOB client.

Tests cover:
- L1 Authentication (EIP-712 signing)
- L2 Authentication (HMAC-SHA256)
- API credentials creation/derivation
- Market data fetching
"""

import base64
import hashlib
import hmac
import time
from datetime import UTC
from decimal import Decimal
from unittest.mock import MagicMock, patch

import httpx
import pytest
from eth_account import Account
from pydantic import SecretStr

from almanak.framework.connectors.polymarket import (
    ApiCredentials,
    ClobClient,
    GammaMarket,
    OrderBook,
    PolymarketConfig,
    SignatureType,
    TokenPrice,
)
from almanak.framework.connectors.polymarket.exceptions import (
    PolymarketAPIError,
    PolymarketAuthenticationError,
    PolymarketRateLimitError,
)
from almanak.framework.connectors.polymarket.models import (
    CLOB_AUTH_DOMAIN,
    CLOB_AUTH_MESSAGE,
    CLOB_AUTH_TYPES,
)

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def test_account():
    """Create a test Ethereum account."""
    # Use a deterministic account for testing
    return Account.from_key("0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")


@pytest.fixture
def config(test_account):
    """Create test configuration."""
    return PolymarketConfig(
        wallet_address=test_account.address,
        private_key=SecretStr(test_account.key.hex()),
        signature_type=SignatureType.EOA,
    )


@pytest.fixture
def credentials():
    """Create test API credentials."""
    # Generate a base64 secret
    secret = base64.b64encode(b"test_secret_key_123").decode()
    return ApiCredentials(
        api_key="test_api_key",
        secret=SecretStr(secret),
        passphrase=SecretStr("test_passphrase"),
    )


@pytest.fixture
def config_with_credentials(config, credentials):
    """Create configuration with pre-existing credentials."""
    return PolymarketConfig(
        wallet_address=config.wallet_address,
        private_key=config.private_key,
        signature_type=config.signature_type,
        api_credentials=credentials,
    )


@pytest.fixture
def mock_http_client():
    """Create a mock HTTP client."""
    return MagicMock(spec=httpx.Client)


# =============================================================================
# L1 Authentication Tests
# =============================================================================


class TestL1Authentication:
    """Tests for L1 (EIP-712) authentication."""

    def test_build_l1_headers_contains_required_fields(self, config):
        """L1 headers should contain all required fields."""
        client = ClobClient(config)
        headers = client._build_l1_headers()

        assert "POLY_ADDRESS" in headers
        assert "POLY_SIGNATURE" in headers
        assert "POLY_TIMESTAMP" in headers
        assert "POLY_NONCE" in headers

    def test_build_l1_headers_wallet_address(self, config):
        """L1 headers should contain correct wallet address."""
        client = ClobClient(config)
        headers = client._build_l1_headers()

        assert headers["POLY_ADDRESS"] == config.wallet_address

    def test_build_l1_headers_signature_is_hex(self, config):
        """L1 signature should be a valid hex string."""
        client = ClobClient(config)
        headers = client._build_l1_headers()

        signature = headers["POLY_SIGNATURE"]
        # Should be hex without 0x prefix (from .hex())
        assert all(c in "0123456789abcdef" for c in signature)
        # EIP-712 signatures are 65 bytes = 130 hex chars
        assert len(signature) == 130

    def test_build_l1_headers_timestamp_is_recent(self, config):
        """L1 timestamp should be recent."""
        client = ClobClient(config)
        headers = client._build_l1_headers()

        timestamp = int(headers["POLY_TIMESTAMP"])
        current_time = int(time.time())

        # Should be within 5 seconds
        assert abs(timestamp - current_time) < 5

    def test_build_l1_headers_nonce_default(self, config):
        """L1 nonce should default to 0."""
        client = ClobClient(config)
        headers = client._build_l1_headers()

        assert headers["POLY_NONCE"] == "0"

    def test_build_l1_headers_custom_nonce(self, config):
        """L1 nonce should accept custom value."""
        client = ClobClient(config)
        headers = client._build_l1_headers(nonce=42)

        assert headers["POLY_NONCE"] == "42"

    def test_signature_is_recoverable(self, config, test_account):
        """Signature should be recoverable to the original address."""
        from eth_account.messages import encode_typed_data

        client = ClobClient(config)
        headers = client._build_l1_headers()

        timestamp = headers["POLY_TIMESTAMP"]
        signature_hex = headers["POLY_SIGNATURE"]

        # Reconstruct the typed data
        typed_data = {
            "types": {
                "EIP712Domain": [
                    {"name": "name", "type": "string"},
                    {"name": "version", "type": "string"},
                    {"name": "chainId", "type": "uint256"},
                ],
                **CLOB_AUTH_TYPES,
            },
            "primaryType": "ClobAuth",
            "domain": CLOB_AUTH_DOMAIN,
            "message": {
                "address": config.wallet_address,
                "timestamp": timestamp,
                "nonce": 0,
                "message": CLOB_AUTH_MESSAGE,
            },
        }

        signable = encode_typed_data(full_message=typed_data)
        recovered = Account.recover_message(signable, signature=bytes.fromhex(signature_hex))

        assert recovered == test_account.address


# =============================================================================
# L2 Authentication Tests
# =============================================================================


class TestL2Authentication:
    """Tests for L2 (HMAC-SHA256) authentication."""

    def test_build_l2_headers_contains_required_fields(self, config_with_credentials):
        """L2 headers should contain all required fields."""
        client = ClobClient(config_with_credentials)
        headers = client._build_l2_headers("GET", "/test")

        assert "POLY_ADDRESS" in headers
        assert "POLY_SIGNATURE" in headers
        assert "POLY_TIMESTAMP" in headers
        assert "POLY_API_KEY" in headers
        assert "POLY_PASSPHRASE" in headers

    def test_build_l2_headers_api_key(self, config_with_credentials):
        """L2 headers should contain correct API key."""
        client = ClobClient(config_with_credentials)
        headers = client._build_l2_headers("GET", "/test")

        assert headers["POLY_API_KEY"] == "test_api_key"

    def test_build_l2_headers_passphrase(self, config_with_credentials):
        """L2 headers should contain correct passphrase."""
        client = ClobClient(config_with_credentials)
        headers = client._build_l2_headers("GET", "/test")

        assert headers["POLY_PASSPHRASE"] == "test_passphrase"

    def test_build_l2_signature_format(self, config_with_credentials):
        """L2 signature should be base64 encoded."""
        client = ClobClient(config_with_credentials)
        headers = client._build_l2_headers("GET", "/test")

        signature = headers["POLY_SIGNATURE"]
        # Should be valid base64
        try:
            decoded = base64.b64decode(signature)
            # SHA256 produces 32 bytes
            assert len(decoded) == 32
        except Exception:
            pytest.fail("Signature is not valid base64")

    def test_build_l2_signature_reproducible(self, config_with_credentials):
        """L2 signature should be reproducible with same inputs."""
        client = ClobClient(config_with_credentials)

        # Get the timestamp from first call
        with patch("time.time", return_value=1704067200):  # Fixed timestamp
            headers1 = client._build_l2_headers("GET", "/test")
            headers2 = client._build_l2_headers("GET", "/test")

        assert headers1["POLY_SIGNATURE"] == headers2["POLY_SIGNATURE"]

    def test_build_l2_signature_includes_body(self, config_with_credentials):
        """L2 signature should include request body."""
        client = ClobClient(config_with_credentials)

        with patch("time.time", return_value=1704067200):
            headers_no_body = client._build_l2_headers("POST", "/order", "")
            headers_with_body = client._build_l2_headers("POST", "/order", '{"test":1}')

        assert headers_no_body["POLY_SIGNATURE"] != headers_with_body["POLY_SIGNATURE"]

    def test_build_l2_signature_manual_verification(self, config_with_credentials, credentials):
        """Manually verify L2 signature computation."""
        client = ClobClient(config_with_credentials)

        timestamp = "1704067200"
        method = "GET"
        path = "/test"
        body = ""

        with patch("time.time", return_value=int(timestamp)):
            headers = client._build_l2_headers(method, path, body)

        # Manually compute expected signature
        secret = credentials.secret.get_secret_value()
        message = f"{timestamp}{method}{path}{body}"
        expected_sig = hmac.new(
            base64.b64decode(secret),
            message.encode("utf-8"),
            hashlib.sha256,
        ).digest()
        expected_b64 = base64.b64encode(expected_sig).decode("utf-8")

        assert headers["POLY_SIGNATURE"] == expected_b64


# =============================================================================
# Credential Management Tests
# =============================================================================


class TestCredentialManagement:
    """Tests for API credential creation and management."""

    def test_create_api_credentials_success(self, config):
        """Should create credentials successfully."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "apiKey": "new_api_key",
            "secret": base64.b64encode(b"new_secret").decode(),
            "passphrase": "new_passphrase",
        }

        mock_http = MagicMock(spec=httpx.Client)
        mock_http.post.return_value = mock_response

        client = ClobClient(config, http_client=mock_http)
        credentials = client.create_api_credentials()

        assert credentials.api_key == "new_api_key"
        assert credentials.passphrase.get_secret_value() == "new_passphrase"
        assert client.credentials == credentials

    def test_create_api_credentials_failure(self, config):
        """Should raise error on credential creation failure."""
        mock_response = MagicMock()
        mock_response.status_code = 401
        mock_response.text = "Unauthorized"
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "Error", request=MagicMock(), response=mock_response
        )

        mock_http = MagicMock(spec=httpx.Client)
        mock_http.post.return_value = mock_response

        client = ClobClient(config, http_client=mock_http)

        with pytest.raises(PolymarketAuthenticationError):
            client.create_api_credentials()

    def test_derive_api_credentials_success(self, config):
        """Should derive existing credentials successfully."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "apiKey": "derived_api_key",
            "secret": base64.b64encode(b"derived_secret").decode(),
            "passphrase": "derived_passphrase",
        }

        mock_http = MagicMock(spec=httpx.Client)
        mock_http.get.return_value = mock_response

        client = ClobClient(config, http_client=mock_http)
        credentials = client.derive_api_credentials()

        assert credentials.api_key == "derived_api_key"

    def test_get_or_create_credentials_uses_existing(self, config_with_credentials):
        """Should return existing credentials if available."""
        client = ClobClient(config_with_credentials)
        credentials = client.get_or_create_credentials()

        assert credentials.api_key == "test_api_key"

    def test_get_or_create_credentials_derives_first(self, config):
        """Should try to derive before creating."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "apiKey": "derived_api_key",
            "secret": base64.b64encode(b"derived_secret").decode(),
            "passphrase": "derived_passphrase",
        }

        mock_http = MagicMock(spec=httpx.Client)
        mock_http.get.return_value = mock_response

        client = ClobClient(config, http_client=mock_http)
        credentials = client.get_or_create_credentials()

        # Should have called derive (GET), not create (POST)
        mock_http.get.assert_called_once()
        mock_http.post.assert_not_called()
        assert credentials.api_key == "derived_api_key"


# =============================================================================
# Model Tests
# =============================================================================


class TestModels:
    """Tests for data models."""

    def test_gamma_market_from_api_response(self):
        """Should parse Gamma API response correctly."""
        response = {
            "id": "123",
            "conditionId": "0xabc",
            "question": "Will BTC hit 100k?",
            "slug": "btc-100k",
            "outcomes": '["Yes", "No"]',
            "outcomePrices": '["0.65", "0.35"]',
            "clobTokenIds": '["token1", "token2"]',
            "volume": "1000000",
            "volume24hr": "50000",
            "liquidity": "25000",
            "endDate": "2025-12-31T23:59:59Z",
            "active": True,
            "closed": False,
            "enableOrderBook": True,
            "bestBid": "0.64",
            "bestAsk": "0.66",
        }

        market = GammaMarket.from_api_response(response)

        assert market.id == "123"
        assert market.condition_id == "0xabc"
        assert market.question == "Will BTC hit 100k?"
        assert market.outcomes == ["Yes", "No"]
        assert market.yes_price == Decimal("0.65")
        assert market.no_price == Decimal("0.35")
        assert market.yes_token_id == "token1"
        assert market.no_token_id == "token2"
        assert market.active is True

    def test_orderbook_from_api_response(self):
        """Should parse orderbook response correctly."""
        response = {
            "market": "token123",
            "asset_id": "token123",
            "bids": [
                {"price": "0.64", "size": "1000"},
                {"price": "0.63", "size": "2000"},
            ],
            "asks": [
                {"price": "0.66", "size": "1500"},
                {"price": "0.67", "size": "2500"},
            ],
            "hash": "0xabc123",
        }

        orderbook = OrderBook.from_api_response(response)

        assert orderbook.market == "token123"
        assert len(orderbook.bids) == 2
        assert len(orderbook.asks) == 2
        assert orderbook.best_bid == Decimal("0.64")
        assert orderbook.best_ask == Decimal("0.66")
        assert orderbook.spread == Decimal("0.02")

    def test_token_price_from_api_response(self):
        """Should parse price response correctly."""
        response = {
            "bid": "0.64",
            "ask": "0.66",
            "mid": "0.65",
        }

        price = TokenPrice.from_api_response(response)

        assert price.bid == Decimal("0.64")
        assert price.ask == Decimal("0.66")
        assert price.mid == Decimal("0.65")

    def test_api_credentials_from_dict(self):
        """Should create credentials from dict."""
        data = {
            "apiKey": "test_key",
            "secret": "dGVzdF9zZWNyZXQ=",  # base64 of "test_secret"
            "passphrase": "test_pass",
        }

        credentials = ApiCredentials.from_dict(data)

        assert credentials.api_key == "test_key"
        assert credentials.passphrase.get_secret_value() == "test_pass"


# =============================================================================
# Signature Type Tests
# =============================================================================


class TestSignatureTypes:
    """Tests for different signature types."""

    def test_eoa_signature_type(self, test_account):
        """EOA signature type should work correctly."""
        config = PolymarketConfig(
            wallet_address=test_account.address,
            private_key=SecretStr(test_account.key.hex()),
            signature_type=SignatureType.EOA,
        )

        client = ClobClient(config)
        headers = client._build_l1_headers()

        # Should still produce valid signature
        assert len(headers["POLY_SIGNATURE"]) == 130

    def test_signature_type_values(self):
        """Signature types should have correct integer values."""
        assert SignatureType.EOA.value == 0
        assert SignatureType.POLY_PROXY.value == 1
        assert SignatureType.POLY_GNOSIS_SAFE.value == 2


# =============================================================================
# Error Handling Tests
# =============================================================================


class TestErrorHandling:
    """Tests for error handling."""

    def test_rate_limit_error(self, config_with_credentials):
        """Should raise rate limit error on 429 response."""
        mock_response = MagicMock()
        mock_response.status_code = 429
        mock_response.headers = {"Retry-After": "60"}

        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.return_value = mock_response

        client = ClobClient(config_with_credentials, http_client=mock_http)

        with pytest.raises(PolymarketRateLimitError) as exc_info:
            client._request("GET", "https://clob.polymarket.com/test")

        assert exc_info.value.retry_after == 60

    def test_api_error_on_4xx(self, config_with_credentials):
        """Should raise API error on 4xx response."""
        mock_response = MagicMock()
        mock_response.status_code = 400
        mock_response.text = "Bad request"
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "Error", request=MagicMock(), response=mock_response
        )

        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.return_value = mock_response

        client = ClobClient(config_with_credentials, http_client=mock_http)

        with pytest.raises(PolymarketAPIError) as exc_info:
            client._request("GET", "https://clob.polymarket.com/test")

        assert exc_info.value.status_code == 400


# =============================================================================
# Caching Tests
# =============================================================================


class TestCaching:
    """Tests for response caching."""

    def test_cache_set_and_get(self, config_with_credentials):
        """Should cache and retrieve values."""
        client = ClobClient(config_with_credentials)

        client._set_cached("test_key", {"data": "value"}, ttl=60)
        result = client._get_cached("test_key")

        assert result == {"data": "value"}

    def test_cache_expiration(self, config_with_credentials):
        """Should return None for expired cache."""
        client = ClobClient(config_with_credentials)

        # Set with very short TTL
        client._set_cached("test_key", {"data": "value"}, ttl=0)

        # Sleep briefly to ensure expiration
        time.sleep(0.1)

        result = client._get_cached("test_key")
        assert result is None

    def test_cache_miss(self, config_with_credentials):
        """Should return None for missing cache key."""
        client = ClobClient(config_with_credentials)
        result = client._get_cached("nonexistent_key")
        assert result is None


# =============================================================================
# Market Data Tests (US-002)
# =============================================================================


class TestMarketData:
    """Tests for market data fetching functionality."""

    def test_get_markets_success(self, config_with_credentials):
        """Should fetch and parse markets from Gamma API."""
        # Real API response format from Polymarket
        api_response = [
            {
                "id": "0x1234",
                "conditionId": "0x9915bea232fa12b20058f9cea1187ea51366352bf833393676cd0db557a58249",
                "question": "Will Bitcoin exceed $100,000 by end of 2025?",
                "slug": "will-bitcoin-exceed-100000-by-end-of-2025",
                "outcomes": '["Yes", "No"]',
                "outcomePrices": '["0.65", "0.35"]',
                "clobTokenIds": '["19045189272319329424023217822141741659150265216200539353252147725932663608488", "28164726938309329424023217822141741659150265216200539353252147725932663608489"]',
                "volume": "1500000",
                "volume24hr": 125000,
                "liquidity": "50000",
                "endDate": "2025-12-31T23:59:59Z",
                "active": True,
                "closed": False,
                "enableOrderBook": True,
                "orderPriceMinTickSize": 0.01,
                "orderMinSize": 5,
                "bestBid": 0.64,
                "bestAsk": 0.66,
            }
        ]

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"data"
        mock_response.json.return_value = api_response

        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.return_value = mock_response

        client = ClobClient(config_with_credentials, http_client=mock_http)
        markets = client.get_markets()

        assert len(markets) == 1
        market = markets[0]
        assert market.id == "0x1234"
        assert market.question == "Will Bitcoin exceed $100,000 by end of 2025?"
        assert market.yes_price == Decimal("0.65")
        assert market.no_price == Decimal("0.35")
        assert market.active is True
        assert market.volume == Decimal("1500000")

    def test_get_markets_with_filters(self, config_with_credentials):
        """Should apply filters to market query."""
        from almanak.framework.connectors.polymarket.models import MarketFilters

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"[]"
        mock_response.json.return_value = []

        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.return_value = mock_response

        client = ClobClient(config_with_credentials, http_client=mock_http)
        filters = MarketFilters(active=True, closed=False, limit=50)
        client.get_markets(filters=filters)

        # Verify the request was made with correct params
        call_args = mock_http.request.call_args
        params = call_args.kwargs.get("params", {})
        assert params.get("active") == "true"
        assert params.get("closed") == "false"
        assert params.get("limit") == 50

    def test_get_market_by_id(self, config_with_credentials):
        """Should fetch single market by ID."""
        api_response = {
            "id": "12345",
            "conditionId": "0xabc123",
            "question": "Test market?",
            "slug": "test-market",
            "outcomes": '["Yes", "No"]',
            "outcomePrices": '["0.50", "0.50"]',
            "clobTokenIds": '["token1", "token2"]',
            "volume": "10000",
            "liquidity": "5000",
            "active": True,
            "closed": False,
            "enableOrderBook": True,
        }

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"data"
        mock_response.json.return_value = api_response

        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.return_value = mock_response

        client = ClobClient(config_with_credentials, http_client=mock_http)
        market = client.get_market("12345")

        assert market.id == "12345"
        assert market.question == "Test market?"

    def test_get_market_caching(self, config_with_credentials):
        """Should cache market data."""
        api_response = {
            "id": "12345",
            "conditionId": "0xabc",
            "question": "Test?",
            "slug": "test",
            "outcomes": '["Yes", "No"]',
            "outcomePrices": '["0.5", "0.5"]',
            "clobTokenIds": "[]",
            "volume": "0",
            "liquidity": "0",
            "active": True,
            "closed": False,
            "enableOrderBook": True,
        }

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"data"
        mock_response.json.return_value = api_response

        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.return_value = mock_response

        client = ClobClient(config_with_credentials, http_client=mock_http)

        # First call should hit API
        market1 = client.get_market("12345")
        # Second call should use cache
        market2 = client.get_market("12345")

        assert market1.id == market2.id
        # Should only have made one request
        assert mock_http.request.call_count == 1

    def test_get_orderbook(self, config_with_credentials):
        """Should fetch and parse orderbook."""
        # Real API response format
        api_response = {
            "market": "19045189272319329424023217822141741659150265216200539353252147725932663608488",
            "asset_id": "19045189272319329424023217822141741659150265216200539353252147725932663608488",
            "bids": [
                {"price": "0.64", "size": "1000"},
                {"price": "0.63", "size": "2500"},
                {"price": "0.62", "size": "5000"},
            ],
            "asks": [
                {"price": "0.66", "size": "1500"},
                {"price": "0.67", "size": "3000"},
                {"price": "0.68", "size": "4500"},
            ],
            "hash": "0xabc123def456",
        }

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"data"
        mock_response.json.return_value = api_response

        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.return_value = mock_response

        client = ClobClient(config_with_credentials, http_client=mock_http)
        orderbook = client.get_orderbook("token123")

        assert len(orderbook.bids) == 3
        assert len(orderbook.asks) == 3
        assert orderbook.best_bid == Decimal("0.64")
        assert orderbook.best_ask == Decimal("0.66")
        assert orderbook.spread == Decimal("0.02")

    def test_get_orderbook_caching(self, config_with_credentials):
        """Should cache orderbook data."""
        api_response = {
            "market": "token123",
            "asset_id": "token123",
            "bids": [{"price": "0.50", "size": "100"}],
            "asks": [{"price": "0.51", "size": "100"}],
            "hash": "0xabc",
        }

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"data"
        mock_response.json.return_value = api_response

        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.return_value = mock_response

        client = ClobClient(config_with_credentials, http_client=mock_http)

        # First call hits API, second uses cache
        client.get_orderbook("token123")
        client.get_orderbook("token123")

        assert mock_http.request.call_count == 1

    def test_get_price(self, config_with_credentials):
        """Should fetch and parse token price."""
        api_response = {
            "bid": "0.64",
            "ask": "0.66",
            "mid": "0.65",
        }

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"data"
        mock_response.json.return_value = api_response

        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.return_value = mock_response

        client = ClobClient(config_with_credentials, http_client=mock_http)
        price = client.get_price("token123")

        assert price.bid == Decimal("0.64")
        assert price.ask == Decimal("0.66")
        assert price.mid == Decimal("0.65")

    def test_get_price_caching(self, config_with_credentials):
        """Should cache price data."""
        api_response = {"bid": "0.50", "ask": "0.51", "mid": "0.505"}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"data"
        mock_response.json.return_value = api_response

        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.return_value = mock_response

        client = ClobClient(config_with_credentials, http_client=mock_http)

        client.get_price("token123")
        client.get_price("token123")

        assert mock_http.request.call_count == 1

    def test_get_midpoint(self, config_with_credentials):
        """Should fetch midpoint price."""
        api_response = {"mid": "0.65"}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"data"
        mock_response.json.return_value = api_response

        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.return_value = mock_response

        client = ClobClient(config_with_credentials, http_client=mock_http)
        midpoint = client.get_midpoint("token123")

        assert midpoint == Decimal("0.65")

    def test_get_tick_size(self, config_with_credentials):
        """Should fetch minimum tick size."""
        api_response = {"minimum_tick_size": "0.01"}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"data"
        mock_response.json.return_value = api_response

        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.return_value = mock_response

        client = ClobClient(config_with_credentials, http_client=mock_http)
        tick_size = client.get_tick_size("token123")

        assert tick_size == Decimal("0.01")

    def test_get_market_by_slug(self, config_with_credentials):
        """Should fetch market by URL slug."""
        api_response = [
            {
                "id": "12345",
                "conditionId": "0xabc",
                "question": "Test market?",
                "slug": "test-market",
                "outcomes": '["Yes", "No"]',
                "outcomePrices": '["0.50", "0.50"]',
                "clobTokenIds": '["token1", "token2"]',
                "volume": "10000",
                "liquidity": "5000",
                "active": True,
                "closed": False,
                "enableOrderBook": True,
            }
        ]

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"data"
        mock_response.json.return_value = api_response

        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.return_value = mock_response

        client = ClobClient(config_with_credentials, http_client=mock_http)
        market = client.get_market_by_slug("test-market")

        assert market is not None
        assert market.slug == "test-market"

    def test_get_market_by_slug_not_found(self, config_with_credentials):
        """Should return None when market slug not found."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"[]"
        mock_response.json.return_value = []

        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.return_value = mock_response

        client = ClobClient(config_with_credentials, http_client=mock_http)
        market = client.get_market_by_slug("nonexistent-slug")

        assert market is None

    def test_health_check_success(self, config_with_credentials):
        """Should return True when API is healthy."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"{}"
        mock_response.json.return_value = {}

        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.return_value = mock_response

        client = ClobClient(config_with_credentials, http_client=mock_http)
        assert client.health_check() is True

    def test_health_check_failure(self, config_with_credentials):
        """Should return False when API is unhealthy."""
        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.side_effect = Exception("Connection failed")

        client = ClobClient(config_with_credentials, http_client=mock_http)
        assert client.health_check() is False

    def test_get_server_time(self, config_with_credentials):
        """Should fetch server timestamp."""
        api_response = {"time": 1704067200}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"data"
        mock_response.json.return_value = api_response

        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.return_value = mock_response

        client = ClobClient(config_with_credentials, http_client=mock_http)
        server_time = client.get_server_time()

        assert server_time == 1704067200

    def test_rate_limit_retry(self, config_with_credentials):
        """Should retry on rate limit with exponential backoff."""
        # First response: rate limited
        rate_limit_response = MagicMock()
        rate_limit_response.status_code = 429
        rate_limit_response.headers = {"Retry-After": "1"}

        # Second response: success
        success_response = MagicMock()
        success_response.status_code = 200
        success_response.content = b"data"
        success_response.json.return_value = {"mid": "0.65"}

        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.side_effect = [rate_limit_response, success_response]

        client = ClobClient(config_with_credentials, http_client=mock_http)

        # Should succeed after retry
        with patch("time.sleep"):  # Don't actually sleep in tests
            midpoint = client.get_midpoint("token123")

        assert midpoint == Decimal("0.65")
        assert mock_http.request.call_count == 2


# =============================================================================
# Order Management Tests (US-003)
# =============================================================================


class TestOrderBuilding:
    """Tests for order building functionality."""

    def test_build_limit_order_buy(self, config_with_credentials):
        """Should build a valid BUY limit order."""
        from almanak.framework.connectors.polymarket.models import LimitOrderParams

        client = ClobClient(config_with_credentials)
        params = LimitOrderParams(
            token_id="19045189272319329424023217822141741659150265216200539353252147725932663608488",
            side="BUY",
            price=Decimal("0.65"),
            size=Decimal("100"),
        )

        order = client.build_limit_order(params)

        # Verify order structure
        assert order.maker == config_with_credentials.wallet_address
        assert order.signer == config_with_credentials.wallet_address
        assert order.taker == "0x0000000000000000000000000000000000000000"
        assert order.side == 0  # BUY
        assert order.signature_type == 0  # EOA

        # Verify amounts: BUY 100 shares at 0.65 = 65 USDC
        # maker_amount = USDC to pay = 100 * 0.65 * 10^6 = 65,000,000
        # taker_amount = shares to receive = 100 * 10^6 = 100,000,000
        assert order.maker_amount == 65_000_000
        assert order.taker_amount == 100_000_000

        # Token ID should be parsed correctly
        assert order.token_id == int(params.token_id)

    def test_build_limit_order_sell(self, config_with_credentials):
        """Should build a valid SELL limit order."""
        from almanak.framework.connectors.polymarket.models import LimitOrderParams

        client = ClobClient(config_with_credentials)
        params = LimitOrderParams(
            token_id="19045189272319329424023217822141741659150265216200539353252147725932663608488",
            side="SELL",
            price=Decimal("0.70"),
            size=Decimal("50"),
        )

        order = client.build_limit_order(params)

        assert order.side == 1  # SELL

        # Verify amounts: SELL 50 shares at 0.70 = 35 USDC
        # maker_amount = shares to sell = 50 * 10^6 = 50,000,000
        # taker_amount = USDC to receive = 50 * 0.70 * 10^6 = 35,000,000
        assert order.maker_amount == 50_000_000
        assert order.taker_amount == 35_000_000

    def test_build_limit_order_with_expiration(self, config_with_credentials):
        """Should build order with expiration timestamp."""
        from almanak.framework.connectors.polymarket.models import LimitOrderParams

        client = ClobClient(config_with_credentials)
        expiration = int(time.time()) + 3600  # 1 hour from now

        params = LimitOrderParams(
            token_id="123",
            side="BUY",
            price=Decimal("0.50"),
            size=Decimal("10"),
            expiration=expiration,
        )

        order = client.build_limit_order(params)

        assert order.expiration == expiration

    def test_build_limit_order_invalid_price_too_low(self, config_with_credentials):
        """Should reject price below 0.01."""
        from almanak.framework.connectors.polymarket.exceptions import PolymarketInvalidPriceError
        from almanak.framework.connectors.polymarket.models import LimitOrderParams

        client = ClobClient(config_with_credentials)
        params = LimitOrderParams(
            token_id="123",
            side="BUY",
            price=Decimal("0.001"),  # Too low
            size=Decimal("100"),
        )

        with pytest.raises(PolymarketInvalidPriceError) as exc_info:
            client.build_limit_order(params)

        assert exc_info.value.price == "0.001"

    def test_build_limit_order_invalid_price_too_high(self, config_with_credentials):
        """Should reject price above 0.99."""
        from almanak.framework.connectors.polymarket.exceptions import PolymarketInvalidPriceError
        from almanak.framework.connectors.polymarket.models import LimitOrderParams

        client = ClobClient(config_with_credentials)
        params = LimitOrderParams(
            token_id="123",
            side="SELL",
            price=Decimal("1.00"),  # Too high
            size=Decimal("100"),
        )

        with pytest.raises(PolymarketInvalidPriceError):
            client.build_limit_order(params)

    def test_build_limit_order_size_too_small(self, config_with_credentials):
        """Should reject size below default minimum (5 shares)."""
        from almanak.framework.connectors.polymarket.exceptions import PolymarketMinimumOrderError
        from almanak.framework.connectors.polymarket.models import LimitOrderParams

        client = ClobClient(config_with_credentials)
        params = LimitOrderParams(
            token_id="123",
            side="BUY",
            price=Decimal("0.50"),
            size=Decimal("4"),  # Below default minimum of 5
        )

        with pytest.raises(PolymarketMinimumOrderError) as exc_info:
            client.build_limit_order(params)

        # Default minimum is 5 shares
        assert exc_info.value.minimum == "5"

    def test_build_market_order_buy(self, config_with_credentials):
        """Should build a valid BUY market order."""
        from almanak.framework.connectors.polymarket.models import MarketOrderParams

        client = ClobClient(config_with_credentials)
        params = MarketOrderParams(
            token_id="123456789",
            side="BUY",
            amount=Decimal("100"),  # USDC to spend
            worst_price=Decimal("0.70"),  # Max price per share
        )

        order = client.build_market_order(params)

        assert order.side == 0  # BUY
        # BUY 100 USDC at worst price 0.70 = ~142.85 shares
        # maker_amount = USDC to spend = 100 * 10^6
        assert order.maker_amount == 100_000_000

        # taker_amount = expected shares = 100 / 0.70 * 10^6 ≈ 142857142
        # (rounded down)
        expected_shares = Decimal("100") / Decimal("0.70")
        assert order.taker_amount == int(expected_shares * 10**6)

    def test_build_market_order_sell(self, config_with_credentials):
        """Should build a valid SELL market order."""
        from almanak.framework.connectors.polymarket.models import MarketOrderParams

        client = ClobClient(config_with_credentials)
        params = MarketOrderParams(
            token_id="123456789",
            side="SELL",
            amount=Decimal("100"),  # Shares to sell
            worst_price=Decimal("0.60"),  # Min price per share
        )

        order = client.build_market_order(params)

        assert order.side == 1  # SELL
        # SELL 100 shares at worst price 0.60 = 60 USDC expected
        assert order.maker_amount == 100_000_000  # 100 shares
        assert order.taker_amount == 60_000_000  # 60 USDC

    def test_build_market_order_default_worst_price_buy(self, config_with_credentials):
        """Should use max price (0.99) for BUY if no worst_price specified."""
        from almanak.framework.connectors.polymarket.models import MarketOrderParams

        client = ClobClient(config_with_credentials)
        params = MarketOrderParams(
            token_id="123456789",
            side="BUY",
            amount=Decimal("99"),  # USDC to spend
            worst_price=None,  # Use default
        )

        order = client.build_market_order(params)

        # At 0.99 per share, 99 USDC buys 100 shares
        assert order.maker_amount == 99_000_000
        assert order.taker_amount == 100_000_000

    def test_build_market_order_default_worst_price_sell(self, config_with_credentials):
        """Should use min price (0.01) for SELL if no worst_price specified."""
        from almanak.framework.connectors.polymarket.models import MarketOrderParams

        client = ClobClient(config_with_credentials)
        params = MarketOrderParams(
            token_id="123456789",
            side="SELL",
            amount=Decimal("100"),  # Shares to sell
            worst_price=None,  # Use default
        )

        order = client.build_market_order(params)

        # At 0.01 per share, 100 shares gets 1 USDC
        assert order.maker_amount == 100_000_000
        assert order.taker_amount == 1_000_000

    def test_salt_is_random(self, config_with_credentials):
        """Salt should be different for each order."""
        from almanak.framework.connectors.polymarket.models import LimitOrderParams

        client = ClobClient(config_with_credentials)
        params = LimitOrderParams(
            token_id="123",
            side="BUY",
            price=Decimal("0.50"),
            size=Decimal("10"),
        )

        order1 = client.build_limit_order(params)
        order2 = client.build_limit_order(params)

        assert order1.salt != order2.salt


class TestOrderSigning:
    """Tests for order signing functionality."""

    def test_sign_order_produces_valid_signature(self, config_with_credentials, test_account):
        """Signed order should have valid EIP-712 signature."""
        from almanak.framework.connectors.polymarket.models import LimitOrderParams

        client = ClobClient(config_with_credentials)
        params = LimitOrderParams(
            token_id="123",
            side="BUY",
            price=Decimal("0.50"),
            size=Decimal("10"),
        )

        unsigned = client.build_limit_order(params)
        signed = client.sign_order(unsigned)

        # Signature should be hex string (65 bytes = 130 hex chars)
        assert len(signed.signature) == 130
        assert all(c in "0123456789abcdef" for c in signed.signature)

        # Order should be preserved
        assert signed.order == unsigned

    def test_sign_order_is_recoverable(self, config_with_credentials, test_account):
        """Signature should be recoverable to the signer's address."""
        from eth_account.messages import encode_typed_data as encode_typed_data_local

        from almanak.framework.connectors.polymarket.models import (
            CTF_EXCHANGE_DOMAIN,
            ORDER_TYPES,
            LimitOrderParams,
        )

        client = ClobClient(config_with_credentials)
        params = LimitOrderParams(
            token_id="123",
            side="BUY",
            price=Decimal("0.50"),
            size=Decimal("10"),
        )

        unsigned = client.build_limit_order(params)
        signed = client.sign_order(unsigned)

        # Reconstruct typed data and verify signature
        typed_data = {
            "types": {
                "EIP712Domain": [
                    {"name": "name", "type": "string"},
                    {"name": "version", "type": "string"},
                    {"name": "chainId", "type": "uint256"},
                    {"name": "verifyingContract", "type": "address"},
                ],
                **ORDER_TYPES,
            },
            "primaryType": "Order",
            "domain": CTF_EXCHANGE_DOMAIN,
            "message": unsigned.to_struct(),
        }

        signable = encode_typed_data_local(full_message=typed_data)
        recovered = Account.recover_message(signable, signature=bytes.fromhex(signed.signature))

        assert recovered == test_account.address

    def test_create_and_sign_limit_order(self, config_with_credentials):
        """Convenience method should build and sign in one call."""
        from almanak.framework.connectors.polymarket.models import LimitOrderParams

        client = ClobClient(config_with_credentials)
        params = LimitOrderParams(
            token_id="123",
            side="BUY",
            price=Decimal("0.50"),
            size=Decimal("10"),
        )

        signed = client.create_and_sign_limit_order(params)

        assert signed.order is not None
        assert len(signed.signature) == 130

    def test_create_and_sign_market_order(self, config_with_credentials):
        """Convenience method should build and sign market order in one call."""
        from almanak.framework.connectors.polymarket.models import MarketOrderParams

        client = ClobClient(config_with_credentials)
        params = MarketOrderParams(
            token_id="123",
            side="SELL",
            amount=Decimal("100"),
            worst_price=Decimal("0.50"),
        )

        signed = client.create_and_sign_market_order(params)

        assert signed.order is not None
        assert signed.order.side == 1  # SELL
        assert len(signed.signature) == 130


class TestOrderSubmission:
    """Tests for order submission functionality."""

    def test_submit_limit_order_success(self, config_with_credentials):
        """Should submit limit order successfully."""
        from almanak.framework.connectors.polymarket.models import LimitOrderParams

        # Mock successful submission response
        api_response = {
            "orderID": "0x123abc",
            "status": "LIVE",
            "market": "123",
            "side": "BUY",
            "price": "0.50",
            "size": "10",
            "filledSize": "0",
            "createdAt": "2025-01-15T10:30:00Z",
        }

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"data"
        mock_response.json.return_value = api_response

        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.return_value = mock_response

        client = ClobClient(config_with_credentials, http_client=mock_http)

        # Build and sign order
        params = LimitOrderParams(
            token_id="123",
            side="BUY",
            price=Decimal("0.50"),
            size=Decimal("10"),
        )
        signed = client.create_and_sign_limit_order(params)

        # Submit order
        response = client.submit_order(signed)

        assert response.order_id == "0x123abc"
        assert response.status.value == "LIVE"
        assert response.side == "BUY"
        assert response.price == Decimal("0.50")

    def test_submit_order_with_order_type(self, config_with_credentials):
        """Should include order type in submission payload."""
        from almanak.framework.connectors.polymarket.models import LimitOrderParams, OrderType

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"data"
        mock_response.json.return_value = {
            "orderID": "0x456",
            "status": "LIVE",
            "market": "123",
            "side": "BUY",
            "price": "0.50",
            "size": "10",
        }

        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.return_value = mock_response

        client = ClobClient(config_with_credentials, http_client=mock_http)

        params = LimitOrderParams(
            token_id="123",
            side="BUY",
            price=Decimal("0.50"),
            size=Decimal("10"),
        )
        signed = client.create_and_sign_limit_order(params)

        # Submit with IOC (Immediate or Cancel)
        client.submit_order(signed, order_type=OrderType.IOC)

        # Verify request was made with correct order type
        call_args = mock_http.request.call_args
        body = call_args.kwargs.get("content", "")
        assert "IOC" in body

    def test_cancel_order_success(self, config_with_credentials):
        """Should cancel order successfully."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b""
        mock_response.json.return_value = None

        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.return_value = mock_response

        client = ClobClient(config_with_credentials, http_client=mock_http)

        result = client.cancel_order("0x123abc")

        assert result is True

        # Verify request was made
        call_args = mock_http.request.call_args
        assert call_args.kwargs.get("method") == "DELETE"

    def test_cancel_multiple_orders(self, config_with_credentials):
        """Should cancel multiple orders in one call."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b""
        mock_response.json.return_value = None

        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.return_value = mock_response

        client = ClobClient(config_with_credentials, http_client=mock_http)

        result = client.cancel_orders(["0x123", "0x456", "0x789"])

        assert result is True

    def test_cancel_all_orders(self, config_with_credentials):
        """Should cancel all open orders."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b""
        mock_response.json.return_value = None

        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.return_value = mock_response

        client = ClobClient(config_with_credentials, http_client=mock_http)

        result = client.cancel_all_orders()

        assert result is True

    def test_get_open_orders(self, config_with_credentials):
        """Should fetch and parse open orders."""
        api_response = [
            {
                "orderID": "0x123",
                "market": "token123",
                "side": "BUY",
                "price": "0.65",
                "size": "100",
                "filledSize": "25",
                "createdAt": "2025-01-15T10:30:00Z",
                "expiration": 1735689600,
            },
            {
                "orderID": "0x456",
                "market": "token456",
                "side": "SELL",
                "price": "0.80",
                "size": "50",
                "filledSize": "0",
                "createdAt": "2025-01-15T11:00:00Z",
            },
        ]

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"data"
        mock_response.json.return_value = api_response

        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.return_value = mock_response

        client = ClobClient(config_with_credentials, http_client=mock_http)

        orders = client.get_open_orders()

        assert len(orders) == 2
        assert orders[0].order_id == "0x123"
        assert orders[0].side == "BUY"
        assert orders[0].price == Decimal("0.65")
        assert orders[0].filled_size == Decimal("25")
        assert orders[1].order_id == "0x456"
        assert orders[1].side == "SELL"


class TestOrderPayload:
    """Tests for order payload structure."""

    def test_unsigned_order_to_struct(self, config_with_credentials):
        """UnsignedOrder.to_struct() should produce correct EIP-712 struct."""
        from almanak.framework.connectors.polymarket.models import LimitOrderParams

        client = ClobClient(config_with_credentials)
        params = LimitOrderParams(
            token_id="12345",
            side="BUY",
            price=Decimal("0.50"),
            size=Decimal("10"),
            fee_rate_bps=50,
        )

        order = client.build_limit_order(params)
        struct = order.to_struct()

        assert struct["salt"] == order.salt
        assert struct["maker"] == config_with_credentials.wallet_address
        assert struct["signer"] == config_with_credentials.wallet_address
        assert struct["taker"] == "0x0000000000000000000000000000000000000000"
        assert struct["tokenId"] == 12345
        assert struct["makerAmount"] == order.maker_amount
        assert struct["takerAmount"] == order.taker_amount
        assert struct["expiration"] == 0
        assert struct["nonce"] == 0
        assert struct["feeRateBps"] == 50
        assert struct["side"] == 0  # BUY
        assert struct["signatureType"] == 0  # EOA

    def test_signed_order_to_api_payload(self, config_with_credentials):
        """SignedOrder.to_api_payload() should produce correct API structure."""
        from almanak.framework.connectors.polymarket.models import LimitOrderParams

        client = ClobClient(config_with_credentials)
        params = LimitOrderParams(
            token_id="12345",
            side="SELL",
            price=Decimal("0.75"),
            size=Decimal("100"),
        )

        signed = client.create_and_sign_limit_order(params)
        payload = signed.to_api_payload()

        assert "order" in payload
        assert "signature" in payload

        order = payload["order"]
        assert order["salt"] == signed.order.salt
        assert order["maker"] == config_with_credentials.wallet_address
        assert order["tokenId"] == "12345"  # API expects string
        assert order["makerAmount"] == str(signed.order.maker_amount)  # API expects string
        assert order["takerAmount"] == str(signed.order.taker_amount)
        assert order["side"] == 1  # SELL
        assert order["signatureType"] == 0  # EOA

        # Signature should be hex string without 0x prefix
        assert len(payload["signature"]) == 130


# =============================================================================
# Positions & Trades Tests (US-004)
# =============================================================================


class TestPositions:
    """Tests for position fetching functionality."""

    def test_get_positions_success(self, config_with_credentials):
        """Should fetch and parse positions from Data API."""
        api_response = [
            {
                "market": "0xmarket123",
                "conditionId": "0xcondition456",
                "tokenId": "token789",
                "outcome": "Yes",
                "size": "100.5",
                "avgPrice": "0.65",
                "currentPrice": "0.72",
                "realizedPnl": "25.00",
            },
            {
                "market": "0xmarket456",
                "conditionId": "0xcondition789",
                "tokenId": "token012",
                "outcome": "No",
                "size": "50.0",
                "avgPrice": "0.30",
                "currentPrice": "0.25",
                "realizedPnl": "0",
            },
        ]

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"data"
        mock_response.json.return_value = api_response

        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.return_value = mock_response

        client = ClobClient(config_with_credentials, http_client=mock_http)
        positions = client.get_positions()

        assert len(positions) == 2

        # Check first position (YES)
        pos1 = positions[0]
        assert pos1.market_id == "0xmarket123"
        assert pos1.condition_id == "0xcondition456"
        assert pos1.token_id == "token789"
        assert pos1.outcome == "YES"
        assert pos1.size == Decimal("100.5")
        assert pos1.avg_price == Decimal("0.65")
        assert pos1.current_price == Decimal("0.72")
        assert pos1.realized_pnl == Decimal("25.00")
        # Unrealized PnL = (0.72 - 0.65) * 100.5 = 7.035
        assert pos1.unrealized_pnl == Decimal("7.035")

        # Check second position (NO)
        pos2 = positions[1]
        assert pos2.outcome == "NO"
        assert pos2.size == Decimal("50.0")
        # Unrealized PnL = (0.25 - 0.30) * 50 = -2.5
        assert pos2.unrealized_pnl == Decimal("-2.5")

    def test_get_positions_uses_config_wallet_by_default(self, config_with_credentials):
        """Should use config wallet address if none specified."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"[]"
        mock_response.json.return_value = []

        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.return_value = mock_response

        client = ClobClient(config_with_credentials, http_client=mock_http)
        client.get_positions()

        # Verify request was made with config wallet address
        call_args = mock_http.request.call_args
        params = call_args.kwargs.get("params", {})
        assert params.get("user") == config_with_credentials.wallet_address

    def test_get_positions_custom_wallet(self, config_with_credentials):
        """Should use custom wallet address when specified."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"[]"
        mock_response.json.return_value = []

        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.return_value = mock_response

        client = ClobClient(config_with_credentials, http_client=mock_http)
        custom_wallet = "0x1234567890123456789012345678901234567890"
        client.get_positions(wallet=custom_wallet)

        # Verify request was made with custom wallet
        call_args = mock_http.request.call_args
        params = call_args.kwargs.get("params", {})
        assert params.get("user") == custom_wallet

    def test_get_positions_with_filters(self, config_with_credentials):
        """Should apply filters to position query."""
        from almanak.framework.connectors.polymarket.models import PositionFilters

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"[]"
        mock_response.json.return_value = []

        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.return_value = mock_response

        client = ClobClient(config_with_credentials, http_client=mock_http)
        filters = PositionFilters(market="0xmarket123", outcome="YES")
        client.get_positions(filters=filters)

        # Verify request was made with filters
        call_args = mock_http.request.call_args
        params = call_args.kwargs.get("params", {})
        assert params.get("market") == "0xmarket123"
        assert params.get("outcome") == "YES"

    def test_get_positions_empty_response(self, config_with_credentials):
        """Should handle empty positions list."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"[]"
        mock_response.json.return_value = []

        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.return_value = mock_response

        client = ClobClient(config_with_credentials, http_client=mock_http)
        positions = client.get_positions()

        assert positions == []

    def test_get_positions_handles_malformed_data(self, config_with_credentials):
        """Should skip malformed position entries."""
        api_response = [
            {
                "market": "0xmarket123",
                "conditionId": "0xcondition456",
                "tokenId": "token789",
                "outcome": "Yes",
                "size": "100",
                "avgPrice": "0.65",
                "currentPrice": "0.72",
                "realizedPnl": "0",
            },
            {
                # Missing required fields - should be skipped
                "market": "0xmarket456",
            },
        ]

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"data"
        mock_response.json.return_value = api_response

        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.return_value = mock_response

        client = ClobClient(config_with_credentials, http_client=mock_http)
        positions = client.get_positions()

        # Should still return the valid position
        assert len(positions) >= 1
        assert positions[0].market_id == "0xmarket123"


class TestTrades:
    """Tests for trade history functionality."""

    def test_get_trades_success(self, config_with_credentials):
        """Should fetch and parse trade history."""
        api_response = [
            {
                "id": "trade123",
                "market": "0xmarket456",
                "tokenId": "token789",
                "side": "BUY",
                "price": "0.65",
                "size": "100",
                "fee": "0.50",
                "timestamp": "2025-01-15T10:30:00Z",
                "status": "CONFIRMED",
            },
            {
                "id": "trade456",
                "market": "0xmarket789",
                "tokenId": "token012",
                "side": "SELL",
                "price": "0.80",
                "size": "50",
                "fee": "0.25",
                "timestamp": "2025-01-15T11:00:00Z",
                "status": "MATCHED",
            },
        ]

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"data"
        mock_response.json.return_value = api_response

        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.return_value = mock_response

        client = ClobClient(config_with_credentials, http_client=mock_http)
        trades = client.get_trades()

        assert len(trades) == 2

        # Check first trade
        trade1 = trades[0]
        assert trade1.id == "trade123"
        assert trade1.market_id == "0xmarket456"
        assert trade1.token_id == "token789"
        assert trade1.side == "BUY"
        assert trade1.price == Decimal("0.65")
        assert trade1.size == Decimal("100")
        assert trade1.fee == Decimal("0.50")
        assert trade1.status.value == "CONFIRMED"

        # Check second trade
        trade2 = trades[1]
        assert trade2.side == "SELL"
        assert trade2.status.value == "MATCHED"

    def test_get_trades_with_filters(self, config_with_credentials):
        """Should apply filters to trade query."""
        from datetime import datetime

        from almanak.framework.connectors.polymarket.models import TradeFilters

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"[]"
        mock_response.json.return_value = []

        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.return_value = mock_response

        client = ClobClient(config_with_credentials, http_client=mock_http)

        after_date = datetime(2025, 1, 1, tzinfo=UTC)
        filters = TradeFilters(market="0xmarket123", after=after_date, limit=50)
        client.get_trades(filters=filters)

        # Verify request was made with filters
        call_args = mock_http.request.call_args
        params = call_args.kwargs.get("params", {})
        assert params.get("market") == "0xmarket123"
        assert params.get("limit") == 50
        assert "after" in params

    def test_get_trades_empty_response(self, config_with_credentials):
        """Should handle empty trades list."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"[]"
        mock_response.json.return_value = []

        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.return_value = mock_response

        client = ClobClient(config_with_credentials, http_client=mock_http)
        trades = client.get_trades()

        assert trades == []


class TestBalanceAllowance:
    """Tests for balance and allowance functionality."""

    def test_get_balance_allowance_collateral(self, config_with_credentials):
        """Should fetch USDC balance and allowance."""
        api_response = {
            "balance": "1000.50",
            "allowance": "999999999999",
        }

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"data"
        mock_response.json.return_value = api_response

        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.return_value = mock_response

        client = ClobClient(config_with_credentials, http_client=mock_http)
        balance = client.get_balance_allowance(asset_type="COLLATERAL")

        assert balance.balance == Decimal("1000.50")
        assert balance.allowance == Decimal("999999999999")

    def test_get_balance_allowance_conditional(self, config_with_credentials):
        """Should fetch conditional token balance."""
        api_response = {
            "balance": "500.0",
            "allowance": "500.0",
        }

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"data"
        mock_response.json.return_value = api_response

        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.return_value = mock_response

        client = ClobClient(config_with_credentials, http_client=mock_http)
        balance = client.get_balance_allowance(asset_type="CONDITIONAL", token_id="token123")

        assert balance.balance == Decimal("500.0")

        # Verify request included token_id
        call_args = mock_http.request.call_args
        params = call_args.kwargs.get("params", {})
        assert params.get("asset_type") == "CONDITIONAL"
        assert params.get("token_id") == "token123"


# =============================================================================
# Market-Specific Minimum Order Size Tests (US-111)
# =============================================================================


class TestMarketSpecificMinimumOrderSize:
    """Tests for market-specific minimum order size validation."""

    def _create_market(self, order_min_size: str = "5") -> GammaMarket:
        """Helper to create a GammaMarket with specific order_min_size."""
        return GammaMarket(
            id="test_market_123",
            condition_id="0xabc",
            question="Test market?",
            slug="test-market",
            outcomes=["Yes", "No"],
            outcome_prices=[Decimal("0.50"), Decimal("0.50")],
            clob_token_ids=["token_yes", "token_no"],
            volume=Decimal("10000"),
            liquidity=Decimal("5000"),
            active=True,
            closed=False,
            enable_order_book=True,
            order_min_size=Decimal(order_min_size),
            order_price_min_tick_size=Decimal("0.01"),
        )

    def test_validate_size_uses_default_when_no_market(self, config_with_credentials):
        """Should use DEFAULT_MIN_ORDER_SIZE (5) when no market provided."""
        from almanak.framework.connectors.polymarket.exceptions import PolymarketMinimumOrderError

        client = ClobClient(config_with_credentials)

        # Size 4 should fail (below default of 5)
        with pytest.raises(PolymarketMinimumOrderError) as exc_info:
            client._validate_size(Decimal("4"))

        assert exc_info.value.size == "4"
        assert exc_info.value.minimum == "5"

    def test_validate_size_uses_market_min_size(self, config_with_credentials):
        """Should use market's order_min_size when provided."""
        from almanak.framework.connectors.polymarket.exceptions import PolymarketMinimumOrderError

        client = ClobClient(config_with_credentials)
        market = self._create_market(order_min_size="10")

        # Size 9 should fail (below market minimum of 10)
        with pytest.raises(PolymarketMinimumOrderError) as exc_info:
            client._validate_size(Decimal("9"), market=market)

        assert exc_info.value.size == "9"
        assert exc_info.value.minimum == "10"

        # Size 10 should pass
        client._validate_size(Decimal("10"), market=market)  # Should not raise

    def test_validate_size_explicit_min_overrides_market(self, config_with_credentials):
        """Explicit min_size should override market minimum."""
        from almanak.framework.connectors.polymarket.exceptions import PolymarketMinimumOrderError

        client = ClobClient(config_with_credentials)
        market = self._create_market(order_min_size="5")

        # Use explicit min_size of 20 which overrides market's 5
        with pytest.raises(PolymarketMinimumOrderError) as exc_info:
            client._validate_size(Decimal("15"), min_size=Decimal("20"), market=market)

        assert exc_info.value.minimum == "20"  # Uses explicit min_size, not market's

    def test_build_limit_order_with_market_min_size(self, config_with_credentials):
        """build_limit_order should validate against market minimum."""
        from almanak.framework.connectors.polymarket.exceptions import PolymarketMinimumOrderError
        from almanak.framework.connectors.polymarket.models import LimitOrderParams

        client = ClobClient(config_with_credentials)
        market = self._create_market(order_min_size="15")

        params = LimitOrderParams(
            token_id="123456789",
            side="BUY",
            price=Decimal("0.50"),
            size=Decimal("10"),  # Below market minimum of 15
        )

        with pytest.raises(PolymarketMinimumOrderError) as exc_info:
            client.build_limit_order(params, market=market)

        assert exc_info.value.minimum == "15"

    def test_build_limit_order_passes_with_market_min_size(self, config_with_credentials):
        """build_limit_order should pass when size meets market minimum."""
        from almanak.framework.connectors.polymarket.models import LimitOrderParams

        client = ClobClient(config_with_credentials)
        market = self._create_market(order_min_size="15")

        params = LimitOrderParams(
            token_id="123456789",
            side="BUY",
            price=Decimal("0.50"),
            size=Decimal("15"),  # Exactly at minimum
        )

        # Should not raise
        order = client.build_limit_order(params, market=market)
        assert order.taker_amount == 15_000_000  # 15 shares in token units

    def test_build_market_order_buy_with_market_min_size(self, config_with_credentials):
        """build_market_order BUY should validate expected shares against market minimum."""
        from almanak.framework.connectors.polymarket.exceptions import PolymarketMinimumOrderError
        from almanak.framework.connectors.polymarket.models import MarketOrderParams

        client = ClobClient(config_with_credentials)
        market = self._create_market(order_min_size="20")

        # BUY 10 USDC at 0.99 = ~10.1 shares, which is below market min of 20
        params = MarketOrderParams(
            token_id="123456789",
            side="BUY",
            amount=Decimal("10"),  # USDC to spend
            worst_price=Decimal("0.99"),
        )

        with pytest.raises(PolymarketMinimumOrderError) as exc_info:
            client.build_market_order(params, market=market)

        assert exc_info.value.minimum == "20"

    def test_build_market_order_sell_with_market_min_size(self, config_with_credentials):
        """build_market_order SELL should validate shares against market minimum."""
        from almanak.framework.connectors.polymarket.exceptions import PolymarketMinimumOrderError
        from almanak.framework.connectors.polymarket.models import MarketOrderParams

        client = ClobClient(config_with_credentials)
        market = self._create_market(order_min_size="25")

        params = MarketOrderParams(
            token_id="123456789",
            side="SELL",
            amount=Decimal("20"),  # Shares to sell, below market min of 25
            worst_price=Decimal("0.50"),
        )

        with pytest.raises(PolymarketMinimumOrderError) as exc_info:
            client.build_market_order(params, market=market)

        assert exc_info.value.minimum == "25"

    def test_create_and_sign_limit_order_passes_market(self, config_with_credentials):
        """create_and_sign_limit_order should pass market to validation."""
        from almanak.framework.connectors.polymarket.exceptions import PolymarketMinimumOrderError
        from almanak.framework.connectors.polymarket.models import LimitOrderParams

        client = ClobClient(config_with_credentials)
        market = self._create_market(order_min_size="50")

        params = LimitOrderParams(
            token_id="123456789",
            side="BUY",
            price=Decimal("0.50"),
            size=Decimal("30"),  # Below market minimum of 50
        )

        with pytest.raises(PolymarketMinimumOrderError) as exc_info:
            client.create_and_sign_limit_order(params, market=market)

        assert exc_info.value.minimum == "50"

    def test_create_and_sign_market_order_passes_market(self, config_with_credentials):
        """create_and_sign_market_order should pass market to validation."""
        from almanak.framework.connectors.polymarket.exceptions import PolymarketMinimumOrderError
        from almanak.framework.connectors.polymarket.models import MarketOrderParams

        client = ClobClient(config_with_credentials)
        market = self._create_market(order_min_size="100")

        params = MarketOrderParams(
            token_id="123456789",
            side="SELL",
            amount=Decimal("50"),  # Shares to sell, below market min of 100
            worst_price=Decimal("0.50"),
        )

        with pytest.raises(PolymarketMinimumOrderError) as exc_info:
            client.create_and_sign_market_order(params, market=market)

        assert exc_info.value.minimum == "100"

    def test_various_market_minimums(self, config_with_credentials):
        """Test with various common market minimums from Polymarket."""
        from almanak.framework.connectors.polymarket.exceptions import PolymarketMinimumOrderError
        from almanak.framework.connectors.polymarket.models import LimitOrderParams

        client = ClobClient(config_with_credentials)

        test_cases = [
            ("1", Decimal("0.5")),  # Minimum 1, size 0.5 should fail
            ("5", Decimal("4")),  # Minimum 5, size 4 should fail
            ("10", Decimal("9.9")),  # Minimum 10, size 9.9 should fail
            ("15", Decimal("14")),  # Minimum 15, size 14 should fail
            ("0.1", Decimal("0.05")),  # Minimum 0.1, size 0.05 should fail
        ]

        for min_size, invalid_size in test_cases:
            market = self._create_market(order_min_size=min_size)
            params = LimitOrderParams(
                token_id="123",
                side="BUY",
                price=Decimal("0.50"),
                size=invalid_size,
            )

            with pytest.raises(PolymarketMinimumOrderError):
                client.build_limit_order(params, market=market)

    def test_error_message_contains_actual_market_minimum(self, config_with_credentials):
        """Error message should contain the actual market minimum, not default."""
        from almanak.framework.connectors.polymarket.exceptions import PolymarketMinimumOrderError
        from almanak.framework.connectors.polymarket.models import LimitOrderParams

        client = ClobClient(config_with_credentials)
        market = self._create_market(order_min_size="42.5")

        params = LimitOrderParams(
            token_id="123",
            side="BUY",
            price=Decimal("0.50"),
            size=Decimal("40"),
        )

        with pytest.raises(PolymarketMinimumOrderError) as exc_info:
            client.build_limit_order(params, market=market)

        # Error should contain market-specific minimum, not default
        assert "42.5" in str(exc_info.value)
        assert exc_info.value.minimum == "42.5"


class TestTickSizeValidation:
    """Tests for market-specific tick size validation."""

    def _create_market(self, tick_size: str = "0.01") -> GammaMarket:
        """Helper to create a GammaMarket with specific tick size."""
        return GammaMarket(
            id="test_market_123",
            condition_id="0xabc",
            question="Test market?",
            slug="test-market",
            outcomes=["Yes", "No"],
            outcome_prices=[Decimal("0.50"), Decimal("0.50")],
            clob_token_ids=["token_yes", "token_no"],
            volume=Decimal("10000"),
            liquidity=Decimal("5000"),
            active=True,
            closed=False,
            enable_order_book=True,
            order_min_size=Decimal("5"),
            order_price_min_tick_size=Decimal(tick_size),
        )

    def test_validate_tick_size_valid_price(self, config_with_credentials):
        """Should pass for prices that are valid tick multiples."""
        client = ClobClient(config_with_credentials)

        # With default tick size of 0.01
        client._validate_tick_size(Decimal("0.50"))  # Should not raise
        client._validate_tick_size(Decimal("0.01"))  # Minimum
        client._validate_tick_size(Decimal("0.99"))  # Maximum
        client._validate_tick_size(Decimal("0.33"))  # 33 cents

    def test_validate_tick_size_invalid_price(self, config_with_credentials):
        """Should raise for prices that are not valid tick multiples."""
        from almanak.framework.connectors.polymarket.exceptions import PolymarketInvalidTickSizeError

        client = ClobClient(config_with_credentials)

        # With default tick size of 0.01, these should fail
        with pytest.raises(PolymarketInvalidTickSizeError) as exc_info:
            client._validate_tick_size(Decimal("0.505"))  # Not a 0.01 multiple

        assert exc_info.value.price == "0.505"
        assert exc_info.value.tick_size == "0.01"
        # Nearest valid could be 0.50 or 0.51 depending on rounding (both are valid)
        assert exc_info.value.nearest_valid in ["0.50", "0.51"]

    def test_validate_tick_size_with_market(self, config_with_credentials):
        """Should use market's tick size when provided."""
        from almanak.framework.connectors.polymarket.exceptions import PolymarketInvalidTickSizeError

        client = ClobClient(config_with_credentials)
        market = self._create_market(tick_size="0.001")  # Finer tick size

        # 0.505 should be valid with 0.001 tick size
        client._validate_tick_size(Decimal("0.505"), market=market)  # Should not raise

        # But 0.5005 should fail
        with pytest.raises(PolymarketInvalidTickSizeError) as exc_info:
            client._validate_tick_size(Decimal("0.5005"), market=market)

        assert exc_info.value.tick_size == "0.001"

    def test_validate_tick_size_explicit_overrides_market(self, config_with_credentials):
        """Explicit tick_size parameter should override market tick size."""
        from almanak.framework.connectors.polymarket.exceptions import PolymarketInvalidTickSizeError

        client = ClobClient(config_with_credentials)
        market = self._create_market(tick_size="0.001")  # Market has fine tick

        # Explicit tick_size=0.01 should override market's 0.001
        with pytest.raises(PolymarketInvalidTickSizeError) as exc_info:
            client._validate_tick_size(Decimal("0.505"), tick_size=Decimal("0.01"), market=market)

        assert exc_info.value.tick_size == "0.01"  # Uses explicit, not market's 0.001

    def test_round_to_tick_size_buy_floors(self, config_with_credentials):
        """BUY orders should floor to avoid overpaying."""
        client = ClobClient(config_with_credentials)

        # 0.655 with tick 0.01 should floor to 0.65 for BUY
        result = client._round_to_tick_size(Decimal("0.655"), Decimal("0.01"), "BUY")
        assert result == Decimal("0.65")

        # 0.659 should still floor to 0.65
        result = client._round_to_tick_size(Decimal("0.659"), Decimal("0.01"), "BUY")
        assert result == Decimal("0.65")

    def test_round_to_tick_size_sell_ceils(self, config_with_credentials):
        """SELL orders should ceil to avoid underselling."""
        client = ClobClient(config_with_credentials)

        # 0.651 with tick 0.01 should ceil to 0.66 for SELL
        result = client._round_to_tick_size(Decimal("0.651"), Decimal("0.01"), "SELL")
        assert result == Decimal("0.66")

        # 0.655 should ceil to 0.66
        result = client._round_to_tick_size(Decimal("0.655"), Decimal("0.01"), "SELL")
        assert result == Decimal("0.66")

    def test_round_to_tick_size_exact_value(self, config_with_credentials):
        """Exact tick values should remain unchanged."""
        client = ClobClient(config_with_credentials)

        # 0.65 is already valid
        result_buy = client._round_to_tick_size(Decimal("0.65"), Decimal("0.01"), "BUY")
        result_sell = client._round_to_tick_size(Decimal("0.65"), Decimal("0.01"), "SELL")

        assert result_buy == Decimal("0.65")
        assert result_sell == Decimal("0.65")

    def test_round_to_tick_size_clamps_to_valid_range(self, config_with_credentials):
        """Rounding should clamp to valid price range (0.01-0.99)."""
        client = ClobClient(config_with_credentials)

        # Should not go below MIN_PRICE (0.01)
        result = client._round_to_tick_size(Decimal("0.005"), Decimal("0.01"), "BUY")
        assert result == Decimal("0.01")

        # Should not go above MAX_PRICE (0.99)
        result = client._round_to_tick_size(Decimal("0.995"), Decimal("0.01"), "SELL")
        assert result == Decimal("0.99")

    def test_round_price_to_tick_public_method(self, config_with_credentials):
        """Public round_price_to_tick method should work correctly."""
        client = ClobClient(config_with_credentials)
        market = self._create_market(tick_size="0.01")

        # Should use market tick size
        result = client.round_price_to_tick(Decimal("0.655"), "BUY", market=market)
        assert result == Decimal("0.65")

        result = client.round_price_to_tick(Decimal("0.651"), "SELL", market=market)
        assert result == Decimal("0.66")

    def test_build_limit_order_validates_tick_size(self, config_with_credentials):
        """build_limit_order should validate price against market tick size."""
        from almanak.framework.connectors.polymarket.exceptions import PolymarketInvalidTickSizeError
        from almanak.framework.connectors.polymarket.models import LimitOrderParams

        client = ClobClient(config_with_credentials)
        market = self._create_market(tick_size="0.01")

        params = LimitOrderParams(
            token_id="123456789",
            side="BUY",
            price=Decimal("0.655"),  # Invalid tick
            size=Decimal("100"),
        )

        with pytest.raises(PolymarketInvalidTickSizeError) as exc_info:
            client.build_limit_order(params, market=market)

        assert exc_info.value.price == "0.655"
        assert exc_info.value.tick_size == "0.01"

    def test_build_limit_order_passes_with_valid_tick(self, config_with_credentials):
        """build_limit_order should pass when price is valid tick."""
        from almanak.framework.connectors.polymarket.models import LimitOrderParams

        client = ClobClient(config_with_credentials)
        market = self._create_market(tick_size="0.01")

        params = LimitOrderParams(
            token_id="123456789",
            side="BUY",
            price=Decimal("0.65"),  # Valid tick
            size=Decimal("100"),
        )

        # Should not raise
        order = client.build_limit_order(params, market=market)
        assert order.taker_amount == 100_000_000  # 100 shares

    def test_build_market_order_validates_worst_price_tick(self, config_with_credentials):
        """build_market_order should validate worst_price against tick size."""
        from almanak.framework.connectors.polymarket.exceptions import PolymarketInvalidTickSizeError
        from almanak.framework.connectors.polymarket.models import MarketOrderParams

        client = ClobClient(config_with_credentials)
        market = self._create_market(tick_size="0.01")

        params = MarketOrderParams(
            token_id="123456789",
            side="BUY",
            amount=Decimal("100"),
            worst_price=Decimal("0.705"),  # Invalid tick
        )

        with pytest.raises(PolymarketInvalidTickSizeError) as exc_info:
            client.build_market_order(params, market=market)

        assert exc_info.value.price == "0.705"

    def test_build_market_order_no_worst_price_uses_defaults(self, config_with_credentials):
        """build_market_order without worst_price should use valid default."""
        from almanak.framework.connectors.polymarket.models import MarketOrderParams

        client = ClobClient(config_with_credentials)
        market = self._create_market(tick_size="0.01")

        # BUY without worst_price uses MAX_PRICE (0.99) which is valid
        params = MarketOrderParams(
            token_id="123456789",
            side="BUY",
            amount=Decimal("100"),
            # worst_price not set
        )

        # Should not raise
        order = client.build_market_order(params, market=market)
        assert order is not None

    def test_various_tick_sizes(self, config_with_credentials):
        """Test with various common tick sizes from Polymarket markets."""
        from almanak.framework.connectors.polymarket.exceptions import PolymarketInvalidTickSizeError

        client = ClobClient(config_with_credentials)

        test_cases = [
            ("0.01", Decimal("0.505"), False),  # 0.01 tick, 0.505 invalid
            ("0.01", Decimal("0.50"), True),  # 0.01 tick, 0.50 valid
            ("0.001", Decimal("0.505"), True),  # 0.001 tick, 0.505 valid
            ("0.001", Decimal("0.5005"), False),  # 0.001 tick, 0.5005 invalid
            ("0.1", Decimal("0.55"), False),  # 0.1 tick, 0.55 invalid
            ("0.1", Decimal("0.5"), True),  # 0.1 tick, 0.5 valid
        ]

        for tick_size, price, should_pass in test_cases:
            market = self._create_market(tick_size=tick_size)
            if should_pass:
                client._validate_tick_size(price, market=market)  # Should not raise
            else:
                with pytest.raises(PolymarketInvalidTickSizeError):
                    client._validate_tick_size(price, market=market)

    def test_error_message_includes_nearest_valid(self, config_with_credentials):
        """Error should include nearest valid price for debugging."""
        from almanak.framework.connectors.polymarket.exceptions import PolymarketInvalidTickSizeError

        client = ClobClient(config_with_credentials)

        with pytest.raises(PolymarketInvalidTickSizeError) as exc_info:
            client._validate_tick_size(Decimal("0.654"), tick_size=Decimal("0.01"))

        # Should suggest 0.65 as nearest valid
        assert exc_info.value.nearest_valid is not None
        # Could be 0.65 or 0.66 depending on rounding
        assert exc_info.value.nearest_valid in ["0.65", "0.66"]

    def test_tiny_tick_size_precision(self, config_with_credentials):
        """Should handle very small tick sizes correctly."""
        client = ClobClient(config_with_credentials)
        market = self._create_market(tick_size="0.0001")

        # 0.5001 should be valid with 0.0001 tick
        client._validate_tick_size(Decimal("0.5001"), market=market)

        # 0.50015 should be invalid
        from almanak.framework.connectors.polymarket.exceptions import PolymarketInvalidTickSizeError

        with pytest.raises(PolymarketInvalidTickSizeError):
            client._validate_tick_size(Decimal("0.50015"), market=market)


# =============================================================================
# Configurable URLs Tests
# =============================================================================


class TestConfigurableUrls:
    """Tests for configurable API URL support (US-115)."""

    def test_default_data_api_url(self, test_account):
        """Default data_api_base_url should be the standard endpoint."""
        from almanak.framework.connectors.polymarket.models import DATA_API_BASE_URL

        config = PolymarketConfig(
            wallet_address=test_account.address,
            private_key=SecretStr(test_account.key.hex()),
        )

        assert config.data_api_base_url == DATA_API_BASE_URL
        assert config.data_api_base_url == "https://data-api.polymarket.com"

    def test_custom_data_api_url(self, test_account):
        """Should accept custom data_api_base_url."""
        custom_url = "https://my-proxy.example.com/data"
        config = PolymarketConfig(
            wallet_address=test_account.address,
            private_key=SecretStr(test_account.key.hex()),
            data_api_base_url=custom_url,
        )

        assert config.data_api_base_url == custom_url

    def test_from_env_with_data_api_url(self, test_account, monkeypatch):
        """from_env() should load POLYMARKET_DATA_API_URL env var."""
        custom_url = "https://custom-data-api.example.com"

        monkeypatch.setenv("POLYMARKET_WALLET_ADDRESS", test_account.address)
        monkeypatch.setenv("POLYMARKET_PRIVATE_KEY", test_account.key.hex())
        monkeypatch.setenv("POLYMARKET_DATA_API_URL", custom_url)

        config = PolymarketConfig.from_env()

        assert config.data_api_base_url == custom_url

    def test_from_env_without_data_api_url_uses_default(self, test_account, monkeypatch):
        """from_env() should use default when POLYMARKET_DATA_API_URL not set."""
        from almanak.framework.connectors.polymarket.models import DATA_API_BASE_URL

        monkeypatch.setenv("POLYMARKET_WALLET_ADDRESS", test_account.address)
        monkeypatch.setenv("POLYMARKET_PRIVATE_KEY", test_account.key.hex())
        # Don't set POLYMARKET_DATA_API_URL

        config = PolymarketConfig.from_env()

        assert config.data_api_base_url == DATA_API_BASE_URL

    def test_from_env_with_all_url_overrides(self, test_account, monkeypatch):
        """from_env() should load all URL override env vars."""
        custom_clob = "https://custom-clob.example.com"
        custom_gamma = "https://custom-gamma.example.com"
        custom_data = "https://custom-data.example.com"

        monkeypatch.setenv("POLYMARKET_WALLET_ADDRESS", test_account.address)
        monkeypatch.setenv("POLYMARKET_PRIVATE_KEY", test_account.key.hex())
        monkeypatch.setenv("POLYMARKET_CLOB_URL", custom_clob)
        monkeypatch.setenv("POLYMARKET_GAMMA_URL", custom_gamma)
        monkeypatch.setenv("POLYMARKET_DATA_API_URL", custom_data)

        config = PolymarketConfig.from_env()

        assert config.clob_base_url == custom_clob
        assert config.gamma_base_url == custom_gamma
        assert config.data_api_base_url == custom_data

    def test_client_uses_config_data_api_url(self, test_account):
        """ClobClient._get_data_api() should use config.data_api_base_url."""
        custom_url = "https://custom-data-api.example.com"
        config = PolymarketConfig(
            wallet_address=test_account.address,
            private_key=SecretStr(test_account.key.hex()),
            data_api_base_url=custom_url,
        )

        # Create mock HTTP client to capture the request URL
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"[]"
        mock_response.json.return_value = []

        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.return_value = mock_response

        client = ClobClient(config, http_client=mock_http)

        # Call _get_data_api and verify the URL
        client._get_data_api("/positions", params={"user": "0x123"})

        # Check the request was made with the custom URL
        call_args = mock_http.request.call_args
        assert call_args is not None
        assert custom_url in call_args.kwargs.get("url", call_args.args[1] if len(call_args.args) > 1 else "")

    def test_docstring_documents_all_urls(self):
        """PolymarketConfig docstring should document all configurable URLs."""
        docstring = PolymarketConfig.__doc__
        assert docstring is not None

        # Check all URLs are documented
        assert "clob_base_url" in docstring
        assert "gamma_base_url" in docstring
        assert "data_api_base_url" in docstring

        # Check env vars are documented
        assert "POLYMARKET_CLOB_URL" in docstring
        assert "POLYMARKET_GAMMA_URL" in docstring
        assert "POLYMARKET_DATA_API_URL" in docstring


# =============================================================================
# Rate Limiting Tests (US-117)
# =============================================================================


class TestTokenBucketRateLimiter:
    """Tests for the TokenBucketRateLimiter class."""

    def test_initial_state_full_bucket(self):
        """Rate limiter should start with a full bucket."""
        from almanak.framework.connectors.polymarket import TokenBucketRateLimiter

        limiter = TokenBucketRateLimiter(rate_per_second=10.0)
        assert limiter.available_tokens == 10.0
        assert limiter.enabled is True
        assert limiter.rate == 10.0

    def test_acquire_consumes_token(self):
        """acquire() should consume one token from the bucket."""
        from almanak.framework.connectors.polymarket import TokenBucketRateLimiter

        limiter = TokenBucketRateLimiter(rate_per_second=10.0)
        initial_tokens = limiter.available_tokens

        result = limiter.acquire()

        assert result is True
        assert limiter.available_tokens < initial_tokens

    def test_acquire_multiple_times(self):
        """Multiple acquires should consume multiple tokens."""
        from almanak.framework.connectors.polymarket import TokenBucketRateLimiter

        limiter = TokenBucketRateLimiter(rate_per_second=10.0)

        # Acquire 5 times
        for _ in range(5):
            assert limiter.acquire() is True

        # Should have about 5 tokens left (some refill happens)
        assert limiter.available_tokens < 6.0

    def test_try_acquire_non_blocking(self):
        """try_acquire() should not block and return immediately."""
        from almanak.framework.connectors.polymarket import TokenBucketRateLimiter

        limiter = TokenBucketRateLimiter(rate_per_second=10.0)

        # Exhaust the bucket
        for _ in range(10):
            limiter.try_acquire()

        # Next try_acquire should fail immediately without blocking
        start = time.time()
        result = limiter.try_acquire()
        elapsed = time.time() - start

        assert result is False
        assert elapsed < 0.1  # Should return almost immediately

    def test_disabled_limiter_always_succeeds(self):
        """Disabled rate limiter should always allow requests."""
        from almanak.framework.connectors.polymarket import TokenBucketRateLimiter

        limiter = TokenBucketRateLimiter(rate_per_second=1.0, enabled=False)

        # Even with rate=1, disabled limiter should allow many requests
        for _ in range(100):
            assert limiter.acquire() is True
            assert limiter.try_acquire() is True

    def test_enable_disable_toggle(self):
        """Should be able to enable/disable rate limiter at runtime."""
        from almanak.framework.connectors.polymarket import TokenBucketRateLimiter

        limiter = TokenBucketRateLimiter(rate_per_second=1.0, enabled=True)
        assert limiter.enabled is True

        limiter.enabled = False
        assert limiter.enabled is False

        # When disabled, should always succeed
        for _ in range(10):
            assert limiter.acquire() is True

        limiter.enabled = True
        assert limiter.enabled is True

    def test_refill_over_time(self):
        """Tokens should refill over time."""
        from almanak.framework.connectors.polymarket import TokenBucketRateLimiter

        limiter = TokenBucketRateLimiter(rate_per_second=100.0)

        # Exhaust the bucket
        for _ in range(100):
            limiter.try_acquire()

        tokens_before = limiter.available_tokens
        time.sleep(0.05)  # Wait 50ms
        tokens_after = limiter.available_tokens

        # With rate=100/s, after 50ms should have ~5 tokens refilled
        assert tokens_after > tokens_before

    def test_bucket_caps_at_capacity(self):
        """Bucket should not exceed capacity even after long idle time."""
        from almanak.framework.connectors.polymarket import TokenBucketRateLimiter

        limiter = TokenBucketRateLimiter(rate_per_second=10.0)

        # Simulate long idle time by calling refill
        time.sleep(0.1)

        # Tokens should be capped at capacity (rate_per_second)
        assert limiter.available_tokens <= 10.0

    def test_reset_restores_full_capacity(self):
        """reset() should restore the bucket to full capacity."""
        from almanak.framework.connectors.polymarket import TokenBucketRateLimiter

        limiter = TokenBucketRateLimiter(rate_per_second=10.0)

        # Exhaust the bucket
        for _ in range(10):
            limiter.try_acquire()

        # Reset
        limiter.reset()

        assert limiter.available_tokens == 10.0

    def test_acquire_with_timeout_success(self):
        """acquire() with timeout should succeed when token becomes available."""
        from almanak.framework.connectors.polymarket import TokenBucketRateLimiter

        limiter = TokenBucketRateLimiter(rate_per_second=100.0)  # Fast refill

        # Exhaust the bucket
        for _ in range(100):
            limiter.try_acquire()

        # Should succeed within timeout as tokens refill quickly
        result = limiter.acquire(timeout=0.5)
        assert result is True

    def test_acquire_with_timeout_failure(self):
        """acquire() should return False when timeout expires."""
        from almanak.framework.connectors.polymarket import TokenBucketRateLimiter

        limiter = TokenBucketRateLimiter(rate_per_second=1.0)  # Slow refill

        # Exhaust the bucket
        limiter.try_acquire()

        # Should fail with very short timeout
        start = time.time()
        result = limiter.acquire(timeout=0.01)
        elapsed = time.time() - start

        assert result is False
        assert elapsed < 0.1  # Should respect timeout

    def test_blocking_acquire_waits(self):
        """acquire() should block when bucket is empty."""
        from almanak.framework.connectors.polymarket import TokenBucketRateLimiter

        limiter = TokenBucketRateLimiter(rate_per_second=50.0)

        # Exhaust the bucket
        for _ in range(50):
            limiter.try_acquire()

        # Blocking acquire should wait for refill
        start = time.time()
        result = limiter.acquire()  # No timeout, will block
        elapsed = time.time() - start

        assert result is True
        assert elapsed > 0.01  # Should have waited some time


class TestClobClientRateLimiting:
    """Tests for rate limiting integration in ClobClient."""

    def test_client_initializes_rate_limiter(self, test_account):
        """ClobClient should initialize rate limiter from config."""
        config = PolymarketConfig(
            wallet_address=test_account.address,
            private_key=SecretStr(test_account.key.hex()),
            rate_limit_requests_per_second=50.0,
            rate_limit_enabled=True,
        )

        mock_http = MagicMock(spec=httpx.Client)
        client = ClobClient(config, http_client=mock_http)

        assert client.rate_limiter is not None
        assert client.rate_limiter.rate == 50.0
        assert client.rate_limiter.enabled is True

    def test_client_rate_limiter_disabled(self, test_account):
        """ClobClient should respect rate_limit_enabled=False."""
        config = PolymarketConfig(
            wallet_address=test_account.address,
            private_key=SecretStr(test_account.key.hex()),
            rate_limit_requests_per_second=30.0,
            rate_limit_enabled=False,
        )

        mock_http = MagicMock(spec=httpx.Client)
        client = ClobClient(config, http_client=mock_http)

        assert client.rate_limiter.enabled is False

    def test_client_accepts_custom_rate_limiter(self, test_account):
        """ClobClient should accept a custom rate limiter for testing."""
        from almanak.framework.connectors.polymarket import TokenBucketRateLimiter

        config = PolymarketConfig(
            wallet_address=test_account.address,
            private_key=SecretStr(test_account.key.hex()),
        )

        custom_limiter = TokenBucketRateLimiter(rate_per_second=100.0, enabled=False)
        mock_http = MagicMock(spec=httpx.Client)
        client = ClobClient(config, http_client=mock_http, rate_limiter=custom_limiter)

        assert client.rate_limiter is custom_limiter
        assert client.rate_limiter.rate == 100.0
        assert client.rate_limiter.enabled is False

    def test_request_acquires_token(self, config):
        """Each API request should acquire a rate limit token."""
        from almanak.framework.connectors.polymarket import TokenBucketRateLimiter

        # Use a mock rate limiter to track calls
        mock_limiter = MagicMock(spec=TokenBucketRateLimiter)
        mock_limiter.acquire.return_value = True

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"{}"
        mock_response.json.return_value = {}

        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.return_value = mock_response

        client = ClobClient(config, http_client=mock_http, rate_limiter=mock_limiter)

        # Make a request
        client._get("/test")

        # Verify rate limiter was called
        mock_limiter.acquire.assert_called_once()

    def test_multiple_requests_acquire_multiple_tokens(self, config):
        """Multiple requests should acquire multiple tokens."""
        from almanak.framework.connectors.polymarket import TokenBucketRateLimiter

        mock_limiter = MagicMock(spec=TokenBucketRateLimiter)
        mock_limiter.acquire.return_value = True

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"{}"
        mock_response.json.return_value = {}

        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.return_value = mock_response

        client = ClobClient(config, http_client=mock_http, rate_limiter=mock_limiter)

        # Make multiple requests
        for _ in range(5):
            client._get("/test")

        # Verify rate limiter was called 5 times
        assert mock_limiter.acquire.call_count == 5

    def test_rate_limiting_applies_to_all_request_types(self, config_with_credentials):
        """Rate limiting should apply to GET, POST, and DELETE requests."""
        from almanak.framework.connectors.polymarket import TokenBucketRateLimiter

        mock_limiter = MagicMock(spec=TokenBucketRateLimiter)
        mock_limiter.acquire.return_value = True

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"{}"
        mock_response.json.return_value = {}

        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.return_value = mock_response

        client = ClobClient(config_with_credentials, http_client=mock_http, rate_limiter=mock_limiter)

        # Make different types of requests
        client._get("/test")
        client._post("/test", json_body={})
        client._delete("/test")

        # Verify rate limiter was called for each
        assert mock_limiter.acquire.call_count == 3

    def test_rate_limiting_with_real_limiter_throttles_requests(self, test_account):
        """Real rate limiter should throttle rapid requests."""
        config = PolymarketConfig(
            wallet_address=test_account.address,
            private_key=SecretStr(test_account.key.hex()),
            rate_limit_requests_per_second=10.0,  # 10 req/s = 100ms between requests when exhausted
            rate_limit_enabled=True,
        )

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"{}"
        mock_response.json.return_value = {}

        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.return_value = mock_response

        client = ClobClient(config, http_client=mock_http)

        # Make more requests than the bucket can hold
        start = time.time()
        for _ in range(15):  # 15 requests with bucket of 10
            client._get("/test")
        elapsed = time.time() - start

        # Should take some time due to rate limiting (5 extra requests / 10 per second = ~0.5s)
        assert elapsed > 0.3  # At least 300ms due to throttling

    def test_disabled_rate_limiting_allows_rapid_requests(self, test_account):
        """Disabled rate limiter should allow rapid requests without delay."""
        config = PolymarketConfig(
            wallet_address=test_account.address,
            private_key=SecretStr(test_account.key.hex()),
            rate_limit_requests_per_second=1.0,  # Very slow limit
            rate_limit_enabled=False,  # But disabled
        )

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"{}"
        mock_response.json.return_value = {}

        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.return_value = mock_response

        client = ClobClient(config, http_client=mock_http)

        # Make many requests rapidly
        start = time.time()
        for _ in range(50):
            client._get("/test")
        elapsed = time.time() - start

        # Should be very fast since rate limiting is disabled
        assert elapsed < 0.5

    def test_rate_limiter_accessible_via_property(self, config):
        """Rate limiter should be accessible via property for runtime config."""
        mock_http = MagicMock(spec=httpx.Client)
        client = ClobClient(config, http_client=mock_http)

        # Access rate limiter
        limiter = client.rate_limiter
        assert limiter is not None

        # Should be able to toggle enabled state at runtime
        original_enabled = limiter.enabled
        limiter.enabled = not original_enabled
        assert limiter.enabled != original_enabled

    def test_config_rate_limit_enabled_default(self):
        """PolymarketConfig.rate_limit_enabled should default to True."""
        config = PolymarketConfig(
            wallet_address="0x0000000000000000000000000000000000000001",
            private_key=SecretStr("0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"),
        )

        assert config.rate_limit_enabled is True

    def test_config_rate_limit_requests_per_second_default(self):
        """PolymarketConfig.rate_limit_requests_per_second should default to 30.0."""
        config = PolymarketConfig(
            wallet_address="0x0000000000000000000000000000000000000001",
            private_key=SecretStr("0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"),
        )

        assert config.rate_limit_requests_per_second == 30.0
