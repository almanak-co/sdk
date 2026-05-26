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

from almanak.connectors.polymarket import (
    ApiCredentials,
    ClobClient,
    GammaMarket,
    OrderBook,
    PolymarketConfig,
    SignatureType,
    TokenPrice,
)
from almanak.connectors.polymarket.exceptions import (
    PolymarketAPIError,
    PolymarketAuthenticationError,
    PolymarketRateLimitError,
    PolymarketSignatureError,
)
from almanak.connectors.polymarket.models import (
    CLOB_AUTH_DOMAIN,
    CLOB_AUTH_MESSAGE,
    CLOB_AUTH_TYPES,
)
from almanak.connectors.polymarket.signer import (
    make_local_signer,
    make_remote_signer,
)

# =============================================================================
# Fixtures
# =============================================================================


# Deterministic test key shared across all fixtures + the module-level
# ``_make_clob_client`` helper. Tests construct ``ClobClient`` via the helper
# so the signer is injected once — keeps the diff for issue #1961 minimal
# without sprinkling ``signer=`` across hundreds of call sites.
_TEST_PRIVATE_KEY = "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"


@pytest.fixture
def test_account():
    """Create a test Ethereum account."""
    # Use a deterministic account for testing
    return Account.from_key(_TEST_PRIVATE_KEY)


def _make_clob_client(config: PolymarketConfig, *args, **kwargs) -> ClobClient:
    """Build a ClobClient for tests with a default local Signer.

    Tests that need read-only mode (``signer=None``) or a remote signer pass
    ``signer=`` explicitly and that overrides this default.
    """
    kwargs.setdefault("signer", make_local_signer(_TEST_PRIVATE_KEY))
    return ClobClient(config, *args, **kwargs)


# =============================================================================
# Read-only mode (signer=None) — issue #1961
# =============================================================================


class TestReadOnlyMode:
    """``ClobClient(config, signer=None)`` — public endpoints work; signing
    paths raise :class:`PolymarketSignatureError`. The whole point of issue
    #1961 is that a strategy that grabs the connector directly cannot sign
    without explicitly handing it a Signer."""

    def test_build_l1_headers_raises_without_signer(self, config):
        client = ClobClient(config, signer=None)
        with pytest.raises(PolymarketSignatureError, match="read-only mode"):
            client._build_l1_headers()

    def test_sign_order_raises_without_signer(self, config_with_credentials):
        from almanak.connectors.polymarket.models import LimitOrderParams

        client = ClobClient(config_with_credentials, signer=None)
        # build_limit_order is a pure helper that does not sign — it should
        # still work even in read-only mode. Only the explicit ``sign_order``
        # call raises.
        params = LimitOrderParams(
            token_id="12345",
            side="BUY",
            price=Decimal("0.50"),
            size=Decimal("10"),
        )
        market = _make_test_market(neg_risk=False)
        unsigned = client.build_limit_order(params, market=market)
        with pytest.raises(PolymarketSignatureError, match="read-only mode"):
            client.sign_order(unsigned)

    def test_default_signer_is_none(self, config):
        """Constructing ``ClobClient`` without ``signer=`` defaults to read-only.
        A regression that defaulted to "local mode if env happens to have a
        key" would re-create the leak issue #1961 closes."""
        client = ClobClient(config)
        assert client._signer is None


class TestPolymarketConfigRejectsLegacySignerKwargs:
    """Pydantic ``extra='forbid'`` makes the #1961 hard break fail fast.

    The credential fields (``private_key``, ``signer_service_url``,
    ``signer_service_jwt``) were removed in #1961. With Pydantic's default
    ``extra='ignore'``, a stale call site that still passes them would
    silently produce a credentials-free config and the break would only
    surface as a runtime ``PolymarketSignatureError`` on the first signing
    call — i.e. a runtime outage instead of an immediate migration error.
    These tests pin the fail-fast behaviour.
    """

    @pytest.mark.parametrize(
        "field_name, value",
        [
            ("private_key", SecretStr("0x" + "0" * 64)),
            ("signer_service_url", "https://signer.example.com"),
            ("signer_service_jwt", SecretStr("jwt-token")),
        ],
    )
    def test_legacy_signer_kwarg_raises(self, test_account, field_name, value):
        from pydantic import ValidationError

        with pytest.raises(ValidationError, match="extra_forbidden"):
            PolymarketConfig(wallet_address=test_account.address, **{field_name: value})

    def test_unknown_kwarg_raises(self, test_account):
        from pydantic import ValidationError

        with pytest.raises(ValidationError, match="extra_forbidden"):
            PolymarketConfig(wallet_address=test_account.address, totally_unknown_field=42)


@pytest.fixture
def config(test_account):
    """Create test configuration."""
    return PolymarketConfig(
        wallet_address=test_account.address,
        signature_type=SignatureType.EOA,
    )


@pytest.fixture
def signer(test_account):
    """Create a local Signer bound to the test account."""
    return make_local_signer(test_account.key.hex())


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
        client = _make_clob_client(config)
        headers = client._build_l1_headers()

        assert "POLY_ADDRESS" in headers
        assert "POLY_SIGNATURE" in headers
        assert "POLY_TIMESTAMP" in headers
        assert "POLY_NONCE" in headers

    def test_build_l1_headers_wallet_address(self, config):
        """L1 headers should contain correct wallet address."""
        client = _make_clob_client(config)
        headers = client._build_l1_headers()

        assert headers["POLY_ADDRESS"] == config.wallet_address

    def test_build_l1_headers_signature_is_hex(self, config):
        """L1 signature should be a valid hex string with the 0x prefix.

        Regression: VIB-3013. Polymarket's GET /auth/derive-api-key rejects
        signatures without the `0x` prefix with `400 "Could not derive api key!"`.
        """
        client = _make_clob_client(config)
        headers = client._build_l1_headers()

        signature = headers["POLY_SIGNATURE"]
        assert signature.startswith("0x")
        # 65-byte signature = 130 hex chars + "0x" = 132 total
        assert len(signature) == 132
        assert all(c in "0123456789abcdef" for c in signature[2:])

    def test_build_l1_headers_timestamp_is_recent(self, config):
        """L1 timestamp should be recent."""
        client = _make_clob_client(config)
        headers = client._build_l1_headers()

        timestamp = int(headers["POLY_TIMESTAMP"])
        current_time = int(time.time())

        # Should be within 5 seconds
        assert abs(timestamp - current_time) < 5

    def test_build_l1_headers_nonce_default(self, config):
        """L1 nonce should default to 0."""
        client = _make_clob_client(config)
        headers = client._build_l1_headers()

        assert headers["POLY_NONCE"] == "0"

    def test_build_l1_headers_custom_nonce(self, config):
        """L1 nonce should accept custom value."""
        client = _make_clob_client(config)
        headers = client._build_l1_headers(nonce=42)

        assert headers["POLY_NONCE"] == "42"

    def test_signature_is_recoverable(self, config, test_account):
        """Signature should be recoverable to the original address."""
        from eth_account.messages import encode_typed_data

        client = _make_clob_client(config)
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
        sig_bytes = bytes.fromhex(signature_hex[2:] if signature_hex.startswith("0x") else signature_hex)
        recovered = Account.recover_message(signable, signature=sig_bytes)

        assert recovered == test_account.address


# =============================================================================
# L2 Authentication Tests
# =============================================================================


class TestL2Authentication:
    """Tests for L2 (HMAC-SHA256) authentication."""

    def test_build_l2_headers_contains_required_fields(self, config_with_credentials):
        """L2 headers should contain all required fields."""
        client = _make_clob_client(config_with_credentials)
        headers = client._build_l2_headers("GET", "/test")

        assert "POLY_ADDRESS" in headers
        assert "POLY_SIGNATURE" in headers
        assert "POLY_TIMESTAMP" in headers
        assert "POLY_API_KEY" in headers
        assert "POLY_PASSPHRASE" in headers

    def test_build_l2_headers_api_key(self, config_with_credentials):
        """L2 headers should contain correct API key."""
        client = _make_clob_client(config_with_credentials)
        headers = client._build_l2_headers("GET", "/test")

        assert headers["POLY_API_KEY"] == "test_api_key"

    def test_build_l2_headers_passphrase(self, config_with_credentials):
        """L2 headers should contain correct passphrase."""
        client = _make_clob_client(config_with_credentials)
        headers = client._build_l2_headers("GET", "/test")

        assert headers["POLY_PASSPHRASE"] == "test_passphrase"

    def test_build_l2_signature_format(self, config_with_credentials):
        """L2 signature should be URL-safe base64 encoded (32 bytes HMAC-SHA256).

        Polymarket API secrets are URL-safe base64 (`-` / `_`) and the server
        verifies URL-safe encoding on the signature too.
        """
        client = _make_clob_client(config_with_credentials)
        headers = client._build_l2_headers("GET", "/test")

        signature = headers["POLY_SIGNATURE"]
        try:
            decoded = base64.urlsafe_b64decode(signature)
            # SHA256 produces 32 bytes
            assert len(decoded) == 32
        except Exception:
            pytest.fail("Signature is not valid URL-safe base64")

    def test_build_l2_signature_reproducible(self, config_with_credentials):
        """L2 signature should be reproducible with same inputs."""
        client = _make_clob_client(config_with_credentials)

        # Get the timestamp from first call
        with patch("time.time", return_value=1704067200):  # Fixed timestamp
            headers1 = client._build_l2_headers("GET", "/test")
            headers2 = client._build_l2_headers("GET", "/test")

        assert headers1["POLY_SIGNATURE"] == headers2["POLY_SIGNATURE"]

    def test_build_l2_signature_includes_body(self, config_with_credentials):
        """L2 signature should include request body."""
        client = _make_clob_client(config_with_credentials)

        with patch("time.time", return_value=1704067200):
            headers_no_body = client._build_l2_headers("POST", "/order", "")
            headers_with_body = client._build_l2_headers("POST", "/order", '{"test":1}')

        assert headers_no_body["POLY_SIGNATURE"] != headers_with_body["POLY_SIGNATURE"]

    def test_build_l2_signature_manual_verification(self, config_with_credentials, credentials):
        """Manually verify L2 signature computation."""
        client = _make_clob_client(config_with_credentials)

        timestamp = "1704067200"
        method = "GET"
        path = "/test"
        body = ""

        with patch("time.time", return_value=int(timestamp)):
            headers = client._build_l2_headers(method, path, body)

        # Manually compute expected signature (URL-safe base64, matches CLOB server)
        secret = credentials.secret.get_secret_value()
        message = f"{timestamp}{method}{path}{body}"
        expected_sig = hmac.new(
            base64.urlsafe_b64decode(secret),
            message.encode("utf-8"),
            hashlib.sha256,
        ).digest()
        expected_b64 = base64.urlsafe_b64encode(expected_sig).decode("utf-8")

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

        client = _make_clob_client(config, http_client=mock_http)
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

        client = _make_clob_client(config, http_client=mock_http)

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

        client = _make_clob_client(config, http_client=mock_http)
        credentials = client.derive_api_credentials()

        assert credentials.api_key == "derived_api_key"

    def test_get_or_create_credentials_uses_existing(self, config_with_credentials):
        """Should return existing credentials if available."""
        client = _make_clob_client(config_with_credentials)
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

        client = _make_clob_client(config, http_client=mock_http)
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
        """Polymarket returns the orderbook in depth-walk order — bids
        ascend (worst→best), asks descend (worst→best). best_bid is
        bids[-1]; best_ask is asks[-1]. Cross-verified against live
        ``GET /price?side=BUY|SELL`` matching ``bids[-1]`` / ``asks[-1]``."""
        response = {
            "market": "token123",
            "asset_id": "token123",
            # Bids ascend: worst (0.63) first, best (0.64) last.
            "bids": [
                {"price": "0.63", "size": "2000"},
                {"price": "0.64", "size": "1000"},
            ],
            # Asks descend: worst (0.67) first, best (0.66) last.
            "asks": [
                {"price": "0.67", "size": "2500"},
                {"price": "0.66", "size": "1500"},
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


class TestOrderResponseFromApiResponse:
    """Direct unit tests for OrderResponse.from_api_response (VIB-3218).

    This is the parser for Polymarket's untrusted POST /order response. It is
    the only boundary between the exchange and the CLOB handler's
    classification logic, so it must not silently normalize away failure
    signals (``rejected`` / ``unmatched``) into "healthy resting order"
    shapes.
    """

    def test_parses_matched_fill_with_avg_price(self):
        """POST /order response with matched status + fill amount + avg price."""
        from almanak.connectors.polymarket.models import OrderResponse, OrderStatus

        response = OrderResponse.from_api_response(
            {
                "orderID": "0xabc",
                "status": "matched",
                "market": "token-123",
                "side": "BUY",
                "price": "0.65",
                "size": "100",
                "filledSize": "100",
                "avgPrice": "0.64",
            }
        )

        assert response.order_id == "0xabc"
        assert response.status == OrderStatus.MATCHED
        assert response.filled_size == Decimal("100")
        assert response.avg_fill_price == Decimal("0.64")

    def test_preserves_unmatched_status_for_ioc_rejection(self):
        """`unmatched` (IOC that didn't fill) must not be silenced to LIVE."""
        from almanak.connectors.polymarket.models import OrderResponse, OrderStatus

        response = OrderResponse.from_api_response(
            {
                "orderID": "0xioc",
                "status": "unmatched",
                "filledSize": "0",
            }
        )
        assert response.status == OrderStatus.UNMATCHED

    def test_preserves_delayed_status(self):
        """`delayed` (matching engine backlog) must stay distinct from LIVE."""
        from almanak.connectors.polymarket.models import OrderResponse, OrderStatus

        response = OrderResponse.from_api_response({"orderID": "0xd", "status": "delayed", "filledSize": "0"})
        assert response.status == OrderStatus.DELAYED

    def test_preserves_rejected_status(self):
        """`rejected` must never silently become LIVE."""
        from almanak.connectors.polymarket.models import OrderResponse, OrderStatus

        response = OrderResponse.from_api_response({"orderID": "0xr", "status": "rejected", "filledSize": "0"})
        assert response.status == OrderStatus.REJECTED

    def test_unknown_status_falls_back_to_failed_not_live(self):
        """Truly-unknown statuses default to FAILED (safe default), not LIVE.

        Regression test for VIB-3218 blocker: the original implementation
        silently coerced any unknown status to ``OrderStatus.LIVE``, which
        makes a rejected order look like a healthy resting order. FAILED is
        the safest default because it forces caller attention via the new
        ``success = (status != FAILED)`` rule in the handler.
        """
        from almanak.connectors.polymarket.models import OrderResponse, OrderStatus

        response = OrderResponse.from_api_response({"orderID": "0xunk", "status": "fabulous", "filledSize": "0"})
        assert response.status == OrderStatus.FAILED

    def test_avg_fill_price_absent_when_zero_or_missing(self):
        """Zero / missing avgPrice should not pollute the typed field."""
        from almanak.connectors.polymarket.models import OrderResponse

        missing = OrderResponse.from_api_response({"orderID": "0x1", "status": "live", "filledSize": "0"})
        assert missing.avg_fill_price is None

        zero = OrderResponse.from_api_response({"orderID": "0x2", "status": "live", "filledSize": "0", "avgPrice": "0"})
        assert zero.avg_fill_price is None

    def test_avg_fill_price_accepts_string_and_number(self):
        """avgPrice can arrive as str or numeric; both should parse."""
        from almanak.connectors.polymarket.models import OrderResponse

        as_str = OrderResponse.from_api_response(
            {"orderID": "0x1", "status": "matched", "filledSize": "10", "avgPrice": "0.73"}
        )
        assert as_str.avg_fill_price == Decimal("0.73")

        as_num = OrderResponse.from_api_response(
            {"orderID": "0x2", "status": "matched", "filledSize": "10", "avgPrice": 0.73}
        )
        assert as_num.avg_fill_price == Decimal(str(0.73))

    def test_handles_both_orderid_spellings(self):
        """Gamma alternates between ``orderID`` and ``orderId``."""
        from almanak.connectors.polymarket.models import OrderResponse

        camel = OrderResponse.from_api_response({"orderId": "0xcamel", "status": "live"})
        upper = OrderResponse.from_api_response({"orderID": "0xupper", "status": "live"})
        assert camel.order_id == "0xcamel"
        assert upper.order_id == "0xupper"


# =============================================================================
# Signature Type Tests
# =============================================================================


class TestSignatureTypes:
    """Tests for different signature types."""

    def test_eoa_signature_type(self, test_account):
        """EOA signature type should work correctly."""
        config = PolymarketConfig(
            wallet_address=test_account.address,
            signature_type=SignatureType.EOA,
        )

        client = _make_clob_client(config)
        headers = client._build_l1_headers()

        # Should still produce valid signature ("0x" + 65-byte hex = 132 chars)
        assert headers["POLY_SIGNATURE"].startswith("0x")
        assert len(headers["POLY_SIGNATURE"]) == 132

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

        client = _make_clob_client(config_with_credentials, http_client=mock_http)

        # Patch time.sleep so the retry-backoff loop (Retry-After: 60s × max_retries)
        # doesn't burn ~90s of real wall-clock waiting on mocked retries.
        with patch("time.sleep"), pytest.raises(PolymarketRateLimitError) as exc_info:
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

        client = _make_clob_client(config_with_credentials, http_client=mock_http)

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
        client = _make_clob_client(config_with_credentials)

        client._set_cached("test_key", {"data": "value"}, ttl=60)
        result = client._get_cached("test_key")

        assert result == {"data": "value"}

    def test_cache_expiration(self, config_with_credentials):
        """Should return None for expired cache."""
        client = _make_clob_client(config_with_credentials)

        # Set with very short TTL
        client._set_cached("test_key", {"data": "value"}, ttl=0)

        # Sleep briefly to ensure expiration
        time.sleep(0.1)

        result = client._get_cached("test_key")
        assert result is None

    def test_cache_miss(self, config_with_credentials):
        """Should return None for missing cache key."""
        client = _make_clob_client(config_with_credentials)
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

        client = _make_clob_client(config_with_credentials, http_client=mock_http)
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
        from almanak.connectors.polymarket.models import MarketFilters

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"[]"
        mock_response.json.return_value = []

        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.return_value = mock_response

        client = _make_clob_client(config_with_credentials, http_client=mock_http)
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

        client = _make_clob_client(config_with_credentials, http_client=mock_http)
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

        client = _make_clob_client(config_with_credentials, http_client=mock_http)

        # First call should hit API
        market1 = client.get_market("12345")
        # Second call should use cache
        market2 = client.get_market("12345")

        assert market1.id == market2.id
        # Should only have made one request
        assert mock_http.request.call_count == 1

    def test_get_orderbook(self, config_with_credentials):
        """Real V2 API shape: bids ascend, asks descend (depth-walk)."""
        api_response = {
            "market": "19045189272319329424023217822141741659150265216200539353252147725932663608488",
            "asset_id": "19045189272319329424023217822141741659150265216200539353252147725932663608488",
            # Bids ascend (worst→best); best bid is the last entry.
            "bids": [
                {"price": "0.62", "size": "5000"},
                {"price": "0.63", "size": "2500"},
                {"price": "0.64", "size": "1000"},
            ],
            # Asks descend (worst→best); best ask is the last entry.
            "asks": [
                {"price": "0.68", "size": "4500"},
                {"price": "0.67", "size": "3000"},
                {"price": "0.66", "size": "1500"},
            ],
            "hash": "0xabc123def456",
        }

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"data"
        mock_response.json.return_value = api_response

        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.return_value = mock_response

        client = _make_clob_client(config_with_credentials, http_client=mock_http)
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

        client = _make_clob_client(config_with_credentials, http_client=mock_http)

        # First call hits API, second uses cache
        client.get_orderbook("token123")
        client.get_orderbook("token123")

        assert mock_http.request.call_count == 1

    def test_get_price(self, config_with_credentials):
        """V2: /price requires side=BUY|SELL and returns {"price": ...} per
        side. get_price makes 2 calls and computes mid as (bid+ask)/2."""
        # First call (side=BUY) → bid; second call (side=SELL) → ask.
        bid_response = MagicMock()
        bid_response.status_code = 200
        bid_response.content = b"data"
        bid_response.json.return_value = {"price": "0.64"}

        ask_response = MagicMock()
        ask_response.status_code = 200
        ask_response.content = b"data"
        ask_response.json.return_value = {"price": "0.66"}

        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.side_effect = [bid_response, ask_response]

        client = _make_clob_client(config_with_credentials, http_client=mock_http)
        price = client.get_price("token123")

        assert price.bid == Decimal("0.64")
        assert price.ask == Decimal("0.66")
        assert price.mid == Decimal("0.65")  # arithmetic mean

        # Verify both calls used the right side parameter (regression: V1
        # passed only token_id and the V2 server returned "Invalid side").
        sides = [call.kwargs["params"]["side"] for call in mock_http.request.call_args_list]
        assert sides == ["BUY", "SELL"]

    def test_get_price_zero_when_one_side_empty(self, config_with_credentials):
        """If a market has bids but no asks (or vice versa), mid must be 0
        rather than half of the populated side. A naive ``(bid+ask)/2`` on a
        ``ask=0`` shape would silently return ``bid/2`` — a midpoint
        that's 50% off and would corrupt downstream pricing decisions."""
        bid_response = MagicMock()
        bid_response.status_code = 200
        bid_response.content = b"data"
        bid_response.json.return_value = {"price": "0.64"}

        ask_response = MagicMock()
        ask_response.status_code = 200
        ask_response.content = b"data"
        ask_response.json.return_value = {"price": "0"}  # no asks resting

        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.side_effect = [bid_response, ask_response]

        client = _make_clob_client(config_with_credentials, http_client=mock_http)
        price = client.get_price("token123")

        assert price.bid == Decimal("0.64")
        assert price.ask == Decimal("0")
        assert price.mid == Decimal("0")  # NOT 0.32

    def test_get_price_caching(self, config_with_credentials):
        """V2: cache key is per-token; cached result skips both BUY+SELL calls."""
        bid_response = MagicMock()
        bid_response.status_code = 200
        bid_response.content = b"data"
        bid_response.json.return_value = {"price": "0.50"}

        ask_response = MagicMock()
        ask_response.status_code = 200
        ask_response.content = b"data"
        ask_response.json.return_value = {"price": "0.51"}

        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.side_effect = [bid_response, ask_response]

        client = _make_clob_client(config_with_credentials, http_client=mock_http)

        client.get_price("token123")
        client.get_price("token123")

        # First call → 2 HTTP requests (BUY + SELL); second call hits cache → 0.
        assert mock_http.request.call_count == 2

    def test_get_midpoint(self, config_with_credentials):
        """Should fetch midpoint price."""
        api_response = {"mid": "0.65"}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"data"
        mock_response.json.return_value = api_response

        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.return_value = mock_response

        client = _make_clob_client(config_with_credentials, http_client=mock_http)
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

        client = _make_clob_client(config_with_credentials, http_client=mock_http)
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

        client = _make_clob_client(config_with_credentials, http_client=mock_http)
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

        client = _make_clob_client(config_with_credentials, http_client=mock_http)
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

        client = _make_clob_client(config_with_credentials, http_client=mock_http)
        assert client.health_check() is True

    def test_health_check_failure(self, config_with_credentials):
        """Should return False when API is unhealthy."""
        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.side_effect = Exception("Connection failed")

        client = _make_clob_client(config_with_credentials, http_client=mock_http)
        assert client.health_check() is False

    def test_get_server_time_raw_int_v2(self, config_with_credentials):
        """V2: /time returns a raw integer (e.g. 1777384897), NOT a dict.

        Regression: pre-fix, ``get_server_time`` did ``data.get("time", ...)``
        which raised ``'int' object has no attribute 'get'`` and broke L1
        auth's clock-drift check.
        """
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"1777384897"
        mock_response.json.return_value = 1777384897  # raw int, not {"time": ...}

        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.return_value = mock_response

        client = _make_clob_client(config_with_credentials, http_client=mock_http)
        assert client.get_server_time() == 1777384897

    def test_get_server_time_dict_shape_back_compat(self, config_with_credentials):
        """Historical / fallback dict shape ({"time": ...}) still works.

        Belt-and-braces: if Polymarket reverts the response shape, or a
        proxy in the path wraps the int, callers must still get a usable
        timestamp instead of a crash.
        """
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b'{"time": 1704067200}'
        mock_response.json.return_value = {"time": 1704067200}

        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.return_value = mock_response

        client = _make_clob_client(config_with_credentials, http_client=mock_http)
        assert client.get_server_time() == 1704067200

    def test_get_server_time_unexpected_shape_falls_back_to_local_clock(self, config_with_credentials):
        """Defensive: an unparseable shape (e.g. string) must not raise.

        L1 auth uses this timestamp to detect clock drift before signing —
        a hard failure here would block every authenticated request, so we
        log and return the local clock instead.
        """
        import time as _time

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b'"not-a-timestamp"'
        mock_response.json.return_value = "not-a-timestamp"

        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.return_value = mock_response

        client = _make_clob_client(config_with_credentials, http_client=mock_http)
        before = int(_time.time())
        result = client.get_server_time()
        after = int(_time.time()) + 1

        # Local clock fallback — must be a recent timestamp, not 0 / a crash.
        assert before <= result <= after

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

        client = _make_clob_client(config_with_credentials, http_client=mock_http)

        # Should succeed after retry
        with patch("time.sleep"):  # Don't actually sleep in tests
            midpoint = client.get_midpoint("token123")

        assert midpoint == Decimal("0.65")
        assert mock_http.request.call_count == 2


# =============================================================================
# Order Management Tests (US-003)
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

        client = _make_clob_client(config_with_credentials, http_client=mock_http)
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

        client = _make_clob_client(config_with_credentials, http_client=mock_http)
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

        client = _make_clob_client(config_with_credentials, http_client=mock_http)
        custom_wallet = "0x1234567890123456789012345678901234567890"
        client.get_positions(wallet=custom_wallet)

        # Verify request was made with custom wallet
        call_args = mock_http.request.call_args
        params = call_args.kwargs.get("params", {})
        assert params.get("user") == custom_wallet

    def test_get_positions_with_filters(self, config_with_credentials):
        """Should apply filters to position query."""
        from almanak.connectors.polymarket.models import PositionFilters

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"[]"
        mock_response.json.return_value = []

        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.return_value = mock_response

        client = _make_clob_client(config_with_credentials, http_client=mock_http)
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

        client = _make_clob_client(config_with_credentials, http_client=mock_http)
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

        client = _make_clob_client(config_with_credentials, http_client=mock_http)
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

        client = _make_clob_client(config_with_credentials, http_client=mock_http)
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

        from almanak.connectors.polymarket.models import TradeFilters

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"[]"
        mock_response.json.return_value = []

        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.return_value = mock_response

        client = _make_clob_client(config_with_credentials, http_client=mock_http)

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

        client = _make_clob_client(config_with_credentials, http_client=mock_http)
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

        client = _make_clob_client(config_with_credentials, http_client=mock_http)
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

        client = _make_clob_client(config_with_credentials, http_client=mock_http)
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


class TestConfigurableUrls:
    """Tests for configurable API URL support (US-115)."""

    def test_default_data_api_url(self, test_account):
        """Default data_api_base_url should be the standard endpoint."""
        from almanak.connectors.polymarket.models import DATA_API_BASE_URL

        config = PolymarketConfig(
            wallet_address=test_account.address,
        )

        assert config.data_api_base_url == DATA_API_BASE_URL
        assert config.data_api_base_url == "https://data-api.polymarket.com"

    def test_custom_data_api_url(self, test_account):
        """Should accept custom data_api_base_url."""
        custom_url = "https://my-proxy.example.com/data"
        config = PolymarketConfig(
            wallet_address=test_account.address,
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
        from almanak.connectors.polymarket.models import DATA_API_BASE_URL

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
            data_api_base_url=custom_url,
        )

        # Create mock HTTP client to capture the request URL
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"[]"
        mock_response.json.return_value = []

        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.return_value = mock_response

        client = _make_clob_client(config, http_client=mock_http)

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
        from almanak.connectors.polymarket import TokenBucketRateLimiter

        limiter = TokenBucketRateLimiter(rate_per_second=10.0)
        assert limiter.available_tokens == 10.0
        assert limiter.enabled is True
        assert limiter.rate == 10.0

    def test_acquire_consumes_token(self):
        """acquire() should consume one token from the bucket."""
        from almanak.connectors.polymarket import TokenBucketRateLimiter

        limiter = TokenBucketRateLimiter(rate_per_second=10.0)
        initial_tokens = limiter.available_tokens

        result = limiter.acquire()

        assert result is True
        assert limiter.available_tokens < initial_tokens

    def test_acquire_multiple_times(self):
        """Multiple acquires should consume multiple tokens."""
        from almanak.connectors.polymarket import TokenBucketRateLimiter

        limiter = TokenBucketRateLimiter(rate_per_second=10.0)

        # Acquire 5 times
        for _ in range(5):
            assert limiter.acquire() is True

        # Should have about 5 tokens left (some refill happens)
        assert limiter.available_tokens < 6.0

    def test_try_acquire_non_blocking(self):
        """try_acquire() should not block and return immediately."""
        from almanak.connectors.polymarket import TokenBucketRateLimiter

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
        from almanak.connectors.polymarket import TokenBucketRateLimiter

        limiter = TokenBucketRateLimiter(rate_per_second=1.0, enabled=False)

        # Even with rate=1, disabled limiter should allow many requests
        for _ in range(100):
            assert limiter.acquire() is True
            assert limiter.try_acquire() is True

    def test_enable_disable_toggle(self):
        """Should be able to enable/disable rate limiter at runtime."""
        from almanak.connectors.polymarket import TokenBucketRateLimiter

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
        from almanak.connectors.polymarket import TokenBucketRateLimiter

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
        from almanak.connectors.polymarket import TokenBucketRateLimiter

        limiter = TokenBucketRateLimiter(rate_per_second=10.0)

        # Simulate long idle time by calling refill
        time.sleep(0.1)

        # Tokens should be capped at capacity (rate_per_second)
        assert limiter.available_tokens <= 10.0

    def test_reset_restores_full_capacity(self):
        """reset() should restore the bucket to full capacity."""
        from almanak.connectors.polymarket import TokenBucketRateLimiter

        limiter = TokenBucketRateLimiter(rate_per_second=10.0)

        # Exhaust the bucket
        for _ in range(10):
            limiter.try_acquire()

        # Reset
        limiter.reset()

        assert limiter.available_tokens == 10.0

    def test_acquire_with_timeout_success(self):
        """acquire() with timeout should succeed when token becomes available."""
        from almanak.connectors.polymarket import TokenBucketRateLimiter

        limiter = TokenBucketRateLimiter(rate_per_second=100.0)  # Fast refill

        # Exhaust the bucket
        for _ in range(100):
            limiter.try_acquire()

        # Should succeed within timeout as tokens refill quickly
        result = limiter.acquire(timeout=0.5)
        assert result is True

    def test_acquire_with_timeout_failure(self):
        """acquire() should return False when timeout expires."""
        from almanak.connectors.polymarket import TokenBucketRateLimiter

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
        from almanak.connectors.polymarket import TokenBucketRateLimiter

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
            rate_limit_requests_per_second=50.0,
            rate_limit_enabled=True,
        )

        mock_http = MagicMock(spec=httpx.Client)
        client = _make_clob_client(config, http_client=mock_http)

        assert client.rate_limiter is not None
        assert client.rate_limiter.rate == 50.0
        assert client.rate_limiter.enabled is True

    def test_client_rate_limiter_disabled(self, test_account):
        """ClobClient should respect rate_limit_enabled=False."""
        config = PolymarketConfig(
            wallet_address=test_account.address,
            rate_limit_requests_per_second=30.0,
            rate_limit_enabled=False,
        )

        mock_http = MagicMock(spec=httpx.Client)
        client = _make_clob_client(config, http_client=mock_http)

        assert client.rate_limiter.enabled is False

    def test_client_accepts_custom_rate_limiter(self, test_account):
        """ClobClient should accept a custom rate limiter for testing."""
        from almanak.connectors.polymarket import TokenBucketRateLimiter

        config = PolymarketConfig(
            wallet_address=test_account.address,
        )

        custom_limiter = TokenBucketRateLimiter(rate_per_second=100.0, enabled=False)
        mock_http = MagicMock(spec=httpx.Client)
        client = _make_clob_client(config, http_client=mock_http, rate_limiter=custom_limiter)

        assert client.rate_limiter is custom_limiter
        assert client.rate_limiter.rate == 100.0
        assert client.rate_limiter.enabled is False

    def test_request_acquires_token(self, config):
        """Each API request should acquire a rate limit token."""
        from almanak.connectors.polymarket import TokenBucketRateLimiter

        # Use a mock rate limiter to track calls
        mock_limiter = MagicMock(spec=TokenBucketRateLimiter)
        mock_limiter.acquire.return_value = True

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"{}"
        mock_response.json.return_value = {}

        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.return_value = mock_response

        client = _make_clob_client(config, http_client=mock_http, rate_limiter=mock_limiter)

        # Make a request
        client._get("/test")

        # Verify rate limiter was called
        mock_limiter.acquire.assert_called_once()

    def test_multiple_requests_acquire_multiple_tokens(self, config):
        """Multiple requests should acquire multiple tokens."""
        from almanak.connectors.polymarket import TokenBucketRateLimiter

        mock_limiter = MagicMock(spec=TokenBucketRateLimiter)
        mock_limiter.acquire.return_value = True

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"{}"
        mock_response.json.return_value = {}

        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.return_value = mock_response

        client = _make_clob_client(config, http_client=mock_http, rate_limiter=mock_limiter)

        # Make multiple requests
        for _ in range(5):
            client._get("/test")

        # Verify rate limiter was called 5 times
        assert mock_limiter.acquire.call_count == 5

    def test_rate_limiting_applies_to_all_request_types(self, config_with_credentials):
        """Rate limiting should apply to GET, POST, and DELETE requests."""
        from almanak.connectors.polymarket import TokenBucketRateLimiter

        mock_limiter = MagicMock(spec=TokenBucketRateLimiter)
        mock_limiter.acquire.return_value = True

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"{}"
        mock_response.json.return_value = {}

        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.return_value = mock_response

        client = _make_clob_client(config_with_credentials, http_client=mock_http, rate_limiter=mock_limiter)

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
            rate_limit_requests_per_second=10.0,  # 10 req/s = 100ms between requests when exhausted
            rate_limit_enabled=True,
        )

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"{}"
        mock_response.json.return_value = {}

        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.return_value = mock_response

        client = _make_clob_client(config, http_client=mock_http)

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
            rate_limit_requests_per_second=1.0,  # Very slow limit
            rate_limit_enabled=False,  # But disabled
        )

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"{}"
        mock_response.json.return_value = {}

        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.return_value = mock_response

        client = _make_clob_client(config, http_client=mock_http)

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
        client = _make_clob_client(config, http_client=mock_http)

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
        )

        assert config.rate_limit_enabled is True

    def test_config_rate_limit_requests_per_second_default(self):
        """PolymarketConfig.rate_limit_requests_per_second should default to 30.0."""
        config = PolymarketConfig(
            wallet_address="0x0000000000000000000000000000000000000001",
        )

        assert config.rate_limit_requests_per_second == 30.0


# =============================================================================
# V2 EOA Signing — focused proof that V2 orders sign + recover correctly
# =============================================================================


def _make_test_market(neg_risk: bool) -> "GammaMarket":
    """Build a minimal GammaMarket for signing tests."""
    from almanak.connectors.polymarket.models import GammaMarket

    return GammaMarket(
        id="test-market",
        condition_id="0x" + "ab" * 32,
        question="Test V2 market",
        slug="test-v2",
        outcomes=["Yes", "No"],
        outcome_prices=[Decimal("0.5"), Decimal("0.5")],
        clob_token_ids=["1", "2"],
        volume=Decimal(0),
        volume_24hr=Decimal(0),
        liquidity=Decimal(0),
        active=True,
        closed=False,
        enable_order_book=True,
        order_price_min_tick_size=Decimal("0.01"),
        order_min_size=Decimal("1"),
        neg_risk=neg_risk,
    )


class TestV2OrderSigning:
    """V2 EOA signing — orders signed locally must recover to the EOA."""

    def test_v2_limit_order_signature_recovers_to_eoa_regular_market(self, config_with_credentials, test_account):
        """V2 BUY on a regular CTF market: signature recovers to EOA, domain == CTF V2."""
        from eth_account.messages import encode_typed_data as encode_typed_data_local

        from almanak.connectors.polymarket.models import (
            CTF_EXCHANGE_V2,
            ORDER_TYPES,
            LimitOrderParams,
            build_ctf_exchange_domain,
        )

        client = _make_clob_client(config_with_credentials)
        params = LimitOrderParams(
            token_id="12345",
            side="BUY",
            price=Decimal("0.50"),
            size=Decimal("10"),
        )

        market = _make_test_market(neg_risk=False)
        unsigned = client.build_limit_order(params, market=market)
        signed = client.sign_order(unsigned)

        assert unsigned.exchange_address == CTF_EXCHANGE_V2

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
            "domain": build_ctf_exchange_domain(unsigned.exchange_address),
            "message": unsigned.to_struct(),
        }
        signable = encode_typed_data_local(full_message=typed_data)
        sig_hex = signed.signature.removeprefix("0x")
        recovered = Account.recover_message(signable, signature=bytes.fromhex(sig_hex))

        assert recovered == test_account.address

    def test_v2_limit_order_routes_to_neg_risk_exchange(self, config_with_credentials):
        """Neg-risk markets must sign with NegRisk Exchange V2 in the domain."""
        from almanak.connectors.polymarket.models import (
            NEG_RISK_EXCHANGE_V2,
            LimitOrderParams,
        )

        client = _make_clob_client(config_with_credentials)
        params = LimitOrderParams(
            token_id="99",
            side="BUY",
            price=Decimal("0.50"),
            size=Decimal("10"),
        )

        market = _make_test_market(neg_risk=True)
        unsigned = client.build_limit_order(params, market=market)

        assert unsigned.exchange_address == NEG_RISK_EXCHANGE_V2

    def test_v2_unsigned_order_struct_has_v2_fields_only(self, config_with_credentials):
        """V2 signed struct: 11 fields, includes timestamp/metadata/builder, drops V1 fields."""
        from almanak.connectors.polymarket.models import LimitOrderParams

        client = _make_clob_client(config_with_credentials)
        params = LimitOrderParams(token_id="1", side="BUY", price=Decimal("0.50"), size=Decimal("10"))
        market = _make_test_market(neg_risk=False)
        unsigned = client.build_limit_order(params, market=market)
        struct = unsigned.to_struct()

        # V2 keys present
        assert set(struct.keys()) == {
            "salt",
            "maker",
            "signer",
            "tokenId",
            "makerAmount",
            "takerAmount",
            "side",
            "signatureType",
            "timestamp",
            "metadata",
            "builder",
        }
        # V1 fields gone
        assert "taker" not in struct
        assert "expiration" not in struct
        assert "nonce" not in struct
        assert "feeRateBps" not in struct
        # Sanity on V2 additions
        assert isinstance(struct["timestamp"], int) and struct["timestamp"] > 0
        assert struct["metadata"].startswith("0x") and len(struct["metadata"]) == 66
        assert struct["builder"].startswith("0x") and len(struct["builder"]) == 66

    def test_v2_api_payload_uses_v2_wire_shape(self, config_with_credentials):
        """SignedOrder.to_api_payload() must produce the V2 envelope shape."""
        from almanak.connectors.polymarket.models import LimitOrderParams

        client = _make_clob_client(config_with_credentials)
        params = LimitOrderParams(token_id="42", side="SELL", price=Decimal("0.75"), size=Decimal("100"))
        market = _make_test_market(neg_risk=False)
        signed = client.create_and_sign_limit_order(params, market=market)
        payload = signed.to_api_payload(owner="api-key-uuid", order_type="GTC")

        assert set(payload.keys()) == {"order", "owner", "orderType"}
        order = payload["order"]
        # V2 wire fields
        assert "timestamp" in order
        assert "metadata" in order
        assert "builder" in order
        assert "expiration" in order  # API-level GTD; "0" when no expiration
        # V1 wire fields removed
        assert "feeRateBps" not in order
        assert "nonce" not in order
        assert "taker" not in order
        # Side is the canonical string
        assert order["side"] == "SELL"

    def test_v2_build_limit_order_requires_market(self, config_with_credentials):
        """V2 limit-order builder must reject missing market — needed for
        tick/min-size validation AND neg-risk routing. A None market would
        silently route to CTFv2 in V1; V2 fails fast instead."""
        from almanak.connectors.polymarket.models import LimitOrderParams

        client = _make_clob_client(config_with_credentials)
        params = LimitOrderParams(token_id="1", side="BUY", price=Decimal("0.50"), size=Decimal("10"))

        with pytest.raises(ValueError, match="requires a GammaMarket"):
            client.build_limit_order(params, market=None)  # type: ignore[arg-type]

    def test_v2_build_market_order_requires_market(self, config_with_credentials):
        """V2 market-order builder must reject missing market — same constraint
        as limit orders (validation + neg-risk routing)."""
        from almanak.connectors.polymarket.models import MarketOrderParams

        client = _make_clob_client(config_with_credentials)
        params = MarketOrderParams(token_id="1", side="BUY", amount=Decimal("100"))

        with pytest.raises(ValueError, match="requires a GammaMarket"):
            client.build_market_order(params, market=None)  # type: ignore[arg-type]

    def test_v2_market_order_routes_to_neg_risk_exchange(self, config_with_credentials):
        """Neg-risk market orders sign against NegRisk Exchange V2.

        Existing TestV2OrderSigning covers the same routing for limit orders
        but not market orders — both share `_resolve_exchange_address` but a
        future divergence (e.g. someone hardcoding CTFv2 in build_market_order)
        would only surface here.
        """
        from almanak.connectors.polymarket.models import (
            NEG_RISK_EXCHANGE_V2,
            MarketOrderParams,
        )

        client = _make_clob_client(config_with_credentials)
        params = MarketOrderParams(token_id="42", side="BUY", amount=Decimal("100"), worst_price=Decimal("0.70"))
        market = _make_test_market(neg_risk=True)

        unsigned = client.build_market_order(params, market=market)

        assert unsigned.exchange_address == NEG_RISK_EXCHANGE_V2

    def test_v2_market_order_signature_recovers_to_eoa_neg_risk_market(self, config_with_credentials, test_account):
        """Round-trip a signed neg-risk market order — proves the V2 signing
        path works end-to-end for market orders, not just limit orders."""
        from eth_account.messages import encode_typed_data as encode_typed_data_local

        from almanak.connectors.polymarket.models import (
            ORDER_TYPES,
            MarketOrderParams,
            build_ctf_exchange_domain,
        )

        client = _make_clob_client(config_with_credentials)
        params = MarketOrderParams(token_id="99", side="BUY", amount=Decimal("100"), worst_price=Decimal("0.70"))
        market = _make_test_market(neg_risk=True)

        unsigned = client.build_market_order(params, market=market)
        signed = client.sign_order(unsigned)

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
            "domain": build_ctf_exchange_domain(unsigned.exchange_address),
            "message": unsigned.to_struct(),
        }
        signable = encode_typed_data_local(full_message=typed_data)
        sig_hex = signed.signature.removeprefix("0x")
        recovered = Account.recover_message(signable, signature=bytes.fromhex(sig_hex))

        assert recovered == test_account.address


# =============================================================================
# V2 Order Building (ported from V1 + V2 adjustments)
#
# In V2 the build_*_order methods require a GammaMarket. We use the helper
# fixtures below to build markets with specific tick / min-size combinations.
# =============================================================================


def _make_market(
    *,
    order_min_size: str = "5",
    tick_size: str = "0.01",
    neg_risk: bool = False,
) -> "GammaMarket":
    """Build a GammaMarket for order-builder tests."""
    return GammaMarket(
        id="test_market_123",
        condition_id="0xabc",
        question="Test market?",
        slug="test-market",
        outcomes=["Yes", "No"],
        outcome_prices=[Decimal("0.50"), Decimal("0.50")],
        clob_token_ids=["token_yes", "token_no"],
        volume=Decimal("10000"),
        volume_24hr=Decimal("0"),
        liquidity=Decimal("5000"),
        active=True,
        closed=False,
        enable_order_book=True,
        order_min_size=Decimal(order_min_size),
        order_price_min_tick_size=Decimal(tick_size),
        neg_risk=neg_risk,
    )


class TestV2OrderBuilding:
    """Order-shape and amount checks for V2 build_limit_order / build_market_order."""

    def test_build_limit_order_buy(self, config_with_credentials):
        """V2 BUY limit: maker = pUSD-out, taker = shares-in, ratio == price."""
        from almanak.connectors.polymarket.models import LimitOrderParams

        client = _make_clob_client(config_with_credentials)
        params = LimitOrderParams(
            token_id="19045189272319329424023217822141741659150265216200539353252147725932663608488",
            side="BUY",
            price=Decimal("0.65"),
            size=Decimal("100"),
        )
        market = _make_market()

        order = client.build_limit_order(params, market=market)

        assert order.maker == config_with_credentials.wallet_address
        assert order.signer == config_with_credentials.wallet_address
        assert order.side == 0  # BUY
        assert order.signature_type == 0  # EOA
        assert order.maker_amount == 65_000_000  # 100 × 0.65 USDC
        assert order.taker_amount == 100_000_000  # 100 shares
        assert order.token_id == int(params.token_id)

    def test_build_limit_order_sell(self, config_with_credentials):
        """V2 SELL limit: maker = shares-out, taker = pUSD-in."""
        from almanak.connectors.polymarket.models import LimitOrderParams

        client = _make_clob_client(config_with_credentials)
        params = LimitOrderParams(
            token_id="19045189272319329424023217822141741659150265216200539353252147725932663608488",
            side="SELL",
            price=Decimal("0.70"),
            size=Decimal("50"),
        )
        market = _make_market()

        order = client.build_limit_order(params, market=market)

        assert order.side == 1  # SELL
        assert order.maker_amount == 50_000_000  # 50 shares
        assert order.taker_amount == 35_000_000  # 50 × 0.70 USDC

    def test_build_limit_order_with_expiration(self, config_with_credentials):
        """V2: LimitOrderParams.expiration routes to UnsignedOrder.api_expiration
        (wire-only GTD, NOT signed). The signed `timestamp` is the order
        creation time, not the expiry."""
        from almanak.connectors.polymarket.models import LimitOrderParams

        client = _make_clob_client(config_with_credentials)
        expiration = int(time.time()) + 3600  # 1 hour out
        params = LimitOrderParams(
            token_id="123",
            side="BUY",
            price=Decimal("0.50"),
            size=Decimal("10"),
            expiration=expiration,
        )
        market = _make_market()

        order = client.build_limit_order(params, market=market)

        assert order.api_expiration == expiration
        assert order.timestamp != expiration

    def test_build_limit_order_invalid_price_too_low(self, config_with_credentials):
        """Reject prices below MIN_PRICE (0.01)."""
        from almanak.connectors.polymarket.exceptions import PolymarketInvalidPriceError
        from almanak.connectors.polymarket.models import LimitOrderParams

        client = _make_clob_client(config_with_credentials)
        params = LimitOrderParams(
            token_id="123",
            side="BUY",
            price=Decimal("0.001"),
            size=Decimal("100"),
        )
        market = _make_market()

        with pytest.raises(PolymarketInvalidPriceError) as exc_info:
            client.build_limit_order(params, market=market)
        assert exc_info.value.price == "0.001"

    def test_build_limit_order_invalid_price_too_high(self, config_with_credentials):
        """Reject prices above MAX_PRICE (0.99)."""
        from almanak.connectors.polymarket.exceptions import PolymarketInvalidPriceError
        from almanak.connectors.polymarket.models import LimitOrderParams

        client = _make_clob_client(config_with_credentials)
        params = LimitOrderParams(
            token_id="123",
            side="SELL",
            price=Decimal("1.00"),
            size=Decimal("100"),
        )
        market = _make_market()

        with pytest.raises(PolymarketInvalidPriceError):
            client.build_limit_order(params, market=market)

    def test_build_limit_order_size_too_small(self, config_with_credentials):
        """Reject sizes below the per-market shares minimum."""
        from almanak.connectors.polymarket.exceptions import PolymarketMinimumOrderError
        from almanak.connectors.polymarket.models import LimitOrderParams

        client = _make_clob_client(config_with_credentials)
        params = LimitOrderParams(
            token_id="123",
            side="BUY",
            price=Decimal("0.50"),
            size=Decimal("4"),  # below market min of 5
        )
        market = _make_market(order_min_size="5")

        with pytest.raises(PolymarketMinimumOrderError) as exc_info:
            client.build_limit_order(params, market=market)
        assert exc_info.value.minimum == "5"

    def test_build_market_order_buy(self, config_with_credentials):
        """V2 market BUY: maker pUSD snapped to keep ratio == worst_price."""
        from almanak.connectors.polymarket.models import MarketOrderParams

        client = _make_clob_client(config_with_credentials)
        params = MarketOrderParams(
            token_id="123456789",
            side="BUY",
            amount=Decimal("100"),
            worst_price=Decimal("0.70"),
        )
        market = _make_market()

        order = client.build_market_order(params, market=market)

        assert order.side == 0
        # 100 / 0.70 = 142.857… → snap to 142.85 shares (multiple of 0.01)
        # → maker = 142.85 × 0.70 = 99.995 USDC.
        assert order.taker_amount == 142_850_000
        assert order.maker_amount == 99_995_000
        assert Decimal(order.maker_amount) / Decimal(order.taker_amount) == Decimal("0.70")

    def test_build_market_order_sell(self, config_with_credentials):
        """V2 market SELL: maker shares-out, taker pUSD-in at worst_price."""
        from almanak.connectors.polymarket.models import MarketOrderParams

        client = _make_clob_client(config_with_credentials)
        params = MarketOrderParams(
            token_id="123456789",
            side="SELL",
            amount=Decimal("100"),
            worst_price=Decimal("0.60"),
        )
        market = _make_market()

        order = client.build_market_order(params, market=market)

        assert order.side == 1
        assert order.maker_amount == 100_000_000
        assert order.taker_amount == 60_000_000

    def test_build_market_order_default_worst_price_buy(self, config_with_credentials):
        """No worst_price on a BUY → use MAX_PRICE (0.99) as the implied price."""
        from almanak.connectors.polymarket.models import MarketOrderParams

        client = _make_clob_client(config_with_credentials)
        params = MarketOrderParams(
            token_id="123456789",
            side="BUY",
            amount=Decimal("99"),
            worst_price=None,
        )
        market = _make_market()

        order = client.build_market_order(params, market=market)
        assert order.maker_amount == 99_000_000
        assert order.taker_amount == 100_000_000

    def test_build_market_order_default_worst_price_sell(self, config_with_credentials):
        """No worst_price on a SELL → use MIN_PRICE (0.01)."""
        from almanak.connectors.polymarket.models import MarketOrderParams

        client = _make_clob_client(config_with_credentials)
        params = MarketOrderParams(
            token_id="123456789",
            side="SELL",
            amount=Decimal("100"),
            worst_price=None,
        )
        market = _make_market()

        order = client.build_market_order(params, market=market)
        assert order.maker_amount == 100_000_000
        assert order.taker_amount == 1_000_000

    def test_build_market_order_no_worst_price_uses_defaults(self, config_with_credentials):
        """Builder accepts a None worst_price by defaulting to MAX/MIN_PRICE."""
        from almanak.connectors.polymarket.models import MarketOrderParams

        client = _make_clob_client(config_with_credentials)
        market = _make_market(tick_size="0.01")
        params = MarketOrderParams(token_id="123456789", side="BUY", amount=Decimal("100"))
        order = client.build_market_order(params, market=market)
        assert order is not None

    def test_salt_is_random(self, config_with_credentials):
        """Salt must vary across orders to prevent replay collisions."""
        from almanak.connectors.polymarket.models import LimitOrderParams

        client = _make_clob_client(config_with_credentials)
        params = LimitOrderParams(
            token_id="123",
            side="BUY",
            price=Decimal("0.50"),
            size=Decimal("10"),
        )
        market = _make_market()

        order1 = client.build_limit_order(params, market=market)
        order2 = client.build_limit_order(params, market=market)
        assert order1.salt != order2.salt

    def test_v2_timestamp_is_recent_milliseconds(self, config_with_credentials):
        """V2 introduces a `timestamp` (ms) on the signed struct in lieu of V1's
        nonce. Confirm it is set to *now* in milliseconds."""
        from almanak.connectors.polymarket.models import LimitOrderParams

        client = _make_clob_client(config_with_credentials)
        params = LimitOrderParams(
            token_id="1",
            side="BUY",
            price=Decimal("0.50"),
            size=Decimal("10"),
        )
        market = _make_market()

        before_ms = int(time.time() * 1000)
        order = client.build_limit_order(params, market=market)
        after_ms = int(time.time() * 1000) + 1

        assert before_ms <= order.timestamp <= after_ms

    def test_v2_market_order_no_api_expiration(self, config_with_credentials):
        """Market orders should not carry GTD; api_expiration must be 0."""
        from almanak.connectors.polymarket.models import MarketOrderParams

        client = _make_clob_client(config_with_credentials)
        market = _make_market()
        params = MarketOrderParams(
            token_id="1",
            side="BUY",
            amount=Decimal("100"),
            worst_price=Decimal("0.50"),
        )
        order = client.build_market_order(params, market=market)
        assert order.api_expiration == 0

    def test_v2_metadata_and_builder_set_correctly(self, config_with_credentials):
        """Metadata is BYTES32_ZERO; builder is the configured builder_code."""
        from almanak.connectors.polymarket.models import BYTES32_ZERO, LimitOrderParams

        client = _make_clob_client(config_with_credentials)
        market = _make_market()
        params = LimitOrderParams(token_id="1", side="BUY", price=Decimal("0.50"), size=Decimal("10"))

        order = client.build_limit_order(params, market=market)
        assert order.metadata == BYTES32_ZERO
        assert order.builder == config_with_credentials.builder_code


class TestV2OrderSigningAdditional:
    """Signing tests ported from V1 with V2 domain adaptations."""

    def test_sign_order_produces_valid_signature(self, config_with_credentials, test_account):
        """Signature must be `0x`-prefixed 65-byte hex (132 chars)."""
        from almanak.connectors.polymarket.models import LimitOrderParams

        client = _make_clob_client(config_with_credentials)
        params = LimitOrderParams(token_id="123", side="BUY", price=Decimal("0.50"), size=Decimal("10"))
        market = _make_market()

        unsigned = client.build_limit_order(params, market=market)
        signed = client.sign_order(unsigned)

        assert signed.signature.startswith("0x")
        assert len(signed.signature) == 132
        assert all(c in "0123456789abcdef" for c in signed.signature[2:])
        assert signed.order == unsigned

    def test_create_and_sign_limit_order(self, config_with_credentials):
        """Convenience helper builds + signs a limit order in one call."""
        from almanak.connectors.polymarket.models import LimitOrderParams

        client = _make_clob_client(config_with_credentials)
        params = LimitOrderParams(token_id="123", side="BUY", price=Decimal("0.50"), size=Decimal("10"))
        market = _make_market()

        signed = client.create_and_sign_limit_order(params, market=market)

        assert signed.order is not None
        assert signed.signature.startswith("0x")
        assert len(signed.signature) == 132

    def test_create_and_sign_market_order(self, config_with_credentials):
        """Convenience helper builds + signs a market order in one call."""
        from almanak.connectors.polymarket.models import MarketOrderParams

        client = _make_clob_client(config_with_credentials)
        params = MarketOrderParams(
            token_id="123",
            side="SELL",
            amount=Decimal("100"),
            worst_price=Decimal("0.50"),
        )
        market = _make_market()

        signed = client.create_and_sign_market_order(params, market=market)

        assert signed.order is not None
        assert signed.order.side == 1  # SELL
        assert signed.signature.startswith("0x")
        assert len(signed.signature) == 132


class TestV2OrderSubmission:
    """Submit / cancel paths against a mocked HTTP layer."""

    def test_submit_limit_order_success(self, config_with_credentials):
        """submit_order parses the OrderResponse from the API JSON."""
        from almanak.connectors.polymarket.models import LimitOrderParams

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

        client = _make_clob_client(config_with_credentials, http_client=mock_http)
        params = LimitOrderParams(token_id="123", side="BUY", price=Decimal("0.50"), size=Decimal("10"))
        signed = client.create_and_sign_limit_order(params, market=_make_market())

        response = client.submit_order(signed)

        assert response.order_id == "0x123abc"
        assert response.status.value == "LIVE"
        assert response.side == "BUY"
        assert response.price == Decimal("0.50")

    def test_submit_order_with_order_type(self, config_with_credentials):
        """orderType field on the submission body must match the requested OrderType."""
        from almanak.connectors.polymarket.models import LimitOrderParams, OrderType

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

        client = _make_clob_client(config_with_credentials, http_client=mock_http)
        params = LimitOrderParams(token_id="123", side="BUY", price=Decimal("0.50"), size=Decimal("10"))
        signed = client.create_and_sign_limit_order(params, market=_make_market())

        client.submit_order(signed, order_type=OrderType.IOC)

        call_args = mock_http.request.call_args
        body = call_args.kwargs.get("content", "")
        assert "IOC" in body

    def test_cancel_order_success(self, config_with_credentials):
        """cancel_order issues a DELETE and returns True on 2xx."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b""
        mock_response.json.return_value = None

        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.return_value = mock_response

        client = _make_clob_client(config_with_credentials, http_client=mock_http)
        result = client.cancel_order("0x123abc")

        assert result is True
        call_args = mock_http.request.call_args
        assert call_args.kwargs.get("method") == "DELETE"

    def test_cancel_multiple_orders(self, config_with_credentials):
        """cancel_orders sends a list and returns True on success."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b""
        mock_response.json.return_value = None

        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.return_value = mock_response

        client = _make_clob_client(config_with_credentials, http_client=mock_http)
        assert client.cancel_orders(["0x123", "0x456", "0x789"]) is True

    def test_cancel_all_orders(self, config_with_credentials):
        """cancel_all_orders returns True on 2xx."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b""
        mock_response.json.return_value = None

        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.return_value = mock_response

        client = _make_clob_client(config_with_credentials, http_client=mock_http)
        assert client.cancel_all_orders() is True

    def test_get_open_orders(self, config_with_credentials):
        """Parses Data-API list response into OpenOrder objects."""
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

        client = _make_clob_client(config_with_credentials, http_client=mock_http)
        orders = client.get_open_orders()

        assert len(orders) == 2
        assert orders[0].order_id == "0x123"
        assert orders[0].side == "BUY"
        assert orders[0].price == Decimal("0.65")
        assert orders[0].filled_size == Decimal("25")
        assert orders[1].order_id == "0x456"
        assert orders[1].side == "SELL"


class TestV2OrderPayload:
    """Pin the V2 wire shape — distinct from V1 (no taker / nonce / feeRateBps)."""

    def test_unsigned_order_to_struct(self, config_with_credentials):
        """to_struct() produces the V2 EIP-712 11-field message."""
        from almanak.connectors.polymarket.models import LimitOrderParams

        client = _make_clob_client(config_with_credentials)
        params = LimitOrderParams(token_id="12345", side="BUY", price=Decimal("0.50"), size=Decimal("10"))
        market = _make_market()

        order = client.build_limit_order(params, market=market)
        struct = order.to_struct()

        assert struct["salt"] == order.salt
        assert struct["maker"] == config_with_credentials.wallet_address
        assert struct["signer"] == config_with_credentials.wallet_address
        assert struct["tokenId"] == 12345
        assert struct["makerAmount"] == order.maker_amount
        assert struct["takerAmount"] == order.taker_amount
        assert struct["side"] == 0
        assert struct["signatureType"] == 0
        assert struct["timestamp"] == order.timestamp
        assert struct["metadata"] == order.metadata
        assert struct["builder"] == order.builder
        for v1_only in ("taker", "expiration", "nonce", "feeRateBps"):
            assert v1_only not in struct

    def test_signed_order_to_api_payload_sell(self, config_with_credentials):
        """to_api_payload(): top-level {order, owner, orderType}; side as string;
        signature inside `order` with `0x` prefix; api_expiration as `expiration`."""
        from almanak.connectors.polymarket.models import LimitOrderParams

        client = _make_clob_client(config_with_credentials)
        params = LimitOrderParams(token_id="12345", side="SELL", price=Decimal("0.75"), size=Decimal("100"))
        market = _make_market()

        signed = client.create_and_sign_limit_order(params, market=market)
        payload = signed.to_api_payload(owner="test_api_key", order_type="GTC")

        assert set(payload.keys()) == {"order", "owner", "orderType"}
        assert payload["owner"] == "test_api_key"
        assert payload["orderType"] == "GTC"

        order = payload["order"]
        assert order["salt"] == signed.order.salt
        assert order["maker"] == config_with_credentials.wallet_address
        assert order["tokenId"] == "12345"
        assert order["makerAmount"] == str(signed.order.maker_amount)
        assert order["takerAmount"] == str(signed.order.taker_amount)
        assert order["side"] == "SELL"
        assert order["signatureType"] == 0

        assert "signature" not in payload
        assert order["signature"].startswith("0x")
        assert len(order["signature"]) == 132
        assert order["timestamp"] == str(signed.order.timestamp)
        assert order["metadata"] == signed.order.metadata
        assert order["builder"] == signed.order.builder
        assert order["expiration"] == "0"
        for v1_only in ("taker", "nonce", "feeRateBps"):
            assert v1_only not in order

    def test_signed_order_to_api_payload_buy_side_is_string(self, config_with_credentials):
        """BUY intent must serialize to "side": "BUY" (string), not 0."""
        from almanak.connectors.polymarket.models import LimitOrderParams

        client = _make_clob_client(config_with_credentials)
        params = LimitOrderParams(token_id="12345", side="BUY", price=Decimal("0.50"), size=Decimal("10"))
        market = _make_market()

        signed = client.create_and_sign_limit_order(params, market=market)
        payload = signed.to_api_payload(owner="test_api_key")

        assert payload["order"]["side"] == "BUY"
        assert payload["orderType"] == "GTC"

    def test_signed_order_payload_repairs_missing_0x(self, config_with_credentials):
        """Defense in depth: signature without `0x` is auto-prefixed in payload."""
        from almanak.connectors.polymarket.models import LimitOrderParams, SignedOrder

        client = _make_clob_client(config_with_credentials)
        params = LimitOrderParams(token_id="12345", side="BUY", price=Decimal("0.50"), size=Decimal("10"))
        market = _make_market()

        signed = client.create_and_sign_limit_order(params, market=market)
        raw_signed = SignedOrder(order=signed.order, signature=signed.signature.removeprefix("0x"))

        payload = raw_signed.to_api_payload(owner="test_api_key")
        assert payload["order"]["signature"].startswith("0x")

    def test_signed_order_api_expiration_carries_through(self, config_with_credentials):
        """api_expiration set on UnsignedOrder serializes to `expiration` on the wire."""
        from almanak.connectors.polymarket.models import LimitOrderParams

        client = _make_clob_client(config_with_credentials)
        expiration = int(time.time()) + 7200
        params = LimitOrderParams(
            token_id="1",
            side="BUY",
            price=Decimal("0.50"),
            size=Decimal("10"),
            expiration=expiration,
        )
        market = _make_market()

        signed = client.create_and_sign_limit_order(params, market=market)
        payload = signed.to_api_payload(owner="api-key")

        assert payload["order"]["expiration"] == str(expiration)


class TestV2MarketSpecificMinimumOrderSize:
    """Per-market min-size and $1 USD floor enforcement."""

    def test_validate_size_uses_default_when_no_market(self, config_with_credentials):
        """No market → DEFAULT_MIN_ORDER_SIZE (5)."""
        from almanak.connectors.polymarket.exceptions import PolymarketMinimumOrderError

        client = _make_clob_client(config_with_credentials)
        with pytest.raises(PolymarketMinimumOrderError) as exc_info:
            client._validate_size(Decimal("4"))
        assert exc_info.value.size == "4"
        assert exc_info.value.minimum == "5"

    def test_validate_size_uses_market_min_size(self, config_with_credentials):
        """market.order_min_size overrides the default."""
        from almanak.connectors.polymarket.exceptions import PolymarketMinimumOrderError

        client = _make_clob_client(config_with_credentials)
        market = _make_market(order_min_size="10")

        with pytest.raises(PolymarketMinimumOrderError) as exc_info:
            client._validate_size(Decimal("9"), market=market)
        assert exc_info.value.minimum == "10"

        client._validate_size(Decimal("10"), market=market)

    def test_validate_size_explicit_min_overrides_market(self, config_with_credentials):
        """An explicit min_size beats the market value."""
        from almanak.connectors.polymarket.exceptions import PolymarketMinimumOrderError

        client = _make_clob_client(config_with_credentials)
        market = _make_market(order_min_size="5")

        with pytest.raises(PolymarketMinimumOrderError) as exc_info:
            client._validate_size(Decimal("15"), min_size=Decimal("20"), market=market)
        assert exc_info.value.minimum == "20"

    def test_build_limit_order_with_market_min_size(self, config_with_credentials):
        """build_limit_order must validate size against market.order_min_size."""
        from almanak.connectors.polymarket.exceptions import PolymarketMinimumOrderError
        from almanak.connectors.polymarket.models import LimitOrderParams

        client = _make_clob_client(config_with_credentials)
        market = _make_market(order_min_size="15")

        params = LimitOrderParams(token_id="123456789", side="BUY", price=Decimal("0.50"), size=Decimal("10"))
        with pytest.raises(PolymarketMinimumOrderError) as exc_info:
            client.build_limit_order(params, market=market)
        assert exc_info.value.minimum == "15"

    def test_build_limit_order_passes_with_market_min_size(self, config_with_credentials):
        """At-or-above the market minimum, build_limit_order succeeds."""
        from almanak.connectors.polymarket.models import LimitOrderParams

        client = _make_clob_client(config_with_credentials)
        market = _make_market(order_min_size="15")
        params = LimitOrderParams(token_id="123456789", side="BUY", price=Decimal("0.50"), size=Decimal("15"))
        order = client.build_limit_order(params, market=market)
        assert order.taker_amount == 15_000_000

    def test_build_market_order_buy_with_market_min_size(self, config_with_credentials):
        """Market BUY: expected shares (= amount / worst_price) checked against min."""
        from almanak.connectors.polymarket.exceptions import PolymarketMinimumOrderError
        from almanak.connectors.polymarket.models import MarketOrderParams

        client = _make_clob_client(config_with_credentials)
        market = _make_market(order_min_size="20")
        params = MarketOrderParams(
            token_id="123456789",
            side="BUY",
            amount=Decimal("10"),
            worst_price=Decimal("0.99"),
        )
        with pytest.raises(PolymarketMinimumOrderError) as exc_info:
            client.build_market_order(params, market=market)
        assert exc_info.value.minimum == "20"

    def test_build_market_order_sell_with_market_min_size(self, config_with_credentials):
        """Market SELL: amount IS shares; checked directly against min."""
        from almanak.connectors.polymarket.exceptions import PolymarketMinimumOrderError
        from almanak.connectors.polymarket.models import MarketOrderParams

        client = _make_clob_client(config_with_credentials)
        market = _make_market(order_min_size="25")
        params = MarketOrderParams(
            token_id="123456789",
            side="SELL",
            amount=Decimal("20"),
            worst_price=Decimal("0.50"),
        )
        with pytest.raises(PolymarketMinimumOrderError) as exc_info:
            client.build_market_order(params, market=market)
        assert exc_info.value.minimum == "25"

    def test_create_and_sign_limit_order_passes_market(self, config_with_credentials):
        """create_and_sign forwards market metadata to the validator."""
        from almanak.connectors.polymarket.exceptions import PolymarketMinimumOrderError
        from almanak.connectors.polymarket.models import LimitOrderParams

        client = _make_clob_client(config_with_credentials)
        market = _make_market(order_min_size="50")
        params = LimitOrderParams(token_id="123456789", side="BUY", price=Decimal("0.50"), size=Decimal("30"))
        with pytest.raises(PolymarketMinimumOrderError) as exc_info:
            client.create_and_sign_limit_order(params, market=market)
        assert exc_info.value.minimum == "50"

    def test_create_and_sign_market_order_passes_market(self, config_with_credentials):
        """create_and_sign forwards market metadata to the validator (market order)."""
        from almanak.connectors.polymarket.exceptions import PolymarketMinimumOrderError
        from almanak.connectors.polymarket.models import MarketOrderParams

        client = _make_clob_client(config_with_credentials)
        market = _make_market(order_min_size="100")
        params = MarketOrderParams(
            token_id="123456789",
            side="SELL",
            amount=Decimal("50"),
            worst_price=Decimal("0.50"),
        )
        with pytest.raises(PolymarketMinimumOrderError) as exc_info:
            client.create_and_sign_market_order(params, market=market)
        assert exc_info.value.minimum == "100"

    @pytest.mark.parametrize(
        "min_size,invalid_size",
        [
            ("1", Decimal("0.5")),
            ("5", Decimal("4")),
            ("10", Decimal("9.9")),
            ("15", Decimal("14")),
            ("0.1", Decimal("0.05")),
        ],
    )
    def test_various_market_minimums(self, config_with_credentials, min_size, invalid_size):
        """Common market minimums, all rejected when sub-min."""
        from almanak.connectors.polymarket.exceptions import PolymarketMinimumOrderError
        from almanak.connectors.polymarket.models import LimitOrderParams

        client = _make_clob_client(config_with_credentials)
        market = _make_market(order_min_size=min_size)
        params = LimitOrderParams(token_id="123", side="BUY", price=Decimal("0.50"), size=invalid_size)
        with pytest.raises(PolymarketMinimumOrderError):
            client.build_limit_order(params, market=market)

    def test_error_message_contains_actual_market_minimum(self, config_with_credentials):
        """Error message should reflect the *market's* minimum, not the default."""
        from almanak.connectors.polymarket.exceptions import PolymarketMinimumOrderError
        from almanak.connectors.polymarket.models import LimitOrderParams

        client = _make_clob_client(config_with_credentials)
        market = _make_market(order_min_size="42.5")
        params = LimitOrderParams(token_id="123", side="BUY", price=Decimal("0.50"), size=Decimal("40"))
        with pytest.raises(PolymarketMinimumOrderError) as exc_info:
            client.build_limit_order(params, market=market)
        assert "42.5" in str(exc_info.value)
        assert exc_info.value.minimum == "42.5"

    def test_validate_order_value_usd_below_floor(self, config_with_credentials):
        """The $1 USD floor on BUYs uses ``$``-prefixed values for clarity."""
        from almanak.connectors.polymarket.exceptions import PolymarketMinimumOrderError

        client = _make_clob_client(config_with_credentials)
        with pytest.raises(PolymarketMinimumOrderError) as exc_info:
            client._validate_order_value_usd(Decimal("0.30"))
        assert exc_info.value.size == "$0.30"
        assert exc_info.value.minimum == "$1"

    def test_validate_order_value_usd_at_floor(self, config_with_credentials):
        """At or above $1 → no raise."""
        client = _make_clob_client(config_with_credentials)
        client._validate_order_value_usd(Decimal("1.00"))
        client._validate_order_value_usd(Decimal("1.01"))

    def test_build_limit_order_buy_rejects_sub_dollar_notional(self, config_with_credentials):
        """5 shares × $0.06 = $0.30: passes share check but fails $1 floor."""
        from almanak.connectors.polymarket.exceptions import PolymarketMinimumOrderError
        from almanak.connectors.polymarket.models import LimitOrderParams

        client = _make_clob_client(config_with_credentials)
        market = _make_market(order_min_size="5")
        params = LimitOrderParams(token_id="123", side="BUY", price=Decimal("0.06"), size=Decimal("5"))
        with pytest.raises(PolymarketMinimumOrderError) as exc_info:
            client.build_limit_order(params, market=market)
        assert exc_info.value.minimum == "$1"

    def test_build_limit_order_sell_not_subject_to_usd_floor(self, config_with_credentials):
        """SELL maker is shares; the $1 floor must NOT fire."""
        from almanak.connectors.polymarket.models import LimitOrderParams

        client = _make_clob_client(config_with_credentials)
        market = _make_market(order_min_size="5")
        params = LimitOrderParams(token_id="123", side="SELL", price=Decimal("0.06"), size=Decimal("5"))
        client.build_limit_order(params, market=market)

    def test_build_market_order_buy_rejects_sub_dollar_amount(self, config_with_credentials):
        """Market BUY < $1 fails locally before hitting the wire."""
        from almanak.connectors.polymarket.exceptions import PolymarketMinimumOrderError
        from almanak.connectors.polymarket.models import MarketOrderParams

        client = _make_clob_client(config_with_credentials)
        market = _make_market(order_min_size="5")
        params = MarketOrderParams(
            token_id="123",
            side="BUY",
            amount=Decimal("0.50"),
            worst_price=Decimal("0.10"),
        )
        with pytest.raises(PolymarketMinimumOrderError) as exc_info:
            client.build_market_order(params, market=market)
        assert exc_info.value.minimum == "$1"


class TestV2TickSizeValidation:
    """Per-market tick-size enforcement and rounding."""

    def test_validate_tick_size_valid_price(self, config_with_credentials):
        """Default (0.01) tick: 0.50, 0.01, 0.99, 0.33 all valid."""
        client = _make_clob_client(config_with_credentials)
        client._validate_tick_size(Decimal("0.50"))
        client._validate_tick_size(Decimal("0.01"))
        client._validate_tick_size(Decimal("0.99"))
        client._validate_tick_size(Decimal("0.33"))

    def test_validate_tick_size_invalid_price(self, config_with_credentials):
        """Default tick: 0.505 fails."""
        from almanak.connectors.polymarket.exceptions import PolymarketInvalidTickSizeError

        client = _make_clob_client(config_with_credentials)
        with pytest.raises(PolymarketInvalidTickSizeError) as exc_info:
            client._validate_tick_size(Decimal("0.505"))
        assert exc_info.value.price == "0.505"
        assert exc_info.value.tick_size == "0.01"
        assert exc_info.value.nearest_valid in ["0.50", "0.51"]

    def test_validate_tick_size_with_market(self, config_with_credentials):
        """market.order_price_min_tick_size is used when provided."""
        from almanak.connectors.polymarket.exceptions import PolymarketInvalidTickSizeError

        client = _make_clob_client(config_with_credentials)
        market = _make_market(tick_size="0.001")
        client._validate_tick_size(Decimal("0.505"), market=market)
        with pytest.raises(PolymarketInvalidTickSizeError) as exc_info:
            client._validate_tick_size(Decimal("0.5005"), market=market)
        assert exc_info.value.tick_size == "0.001"

    def test_validate_tick_size_explicit_overrides_market(self, config_with_credentials):
        """Explicit tick_size kwarg wins over market.order_price_min_tick_size."""
        from almanak.connectors.polymarket.exceptions import PolymarketInvalidTickSizeError

        client = _make_clob_client(config_with_credentials)
        market = _make_market(tick_size="0.001")
        with pytest.raises(PolymarketInvalidTickSizeError) as exc_info:
            client._validate_tick_size(Decimal("0.505"), tick_size=Decimal("0.01"), market=market)
        assert exc_info.value.tick_size == "0.01"

    def test_round_to_tick_size_buy_floors(self, config_with_credentials):
        """BUY rounds DOWN to avoid overpaying."""
        client = _make_clob_client(config_with_credentials)
        assert client._round_to_tick_size(Decimal("0.655"), Decimal("0.01"), "BUY") == Decimal("0.65")
        assert client._round_to_tick_size(Decimal("0.659"), Decimal("0.01"), "BUY") == Decimal("0.65")

    def test_round_to_tick_size_sell_ceils(self, config_with_credentials):
        """SELL rounds UP to avoid underselling."""
        client = _make_clob_client(config_with_credentials)
        assert client._round_to_tick_size(Decimal("0.651"), Decimal("0.01"), "SELL") == Decimal("0.66")
        assert client._round_to_tick_size(Decimal("0.655"), Decimal("0.01"), "SELL") == Decimal("0.66")

    def test_round_to_tick_size_exact_value(self, config_with_credentials):
        """Already-on-tick price stays the same on both sides."""
        client = _make_clob_client(config_with_credentials)
        assert client._round_to_tick_size(Decimal("0.65"), Decimal("0.01"), "BUY") == Decimal("0.65")
        assert client._round_to_tick_size(Decimal("0.65"), Decimal("0.01"), "SELL") == Decimal("0.65")

    def test_round_to_tick_size_clamps_to_valid_range(self, config_with_credentials):
        """Rounding clamps to MIN_PRICE / MAX_PRICE (0.01 / 0.99)."""
        client = _make_clob_client(config_with_credentials)
        assert client._round_to_tick_size(Decimal("0.005"), Decimal("0.01"), "BUY") == Decimal("0.01")
        assert client._round_to_tick_size(Decimal("0.995"), Decimal("0.01"), "SELL") == Decimal("0.99")

    def test_round_price_to_tick_public_method(self, config_with_credentials):
        """Public alias uses market tick-size by default."""
        client = _make_clob_client(config_with_credentials)
        market = _make_market(tick_size="0.01")
        assert client.round_price_to_tick(Decimal("0.655"), "BUY", market=market) == Decimal("0.65")
        assert client.round_price_to_tick(Decimal("0.651"), "SELL", market=market) == Decimal("0.66")

    def test_build_limit_order_validates_tick_size(self, config_with_credentials):
        """build_limit_order rejects an off-tick price for the market's tick."""
        from almanak.connectors.polymarket.exceptions import PolymarketInvalidTickSizeError
        from almanak.connectors.polymarket.models import LimitOrderParams

        client = _make_clob_client(config_with_credentials)
        market = _make_market(tick_size="0.01")
        params = LimitOrderParams(token_id="123456789", side="BUY", price=Decimal("0.655"), size=Decimal("100"))
        with pytest.raises(PolymarketInvalidTickSizeError) as exc_info:
            client.build_limit_order(params, market=market)
        assert exc_info.value.price == "0.655"
        assert exc_info.value.tick_size == "0.01"

    def test_build_limit_order_passes_with_valid_tick(self, config_with_credentials):
        """On-tick price passes."""
        from almanak.connectors.polymarket.models import LimitOrderParams

        client = _make_clob_client(config_with_credentials)
        market = _make_market(tick_size="0.01")
        params = LimitOrderParams(token_id="123456789", side="BUY", price=Decimal("0.65"), size=Decimal("100"))
        order = client.build_limit_order(params, market=market)
        assert order.taker_amount == 100_000_000

    def test_build_market_order_validates_worst_price_tick(self, config_with_credentials):
        """worst_price must conform to tick size."""
        from almanak.connectors.polymarket.exceptions import PolymarketInvalidTickSizeError
        from almanak.connectors.polymarket.models import MarketOrderParams

        client = _make_clob_client(config_with_credentials)
        market = _make_market(tick_size="0.01")
        params = MarketOrderParams(
            token_id="123456789",
            side="BUY",
            amount=Decimal("100"),
            worst_price=Decimal("0.705"),
        )
        with pytest.raises(PolymarketInvalidTickSizeError) as exc_info:
            client.build_market_order(params, market=market)
        assert exc_info.value.price == "0.705"

    @pytest.mark.parametrize(
        "tick_size,price,should_pass",
        [
            ("0.01", Decimal("0.505"), False),
            ("0.01", Decimal("0.50"), True),
            ("0.001", Decimal("0.505"), True),
            ("0.001", Decimal("0.5005"), False),
            ("0.1", Decimal("0.55"), False),
            ("0.1", Decimal("0.5"), True),
        ],
    )
    def test_various_tick_sizes(self, config_with_credentials, tick_size, price, should_pass):
        """Tick sizes 0.1, 0.01, 0.001 — all enforced."""
        from almanak.connectors.polymarket.exceptions import PolymarketInvalidTickSizeError

        client = _make_clob_client(config_with_credentials)
        market = _make_market(tick_size=tick_size)
        if should_pass:
            client._validate_tick_size(price, market=market)
        else:
            with pytest.raises(PolymarketInvalidTickSizeError):
                client._validate_tick_size(price, market=market)

    def test_error_message_includes_nearest_valid(self, config_with_credentials):
        """Error carries the nearest valid price (helpful for callers)."""
        from almanak.connectors.polymarket.exceptions import PolymarketInvalidTickSizeError

        client = _make_clob_client(config_with_credentials)
        with pytest.raises(PolymarketInvalidTickSizeError) as exc_info:
            client._validate_tick_size(Decimal("0.654"), tick_size=Decimal("0.01"))
        assert exc_info.value.nearest_valid is not None
        assert exc_info.value.nearest_valid in ["0.65", "0.66"]

    def test_tiny_tick_size_precision(self, config_with_credentials):
        """0.0001 tick: 0.5001 valid, 0.50015 invalid."""
        from almanak.connectors.polymarket.exceptions import PolymarketInvalidTickSizeError

        client = _make_clob_client(config_with_credentials)
        market = _make_market(tick_size="0.0001")
        client._validate_tick_size(Decimal("0.5001"), market=market)
        with pytest.raises(PolymarketInvalidTickSizeError):
            client._validate_tick_size(Decimal("0.50015"), market=market)


class TestV2RatioPreservation:
    """The CLOB rejects orders whose maker/taker (BUY) or taker/maker (SELL)
    ratio is not on the market's tick. _build_amounts_at_price snaps amounts
    so the integer ratio == the requested tick-aligned price exactly."""

    @pytest.mark.parametrize(
        "price, tick",
        [
            (Decimal("0.015"), "0.001"),
            (Decimal("0.5"), "0.01"),
            (Decimal("0.65"), "0.01"),
            (Decimal("0.70"), "0.01"),
            (Decimal("0.989"), "0.001"),
            (Decimal("0.99"), "0.01"),
        ],
    )
    def test_market_buy_ratio_equals_tick_aligned_price(self, config_with_credentials, price, tick):
        """Market BUY: maker/taker == price exactly, on the shares-step grid."""
        from almanak.connectors.polymarket.models import MarketOrderParams

        client = _make_clob_client(config_with_credentials)
        market = _make_market(tick_size=tick)
        params = MarketOrderParams(
            token_id="123",
            side="BUY",
            amount=Decimal("81.481") * price,
            worst_price=price,
        )

        order = client.build_market_order(params, market=market)

        assert order.taker_amount > 0
        assert order.taker_amount % ClobClient._SHARES_STEP == 0
        assert order.maker_amount % ClobClient._USDC_STEP == 0
        assert Decimal(order.maker_amount) / Decimal(order.taker_amount) == price

    def test_market_buy_on_tick_0001_regression(self, config_with_credentials):
        """0.001 tick: 81.481 shares @ 0.99 — pre-fix ratio drifted off-tick."""
        from almanak.connectors.polymarket.models import MarketOrderParams

        client = _make_clob_client(config_with_credentials)
        market = _make_market(tick_size="0.001")
        params = MarketOrderParams(
            token_id="123",
            side="BUY",
            amount=Decimal("80.66619"),
            worst_price=Decimal("0.99"),
        )

        order = client.build_market_order(params, market=market)
        ratio = Decimal(order.maker_amount) / Decimal(order.taker_amount)
        assert ratio == Decimal("0.99")
        assert ratio % Decimal("0.001") == 0

    @pytest.mark.parametrize(
        "price, tick",
        [(Decimal("0.015"), "0.001"), (Decimal("0.50"), "0.01"), (Decimal("0.989"), "0.001")],
    )
    def test_limit_order_ratio_equals_price(self, config_with_credentials, price, tick):
        """Limit BUY/SELL: ratio == price for both sides."""
        from almanak.connectors.polymarket.models import LimitOrderParams

        client = _make_clob_client(config_with_credentials)
        market = _make_market(tick_size=tick)
        for side in ("BUY", "SELL"):
            params = LimitOrderParams(
                token_id="123",
                side=side,
                price=price,
                size=Decimal("81.481"),
            )
            order = client.build_limit_order(params, market=market)
            ratio = (
                Decimal(order.maker_amount) / Decimal(order.taker_amount)
                if side == "BUY"
                else Decimal(order.taker_amount) / Decimal(order.maker_amount)
            )
            assert ratio == price, f"{side} ratio {ratio} != price {price}"

    def test_market_sell_ratio_equals_price(self, config_with_credentials):
        """Market SELL: taker/maker == worst_price exactly."""
        from almanak.connectors.polymarket.models import MarketOrderParams

        client = _make_clob_client(config_with_credentials)
        market = _make_market(tick_size="0.001")
        params = MarketOrderParams(
            token_id="123",
            side="SELL",
            amount=Decimal("81.481"),
            worst_price=Decimal("0.015"),
        )
        order = client.build_market_order(params, market=market)
        ratio = Decimal(order.taker_amount) / Decimal(order.maker_amount)
        assert ratio == Decimal("0.015")

    def test_post_snap_revalidation_rejects_sub_dollar_buy(self, config_with_credentials):
        """Pre-snap notional ≥ $1 but post-snap drops below — must reject locally."""
        from almanak.connectors.polymarket.exceptions import PolymarketMinimumOrderError
        from almanak.connectors.polymarket.models import MarketOrderParams

        client = _make_clob_client(config_with_credentials)
        market = _make_market(tick_size="0.01")
        params = MarketOrderParams(
            token_id="123",
            side="BUY",
            amount=Decimal("1.00"),
            worst_price=Decimal("0.99"),
        )
        with pytest.raises(PolymarketMinimumOrderError):
            client.build_market_order(params, market=market)

    def test_build_amounts_at_price_rejects_float_derived_decimal(self):
        """`Decimal(0.7)`-style high-precision price → ValueError, not silent snap to (0,0)."""
        with pytest.raises(ValueError, match="too much precision"):
            ClobClient._build_amounts_at_price("BUY", Decimal(0.7), 100_000_000)

    def test_buy_at_dollar_floor_with_low_min_size_rejected_post_snap(self, config_with_credentials):
        """A low per-market shares minimum doesn't suppress the post-snap $1 floor."""
        from almanak.connectors.polymarket.exceptions import PolymarketMinimumOrderError
        from almanak.connectors.polymarket.models import MarketOrderParams

        client = _make_clob_client(config_with_credentials)
        market = _make_market(tick_size="0.01", order_min_size="0.1")
        params = MarketOrderParams(
            token_id="123",
            side="BUY",
            amount=Decimal("1.00"),
            worst_price=Decimal("0.70"),
        )
        with pytest.raises(PolymarketMinimumOrderError):
            client.build_market_order(params, market=market)


class TestV2OrderSigningRemote:
    """V2 signing via the Almanak Signer Service (platform mode).

    With a remote :class:`Signer` (built via :func:`make_remote_signer`)
    injected into ``ClobClient``, both ``_build_l1_headers`` and
    ``sign_order`` must delegate to ``/sign/hash`` and the resulting
    signature must reassemble to ``0x<r><s><v>`` from the service's
    ethers-v6 ``Signature.toJSON()`` shape. After issue #1961 the
    signer-service URL/JWT live on the Signer closure, not on
    ``PolymarketConfig``.
    """

    @pytest.fixture
    def remote_config(self, test_account):
        return PolymarketConfig(
            wallet_address=test_account.address,
            signature_type=SignatureType.POLY_GNOSIS_SAFE,
            funder_address="0x1234567890123456789012345678901234567890",
        )

    @staticmethod
    def _make_signer_response(r_hex: str, s_hex: str, v: int) -> MagicMock:
        response = MagicMock(spec=httpx.Response)
        response.status_code = 200
        response.json.return_value = {
            "signed_transactions": [
                {"_type": "signature", "r": "0x" + r_hex, "s": "0x" + s_hex, "v": v, "networkV": None}
            ]
        }
        response.text = ""
        return response

    @staticmethod
    def _make_remote_signer_for(test_account, mock_http):
        """Build a remote :class:`Signer` whose HTTP client is the mock."""
        return make_remote_signer(
            eoa_address=test_account.address,
            signer_service_url="https://signer.example.com",
            signer_service_jwt="jwt-token",
            http_client=mock_http,
        )

    def test_l1_headers_use_remote_signer(self, remote_config, test_account):
        mock_http = MagicMock(spec=httpx.Client)
        mock_http.post.return_value = self._make_signer_response("ab" * 32, "cd" * 32, 27)

        remote_signer = self._make_remote_signer_for(test_account, mock_http)
        client = _make_clob_client(remote_config, http_client=mock_http, signer=remote_signer)
        headers = client._build_l1_headers()

        assert mock_http.post.called
        call_url = mock_http.post.call_args.args[0]
        assert call_url.endswith("/sign/hash")
        body = mock_http.post.call_args.kwargs["json"]
        assert body["eoa_address"] == test_account.address
        assert body["signing_type"] == "EVM"
        assert mock_http.post.call_args.kwargs["headers"]["Authorization"] == "Bearer jwt-token"

        # Explicit digest payload assertions: a regression that posts the
        # full typed-data blob, the wrong key name, or a non-32-byte value
        # would still pass the headers-only checks above.
        assert "transaction_payload" in body
        assert isinstance(body["transaction_payload"], list)
        assert len(body["transaction_payload"]) == 1
        digest_hex = body["transaction_payload"][0]
        assert digest_hex.startswith("0x")
        # 32-byte digest = 64 hex chars + "0x" prefix.
        assert len(digest_hex) == 2 + 64
        # Must be valid hex.
        int(digest_hex, 16)

        assert headers["POLY_ADDRESS"] == test_account.address
        assert headers["POLY_SIGNATURE"] == "0x" + "ab" * 32 + "cd" * 32 + "1b"

    def test_sign_order_uses_remote_signer(self, remote_config, test_account):
        from almanak.connectors.polymarket.models import LimitOrderParams

        mock_http = MagicMock(spec=httpx.Client)
        mock_http.post.return_value = self._make_signer_response("11" * 32, "22" * 32, 28)

        remote_signer = self._make_remote_signer_for(test_account, mock_http)
        client = _make_clob_client(remote_config, http_client=mock_http, signer=remote_signer)
        params = LimitOrderParams(
            token_id="12345",
            side="BUY",
            price=Decimal("0.50"),
            size=Decimal("10"),
        )
        market = _make_test_market(neg_risk=False)
        unsigned = client.build_limit_order(params, market=market)
        signed = client.sign_order(unsigned)

        assert mock_http.post.called
        # Same digest-payload sanity checks as the L1 path above.
        body = mock_http.post.call_args.kwargs["json"]
        assert "transaction_payload" in body
        assert len(body["transaction_payload"]) == 1
        digest_hex = body["transaction_payload"][0]
        assert digest_hex.startswith("0x") and len(digest_hex) == 2 + 64
        int(digest_hex, 16)

        assert signed.signature == "0x" + "11" * 32 + "22" * 32 + "1c"
        assert unsigned.signature_type == SignatureType.POLY_GNOSIS_SAFE.value
        assert unsigned.maker == "0x1234567890123456789012345678901234567890"
        assert unsigned.signer == remote_config.wallet_address
