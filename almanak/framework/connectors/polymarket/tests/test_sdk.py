"""Tests for Polymarket SDK Orchestrator.

Tests cover:
- SDK initialization
- Lazy credential creation
- Market lookup convenience methods
- Price convenience methods
- Allowance convenience methods
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest
from pydantic import SecretStr

from almanak.framework.connectors.polymarket import (
    ApiCredentials,
    GammaMarket,
    PolymarketConfig,
    PolymarketSDK,
    SignatureType,
)
from almanak.framework.connectors.polymarket.ctf_sdk import AllowanceStatus, TransactionData
from almanak.framework.connectors.polymarket.exceptions import PolymarketMarketNotFoundError

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def test_wallet_address():
    """Test wallet address."""
    return "0x742d35Cc6634C0532925a3b844Bc9e7595f0Ab42"


@pytest.fixture
def test_private_key():
    """Test private key."""
    return "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"


@pytest.fixture
def config(test_wallet_address, test_private_key):
    """Create test configuration."""
    return PolymarketConfig(
        wallet_address=test_wallet_address,
        private_key=SecretStr(test_private_key),
        signature_type=SignatureType.EOA,
    )


@pytest.fixture
def credentials():
    """Create test API credentials."""
    import base64

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
def mock_web3():
    """Create a mock Web3 instance."""
    return MagicMock()


@pytest.fixture
def sample_market():
    """Create a sample GammaMarket."""
    return GammaMarket(
        id="12345",
        condition_id="0x9915bea232fa12b20058f9cea1187ea51366352bf833393676cd0db557a58249",
        question="Will Bitcoin exceed $100,000 by end of 2025?",
        slug="will-bitcoin-exceed-100000-by-end-of-2025",
        outcomes=["Yes", "No"],
        outcome_prices=[Decimal("0.65"), Decimal("0.35")],
        clob_token_ids=["19045189272319", "28164726938309"],
        volume=Decimal("1500000"),
        volume_24hr=Decimal("125000"),
        liquidity=Decimal("50000"),
        active=True,
        closed=False,
        enable_order_book=True,
    )


# =============================================================================
# Initialization Tests
# =============================================================================


class TestSDKInitialization:
    """Tests for SDK initialization."""

    def test_init_without_web3(self, config):
        """SDK should initialize without web3."""
        sdk = PolymarketSDK(config)

        assert sdk.config == config
        assert sdk.web3 is None
        assert sdk.clob is not None
        assert sdk.ctf is not None
        sdk.close()

    def test_init_with_web3(self, config, mock_web3):
        """SDK should initialize with web3."""
        sdk = PolymarketSDK(config, web3=mock_web3)

        assert sdk.config == config
        assert sdk.web3 == mock_web3
        sdk.close()

    def test_init_without_credentials(self, config):
        """SDK should initialize without pre-existing credentials."""
        sdk = PolymarketSDK(config)

        assert sdk.credentials is None
        sdk.close()

    def test_init_with_credentials(self, config_with_credentials):
        """SDK should use pre-existing credentials."""
        sdk = PolymarketSDK(config_with_credentials)

        assert sdk.credentials is not None
        assert sdk.credentials.api_key == "test_api_key"
        sdk.close()

    def test_context_manager(self, config):
        """SDK should work as context manager."""
        with PolymarketSDK(config) as sdk:
            assert sdk is not None


# =============================================================================
# Credential Management Tests
# =============================================================================


class TestCredentialManagement:
    """Tests for lazy credential management."""

    def test_get_or_create_credentials_returns_existing(self, config_with_credentials, credentials):
        """Should return existing credentials without API call."""
        sdk = PolymarketSDK(config_with_credentials)

        result = sdk.get_or_create_credentials()

        assert result == credentials
        sdk.close()

    def test_get_or_create_credentials_creates_new(self, config):
        """Should create new credentials when none exist."""
        sdk = PolymarketSDK(config)

        mock_credentials = ApiCredentials(
            api_key="new_key",
            secret=SecretStr("new_secret"),
            passphrase=SecretStr("new_pass"),
        )

        with patch.object(sdk.clob, "get_or_create_credentials", return_value=mock_credentials):
            result = sdk.get_or_create_credentials()

            assert result.api_key == "new_key"
            assert sdk.credentials is not None

        sdk.close()


# =============================================================================
# Market Lookup Tests
# =============================================================================


class TestMarketLookup:
    """Tests for market lookup convenience methods."""

    def test_get_market_by_slug_found(self, config, sample_market):
        """Should return market when found by slug."""
        sdk = PolymarketSDK(config)

        with patch.object(sdk.clob, "get_markets", return_value=[sample_market]):
            result = sdk.get_market_by_slug("will-bitcoin-exceed-100000-by-end-of-2025")

            assert result == sample_market

        sdk.close()

    def test_get_market_by_slug_not_found(self, config):
        """Should raise error when market not found by slug."""
        sdk = PolymarketSDK(config)

        with patch.object(sdk.clob, "get_markets", return_value=[]):
            with pytest.raises(PolymarketMarketNotFoundError) as exc_info:
                sdk.get_market_by_slug("nonexistent-market")

            assert "nonexistent-market" in str(exc_info.value)

        sdk.close()

    def test_get_market_by_condition_id_found(self, config, sample_market):
        """Should return market when found by condition ID."""
        sdk = PolymarketSDK(config)

        with patch.object(sdk.clob, "get_markets", return_value=[sample_market]):
            result = sdk.get_market_by_condition_id("0x9915bea...")

            assert result == sample_market

        sdk.close()

    def test_get_market_by_condition_id_not_found(self, config):
        """Should raise error when market not found by condition ID."""
        sdk = PolymarketSDK(config)

        with patch.object(sdk.clob, "get_markets", return_value=[]):
            with pytest.raises(PolymarketMarketNotFoundError) as exc_info:
                sdk.get_market_by_condition_id("0xnonexistent")

            assert "0xnonexistent" in str(exc_info.value)

        sdk.close()

    def test_get_market_by_token_id_found(self, config, sample_market):
        """Should return market when found by token ID."""
        sdk = PolymarketSDK(config)

        with patch.object(sdk.clob, "get_markets", return_value=[sample_market]):
            result = sdk.get_market_by_token_id("19045189272319")

            assert result == sample_market

        sdk.close()

    def test_get_market_by_token_id_not_found(self, config):
        """Should raise error when market not found by token ID."""
        sdk = PolymarketSDK(config)

        with patch.object(sdk.clob, "get_markets", return_value=[]):
            with pytest.raises(PolymarketMarketNotFoundError) as exc_info:
                sdk.get_market_by_token_id("nonexistent_token")

            assert "nonexistent_token" in str(exc_info.value)

        sdk.close()


# =============================================================================
# Price Convenience Tests
# =============================================================================


class TestPriceConvenience:
    """Tests for price convenience methods."""

    def test_get_yes_no_prices(self, config, sample_market):
        """Should return YES and NO prices from market."""
        sdk = PolymarketSDK(config)

        with patch.object(sdk.clob, "get_market", return_value=sample_market):
            yes_price, no_price = sdk.get_yes_no_prices("12345")

            assert yes_price == Decimal("0.65")
            assert no_price == Decimal("0.35")

        sdk.close()

    def test_get_prices_by_slug(self, config, sample_market):
        """Should return prices when looking up by slug."""
        sdk = PolymarketSDK(config)

        with patch.object(sdk.clob, "get_markets", return_value=[sample_market]):
            yes_price, no_price = sdk.get_prices_by_slug("btc-100k")

            assert yes_price == Decimal("0.65")
            assert no_price == Decimal("0.35")

        sdk.close()


# =============================================================================
# Allowance Convenience Tests
# =============================================================================


class TestAllowanceConvenience:
    """Tests for allowance convenience methods."""

    def test_ensure_allowances_without_web3_raises(self, config):
        """Should raise error when web3 not configured."""
        sdk = PolymarketSDK(config)  # No web3

        with pytest.raises(ValueError) as exc_info:
            sdk.ensure_allowances()

        assert "Web3 instance required" in str(exc_info.value)
        sdk.close()

    def test_ensure_allowances_with_web3(self, config, mock_web3):
        """Should call CTF SDK ensure_allowances when web3 configured."""
        sdk = PolymarketSDK(config, web3=mock_web3)

        mock_txs = [
            TransactionData(
                to="0x1234",
                data="0xabcd",
                description="Test approval",
            )
        ]

        with patch.object(sdk.ctf, "ensure_allowances", return_value=mock_txs):
            result = sdk.ensure_allowances()

            assert len(result) == 1
            assert result[0].description == "Test approval"

        sdk.close()

    def test_check_allowances_without_web3_raises(self, config):
        """Should raise error when web3 not configured."""
        sdk = PolymarketSDK(config)  # No web3

        with pytest.raises(ValueError) as exc_info:
            sdk.check_allowances()

        assert "Web3 instance required" in str(exc_info.value)
        sdk.close()

    def test_check_allowances_with_web3(self, config, mock_web3):
        """Should call CTF SDK check_allowances when web3 configured."""
        sdk = PolymarketSDK(config, web3=mock_web3)

        mock_status = AllowanceStatus(
            usdc_balance=1000000,
            usdc_allowance_ctf_exchange=1000000,
            usdc_allowance_neg_risk_exchange=1000000,
            ctf_approved_for_ctf_exchange=True,
            ctf_approved_for_neg_risk_adapter=True,
        )

        with patch.object(sdk.ctf, "check_allowances", return_value=mock_status):
            result = sdk.check_allowances()

            assert result.usdc_balance == 1000000
            assert result.fully_approved is True

        sdk.close()


# =============================================================================
# Balance Tests
# =============================================================================


class TestBalanceMethods:
    """Tests for balance convenience methods."""

    def test_get_usdc_balance_without_web3_raises(self, config):
        """Should raise error when web3 not configured."""
        sdk = PolymarketSDK(config)

        with pytest.raises(ValueError) as exc_info:
            sdk.get_usdc_balance()

        assert "Web3 instance required" in str(exc_info.value)
        sdk.close()

    def test_get_usdc_balance_with_web3(self, config, mock_web3):
        """Should return USDC balance when web3 configured."""
        sdk = PolymarketSDK(config, web3=mock_web3)

        with patch.object(sdk.ctf, "get_usdc_balance", return_value=1000000):
            result = sdk.get_usdc_balance()

            assert result == 1000000

        sdk.close()

    def test_get_position_balance_without_web3_raises(self, config):
        """Should raise error when web3 not configured."""
        sdk = PolymarketSDK(config)

        with pytest.raises(ValueError) as exc_info:
            sdk.get_position_balance(12345)

        assert "Web3 instance required" in str(exc_info.value)
        sdk.close()

    def test_get_position_balance_with_web3(self, config, mock_web3):
        """Should return position balance when web3 configured."""
        sdk = PolymarketSDK(config, web3=mock_web3)

        with patch.object(sdk.ctf, "get_token_balance", return_value=500000):
            result = sdk.get_position_balance(12345)

            assert result == 500000

        sdk.close()


# =============================================================================
# Integration Tests
# =============================================================================


class TestSDKIntegration:
    """Integration tests for SDK components."""

    def test_clob_client_accessible(self, config):
        """CLOB client should be accessible through SDK."""
        sdk = PolymarketSDK(config)

        assert hasattr(sdk.clob, "get_markets")
        assert hasattr(sdk.clob, "get_orderbook")
        assert hasattr(sdk.clob, "get_price")

        sdk.close()

    def test_ctf_sdk_accessible(self, config):
        """CTF SDK should be accessible through SDK."""
        sdk = PolymarketSDK(config)

        assert hasattr(sdk.ctf, "build_approve_usdc_tx")
        assert hasattr(sdk.ctf, "build_redeem_tx")
        assert hasattr(sdk.ctf, "check_allowances")

        sdk.close()

    def test_config_accessible(self, config):
        """Config should be accessible through SDK."""
        sdk = PolymarketSDK(config)

        assert sdk.config.wallet_address == config.wallet_address

        sdk.close()
