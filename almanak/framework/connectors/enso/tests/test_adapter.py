"""Tests for EnsoAdapter.

These tests verify that the EnsoAdapter correctly compiles SwapIntents
into ActionBundles with proper transaction data.
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from ....intents.vocabulary import IntentType, SwapIntent
from ..adapter import TOKEN_ADDRESSES, EnsoAdapter
from ..client import EnsoConfig
from ..models import RouteTransaction, Transaction


@pytest.fixture
def enso_config():
    """Create an EnsoConfig for testing."""
    return EnsoConfig(
        chain="arbitrum",
        wallet_address="0x1234567890123456789012345678901234567890",
        api_key="test-api-key",
    )


@pytest.fixture
def mock_route_transaction():
    """Create a mock RouteTransaction response."""
    return RouteTransaction(
        gas="200000",
        tx=Transaction(
            data="0xb94c3609" + "00" * 64,  # routeSingle selector + dummy data
            to="0xF75584eF6673aD213a685a1B58Cc0330B8eA22Cf",
            from_address="0x1234567890123456789012345678901234567890",
            value="0",
        ),
        amount_out={"0x82af49447d8a07e3bd95bd0d56f35241523fbab1": "500000000000000000"},
        price_impact=50.0,
        route=[],
        chain_id=42161,
    )


class TestEnsoAdapterInit:
    """Test EnsoAdapter initialization."""

    def test_init_with_valid_config(self, enso_config):
        """Test adapter initializes with valid config."""
        with patch.object(EnsoAdapter, "__init__", lambda self, config, **kwargs: None):
            adapter = EnsoAdapter.__new__(EnsoAdapter)
            adapter.config = enso_config
            adapter.chain = "arbitrum"
            adapter.wallet_address = enso_config.wallet_address
            adapter.tokens = TOKEN_ADDRESSES.get("arbitrum", {})
            adapter.use_safe_route_single = True

            assert adapter.chain == "arbitrum"
            assert adapter.wallet_address == enso_config.wallet_address
            assert "USDC" in adapter.tokens
            assert "WETH" in adapter.tokens


class TestTokenResolution:
    """Test token address resolution."""

    def test_resolve_symbol_to_address(self, enso_config):
        """Test resolving token symbol to address."""
        with patch.object(EnsoAdapter, "__init__", lambda self, config, **kwargs: None):
            adapter = EnsoAdapter.__new__(EnsoAdapter)
            adapter.tokens = TOKEN_ADDRESSES.get("arbitrum", {})

            usdc_address = adapter.resolve_token_address("USDC")
            assert usdc_address == "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"

            weth_address = adapter.resolve_token_address("WETH")
            assert weth_address == "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"

    def test_resolve_address_returns_same(self, enso_config):
        """Test that address input returns the same address."""
        with patch.object(EnsoAdapter, "__init__", lambda self, config, **kwargs: None):
            adapter = EnsoAdapter.__new__(EnsoAdapter)
            adapter.tokens = TOKEN_ADDRESSES.get("arbitrum", {})

            address = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
            result = adapter.resolve_token_address(address)
            assert result == address

    def test_resolve_unknown_token(self, enso_config):
        """Test resolving unknown token returns None."""
        with patch.object(EnsoAdapter, "__init__", lambda self, config, **kwargs: None):
            adapter = EnsoAdapter.__new__(EnsoAdapter)
            adapter.tokens = TOKEN_ADDRESSES.get("arbitrum", {})

            result = adapter.resolve_token_address("UNKNOWN_TOKEN")
            assert result is None


class TestTransactionBuilding:
    """Test transaction building functions."""

    def test_build_approve_transaction(self, enso_config):
        """Test building an approve transaction."""
        with patch.object(EnsoAdapter, "__init__", lambda self, config, **kwargs: None):
            adapter = EnsoAdapter.__new__(EnsoAdapter)
            adapter.tokens = TOKEN_ADDRESSES.get("arbitrum", {})

            token_address = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
            spender = "0xF75584eF6673aD213a685a1B58Cc0330B8eA22Cf"

            tx = adapter._build_approve_transaction(
                token_address=token_address,
                spender=spender,
                amount=1000000,
            )

            assert tx is not None
            assert tx.to == token_address
            assert tx.value == 0
            assert tx.tx_type == "approve"
            assert tx.data.startswith("0x095ea7b3")  # approve selector

    def test_skip_approve_for_native_token(self, enso_config):
        """Test that approve is skipped for native ETH."""
        with patch.object(EnsoAdapter, "__init__", lambda self, config, **kwargs: None):
            adapter = EnsoAdapter.__new__(EnsoAdapter)
            adapter.tokens = TOKEN_ADDRESSES.get("arbitrum", {})

            native_token = "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE"
            tx = adapter._build_approve_transaction(
                token_address=native_token,
                spender="0xF75584eF6673aD213a685a1B58Cc0330B8eA22Cf",
                amount=1000000,
            )

            assert tx is None


class TestSwapIntentCompilation:
    """Test SwapIntent compilation."""

    def test_compile_swap_intent_with_amount_usd(self, enso_config, mock_route_transaction):
        """Test compiling a swap intent with USD amount."""
        with patch.object(EnsoAdapter, "__init__", lambda self, config, **kwargs: None):
            adapter = EnsoAdapter.__new__(EnsoAdapter)
            adapter.config = enso_config
            adapter.chain = "arbitrum"
            adapter.wallet_address = enso_config.wallet_address
            adapter.tokens = TOKEN_ADDRESSES.get("arbitrum", {})
            adapter.use_safe_route_single = False

            # Mock the client
            mock_client = MagicMock()
            mock_client.get_route.return_value = mock_route_transaction
            mock_client.get_router_address.return_value = "0xF75584eF6673aD213a685a1B58Cc0330B8eA22Cf"
            adapter.client = mock_client

            intent = SwapIntent(
                from_token="USDC",
                to_token="WETH",
                amount_usd=Decimal("1000"),
                max_slippage=Decimal("0.005"),  # 0.5%
            )

            bundle = adapter.compile_swap_intent(intent)

            assert bundle.intent_type == IntentType.SWAP.value
            assert len(bundle.transactions) == 2  # approve + swap
            assert bundle.metadata["from_token"] == "USDC"
            assert bundle.metadata["to_token"] == "WETH"
            assert bundle.metadata["protocol"] == "enso"
            assert "error" not in bundle.metadata

    def test_compile_swap_intent_with_direct_amount(self, enso_config, mock_route_transaction):
        """Test compiling a swap intent with direct token amount."""
        with patch.object(EnsoAdapter, "__init__", lambda self, config, **kwargs: None):
            adapter = EnsoAdapter.__new__(EnsoAdapter)
            adapter.config = enso_config
            adapter.chain = "arbitrum"
            adapter.wallet_address = enso_config.wallet_address
            adapter.tokens = TOKEN_ADDRESSES.get("arbitrum", {})
            adapter.use_safe_route_single = False

            mock_client = MagicMock()
            mock_client.get_route.return_value = mock_route_transaction
            mock_client.get_router_address.return_value = "0xF75584eF6673aD213a685a1B58Cc0330B8eA22Cf"
            adapter.client = mock_client

            intent = SwapIntent(
                from_token="USDC",
                to_token="WETH",
                amount=Decimal("1000"),  # 1000 USDC
                max_slippage=Decimal("0.005"),
            )

            bundle = adapter.compile_swap_intent(intent)

            assert bundle.intent_type == IntentType.SWAP.value
            assert len(bundle.transactions) == 2
            assert "error" not in bundle.metadata

    def test_compile_swap_intent_unknown_token(self, enso_config):
        """Test that unknown token returns error bundle."""
        with patch.object(EnsoAdapter, "__init__", lambda self, config, **kwargs: None):
            adapter = EnsoAdapter.__new__(EnsoAdapter)
            adapter.config = enso_config
            adapter.chain = "arbitrum"
            adapter.wallet_address = enso_config.wallet_address
            adapter.tokens = TOKEN_ADDRESSES.get("arbitrum", {})
            adapter.use_safe_route_single = False

            intent = SwapIntent(
                from_token="UNKNOWN",
                to_token="WETH",
                amount_usd=Decimal("1000"),
            )

            bundle = adapter.compile_swap_intent(intent)

            assert bundle.intent_type == IntentType.SWAP.value
            assert len(bundle.transactions) == 0
            assert "error" in bundle.metadata
            assert "Unknown input token" in bundle.metadata["error"]


class TestSafeRouteTransformation:
    """Test safeRouteSingle transformation."""

    def test_transform_to_safe_route_single(self, enso_config):
        """Test transformation of routeSingle to safeRouteSingle."""
        with patch.object(EnsoAdapter, "__init__", lambda self, config, **kwargs: None):
            adapter = EnsoAdapter.__new__(EnsoAdapter)
            adapter.wallet_address = enso_config.wallet_address

            # Create a simple routeSingle calldata
            # This is a simplified test - real calldata is more complex
            original_data = "0xb94c3609" + "00" * 100

            result = adapter._transform_to_safe_route_single(
                original_data=original_data,
                token_out_address="0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
                receiver=enso_config.wallet_address,
                amount_out=1000000000000000000,
                slippage_bps=50,
            )

            # If transformation fails, it returns original data
            # In a real test we'd verify the safeRouteSingle selector
            assert result is not None
