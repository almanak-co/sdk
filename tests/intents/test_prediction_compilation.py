"""Tests for Prediction Intent Compilation in IntentCompiler.

Tests verify that IntentCompiler correctly routes prediction intents to
the PolymarketAdapter and handles compilation results.

To run:
    uv run pytest tests/intents/test_prediction_compilation.py -v
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest
from pydantic import SecretStr

from almanak.framework.intents import (
    IntentType,
    PredictionBuyIntent,
    PredictionRedeemIntent,
    PredictionSellIntent,
)
from almanak.framework.intents.compiler import (
    CompilationStatus,
    IntentCompiler,
    IntentCompilerConfig,
)
from almanak.framework.models.reproduction_bundle import ActionBundle

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def test_wallet():
    """Return a test wallet address."""
    return "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"


@pytest.fixture
def mock_polymarket_config():
    """Create a mock PolymarketConfig."""
    config = MagicMock()
    config.wallet_address = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
    config.private_key = SecretStr("0x" + "1" * 64)
    return config


@pytest.fixture
def mock_successful_buy_bundle():
    """Create a mock successful buy ActionBundle."""
    return ActionBundle(
        intent_type=IntentType.PREDICTION_BUY.value,
        transactions=[],
        metadata={
            "intent_id": "test-intent-123",
            "market_id": "12345",
            "market_question": "Will Bitcoin exceed $100k?",
            "token_id": "111111111111111111111111",
            "outcome": "YES",
            "side": "BUY",
            "price": "0.65",
            "size": "100",
            "order_type": "GTC",
            "order_payload": {"order": {}, "signature": "0x"},
            "protocol": "polymarket",
            "chain": "polygon",
        },
    )


@pytest.fixture
def mock_successful_sell_bundle():
    """Create a mock successful sell ActionBundle."""
    return ActionBundle(
        intent_type=IntentType.PREDICTION_SELL.value,
        transactions=[],
        metadata={
            "intent_id": "test-intent-456",
            "market_id": "12345",
            "market_question": "Will Bitcoin exceed $100k?",
            "token_id": "111111111111111111111111",
            "outcome": "YES",
            "side": "SELL",
            "price": "0.70",
            "size": "50",
            "order_type": "GTC",
            "order_payload": {"order": {}, "signature": "0x"},
            "protocol": "polymarket",
            "chain": "polygon",
        },
    )


@pytest.fixture
def mock_successful_redeem_bundle():
    """Create a mock successful redeem ActionBundle."""
    return ActionBundle(
        intent_type=IntentType.PREDICTION_REDEEM.value,
        transactions=[
            {
                "to": "0xConditionalTokens",
                "value": 0,
                "data": "0xredeemdata",
                "gas_estimate": 200000,
                "description": "Redeem winning positions",
                "tx_type": "redeem",
            }
        ],
        metadata={
            "intent_id": "test-intent-789",
            "market_id": "12345",
            "market_question": "Will Bitcoin exceed $100k?",
            "condition_id": "0x1234567890abcdef",
            "outcome": "YES",
            "winning_outcome": "YES",
            "protocol": "polymarket",
            "chain": "polygon",
        },
    )


@pytest.fixture
def mock_error_bundle():
    """Create a mock error ActionBundle."""
    return ActionBundle(
        intent_type=IntentType.PREDICTION_BUY.value,
        transactions=[],
        metadata={
            "error": "Market not found: nonexistent",
            "intent_id": "test-intent-err",
        },
    )


# =============================================================================
# Non-Polygon Chain Tests
# =============================================================================


class TestPredictionCompilationNonPolygon:
    """Tests for prediction intent compilation on non-Polygon chains."""

    def test_prediction_buy_fails_on_non_polygon(self, test_wallet):
        """Test that prediction buy fails on non-Polygon chains."""
        compiler = IntentCompiler(
            chain="arbitrum",
            wallet_address=test_wallet,
            config=IntentCompilerConfig(allow_placeholder_prices=True),
        )

        intent = PredictionBuyIntent(
            market_id="test-market",
            outcome="YES",
            amount_usd=Decimal("100"),
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "only supported on Polygon" in result.error

    def test_prediction_sell_fails_on_non_polygon(self, test_wallet):
        """Test that prediction sell fails on non-Polygon chains."""
        compiler = IntentCompiler(
            chain="ethereum",
            wallet_address=test_wallet,
            config=IntentCompilerConfig(allow_placeholder_prices=True),
        )

        intent = PredictionSellIntent(
            market_id="test-market",
            outcome="YES",
            shares=Decimal("50"),
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "only supported on Polygon" in result.error

    def test_prediction_redeem_fails_on_non_polygon(self, test_wallet):
        """Test that prediction redeem fails on non-Polygon chains."""
        compiler = IntentCompiler(
            chain="base",
            wallet_address=test_wallet,
            config=IntentCompilerConfig(allow_placeholder_prices=True),
        )

        intent = PredictionRedeemIntent(
            market_id="test-market",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "only supported on Polygon" in result.error


# =============================================================================
# Polygon Without Config Tests
# =============================================================================


class TestPredictionCompilationNoConfig:
    """Tests for prediction compilation on Polygon without config."""

    def test_prediction_buy_fails_without_config(self, test_wallet):
        """Test that prediction buy fails without Polymarket config."""
        compiler = IntentCompiler(
            chain="polygon",
            wallet_address=test_wallet,
            config=IntentCompilerConfig(allow_placeholder_prices=True),
        )

        intent = PredictionBuyIntent(
            market_id="test-market",
            outcome="YES",
            amount_usd=Decimal("100"),
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "PolymarketAdapter not initialized" in result.error
        assert "polymarket_config" in result.error

    def test_prediction_sell_fails_without_config(self, test_wallet):
        """Test that prediction sell fails without Polymarket config."""
        compiler = IntentCompiler(
            chain="polygon",
            wallet_address=test_wallet,
            config=IntentCompilerConfig(allow_placeholder_prices=True),
        )

        intent = PredictionSellIntent(
            market_id="test-market",
            outcome="YES",
            shares=Decimal("50"),
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "PolymarketAdapter not initialized" in result.error

    def test_prediction_redeem_fails_without_config(self, test_wallet):
        """Test that prediction redeem fails without Polymarket config."""
        compiler = IntentCompiler(
            chain="polygon",
            wallet_address=test_wallet,
            config=IntentCompilerConfig(allow_placeholder_prices=True),
        )

        intent = PredictionRedeemIntent(
            market_id="test-market",
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "PolymarketAdapter not initialized" in result.error


# =============================================================================
# Successful Compilation Tests (with mocked adapter)
# =============================================================================


class TestPredictionBuyCompilation:
    """Tests for successful PredictionBuyIntent compilation."""

    def test_compile_prediction_buy_success(self, test_wallet, mock_polymarket_config, mock_successful_buy_bundle):
        """Test successful compilation of prediction buy intent."""
        with patch("almanak.framework.intents.compiler.IntentCompiler._init_polymarket_adapter"):
            compiler = IntentCompiler(
                chain="polygon",
                wallet_address=test_wallet,
                config=IntentCompilerConfig(
                    allow_placeholder_prices=True,
                    polymarket_config=mock_polymarket_config,
                ),
            )

            # Mock the adapter
            mock_adapter = MagicMock()
            mock_adapter.compile_intent.return_value = mock_successful_buy_bundle
            compiler._polymarket_adapter = mock_adapter

            intent = PredictionBuyIntent(
                market_id="test-market",
                outcome="YES",
                amount_usd=Decimal("100"),
            )

            result = compiler.compile(intent)

            assert result.status == CompilationStatus.SUCCESS
            assert result.action_bundle is not None
            assert result.action_bundle.intent_type == IntentType.PREDICTION_BUY.value
            assert result.action_bundle.metadata["outcome"] == "YES"
            assert result.action_bundle.metadata["protocol"] == "polymarket"
            assert result.total_gas_estimate == 0  # CLOB orders are off-chain
            assert len(result.transactions) == 0

    def test_compile_prediction_buy_with_limit_order(
        self, test_wallet, mock_polymarket_config, mock_successful_buy_bundle
    ):
        """Test compilation of prediction buy with limit order."""
        with patch("almanak.framework.intents.compiler.IntentCompiler._init_polymarket_adapter"):
            compiler = IntentCompiler(
                chain="polygon",
                wallet_address=test_wallet,
                config=IntentCompilerConfig(
                    allow_placeholder_prices=True,
                    polymarket_config=mock_polymarket_config,
                ),
            )

            mock_adapter = MagicMock()
            mock_adapter.compile_intent.return_value = mock_successful_buy_bundle
            compiler._polymarket_adapter = mock_adapter

            intent = PredictionBuyIntent(
                market_id="test-market",
                outcome="YES",
                shares=Decimal("100"),
                max_price=Decimal("0.65"),
                order_type="limit",
            )

            result = compiler.compile(intent)

            assert result.status == CompilationStatus.SUCCESS
            mock_adapter.compile_intent.assert_called_once_with(intent)


class TestPredictionSellCompilation:
    """Tests for successful PredictionSellIntent compilation."""

    def test_compile_prediction_sell_success(self, test_wallet, mock_polymarket_config, mock_successful_sell_bundle):
        """Test successful compilation of prediction sell intent."""
        with patch("almanak.framework.intents.compiler.IntentCompiler._init_polymarket_adapter"):
            compiler = IntentCompiler(
                chain="polygon",
                wallet_address=test_wallet,
                config=IntentCompilerConfig(
                    allow_placeholder_prices=True,
                    polymarket_config=mock_polymarket_config,
                ),
            )

            mock_adapter = MagicMock()
            mock_adapter.compile_intent.return_value = mock_successful_sell_bundle
            compiler._polymarket_adapter = mock_adapter

            intent = PredictionSellIntent(
                market_id="test-market",
                outcome="YES",
                shares=Decimal("50"),
            )

            result = compiler.compile(intent)

            assert result.status == CompilationStatus.SUCCESS
            assert result.action_bundle is not None
            assert result.action_bundle.intent_type == IntentType.PREDICTION_SELL.value
            assert result.action_bundle.metadata["side"] == "SELL"
            assert result.total_gas_estimate == 0

    def test_compile_prediction_sell_all_shares(self, test_wallet, mock_polymarket_config, mock_successful_sell_bundle):
        """Test compilation of prediction sell with shares='all'."""
        with patch("almanak.framework.intents.compiler.IntentCompiler._init_polymarket_adapter"):
            compiler = IntentCompiler(
                chain="polygon",
                wallet_address=test_wallet,
                config=IntentCompilerConfig(
                    allow_placeholder_prices=True,
                    polymarket_config=mock_polymarket_config,
                ),
            )

            mock_adapter = MagicMock()
            mock_adapter.compile_intent.return_value = mock_successful_sell_bundle
            compiler._polymarket_adapter = mock_adapter

            intent = PredictionSellIntent(
                market_id="test-market",
                outcome="YES",
                shares="all",
            )

            result = compiler.compile(intent)

            assert result.status == CompilationStatus.SUCCESS
            mock_adapter.compile_intent.assert_called_once_with(intent)


class TestPredictionRedeemCompilation:
    """Tests for successful PredictionRedeemIntent compilation."""

    def test_compile_prediction_redeem_success(
        self, test_wallet, mock_polymarket_config, mock_successful_redeem_bundle
    ):
        """Test successful compilation of prediction redeem intent."""
        with patch("almanak.framework.intents.compiler.IntentCompiler._init_polymarket_adapter"):
            compiler = IntentCompiler(
                chain="polygon",
                wallet_address=test_wallet,
                config=IntentCompilerConfig(
                    allow_placeholder_prices=True,
                    polymarket_config=mock_polymarket_config,
                ),
            )

            mock_adapter = MagicMock()
            mock_adapter.compile_intent.return_value = mock_successful_redeem_bundle
            compiler._polymarket_adapter = mock_adapter

            intent = PredictionRedeemIntent(
                market_id="test-market",
            )

            result = compiler.compile(intent)

            assert result.status == CompilationStatus.SUCCESS
            assert result.action_bundle is not None
            assert result.action_bundle.intent_type == IntentType.PREDICTION_REDEEM.value
            assert result.action_bundle.metadata["winning_outcome"] == "YES"
            assert len(result.transactions) == 1
            assert result.total_gas_estimate == 200000

    def test_compile_prediction_redeem_specific_outcome(
        self, test_wallet, mock_polymarket_config, mock_successful_redeem_bundle
    ):
        """Test compilation of prediction redeem with specific outcome."""
        with patch("almanak.framework.intents.compiler.IntentCompiler._init_polymarket_adapter"):
            compiler = IntentCompiler(
                chain="polygon",
                wallet_address=test_wallet,
                config=IntentCompilerConfig(
                    allow_placeholder_prices=True,
                    polymarket_config=mock_polymarket_config,
                ),
            )

            mock_adapter = MagicMock()
            mock_adapter.compile_intent.return_value = mock_successful_redeem_bundle
            compiler._polymarket_adapter = mock_adapter

            intent = PredictionRedeemIntent(
                market_id="test-market",
                outcome="YES",
            )

            result = compiler.compile(intent)

            assert result.status == CompilationStatus.SUCCESS


# =============================================================================
# Error Handling Tests
# =============================================================================


class TestPredictionCompilationErrors:
    """Tests for error handling in prediction compilation."""

    def test_compile_prediction_buy_adapter_error(self, test_wallet, mock_polymarket_config, mock_error_bundle):
        """Test compilation returns error from adapter."""
        with patch("almanak.framework.intents.compiler.IntentCompiler._init_polymarket_adapter"):
            compiler = IntentCompiler(
                chain="polygon",
                wallet_address=test_wallet,
                config=IntentCompilerConfig(
                    allow_placeholder_prices=True,
                    polymarket_config=mock_polymarket_config,
                ),
            )

            mock_adapter = MagicMock()
            mock_adapter.compile_intent.return_value = mock_error_bundle
            compiler._polymarket_adapter = mock_adapter

            intent = PredictionBuyIntent(
                market_id="nonexistent",
                outcome="YES",
                amount_usd=Decimal("100"),
            )

            result = compiler.compile(intent)

            assert result.status == CompilationStatus.FAILED
            assert "Market not found" in result.error

    def test_compile_prediction_buy_exception(self, test_wallet, mock_polymarket_config):
        """Test compilation handles exceptions from adapter."""
        with patch("almanak.framework.intents.compiler.IntentCompiler._init_polymarket_adapter"):
            compiler = IntentCompiler(
                chain="polygon",
                wallet_address=test_wallet,
                config=IntentCompilerConfig(
                    allow_placeholder_prices=True,
                    polymarket_config=mock_polymarket_config,
                ),
            )

            mock_adapter = MagicMock()
            mock_adapter.compile_intent.side_effect = RuntimeError("Connection failed")
            compiler._polymarket_adapter = mock_adapter

            intent = PredictionBuyIntent(
                market_id="test-market",
                outcome="YES",
                amount_usd=Decimal("100"),
            )

            result = compiler.compile(intent)

            assert result.status == CompilationStatus.FAILED
            assert "Connection failed" in result.error

    def test_compile_prediction_redeem_market_not_resolved(self, test_wallet, mock_polymarket_config):
        """Test redeem compilation handles unresolved market."""
        from almanak.framework.connectors.polymarket.exceptions import (
            PolymarketMarketNotResolvedError,
        )

        with patch("almanak.framework.intents.compiler.IntentCompiler._init_polymarket_adapter"):
            compiler = IntentCompiler(
                chain="polygon",
                wallet_address=test_wallet,
                config=IntentCompilerConfig(
                    allow_placeholder_prices=True,
                    polymarket_config=mock_polymarket_config,
                ),
            )

            mock_adapter = MagicMock()
            mock_adapter.compile_intent.side_effect = PolymarketMarketNotResolvedError("test-market")
            compiler._polymarket_adapter = mock_adapter

            intent = PredictionRedeemIntent(
                market_id="test-market",
            )

            result = compiler.compile(intent)

            assert result.status == CompilationStatus.FAILED
            assert "not resolved" in result.error


# =============================================================================
# Intent ID Preservation Tests
# =============================================================================


class TestIntentIdPreservation:
    """Tests that intent IDs are correctly preserved in results."""

    def test_buy_intent_id_in_result(self, test_wallet):
        """Test that buy intent ID is in failed result."""
        compiler = IntentCompiler(
            chain="ethereum",  # Non-Polygon, will fail
            wallet_address=test_wallet,
            config=IntentCompilerConfig(allow_placeholder_prices=True),
        )

        intent = PredictionBuyIntent(
            market_id="test-market",
            outcome="YES",
            amount_usd=Decimal("100"),
        )

        result = compiler.compile(intent)

        assert result.intent_id == intent.intent_id

    def test_sell_intent_id_in_result(self, test_wallet):
        """Test that sell intent ID is in failed result."""
        compiler = IntentCompiler(
            chain="arbitrum",
            wallet_address=test_wallet,
            config=IntentCompilerConfig(allow_placeholder_prices=True),
        )

        intent = PredictionSellIntent(
            market_id="test-market",
            outcome="YES",
            shares=Decimal("50"),
        )

        result = compiler.compile(intent)

        assert result.intent_id == intent.intent_id

    def test_redeem_intent_id_in_result(self, test_wallet):
        """Test that redeem intent ID is in failed result."""
        compiler = IntentCompiler(
            chain="base",
            wallet_address=test_wallet,
            config=IntentCompilerConfig(allow_placeholder_prices=True),
        )

        intent = PredictionRedeemIntent(
            market_id="test-market",
        )

        result = compiler.compile(intent)

        assert result.intent_id == intent.intent_id
