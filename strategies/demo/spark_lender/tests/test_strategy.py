"""Tests for Spark Lender demo strategy.

Tests verify the strategy's decision logic for supplying DAI to Spark.

To run:
    uv run pytest strategies/demo/spark_lender/tests/ -v
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from strategies.demo.spark_lender.strategy import SparkLenderStrategy

# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def mock_market():
    """Create a mock MarketSnapshot."""
    market = MagicMock()
    return market


def create_strategy(config: dict | None = None) -> SparkLenderStrategy:
    """Create a SparkLenderStrategy with test configuration."""
    default_config = {
        "min_supply_amount": "100",
        "force_action": "",
    }
    if config:
        default_config.update(config)

    # Mock the base class initialization
    with patch.object(SparkLenderStrategy, "__init__", lambda self, *args, **kwargs: None):
        strategy = SparkLenderStrategy.__new__(SparkLenderStrategy)

    # Set required attributes manually (use underscore prefix for properties)
    strategy.config = default_config
    strategy._chain = "ethereum"
    strategy._wallet_address = "0x1234567890123456789012345678901234567890"

    # Initialize strategy-specific attributes
    strategy.min_supply_amount = Decimal(str(default_config.get("min_supply_amount", "100")))
    strategy.force_action = str(default_config.get("force_action", "")).lower()
    strategy._supplied = False
    strategy._supplied_amount = Decimal("0")

    return strategy


# =============================================================================
# Initialization Tests
# =============================================================================


class TestSparkLenderInitialization:
    """Test strategy initialization and configuration."""

    def test_default_configuration(self):
        """Test strategy initializes with correct defaults."""
        strategy = create_strategy()

        assert strategy.min_supply_amount == Decimal("100")
        assert strategy.force_action == ""
        assert strategy._supplied is False
        assert strategy._supplied_amount == Decimal("0")

    def test_custom_min_supply_amount(self):
        """Test custom min_supply_amount configuration."""
        strategy = create_strategy({"min_supply_amount": "500"})

        assert strategy.min_supply_amount == Decimal("500")

    def test_force_action_supply(self):
        """Test force_action='supply' configuration."""
        strategy = create_strategy({"force_action": "supply"})

        assert strategy.force_action == "supply"


# =============================================================================
# Decision Logic Tests
# =============================================================================


class TestSparkLenderDecisionLogic:
    """Test strategy decision logic."""

    def test_supply_when_sufficient_balance(self, mock_market):
        """Test strategy returns SupplyIntent when balance >= min_supply_amount."""
        strategy = create_strategy({"min_supply_amount": "100"})

        # Mock balance to return sufficient DAI
        mock_balance = MagicMock()
        mock_balance.balance = Decimal("500")
        mock_market.balance.return_value = mock_balance

        intent = strategy.decide(mock_market)

        assert intent is not None
        assert intent.intent_type.value == "SUPPLY"
        assert intent.protocol == "spark"
        assert intent.token == "DAI"
        assert intent.amount == Decimal("500")

    def test_hold_when_insufficient_balance(self, mock_market):
        """Test strategy returns HoldIntent when balance < min_supply_amount."""
        strategy = create_strategy({"min_supply_amount": "1000"})

        # Mock balance to return insufficient DAI
        mock_balance = MagicMock()
        mock_balance.balance = Decimal("500")
        mock_market.balance.return_value = mock_balance

        intent = strategy.decide(mock_market)

        assert intent is not None
        assert intent.intent_type.value == "HOLD"
        assert "Insufficient DAI balance" in intent.reason

    def test_hold_when_already_supplied(self, mock_market):
        """Test strategy returns HoldIntent when already supplied."""
        strategy = create_strategy()
        strategy._supplied = True
        strategy._supplied_amount = Decimal("1000")

        intent = strategy.decide(mock_market)

        assert intent is not None
        assert intent.intent_type.value == "HOLD"
        assert "Already supplied" in intent.reason

    def test_force_action_supply(self, mock_market):
        """Test strategy forces supply action when force_action='supply'."""
        strategy = create_strategy({"force_action": "supply", "min_supply_amount": "100"})

        intent = strategy.decide(mock_market)

        assert intent is not None
        assert intent.intent_type.value == "SUPPLY"
        assert intent.amount == Decimal("100")

    def test_supply_uses_full_balance(self, mock_market):
        """Test supply intent uses full available balance."""
        strategy = create_strategy({"min_supply_amount": "100"})

        # Mock balance to return more than min
        mock_balance = MagicMock()
        mock_balance.balance = Decimal("5000")
        mock_market.balance.return_value = mock_balance

        intent = strategy.decide(mock_market)

        assert intent is not None
        assert intent.intent_type.value == "SUPPLY"
        assert intent.amount == Decimal("5000")  # Full balance

    def test_balance_fetch_error_holds(self, mock_market):
        """Test strategy holds when balance fetch fails."""
        strategy = create_strategy()

        # Mock balance to raise error
        mock_market.balance.side_effect = ValueError("Could not fetch balance")

        intent = strategy.decide(mock_market)

        assert intent is not None
        assert intent.intent_type.value == "HOLD"
        assert "Could not fetch DAI balance" in intent.reason


# =============================================================================
# SupplyIntent Configuration Tests
# =============================================================================


class TestSupplyIntentConfiguration:
    """Test SupplyIntent is configured correctly."""

    def test_supply_intent_collateral_always_enabled(self, mock_market):
        """Test SupplyIntent always has use_as_collateral=True.

        Note: Spark automatically uses all supplied assets as collateral.
        Unlike Aave V3, this cannot be disabled per-asset.
        """
        strategy = create_strategy()

        mock_balance = MagicMock()
        mock_balance.balance = Decimal("1000")
        mock_market.balance.return_value = mock_balance

        intent = strategy.decide(mock_market)

        # Spark always uses supplied assets as collateral
        assert intent.use_as_collateral is True

    def test_supply_intent_chain(self, mock_market):
        """Test SupplyIntent targets ethereum chain."""
        strategy = create_strategy()

        mock_balance = MagicMock()
        mock_balance.balance = Decimal("1000")
        mock_market.balance.return_value = mock_balance

        intent = strategy.decide(mock_market)

        assert intent.chain == "ethereum"

    def test_supply_intent_protocol(self, mock_market):
        """Test SupplyIntent targets spark protocol."""
        strategy = create_strategy()

        mock_balance = MagicMock()
        mock_balance.balance = Decimal("1000")
        mock_market.balance.return_value = mock_balance

        intent = strategy.decide(mock_market)

        assert intent.protocol == "spark"

    def test_supply_intent_token(self, mock_market):
        """Test SupplyIntent targets DAI token."""
        strategy = create_strategy()

        mock_balance = MagicMock()
        mock_balance.balance = Decimal("1000")
        mock_market.balance.return_value = mock_balance

        intent = strategy.decide(mock_market)

        assert intent.token == "DAI"


# =============================================================================
# State Management Tests
# =============================================================================


class TestStateManagement:
    """Test strategy state management."""

    def test_on_intent_executed_success(self, mock_market):
        """Test state is updated after successful supply."""
        strategy = create_strategy()

        # Create a mock successful supply intent
        mock_intent = MagicMock()
        mock_intent.intent_type.value = "SUPPLY"
        mock_intent.amount = Decimal("1000")

        strategy.on_intent_executed(mock_intent, success=True, result=None)

        assert strategy._supplied is True
        assert strategy._supplied_amount == Decimal("1000")

    def test_on_intent_executed_failure(self, mock_market):
        """Test state is not updated after failed supply."""
        strategy = create_strategy()

        mock_intent = MagicMock()
        mock_intent.intent_type.value = "SUPPLY"
        mock_intent.amount = Decimal("1000")

        strategy.on_intent_executed(mock_intent, success=False, result=None)

        assert strategy._supplied is False
        assert strategy._supplied_amount == Decimal("0")

    def test_get_persistent_state(self):
        """Test get_persistent_state returns correct state."""
        strategy = create_strategy()
        strategy._supplied = True
        strategy._supplied_amount = Decimal("2500")

        state = strategy.get_persistent_state()

        assert state["supplied"] is True
        assert state["supplied_amount"] == "2500"

    def test_load_persistent_state(self):
        """Test load_persistent_state restores state correctly."""
        strategy = create_strategy()

        state = {"supplied": True, "supplied_amount": "3000"}
        strategy.load_persistent_state(state)

        assert strategy._supplied is True
        assert strategy._supplied_amount == Decimal("3000")


# =============================================================================
# Status Reporting Tests
# =============================================================================


class TestStatusReporting:
    """Test strategy status reporting."""

    def test_get_status(self):
        """Test get_status returns correct information."""
        strategy = create_strategy()
        strategy._supplied = True
        strategy._supplied_amount = Decimal("1500")

        status = strategy.get_status()

        assert status["strategy"] == "demo_spark_lender"
        assert status["chain"] == "ethereum"
        assert status["config"]["min_supply_amount"] == "100"
        assert status["state"]["supplied"] is True
        assert status["state"]["supplied_amount"] == "1500"
