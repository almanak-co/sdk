"""Tests for Lido Staker demo strategy.

Tests verify the strategy's decision logic for staking ETH with Lido.

To run:
    uv run pytest strategies/demo/lido_staker/tests/ -v
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from strategies.demo.lido_staker.strategy import LidoStakerStrategy

# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def mock_market():
    """Create a mock MarketSnapshot."""
    market = MagicMock()
    return market


def create_strategy(config: dict | None = None) -> LidoStakerStrategy:
    """Create a LidoStakerStrategy with test configuration."""
    default_config = {
        "min_stake_amount": "0.1",
        "receive_wrapped": True,
        "force_action": "",
    }
    if config:
        default_config.update(config)

    # Mock the base class initialization
    with patch.object(LidoStakerStrategy, "__init__", lambda self, *args, **kwargs: None):
        strategy = LidoStakerStrategy.__new__(LidoStakerStrategy)

    # Set required attributes manually (use underscore prefix for properties)
    strategy.config = default_config
    strategy._chain = "ethereum"
    strategy._wallet_address = "0x1234567890123456789012345678901234567890"

    # Initialize strategy-specific attributes
    strategy.min_stake_amount = Decimal(str(default_config.get("min_stake_amount", "0.1")))
    strategy.receive_wrapped = default_config.get("receive_wrapped", True)
    strategy.force_action = str(default_config.get("force_action", "")).lower()
    strategy._staked = False
    strategy._staked_amount = Decimal("0")

    return strategy


# =============================================================================
# Initialization Tests
# =============================================================================


class TestLidoStakerInitialization:
    """Test strategy initialization and configuration."""

    def test_default_configuration(self):
        """Test strategy initializes with correct defaults."""
        strategy = create_strategy()

        assert strategy.min_stake_amount == Decimal("0.1")
        assert strategy.receive_wrapped is True
        assert strategy.force_action == ""
        assert strategy._staked is False
        assert strategy._staked_amount == Decimal("0")

    def test_custom_min_stake_amount(self):
        """Test custom min_stake_amount configuration."""
        strategy = create_strategy({"min_stake_amount": "1.5"})

        assert strategy.min_stake_amount == Decimal("1.5")

    def test_receive_wrapped_false(self):
        """Test receive_wrapped=False configuration."""
        strategy = create_strategy({"receive_wrapped": False})

        assert strategy.receive_wrapped is False

    def test_force_action_stake(self):
        """Test force_action='stake' configuration."""
        strategy = create_strategy({"force_action": "stake"})

        assert strategy.force_action == "stake"


# =============================================================================
# Decision Logic Tests
# =============================================================================


class TestLidoStakerDecisionLogic:
    """Test strategy decision logic."""

    def test_stake_when_sufficient_balance(self, mock_market):
        """Test strategy returns StakeIntent when balance >= min_stake_amount."""
        strategy = create_strategy({"min_stake_amount": "0.1"})

        # Mock balance to return sufficient ETH
        mock_balance = MagicMock()
        mock_balance.balance = Decimal("0.5")
        mock_market.balance.return_value = mock_balance

        intent = strategy.decide(mock_market)

        assert intent is not None
        assert intent.intent_type.value == "STAKE"
        assert intent.protocol == "lido"
        assert intent.token_in == "ETH"
        assert intent.amount == Decimal("0.5")

    def test_hold_when_insufficient_balance(self, mock_market):
        """Test strategy returns HoldIntent when balance < min_stake_amount."""
        strategy = create_strategy({"min_stake_amount": "1.0"})

        # Mock balance to return insufficient ETH
        mock_balance = MagicMock()
        mock_balance.balance = Decimal("0.5")
        mock_market.balance.return_value = mock_balance

        intent = strategy.decide(mock_market)

        assert intent is not None
        assert intent.intent_type.value == "HOLD"
        assert "Insufficient ETH balance" in intent.reason

    def test_hold_when_already_staked(self, mock_market):
        """Test strategy returns HoldIntent when already staked."""
        strategy = create_strategy()
        strategy._staked = True
        strategy._staked_amount = Decimal("1.0")

        intent = strategy.decide(mock_market)

        assert intent is not None
        assert intent.intent_type.value == "HOLD"
        assert "Already staked" in intent.reason

    def test_force_action_stake(self, mock_market):
        """Test strategy forces stake action when force_action='stake'."""
        strategy = create_strategy({"force_action": "stake", "min_stake_amount": "0.1"})

        intent = strategy.decide(mock_market)

        assert intent is not None
        assert intent.intent_type.value == "STAKE"
        assert intent.amount == Decimal("0.1")

    def test_stake_uses_full_balance(self, mock_market):
        """Test stake intent uses full available balance."""
        strategy = create_strategy({"min_stake_amount": "0.1"})

        # Mock balance to return more than min
        mock_balance = MagicMock()
        mock_balance.balance = Decimal("5.0")
        mock_market.balance.return_value = mock_balance

        intent = strategy.decide(mock_market)

        assert intent is not None
        assert intent.intent_type.value == "STAKE"
        assert intent.amount == Decimal("5.0")  # Full balance

    def test_balance_fetch_error_holds(self, mock_market):
        """Test strategy holds when balance fetch fails."""
        strategy = create_strategy()

        # Mock balance to raise error
        mock_market.balance.side_effect = ValueError("Could not fetch balance")

        intent = strategy.decide(mock_market)

        assert intent is not None
        assert intent.intent_type.value == "HOLD"
        assert "Could not fetch ETH balance" in intent.reason


# =============================================================================
# StakeIntent Configuration Tests
# =============================================================================


class TestStakeIntentConfiguration:
    """Test StakeIntent is configured correctly."""

    def test_stake_intent_wrapped(self, mock_market):
        """Test StakeIntent with receive_wrapped=True."""
        strategy = create_strategy({"receive_wrapped": True})

        mock_balance = MagicMock()
        mock_balance.balance = Decimal("1.0")
        mock_market.balance.return_value = mock_balance

        intent = strategy.decide(mock_market)

        assert intent.receive_wrapped is True

    def test_stake_intent_unwrapped(self, mock_market):
        """Test StakeIntent with receive_wrapped=False."""
        strategy = create_strategy({"receive_wrapped": False})

        mock_balance = MagicMock()
        mock_balance.balance = Decimal("1.0")
        mock_market.balance.return_value = mock_balance

        intent = strategy.decide(mock_market)

        assert intent.receive_wrapped is False

    def test_stake_intent_chain(self, mock_market):
        """Test StakeIntent targets ethereum chain."""
        strategy = create_strategy()

        mock_balance = MagicMock()
        mock_balance.balance = Decimal("1.0")
        mock_market.balance.return_value = mock_balance

        intent = strategy.decide(mock_market)

        assert intent.chain == "ethereum"


# =============================================================================
# State Management Tests
# =============================================================================


class TestStateManagement:
    """Test strategy state management."""

    def test_on_intent_executed_success(self, mock_market):
        """Test state is updated after successful stake."""
        strategy = create_strategy()

        # Create a mock successful stake intent
        mock_intent = MagicMock()
        mock_intent.intent_type.value = "STAKE"
        mock_intent.amount = Decimal("1.0")

        strategy.on_intent_executed(mock_intent, success=True, result=None)

        assert strategy._staked is True
        assert strategy._staked_amount == Decimal("1.0")

    def test_on_intent_executed_failure(self, mock_market):
        """Test state is not updated after failed stake."""
        strategy = create_strategy()

        mock_intent = MagicMock()
        mock_intent.intent_type.value = "STAKE"
        mock_intent.amount = Decimal("1.0")

        strategy.on_intent_executed(mock_intent, success=False, result=None)

        assert strategy._staked is False
        assert strategy._staked_amount == Decimal("0")

    def test_get_persistent_state(self):
        """Test get_persistent_state returns correct state."""
        strategy = create_strategy()
        strategy._staked = True
        strategy._staked_amount = Decimal("2.5")

        state = strategy.get_persistent_state()

        assert state["staked"] is True
        assert state["staked_amount"] == "2.5"

    def test_load_persistent_state(self):
        """Test load_persistent_state restores state correctly."""
        strategy = create_strategy()

        state = {"staked": True, "staked_amount": "3.0"}
        strategy.load_persistent_state(state)

        assert strategy._staked is True
        assert strategy._staked_amount == Decimal("3.0")


# =============================================================================
# Status Reporting Tests
# =============================================================================


class TestStatusReporting:
    """Test strategy status reporting."""

    def test_get_status(self):
        """Test get_status returns correct information."""
        strategy = create_strategy()
        strategy._staked = True
        strategy._staked_amount = Decimal("1.5")

        status = strategy.get_status()

        assert status["strategy"] == "demo_lido_staker"
        assert status["chain"] == "ethereum"
        assert status["config"]["min_stake_amount"] == "0.1"
        assert status["config"]["receive_wrapped"] is True
        assert status["state"]["staked"] is True
        assert status["state"]["staked_amount"] == "1.5"
