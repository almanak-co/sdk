"""Tests for Ethena Yield demo strategy.

Tests verify the strategy's decision logic for staking USDe with Ethena.

To run:
    uv run pytest strategies/demo/ethena_yield/tests/ -v
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from strategies.demo.ethena_yield.strategy import EthenaYieldStrategy

# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def mock_market():
    """Create a mock MarketSnapshot."""
    market = MagicMock()
    return market


def create_strategy(config: dict | None = None) -> EthenaYieldStrategy:
    """Create a EthenaYieldStrategy with test configuration."""
    default_config = {
        "min_stake_amount": "100",
        "swap_usdc_to_usde": False,
        "min_usdc_amount": "100",
        "max_slippage_pct": 0.5,
        "force_action": "",
    }
    if config:
        default_config.update(config)

    # Mock the base class initialization
    with patch.object(EthenaYieldStrategy, "__init__", lambda self, *args, **kwargs: None):
        strategy = EthenaYieldStrategy.__new__(EthenaYieldStrategy)

    # Set required attributes manually (use underscore prefix for properties)
    strategy.config = default_config
    strategy._chain = "ethereum"
    strategy._wallet_address = "0x1234567890123456789012345678901234567890"

    # Initialize strategy-specific attributes
    strategy.min_stake_amount = Decimal(str(default_config.get("min_stake_amount", "100")))
    strategy.swap_usdc_to_usde = bool(default_config.get("swap_usdc_to_usde", False))
    strategy.min_usdc_amount = Decimal(str(default_config.get("min_usdc_amount", "100")))
    strategy.max_slippage_pct = float(default_config.get("max_slippage_pct", 0.5))
    strategy.force_action = str(default_config.get("force_action", "")).lower()
    strategy._swapped = False
    strategy._swapped_amount = Decimal("0")
    strategy._staked = False
    strategy._staked_amount = Decimal("0")

    return strategy


# =============================================================================
# Initialization Tests
# =============================================================================


class TestEthenaYieldInitialization:
    """Test strategy initialization and configuration."""

    def test_default_configuration(self):
        """Test strategy initializes with correct defaults."""
        strategy = create_strategy()

        assert strategy.min_stake_amount == Decimal("100")
        assert strategy.force_action == ""
        assert strategy._staked is False
        assert strategy._staked_amount == Decimal("0")

    def test_custom_min_stake_amount(self):
        """Test custom min_stake_amount configuration."""
        strategy = create_strategy({"min_stake_amount": "500"})

        assert strategy.min_stake_amount == Decimal("500")

    def test_force_action_stake(self):
        """Test force_action='stake' configuration."""
        strategy = create_strategy({"force_action": "stake"})

        assert strategy.force_action == "stake"


# =============================================================================
# Decision Logic Tests
# =============================================================================


class TestEthenaYieldDecisionLogic:
    """Test strategy decision logic."""

    def test_stake_when_sufficient_balance(self, mock_market):
        """Test strategy returns StakeIntent when balance >= min_stake_amount."""
        strategy = create_strategy({"min_stake_amount": "100"})

        # Mock balance to return sufficient USDe
        mock_balance = MagicMock()
        mock_balance.balance = Decimal("500")
        mock_market.balance.return_value = mock_balance

        intent = strategy.decide(mock_market)

        assert intent is not None
        assert intent.intent_type.value == "STAKE"
        assert intent.protocol == "ethena"
        assert intent.token_in == "USDe"
        assert intent.amount == Decimal("500")

    def test_hold_when_insufficient_balance(self, mock_market):
        """Test strategy returns HoldIntent when balance < min_stake_amount."""
        strategy = create_strategy({"min_stake_amount": "1000"})

        # Mock balance to return insufficient USDe
        mock_balance = MagicMock()
        mock_balance.balance = Decimal("500")
        mock_market.balance.return_value = mock_balance

        intent = strategy.decide(mock_market)

        assert intent is not None
        assert intent.intent_type.value == "HOLD"
        assert "Insufficient USDe balance" in intent.reason

    def test_hold_when_already_staked(self, mock_market):
        """Test strategy returns HoldIntent when already staked."""
        strategy = create_strategy()
        strategy._staked = True
        strategy._staked_amount = Decimal("1000")

        intent = strategy.decide(mock_market)

        assert intent is not None
        assert intent.intent_type.value == "HOLD"
        assert "Already staked" in intent.reason

    def test_force_action_stake(self, mock_market):
        """Test strategy forces stake action when force_action='stake'."""
        strategy = create_strategy({"force_action": "stake", "min_stake_amount": "100"})

        intent = strategy.decide(mock_market)

        assert intent is not None
        assert intent.intent_type.value == "STAKE"
        assert intent.amount == Decimal("100")

    def test_stake_uses_full_balance(self, mock_market):
        """Test stake intent uses full available balance."""
        strategy = create_strategy({"min_stake_amount": "100"})

        # Mock balance to return more than min
        mock_balance = MagicMock()
        mock_balance.balance = Decimal("5000")
        mock_market.balance.return_value = mock_balance

        intent = strategy.decide(mock_market)

        assert intent is not None
        assert intent.intent_type.value == "STAKE"
        assert intent.amount == Decimal("5000")  # Full balance

    def test_balance_fetch_error_holds(self, mock_market):
        """Test strategy holds when balance fetch fails (balance treated as 0)."""
        strategy = create_strategy()

        # Mock balance to raise error - will be treated as 0 balance
        mock_market.balance.side_effect = ValueError("Could not fetch balance")

        intent = strategy.decide(mock_market)

        assert intent is not None
        assert intent.intent_type.value == "HOLD"
        # Balance errors are caught and balance is treated as 0
        assert "Insufficient USDe balance" in intent.reason


# =============================================================================
# USDC -> USDe Swap Tests
# =============================================================================


class TestSwapFunctionality:
    """Test USDC -> USDe swap functionality."""

    def test_swap_when_usdc_sufficient_and_usde_insufficient(self, mock_market):
        """Test strategy swaps USDC to USDe when enabled and USDC is sufficient."""
        strategy = create_strategy(
            {
                "swap_usdc_to_usde": True,
                "min_usdc_amount": "100",
                "min_stake_amount": "100",
            }
        )

        # Mock: low USDe, sufficient USDC
        def mock_balance(token):
            mock = MagicMock()
            if token == "USDe":
                mock.balance = Decimal("50")  # Below min_stake
            elif token == "USDC":
                mock.balance = Decimal("500")  # Above min_usdc
            return mock

        mock_market.balance.side_effect = mock_balance

        intent = strategy.decide(mock_market)

        assert intent is not None
        assert intent.intent_type.value == "SWAP"
        assert intent.from_token == "USDC"
        assert intent.to_token == "USDe"
        assert intent.amount == Decimal("500")
        assert intent.protocol == "enso"

    def test_no_swap_when_disabled(self, mock_market):
        """Test strategy does not swap when swap_usdc_to_usde is False."""
        strategy = create_strategy(
            {
                "swap_usdc_to_usde": False,  # Disabled
                "min_stake_amount": "100",
            }
        )

        # Mock: low USDe, sufficient USDC
        def mock_balance(token):
            mock = MagicMock()
            if token == "USDe":
                mock.balance = Decimal("50")
            elif token == "USDC":
                mock.balance = Decimal("500")
            return mock

        mock_market.balance.side_effect = mock_balance

        intent = strategy.decide(mock_market)

        assert intent is not None
        assert intent.intent_type.value == "HOLD"
        assert "Insufficient USDe balance" in intent.reason

    def test_stake_takes_priority_over_swap(self, mock_market):
        """Test strategy stakes if USDe sufficient, even with USDC available."""
        strategy = create_strategy(
            {
                "swap_usdc_to_usde": True,
                "min_usdc_amount": "100",
                "min_stake_amount": "100",
            }
        )

        # Mock: sufficient USDe and USDC
        def mock_balance(token):
            mock = MagicMock()
            if token == "USDe":
                mock.balance = Decimal("500")  # Above min_stake
            elif token == "USDC":
                mock.balance = Decimal("500")  # Above min_usdc
            return mock

        mock_market.balance.side_effect = mock_balance

        intent = strategy.decide(mock_market)

        assert intent is not None
        assert intent.intent_type.value == "STAKE"  # Stake takes priority

    def test_force_action_swap(self, mock_market):
        """Test force_action='swap' triggers swap."""
        strategy = create_strategy(
            {
                "force_action": "swap",
                "min_usdc_amount": "100",
            }
        )

        intent = strategy.decide(mock_market)

        assert intent is not None
        assert intent.intent_type.value == "SWAP"
        assert intent.from_token == "USDC"
        assert intent.to_token == "USDe"

    def test_swap_intent_slippage(self, mock_market):
        """Test swap intent uses configured slippage."""
        strategy = create_strategy(
            {
                "swap_usdc_to_usde": True,
                "max_slippage_pct": 1.0,  # 1%
                "min_usdc_amount": "100",
                "min_stake_amount": "1000",  # High so swap triggers
            }
        )

        def mock_balance(token):
            mock = MagicMock()
            if token == "USDe":
                mock.balance = Decimal("50")
            elif token == "USDC":
                mock.balance = Decimal("500")
            return mock

        mock_market.balance.side_effect = mock_balance

        intent = strategy.decide(mock_market)

        assert intent is not None
        assert intent.intent_type.value == "SWAP"
        assert intent.max_slippage == Decimal("0.01")  # 1% = 0.01

    def test_on_intent_executed_swap_success(self):
        """Test state is updated after successful swap."""
        strategy = create_strategy()

        mock_intent = MagicMock()
        mock_intent.intent_type.value = "SWAP"
        mock_intent.amount = Decimal("500")

        strategy.on_intent_executed(mock_intent, success=True, result=None)

        assert strategy._swapped is True
        assert strategy._swapped_amount == Decimal("500")


# =============================================================================
# StakeIntent Configuration Tests
# =============================================================================


class TestStakeIntentConfiguration:
    """Test StakeIntent is configured correctly."""

    def test_stake_intent_protocol(self, mock_market):
        """Test StakeIntent targets ethena protocol."""
        strategy = create_strategy()

        mock_balance = MagicMock()
        mock_balance.balance = Decimal("1000")
        mock_market.balance.return_value = mock_balance

        intent = strategy.decide(mock_market)

        assert intent.protocol == "ethena"

    def test_stake_intent_token_in(self, mock_market):
        """Test StakeIntent uses USDe as token_in."""
        strategy = create_strategy()

        mock_balance = MagicMock()
        mock_balance.balance = Decimal("1000")
        mock_market.balance.return_value = mock_balance

        intent = strategy.decide(mock_market)

        assert intent.token_in == "USDe"

    def test_stake_intent_chain(self, mock_market):
        """Test StakeIntent targets ethereum chain."""
        strategy = create_strategy()

        mock_balance = MagicMock()
        mock_balance.balance = Decimal("1000")
        mock_market.balance.return_value = mock_balance

        intent = strategy.decide(mock_market)

        assert intent.chain == "ethereum"

    def test_stake_intent_receive_wrapped(self, mock_market):
        """Test StakeIntent has receive_wrapped=False (Ethena always outputs sUSDe)."""
        strategy = create_strategy()

        mock_balance = MagicMock()
        mock_balance.balance = Decimal("1000")
        mock_market.balance.return_value = mock_balance

        intent = strategy.decide(mock_market)

        assert intent.receive_wrapped is False


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
        mock_intent.amount = Decimal("1000")

        strategy.on_intent_executed(mock_intent, success=True, result=None)

        assert strategy._staked is True
        assert strategy._staked_amount == Decimal("1000")

    def test_on_intent_executed_failure(self, mock_market):
        """Test state is not updated after failed stake."""
        strategy = create_strategy()

        mock_intent = MagicMock()
        mock_intent.intent_type.value = "STAKE"
        mock_intent.amount = Decimal("1000")

        strategy.on_intent_executed(mock_intent, success=False, result=None)

        assert strategy._staked is False
        assert strategy._staked_amount == Decimal("0")

    def test_get_persistent_state(self):
        """Test get_persistent_state returns correct state."""
        strategy = create_strategy()
        strategy._swapped = True
        strategy._swapped_amount = Decimal("1000")
        strategy._staked = True
        strategy._staked_amount = Decimal("2500")

        state = strategy.get_persistent_state()

        assert state["swapped"] is True
        assert state["swapped_amount"] == "1000"
        assert state["staked"] is True
        assert state["staked_amount"] == "2500"

    def test_load_persistent_state(self):
        """Test load_persistent_state restores state correctly."""
        strategy = create_strategy()

        state = {"staked": True, "staked_amount": "3000"}
        strategy.load_persistent_state(state)

        assert strategy._staked is True
        assert strategy._staked_amount == Decimal("3000")


# =============================================================================
# Status Reporting Tests
# =============================================================================


class TestStatusReporting:
    """Test strategy status reporting."""

    def test_get_status(self):
        """Test get_status returns correct information."""
        strategy = create_strategy()
        strategy._swapped = True
        strategy._swapped_amount = Decimal("1000")
        strategy._staked = True
        strategy._staked_amount = Decimal("1500")

        status = strategy.get_status()

        assert status["strategy"] == "demo_ethena_yield"
        assert status["chain"] == "ethereum"
        assert status["config"]["min_stake_amount"] == "100"
        assert status["config"]["swap_usdc_to_usde"] is False
        assert status["config"]["min_usdc_amount"] == "100"
        assert status["state"]["swapped"] is True
        assert status["state"]["swapped_amount"] == "1000"
        assert status["state"]["staked"] is True
        assert status["state"]["staked_amount"] == "1500"
