"""Unit tests for Morpho Looping Strategy.

Tests verify the strategy's decision logic for leveraged yield farming.

To run:
    uv run pytest strategies/demo/morpho_looping/tests/ -v
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from strategies.demo.morpho_looping import MorphoLoopingStrategy

# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def mock_market() -> MagicMock:
    """Create a mock market snapshot."""
    market = MagicMock()

    # Mock prices
    def price_side_effect(token: str) -> Decimal:
        prices = {
            "wstETH": Decimal("3400"),
            "USDC": Decimal("1"),
            "ETH": Decimal("3400"),
        }
        return prices.get(token, Decimal("1"))

    market.price = MagicMock(side_effect=price_side_effect)

    # Mock balances
    def balance_side_effect(token: str) -> MagicMock:
        balance_obj = MagicMock()
        if token == "wstETH":
            balance_obj.balance = Decimal("10.0")
        elif token == "USDC":
            balance_obj.balance = Decimal("10000")
        else:
            balance_obj.balance = Decimal("0")
        return balance_obj

    market.balance = MagicMock(side_effect=balance_side_effect)

    return market


def create_strategy(config: dict | None = None) -> MorphoLoopingStrategy:
    """Create a MorphoLoopingStrategy with test configuration."""
    default_config = {
        "market_id": "0xb323495f7e4148be5643a4ea4a8221eef163e4bccfdedc2a6f4696baacbc86cc",
        "collateral_token": "wstETH",
        "borrow_token": "USDC",
        "initial_collateral": "1.0",
        "target_loops": 3,
        "target_ltv": "0.75",
        "min_health_factor": "1.5",
        "swap_slippage": "0.005",
        "force_action": "",
    }
    if config:
        default_config.update(config)

    # Mock the base class initialization
    with patch.object(MorphoLoopingStrategy, "__init__", lambda self, *args, **kwargs: None):
        strategy = MorphoLoopingStrategy.__new__(MorphoLoopingStrategy)

    # Set required attributes manually
    strategy.config = default_config
    strategy._chain = "ethereum"
    strategy._wallet_address = "0x1234567890123456789012345678901234567890"
    strategy._strategy_id = "test-morpho-looping"

    # Initialize strategy-specific attributes
    strategy.market_id = default_config["market_id"]
    strategy.collateral_token = default_config["collateral_token"]
    strategy.borrow_token = default_config["borrow_token"]
    strategy.initial_collateral = Decimal(str(default_config["initial_collateral"]))
    strategy.target_loops = int(default_config["target_loops"])
    strategy.target_ltv = Decimal(str(default_config["target_ltv"]))
    strategy.min_health_factor = Decimal(str(default_config["min_health_factor"]))
    strategy.swap_slippage = Decimal(str(default_config["swap_slippage"]))
    strategy.force_action = str(default_config.get("force_action", "")).lower()

    # Initialize state tracking
    strategy._loop_state = "idle"
    strategy._current_loop = 0
    strategy._loops_completed = 0
    strategy._total_collateral = Decimal("0")
    strategy._total_borrowed = Decimal("0")
    strategy._pending_swap_amount = Decimal("0")
    strategy._current_health_factor = Decimal("0")

    return strategy


@pytest.fixture
def strategy() -> MorphoLoopingStrategy:
    """Create a strategy instance."""
    return create_strategy()


# =============================================================================
# Initialization Tests
# =============================================================================


class TestStrategyInit:
    """Tests for strategy initialization."""

    def test_init_with_default_config(self) -> None:
        """Test initialization with default configuration."""
        strategy = create_strategy()

        assert strategy.market_id == "0xb323495f7e4148be5643a4ea4a8221eef163e4bccfdedc2a6f4696baacbc86cc"
        assert strategy.collateral_token == "wstETH"
        assert strategy.borrow_token == "USDC"
        assert strategy.initial_collateral == Decimal("1.0")
        assert strategy.target_loops == 3
        assert strategy.target_ltv == Decimal("0.75")
        assert strategy.min_health_factor == Decimal("1.5")

    def test_init_state(self, strategy: MorphoLoopingStrategy) -> None:
        """Test initial state values."""
        assert strategy._loop_state == "idle"
        assert strategy._current_loop == 0
        assert strategy._loops_completed == 0
        assert strategy._total_collateral == Decimal("0")
        assert strategy._total_borrowed == Decimal("0")

    def test_custom_config(self) -> None:
        """Test initialization with custom configuration."""
        strategy = create_strategy(
            {
                "target_loops": 5,
                "target_ltv": "0.80",
                "initial_collateral": "2.0",
            }
        )

        assert strategy.target_loops == 5
        assert strategy.target_ltv == Decimal("0.80")
        assert strategy.initial_collateral == Decimal("2.0")


# =============================================================================
# Decision Tests
# =============================================================================


class TestDecide:
    """Tests for the decide method."""

    def test_decide_idle_state_supplies(self, strategy: MorphoLoopingStrategy, mock_market: MagicMock) -> None:
        """Test that idle state triggers supply."""
        intent = strategy.decide(mock_market)

        assert intent is not None
        assert intent.intent_type.value == "SUPPLY"
        assert strategy._loop_state == "supplying"

    def test_decide_insufficient_balance_holds(self, strategy: MorphoLoopingStrategy, mock_market: MagicMock) -> None:
        """Test that insufficient balance causes hold."""

        # Set low balance
        def low_balance(token: str) -> MagicMock:
            balance_obj = MagicMock()
            balance_obj.balance = Decimal("0.001")
            return balance_obj

        mock_market.balance = MagicMock(side_effect=low_balance)

        intent = strategy.decide(mock_market)

        assert intent is not None
        assert intent.intent_type.value == "HOLD"
        assert "Insufficient" in str(intent.reason)

    def test_decide_supplied_state_borrows(self, strategy: MorphoLoopingStrategy, mock_market: MagicMock) -> None:
        """Test that supplied state triggers borrow."""
        # Set up state
        strategy._loop_state = "supplied"
        strategy._total_collateral = Decimal("1.0")

        intent = strategy.decide(mock_market)

        assert intent is not None
        assert intent.intent_type.value == "BORROW"
        assert strategy._loop_state == "borrowing"

    def test_decide_borrowed_state_swaps(self, strategy: MorphoLoopingStrategy, mock_market: MagicMock) -> None:
        """Test that borrowed state triggers swap."""
        # Set up state
        strategy._loop_state = "borrowed"
        strategy._pending_swap_amount = Decimal("1000")

        intent = strategy.decide(mock_market)

        assert intent is not None
        assert intent.intent_type.value == "SWAP"
        assert strategy._loop_state == "swapping"

    def test_decide_complete_state_holds(self, strategy: MorphoLoopingStrategy, mock_market: MagicMock) -> None:
        """Test that complete state holds."""
        # Set up complete state
        strategy._loop_state = "complete"
        strategy._total_collateral = Decimal("3.0")
        strategy._total_borrowed = Decimal("5000")

        intent = strategy.decide(mock_market)

        assert intent is not None
        assert intent.intent_type.value == "HOLD"
        assert "Position active" in str(intent.reason)

    def test_force_action_supply(self, strategy: MorphoLoopingStrategy, mock_market: MagicMock) -> None:
        """Test forced supply action."""
        strategy.force_action = "supply"
        intent = strategy.decide(mock_market)

        assert intent is not None
        assert intent.intent_type.value == "SUPPLY"

    def test_force_action_borrow(self, strategy: MorphoLoopingStrategy, mock_market: MagicMock) -> None:
        """Test forced borrow action."""
        strategy.force_action = "borrow"
        strategy._total_collateral = Decimal("1.0")  # Need collateral for borrow calc
        intent = strategy.decide(mock_market)

        assert intent is not None
        assert intent.intent_type.value == "BORROW"


# =============================================================================
# State Machine Tests
# =============================================================================


class TestStateMachine:
    """Tests for the state machine transitions."""

    def test_swapped_state_continues_loop(self, strategy: MorphoLoopingStrategy, mock_market: MagicMock) -> None:
        """Test that swapped state continues to next loop if not complete."""
        strategy._current_loop = 0
        strategy._loop_state = "swapped"
        strategy._pending_swap_amount = Decimal("0.5")

        intent = strategy._handle_swapped_state(mock_market)

        # Should continue to next supply
        assert strategy._loop_state == "supplying"
        assert strategy._loops_completed == 1
        assert intent.intent_type.value == "SUPPLY"

    def test_swapped_state_completes_when_done(self, strategy: MorphoLoopingStrategy, mock_market: MagicMock) -> None:
        """Test that swapped state completes when all loops done."""
        strategy._current_loop = strategy.target_loops - 1
        strategy._loops_completed = strategy.target_loops - 1
        strategy._loop_state = "swapped"

        strategy._handle_swapped_state(mock_market)

        assert strategy._loop_state == "complete"
        assert strategy._loops_completed == strategy.target_loops

    def test_borrow_calculation(self, strategy: MorphoLoopingStrategy) -> None:
        """Test borrow amount calculation."""
        strategy._total_collateral = Decimal("1.0")
        strategy._total_borrowed = Decimal("0")

        collateral_price = Decimal("3400")
        borrow_price = Decimal("1")

        # Collateral value = 1.0 * 3400 = 3400
        # Max borrow at 75% LTV = 3400 * 0.75 = 2550
        intent = strategy._create_borrow_intent(collateral_price, borrow_price)

        assert intent.intent_type.value == "BORROW"
        # Borrow amount should be around 2550 USDC
        assert strategy._pending_swap_amount == Decimal("2550.00")


# =============================================================================
# Status Tests
# =============================================================================


class TestStatus:
    """Tests for status reporting."""

    def test_get_status(self, strategy: MorphoLoopingStrategy) -> None:
        """Test get_status returns correct data."""
        status = strategy.get_status()

        assert status["strategy"] == "demo_morpho_looping"
        assert status["chain"] == "ethereum"
        assert "config" in status
        assert "state" in status
        assert status["state"]["loop_state"] == "idle"

    def test_get_persistent_state(self, strategy: MorphoLoopingStrategy) -> None:
        """Test persistent state serialization."""
        strategy._loop_state = "borrowed"
        strategy._current_loop = 2
        strategy._total_collateral = Decimal("2.5")
        strategy._total_borrowed = Decimal("4000")

        state = strategy.get_persistent_state()

        assert state["loop_state"] == "borrowed"
        assert state["current_loop"] == 2
        assert state["total_collateral"] == "2.5"
        assert state["total_borrowed"] == "4000"

    def test_load_persistent_state(self, strategy: MorphoLoopingStrategy) -> None:
        """Test persistent state loading."""
        state = {
            "loop_state": "complete",
            "current_loop": 3,
            "loops_completed": 3,
            "total_collateral": "3.0",
            "total_borrowed": "6000",
        }

        strategy.load_persistent_state(state)

        assert strategy._loop_state == "complete"
        assert strategy._current_loop == 3
        assert strategy._total_collateral == Decimal("3.0")
        assert strategy._total_borrowed == Decimal("6000")


# =============================================================================
# Teardown Tests
# =============================================================================


class TestTeardown:
    """Tests for teardown functionality."""

    def test_generate_teardown_intents(self, strategy: MorphoLoopingStrategy) -> None:
        """Test teardown intent generation."""
        from almanak.framework.teardown import TeardownMode

        # Set up a position
        strategy._total_collateral = Decimal("3.0")
        strategy._total_borrowed = Decimal("5000")

        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)

        # Should have: repay, withdraw_collateral, swap
        assert len(intents) == 3
        assert intents[0].intent_type.value == "REPAY"
        assert intents[1].intent_type.value == "WITHDRAW"
        assert intents[2].intent_type.value == "SWAP"

    def test_generate_teardown_intents_no_position(self, strategy: MorphoLoopingStrategy) -> None:
        """Test teardown with no position."""
        from almanak.framework.teardown import TeardownMode

        strategy._total_collateral = Decimal("0")
        strategy._total_borrowed = Decimal("0")

        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)

        # No intents needed
        assert len(intents) == 0

    def test_get_open_positions(self, strategy: MorphoLoopingStrategy) -> None:
        """Test getting open positions."""
        strategy._total_collateral = Decimal("3.0")
        strategy._total_borrowed = Decimal("5000")

        positions = strategy.get_open_positions()

        assert len(positions.positions) == 2
        # Should have both supply and borrow positions
        position_types = [p.position_type.value for p in positions.positions]
        assert "SUPPLY" in position_types
        assert "BORROW" in position_types

    def test_get_open_positions_empty(self, strategy: MorphoLoopingStrategy) -> None:
        """Test getting open positions when empty."""
        strategy._total_collateral = Decimal("0")
        strategy._total_borrowed = Decimal("0")

        positions = strategy.get_open_positions()

        assert len(positions.positions) == 0
