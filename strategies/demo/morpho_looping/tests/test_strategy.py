"""Unit tests for Morpho Looping Strategy.

Tests verify the strategy's decision logic for leveraged yield farming.

To run:
    uv run pytest strategies/demo/morpho_looping/tests/ -v
"""

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from strategies.demo.morpho_looping import MorphoLoopingStrategy


@pytest.fixture
def mock_market() -> MagicMock:
    """Create a mock market snapshot."""
    market = MagicMock()

    def price_side_effect(token: str) -> Decimal:
        prices = {
            "wstETH": Decimal("3400"),
            "USDC": Decimal("1"),
            "ETH": Decimal("3400"),
        }
        return prices.get(token, Decimal("1"))

    market.price = MagicMock(side_effect=price_side_effect)

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

    with patch.object(MorphoLoopingStrategy, "__init__", lambda self, *args, **kwargs: None):
        strategy = MorphoLoopingStrategy.__new__(MorphoLoopingStrategy)

    strategy.config = default_config
    strategy._chain = "ethereum"
    strategy._wallet_address = "0x1234567890123456789012345678901234567890"
    strategy._deployment_id = "test-morpho-looping"

    strategy.market_id = default_config["market_id"]
    strategy.collateral_token = default_config["collateral_token"]
    strategy.borrow_token = default_config["borrow_token"]
    strategy.initial_collateral = Decimal(str(default_config["initial_collateral"]))
    strategy.target_loops = int(default_config["target_loops"])
    strategy.target_ltv = Decimal(str(default_config["target_ltv"]))
    strategy.min_health_factor = Decimal(str(default_config["min_health_factor"]))
    strategy.swap_slippage = Decimal(str(default_config["swap_slippage"]))
    strategy.force_action = str(default_config.get("force_action", "")).lower()

    strategy._loop_state = "idle"
    strategy._previous_stable_state = "idle"
    strategy._current_loop = 0
    strategy._loops_completed = 0
    strategy._total_collateral = Decimal("0")
    strategy._total_borrowed = Decimal("0")
    strategy._pending_swap_amount = Decimal("0")
    strategy._pending_wallet_collateral = Decimal("0")
    strategy._current_health_factor = Decimal("0")

    return strategy


@pytest.fixture
def strategy() -> MorphoLoopingStrategy:
    """Create a strategy instance."""
    return create_strategy()


class TestStrategyInit:
    """Tests for strategy initialization."""

    def test_init_with_default_config(self) -> None:
        strategy = create_strategy()

        assert strategy.market_id == "0xb323495f7e4148be5643a4ea4a8221eef163e4bccfdedc2a6f4696baacbc86cc"
        assert strategy.collateral_token == "wstETH"
        assert strategy.borrow_token == "USDC"
        assert strategy.initial_collateral == Decimal("1.0")
        assert strategy.target_loops == 3
        assert strategy.target_ltv == Decimal("0.75")
        assert strategy.min_health_factor == Decimal("1.5")

    def test_init_state(self, strategy: MorphoLoopingStrategy) -> None:
        assert strategy._loop_state == "idle"
        assert strategy._current_loop == 0
        assert strategy._loops_completed == 0
        assert strategy._total_collateral == Decimal("0")
        assert strategy._total_borrowed == Decimal("0")
        assert strategy._pending_wallet_collateral == Decimal("0")

    def test_custom_config(self) -> None:
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


class TestDecide:
    """Tests for the decide method."""

    def test_decide_idle_state_supplies(self, strategy: MorphoLoopingStrategy, mock_market: MagicMock) -> None:
        intent = strategy.decide(mock_market)

        assert intent is not None
        assert intent.intent_type.value == "SUPPLY"
        assert strategy._loop_state == "supplying"

    def test_decide_insufficient_balance_holds(self, strategy: MorphoLoopingStrategy, mock_market: MagicMock) -> None:
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
        strategy._loop_state = "supplied"
        strategy._total_collateral = Decimal("1.0")

        intent = strategy.decide(mock_market)

        assert intent is not None
        assert intent.intent_type.value == "BORROW"
        assert strategy._loop_state == "borrowing"

    def test_decide_borrowed_state_swaps(self, strategy: MorphoLoopingStrategy, mock_market: MagicMock) -> None:
        strategy._loop_state = "borrowed"
        strategy._pending_swap_amount = Decimal("1000")

        intent = strategy.decide(mock_market)

        assert intent is not None
        assert intent.intent_type.value == "SWAP"
        assert strategy._loop_state == "swapping"

    def test_decide_complete_state_holds(self, strategy: MorphoLoopingStrategy, mock_market: MagicMock) -> None:
        strategy._loop_state = "complete"
        strategy._total_collateral = Decimal("3.0")
        strategy._total_borrowed = Decimal("5000")

        intent = strategy.decide(mock_market)

        assert intent is not None
        assert intent.intent_type.value == "HOLD"
        assert "Position active" in str(intent.reason)

    def test_force_action_supply(self, strategy: MorphoLoopingStrategy, mock_market: MagicMock) -> None:
        strategy.force_action = "supply"
        intent = strategy.decide(mock_market)

        assert intent is not None
        assert intent.intent_type.value == "SUPPLY"

    def test_force_action_borrow(self, strategy: MorphoLoopingStrategy, mock_market: MagicMock) -> None:
        strategy.force_action = "borrow"
        strategy._total_collateral = Decimal("1.0")
        intent = strategy.decide(mock_market)

        assert intent is not None
        assert intent.intent_type.value == "BORROW"


class TestStateMachine:
    """Tests for the state machine transitions."""

    def test_swapped_state_continues_loop(self, strategy: MorphoLoopingStrategy, mock_market: MagicMock) -> None:
        strategy._current_loop = 1
        strategy._loops_completed = 1
        strategy._loop_state = "swapped"
        strategy._pending_swap_amount = Decimal("0.5")

        intent = strategy._handle_swapped_state(mock_market)

        assert strategy._loop_state == "supplying"
        assert intent.intent_type.value == "SUPPLY"

    def test_swapped_state_completes_when_done(self, strategy: MorphoLoopingStrategy, mock_market: MagicMock) -> None:
        strategy._current_loop = strategy.target_loops
        strategy._loops_completed = strategy.target_loops
        strategy._loop_state = "swapped"

        strategy._handle_swapped_state(mock_market)

        assert strategy._loop_state == "complete"
        assert strategy._loops_completed == strategy.target_loops

    def test_borrow_calculation(self, strategy: MorphoLoopingStrategy) -> None:
        strategy._total_collateral = Decimal("1.0")
        strategy._total_borrowed = Decimal("0")

        collateral_price = Decimal("3400")
        borrow_price = Decimal("1")

        intent = strategy._create_borrow_intent(collateral_price, borrow_price)

        assert intent.intent_type.value == "BORROW"
        assert strategy._pending_swap_amount == Decimal("2550.00")


class TestStatus:
    """Tests for status reporting."""

    def test_get_status(self, strategy: MorphoLoopingStrategy) -> None:
        status = strategy.get_status()

        assert status["strategy"] == "demo_morpho_looping"
        assert status["chain"] == "ethereum"
        assert status["state"]["loop_state"] == "idle"
        assert status["state"]["pending_wallet_collateral"] == "0"

    def test_get_persistent_state(self, strategy: MorphoLoopingStrategy) -> None:
        strategy._loop_state = "borrowed"
        strategy._current_loop = 2
        strategy._total_collateral = Decimal("2.5")
        strategy._total_borrowed = Decimal("4000")
        strategy._pending_wallet_collateral = Decimal("0.75")

        state = strategy.get_persistent_state()

        assert state["loop_state"] == "borrowed"
        assert state["current_loop"] == 2
        assert state["total_collateral"] == "2.5"
        assert state["total_borrowed"] == "4000"
        assert state["pending_wallet_collateral"] == "0.75"

    def test_load_persistent_state(self, strategy: MorphoLoopingStrategy) -> None:
        state = {
            "loop_state": "complete",
            "current_loop": 3,
            "loops_completed": 3,
            "total_collateral": "3.0",
            "total_borrowed": "6000",
            "pending_wallet_collateral": "0.25",
        }

        strategy.load_persistent_state(state)

        assert strategy._loop_state == "complete"
        assert strategy._current_loop == 3
        assert strategy._total_collateral == Decimal("3.0")
        assert strategy._total_borrowed == Decimal("6000")
        assert strategy._pending_wallet_collateral == Decimal("0.25")


class TestTeardown:
    """Tests for teardown functionality."""

    def test_generate_teardown_intents(self, strategy: MorphoLoopingStrategy) -> None:
        from almanak.framework.teardown import TeardownMode

        strategy._total_collateral = Decimal("3.0")
        strategy._total_borrowed = Decimal("5000")

        market = MagicMock()
        market.price.side_effect = lambda token: {
            "USDC": Decimal("1"),
            "wstETH": Decimal("3400"),
        }[token]

        intents = strategy.generate_teardown_intents(TeardownMode.SOFT, market=market)

        assert [intent.intent_type.value for intent in intents] == [
            "WITHDRAW",
            "SWAP",
            "REPAY",
            "WITHDRAW",
            "SWAP",
        ]

    def test_generate_teardown_intents_wallet_collateral_only(self, strategy: MorphoLoopingStrategy) -> None:
        from almanak.framework.teardown import TeardownMode

        strategy._pending_wallet_collateral = Decimal("1.25")

        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)

        assert len(intents) == 1
        assert intents[0].intent_type.value == "SWAP"

    def test_generate_teardown_intents_no_position(self, strategy: MorphoLoopingStrategy) -> None:
        from almanak.framework.teardown import TeardownMode

        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)

        assert len(intents) == 0

    def test_get_open_positions(self, strategy: MorphoLoopingStrategy) -> None:
        strategy._total_collateral = Decimal("3.0")
        strategy._total_borrowed = Decimal("5000")

        positions = strategy.get_open_positions()

        assert len(positions.positions) == 2
        position_types = [p.position_type.value for p in positions.positions]
        assert "SUPPLY" in position_types
        assert "BORROW" in position_types

    def test_get_open_positions_empty(self, strategy: MorphoLoopingStrategy) -> None:
        positions = strategy.get_open_positions()

        assert len(positions.positions) == 0


class TestOnIntentExecuted:
    """Regression tests for on_intent_executed state updates."""

    def test_supply_updates_total_collateral(self, strategy: MorphoLoopingStrategy) -> None:
        strategy._loop_state = "supplying"
        strategy._previous_stable_state = "idle"

        mock_intent = MagicMock()
        mock_intent.intent_type.value = "SUPPLY"
        mock_intent.amount = Decimal("1.5")

        strategy.on_intent_executed(mock_intent, success=True, result=None)

        assert strategy._total_collateral == Decimal("1.5")
        assert strategy._loop_state == "supplied"

    def test_supply_collateral_also_updates_total_collateral(self, strategy: MorphoLoopingStrategy) -> None:
        strategy._loop_state = "supplying"
        strategy._previous_stable_state = "idle"

        mock_intent = MagicMock()
        mock_intent.intent_type.value = "SUPPLY_COLLATERAL"
        mock_intent.amount = Decimal("2.0")

        strategy.on_intent_executed(mock_intent, success=True, result=None)

        assert strategy._total_collateral == Decimal("2.0")
        assert strategy._loop_state == "supplied"

    def test_supply_then_teardown_reports_open_position(self, strategy: MorphoLoopingStrategy) -> None:
        mock_intent = MagicMock()
        mock_intent.intent_type.value = "SUPPLY"
        mock_intent.amount = Decimal("1.0")

        strategy.on_intent_executed(mock_intent, success=True, result=None)

        positions = strategy.get_open_positions()
        assert len(positions.positions) == 1
        assert positions.positions[0].position_type.value == "SUPPLY"

        from almanak.framework.teardown import TeardownMode

        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        intent_types = [i.intent_type.value for i in intents]
        assert "WITHDRAW" in intent_types

    def test_failed_supply_does_not_update_total_collateral(self, strategy: MorphoLoopingStrategy) -> None:
        strategy._loop_state = "supplying"
        strategy._previous_stable_state = "idle"

        mock_intent = MagicMock()
        mock_intent.intent_type.value = "SUPPLY"
        mock_intent.amount = Decimal("1.5")

        strategy.on_intent_executed(mock_intent, success=False, result=None)

        assert strategy._total_collateral == Decimal("0")
        assert strategy._loop_state == "idle"

    def test_supply_survives_persistent_state_round_trip(self, strategy: MorphoLoopingStrategy) -> None:
        mock_intent = MagicMock()
        mock_intent.intent_type.value = "SUPPLY"
        mock_intent.amount = Decimal("1.0")
        strategy.on_intent_executed(mock_intent, success=True, result=None)

        fresh = create_strategy()
        fresh.load_persistent_state(strategy.get_persistent_state())

        assert fresh._total_collateral == Decimal("1.0")
        positions = fresh.get_open_positions()
        assert len(positions.positions) == 1
        assert positions.positions[0].position_type.value == "SUPPLY"

    def test_withdraw_all_moves_collateral_to_wallet_pending_swap(self, strategy: MorphoLoopingStrategy) -> None:
        strategy._total_collateral = Decimal("1.5")

        mock_intent = MagicMock()
        mock_intent.intent_type.value = "WITHDRAW"
        mock_intent.withdraw_all = True
        mock_intent.amount = Decimal("1.5")

        strategy.on_intent_executed(mock_intent, success=True, result=None)

        assert strategy._total_collateral == Decimal("0")
        assert strategy._pending_wallet_collateral == Decimal("1.5")

    def test_withdraw_partial_decrements_total_collateral(self, strategy: MorphoLoopingStrategy) -> None:
        strategy._total_collateral = Decimal("2.0")

        mock_intent = MagicMock()
        mock_intent.intent_type.value = "WITHDRAW"
        mock_intent.withdraw_all = False
        mock_intent.amount = Decimal("0.5")

        strategy.on_intent_executed(mock_intent, success=True, result=None)

        assert strategy._total_collateral == Decimal("1.5")
        assert strategy._pending_wallet_collateral == Decimal("0.5")

    def test_withdraw_collateral_forward_compat(self, strategy: MorphoLoopingStrategy) -> None:
        strategy._total_collateral = Decimal("3.0")

        mock_intent = MagicMock()
        mock_intent.intent_type.value = "WITHDRAW_COLLATERAL"
        mock_intent.withdraw_all = True
        mock_intent.amount = Decimal("3.0")

        strategy.on_intent_executed(mock_intent, success=True, result=None)

        assert strategy._total_collateral == Decimal("0")
        assert strategy._pending_wallet_collateral == Decimal("3.0")

    def test_withdraw_never_goes_negative(self, strategy: MorphoLoopingStrategy) -> None:
        strategy._total_collateral = Decimal("1.0")

        mock_intent = MagicMock()
        mock_intent.intent_type.value = "WITHDRAW"
        mock_intent.withdraw_all = False
        mock_intent.amount = Decimal("5.0")

        strategy.on_intent_executed(mock_intent, success=True, result=None)
        assert strategy._total_collateral == Decimal("0")
        assert strategy._pending_wallet_collateral == Decimal("1.0")

        strategy.on_intent_executed(mock_intent, success=True, result=None)
        assert strategy._total_collateral == Decimal("0")
        assert strategy._pending_wallet_collateral == Decimal("1.0")

    def test_repay_full_zeroes_total_borrowed(self, strategy: MorphoLoopingStrategy) -> None:
        strategy._total_borrowed = Decimal("2500")

        mock_intent = MagicMock()
        mock_intent.intent_type.value = "REPAY"
        mock_intent.repay_full = True
        mock_intent.amount = Decimal("0")

        strategy.on_intent_executed(mock_intent, success=True, result=None)

        assert strategy._total_borrowed == Decimal("0")

    def test_repay_partial_decrements_total_borrowed(self, strategy: MorphoLoopingStrategy) -> None:
        strategy._total_borrowed = Decimal("1000")

        mock_intent = MagicMock()
        mock_intent.intent_type.value = "REPAY"
        mock_intent.repay_full = False
        mock_intent.amount = Decimal("400")

        strategy.on_intent_executed(mock_intent, success=True, result=None)

        assert strategy._total_borrowed == Decimal("600")

    def test_repay_never_goes_negative(self, strategy: MorphoLoopingStrategy) -> None:
        strategy._total_borrowed = Decimal("100")

        mock_intent = MagicMock()
        mock_intent.intent_type.value = "REPAY"
        mock_intent.repay_full = False
        mock_intent.amount = Decimal("500")

        strategy.on_intent_executed(mock_intent, success=True, result=None)
        assert strategy._total_borrowed == Decimal("0")

    def test_restart_after_withdraw_still_schedules_final_swap(self, strategy: MorphoLoopingStrategy) -> None:
        strategy._total_collateral = Decimal("2.0")

        withdraw = MagicMock()
        withdraw.intent_type.value = "WITHDRAW"
        withdraw.withdraw_all = True
        withdraw.amount = Decimal("2.0")
        strategy.on_intent_executed(withdraw, success=True, result=None)

        fresh = create_strategy()
        fresh.load_persistent_state(strategy.get_persistent_state())

        from almanak.framework.teardown import TeardownMode

        intents = fresh.generate_teardown_intents(TeardownMode.SOFT)
        assert [intent.intent_type.value for intent in intents] == ["SWAP"]

    def test_swap_to_usdc_clears_pending_wallet_collateral(self, strategy: MorphoLoopingStrategy) -> None:
        strategy._pending_wallet_collateral = Decimal("2.0")

        swap = MagicMock()
        swap.intent_type.value = "SWAP"
        swap.from_token = "wstETH"
        swap.to_token = "USDC"
        swap.amount = "all"

        strategy.on_intent_executed(swap, success=True, result=None)

        assert strategy._pending_wallet_collateral == Decimal("0")

    def test_loop_swap_persists_realized_collateral_output(self, strategy: MorphoLoopingStrategy) -> None:
        strategy._pending_swap_amount = Decimal("2550")

        swap = MagicMock()
        swap.intent_type.value = "SWAP"
        swap.from_token = "USDC"
        swap.to_token = "wstETH"
        swap.amount = Decimal("2550")

        result = SimpleNamespace(
            swap_amounts=SimpleNamespace(amount_out_decimal=Decimal("0.73")),
        )

        strategy.on_intent_executed(swap, success=True, result=result)

        assert strategy._pending_swap_amount == Decimal("0.73")
        assert strategy._loop_state == "swapped"
        assert strategy._current_loop == 1
        assert strategy._loops_completed == 1

    def test_loop_swap_without_realized_output_fails_closed(self, strategy: MorphoLoopingStrategy) -> None:
        strategy._pending_swap_amount = Decimal("2550")

        swap = MagicMock()
        swap.intent_type.value = "SWAP"
        swap.from_token = "USDC"
        swap.to_token = "wstETH"
        swap.amount = Decimal("2550")

        strategy.on_intent_executed(swap, success=True, result=None)

        assert strategy._pending_swap_amount == Decimal("0")

    def test_full_lifecycle_round_trip(self, strategy: MorphoLoopingStrategy) -> None:
        strategy._previous_stable_state = "idle"

        supply = MagicMock()
        supply.intent_type.value = "SUPPLY"
        supply.amount = Decimal("2.0")
        strategy.on_intent_executed(supply, success=True, result=None)
        assert strategy._total_collateral == Decimal("2.0")

        borrow = MagicMock()
        borrow.intent_type.value = "BORROW"
        borrow.borrow_amount = Decimal("3000")
        strategy.on_intent_executed(borrow, success=True, result=None)
        assert strategy._total_borrowed == Decimal("3000")

        fresh = create_strategy()
        fresh.load_persistent_state(strategy.get_persistent_state())
        assert fresh._total_collateral == Decimal("2.0")
        assert fresh._total_borrowed == Decimal("3000")

        repay = MagicMock()
        repay.intent_type.value = "REPAY"
        repay.repay_full = True
        repay.amount = Decimal("0")
        strategy.on_intent_executed(repay, success=True, result=None)
        assert strategy._total_borrowed == Decimal("0")

        withdraw = MagicMock()
        withdraw.intent_type.value = "WITHDRAW"
        withdraw.withdraw_all = True
        withdraw.amount = Decimal("2.0")
        strategy.on_intent_executed(withdraw, success=True, result=None)
        assert strategy._total_collateral == Decimal("0")
        assert strategy._pending_wallet_collateral == Decimal("2.0")

        positions = strategy.get_open_positions()
        assert len(positions.positions) == 0

        from almanak.framework.teardown import TeardownMode

        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert [intent.intent_type.value for intent in intents] == ["SWAP"]

        swap = MagicMock()
        swap.intent_type.value = "SWAP"
        swap.from_token = "wstETH"
        swap.to_token = "USDC"
        swap.amount = "all"
        strategy.on_intent_executed(swap, success=True, result=None)

        assert strategy._pending_wallet_collateral == Decimal("0")
        assert strategy.generate_teardown_intents(TeardownMode.SOFT) == []
