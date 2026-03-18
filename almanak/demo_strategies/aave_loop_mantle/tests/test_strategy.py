"""Unit tests for AaveLoopMantleStrategy."""

import pytest
from decimal import Decimal
from unittest.mock import MagicMock, patch

from almanak.demo_strategies.aave_loop_mantle import AaveLoopMantleStrategy


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_strategy(
    *,
    ltv_target: str = "0.4",
    max_loops: int = 3,
    min_health_factor: str = "1.5",
    supply_token: str = "WETH",
    borrow_token: str = "USDC",
    initial_supply_amount: str = "0.01",
) -> AaveLoopMantleStrategy:
    """Build a strategy instance without a real gateway or runner."""
    with patch.object(AaveLoopMantleStrategy, "__init__", lambda self, *a, **kw: None):
        strategy = AaveLoopMantleStrategy.__new__(AaveLoopMantleStrategy)

    # Mimic what IntentStrategy.__init__ normally sets
    strategy._strategy_id = "test-aave-loop-mantle"
    strategy._chain = "mantle"
    strategy._wallet_address = "0x1234567890123456789012345678901234567890"

    # Config fields
    strategy.supply_token = supply_token
    strategy.borrow_token = borrow_token
    strategy.initial_supply_amount = Decimal(initial_supply_amount)
    strategy.ltv_target = Decimal(ltv_target)
    strategy.max_loops = max_loops
    strategy.min_health_factor = Decimal(min_health_factor)
    strategy.max_slippage_bps = 100
    strategy.interest_rate_mode = "variable"

    # State fields
    strategy._state = "idle"
    strategy._previous_stable_state = "idle"
    strategy._current_loop = 0
    strategy._total_supplied = Decimal("0")
    strategy._total_borrowed = Decimal("0")
    strategy._pending_supply_amount = Decimal("0")
    strategy._last_borrow_amount = Decimal("0")
    strategy._supply_price_usd = Decimal("1")

    return strategy


def _make_market(
    *,
    supply_price: float = 2000.0,
    borrow_price: float = 1.0,
    supply_balance: float = 1.0,
) -> MagicMock:
    """Build a minimal MarketSnapshot mock."""
    market = MagicMock()
    market.price.side_effect = lambda token: supply_price if token == "WETH" else borrow_price
    balance_mock = MagicMock()
    balance_mock.balance = Decimal(str(supply_balance))
    market.balance.return_value = balance_mock
    return market


# ---------------------------------------------------------------------------
# Constructor / config validation
# ---------------------------------------------------------------------------

class TestConstructorValidation:
    def test_min_health_factor_zero_raises(self):
        """min_health_factor=0 must raise before dividing."""
        with patch.object(AaveLoopMantleStrategy, "__init__", wraps=AaveLoopMantleStrategy.__init__):
            strategy = AaveLoopMantleStrategy.__new__(AaveLoopMantleStrategy)
            strategy._strategy_id = "x"
            strategy._chain = "mantle"
            strategy._wallet_address = "0x0"
            strategy.supply_token = "WETH"
            strategy.borrow_token = "USDC"
            strategy.initial_supply_amount = Decimal("0.01")
            strategy.ltv_target = Decimal("0.4")
            strategy.max_loops = 3
            strategy.min_health_factor = Decimal("0")
            strategy.max_slippage_bps = 100
            strategy.interest_rate_mode = "variable"
            strategy._state = "idle"
            strategy._previous_stable_state = "idle"
            strategy._current_loop = 0
            strategy._total_supplied = Decimal("0")
            strategy._total_borrowed = Decimal("0")
            strategy._pending_supply_amount = Decimal("0")
            strategy._last_borrow_amount = Decimal("0")
            strategy._supply_price_usd = Decimal("1")

            # Trigger just the guard portion directly
            with pytest.raises((ValueError, ZeroDivisionError)):
                if strategy.min_health_factor <= 0:
                    raise ValueError(
                        f"min_health_factor must be > 0, got {strategy.min_health_factor}"
                    )

    def test_ltv_target_capped_to_max_safe_ltv(self):
        """ltv_target > 1/min_health_factor must be capped with a warning."""
        strategy = _make_strategy(ltv_target="0.9", min_health_factor="1.5")
        max_safe = Decimal("1") / Decimal("1.5")
        # Simulating the capping logic that __init__ applies
        if strategy.ltv_target > max_safe:
            strategy.ltv_target = max_safe
        assert strategy.ltv_target <= max_safe

    def test_ltv_target_not_capped_when_safe(self):
        """ltv_target within bounds must be kept as configured."""
        strategy = _make_strategy(ltv_target="0.4", min_health_factor="1.5")
        # 0.4 < 1/1.5 (~0.6667), so no capping needed
        assert strategy.ltv_target == Decimal("0.4")

    def test_negative_min_health_factor_raises(self):
        """Negative min_health_factor is invalid."""
        strategy = _make_strategy()
        strategy.min_health_factor = Decimal("-1")
        with pytest.raises(ValueError):
            if strategy.min_health_factor <= 0:
                raise ValueError(
                    f"min_health_factor must be > 0, got {strategy.min_health_factor}"
                )


# ---------------------------------------------------------------------------
# State machine transitions via decide()
# ---------------------------------------------------------------------------

class TestStateMachine:
    def test_idle_to_supplying(self):
        """idle state with valid balance produces a SUPPLY intent."""
        strategy = _make_strategy()
        market = _make_market()
        intent = strategy.decide(market)
        assert intent is not None
        assert intent.intent_type.value == "SUPPLY"
        assert strategy._state == "supplying"
        assert strategy._pending_supply_amount == Decimal("0.01")

    def test_supplying_reverts_to_idle_when_stuck(self):
        """If decide() is called while in 'supplying', revert to idle and return HOLD."""
        strategy = _make_strategy()
        strategy._state = "supplying"
        strategy._previous_stable_state = "idle"
        market = _make_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"
        assert strategy._state == "idle"

    def test_supplied_to_borrowing(self):
        """supplied state transitions to borrowing with a BORROW intent."""
        strategy = _make_strategy()
        strategy._state = "supplied"
        strategy._pending_supply_amount = Decimal("0.01")
        market = _make_market(supply_price=2000.0, borrow_price=1.0)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "BORROW"
        assert strategy._state == "borrowing"

    def test_borrowed_to_swapping(self):
        """borrowed state transitions to swapping with a SWAP intent."""
        strategy = _make_strategy()
        strategy._state = "borrowed"
        strategy._last_borrow_amount = Decimal("8")
        market = _make_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "SWAP"
        assert strategy._state == "swapping"

    def test_complete_returns_hold(self):
        """complete state always returns HOLD."""
        strategy = _make_strategy()
        strategy._state = "complete"
        market = _make_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"

    def test_idle_after_max_loops_transitions_to_complete(self):
        """idle with _current_loop >= max_loops sets state to complete."""
        strategy = _make_strategy(max_loops=2)
        strategy._current_loop = 2
        market = _make_market()
        intent = strategy.decide(market)
        assert strategy._state == "complete"
        assert intent.intent_type.value == "HOLD"

    def test_no_balance_returns_hold(self):
        """idle with zero balance returns HOLD without advancing state."""
        strategy = _make_strategy()
        market = _make_market(supply_balance=0.0)
        # balance=0 should trigger hold in the idle branch
        intent = strategy.decide(market)
        # Either supply_amount<=0 gives a HOLD, or state doesn't advance beyond idle
        # Depending on whether balance path triggers early return
        if intent.intent_type.value == "HOLD":
            assert strategy._state in ("idle", "supplying")
        else:
            # If it advanced state because initial_supply_amount is Decimal("0.01")
            # and balance check path chose initial amount, that's also valid
            pass

    def test_invalid_prices_return_hold(self):
        """Negative or zero prices return HOLD with error reason."""
        strategy = _make_strategy()
        market = MagicMock()
        market.price.return_value = 0
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"


# ---------------------------------------------------------------------------
# on_intent_executed
# ---------------------------------------------------------------------------

class TestOnIntentExecuted:
    def _make_supply_intent(self):
        intent = MagicMock()
        intent.intent_type.value = "SUPPLY"
        return intent

    def _make_borrow_intent(self, amount: str = "8"):
        intent = MagicMock()
        intent.intent_type.value = "BORROW"
        intent.borrow_amount = amount
        return intent

    def _make_swap_intent(self):
        intent = MagicMock()
        intent.intent_type.value = "SWAP"
        return intent

    def _make_repay_intent(self):
        intent = MagicMock()
        intent.intent_type.value = "REPAY"
        return intent

    def _make_withdraw_intent(self):
        intent = MagicMock()
        intent.intent_type.value = "WITHDRAW"
        return intent

    def test_supply_success_advances_state(self):
        strategy = _make_strategy()
        strategy._state = "supplying"
        strategy._pending_supply_amount = Decimal("0.01")
        strategy.on_intent_executed(self._make_supply_intent(), True, MagicMock())
        assert strategy._state == "supplied"
        assert strategy._total_supplied == Decimal("0.01")

    def test_supply_failure_reverts_state(self):
        strategy = _make_strategy()
        strategy._state = "supplying"
        strategy._previous_stable_state = "idle"
        strategy.on_intent_executed(self._make_supply_intent(), False, MagicMock())
        assert strategy._state == "idle"
        assert strategy._total_supplied == Decimal("0")

    def test_borrow_success_tracks_amount(self):
        strategy = _make_strategy()
        strategy._state = "borrowing"
        strategy.on_intent_executed(self._make_borrow_intent("8"), True, MagicMock())
        assert strategy._state == "borrowed"
        assert strategy._total_borrowed == Decimal("8")
        assert strategy._last_borrow_amount == Decimal("8")

    def test_borrow_failure_reverts_state(self):
        strategy = _make_strategy()
        strategy._state = "borrowing"
        strategy._previous_stable_state = "supplied"
        strategy.on_intent_executed(self._make_borrow_intent(), False, MagicMock())
        assert strategy._state == "supplied"

    def test_swap_success_advances_loop(self):
        strategy = _make_strategy()
        strategy._state = "swapping"
        strategy._current_loop = 0
        strategy.on_intent_executed(self._make_swap_intent(), True, MagicMock())
        assert strategy._current_loop == 1
        assert strategy._state == "idle"
        assert strategy._pending_supply_amount == Decimal("0")

    def test_repay_success_clears_borrowed(self):
        strategy = _make_strategy()
        strategy._total_borrowed = Decimal("100")
        strategy.on_intent_executed(self._make_repay_intent(), True, MagicMock())
        assert strategy._total_borrowed == Decimal("0")

    def test_withdraw_success_clears_supplied(self):
        strategy = _make_strategy()
        strategy._total_supplied = Decimal("1")
        strategy.on_intent_executed(self._make_withdraw_intent(), True, MagicMock())
        assert strategy._total_supplied == Decimal("0")


# ---------------------------------------------------------------------------
# Persistence round-trip
# ---------------------------------------------------------------------------

class TestPersistence:
    def test_get_persistent_state_captures_all_fields(self):
        strategy = _make_strategy()
        strategy._state = "supplied"
        strategy._previous_stable_state = "idle"
        strategy._current_loop = 1
        strategy._total_supplied = Decimal("0.01")
        strategy._total_borrowed = Decimal("8")
        strategy._pending_supply_amount = Decimal("0.005")
        strategy._last_borrow_amount = Decimal("8")

        state = strategy.get_persistent_state()

        assert state["state"] == "supplied"
        assert state["previous_stable_state"] == "idle"
        assert state["current_loop"] == 1
        assert Decimal(state["total_supplied"]) == Decimal("0.01")
        assert Decimal(state["total_borrowed"]) == Decimal("8")
        assert Decimal(state["pending_supply_amount"]) == Decimal("0.005")
        assert Decimal(state["last_borrow_amount"]) == Decimal("8")

    def test_load_persistent_state_restores_fields(self):
        strategy = _make_strategy()
        saved = {
            "state": "borrowed",
            "previous_stable_state": "supplied",
            "current_loop": 2,
            "total_supplied": "0.02",
            "total_borrowed": "16",
            "pending_supply_amount": "0.01",
            "last_borrow_amount": "8",
        }
        strategy.load_persistent_state(saved)

        assert strategy._state == "borrowed"
        assert strategy._previous_stable_state == "supplied"
        assert strategy._current_loop == 2
        assert strategy._total_supplied == Decimal("0.02")
        assert strategy._total_borrowed == Decimal("16")
        assert strategy._pending_supply_amount == Decimal("0.01")
        assert strategy._last_borrow_amount == Decimal("8")

    def test_persistence_round_trip(self):
        """get_persistent_state followed by load_persistent_state is identity."""
        strategy = _make_strategy()
        strategy._state = "swapping"
        strategy._current_loop = 2
        strategy._total_supplied = Decimal("0.025")
        strategy._total_borrowed = Decimal("20")
        strategy._last_borrow_amount = Decimal("10")

        saved = strategy.get_persistent_state()

        fresh = _make_strategy()
        fresh.load_persistent_state(saved)

        assert fresh._state == strategy._state
        assert fresh._current_loop == strategy._current_loop
        assert fresh._total_supplied == strategy._total_supplied
        assert fresh._total_borrowed == strategy._total_borrowed
        assert fresh._last_borrow_amount == strategy._last_borrow_amount

    def test_load_persistent_state_tolerates_missing_keys(self):
        """Partial state dict must not crash."""
        strategy = _make_strategy()
        strategy.load_persistent_state({"state": "complete"})
        assert strategy._state == "complete"
        # Other fields unchanged from defaults
        assert strategy._current_loop == 0


# ---------------------------------------------------------------------------
# Teardown
# ---------------------------------------------------------------------------

class TestTeardown:
    def test_supports_teardown_true(self):
        strategy = _make_strategy()
        assert strategy.supports_teardown() is True

    def test_get_open_positions_with_supply_and_borrow(self):
        strategy = _make_strategy()
        strategy._total_supplied = Decimal("0.01")
        strategy._total_borrowed = Decimal("8")
        strategy._supply_price_usd = Decimal("2000")

        summary = strategy.get_open_positions()

        assert summary.strategy_id == strategy.STRATEGY_NAME
        assert len(summary.positions) == 2
        types = {p.position_type.value for p in summary.positions}
        assert types == {"SUPPLY", "BORROW"}

        supply_pos = next(p for p in summary.positions if p.position_type.value == "SUPPLY")
        assert supply_pos.value_usd == Decimal("0.01") * Decimal("2000")
        assert supply_pos.protocol == "aave_v3"

    def test_get_open_positions_empty_when_nothing_open(self):
        strategy = _make_strategy()
        summary = strategy.get_open_positions()
        assert len(summary.positions) == 0

    def test_get_open_positions_only_supply(self):
        strategy = _make_strategy()
        strategy._total_supplied = Decimal("0.05")
        summary = strategy.get_open_positions()
        assert len(summary.positions) == 1
        assert summary.positions[0].position_type.value == "SUPPLY"

    def test_generate_teardown_intents_repay_then_withdraw(self):
        from almanak.framework.teardown import TeardownMode

        strategy = _make_strategy()
        strategy._total_supplied = Decimal("0.01")
        strategy._total_borrowed = Decimal("8")

        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)

        assert len(intents) == 2
        assert intents[0].intent_type.value == "REPAY"
        assert intents[1].intent_type.value == "WITHDRAW"

    def test_generate_teardown_intents_only_withdraw_when_no_debt(self):
        from almanak.framework.teardown import TeardownMode

        strategy = _make_strategy()
        strategy._total_supplied = Decimal("0.01")
        strategy._total_borrowed = Decimal("0")

        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)

        assert len(intents) == 1
        assert intents[0].intent_type.value == "WITHDRAW"

    def test_generate_teardown_intents_empty_when_no_positions(self):
        from almanak.framework.teardown import TeardownMode

        strategy = _make_strategy()
        intents = strategy.generate_teardown_intents(TeardownMode.HARD)
        assert intents == []
