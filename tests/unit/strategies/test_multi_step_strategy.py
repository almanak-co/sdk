"""Tests for MultiStepStrategy base class.

Covers:
- Step validation (graph integrity, reserved names)
- Full lifecycle: idle -> step1 -> step2 -> ... -> terminal
- Retry logic with configurable MAX_RETRIES
- Revert to previous stable state on max retries
- Terminal check callbacks
- State persistence (get/load)
- step_data dict usage
- on_step_completed / on_step_failed hooks
- ready_to_start precondition
- Reset functionality
- Edge cases: None intent, unknown states
"""

from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.intents import Intent
from almanak.framework.strategies.intent_strategy import MarketSnapshot
from almanak.framework.strategies.multi_step_strategy import MultiStepStrategy, Step


# ---------------------------------------------------------------------------
# Helpers: minimal concrete subclass for testing
# ---------------------------------------------------------------------------


def _make_market() -> MarketSnapshot:
    """Create a minimal MarketSnapshot mock."""
    market = MagicMock(spec=MarketSnapshot)
    market.price.return_value = Decimal("3000")
    balance = MagicMock()
    balance.balance = Decimal("10")
    balance.balance_usd = Decimal("30000")
    market.balance.return_value = balance
    return market


def _make_strategy_class(
    steps: list[Step],
    terminal_state: str = "complete",
    max_retries: int = 3,
    intent_map: dict[str, Intent] | None = None,
    ready: bool = True,
    terminal_check_fn=None,
):
    """Dynamically create a concrete MultiStepStrategy subclass."""
    _intent_map = intent_map or {}

    class TestStrategy(MultiStepStrategy):
        STEPS = steps
        TERMINAL_STATE = terminal_state
        MAX_RETRIES = max_retries

        def __init__(self):
            # Bypass IntentStrategy.__init__ for unit testing
            # We only test the state machine logic, not the full framework init
            self.STEPS = steps
            self.TERMINAL_STATE = terminal_state
            self.MAX_RETRIES = max_retries

            # Validate STEPS
            if not self.STEPS:
                raise ValueError(f"{self.__class__.__name__} must define STEPS (non-empty list of Step)")

            # Minimal attributes needed by MultiStepStrategy.__init__ (via IntentStrategy)
            # We patch around the super().__init__ call
            self._step_map = {step.name: step for step in self.STEPS}
            self._step_order = [step.name for step in self.STEPS]

            # Validate step graph
            valid_names = set(self._step_order)
            for step in self.STEPS:
                if step.name == self.TERMINAL_STATE:
                    raise ValueError(
                        f"Step name '{step.name}' collides with TERMINAL_STATE='{self.TERMINAL_STATE}'. "
                        f"Use a different step name or a different TERMINAL_STATE value."
                    )
                if step.next is not None and step.next not in valid_names:
                    raise ValueError(
                        f"Step '{step.name}' references next='{step.next}' "
                        f"which is not a defined step. Valid steps: {sorted(valid_names)}"
                    )
                if step.terminal_check is not None and not hasattr(self, step.terminal_check):
                    raise ValueError(
                        f"Step '{step.name}' references terminal_check='{step.terminal_check}' "
                        f"but {self.__class__.__name__} has no such method"
                    )

            self._ms_state = "idle"
            self._ms_previous_stable = "idle"
            self._ms_retry_count = 0
            self._ms_step_data = {}
            self._strategy_id = "test-strategy-1"
            self._ready = ready

            # Track hook calls
            self.completed_steps: list[str] = []
            self.failed_steps: list[tuple[str, bool]] = []

        def intent_for_step(self, step_name: str, market: MarketSnapshot) -> Intent:
            if step_name in _intent_map:
                return _intent_map[step_name]
            return Intent.swap(
                from_token="USDC",
                to_token="ETH",
                amount=Decimal("100"),
                max_slippage=Decimal("0.005"),
            )

        def ready_to_start(self, market: MarketSnapshot) -> bool:
            return self._ready

        def on_step_completed(self, step_name, intent, result):
            self.completed_steps.append(step_name)

        def on_step_failed(self, step_name, intent, result, will_retry):
            self.failed_steps.append((step_name, will_retry))

    if terminal_check_fn is not None:
        TestStrategy.is_done = terminal_check_fn

    return TestStrategy


# ---------------------------------------------------------------------------
# Step validation tests
# ---------------------------------------------------------------------------


class TestStepValidation:
    def test_empty_name_raises(self):
        with pytest.raises(ValueError, match="cannot be empty"):
            Step(name="")

    def test_idle_name_raises(self):
        with pytest.raises(ValueError, match="reserved"):
            Step(name="idle")

    def test_valid_step(self):
        step = Step(name="supply", next="borrow")
        assert step.name == "supply"
        assert step.next == "borrow"
        assert step.terminal_check is None

    def test_invalid_next_reference(self):
        steps = [Step(name="supply", next="nonexistent")]
        with pytest.raises(ValueError, match="not a defined step"):
            _make_strategy_class(steps)()

    def test_invalid_terminal_check_reference(self):
        steps = [Step(name="supply", terminal_check="no_such_method")]
        with pytest.raises(ValueError, match="has no such method"):
            _make_strategy_class(steps)()

    def test_step_name_collides_with_terminal_state(self):
        """Step named same as TERMINAL_STATE should be rejected."""
        steps = [Step(name="complete")]
        with pytest.raises(ValueError, match="collides with TERMINAL_STATE"):
            _make_strategy_class(steps)()

    def test_empty_steps_raises(self):
        """MultiStepStrategy with no STEPS should fail."""
        steps = []
        with pytest.raises(ValueError, match="must define STEPS"):
            _make_strategy_class(steps)()


# ---------------------------------------------------------------------------
# Full lifecycle tests
# ---------------------------------------------------------------------------


class TestLifecycle:
    @patch("almanak.framework.strategies.multi_step_strategy.add_event")
    def test_two_step_lifecycle(self, mock_add_event):
        """supply -> borrow -> complete."""
        steps = [
            Step(name="supply", next="borrow"),
            Step(name="borrow"),  # next=None -> terminal
        ]
        strat = _make_strategy_class(steps)()
        market = _make_market()

        # idle -> supply:executing
        intent = strat.decide(market)
        assert strat.current_step_state == "supply:executing"
        assert intent is not None

        # supply:executing -> supply (success)
        strat.on_intent_executed(intent, True, MagicMock())
        assert strat.current_step_state == "supply"
        assert strat.completed_steps == ["supply"]

        # supply -> borrow:executing
        intent = strat.decide(market)
        assert strat.current_step_state == "borrow:executing"

        # borrow:executing -> borrow (success)
        strat.on_intent_executed(intent, True, MagicMock())
        assert strat.current_step_state == "borrow"

        # borrow -> terminal (next=None)
        intent = strat.decide(market)
        assert strat.is_terminal
        assert strat.current_step_state == "complete"

    @patch("almanak.framework.strategies.multi_step_strategy.add_event")
    def test_three_step_loop_with_terminal_check(self, mock_add_event):
        """supply -> borrow -> swap -> (check loops) -> supply again or complete."""
        loop_count = {"value": 0}

        def is_done(self, market):
            return loop_count["value"] >= 2

        steps = [
            Step(name="supply", next="borrow"),
            Step(name="borrow", next="swap"),
            Step(name="swap", next="supply", terminal_check="is_done"),
        ]
        strat = _make_strategy_class(steps, terminal_check_fn=is_done)()
        market = _make_market()

        # Loop 1: supply -> borrow -> swap
        for step_name in ["supply", "borrow", "swap"]:
            intent = strat.decide(market)
            assert strat.current_step_state == f"{step_name}:executing"
            strat.on_intent_executed(intent, True, MagicMock())
            assert strat.current_step_state == step_name

        loop_count["value"] = 1

        # Loop 2: supply -> borrow -> swap (is_done returns False, then True)
        for step_name in ["supply", "borrow", "swap"]:
            intent = strat.decide(market)
            assert strat.current_step_state == f"{step_name}:executing"
            strat.on_intent_executed(intent, True, MagicMock())

        loop_count["value"] = 2

        # Now is_done returns True -> terminal
        intent = strat.decide(market)
        assert strat.is_terminal


# ---------------------------------------------------------------------------
# Retry and revert tests
# ---------------------------------------------------------------------------


class TestRetryAndRevert:
    @patch("almanak.framework.strategies.multi_step_strategy.add_event")
    def test_retry_on_failure(self, mock_add_event):
        """Step retries up to MAX_RETRIES before reverting."""
        steps = [Step(name="supply", next="borrow"), Step(name="borrow")]
        strat = _make_strategy_class(steps, max_retries=2)()
        market = _make_market()

        # idle -> supply:executing
        intent = strat.decide(market)
        assert strat.current_step_state == "supply:executing"

        # Fail once -> stays in executing (will retry)
        strat.on_intent_executed(intent, False, MagicMock())
        assert strat.current_step_state == "supply:executing"
        assert strat._ms_retry_count == 1
        assert strat.failed_steps == [("supply", True)]  # will_retry=True

        # Retry from decide()
        intent = strat.decide(market)
        assert strat.current_step_state == "supply:executing"

        # Fail again -> reverts to idle (MAX_RETRIES=2, count now 2)
        strat.on_intent_executed(intent, False, MagicMock())
        assert strat.current_step_state == "idle"
        assert strat._ms_retry_count == 0
        assert strat.failed_steps[-1] == ("supply", False)  # will_retry=False

    @patch("almanak.framework.strategies.multi_step_strategy.add_event")
    def test_revert_to_previous_stable_not_idle(self, mock_add_event):
        """When step 2 fails enough, revert to step 1's stable state."""
        steps = [Step(name="supply", next="borrow"), Step(name="borrow")]
        strat = _make_strategy_class(steps, max_retries=1)()
        market = _make_market()

        # Complete supply
        intent = strat.decide(market)
        strat.on_intent_executed(intent, True, MagicMock())
        assert strat.current_step_state == "supply"

        # Start borrow
        intent = strat.decide(market)
        assert strat.current_step_state == "borrow:executing"

        # Fail borrow -> revert to supply (not idle)
        strat.on_intent_executed(intent, False, MagicMock())
        assert strat.current_step_state == "supply"

    @patch("almanak.framework.strategies.multi_step_strategy.add_event")
    def test_success_after_retry(self, mock_add_event):
        """Step succeeds on retry."""
        steps = [Step(name="supply")]
        strat = _make_strategy_class(steps, max_retries=3)()
        market = _make_market()

        # Start supply
        intent = strat.decide(market)

        # Fail
        strat.on_intent_executed(intent, False, MagicMock())
        assert strat._ms_retry_count == 1

        # Retry
        intent = strat.decide(market)

        # Succeed
        strat.on_intent_executed(intent, True, MagicMock())
        assert strat.current_step_state == "supply"
        assert strat._ms_retry_count == 0


# ---------------------------------------------------------------------------
# State persistence tests
# ---------------------------------------------------------------------------


class TestStatePersistence:
    @patch("almanak.framework.strategies.multi_step_strategy.add_event")
    def test_persist_and_restore(self, mock_add_event):
        steps = [Step(name="supply", next="borrow"), Step(name="borrow")]
        strat = _make_strategy_class(steps)()
        market = _make_market()

        # Advance to supply completed
        intent = strat.decide(market)
        strat.on_intent_executed(intent, True, MagicMock())
        strat.step_data["total_supplied"] = "1000"

        # Persist
        state = strat.get_persistent_state()
        assert state["_ms_state"] == "supply"
        assert state["_ms_step_data"]["total_supplied"] == "1000"

        # Create fresh strategy and restore
        strat2 = _make_strategy_class(steps)()
        strat2.load_persistent_state(state)
        assert strat2.current_step_state == "supply"
        assert strat2.step_data["total_supplied"] == "1000"

    @patch("almanak.framework.strategies.multi_step_strategy.add_event")
    def test_subclass_state_persistence(self, mock_add_event):
        """Subclass can persist additional state via hooks."""
        steps = [Step(name="supply")]

        class SubStrategy(_make_strategy_class(steps)):
            def __init__(self):
                super().__init__()
                self.custom_value = 42

            def get_step_persistent_state(self):
                return {"custom_value": self.custom_value}

            def load_step_persistent_state(self, state):
                self.custom_value = state.get("custom_value", 0)

        strat = SubStrategy()
        state = strat.get_persistent_state()
        assert state["_subclass"]["custom_value"] == 42

        strat2 = SubStrategy()
        strat2.custom_value = 0
        strat2.load_persistent_state(state)
        assert strat2.custom_value == 42


# ---------------------------------------------------------------------------
# ready_to_start tests
# ---------------------------------------------------------------------------


class TestReadyToStart:
    @patch("almanak.framework.strategies.multi_step_strategy.add_event")
    def test_not_ready_holds(self, mock_add_event):
        steps = [Step(name="supply")]
        strat = _make_strategy_class(steps, ready=False)()
        market = _make_market()

        intent = strat.decide(market)
        assert strat.current_step_state == "idle"
        # Should be a hold intent
        assert intent is not None

    @patch("almanak.framework.strategies.multi_step_strategy.add_event")
    def test_ready_starts(self, mock_add_event):
        steps = [Step(name="supply")]
        strat = _make_strategy_class(steps, ready=True)()
        market = _make_market()

        intent = strat.decide(market)
        assert strat.current_step_state == "supply:executing"


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    @patch("almanak.framework.strategies.multi_step_strategy.add_event")
    def test_none_intent_reverts(self, mock_add_event):
        """If intent_for_step returns None, revert to previous stable."""
        steps = [Step(name="supply")]
        strat = _make_strategy_class(steps, intent_map={"supply": None})()
        market = _make_market()

        intent = strat.decide(market)
        # Should revert to idle since supply returned None
        assert strat.current_step_state == "idle"
        assert intent is not None  # Returns a hold intent

    @patch("almanak.framework.strategies.multi_step_strategy.add_event")
    def test_on_intent_executed_outside_executing_state_is_noop(self, mock_add_event):
        """Calling on_intent_executed when not in :executing state does nothing."""
        steps = [Step(name="supply")]
        strat = _make_strategy_class(steps)()

        # In idle state, on_intent_executed should be a no-op
        strat.on_intent_executed(MagicMock(), True, MagicMock())
        assert strat.current_step_state == "idle"

    @patch("almanak.framework.strategies.multi_step_strategy.add_event")
    def test_reset_state_machine(self, mock_add_event):
        steps = [Step(name="supply")]
        strat = _make_strategy_class(steps)()
        market = _make_market()

        # Advance to supply:executing
        strat.decide(market)
        assert strat.current_step_state == "supply:executing"

        strat.reset_state_machine()
        assert strat.current_step_state == "idle"
        assert strat._ms_retry_count == 0
        assert strat.step_data == {}

    @patch("almanak.framework.strategies.multi_step_strategy.add_event")
    def test_terminal_state_holds(self, mock_add_event):
        """Once terminal, decide() always returns terminal hold."""
        steps = [Step(name="supply")]
        strat = _make_strategy_class(steps)()
        market = _make_market()

        # Manually set terminal
        strat._ms_state = "complete"

        intent = strat.decide(market)
        assert strat.is_terminal
        assert intent is not None

        # Calling again still holds
        intent = strat.decide(market)
        assert strat.is_terminal

    @patch("almanak.framework.strategies.multi_step_strategy.add_event")
    def test_single_step_to_terminal(self, mock_add_event):
        """Single step with next=None goes directly to terminal."""
        steps = [Step(name="supply")]
        strat = _make_strategy_class(steps)()
        market = _make_market()

        # idle -> supply:executing
        intent = strat.decide(market)
        strat.on_intent_executed(intent, True, MagicMock())
        assert strat.current_step_state == "supply"

        # supply -> terminal
        intent = strat.decide(market)
        assert strat.is_terminal


# ---------------------------------------------------------------------------
# Exception handling: timeline event emission (VIB-577)
# ---------------------------------------------------------------------------


class TestDecideExceptionEmitsEvent:
    """VIB-577: Caught exceptions in decide() must emit an ERROR timeline event."""

    @patch("almanak.framework.strategies.multi_step_strategy.add_event")
    def test_exception_emits_error_event(self, mock_add_event):
        """When intent_for_step raises, decide() emits a TimelineEventType.ERROR event."""
        from almanak.framework.api.timeline import TimelineEventType

        def broken_intent(self, step_name, market):
            raise KeyError("missing_key")

        steps = [Step(name="supply", next="borrow"), Step(name="borrow")]
        cls = _make_strategy_class(steps)
        cls.intent_for_step = broken_intent
        strat = cls()
        market = _make_market()

        intent = strat.decide(market)

        # Should return HoldIntent (not crash)
        assert intent is not None
        assert "Error" in str(intent.reason) or "error" in str(intent.reason).lower()

        # Should have emitted an ERROR timeline event
        error_calls = [
            call
            for call in mock_add_event.call_args_list
            if call.args[0].event_type == TimelineEventType.ERROR
        ]
        assert len(error_calls) == 1, f"Expected 1 ERROR event, got {len(error_calls)}"

        event = error_calls[0].args[0]
        assert "KeyError" in event.description
        assert event.details["exception_type"] == "KeyError"
        assert "missing_key" in event.details["exception_message"]

    @patch("almanak.framework.strategies.multi_step_strategy.add_event")
    def test_exception_preserves_state(self, mock_add_event):
        """Exception in decide() should not corrupt the state machine."""
        call_count = 0

        def sometimes_broken(self, step_name, market):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise TypeError("first call fails")
            return Intent.swap(
                from_token="USDC", to_token="ETH",
                amount=Decimal("100"), max_slippage=Decimal("0.005"),
            )

        steps = [Step(name="supply")]
        cls = _make_strategy_class(steps)
        cls.intent_for_step = sometimes_broken
        strat = cls()
        market = _make_market()

        # First call: exception -> hold + error event
        intent1 = strat.decide(market)
        assert "Error" in str(intent1.reason) or "error" in str(intent1.reason).lower()

        # Second call: should recover and work normally
        intent2 = strat.decide(market)
        assert intent2 is not None
