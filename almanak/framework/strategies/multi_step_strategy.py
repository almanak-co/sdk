"""MultiStepStrategy - Declarative state machine for multi-step DeFi strategies.

Eliminates ~150 lines of boilerplate per strategy by providing:
- Declarative state/transition definitions
- Automatic dispatch in decide()
- Automatic transition handling in on_intent_executed()
- Automatic retry with configurable max retries per step
- Automatic revert to previous stable state on max retries exceeded
- Automatic state persistence (no manual get/load_persistent_state)
- Timeline event emission for state changes

Strategy authors define a list of Steps, each mapping a stable state to
an intent factory. The base class handles all state machine mechanics.

Example::

    from almanak.framework.strategies.multi_step_strategy import MultiStepStrategy, Step

    @almanak_strategy(name="my_looping_strategy")
    class MyLoopingStrategy(MultiStepStrategy):

        STEPS = [
            Step(name="supply", next="borrow"),
            Step(name="borrow", next="swap"),
            Step(name="swap", next="supply", terminal_check="is_looping_complete"),
        ]
        TERMINAL_STATE = "complete"
        MAX_RETRIES = 3

        def intent_for_step(self, step_name: str, market: MarketSnapshot) -> AnyIntent:
            if step_name == "supply":
                return Intent.supply(...)
            elif step_name == "borrow":
                return Intent.borrow(...)
            elif step_name == "swap":
                return Intent.swap(...)

        def is_looping_complete(self, market: MarketSnapshot) -> bool:
            return self.step_data.get("loops_completed", 0) >= 3
"""

import logging
from abc import abstractmethod
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from ..api.timeline import TimelineEvent, TimelineEventType, add_event
from ..intents import Intent
from ..intents.vocabulary import AnyIntent, DecideResult
from .intent_strategy import IntentStrategy, MarketSnapshot

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Step:
    """Defines a step in a multi-step strategy.

    Attributes:
        name: The stable state name for this step (e.g., "supply", "borrow").
        next: The name of the next step after this one succeeds.
            If None, this step transitions to the terminal state.
        terminal_check: Optional method name on the strategy class.
            Called after this step succeeds. If it returns True, the state
            machine transitions to the terminal state instead of ``next``.
            Signature: ``(self, market: MarketSnapshot) -> bool``
    """

    name: str
    next: str | None = None
    terminal_check: str | None = None

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("Step name cannot be empty")
        if self.name == "idle":
            raise ValueError("'idle' is reserved as the initial state")


class MultiStepStrategy(IntentStrategy):
    """Base class for multi-step strategies with declarative state machines.

    Subclasses MUST define:
        STEPS: list[Step] - ordered list of steps in the strategy lifecycle.
        TERMINAL_STATE: str - name of the final state (default: "complete").

    Subclasses MUST implement:
        intent_for_step(step_name, market) -> Intent
        ready_to_start(market) -> bool  (optional, defaults to True)

    The state machine flow is:
        idle -> step[0] (executing) -> step[0] (done) -> step[1] (executing) -> ...
                                                                -> terminal_state

    Internal state representation:
        - "idle": waiting to start
        - "{step_name}": stable state, step completed successfully
        - "{step_name}:executing": transitional state, intent submitted
        - TERMINAL_STATE: all steps complete

    Retry behavior:
        On intent failure, the retry counter increments. If retries < MAX_RETRIES,
        the step is re-attempted on the next decide() call. If retries exhausted,
        the state reverts to the previous stable state (or idle if no previous).

    State persistence:
        The base class automatically persists: current_state, previous_stable_state,
        retry_count, and step_data (a dict subclasses can use for tracking).
    """

    # --- Subclass configuration ---
    STEPS: tuple[Step, ...] = ()
    TERMINAL_STATE: str = "complete"
    MAX_RETRIES: int = 3

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

        # Validate STEPS
        if not self.STEPS:
            raise ValueError(f"{self.__class__.__name__} must define STEPS (non-empty tuple of Step)")

        # Build lookup structures
        self._step_map: dict[str, Step] = {step.name: step for step in self.STEPS}
        self._step_order: list[str] = [step.name for step in self.STEPS]

        # Fail fast on duplicate step names
        if len(self._step_order) != len(self._step_map):
            seen = set()
            dupes = [s.name for s in self.STEPS if s.name in seen or seen.add(s.name)]  # type: ignore[func-returns-value]
            raise ValueError(f"Duplicate step names detected: {dupes}")

        # Validate step graph: every step.next must reference a valid step
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
            if step.terminal_check is not None:
                check_attr = getattr(self, step.terminal_check, None)
                if check_attr is None:
                    raise ValueError(
                        f"Step '{step.name}' references terminal_check='{step.terminal_check}' "
                        f"but {self.__class__.__name__} has no such method"
                    )
                if not callable(check_attr):
                    raise ValueError(
                        f"Step '{step.name}' terminal_check='{step.terminal_check}' exists but is not callable"
                    )

        # State machine internals
        self._ms_state: str = "idle"
        self._ms_previous_stable: str = "idle"
        self._ms_retry_count: int = 0
        self._ms_step_data: dict[str, Any] = {}

    # --- Properties for subclass convenience ---

    @property
    def current_step_state(self) -> str:
        """The current state machine state (e.g., 'idle', 'supply', 'supply:executing')."""
        return self._ms_state

    @property
    def step_data(self) -> dict[str, Any]:
        """Mutable dict for subclasses to store step-specific tracking data.

        Automatically persisted across restarts. Use this instead of custom
        instance variables for any data that needs to survive restarts.
        """
        return self._ms_step_data

    @property
    def is_terminal(self) -> bool:
        """Whether the strategy has reached its terminal state."""
        return self._ms_state == self.TERMINAL_STATE

    # --- Abstract methods for subclasses ---

    @abstractmethod
    def intent_for_step(self, step_name: str, market: MarketSnapshot) -> AnyIntent:
        """Return the Intent to execute for a given step.

        Called when the state machine is ready to execute a step.
        Must return a valid Intent (not None, not HoldIntent).

        Args:
            step_name: The step to generate an intent for (e.g., "supply", "borrow").
            market: Current market snapshot.

        Returns:
            An Intent to execute.
        """
        ...

    def ready_to_start(self, market: MarketSnapshot) -> bool:
        """Check if the strategy is ready to begin its first step.

        Override this to check preconditions like sufficient balance.
        Called only when state is 'idle'. Return False to emit a HoldIntent.

        Args:
            market: Current market snapshot.

        Returns:
            True if ready to start, False to hold.
        """
        return True

    def on_step_completed(self, step_name: str, intent: Any, result: Any) -> None:
        """Called after a step's intent executes successfully.

        Override to extract data from results (e.g., position IDs, amounts).
        Default implementation does nothing.

        Args:
            step_name: The step that completed.
            intent: The intent that was executed.
            result: The execution result (enriched with extracted data).
        """
        pass

    def on_step_failed(self, step_name: str, intent: Any, result: Any, will_retry: bool) -> None:
        """Called when a step's intent execution fails.

        Override to log or react to failures. Default does nothing.

        Args:
            step_name: The step that failed.
            intent: The intent that failed.
            result: The execution result.
            will_retry: True if the step will be retried, False if reverting.
        """
        pass

    def on_terminal_reached(self, market: MarketSnapshot) -> AnyIntent:
        """Called on every decide() when the strategy is in terminal state.

        Override to provide monitoring behavior (e.g., health factor checks).
        Default returns HoldIntent.

        Args:
            market: Current market snapshot.

        Returns:
            An Intent (typically HoldIntent with status info).
        """
        return Intent.hold(reason=f"Strategy complete (state: {self.TERMINAL_STATE})")

    # --- Core state machine: decide() ---

    def decide(self, market: MarketSnapshot) -> DecideResult:
        """Dispatch to the correct step based on current state.

        Do NOT override this method. Implement intent_for_step() instead.
        """
        try:
            # Terminal state
            if self._ms_state == self.TERMINAL_STATE:
                return self.on_terminal_reached(market)

            # Idle -> start first step
            if self._ms_state == "idle":
                if not self.ready_to_start(market):
                    return Intent.hold(reason="Waiting for preconditions to start")
                return self._start_step(self._step_order[0], market)

            # Executing state (transitional) -> retry the same step
            if self._ms_state.endswith(":executing"):
                step_name = self._ms_state.rsplit(":", 1)[0]
                if step_name in self._step_map:
                    logger.warning(
                        f"Re-entering executing state '{self._ms_state}' "
                        f"(retry {self._ms_retry_count}/{self.MAX_RETRIES})"
                    )
                    return self._start_step(step_name, market)
                # Unknown executing state - revert
                logger.error(f"Unknown executing state '{self._ms_state}', reverting to '{self._ms_previous_stable}'")
                self._ms_state = self._ms_previous_stable
                return Intent.hold(reason=f"Recovered from unknown state, now at '{self._ms_state}'")

            # Stable state for a completed step -> advance to next step
            if self._ms_state in self._step_map:
                step = self._step_map[self._ms_state]
                next_step = self._resolve_next_step(step, market)
                if next_step is None:
                    # Terminal
                    self._transition_to_terminal()
                    return self.on_terminal_reached(market)
                return self._start_step(next_step, market)

            # Unknown state - attempt recovery
            unknown_state = self._ms_state
            logger.error(f"Unknown state '{unknown_state}', reverting to '{self._ms_previous_stable}'")
            self._ms_state = self._ms_previous_stable
            return Intent.hold(reason=f"Recovered from unknown state '{unknown_state}'")

        except Exception as e:
            logger.exception(f"Error in MultiStepStrategy.decide(): {e}")
            add_event(
                TimelineEvent(
                    timestamp=datetime.now(UTC),
                    event_type=TimelineEventType.ERROR,
                    description=f"MultiStep decide() caught exception: {type(e).__name__}: {e}",
                    strategy_id=getattr(self, "strategy_id", ""),
                    details={
                        "step": self._ms_state,
                        "exception_type": type(e).__name__,
                        "exception_message": str(e),
                        "retry_count": self._ms_retry_count,
                    },
                )
            )
            return Intent.hold(reason=f"Error: {e}")

    # --- Core state machine: on_intent_executed() ---

    def on_intent_executed(self, intent: Any, success: bool, result: Any) -> None:
        """Handle intent execution results and advance/revert state.

        Do NOT override this method. Use on_step_completed() and
        on_step_failed() hooks instead.
        """
        # Only handle transitions when in an executing state
        if not self._ms_state.endswith(":executing"):
            return

        step_name = self._ms_state.rsplit(":", 1)[0]

        if success:
            # Transition to the stable state for this step
            self._ms_state = step_name
            self._ms_previous_stable = step_name
            self._ms_retry_count = 0

            logger.info(f"Step '{step_name}' completed successfully")
            self._emit_step_event(step_name, "completed")

            # Call subclass hook
            self.on_step_completed(step_name, intent, result)

        else:
            # Failure handling
            self._ms_retry_count += 1
            will_retry = self._ms_retry_count < self.MAX_RETRIES

            if will_retry:
                # Stay in executing state for retry on next decide() call
                logger.warning(
                    f"Step '{step_name}' failed (attempt {self._ms_retry_count}/{self.MAX_RETRIES}), will retry"
                )
            else:
                # Revert to previous stable state
                revert_to = self._ms_previous_stable
                logger.warning(f"Step '{step_name}' failed {self.MAX_RETRIES} times, reverting to '{revert_to}'")
                self._ms_state = revert_to
                self._ms_retry_count = 0
                self._emit_step_event(step_name, "reverted", revert_to=revert_to)

            # Call subclass hook
            self.on_step_failed(step_name, intent, result, will_retry)

    # --- State persistence (automatic) ---

    def get_persistent_state(self) -> dict[str, Any]:
        """Automatically persist state machine internals + step_data."""
        state = {
            "_ms_state": self._ms_state,
            "_ms_previous_stable": self._ms_previous_stable,
            "_ms_retry_count": self._ms_retry_count,
            "_ms_step_data": self._ms_step_data,
        }
        # Merge any subclass state
        subclass_state = self.get_step_persistent_state()
        if subclass_state:
            state["_subclass"] = subclass_state
        return state

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        """Automatically restore state machine internals + step_data."""
        if "_ms_state" in state:
            self._ms_state = state["_ms_state"]
        if "_ms_previous_stable" in state:
            self._ms_previous_stable = state["_ms_previous_stable"]
        if "_ms_retry_count" in state:
            self._ms_retry_count = int(state["_ms_retry_count"])
        if "_ms_step_data" in state:
            self._ms_step_data = state["_ms_step_data"]

        # Restore subclass state
        if "_subclass" in state:
            self.load_step_persistent_state(state["_subclass"])

        logger.info(
            f"Restored MultiStepStrategy state: state={self._ms_state}, "
            f"previous_stable={self._ms_previous_stable}, retries={self._ms_retry_count}"
        )

    def get_step_persistent_state(self) -> dict[str, Any]:
        """Override to persist additional subclass-specific state.

        Returned dict is stored under a separate key, so there's no
        risk of colliding with base class state keys.
        """
        return {}

    def load_step_persistent_state(self, state: dict[str, Any]) -> None:
        """Override to restore additional subclass-specific state."""
        pass

    # --- Internal helpers ---

    def _start_step(self, step_name: str, market: MarketSnapshot) -> AnyIntent:
        """Transition to executing state and generate intent for a step."""
        executing_state = f"{step_name}:executing"
        old_state = self._ms_state

        # Save previous stable state before transitioning
        if not old_state.endswith(":executing"):
            self._ms_previous_stable = old_state

        logger.info(f"Starting step '{step_name}' (state: {old_state} -> {executing_state})")

        intent = self.intent_for_step(step_name, market)

        # Validate intent - should not be None or HoldIntent
        if intent is None:
            logger.warning(f"intent_for_step('{step_name}') returned None, staying at '{old_state}'")
            return Intent.hold(reason=f"Step '{step_name}' returned no intent")

        # Only transition to executing state after intent is successfully generated
        self._ms_state = executing_state
        self._emit_step_event(step_name, "started", old_state=old_state)

        return intent

    def _resolve_next_step(self, current_step: Step, market: MarketSnapshot) -> str | None:
        """Determine the next step, checking terminal conditions."""
        # Check terminal_check if defined
        if current_step.terminal_check is not None:
            check_method = getattr(self, current_step.terminal_check)
            if check_method(market):
                return None  # -> terminal

        # If no next step defined, go terminal
        if current_step.next is None:
            return None

        return current_step.next

    def _transition_to_terminal(self) -> None:
        """Transition to the terminal state."""
        old_state = self._ms_state
        self._ms_state = self.TERMINAL_STATE
        self._ms_previous_stable = self.TERMINAL_STATE
        self._ms_retry_count = 0
        logger.info(f"Reached terminal state '{self.TERMINAL_STATE}' (from '{old_state}')")
        self._emit_step_event(self.TERMINAL_STATE, "terminal", old_state=old_state)

    def _emit_step_event(self, step_name: str, action: str, **details: Any) -> None:
        """Emit a timeline event for a state change."""
        description = f"MultiStep: {step_name} {action}"
        if "old_state" in details:
            description = f"MultiStep: {details['old_state']} -> {step_name} ({action})"
        if "revert_to" in details:
            description = f"MultiStep: {step_name} failed, reverted to {details['revert_to']}"

        event_details = {
            "step": step_name,
            "action": action,
            "state": self._ms_state,
            "retry_count": self._ms_retry_count,
            **details,
        }

        add_event(
            TimelineEvent(
                timestamp=datetime.now(UTC),
                event_type=TimelineEventType.STATE_CHANGE,
                description=description,
                strategy_id=getattr(self, "strategy_id", ""),
                details=event_details,
            )
        )

    # --- Reset for testing ---

    def reset_state_machine(self) -> None:
        """Reset the state machine to idle. Useful for testing or fresh starts."""
        self._ms_state = "idle"
        self._ms_previous_stable = "idle"
        self._ms_retry_count = 0
        self._ms_step_data = {}
        logger.info("State machine reset to idle")
