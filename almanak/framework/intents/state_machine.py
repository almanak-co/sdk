"""Auto-generated state machine for Intent execution.

This module provides state machine generation for any Intent type, automatically
creating the PREPARING, VALIDATING, and SADFLOW states needed for proper
execution flow with retry logic and error handling.

The state machine pattern ensures:
1. Proper preparation of ActionBundles from Intents
2. Validation of transaction receipts
3. Retry logic with exponential backoff on failures
4. Metrics emission for monitoring

Example:
    from almanak.framework.intents import Intent, IntentCompiler
    from almanak.framework.intents.state_machine import IntentStateMachine

    # Create intent and state machine
    intent = Intent.swap("USDC", "ETH", amount_usd=Decimal("1000"))
    compiler = IntentCompiler(chain="arbitrum")
    state_machine = IntentStateMachine(intent, compiler)

    # Execute through states
    while not state_machine.is_complete:
        result = state_machine.step()
        if result.action_bundle:
            # Execute the action bundle
            receipt = execute(result.action_bundle)
            state_machine.set_receipt(receipt)
"""

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum, StrEnum, auto
from typing import Any, Optional

from ..models.reproduction_bundle import ActionBundle
from .compiler import (
    CompilationResult,
    CompilationStatus,
    IntentCompiler,
)
from .vocabulary import (
    AnyIntent,
    IntentType,
)

logger = logging.getLogger(__name__)


# =============================================================================
# State Generation
# =============================================================================


class IntentState(Enum):
    """Generated states for intent execution.

    For each intent type, the state machine generates three states:
    - PREPARING_{INTENT}: Building the ActionBundle
    - VALIDATING_{INTENT}: Checking the transaction receipt
    - SADFLOW_{INTENT}: Handling failures with retry logic

    Plus common states:
    - IDLE: Initial state, no intent to process
    - COMPLETED: Intent successfully executed
    - FAILED: Intent failed after all retries exhausted
    """

    # Common states
    IDLE = auto()
    COMPLETED = auto()
    FAILED = auto()

    # SWAP intent states
    PREPARING_SWAP = auto()
    VALIDATING_SWAP = auto()
    SADFLOW_SWAP = auto()

    # LP_OPEN intent states
    PREPARING_LP_OPEN = auto()
    VALIDATING_LP_OPEN = auto()
    SADFLOW_LP_OPEN = auto()

    # LP_CLOSE intent states
    PREPARING_LP_CLOSE = auto()
    VALIDATING_LP_CLOSE = auto()
    SADFLOW_LP_CLOSE = auto()

    # BORROW intent states
    PREPARING_BORROW = auto()
    VALIDATING_BORROW = auto()
    SADFLOW_BORROW = auto()

    # REPAY intent states
    PREPARING_REPAY = auto()
    VALIDATING_REPAY = auto()
    SADFLOW_REPAY = auto()

    # SUPPLY intent states
    PREPARING_SUPPLY = auto()
    VALIDATING_SUPPLY = auto()
    SADFLOW_SUPPLY = auto()

    # WITHDRAW intent states
    PREPARING_WITHDRAW = auto()
    VALIDATING_WITHDRAW = auto()
    SADFLOW_WITHDRAW = auto()

    # PERP_OPEN intent states
    PREPARING_PERP_OPEN = auto()
    VALIDATING_PERP_OPEN = auto()
    SADFLOW_PERP_OPEN = auto()

    # PERP_CLOSE intent states
    PREPARING_PERP_CLOSE = auto()
    VALIDATING_PERP_CLOSE = auto()
    SADFLOW_PERP_CLOSE = auto()

    # HOLD intent states (simplified - just completes)
    PREPARING_HOLD = auto()
    VALIDATING_HOLD = auto()
    SADFLOW_HOLD = auto()

    # STAKE intent states
    PREPARING_STAKE = auto()
    VALIDATING_STAKE = auto()
    SADFLOW_STAKE = auto()

    # UNSTAKE intent states
    PREPARING_UNSTAKE = auto()
    VALIDATING_UNSTAKE = auto()
    SADFLOW_UNSTAKE = auto()

    # LP_COLLECT_FEES intent states
    PREPARING_LP_COLLECT_FEES = auto()
    VALIDATING_LP_COLLECT_FEES = auto()
    SADFLOW_LP_COLLECT_FEES = auto()


def get_preparing_state(intent_type: IntentType) -> IntentState:
    """Get the PREPARING state for an intent type.

    Args:
        intent_type: The type of intent.

    Returns:
        The corresponding PREPARING state.
    """
    state_map = {
        IntentType.SWAP: IntentState.PREPARING_SWAP,
        IntentType.LP_OPEN: IntentState.PREPARING_LP_OPEN,
        IntentType.LP_CLOSE: IntentState.PREPARING_LP_CLOSE,
        IntentType.BORROW: IntentState.PREPARING_BORROW,
        IntentType.REPAY: IntentState.PREPARING_REPAY,
        IntentType.SUPPLY: IntentState.PREPARING_SUPPLY,
        IntentType.WITHDRAW: IntentState.PREPARING_WITHDRAW,
        IntentType.PERP_OPEN: IntentState.PREPARING_PERP_OPEN,
        IntentType.PERP_CLOSE: IntentState.PREPARING_PERP_CLOSE,
        IntentType.HOLD: IntentState.PREPARING_HOLD,
        IntentType.STAKE: IntentState.PREPARING_STAKE,
        IntentType.UNSTAKE: IntentState.PREPARING_UNSTAKE,
        IntentType.LP_COLLECT_FEES: IntentState.PREPARING_LP_COLLECT_FEES,
    }
    return state_map.get(intent_type, IntentState.IDLE)


def get_validating_state(intent_type: IntentType) -> IntentState:
    """Get the VALIDATING state for an intent type.

    Args:
        intent_type: The type of intent.

    Returns:
        The corresponding VALIDATING state.
    """
    state_map = {
        IntentType.SWAP: IntentState.VALIDATING_SWAP,
        IntentType.LP_OPEN: IntentState.VALIDATING_LP_OPEN,
        IntentType.LP_CLOSE: IntentState.VALIDATING_LP_CLOSE,
        IntentType.BORROW: IntentState.VALIDATING_BORROW,
        IntentType.REPAY: IntentState.VALIDATING_REPAY,
        IntentType.SUPPLY: IntentState.VALIDATING_SUPPLY,
        IntentType.WITHDRAW: IntentState.VALIDATING_WITHDRAW,
        IntentType.PERP_OPEN: IntentState.VALIDATING_PERP_OPEN,
        IntentType.PERP_CLOSE: IntentState.VALIDATING_PERP_CLOSE,
        IntentType.HOLD: IntentState.VALIDATING_HOLD,
        IntentType.STAKE: IntentState.VALIDATING_STAKE,
        IntentType.UNSTAKE: IntentState.VALIDATING_UNSTAKE,
        IntentType.LP_COLLECT_FEES: IntentState.VALIDATING_LP_COLLECT_FEES,
    }
    return state_map.get(intent_type, IntentState.IDLE)


def get_sadflow_state(intent_type: IntentType) -> IntentState:
    """Get the SADFLOW state for an intent type.

    Args:
        intent_type: The type of intent.

    Returns:
        The corresponding SADFLOW state.
    """
    state_map = {
        IntentType.SWAP: IntentState.SADFLOW_SWAP,
        IntentType.LP_OPEN: IntentState.SADFLOW_LP_OPEN,
        IntentType.LP_CLOSE: IntentState.SADFLOW_LP_CLOSE,
        IntentType.BORROW: IntentState.SADFLOW_BORROW,
        IntentType.REPAY: IntentState.SADFLOW_REPAY,
        IntentType.SUPPLY: IntentState.SADFLOW_SUPPLY,
        IntentType.WITHDRAW: IntentState.SADFLOW_WITHDRAW,
        IntentType.PERP_OPEN: IntentState.SADFLOW_PERP_OPEN,
        IntentType.PERP_CLOSE: IntentState.SADFLOW_PERP_CLOSE,
        IntentType.HOLD: IntentState.SADFLOW_HOLD,
        IntentType.STAKE: IntentState.SADFLOW_STAKE,
        IntentType.UNSTAKE: IntentState.SADFLOW_UNSTAKE,
        IntentType.LP_COLLECT_FEES: IntentState.SADFLOW_LP_COLLECT_FEES,
    }
    return state_map.get(intent_type, IntentState.IDLE)


def is_preparing_state(state: IntentState) -> bool:
    """Check if a state is a PREPARING state."""
    return state in {
        IntentState.PREPARING_SWAP,
        IntentState.PREPARING_LP_OPEN,
        IntentState.PREPARING_LP_CLOSE,
        IntentState.PREPARING_BORROW,
        IntentState.PREPARING_REPAY,
        IntentState.PREPARING_SUPPLY,
        IntentState.PREPARING_WITHDRAW,
        IntentState.PREPARING_PERP_OPEN,
        IntentState.PREPARING_PERP_CLOSE,
        IntentState.PREPARING_HOLD,
        IntentState.PREPARING_STAKE,
        IntentState.PREPARING_UNSTAKE,
        IntentState.PREPARING_LP_COLLECT_FEES,
    }


def is_validating_state(state: IntentState) -> bool:
    """Check if a state is a VALIDATING state."""
    return state in {
        IntentState.VALIDATING_SWAP,
        IntentState.VALIDATING_LP_OPEN,
        IntentState.VALIDATING_LP_CLOSE,
        IntentState.VALIDATING_BORROW,
        IntentState.VALIDATING_REPAY,
        IntentState.VALIDATING_SUPPLY,
        IntentState.VALIDATING_WITHDRAW,
        IntentState.VALIDATING_PERP_OPEN,
        IntentState.VALIDATING_PERP_CLOSE,
        IntentState.VALIDATING_HOLD,
        IntentState.VALIDATING_STAKE,
        IntentState.VALIDATING_UNSTAKE,
        IntentState.VALIDATING_LP_COLLECT_FEES,
    }


def is_sadflow_state(state: IntentState) -> bool:
    """Check if a state is a SADFLOW state."""
    return state in {
        IntentState.SADFLOW_SWAP,
        IntentState.SADFLOW_LP_OPEN,
        IntentState.SADFLOW_LP_CLOSE,
        IntentState.SADFLOW_BORROW,
        IntentState.SADFLOW_REPAY,
        IntentState.SADFLOW_SUPPLY,
        IntentState.SADFLOW_WITHDRAW,
        IntentState.SADFLOW_PERP_OPEN,
        IntentState.SADFLOW_PERP_CLOSE,
        IntentState.SADFLOW_HOLD,
        IntentState.SADFLOW_STAKE,
        IntentState.SADFLOW_UNSTAKE,
        IntentState.SADFLOW_LP_COLLECT_FEES,
    }


# =============================================================================
# Configuration
# =============================================================================


@dataclass
class RetryConfig:
    """Configuration for retry logic.

    Attributes:
        max_retries: Maximum number of retry attempts (default 3).
        initial_delay_seconds: Initial delay between retries in seconds (default 1.0).
        max_delay_seconds: Maximum delay between retries (default 60.0).
        exponential_base: Base for exponential backoff (default 2.0).
        jitter_factor: Random jitter factor (0-1) to add to delays (default 0.1).
    """

    max_retries: int = 3
    initial_delay_seconds: float = 1.0
    max_delay_seconds: float = 60.0
    exponential_base: float = 2.0
    jitter_factor: float = 0.1

    def calculate_delay(self, attempt: int) -> float:
        """Calculate delay for a given retry attempt.

        Uses exponential backoff with optional jitter.

        Args:
            attempt: The retry attempt number (0-indexed).

        Returns:
            Delay in seconds.
        """
        import random

        # Exponential backoff: initial_delay * base^attempt
        delay = self.initial_delay_seconds * (self.exponential_base**attempt)

        # Apply max cap
        delay = min(delay, self.max_delay_seconds)

        # Add jitter
        if self.jitter_factor > 0:
            jitter = delay * self.jitter_factor * random.random()
            delay += jitter

        return delay


@dataclass
class StateMachineConfig:
    """Configuration for the intent state machine.

    Attributes:
        retry_config: Configuration for retry logic.
        emit_metrics: Whether to emit metrics for state transitions.
        auto_advance_hold: Automatically complete HOLD intents without validation.
    """

    retry_config: RetryConfig = field(default_factory=RetryConfig)
    emit_metrics: bool = True
    auto_advance_hold: bool = True


# =============================================================================
# Receipt and Result Types
# =============================================================================


@dataclass
class TransactionReceipt:
    """Simplified transaction receipt for validation.

    Attributes:
        success: Whether the transaction succeeded.
        tx_hash: Transaction hash.
        block_number: Block number where transaction was included.
        gas_used: Amount of gas used.
        error: Error message if transaction failed.
        logs: Event logs from the transaction.
    """

    success: bool
    tx_hash: str = ""
    block_number: int = 0
    gas_used: int = 0
    error: str | None = None
    logs: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "tx_hash": self.tx_hash,
            "block_number": self.block_number,
            "gas_used": self.gas_used,
            "error": self.error,
            "logs": self.logs,
        }


@dataclass
class StepResult:
    """Result of a state machine step.

    Attributes:
        state: Current state after the step.
        action_bundle: ActionBundle to execute (if in PREPARING state).
        needs_execution: Whether an action bundle needs to be executed.
        is_complete: Whether the state machine has completed.
        success: Whether execution was successful (only valid when is_complete).
        error: Error message if failed.
        retry_delay: Suggested delay before retry (if in SADFLOW).
    """

    state: IntentState
    action_bundle: ActionBundle | None = None
    needs_execution: bool = False
    is_complete: bool = False
    success: bool = False
    error: str | None = None
    retry_delay: float | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "state": self.state.name,
            "action_bundle": self.action_bundle.to_dict() if self.action_bundle else None,
            "needs_execution": self.needs_execution,
            "is_complete": self.is_complete,
            "success": self.success,
            "error": self.error,
            "retry_delay": self.retry_delay,
        }


# =============================================================================
# Sadflow Hooks Types
# =============================================================================


class SadflowActionType(StrEnum):
    """Type of action to take in response to a sadflow event.

    Used by strategies to customize sadflow behavior via lifecycle hooks.

    Attributes:
        RETRY: Continue with retry (default behavior)
        ABORT: Abort execution immediately, transition to FAILED
        MODIFY: Modify the action bundle before retrying (requires modified_bundle)
        SKIP: Skip this intent and mark as completed (use cautiously)
    """

    RETRY = "retry"
    ABORT = "abort"
    MODIFY = "modify"
    SKIP = "skip"


@dataclass
class SadflowAction:
    """Action to take in response to a sadflow event.

    Returned by sadflow lifecycle hooks to customize retry behavior.

    Attributes:
        action_type: The type of action to take
        modified_bundle: Modified ActionBundle to use (only for MODIFY action)
        reason: Optional reason for the action (for logging)
        custom_delay: Optional custom delay in seconds (overrides exponential backoff)

    Example:
        # Continue with default retry
        return SadflowAction(SadflowActionType.RETRY)

        # Abort on specific error
        if "insufficient funds" in context.error_message:
            return SadflowAction(SadflowActionType.ABORT, reason="Insufficient funds")

        # Modify and retry with adjusted parameters
        modified = modify_gas(context.action_bundle)
        return SadflowAction(SadflowActionType.MODIFY, modified_bundle=modified)
    """

    action_type: SadflowActionType = SadflowActionType.RETRY
    modified_bundle: ActionBundle | None = None
    reason: str | None = None
    custom_delay: float | None = None

    def __post_init__(self) -> None:
        """Validate action configuration."""
        if self.action_type == SadflowActionType.MODIFY and self.modified_bundle is None:
            raise ValueError("MODIFY action requires modified_bundle to be set")

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "action_type": self.action_type.value,
            "modified_bundle": self.modified_bundle.to_dict() if self.modified_bundle else None,
            "reason": self.reason,
            "custom_delay": self.custom_delay,
        }

    @classmethod
    def retry(cls, reason: str | None = None, custom_delay: float | None = None) -> "SadflowAction":
        """Create a RETRY action."""
        return cls(SadflowActionType.RETRY, reason=reason, custom_delay=custom_delay)

    @classmethod
    def abort(cls, reason: str | None = None) -> "SadflowAction":
        """Create an ABORT action."""
        return cls(SadflowActionType.ABORT, reason=reason)

    @classmethod
    def modify(cls, modified_bundle: ActionBundle, reason: str | None = None) -> "SadflowAction":
        """Create a MODIFY action with a modified bundle."""
        return cls(SadflowActionType.MODIFY, modified_bundle=modified_bundle, reason=reason)

    @classmethod
    def skip(cls, reason: str | None = None) -> "SadflowAction":
        """Create a SKIP action."""
        return cls(SadflowActionType.SKIP, reason=reason)


@dataclass
class SadflowContext:
    """Context provided to sadflow lifecycle hooks.

    Contains error details and execution state for strategy-level decision making
    during sadflow handling.

    Attributes:
        intent_id: ID of the intent that triggered sadflow
        intent_type: Type of the intent (SWAP, BORROW, etc.)
        error_message: Error message from the failed transaction
        error_type: Categorized error type (if determinable)
        attempt_number: Current attempt number (1-indexed)
        max_attempts: Maximum number of attempts configured
        action_bundle: The ActionBundle that was executed (may be None on compilation failure)
        receipt: The transaction receipt (may be None if tx wasn't submitted)
        state: Current state machine state
        started_at: When the state machine started
        total_duration_seconds: Total time since start
        metadata: Additional context-specific metadata

    Example:
        def on_sadflow_enter(self, error_type, attempt, context):
            if context.error_type == "INSUFFICIENT_GAS":
                # Modify gas and retry
                modified = context.action_bundle.with_gas_multiplier(1.5)
                return SadflowAction.modify(modified)

            if attempt >= 2 and "timeout" in context.error_message.lower():
                # Abort after 2 timeout errors
                return SadflowAction.abort("Too many timeouts")

            return None  # Use default retry behavior
    """

    intent_id: str
    intent_type: str
    error_message: str
    error_type: str | None = None
    attempt_number: int = 1
    max_attempts: int = 3
    action_bundle: ActionBundle | None = None
    receipt: Optional["TransactionReceipt"] = None
    state: IntentState | None = None
    started_at: datetime | None = None
    total_duration_seconds: float = 0.0
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "intent_id": self.intent_id,
            "intent_type": self.intent_type,
            "error_message": self.error_message,
            "error_type": self.error_type,
            "attempt_number": self.attempt_number,
            "max_attempts": self.max_attempts,
            "action_bundle": self.action_bundle.to_dict() if self.action_bundle else None,
            "receipt": self.receipt.to_dict() if self.receipt else None,
            "state": self.state.name if self.state else None,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "total_duration_seconds": self.total_duration_seconds,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "SadflowContext":
        """Create SadflowContext from dictionary."""
        started_at = None
        if data.get("started_at"):
            started_at = datetime.fromisoformat(data["started_at"])

        state = None
        if data.get("state"):
            state = IntentState[data["state"]]

        return cls(
            intent_id=data["intent_id"],
            intent_type=data["intent_type"],
            error_message=data["error_message"],
            error_type=data.get("error_type"),
            attempt_number=data.get("attempt_number", 1),
            max_attempts=data.get("max_attempts", 3),
            action_bundle=None,  # Cannot deserialize without ActionBundle.from_dict
            receipt=None,  # Cannot deserialize without TransactionReceipt.from_dict
            state=state,
            started_at=started_at,
            total_duration_seconds=data.get("total_duration_seconds", 0.0),
            metadata=data.get("metadata", {}),
        )


# Type alias for sadflow hook callbacks
SadflowEnterCallback = Callable[[str | None, int, SadflowContext], SadflowAction | None]
SadflowExitCallback = Callable[[bool, int], None]
SadflowRetryCallback = Callable[[SadflowContext, SadflowAction], SadflowAction]


# =============================================================================
# Metrics
# =============================================================================


@dataclass
class StateTransitionMetric:
    """Metric for a state transition.

    Attributes:
        intent_id: ID of the intent being processed.
        intent_type: Type of the intent.
        from_state: State before transition.
        to_state: State after transition.
        timestamp: When the transition occurred.
        duration_ms: Duration of the transition in milliseconds.
        success: Whether the transition was successful.
        error: Error message if transition failed.
        metadata: Additional metadata.
    """

    intent_id: str
    intent_type: str
    from_state: str
    to_state: str
    timestamp: datetime
    duration_ms: float = 0.0
    success: bool = True
    error: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "intent_id": self.intent_id,
            "intent_type": self.intent_type,
            "from_state": self.from_state,
            "to_state": self.to_state,
            "timestamp": self.timestamp.isoformat(),
            "duration_ms": self.duration_ms,
            "success": self.success,
            "error": self.error,
            "metadata": self.metadata,
        }


# Type for metrics callback
MetricsCallback = Callable[[StateTransitionMetric], None]

# Default metrics storage (for testing/inspection)
_metrics_store: list[StateTransitionMetric] = []


def default_metrics_callback(metric: StateTransitionMetric) -> None:
    """Default metrics callback that stores metrics in memory.

    Args:
        metric: The metric to store.
    """
    _metrics_store.append(metric)
    logger.debug(
        f"State transition: {metric.from_state} -> {metric.to_state} "
        f"(intent={metric.intent_id}, duration={metric.duration_ms:.2f}ms)"
    )


def get_metrics() -> list[StateTransitionMetric]:
    """Get all stored metrics.

    Returns:
        List of state transition metrics.
    """
    return list(_metrics_store)


def clear_metrics() -> None:
    """Clear all stored metrics."""
    _metrics_store.clear()


# =============================================================================
# Intent State Machine
# =============================================================================


class IntentStateMachine:
    """State machine for executing intents.

    The state machine manages the lifecycle of intent execution:
    1. PREPARING: Compile the intent into an ActionBundle
    2. VALIDATING: Check the transaction receipt for success
    3. SADFLOW: Handle failures with retry logic

    The machine automatically generates the appropriate states based on
    the intent type and handles state transitions.

    Example:
        # Create state machine
        intent = Intent.swap("USDC", "ETH", amount_usd=Decimal("1000"))
        compiler = IntentCompiler(chain="arbitrum")
        sm = IntentStateMachine(intent, compiler)

        # Step through execution
        while not sm.is_complete:
            result = sm.step()
            if result.needs_execution:
                # Execute the action bundle and get receipt
                receipt = execute_on_chain(result.action_bundle)
                sm.set_receipt(receipt)

        if sm.success:
            print("Intent executed successfully!")
        else:
            print(f"Intent failed: {sm.error}")
    """

    def __init__(
        self,
        intent: AnyIntent,
        compiler: IntentCompiler,
        config: StateMachineConfig | None = None,
        metrics_callback: MetricsCallback | None = None,
        on_sadflow_enter: SadflowEnterCallback | None = None,
        on_sadflow_exit: SadflowExitCallback | None = None,
        on_retry: SadflowRetryCallback | None = None,
    ) -> None:
        """Initialize the state machine.

        Args:
            intent: The intent to execute.
            compiler: The compiler to use for building ActionBundles.
            config: Configuration for the state machine.
            metrics_callback: Callback for metrics emission.
            on_sadflow_enter: Callback when entering sadflow state. Receives
                (error_type, attempt, context) and may return SadflowAction.
            on_sadflow_exit: Callback when exiting sadflow (on completion).
                Receives (success, total_attempts).
            on_retry: Callback before each retry. Receives (context, default_action)
                and returns the SadflowAction to use.
        """
        self.intent = intent
        self.compiler = compiler
        self.config = config or StateMachineConfig()
        self.metrics_callback = metrics_callback or default_metrics_callback

        # Sadflow lifecycle hooks
        self._on_sadflow_enter = on_sadflow_enter
        self._on_sadflow_exit = on_sadflow_exit
        self._on_retry = on_retry

        # State tracking
        self._state = IntentState.IDLE
        self._receipt: TransactionReceipt | None = None
        self._compilation_result: CompilationResult | None = None
        self._retry_count = 0
        self._last_error: str | None = None
        self._error_type: str | None = None
        self._in_sadflow = False  # Track if we've entered sadflow for exit callback

        # Modified action bundle from MODIFY action
        self._modified_bundle: ActionBundle | None = None

        # Timing
        self._started_at: datetime | None = None
        self._completed_at: datetime | None = None
        self._last_step_start: float | None = None

        # Initialize to PREPARING state
        self._transition_to(get_preparing_state(intent.intent_type))

    @property
    def state(self) -> IntentState:
        """Get the current state."""
        return self._state

    @property
    def is_complete(self) -> bool:
        """Check if the state machine has completed."""
        return self._state in {IntentState.COMPLETED, IntentState.FAILED}

    @property
    def success(self) -> bool:
        """Check if execution was successful."""
        return self._state == IntentState.COMPLETED

    @property
    def error(self) -> str | None:
        """Get the last error message."""
        return self._last_error

    @property
    def retry_count(self) -> int:
        """Get the current retry count."""
        return self._retry_count

    @property
    def action_bundle(self) -> ActionBundle | None:
        """Get the compiled ActionBundle."""
        # Return modified bundle if set (from MODIFY action)
        if self._modified_bundle is not None:
            return self._modified_bundle
        if self._compilation_result and self._compilation_result.action_bundle:
            return self._compilation_result.action_bundle
        return None

    def _build_sadflow_context(self) -> SadflowContext:
        """Build SadflowContext for hook callbacks.

        Returns:
            SadflowContext with current execution state.
        """
        total_duration = 0.0
        if self._started_at:
            total_duration = (datetime.now(UTC) - self._started_at).total_seconds()

        return SadflowContext(
            intent_id=self.intent.intent_id,
            intent_type=self.intent.intent_type.value,
            error_message=self._last_error or "Unknown error",
            error_type=self._error_type,
            attempt_number=self._retry_count + 1,  # 1-indexed for user
            max_attempts=self.config.retry_config.max_retries,
            action_bundle=self.action_bundle,
            receipt=self._receipt,
            state=self._state,
            started_at=self._started_at,
            total_duration_seconds=total_duration,
        )

    def _categorize_error(self, error_message: str) -> str | None:
        """Categorize an error message into a known error type.

        Args:
            error_message: The error message to categorize.

        Returns:
            Error type string or None if unknown.
        """
        error_lower = error_message.lower()

        # Common error categories
        if "insufficient" in error_lower and ("funds" in error_lower or "balance" in error_lower):
            return "INSUFFICIENT_FUNDS"
        if "gas" in error_lower and ("limit" in error_lower or "price" in error_lower):
            return "GAS_ERROR"
        if "nonce" in error_lower:
            return "NONCE_ERROR"
        if "timeout" in error_lower or "timed out" in error_lower:
            return "TIMEOUT"
        if "revert" in error_lower:
            return "REVERT"
        if "slippage" in error_lower:
            return "SLIPPAGE"
        if "rate limit" in error_lower or "ratelimit" in error_lower:
            return "RATE_LIMIT"
        if "connection" in error_lower or "network" in error_lower:
            return "NETWORK_ERROR"

        # Permanent configuration/support errors (non-retriable)
        # These indicate missing protocol support, unsupported chains, etc.
        # Placed last so transient errors (timeout, revert, network) are caught first.
        permanent_keywords = ("not supported", "unsupported", "feature not available")
        if any(kw in error_lower for kw in permanent_keywords):
            return "COMPILATION_PERMANENT"

        return None

    def set_receipt(self, receipt: TransactionReceipt) -> None:
        """Set the transaction receipt for validation.

        This should be called after executing the ActionBundle.

        Args:
            receipt: The transaction receipt.
        """
        self._receipt = receipt

    def _call_sadflow_exit(self, success: bool) -> None:
        """Call the on_sadflow_exit hook if set and we were in sadflow.

        Args:
            success: Whether the execution was successful.
        """
        if self._in_sadflow and self._on_sadflow_exit:
            try:
                self._on_sadflow_exit(success, self._retry_count + 1)
            except Exception as e:
                logger.warning(f"on_sadflow_exit hook raised exception: {e}")

    def step(self) -> StepResult:
        """Execute one step of the state machine.

        Returns:
            StepResult indicating what action is needed or completion status.
        """
        self._last_step_start = time.time()

        if self.is_complete:
            return StepResult(
                state=self._state,
                is_complete=True,
                success=self.success,
                error=self._last_error,
            )

        # Handle state-specific logic
        if is_preparing_state(self._state):
            return self._handle_preparing()
        elif is_validating_state(self._state):
            return self._handle_validating()
        elif is_sadflow_state(self._state):
            return self._handle_sadflow()
        else:
            # Unknown state - fail
            self._last_error = f"Unknown state: {self._state}"
            self._transition_to(IntentState.FAILED)
            return StepResult(
                state=self._state,
                is_complete=True,
                success=False,
                error=self._last_error,
            )

    def _handle_preparing(self) -> StepResult:
        """Handle PREPARING state - compile intent to ActionBundle.

        Returns:
            StepResult with ActionBundle if compilation succeeds.
        """
        # Mark start time
        if self._started_at is None:
            self._started_at = datetime.now(UTC)

        # Compile the intent
        try:
            self._compilation_result = self.compiler.compile(self.intent)
        except Exception as e:
            logger.exception(f"Compilation failed: {e}")
            self._last_error = f"Compilation error: {str(e)}"
            self._transition_to(get_sadflow_state(self.intent.intent_type))
            return StepResult(
                state=self._state,
                error=self._last_error,
            )

        # Check compilation status
        if self._compilation_result.status == CompilationStatus.FAILED:
            self._last_error = self._compilation_result.error
            self._transition_to(get_sadflow_state(self.intent.intent_type))
            return StepResult(
                state=self._state,
                error=self._last_error,
            )

        # Handle HOLD intents - they complete immediately
        if self.intent.intent_type == IntentType.HOLD:
            if self.config.auto_advance_hold:
                self._transition_to(IntentState.COMPLETED)
                return StepResult(
                    state=self._state,
                    is_complete=True,
                    success=True,
                )

        # Transition to validating state
        self._transition_to(get_validating_state(self.intent.intent_type))

        return StepResult(
            state=self._state,
            action_bundle=self._compilation_result.action_bundle,
            needs_execution=True,
        )

    def _handle_validating(self) -> StepResult:
        """Handle VALIDATING state - check transaction receipt.

        Returns:
            StepResult indicating success or need to transition to SADFLOW.
        """
        # Check if receipt has been provided
        if self._receipt is None:
            return StepResult(
                state=self._state,
                needs_execution=True,
                action_bundle=self.action_bundle,
            )

        # Validate the receipt
        if self._receipt.success:
            # Success! Complete the state machine
            self._completed_at = datetime.now(UTC)
            # Call sadflow exit hook if we recovered from sadflow
            self._call_sadflow_exit(success=True)
            self._transition_to(IntentState.COMPLETED)
            return StepResult(
                state=self._state,
                is_complete=True,
                success=True,
            )
        else:
            # Failure - transition to sadflow
            self._last_error = self._receipt.error or "Transaction failed"
            self._transition_to(get_sadflow_state(self.intent.intent_type))
            return StepResult(
                state=self._state,
                error=self._last_error,
            )

    def _handle_sadflow(self) -> StepResult:
        """Handle SADFLOW state - retry logic with lifecycle hooks.

        Calls on_sadflow_enter hook when first entering sadflow, then
        on_retry hook before each retry attempt. The hooks can customize
        retry behavior via SadflowAction.

        Returns:
            StepResult indicating retry or final failure.
        """
        # Categorize the error for hooks
        if self._last_error and not self._error_type:
            self._error_type = self._categorize_error(self._last_error)

        # Build context for hooks
        context = self._build_sadflow_context()

        # Call on_sadflow_enter hook if this is first time entering sadflow
        if not self._in_sadflow:
            self._in_sadflow = True
            if self._on_sadflow_enter:
                try:
                    hook_action = self._on_sadflow_enter(
                        self._error_type,
                        self._retry_count + 1,  # 1-indexed attempt
                        context,
                    )
                    if hook_action:
                        # Process the action from the hook
                        if hook_action.action_type == SadflowActionType.ABORT:
                            logger.info(f"Sadflow hook requested ABORT: {hook_action.reason or 'no reason'}")
                            self._completed_at = datetime.now(UTC)
                            self._call_sadflow_exit(success=False)
                            self._transition_to(IntentState.FAILED)
                            return StepResult(
                                state=self._state,
                                is_complete=True,
                                success=False,
                                error=hook_action.reason or self._last_error,
                            )
                        elif hook_action.action_type == SadflowActionType.SKIP:
                            logger.info(f"Sadflow hook requested SKIP: {hook_action.reason or 'no reason'}")
                            self._completed_at = datetime.now(UTC)
                            self._call_sadflow_exit(success=True)
                            self._transition_to(IntentState.COMPLETED)
                            return StepResult(
                                state=self._state,
                                is_complete=True,
                                success=True,
                            )
                        elif hook_action.action_type == SadflowActionType.MODIFY:
                            logger.info(f"Sadflow hook requested MODIFY: {hook_action.reason or 'modified bundle'}")
                            self._modified_bundle = hook_action.modified_bundle
                except Exception as e:
                    logger.warning(f"on_sadflow_enter hook raised exception: {e}")

        # Check if we have retries left
        if self._retry_count >= self.config.retry_config.max_retries:
            # Exhausted retries - fail
            self._completed_at = datetime.now(UTC)
            self._call_sadflow_exit(success=False)
            self._transition_to(IntentState.FAILED)
            return StepResult(
                state=self._state,
                is_complete=True,
                success=False,
                error=self._last_error or "Max retries exceeded",
            )

        # Calculate retry delay
        delay = self.config.retry_config.calculate_delay(self._retry_count)

        # Build default retry action
        default_action = SadflowAction.retry(custom_delay=delay)

        # Call on_retry hook to allow customization
        final_action = default_action
        if self._on_retry:
            try:
                final_action = self._on_retry(context, default_action)
            except Exception as e:
                logger.warning(f"on_retry hook raised exception: {e}")
                final_action = default_action

        # Process the final action
        if final_action.action_type == SadflowActionType.ABORT:
            logger.info(f"on_retry hook requested ABORT: {final_action.reason or 'no reason'}")
            self._completed_at = datetime.now(UTC)
            self._call_sadflow_exit(success=False)
            self._transition_to(IntentState.FAILED)
            return StepResult(
                state=self._state,
                is_complete=True,
                success=False,
                error=final_action.reason or self._last_error,
            )
        elif final_action.action_type == SadflowActionType.SKIP:
            logger.info(f"on_retry hook requested SKIP: {final_action.reason or 'no reason'}")
            self._completed_at = datetime.now(UTC)
            self._call_sadflow_exit(success=True)
            self._transition_to(IntentState.COMPLETED)
            return StepResult(
                state=self._state,
                is_complete=True,
                success=True,
            )
        elif final_action.action_type == SadflowActionType.MODIFY:
            logger.info(f"on_retry hook requested MODIFY: {final_action.reason or 'modified bundle'}")
            self._modified_bundle = final_action.modified_bundle

        # Use custom delay if provided
        if final_action.custom_delay is not None:
            delay = final_action.custom_delay

        # Increment retry count
        self._retry_count += 1

        # Clear receipt for retry
        self._receipt = None

        # Transition back to preparing
        self._transition_to(get_preparing_state(self.intent.intent_type))

        logger.info(
            f"Retrying intent {self.intent.intent_id} "
            f"(attempt {self._retry_count}/{self.config.retry_config.max_retries}, "
            f"delay={delay:.2f}s)"
        )

        return StepResult(
            state=self._state,
            retry_delay=delay,
        )

    def _transition_to(self, new_state: IntentState) -> None:
        """Transition to a new state with metrics emission.

        Args:
            new_state: The state to transition to.
        """
        old_state = self._state

        # Calculate duration if we have a step start time
        duration_ms = 0.0
        if self._last_step_start is not None:
            duration_ms = (time.time() - self._last_step_start) * 1000

        # Update state
        self._state = new_state

        # Emit metrics if enabled
        if self.config.emit_metrics:
            metric = StateTransitionMetric(
                intent_id=self.intent.intent_id,
                intent_type=self.intent.intent_type.value,
                from_state=old_state.name,
                to_state=new_state.name,
                timestamp=datetime.now(UTC),
                duration_ms=duration_ms,
                success=new_state != IntentState.FAILED,
                error=self._last_error if new_state == IntentState.FAILED else None,
                metadata={
                    "retry_count": self._retry_count,
                },
            )
            self.metrics_callback(metric)

        logger.debug(f"State transition: {old_state.name} -> {new_state.name} (intent={self.intent.intent_id})")

    def reset(self) -> None:
        """Reset the state machine to initial state.

        This allows re-execution of the same intent.
        """
        self._state = IntentState.IDLE
        self._receipt = None
        self._compilation_result = None
        self._retry_count = 0
        self._last_error = None
        self._error_type = None
        self._in_sadflow = False
        self._modified_bundle = None
        self._started_at = None
        self._completed_at = None

        # Re-initialize to PREPARING state
        self._transition_to(get_preparing_state(self.intent.intent_type))

    def to_dict(self) -> dict[str, Any]:
        """Serialize state machine to dictionary.

        Returns:
            Dictionary representation of the state machine.
        """
        return {
            "intent_id": self.intent.intent_id,
            "intent_type": self.intent.intent_type.value,
            "state": self._state.name,
            "is_complete": self.is_complete,
            "success": self.success,
            "error": self._last_error,
            "retry_count": self._retry_count,
            "started_at": self._started_at.isoformat() if self._started_at else None,
            "completed_at": self._completed_at.isoformat() if self._completed_at else None,
            "compilation_result": self._compilation_result.to_dict() if self._compilation_result else None,
        }


# =============================================================================
# Factory Functions
# =============================================================================


def create_state_machine(
    intent: AnyIntent,
    compiler: IntentCompiler,
    max_retries: int = 3,
    initial_delay: float = 1.0,
    emit_metrics: bool = True,
) -> IntentStateMachine:
    """Create a state machine with common configuration.

    This is a convenience function for creating state machines with
    typical settings.

    Args:
        intent: The intent to execute.
        compiler: The compiler to use.
        max_retries: Maximum number of retries on failure.
        initial_delay: Initial delay between retries in seconds.
        emit_metrics: Whether to emit metrics.

    Returns:
        Configured IntentStateMachine.
    """
    config = StateMachineConfig(
        retry_config=RetryConfig(
            max_retries=max_retries,
            initial_delay_seconds=initial_delay,
        ),
        emit_metrics=emit_metrics,
    )
    return IntentStateMachine(intent, compiler, config)


def generate_state_diagram(intent_type: IntentType) -> str:
    """Generate a text-based state diagram for an intent type.

    Args:
        intent_type: The intent type to generate diagram for.

    Returns:
        Text representation of the state diagram.
    """
    intent_name = intent_type.value

    return f"""
State Machine for {intent_name}:

  ┌──────────────────────────────────────────────────────────────┐
  │                                                              │
  │  ┌─────────┐         ┌──────────────────┐                   │
  │  │  IDLE   │────────▶│ PREPARING_{intent_name:8}│                   │
  │  └─────────┘         └──────────────────┘                   │
  │                               │                              │
  │                               │ (compile intent)             │
  │                               ▼                              │
  │                      ┌──────────────────┐                   │
  │                      │VALIDATING_{intent_name:8}│                   │
  │                      └──────────────────┘                   │
  │                        │              │                      │
  │                success │              │ failure              │
  │                        ▼              ▼                      │
  │               ┌───────────┐  ┌──────────────────┐           │
  │               │ COMPLETED │  │  SADFLOW_{intent_name:8} │           │
  │               └───────────┘  └──────────────────┘           │
  │                                       │                      │
  │                         retry         │  max retries         │
  │                    ┌──────────────────┘  exceeded            │
  │                    │                     ▼                   │
  │                    │              ┌──────────┐               │
  │                    │              │  FAILED  │               │
  │                    ▼              └──────────┘               │
  │           ┌──────────────────┐                              │
  │           │ PREPARING_{intent_name:8}│ (retry loop)                 │
  │           └──────────────────┘                              │
  │                                                              │
  └──────────────────────────────────────────────────────────────┘

States:
  - PREPARING_{intent_name}: Compile intent to ActionBundle
  - VALIDATING_{intent_name}: Validate transaction receipt
  - SADFLOW_{intent_name}: Handle failure, retry with backoff
  - COMPLETED: Intent executed successfully
  - FAILED: All retries exhausted, intent failed
"""


# =============================================================================
# Exports
# =============================================================================


__all__ = [
    # State enum and helpers
    "IntentState",
    "get_preparing_state",
    "get_validating_state",
    "get_sadflow_state",
    "is_preparing_state",
    "is_validating_state",
    "is_sadflow_state",
    # Configuration
    "RetryConfig",
    "StateMachineConfig",
    # Types
    "TransactionReceipt",
    "StepResult",
    "StateTransitionMetric",
    "MetricsCallback",
    # Sadflow hooks types
    "SadflowActionType",
    "SadflowAction",
    "SadflowContext",
    "SadflowEnterCallback",
    "SadflowExitCallback",
    "SadflowRetryCallback",
    # State machine
    "IntentStateMachine",
    "create_state_machine",
    "generate_state_diagram",
    # Metrics
    "default_metrics_callback",
    "get_metrics",
    "clear_metrics",
]
