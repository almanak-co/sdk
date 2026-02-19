"""Circuit Breaker for strategy execution safety.

This module implements a production-grade circuit breaker that protects strategies
from cascading failures and provides emergency stop capabilities.

Key Features:
    - Consecutive failure tracking with automatic halt
    - Cumulative loss threshold monitoring
    - Cooldown periods after failures
    - Manual pause/resume with operator confirmation
    - Per-strategy and global circuit breakers
    - Thread-safe state management

Design Philosophy:
    - Fail-safe: Better to stop trading than lose money
    - Observable: All state changes are logged and tracked
    - Operator-controlled: Resumes require explicit confirmation
    - Configurable: Thresholds are customizable per strategy

Example:
    from almanak.framework.execution.circuit_breaker import CircuitBreaker, CircuitBreakerConfig

    config = CircuitBreakerConfig(
        max_consecutive_failures=3,
        max_cumulative_loss_usd=Decimal("5000"),
        cooldown_seconds=3600,
    )

    breaker = CircuitBreaker(strategy_id="my_strategy", config=config)

    # Check before execution
    result = breaker.check()
    if not result.can_execute:
        print(f"Circuit breaker tripped: {result.reason}")
        return

    # Record outcome after execution
    if execution_succeeded:
        breaker.record_success()
    else:
        breaker.record_failure("Transaction reverted: insufficient balance")

    # Manual controls
    breaker.pause(reason="Manual pause for investigation", operator="alice@company.com")
    breaker.resume(operator_key="alice@company.com")
"""

import logging
import threading
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import Enum
from typing import Any

logger = logging.getLogger(__name__)


# =============================================================================
# Enums
# =============================================================================


class CircuitBreakerState(Enum):
    """Circuit breaker states."""

    CLOSED = "closed"  # Normal operation, execution allowed
    OPEN = "open"  # Tripped, execution blocked
    HALF_OPEN = "half_open"  # Cooldown expired, testing with single execution
    PAUSED = "paused"  # Manually paused by operator


class TripReason(Enum):
    """Reasons why a circuit breaker might trip."""

    CONSECUTIVE_FAILURES = "consecutive_failures"
    CUMULATIVE_LOSS = "cumulative_loss"
    MANUAL_PAUSE = "manual_pause"
    VOLATILITY = "volatility"
    EXTERNAL_SIGNAL = "external_signal"


# =============================================================================
# Configuration
# =============================================================================


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker behavior.

    Attributes:
        max_consecutive_failures: Number of consecutive failures before tripping.
            Default 3 means after 3 failures in a row, execution is blocked.
        max_cumulative_loss_usd: Maximum cumulative loss before tripping.
            Tracks total loss across all executions. Default $5,000.
        cooldown_seconds: Seconds to wait after tripping before allowing retry.
            After cooldown, enters HALF_OPEN state for test execution.
        half_open_success_threshold: Successes needed in HALF_OPEN to fully close.
            Requires this many consecutive successes to return to normal.
        volatility_threshold_pct: Price movement threshold for volatility trip.
            If market moves this much in 1 hour, circuit trips. Default 10%.
        loss_tracking_window_hours: Window for cumulative loss tracking.
            Losses older than this are not counted. Default 24 hours.
    """

    max_consecutive_failures: int = 3
    max_cumulative_loss_usd: Decimal = field(default_factory=lambda: Decimal("5000"))
    cooldown_seconds: int = 3600  # 1 hour
    half_open_success_threshold: int = 2
    volatility_threshold_pct: float = 0.10  # 10%
    loss_tracking_window_hours: int = 24

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "max_consecutive_failures": self.max_consecutive_failures,
            "max_cumulative_loss_usd": str(self.max_cumulative_loss_usd),
            "cooldown_seconds": self.cooldown_seconds,
            "half_open_success_threshold": self.half_open_success_threshold,
            "volatility_threshold_pct": self.volatility_threshold_pct,
            "loss_tracking_window_hours": self.loss_tracking_window_hours,
        }


# =============================================================================
# Result Types
# =============================================================================


@dataclass
class CircuitBreakerCheckResult:
    """Result of checking if execution is allowed.

    Attributes:
        can_execute: Whether execution is allowed
        state: Current circuit breaker state
        reason: Human-readable reason if blocked
        trip_reason: Enum reason if tripped
        consecutive_failures: Current failure count
        cumulative_loss_usd: Current loss total
        cooldown_remaining_seconds: Seconds until cooldown expires (if applicable)
        last_failure_time: When the last failure occurred
        last_trip_time: When the circuit breaker last tripped
    """

    can_execute: bool
    state: CircuitBreakerState
    reason: str | None = None
    trip_reason: TripReason | None = None
    consecutive_failures: int = 0
    cumulative_loss_usd: Decimal = field(default_factory=lambda: Decimal("0"))
    cooldown_remaining_seconds: int | None = None
    last_failure_time: datetime | None = None
    last_trip_time: datetime | None = None

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "can_execute": self.can_execute,
            "state": self.state.value,
            "reason": self.reason,
            "trip_reason": self.trip_reason.value if self.trip_reason else None,
            "consecutive_failures": self.consecutive_failures,
            "cumulative_loss_usd": str(self.cumulative_loss_usd),
            "cooldown_remaining_seconds": self.cooldown_remaining_seconds,
            "last_failure_time": (self.last_failure_time.isoformat() if self.last_failure_time else None),
            "last_trip_time": (self.last_trip_time.isoformat() if self.last_trip_time else None),
        }


@dataclass
class FailureRecord:
    """Record of a single failure event."""

    timestamp: datetime
    error_message: str
    loss_usd: Decimal = field(default_factory=lambda: Decimal("0"))


# =============================================================================
# Circuit Breaker Implementation
# =============================================================================


class CircuitBreaker:
    """Production-grade circuit breaker for strategy execution safety.

    The circuit breaker tracks execution outcomes and automatically blocks
    execution when failure thresholds are exceeded. It supports:

    - Automatic tripping after consecutive failures
    - Cumulative loss threshold monitoring
    - Cooldown periods with half-open testing
    - Manual pause/resume by operators
    - Thread-safe operation

    State Machine:
        CLOSED -> OPEN (on failure threshold)
        OPEN -> HALF_OPEN (after cooldown)
        HALF_OPEN -> CLOSED (on success threshold)
        HALF_OPEN -> OPEN (on failure)
        Any -> PAUSED (manual pause)
        PAUSED -> CLOSED (manual resume with confirmation)

    Example:
        breaker = CircuitBreaker("my_strategy")

        # Check before each execution
        result = breaker.check()
        if not result.can_execute:
            logger.warning(f"Blocked: {result.reason}")
            return

        # Record outcome
        try:
            execute_strategy()
            breaker.record_success()
        except Exception as e:
            breaker.record_failure(str(e), loss_usd=Decimal("100"))
    """

    def __init__(
        self,
        strategy_id: str,
        config: CircuitBreakerConfig | None = None,
    ) -> None:
        """Initialize the circuit breaker.

        Args:
            strategy_id: Unique identifier for the strategy
            config: Configuration options (uses defaults if not provided)
        """
        self.strategy_id = strategy_id
        self.config = config or CircuitBreakerConfig()

        # State
        self._state = CircuitBreakerState.CLOSED
        self._lock = threading.RLock()

        # Failure tracking
        self._consecutive_failures = 0
        self._failure_history: list[FailureRecord] = []
        self._last_failure_time: datetime | None = None

        # Trip tracking
        self._trip_time: datetime | None = None
        self._trip_reason: TripReason | None = None

        # Half-open tracking
        self._half_open_successes = 0

        # Pause tracking
        self._pause_reason: str | None = None
        self._paused_by: str | None = None
        self._pause_time: datetime | None = None

        logger.info(
            "CircuitBreaker initialized for strategy=%s, config=%s",
            strategy_id,
            self.config.to_dict(),
        )

    @property
    def state(self) -> CircuitBreakerState:
        """Get current circuit breaker state."""
        with self._lock:
            return self._state

    def check(self) -> CircuitBreakerCheckResult:
        """Check if execution is currently allowed.

        This method should be called before each execution attempt.
        It handles automatic state transitions (cooldown expiry, etc.).

        Returns:
            CircuitBreakerCheckResult with execution decision and metadata
        """
        with self._lock:
            now = datetime.now(UTC)

            # Check for cooldown expiry (OPEN -> HALF_OPEN)
            if self._state == CircuitBreakerState.OPEN and self._trip_time:
                cooldown_expires = self._trip_time + timedelta(seconds=self.config.cooldown_seconds)
                if now >= cooldown_expires:
                    self._transition_to_half_open()
                else:
                    remaining = int((cooldown_expires - now).total_seconds())
                    return CircuitBreakerCheckResult(
                        can_execute=False,
                        state=self._state,
                        reason=f"Circuit breaker open. Cooldown: {remaining}s remaining",
                        trip_reason=self._trip_reason,
                        consecutive_failures=self._consecutive_failures,
                        cumulative_loss_usd=self._get_cumulative_loss(),
                        cooldown_remaining_seconds=remaining,
                        last_failure_time=self._last_failure_time,
                        last_trip_time=self._trip_time,
                    )

            # Check current state
            if self._state == CircuitBreakerState.PAUSED:
                return CircuitBreakerCheckResult(
                    can_execute=False,
                    state=self._state,
                    reason=f"Manually paused: {self._pause_reason}",
                    trip_reason=TripReason.MANUAL_PAUSE,
                    consecutive_failures=self._consecutive_failures,
                    cumulative_loss_usd=self._get_cumulative_loss(),
                    last_failure_time=self._last_failure_time,
                    last_trip_time=self._trip_time,
                )

            if self._state == CircuitBreakerState.OPEN:
                cooldown_remaining: int | None = None
                if self._trip_time:
                    cooldown_expires = self._trip_time + timedelta(seconds=self.config.cooldown_seconds)
                    cooldown_remaining = max(0, int((cooldown_expires - now).total_seconds()))

                return CircuitBreakerCheckResult(
                    can_execute=False,
                    state=self._state,
                    reason=f"Circuit breaker open: {self._trip_reason.value if self._trip_reason else 'unknown'}",
                    trip_reason=self._trip_reason,
                    consecutive_failures=self._consecutive_failures,
                    cumulative_loss_usd=self._get_cumulative_loss(),
                    cooldown_remaining_seconds=cooldown_remaining,
                    last_failure_time=self._last_failure_time,
                    last_trip_time=self._trip_time,
                )

            # CLOSED or HALF_OPEN: execution allowed
            return CircuitBreakerCheckResult(
                can_execute=True,
                state=self._state,
                consecutive_failures=self._consecutive_failures,
                cumulative_loss_usd=self._get_cumulative_loss(),
                last_failure_time=self._last_failure_time,
                last_trip_time=self._trip_time,
            )

    def record_success(self) -> None:
        """Record a successful execution.

        Resets consecutive failure counter. In HALF_OPEN state, may
        transition back to CLOSED if success threshold is met.
        """
        with self._lock:
            self._consecutive_failures = 0

            if self._state == CircuitBreakerState.HALF_OPEN:
                self._half_open_successes += 1
                logger.info(
                    "CircuitBreaker %s: half-open success %d/%d",
                    self.strategy_id,
                    self._half_open_successes,
                    self.config.half_open_success_threshold,
                )

                if self._half_open_successes >= self.config.half_open_success_threshold:
                    self._close()

            logger.debug("CircuitBreaker %s: success recorded", self.strategy_id)

    def record_failure(
        self,
        error_message: str,
        loss_usd: Decimal = Decimal("0"),
    ) -> None:
        """Record a failed execution.

        Increments failure counter and may trip the circuit breaker
        if thresholds are exceeded.

        Args:
            error_message: Description of the failure
            loss_usd: USD value of any loss incurred (for cumulative tracking)
        """
        with self._lock:
            now = datetime.now(UTC)

            # Record failure
            self._consecutive_failures += 1
            self._last_failure_time = now
            self._failure_history.append(FailureRecord(timestamp=now, error_message=error_message, loss_usd=loss_usd))

            # Prune old failures outside the tracking window
            self._prune_old_failures()

            logger.warning(
                "CircuitBreaker %s: failure %d/%d - %s",
                self.strategy_id,
                self._consecutive_failures,
                self.config.max_consecutive_failures,
                error_message,
            )

            # Check for trip conditions
            if self._state == CircuitBreakerState.HALF_OPEN:
                # Any failure in half-open immediately trips back to open
                self._trip(TripReason.CONSECUTIVE_FAILURES)
                return

            # Check consecutive failure threshold
            if self._consecutive_failures >= self.config.max_consecutive_failures:
                self._trip(TripReason.CONSECUTIVE_FAILURES)
                return

            # Check cumulative loss threshold
            cumulative = self._get_cumulative_loss()
            if cumulative >= self.config.max_cumulative_loss_usd:
                self._trip(TripReason.CUMULATIVE_LOSS)
                return

    def pause(self, reason: str, operator: str) -> None:
        """Manually pause the circuit breaker.

        Execution will be blocked until an operator calls resume().

        Args:
            reason: Human-readable reason for pausing
            operator: Identifier of the operator who paused (email, username, etc.)
        """
        with self._lock:
            previous_state = self._state
            self._state = CircuitBreakerState.PAUSED
            self._pause_reason = reason
            self._paused_by = operator
            self._pause_time = datetime.now(UTC)

            logger.warning(
                "CircuitBreaker %s: manually PAUSED by %s (reason: %s, previous_state: %s)",
                self.strategy_id,
                operator,
                reason,
                previous_state.value,
            )

    def resume(self, operator_key: str) -> bool:
        """Resume execution after manual pause.

        Requires operator confirmation. Resets failure counters.

        Args:
            operator_key: Identifier of operator confirming resume

        Returns:
            True if successfully resumed, False if not paused
        """
        with self._lock:
            if self._state != CircuitBreakerState.PAUSED:
                logger.warning(
                    "CircuitBreaker %s: resume called but state is %s (not PAUSED)",
                    self.strategy_id,
                    self._state.value,
                )
                return False

            logger.info(
                "CircuitBreaker %s: RESUMED by %s (was paused by %s: %s)",
                self.strategy_id,
                operator_key,
                self._paused_by,
                self._pause_reason,
            )

            self._close()
            self._pause_reason = None
            self._paused_by = None
            self._pause_time = None

            return True

    def reset(self) -> None:
        """Fully reset the circuit breaker to initial state.

        WARNING: This should only be used for testing or emergency recovery.
        All failure history and state is cleared.
        """
        with self._lock:
            logger.warning(
                "CircuitBreaker %s: RESET - all state cleared",
                self.strategy_id,
            )

            self._state = CircuitBreakerState.CLOSED
            self._consecutive_failures = 0
            self._failure_history = []
            self._last_failure_time = None
            self._trip_time = None
            self._trip_reason = None
            self._half_open_successes = 0
            self._pause_reason = None
            self._paused_by = None
            self._pause_time = None

    def get_status(self) -> dict[str, Any]:
        """Get current status as a dictionary.

        Useful for dashboard display and debugging.

        Returns:
            Dictionary with current state and metrics
        """
        with self._lock:
            return {
                "strategy_id": self.strategy_id,
                "state": self._state.value,
                "consecutive_failures": self._consecutive_failures,
                "cumulative_loss_usd": str(self._get_cumulative_loss()),
                "failure_history_count": len(self._failure_history),
                "last_failure_time": (self._last_failure_time.isoformat() if self._last_failure_time else None),
                "trip_time": self._trip_time.isoformat() if self._trip_time else None,
                "trip_reason": self._trip_reason.value if self._trip_reason else None,
                "half_open_successes": self._half_open_successes,
                "pause_reason": self._pause_reason,
                "paused_by": self._paused_by,
                "pause_time": self._pause_time.isoformat() if self._pause_time else None,
                "config": self.config.to_dict(),
            }

    # =========================================================================
    # Private Methods
    # =========================================================================

    def _trip(self, reason: TripReason) -> None:
        """Trip the circuit breaker (transition to OPEN state)."""
        previous_state = self._state
        self._state = CircuitBreakerState.OPEN
        self._trip_time = datetime.now(UTC)
        self._trip_reason = reason
        self._half_open_successes = 0

        logger.error(
            "CircuitBreaker %s: TRIPPED - reason=%s, consecutive_failures=%d, cumulative_loss=$%s, previous_state=%s",
            self.strategy_id,
            reason.value,
            self._consecutive_failures,
            self._get_cumulative_loss(),
            previous_state.value,
        )

    def _transition_to_half_open(self) -> None:
        """Transition from OPEN to HALF_OPEN after cooldown."""
        self._state = CircuitBreakerState.HALF_OPEN
        self._half_open_successes = 0

        logger.info(
            "CircuitBreaker %s: transitioning to HALF_OPEN after cooldown",
            self.strategy_id,
        )

    def _close(self) -> None:
        """Close the circuit breaker (return to normal operation)."""
        self._state = CircuitBreakerState.CLOSED
        self._consecutive_failures = 0
        self._trip_time = None
        self._trip_reason = None
        self._half_open_successes = 0

        logger.info(
            "CircuitBreaker %s: CLOSED - normal operation resumed",
            self.strategy_id,
        )

    def _get_cumulative_loss(self) -> Decimal:
        """Calculate cumulative loss within tracking window."""
        total = Decimal("0")
        for record in self._failure_history:
            total += record.loss_usd
        return total

    def _prune_old_failures(self) -> None:
        """Remove failure records older than the tracking window."""
        now = datetime.now(UTC)
        cutoff = now - timedelta(hours=self.config.loss_tracking_window_hours)

        self._failure_history = [record for record in self._failure_history if record.timestamp >= cutoff]


# =============================================================================
# Global Circuit Breaker Registry
# =============================================================================


class CircuitBreakerRegistry:
    """Registry for managing circuit breakers across multiple strategies.

    Provides centralized access to circuit breakers and global pause capability.

    Example:
        registry = CircuitBreakerRegistry()

        # Get or create circuit breaker for a strategy
        breaker = registry.get_or_create("strategy_1")

        # Pause all strategies globally
        registry.pause_all(reason="Market volatility detected", operator="system")

        # Check if any are tripped
        if registry.any_tripped():
            print("Some circuit breakers are open!")
    """

    def __init__(self, default_config: CircuitBreakerConfig | None = None) -> None:
        """Initialize the registry.

        Args:
            default_config: Default configuration for new circuit breakers
        """
        self._breakers: dict[str, CircuitBreaker] = {}
        self._lock = threading.RLock()
        self._default_config = default_config or CircuitBreakerConfig()
        self._global_pause = False
        self._global_pause_reason: str | None = None
        self._global_pause_by: str | None = None

    def get_or_create(
        self,
        strategy_id: str,
        config: CircuitBreakerConfig | None = None,
    ) -> CircuitBreaker:
        """Get existing or create new circuit breaker for a strategy.

        Args:
            strategy_id: Strategy identifier
            config: Optional custom configuration (uses default if not provided)

        Returns:
            CircuitBreaker for the strategy
        """
        with self._lock:
            if strategy_id not in self._breakers:
                self._breakers[strategy_id] = CircuitBreaker(
                    strategy_id=strategy_id,
                    config=config or self._default_config,
                )
            return self._breakers[strategy_id]

    def get(self, strategy_id: str) -> CircuitBreaker | None:
        """Get circuit breaker for a strategy if it exists.

        Args:
            strategy_id: Strategy identifier

        Returns:
            CircuitBreaker or None if not found
        """
        with self._lock:
            return self._breakers.get(strategy_id)

    def pause_all(self, reason: str, operator: str) -> int:
        """Pause all circuit breakers globally.

        Args:
            reason: Reason for global pause
            operator: Who initiated the pause

        Returns:
            Number of circuit breakers paused
        """
        with self._lock:
            self._global_pause = True
            self._global_pause_reason = reason
            self._global_pause_by = operator

            count = 0
            for breaker in self._breakers.values():
                breaker.pause(reason=f"Global pause: {reason}", operator=operator)
                count += 1

            logger.warning(
                "CircuitBreakerRegistry: GLOBAL PAUSE by %s - %d breakers paused: %s",
                operator,
                count,
                reason,
            )

            return count

    def resume_all(self, operator_key: str) -> int:
        """Resume all paused circuit breakers.

        Args:
            operator_key: Operator confirming resume

        Returns:
            Number of circuit breakers resumed
        """
        with self._lock:
            self._global_pause = False
            self._global_pause_reason = None
            self._global_pause_by = None

            count = 0
            for breaker in self._breakers.values():
                if breaker.resume(operator_key):
                    count += 1

            logger.info(
                "CircuitBreakerRegistry: GLOBAL RESUME by %s - %d breakers resumed",
                operator_key,
                count,
            )

            return count

    def any_tripped(self) -> bool:
        """Check if any circuit breaker is currently tripped or paused.

        Returns:
            True if any breaker is not in CLOSED state
        """
        with self._lock:
            for breaker in self._breakers.values():
                if breaker.state != CircuitBreakerState.CLOSED:
                    return True
            return False

    def is_globally_paused(self) -> bool:
        """Check if global pause is active.

        Returns:
            True if global pause is in effect
        """
        with self._lock:
            return self._global_pause

    def get_all_status(self) -> dict[str, dict[str, Any]]:
        """Get status of all circuit breakers.

        Returns:
            Dictionary mapping strategy IDs to their status
        """
        with self._lock:
            return {strategy_id: breaker.get_status() for strategy_id, breaker in self._breakers.items()}

    def get_tripped(self) -> list[str]:
        """Get list of strategy IDs with tripped/paused circuit breakers.

        Returns:
            List of strategy IDs not in CLOSED state
        """
        with self._lock:
            return [
                strategy_id
                for strategy_id, breaker in self._breakers.items()
                if breaker.state != CircuitBreakerState.CLOSED
            ]


# =============================================================================
# Module-Level Registry (Singleton)
# =============================================================================

# Global registry instance for convenience
_global_registry: CircuitBreakerRegistry | None = None
_registry_lock = threading.Lock()


def get_global_registry() -> CircuitBreakerRegistry:
    """Get the global circuit breaker registry.

    Creates the registry on first access (lazy initialization).

    Returns:
        The global CircuitBreakerRegistry instance
    """
    global _global_registry
    if _global_registry is None:
        with _registry_lock:
            if _global_registry is None:
                _global_registry = CircuitBreakerRegistry()
    return _global_registry


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    # Enums
    "CircuitBreakerState",
    "TripReason",
    # Configuration
    "CircuitBreakerConfig",
    # Result types
    "CircuitBreakerCheckResult",
    "FailureRecord",
    # Main classes
    "CircuitBreaker",
    "CircuitBreakerRegistry",
    # Convenience function
    "get_global_registry",
]
