"""Error classification and circuit breaker for backtesting.

This module provides error classification and circuit breaker protection
for the backtesting system, enabling robust handling of external API failures
while continuing backtest execution when possible.

Key Features:
    - Error classification: recoverable vs fatal errors
    - Circuit breaker: trips after N consecutive failures (default 5)
    - Continue-with-warning: non-critical errors allow backtest to proceed
    - Detailed error tracking with timestamps and context

Design Philosophy:
    - Rate limits and timeouts are recoverable - retry with backoff
    - Invalid config and data corruption are fatal - stop immediately
    - Missing data for single ticks is non-critical - continue with warning
    - Track all errors for post-analysis and debugging

Example:
    from almanak.framework.backtesting.pnl.error_handling import (
        BacktestErrorHandler,
        BacktestErrorConfig,
        classify_error,
    )

    config = BacktestErrorConfig(circuit_breaker_threshold=5)
    handler = BacktestErrorHandler(config)

    try:
        price = await fetch_price(token)
    except Exception as e:
        result = handler.handle_error(e, context="price_fetch")
        if result.should_stop:
            raise BacktestFatalError(f"Fatal error: {e}")
        # Continue with fallback/cached value
        price = handler.get_fallback_price(token)
"""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import Enum
from typing import Any

logger = logging.getLogger(__name__)


# =============================================================================
# Error Classification
# =============================================================================


class ErrorCategory(Enum):
    """Categories of errors for classification."""

    RECOVERABLE = "recoverable"  # Can retry: rate limits, timeouts, network errors
    FATAL = "fatal"  # Must stop: invalid config, data corruption
    NON_CRITICAL = "non_critical"  # Can continue: missing single data point


class ErrorType(Enum):
    """Specific error types with associated categories."""

    # Recoverable errors - retry with backoff
    RATE_LIMIT = "rate_limit"
    TIMEOUT = "timeout"
    CONNECTION_ERROR = "connection_error"
    TEMPORARY_UNAVAILABLE = "temporary_unavailable"
    RPC_ERROR = "rpc_error"

    # Fatal errors - stop backtest
    INVALID_CONFIG = "invalid_config"
    DATA_CORRUPTION = "data_corruption"
    AUTHENTICATION_FAILED = "authentication_failed"
    INSUFFICIENT_FUNDS = "insufficient_funds"
    INVALID_STATE = "invalid_state"

    # Non-critical errors - continue with warning
    MISSING_PRICE = "missing_price"
    STALE_DATA = "stale_data"
    PARTIAL_DATA = "partial_data"
    OPTIONAL_FEATURE_FAILED = "optional_feature_failed"

    # Unknown
    UNKNOWN = "unknown"


# Map error types to categories
ERROR_CATEGORY_MAP: dict[ErrorType, ErrorCategory] = {
    # Recoverable
    ErrorType.RATE_LIMIT: ErrorCategory.RECOVERABLE,
    ErrorType.TIMEOUT: ErrorCategory.RECOVERABLE,
    ErrorType.CONNECTION_ERROR: ErrorCategory.RECOVERABLE,
    ErrorType.TEMPORARY_UNAVAILABLE: ErrorCategory.RECOVERABLE,
    ErrorType.RPC_ERROR: ErrorCategory.RECOVERABLE,
    # Fatal
    ErrorType.INVALID_CONFIG: ErrorCategory.FATAL,
    ErrorType.DATA_CORRUPTION: ErrorCategory.FATAL,
    ErrorType.AUTHENTICATION_FAILED: ErrorCategory.FATAL,
    ErrorType.INSUFFICIENT_FUNDS: ErrorCategory.FATAL,
    ErrorType.INVALID_STATE: ErrorCategory.FATAL,
    # Non-critical
    ErrorType.MISSING_PRICE: ErrorCategory.NON_CRITICAL,
    ErrorType.STALE_DATA: ErrorCategory.NON_CRITICAL,
    ErrorType.PARTIAL_DATA: ErrorCategory.NON_CRITICAL,
    ErrorType.OPTIONAL_FEATURE_FAILED: ErrorCategory.NON_CRITICAL,
    # Unknown defaults to fatal for safety
    ErrorType.UNKNOWN: ErrorCategory.FATAL,
}


@dataclass
class ErrorClassification:
    """Result of error classification.

    Attributes:
        error_type: The classified error type
        category: The error category (recoverable, fatal, non_critical)
        is_recoverable: True if error can be retried
        is_fatal: True if backtest must stop
        is_non_critical: True if backtest can continue with warning
        suggested_action: Human-readable suggested action
    """

    error_type: ErrorType
    category: ErrorCategory
    is_recoverable: bool = False
    is_fatal: bool = False
    is_non_critical: bool = False
    suggested_action: str = ""

    def __post_init__(self) -> None:
        """Set boolean flags based on category."""
        self.is_recoverable = self.category == ErrorCategory.RECOVERABLE
        self.is_fatal = self.category == ErrorCategory.FATAL
        self.is_non_critical = self.category == ErrorCategory.NON_CRITICAL

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "error_type": self.error_type.value,
            "category": self.category.value,
            "is_recoverable": self.is_recoverable,
            "is_fatal": self.is_fatal,
            "is_non_critical": self.is_non_critical,
            "suggested_action": self.suggested_action,
        }


class PreflightValidationError(Exception):
    """Raised when preflight validation fails and fail_on_preflight_error=True.

    This exception provides actionable error messages with details about:
    - What failed: Specific checks that did not pass
    - Why it failed: The underlying cause of each failure
    - How to fix: Recommendations for resolving the issues

    Attributes:
        message: Summary of the validation failure
        failed_checks: List of check names that failed
        recommendations: List of actionable recommendations
        error_count: Number of error-severity check failures
        warning_count: Number of warning-severity check failures

    Example:
        try:
            result = await backtester.run(config, strategy)
        except PreflightValidationError as e:
            print(f"Preflight failed: {e}")
            print(f"Failed checks: {e.failed_checks}")
            print(f"To fix: {e.recommendations}")
    """

    def __init__(
        self,
        message: str,
        failed_checks: list[str] | None = None,
        recommendations: list[str] | None = None,
        error_count: int = 0,
        warning_count: int = 0,
    ) -> None:
        """Initialize PreflightValidationError.

        Args:
            message: Summary error message
            failed_checks: List of check names that failed
            recommendations: List of actionable recommendations
            error_count: Number of error-severity failures
            warning_count: Number of warning-severity failures
        """
        super().__init__(message)
        self.message = message
        self.failed_checks = failed_checks or []
        self.recommendations = recommendations or []
        self.error_count = error_count
        self.warning_count = warning_count

    def __str__(self) -> str:
        """Format error message with details."""
        lines = [self.message]

        if self.failed_checks:
            lines.append("")
            lines.append("Failed checks:")
            for check in self.failed_checks:
                lines.append(f"  - {check}")

        if self.recommendations:
            lines.append("")
            lines.append("How to fix:")
            for rec in self.recommendations:
                lines.append(f"  - {rec}")

        return "\n".join(lines)


def classify_error(error: Exception) -> ErrorClassification:
    """Classify an exception into error type and category.

    This function examines the exception type and message to determine
    the most appropriate classification for backtest error handling.

    Args:
        error: The exception to classify

    Returns:
        ErrorClassification with type, category, and suggested action
    """
    error_type = ErrorType.UNKNOWN
    error_msg = str(error).lower()
    error_class_name = type(error).__name__.lower()

    # Check for rate limiting
    if any(keyword in error_msg for keyword in ["rate limit", "too many requests", "429", "throttl"]):
        error_type = ErrorType.RATE_LIMIT

    # Check for timeouts
    elif any(keyword in error_msg or keyword in error_class_name for keyword in ["timeout", "timed out", "timedout"]):
        error_type = ErrorType.TIMEOUT

    # Check for connection errors
    elif any(
        keyword in error_msg or keyword in error_class_name
        for keyword in [
            "connection",
            "network",
            "socket",
            "refused",
            "reset",
            "broken pipe",
        ]
    ):
        error_type = ErrorType.CONNECTION_ERROR

    # Check for temporary unavailability
    elif any(keyword in error_msg for keyword in ["unavailable", "503", "502", "maintenance", "overload"]):
        error_type = ErrorType.TEMPORARY_UNAVAILABLE

    # Check for RPC errors
    elif any(
        keyword in error_msg or keyword in error_class_name for keyword in ["rpc", "jsonrpc", "web3", "node error"]
    ):
        error_type = ErrorType.RPC_ERROR

    # Check for authentication errors
    elif any(keyword in error_msg for keyword in ["auth", "401", "403", "forbidden", "api key", "token invalid"]):
        error_type = ErrorType.AUTHENTICATION_FAILED

    # Check for configuration errors
    elif any(
        keyword in error_msg
        for keyword in [
            "invalid config",
            "configuration error",
            "missing required",
            "invalid parameter",
        ]
    ):
        error_type = ErrorType.INVALID_CONFIG

    # Check for data corruption
    elif any(keyword in error_msg for keyword in ["corrupt", "malformed", "invalid data", "parse error"]):
        error_type = ErrorType.DATA_CORRUPTION

    # Check for insufficient funds
    elif any(keyword in error_msg for keyword in ["insufficient", "balance too low", "not enough"]):
        error_type = ErrorType.INSUFFICIENT_FUNDS

    # Check for missing price data
    elif any(keyword in error_msg for keyword in ["no price", "price not found", "missing price", "unknown token"]):
        error_type = ErrorType.MISSING_PRICE

    # Check for stale data
    elif any(keyword in error_msg for keyword in ["stale", "outdated", "old data"]):
        error_type = ErrorType.STALE_DATA

    # Check for partial data
    elif any(keyword in error_msg for keyword in ["partial", "incomplete", "missing field"]):
        error_type = ErrorType.PARTIAL_DATA

    # Get category from map
    category = ERROR_CATEGORY_MAP[error_type]

    # Generate suggested action
    suggested_action = _get_suggested_action(error_type)

    return ErrorClassification(
        error_type=error_type,
        category=category,
        suggested_action=suggested_action,
    )


def _get_suggested_action(error_type: ErrorType) -> str:
    """Get suggested action for an error type."""
    actions = {
        ErrorType.RATE_LIMIT: "Wait and retry with exponential backoff",
        ErrorType.TIMEOUT: "Retry with increased timeout or check network",
        ErrorType.CONNECTION_ERROR: "Check network connectivity and retry",
        ErrorType.TEMPORARY_UNAVAILABLE: "Wait for service to recover and retry",
        ErrorType.RPC_ERROR: "Try alternative RPC endpoint or retry",
        ErrorType.INVALID_CONFIG: "Fix configuration and restart backtest",
        ErrorType.DATA_CORRUPTION: "Check data source and clear cache",
        ErrorType.AUTHENTICATION_FAILED: "Verify API keys and credentials",
        ErrorType.INSUFFICIENT_FUNDS: "Add funds or reduce position size",
        ErrorType.INVALID_STATE: "Reset state and restart backtest",
        ErrorType.MISSING_PRICE: "Use fallback price or skip tick",
        ErrorType.STALE_DATA: "Use cached data with staleness warning",
        ErrorType.PARTIAL_DATA: "Continue with available data",
        ErrorType.OPTIONAL_FEATURE_FAILED: "Continue without optional feature",
        ErrorType.UNKNOWN: "Investigate error and consider stopping backtest",
    }
    return actions.get(error_type, "Unknown action")


# =============================================================================
# Circuit Breaker for Backtesting
# =============================================================================


class BacktestCircuitBreakerState(Enum):
    """Circuit breaker states."""

    CLOSED = "closed"  # Normal operation
    OPEN = "open"  # Tripped, blocking calls
    HALF_OPEN = "half_open"  # Testing recovery


@dataclass
class BacktestErrorConfig:
    """Configuration for backtest error handling.

    Attributes:
        circuit_breaker_threshold: Consecutive failures before tripping (default 5)
        circuit_breaker_cooldown_seconds: Cooldown before retry (default 60)
        max_non_critical_errors: Max non-critical errors before warning (default 100)
        continue_on_non_critical: Continue backtest for non-critical errors
        log_all_errors: Log all errors, not just first occurrence
    """

    circuit_breaker_threshold: int = 5
    circuit_breaker_cooldown_seconds: int = 60
    max_non_critical_errors: int = 100
    continue_on_non_critical: bool = True
    log_all_errors: bool = True

    def __post_init__(self) -> None:
        """Validate configuration."""
        if self.circuit_breaker_threshold < 1:
            raise ValueError("circuit_breaker_threshold must be >= 1")
        if self.circuit_breaker_cooldown_seconds < 0:
            raise ValueError("circuit_breaker_cooldown_seconds must be >= 0")
        if self.max_non_critical_errors < 0:
            raise ValueError("max_non_critical_errors must be >= 0")

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "circuit_breaker_threshold": self.circuit_breaker_threshold,
            "circuit_breaker_cooldown_seconds": self.circuit_breaker_cooldown_seconds,
            "max_non_critical_errors": self.max_non_critical_errors,
            "continue_on_non_critical": self.continue_on_non_critical,
            "log_all_errors": self.log_all_errors,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> BacktestErrorConfig:
        """Deserialize from dictionary."""
        return cls(
            circuit_breaker_threshold=data.get("circuit_breaker_threshold", 5),
            circuit_breaker_cooldown_seconds=data.get("circuit_breaker_cooldown_seconds", 60),
            max_non_critical_errors=data.get("max_non_critical_errors", 100),
            continue_on_non_critical=data.get("continue_on_non_critical", True),
            log_all_errors=data.get("log_all_errors", True),
        )


@dataclass
class ErrorRecord:
    """Record of a single error event.

    Attributes:
        timestamp: When the error occurred
        error: The exception that occurred
        classification: Error classification result
        context: Additional context (e.g., "price_fetch", "tick_42")
        handled: How the error was handled
    """

    timestamp: datetime
    error: Exception
    classification: ErrorClassification
    context: str = ""
    handled: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "timestamp": self.timestamp.isoformat(),
            "error_type": type(self.error).__name__,
            "error_message": str(self.error),
            "classification": self.classification.to_dict(),
            "context": self.context,
            "handled": self.handled,
        }


@dataclass
class HandleErrorResult:
    """Result of handling an error.

    Attributes:
        should_stop: True if backtest should stop
        should_retry: True if operation should be retried
        should_continue: True if backtest can continue without retry
        warning_message: Warning message if continuing
        error_record: The recorded error
    """

    should_stop: bool = False
    should_retry: bool = False
    should_continue: bool = False
    warning_message: str | None = None
    error_record: ErrorRecord | None = None

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "should_stop": self.should_stop,
            "should_retry": self.should_retry,
            "should_continue": self.should_continue,
            "warning_message": self.warning_message,
            "error_record": self.error_record.to_dict() if self.error_record else None,
        }


class BacktestCircuitBreaker:
    """Circuit breaker for backtesting operations.

    Tracks consecutive failures and trips after threshold is exceeded.
    After tripping, waits for cooldown before allowing retry.

    Thread-safe for concurrent access.

    Example:
        breaker = BacktestCircuitBreaker(threshold=5)

        if not breaker.can_proceed():
            raise CircuitBreakerOpenError()

        try:
            result = await external_call()
            breaker.record_success()
        except Exception as e:
            breaker.record_failure(e)
    """

    def __init__(
        self,
        threshold: int = 5,
        cooldown_seconds: int = 60,
    ) -> None:
        """Initialize the circuit breaker.

        Args:
            threshold: Consecutive failures before tripping
            cooldown_seconds: Seconds to wait before retry
        """
        self._threshold = threshold
        self._cooldown_seconds = cooldown_seconds

        self._state = BacktestCircuitBreakerState.CLOSED
        self._lock = threading.RLock()

        self._consecutive_failures = 0
        self._last_failure_time: datetime | None = None
        self._trip_time: datetime | None = None
        self._half_open_success = False

    @property
    def state(self) -> BacktestCircuitBreakerState:
        """Get current state."""
        with self._lock:
            return self._state

    @property
    def consecutive_failures(self) -> int:
        """Get consecutive failure count."""
        with self._lock:
            return self._consecutive_failures

    @property
    def is_open(self) -> bool:
        """Check if circuit breaker is open (tripped)."""
        with self._lock:
            return self._state == BacktestCircuitBreakerState.OPEN

    def can_proceed(self) -> bool:
        """Check if operations can proceed.

        Handles automatic state transitions based on cooldown.

        Returns:
            True if operations are allowed
        """
        with self._lock:
            now = datetime.now(UTC)

            # Check cooldown expiry for OPEN state
            if self._state == BacktestCircuitBreakerState.OPEN and self._trip_time:
                cooldown_expires = self._trip_time + timedelta(seconds=self._cooldown_seconds)
                if now >= cooldown_expires:
                    self._state = BacktestCircuitBreakerState.HALF_OPEN
                    self._half_open_success = False
                    logger.info("Circuit breaker transitioning to HALF_OPEN after cooldown")
                else:
                    return False

            return self._state != BacktestCircuitBreakerState.OPEN

    def record_success(self) -> None:
        """Record a successful operation."""
        with self._lock:
            self._consecutive_failures = 0

            if self._state == BacktestCircuitBreakerState.HALF_OPEN:
                self._state = BacktestCircuitBreakerState.CLOSED
                self._trip_time = None
                logger.info("Circuit breaker CLOSED after successful test")

    def record_failure(self, error: Exception) -> None:
        """Record a failed operation.

        Args:
            error: The exception that occurred
        """
        with self._lock:
            self._consecutive_failures += 1
            self._last_failure_time = datetime.now(UTC)

            # In HALF_OPEN, any failure trips back to OPEN
            if self._state == BacktestCircuitBreakerState.HALF_OPEN:
                self._trip()
                logger.warning(f"Circuit breaker tripped back to OPEN from HALF_OPEN: {error}")
                return

            # Check threshold
            if self._consecutive_failures >= self._threshold:
                self._trip()
                logger.error(
                    f"Circuit breaker TRIPPED after {self._consecutive_failures} consecutive failures: {error}"
                )

    def _trip(self) -> None:
        """Trip the circuit breaker to OPEN state."""
        self._state = BacktestCircuitBreakerState.OPEN
        self._trip_time = datetime.now(UTC)

    def reset(self) -> None:
        """Reset the circuit breaker to initial state."""
        with self._lock:
            self._state = BacktestCircuitBreakerState.CLOSED
            self._consecutive_failures = 0
            self._last_failure_time = None
            self._trip_time = None
            self._half_open_success = False

    def get_status(self) -> dict[str, Any]:
        """Get current status as dictionary."""
        with self._lock:
            remaining_cooldown = None
            if self._state == BacktestCircuitBreakerState.OPEN and self._trip_time:
                cooldown_expires = self._trip_time + timedelta(seconds=self._cooldown_seconds)
                remaining = (cooldown_expires - datetime.now(UTC)).total_seconds()
                remaining_cooldown = max(0, int(remaining))

            return {
                "state": self._state.value,
                "consecutive_failures": self._consecutive_failures,
                "threshold": self._threshold,
                "cooldown_seconds": self._cooldown_seconds,
                "remaining_cooldown": remaining_cooldown,
                "last_failure_time": (self._last_failure_time.isoformat() if self._last_failure_time else None),
                "trip_time": (self._trip_time.isoformat() if self._trip_time else None),
            }


# =============================================================================
# Error Handler
# =============================================================================


class BacktestErrorHandler:
    """Comprehensive error handler for backtesting operations.

    Combines error classification with circuit breaker protection
    and provides detailed error tracking.

    Example:
        handler = BacktestErrorHandler()

        try:
            result = await risky_operation()
        except Exception as e:
            result = handler.handle_error(e, context="fetch_price:ETH")
            if result.should_stop:
                raise BacktestFatalError(str(e))
            elif result.should_retry:
                # Retry with backoff
                pass
            else:
                # Continue with warning
                logger.warning(result.warning_message)
    """

    def __init__(self, config: BacktestErrorConfig | None = None) -> None:
        """Initialize the error handler.

        Args:
            config: Configuration options (uses defaults if not provided)
        """
        self._config = config or BacktestErrorConfig()
        self._circuit_breaker = BacktestCircuitBreaker(
            threshold=self._config.circuit_breaker_threshold,
            cooldown_seconds=self._config.circuit_breaker_cooldown_seconds,
        )

        self._lock = threading.RLock()
        self._error_history: list[ErrorRecord] = []
        self._non_critical_count = 0
        self._recoverable_count = 0
        self._fatal_count = 0
        self._errors_by_type: dict[ErrorType, int] = {}

    @property
    def config(self) -> BacktestErrorConfig:
        """Get configuration."""
        return self._config

    @property
    def circuit_breaker(self) -> BacktestCircuitBreaker:
        """Get circuit breaker."""
        return self._circuit_breaker

    @property
    def error_count(self) -> int:
        """Get total error count."""
        with self._lock:
            return len(self._error_history)

    @property
    def non_critical_count(self) -> int:
        """Get non-critical error count."""
        with self._lock:
            return self._non_critical_count

    def handle_error(
        self,
        error: Exception,
        context: str = "",
    ) -> HandleErrorResult:
        """Handle an error and determine appropriate action.

        Args:
            error: The exception to handle
            context: Additional context (e.g., "tick_42", "price_fetch")

        Returns:
            HandleErrorResult indicating what action to take
        """
        with self._lock:
            # Classify the error
            classification = classify_error(error)

            # Create error record
            record = ErrorRecord(
                timestamp=datetime.now(UTC),
                error=error,
                classification=classification,
                context=context,
            )

            # Track by type
            self._errors_by_type[classification.error_type] = self._errors_by_type.get(classification.error_type, 0) + 1

            # Handle based on category
            if classification.is_fatal:
                return self._handle_fatal_error(record)
            elif classification.is_recoverable:
                return self._handle_recoverable_error(record)
            else:  # Non-critical
                return self._handle_non_critical_error(record)

    def _handle_fatal_error(self, record: ErrorRecord) -> HandleErrorResult:
        """Handle a fatal error - backtest must stop."""
        record.handled = "stop"
        self._error_history.append(record)
        self._fatal_count += 1

        if self._config.log_all_errors:
            logger.error(
                f"Fatal error in backtest [{record.context}]: {record.classification.error_type.value} - {record.error}"
            )

        return HandleErrorResult(
            should_stop=True,
            error_record=record,
        )

    def _handle_recoverable_error(self, record: ErrorRecord) -> HandleErrorResult:
        """Handle a recoverable error - retry with backoff."""
        self._recoverable_count += 1

        # Check circuit breaker
        if not self._circuit_breaker.can_proceed():
            record.handled = "circuit_breaker_open"
            self._error_history.append(record)

            logger.error(f"Circuit breaker open, cannot retry [{record.context}]: {record.error}")

            return HandleErrorResult(
                should_stop=True,
                error_record=record,
                warning_message=(
                    f"Circuit breaker tripped after {self._circuit_breaker.consecutive_failures} failures"
                ),
            )

        # Record failure in circuit breaker
        self._circuit_breaker.record_failure(record.error)

        # Check if circuit breaker just tripped
        if self._circuit_breaker.is_open:
            record.handled = "circuit_breaker_tripped"
            self._error_history.append(record)

            logger.error(f"Circuit breaker tripped [{record.context}]: {record.error}")

            return HandleErrorResult(
                should_stop=True,
                error_record=record,
                warning_message=(f"Circuit breaker tripped: {record.classification.suggested_action}"),
            )

        # Can retry
        record.handled = "retry"
        self._error_history.append(record)

        if self._config.log_all_errors:
            logger.warning(
                f"Recoverable error [{record.context}], will retry: "
                f"{record.classification.error_type.value} - {record.error}"
            )

        return HandleErrorResult(
            should_retry=True,
            error_record=record,
            warning_message=record.classification.suggested_action,
        )

    def _handle_non_critical_error(self, record: ErrorRecord) -> HandleErrorResult:
        """Handle a non-critical error - continue with warning."""
        self._non_critical_count += 1

        # Check if we've exceeded the threshold
        if self._non_critical_count > self._config.max_non_critical_errors:
            record.handled = "too_many_non_critical"
            self._error_history.append(record)

            logger.warning(
                f"Exceeded max non-critical errors ({self._config.max_non_critical_errors}), stopping backtest"
            )

            return HandleErrorResult(
                should_stop=True,
                error_record=record,
                warning_message=(
                    f"Exceeded max non-critical errors: "
                    f"{self._non_critical_count}/{self._config.max_non_critical_errors}"
                ),
            )

        # Can continue
        if self._config.continue_on_non_critical:
            record.handled = "continue_with_warning"
            self._error_history.append(record)

            if self._config.log_all_errors:
                logger.warning(
                    f"Non-critical error [{record.context}], continuing: "
                    f"{record.classification.error_type.value} - {record.error}"
                )

            return HandleErrorResult(
                should_continue=True,
                error_record=record,
                warning_message=(f"Non-critical error: {record.classification.suggested_action}"),
            )
        else:
            # Non-critical but config says stop
            record.handled = "stop_on_non_critical"
            self._error_history.append(record)

            return HandleErrorResult(
                should_stop=True,
                error_record=record,
            )

    def record_success(self) -> None:
        """Record a successful operation (resets circuit breaker)."""
        self._circuit_breaker.record_success()

    def reset(self) -> None:
        """Reset all error tracking state."""
        with self._lock:
            self._circuit_breaker.reset()
            self._error_history = []
            self._non_critical_count = 0
            self._recoverable_count = 0
            self._fatal_count = 0
            self._errors_by_type = {}

    def get_error_summary(self) -> dict[str, Any]:
        """Get summary of all errors.

        Returns:
            Dictionary with error statistics
        """
        with self._lock:
            return {
                "total_errors": len(self._error_history),
                "fatal_errors": self._fatal_count,
                "recoverable_errors": self._recoverable_count,
                "non_critical_errors": self._non_critical_count,
                "errors_by_type": {k.value: v for k, v in self._errors_by_type.items()},
                "circuit_breaker": self._circuit_breaker.get_status(),
            }

    def get_errors(self) -> list[ErrorRecord]:
        """Get all error records."""
        with self._lock:
            return list(self._error_history)

    def get_errors_as_dicts(self) -> list[dict[str, Any]]:
        """Get all error records as dictionaries."""
        with self._lock:
            return [record.to_dict() for record in self._error_history]


# =============================================================================
# Convenience Functions
# =============================================================================


def is_recoverable_error(error: Exception) -> bool:
    """Check if an error is recoverable (can be retried).

    Args:
        error: The exception to check

    Returns:
        True if the error is recoverable
    """
    return classify_error(error).is_recoverable


def is_fatal_error(error: Exception) -> bool:
    """Check if an error is fatal (must stop backtest).

    Args:
        error: The exception to check

    Returns:
        True if the error is fatal
    """
    return classify_error(error).is_fatal


def is_non_critical_error(error: Exception) -> bool:
    """Check if an error is non-critical (can continue with warning).

    Args:
        error: The exception to check

    Returns:
        True if the error is non-critical
    """
    return classify_error(error).is_non_critical


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    # Enums
    "ErrorCategory",
    "ErrorType",
    "BacktestCircuitBreakerState",
    # Configuration
    "BacktestErrorConfig",
    # Data classes
    "ErrorClassification",
    "ErrorRecord",
    "HandleErrorResult",
    # Exceptions
    "PreflightValidationError",
    # Classes
    "BacktestCircuitBreaker",
    "BacktestErrorHandler",
    # Functions
    "classify_error",
    "is_recoverable_error",
    "is_fatal_error",
    "is_non_critical_error",
    # Map
    "ERROR_CATEGORY_MAP",
]
