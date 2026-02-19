"""Circuit breaker for provider failover.

Implements the circuit breaker pattern to temporarily skip failing providers
instead of causing repeated timeouts.

States:
    CLOSED  - Provider is healthy; requests flow through normally.
    OPEN    - Provider is failing; requests are rejected immediately.
    HALF_OPEN - Cooldown elapsed; one test request is allowed through.

Usage:
    cb = CircuitBreaker(name="geckoterminal", failure_threshold=5, cooldown_seconds=60)

    if cb.allow_request():
        try:
            result = provider.fetch(...)
            cb.record_success()
        except Exception:
            cb.record_failure()
    else:
        # Skip this provider, try fallback
        ...
"""

from __future__ import annotations

import enum
import logging
import threading
import time

logger = logging.getLogger(__name__)


class CircuitState(enum.Enum):
    """Circuit breaker states."""

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    """Thread-safe circuit breaker for a single data provider.

    Opens after ``failure_threshold`` consecutive failures, then enters
    a cooldown period. After cooldown, allows one test request (HALF_OPEN).
    If the test succeeds the circuit closes; if it fails the circuit reopens.

    Attributes:
        name: Provider name this breaker guards.
        failure_threshold: Consecutive failures before opening.
        cooldown_seconds: Seconds to wait before half-open test.
    """

    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        cooldown_seconds: float = 60.0,
    ) -> None:
        if failure_threshold <= 0:
            raise ValueError(f"failure_threshold must be positive, got {failure_threshold}")
        if cooldown_seconds <= 0:
            raise ValueError(f"cooldown_seconds must be positive, got {cooldown_seconds}")

        self.name = name
        self.failure_threshold = failure_threshold
        self.cooldown_seconds = cooldown_seconds

        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._last_failure_time: float | None = None
        self._lock = threading.Lock()

    @property
    def state(self) -> CircuitState:
        """Current circuit state, accounting for cooldown expiry."""
        with self._lock:
            return self._effective_state()

    @property
    def failure_count(self) -> int:
        """Number of consecutive failures."""
        with self._lock:
            return self._failure_count

    @property
    def last_failure_time(self) -> float | None:
        """Monotonic timestamp of the last recorded failure, or None."""
        with self._lock:
            return self._last_failure_time

    def allow_request(self) -> bool:
        """Check whether a request should be allowed through.

        Returns:
            True if the circuit is CLOSED or transitioning to HALF_OPEN.
            False if the circuit is OPEN (still within cooldown).
        """
        with self._lock:
            effective = self._effective_state()

            if effective == CircuitState.CLOSED:
                return True

            if effective == CircuitState.HALF_OPEN:
                # Transition to HALF_OPEN and allow the test request
                self._state = CircuitState.HALF_OPEN
                return True

            # OPEN and still within cooldown
            return False

    def record_success(self) -> None:
        """Record a successful request, resetting the breaker to CLOSED."""
        with self._lock:
            if self._state != CircuitState.CLOSED:
                logger.info("circuit_breaker.closed provider=%s previous_state=%s", self.name, self._state.value)
            self._state = CircuitState.CLOSED
            self._failure_count = 0

    def record_failure(self) -> None:
        """Record a failed request, potentially opening the circuit."""
        with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.monotonic()

            if self._state == CircuitState.HALF_OPEN:
                # Test request failed; reopen
                self._state = CircuitState.OPEN
                logger.warning(
                    "circuit_breaker.reopened provider=%s failure_count=%d",
                    self.name,
                    self._failure_count,
                )
            elif self._failure_count >= self.failure_threshold:
                if self._state != CircuitState.OPEN:
                    self._state = CircuitState.OPEN
                    logger.warning(
                        "circuit_breaker.opened provider=%s failure_count=%d threshold=%d",
                        self.name,
                        self._failure_count,
                        self.failure_threshold,
                    )

    def reset(self) -> None:
        """Manually reset the breaker to CLOSED state."""
        with self._lock:
            self._state = CircuitState.CLOSED
            self._failure_count = 0
            self._last_failure_time = None

    def health(self) -> dict[str, object]:
        """Return health metrics for monitoring.

        Returns:
            Dict with state, failure_count, last_failure_time, and config.
        """
        with self._lock:
            return {
                "state": self._effective_state().value,
                "failure_count": self._failure_count,
                "last_failure_time": self._last_failure_time,
                "failure_threshold": self.failure_threshold,
                "cooldown_seconds": self.cooldown_seconds,
            }

    def _effective_state(self) -> CircuitState:
        """Compute effective state, promoting OPEN -> HALF_OPEN after cooldown.

        Must be called with self._lock held.
        """
        if self._state == CircuitState.OPEN and self._last_failure_time is not None:
            elapsed = time.monotonic() - self._last_failure_time
            if elapsed >= self.cooldown_seconds:
                return CircuitState.HALF_OPEN
        return self._state
