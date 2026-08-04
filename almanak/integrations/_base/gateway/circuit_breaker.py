"""Simple circuit breaker for integration failover.

Tracks failures in a sliding window and opens the circuit (skips the
provider) for a cooldown period when the failure threshold is reached.
No half-open state — the first request after cooldown is the probe.
"""

from __future__ import annotations

import time


class CircuitBreaker:
    """Circuit breaker: N failures in a time window -> skip for cooldown."""

    def __init__(
        self,
        failure_threshold: int = 3,
        failure_window_seconds: int = 60,
        recovery_seconds: int = 300,
    ):
        self._failures: list[float] = []
        self._open_until: float = 0.0
        self._threshold = failure_threshold
        self._window = failure_window_seconds
        self._recovery = recovery_seconds

    @property
    def is_open(self) -> bool:
        """True when the circuit is open (provider should be skipped)."""
        if self._open_until == 0.0:
            return False
        if time.monotonic() >= self._open_until:
            # Cooldown expired — close the circuit and let the next request probe
            self._open_until = 0.0
            self._failures.clear()
            return False
        return True

    def record_success(self) -> None:
        """Record a successful call. Resets failure tracking."""
        self._failures.clear()
        self._open_until = 0.0

    def record_failure(self) -> None:
        """Record a failed call. Opens the circuit when threshold is reached."""
        now = time.monotonic()
        # Prune failures outside the window
        cutoff = now - self._window
        self._failures = [t for t in self._failures if t > cutoff]
        self._failures.append(now)

        if len(self._failures) >= self._threshold:
            self._open_until = now + self._recovery
