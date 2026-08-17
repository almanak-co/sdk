"""Canonical retry budgeting and backoff policy.

The policy deliberately owns only attempt numbering and delay calculation.
Callers still classify operations and failures: in particular, creating a
policy never makes a non-idempotent transaction submission safe to retry.
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass
from numbers import Real


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """An explicit total-attempt budget with bounded exponential backoff.

    ``max_attempts`` includes the initial call. Attempt numbers are one-based:
    after attempt 1 fails, ``delay_for_attempt(1)`` returns the delay before
    attempt 2. This removes the repository's historical ambiguity between
    ``max_retries`` meaning "extra calls" and meaning "total calls".
    """

    max_attempts: int = 3
    initial_delay_seconds: float = 0.5
    max_delay_seconds: float = 60.0
    backoff_multiplier: float = 2.0
    jitter_ratio: float = 0.0

    def __post_init__(self) -> None:
        if not isinstance(self.max_attempts, int) or isinstance(self.max_attempts, bool):
            raise TypeError(f"max_attempts must be an int (got {type(self.max_attempts).__name__})")
        if self.max_attempts < 1:
            raise ValueError(f"max_attempts must be >= 1 (got {self.max_attempts})")
        self._validate_finite("initial_delay_seconds", self.initial_delay_seconds, minimum=0.0)
        self._validate_finite("max_delay_seconds", self.max_delay_seconds, minimum=0.0, inclusive=False)
        self._validate_finite("backoff_multiplier", self.backoff_multiplier, minimum=1.0)
        self._validate_finite("jitter_ratio", self.jitter_ratio, minimum=0.0)
        if self.jitter_ratio > 1:
            raise ValueError(f"jitter_ratio must be <= 1 (got {self.jitter_ratio})")

    @staticmethod
    def _validate_finite(name: str, value: float, *, minimum: float, inclusive: bool = True) -> None:
        if not isinstance(value, Real) or isinstance(value, bool):
            raise TypeError(f"{name} must be a real number (got {type(value).__name__})")
        numeric = float(value)
        if not math.isfinite(numeric):
            raise ValueError(f"{name} must be finite (got {value})")
        if numeric < minimum or (not inclusive and numeric == minimum):
            operator = ">=" if inclusive else ">"
            raise ValueError(f"{name} must be {operator} {minimum:g} (got {value})")

    @property
    def attempt_numbers(self) -> range:
        """One-based attempt numbers in this budget."""
        return range(1, self.max_attempts + 1)

    def can_retry(self, attempt_number: int) -> bool:
        """Whether another attempt remains after ``attempt_number`` fails."""
        self._validate_attempt_number(attempt_number)
        return attempt_number < self.max_attempts

    def delay_for_attempt(
        self,
        attempt_number: int,
        *,
        retry_after_seconds: float | None = None,
        random_value: float | None = None,
    ) -> float:
        """Return the delay after a failed one-based attempt.

        A non-negative upstream ``retry_after_seconds`` takes precedence and is
        capped by ``max_delay_seconds``. Otherwise exponential backoff is
        applied, followed by symmetric proportional jitter. ``random_value``
        is injectable in ``[0, 1]`` for deterministic tests.
        """
        self._validate_attempt_number(attempt_number)
        if retry_after_seconds is not None:
            self._validate_finite("retry_after_seconds", retry_after_seconds, minimum=0.0)
            return min(float(retry_after_seconds), float(self.max_delay_seconds))

        initial_delay = float(self.initial_delay_seconds)
        try:
            delay = (
                0.0 if initial_delay == 0 else initial_delay * (float(self.backoff_multiplier) ** (attempt_number - 1))
            )
        except OverflowError:
            delay = float(self.max_delay_seconds)
        delay = min(delay, float(self.max_delay_seconds))
        if self.jitter_ratio == 0 or delay == 0:
            return delay

        sample = random.random() if random_value is None else random_value
        self._validate_finite("random_value", sample, minimum=0.0)
        if sample > 1:
            raise ValueError(f"random_value must be <= 1 (got {sample})")
        jitter_multiplier = 1 - float(self.jitter_ratio) + 2 * float(self.jitter_ratio) * float(sample)
        return min(delay * jitter_multiplier, float(self.max_delay_seconds))

    @staticmethod
    def _validate_attempt_number(attempt_number: int) -> None:
        if not isinstance(attempt_number, int) or isinstance(attempt_number, bool):
            raise TypeError(f"attempt_number must be an int (got {type(attempt_number).__name__})")
        if attempt_number < 1:
            raise ValueError(f"attempt_number must be >= 1 (got {attempt_number})")


__all__ = ["RetryPolicy"]
