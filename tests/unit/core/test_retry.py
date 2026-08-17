from __future__ import annotations

import pytest

from almanak.core.retry import RetryPolicy


def test_attempt_budget_is_explicit_and_one_based() -> None:
    policy = RetryPolicy(max_attempts=3)

    assert list(policy.attempt_numbers) == [1, 2, 3]
    assert policy.can_retry(1) is True
    assert policy.can_retry(2) is True
    assert policy.can_retry(3) is False


def test_exponential_delay_is_capped() -> None:
    policy = RetryPolicy(
        max_attempts=4,
        initial_delay_seconds=0.5,
        max_delay_seconds=1.5,
        backoff_multiplier=2,
    )

    assert policy.delay_for_attempt(1) == 0.5
    assert policy.delay_for_attempt(2) == 1.0
    assert policy.delay_for_attempt(3) == 1.5


def test_exponential_delay_caps_before_numeric_overflow_escapes() -> None:
    policy = RetryPolicy(
        max_attempts=2,
        initial_delay_seconds=1,
        max_delay_seconds=60,
        backoff_multiplier=2,
    )

    assert policy.delay_for_attempt(10_000) == 60


def test_zero_delay_remains_zero_for_large_attempt_numbers() -> None:
    policy = RetryPolicy(max_attempts=2, initial_delay_seconds=0, backoff_multiplier=2)

    assert policy.delay_for_attempt(10_000) == 0


def test_retry_after_takes_precedence_and_is_capped() -> None:
    policy = RetryPolicy(max_attempts=3, initial_delay_seconds=0.5, max_delay_seconds=5)

    assert policy.delay_for_attempt(1, retry_after_seconds=2) == 2
    assert policy.delay_for_attempt(1, retry_after_seconds=30) == 5


def test_jitter_is_deterministic_when_sample_is_injected() -> None:
    policy = RetryPolicy(max_attempts=2, initial_delay_seconds=10, jitter_ratio=0.5)

    assert policy.delay_for_attempt(1, random_value=0) == 5
    assert policy.delay_for_attempt(1, random_value=0.5) == 10
    assert policy.delay_for_attempt(1, random_value=1) == 15


def test_jitter_cannot_exceed_max_delay() -> None:
    policy = RetryPolicy(
        max_attempts=2,
        initial_delay_seconds=60,
        max_delay_seconds=60,
        jitter_ratio=1,
    )

    assert policy.delay_for_attempt(1, random_value=1) == 60


@pytest.mark.parametrize("invalid", [0, -1, True, 3.0])
def test_invalid_attempt_budget_is_rejected(invalid: object) -> None:
    with pytest.raises((TypeError, ValueError), match="max_attempts"):
        RetryPolicy(max_attempts=invalid)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("kwargs", "field"),
    [
        ({"initial_delay_seconds": -1}, "initial_delay_seconds"),
        ({"max_delay_seconds": 0}, "max_delay_seconds"),
        ({"backoff_multiplier": 0.5}, "backoff_multiplier"),
        ({"jitter_ratio": 1.1}, "jitter_ratio"),
        ({"initial_delay_seconds": float("inf")}, "initial_delay_seconds"),
    ],
)
def test_invalid_delay_configuration_is_rejected(kwargs: dict[str, float], field: str) -> None:
    with pytest.raises((TypeError, ValueError), match=field):
        RetryPolicy(**kwargs)  # type: ignore[arg-type]
