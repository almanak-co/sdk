from __future__ import annotations

from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.execution.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitBreakerState,
)


@pytest.fixture
def breaker_clock() -> Iterator[MagicMock]:
    with patch("almanak.framework.execution.circuit_breaker.datetime", wraps=datetime) as clock:
        clock.now.return_value = datetime(2026, 1, 1, tzinfo=UTC)
        yield clock


@pytest.fixture
def make_tripped_breaker(breaker_clock: MagicMock) -> Callable[[int], CircuitBreaker]:
    def make_tripped_breaker(max_consecutive_failures: int) -> CircuitBreaker:
        breaker = CircuitBreaker(
            deployment_id="test-strategy",
            config=CircuitBreakerConfig(
                max_consecutive_failures=max_consecutive_failures,
                max_cumulative_loss_usd=Decimal("1000"),
                cooldown_seconds=2,
            ),
        )
        for failure_number in range(max_consecutive_failures):
            breaker.record_failure(f"fail {failure_number + 1}")
        assert breaker.state is CircuitBreakerState.OPEN
        return breaker

    return make_tripped_breaker
