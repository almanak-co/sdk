"""Unit tests for provider circuit breaker behavior."""

import asyncio

import pytest

from almanak.framework.backtesting.pnl.providers.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitBreakerOpenError,
    CircuitBreakerState,
)


async def _ok() -> str:
    return "ok"


async def _runtime_failure() -> str:
    raise RuntimeError("provider down")


async def _excluded_failure() -> str:
    raise ValueError("validation failure")


class TestCircuitBreakerConfig:
    def test_half_open_success_threshold_cannot_exceed_allowed_calls(self) -> None:
        """Otherwise a half-open circuit can never collect enough successes to close."""
        with pytest.raises(ValueError, match="success_threshold_half_open"):
            CircuitBreakerConfig(
                half_open_max_calls=1,
                success_threshold_half_open=2,
            )


class TestCircuitBreakerStateTransitions:
    @pytest.mark.asyncio
    async def test_failure_threshold_opens_and_blocks_without_fallback(self) -> None:
        breaker = CircuitBreaker(
            "price-api",
            CircuitBreakerConfig(failure_threshold=1, reset_timeout_seconds=60),
        )

        with pytest.raises(RuntimeError):
            await breaker.execute(_runtime_failure)

        assert breaker.state == CircuitBreakerState.OPEN
        with pytest.raises(CircuitBreakerOpenError):
            await breaker.execute(_ok)

        metrics = breaker.get_metrics()
        assert metrics.failure_count == 1
        assert metrics.blocked_count == 1

    @pytest.mark.asyncio
    async def test_excluded_exception_does_not_open_circuit(self) -> None:
        breaker = CircuitBreaker(
            "price-api",
            CircuitBreakerConfig(failure_threshold=1, excluded_exceptions=(ValueError,)),
        )

        with pytest.raises(ValueError):
            await breaker.execute(_excluded_failure)

        assert breaker.state == CircuitBreakerState.CLOSED
        metrics = breaker.get_metrics()
        assert metrics.failure_count == 0

    @pytest.mark.asyncio
    async def test_failure_rate_threshold_opens_after_minimum_calls(self) -> None:
        breaker = CircuitBreaker(
            "price-api",
            CircuitBreakerConfig(
                failure_threshold=99,
                failure_rate_threshold=0.5,
                min_calls_for_rate=4,
            ),
        )

        assert await breaker.execute(_ok) == "ok"
        assert await breaker.execute(_ok) == "ok"
        with pytest.raises(RuntimeError):
            await breaker.execute(_runtime_failure)
        assert breaker.state == CircuitBreakerState.CLOSED

        with pytest.raises(RuntimeError):
            await breaker.execute(_runtime_failure)

        assert breaker.state == CircuitBreakerState.OPEN

    @pytest.mark.asyncio
    async def test_reset_timeout_allows_half_open_probe_to_close(self) -> None:
        breaker = CircuitBreaker(
            "price-api",
            CircuitBreakerConfig(
                failure_threshold=1,
                reset_timeout_seconds=0.01,
                half_open_max_calls=1,
                success_threshold_half_open=1,
            ),
        )

        with pytest.raises(RuntimeError):
            await breaker.execute(_runtime_failure)
        assert breaker.state == CircuitBreakerState.OPEN

        await asyncio.sleep(0.02)
        assert await breaker.execute(_ok) == "ok"

        assert breaker.state == CircuitBreakerState.CLOSED

    @pytest.mark.asyncio
    async def test_half_open_failure_reopens_circuit(self) -> None:
        breaker = CircuitBreaker(
            "price-api",
            CircuitBreakerConfig(
                failure_threshold=1,
                reset_timeout_seconds=0.01,
                half_open_max_calls=1,
                success_threshold_half_open=1,
            ),
        )

        with pytest.raises(RuntimeError):
            await breaker.execute(_runtime_failure)
        await asyncio.sleep(0.02)

        with pytest.raises(RuntimeError):
            await breaker.execute(_runtime_failure)

        assert breaker.state == CircuitBreakerState.OPEN
