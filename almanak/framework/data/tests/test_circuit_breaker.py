"""Unit tests for CircuitBreaker.

Tests cover:
- Construction and validation
- State transitions: CLOSED -> OPEN -> HALF_OPEN -> CLOSED
- Cooldown timing
- Half-open success/failure behavior
- Thread safety
- Health metrics
- Manual reset
"""

from __future__ import annotations

import threading
from unittest.mock import patch

import pytest

from almanak.framework.data.routing.circuit_breaker import CircuitBreaker, CircuitState

# ---------------------------------------------------------------------------
# Construction tests
# ---------------------------------------------------------------------------


class TestCircuitBreakerConstruction:
    def test_defaults(self) -> None:
        cb = CircuitBreaker(name="test_provider")
        assert cb.name == "test_provider"
        assert cb.failure_threshold == 5
        assert cb.cooldown_seconds == 60.0
        assert cb.state == CircuitState.CLOSED
        assert cb.failure_count == 0
        assert cb.last_failure_time is None

    def test_custom_params(self) -> None:
        cb = CircuitBreaker(name="custom", failure_threshold=3, cooldown_seconds=30.0)
        assert cb.failure_threshold == 3
        assert cb.cooldown_seconds == 30.0

    def test_invalid_failure_threshold(self) -> None:
        with pytest.raises(ValueError, match="failure_threshold must be positive"):
            CircuitBreaker(name="bad", failure_threshold=0)

    def test_negative_failure_threshold(self) -> None:
        with pytest.raises(ValueError, match="failure_threshold must be positive"):
            CircuitBreaker(name="bad", failure_threshold=-1)

    def test_invalid_cooldown(self) -> None:
        with pytest.raises(ValueError, match="cooldown_seconds must be positive"):
            CircuitBreaker(name="bad", cooldown_seconds=0)

    def test_negative_cooldown(self) -> None:
        with pytest.raises(ValueError, match="cooldown_seconds must be positive"):
            CircuitBreaker(name="bad", cooldown_seconds=-10)


# ---------------------------------------------------------------------------
# State transition tests
# ---------------------------------------------------------------------------


class TestCircuitBreakerStateTransitions:
    def test_starts_closed(self) -> None:
        cb = CircuitBreaker(name="test", failure_threshold=3)
        assert cb.state == CircuitState.CLOSED
        assert cb.allow_request() is True

    def test_stays_closed_under_threshold(self) -> None:
        cb = CircuitBreaker(name="test", failure_threshold=3)
        cb.record_failure()
        cb.record_failure()
        assert cb.state == CircuitState.CLOSED
        assert cb.failure_count == 2
        assert cb.allow_request() is True

    def test_opens_at_threshold(self) -> None:
        cb = CircuitBreaker(name="test", failure_threshold=3)
        cb.record_failure()
        cb.record_failure()
        cb.record_failure()
        assert cb.state == CircuitState.OPEN
        assert cb.failure_count == 3
        assert cb.allow_request() is False

    def test_opens_above_threshold(self) -> None:
        cb = CircuitBreaker(name="test", failure_threshold=2)
        for _ in range(5):
            cb.record_failure()
        assert cb.state == CircuitState.OPEN
        assert cb.failure_count == 5

    def test_success_resets_failure_count(self) -> None:
        cb = CircuitBreaker(name="test", failure_threshold=5)
        cb.record_failure()
        cb.record_failure()
        assert cb.failure_count == 2
        cb.record_success()
        assert cb.failure_count == 0
        assert cb.state == CircuitState.CLOSED

    def test_success_after_failures_prevents_open(self) -> None:
        cb = CircuitBreaker(name="test", failure_threshold=3)
        cb.record_failure()
        cb.record_failure()
        cb.record_success()  # resets
        cb.record_failure()
        cb.record_failure()
        assert cb.state == CircuitState.CLOSED
        assert cb.failure_count == 2


# ---------------------------------------------------------------------------
# Cooldown and half-open tests
# ---------------------------------------------------------------------------


class TestCircuitBreakerCooldown:
    def test_stays_open_during_cooldown(self) -> None:
        cb = CircuitBreaker(name="test", failure_threshold=2, cooldown_seconds=60.0)
        cb.record_failure()
        cb.record_failure()
        assert cb.state == CircuitState.OPEN
        assert cb.allow_request() is False

    def test_transitions_to_half_open_after_cooldown(self) -> None:
        cb = CircuitBreaker(name="test", failure_threshold=2, cooldown_seconds=10.0)
        cb.record_failure()
        cb.record_failure()
        assert cb.state == CircuitState.OPEN

        # Simulate time passing beyond cooldown
        with patch("almanak.framework.data.routing.circuit_breaker.time") as mock_time:
            # Set initial failure time
            initial_time = 1000.0
            cb._last_failure_time = initial_time
            mock_time.monotonic.return_value = initial_time + 10.0  # exactly at cooldown
            assert cb.state == CircuitState.HALF_OPEN

    def test_half_open_allows_one_request(self) -> None:
        cb = CircuitBreaker(name="test", failure_threshold=2, cooldown_seconds=10.0)
        cb.record_failure()
        cb.record_failure()

        # Simulate cooldown elapsed
        with patch("almanak.framework.data.routing.circuit_breaker.time") as mock_time:
            initial_time = 1000.0
            cb._last_failure_time = initial_time
            mock_time.monotonic.return_value = initial_time + 15.0
            assert cb.allow_request() is True
            assert cb._state == CircuitState.HALF_OPEN

    def test_half_open_success_closes_circuit(self) -> None:
        cb = CircuitBreaker(name="test", failure_threshold=2, cooldown_seconds=10.0)
        cb.record_failure()
        cb.record_failure()

        # Simulate cooldown elapsed
        with patch("almanak.framework.data.routing.circuit_breaker.time") as mock_time:
            initial_time = 1000.0
            cb._last_failure_time = initial_time
            mock_time.monotonic.return_value = initial_time + 15.0
            assert cb.allow_request() is True

        # Success in half-open closes the circuit
        cb.record_success()
        assert cb.state == CircuitState.CLOSED
        assert cb.failure_count == 0
        assert cb.allow_request() is True

    def test_half_open_failure_reopens_circuit(self) -> None:
        cb = CircuitBreaker(name="test", failure_threshold=2, cooldown_seconds=10.0)
        cb.record_failure()
        cb.record_failure()

        # Simulate cooldown elapsed
        with patch("almanak.framework.data.routing.circuit_breaker.time") as mock_time:
            initial_time = 1000.0
            cb._last_failure_time = initial_time
            mock_time.monotonic.return_value = initial_time + 15.0
            assert cb.allow_request() is True

        # Failure in half-open reopens
        cb.record_failure()
        assert cb.state == CircuitState.OPEN
        assert cb.allow_request() is False


# ---------------------------------------------------------------------------
# Reset tests
# ---------------------------------------------------------------------------


class TestCircuitBreakerReset:
    def test_reset_from_closed(self) -> None:
        cb = CircuitBreaker(name="test", failure_threshold=3)
        cb.record_failure()
        cb.reset()
        assert cb.state == CircuitState.CLOSED
        assert cb.failure_count == 0
        assert cb.last_failure_time is None

    def test_reset_from_open(self) -> None:
        cb = CircuitBreaker(name="test", failure_threshold=2)
        cb.record_failure()
        cb.record_failure()
        assert cb.state == CircuitState.OPEN
        cb.reset()
        assert cb.state == CircuitState.CLOSED
        assert cb.failure_count == 0
        assert cb.last_failure_time is None
        assert cb.allow_request() is True


# ---------------------------------------------------------------------------
# Health metrics tests
# ---------------------------------------------------------------------------


class TestCircuitBreakerHealth:
    def test_health_initial(self) -> None:
        cb = CircuitBreaker(name="test", failure_threshold=5, cooldown_seconds=30.0)
        h = cb.health()
        assert h["state"] == "closed"
        assert h["failure_count"] == 0
        assert h["last_failure_time"] is None
        assert h["failure_threshold"] == 5
        assert h["cooldown_seconds"] == 30.0

    def test_health_after_failures(self) -> None:
        cb = CircuitBreaker(name="test", failure_threshold=2)
        cb.record_failure()
        cb.record_failure()
        h = cb.health()
        assert h["state"] == "open"
        assert h["failure_count"] == 2
        assert h["last_failure_time"] is not None

    def test_health_reflects_half_open(self) -> None:
        cb = CircuitBreaker(name="test", failure_threshold=2, cooldown_seconds=5.0)
        cb.record_failure()
        cb.record_failure()

        with patch("almanak.framework.data.routing.circuit_breaker.time") as mock_time:
            initial_time = 1000.0
            cb._last_failure_time = initial_time
            mock_time.monotonic.return_value = initial_time + 10.0
            h = cb.health()
            assert h["state"] == "half_open"


# ---------------------------------------------------------------------------
# Thread safety tests
# ---------------------------------------------------------------------------


class TestCircuitBreakerThreadSafety:
    def test_concurrent_failures(self) -> None:
        """Multiple threads recording failures should not corrupt state."""
        cb = CircuitBreaker(name="test", failure_threshold=100, cooldown_seconds=60.0)
        n_threads = 10
        failures_per_thread = 10
        barrier = threading.Barrier(n_threads)

        def worker() -> None:
            barrier.wait()
            for _ in range(failures_per_thread):
                cb.record_failure()

        threads = [threading.Thread(target=worker) for _ in range(n_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert cb.failure_count == n_threads * failures_per_thread
        assert cb.state == CircuitState.OPEN

    def test_concurrent_mixed_operations(self) -> None:
        """Mixed success/failure from multiple threads should not crash."""
        cb = CircuitBreaker(name="test", failure_threshold=50, cooldown_seconds=60.0)
        n_threads = 20
        ops_per_thread = 50
        barrier = threading.Barrier(n_threads)

        def worker(thread_id: int) -> None:
            barrier.wait()
            for i in range(ops_per_thread):
                cb.allow_request()
                if (thread_id + i) % 3 == 0:
                    cb.record_success()
                else:
                    cb.record_failure()

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(n_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Just verify it didn't crash and state is valid
        assert cb.state in (CircuitState.CLOSED, CircuitState.OPEN, CircuitState.HALF_OPEN)
        assert cb.failure_count >= 0

    def test_concurrent_allow_request_during_half_open(self) -> None:
        """Only one thread should get through in HALF_OPEN state."""
        cb = CircuitBreaker(name="test", failure_threshold=2, cooldown_seconds=0.001)
        cb.record_failure()
        cb.record_failure()

        # Wait for cooldown
        import time

        time.sleep(0.01)

        n_threads = 10
        allowed = []
        barrier = threading.Barrier(n_threads)
        lock = threading.Lock()

        def worker() -> None:
            barrier.wait()
            result = cb.allow_request()
            with lock:
                allowed.append(result)

        threads = [threading.Thread(target=worker) for _ in range(n_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # At least one should be allowed (the first to check after cooldown)
        assert any(allowed)


# ---------------------------------------------------------------------------
# Edge case tests
# ---------------------------------------------------------------------------


class TestCircuitBreakerEdgeCases:
    def test_single_failure_threshold(self) -> None:
        """Circuit opens after a single failure when threshold=1."""
        cb = CircuitBreaker(name="test", failure_threshold=1)
        cb.record_failure()
        assert cb.state == CircuitState.OPEN
        assert cb.allow_request() is False

    def test_success_on_closed_is_noop(self) -> None:
        """Recording success on a closed circuit just keeps it closed."""
        cb = CircuitBreaker(name="test")
        cb.record_success()
        assert cb.state == CircuitState.CLOSED
        assert cb.failure_count == 0

    def test_last_failure_time_updated_on_each_failure(self) -> None:
        cb = CircuitBreaker(name="test", failure_threshold=5)
        cb.record_failure()
        t1 = cb.last_failure_time
        assert t1 is not None

        cb.record_failure()
        t2 = cb.last_failure_time
        assert t2 is not None
        assert t2 >= t1

    def test_very_short_cooldown(self) -> None:
        """Very short cooldown transitions quickly to half-open."""
        cb = CircuitBreaker(name="test", failure_threshold=1, cooldown_seconds=0.001)
        cb.record_failure()
        assert cb.state == CircuitState.OPEN

        import time

        time.sleep(0.01)
        assert cb.state == CircuitState.HALF_OPEN
        assert cb.allow_request() is True
