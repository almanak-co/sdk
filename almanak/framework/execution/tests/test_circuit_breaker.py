"""Tests for Circuit Breaker.

This module contains comprehensive tests for the CircuitBreaker class
and related components.
"""

import time
from decimal import Decimal

import pytest

from ..circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitBreakerRegistry,
    CircuitBreakerState,
    TripReason,
    get_global_registry,
)

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def config() -> CircuitBreakerConfig:
    """Create a test configuration with short timeouts."""
    return CircuitBreakerConfig(
        max_consecutive_failures=3,
        max_cumulative_loss_usd=Decimal("1000"),
        cooldown_seconds=2,  # Short for testing
        half_open_success_threshold=2,
    )


@pytest.fixture
def breaker(config: CircuitBreakerConfig) -> CircuitBreaker:
    """Create a test circuit breaker."""
    return CircuitBreaker(strategy_id="test_strategy", config=config)


@pytest.fixture
def registry(config: CircuitBreakerConfig) -> CircuitBreakerRegistry:
    """Create a test registry."""
    return CircuitBreakerRegistry(default_config=config)


# =============================================================================
# Configuration Tests
# =============================================================================


class TestCircuitBreakerConfig:
    """Tests for CircuitBreakerConfig."""

    def test_default_values(self) -> None:
        """Test that defaults are reasonable."""
        config = CircuitBreakerConfig()
        assert config.max_consecutive_failures == 3
        assert config.max_cumulative_loss_usd == Decimal("5000")
        assert config.cooldown_seconds == 3600
        assert config.half_open_success_threshold == 2

    def test_to_dict(self) -> None:
        """Test serialization."""
        config = CircuitBreakerConfig(
            max_consecutive_failures=5,
            max_cumulative_loss_usd=Decimal("10000"),
        )
        d = config.to_dict()
        assert d["max_consecutive_failures"] == 5
        assert d["max_cumulative_loss_usd"] == "10000"


# =============================================================================
# Basic State Tests
# =============================================================================


class TestCircuitBreakerState:
    """Tests for basic circuit breaker state management."""

    def test_initial_state_is_closed(self, breaker: CircuitBreaker) -> None:
        """Test that breaker starts in closed state."""
        assert breaker.state == CircuitBreakerState.CLOSED

    def test_closed_allows_execution(self, breaker: CircuitBreaker) -> None:
        """Test that closed state allows execution."""
        result = breaker.check()
        assert result.can_execute is True
        assert result.state == CircuitBreakerState.CLOSED

    def test_success_keeps_closed(self, breaker: CircuitBreaker) -> None:
        """Test that success keeps breaker closed."""
        breaker.record_success()
        assert breaker.state == CircuitBreakerState.CLOSED

    def test_single_failure_stays_closed(self, breaker: CircuitBreaker) -> None:
        """Test that a single failure doesn't trip the breaker."""
        breaker.record_failure("Test failure")
        assert breaker.state == CircuitBreakerState.CLOSED

        result = breaker.check()
        assert result.can_execute is True
        assert result.consecutive_failures == 1


# =============================================================================
# Consecutive Failure Tests
# =============================================================================


class TestConsecutiveFailures:
    """Tests for consecutive failure tripping."""

    def test_trips_on_consecutive_failures(self, breaker: CircuitBreaker) -> None:
        """Test that breaker trips after consecutive failures threshold."""
        for i in range(3):  # max_consecutive_failures = 3
            breaker.record_failure(f"Failure {i + 1}")

        assert breaker.state == CircuitBreakerState.OPEN

        result = breaker.check()
        assert result.can_execute is False
        assert result.trip_reason == TripReason.CONSECUTIVE_FAILURES

    def test_success_resets_consecutive_count(self, breaker: CircuitBreaker) -> None:
        """Test that success resets the consecutive failure counter."""
        breaker.record_failure("Failure 1")
        breaker.record_failure("Failure 2")
        assert breaker.state == CircuitBreakerState.CLOSED

        breaker.record_success()

        result = breaker.check()
        assert result.consecutive_failures == 0

        # Now need 3 more failures to trip
        breaker.record_failure("Failure 1")
        breaker.record_failure("Failure 2")
        assert breaker.state == CircuitBreakerState.CLOSED

    def test_interleaved_success_prevents_trip(self, breaker: CircuitBreaker) -> None:
        """Test that success between failures prevents trip."""
        breaker.record_failure("Failure 1")
        breaker.record_failure("Failure 2")
        breaker.record_success()  # Reset counter
        breaker.record_failure("Failure 1")
        breaker.record_failure("Failure 2")
        breaker.record_success()  # Reset counter

        assert breaker.state == CircuitBreakerState.CLOSED


# =============================================================================
# Cumulative Loss Tests
# =============================================================================


class TestCumulativeLoss:
    """Tests for cumulative loss threshold tripping."""

    def test_trips_on_cumulative_loss(self) -> None:
        """Test that breaker trips when cumulative loss exceeds threshold."""
        # Use config with higher consecutive failure threshold
        # so loss threshold is hit first
        config = CircuitBreakerConfig(
            max_consecutive_failures=10,  # High, won't be hit
            max_cumulative_loss_usd=Decimal("1000"),
        )
        breaker = CircuitBreaker("test_loss", config=config)

        breaker.record_failure("Loss 1", loss_usd=Decimal("400"))
        breaker.record_failure("Loss 2", loss_usd=Decimal("400"))
        assert breaker.state == CircuitBreakerState.CLOSED

        breaker.record_failure("Loss 3", loss_usd=Decimal("300"))  # Total: 1100
        assert breaker.state == CircuitBreakerState.OPEN

        result = breaker.check()
        assert result.trip_reason == TripReason.CUMULATIVE_LOSS

    def test_cumulative_loss_tracked_in_result(self, breaker: CircuitBreaker) -> None:
        """Test that cumulative loss is tracked in check result."""
        breaker.record_failure("Loss 1", loss_usd=Decimal("500"))

        result = breaker.check()
        assert result.cumulative_loss_usd == Decimal("500")


# =============================================================================
# Cooldown Tests
# =============================================================================


class TestCooldown:
    """Tests for cooldown behavior."""

    def test_blocks_during_cooldown(self, breaker: CircuitBreaker) -> None:
        """Test that execution is blocked during cooldown."""
        # Trip the breaker
        for _ in range(3):
            breaker.record_failure("Failure")

        result = breaker.check()
        assert result.can_execute is False
        assert result.cooldown_remaining_seconds is not None
        assert result.cooldown_remaining_seconds > 0

    def test_transitions_to_half_open_after_cooldown(self, config: CircuitBreakerConfig) -> None:
        """Test that breaker transitions to HALF_OPEN after cooldown."""
        # Use a very short cooldown for testing
        short_config = CircuitBreakerConfig(
            max_consecutive_failures=1,
            cooldown_seconds=0,  # Immediate
        )
        breaker = CircuitBreaker("test", config=short_config)

        breaker.record_failure("Failure")
        assert breaker.state == CircuitBreakerState.OPEN

        # Check after cooldown expires
        time.sleep(0.1)  # Give it a moment
        result = breaker.check()
        assert breaker.state == CircuitBreakerState.HALF_OPEN
        assert result.can_execute is True


# =============================================================================
# Half-Open State Tests
# =============================================================================


class TestHalfOpenState:
    """Tests for half-open state behavior."""

    def test_half_open_allows_execution(self, breaker: CircuitBreaker) -> None:
        """Test that half-open state allows execution."""
        # Force to half-open state
        breaker._state = CircuitBreakerState.HALF_OPEN

        result = breaker.check()
        assert result.can_execute is True
        assert result.state == CircuitBreakerState.HALF_OPEN

    def test_half_open_success_threshold(self, breaker: CircuitBreaker) -> None:
        """Test that enough successes in half-open closes the breaker."""
        breaker._state = CircuitBreakerState.HALF_OPEN

        # half_open_success_threshold = 2
        breaker.record_success()
        assert breaker.state == CircuitBreakerState.HALF_OPEN

        breaker.record_success()
        assert breaker.state == CircuitBreakerState.CLOSED

    def test_half_open_failure_trips_again(self, breaker: CircuitBreaker) -> None:
        """Test that failure in half-open trips back to open."""
        breaker._state = CircuitBreakerState.HALF_OPEN

        breaker.record_failure("Half-open failure")
        assert breaker.state == CircuitBreakerState.OPEN


# =============================================================================
# Manual Pause/Resume Tests
# =============================================================================


class TestManualPauseResume:
    """Tests for manual pause and resume functionality."""

    def test_pause_blocks_execution(self, breaker: CircuitBreaker) -> None:
        """Test that pause blocks execution."""
        breaker.pause(reason="Testing", operator="test_user")

        result = breaker.check()
        assert result.can_execute is False
        assert result.state == CircuitBreakerState.PAUSED
        assert "Testing" in result.reason

    def test_resume_restores_execution(self, breaker: CircuitBreaker) -> None:
        """Test that resume restores execution."""
        breaker.pause(reason="Testing", operator="test_user")
        assert breaker.state == CircuitBreakerState.PAUSED

        success = breaker.resume(operator_key="test_user")
        assert success is True
        assert breaker.state == CircuitBreakerState.CLOSED

        result = breaker.check()
        assert result.can_execute is True

    def test_resume_on_non_paused_returns_false(self, breaker: CircuitBreaker) -> None:
        """Test that resume on non-paused breaker returns False."""
        success = breaker.resume(operator_key="test_user")
        assert success is False

    def test_resume_resets_failures(self, breaker: CircuitBreaker) -> None:
        """Test that resume resets failure counter."""
        breaker.record_failure("Failure 1")
        breaker.record_failure("Failure 2")
        breaker.pause(reason="Testing", operator="test_user")
        breaker.resume(operator_key="test_user")

        result = breaker.check()
        assert result.consecutive_failures == 0


# =============================================================================
# Reset Tests
# =============================================================================


class TestReset:
    """Tests for reset functionality."""

    def test_reset_clears_all_state(self, breaker: CircuitBreaker) -> None:
        """Test that reset clears all state."""
        # Accumulate some state
        breaker.record_failure("Failure 1", loss_usd=Decimal("500"))
        breaker.record_failure("Failure 2", loss_usd=Decimal("500"))
        breaker.record_failure("Failure 3")  # Trip it
        assert breaker.state == CircuitBreakerState.OPEN

        breaker.reset()

        assert breaker.state == CircuitBreakerState.CLOSED
        result = breaker.check()
        assert result.consecutive_failures == 0
        assert result.cumulative_loss_usd == Decimal("0")


# =============================================================================
# Status Tests
# =============================================================================


class TestStatus:
    """Tests for status reporting."""

    def test_get_status(self, breaker: CircuitBreaker) -> None:
        """Test that status returns complete information."""
        breaker.record_failure("Test failure", loss_usd=Decimal("100"))

        status = breaker.get_status()

        assert status["strategy_id"] == "test_strategy"
        assert status["state"] == "closed"
        assert status["consecutive_failures"] == 1
        assert status["cumulative_loss_usd"] == "100"
        assert "config" in status


# =============================================================================
# Registry Tests
# =============================================================================


class TestCircuitBreakerRegistry:
    """Tests for CircuitBreakerRegistry."""

    def test_get_or_create(self, registry: CircuitBreakerRegistry) -> None:
        """Test get_or_create returns same instance."""
        breaker1 = registry.get_or_create("strategy_1")
        breaker2 = registry.get_or_create("strategy_1")
        assert breaker1 is breaker2

    def test_get_or_create_different_strategies(self, registry: CircuitBreakerRegistry) -> None:
        """Test get_or_create creates different instances for different strategies."""
        breaker1 = registry.get_or_create("strategy_1")
        breaker2 = registry.get_or_create("strategy_2")
        assert breaker1 is not breaker2

    def test_get_returns_none_for_unknown(self, registry: CircuitBreakerRegistry) -> None:
        """Test get returns None for unknown strategy."""
        result = registry.get("unknown")
        assert result is None

    def test_pause_all(self, registry: CircuitBreakerRegistry) -> None:
        """Test pause_all pauses all breakers."""
        registry.get_or_create("strategy_1")
        registry.get_or_create("strategy_2")

        count = registry.pause_all(reason="Global pause", operator="admin")
        assert count == 2
        assert registry.is_globally_paused() is True

        # All should be paused
        for breaker in [registry.get("strategy_1"), registry.get("strategy_2")]:
            assert breaker.state == CircuitBreakerState.PAUSED

    def test_resume_all(self, registry: CircuitBreakerRegistry) -> None:
        """Test resume_all resumes all paused breakers."""
        registry.get_or_create("strategy_1")
        registry.get_or_create("strategy_2")
        registry.pause_all(reason="Global pause", operator="admin")

        count = registry.resume_all(operator_key="admin")
        assert count == 2
        assert registry.is_globally_paused() is False

    def test_any_tripped(self, registry: CircuitBreakerRegistry) -> None:
        """Test any_tripped detection."""
        breaker1 = registry.get_or_create("strategy_1")
        registry.get_or_create("strategy_2")

        assert registry.any_tripped() is False

        breaker1.pause(reason="Test", operator="admin")
        assert registry.any_tripped() is True

    def test_get_tripped(self, registry: CircuitBreakerRegistry) -> None:
        """Test get_tripped returns correct list."""
        breaker1 = registry.get_or_create("strategy_1")
        registry.get_or_create("strategy_2")

        breaker1.pause(reason="Test", operator="admin")

        tripped = registry.get_tripped()
        assert tripped == ["strategy_1"]

    def test_get_all_status(self, registry: CircuitBreakerRegistry) -> None:
        """Test get_all_status returns all statuses."""
        registry.get_or_create("strategy_1")
        registry.get_or_create("strategy_2")

        all_status = registry.get_all_status()
        assert "strategy_1" in all_status
        assert "strategy_2" in all_status


# =============================================================================
# Global Registry Tests
# =============================================================================


class TestGlobalRegistry:
    """Tests for global registry singleton."""

    def test_global_registry_singleton(self) -> None:
        """Test that get_global_registry returns same instance."""
        registry1 = get_global_registry()
        registry2 = get_global_registry()
        assert registry1 is registry2

    def test_global_registry_is_functional(self) -> None:
        """Test that global registry works correctly."""
        registry = get_global_registry()
        breaker = registry.get_or_create("global_test_strategy")
        assert breaker is not None
        assert breaker.state == CircuitBreakerState.CLOSED
