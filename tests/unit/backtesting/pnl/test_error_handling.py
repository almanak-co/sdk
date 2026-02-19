"""Unit tests for error handling and retry behavior in backtesting.

Tests the error classification, circuit breaker, and error handler
functionality including retry behavior.
"""

import logging
from datetime import datetime

import pytest

from almanak.framework.backtesting.models import BacktestEngine, BacktestMetrics, BacktestResult
from almanak.framework.backtesting.pnl.error_handling import (
    BacktestCircuitBreaker,
    BacktestCircuitBreakerState,
    BacktestErrorConfig,
    BacktestErrorHandler,
    ErrorCategory,
    ErrorType,
    classify_error,
    is_fatal_error,
    is_non_critical_error,
    is_recoverable_error,
)

# =============================================================================
# Error Classification Tests
# =============================================================================


class TestErrorClassification:
    """Tests for error classification."""

    def test_classify_rate_limit_error(self) -> None:
        """Rate limit errors should be classified as recoverable."""
        error = Exception("Rate limit exceeded: too many requests")
        classification = classify_error(error)

        assert classification.error_type == ErrorType.RATE_LIMIT
        assert classification.category == ErrorCategory.RECOVERABLE
        assert classification.is_recoverable is True
        assert classification.is_fatal is False

    def test_classify_429_error(self) -> None:
        """HTTP 429 errors should be classified as rate limit."""
        error = Exception("HTTP 429: Too Many Requests")
        classification = classify_error(error)

        assert classification.error_type == ErrorType.RATE_LIMIT
        assert classification.is_recoverable is True

    def test_classify_timeout_error(self) -> None:
        """Timeout errors should be classified as recoverable."""
        error = TimeoutError("Connection timed out")
        classification = classify_error(error)

        assert classification.error_type == ErrorType.TIMEOUT
        assert classification.is_recoverable is True

    def test_classify_connection_error(self) -> None:
        """Connection errors should be classified as recoverable."""
        error = ConnectionError("Connection refused")
        classification = classify_error(error)

        assert classification.error_type == ErrorType.CONNECTION_ERROR
        assert classification.is_recoverable is True

    def test_classify_rpc_error(self) -> None:
        """RPC errors should be classified as recoverable."""
        error = Exception("JSON-RPC error: node error")
        classification = classify_error(error)

        assert classification.error_type == ErrorType.RPC_ERROR
        assert classification.is_recoverable is True

    def test_classify_authentication_error(self) -> None:
        """Authentication errors should be classified as fatal."""
        error = Exception("HTTP 401: Unauthorized - invalid API key")
        classification = classify_error(error)

        assert classification.error_type == ErrorType.AUTHENTICATION_FAILED
        assert classification.category == ErrorCategory.FATAL
        assert classification.is_fatal is True

    def test_classify_invalid_config_error(self) -> None:
        """Invalid config errors should be classified as fatal."""
        error = ValueError("Invalid config: missing required field")
        classification = classify_error(error)

        assert classification.error_type == ErrorType.INVALID_CONFIG
        assert classification.is_fatal is True

    def test_classify_missing_price_error(self) -> None:
        """Missing price errors should be classified as non-critical."""
        error = Exception("No price found for unknown token")
        classification = classify_error(error)

        assert classification.error_type == ErrorType.MISSING_PRICE
        assert classification.category == ErrorCategory.NON_CRITICAL
        assert classification.is_non_critical is True

    def test_classify_stale_data_error(self) -> None:
        """Stale data errors should be classified as non-critical."""
        error = Exception("Data is stale: last update 2 hours ago")
        classification = classify_error(error)

        assert classification.error_type == ErrorType.STALE_DATA
        assert classification.is_non_critical is True

    def test_classify_unknown_error(self) -> None:
        """Unknown errors should be classified as fatal (safe default)."""
        error = Exception("Something unexpected happened")
        classification = classify_error(error)

        assert classification.error_type == ErrorType.UNKNOWN
        assert classification.is_fatal is True

    def test_suggested_action_provided(self) -> None:
        """Classification should include suggested action."""
        error = Exception("Rate limit exceeded")
        classification = classify_error(error)

        assert classification.suggested_action
        assert "retry" in classification.suggested_action.lower() or "backoff" in classification.suggested_action.lower()


# =============================================================================
# Convenience Function Tests
# =============================================================================


class TestConvenienceFunctions:
    """Tests for convenience functions."""

    def test_is_recoverable_error(self) -> None:
        """is_recoverable_error should return True for recoverable errors."""
        assert is_recoverable_error(Exception("Rate limit exceeded")) is True
        assert is_recoverable_error(TimeoutError("timed out")) is True
        assert is_recoverable_error(Exception("invalid config")) is False

    def test_is_fatal_error(self) -> None:
        """is_fatal_error should return True for fatal errors."""
        assert is_fatal_error(Exception("invalid config")) is True
        assert is_fatal_error(Exception("authentication failed")) is True
        assert is_fatal_error(Exception("rate limit")) is False

    def test_is_non_critical_error(self) -> None:
        """is_non_critical_error should return True for non-critical errors."""
        assert is_non_critical_error(Exception("missing price")) is True
        assert is_non_critical_error(Exception("stale data")) is True
        assert is_non_critical_error(Exception("rate limit")) is False


# =============================================================================
# Circuit Breaker Tests
# =============================================================================


class TestBacktestCircuitBreaker:
    """Tests for circuit breaker functionality."""

    def test_initial_state_is_closed(self) -> None:
        """Circuit breaker should start in closed state."""
        breaker = BacktestCircuitBreaker(threshold=3)

        assert breaker.state == BacktestCircuitBreakerState.CLOSED
        assert breaker.can_proceed() is True
        assert breaker.consecutive_failures == 0

    def test_single_failure_does_not_trip(self) -> None:
        """Single failure should not trip the circuit breaker."""
        breaker = BacktestCircuitBreaker(threshold=3)

        breaker.record_failure(Exception("error 1"))

        assert breaker.state == BacktestCircuitBreakerState.CLOSED
        assert breaker.can_proceed() is True
        assert breaker.consecutive_failures == 1

    def test_trips_after_threshold_failures(self) -> None:
        """Circuit breaker should trip after threshold consecutive failures."""
        breaker = BacktestCircuitBreaker(threshold=3)

        breaker.record_failure(Exception("error 1"))
        breaker.record_failure(Exception("error 2"))
        breaker.record_failure(Exception("error 3"))

        assert breaker.state == BacktestCircuitBreakerState.OPEN
        assert breaker.is_open is True
        assert breaker.can_proceed() is False

    def test_success_resets_failure_count(self) -> None:
        """Success should reset consecutive failure count."""
        breaker = BacktestCircuitBreaker(threshold=3)

        breaker.record_failure(Exception("error 1"))
        breaker.record_failure(Exception("error 2"))
        breaker.record_success()

        assert breaker.consecutive_failures == 0
        assert breaker.state == BacktestCircuitBreakerState.CLOSED

    def test_reset_clears_state(self) -> None:
        """Reset should clear all state."""
        breaker = BacktestCircuitBreaker(threshold=3)

        # Trip the breaker
        for _ in range(3):
            breaker.record_failure(Exception("error"))

        # Reset
        breaker.reset()

        assert breaker.state == BacktestCircuitBreakerState.CLOSED
        assert breaker.consecutive_failures == 0
        assert breaker.can_proceed() is True

    def test_get_status_returns_dict(self) -> None:
        """get_status should return status dictionary."""
        breaker = BacktestCircuitBreaker(threshold=3, cooldown_seconds=60)

        status = breaker.get_status()

        assert "state" in status
        assert "consecutive_failures" in status
        assert "threshold" in status
        assert status["threshold"] == 3
        assert status["cooldown_seconds"] == 60


# =============================================================================
# Error Handler Tests
# =============================================================================


class TestBacktestErrorHandler:
    """Tests for error handler functionality."""

    def test_handler_initializes_with_defaults(self) -> None:
        """Handler should initialize with default config."""
        handler = BacktestErrorHandler()

        assert handler.config.circuit_breaker_threshold == 5
        assert handler.error_count == 0

    def test_handler_initializes_with_custom_config(self) -> None:
        """Handler should accept custom config."""
        config = BacktestErrorConfig(circuit_breaker_threshold=3)
        handler = BacktestErrorHandler(config)

        assert handler.config.circuit_breaker_threshold == 3

    def test_handle_fatal_error_returns_should_stop(self) -> None:
        """Fatal errors should return should_stop=True."""
        handler = BacktestErrorHandler()

        result = handler.handle_error(
            Exception("Invalid config error"),
            context="initialization"
        )

        assert result.should_stop is True
        assert result.should_retry is False
        assert result.error_record is not None
        assert handler.error_count == 1

    def test_handle_recoverable_error_returns_should_retry(self) -> None:
        """Recoverable errors should return should_retry=True."""
        handler = BacktestErrorHandler()

        result = handler.handle_error(
            Exception("Rate limit exceeded"),
            context="price_fetch"
        )

        assert result.should_retry is True
        assert result.should_stop is False
        assert result.error_record is not None

    def test_handle_non_critical_error_returns_should_continue(self) -> None:
        """Non-critical errors should return should_continue=True."""
        config = BacktestErrorConfig(continue_on_non_critical=True)
        handler = BacktestErrorHandler(config)

        result = handler.handle_error(
            Exception("Missing price for token"),
            context="tick_42"
        )

        assert result.should_continue is True
        assert result.should_stop is False
        assert result.warning_message is not None

    def test_circuit_breaker_trips_after_repeated_failures(self) -> None:
        """Circuit breaker should trip after repeated recoverable errors."""
        config = BacktestErrorConfig(circuit_breaker_threshold=3)
        handler = BacktestErrorHandler(config)

        # First two failures should allow retry
        for i in range(2):
            result = handler.handle_error(
                Exception("Rate limit exceeded"),
                context=f"fetch_{i}"
            )
            assert result.should_retry is True

        # Third failure trips the breaker
        result = handler.handle_error(
            Exception("Rate limit exceeded"),
            context="fetch_2"
        )
        assert result.should_stop is True
        assert handler.circuit_breaker.is_open is True

    def test_success_resets_circuit_breaker(self) -> None:
        """Recording success should reset circuit breaker."""
        config = BacktestErrorConfig(circuit_breaker_threshold=3)
        handler = BacktestErrorHandler(config)

        # Record two failures
        for _ in range(2):
            handler.handle_error(Exception("Rate limit exceeded"), context="test")

        # Record success
        handler.record_success()

        # Circuit breaker should be reset
        assert handler.circuit_breaker.consecutive_failures == 0

    def test_get_error_summary(self) -> None:
        """get_error_summary should return statistics."""
        handler = BacktestErrorHandler()

        handler.handle_error(Exception("Invalid config"), context="init")
        handler.handle_error(Exception("Rate limit"), context="fetch")
        handler.handle_error(Exception("Missing price"), context="tick")

        summary = handler.get_error_summary()

        assert summary["total_errors"] == 3
        assert summary["fatal_errors"] >= 1
        assert "errors_by_type" in summary
        assert "circuit_breaker" in summary

    def test_get_errors_as_dicts(self) -> None:
        """get_errors_as_dicts should return serializable list."""
        handler = BacktestErrorHandler()

        handler.handle_error(Exception("Test error"), context="test")

        errors = handler.get_errors_as_dicts()

        assert len(errors) == 1
        assert "timestamp" in errors[0]
        assert "error_message" in errors[0]
        assert "classification" in errors[0]
        assert "context" in errors[0]

    def test_reset_clears_all_state(self) -> None:
        """Reset should clear all error tracking state."""
        handler = BacktestErrorHandler()

        # Add some errors
        for _ in range(3):
            handler.handle_error(Exception("Test error"), context="test")

        # Reset
        handler.reset()

        assert handler.error_count == 0
        assert handler.non_critical_count == 0
        assert handler.circuit_breaker.consecutive_failures == 0

    def test_max_non_critical_errors_triggers_stop(self) -> None:
        """Exceeding max non-critical errors should trigger stop."""
        config = BacktestErrorConfig(max_non_critical_errors=3)
        handler = BacktestErrorHandler(config)

        # First 3 non-critical errors should continue
        for i in range(3):
            result = handler.handle_error(
                Exception("Missing price"),
                context=f"tick_{i}"
            )
            assert result.should_continue is True

        # 4th non-critical error should stop
        result = handler.handle_error(
            Exception("Missing price"),
            context="tick_3"
        )
        assert result.should_stop is True


# =============================================================================
# BacktestResult Error Integration Tests
# =============================================================================


class TestBacktestResultErrors:
    """Tests for BacktestResult error tracking."""

    def test_errors_field_defaults_to_empty_list(self) -> None:
        """errors field should default to empty list."""
        result = BacktestResult(
            engine=BacktestEngine.PNL,
            strategy_id="test_strategy",
            start_time=datetime(2024, 1, 1),
            end_time=datetime(2024, 1, 31),
            metrics=BacktestMetrics(),
        )

        assert result.errors == []

    def test_add_error_appends_to_list(self) -> None:
        """add_error should append error to list."""
        result = BacktestResult(
            engine=BacktestEngine.PNL,
            strategy_id="test_strategy",
            start_time=datetime(2024, 1, 1),
            end_time=datetime(2024, 1, 31),
            metrics=BacktestMetrics(),
        )

        error_dict = {
            "timestamp": "2024-01-15T10:30:00+00:00",
            "error_type": "RateLimit",
            "error_message": "Rate limit exceeded",
            "classification": {
                "error_type": "rate_limit",
                "category": "recoverable",
                "is_recoverable": True,
                "is_fatal": False,
                "is_non_critical": False,
                "suggested_action": "Retry with backoff",
            },
            "context": "price_fetch",
            "handled": "retry",
        }

        result.add_error(error_dict)

        assert len(result.errors) == 1
        assert result.errors[0]["error_type"] == "RateLimit"

    def test_add_error_logs_fatal_error(self, caplog: pytest.LogCaptureFixture) -> None:
        """add_error should log fatal errors at ERROR level."""
        result = BacktestResult(
            engine=BacktestEngine.PNL,
            strategy_id="test_strategy",
            start_time=datetime(2024, 1, 1),
            end_time=datetime(2024, 1, 31),
            metrics=BacktestMetrics(),
        )

        error_dict = {
            "timestamp": "2024-01-15T10:30:00+00:00",
            "error_type": "ValueError",
            "error_message": "Invalid configuration",
            "classification": {
                "is_fatal": True,
                "is_recoverable": False,
                "is_non_critical": False,
            },
            "context": "init",
            "handled": "stop",
        }

        with caplog.at_level(logging.ERROR, logger="almanak.framework.backtesting.models"):
            result.add_error(error_dict)

        assert "Backtest error" in caplog.text
        assert "ValueError" in caplog.text

    def test_add_error_logs_recoverable_error(self, caplog: pytest.LogCaptureFixture) -> None:
        """add_error should log recoverable errors at WARNING level."""
        result = BacktestResult(
            engine=BacktestEngine.PNL,
            strategy_id="test_strategy",
            start_time=datetime(2024, 1, 1),
            end_time=datetime(2024, 1, 31),
            metrics=BacktestMetrics(),
        )

        error_dict = {
            "timestamp": "2024-01-15T10:30:00+00:00",
            "error_type": "RateLimitError",
            "error_message": "Too many requests",
            "classification": {
                "is_fatal": False,
                "is_recoverable": True,
                "is_non_critical": False,
            },
            "context": "fetch",
            "handled": "retry",
        }

        with caplog.at_level(logging.WARNING, logger="almanak.framework.backtesting.models"):
            result.add_error(error_dict)

        assert "Recoverable error" in caplog.text

    def test_to_dict_includes_errors(self) -> None:
        """to_dict should include errors field."""
        result = BacktestResult(
            engine=BacktestEngine.PNL,
            strategy_id="test_strategy",
            start_time=datetime(2024, 1, 1),
            end_time=datetime(2024, 1, 31),
            metrics=BacktestMetrics(),
            errors=[{"timestamp": "2024-01-15T10:30:00", "error_type": "Test"}],
        )

        data = result.to_dict()

        assert "errors" in data
        assert len(data["errors"]) == 1

    def test_from_dict_restores_errors(self) -> None:
        """from_dict should restore errors field."""
        original = BacktestResult(
            engine=BacktestEngine.PNL,
            strategy_id="test_strategy",
            start_time=datetime(2024, 1, 1),
            end_time=datetime(2024, 1, 31),
            metrics=BacktestMetrics(),
            errors=[
                {
                    "timestamp": "2024-01-15T10:30:00",
                    "error_type": "Test",
                    "error_message": "Test error",
                }
            ],
        )

        data = original.to_dict()
        restored = BacktestResult.from_dict(data)

        assert len(restored.errors) == 1
        assert restored.errors[0]["error_type"] == "Test"

    def test_summary_includes_error_stats_when_errors_present(self) -> None:
        """summary should include error statistics when errors are present."""
        result = BacktestResult(
            engine=BacktestEngine.PNL,
            strategy_id="test_strategy",
            start_time=datetime(2024, 1, 1),
            end_time=datetime(2024, 1, 31),
            metrics=BacktestMetrics(),
            errors=[
                {
                    "classification": {"is_fatal": True, "is_recoverable": False, "is_non_critical": False},
                },
                {
                    "classification": {"is_fatal": False, "is_recoverable": True, "is_non_critical": False},
                },
                {
                    "classification": {"is_fatal": False, "is_recoverable": False, "is_non_critical": True},
                },
            ],
        )

        summary = result.summary()

        assert "ERROR SUMMARY" in summary
        assert "Total Errors:" in summary
        assert "3" in summary
        assert "Fatal Errors:" in summary
        assert "Recoverable Errors:" in summary
        assert "Non-Critical Errors:" in summary

    def test_summary_excludes_error_stats_when_no_errors(self) -> None:
        """summary should not include error section when no errors."""
        result = BacktestResult(
            engine=BacktestEngine.PNL,
            strategy_id="test_strategy",
            start_time=datetime(2024, 1, 1),
            end_time=datetime(2024, 1, 31),
            metrics=BacktestMetrics(),
        )

        summary = result.summary()

        assert "ERROR SUMMARY" not in summary


# =============================================================================
# Retry Behavior Tests
# =============================================================================


class TestRetryBehavior:
    """Tests specifically for retry behavior validation."""

    def test_retry_returns_true_for_rate_limit(self) -> None:
        """should_retry should be True for rate limit errors."""
        handler = BacktestErrorHandler()

        result = handler.handle_error(
            Exception("Rate limit exceeded"),
            context="api_call"
        )

        assert result.should_retry is True
        assert result.should_stop is False

    def test_retry_returns_true_for_timeout(self) -> None:
        """should_retry should be True for timeout errors."""
        handler = BacktestErrorHandler()

        result = handler.handle_error(
            TimeoutError("Connection timed out"),
            context="api_call"
        )

        assert result.should_retry is True

    def test_retry_returns_true_for_connection_error(self) -> None:
        """should_retry should be True for connection errors."""
        handler = BacktestErrorHandler()

        result = handler.handle_error(
            ConnectionError("Connection refused"),
            context="api_call"
        )

        assert result.should_retry is True

    def test_no_retry_for_fatal_errors(self) -> None:
        """should_retry should be False for fatal errors."""
        handler = BacktestErrorHandler()

        result = handler.handle_error(
            Exception("Invalid configuration error"),
            context="init"
        )

        assert result.should_retry is False
        assert result.should_stop is True

    def test_no_retry_after_circuit_breaker_trips(self) -> None:
        """should_retry should be False after circuit breaker trips."""
        config = BacktestErrorConfig(circuit_breaker_threshold=2)
        handler = BacktestErrorHandler(config)

        # First failure - should retry
        result1 = handler.handle_error(Exception("Rate limit"), context="call_1")
        assert result1.should_retry is True

        # Second failure - trips breaker
        result2 = handler.handle_error(Exception("Rate limit"), context="call_2")
        assert result2.should_stop is True

        # Third call - breaker is open
        result3 = handler.handle_error(Exception("Rate limit"), context="call_3")
        assert result3.should_stop is True
        assert result3.should_retry is False

    def test_retry_count_tracked_in_handler(self) -> None:
        """Handler should track number of recoverable errors."""
        handler = BacktestErrorHandler()

        # Multiple recoverable errors
        for _ in range(3):
            handler.handle_error(Exception("Rate limit"), context="test")

        summary = handler.get_error_summary()
        assert summary["recoverable_errors"] == 3

    def test_retry_with_different_contexts(self) -> None:
        """Retry should work with different contexts."""
        handler = BacktestErrorHandler()

        contexts = ["price_fetch", "balance_query", "tx_submission"]

        for ctx in contexts:
            result = handler.handle_error(
                Exception("Rate limit exceeded"),
                context=ctx
            )
            assert result.should_retry is True
            assert result.error_record.context == ctx

    def test_error_record_contains_handled_action(self) -> None:
        """Error record should contain how it was handled."""
        handler = BacktestErrorHandler()

        result = handler.handle_error(
            Exception("Rate limit exceeded"),
            context="test"
        )

        assert result.error_record is not None
        assert result.error_record.handled == "retry"
