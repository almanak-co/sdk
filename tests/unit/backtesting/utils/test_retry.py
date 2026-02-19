"""Unit tests for retry utilities with exponential backoff.

Tests cover:
- RetryConfig validation
- calculate_backoff_delay formula
- retry_with_backoff decorator (sync and async)
- RetryContext context manager
"""

import logging

import pytest

from almanak.framework.utils.retry import (
    DEFAULT_RETRY_CONFIG,
    RetryConfig,
    RetryContext,
    calculate_backoff_delay,
    retry_with_backoff,
)

# =============================================================================
# RetryConfig Tests
# =============================================================================


class TestRetryConfig:
    """Tests for RetryConfig dataclass."""

    def test_default_values(self) -> None:
        """Test default configuration values."""
        config = RetryConfig()
        assert config.max_retries == 3
        assert config.base_delay == 1.0
        assert config.max_delay == 32.0
        assert config.jitter_factor == 0.5
        assert TimeoutError in config.retryable_exceptions
        assert ConnectionError in config.retryable_exceptions
        assert OSError in config.retryable_exceptions

    def test_custom_values(self) -> None:
        """Test custom configuration values."""
        config = RetryConfig(
            max_retries=5,
            base_delay=2.0,
            max_delay=60.0,
            jitter_factor=0.25,
            retryable_exceptions=(ValueError,),
        )
        assert config.max_retries == 5
        assert config.base_delay == 2.0
        assert config.max_delay == 60.0
        assert config.jitter_factor == 0.25
        assert config.retryable_exceptions == (ValueError,)

    def test_validation_max_retries_negative(self) -> None:
        """Test that negative max_retries raises error."""
        with pytest.raises(ValueError, match="max_retries must be >= 0"):
            RetryConfig(max_retries=-1)

    def test_validation_max_retries_zero(self) -> None:
        """Test that zero max_retries is valid (no retries)."""
        config = RetryConfig(max_retries=0)
        assert config.max_retries == 0

    def test_validation_base_delay_zero(self) -> None:
        """Test that zero base_delay raises error."""
        with pytest.raises(ValueError, match="base_delay must be > 0"):
            RetryConfig(base_delay=0)

    def test_validation_base_delay_negative(self) -> None:
        """Test that negative base_delay raises error."""
        with pytest.raises(ValueError, match="base_delay must be > 0"):
            RetryConfig(base_delay=-1)

    def test_validation_max_delay_zero(self) -> None:
        """Test that zero max_delay raises error."""
        with pytest.raises(ValueError, match="max_delay must be > 0"):
            RetryConfig(max_delay=0)

    def test_validation_jitter_factor_out_of_range(self) -> None:
        """Test that jitter_factor outside 0-1 raises error."""
        with pytest.raises(ValueError, match="jitter_factor must be between 0 and 1"):
            RetryConfig(jitter_factor=1.5)
        with pytest.raises(ValueError, match="jitter_factor must be between 0 and 1"):
            RetryConfig(jitter_factor=-0.1)


# =============================================================================
# calculate_backoff_delay Tests
# =============================================================================


class TestCalculateBackoffDelay:
    """Tests for calculate_backoff_delay function."""

    def test_exponential_growth(self) -> None:
        """Test exponential growth of delay."""
        # Without jitter (set jitter_factor=0)
        delay_0 = calculate_backoff_delay(0, base_delay=1.0, jitter_factor=0)
        delay_1 = calculate_backoff_delay(1, base_delay=1.0, jitter_factor=0)
        delay_2 = calculate_backoff_delay(2, base_delay=1.0, jitter_factor=0)
        delay_3 = calculate_backoff_delay(3, base_delay=1.0, jitter_factor=0)

        assert delay_0 == 1.0  # 1 * 2^0 = 1
        assert delay_1 == 2.0  # 1 * 2^1 = 2
        assert delay_2 == 4.0  # 1 * 2^2 = 4
        assert delay_3 == 8.0  # 1 * 2^3 = 8

    def test_max_delay_cap(self) -> None:
        """Test that delay is capped at max_delay."""
        delay = calculate_backoff_delay(
            10, base_delay=1.0, max_delay=32.0, jitter_factor=0
        )
        assert delay == 32.0  # Would be 1024 without cap

    def test_custom_base_delay(self) -> None:
        """Test custom base delay."""
        delay = calculate_backoff_delay(2, base_delay=2.0, jitter_factor=0)
        assert delay == 8.0  # 2 * 2^2 = 8

    def test_jitter_adds_randomness(self) -> None:
        """Test that jitter adds randomness to delay."""
        # With jitter_factor=0.5, delay should be in range [base, base * 1.5]
        delays = [
            calculate_backoff_delay(0, base_delay=1.0, jitter_factor=0.5)
            for _ in range(100)
        ]

        # All delays should be >= base
        assert all(d >= 1.0 for d in delays)

        # All delays should be <= base + jitter (1.5)
        assert all(d <= 1.5 for d in delays)

        # There should be some variance (not all the same)
        assert len(set(delays)) > 1

    def test_jitter_respects_max_delay(self) -> None:
        """Test that jitter is calculated on capped delay."""
        # With high attempt, delay is capped first, then jitter applied
        delays = [
            calculate_backoff_delay(10, base_delay=1.0, max_delay=32.0, jitter_factor=0.5)
            for _ in range(100)
        ]

        # All delays should be >= max_delay (32)
        assert all(d >= 32.0 for d in delays)

        # All delays should be <= max_delay * (1 + jitter_factor)
        assert all(d <= 48.0 for d in delays)  # 32 * 1.5 = 48


# =============================================================================
# retry_with_backoff Decorator Tests (Async)
# =============================================================================


class TestRetryWithBackoffAsync:
    """Tests for retry_with_backoff decorator with async functions."""

    @pytest.mark.asyncio
    async def test_success_no_retry(self) -> None:
        """Test that successful call doesn't retry."""
        call_count = 0

        @retry_with_backoff(max_retries=3)
        async def succeeds() -> str:
            nonlocal call_count
            call_count += 1
            return "success"

        result = await succeeds()
        assert result == "success"
        assert call_count == 1

    @pytest.mark.asyncio
    async def test_retry_on_retryable_exception(self) -> None:
        """Test that retryable exception triggers retry."""
        call_count = 0

        @retry_with_backoff(
            max_retries=3,
            base_delay=0.01,  # Fast for testing
            retryable_exceptions=(TimeoutError,),
        )
        async def fails_then_succeeds() -> str:
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise TimeoutError("timeout")
            return "success"

        result = await fails_then_succeeds()
        assert result == "success"
        assert call_count == 3

    @pytest.mark.asyncio
    async def test_max_retries_exceeded(self) -> None:
        """Test that exception is raised after max retries."""
        call_count = 0

        @retry_with_backoff(
            max_retries=2,
            base_delay=0.01,
            retryable_exceptions=(TimeoutError,),
        )
        async def always_fails() -> str:
            nonlocal call_count
            call_count += 1
            raise TimeoutError("timeout")

        with pytest.raises(TimeoutError):
            await always_fails()

        # Initial call + 2 retries = 3 total attempts
        assert call_count == 3

    @pytest.mark.asyncio
    async def test_non_retryable_exception_not_retried(self) -> None:
        """Test that non-retryable exceptions are raised immediately."""
        call_count = 0

        @retry_with_backoff(
            max_retries=3,
            base_delay=0.01,
            retryable_exceptions=(TimeoutError,),
        )
        async def raises_value_error() -> str:
            nonlocal call_count
            call_count += 1
            raise ValueError("not retryable")

        with pytest.raises(ValueError):
            await raises_value_error()

        assert call_count == 1  # No retries for non-retryable exception

    @pytest.mark.asyncio
    async def test_on_retry_callback(self) -> None:
        """Test that on_retry callback is called on each retry."""
        retry_calls: list[tuple[int, Exception, float]] = []

        def on_retry(attempt: int, exc: Exception, delay: float) -> None:
            retry_calls.append((attempt, exc, delay))

        @retry_with_backoff(
            max_retries=2,
            base_delay=0.01,
            retryable_exceptions=(TimeoutError,),
            on_retry=on_retry,
        )
        async def fails_twice() -> str:
            if len(retry_calls) < 2:
                raise TimeoutError("timeout")
            return "success"

        result = await fails_twice()
        assert result == "success"
        assert len(retry_calls) == 2
        assert retry_calls[0][0] == 0  # First attempt
        assert retry_calls[1][0] == 1  # Second attempt
        assert all(isinstance(r[1], TimeoutError) for r in retry_calls)

    @pytest.mark.asyncio
    async def test_config_object(self) -> None:
        """Test using RetryConfig object."""
        config = RetryConfig(
            max_retries=1,
            base_delay=0.01,
            retryable_exceptions=(ConnectionError,),
        )
        call_count = 0

        @retry_with_backoff(config=config)
        async def fails_once() -> str:
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise ConnectionError("connection error")
            return "success"

        result = await fails_once()
        assert result == "success"
        assert call_count == 2


# =============================================================================
# retry_with_backoff Decorator Tests (Sync)
# =============================================================================


class TestRetryWithBackoffSync:
    """Tests for retry_with_backoff decorator with sync functions."""

    def test_success_no_retry_sync(self) -> None:
        """Test that successful sync call doesn't retry."""
        call_count = 0

        @retry_with_backoff(max_retries=3)
        def succeeds() -> str:
            nonlocal call_count
            call_count += 1
            return "success"

        result = succeeds()
        assert result == "success"
        assert call_count == 1

    def test_retry_on_retryable_exception_sync(self) -> None:
        """Test that retryable exception triggers retry for sync function."""
        call_count = 0

        @retry_with_backoff(
            max_retries=3,
            base_delay=0.01,
            retryable_exceptions=(OSError,),
        )
        def fails_then_succeeds() -> str:
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise OSError("os error")
            return "success"

        result = fails_then_succeeds()
        assert result == "success"
        assert call_count == 3

    def test_max_retries_exceeded_sync(self) -> None:
        """Test that exception is raised after max retries for sync function."""
        call_count = 0

        @retry_with_backoff(
            max_retries=2,
            base_delay=0.01,
            retryable_exceptions=(ConnectionError,),
        )
        def always_fails() -> str:
            nonlocal call_count
            call_count += 1
            raise ConnectionError("connection error")

        with pytest.raises(ConnectionError):
            always_fails()

        assert call_count == 3


# =============================================================================
# RetryContext Tests
# =============================================================================


class TestRetryContext:
    """Tests for RetryContext context manager."""

    @pytest.mark.asyncio
    async def test_async_context_success(self) -> None:
        """Test async context manager with successful operation."""
        attempts = 0

        async with RetryContext(max_retries=3) as ctx:
            while ctx.should_retry():
                try:
                    attempts += 1
                    if attempts < 3:
                        raise TimeoutError("timeout")
                    break
                except TimeoutError as e:
                    await ctx.handle_error_async(e)

        assert attempts == 3
        assert ctx.attempt == 2  # 0, 1, 2

    @pytest.mark.asyncio
    async def test_async_context_all_retries_fail(self) -> None:
        """Test async context manager when all retries fail."""
        async with RetryContext(max_retries=2, base_delay=0.01) as ctx:
            with pytest.raises(TimeoutError):
                while ctx.should_retry():
                    try:
                        raise TimeoutError("timeout")
                    except TimeoutError as e:
                        await ctx.handle_error_async(e)

    def test_sync_context_success(self) -> None:
        """Test sync context manager with successful operation."""
        attempts = 0

        with RetryContext(max_retries=3, base_delay=0.01) as ctx:
            while ctx.should_retry():
                try:
                    attempts += 1
                    if attempts < 2:
                        raise ConnectionError("connection")
                    break
                except ConnectionError as e:
                    ctx.handle_error_sync(e)

        assert attempts == 2

    def test_context_properties(self) -> None:
        """Test context manager properties."""
        with RetryContext(max_retries=3) as ctx:
            assert ctx.attempt == 0
            assert ctx.last_error is None
            assert ctx.should_retry() is True


# =============================================================================
# Logging Tests
# =============================================================================


class TestRetryLogging:
    """Tests for retry logging behavior."""

    @pytest.mark.asyncio
    async def test_logs_retry_attempts(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test that retry attempts are logged."""
        @retry_with_backoff(
            max_retries=2,
            base_delay=0.01,
            retryable_exceptions=(TimeoutError,),
        )
        async def fails_once() -> str:
            if not hasattr(fails_once, "called"):
                fails_once.called = True  # type: ignore[attr-defined]
                raise TimeoutError("timeout")
            return "success"

        with caplog.at_level(logging.DEBUG, logger="almanak.framework.utils.retry"):
            result = await fails_once()

        assert result == "success"
        assert any("Retry 1/2" in record.message for record in caplog.records)

    @pytest.mark.asyncio
    async def test_logs_all_attempts_failed(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test that final failure is logged at WARNING level."""
        @retry_with_backoff(
            max_retries=1,
            base_delay=0.01,
            retryable_exceptions=(TimeoutError,),
        )
        async def always_fails() -> str:
            raise TimeoutError("timeout")

        with caplog.at_level(logging.WARNING, logger="almanak.framework.utils.retry"):
            with pytest.raises(TimeoutError):
                await always_fails()

        assert any(
            "All 2 attempts failed" in record.message for record in caplog.records
        )


# =============================================================================
# Default Config Tests
# =============================================================================


class TestDefaultRetryConfig:
    """Tests for DEFAULT_RETRY_CONFIG."""

    def test_default_config_exists(self) -> None:
        """Test that default config is properly defined."""
        assert DEFAULT_RETRY_CONFIG is not None
        assert isinstance(DEFAULT_RETRY_CONFIG, RetryConfig)
        assert DEFAULT_RETRY_CONFIG.max_retries == 3
        assert DEFAULT_RETRY_CONFIG.base_delay == 1.0
        assert DEFAULT_RETRY_CONFIG.max_delay == 32.0
