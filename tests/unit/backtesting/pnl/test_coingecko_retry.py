"""Unit tests for CoinGecko retry logic with exponential backoff.

This module tests the retry behavior of CoinGeckoDataProvider, covering:
- RetryConfig configuration
- Exponential backoff delay calculation
- Automatic retry on 429 responses
- Max retries exceeded behavior
- CoinGeckoRateLimitError exception
- Successful retry after initial failures
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest

from almanak.framework.backtesting.pnl.providers.coingecko import (
    CoinGeckoDataProvider,
    CoinGeckoRateLimitError,
    RetryConfig,
)


class TestRetryConfig:
    """Tests for RetryConfig dataclass."""

    def test_default_values(self):
        """Test default configuration values."""
        config = RetryConfig()
        assert config.max_retries == 3
        assert config.base_delay == 1.0
        assert config.max_delay == 8.0
        assert config.exponential_base == 2

    def test_custom_values(self):
        """Test custom configuration values."""
        config = RetryConfig(
            max_retries=5,
            base_delay=0.5,
            max_delay=16.0,
            exponential_base=3,
        )
        assert config.max_retries == 5
        assert config.base_delay == 0.5
        assert config.max_delay == 16.0
        assert config.exponential_base == 3

    def test_get_delay_for_attempt_first(self):
        """Test delay calculation for first attempt."""
        config = RetryConfig()
        delay = config.get_delay_for_attempt(1)
        assert delay == 1.0  # 1 * 2^0 = 1

    def test_get_delay_for_attempt_second(self):
        """Test delay calculation for second attempt."""
        config = RetryConfig()
        delay = config.get_delay_for_attempt(2)
        assert delay == 2.0  # 1 * 2^1 = 2

    def test_get_delay_for_attempt_third(self):
        """Test delay calculation for third attempt."""
        config = RetryConfig()
        delay = config.get_delay_for_attempt(3)
        assert delay == 4.0  # 1 * 2^2 = 4

    def test_get_delay_for_attempt_fourth(self):
        """Test delay calculation for fourth attempt."""
        config = RetryConfig()
        delay = config.get_delay_for_attempt(4)
        assert delay == 8.0  # 1 * 2^3 = 8

    def test_get_delay_capped_at_max(self):
        """Test delay is capped at max_delay."""
        config = RetryConfig(max_delay=8.0)
        delay = config.get_delay_for_attempt(10)
        assert delay == 8.0  # Capped at max_delay

    def test_get_delay_custom_base(self):
        """Test delay with custom exponential base."""
        config = RetryConfig(base_delay=1.0, exponential_base=3)
        assert config.get_delay_for_attempt(1) == 1.0  # 1 * 3^0 = 1
        assert config.get_delay_for_attempt(2) == 3.0  # 1 * 3^1 = 3
        assert config.get_delay_for_attempt(3) == 8.0  # 1 * 3^2 = 9, capped at 8


class TestCoinGeckoRateLimitError:
    """Tests for CoinGeckoRateLimitError exception."""

    def test_error_message(self):
        """Test error message is preserved."""
        error = CoinGeckoRateLimitError("Rate limit exceeded")
        assert str(error) == "Rate limit exceeded"

    def test_error_with_retry_count(self):
        """Test error with retry count attribute."""
        error = CoinGeckoRateLimitError("Error", retry_count=3)
        assert error.retry_count == 3

    def test_error_with_last_backoff(self):
        """Test error with last backoff attribute."""
        error = CoinGeckoRateLimitError("Error", last_backoff=8.0)
        assert error.last_backoff == 8.0

    def test_error_default_values(self):
        """Test error default attribute values."""
        error = CoinGeckoRateLimitError("Error")
        assert error.retry_count == 0
        assert error.last_backoff == 0.0

    def test_error_all_attributes(self):
        """Test error with all attributes."""
        error = CoinGeckoRateLimitError(
            "Rate limit exceeded after 3 retries",
            retry_count=3,
            last_backoff=4.0,
        )
        assert "3 retries" in str(error)
        assert error.retry_count == 3
        assert error.last_backoff == 4.0


class TestCoinGeckoProviderRetry:
    """Tests for CoinGeckoDataProvider retry behavior."""

    @pytest.fixture
    def mock_response_429(self):
        """Create a mock 429 response."""
        response = MagicMock()
        response.status = 429
        response.__aenter__ = AsyncMock(return_value=response)
        response.__aexit__ = AsyncMock(return_value=None)
        return response

    @pytest.fixture
    def mock_response_200(self):
        """Create a mock 200 response."""
        response = MagicMock()
        response.status = 200
        response.json = AsyncMock(return_value={"data": "test"})
        response.__aenter__ = AsyncMock(return_value=response)
        response.__aexit__ = AsyncMock(return_value=None)
        return response

    @pytest.fixture
    def provider(self):
        """Create a provider with minimal retry config for faster tests."""
        return CoinGeckoDataProvider(
            retry_config=RetryConfig(
                max_retries=3,
                base_delay=0.01,  # Very short delay for tests
                max_delay=0.04,
            )
        )

    @pytest.mark.asyncio
    async def test_retry_config_passed_to_provider(self):
        """Test retry config is stored in provider."""
        config = RetryConfig(max_retries=5)
        provider = CoinGeckoDataProvider(retry_config=config)
        assert provider._retry_config.max_retries == 5
        await provider.close()

    @pytest.mark.asyncio
    async def test_default_retry_config(self):
        """Test default retry config is created."""
        provider = CoinGeckoDataProvider()
        assert provider._retry_config.max_retries == 3
        assert provider._retry_config.base_delay == 1.0
        await provider.close()

    @pytest.mark.asyncio
    async def test_successful_request_no_retry(self, provider, mock_response_200):
        """Test successful request doesn't trigger retry."""
        with patch.object(
            provider, "_get_session"
        ) as mock_session, patch.object(
            provider, "_wait_for_rate_limit", new_callable=AsyncMock
        ):
            session = MagicMock()
            session.get = MagicMock(return_value=mock_response_200)
            mock_session.return_value = session

            result = await provider._make_request("/test", {})

            assert result == {"data": "test"}
            assert session.get.call_count == 1

        await provider.close()

    @pytest.mark.asyncio
    async def test_retry_on_429_then_success(
        self, provider, mock_response_429, mock_response_200
    ):
        """Test retry on 429 followed by success."""
        with patch.object(
            provider, "_get_session"
        ) as mock_session, patch.object(
            provider, "_wait_for_rate_limit", new_callable=AsyncMock
        ):
            session = MagicMock()
            # First call returns 429, second returns 200
            session.get = MagicMock(
                side_effect=[mock_response_429, mock_response_200]
            )
            mock_session.return_value = session

            result = await provider._make_request("/test", {})

            assert result == {"data": "test"}
            assert session.get.call_count == 2

        await provider.close()

    @pytest.mark.asyncio
    async def test_retry_multiple_429s_then_success(
        self, provider, mock_response_429, mock_response_200
    ):
        """Test multiple retries on 429 followed by success."""
        with patch.object(
            provider, "_get_session"
        ) as mock_session, patch.object(
            provider, "_wait_for_rate_limit", new_callable=AsyncMock
        ):
            session = MagicMock()
            # First two calls return 429, third returns 200
            session.get = MagicMock(
                side_effect=[
                    mock_response_429,
                    mock_response_429,
                    mock_response_200,
                ]
            )
            mock_session.return_value = session

            result = await provider._make_request("/test", {})

            assert result == {"data": "test"}
            assert session.get.call_count == 3

        await provider.close()

    @pytest.mark.asyncio
    async def test_max_retries_exceeded_raises_error(
        self, provider, mock_response_429
    ):
        """Test CoinGeckoRateLimitError raised after max retries."""
        with patch.object(
            provider, "_get_session"
        ) as mock_session, patch.object(
            provider, "_wait_for_rate_limit", new_callable=AsyncMock
        ):
            session = MagicMock()
            # All calls return 429
            session.get = MagicMock(return_value=mock_response_429)
            mock_session.return_value = session

            with pytest.raises(CoinGeckoRateLimitError) as exc_info:
                await provider._make_request("/test", {})

            # 1 initial + 3 retries = 4 total calls
            assert session.get.call_count == 4
            assert exc_info.value.retry_count == 4
            assert "3 retries" in str(exc_info.value)

        await provider.close()

    @pytest.mark.asyncio
    async def test_retry_logs_warning(
        self, provider, mock_response_429, mock_response_200, caplog
    ):
        """Test retry attempts are logged."""
        import logging

        with patch.object(
            provider, "_get_session"
        ) as mock_session, patch.object(
            provider, "_wait_for_rate_limit", new_callable=AsyncMock
        ):
            session = MagicMock()
            session.get = MagicMock(
                side_effect=[mock_response_429, mock_response_200]
            )
            mock_session.return_value = session

            with caplog.at_level(
                logging.WARNING,
                logger="almanak.framework.backtesting.pnl.providers.coingecko",
            ):
                await provider._make_request("/test", {})

            # Check that warning was logged
            assert any("429" in record.message for record in caplog.records)
            assert any("retry" in record.message.lower() for record in caplog.records)

        await provider.close()

    @pytest.mark.asyncio
    async def test_retry_with_backoff_delay(self, mock_response_429, mock_response_200):
        """Test retry waits for backoff delay."""
        # Use slightly longer delays to measure
        provider = CoinGeckoDataProvider(
            retry_config=RetryConfig(
                max_retries=1,
                base_delay=0.1,  # 100ms
            )
        )

        with patch.object(
            provider, "_get_session"
        ) as mock_session, patch.object(
            provider, "_wait_for_rate_limit", new_callable=AsyncMock
        ):
            session = MagicMock()
            session.get = MagicMock(
                side_effect=[mock_response_429, mock_response_200]
            )
            mock_session.return_value = session

            import time

            start = time.monotonic()
            await provider._make_request("/test", {})
            elapsed = time.monotonic() - start

            # Should have waited at least the backoff delay
            assert elapsed >= 0.1

        await provider.close()

    @pytest.mark.asyncio
    async def test_non_429_error_not_retried(self, provider):
        """Test non-429 errors are not retried."""
        mock_response = MagicMock()
        mock_response.status = 500
        mock_response.text = AsyncMock(return_value="Server Error")
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        with patch.object(
            provider, "_get_session"
        ) as mock_session, patch.object(
            provider, "_wait_for_rate_limit", new_callable=AsyncMock
        ):
            session = MagicMock()
            session.get = MagicMock(return_value=mock_response)
            mock_session.return_value = session

            with pytest.raises(ValueError) as exc_info:
                await provider._make_request("/test", {})

            assert "500" in str(exc_info.value)
            assert session.get.call_count == 1  # No retry

        await provider.close()

    @pytest.mark.asyncio
    async def test_timeout_error_not_retried(self, provider):
        """Test timeout errors are not retried."""
        with patch.object(
            provider, "_get_session"
        ) as mock_session, patch.object(
            provider, "_wait_for_rate_limit", new_callable=AsyncMock
        ):
            session = MagicMock()
            session.get = MagicMock(side_effect=TimeoutError("Request timed out"))
            mock_session.return_value = session

            with pytest.raises(ValueError) as exc_info:
                await provider._make_request("/test", {})

            assert "timed out" in str(exc_info.value)
            assert session.get.call_count == 1  # No retry

        await provider.close()

    @pytest.mark.asyncio
    async def test_network_error_not_retried(self, provider):
        """Test network errors are not retried."""
        with patch.object(
            provider, "_get_session"
        ) as mock_session, patch.object(
            provider, "_wait_for_rate_limit", new_callable=AsyncMock
        ):
            session = MagicMock()
            session.get = MagicMock(
                side_effect=aiohttp.ClientError("Connection failed")
            )
            mock_session.return_value = session

            with pytest.raises(ValueError) as exc_info:
                await provider._make_request("/test", {})

            assert "Network error" in str(exc_info.value)
            assert session.get.call_count == 1  # No retry

        await provider.close()

    @pytest.mark.asyncio
    async def test_max_retries_zero_no_retry(
        self, mock_response_429
    ):
        """Test with max_retries=0, no retry occurs."""
        provider = CoinGeckoDataProvider(
            retry_config=RetryConfig(max_retries=0)
        )

        with patch.object(
            provider, "_get_session"
        ) as mock_session, patch.object(
            provider, "_wait_for_rate_limit", new_callable=AsyncMock
        ):
            session = MagicMock()
            session.get = MagicMock(return_value=mock_response_429)
            mock_session.return_value = session

            with pytest.raises(CoinGeckoRateLimitError):
                await provider._make_request("/test", {})

            # Only 1 call, no retries
            assert session.get.call_count == 1

        await provider.close()

    @pytest.mark.asyncio
    async def test_retry_count_in_error_matches_attempts(self, mock_response_429):
        """Test retry count in error matches actual attempts."""
        provider = CoinGeckoDataProvider(
            retry_config=RetryConfig(max_retries=2, base_delay=0.01)
        )

        with patch.object(
            provider, "_get_session"
        ) as mock_session, patch.object(
            provider, "_wait_for_rate_limit", new_callable=AsyncMock
        ):
            session = MagicMock()
            session.get = MagicMock(return_value=mock_response_429)
            mock_session.return_value = session

            with pytest.raises(CoinGeckoRateLimitError) as exc_info:
                await provider._make_request("/test", {})

            # 1 initial + 2 retries = 3 total
            assert exc_info.value.retry_count == 3

        await provider.close()


class TestExponentialBackoffSequence:
    """Tests for exponential backoff sequence."""

    def test_backoff_sequence_default(self):
        """Test default backoff sequence: 1s, 2s, 4s, 8s."""
        config = RetryConfig()
        delays = [config.get_delay_for_attempt(i) for i in range(1, 5)]
        assert delays == [1.0, 2.0, 4.0, 8.0]

    def test_backoff_sequence_custom_base(self):
        """Test custom base delay backoff sequence."""
        config = RetryConfig(base_delay=0.5)
        delays = [config.get_delay_for_attempt(i) for i in range(1, 5)]
        assert delays == [0.5, 1.0, 2.0, 4.0]

    def test_backoff_capped_all_same_at_max(self):
        """Test delays are capped at max when high attempts."""
        config = RetryConfig(max_delay=4.0)
        delays = [config.get_delay_for_attempt(i) for i in range(1, 10)]
        # After 3rd attempt, all should be capped at 4.0
        assert all(d <= 4.0 for d in delays)
        assert delays[-1] == 4.0
