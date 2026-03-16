"""Unit tests for CoinGecko backoff cap and reset behavior.

Tests cover:
- RateLimitState backoff cap at 10s (not unbounded 32s)
- RateLimitState.record_success() fully resets state including last_429_time
- Multi-iteration scenario: backoff doesn't accumulate across iterations
- TokenBucketRateLimiter rate reset after successful request in _make_request
- Backoff with jitter stays within capped range

Addresses VIB-237: CoinGecko exponential backoff grows unboundedly between
strategy iterations.
"""

from __future__ import annotations

import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.backtesting.pnl.providers.coingecko import (
    CoinGeckoDataProvider,
    RateLimitState,
    RetryConfig,
)


# =============================================================================
# RateLimitState Tests
# =============================================================================


class TestRateLimitStateBackoffCap:
    """Tests for backoff capping in RateLimitState."""

    def test_backoff_capped_at_30s(self) -> None:
        """Backoff should never exceed 30s (the default max)."""
        state = RateLimitState()
        for _ in range(20):
            state.record_rate_limit()
        assert state.backoff_seconds <= 30.0

    def test_backoff_sequence_is_capped(self) -> None:
        """Verify the full backoff sequence: 1s, 2s, 4s, 8s, 16s, 30s, 30s, ..."""
        state = RateLimitState()
        expected = [1.0, 2.0, 4.0, 8.0, 16.0, 30.0, 30.0]
        for i, expected_backoff in enumerate(expected):
            state.record_rate_limit()
            assert state.backoff_seconds == expected_backoff, (
                f"After {i + 1} rate limits, backoff should be {expected_backoff}, "
                f"got {state.backoff_seconds}"
            )

    def test_custom_max_backoff(self) -> None:
        """Allow custom max_backoff_seconds."""
        state = RateLimitState(max_backoff_seconds=4.0)
        for _ in range(10):
            state.record_rate_limit()
        assert state.backoff_seconds <= 4.0

    def test_consecutive_429s_still_tracks(self) -> None:
        """consecutive_429s should still increment even when backoff is capped."""
        state = RateLimitState()
        for i in range(10):
            state.record_rate_limit()
        assert state.consecutive_429s == 10
        assert state.backoff_seconds == 30.0


class TestRateLimitStateReset:
    """Tests for full state reset on success."""

    def test_record_success_clears_last_429_time(self) -> None:
        """record_success() must clear last_429_time so get_wait_time() returns 0."""
        state = RateLimitState()
        state.record_rate_limit()
        assert state.last_429_time is not None

        state.record_success()
        assert state.last_429_time is None

    def test_record_success_resets_consecutive_429s(self) -> None:
        """record_success() must reset consecutive_429s to 0."""
        state = RateLimitState()
        for _ in range(5):
            state.record_rate_limit()
        assert state.consecutive_429s == 5

        state.record_success()
        assert state.consecutive_429s == 0

    def test_record_success_resets_backoff(self) -> None:
        """record_success() must reset backoff_seconds to 1.0."""
        state = RateLimitState()
        for _ in range(5):
            state.record_rate_limit()
        assert state.backoff_seconds > 1.0

        state.record_success()
        assert state.backoff_seconds == 1.0

    def test_get_wait_time_zero_after_success(self) -> None:
        """get_wait_time() must return 0 after record_success()."""
        state = RateLimitState()
        state.record_rate_limit()
        # Immediately after rate limit, wait_time should be positive
        assert state.get_wait_time() > 0

        state.record_success()
        # After success, wait_time should be exactly 0
        assert state.get_wait_time() == 0.0

    def test_backoff_restarts_from_1s_after_success(self) -> None:
        """After success reset, next rate limit should start from 1s backoff."""
        state = RateLimitState()
        # Build up backoff to cap
        for _ in range(10):
            state.record_rate_limit()
        assert state.backoff_seconds == 30.0

        # Reset
        state.record_success()

        # Next rate limit should start from 1s
        state.record_rate_limit()
        assert state.backoff_seconds == 1.0
        assert state.consecutive_429s == 1


class TestRateLimitStateMultiIteration:
    """Tests simulating multi-iteration strategy runs."""

    def test_backoff_does_not_accumulate_across_iterations(self) -> None:
        """Simulate: rate-limited in iter 1, success resets for iter 2."""
        state = RateLimitState()

        # Iteration 1: hit rate limit 3 times
        for _ in range(3):
            state.record_rate_limit()
        assert state.consecutive_429s == 3
        assert state.backoff_seconds == 4.0

        # Successful request resets everything
        state.record_success()
        assert state.get_wait_time() == 0.0
        assert state.consecutive_429s == 0

        # Iteration 2: if rate limited again, backoff starts fresh from 1s
        state.record_rate_limit()
        assert state.backoff_seconds == 1.0

    def test_five_iteration_scenario(self) -> None:
        """Simulate 5 strategy iterations with intermittent rate limits."""
        state = RateLimitState()

        for iteration in range(5):
            # Each iteration: 1-2 rate limits then success
            for _ in range(iteration % 3 + 1):
                state.record_rate_limit()

            # Success at end of iteration
            state.record_success()

            # After each iteration, state is clean
            assert state.get_wait_time() == 0.0, (
                f"Iteration {iteration}: wait_time should be 0 after success"
            )
            assert state.consecutive_429s == 0


# =============================================================================
# Gateway RateLimitState Tests (same dataclass, different module)
# =============================================================================


class TestGatewayRateLimitState:
    """Tests for the gateway CoinGecko provider's RateLimitState."""

    def test_gateway_backoff_capped_at_10s(self) -> None:
        """Gateway backoff should be capped at 10s."""
        from almanak.gateway.data.price.coingecko import RateLimitState as GatewayRateLimitState

        state = GatewayRateLimitState()
        for _ in range(20):
            state.record_rate_limit()
        assert state.backoff_seconds <= 10.0

    def test_gateway_record_success_clears_last_429_time(self) -> None:
        """Gateway record_success() must clear last_429_time."""
        from almanak.gateway.data.price.coingecko import RateLimitState as GatewayRateLimitState

        state = GatewayRateLimitState()
        state.record_rate_limit()
        assert state.last_429_time is not None

        state.record_success()
        assert state.last_429_time is None
        assert state.get_wait_time() == 0.0

    def test_gateway_backoff_sequence(self) -> None:
        """Gateway backoff should follow: 1s, 2s, 4s, 8s, 10s, 10s."""
        from almanak.gateway.data.price.coingecko import RateLimitState as GatewayRateLimitState

        state = GatewayRateLimitState()
        expected = [1.0, 2.0, 4.0, 8.0, 10.0, 10.0]
        for i, expected_backoff in enumerate(expected):
            state.record_rate_limit()
            assert state.backoff_seconds == expected_backoff


# =============================================================================
# Provider-Level Tests (rate limiter reset after success)
# =============================================================================


class TestProviderRateLimiterReset:
    """Tests for TokenBucketRateLimiter rate reset in CoinGeckoDataProvider."""

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
                base_delay=0.01,
                max_delay=0.04,
            )
        )

    @pytest.mark.asyncio
    async def test_rate_limiter_reset_after_success(
        self, provider, mock_response_429, mock_response_200
    ):
        """TokenBucketRateLimiter rate should reset after a successful request."""
        initial_rate = provider._rate_limiter.requests_per_minute

        with patch.object(
            provider, "_get_session"
        ) as mock_session, patch.object(
            provider, "_wait_for_rate_limit", new_callable=AsyncMock
        ):
            session = MagicMock()
            # 429 then 200
            session.get = MagicMock(
                side_effect=[mock_response_429, mock_response_200]
            )
            mock_session.return_value = session

            await provider._make_request("/test", {})

        # Rate should be back to initial after success
        assert provider._rate_limiter.requests_per_minute == initial_rate
        await provider.close()

    @pytest.mark.asyncio
    async def test_rate_limit_state_reset_after_success(
        self, provider, mock_response_200
    ):
        """RateLimitState should be fully clean after a successful request."""
        # Artificially set rate limit state
        provider._rate_limit_state.record_rate_limit()
        provider._rate_limit_state.record_rate_limit()
        assert provider._rate_limit_state.consecutive_429s == 2

        with patch.object(
            provider, "_get_session"
        ) as mock_session, patch.object(
            provider, "_wait_for_rate_limit", new_callable=AsyncMock
        ):
            session = MagicMock()
            session.get = MagicMock(return_value=mock_response_200)
            mock_session.return_value = session

            await provider._make_request("/test", {})

        assert provider._rate_limit_state.consecutive_429s == 0
        assert provider._rate_limit_state.last_429_time is None
        assert provider._rate_limit_state.get_wait_time() == 0.0
        await provider.close()

    @pytest.mark.asyncio
    async def test_multi_request_cycle_rate_recovery(
        self, provider, mock_response_429, mock_response_200
    ):
        """Rate should recover to initial after 429 -> success -> 429 -> success."""
        initial_rate = provider._rate_limiter.requests_per_minute

        with patch.object(
            provider, "_get_session"
        ) as mock_session, patch.object(
            provider, "_wait_for_rate_limit", new_callable=AsyncMock
        ):
            session = MagicMock()
            # Cycle: 429 -> 200, then 429 -> 200
            session.get = MagicMock(
                side_effect=[
                    mock_response_429, mock_response_200,
                    mock_response_429, mock_response_200,
                ]
            )
            mock_session.return_value = session

            # First cycle
            await provider._make_request("/test", {})
            assert provider._rate_limiter.requests_per_minute == initial_rate

            # Second cycle
            await provider._make_request("/test", {})
            assert provider._rate_limiter.requests_per_minute == initial_rate

        await provider.close()
