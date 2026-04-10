"""Tests for BaseIntegration 429 retry behavior."""

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import pytest

from almanak.gateway.integrations.base import BaseIntegration, IntegrationRateLimitError


class _StubIntegration(BaseIntegration):
    """Minimal concrete subclass for testing."""

    name = "stub"
    rate_limit_requests = 600
    rate_limit_max_retries = 2
    rate_limit_max_wait = 10.0

    async def health_check(self) -> bool:
        return True


def _make_response(status=200, json_data=None, headers=None):
    """Create a mock aiohttp response as an async context manager."""
    resp = AsyncMock()
    resp.status = status
    resp.json = AsyncMock(return_value=json_data or {})
    resp.text = AsyncMock(return_value="")
    resp.headers = headers or {}
    return resp


def _session_with_responses(responses):
    """Create a mock session that yields responses in order."""
    call_count = {"n": 0}

    @asynccontextmanager
    async def fake_request(*args, **kwargs):
        idx = min(call_count["n"], len(responses) - 1)
        call_count["n"] += 1
        yield responses[idx]

    session = AsyncMock()
    session.request = fake_request
    return session, call_count


class TestRetryOn429:
    @pytest.fixture
    def integration(self):
        return _StubIntegration(api_key="test", base_url="https://example.com")

    @pytest.mark.asyncio
    async def test_retry_succeeds_after_429(self, integration):
        """A 429 followed by a 200 should retry and return data."""
        r429 = _make_response(429, headers={"Retry-After": "0"})
        r200 = _make_response(200, json_data={"ok": True})
        session, calls = _session_with_responses([r429, r200])

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(integration, "_get_session", AsyncMock(return_value=session))
            mp.setattr(integration._rate_limiter, "acquire", AsyncMock(return_value=0))

            result = await integration._fetch("/test")

        assert result == {"ok": True}
        assert calls["n"] == 2
        assert integration._metrics.rate_limited_requests == 0  # only incremented on final exhaustion
        assert integration._metrics.successful_requests == 1

    @pytest.mark.asyncio
    async def test_exhausted_retries_raises(self, integration):
        """Repeated 429s beyond max_retries should raise IntegrationRateLimitError."""
        r429 = _make_response(429, headers={"Retry-After": "0"})
        session, calls = _session_with_responses([r429, r429, r429])

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(integration, "_get_session", AsyncMock(return_value=session))
            mp.setattr(integration._rate_limiter, "acquire", AsyncMock(return_value=0))

            with pytest.raises(IntegrationRateLimitError):
                await integration._fetch("/test")

        assert calls["n"] == 3  # 1 initial + 2 retries
        assert integration._metrics.rate_limited_requests == 1  # counted once per logical call
        assert integration._metrics.failed_requests == 1

    @pytest.mark.asyncio
    async def test_retry_after_capped_at_max_wait(self, integration):
        """Retry-After values exceeding rate_limit_max_wait should be capped."""
        integration.rate_limit_max_retries = 1
        r429 = _make_response(429, headers={"Retry-After": "9999"})
        r200 = _make_response(200, json_data={"ok": True})
        session, _ = _session_with_responses([r429, r200])

        import asyncio
        sleep_values = []
        original_sleep = asyncio.sleep

        async def capture_sleep(t):
            sleep_values.append(t)

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(integration, "_get_session", AsyncMock(return_value=session))
            mp.setattr(integration._rate_limiter, "acquire", AsyncMock(return_value=0))
            mp.setattr(asyncio, "sleep", capture_sleep)

            await integration._fetch("/test")

        assert sleep_values[0] == integration.rate_limit_max_wait  # capped at 10.0

    @pytest.mark.asyncio
    async def test_negative_retry_after_clamped_to_zero(self, integration):
        """Negative Retry-After should be clamped to 0."""
        integration.rate_limit_max_retries = 1
        r429 = _make_response(429, headers={"Retry-After": "-5"})
        r200 = _make_response(200, json_data={"ok": True})
        session, _ = _session_with_responses([r429, r200])

        import asyncio
        sleep_values = []

        async def capture_sleep(t):
            sleep_values.append(t)

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(integration, "_get_session", AsyncMock(return_value=session))
            mp.setattr(integration._rate_limiter, "acquire", AsyncMock(return_value=0))
            mp.setattr(asyncio, "sleep", capture_sleep)

            await integration._fetch("/test")

        assert sleep_values[0] == 0  # clamped from -5 to 0

    @pytest.mark.asyncio
    async def test_total_requests_incremented_once(self, integration):
        """total_requests should only increment once per _fetch call, not per retry."""
        r429 = _make_response(429, headers={"Retry-After": "0"})
        r200 = _make_response(200, json_data={"ok": True})
        session, _ = _session_with_responses([r429, r200])

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(integration, "_get_session", AsyncMock(return_value=session))
            mp.setattr(integration._rate_limiter, "acquire", AsyncMock(return_value=0))

            await integration._fetch("/test")

        assert integration._metrics.total_requests == 1
