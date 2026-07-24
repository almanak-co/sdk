"""Tests for OkxIntegration._fetch HTTP transport behavior.

_fetch overrides BaseIntegration._fetch to inject per-request HMAC auth
headers. These tests mock the aiohttp session seam (via _get_session) and
cover:

- success parsing for GET (query-string signing) and POST (JSON body),
- HTTP 429 retry then success, and retry exhaustion,
- Retry-After header parsing (valid, invalid -> 5s default, capped),
- HTTP >= 400 error wrapping,
- malformed JSON bodies,
- OKX in-body error codes (HTTP 200 with code != "0"),
- network error and timeout wrapping.

The invalid-envelope case (HTTP 200 body without a "code" key) is covered by
test_okx_integration.py::test_fetch_rejects_invalid_response_envelope.
"""

from __future__ import annotations

import json
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, patch

import aiohttp
import pytest

from almanak.gateway.integrations.base import IntegrationError, IntegrationRateLimitError
from almanak.gateway.integrations.okx import OkxIntegration


class FakeResponse:
    """Minimal stand-in for aiohttp.ClientResponse."""

    def __init__(
        self,
        status: int = 200,
        body: object = None,
        headers: dict[str, str] | None = None,
        text_body: str = "",
        json_exc: Exception | None = None,
    ) -> None:
        self.status = status
        self.headers = headers or {}
        self._body = body
        self._text = text_body
        self._json_exc = json_exc

    async def json(self):
        if self._json_exc is not None:
            raise self._json_exc
        return self._body

    async def text(self):
        return self._text


def _session_with(responses: list[FakeResponse], calls: list[dict] | None = None, request_exc: Exception | None = None):
    """Build a fake aiohttp session serving canned responses in order."""

    @asynccontextmanager
    async def fake_request(method, url, data=None, headers=None):
        if calls is not None:
            calls.append({"method": method, "url": url, "data": data, "headers": headers})
        if request_exc is not None:
            raise request_exc
        yield responses.pop(0)

    session = AsyncMock()
    session.request = fake_request
    return session


@pytest.fixture
def okx():
    return OkxIntegration(
        api_key="test-key",
        api_secret="test-secret",  # noqa: S106 - test fixture
        api_passphrase="test-passphrase",  # noqa: S106 - test fixture
        cache_ttl=60,
    )


def _patched(okx, session, wait_time: float = 0.0):
    return (
        patch.object(okx, "_get_session", AsyncMock(return_value=session)),
        patch.object(okx._rate_limiter, "acquire", AsyncMock(return_value=wait_time)),
    )


OK_BODY = {"code": "0", "data": [{"value": "1"}]}


class TestFetchSuccess:
    @pytest.mark.asyncio
    async def test_get_with_params_signs_query_string(self, okx):
        calls: list[dict] = []
        session = _session_with([FakeResponse(body=OK_BODY)], calls)
        get_session, acquire = _patched(okx, session, wait_time=0.01)

        with get_session, acquire:
            data = await okx._fetch("/api/v6/dex/balance/all-token-balances-by-address", params={"address": "0xabc", "chains": "1"})

        assert data == OK_BODY
        assert okx._metrics.successful_requests == 1
        assert okx._metrics.total_requests == 1

        (call,) = calls
        assert call["method"] == "GET"
        assert call["url"].endswith("/api/v6/dex/balance/all-token-balances-by-address?address=0xabc&chains=1")
        # GET has no body
        assert call["data"] is None
        headers = call["headers"]
        assert "Content-Type" not in headers
        assert headers["OK-ACCESS-KEY"] == "test-key"
        assert headers["OK-ACCESS-PASSPHRASE"] == "test-passphrase"  # noqa: S105
        # Signature covers method + path WITH query string (and empty body)
        expected_sig = okx._sign(
            headers["OK-ACCESS-TIMESTAMP"],
            "GET",
            "/api/v6/dex/balance/all-token-balances-by-address?address=0xabc&chains=1",
        )
        assert headers["OK-ACCESS-SIGN"] == expected_sig

    @pytest.mark.asyncio
    async def test_post_json_body_sets_content_type_and_signs_body(self, okx):
        calls: list[dict] = []
        session = _session_with([FakeResponse(body=OK_BODY)], calls)
        get_session, acquire = _patched(okx, session)

        with get_session, acquire:
            data = await okx._fetch("/api/v6/defi/user/asset-list", method="POST", json_data={"walletAddress": "0xabc"})

        assert data == OK_BODY

        (call,) = calls
        assert call["method"] == "POST"
        body = json.dumps({"walletAddress": "0xabc"})
        assert call["data"] == body
        headers = call["headers"]
        assert headers["Content-Type"] == "application/json"
        expected_sig = okx._sign(
            headers["OK-ACCESS-TIMESTAMP"],
            "POST",
            "/api/v6/defi/user/asset-list",
            body,
        )
        assert headers["OK-ACCESS-SIGN"] == expected_sig


class TestFetchRateLimiting:
    @pytest.mark.asyncio
    async def test_429_retries_then_succeeds(self, okx):
        responses = [
            FakeResponse(status=429, headers={"Retry-After": "0"}),
            FakeResponse(body=OK_BODY),
        ]
        calls: list[dict] = []
        session = _session_with(responses, calls)
        get_session, acquire = _patched(okx, session)

        with get_session, acquire:
            data = await okx._fetch("/api/v6/dex/balance/total-value-by-address")

        assert data == OK_BODY
        assert len(calls) == 2
        # A retried-then-successful request is not counted as rate limited
        assert okx._metrics.rate_limited_requests == 0
        assert okx._metrics.successful_requests == 1
        # total_requests counts _fetch invocations, not attempts
        assert okx._metrics.total_requests == 1

    @pytest.mark.asyncio
    async def test_429_exhausted_raises_rate_limit_error(self, okx):
        okx.rate_limit_max_retries = 0
        session = _session_with([FakeResponse(status=429, headers={"Retry-After": "7"})])
        get_session, acquire = _patched(okx, session)

        with get_session, acquire:
            with pytest.raises(IntegrationRateLimitError) as exc_info:
                await okx._fetch("/api/v6/dex/balance/total-value-by-address")

        assert exc_info.value.retry_after == 7.0
        assert exc_info.value.code == "RATE_LIMITED"
        assert okx._metrics.rate_limited_requests == 1
        assert okx._metrics.failed_requests == 1

    @pytest.mark.asyncio
    async def test_invalid_retry_after_header_defaults_to_5s(self, okx):
        okx.rate_limit_max_retries = 0
        session = _session_with([FakeResponse(status=429, headers={"Retry-After": "soon"})])
        get_session, acquire = _patched(okx, session)

        with get_session, acquire:
            with pytest.raises(IntegrationRateLimitError) as exc_info:
                await okx._fetch("/api/v6/dex/balance/total-value-by-address")

        assert exc_info.value.retry_after == 5.0

    @pytest.mark.asyncio
    async def test_retry_after_capped_at_rate_limit_max_wait(self, okx):
        okx.rate_limit_max_retries = 0
        session = _session_with([FakeResponse(status=429, headers={"Retry-After": "99999"})])
        get_session, acquire = _patched(okx, session)

        with get_session, acquire:
            with pytest.raises(IntegrationRateLimitError) as exc_info:
                await okx._fetch("/api/v6/dex/balance/total-value-by-address")

        assert exc_info.value.retry_after == okx.rate_limit_max_wait


class TestFetchErrors:
    @pytest.mark.asyncio
    async def test_http_error_status_raises_integration_error(self, okx):
        session = _session_with([FakeResponse(status=500, text_body="internal boom")])
        get_session, acquire = _patched(okx, session)

        with get_session, acquire:
            with pytest.raises(IntegrationError, match="HTTP 500: internal boom") as exc_info:
                await okx._fetch("/api/v6/dex/balance/total-value-by-address")

        assert exc_info.value.code == "HTTP_500"
        assert okx._metrics.failed_requests == 1
        assert "HTTP 500" in okx._metrics.last_error

    @pytest.mark.asyncio
    async def test_malformed_json_body_raises_integration_error(self, okx):
        session = _session_with(
            [FakeResponse(json_exc=json.JSONDecodeError("Expecting value", "<html>", 0))]
        )
        get_session, acquire = _patched(okx, session)

        with get_session, acquire:
            with pytest.raises(IntegrationError, match="Invalid JSON response from OKX") as exc_info:
                await okx._fetch("/api/v6/dex/balance/total-value-by-address")

        assert exc_info.value.code == "INVALID_RESPONSE"

    @pytest.mark.asyncio
    async def test_okx_body_error_code_raises_integration_error(self, okx):
        """OKX returns HTTP 200 with error codes in the body."""
        session = _session_with(
            [FakeResponse(body={"code": "50011", "msg": "Invalid API key"})]
        )
        get_session, acquire = _patched(okx, session)

        with get_session, acquire:
            with pytest.raises(IntegrationError, match="OKX API error 50011: Invalid API key") as exc_info:
                await okx._fetch("/api/v6/dex/balance/total-value-by-address")

        assert exc_info.value.code == "OKX_50011"
        assert okx._metrics.failed_requests == 1

    @pytest.mark.asyncio
    async def test_okx_body_error_without_msg_uses_unknown_error(self, okx):
        session = _session_with([FakeResponse(body={"code": "1"})])
        get_session, acquire = _patched(okx, session)

        with get_session, acquire:
            with pytest.raises(IntegrationError, match="OKX API error 1: unknown error"):
                await okx._fetch("/api/v6/dex/balance/total-value-by-address")

    @pytest.mark.asyncio
    async def test_client_error_wrapped_as_network_error(self, okx):
        session = _session_with([], request_exc=aiohttp.ClientError("connection reset"))
        get_session, acquire = _patched(okx, session)

        with get_session, acquire:
            with pytest.raises(IntegrationError, match="connection reset") as exc_info:
                await okx._fetch("/api/v6/dex/balance/total-value-by-address")

        assert exc_info.value.code == "NETWORK_ERROR"
        assert okx._metrics.failed_requests == 1

    @pytest.mark.asyncio
    async def test_timeout_wrapped_as_timeout_error(self, okx):
        session = _session_with([], request_exc=TimeoutError())
        get_session, acquire = _patched(okx, session)

        with get_session, acquire:
            with pytest.raises(IntegrationError, match="Timeout after") as exc_info:
                await okx._fetch("/api/v6/dex/balance/total-value-by-address")

        assert exc_info.value.code == "TIMEOUT"
        assert okx._metrics.last_error.startswith("Timeout after")
