"""Behavioral tests for ``TelegramChannel._send_message``.

Covers every response branch of the Telegram Bot API send path with
``httpx.AsyncClient`` patched at the module seam — no network:

- 200 + ok -> success with message_id
- 200 + ok:false -> failure with the API description
- 429 -> rate-limited result, retry_after from parameters (and the 60s default)
- non-200 error -> failure with description / "Unknown error" fallback
- httpx.TimeoutException -> "Request timeout"
- httpx.RequestError -> "Request error: ..."
- non-JSON body (e.g. an HTML 502 page from a proxy) -> failure result, no raise
"""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock, patch

import httpx

from almanak.framework.alerting.channels import telegram as telegram_mod
from almanak.framework.alerting.channels.telegram import TelegramChannel, TelegramSendResult


class _FakeAsyncClient:
    """Async-context-manager stand-in for httpx.AsyncClient."""

    def __init__(self, response=None, exc: Exception | None = None) -> None:
        self._response = response
        self._exc = exc
        self.posts: list[tuple[str, dict, float]] = []

    async def __aenter__(self) -> _FakeAsyncClient:
        return self

    async def __aexit__(self, *args) -> bool:
        return False

    async def post(self, url: str, json=None, timeout=None):
        self.posts.append((url, json, timeout))
        if self._exc is not None:
            raise self._exc
        return self._response


def _response(status_code: int, data: dict) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    resp.json = MagicMock(return_value=data)
    return resp


def _channel() -> TelegramChannel:
    return TelegramChannel(chat_id="-100123", bot_token="bot-token-abc")


def _send(channel: TelegramChannel, fake_client: _FakeAsyncClient, **kwargs) -> TelegramSendResult:
    with patch.object(telegram_mod.httpx, "AsyncClient", return_value=fake_client):
        return asyncio.run(channel._send_message("hello <b>world</b>", **kwargs))


class TestSendMessageSuccess:
    def test_success_returns_message_id(self) -> None:
        channel = _channel()
        fake = _FakeAsyncClient(response=_response(200, {"ok": True, "result": {"message_id": 42}}))

        result = _send(channel, fake)

        assert result == TelegramSendResult(success=True, message_id=42)
        url, payload, timeout = fake.posts[0]
        assert url == "https://api.telegram.org/botbot-token-abc/sendMessage"
        assert payload == {
            "chat_id": "-100123",
            "text": "hello <b>world</b>",
            "parse_mode": "HTML",
            "disable_notification": False,
        }
        assert timeout == 30.0

    def test_custom_parse_mode_and_silent_flag_forwarded(self) -> None:
        channel = _channel()
        fake = _FakeAsyncClient(response=_response(200, {"ok": True, "result": {}}))

        result = _send(channel, fake, parse_mode="Markdown", disable_notification=True)

        # No message_id in result payload -> None, still a success.
        assert result.success is True
        assert result.message_id is None
        _, payload, _ = fake.posts[0]
        assert payload["parse_mode"] == "Markdown"
        assert payload["disable_notification"] is True

    def test_status_200_but_not_ok_is_failure(self) -> None:
        channel = _channel()
        fake = _FakeAsyncClient(
            response=_response(200, {"ok": False, "description": "Bad Request: chat not found"})
        )

        result = _send(channel, fake)

        assert result.success is False
        assert result.error == "Bad Request: chat not found"
        assert result.retry_after is None


class TestSendMessageRateLimit:
    def test_429_uses_server_retry_after(self) -> None:
        channel = _channel()
        fake = _FakeAsyncClient(
            response=_response(429, {"ok": False, "parameters": {"retry_after": 7}})
        )

        result = _send(channel, fake)

        assert result.success is False
        assert result.error == "Rate limited by Telegram"
        assert result.retry_after == 7

    def test_429_without_parameters_defaults_to_60(self) -> None:
        channel = _channel()
        fake = _FakeAsyncClient(response=_response(429, {"ok": False}))

        result = _send(channel, fake)

        assert result.success is False
        assert result.retry_after == 60


class TestSendMessageErrors:
    def test_error_status_uses_description(self) -> None:
        channel = _channel()
        fake = _FakeAsyncClient(
            response=_response(400, {"ok": False, "description": "Bad Request: message is empty"})
        )

        result = _send(channel, fake)

        assert result.success is False
        assert result.error == "Bad Request: message is empty"

    def test_error_status_without_description_is_unknown(self) -> None:
        channel = _channel()
        fake = _FakeAsyncClient(response=_response(500, {}))

        result = _send(channel, fake)

        assert result.success is False
        assert result.error == "Unknown error"

    def test_timeout_maps_to_request_timeout(self) -> None:
        channel = _channel()
        fake = _FakeAsyncClient(exc=httpx.TimeoutException("timed out"))

        result = _send(channel, fake)

        assert result == TelegramSendResult(success=False, error="Request timeout")

    def test_request_error_maps_to_request_error(self) -> None:
        channel = _channel()
        fake = _FakeAsyncClient(exc=httpx.ConnectError("connection refused"))

        result = _send(channel, fake)

        assert result.success is False
        assert result.error == "Request error: connection refused"


class TestSendMessageNonJsonBody:
    """A non-JSON body must produce a failure result, never a raised decode error.

    Uses real ``httpx.Response`` objects so ``.json()`` genuinely fails to
    decode, instead of mocking the raise.
    """

    def test_html_502_body_returns_failure(self) -> None:
        channel = _channel()
        fake = _FakeAsyncClient(response=httpx.Response(502, text="<html><body>Bad Gateway</body></html>"))

        result = _send(channel, fake)

        assert result == TelegramSendResult(success=False, error="Non-JSON response from Telegram (HTTP 502)")

    def test_non_json_body_with_status_200_is_failure_not_success(self) -> None:
        channel = _channel()
        fake = _FakeAsyncClient(response=httpx.Response(200, text="OK"))

        result = _send(channel, fake)

        assert result.success is False
        assert result.error == "Non-JSON response from Telegram (HTTP 200)"

    def test_empty_body_returns_failure(self) -> None:
        channel = _channel()
        fake = _FakeAsyncClient(response=httpx.Response(429, text=""))

        result = _send(channel, fake)

        # A proxy-level 429 with no JSON carries no Telegram retry_after;
        # it falls to the generic non-JSON failure, not the rate-limit branch.
        assert result.success is False
        assert result.error == "Non-JSON response from Telegram (HTTP 429)"
        assert result.retry_after is None
