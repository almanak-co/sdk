"""Behavioral, formatting, and transport tests for the Telegram alert channel."""

from __future__ import annotations

import asyncio
import logging
from copy import deepcopy
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from unittest.mock import AsyncMock, MagicMock, call, patch

import httpx
import pytest

from almanak.framework.alerting.channels import telegram as telegram_mod
from almanak.framework.alerting.channels.telegram import TelegramChannel, TelegramSendResult
from almanak.framework.models.actions import AvailableAction, SuggestedAction
from almanak.framework.models.operator_card import EventType, OperatorCard, PositionSummary, Severity
from almanak.framework.models.stuck_reason import StuckReason


class _UnexpectedSeverity(StrEnum):
    UNKNOWN = "UNKNOWN"


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


def _response(status_code: int, data: object) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    resp.json = MagicMock(return_value=data)
    return resp


def _channel() -> TelegramChannel:
    return TelegramChannel(chat_id="-100123", bot_token="bot-token-abc")


def _make_card(**overrides: object) -> OperatorCard:
    values = {
        "deployment_id": "deployment:alpha",
        "timestamp": datetime(2026, 8, 20, 12, 34, 56, tzinfo=UTC),
        "event_type": EventType.STUCK,
        "reason": StuckReason.GAS_PRICE_BLOCKED,
        "context": {"tx_hash": "0xabc", "retry_count": 2},
        "severity": Severity.HIGH,
        "position_summary": PositionSummary(
            total_value_usd=Decimal("1234.56"),
            available_balance_usd=Decimal("78.90"),
        ),
        "risk_description": "Pending transaction may miss its execution window",
        "suggested_actions": [
            SuggestedAction(
                action=AvailableAction.BUMP_GAS,
                description="Increase gas and resubmit",
                is_recommended=True,
            )
        ],
        "available_actions": [AvailableAction.BUMP_GAS, AvailableAction.PAUSE],
    }
    values.update(overrides)
    return OperatorCard(**values)  # type: ignore[arg-type]


def _send(channel: TelegramChannel, fake_client: _FakeAsyncClient, **kwargs) -> TelegramSendResult:
    with patch.object(telegram_mod.httpx, "AsyncClient", return_value=fake_client):
        return asyncio.run(channel._send_message("hello <b>world</b>", **kwargs))


class TestFormatAlertMessage:
    def test_exact_full_message(self) -> None:
        channel = TelegramChannel(
            chat_id="-100123",
            bot_token="bot-token-abc",
            dashboard_base_url="https://dashboard.example",
        )
        card = _make_card()
        original = deepcopy(card)

        message = channel._format_alert_message(card)

        assert message == (
            "\ud83d\udea8 <b>HIGH Alert</b>\n"
            "\n"
            "<b>Strategy:</b> deployment:alpha\n"
            "<b>Status:</b> STUCK\n"
            "<b>Reason:</b> Gas Price Blocked\n"
            "\n"
            "<b>Context:</b>\n"
            "  \u2022 Tx Hash: 0xabc\n"
            "  \u2022 Retry Count: 2\n"
            "\n"
            "<b>Position at Risk:</b> $1234.56\n"
            "\n"
            "<b>Risk:</b> Pending transaction may miss its execution window\n"
            "\n"
            "<b>Recommended:</b> Increase gas and resubmit\n"
            "\n"
            '\ud83d\udcca <a href="https://dashboard.example/strategy/deployment:alpha">View in Dashboard</a>\n'
            "\n"
            "<i>2026-08-20 12:34:56 UTC</i>"
        )
        assert card == original

    def test_exact_message_without_optional_fields(self) -> None:
        channel = _channel()
        card = _make_card(context={}, risk_description="")
        card.position_summary = None  # type: ignore[assignment]
        card.suggested_actions = []

        assert channel._format_alert_message(card) == (
            "\ud83d\udea8 <b>HIGH Alert</b>\n"
            "\n"
            "<b>Strategy:</b> deployment:alpha\n"
            "<b>Status:</b> STUCK\n"
            "<b>Reason:</b> Gas Price Blocked\n"
            "\n"
            "<i>2026-08-20 12:34:56 UTC</i>"
        )

    @pytest.mark.parametrize(
        ("severity", "emoji"),
        [
            (Severity.LOW, "\u2139\ufe0f"),
            (Severity.MEDIUM, "\u26a0\ufe0f"),
            (Severity.HIGH, "\ud83d\udea8"),
            (Severity.CRITICAL, "\ud83d\udd34"),
            (_UnexpectedSeverity.UNKNOWN, "\u2753"),
        ],
    )
    def test_severity_emoji_and_fallback(self, severity: Severity, emoji: str) -> None:
        card = _make_card(severity=severity)

        first_line = _channel()._format_alert_message(card).splitlines()[0]

        assert first_line == f"{emoji} <b>{severity.value} Alert</b>"

    def test_dynamic_values_are_redacted_and_html_escaped(self) -> None:
        channel = TelegramChannel(
            chat_id="-100123",
            bot_token="bot-token-abc",
            dashboard_base_url='https://dashboard.example/?next="alerts"&token=known-secret',
        )
        card = _make_card(
            deployment_id='deployment:<alpha>&"',
            context={"api_<key>": "known-secret <untrusted> & value"},
            risk_description="A < B & known-secret",
        )
        card.suggested_actions[0].description = 'Use "safe" <mode>'

        with patch.object(telegram_mod, "redact", side_effect=lambda value: value.replace("known-secret", "***")):
            message = channel._format_alert_message(card)

        assert "known-secret" not in message
        assert "<b>Strategy:</b> deployment:&lt;alpha&gt;&amp;&quot;" in message
        assert "Api &lt;Key&gt;: *** &lt;untrusted&gt; &amp; value" in message
        assert "<b>Risk:</b> A &lt; B &amp; ***" in message
        assert "<b>Recommended:</b> Use &quot;safe&quot; &lt;mode&gt;" in message
        assert (
            'href="https://dashboard.example/?next=&quot;alerts&quot;&amp;token=***/strategy/'
            'deployment:&lt;alpha&gt;&amp;&quot;"'
        ) in message

    def test_over_limit_message_preserves_current_unbounded_contract(self) -> None:
        card = _make_card(risk_description="x" * 4096)

        message = _channel()._format_alert_message(card)

        assert len(message) > 4096
        assert f"<b>Risk:</b> {'x' * 4096}" in message
        assert message.endswith("<i>2026-08-20 12:34:56 UTC</i>")


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
        fake = _FakeAsyncClient(response=_response(200, {"ok": False, "description": "Bad Request: chat not found"}))

        result = _send(channel, fake)

        assert result.success is False
        assert result.error == "Bad Request: chat not found"
        assert result.retry_after is None

    def test_success_with_malformed_result_has_no_message_id(self) -> None:
        channel = _channel()
        fake = _FakeAsyncClient(response=_response(200, {"ok": True, "result": "unexpected"}))

        result = _send(channel, fake)

        assert result == TelegramSendResult(success=True)


class TestSendMessageRateLimit:
    def test_429_uses_server_retry_after(self) -> None:
        channel = _channel()
        fake = _FakeAsyncClient(response=_response(429, {"ok": False, "parameters": {"retry_after": 7}}))

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

    @pytest.mark.parametrize(
        ("parameters", "expected"),
        [
            ([], 60),
            ({"retry_after": None}, 60),
            ({"retry_after": "later"}, 60),
            ({"retry_after": "7"}, 7),
        ],
    )
    def test_429_handles_malformed_retry_parameters(self, parameters: object, expected: int) -> None:
        channel = _channel()
        fake = _FakeAsyncClient(response=_response(429, {"ok": False, "parameters": parameters}))

        result = _send(channel, fake)

        assert result == TelegramSendResult(
            success=False,
            error="Rate limited by Telegram",
            retry_after=expected,
        )


class TestSendMessageErrors:
    def test_error_status_uses_description(self) -> None:
        channel = _channel()
        fake = _FakeAsyncClient(response=_response(400, {"ok": False, "description": "Bad Request: message is empty"}))

        result = _send(channel, fake)

        assert result.success is False
        assert result.error == "Bad Request: message is empty"

    def test_error_status_without_description_is_unknown(self) -> None:
        channel = _channel()
        fake = _FakeAsyncClient(response=_response(500, {}))

        result = _send(channel, fake)

        assert result.success is False
        assert result.error == "Unknown error"

    def test_error_description_redacts_bot_and_registered_secrets(self) -> None:
        channel = _channel()
        description = f"failed via {channel.api_url}/sendMessage with bot-token-abc and registered-secret"
        fake = _FakeAsyncClient(response=_response(400, {"ok": False, "description": description}))

        with patch.object(telegram_mod, "redact", side_effect=lambda value: value.replace("registered-secret", "***")):
            result = _send(channel, fake)

        assert result.error == "failed via <redacted Telegram API URL>/sendMessage with *** and ***"
        assert channel.bot_token not in result.error

    @pytest.mark.parametrize("data", [None, [], "OK"])
    def test_valid_json_with_wrong_shape_is_failure(self, data: object) -> None:
        channel = _channel()
        fake = _FakeAsyncClient(response=_response(502, data))

        result = _send(channel, fake)

        assert result == TelegramSendResult(success=False, error="Malformed response from Telegram (HTTP 502)")

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

    def test_request_error_redacts_bot_url_and_token(self) -> None:
        channel = _channel()
        error = httpx.ConnectError(
            f"connection refused for {channel.api_url}/sendMessage using bot-token-abc",
            request=httpx.Request("POST", f"{channel.api_url}/sendMessage"),
        )

        result = _send(channel, _FakeAsyncClient(exc=error))

        assert result.error == "Request error: connection refused for <redacted Telegram API URL>/sendMessage using ***"
        assert channel.bot_token not in result.error


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


class TestSendAlert:
    def test_low_severity_posts_exact_message_silently_and_enforces_rate_limit(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        channel = _channel()
        card = _make_card(severity=Severity.LOW)
        original = deepcopy(card)
        channel._last_send_time = 9.98
        channel._format_alert_message = MagicMock(return_value="exact formatted message")  # type: ignore[method-assign]
        channel._send_message = AsyncMock(return_value=TelegramSendResult(success=True, message_id=42))  # type: ignore[method-assign]

        with (
            patch.object(telegram_mod, "redact", side_effect=lambda value: value.replace("registered-secret", "***")),
            patch.object(telegram_mod.time, "time", side_effect=[10.0, 10.25, 10.5]),
            patch.object(telegram_mod.asyncio, "sleep", new_callable=AsyncMock) as sleep,
            caplog.at_level(logging.INFO),
        ):
            card.deployment_id = "deployment:registered-secret"
            result = asyncio.run(channel.send_alert(card))

        assert result == TelegramSendResult(success=True, message_id=42)
        channel._format_alert_message.assert_called_once_with(card)
        channel._send_message.assert_awaited_once_with(
            text="exact formatted message",
            parse_mode="HTML",
            disable_notification=True,
        )
        assert sleep.await_args_list[0].args[0] == pytest.approx(0.03)
        assert channel._last_send_time == 10.25
        original.deployment_id = "deployment:registered-secret"
        assert card == original
        assert (
            "Telegram alert sent successfully for strategy deployment:*** (message_id=42, severity=LOW)" in caplog.text
        )
        assert "registered-secret" not in caplog.text

    def test_non_rate_failures_retry_with_exponential_backoff(self) -> None:
        channel = TelegramChannel(chat_id="-100123", bot_token="bot-token-abc", max_retries=2, base_delay=0.25)
        channel._send_message = AsyncMock(  # type: ignore[method-assign]
            side_effect=[
                TelegramSendResult(success=False, error="first"),
                TelegramSendResult(success=False, error="second"),
                TelegramSendResult(success=True, message_id=99),
            ]
        )

        with patch.object(telegram_mod.asyncio, "sleep", new_callable=AsyncMock) as sleep:
            result = asyncio.run(channel.send_alert(_make_card()))

        assert result == TelegramSendResult(success=True, message_id=99)
        assert channel._send_message.await_count == 3
        assert channel._send_message.await_args_list == [
            call(text=channel._format_alert_message(_make_card()), parse_mode="HTML", disable_notification=False),
            call(text=channel._format_alert_message(_make_card()), parse_mode="HTML", disable_notification=False),
            call(text=channel._format_alert_message(_make_card()), parse_mode="HTML", disable_notification=False),
        ]
        assert sleep.await_args_list == [call(0.25), call(0.5)]

    def test_rate_limit_delay_is_followed_by_retry_backoff(self) -> None:
        channel = TelegramChannel(chat_id="-100123", bot_token="bot-token-abc", max_retries=1, base_delay=0.5)
        channel._send_message = AsyncMock(  # type: ignore[method-assign]
            side_effect=[
                TelegramSendResult(success=False, error="rate", retry_after=7),
                TelegramSendResult(success=True, message_id=1),
            ]
        )

        with patch.object(telegram_mod.asyncio, "sleep", new_callable=AsyncMock) as sleep:
            result = asyncio.run(channel.send_alert(_make_card()))

        assert result.success is True
        assert sleep.await_args_list == [call(7), call(0.5)]

    def test_exhaustion_redacts_returned_error_and_logs_without_final_rate_sleep(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        channel = TelegramChannel(chat_id="-100123", bot_token="bot-token-abc", max_retries=0)
        raw_error = f"failed at {channel.api_url}/sendMessage with bot-token-abc and registered-secret"
        channel._send_message = AsyncMock(  # type: ignore[method-assign]
            return_value=TelegramSendResult(success=False, error=raw_error, retry_after=9)
        )

        with (
            patch.object(telegram_mod, "redact", side_effect=lambda value: value.replace("registered-secret", "***")),
            patch.object(telegram_mod.asyncio, "sleep", new_callable=AsyncMock) as sleep,
            caplog.at_level(logging.WARNING),
        ):
            result = asyncio.run(channel.send_alert(_make_card(deployment_id="deployment:registered-secret")))

        expected = "failed at <redacted Telegram API URL>/sendMessage with *** and ***"
        assert result == TelegramSendResult(success=False, error=expected)
        sleep.assert_not_awaited()
        assert expected in caplog.text
        assert channel.bot_token not in caplog.text
        assert "registered-secret" not in caplog.text
        assert "for strategy deployment:***" in caplog.text
        assert "Failed to send Telegram alert after 1 attempts" in caplog.text

    def test_negative_max_retries_makes_no_send_attempt(self) -> None:
        channel = TelegramChannel(chat_id="-100123", bot_token="bot-token-abc", max_retries=-1)
        channel._send_message = AsyncMock()  # type: ignore[method-assign]

        result = asyncio.run(channel.send_alert(_make_card()))

        assert result == TelegramSendResult(success=False, error=None)
        channel._send_message.assert_not_awaited()

    def test_unexpected_send_exception_propagates(self) -> None:
        channel = _channel()
        channel._send_message = AsyncMock(side_effect=RuntimeError("unexpected"))  # type: ignore[method-assign]

        with pytest.raises(RuntimeError, match="unexpected"):
            asyncio.run(channel.send_alert(_make_card()))

        assert channel._last_send_time == 0.0

    def test_sync_wrapper_forwards_card(self) -> None:
        channel = _channel()
        card = _make_card()
        channel.send_alert = AsyncMock(return_value=TelegramSendResult(success=True, message_id=7))  # type: ignore[method-assign]

        result = channel.send_alert_sync(card)

        assert result == TelegramSendResult(success=True, message_id=7)
        channel.send_alert.assert_awaited_once_with(card)
