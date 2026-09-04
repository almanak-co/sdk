"""Behavioral and payload characterization for the Slack alert channel."""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from unittest.mock import AsyncMock, call, patch

import httpx
import pytest

from almanak.framework.alerting.channels import slack as slack_mod
from almanak.framework.alerting.channels.slack import SlackChannel, SlackSendResult
from almanak.framework.models.actions import AvailableAction, SuggestedAction
from almanak.framework.models.operator_card import EventType, OperatorCard, PositionSummary, Severity
from almanak.framework.models.stuck_reason import StuckReason

WEBHOOK_URL = "https://hooks.slack.com/services/T00000000/B00000000/secret-value"


class _UnexpectedSeverity(StrEnum):
    UNKNOWN = "UNKNOWN"


class _UnexpectedAction(StrEnum):
    ACKNOWLEDGE = "ACKNOWLEDGE"


class _FakeAsyncClient:
    def __init__(self, response: httpx.Response | None = None, exc: Exception | None = None) -> None:
        self.response = response
        self.exc = exc
        self.posts: list[tuple[str, dict, float]] = []

    async def __aenter__(self) -> _FakeAsyncClient:
        return self

    async def __aexit__(self, *args: object) -> bool:
        return False

    async def post(self, url: str, json: dict, timeout: float) -> httpx.Response:
        self.posts.append((url, json, timeout))
        if self.exc is not None:
            raise self.exc
        assert self.response is not None
        return self.response


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
        "available_actions": list(AvailableAction),
    }
    values.update(overrides)
    return OperatorCard(**values)  # type: ignore[arg-type]


def _full_blocks() -> list[dict]:
    return [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": ":rotating_light: HIGH Alert",
                "emoji": True,
            },
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": "*Strategy:*\ndeployment:alpha"},
                {"type": "mrkdwn", "text": "*Status:*\nSTUCK"},
                {"type": "mrkdwn", "text": "*Reason:*\nGas Price Blocked"},
                {"type": "mrkdwn", "text": "*Timestamp:*\n2026-08-20 12:34:56 UTC"},
            ],
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "*Context:*\n• Tx Hash: 0xabc\n• Retry Count: 2",
            },
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": "*Position at Risk:*\n$1234.56"},
                {"type": "mrkdwn", "text": "*Available Balance:*\n$78.90"},
            ],
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "*Risk:* Pending transaction may miss its execution window",
            },
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": ":point_right: *Recommended Action:* Increase gas and resubmit",
            },
        },
        {"type": "divider"},
        {
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {
                        "type": "plain_text",
                        "text": ":bar_chart: View Dashboard",
                        "emoji": True,
                    },
                    "url": "https://dashboard.example/strategy/deployment:alpha",
                    "action_id": "view_dashboard",
                },
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": ":zap: Bump Gas", "emoji": True},
                    "url": "https://dashboard.example/strategy/deployment:alpha/action/bump_gas",
                    "action_id": "action_bump_gas",
                },
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": ":x: Cancel TX", "emoji": True},
                    "url": "https://dashboard.example/strategy/deployment:alpha/action/cancel_tx",
                    "action_id": "action_cancel_tx",
                },
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": ":pause_button: Pause", "emoji": True},
                    "url": "https://dashboard.example/strategy/deployment:alpha/action/pause",
                    "action_id": "action_pause",
                },
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": ":arrow_forward: Resume", "emoji": True},
                    "url": "https://dashboard.example/strategy/deployment:alpha/action/resume",
                    "action_id": "action_resume",
                },
            ],
        },
    ]


def _send_payload(channel: SlackChannel, fake_client: _FakeAsyncClient, payload: dict | None = None) -> SlackSendResult:
    with patch.object(slack_mod.httpx, "AsyncClient", return_value=fake_client):
        return asyncio.run(channel._send_payload(payload or {"blocks": []}))


class TestBuildPayload:
    def test_exact_full_block_kit_payload_and_action_truncation(self) -> None:
        channel = SlackChannel(WEBHOOK_URL, dashboard_base_url="https://dashboard.example")

        payload = channel._build_payload(_make_card(), thread_ts="1712345678.123")

        assert payload == {
            "blocks": _full_blocks(),
            "attachments": [
                {
                    "color": "#ff9900",
                    "fallback": "HIGH Alert: deployment:alpha - GAS_PRICE_BLOCKED",
                }
            ],
            "thread_ts": "1712345678.123",
            "reply_broadcast": True,
        }
        action_elements = payload["blocks"][-1]["elements"]
        assert len(action_elements) == 5
        assert all("emergency_unwind" not in element["action_id"] for element in action_elements)

    def test_exact_minimal_blocks_without_optional_sections_or_dashboard(self) -> None:
        channel = SlackChannel(WEBHOOK_URL)
        card = _make_card(context={}, risk_description="", available_actions=[AvailableAction.PAUSE])
        card.position_summary = None  # type: ignore[assignment]
        card.suggested_actions = []

        assert channel._build_blocks(card) == [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": ":rotating_light: HIGH Alert",
                    "emoji": True,
                },
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": "*Strategy:*\ndeployment:alpha"},
                    {"type": "mrkdwn", "text": "*Status:*\nSTUCK"},
                    {"type": "mrkdwn", "text": "*Reason:*\nGas Price Blocked"},
                    {"type": "mrkdwn", "text": "*Timestamp:*\n2026-08-20 12:34:56 UTC"},
                ],
            },
            {"type": "divider"},
        ]

    def test_emergency_and_unknown_action_labels(self) -> None:
        channel = SlackChannel(WEBHOOK_URL, dashboard_base_url="https://dashboard.example")
        card = _make_card(available_actions=[AvailableAction.EMERGENCY_UNWIND, _UnexpectedAction.ACKNOWLEDGE])

        elements = channel._build_blocks(card)[-1]["elements"]

        assert elements[-2:] == [
            {
                "type": "button",
                "text": {
                    "type": "plain_text",
                    "text": ":rotating_light: Emergency Unwind",
                    "emoji": True,
                },
                "url": "https://dashboard.example/strategy/deployment:alpha/action/emergency_unwind",
                "action_id": "action_emergency_unwind",
            },
            {
                "type": "button",
                "text": {"type": "plain_text", "text": "ACKNOWLEDGE", "emoji": True},
                "url": "https://dashboard.example/strategy/deployment:alpha/action/acknowledge",
                "action_id": "action_acknowledge",
            },
        ]

    @pytest.mark.parametrize(
        ("severity", "emoji", "color"),
        [
            (Severity.LOW, ":information_source:", "#36a64f"),
            (Severity.MEDIUM, ":warning:", "#ffcc00"),
            (Severity.HIGH, ":rotating_light:", "#ff9900"),
            (Severity.CRITICAL, ":red_circle:", "#ff0000"),
            (_UnexpectedSeverity.UNKNOWN, ":question:", "#808080"),
        ],
    )
    def test_severity_emoji_color_and_fallback(self, severity: Severity, emoji: str, color: str) -> None:
        channel = SlackChannel(WEBHOOK_URL)
        card = _make_card(severity=severity)

        payload = channel._build_payload(card)

        assert payload["blocks"][0]["text"]["text"] == f"{emoji} {severity.value} Alert"
        assert payload["attachments"] == [
            {
                "color": color,
                "fallback": f"{severity.value} Alert: deployment:alpha - GAS_PRICE_BLOCKED",
            }
        ]
        assert "thread_ts" not in payload
        assert "reply_broadcast" not in payload


class TestThreadState:
    def test_disabled_threading_never_reads_or_stores_threads(self) -> None:
        channel = SlackChannel(WEBHOOK_URL, enable_threading=False)

        channel.set_thread_for_strategy("deployment:alpha", "111.222")

        assert channel._strategy_threads == {}
        assert channel._get_thread_ts("deployment:alpha") is None

    def test_active_expired_and_cleared_threads(self, caplog: pytest.LogCaptureFixture) -> None:
        channel = SlackChannel(WEBHOOK_URL, thread_timeout_seconds=60)
        with patch.object(slack_mod.time, "time", return_value=100.0):
            channel.set_thread_for_strategy("deployment:alpha", "111.222")
        with patch.object(slack_mod.time, "time", return_value=159.0):
            assert channel._get_thread_ts("deployment:alpha") == "111.222"
        with patch.object(slack_mod.time, "time", return_value=161.0), caplog.at_level(logging.DEBUG):
            assert channel._get_thread_ts("deployment:alpha") is None
        assert "Thread expired for strategy deployment:alpha" in caplog.text

        channel.clear_thread("missing")
        channel.set_thread_for_strategy("deployment:alpha", "333.444")
        channel.clear_thread("deployment:alpha")
        assert channel._strategy_threads == {}

        channel.set_thread_for_strategy("deployment:alpha", "555.666")
        channel.clear_all_threads()
        assert channel._strategy_threads == {}

    def test_missing_thread_returns_none(self) -> None:
        assert SlackChannel(WEBHOOK_URL)._get_thread_ts("missing") is None


class TestSendPayload:
    def test_empty_webhook_url_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="webhook_url is required"):
            SlackChannel("")

    def test_posts_exact_payload_and_accepts_http_200(self) -> None:
        channel = SlackChannel(WEBHOOK_URL)
        fake = _FakeAsyncClient(httpx.Response(200, text="ok"))
        payload = {"blocks": [{"type": "divider"}]}

        result = _send_payload(channel, fake, payload)

        assert result == SlackSendResult(success=True)
        assert fake.posts == [(WEBHOOK_URL, payload, 30.0)]

    @pytest.mark.parametrize(("headers", "expected"), [({"Retry-After": "7"}, 7), ({}, 60)])
    def test_rate_limit_retry_after_header_and_default(self, headers: dict[str, str], expected: int) -> None:
        channel = SlackChannel(WEBHOOK_URL)
        fake = _FakeAsyncClient(httpx.Response(429, text="rate_limited", headers=headers))

        result = _send_payload(channel, fake)

        assert result == SlackSendResult(success=False, error="Rate limited by Slack", retry_after=expected)

    def test_malformed_retry_after_uses_default(self) -> None:
        channel = SlackChannel(WEBHOOK_URL)
        fake = _FakeAsyncClient(httpx.Response(429, text="rate_limited", headers={"Retry-After": "later"}))

        result = _send_payload(channel, fake)

        assert result == SlackSendResult(success=False, error="Rate limited by Slack", retry_after=60)

    def test_non_200_response_includes_status_and_body(self) -> None:
        channel = SlackChannel(WEBHOOK_URL)
        fake = _FakeAsyncClient(httpx.Response(400, text="invalid_blocks"))

        result = _send_payload(channel, fake)

        assert result == SlackSendResult(success=False, error="HTTP 400: invalid_blocks")

    def test_non_200_response_redacts_secrets_before_truncating_body(self) -> None:
        channel = SlackChannel(WEBHOOK_URL)
        body = f"failed for {WEBHOOK_URL}; token=secret-value; " + "x" * 250
        fake = _FakeAsyncClient(httpx.Response(500, text=body))

        result = _send_payload(channel, fake)

        assert result.error is not None
        assert result.error.startswith("HTTP 500: failed for <redacted webhook URL>; token=***; ")
        assert WEBHOOK_URL not in result.error
        assert "secret-value" not in result.error
        assert len(result.error.removeprefix("HTTP 500: ")) == 200

    @pytest.mark.parametrize(
        ("exc", "expected"),
        [
            (httpx.TimeoutException("timed out"), "Request timeout"),
            (httpx.ConnectError("connection refused"), "Request error: connection refused"),
        ],
    )
    def test_transport_exceptions_are_results(self, exc: Exception, expected: str) -> None:
        channel = SlackChannel(WEBHOOK_URL)

        result = _send_payload(channel, _FakeAsyncClient(exc=exc))

        assert result == SlackSendResult(success=False, error=expected)

    def test_request_error_redacts_webhook_url_and_secret(self) -> None:
        channel = SlackChannel(WEBHOOK_URL)
        exc = httpx.ConnectError(
            f"connection failed for {WEBHOOK_URL}; credential secret-value",
            request=httpx.Request("POST", WEBHOOK_URL),
        )

        result = _send_payload(channel, _FakeAsyncClient(exc=exc))

        assert result.error == "Request error: connection failed for <redacted webhook URL>; credential ***"
        assert WEBHOOK_URL not in result.error
        assert "secret-value" not in result.error


class TestSendAlert:
    def test_success_uses_stored_thread_and_enforces_local_rate_limit(self) -> None:
        channel = SlackChannel(WEBHOOK_URL)
        channel._strategy_threads["deployment:alpha"] = ("111.222", 9.0)
        channel._last_send_time = 9.5
        channel._send_payload = AsyncMock(return_value=SlackSendResult(success=True))

        with (
            patch.object(slack_mod.time, "time", side_effect=[10.0, 10.0, 10.25]),
            patch.object(slack_mod.asyncio, "sleep", new_callable=AsyncMock) as sleep,
        ):
            result = asyncio.run(channel.send_alert(_make_card()))

        assert result == SlackSendResult(success=True, thread_ts="111.222")
        assert channel._last_send_time == 10.25
        sleep.assert_awaited_once_with(0.5)
        payload = channel._send_payload.await_args.args[0]
        assert payload["thread_ts"] == "111.222"
        assert payload["reply_broadcast"] is True

    def test_explicit_thread_wins_over_stored_thread(self) -> None:
        channel = SlackChannel(WEBHOOK_URL)
        channel._strategy_threads["deployment:alpha"] = ("stored", 0.0)
        channel._send_payload = AsyncMock(return_value=SlackSendResult(success=True))

        result = asyncio.run(channel.send_alert(_make_card(), thread_ts="explicit"))

        assert result.thread_ts == "explicit"
        assert channel._send_payload.await_args.args[0]["thread_ts"] == "explicit"

    def test_non_rate_failures_retry_with_exponential_backoff(self) -> None:
        channel = SlackChannel(WEBHOOK_URL, max_retries=2, base_delay=0.25)
        channel._send_payload = AsyncMock(
            side_effect=[
                SlackSendResult(success=False, error="first"),
                SlackSendResult(success=False, error="second"),
                SlackSendResult(success=True),
            ]
        )

        with patch.object(slack_mod.asyncio, "sleep", new_callable=AsyncMock) as sleep:
            result = asyncio.run(channel.send_alert(_make_card()))

        assert result.success is True
        assert channel._send_payload.await_count == 3
        assert channel._send_payload.await_args_list[0] == channel._send_payload.await_args_list[1]
        assert channel._send_payload.await_args_list[1] == channel._send_payload.await_args_list[2]
        assert sleep.await_args_list == [call(0.25), call(0.5)]

    def test_rate_limit_delay_is_followed_by_retry_backoff(self) -> None:
        channel = SlackChannel(WEBHOOK_URL, max_retries=1, base_delay=0.5)
        channel._send_payload = AsyncMock(
            side_effect=[
                SlackSendResult(success=False, error="rate", retry_after=7),
                SlackSendResult(success=True),
            ]
        )

        with patch.object(slack_mod.asyncio, "sleep", new_callable=AsyncMock) as sleep:
            result = asyncio.run(channel.send_alert(_make_card()))

        assert result.success is True
        assert sleep.await_args_list == [call(7), call(0.5)]

    def test_exhaustion_returns_last_error_without_sleeping_after_final_rate_limit(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        channel = SlackChannel(WEBHOOK_URL, max_retries=0)
        channel._send_payload = AsyncMock(
            return_value=SlackSendResult(success=False, error="still limited", retry_after=9)
        )

        with (
            patch.object(slack_mod.asyncio, "sleep", new_callable=AsyncMock) as sleep,
            caplog.at_level(logging.WARNING),
        ):
            result = asyncio.run(channel.send_alert(_make_card()))

        assert result == SlackSendResult(success=False, error="still limited")
        sleep.assert_not_awaited()
        assert "Failed to send Slack alert after 1 attempts" in caplog.text

    def test_negative_max_retries_makes_no_send_attempt(self) -> None:
        channel = SlackChannel(WEBHOOK_URL, max_retries=-1)
        channel._send_payload = AsyncMock()

        result = asyncio.run(channel.send_alert(_make_card()))

        assert result == SlackSendResult(success=False, error=None)
        channel._send_payload.assert_not_awaited()

    def test_http_error_does_not_expose_webhook_secret_in_logs(self, caplog: pytest.LogCaptureFixture) -> None:
        channel = SlackChannel(WEBHOOK_URL, max_retries=0)
        fake = _FakeAsyncClient(httpx.Response(500, text=f"failed for {WEBHOOK_URL}"))

        with (
            patch.object(slack_mod.httpx, "AsyncClient", return_value=fake),
            caplog.at_level(logging.WARNING),
        ):
            result = asyncio.run(channel.send_alert(_make_card()))

        assert result.error == "HTTP 500: failed for <redacted webhook URL>"
        assert WEBHOOK_URL not in caplog.text
        assert "secret-value" not in caplog.text

    def test_sync_wrapper_forwards_card_and_thread(self) -> None:
        channel = SlackChannel(WEBHOOK_URL)
        channel.send_alert = AsyncMock(return_value=SlackSendResult(success=True, thread_ts="111.222"))
        card = _make_card()

        result = channel.send_alert_sync(card, thread_ts="111.222")

        assert result == SlackSendResult(success=True, thread_ts="111.222")
        channel.send_alert.assert_awaited_once_with(card, thread_ts="111.222")


class TestSendCustomMessage:
    def test_exact_payload_preserves_custom_inputs_and_uses_stored_thread(self) -> None:
        channel = SlackChannel(WEBHOOK_URL, dashboard_base_url="https://dashboard.example")
        channel._strategy_threads["deployment:custom"] = ("222.333", 1.0)
        channel._send_payload = AsyncMock(return_value=SlackSendResult(success=True))

        with patch.object(slack_mod.time, "time", return_value=2.0):
            result = asyncio.run(
                channel.send_custom_message(
                    deployment_id="deployment:custom",
                    severity=Severity.CRITICAL,
                    title="*Manual* intervention",
                    message="Inspect <https://example.com|the run> now",
                    context={"owner_name": "ops", "attempt": 0, "note": None},
                )
            )

        assert result == SlackSendResult(success=True, thread_ts="222.333")
        assert channel._send_payload.await_args.args[0] == {
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": ":red_circle: CRITICAL: *Manual* intervention",
                        "emoji": True,
                    },
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "*Strategy:* deployment:custom\n\nInspect <https://example.com|the run> now",
                    },
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "*Details:*\n• Owner Name: ops\n• Attempt: 0\n• Note: None",
                    },
                },
                {
                    "type": "actions",
                    "elements": [
                        {
                            "type": "button",
                            "text": {
                                "type": "plain_text",
                                "text": ":bar_chart: View Dashboard",
                                "emoji": True,
                            },
                            "url": "https://dashboard.example/strategy/deployment:custom",
                            "action_id": "view_dashboard",
                        }
                    ],
                },
            ],
            "attachments": [
                {
                    "color": "#ff0000",
                    "fallback": "CRITICAL: *Manual* intervention - deployment:custom",
                }
            ],
            "thread_ts": "222.333",
            "reply_broadcast": True,
        }

    def test_exact_minimal_payload_and_success_without_thread(self) -> None:
        channel = SlackChannel(WEBHOOK_URL)
        channel._send_payload = AsyncMock(return_value=SlackSendResult(success=True))

        result = asyncio.run(
            channel.send_custom_message(
                deployment_id="deployment:custom",
                severity=Severity.LOW,
                title="",
                message="",
                context={},
            )
        )

        assert result == SlackSendResult(success=True)
        assert channel._send_payload.await_args.args[0] == {
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": ":information_source: LOW: ",
                        "emoji": True,
                    },
                },
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": "*Strategy:* deployment:custom\n\n"},
                },
            ],
            "attachments": [
                {
                    "color": "#36a64f",
                    "fallback": "LOW:  - deployment:custom",
                }
            ],
        }

    def test_failure_is_returned_unchanged(self, caplog: pytest.LogCaptureFixture) -> None:
        channel = SlackChannel(WEBHOOK_URL)
        failure = SlackSendResult(success=False, error="invalid_payload")
        channel._send_payload = AsyncMock(return_value=failure)

        with caplog.at_level(logging.ERROR):
            result = asyncio.run(
                channel.send_custom_message(
                    "deployment:custom",
                    Severity.MEDIUM,
                    "Title",
                    "Message",
                )
            )

        assert result is failure
        assert "Failed to send custom Slack message for strategy deployment:custom: invalid_payload" in caplog.text

    def test_rate_limit_result_is_not_retried(self) -> None:
        channel = SlackChannel(WEBHOOK_URL, max_retries=3)
        channel._send_payload = AsyncMock(
            return_value=SlackSendResult(success=False, error="Rate limited by Slack", retry_after=12)
        )

        result = asyncio.run(
            channel.send_custom_message(
                "deployment:custom",
                Severity.HIGH,
                "Title",
                "Message",
            )
        )

        assert result.retry_after == 12
        channel._send_payload.assert_awaited_once()
