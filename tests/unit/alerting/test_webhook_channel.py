"""Unit tests for WebhookChannel (almanak/framework/alerting/channels/webhook.py).

Branch coverage for ``send_alert`` (async, aiohttp) and ``send_alert_sync``
(requests): success, non-2xx failure, 429 retry-then-success, 429 with
retries exhausted, exception retry-then-success, exception on the final
attempt, the ``max_retries=-1`` loop fall-through, and the rate-limit
branches.

The HTTP layer is faked at the module seam: the ``aiohttp`` binding inside
the webhook module is replaced with a scripted namespace, and
``requests.post`` is monkeypatched for the sync path. No sockets are opened;
retry backoff uses ``base_delay=0.0`` so retries are instant.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from copy import deepcopy
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

import pytest

import almanak.framework.alerting.channels.webhook as webhook_module
from almanak.framework.alerting.channels.webhook import WebhookChannel
from almanak.framework.models.actions import AvailableAction, SuggestedAction
from almanak.framework.models.operator_card import (
    EventType,
    OperatorCard,
    PositionSummary,
    Severity,
)
from almanak.framework.models.stuck_reason import StuckReason

T0 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)


def _make_card(**overrides: object) -> OperatorCard:
    """Build a minimal OperatorCard for webhook tests."""
    values = {
        "deployment_id": "strat-1",
        "timestamp": T0,
        "event_type": EventType.ALERT,
        "reason": StuckReason.RPC_FAILURE,
        "context": {"err": "boom"},
        "severity": Severity.HIGH,
        "position_summary": PositionSummary(
            total_value_usd=Decimal("1000"),
            available_balance_usd=Decimal("100"),
        ),
        "risk_description": "Strategy cannot reach RPC",
        "suggested_actions": [
            SuggestedAction(
                action=AvailableAction.PAUSE,
                description="Pause until RPC restored",
                priority=1,
                is_recommended=True,
            )
        ],
        "available_actions": [AvailableAction.PAUSE, AvailableAction.RESUME],
    }
    values.update(overrides)
    return OperatorCard(**values)  # type: ignore[arg-type]


class _FakeResponse:
    """Async-context-manager response with scripted status/body."""

    def __init__(self, status: int = 200, body: str | Exception = "ok") -> None:
        self.status = status
        self._body = body

    async def text(self) -> str:
        if isinstance(self._body, Exception):
            raise self._body
        return self._body

    async def __aenter__(self) -> _FakeResponse:
        return self

    async def __aexit__(self, *exc: object) -> bool:
        return False


class _FakeAiohttp:
    """Scripted stand-in for the ``aiohttp`` module inside webhook.py.

    Each ``session.post`` pops the next script item: a ``_FakeResponse``
    is returned, an ``Exception`` instance is raised.
    """

    def __init__(self, script: list[Any]) -> None:
        self._script = list(script)
        self.posts: list[dict[str, Any]] = []
        fake = self

        class _Session:
            async def __aenter__(self) -> Any:
                return self

            async def __aexit__(self, *exc: object) -> bool:
                return False

            def post(self, url: str, json: Any = None, headers: Any = None, timeout: Any = None) -> Any:
                fake.posts.append({"url": url, "json": json, "headers": headers, "timeout": timeout})
                item = fake._script.pop(0)
                if isinstance(item, Exception):
                    raise item
                return item

        self.ClientSession = _Session

    @staticmethod
    def ClientTimeout(total: float | None = None) -> dict[str, Any]:
        return {"total": total}


def _run_send(
    monkeypatch: pytest.MonkeyPatch,
    script: list[Any],
    **channel_kwargs: Any,
) -> tuple[Any, _FakeAiohttp, WebhookChannel]:
    fake = _FakeAiohttp(script)
    monkeypatch.setattr(webhook_module, "aiohttp", fake)
    channel_kwargs.setdefault("base_delay", 0.0)
    channel = WebhookChannel("https://hooks.example/x", **channel_kwargs)
    result = asyncio.run(channel.send_alert(_make_card()))
    return result, fake, channel


class TestInit:
    def test_empty_url_raises(self):
        with pytest.raises(ValueError, match="url is required"):
            WebhookChannel("")


class TestFormatPayload:
    def test_exact_payload_and_no_input_mutation(self) -> None:
        channel = WebhookChannel("https://hooks.example/x")
        card = _make_card()
        original = deepcopy(card)

        payload = channel._format_payload(card)

        assert payload == {
            "deployment_id": "strat-1",
            "event_type": "ALERT",
            "severity": "HIGH",
            "reason": "RPC_FAILURE",
            "risk_description": "Strategy cannot reach RPC",
            "suggested_actions": [
                {
                    "action": "PAUSE",
                    "description": "Pause until RPC restored",
                }
            ],
            "timestamp": "2026-01-01T12:00:00+00:00",
        }
        assert card == original

    def test_optional_and_non_enum_fields_use_documented_fallbacks(self) -> None:
        card = _make_card(risk_description="")
        card.event_type = "CUSTOM_EVENT"  # type: ignore[assignment]
        card.severity = "CUSTOM_SEVERITY"  # type: ignore[assignment]
        card.reason = "CUSTOM_REASON"  # type: ignore[assignment]
        card.suggested_actions = []
        card.timestamp = None  # type: ignore[assignment]

        assert WebhookChannel("https://hooks.example/x")._format_payload(card) == {
            "deployment_id": "strat-1",
            "event_type": "CUSTOM_EVENT",
            "severity": "CUSTOM_SEVERITY",
            "reason": "CUSTOM_REASON",
            "risk_description": "",
            "suggested_actions": [],
            "timestamp": "",
        }

    def test_payload_redacts_dynamic_text_without_mutating_card(self, monkeypatch) -> None:
        card = _make_card(
            deployment_id="deployment-registered-secret",
            risk_description="failed with registered-secret",
        )
        card.suggested_actions[0].description = "remove registered-secret"
        original = deepcopy(card)
        monkeypatch.setattr(webhook_module, "redact", lambda value: value.replace("registered-secret", "***"))

        payload = WebhookChannel("https://hooks.example/x")._format_payload(card)

        assert payload["deployment_id"] == "deployment-***"
        assert payload["risk_description"] == "failed with ***"
        assert payload["suggested_actions"] == [{"action": "PAUSE", "description": "remove ***"}]
        assert card == original

    @pytest.mark.parametrize("severity", list(Severity))
    def test_each_severity_is_serialized_exactly(self, severity: Severity) -> None:
        card = _make_card(severity=severity)

        payload = WebhookChannel("https://hooks.example/x")._format_payload(card)

        assert payload["severity"] == severity.value


class TestSendAlertAsync:
    def test_success_posts_formatted_payload(self, monkeypatch):
        result, fake, channel = _run_send(
            monkeypatch,
            [_FakeResponse(200, "created")],
            headers={"X-Auth": "tok"},
        )
        assert result.success
        assert result.status_code == 200
        assert result.response_body == "created"
        assert result.error == ""
        assert len(fake.posts) == 1
        post = fake.posts[0]
        assert post["url"] == "https://hooks.example/x"
        assert post["json"]["deployment_id"] == "strat-1"
        assert post["json"]["severity"] == Severity.HIGH.value
        assert post["headers"]["Content-Type"] == "application/json"
        assert post["headers"]["X-Auth"] == "tok"
        assert post["timeout"] == {"total": 10.0}
        assert channel._last_send_time > 0.0

    def test_non_2xx_returns_failure_with_truncated_body(self, monkeypatch):
        result, fake, _ = _run_send(monkeypatch, [_FakeResponse(500, "boom" * 100)])
        assert not result.success
        assert result.status_code == 500
        assert result.error.startswith("HTTP 500: ")
        assert len(result.error) <= len("HTTP 500: ") + 200
        assert len(fake.posts) == 1

    def test_non_2xx_redacts_endpoint_headers_and_registered_secrets_before_truncating(self, monkeypatch) -> None:
        url = "https://hooks.example/hook-secret"
        headers = {"Authorization": "Bearer auth-secret"}
        body = f"failed for {url}; Bearer auth-secret; registered-secret; " + "x" * 250
        fake = _FakeAiohttp([_FakeResponse(500, body)])
        monkeypatch.setattr(webhook_module, "aiohttp", fake)
        monkeypatch.setattr(webhook_module, "redact", lambda value: value.replace("registered-secret", "***"))
        channel = WebhookChannel(url, headers=headers, max_retries=0)

        result = asyncio.run(channel.send_alert(_make_card()))

        assert result.error.startswith("HTTP 500: failed for <redacted webhook URL>; ***; ***; ")
        assert len(result.error.removeprefix("HTTP 500: ")) == 200
        assert url not in result.error
        assert "hook-secret" not in result.error
        assert "auth-secret" not in result.error
        assert "registered-secret" not in result.error

    def test_success_does_not_mutate_card_or_caller_headers(self, monkeypatch) -> None:
        headers = {"X-Auth": "auth-token"}
        card = _make_card()
        original_card = deepcopy(card)
        original_headers = dict(headers)
        fake = _FakeAiohttp([_FakeResponse(200, "ok")])
        monkeypatch.setattr(webhook_module, "aiohttp", fake)
        channel = WebhookChannel("https://hooks.example/x", headers=headers)

        result = asyncio.run(channel.send_alert(card))

        assert result.success
        assert card == original_card
        assert headers == original_headers

    def test_success_response_body_is_redacted(self, monkeypatch) -> None:
        url = "https://hooks.example/hook-secret"
        body = f"accepted by {url} using auth-secret and registered-secret"
        fake = _FakeAiohttp([_FakeResponse(200, body)])
        monkeypatch.setattr(webhook_module, "aiohttp", fake)
        monkeypatch.setattr(webhook_module, "redact", lambda value: value.replace("registered-secret", "***"))
        channel = WebhookChannel(url, headers={"X-Auth": "auth-secret"})

        result = asyncio.run(channel.send_alert(_make_card()))

        assert result.response_body == "accepted by <redacted webhook URL> using *** and ***"

    def test_429_retries_then_succeeds(self, monkeypatch):
        result, fake, _ = _run_send(
            monkeypatch,
            [_FakeResponse(429, "slow down"), _FakeResponse(201, "ok")],
        )
        assert result.success
        assert result.status_code == 201
        assert len(fake.posts) == 2

    def test_429_on_final_attempt_is_failure(self, monkeypatch):
        result, fake, _ = _run_send(monkeypatch, [_FakeResponse(429, "slow")], max_retries=0)
        assert not result.success
        assert result.status_code == 429
        assert result.error.startswith("HTTP 429")
        assert len(fake.posts) == 1

    def test_exception_retries_then_succeeds(self, monkeypatch):
        result, fake, _ = _run_send(
            monkeypatch,
            [RuntimeError("conn reset"), _FakeResponse(200, "ok")],
        )
        assert result.success
        assert len(fake.posts) == 2

    def test_exception_retry_log_redacts_transport_credentials(self, monkeypatch, caplog) -> None:
        url = "https://hooks.example/hook-secret"
        raw_error = f"failure from {url} using auth-secret and registered-secret"
        fake = _FakeAiohttp([RuntimeError(raw_error), _FakeResponse(200, "ok")])
        monkeypatch.setattr(webhook_module, "aiohttp", fake)
        monkeypatch.setattr(webhook_module, "redact", lambda value: value.replace("registered-secret", "***"))
        channel = WebhookChannel(url, headers={"X-Auth": "auth-secret"}, max_retries=1, base_delay=0.0)

        with caplog.at_level(logging.WARNING):
            result = asyncio.run(channel.send_alert(_make_card()))

        assert result.success
        assert "failure from <redacted webhook URL> using *** and ***" in caplog.text
        assert url not in caplog.text
        assert "hook-secret" not in caplog.text
        assert "auth-secret" not in caplog.text
        assert "registered-secret" not in caplog.text

    def test_exception_on_final_attempt_is_failure(self, monkeypatch):
        result, fake, _ = _run_send(monkeypatch, [RuntimeError("conn reset")], max_retries=0)
        assert not result.success
        assert result.status_code == 0
        assert result.error == "conn reset"
        assert len(fake.posts) == 1

    def test_malformed_response_body_error_is_returned_without_escaping(self, monkeypatch) -> None:
        result, fake, _ = _run_send(
            monkeypatch,
            [_FakeResponse(502, UnicodeError("response decode failed"))],
            max_retries=0,
        )

        assert result.error == "response decode failed"
        assert len(fake.posts) == 1

    def test_negative_max_retries_reports_max_retries_exceeded(self, monkeypatch):
        # range(max_retries + 1) is empty -> the loop body never runs and the
        # fall-through return is taken.
        result, fake, _ = _run_send(monkeypatch, [], max_retries=-1)
        assert not result.success
        assert result.error == "Max retries exceeded"
        assert fake.posts == []

    def test_rate_limit_branch_sleeps_before_posting(self, monkeypatch):
        # The async path shares the module-level `asyncio` binding; replace it
        # with a namespace whose sleep records instead of blocking.
        sleeps: list[float] = []

        async def _record_sleep(delay: float) -> None:
            sleeps.append(delay)

        monkeypatch.setattr(webhook_module, "asyncio", SimpleNamespace(sleep=_record_sleep))

        fake = _FakeAiohttp([_FakeResponse(200, "ok")])
        monkeypatch.setattr(webhook_module, "aiohttp", fake)
        channel = WebhookChannel("https://hooks.example/x", base_delay=0.0)
        channel._last_send_time = time.monotonic() + 100.0

        result = asyncio.run(channel.send_alert(_make_card()))
        assert result.success
        assert len(fake.posts) == 1
        assert len(sleeps) == 1
        assert sleeps[0] > 0.0


def _run_sync(
    monkeypatch: pytest.MonkeyPatch,
    script: list[Any],
    **channel_kwargs: Any,
) -> tuple[Any, list[dict[str, Any]], WebhookChannel]:
    remaining = list(script)
    calls: list[dict[str, Any]] = []

    def _post(url: str, data: Any = None, headers: Any = None, timeout: Any = None) -> Any:
        calls.append({"url": url, "data": data, "headers": headers, "timeout": timeout})
        item = remaining.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    monkeypatch.setattr("requests.post", _post)
    channel_kwargs.setdefault("base_delay", 0.0)
    channel = WebhookChannel("https://hooks.example/x", **channel_kwargs)
    return channel.send_alert_sync(_make_card()), calls, channel


def _sync_response(status_code: int = 200, text: str = "ok") -> SimpleNamespace:
    return SimpleNamespace(status_code=status_code, text=text)


class TestSendAlertSync:
    def test_success_posts_json_payload(self, monkeypatch):
        result, calls, channel = _run_sync(
            monkeypatch,
            [_sync_response(200, "created")],
            headers={"X-Auth": "tok"},
        )
        assert result.success
        assert result.status_code == 200
        assert result.response_body == "created"
        assert len(calls) == 1
        call = calls[0]
        assert call["url"] == "https://hooks.example/x"
        payload = json.loads(call["data"])
        assert payload["deployment_id"] == "strat-1"
        assert payload["timestamp"] == T0.isoformat()
        assert call["headers"]["Content-Type"] == "application/json"
        assert call["headers"]["X-Auth"] == "tok"
        assert call["timeout"] == 10.0
        assert channel._last_send_time > 0.0

    def test_non_2xx_returns_failure(self, monkeypatch):
        result, calls, _ = _run_sync(monkeypatch, [_sync_response(500, "boom")])
        assert not result.success
        assert result.status_code == 500
        assert result.error == "HTTP 500: boom"
        assert len(calls) == 1

    def test_non_2xx_redacts_endpoint_headers_and_registered_secrets(self, monkeypatch) -> None:
        url = "https://hooks.example/hook-secret"
        body = f"failed for {url} using auth-secret and registered-secret"
        monkeypatch.setattr(webhook_module, "redact", lambda value: value.replace("registered-secret", "***"))
        monkeypatch.setattr("requests.post", lambda *args, **kwargs: _sync_response(500, body))
        channel = WebhookChannel(url, headers={"X-Auth": "auth-secret"}, max_retries=0)

        result = channel.send_alert_sync(_make_card())

        assert result.error == "HTTP 500: failed for <redacted webhook URL> using *** and ***"
        assert url not in result.error
        assert "hook-secret" not in result.error
        assert "auth-secret" not in result.error
        assert "registered-secret" not in result.error

    def test_non_string_response_body_has_stable_error(self, monkeypatch) -> None:
        monkeypatch.setattr("requests.post", lambda *args, **kwargs: _sync_response(500, None))
        channel = WebhookChannel("https://hooks.example/x", max_retries=0)

        result = channel.send_alert_sync(_make_card())

        assert result.error == "HTTP 500: None"

    def test_success_response_body_is_redacted(self, monkeypatch) -> None:
        url = "https://hooks.example/hook-secret"
        body = f"accepted by {url} using auth-secret and registered-secret"
        monkeypatch.setattr("requests.post", lambda *args, **kwargs: _sync_response(200, body))
        monkeypatch.setattr(webhook_module, "redact", lambda value: value.replace("registered-secret", "***"))
        channel = WebhookChannel(url, headers={"X-Auth": "auth-secret"})

        result = channel.send_alert_sync(_make_card())

        assert result.response_body == "accepted by <redacted webhook URL> using *** and ***"

    def test_429_retries_then_succeeds(self, monkeypatch):
        result, calls, _ = _run_sync(
            monkeypatch,
            [_sync_response(429, "slow"), _sync_response(201, "ok")],
        )
        assert result.success
        assert result.status_code == 201
        assert len(calls) == 2

    def test_429_on_final_attempt_is_failure(self, monkeypatch):
        result, calls, _ = _run_sync(monkeypatch, [_sync_response(429, "slow")], max_retries=0)
        assert not result.success
        assert result.status_code == 429
        assert result.error.startswith("HTTP 429")
        assert len(calls) == 1

    def test_exception_retries_then_succeeds(self, monkeypatch):
        result, calls, _ = _run_sync(
            monkeypatch,
            [RuntimeError("conn reset"), _sync_response(200, "ok")],
        )
        assert result.success
        assert len(calls) == 2

    def test_exception_on_final_attempt_is_failure(self, monkeypatch):
        result, calls, _ = _run_sync(monkeypatch, [RuntimeError("conn reset")], max_retries=0)
        assert not result.success
        assert result.error == "conn reset"
        assert len(calls) == 1

    def test_exception_error_redacts_transport_credentials(self, monkeypatch) -> None:
        url = "https://hooks.example/hook-secret"
        raw_error = f"failure from {url} using auth-secret and registered-secret"

        def _raise(*args, **kwargs):
            raise RuntimeError(raw_error)

        monkeypatch.setattr("requests.post", _raise)
        monkeypatch.setattr(webhook_module, "redact", lambda value: value.replace("registered-secret", "***"))
        channel = WebhookChannel(url, headers={"X-Auth": "auth-secret"}, max_retries=0)

        result = channel.send_alert_sync(_make_card())

        assert result.error == "failure from <redacted webhook URL> using *** and ***"
        assert url not in result.error
        assert "hook-secret" not in result.error
        assert "auth-secret" not in result.error
        assert "registered-secret" not in result.error

    def test_negative_max_retries_reports_max_retries_exceeded(self, monkeypatch):
        result, calls, _ = _run_sync(monkeypatch, [], max_retries=-1)
        assert not result.success
        assert result.error == "Max retries exceeded"
        assert calls == []

    def test_rate_limit_branch_sleeps_before_posting(self, monkeypatch):
        # The sync path shares the module-level `time` binding; replace it with
        # a namespace whose sleep records instead of blocking.
        sleeps: list[float] = []
        fake_time = SimpleNamespace(monotonic=time.monotonic, sleep=sleeps.append)
        monkeypatch.setattr(webhook_module, "time", fake_time)

        calls: list[str] = []

        def _post(url: str, data: Any = None, headers: Any = None, timeout: Any = None) -> Any:
            calls.append(url)
            return _sync_response(200, "ok")

        monkeypatch.setattr("requests.post", _post)
        channel = WebhookChannel("https://hooks.example/x", base_delay=0.0)
        channel._last_send_time = time.monotonic() + 100.0

        result = channel.send_alert_sync(_make_card())
        assert result.success
        assert len(calls) == 1
        assert len(sleeps) == 1
        assert sleeps[0] > 0.0
