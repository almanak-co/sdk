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
import time
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


def _make_card(deployment_id: str = "strat-1") -> OperatorCard:
    """Build a minimal OperatorCard for webhook tests."""
    return OperatorCard(
        deployment_id=deployment_id,
        timestamp=T0,
        event_type=EventType.ALERT,
        reason=StuckReason.RPC_FAILURE,
        context={"err": "boom"},
        severity=Severity.HIGH,
        position_summary=PositionSummary(
            total_value_usd=Decimal("1000"),
            available_balance_usd=Decimal("100"),
        ),
        risk_description="Strategy cannot reach RPC",
        suggested_actions=[
            SuggestedAction(
                action=AvailableAction.PAUSE,
                description="Pause until RPC restored",
                priority=1,
                is_recommended=True,
            )
        ],
        available_actions=[AvailableAction.PAUSE, AvailableAction.RESUME],
    )


class _FakeResponse:
    """Async-context-manager response with scripted status/body."""

    def __init__(self, status: int = 200, body: str = "ok") -> None:
        self.status = status
        self._body = body

    async def text(self) -> str:
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

    def test_exception_on_final_attempt_is_failure(self, monkeypatch):
        result, fake, _ = _run_send(monkeypatch, [RuntimeError("conn reset")], max_retries=0)
        assert not result.success
        assert result.status_code == 0
        assert result.error == "conn reset"
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
