"""Contracts for AlertManager formatting and direct channel delivery."""

from __future__ import annotations

import logging
from datetime import UTC, datetime, time
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.alerting import alert_manager as alert_manager_module
from almanak.framework.alerting.alert_config import AlertChannel, AlertConfig, TimeRange
from almanak.framework.alerting.alert_manager import AlertManager
from almanak.framework.models.actions import AvailableAction, SuggestedAction
from almanak.framework.models.operator_card import EventType, OperatorCard, PositionSummary, Severity
from almanak.framework.models.stuck_reason import StuckReason

LOGGER = "almanak.framework.alerting.alert_manager"


class _UnknownSeverity:
    value = "UNKNOWN"


def _card(*, severity: Severity = Severity.HIGH) -> OperatorCard:
    return OperatorCard(
        deployment_id="deployment:test-1",
        timestamp=datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC),
        event_type=EventType.ERROR,
        reason=StuckReason.RPC_FAILURE,
        context={"rpc_error": "boom", "attempt": 2},
        severity=severity,
        position_summary=PositionSummary(
            total_value_usd=Decimal("1234.50"),
            available_balance_usd=Decimal("50"),
        ),
        risk_description="Position may become unhealthy",
        suggested_actions=[
            SuggestedAction(
                action=AvailableAction.PAUSE,
                description="Pause and inspect",
                priority=1,
                is_recommended=True,
            )
        ],
        available_actions=[AvailableAction.PAUSE],
    )


def _manager(
    *,
    dashboard_base_url: str | None = None,
    enabled: bool = True,
    quiet_hours: TimeRange | None = None,
) -> AlertManager:
    return AlertManager(
        AlertConfig(
            dashboard_base_url=dashboard_base_url,
            enabled=enabled,
            quiet_hours=quiet_hours,
        )
    )


def _channel(manager: AlertManager, channel: AlertChannel, result: object | None = None) -> MagicMock:
    transport = MagicMock()
    transport.send_alert = AsyncMock(return_value=result)
    if channel == AlertChannel.TELEGRAM:
        manager._telegram_channel = transport
    else:
        manager._slack_channel = transport
    return transport


async def _send_direct(
    manager: AlertManager,
    channel: AlertChannel,
    card: OperatorCard,
    *,
    thread_ts: str | None = None,
):
    if channel == AlertChannel.TELEGRAM:
        return await manager.send_direct_telegram_alert(card)
    return await manager.send_direct_slack_alert(card, thread_ts=thread_ts)


@pytest.mark.parametrize(
    ("severity", "prefix"),
    [
        (Severity.LOW, "\u2139\ufe0f <b>LOW Alert</b>"),
        (Severity.MEDIUM, "\u26a0\ufe0f <b>MEDIUM Alert</b>"),
        (Severity.HIGH, "\ud83d\udea8 <b>HIGH Alert</b>"),
        (Severity.CRITICAL, "\ud83d\udd34 <b>CRITICAL Alert</b>"),
    ],
)
def test_format_telegram_message_preserves_severity(severity: Severity, prefix: str) -> None:
    assert _manager()._format_telegram_message(_card(severity=severity)).splitlines()[0] == prefix


def test_format_telegram_message_uses_unknown_severity_fallback() -> None:
    card = _card()
    card.severity = _UnknownSeverity()  # type: ignore[assignment]

    assert _manager()._format_telegram_message(card).splitlines()[0] == "\u2753 <b>UNKNOWN Alert</b>"


def test_format_telegram_message_exact_full_payload() -> None:
    message = _manager(dashboard_base_url="https://dashboard.example")._format_telegram_message(_card())

    assert message == (
        "\ud83d\udea8 <b>HIGH Alert</b>\n"
        "\n"
        "<b>Strategy:</b> deployment:test-1\n"
        "<b>Status:</b> ERROR\n"
        "<b>Reason:</b> Rpc Failure\n"
        "\n"
        "<b>Context:</b>\n"
        "  \u2022 Rpc Error: boom\n"
        "  \u2022 Attempt: 2\n"
        "\n"
        "<b>Position at Risk:</b> $1234.50\n"
        "\n"
        "<b>Risk:</b> Position may become unhealthy\n"
        "\n"
        "<b>Recommended:</b> Pause and inspect\n"
        "\n"
        '\ud83d\udcca <a href="https://dashboard.example/strategy/deployment:test-1">View in Dashboard</a>\n'
        "\n"
        "<i>2026-01-02 03:04:05 UTC</i>"
    )


def test_format_telegram_message_exact_minimal_payload() -> None:
    card = _card(severity=Severity.MEDIUM)
    card.context = {}
    card.position_summary = None  # type: ignore[assignment]
    card.risk_description = ""
    card.suggested_actions = []

    assert _manager()._format_telegram_message(card) == (
        "\u26a0\ufe0f <b>MEDIUM Alert</b>\n"
        "\n"
        "<b>Strategy:</b> deployment:test-1\n"
        "<b>Status:</b> ERROR\n"
        "<b>Reason:</b> Rpc Failure\n"
        "\n"
        "<i>2026-01-02 03:04:05 UTC</i>"
    )


def test_format_telegram_message_redacts_registered_secrets(monkeypatch: pytest.MonkeyPatch) -> None:
    secret = "super-secret-token"
    card = _card()
    card.context = {"upstream_error": f"request used {secret}"}
    monkeypatch.setattr(alert_manager_module, "redact", lambda value: value.replace(secret, "<redacted>"))

    message = _manager()._format_telegram_message(card)

    assert secret not in message
    assert "request used <redacted>" in message


@pytest.mark.parametrize("channel", [AlertChannel.TELEGRAM, AlertChannel.SLACK])
@pytest.mark.asyncio
async def test_direct_alert_obeys_quiet_hours(channel: AlertChannel, caplog: pytest.LogCaptureFixture) -> None:
    quiet_hours = TimeRange(start=time(0), end=time(6), timezone="UTC")
    manager = _manager(quiet_hours=quiet_hours)
    manager._get_current_time = MagicMock(return_value=time(3))
    transport = _channel(manager, channel)
    card = _card()
    caplog.set_level(logging.INFO, logger=LOGGER)

    result = await _send_direct(manager, channel, card)

    assert result.success is False
    assert result.skipped_reason == "Quiet hours active (severity=HIGH)"
    assert result.channels_sent == []
    assert result.channels_failed == []
    assert result.errors == {}
    transport.send_alert.assert_not_awaited()
    channel_prefix = "" if channel == AlertChannel.TELEGRAM else " Slack"
    assert f"Direct{channel_prefix} alert skipped for {card.deployment_id}: {result.skipped_reason}" in caplog.messages


@pytest.mark.parametrize("channel", [AlertChannel.TELEGRAM, AlertChannel.SLACK])
@pytest.mark.asyncio
async def test_direct_alert_obeys_global_disable(channel: AlertChannel) -> None:
    manager = _manager(enabled=False)
    transport = _channel(manager, channel)

    result = await _send_direct(manager, channel, _card(severity=Severity.CRITICAL))

    assert result.skipped_reason == "Quiet hours active (severity=CRITICAL)"
    transport.send_alert.assert_not_awaited()


@pytest.mark.parametrize("channel", [AlertChannel.TELEGRAM, AlertChannel.SLACK])
@pytest.mark.asyncio
async def test_direct_alert_reports_unconfigured_channel(
    channel: AlertChannel, caplog: pytest.LogCaptureFixture
) -> None:
    manager = _manager()
    caplog.set_level(logging.WARNING, logger=LOGGER)

    result = await _send_direct(manager, channel, _card())

    channel_name = channel.value.title()
    assert result.success is False
    assert result.skipped_reason == f"{channel_name} channel not configured"
    assert result.channels_failed == [channel]
    assert result.errors == {channel: "Channel not configured"}
    assert f"Direct {channel_name} alert skipped: channel not configured" in caplog.messages


@pytest.mark.parametrize("channel", [AlertChannel.TELEGRAM, AlertChannel.SLACK])
@pytest.mark.asyncio
async def test_direct_alert_success_preserves_transport_arguments_and_logs(
    channel: AlertChannel, caplog: pytest.LogCaptureFixture
) -> None:
    manager = _manager(quiet_hours=TimeRange(start=time(0), end=time(6), timezone="UTC"))
    manager._get_current_time = MagicMock(return_value=time(3))
    send_result = SimpleNamespace(success=True, error=None, message_id=17, thread_ts="response-thread")
    transport = _channel(manager, channel, send_result)
    card = _card(severity=Severity.CRITICAL)
    caplog.set_level(logging.INFO, logger=LOGGER)

    result = await _send_direct(manager, channel, card, thread_ts="request-thread")

    assert result.success is True
    assert result.channels_sent == [channel]
    assert result.channels_failed == []
    assert result.errors == {}
    if channel == AlertChannel.TELEGRAM:
        transport.send_alert.assert_awaited_once_with(card)
        expected_log = f"Direct Telegram alert sent for {card.deployment_id} (severity=CRITICAL, message_id=17)"
    else:
        transport.send_alert.assert_awaited_once_with(card, thread_ts="request-thread")
        expected_log = (
            f"Direct Slack alert sent for {card.deployment_id} (severity=CRITICAL, thread_ts=response-thread)"
        )
    assert expected_log in caplog.messages


@pytest.mark.asyncio
async def test_direct_slack_success_without_response_thread_preserves_log(caplog: pytest.LogCaptureFixture) -> None:
    manager = _manager()
    transport = _channel(manager, AlertChannel.SLACK, SimpleNamespace(success=True, error=None, thread_ts=None))
    card = _card()
    caplog.set_level(logging.INFO, logger=LOGGER)

    result = await manager.send_direct_slack_alert(card)

    assert result.success is True
    transport.send_alert.assert_awaited_once_with(card, thread_ts=None)
    assert f"Direct Slack alert sent for {card.deployment_id} (severity=HIGH)" in caplog.messages


@pytest.mark.parametrize("channel", [AlertChannel.TELEGRAM, AlertChannel.SLACK])
@pytest.mark.asyncio
async def test_direct_channel_failure_preserves_error_and_does_not_retry(
    channel: AlertChannel, caplog: pytest.LogCaptureFixture
) -> None:
    manager = _manager()
    transport = _channel(
        manager,
        channel,
        SimpleNamespace(success=False, error="transport failed", message_id=None, thread_ts=None),
    )
    card = _card()
    caplog.set_level(logging.ERROR, logger=LOGGER)

    result = await _send_direct(manager, channel, card)

    channel_name = channel.value.title()
    assert result.success is False
    assert result.channels_sent == []
    assert result.channels_failed == [channel]
    assert result.errors == {channel: "transport failed"}
    assert transport.send_alert.await_count == 1
    assert f"Direct {channel_name} alert failed for {card.deployment_id}: transport failed" in caplog.messages


@pytest.mark.parametrize("channel", [AlertChannel.TELEGRAM, AlertChannel.SLACK])
@pytest.mark.asyncio
async def test_direct_channel_failure_defaults_missing_error(channel: AlertChannel) -> None:
    manager = _manager()
    _channel(
        manager,
        channel,
        SimpleNamespace(success=False, error=None, message_id=None, thread_ts=None),
    )

    result = await _send_direct(manager, channel, _card())

    assert result.errors == {channel: "Unknown error"}


@pytest.mark.parametrize("channel", [AlertChannel.TELEGRAM, AlertChannel.SLACK])
@pytest.mark.asyncio
async def test_direct_channel_failure_redacts_error_and_log(
    channel: AlertChannel,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    secret = "super-secret-token"
    manager = _manager()
    _channel(
        manager,
        channel,
        SimpleNamespace(success=False, error=f"request exposed {secret}", message_id=None, thread_ts=None),
    )
    monkeypatch.setattr(alert_manager_module, "redact", lambda value: value.replace(secret, "<redacted>"))
    caplog.set_level(logging.ERROR, logger=LOGGER)

    result = await _send_direct(manager, channel, _card())

    assert result.errors == {channel: "request exposed <redacted>"}
    assert secret not in caplog.text


@pytest.mark.parametrize("channel", [AlertChannel.TELEGRAM, AlertChannel.SLACK])
@pytest.mark.asyncio
async def test_direct_channel_exception_is_redacted_with_traceback(
    channel: AlertChannel,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    secret = "super-secret-token"
    manager = _manager()
    transport = _channel(manager, channel)
    transport.send_alert.side_effect = RuntimeError(f"request exposed {secret}")
    monkeypatch.setattr(alert_manager_module, "redact", lambda value: value.replace(secret, "<redacted>"))
    card = _card()
    caplog.set_level(logging.ERROR, logger=LOGGER)

    result = await _send_direct(manager, channel, card)

    channel_name = channel.value.title()
    assert result.success is False
    assert result.channels_failed == [channel]
    assert result.errors == {channel: "request exposed <redacted>"}
    assert transport.send_alert.await_count == 1
    assert (
        f"Exception sending direct {channel_name} alert for {card.deployment_id}: request exposed <redacted>"
        in caplog.messages
    )
    assert secret not in caplog.text
    assert caplog.records[-1].exc_info is not None
