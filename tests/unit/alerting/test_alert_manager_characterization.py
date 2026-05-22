"""Characterization tests for AlertManager.send_alert and helpers.

These tests pin the current observable behavior of send_alert end-to-end:
- globally-disabled short-circuit
- rule matching (event-type + threshold with BELOW/ABOVE/LOW/HIGH branches)
- quiet-hours and cooldown suppression precedence
- per-channel fan-out to Telegram / Slack (with unconfigured-channel paths)
- exception handling inside each channel dispatch
- unsupported-channel short-circuit (EMAIL / PAGERDUTY)
- cooldown recording rule: only rules whose channels actually sent record
- AlertSendResult shape (success, channels_sent, channels_failed, errors, skipped_reason)

No real network calls are made; every channel client is a mock.

These tests must pass on the UNCHANGED code and continue to pass after refactor.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, time
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.alerting.alert_config import (
    AlertChannel,
    AlertCondition,
    AlertConfig,
    AlertRule,
    TimeRange,
)
from almanak.framework.alerting.alert_manager import (
    CONDITION_EVENT_MAPPING,
    AlertManager,
    AlertSendResult,
    CooldownTracker,
)
from almanak.framework.models.actions import AvailableAction, SuggestedAction
from almanak.framework.models.operator_card import (
    EventType,
    OperatorCard,
    PositionSummary,
    Severity,
)
from almanak.framework.models.stuck_reason import StuckReason


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_card(
    *,
    deployment_id: str = "strat-1",
    event_type: EventType = EventType.ERROR,
    severity: Severity = Severity.HIGH,
    reason: StuckReason = StuckReason.RPC_FAILURE,
) -> OperatorCard:
    """Build a minimal OperatorCard for tests."""
    return OperatorCard(
        deployment_id=deployment_id,
        timestamp=datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC),
        event_type=event_type,
        reason=reason,
        context={"err": "boom"},
        severity=severity,
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


def _rule(
    *,
    condition: AlertCondition = AlertCondition.STRATEGY_ERROR,
    severity: Severity = Severity.HIGH,
    channels: list[AlertChannel] | None = None,
    cooldown_seconds: int = 0,
    threshold: Decimal = Decimal("0"),
    enabled: bool = True,
) -> AlertRule:
    return AlertRule(
        condition=condition,
        threshold=threshold,
        severity=severity,
        channels=channels or [AlertChannel.TELEGRAM],
        cooldown_seconds=cooldown_seconds,
        enabled=enabled,
        description="test rule",
    )


def _make_manager(
    *,
    rules: list[AlertRule] | None = None,
    enabled: bool = True,
    quiet_hours: TimeRange | None = None,
    telegram_chat: str | None = "tg-chat",
    telegram_bot_token: str | None = "tg-bot",
    slack_webhook_url: str | None = "https://hooks.slack.com/x",
    dashboard_base_url: str | None = "https://dash.example",
) -> AlertManager:
    """Build an AlertManager wired with mock channel clients by default."""
    cfg = AlertConfig(
        telegram_chat_id=telegram_chat,
        slack_webhook=None,  # use explicit override for clarity
        dashboard_base_url=dashboard_base_url,
        rules=rules or [],
        quiet_hours=quiet_hours,
        enabled=enabled,
    )
    mgr = AlertManager(
        config=cfg,
        telegram_bot_token=telegram_bot_token,
        slack_webhook_url=slack_webhook_url,
    )
    # Replace real channels with mocks so no network happens.
    if mgr._telegram_channel is not None:
        mgr._telegram_channel = MagicMock()
        mgr._telegram_channel.send_alert = AsyncMock(
            return_value=MagicMock(success=True, error=None, message_id=42)
        )
    if mgr._slack_channel is not None:
        mgr._slack_channel = MagicMock()
        mgr._slack_channel.send_alert = AsyncMock(
            return_value=MagicMock(success=True, error=None, thread_ts="t-1")
        )
    return mgr


# ---------------------------------------------------------------------------
# AlertSendResult shape
# ---------------------------------------------------------------------------


class TestAlertSendResultDefaults:
    def test_defaults_are_empty_containers(self):
        r = AlertSendResult(success=False)
        assert r.success is False
        assert r.channels_sent == []
        assert r.channels_failed == []
        assert r.errors == {}
        assert r.skipped_reason is None


# ---------------------------------------------------------------------------
# Global-disabled short-circuit
# ---------------------------------------------------------------------------


class TestGloballyDisabled:
    @pytest.mark.asyncio
    async def test_returns_skipped_when_globally_disabled(self):
        rule = _rule(channels=[AlertChannel.TELEGRAM])
        mgr = _make_manager(rules=[rule], enabled=False)
        result = await mgr.send_alert(_make_card())
        assert result.success is False
        assert result.skipped_reason == "Alerting is globally disabled"
        assert result.channels_sent == []
        assert result.channels_failed == []
        # Channel must NOT have been called.
        mgr._telegram_channel.send_alert.assert_not_awaited()


# ---------------------------------------------------------------------------
# No matching rules
# ---------------------------------------------------------------------------


class TestNoMatchingRules:
    @pytest.mark.asyncio
    async def test_no_rules_at_all(self):
        mgr = _make_manager(rules=[])
        result = await mgr.send_alert(_make_card())
        assert result.success is False
        assert result.skipped_reason == "No matching alert rules"

    @pytest.mark.asyncio
    async def test_rule_disabled_is_skipped(self):
        rule = _rule(enabled=False)
        mgr = _make_manager(rules=[rule])
        result = await mgr.send_alert(_make_card())
        assert result.success is False
        assert result.skipped_reason == "No matching alert rules"

    @pytest.mark.asyncio
    async def test_event_type_mismatch_skips(self):
        # Rule listens for STRATEGY_STUCK (events: STUCK) but card is ERROR.
        rule = _rule(
            condition=AlertCondition.STRATEGY_STUCK,
            channels=[AlertChannel.TELEGRAM],
        )
        mgr = _make_manager(rules=[rule])
        result = await mgr.send_alert(
            _make_card(event_type=EventType.ERROR)
        )
        assert result.skipped_reason == "No matching alert rules"


# ---------------------------------------------------------------------------
# Rule matching - event-type branches
# ---------------------------------------------------------------------------


class TestEventTypeMatching:
    @pytest.mark.asyncio
    async def test_strategy_stuck_matches_stuck_event(self):
        rule = _rule(
            condition=AlertCondition.STRATEGY_STUCK,
            channels=[AlertChannel.TELEGRAM],
        )
        mgr = _make_manager(rules=[rule])
        result = await mgr.send_alert(
            _make_card(event_type=EventType.STUCK)
        )
        assert result.success is True
        assert AlertChannel.TELEGRAM in result.channels_sent

    @pytest.mark.asyncio
    async def test_strategy_error_matches_error_event(self):
        rule = _rule(
            condition=AlertCondition.STRATEGY_ERROR,
            channels=[AlertChannel.TELEGRAM],
        )
        mgr = _make_manager(rules=[rule])
        result = await mgr.send_alert(
            _make_card(event_type=EventType.ERROR)
        )
        assert result.success is True

    @pytest.mark.asyncio
    async def test_risk_guard_triggered_matches_both_error_and_alert(self):
        # RISK_GUARD_TRIGGERED maps to ["ERROR", "ALERT"].
        rule = _rule(
            condition=AlertCondition.RISK_GUARD_TRIGGERED,
            channels=[AlertChannel.TELEGRAM],
        )
        mgr = _make_manager(rules=[rule])
        r1 = await mgr.send_alert(_make_card(event_type=EventType.ERROR))
        r2 = await mgr.send_alert(_make_card(event_type=EventType.ALERT))
        assert r1.success and r2.success


# ---------------------------------------------------------------------------
# Rule matching - threshold branches
# ---------------------------------------------------------------------------


class TestThresholdMatching:
    @pytest.mark.asyncio
    async def test_below_matches_when_value_below_threshold(self):
        rule = _rule(
            condition=AlertCondition.PNL_24H_BELOW,
            threshold=Decimal("-100"),
            channels=[AlertChannel.TELEGRAM],
        )
        mgr = _make_manager(rules=[rule])
        result = await mgr.send_alert(
            _make_card(event_type=EventType.WARNING),
            metric_values={AlertCondition.PNL_24H_BELOW: Decimal("-200")},
        )
        assert result.success is True

    @pytest.mark.asyncio
    async def test_below_not_matching_when_value_at_or_above(self):
        rule = _rule(
            condition=AlertCondition.PNL_24H_BELOW,
            threshold=Decimal("-100"),
            channels=[AlertChannel.TELEGRAM],
        )
        mgr = _make_manager(rules=[rule])
        result = await mgr.send_alert(
            _make_card(event_type=EventType.WARNING),
            metric_values={AlertCondition.PNL_24H_BELOW: Decimal("0")},
        )
        assert result.skipped_reason == "No matching alert rules"

    @pytest.mark.asyncio
    async def test_above_matches_when_value_exceeds_threshold(self):
        rule = _rule(
            condition=AlertCondition.POSITION_SIZE_ABOVE,
            threshold=Decimal("1000"),
            channels=[AlertChannel.TELEGRAM],
        )
        mgr = _make_manager(rules=[rule])
        result = await mgr.send_alert(
            _make_card(event_type=EventType.WARNING),
            metric_values={AlertCondition.POSITION_SIZE_ABOVE: Decimal("5000")},
        )
        assert result.success is True

    @pytest.mark.asyncio
    async def test_low_matches_when_value_below_threshold(self):
        rule = _rule(
            condition=AlertCondition.BALANCE_LOW,
            threshold=Decimal("50"),
            channels=[AlertChannel.TELEGRAM],
        )
        mgr = _make_manager(rules=[rule])
        result = await mgr.send_alert(
            _make_card(event_type=EventType.WARNING),
            metric_values={AlertCondition.BALANCE_LOW: Decimal("10")},
        )
        assert result.success is True

    @pytest.mark.asyncio
    async def test_high_matches_when_value_exceeds_threshold(self):
        rule = _rule(
            condition=AlertCondition.SLIPPAGE_HIGH,
            threshold=Decimal("100"),
            channels=[AlertChannel.TELEGRAM],
        )
        mgr = _make_manager(rules=[rule])
        result = await mgr.send_alert(
            _make_card(event_type=EventType.WARNING),
            metric_values={AlertCondition.SLIPPAGE_HIGH: Decimal("500")},
        )
        assert result.success is True


# ---------------------------------------------------------------------------
# Quiet hours suppression
# ---------------------------------------------------------------------------


class TestQuietHours:
    @pytest.mark.asyncio
    async def test_non_critical_blocked_in_quiet_hours(self, monkeypatch):
        # Quiet hours 00:00-23:59 (always on), severity HIGH -> suppressed.
        quiet = TimeRange(start=time(0, 0), end=time(23, 59), timezone="UTC")
        rule = _rule(severity=Severity.HIGH, channels=[AlertChannel.TELEGRAM])
        mgr = _make_manager(rules=[rule], quiet_hours=quiet)
        # Force current time inside quiet range.
        monkeypatch.setattr(
            mgr, "_get_current_time", lambda: time(3, 0)
        )
        result = await mgr.send_alert(
            _make_card(severity=Severity.HIGH)
        )
        assert result.success is False
        assert result.skipped_reason == "All matching rules on cooldown or quiet hours"
        mgr._telegram_channel.send_alert.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_critical_passes_through_quiet_hours(self, monkeypatch):
        quiet = TimeRange(start=time(0, 0), end=time(23, 59), timezone="UTC")
        rule = _rule(severity=Severity.CRITICAL, channels=[AlertChannel.TELEGRAM])
        mgr = _make_manager(rules=[rule], quiet_hours=quiet)
        monkeypatch.setattr(
            mgr, "_get_current_time", lambda: time(3, 0)
        )
        result = await mgr.send_alert(
            _make_card(severity=Severity.CRITICAL)
        )
        assert result.success is True
        assert AlertChannel.TELEGRAM in result.channels_sent


# ---------------------------------------------------------------------------
# Cooldown suppression
# ---------------------------------------------------------------------------


class TestCooldown:
    @pytest.mark.asyncio
    async def test_second_fire_within_cooldown_is_suppressed(self):
        rule = _rule(
            channels=[AlertChannel.TELEGRAM],
            cooldown_seconds=3600,
        )
        mgr = _make_manager(rules=[rule])
        card = _make_card()
        r1 = await mgr.send_alert(card)
        assert r1.success is True
        r2 = await mgr.send_alert(card)
        assert r2.success is False
        assert r2.skipped_reason == "All matching rules on cooldown or quiet hours"
        # Only one call to telegram send.
        assert mgr._telegram_channel.send_alert.await_count == 1

    @pytest.mark.asyncio
    async def test_cooldown_not_recorded_when_all_channels_fail(self):
        rule = _rule(
            channels=[AlertChannel.TELEGRAM],
            cooldown_seconds=3600,
        )
        mgr = _make_manager(rules=[rule])
        mgr._telegram_channel.send_alert = AsyncMock(
            return_value=MagicMock(success=False, error="boom", message_id=None)
        )
        card = _make_card()
        r1 = await mgr.send_alert(card)
        assert r1.success is False
        assert AlertChannel.TELEGRAM in r1.channels_failed
        # Since nothing was sent, cooldown must NOT be recorded: rule fires again.
        r2 = await mgr.send_alert(card)
        assert AlertChannel.TELEGRAM in r2.channels_failed
        assert mgr._telegram_channel.send_alert.await_count == 2


# ---------------------------------------------------------------------------
# Per-channel dispatch
# ---------------------------------------------------------------------------


class TestTelegramDispatch:
    @pytest.mark.asyncio
    async def test_success_path(self):
        rule = _rule(channels=[AlertChannel.TELEGRAM])
        mgr = _make_manager(rules=[rule])
        result = await mgr.send_alert(_make_card())
        assert result.success is True
        assert result.channels_sent == [AlertChannel.TELEGRAM]
        assert result.channels_failed == []
        assert result.errors == {}

    @pytest.mark.asyncio
    async def test_failure_from_channel_result(self):
        rule = _rule(channels=[AlertChannel.TELEGRAM])
        mgr = _make_manager(rules=[rule])
        mgr._telegram_channel.send_alert = AsyncMock(
            return_value=MagicMock(success=False, error="rate-limited", message_id=None)
        )
        result = await mgr.send_alert(_make_card())
        assert result.success is False
        assert result.channels_failed == [AlertChannel.TELEGRAM]
        assert result.errors[AlertChannel.TELEGRAM] == "rate-limited"

    @pytest.mark.asyncio
    async def test_failure_unknown_error_default(self):
        rule = _rule(channels=[AlertChannel.TELEGRAM])
        mgr = _make_manager(rules=[rule])
        mgr._telegram_channel.send_alert = AsyncMock(
            return_value=MagicMock(success=False, error=None, message_id=None)
        )
        result = await mgr.send_alert(_make_card())
        assert result.errors[AlertChannel.TELEGRAM] == "Unknown error"

    @pytest.mark.asyncio
    async def test_exception_path(self):
        rule = _rule(channels=[AlertChannel.TELEGRAM])
        mgr = _make_manager(rules=[rule])
        mgr._telegram_channel.send_alert = AsyncMock(
            side_effect=RuntimeError("network down")
        )
        result = await mgr.send_alert(_make_card())
        assert result.success is False
        assert result.channels_failed == [AlertChannel.TELEGRAM]
        assert "network down" in result.errors[AlertChannel.TELEGRAM]

    @pytest.mark.asyncio
    async def test_channel_not_configured(self):
        rule = _rule(channels=[AlertChannel.TELEGRAM])
        mgr = _make_manager(
            rules=[rule],
            telegram_chat=None,
            telegram_bot_token=None,
        )
        # Slack channel still exists; we want Telegram unconfigured.
        assert mgr._telegram_channel is None
        result = await mgr.send_alert(_make_card())
        assert result.success is False
        assert AlertChannel.TELEGRAM in result.channels_failed
        assert (
            result.errors[AlertChannel.TELEGRAM]
            == "Telegram channel not configured"
        )


class TestSlackDispatch:
    @pytest.mark.asyncio
    async def test_success_path(self):
        rule = _rule(channels=[AlertChannel.SLACK])
        mgr = _make_manager(rules=[rule])
        result = await mgr.send_alert(_make_card())
        assert result.success is True
        assert result.channels_sent == [AlertChannel.SLACK]

    @pytest.mark.asyncio
    async def test_failure_from_channel_result(self):
        rule = _rule(channels=[AlertChannel.SLACK])
        mgr = _make_manager(rules=[rule])
        mgr._slack_channel.send_alert = AsyncMock(
            return_value=MagicMock(success=False, error="webhook gone", thread_ts=None)
        )
        result = await mgr.send_alert(_make_card())
        assert result.success is False
        assert result.channels_failed == [AlertChannel.SLACK]
        assert result.errors[AlertChannel.SLACK] == "webhook gone"

    @pytest.mark.asyncio
    async def test_failure_unknown_error_default(self):
        rule = _rule(channels=[AlertChannel.SLACK])
        mgr = _make_manager(rules=[rule])
        mgr._slack_channel.send_alert = AsyncMock(
            return_value=MagicMock(success=False, error=None, thread_ts=None)
        )
        result = await mgr.send_alert(_make_card())
        assert result.errors[AlertChannel.SLACK] == "Unknown error"

    @pytest.mark.asyncio
    async def test_exception_path(self):
        rule = _rule(channels=[AlertChannel.SLACK])
        mgr = _make_manager(rules=[rule])
        mgr._slack_channel.send_alert = AsyncMock(
            side_effect=ValueError("bad payload")
        )
        result = await mgr.send_alert(_make_card())
        assert AlertChannel.SLACK in result.channels_failed
        assert "bad payload" in result.errors[AlertChannel.SLACK]

    @pytest.mark.asyncio
    async def test_channel_not_configured(self):
        rule = _rule(channels=[AlertChannel.SLACK])
        mgr = _make_manager(rules=[rule], slack_webhook_url=None)
        assert mgr._slack_channel is None
        result = await mgr.send_alert(_make_card())
        assert result.success is False
        assert result.errors[AlertChannel.SLACK] == "Slack channel not configured"


class TestUnsupportedChannels:
    @pytest.mark.asyncio
    async def test_email_channel_is_no_op(self):
        rule = _rule(channels=[AlertChannel.EMAIL])
        mgr = _make_manager(rules=[rule])
        result = await mgr.send_alert(_make_card())
        # Nothing sent, nothing failed (per existing behavior).
        assert result.success is False
        assert result.channels_sent == []
        assert result.channels_failed == []
        assert result.errors == {}

    @pytest.mark.asyncio
    async def test_pagerduty_channel_is_no_op(self):
        rule = _rule(channels=[AlertChannel.PAGERDUTY])
        mgr = _make_manager(rules=[rule])
        result = await mgr.send_alert(_make_card())
        assert result.channels_sent == []
        assert result.channels_failed == []


# ---------------------------------------------------------------------------
# Multi-channel fan-out + mixed success/failure
# ---------------------------------------------------------------------------


class TestMultiChannel:
    @pytest.mark.asyncio
    async def test_both_channels_fanout_success(self):
        rule = _rule(channels=[AlertChannel.TELEGRAM, AlertChannel.SLACK])
        mgr = _make_manager(rules=[rule])
        result = await mgr.send_alert(_make_card())
        assert result.success is True
        assert set(result.channels_sent) == {
            AlertChannel.TELEGRAM,
            AlertChannel.SLACK,
        }

    @pytest.mark.asyncio
    async def test_mixed_success_and_failure(self):
        rule = _rule(channels=[AlertChannel.TELEGRAM, AlertChannel.SLACK])
        mgr = _make_manager(rules=[rule])
        mgr._slack_channel.send_alert = AsyncMock(
            return_value=MagicMock(success=False, error="gone", thread_ts=None)
        )
        result = await mgr.send_alert(_make_card())
        # Overall success because telegram worked.
        assert result.success is True
        assert AlertChannel.TELEGRAM in result.channels_sent
        assert AlertChannel.SLACK in result.channels_failed

    @pytest.mark.asyncio
    async def test_dedupes_channels_across_rules(self):
        # Two rules both fire on this event and both want TELEGRAM.
        rule_a = _rule(
            condition=AlertCondition.STRATEGY_ERROR,
            channels=[AlertChannel.TELEGRAM],
        )
        rule_b = _rule(
            condition=AlertCondition.RISK_GUARD_TRIGGERED,
            channels=[AlertChannel.TELEGRAM],
        )
        mgr = _make_manager(rules=[rule_a, rule_b])
        result = await mgr.send_alert(_make_card(event_type=EventType.ERROR))
        assert result.success is True
        # Channel only called once despite two matching rules.
        assert mgr._telegram_channel.send_alert.await_count == 1


# ---------------------------------------------------------------------------
# Cooldown recording targeting
# ---------------------------------------------------------------------------


class TestCooldownRecording:
    @pytest.mark.asyncio
    async def test_only_successfully_sent_rules_record_cooldown(self):
        # rule_a fires TELEGRAM (succeeds), rule_b fires SLACK (fails).
        rule_a = _rule(
            condition=AlertCondition.STRATEGY_ERROR,
            channels=[AlertChannel.TELEGRAM],
            cooldown_seconds=3600,
        )
        rule_b = _rule(
            condition=AlertCondition.RISK_GUARD_TRIGGERED,
            channels=[AlertChannel.SLACK],
            cooldown_seconds=3600,
        )
        mgr = _make_manager(rules=[rule_a, rule_b])
        mgr._slack_channel.send_alert = AsyncMock(
            return_value=MagicMock(success=False, error="x", thread_ts=None)
        )
        card = _make_card(event_type=EventType.ERROR)
        result = await mgr.send_alert(card)
        assert AlertChannel.TELEGRAM in result.channels_sent
        assert AlertChannel.SLACK in result.channels_failed

        # rule_a (TELEGRAM, succeeded) -> cooldown recorded.
        assert mgr.cooldown_tracker.is_on_cooldown(
            deployment_id=card.deployment_id,
            condition=AlertCondition.STRATEGY_ERROR,
            cooldown_seconds=3600,
        )
        # rule_b (SLACK, failed) -> cooldown NOT recorded.
        assert not mgr.cooldown_tracker.is_on_cooldown(
            deployment_id=card.deployment_id,
            condition=AlertCondition.RISK_GUARD_TRIGGERED,
            cooldown_seconds=3600,
        )


# ---------------------------------------------------------------------------
# CooldownTracker primitives
# ---------------------------------------------------------------------------


class TestCooldownTrackerBehavior:
    def test_initially_not_on_cooldown(self):
        t = CooldownTracker()
        assert not t.is_on_cooldown("s", AlertCondition.STRATEGY_ERROR, 60)

    def test_record_then_within_cooldown(self):
        t = CooldownTracker()
        now = datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC)
        t.record_alert("s", AlertCondition.STRATEGY_ERROR, sent_time=now)
        assert t.is_on_cooldown(
            "s",
            AlertCondition.STRATEGY_ERROR,
            cooldown_seconds=60,
            current_time=datetime(2026, 1, 1, 0, 0, 30, tzinfo=UTC),
        )
        assert not t.is_on_cooldown(
            "s",
            AlertCondition.STRATEGY_ERROR,
            cooldown_seconds=60,
            current_time=datetime(2026, 1, 1, 0, 1, 1, tzinfo=UTC),
        )

    def test_clear_cooldown_removes_entry(self):
        t = CooldownTracker()
        t.record_alert("s", AlertCondition.STRATEGY_ERROR)
        t.clear_cooldown("s", AlertCondition.STRATEGY_ERROR)
        assert not t.is_on_cooldown("s", AlertCondition.STRATEGY_ERROR, 3600)

    def test_clear_all_for_strategy(self):
        t = CooldownTracker()
        t.record_alert("s1", AlertCondition.STRATEGY_ERROR)
        t.record_alert("s1", AlertCondition.RISK_GUARD_TRIGGERED)
        t.record_alert("s2", AlertCondition.STRATEGY_ERROR)
        t.clear_all_for_strategy("s1")
        assert not t.is_on_cooldown("s1", AlertCondition.STRATEGY_ERROR, 3600)
        assert not t.is_on_cooldown(
            "s1", AlertCondition.RISK_GUARD_TRIGGERED, 3600
        )
        assert t.is_on_cooldown("s2", AlertCondition.STRATEGY_ERROR, 3600)


# ---------------------------------------------------------------------------
# Event mapping stability (pin the constant)
# ---------------------------------------------------------------------------


class TestConditionEventMapping:
    def test_mapping_has_expected_keys(self):
        # Pin the exact event mapping. If the mapping changes, chars break
        # and we know operator-facing behavior changed.
        assert CONDITION_EVENT_MAPPING[AlertCondition.STRATEGY_STUCK] == ["STUCK"]
        assert CONDITION_EVENT_MAPPING[AlertCondition.STRATEGY_ERROR] == ["ERROR"]
        assert CONDITION_EVENT_MAPPING[AlertCondition.STRATEGY_PAUSED] == ["WARNING"]
        assert CONDITION_EVENT_MAPPING[AlertCondition.STRATEGY_RESUMED] == ["ALERT"]
        assert CONDITION_EVENT_MAPPING[AlertCondition.RISK_GUARD_TRIGGERED] == ["ERROR", "ALERT"]
        assert CONDITION_EVENT_MAPPING[AlertCondition.CIRCUIT_BREAKER_TRIGGERED] == ["ERROR"]
        assert CONDITION_EVENT_MAPPING[AlertCondition.LIQUIDATION_RISK] == ["ALERT", "WARNING"]
        assert CONDITION_EVENT_MAPPING[AlertCondition.TRANSACTION_PENDING_TIMEOUT] == ["STUCK"]
