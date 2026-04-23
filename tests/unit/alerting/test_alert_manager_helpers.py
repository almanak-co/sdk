"""Unit tests for the extracted ``_alert_manager_helpers`` module.

These tests target each helper directly (not through ``send_alert``), to
drive coverage on the helper module up to >= 85%.

No real network I/O - every channel client is a mock.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.alerting import _alert_manager_helpers as helpers
from almanak.framework.alerting.alert_config import (
    AlertChannel,
    AlertCondition,
    AlertRule,
)
from almanak.framework.alerting.alert_manager import (
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
# Shared fixtures
# ---------------------------------------------------------------------------


def _card(strategy_id: str = "s-1", severity: Severity = Severity.HIGH) -> OperatorCard:
    return OperatorCard(
        strategy_id=strategy_id,
        timestamp=datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC),
        event_type=EventType.ERROR,
        reason=StuckReason.RPC_FAILURE,
        context={},
        severity=severity,
        position_summary=PositionSummary(
            total_value_usd=Decimal("0"),
            available_balance_usd=Decimal("0"),
        ),
        risk_description="desc",
        suggested_actions=[
            SuggestedAction(
                action=AvailableAction.PAUSE,
                description="d",
                priority=1,
                is_recommended=True,
            )
        ],
        available_actions=[AvailableAction.PAUSE],
    )


def _rule(
    *,
    condition: AlertCondition = AlertCondition.STRATEGY_ERROR,
    channels: list[AlertChannel] | None = None,
) -> AlertRule:
    return AlertRule(
        condition=condition,
        threshold=Decimal("0"),
        severity=Severity.HIGH,
        channels=channels or [AlertChannel.TELEGRAM],
        cooldown_seconds=0,
    )


# ---------------------------------------------------------------------------
# collect_channels_to_send
# ---------------------------------------------------------------------------


class TestCollectChannelsToSend:
    def test_returns_channels_for_passing_rules(self):
        mgr = MagicMock()
        mgr._should_send_alert = MagicMock(return_value=(True, None))
        rule = _rule(channels=[AlertChannel.TELEGRAM, AlertChannel.SLACK])
        out = helpers.collect_channels_to_send(mgr, [rule], _card())
        assert out == {AlertChannel.TELEGRAM, AlertChannel.SLACK}

    def test_skips_suppressed_rules(self):
        mgr = MagicMock()
        mgr._should_send_alert = MagicMock(return_value=(False, "on cooldown"))
        rule = _rule(channels=[AlertChannel.TELEGRAM])
        out = helpers.collect_channels_to_send(mgr, [rule], _card())
        assert out == set()

    def test_mixed_passing_and_suppressed(self):
        rule_a = _rule(channels=[AlertChannel.TELEGRAM])
        rule_b = _rule(channels=[AlertChannel.SLACK])
        mgr = MagicMock()
        mgr._should_send_alert = MagicMock(side_effect=[(True, None), (False, "cd")])
        out = helpers.collect_channels_to_send(mgr, [rule_a, rule_b], _card())
        assert out == {AlertChannel.TELEGRAM}

    def test_dedupes_channels_across_rules(self):
        rule_a = _rule(channels=[AlertChannel.TELEGRAM])
        rule_b = _rule(channels=[AlertChannel.TELEGRAM])
        mgr = MagicMock()
        mgr._should_send_alert = MagicMock(return_value=(True, None))
        out = helpers.collect_channels_to_send(mgr, [rule_a, rule_b], _card())
        assert out == {AlertChannel.TELEGRAM}

    def test_empty_rules_returns_empty_set(self):
        mgr = MagicMock()
        out = helpers.collect_channels_to_send(mgr, [], _card())
        assert out == set()


# ---------------------------------------------------------------------------
# dispatch_telegram
# ---------------------------------------------------------------------------


class TestDispatchTelegram:
    @pytest.mark.asyncio
    async def test_not_configured(self):
        result = AlertSendResult(success=False)
        await helpers.dispatch_telegram(None, _card(), result)
        assert result.channels_failed == [AlertChannel.TELEGRAM]
        assert result.errors[AlertChannel.TELEGRAM] == "Telegram channel not configured"

    @pytest.mark.asyncio
    async def test_success(self):
        ch = MagicMock()
        ch.send_alert = AsyncMock(
            return_value=MagicMock(success=True, error=None, message_id=1)
        )
        result = AlertSendResult(success=False)
        await helpers.dispatch_telegram(ch, _card(), result)
        assert result.channels_sent == [AlertChannel.TELEGRAM]
        assert result.channels_failed == []

    @pytest.mark.asyncio
    async def test_failure_with_error_string(self):
        ch = MagicMock()
        ch.send_alert = AsyncMock(
            return_value=MagicMock(success=False, error="boom", message_id=None)
        )
        result = AlertSendResult(success=False)
        await helpers.dispatch_telegram(ch, _card(), result)
        assert result.channels_failed == [AlertChannel.TELEGRAM]
        assert result.errors[AlertChannel.TELEGRAM] == "boom"

    @pytest.mark.asyncio
    async def test_failure_with_no_error_defaults_unknown(self):
        ch = MagicMock()
        ch.send_alert = AsyncMock(
            return_value=MagicMock(success=False, error=None, message_id=None)
        )
        result = AlertSendResult(success=False)
        await helpers.dispatch_telegram(ch, _card(), result)
        assert result.errors[AlertChannel.TELEGRAM] == "Unknown error"

    @pytest.mark.asyncio
    async def test_exception_captured(self):
        ch = MagicMock()
        ch.send_alert = AsyncMock(side_effect=RuntimeError("net down"))
        result = AlertSendResult(success=False)
        await helpers.dispatch_telegram(ch, _card(), result)
        assert result.channels_failed == [AlertChannel.TELEGRAM]
        assert "net down" in result.errors[AlertChannel.TELEGRAM]


# ---------------------------------------------------------------------------
# dispatch_slack
# ---------------------------------------------------------------------------


class TestDispatchSlack:
    @pytest.mark.asyncio
    async def test_not_configured(self):
        result = AlertSendResult(success=False)
        await helpers.dispatch_slack(None, _card(), result)
        assert result.channels_failed == [AlertChannel.SLACK]
        assert result.errors[AlertChannel.SLACK] == "Slack channel not configured"

    @pytest.mark.asyncio
    async def test_success_without_thread_ts(self):
        ch = MagicMock()
        ch.send_alert = AsyncMock(
            return_value=MagicMock(success=True, error=None, thread_ts=None)
        )
        result = AlertSendResult(success=False)
        await helpers.dispatch_slack(ch, _card(), result)
        assert result.channels_sent == [AlertChannel.SLACK]

    @pytest.mark.asyncio
    async def test_success_with_thread_ts(self):
        ch = MagicMock()
        ch.send_alert = AsyncMock(
            return_value=MagicMock(success=True, error=None, thread_ts="t-42")
        )
        result = AlertSendResult(success=False)
        await helpers.dispatch_slack(ch, _card(), result)
        assert result.channels_sent == [AlertChannel.SLACK]

    @pytest.mark.asyncio
    async def test_failure_with_error(self):
        ch = MagicMock()
        ch.send_alert = AsyncMock(
            return_value=MagicMock(success=False, error="gone", thread_ts=None)
        )
        result = AlertSendResult(success=False)
        await helpers.dispatch_slack(ch, _card(), result)
        assert result.channels_failed == [AlertChannel.SLACK]
        assert result.errors[AlertChannel.SLACK] == "gone"

    @pytest.mark.asyncio
    async def test_failure_without_error_defaults_unknown(self):
        ch = MagicMock()
        ch.send_alert = AsyncMock(
            return_value=MagicMock(success=False, error=None, thread_ts=None)
        )
        result = AlertSendResult(success=False)
        await helpers.dispatch_slack(ch, _card(), result)
        assert result.errors[AlertChannel.SLACK] == "Unknown error"

    @pytest.mark.asyncio
    async def test_exception_captured(self):
        ch = MagicMock()
        ch.send_alert = AsyncMock(side_effect=ValueError("payload"))
        result = AlertSendResult(success=False)
        await helpers.dispatch_slack(ch, _card(), result)
        assert result.channels_failed == [AlertChannel.SLACK]
        assert "payload" in result.errors[AlertChannel.SLACK]


# ---------------------------------------------------------------------------
# dispatch_unsupported
# ---------------------------------------------------------------------------


class TestDispatchUnsupported:
    def test_logs_debug_and_returns(self, caplog):
        caplog.set_level("DEBUG", logger="almanak.framework.alerting._alert_manager_helpers")
        helpers.dispatch_unsupported(AlertChannel.EMAIL, _card("s-email"))
        helpers.dispatch_unsupported(AlertChannel.PAGERDUTY, _card("s-pd"))
        # It is a no-op - nothing mutated, no exception. Log shape is
        # load-bearing for operator log scrapers, pin it.
        messages = [r.getMessage() for r in caplog.records]
        assert any(
            m == "Channel EMAIL not yet implemented, skipping for s-email"
            for m in messages
        )
        assert any(
            m == "Channel PAGERDUTY not yet implemented, skipping for s-pd"
            for m in messages
        )


# ---------------------------------------------------------------------------
# dispatch_to_channels
# ---------------------------------------------------------------------------


class TestDispatchToChannels:
    @pytest.mark.asyncio
    async def test_dispatches_telegram_and_slack(self):
        mgr = MagicMock()
        mgr._telegram_channel = MagicMock()
        mgr._telegram_channel.send_alert = AsyncMock(
            return_value=MagicMock(success=True, error=None, message_id=1)
        )
        mgr._slack_channel = MagicMock()
        mgr._slack_channel.send_alert = AsyncMock(
            return_value=MagicMock(success=True, error=None, thread_ts="t-1")
        )
        result = AlertSendResult(success=False)
        await helpers.dispatch_to_channels(
            mgr,
            {AlertChannel.TELEGRAM, AlertChannel.SLACK},
            _card(),
            result,
        )
        assert set(result.channels_sent) == {
            AlertChannel.TELEGRAM,
            AlertChannel.SLACK,
        }

    @pytest.mark.asyncio
    async def test_email_and_pagerduty_are_no_ops(self):
        mgr = MagicMock()
        mgr._telegram_channel = None
        mgr._slack_channel = None
        result = AlertSendResult(success=False)
        await helpers.dispatch_to_channels(
            mgr,
            {AlertChannel.EMAIL, AlertChannel.PAGERDUTY},
            _card(),
            result,
        )
        assert result.channels_sent == []
        assert result.channels_failed == []


# ---------------------------------------------------------------------------
# record_cooldowns_for_sent_rules
# ---------------------------------------------------------------------------


class TestRecordCooldowns:
    def test_no_sent_channels_is_noop(self):
        tracker = CooldownTracker()
        rule = _rule()
        result = AlertSendResult(success=False)
        helpers.record_cooldowns_for_sent_rules(tracker, [rule], result, "s-1")
        assert tracker.last_alert_time == {}

    def test_records_for_rules_whose_channels_sent(self):
        tracker = CooldownTracker()
        rule_a = _rule(
            condition=AlertCondition.STRATEGY_ERROR,
            channels=[AlertChannel.TELEGRAM],
        )
        rule_b = _rule(
            condition=AlertCondition.RISK_GUARD_TRIGGERED,
            channels=[AlertChannel.SLACK],
        )
        result = AlertSendResult(success=True)
        result.channels_sent = [AlertChannel.TELEGRAM]
        helpers.record_cooldowns_for_sent_rules(
            tracker, [rule_a, rule_b], result, "s-1"
        )
        assert tracker.is_on_cooldown("s-1", AlertCondition.STRATEGY_ERROR, 3600)
        assert not tracker.is_on_cooldown(
            "s-1", AlertCondition.RISK_GUARD_TRIGGERED, 3600
        )


# ---------------------------------------------------------------------------
# finalize_result
# ---------------------------------------------------------------------------


class TestFinalizeResult:
    def test_success_when_any_channels_sent(self):
        result = AlertSendResult(success=False)
        result.channels_sent = [AlertChannel.TELEGRAM]
        out = helpers.finalize_result(result, "s-1")
        assert out.success is True
        assert out is result

    def test_failure_when_no_channels_sent(self):
        result = AlertSendResult(success=False)
        result.skipped_reason = "all gated"
        out = helpers.finalize_result(result, "s-1")
        assert out.success is False
