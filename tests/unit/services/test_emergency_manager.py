"""Lifecycle and alert fan-out coverage for EmergencyManager."""

import logging
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, Mock, patch

import pytest

from almanak.framework.alerting import AlertChannel, AlertSendResult
from almanak.framework.models import EventType, Severity
from almanak.framework.services.emergency_manager import (
    BorrowPosition,
    EmergencyManager,
    FullPositionSummary,
    LPPositionInfo,
    TokenPosition,
)

DEPLOYMENT_ID = "deployment:emergency123"
CHAIN = "arbitrum"
TIMESTAMP = datetime(2026, 9, 4, 12, 0, tzinfo=UTC)


def _position_summary(
    deployment_id: str = DEPLOYMENT_ID,
    *,
    timestamp: datetime = TIMESTAMP,
) -> FullPositionSummary:
    return FullPositionSummary(
        deployment_id=deployment_id,
        chain=CHAIN,
        timestamp=timestamp,
        token_positions=[
            TokenPosition(
                token_symbol="USDC",
                token_address="0x0000000000000000000000000000000000000001",
                balance=Decimal("100"),
                value_usd=Decimal("100"),
                chain=CHAIN,
            )
        ],
        lp_positions=[
            LPPositionInfo(
                position_id="42",
                pool_address="0x0000000000000000000000000000000000000002",
                pool_name="ETH/USDC",
                token0_symbol="ETH",
                token1_symbol="USDC",
                token0_amount=Decimal("1"),
                token1_amount=Decimal("100"),
                value_usd=Decimal("200"),
                range_lower=Decimal("0"),
                range_upper=Decimal("3000"),
                fees_earned_usd=Decimal("2"),
                chain=CHAIN,
            )
        ],
        borrow_positions=[
            BorrowPosition(
                protocol="aave_v3",
                collateral_token="ETH",
                collateral_amount=Decimal("1"),
                collateral_value_usd=Decimal("200"),
                borrow_token="USDC",
                borrow_amount=Decimal("50"),
                borrow_value_usd=Decimal("50"),
                health_factor=Decimal("1.5"),
                liquidation_price=Decimal("0"),
                chain=CHAIN,
            )
        ],
        total_token_value_usd=Decimal("100"),
        total_lp_value_usd=Decimal("200"),
        total_collateral_value_usd=Decimal("200"),
        total_borrowed_value_usd=Decimal("50"),
    )


def _sent(channel: AlertChannel) -> AlertSendResult:
    return AlertSendResult(success=True, channels_sent=[channel])


def _failed(channel: AlertChannel, error: str) -> AlertSendResult:
    return AlertSendResult(success=False, channels_failed=[channel], errors={channel: error})


def _card() -> object:
    summary = _position_summary()
    return EmergencyManager()._generate_emergency_card(
        deployment_id=DEPLOYMENT_ID,
        reason="health factor breached",
        position_summary=summary,
        timestamp=TIMESTAMP,
        trigger_context={"attempt": 2},
    )


def test_emergency_stop_preserves_order_identity_and_complete_state(caplog: pytest.LogCaptureFixture) -> None:
    order: list[tuple[str, object]] = []
    summary = _position_summary()

    def pause(deployment_id: str) -> bool:
        order.append(("pause", deployment_id))
        return True

    def positions(deployment_id: str) -> FullPositionSummary:
        order.append(("positions", deployment_id))
        return summary

    alert_manager = Mock()
    alert_manager.send_direct_telegram_alert_sync.side_effect = lambda card: (
        order.append(("telegram", card)) or _sent(AlertChannel.TELEGRAM)
    )
    alert_manager.send_direct_slack_alert_sync.side_effect = lambda card: (
        order.append(("slack", card)) or _failed(AlertChannel.SLACK, "slack unavailable")
    )
    manager = EmergencyManager(
        alert_manager=alert_manager,
        pause_callback=pause,
        position_callback=positions,
    )

    with (
        patch(
            "almanak.framework.services.emergency_manager.datetime",
        ) as datetime_mock,
        patch(
            "almanak.framework.services.emergency_manager.add_event",
            side_effect=lambda event: order.append(("timeline", event)),
        ),
        caplog.at_level(logging.INFO),
    ):
        datetime_mock.now.return_value = TIMESTAMP
        result = manager.emergency_stop(
            deployment_id=DEPLOYMENT_ID,
            reason="health factor breached",
            chain=CHAIN,
            trigger_context={"attempt": 2},
        )

    assert [name for name, _ in order] == ["pause", "positions", "timeline", "telegram", "slack"]
    timeline_event = order[2][1]
    telegram_card = order[3][1]
    slack_card = order[4][1]
    assert timeline_event.deployment_id == DEPLOYMENT_ID
    assert timeline_event.timestamp is TIMESTAMP
    assert timeline_event.details["event_subtype"] == "EMERGENCY_STOP"
    assert telegram_card is result.operator_card
    assert slack_card is result.operator_card
    assert result.position_summary is summary
    assert result.timestamp is TIMESTAMP
    assert result.operator_card.timestamp is TIMESTAMP
    assert result.operator_card.deployment_id == DEPLOYMENT_ID
    assert result.operator_card.event_type == EventType.EMERGENCY_STOP
    assert result.operator_card.severity == Severity.CRITICAL
    assert result.operator_card.context["trigger_context"] == {"attempt": 2}
    assert result.success is True
    assert result.pause_successful is True
    assert result.alerts_sent is True
    assert result.error is None
    assert result.alert_result == AlertSendResult(
        success=True,
        channels_sent=[AlertChannel.TELEGRAM],
        channels_failed=[AlertChannel.SLACK],
        errors={AlertChannel.SLACK: "slack unavailable"},
    )
    assert "Emergency stop completed" in caplog.text


@pytest.mark.parametrize(
    ("pause_callback", "alert_result", "expected_error", "expected_pause"),
    [
        (Mock(return_value=False), _failed(AlertChannel.TELEGRAM, "down"), "Failed to pause strategy", False),
        (Mock(return_value=True), _failed(AlertChannel.TELEGRAM, "down"), "Failed to send alerts", True),
        (Mock(return_value=True), None, "Failed to send alerts", True),
    ],
)
def test_emergency_stop_preserves_failure_precedence(
    pause_callback: Mock,
    alert_result: AlertSendResult | None,
    expected_error: str,
    expected_pause: bool,
) -> None:
    alert_manager = Mock()
    manager = EmergencyManager(alert_manager=alert_manager, pause_callback=pause_callback)

    with (
        patch.object(manager, "_send_critical_alerts", return_value=alert_result),
        patch("almanak.framework.services.emergency_manager.add_event"),
    ):
        result = manager.emergency_stop(DEPLOYMENT_ID, "reason", CHAIN)

    assert result.success is False
    assert result.pause_successful is expected_pause
    assert result.alerts_sent is False
    assert result.error == expected_error


def test_emergency_stop_contains_pause_and_position_exceptions(caplog: pytest.LogCaptureFixture) -> None:
    pause_callback = Mock(side_effect=RuntimeError("pause backend down"))
    position_callback = Mock(side_effect=RuntimeError("position backend down"))
    alert_manager = Mock()
    manager = EmergencyManager(
        alert_manager=alert_manager,
        pause_callback=pause_callback,
        position_callback=position_callback,
    )

    with (
        patch.object(manager, "_send_critical_alerts", return_value=_sent(AlertChannel.TELEGRAM)),
        patch("almanak.framework.services.emergency_manager.add_event"),
        caplog.at_level(logging.ERROR),
    ):
        result = manager.emergency_stop(DEPLOYMENT_ID, "reason", CHAIN)

    assert result.success is False
    assert result.error == "Failed to pause strategy"
    assert result.position_summary.deployment_id == DEPLOYMENT_ID
    assert result.position_summary.chain == CHAIN
    assert result.position_summary.timestamp is result.timestamp
    assert "Error pausing strategy" in caplog.text
    assert "Error getting position summary" in caplog.text


def test_emergency_stop_without_callbacks_preserves_failed_completion(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with (
        patch("almanak.framework.services.emergency_manager.add_event"),
        caplog.at_level(logging.WARNING),
    ):
        result = EmergencyManager().emergency_stop(DEPLOYMENT_ID, "reason", CHAIN)

    assert result.success is False
    assert result.pause_successful is False
    assert result.alert_result is None
    assert result.alerts_sent is False
    assert result.error == "Failed to pause strategy"
    assert "No pause callback configured" in caplog.text


@pytest.mark.asyncio
async def test_emergency_stop_async_preserves_order_identity_and_complete_state() -> None:
    order: list[tuple[str, object]] = []
    summary = _position_summary()

    def pause(deployment_id: str) -> bool:
        order.append(("pause", deployment_id))
        return True

    def positions(deployment_id: str) -> FullPositionSummary:
        order.append(("positions", deployment_id))
        return summary

    async def telegram(card: object) -> AlertSendResult:
        order.append(("telegram", card))
        return _failed(AlertChannel.TELEGRAM, "telegram unavailable")

    async def slack(card: object) -> AlertSendResult:
        order.append(("slack", card))
        return _sent(AlertChannel.SLACK)

    alert_manager = Mock()
    alert_manager.send_direct_telegram_alert.side_effect = telegram
    alert_manager.send_direct_slack_alert.side_effect = slack
    manager = EmergencyManager(
        alert_manager=alert_manager,
        pause_callback=pause,
        position_callback=positions,
    )

    with (
        patch("almanak.framework.services.emergency_manager.datetime") as datetime_mock,
        patch(
            "almanak.framework.services.emergency_manager.add_event",
            side_effect=lambda event: order.append(("timeline", event)),
        ),
    ):
        datetime_mock.now.return_value = TIMESTAMP
        result = await manager.emergency_stop_async(
            deployment_id=DEPLOYMENT_ID,
            reason="health factor breached",
            chain=CHAIN,
            trigger_context={"attempt": 2},
        )

    assert [name for name, _ in order[:3]] == ["pause", "positions", "timeline"]
    assert {name for name, _ in order[3:]} == {"telegram", "slack"}
    assert all(card is result.operator_card for _, card in order[3:])
    assert result.position_summary is summary
    assert result.timestamp is TIMESTAMP
    assert result.operator_card.timestamp is TIMESTAMP
    assert result.operator_card.deployment_id == DEPLOYMENT_ID
    assert result.success is True
    assert result.pause_successful is True
    assert result.alerts_sent is True
    assert result.error is None
    assert result.alert_result == AlertSendResult(
        success=True,
        channels_sent=[AlertChannel.SLACK],
        channels_failed=[AlertChannel.TELEGRAM],
        errors={AlertChannel.TELEGRAM: "telegram unavailable"},
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("pause_callback", "alert_result", "expected_error", "expected_pause"),
    [
        (Mock(return_value=False), _failed(AlertChannel.SLACK, "down"), "Failed to pause strategy", False),
        (Mock(return_value=True), _failed(AlertChannel.SLACK, "down"), "Failed to send alerts", True),
        (Mock(return_value=True), None, "Failed to send alerts", True),
    ],
)
async def test_emergency_stop_async_preserves_failure_precedence(
    pause_callback: Mock,
    alert_result: AlertSendResult | None,
    expected_error: str,
    expected_pause: bool,
) -> None:
    alert_manager = Mock()
    manager = EmergencyManager(alert_manager=alert_manager, pause_callback=pause_callback)

    with (
        patch.object(manager, "_send_critical_alerts_async", new=AsyncMock(return_value=alert_result)),
        patch("almanak.framework.services.emergency_manager.add_event"),
    ):
        result = await manager.emergency_stop_async(DEPLOYMENT_ID, "reason", CHAIN)

    assert result.success is False
    assert result.pause_successful is expected_pause
    assert result.alerts_sent is False
    assert result.error == expected_error


@pytest.mark.asyncio
async def test_emergency_stop_async_contains_pause_and_position_exceptions(
    caplog: pytest.LogCaptureFixture,
) -> None:
    pause_callback = Mock(side_effect=RuntimeError("pause backend down"))
    position_callback = Mock(side_effect=RuntimeError("position backend down"))
    alert_manager = Mock()
    manager = EmergencyManager(
        alert_manager=alert_manager,
        pause_callback=pause_callback,
        position_callback=position_callback,
    )

    with (
        patch.object(
            manager,
            "_send_critical_alerts_async",
            new=AsyncMock(return_value=_sent(AlertChannel.SLACK)),
        ),
        patch("almanak.framework.services.emergency_manager.add_event"),
        caplog.at_level(logging.ERROR),
    ):
        result = await manager.emergency_stop_async(DEPLOYMENT_ID, "reason", CHAIN)

    assert result.success is False
    assert result.error == "Failed to pause strategy"
    assert result.position_summary.deployment_id == DEPLOYMENT_ID
    assert result.position_summary.chain == CHAIN
    assert result.position_summary.timestamp is result.timestamp
    assert "Error pausing strategy" in caplog.text
    assert "Error getting position summary" in caplog.text


@pytest.mark.asyncio
async def test_emergency_stop_async_without_callbacks_preserves_failed_completion(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with (
        patch("almanak.framework.services.emergency_manager.add_event"),
        caplog.at_level(logging.WARNING),
    ):
        result = await EmergencyManager().emergency_stop_async(DEPLOYMENT_ID, "reason", CHAIN)

    assert result.success is False
    assert result.pause_successful is False
    assert result.alert_result is None
    assert result.alerts_sent is False
    assert result.error == "Failed to pause strategy"
    assert "No pause callback configured" in caplog.text


@pytest.mark.parametrize(
    ("telegram_result", "slack_result", "expected"),
    [
        (
            _sent(AlertChannel.TELEGRAM),
            _sent(AlertChannel.SLACK),
            AlertSendResult(
                success=True,
                channels_sent=[AlertChannel.TELEGRAM, AlertChannel.SLACK],
            ),
        ),
        (
            _failed(AlertChannel.TELEGRAM, "telegram failed"),
            _failed(AlertChannel.SLACK, "slack failed"),
            AlertSendResult(
                success=False,
                channels_failed=[AlertChannel.TELEGRAM, AlertChannel.SLACK],
                errors={
                    AlertChannel.TELEGRAM: "telegram failed",
                    AlertChannel.SLACK: "slack failed",
                },
            ),
        ),
        (
            _sent(AlertChannel.TELEGRAM),
            _failed(AlertChannel.SLACK, "slack failed"),
            AlertSendResult(
                success=True,
                channels_sent=[AlertChannel.TELEGRAM],
                channels_failed=[AlertChannel.SLACK],
                errors={AlertChannel.SLACK: "slack failed"},
            ),
        ),
    ],
)
def test_send_critical_alerts_aggregates_channels_without_retrying_direct_senders(
    telegram_result: AlertSendResult,
    slack_result: AlertSendResult,
    expected: AlertSendResult,
    caplog: pytest.LogCaptureFixture,
) -> None:
    alert_manager = Mock()
    alert_manager.send_direct_telegram_alert_sync.return_value = telegram_result
    alert_manager.send_direct_slack_alert_sync.return_value = slack_result
    manager = EmergencyManager(alert_manager=alert_manager)
    card = _card()

    with caplog.at_level(logging.INFO):
        result = manager._send_critical_alerts(card)

    assert result == expected
    alert_manager.send_direct_telegram_alert_sync.assert_called_once_with(card)
    alert_manager.send_direct_slack_alert_sync.assert_called_once_with(card)
    assert telegram_result != result
    assert slack_result != result
    expected_log = "CRITICAL alerts sent" if expected.success else "Failed to send CRITICAL alerts"
    assert expected_log in caplog.text


def test_send_critical_alerts_records_each_exception_and_continues(
    caplog: pytest.LogCaptureFixture,
) -> None:
    alert_manager = Mock()
    alert_manager.send_direct_telegram_alert_sync.side_effect = RuntimeError("telegram exploded")
    alert_manager.send_direct_slack_alert_sync.side_effect = RuntimeError("slack exploded")
    manager = EmergencyManager(alert_manager=alert_manager)
    card = _card()

    with caplog.at_level(logging.ERROR):
        result = manager._send_critical_alerts(card)

    assert result == AlertSendResult(
        success=False,
        channels_failed=[AlertChannel.TELEGRAM, AlertChannel.SLACK],
        errors={
            AlertChannel.TELEGRAM: "telegram exploded",
            AlertChannel.SLACK: "slack exploded",
        },
    )
    alert_manager.send_direct_slack_alert_sync.assert_called_once_with(card)
    assert "Telegram alert exception" in caplog.text
    assert "Slack alert exception" in caplog.text


def test_send_critical_alerts_contains_malformed_channel_result(
    caplog: pytest.LogCaptureFixture,
) -> None:
    class MalformedResult:
        @property
        def success(self) -> bool:
            raise ValueError("bad result")

    alert_manager = Mock()
    alert_manager.send_direct_telegram_alert_sync.return_value = MalformedResult()
    manager = EmergencyManager(alert_manager=alert_manager)

    with caplog.at_level(logging.ERROR):
        result = manager._send_critical_alerts(_card())

    assert result == AlertSendResult(success=False, skipped_reason="bad result")
    alert_manager.send_direct_slack_alert_sync.assert_not_called()
    assert "Error sending CRITICAL alerts" in caplog.text


def test_send_critical_alerts_without_manager_returns_none(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.WARNING):
        result = EmergencyManager()._send_critical_alerts(_card())

    assert result is None
    assert "No alert manager configured" in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("telegram_result", "slack_result", "expected"),
    [
        (
            _sent(AlertChannel.TELEGRAM),
            _sent(AlertChannel.SLACK),
            AlertSendResult(
                success=True,
                channels_sent=[AlertChannel.TELEGRAM, AlertChannel.SLACK],
            ),
        ),
        (
            _failed(AlertChannel.TELEGRAM, "telegram failed"),
            _failed(AlertChannel.SLACK, "slack failed"),
            AlertSendResult(
                success=False,
                channels_failed=[AlertChannel.TELEGRAM, AlertChannel.SLACK],
                errors={
                    AlertChannel.TELEGRAM: "telegram failed",
                    AlertChannel.SLACK: "slack failed",
                },
            ),
        ),
        (
            _failed(AlertChannel.TELEGRAM, "telegram failed"),
            _sent(AlertChannel.SLACK),
            AlertSendResult(
                success=True,
                channels_sent=[AlertChannel.SLACK],
                channels_failed=[AlertChannel.TELEGRAM],
                errors={AlertChannel.TELEGRAM: "telegram failed"},
            ),
        ),
    ],
)
async def test_send_critical_alerts_async_aggregates_channels_without_retrying_direct_senders(
    telegram_result: AlertSendResult,
    slack_result: AlertSendResult,
    expected: AlertSendResult,
    caplog: pytest.LogCaptureFixture,
) -> None:
    alert_manager = Mock()
    alert_manager.send_direct_telegram_alert = AsyncMock(return_value=telegram_result)
    alert_manager.send_direct_slack_alert = AsyncMock(return_value=slack_result)
    manager = EmergencyManager(alert_manager=alert_manager)
    card = _card()

    with caplog.at_level(logging.INFO):
        result = await manager._send_critical_alerts_async(card)

    assert result == expected
    alert_manager.send_direct_telegram_alert.assert_awaited_once_with(card)
    alert_manager.send_direct_slack_alert.assert_awaited_once_with(card)
    expected_log = "CRITICAL alerts sent" if expected.success else "Failed to send CRITICAL alerts"
    assert expected_log in caplog.text


@pytest.mark.asyncio
async def test_send_critical_alerts_async_records_each_exception(
    caplog: pytest.LogCaptureFixture,
) -> None:
    alert_manager = Mock()
    alert_manager.send_direct_telegram_alert = AsyncMock(side_effect=RuntimeError("telegram exploded"))
    alert_manager.send_direct_slack_alert = AsyncMock(side_effect=RuntimeError("slack exploded"))
    manager = EmergencyManager(alert_manager=alert_manager)

    with caplog.at_level(logging.ERROR):
        result = await manager._send_critical_alerts_async(_card())

    assert result == AlertSendResult(
        success=False,
        channels_failed=[AlertChannel.TELEGRAM, AlertChannel.SLACK],
        errors={
            AlertChannel.TELEGRAM: "telegram exploded",
            AlertChannel.SLACK: "slack exploded",
        },
    )
    assert "Telegram alert exception" in caplog.text
    assert "Slack alert exception" in caplog.text


@pytest.mark.asyncio
async def test_send_critical_alerts_async_contains_setup_exception(
    caplog: pytest.LogCaptureFixture,
) -> None:
    alert_manager = Mock()
    alert_manager.send_direct_telegram_alert.side_effect = RuntimeError("failed to create coroutine")
    manager = EmergencyManager(alert_manager=alert_manager)

    with caplog.at_level(logging.ERROR):
        result = await manager._send_critical_alerts_async(_card())

    assert result == AlertSendResult(success=False, skipped_reason="failed to create coroutine")
    assert "Error sending CRITICAL alerts (async)" in caplog.text


@pytest.mark.asyncio
async def test_send_critical_alerts_async_without_manager_returns_none(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(logging.WARNING):
        result = await EmergencyManager()._send_critical_alerts_async(_card())

    assert result is None
    assert "No alert manager configured" in caplog.text


def test_zero_position_bounds_and_health_factor_remain_measured() -> None:
    summary = _position_summary()
    summary.lp_positions[0].range_upper = Decimal("0")
    summary.borrow_positions[0].health_factor = Decimal("0")
    card = EmergencyManager()._generate_emergency_card(
        DEPLOYMENT_ID,
        "liquidation threshold crossed",
        summary,
        TIMESTAMP,
    )

    data = summary.to_dict()
    assert data["lp_positions"][0]["range_lower"] == "0"
    assert data["lp_positions"][0]["range_upper"] == "0"
    assert data["borrow_positions"][0]["liquidation_price"] == "0"
    assert card.context["min_health_factor"] == "0"
    assert "health factor: 0.00" in card.risk_description
