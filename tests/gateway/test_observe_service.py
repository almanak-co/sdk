"""Public-contract tests for the gateway ObserveService."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import grpc
import pytest

from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.observe_service import ObserveServiceServicer

_STORE_PATCH = "almanak.gateway.services.observe_service.get_timeline_store"
_SLACK_URL = "https://hooks.slack.test/services/secret-webhook"
_TELEGRAM_TOKEN = "secret-telegram-token"
_DATABASE_URL = "postgresql://user:secret-password@db.test/almanak"


@pytest.fixture
def settings() -> GatewaySettings:
    return GatewaySettings(
        slack_webhook_url=_SLACK_URL,
        telegram_bot_token=_TELEGRAM_TOKEN,
        telegram_chat_id="-100123",
        database_url=_DATABASE_URL,
    )


@pytest.fixture
def service(settings: GatewaySettings) -> ObserveServiceServicer:
    return ObserveServiceServicer(settings)


@pytest.fixture
def context() -> MagicMock:
    return MagicMock(spec=grpc.aio.ServicerContext)


def _session_returning(status: int, body: str = "") -> tuple[MagicMock, MagicMock]:
    response = MagicMock(status=status)
    response.text = AsyncMock(return_value=body)
    response_context = MagicMock()
    response_context.__aenter__ = AsyncMock(return_value=response)
    response_context.__aexit__ = AsyncMock(return_value=None)
    session = MagicMock()
    session.post = MagicMock(return_value=response_context)
    return session, response


class TestRecordTimelineEvent:
    @pytest.mark.parametrize(
        ("timeline_request", "detail", "error"),
        [
            (
                gateway_pb2.RecordTimelineEventRequest(event_type="TRADE"),
                "deployment_id is required",
                "deployment_id is required",
            ),
            (
                gateway_pb2.RecordTimelineEventRequest(deployment_id="deployment:one"),
                "event_type is required",
                "event_type is required",
            ),
            (
                gateway_pb2.RecordTimelineEventRequest(
                    deployment_id="deployment:one", event_type="TRADE", timestamp=-1
                ),
                "timestamp must be non-negative (use 0 to mean now)",
                "timestamp must be non-negative",
            ),
            (
                gateway_pb2.RecordTimelineEventRequest(
                    deployment_id="deployment:one", event_type="TRADE", timestamp=2**63 - 1
                ),
                "timestamp out of range",
                "timestamp out of range",
            ),
        ],
    )
    @pytest.mark.asyncio
    async def test_invalid_request_sets_exact_status_and_error(
        self,
        service: ObserveServiceServicer,
        context: MagicMock,
        timeline_request: gateway_pb2.RecordTimelineEventRequest,
        detail: str,
        error: str,
    ) -> None:
        with patch(_STORE_PATCH) as get_store:
            response = await service.RecordTimelineEvent(timeline_request, context)

        assert response.success is False
        assert response.error == error
        context.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)
        context.set_details.assert_called_once_with(detail)
        get_store.assert_not_called()

    @pytest.mark.asyncio
    async def test_rich_event_preserves_payload_and_client_timestamp(
        self, service: ObserveServiceServicer, context: MagicMock
    ) -> None:
        store = MagicMock()
        request = gateway_pb2.RecordTimelineEventRequest(
            deployment_id="deployment:one",
            event_type="POSITION_OPENED",
            description="Opened LP",
            tx_hash="0xabc",
            chain="arbitrum",
            details_json='{"amount": "12.5", "nested": {"ok": true}}',
            timestamp=1_767_225_600,
            cycle_id="cycle-7",
            phase="EXECUTE",
            related_ledger_entry_id="ledger-9",
        )

        with (
            patch(_STORE_PATCH, return_value=store),
            patch("almanak.gateway.services.observe_service.uuid4", return_value="event-7"),
        ):
            response = await service.RecordTimelineEvent(request, context)

        assert response == gateway_pb2.RecordTimelineEventResponse(success=True, event_id="event-7")
        event = store.add_event.call_args.args[0]
        assert event.event_id == "event-7"
        assert event.deployment_id == "deployment:one"
        assert event.timestamp == datetime(2026, 1, 1, tzinfo=UTC)
        assert event.event_type == "POSITION_OPENED"
        assert event.description == "Opened LP"
        assert event.tx_hash == "0xabc"
        assert event.chain == "arbitrum"
        assert event.details == {"amount": "12.5", "nested": {"ok": True}}
        assert event.cycle_id == "cycle-7"
        assert event.phase == "EXECUTE"
        assert event.related_ledger_entry_id == "ledger-9"
        context.set_code.assert_not_called()

    @pytest.mark.asyncio
    async def test_invalid_details_are_ignored_and_defaults_are_preserved(
        self, service: ObserveServiceServicer, context: MagicMock, caplog: pytest.LogCaptureFixture
    ) -> None:
        store = MagicMock()
        before = datetime.now(UTC)
        request = gateway_pb2.RecordTimelineEventRequest(
            deployment_id="deployment:one",
            event_type="STATE_CHANGE",
            details_json="not-json",
        )

        with patch(_STORE_PATCH, return_value=store), caplog.at_level("WARNING"):
            response = await service.RecordTimelineEvent(request, context)
        after = datetime.now(UTC)

        assert response.success is True
        event = store.add_event.call_args.args[0]
        assert before <= event.timestamp <= after
        assert event.details == {}
        assert event.tx_hash is None
        assert event.chain is None
        assert event.cycle_id == ""
        assert event.phase == ""
        assert event.related_ledger_entry_id == ""
        assert "Invalid details_json" in caplog.text

    @pytest.mark.asyncio
    async def test_storage_error_preserves_status_without_leaking_database_url(
        self, service: ObserveServiceServicer, context: MagicMock, caplog: pytest.LogCaptureFixture
    ) -> None:
        store = MagicMock()
        store.add_event.side_effect = RuntimeError(f"database unavailable at {_DATABASE_URL}")
        request = gateway_pb2.RecordTimelineEventRequest(deployment_id="deployment:one", event_type="ERROR")

        with patch(_STORE_PATCH, return_value=store), caplog.at_level("ERROR"):
            response = await service.RecordTimelineEvent(request, context)

        assert response.success is False
        assert response.error == "Failed to record timeline event: database unavailable at ***"
        context.set_code.assert_called_once_with(grpc.StatusCode.INTERNAL)
        context.set_details.assert_called_once_with(response.error)
        assert _DATABASE_URL not in caplog.text


class TestAlert:
    @pytest.mark.asyncio
    async def test_missing_message_is_invalid_argument(
        self, service: ObserveServiceServicer, context: MagicMock
    ) -> None:
        response = await service.Alert(gateway_pb2.AlertRequest(channel="slack"), context)

        assert response == gateway_pb2.AlertResponse(success=False, error="message required")
        context.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)
        context.set_details.assert_called_once_with("message is required")

    @pytest.mark.parametrize(
        ("channel", "method_name"),
        [("SLACK", "_send_slack_alert"), ("TELEGRAM", "_send_telegram_alert")],
    )
    @pytest.mark.asyncio
    async def test_explicit_channel_routes_normalized_request_and_preserves_result(
        self,
        service: ObserveServiceServicer,
        context: MagicMock,
        channel: str,
        method_name: str,
    ) -> None:
        sender = AsyncMock(return_value=(False, "upstream rejected"))
        request = gateway_pb2.AlertRequest(
            channel=channel,
            message="risk high",
            severity="WARNING",
            deployment_id="deployment:one",
            metadata={"position": "lp-1"},
        )

        with (
            patch.object(service, method_name, sender),
            patch("almanak.gateway.services.observe_service.uuid4", return_value="alert-7"),
        ):
            response = await service.Alert(request, context)

        sender.assert_awaited_once_with("risk high", "warning", "deployment:one", {"position": "lp-1"})
        assert response == gateway_pb2.AlertResponse(success=False, error="upstream rejected", alert_id="alert-alert-7")

    @pytest.mark.asyncio
    async def test_unspecified_channel_fans_out_and_aggregates_failures(
        self, service: ObserveServiceServicer, context: MagicMock
    ) -> None:
        slack = AsyncMock(return_value=(False, "slack down"))
        telegram = AsyncMock(return_value=(False, "telegram down"))

        with patch.object(service, "_send_slack_alert", slack), patch.object(service, "_send_telegram_alert", telegram):
            response = await service.Alert(
                gateway_pb2.AlertRequest(
                    channel="all",
                    message="risk high",
                    severity="critical",
                    deployment_id="deployment:one",
                ),
                context,
            )

        assert response.success is False
        assert response.error == "Slack: slack down; Telegram: telegram down"
        slack.assert_awaited_once()
        telegram.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_unspecified_channel_succeeds_when_all_configured_channels_succeed(
        self, service: ObserveServiceServicer, context: MagicMock
    ) -> None:
        with (
            patch.object(service, "_send_slack_alert", AsyncMock(return_value=(True, None))),
            patch.object(service, "_send_telegram_alert", AsyncMock(return_value=(True, None))),
        ):
            response = await service.Alert(
                gateway_pb2.AlertRequest(message="healthy", severity="info", deployment_id="deployment:one"),
                context,
            )

        assert response.success is True
        assert response.error == ""

    @pytest.mark.asyncio
    async def test_no_configured_channels_falls_back_to_logging(
        self, context: MagicMock, caplog: pytest.LogCaptureFixture
    ) -> None:
        service = ObserveServiceServicer(GatewaySettings())

        with caplog.at_level("WARNING"):
            response = await service.Alert(
                gateway_pb2.AlertRequest(
                    channel="all",
                    message="operator notice",
                    severity="warning",
                    deployment_id="deployment:one",
                ),
                context,
            )

        assert response.success is True
        assert response.alert_id.startswith("alert-")
        assert "Alert [warning] from deployment:one: operator notice" in caplog.text

    @pytest.mark.asyncio
    async def test_unexpected_sender_error_is_internal_and_redacted(
        self,
        service: ObserveServiceServicer,
        context: MagicMock,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        sender = AsyncMock(side_effect=RuntimeError(f"failed POST {_SLACK_URL}"))

        with patch.object(service, "_send_slack_alert", sender), caplog.at_level("ERROR"):
            response = await service.Alert(gateway_pb2.AlertRequest(channel="slack", message="risk high"), context)

        assert response == gateway_pb2.AlertResponse(success=False, error="failed POST ***")
        context.set_code.assert_called_once_with(grpc.StatusCode.INTERNAL)
        context.set_details.assert_called_once_with("failed POST ***")
        assert _SLACK_URL not in caplog.text


class TestSendSlackAlert:
    @pytest.mark.asyncio
    async def test_unconfigured_channel_fails_without_http(self) -> None:
        service = ObserveServiceServicer(GatewaySettings())
        service._get_session = AsyncMock()

        result = await service._send_slack_alert("message", "info", "deployment:one", {})

        assert result == (False, "Slack webhook not configured")
        service._get_session.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_whitespace_only_webhook_fails_without_http(self) -> None:
        service = ObserveServiceServicer(GatewaySettings(slack_webhook_url="   "))
        service._get_session = AsyncMock()

        result = await service._send_slack_alert("message", "info", "deployment:one", {})

        assert result == (False, "Slack webhook URL is empty")
        service._get_session.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_success_posts_exact_escaped_block_kit_payload(self, service: ObserveServiceServicer) -> None:
        session, _ = _session_returning(200)
        service._get_session = AsyncMock(return_value=session)

        result = await service._send_slack_alert(
            "risk <high> & rising",
            "warning",
            "deployment:<one>&",
            {"position<": "LP&1"},
        )

        assert result == (True, None)
        session.post.assert_called_once()
        assert session.post.call_args.args == (_SLACK_URL,)
        assert session.post.call_args.kwargs["headers"] == {"Content-Type": "application/json"}
        payload = session.post.call_args.kwargs["json"]
        assert payload["blocks"] == [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": ":warning: WARNING Alert",
                    "emoji": True,
                },
            },
            {
                "type": "section",
                "fields": [
                    {
                        "type": "mrkdwn",
                        "text": "*Strategy:*\ndeployment:&lt;one&gt;&amp;",
                    },
                    {"type": "mrkdwn", "text": "*Severity:*\nWARNING"},
                ],
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*Message:*\nrisk &lt;high&gt; &amp; rising",
                },
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*Context:*\n- position&lt;: LP&amp;1",
                },
            },
        ]
        assert payload["attachments"] == [
            {
                "color": "#ffcc00",
                "fallback": "WARNING: deployment:&lt;one&gt;&amp; - risk &lt;high&gt; &amp; rising",
            }
        ]

    @pytest.mark.asyncio
    async def test_unknown_severity_and_http_error_preserve_defaults_and_redact_body(
        self, service: ObserveServiceServicer
    ) -> None:
        session, _ = _session_returning(503, f"webhook {_SLACK_URL} rejected")
        service._get_session = AsyncMock(return_value=session)

        success, error = await service._send_slack_alert("message", "unknown", "deployment:one", {})

        assert success is False
        assert error == "Slack API error: HTTP 503: webhook *** rejected"
        payload = session.post.call_args.kwargs["json"]
        assert payload["blocks"][0]["text"]["text"] == ":question: UNKNOWN Alert"
        assert payload["attachments"][0]["color"] == "#808080"
        assert len(payload["blocks"]) == 3

    @pytest.mark.asyncio
    async def test_transport_error_is_returned_without_webhook_leak(self) -> None:
        service = ObserveServiceServicer(GatewaySettings(slack_webhook_url=_SLACK_URL))
        session = MagicMock()
        session.post.side_effect = aiohttp.ClientError(f"cannot connect to {_SLACK_URL}")
        service._get_session = AsyncMock(return_value=session)

        result = await service._send_slack_alert("message", "info", "deployment:one", {})

        assert result == (False, "Slack request failed: cannot connect to ***")


class TestSendTelegramAlert:
    @pytest.mark.asyncio
    async def test_unconfigured_channel_fails_without_http(self) -> None:
        service = ObserveServiceServicer(GatewaySettings())
        service._get_session = AsyncMock()

        result = await service._send_telegram_alert("message", "info", "deployment:one", {})

        assert result == (False, "Telegram not configured")
        service._get_session.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_success_posts_exact_escaped_html_payload(self, service: ObserveServiceServicer) -> None:
        session, _ = _session_returning(200)
        service._get_session = AsyncMock(return_value=session)

        result = await service._send_telegram_alert(
            "risk <high> & rising",
            "critical",
            "deployment:<one>&",
            {"position<": "LP&1"},
        )

        assert result == (True, None)
        session.post.assert_called_once_with(
            f"https://api.telegram.org/bot{_TELEGRAM_TOKEN}/sendMessage",
            json={
                "chat_id": "-100123",
                "text": (
                    "🔴 <b>CRITICAL Alert</b>\n"
                    "<b>Strategy:</b> <code>deployment:&lt;one&gt;&amp;</code>\n\n"
                    "risk &lt;high&gt; &amp; rising\n\n"
                    "<b>Context:</b>\n- position&lt;: LP&amp;1"
                ),
                "parse_mode": "HTML",
            },
        )

    @pytest.mark.asyncio
    async def test_unknown_severity_and_http_error_preserve_defaults_and_redact_body(
        self, service: ObserveServiceServicer
    ) -> None:
        session, _ = _session_returning(429, f"token {_TELEGRAM_TOKEN} rejected")
        service._get_session = AsyncMock(return_value=session)

        success, error = await service._send_telegram_alert("message", "unknown", "deployment:one", {})

        assert success is False
        assert error == "Telegram API error: HTTP 429: token *** rejected"
        payload = session.post.call_args.kwargs["json"]
        assert payload["text"].startswith("❓ <b>UNKNOWN Alert</b>")
        assert "<b>Context:</b>" not in payload["text"]

    @pytest.mark.asyncio
    async def test_transport_error_is_returned_without_bot_token_leak(self, service: ObserveServiceServicer) -> None:
        session = MagicMock()
        session.post.side_effect = TimeoutError(f"timed out for bot {_TELEGRAM_TOKEN}")
        service._get_session = AsyncMock(return_value=session)

        result = await service._send_telegram_alert("message", "info", "deployment:one", {})

        assert result == (False, "Telegram request failed: timed out for bot ***")
