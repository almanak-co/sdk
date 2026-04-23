"""Internal helpers extracted from ``AlertManager.send_alert``.

These helpers decompose the send-alert pipeline into phase-focused functions
so the orchestrator stays readable (CC <= 12) while preserving byte-for-byte
per-channel payload formatting and suppression precedence semantics.

Public API of ``AlertManager`` is unchanged. These helpers are module-private
(underscore-prefixed module) and are called from ``AlertManager.send_alert``.

Phases:
1. Rule matching / severity routing (done in ``AlertManager._find_matching_rules``)
2. Suppression gates (quiet-hours + cooldown) -> ``collect_channels_to_send``
3. Per-channel dispatch (Slack/Telegram + unsupported no-ops) ->
   ``dispatch_telegram`` / ``dispatch_slack`` / ``dispatch_unsupported``
4. State persistence (cooldown record) -> ``record_cooldowns_for_sent_rules``
5. Final summary logging + success bit -> ``finalize_result``

No channel payload formatting lives here - each channel owns its own
payload shape inside ``SlackChannel`` / ``TelegramChannel``. These helpers
only orchestrate.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from ..models.operator_card import OperatorCard
from .alert_config import AlertChannel, AlertRule

if TYPE_CHECKING:  # pragma: no cover - imports for typing only
    from .alert_manager import AlertManager, AlertSendResult, CooldownTracker
    from .channels import SlackChannel, TelegramChannel

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Phase 2: suppression gates (quiet hours + cooldown)
# ---------------------------------------------------------------------------


def collect_channels_to_send(
    manager: AlertManager,
    matching_rules: list[AlertRule],
    card: OperatorCard,
) -> set[AlertChannel]:
    """Evaluate suppression gates per rule and collect channels to fan out to.

    Precedence preserved from the original ``send_alert`` body:
        global-disabled > quiet-hours > cooldown > rule channels

    For each matching rule that survives the gates, its channels are added to
    the returned set. Per-rule skip reasons are logged at DEBUG.

    Args:
        manager: the AlertManager instance (used to call _should_send_alert)
        matching_rules: output of ``_find_matching_rules``
        card: the OperatorCard being sent (for strategy_id / severity / logging)

    Returns:
        Deduplicated set of channels that should receive this alert.
    """
    channels_to_send: set[AlertChannel] = set()
    for rule in matching_rules:
        should_send, skip_reason = manager._should_send_alert(
            rule=rule,
            strategy_id=card.strategy_id,
            severity=card.severity,
        )
        if should_send:
            for channel in rule.channels:
                channels_to_send.add(channel)
        else:
            logger.debug(f"Rule {rule.condition.value} skipped for {card.strategy_id}: {skip_reason}")
    return channels_to_send


# ---------------------------------------------------------------------------
# Phase 3: per-channel dispatchers
# ---------------------------------------------------------------------------


async def dispatch_telegram(
    telegram_channel: TelegramChannel | None,
    card: OperatorCard,
    result: AlertSendResult,
) -> None:
    """Dispatch alert to Telegram, mutating ``result`` with outcome.

    Preserves the exact error strings, log messages and failure semantics
    from the original in-line implementation - operators grep on these.
    """
    channel = AlertChannel.TELEGRAM
    if telegram_channel is None:
        result.channels_failed.append(channel)
        result.errors[channel] = "Telegram channel not configured"
        logger.warning(f"Telegram alert requested but channel not configured for {card.strategy_id}")
        return

    try:
        send_result = await telegram_channel.send_alert(card)
    except Exception as e:  # noqa: BLE001 - intentionally broad, operator-facing
        result.channels_failed.append(channel)
        result.errors[channel] = str(e)
        logger.exception(f"Exception sending Telegram alert for {card.strategy_id}: {e}")
        return

    if send_result.success:
        result.channels_sent.append(channel)
        logger.info(f"Telegram alert sent for {card.strategy_id} (severity={card.severity.value})")
    else:
        result.channels_failed.append(channel)
        result.errors[channel] = send_result.error or "Unknown error"
        logger.error(f"Telegram alert failed for {card.strategy_id}: {send_result.error}")


async def dispatch_slack(
    slack_channel: SlackChannel | None,
    card: OperatorCard,
    result: AlertSendResult,
) -> None:
    """Dispatch alert to Slack, mutating ``result`` with outcome.

    Preserves exact log strings (including thread-ts suffix) and failure
    semantics from the original inline path.
    """
    channel = AlertChannel.SLACK
    if slack_channel is None:
        result.channels_failed.append(channel)
        result.errors[channel] = "Slack channel not configured"
        logger.warning(f"Slack alert requested but channel not configured for {card.strategy_id}")
        return

    try:
        slack_send_result = await slack_channel.send_alert(card)
    except Exception as e:  # noqa: BLE001
        result.channels_failed.append(channel)
        result.errors[channel] = str(e)
        logger.exception(f"Exception sending Slack alert for {card.strategy_id}: {e}")
        return

    if slack_send_result.success:
        result.channels_sent.append(channel)
        thread_info = ""
        if slack_send_result.thread_ts:
            thread_info = f", thread_ts={slack_send_result.thread_ts}"
        logger.info(f"Slack alert sent for {card.strategy_id} (severity={card.severity.value}{thread_info})")
    else:
        result.channels_failed.append(channel)
        result.errors[channel] = slack_send_result.error or "Unknown error"
        logger.error(f"Slack alert failed for {card.strategy_id}: {slack_send_result.error}")


def dispatch_unsupported(channel: AlertChannel, card: OperatorCard) -> None:
    """No-op dispatcher for channels not yet implemented (EMAIL, PAGERDUTY).

    Matches the original inline log shape so ops scraping these lines
    continues to work.
    """
    logger.debug(f"Channel {channel.value} not yet implemented, skipping for {card.strategy_id}")


async def dispatch_to_channels(
    manager: AlertManager,
    channels_to_send: set[AlertChannel],
    card: OperatorCard,
    result: AlertSendResult,
) -> None:
    """Fan out the alert to each configured channel.

    Delegates to per-channel dispatchers and leaves unsupported channels
    as silent no-ops (matching legacy behavior).
    """
    for channel in channels_to_send:
        if channel == AlertChannel.TELEGRAM:
            await dispatch_telegram(manager._telegram_channel, card, result)
        elif channel == AlertChannel.SLACK:
            await dispatch_slack(manager._slack_channel, card, result)
        elif channel in (AlertChannel.EMAIL, AlertChannel.PAGERDUTY):
            dispatch_unsupported(channel, card)


# ---------------------------------------------------------------------------
# Phase 4: state persistence (cooldown recording)
# ---------------------------------------------------------------------------


def record_cooldowns_for_sent_rules(
    cooldown_tracker: CooldownTracker,
    matching_rules: list[AlertRule],
    result: AlertSendResult,
    strategy_id: str,
) -> None:
    """Record cooldown for each rule whose channels successfully sent.

    Semantics preserved: a rule is considered "sent" if any of its channels
    appears in ``result.channels_sent``. This is what gates the next fire.
    """
    if not result.channels_sent:
        return
    for rule in matching_rules:
        if any(ch in result.channels_sent for ch in rule.channels):
            cooldown_tracker.record_alert(
                strategy_id=strategy_id,
                condition=rule.condition,
            )


# ---------------------------------------------------------------------------
# Phase 5: final result bookkeeping + summary log
# ---------------------------------------------------------------------------


def finalize_result(result: AlertSendResult, strategy_id: str) -> AlertSendResult:
    """Set ``success`` from ``channels_sent`` and emit the summary log line.

    Log shapes preserved byte-for-byte from the legacy in-line block.
    """
    result.success = len(result.channels_sent) > 0
    if result.success:
        logger.info(
            f"Alert sent for {strategy_id}: "
            f"channels_sent={[c.value for c in result.channels_sent]}, "
            f"channels_failed={[c.value for c in result.channels_failed]}"
        )
    else:
        logger.warning(
            f"Alert failed for {strategy_id}: errors={result.errors}, skipped_reason={result.skipped_reason}"
        )
    return result
