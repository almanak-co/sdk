"""Almanak Strategy Framework v2.0 - Alert Channels

This module provides implementations for various notification channels
including Telegram, Slack, Email, and PagerDuty.
"""

from .slack import SlackChannel
from .telegram import TelegramChannel

__all__ = [
    "SlackChannel",
    "TelegramChannel",
    "WebhookChannel",
]


def __getattr__(name: str):
    if name == "WebhookChannel":
        from .webhook import WebhookChannel

        return WebhookChannel
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
