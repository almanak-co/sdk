"""Almanak Strategy Framework v2.0 - Alerting Module"""

from .alert_config import (
    AlertChannel,
    AlertCondition,
    AlertConfig,
    AlertRule,
    TimeRange,
)
from .alert_manager import AlertManager, AlertSendResult, CooldownTracker
from .channels import SlackChannel, TelegramChannel
from .escalation import (
    EscalationLevel,
    EscalationPolicy,
    EscalationResult,
    EscalationState,
    EscalationStatus,
)
from .gateway_alert_manager import GatewayAlertManager, GatewayAlertResult

__all__ = [
    "AlertChannel",
    "AlertCondition",
    "AlertRule",
    "AlertConfig",
    "TimeRange",
    "AlertManager",
    "AlertSendResult",
    "CooldownTracker",
    "SlackChannel",
    "TelegramChannel",
    "EscalationLevel",
    "EscalationPolicy",
    "EscalationResult",
    "EscalationState",
    "EscalationStatus",
    "GatewayAlertManager",
    "GatewayAlertResult",
]
