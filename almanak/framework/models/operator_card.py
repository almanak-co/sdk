"""Operator Card model for structured, actionable information about strategy issues.

The OperatorCard is the primary data structure for communicating strategy state
and issues to operators. It provides all context needed to understand and act on
any situation requiring attention.
"""

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any

from .actions import AvailableAction, SuggestedAction
from .stuck_reason import StuckReason


class EventType(StrEnum):
    """Type of event that triggered the operator card."""

    STUCK = "STUCK"
    ERROR = "ERROR"
    ALERT = "ALERT"
    WARNING = "WARNING"
    EMERGENCY_STOP = "EMERGENCY_STOP"


class Severity(StrEnum):
    """Severity level of the issue."""

    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


@dataclass
class PositionSummary:
    """Summary of the strategy's current position and exposure."""

    # Current funds in USD
    total_value_usd: Decimal
    available_balance_usd: Decimal

    # Exposure breakdown
    lp_value_usd: Decimal = Decimal("0")
    borrowed_value_usd: Decimal = Decimal("0")
    collateral_value_usd: Decimal = Decimal("0")

    # Token balances
    token_balances: dict[str, Decimal] = field(default_factory=dict)

    # LP position details (if applicable)
    lp_positions: list[dict[str, Any]] = field(default_factory=list)

    # Health metrics
    health_factor: Decimal | None = None
    leverage: Decimal | None = None


@dataclass
class AutoRemediation:
    """Configuration for automatic remediation of issues."""

    enabled: bool
    action: AvailableAction
    trigger_after_seconds: int
    max_attempts: int = 3
    current_attempt: int = 0
    last_attempt_at: datetime | None = None
    scheduled_at: datetime | None = None


@dataclass
class OperatorCard:
    """Structured, actionable information about a strategy issue.

    The OperatorCard provides operators with all the context they need to
    understand what happened, what's at risk, and what actions they can take.

    Attributes:
        strategy_id: Unique identifier for the strategy
        timestamp: When this card was generated
        event_type: Type of event (STUCK, ERROR, ALERT, WARNING)
        reason: Classification of why this happened (StuckReason enum)
        context: Additional context about the issue
        severity: Severity level (LOW, MEDIUM, HIGH, CRITICAL)
        position_summary: Current funds and exposure information
        risk_description: Human-readable description of what's at risk
        suggested_actions: List of suggested actions with descriptions
        auto_remediation: Optional auto-remediation configuration
        available_actions: List of available actions for this situation
    """

    strategy_id: str
    timestamp: datetime
    event_type: EventType
    reason: StuckReason
    context: dict[str, Any]
    severity: Severity
    position_summary: PositionSummary
    risk_description: str
    suggested_actions: list[SuggestedAction]
    available_actions: list[AvailableAction]
    auto_remediation: AutoRemediation | None = None

    def __post_init__(self) -> None:
        """Validate the operator card after initialization."""
        if not self.strategy_id:
            raise ValueError("strategy_id is required")
        if not self.suggested_actions:
            raise ValueError("At least one suggested action is required")
        if not self.available_actions:
            raise ValueError("At least one available action is required")

    @property
    def is_critical(self) -> bool:
        """Check if this is a critical issue requiring immediate attention."""
        return self.severity == Severity.CRITICAL

    @property
    def has_auto_remediation(self) -> bool:
        """Check if auto-remediation is configured and enabled."""
        return self.auto_remediation is not None and self.auto_remediation.enabled

    @property
    def recommended_action(self) -> SuggestedAction | None:
        """Get the recommended action, if any."""
        for action in self.suggested_actions:
            if action.is_recommended:
                return action
        # Return highest priority action if no explicit recommendation
        if self.suggested_actions:
            return min(self.suggested_actions, key=lambda a: a.priority)
        return None

    def to_dict(self) -> dict[str, Any]:
        """Convert the operator card to a dictionary for serialization."""
        return {
            "strategy_id": self.strategy_id,
            "timestamp": self.timestamp.isoformat(),
            "event_type": self.event_type.value,
            "reason": self.reason.value,
            "context": self.context,
            "severity": self.severity.value,
            "position_summary": {
                "total_value_usd": str(self.position_summary.total_value_usd),
                "available_balance_usd": str(self.position_summary.available_balance_usd),
                "lp_value_usd": str(self.position_summary.lp_value_usd),
                "borrowed_value_usd": str(self.position_summary.borrowed_value_usd),
                "collateral_value_usd": str(self.position_summary.collateral_value_usd),
                "token_balances": {k: str(v) for k, v in self.position_summary.token_balances.items()},
                "lp_positions": self.position_summary.lp_positions,
                "health_factor": str(self.position_summary.health_factor)
                if self.position_summary.health_factor
                else None,
                "leverage": str(self.position_summary.leverage) if self.position_summary.leverage else None,
            },
            "risk_description": self.risk_description,
            "suggested_actions": [
                {
                    "action": a.action.value,
                    "description": a.description,
                    "priority": a.priority,
                    "params": a.params,
                    "is_recommended": a.is_recommended,
                }
                for a in self.suggested_actions
            ],
            "available_actions": [a.value for a in self.available_actions],
            "auto_remediation": {
                "enabled": self.auto_remediation.enabled,
                "action": self.auto_remediation.action.value,
                "trigger_after_seconds": self.auto_remediation.trigger_after_seconds,
                "max_attempts": self.auto_remediation.max_attempts,
                "current_attempt": self.auto_remediation.current_attempt,
                "last_attempt_at": self.auto_remediation.last_attempt_at.isoformat()
                if self.auto_remediation.last_attempt_at
                else None,
                "scheduled_at": self.auto_remediation.scheduled_at.isoformat()
                if self.auto_remediation.scheduled_at
                else None,
            }
            if self.auto_remediation
            else None,
        }
