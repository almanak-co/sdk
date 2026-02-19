"""Strategy Base Class with hot-reload configuration support.

This module provides the StrategyBase class that all strategies should inherit from.
It implements:
- Hot-reload configuration updates with validation
- RiskGuard integration for safety checks
- Atomic config application with persistence
- CONFIG_UPDATED event emission
- Operator notifications on config changes

Usage:
    from almanak.framework.strategies.base import StrategyBase

    class MyStrategy(StrategyBase):
        def __init__(self, config):
            super().__init__(config)
            # ... strategy initialization

        def run(self):
            # ... strategy logic
            pass
"""

import logging
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, TypeVar

from ..api.timeline import TimelineEvent, TimelineEventType, add_event
from ..models.actions import AvailableAction, SuggestedAction
from ..models.hot_reload_config import ConfigUpdateResult, HotReloadableConfig
from ..models.operator_card import EventType, OperatorCard, PositionSummary, Severity
from ..models.stuck_reason import StuckReason

logger = logging.getLogger(__name__)


# Type variable for config types
ConfigT = TypeVar("ConfigT", bound=HotReloadableConfig)


@dataclass
class RiskGuardGuidance:
    """Human-readable guidance for a Risk Guard check failure.

    Provides operators with clear explanations of what failed,
    what the current limits are, and how to proceed.

    Attributes:
        field_name: The configuration field that failed validation
        limit_name: Human-readable name of the limit (e.g., "Maximum Slippage")
        requested_value: The value the operator tried to set
        limit_value: The maximum/minimum allowed value
        explanation: Human-readable explanation of what the limit is for
        suggestion: Actionable suggestion for how to proceed
    """

    field_name: str
    limit_name: str
    requested_value: Decimal
    limit_value: Decimal
    explanation: str
    suggestion: str

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "field_name": self.field_name,
            "limit_name": self.limit_name,
            "requested_value": str(self.requested_value),
            "limit_value": str(self.limit_value),
            "explanation": self.explanation,
            "suggestion": self.suggestion,
        }

    def format_message(self) -> str:
        """Format as a user-friendly message."""
        return (
            f"**{self.limit_name}**\n"
            f"• Requested: {self.requested_value}\n"
            f"• Limit: {self.limit_value}\n"
            f"• {self.explanation}\n"
            f"• **Suggestion:** {self.suggestion}"
        )


@dataclass
class RiskGuardResult:
    """Result of a RiskGuard validation check.

    Attributes:
        passed: Whether the validation passed
        violations: List of violated risk limits (technical descriptions)
        max_allowed_values: Dict of field -> max allowed value for violations
        guidance: List of human-readable guidance for each failed check
    """

    passed: bool
    violations: list[str] = field(default_factory=list)
    max_allowed_values: dict[str, Decimal] = field(default_factory=dict)
    guidance: list[RiskGuardGuidance] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "passed": self.passed,
            "violations": self.violations,
            "max_allowed_values": {k: str(v) for k, v in self.max_allowed_values.items()},
            "guidance": [g.to_dict() for g in self.guidance],
        }

    def format_guidance(self) -> str:
        """Format all guidance as a user-friendly message."""
        if not self.guidance:
            return ""
        return "\n\n".join(g.format_message() for g in self.guidance)


@dataclass
class RiskGuardConfig:
    """Configuration for RiskGuard validation.

    Defines the maximum allowed values for risk-sensitive parameters.
    These limits cannot be exceeded through hot-reload updates.

    Attributes:
        max_slippage_limit: Maximum allowed slippage (e.g., 0.05 = 5%)
        max_leverage_limit: Maximum allowed leverage (e.g., 10x)
        max_daily_loss_limit_usd: Maximum allowed daily loss limit
        min_health_factor_floor: Minimum allowed health factor setting
    """

    max_slippage_limit: Decimal = Decimal("0.1")  # 10% max slippage
    max_leverage_limit: Decimal = Decimal("10")  # 10x max leverage
    max_daily_loss_limit_usd: Decimal = Decimal("100000")  # $100k max daily loss
    min_health_factor_floor: Decimal = Decimal("1.05")  # Min health factor floor


class RiskGuard:
    """Validates configuration changes against risk limits.

    RiskGuard ensures that configuration updates cannot bypass
    critical risk limits defined by the operator or system.
    It provides human-readable guidance when validation fails.
    """

    # Human-readable explanations for each risk limit
    LIMIT_EXPLANATIONS: dict[str, dict[str, str]] = {
        "max_slippage": {
            "limit_name": "Maximum Slippage",
            "explanation": (
                "Slippage is the difference between expected and actual trade price. "
                "High slippage can result in significant losses on trades, especially "
                "in volatile markets or with large trade sizes."
            ),
            "suggestion": (
                "Reduce the requested slippage value. If you need higher slippage for "
                "specific market conditions, consider splitting trades into smaller sizes "
                "or trading during periods of higher liquidity."
            ),
        },
        "max_leverage": {
            "limit_name": "Maximum Leverage",
            "explanation": (
                "Leverage amplifies both gains and losses. Higher leverage increases "
                "liquidation risk and can lead to rapid loss of capital during "
                "adverse market movements."
            ),
            "suggestion": (
                "Reduce the requested leverage value. Consider the current market "
                "volatility and your risk tolerance. If higher leverage is required, "
                "contact your system administrator to adjust risk limits."
            ),
        },
        "daily_loss_limit_usd": {
            "limit_name": "Daily Loss Limit",
            "explanation": (
                "The daily loss limit prevents catastrophic losses by halting trading "
                "when cumulative daily losses exceed a threshold. This protects capital "
                "during extended periods of adverse conditions."
            ),
            "suggestion": (
                "Reduce the requested daily loss limit. If the current limit is too "
                "restrictive for your strategy, work with your risk manager to evaluate "
                "and potentially adjust the system-wide limit."
            ),
        },
        "min_health_factor": {
            "limit_name": "Minimum Health Factor",
            "explanation": (
                "Health factor measures the safety of lending positions. A health factor "
                "below 1.0 triggers liquidation. Setting the minimum too low increases "
                "the risk of liquidation during market volatility."
            ),
            "suggestion": (
                "Increase the requested minimum health factor. A buffer above 1.0 (e.g., 1.1+) "
                "provides protection against sudden market moves. Consider current market "
                "volatility when setting this parameter."
            ),
        },
    }

    def __init__(self, config: RiskGuardConfig | None = None) -> None:
        """Initialize RiskGuard with configuration.

        Args:
            config: Risk guard configuration (uses defaults if not provided)
        """
        self.config = config or RiskGuardConfig()

    def generate_guidance(
        self,
        field_name: str,
        requested_value: Decimal,
        limit_value: Decimal,
    ) -> RiskGuardGuidance:
        """Generate human-readable guidance for a failed risk check.

        This method creates a RiskGuardGuidance object with:
        - What the limit is and what value was requested
        - Clear explanation of what the limit protects against
        - Actionable suggestion for how to proceed

        Args:
            field_name: The configuration field that failed validation
            requested_value: The value the operator tried to set
            limit_value: The maximum/minimum allowed value

        Returns:
            RiskGuardGuidance with human-readable explanation and suggestion
        """
        # Get explanations for the field, with fallback for unknown fields
        field_info = self.LIMIT_EXPLANATIONS.get(
            field_name,
            {
                "limit_name": field_name.replace("_", " ").title(),
                "explanation": (
                    f"The value {requested_value} exceeds the configured risk limit of {limit_value}. "
                    "This limit exists to protect against excessive risk exposure."
                ),
                "suggestion": (
                    f"Adjust the {field_name} value to be within the allowed limit. "
                    "Contact your system administrator if you believe this limit should be changed."
                ),
            },
        )

        return RiskGuardGuidance(
            field_name=field_name,
            limit_name=field_info["limit_name"],
            requested_value=requested_value,
            limit_value=limit_value,
            explanation=field_info["explanation"],
            suggestion=field_info["suggestion"],
        )

    def validate_config_update(
        self,
        current_config: HotReloadableConfig,
        updates: dict[str, Any],
    ) -> RiskGuardResult:
        """Validate proposed configuration updates against risk limits.

        This method checks each proposed update against the risk guard's
        limits to ensure operators cannot accidentally configure
        dangerous parameters. When validation fails, it includes
        human-readable guidance explaining what went wrong and how to fix it.

        Args:
            current_config: Current configuration
            updates: Dict of field -> new value proposed updates

        Returns:
            RiskGuardResult indicating if validation passed, with guidance if not
        """
        result = RiskGuardResult(passed=True)

        for field_name, value in updates.items():
            # Ensure value is Decimal for comparison
            if not isinstance(value, Decimal):
                try:
                    value = Decimal(str(value))
                except Exception:
                    result.passed = False
                    result.violations.append(f"{field_name}: invalid value format")
                    # Add guidance for invalid format
                    result.guidance.append(
                        RiskGuardGuidance(
                            field_name=field_name,
                            limit_name=field_name.replace("_", " ").title(),
                            requested_value=Decimal("0"),
                            limit_value=Decimal("0"),
                            explanation=f"The value provided for {field_name} is not a valid number format.",
                            suggestion="Ensure the value is a valid decimal number (e.g., 0.05 or 1000).",
                        )
                    )
                    continue

            # Check specific risk limits
            if field_name == "max_slippage":
                if value > self.config.max_slippage_limit:
                    result.passed = False
                    result.violations.append(
                        f"max_slippage ({value}) exceeds risk limit ({self.config.max_slippage_limit})"
                    )
                    result.max_allowed_values[field_name] = self.config.max_slippage_limit
                    result.guidance.append(self.generate_guidance(field_name, value, self.config.max_slippage_limit))

            elif field_name == "max_leverage":
                if value > self.config.max_leverage_limit:
                    result.passed = False
                    result.violations.append(
                        f"max_leverage ({value}) exceeds risk limit ({self.config.max_leverage_limit})"
                    )
                    result.max_allowed_values[field_name] = self.config.max_leverage_limit
                    result.guidance.append(self.generate_guidance(field_name, value, self.config.max_leverage_limit))

            elif field_name == "daily_loss_limit_usd":
                if value > self.config.max_daily_loss_limit_usd:
                    result.passed = False
                    result.violations.append(
                        f"daily_loss_limit_usd ({value}) exceeds risk limit ({self.config.max_daily_loss_limit_usd})"
                    )
                    result.max_allowed_values[field_name] = self.config.max_daily_loss_limit_usd
                    result.guidance.append(
                        self.generate_guidance(field_name, value, self.config.max_daily_loss_limit_usd)
                    )

            elif field_name == "min_health_factor":
                if value < self.config.min_health_factor_floor:
                    result.passed = False
                    result.violations.append(
                        f"min_health_factor ({value}) is below risk floor ({self.config.min_health_factor_floor})"
                    )
                    result.max_allowed_values[field_name] = self.config.min_health_factor_floor
                    result.guidance.append(
                        self.generate_guidance(field_name, value, self.config.min_health_factor_floor)
                    )

        return result


# Type alias for notification callback
NotificationCallback = Callable[[OperatorCard], None]


@dataclass
class ConfigSnapshot:
    """Snapshot of configuration state for persistence.

    Attributes:
        config_dict: Serialized configuration as dictionary
        timestamp: When the snapshot was created
        version: Config version number
        updated_by: Who made the update (operator/system)
    """

    config_dict: dict[str, Any]
    timestamp: datetime
    version: int
    updated_by: str = "system"

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "config_dict": self.config_dict,
            "timestamp": self.timestamp.isoformat(),
            "version": self.version,
            "updated_by": self.updated_by,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ConfigSnapshot":
        """Create from dictionary."""
        return cls(
            config_dict=data["config_dict"],
            timestamp=datetime.fromisoformat(data["timestamp"]),
            version=data["version"],
            updated_by=data.get("updated_by", "system"),
        )


class StrategyBase[ConfigT: HotReloadableConfig](ABC):
    """Base class for all strategies with hot-reload configuration support.

    This class provides:
    - Hot-reload configuration updates via update_config()
    - RiskGuard validation to prevent dangerous config changes
    - Atomic config application with rollback on failure
    - CONFIG_UPDATED event emission to timeline
    - Operator notification support

    Strategies should inherit from this class and implement the
    abstract run() method.

    Attributes:
        config: Hot-reloadable configuration
        risk_guard: RiskGuard instance for validation
        persistent_state: Dict containing strategy state including config snapshots
        notification_callback: Optional callback for operator notifications
    """

    STRATEGY_NAME: str = "BASE"

    def __init__(
        self,
        config: ConfigT,
        risk_guard_config: RiskGuardConfig | None = None,
        notification_callback: NotificationCallback | None = None,
    ) -> None:
        """Initialize the strategy base.

        Args:
            config: Hot-reloadable configuration
            risk_guard_config: Optional RiskGuard configuration
            notification_callback: Optional callback for sending notifications
        """
        self.config = config
        self.risk_guard = RiskGuard(risk_guard_config)
        self.persistent_state: dict[str, Any] = {
            "config_snapshots": [],
            "config_version": 0,
        }
        self._notification_callback = notification_callback
        self._strategy_id: str = getattr(config, "strategy_id", "unknown")
        self._chain: str = getattr(config, "chain", "unknown")

        # Take initial config snapshot
        self._save_config_snapshot("initialization")

    @property
    def strategy_id(self) -> str:
        """Get the strategy ID."""
        return self._strategy_id

    @property
    def chain(self) -> str:
        """Get the chain."""
        return self._chain

    @abstractmethod
    def run(self) -> Any:
        """Execute one iteration of the strategy.

        Must be implemented by subclasses.

        Returns:
            ActionBundle or None if no action needed
        """
        pass

    def update_config(
        self,
        updates: dict[str, Any],
        updated_by: str = "operator",
    ) -> ConfigUpdateResult:
        """Update configuration with validation and persistence.

        This method:
        1. Validates new config values against the config's schema
        2. Validates changes against RiskGuard (can't bypass risk limits)
        3. Applies changes atomically
        4. Persists config snapshot to persistent_state
        5. Emits CONFIG_UPDATED event with old and new values
        6. Sends notification to operator

        Args:
            updates: Dict of field -> new value to update
            updated_by: Who is making the update (for audit trail)

        Returns:
            ConfigUpdateResult indicating success or failure
        """
        if not updates:
            return ConfigUpdateResult(
                success=False,
                error="No updates provided",
            )

        logger.info(f"Config update requested for {self.strategy_id}: fields={list(updates.keys())}, by={updated_by}")

        # Step 1: Validate against RiskGuard
        risk_result = self.risk_guard.validate_config_update(self.config, updates)
        if not risk_result.passed:
            error_msg = f"RiskGuard validation failed: {'; '.join(risk_result.violations)}"
            logger.warning(f"Config update rejected for {self.strategy_id}: {error_msg}")
            return ConfigUpdateResult(
                success=False,
                error=error_msg,
            )

        # Step 2: Convert values to Decimal if needed and validate format
        processed_updates: dict[str, Any] = {}
        for field_name, value in updates.items():
            if not isinstance(value, Decimal):
                try:
                    processed_updates[field_name] = Decimal(str(value))
                except Exception:
                    return ConfigUpdateResult(
                        success=False,
                        error=f"Invalid value format for {field_name}: {value}",
                    )
            else:
                processed_updates[field_name] = value

        # Step 3: Apply changes atomically via config's update method
        # This validates against the config's own schema and ranges
        result = self.config.update(**processed_updates)

        if not result.success:
            logger.warning(f"Config update failed for {self.strategy_id}: {result.error}")
            return result

        # Step 4: Persist config snapshot to persistent_state
        self._save_config_snapshot(updated_by)

        # Step 5: Emit CONFIG_UPDATED event
        self._emit_config_updated_event(
            updated_fields=result.updated_fields,
            previous_values=result.previous_values,
            new_values={f: getattr(self.config, f) for f in result.updated_fields},
            updated_by=updated_by,
        )

        # Step 6: Send notification to operator
        self._send_config_update_notification(
            updated_fields=result.updated_fields,
            previous_values=result.previous_values,
            new_values={f: getattr(self.config, f) for f in result.updated_fields},
            updated_by=updated_by,
        )

        logger.info(f"Config updated successfully for {self.strategy_id}: updated_fields={result.updated_fields}")

        return result

    def _save_config_snapshot(self, updated_by: str) -> None:
        """Save current config as a snapshot in persistent_state.

        Args:
            updated_by: Who triggered the save
        """
        self.persistent_state["config_version"] += 1
        version = self.persistent_state["config_version"]

        snapshot = ConfigSnapshot(
            config_dict=self.config.to_dict(),
            timestamp=datetime.now(UTC),
            version=version,
            updated_by=updated_by,
        )

        # Keep last 10 snapshots for history
        snapshots = self.persistent_state.get("config_snapshots", [])
        snapshots.append(snapshot.to_dict())
        if len(snapshots) > 10:
            snapshots = snapshots[-10:]
        self.persistent_state["config_snapshots"] = snapshots

        logger.debug(f"Config snapshot saved for {self.strategy_id}: version={version}")

    def _emit_config_updated_event(
        self,
        updated_fields: list[str],
        previous_values: dict[str, Any],
        new_values: dict[str, Any],
        updated_by: str,
    ) -> None:
        """Emit CONFIG_UPDATED event to timeline.

        Args:
            updated_fields: List of fields that were updated
            previous_values: Previous values of updated fields
            new_values: New values of updated fields
            updated_by: Who made the update
        """
        # Build details dict
        changes: list[dict[str, Any]] = []
        for field_name in updated_fields:
            old_val = previous_values.get(field_name)
            new_val = new_values.get(field_name)
            changes.append(
                {
                    "field": field_name,
                    "old_value": str(old_val) if old_val is not None else None,
                    "new_value": str(new_val) if new_val is not None else None,
                }
            )

        event = TimelineEvent(
            timestamp=datetime.now(UTC),
            event_type=TimelineEventType.CONFIG_UPDATED,
            description=f"Configuration updated: {', '.join(updated_fields)}",
            strategy_id=self.strategy_id,
            chain=self.chain,
            details={
                "changes": changes,
                "updated_by": updated_by,
                "config_version": self.persistent_state["config_version"],
            },
        )

        add_event(event)
        logger.debug(f"CONFIG_UPDATED event emitted for {self.strategy_id}")

    def _send_config_update_notification(
        self,
        updated_fields: list[str],
        previous_values: dict[str, Any],
        new_values: dict[str, Any],
        updated_by: str,
    ) -> None:
        """Send notification to operator about config change.

        Args:
            updated_fields: List of fields that were updated
            previous_values: Previous values of updated fields
            new_values: New values of updated fields
            updated_by: Who made the update
        """
        if not self._notification_callback:
            logger.debug(
                f"No notification callback configured for {self.strategy_id}, skipping config update notification"
            )
            return

        # Build context for operator card
        context: dict[str, Any] = {
            "updated_by": updated_by,
            "config_version": self.persistent_state["config_version"],
        }

        for field_name in updated_fields:
            old_val = previous_values.get(field_name)
            new_val = new_values.get(field_name)
            context[f"{field_name}_old"] = str(old_val) if old_val is not None else None
            context[f"{field_name}_new"] = str(new_val) if new_val is not None else None

        # Create operator card for notification
        # Config updates are informational, so we use minimal position summary
        # and a simple "acknowledge" action
        card = OperatorCard(
            strategy_id=self.strategy_id,
            timestamp=datetime.now(UTC),
            event_type=EventType.ALERT,
            reason=StuckReason.UNKNOWN,  # CONFIG_UPDATED doesn't have a stuck reason
            context=context,
            severity=Severity.LOW,
            position_summary=PositionSummary(
                total_value_usd=Decimal("0"),
                available_balance_usd=Decimal("0"),
            ),
            risk_description=f"Configuration updated by {updated_by}: {', '.join(updated_fields)}",
            suggested_actions=[
                SuggestedAction(
                    action=AvailableAction.RESUME,
                    description="Configuration change acknowledged. No action required.",
                    priority=1,
                    is_recommended=True,
                ),
            ],
            available_actions=[AvailableAction.RESUME],
        )

        try:
            self._notification_callback(card)
            logger.info(f"Config update notification sent for {self.strategy_id}")
        except Exception as e:
            logger.error(f"Failed to send config update notification for {self.strategy_id}: {e}")

    def set_notification_callback(
        self,
        callback: NotificationCallback | None,
    ) -> None:
        """Set the notification callback for operator alerts.

        Args:
            callback: Callback function that takes an OperatorCard
        """
        self._notification_callback = callback

    def get_config_history(self) -> list[dict[str, Any]]:
        """Get the configuration update history.

        Returns:
            List of config snapshots in chronological order
        """
        return self.persistent_state.get("config_snapshots", [])

    def get_current_config_version(self) -> int:
        """Get the current config version number.

        Returns:
            Current config version
        """
        return self.persistent_state.get("config_version", 0)

    def restore_config_from_snapshot(
        self,
        version: int,
    ) -> ConfigUpdateResult:
        """Restore configuration from a previous snapshot.

        Args:
            version: The config version to restore to

        Returns:
            ConfigUpdateResult indicating success or failure
        """
        snapshots = self.persistent_state.get("config_snapshots", [])

        # Find snapshot with matching version
        target_snapshot: dict[str, Any] | None = None
        for snapshot in snapshots:
            if snapshot.get("version") == version:
                target_snapshot = snapshot
                break

        if not target_snapshot:
            return ConfigUpdateResult(
                success=False,
                error=f"Config version {version} not found in history",
            )

        # Extract the config values from snapshot
        config_dict = target_snapshot.get("config_dict", {})
        trading_params = config_dict.get("trading_parameters", {})
        risk_params = config_dict.get("risk_parameters", {})

        # Build updates dict
        updates: dict[str, Any] = {}
        for key, value in trading_params.items():
            if key in self.config.HOT_RELOADABLE_FIELDS:
                updates[key] = value
        for key, value in risk_params.items():
            if key in self.config.HOT_RELOADABLE_FIELDS:
                updates[key] = value

        if not updates:
            return ConfigUpdateResult(
                success=False,
                error=f"No restorable fields found in config version {version}",
            )

        return self.update_config(updates, updated_by=f"restore_v{version}")


__all__ = [
    "StrategyBase",
    "RiskGuard",
    "RiskGuardConfig",
    "RiskGuardResult",
    "RiskGuardGuidance",
    "ConfigSnapshot",
    "NotificationCallback",
]
