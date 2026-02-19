"""Emergency Manager service for safe emergency stops.

This module implements the EmergencyManager class which handles emergency
stop procedures for strategies, including:
- Immediately pausing the strategy
- Gathering position summary information
- Generating EMERGENCY_STOP OperatorCard
- Sending CRITICAL alerts
"""

import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from ..alerting import AlertManager, AlertSendResult
from ..api.timeline import TimelineEvent, TimelineEventType, add_event
from ..models import (
    AvailableAction,
    EventType,
    OperatorCard,
    PositionSummary,
    Severity,
    StuckReason,
    SuggestedAction,
)

logger = logging.getLogger(__name__)


@dataclass
class TokenPosition:
    """Represents a token position held by the strategy."""

    token_symbol: str
    token_address: str
    balance: Decimal
    value_usd: Decimal
    chain: str


@dataclass
class LPPositionInfo:
    """Represents an LP position held by the strategy."""

    position_id: str
    pool_address: str
    pool_name: str
    token0_symbol: str
    token1_symbol: str
    token0_amount: Decimal
    token1_amount: Decimal
    value_usd: Decimal
    range_lower: Decimal | None = None
    range_upper: Decimal | None = None
    fees_earned_usd: Decimal = Decimal("0")
    chain: str = ""


@dataclass
class BorrowPosition:
    """Represents a borrow position held by the strategy."""

    protocol: str
    collateral_token: str
    collateral_amount: Decimal
    collateral_value_usd: Decimal
    borrow_token: str
    borrow_amount: Decimal
    borrow_value_usd: Decimal
    health_factor: Decimal
    liquidation_price: Decimal | None = None
    chain: str = ""


@dataclass
class FullPositionSummary:
    """Complete position summary including all tokens, LP positions, and borrows."""

    strategy_id: str
    chain: str
    timestamp: datetime

    # Token balances
    token_positions: list[TokenPosition] = field(default_factory=list)

    # LP positions
    lp_positions: list[LPPositionInfo] = field(default_factory=list)

    # Borrow positions
    borrow_positions: list[BorrowPosition] = field(default_factory=list)

    # Aggregated values
    total_token_value_usd: Decimal = Decimal("0")
    total_lp_value_usd: Decimal = Decimal("0")
    total_collateral_value_usd: Decimal = Decimal("0")
    total_borrowed_value_usd: Decimal = Decimal("0")

    @property
    def total_value_usd(self) -> Decimal:
        """Calculate total portfolio value in USD."""
        return (
            self.total_token_value_usd
            + self.total_lp_value_usd
            + self.total_collateral_value_usd
            - self.total_borrowed_value_usd
        )

    @property
    def net_exposure_usd(self) -> Decimal:
        """Calculate net exposure (total value - borrowed)."""
        return self.total_value_usd

    @property
    def has_lp_positions(self) -> bool:
        """Check if strategy has any LP positions."""
        return len(self.lp_positions) > 0

    @property
    def has_borrow_positions(self) -> bool:
        """Check if strategy has any borrow positions."""
        return len(self.borrow_positions) > 0

    @property
    def min_health_factor(self) -> Decimal | None:
        """Get the minimum health factor across all borrow positions."""
        if not self.borrow_positions:
            return None
        return min(p.health_factor for p in self.borrow_positions)

    def to_position_summary(self) -> PositionSummary:
        """Convert to the standard PositionSummary format for OperatorCard."""
        token_balances: dict[str, Decimal] = {}
        for pos in self.token_positions:
            token_balances[pos.token_symbol] = pos.balance

        lp_positions_list: list[dict[str, Any]] = []
        for lp in self.lp_positions:
            lp_dict: dict[str, Any] = {
                "position_id": lp.position_id,
                "pool_address": lp.pool_address,
                "pool_name": lp.pool_name,
                "token0_symbol": lp.token0_symbol,
                "token1_symbol": lp.token1_symbol,
                "token0_amount": str(lp.token0_amount),
                "token1_amount": str(lp.token1_amount),
                "value_usd": str(lp.value_usd),
                "fees_earned_usd": str(lp.fees_earned_usd),
            }
            if lp.range_lower is not None:
                lp_dict["range_lower"] = str(lp.range_lower)
            if lp.range_upper is not None:
                lp_dict["range_upper"] = str(lp.range_upper)
            lp_positions_list.append(lp_dict)

        return PositionSummary(
            total_value_usd=self.total_value_usd,
            available_balance_usd=self.total_token_value_usd,
            lp_value_usd=self.total_lp_value_usd,
            borrowed_value_usd=self.total_borrowed_value_usd,
            collateral_value_usd=self.total_collateral_value_usd,
            token_balances=token_balances,
            lp_positions=lp_positions_list,
            health_factor=self.min_health_factor,
            leverage=self._calculate_leverage(),
        )

    def _calculate_leverage(self) -> Decimal | None:
        """Calculate the effective leverage of the position."""
        if not self.has_borrow_positions:
            return None
        if self.total_collateral_value_usd == Decimal("0"):
            return None
        # Leverage = Total Assets / Equity = Total Assets / (Total Assets - Debt)
        total_assets = self.total_collateral_value_usd + self.total_lp_value_usd + self.total_token_value_usd
        equity = total_assets - self.total_borrowed_value_usd
        if equity <= Decimal("0"):
            return Decimal("999")  # Indicate extreme leverage
        return total_assets / equity

    def to_dict(self) -> dict[str, Any]:
        """Convert the full position summary to a dictionary for serialization."""
        return {
            "strategy_id": self.strategy_id,
            "chain": self.chain,
            "timestamp": self.timestamp.isoformat(),
            "token_positions": [
                {
                    "token_symbol": p.token_symbol,
                    "token_address": p.token_address,
                    "balance": str(p.balance),
                    "value_usd": str(p.value_usd),
                    "chain": p.chain,
                }
                for p in self.token_positions
            ],
            "lp_positions": [
                {
                    "position_id": lp.position_id,
                    "pool_address": lp.pool_address,
                    "pool_name": lp.pool_name,
                    "token0_symbol": lp.token0_symbol,
                    "token1_symbol": lp.token1_symbol,
                    "token0_amount": str(lp.token0_amount),
                    "token1_amount": str(lp.token1_amount),
                    "value_usd": str(lp.value_usd),
                    "range_lower": str(lp.range_lower) if lp.range_lower else None,
                    "range_upper": str(lp.range_upper) if lp.range_upper else None,
                    "fees_earned_usd": str(lp.fees_earned_usd),
                    "chain": lp.chain,
                }
                for lp in self.lp_positions
            ],
            "borrow_positions": [
                {
                    "protocol": b.protocol,
                    "collateral_token": b.collateral_token,
                    "collateral_amount": str(b.collateral_amount),
                    "collateral_value_usd": str(b.collateral_value_usd),
                    "borrow_token": b.borrow_token,
                    "borrow_amount": str(b.borrow_amount),
                    "borrow_value_usd": str(b.borrow_value_usd),
                    "health_factor": str(b.health_factor),
                    "liquidation_price": str(b.liquidation_price) if b.liquidation_price else None,
                    "chain": b.chain,
                }
                for b in self.borrow_positions
            ],
            "total_token_value_usd": str(self.total_token_value_usd),
            "total_lp_value_usd": str(self.total_lp_value_usd),
            "total_collateral_value_usd": str(self.total_collateral_value_usd),
            "total_borrowed_value_usd": str(self.total_borrowed_value_usd),
            "total_value_usd": str(self.total_value_usd),
            "net_exposure_usd": str(self.net_exposure_usd),
        }


@dataclass
class EmergencyResult:
    """Result of an emergency stop operation."""

    success: bool
    strategy_id: str
    timestamp: datetime
    position_summary: FullPositionSummary
    operator_card: OperatorCard
    alert_result: AlertSendResult | None = None
    error: str | None = None
    pause_successful: bool = False
    alerts_sent: bool = False

    def to_dict(self) -> dict[str, Any]:
        """Convert the emergency result to a dictionary for serialization."""
        return {
            "success": self.success,
            "strategy_id": self.strategy_id,
            "timestamp": self.timestamp.isoformat(),
            "position_summary": self.position_summary.to_dict(),
            "operator_card": self.operator_card.to_dict(),
            "alert_result": {
                "success": self.alert_result.success,
                "channels_sent": [c.value for c in self.alert_result.channels_sent],
                "channels_failed": [c.value for c in self.alert_result.channels_failed],
                "errors": {c.value: e for c, e in self.alert_result.errors.items()},
            }
            if self.alert_result
            else None,
            "error": self.error,
            "pause_successful": self.pause_successful,
            "alerts_sent": self.alerts_sent,
        }


# Type aliases for callbacks
PauseStrategyCallback = Callable[[str], bool]
"""Callback to pause a strategy. Takes strategy_id, returns success bool."""

GetPositionCallback = Callable[[str], FullPositionSummary]
"""Callback to get position summary. Takes strategy_id, returns FullPositionSummary."""


class EmergencyManager:
    """Manages emergency stop procedures for strategies.

    The EmergencyManager handles the full emergency stop workflow:
    1. Immediately pause the strategy
    2. Gather complete position summary
    3. Generate EMERGENCY_STOP OperatorCard with suggested actions
    4. Send CRITICAL alerts to configured channels
    5. Return complete EmergencyResult

    Attributes:
        alert_manager: AlertManager for sending CRITICAL alerts
        pause_callback: Callback to pause a strategy
        position_callback: Callback to get position summary
    """

    def __init__(
        self,
        alert_manager: AlertManager | None = None,
        pause_callback: PauseStrategyCallback | None = None,
        position_callback: GetPositionCallback | None = None,
        dashboard_base_url: str | None = None,
    ) -> None:
        """Initialize the EmergencyManager.

        Args:
            alert_manager: AlertManager instance for sending alerts
            pause_callback: Callback function to pause a strategy
            position_callback: Callback function to get position summary
            dashboard_base_url: Base URL for dashboard links
        """
        self.alert_manager = alert_manager
        self.pause_callback = pause_callback
        self.position_callback = position_callback
        self.dashboard_base_url = dashboard_base_url

    def emergency_stop(
        self,
        strategy_id: str,
        reason: str,
        chain: str = "",
        trigger_context: dict[str, Any] | None = None,
    ) -> EmergencyResult:
        """Execute an emergency stop for a strategy.

        This method:
        1. Immediately pauses the strategy
        2. Gets the full position summary (all tokens, LP positions, borrows)
        3. Generates an OperatorCard with EMERGENCY_STOP event
        4. Suggests actions: EMERGENCY_UNWIND, MANUAL_REVIEW
        5. Sends CRITICAL alerts
        6. Returns EmergencyResult with position summary

        Args:
            strategy_id: The ID of the strategy to stop
            reason: Human-readable reason for the emergency stop
            chain: The blockchain network the strategy operates on
            trigger_context: Optional additional context about what triggered the emergency

        Returns:
            EmergencyResult with full details of the emergency stop operation
        """
        timestamp = datetime.now(UTC)
        logger.warning(f"EMERGENCY STOP initiated for strategy {strategy_id}: {reason}")

        # Step 1: Immediately pause the strategy
        pause_successful = False
        if self.pause_callback:
            try:
                pause_successful = self.pause_callback(strategy_id)
                if pause_successful:
                    logger.info(f"Strategy {strategy_id} paused successfully")
                else:
                    logger.error(f"Failed to pause strategy {strategy_id}")
            except Exception as e:
                logger.exception(f"Error pausing strategy {strategy_id}: {e}")
        else:
            logger.warning("No pause callback configured, skipping pause step")

        # Step 2: Get position summary
        position_summary = self._get_position_summary(strategy_id, chain, timestamp)

        # Step 3: Generate OperatorCard with EMERGENCY_STOP event
        operator_card = self._generate_emergency_card(
            strategy_id=strategy_id,
            reason=reason,
            position_summary=position_summary,
            timestamp=timestamp,
            trigger_context=trigger_context,
        )

        # Step 4: Emit timeline event
        self._emit_emergency_event(
            strategy_id=strategy_id,
            chain=chain,
            reason=reason,
            position_summary=position_summary,
            timestamp=timestamp,
        )

        # Step 5: Send CRITICAL alerts
        alert_result = None
        alerts_sent = False
        if self.alert_manager:
            alert_result = self._send_critical_alerts(operator_card)
            alerts_sent = alert_result.success if alert_result else False

        # Build result
        success = pause_successful and alerts_sent
        error = None
        if not pause_successful:
            error = "Failed to pause strategy"
        elif not alerts_sent:
            error = "Failed to send alerts"

        result = EmergencyResult(
            success=success,
            strategy_id=strategy_id,
            timestamp=timestamp,
            position_summary=position_summary,
            operator_card=operator_card,
            alert_result=alert_result,
            error=error,
            pause_successful=pause_successful,
            alerts_sent=alerts_sent,
        )

        logger.info(
            f"Emergency stop completed for {strategy_id}: "
            f"success={success}, pause={pause_successful}, alerts={alerts_sent}"
        )

        return result

    async def emergency_stop_async(
        self,
        strategy_id: str,
        reason: str,
        chain: str = "",
        trigger_context: dict[str, Any] | None = None,
    ) -> EmergencyResult:
        """Async version of emergency_stop.

        Args:
            strategy_id: The ID of the strategy to stop
            reason: Human-readable reason for the emergency stop
            chain: The blockchain network the strategy operates on
            trigger_context: Optional additional context about what triggered the emergency

        Returns:
            EmergencyResult with full details of the emergency stop operation
        """
        timestamp = datetime.now(UTC)
        logger.warning(f"EMERGENCY STOP (async) initiated for strategy {strategy_id}: {reason}")

        # Step 1: Immediately pause the strategy
        pause_successful = False
        if self.pause_callback:
            try:
                pause_successful = self.pause_callback(strategy_id)
                if pause_successful:
                    logger.info(f"Strategy {strategy_id} paused successfully")
                else:
                    logger.error(f"Failed to pause strategy {strategy_id}")
            except Exception as e:
                logger.exception(f"Error pausing strategy {strategy_id}: {e}")
        else:
            logger.warning("No pause callback configured, skipping pause step")

        # Step 2: Get position summary
        position_summary = self._get_position_summary(strategy_id, chain, timestamp)

        # Step 3: Generate OperatorCard with EMERGENCY_STOP event
        operator_card = self._generate_emergency_card(
            strategy_id=strategy_id,
            reason=reason,
            position_summary=position_summary,
            timestamp=timestamp,
            trigger_context=trigger_context,
        )

        # Step 4: Emit timeline event
        self._emit_emergency_event(
            strategy_id=strategy_id,
            chain=chain,
            reason=reason,
            position_summary=position_summary,
            timestamp=timestamp,
        )

        # Step 5: Send CRITICAL alerts (async)
        alert_result = None
        alerts_sent = False
        if self.alert_manager:
            alert_result = await self._send_critical_alerts_async(operator_card)
            alerts_sent = alert_result.success if alert_result else False

        # Build result
        success = pause_successful and alerts_sent
        error = None
        if not pause_successful:
            error = "Failed to pause strategy"
        elif not alerts_sent:
            error = "Failed to send alerts"

        result = EmergencyResult(
            success=success,
            strategy_id=strategy_id,
            timestamp=timestamp,
            position_summary=position_summary,
            operator_card=operator_card,
            alert_result=alert_result,
            error=error,
            pause_successful=pause_successful,
            alerts_sent=alerts_sent,
        )

        logger.info(
            f"Emergency stop (async) completed for {strategy_id}: "
            f"success={success}, pause={pause_successful}, alerts={alerts_sent}"
        )

        return result

    def _get_position_summary(
        self,
        strategy_id: str,
        chain: str,
        timestamp: datetime,
    ) -> FullPositionSummary:
        """Get the full position summary for a strategy.

        Args:
            strategy_id: The strategy ID
            chain: The blockchain network
            timestamp: Current timestamp

        Returns:
            FullPositionSummary with all position details
        """
        if self.position_callback:
            try:
                return self.position_callback(strategy_id)
            except Exception as e:
                logger.exception(f"Error getting position summary for {strategy_id}: {e}")

        # Return empty summary if callback not available or fails
        return FullPositionSummary(
            strategy_id=strategy_id,
            chain=chain,
            timestamp=timestamp,
        )

    def _generate_emergency_card(
        self,
        strategy_id: str,
        reason: str,
        position_summary: FullPositionSummary,
        timestamp: datetime,
        trigger_context: dict[str, Any] | None = None,
    ) -> OperatorCard:
        """Generate an OperatorCard for the emergency stop.

        Args:
            strategy_id: The strategy ID
            reason: Human-readable reason for the emergency
            position_summary: Full position summary
            timestamp: When the emergency was triggered
            trigger_context: Optional additional context

        Returns:
            OperatorCard with EMERGENCY_STOP event type
        """
        # Build context dict
        context: dict[str, Any] = {
            "emergency_reason": reason,
            "strategy_status": "emergency_stopped",
            "total_value_at_risk_usd": str(position_summary.total_value_usd),
        }

        if position_summary.has_lp_positions:
            context["lp_positions_count"] = len(position_summary.lp_positions)
            context["total_lp_value_usd"] = str(position_summary.total_lp_value_usd)

        if position_summary.has_borrow_positions:
            context["borrow_positions_count"] = len(position_summary.borrow_positions)
            context["total_borrowed_usd"] = str(position_summary.total_borrowed_value_usd)
            if position_summary.min_health_factor:
                context["min_health_factor"] = str(position_summary.min_health_factor)

        if trigger_context:
            context["trigger_context"] = trigger_context

        # Generate risk description
        risk_description = self._generate_risk_description(reason, position_summary)

        # Create suggested actions - EMERGENCY_UNWIND as recommended, MANUAL_REVIEW via PAUSE
        suggested_actions = [
            SuggestedAction(
                action=AvailableAction.EMERGENCY_UNWIND,
                description="Emergency unwind all positions to minimize further risk",
                priority=1,
                is_recommended=True,
                params={
                    "total_value_usd": str(position_summary.total_value_usd),
                    "has_lp": position_summary.has_lp_positions,
                    "has_borrows": position_summary.has_borrow_positions,
                },
            ),
            SuggestedAction(
                action=AvailableAction.PAUSE,
                description="Keep strategy paused for manual review before taking action",
                priority=2,
                params={"reason": "emergency_stop_manual_review"},
            ),
        ]

        # Available actions for emergency stop
        available_actions = [
            AvailableAction.EMERGENCY_UNWIND,
            AvailableAction.PAUSE,
            AvailableAction.RESUME,  # In case operator wants to resume after review
        ]

        return OperatorCard(
            strategy_id=strategy_id,
            timestamp=timestamp,
            event_type=EventType.EMERGENCY_STOP,
            reason=StuckReason.CIRCUIT_BREAKER,  # Emergency stops typically from circuit breaker or similar
            context=context,
            severity=Severity.CRITICAL,  # Emergency stops are always CRITICAL
            position_summary=position_summary.to_position_summary(),
            risk_description=risk_description,
            suggested_actions=suggested_actions,
            available_actions=available_actions,
            auto_remediation=None,  # No auto-remediation for emergencies - requires human decision
        )

    def _generate_risk_description(
        self,
        reason: str,
        position_summary: FullPositionSummary,
    ) -> str:
        """Generate a human-readable risk description.

        Args:
            reason: Emergency stop reason
            position_summary: Full position summary

        Returns:
            Human-readable risk description
        """
        parts = [
            f"EMERGENCY STOP: {reason}",
            f"Total value at risk: ${position_summary.total_value_usd:,.2f} USD",
        ]

        if position_summary.has_lp_positions:
            parts.append(
                f"LP positions: {len(position_summary.lp_positions)} "
                f"(${position_summary.total_lp_value_usd:,.2f} USD) - "
                "subject to impermanent loss and liquidity risk"
            )

        if position_summary.has_borrow_positions:
            min_hf = position_summary.min_health_factor
            hf_str = f", health factor: {min_hf:.2f}" if min_hf else ""
            parts.append(
                f"Borrow positions: {len(position_summary.borrow_positions)} "
                f"(${position_summary.total_borrowed_value_usd:,.2f} USD borrowed{hf_str}) - "
                "liquidation risk if not managed"
            )

        parts.append("IMMEDIATE ACTION REQUIRED")

        return " | ".join(parts)

    def _emit_emergency_event(
        self,
        strategy_id: str,
        chain: str,
        reason: str,
        position_summary: FullPositionSummary,
        timestamp: datetime,
    ) -> None:
        """Emit a timeline event for the emergency stop.

        Args:
            strategy_id: The strategy ID
            chain: The blockchain network
            reason: Emergency stop reason
            position_summary: Full position summary
            timestamp: When the emergency was triggered
        """
        event = TimelineEvent(
            timestamp=timestamp,
            event_type=TimelineEventType.CIRCUIT_BREAKER_TRIGGERED,
            description=f"EMERGENCY STOP: {reason}",
            strategy_id=strategy_id,
            chain=chain,
            details={
                "emergency_reason": reason,
                "total_value_usd": str(position_summary.total_value_usd),
                "lp_positions_count": len(position_summary.lp_positions),
                "borrow_positions_count": len(position_summary.borrow_positions),
                "event_subtype": "EMERGENCY_STOP",
            },
        )
        add_event(event)
        logger.info(f"Emergency stop event emitted for {strategy_id}")

    def _send_critical_alerts(
        self,
        operator_card: OperatorCard,
    ) -> AlertSendResult | None:
        """Send CRITICAL alerts for the emergency stop.

        Args:
            operator_card: The OperatorCard to send alerts about

        Returns:
            AlertSendResult or None if no alert manager configured
        """
        if not self.alert_manager:
            logger.warning("No alert manager configured, skipping alerts")
            return None

        try:
            # Send via both direct methods to bypass rule matching for emergency
            result = AlertSendResult(success=False)

            # Try Telegram first
            telegram_result = self.alert_manager.send_direct_telegram_alert_sync(operator_card)
            if telegram_result.success:
                result.success = True
                result.channels_sent.extend(telegram_result.channels_sent)
            else:
                result.channels_failed.extend(telegram_result.channels_failed)
                result.errors.update(telegram_result.errors)

            # Try Slack
            slack_result = self.alert_manager.send_direct_slack_alert_sync(operator_card)
            if slack_result.success:
                result.success = True
                result.channels_sent.extend(slack_result.channels_sent)
            else:
                result.channels_failed.extend(slack_result.channels_failed)
                result.errors.update(slack_result.errors)

            # Log result
            if result.success:
                logger.info(
                    f"CRITICAL alerts sent for {operator_card.strategy_id}: "
                    f"channels={[c.value for c in result.channels_sent]}"
                )
            else:
                logger.error(f"Failed to send CRITICAL alerts for {operator_card.strategy_id}: errors={result.errors}")

            return result

        except Exception as e:
            logger.exception(f"Error sending CRITICAL alerts: {e}")
            return AlertSendResult(
                success=False,
                errors={},
                skipped_reason=str(e),
            )

    async def _send_critical_alerts_async(
        self,
        operator_card: OperatorCard,
    ) -> AlertSendResult | None:
        """Send CRITICAL alerts asynchronously.

        Args:
            operator_card: The OperatorCard to send alerts about

        Returns:
            AlertSendResult or None if no alert manager configured
        """
        if not self.alert_manager:
            logger.warning("No alert manager configured, skipping alerts")
            return None

        try:
            result = AlertSendResult(success=False)

            # Send to both channels concurrently
            telegram_task = self.alert_manager.send_direct_telegram_alert(operator_card)
            slack_task = self.alert_manager.send_direct_slack_alert(operator_card)

            telegram_result, slack_result = await asyncio.gather(
                telegram_task,
                slack_task,
                return_exceptions=True,
            )

            # Process Telegram result
            if isinstance(telegram_result, BaseException):
                logger.error(f"Telegram alert exception: {telegram_result}")
            else:
                tg_result: AlertSendResult = telegram_result
                if tg_result.success:
                    result.success = True
                    result.channels_sent.extend(tg_result.channels_sent)
                else:
                    result.channels_failed.extend(tg_result.channels_failed)
                    result.errors.update(tg_result.errors)

            # Process Slack result
            if isinstance(slack_result, BaseException):
                logger.error(f"Slack alert exception: {slack_result}")
            else:
                sl_result: AlertSendResult = slack_result
                if sl_result.success:
                    result.success = True
                    result.channels_sent.extend(sl_result.channels_sent)
                else:
                    result.channels_failed.extend(sl_result.channels_failed)
                    result.errors.update(sl_result.errors)

            # Log result
            if result.success:
                logger.info(
                    f"CRITICAL alerts sent (async) for {operator_card.strategy_id}: "
                    f"channels={[c.value for c in result.channels_sent]}"
                )
            else:
                logger.error(
                    f"Failed to send CRITICAL alerts (async) for {operator_card.strategy_id}: errors={result.errors}"
                )

            return result

        except Exception as e:
            logger.exception(f"Error sending CRITICAL alerts (async): {e}")
            return AlertSendResult(
                success=False,
                errors={},
                skipped_reason=str(e),
            )


def create_emergency_manager(
    alert_manager: AlertManager | None = None,
    pause_callback: PauseStrategyCallback | None = None,
    position_callback: GetPositionCallback | None = None,
    dashboard_base_url: str | None = None,
) -> EmergencyManager:
    """Factory function to create an EmergencyManager instance.

    Args:
        alert_manager: AlertManager for sending alerts
        pause_callback: Callback to pause strategies
        position_callback: Callback to get position summaries
        dashboard_base_url: Base URL for dashboard links

    Returns:
        Configured EmergencyManager instance
    """
    return EmergencyManager(
        alert_manager=alert_manager,
        pause_callback=pause_callback,
        position_callback=position_callback,
        dashboard_base_url=dashboard_base_url,
    )
