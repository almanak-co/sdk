"""Operator Card Generator service for automatic card generation.

This service generates OperatorCards from strategy state and errors/events,
providing structured, actionable information for operators.
"""

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from ..models import (
    AUTO_REMEDIABLE,
    REMEDIATION_MAP,
    AutoRemediation,
    AvailableAction,
    EventType,
    OperatorCard,
    PositionSummary,
    Severity,
    StuckReason,
    SuggestedAction,
)


@dataclass
class StrategyState:
    """Represents the current state of a strategy.

    This is the input structure for generating operator cards.
    """

    strategy_id: str
    status: str  # "running", "paused", "stuck", "error"

    # Position information
    total_value_usd: Decimal
    available_balance_usd: Decimal
    lp_value_usd: Decimal = Decimal("0")
    borrowed_value_usd: Decimal = Decimal("0")
    collateral_value_usd: Decimal = Decimal("0")
    token_balances: dict[str, Decimal] | None = None
    lp_positions: list[dict[str, Any]] | None = None
    health_factor: Decimal | None = None
    leverage: Decimal | None = None

    # Time tracking
    stuck_since: datetime | None = None
    last_successful_action: datetime | None = None

    # Transaction context
    pending_tx_hash: str | None = None
    pending_tx_gas_price: int | None = None
    current_gas_price: int | None = None

    # Market context
    current_price: Decimal | None = None
    price_24h_ago: Decimal | None = None
    pool_liquidity_usd: Decimal | None = None


@dataclass
class ErrorContext:
    """Context about an error or event that triggered the card generation."""

    error_type: str
    error_message: str
    tx_hash: str | None = None
    revert_reason: str | None = None
    gas_used: int | None = None
    gas_limit: int | None = None
    allowance: Decimal | None = None
    required_allowance: Decimal | None = None
    balance: Decimal | None = None
    required_balance: Decimal | None = None
    slippage_actual: Decimal | None = None
    slippage_max: Decimal | None = None
    oracle_timestamp: datetime | None = None
    rpc_error: str | None = None
    protocol_status: str | None = None


# Mapping of error types to StuckReason
ERROR_TYPE_TO_REASON: dict[str, StuckReason] = {
    # Transaction errors
    "TransactionUnderpriced": StuckReason.GAS_PRICE_BLOCKED,
    "ReplacementUnderpriced": StuckReason.GAS_PRICE_BLOCKED,
    "NonceTooLow": StuckReason.NONCE_CONFLICT,
    "NonceTooHigh": StuckReason.NONCE_CONFLICT,
    "TransactionReverted": StuckReason.TRANSACTION_REVERTED,
    "ExecutionReverted": StuckReason.TRANSACTION_REVERTED,
    "TransactionTimeout": StuckReason.NOT_INCLUDED_TIMEOUT,
    "PendingTimeout": StuckReason.NOT_INCLUDED_TIMEOUT,
    # Balance errors
    "InsufficientFunds": StuckReason.INSUFFICIENT_BALANCE,
    "InsufficientBalance": StuckReason.INSUFFICIENT_BALANCE,
    "InsufficientGas": StuckReason.INSUFFICIENT_GAS,
    "OutOfGas": StuckReason.INSUFFICIENT_GAS,
    "AllowanceTooLow": StuckReason.ALLOWANCE_MISSING,
    "NotApproved": StuckReason.ALLOWANCE_MISSING,
    # Protocol errors
    "SlippageExceeded": StuckReason.SLIPPAGE_EXCEEDED,
    "TooMuchSlippage": StuckReason.SLIPPAGE_EXCEEDED,
    "InsufficientLiquidity": StuckReason.POOL_LIQUIDITY_LOW,
    "LiquidityTooLow": StuckReason.POOL_LIQUIDITY_LOW,
    "OracleStale": StuckReason.ORACLE_STALE,
    "PriceStale": StuckReason.ORACLE_STALE,
    "ProtocolPaused": StuckReason.PROTOCOL_PAUSED,
    "ContractPaused": StuckReason.PROTOCOL_PAUSED,
    # System errors
    "RPCError": StuckReason.RPC_FAILURE,
    "ConnectionError": StuckReason.RPC_FAILURE,
    "TimeoutError": StuckReason.RPC_FAILURE,
    "ReceiptParseError": StuckReason.RECEIPT_PARSE_FAILED,
    "EventDecodeError": StuckReason.RECEIPT_PARSE_FAILED,
    "StateConflict": StuckReason.STATE_CONFLICT,
    "CASFailure": StuckReason.STATE_CONFLICT,
    "OptimisticLockError": StuckReason.STATE_CONFLICT,
    # Risk errors
    "RiskGuardBlocked": StuckReason.RISK_GUARD_BLOCKED,
    "RiskLimitExceeded": StuckReason.RISK_GUARD_BLOCKED,
    "CircuitBreaker": StuckReason.CIRCUIT_BREAKER,
    "LossLimitExceeded": StuckReason.CIRCUIT_BREAKER,
}

# Keywords in error messages to detect reason
ERROR_MESSAGE_KEYWORDS: dict[str, StuckReason] = {
    "gas price": StuckReason.GAS_PRICE_BLOCKED,
    "underpriced": StuckReason.GAS_PRICE_BLOCKED,
    "nonce": StuckReason.NONCE_CONFLICT,
    "reverted": StuckReason.TRANSACTION_REVERTED,
    "revert": StuckReason.TRANSACTION_REVERTED,
    "timeout": StuckReason.NOT_INCLUDED_TIMEOUT,
    "insufficient funds": StuckReason.INSUFFICIENT_BALANCE,
    "insufficient balance": StuckReason.INSUFFICIENT_BALANCE,
    "out of gas": StuckReason.INSUFFICIENT_GAS,
    "gas required exceeds": StuckReason.INSUFFICIENT_GAS,
    "allowance": StuckReason.ALLOWANCE_MISSING,
    "not approved": StuckReason.ALLOWANCE_MISSING,
    "slippage": StuckReason.SLIPPAGE_EXCEEDED,
    "price impact": StuckReason.SLIPPAGE_EXCEEDED,
    "liquidity": StuckReason.POOL_LIQUIDITY_LOW,
    "oracle": StuckReason.ORACLE_STALE,
    "stale price": StuckReason.ORACLE_STALE,
    "paused": StuckReason.PROTOCOL_PAUSED,
    "rpc": StuckReason.RPC_FAILURE,
    "connection": StuckReason.RPC_FAILURE,
    "risk guard": StuckReason.RISK_GUARD_BLOCKED,
    "risk limit": StuckReason.RISK_GUARD_BLOCKED,
    "circuit breaker": StuckReason.CIRCUIT_BREAKER,
    "loss limit": StuckReason.CIRCUIT_BREAKER,
}


class OperatorCardGenerator:
    """Generates OperatorCards from strategy state and events.

    The generator automatically:
    - Detects the StuckReason from error type and context
    - Calculates severity based on position at risk and time stuck
    - Looks up suggested actions from REMEDIATION_MAP
    - Generates human-readable risk descriptions
    - Sets up auto-remediation when applicable
    """

    # Severity thresholds for position at risk (in USD)
    POSITION_RISK_THRESHOLDS = {
        Severity.LOW: Decimal("100"),
        Severity.MEDIUM: Decimal("1000"),
        Severity.HIGH: Decimal("10000"),
        Severity.CRITICAL: Decimal("50000"),
    }

    # Time stuck thresholds (in seconds)
    TIME_STUCK_THRESHOLDS = {
        Severity.LOW: 60,  # 1 minute
        Severity.MEDIUM: 300,  # 5 minutes
        Severity.HIGH: 900,  # 15 minutes
        Severity.CRITICAL: 1800,  # 30 minutes
    }

    # Auto-remediation delay (in seconds)
    AUTO_REMEDIATION_DELAY = 120  # 2 minutes

    def __init__(self, dashboard_base_url: str | None = None) -> None:
        """Initialize the generator.

        Args:
            dashboard_base_url: Base URL for dashboard links in risk descriptions.
        """
        self.dashboard_base_url = dashboard_base_url

    def generate_card(
        self,
        strategy_state: StrategyState,
        error_context: ErrorContext | None = None,
        event_type: EventType = EventType.STUCK,
    ) -> OperatorCard:
        """Generate an OperatorCard from strategy state and error context.

        Args:
            strategy_state: Current state of the strategy.
            error_context: Optional context about the error that triggered this.
            event_type: Type of event (defaults to STUCK).

        Returns:
            A fully populated OperatorCard.
        """
        # Auto-detect stuck reason
        reason = self._detect_reason(strategy_state, error_context)

        # Build position summary
        position_summary = self._build_position_summary(strategy_state)

        # Calculate severity
        severity = self._calculate_severity(strategy_state, position_summary, reason)

        # Get suggested actions from REMEDIATION_MAP
        suggested_actions = self._get_suggested_actions(reason, strategy_state)

        # Get available actions
        available_actions = self._get_available_actions(reason)

        # Build context dict
        context = self._build_context(strategy_state, error_context)

        # Generate risk description
        risk_description = self._generate_risk_description(strategy_state, position_summary, reason, severity)

        # Set up auto-remediation if applicable
        auto_remediation = self._setup_auto_remediation(reason, suggested_actions)

        return OperatorCard(
            strategy_id=strategy_state.strategy_id,
            timestamp=datetime.now(UTC),
            event_type=event_type,
            reason=reason,
            context=context,
            severity=severity,
            position_summary=position_summary,
            risk_description=risk_description,
            suggested_actions=suggested_actions,
            available_actions=available_actions,
            auto_remediation=auto_remediation,
        )

    def _detect_reason(
        self,
        strategy_state: StrategyState,
        error_context: ErrorContext | None,
    ) -> StuckReason:
        """Auto-detect StuckReason from error type and context.

        Detection priority:
        1. Exact match on error_type
        2. Keyword match in error_message
        3. Inference from context fields
        4. Default to UNKNOWN
        """
        if error_context:
            # Try exact match on error type
            if error_context.error_type in ERROR_TYPE_TO_REASON:
                return ERROR_TYPE_TO_REASON[error_context.error_type]

            # Try keyword match in error message
            error_msg_lower = error_context.error_message.lower()
            for keyword, reason in ERROR_MESSAGE_KEYWORDS.items():
                if keyword in error_msg_lower:
                    return reason

            # Infer from context fields
            if error_context.allowance is not None and error_context.required_allowance is not None:
                if error_context.allowance < error_context.required_allowance:
                    return StuckReason.ALLOWANCE_MISSING

            if error_context.balance is not None and error_context.required_balance is not None:
                if error_context.balance < error_context.required_balance:
                    return StuckReason.INSUFFICIENT_BALANCE

            if error_context.slippage_actual is not None and error_context.slippage_max is not None:
                if error_context.slippage_actual > error_context.slippage_max:
                    return StuckReason.SLIPPAGE_EXCEEDED

            if error_context.oracle_timestamp is not None:
                age = (datetime.now(UTC) - error_context.oracle_timestamp).total_seconds()
                if age > 3600:  # Oracle older than 1 hour
                    return StuckReason.ORACLE_STALE

            if error_context.rpc_error:
                return StuckReason.RPC_FAILURE

            if error_context.protocol_status == "paused":
                return StuckReason.PROTOCOL_PAUSED

            if error_context.revert_reason:
                return StuckReason.TRANSACTION_REVERTED

        # Check strategy state for clues
        if strategy_state.pending_tx_hash and strategy_state.pending_tx_gas_price:
            if strategy_state.current_gas_price:
                if strategy_state.pending_tx_gas_price < strategy_state.current_gas_price:
                    return StuckReason.GAS_PRICE_BLOCKED

        if strategy_state.pool_liquidity_usd is not None:
            if strategy_state.pool_liquidity_usd < Decimal("1000"):
                return StuckReason.POOL_LIQUIDITY_LOW

        return StuckReason.UNKNOWN

    def _build_position_summary(self, strategy_state: StrategyState) -> PositionSummary:
        """Build a PositionSummary from strategy state."""
        return PositionSummary(
            total_value_usd=strategy_state.total_value_usd,
            available_balance_usd=strategy_state.available_balance_usd,
            lp_value_usd=strategy_state.lp_value_usd,
            borrowed_value_usd=strategy_state.borrowed_value_usd,
            collateral_value_usd=strategy_state.collateral_value_usd,
            token_balances=strategy_state.token_balances or {},
            lp_positions=strategy_state.lp_positions or [],
            health_factor=strategy_state.health_factor,
            leverage=strategy_state.leverage,
        )

    def _calculate_severity(
        self,
        strategy_state: StrategyState,
        position_summary: PositionSummary,
        reason: StuckReason,
    ) -> Severity:
        """Calculate severity based on position at risk and time stuck.

        Severity is the maximum of:
        - Severity based on position value at risk
        - Severity based on time stuck
        - Baseline severity for certain critical reasons
        """
        # Calculate position at risk
        position_at_risk = position_summary.total_value_usd
        if position_summary.lp_value_usd > 0:
            # LP positions have higher risk due to impermanent loss
            position_at_risk = position_summary.total_value_usd * Decimal("1.2")
        if position_summary.borrowed_value_usd > 0:
            # Leveraged positions have even higher risk
            position_at_risk = position_summary.total_value_usd * Decimal("1.5")

        # Determine severity from position
        position_severity = Severity.LOW
        for sev_level, pos_threshold in sorted(self.POSITION_RISK_THRESHOLDS.items(), key=lambda x: x[1], reverse=True):
            if position_at_risk >= pos_threshold:
                position_severity = sev_level
                break

        # Determine severity from time stuck
        time_severity = Severity.LOW
        if strategy_state.stuck_since:
            seconds_stuck = (datetime.now(UTC) - strategy_state.stuck_since).total_seconds()
            for sev_level, time_threshold in sorted(
                self.TIME_STUCK_THRESHOLDS.items(), key=lambda x: x[1], reverse=True
            ):
                if seconds_stuck >= time_threshold:
                    time_severity = sev_level
                    break

        # Certain reasons have minimum severity
        reason_severity_map: dict[StuckReason, Severity] = {
            StuckReason.CIRCUIT_BREAKER: Severity.CRITICAL,
            StuckReason.STATE_CONFLICT: Severity.HIGH,
            StuckReason.TRANSACTION_REVERTED: Severity.MEDIUM,
            StuckReason.NONCE_CONFLICT: Severity.MEDIUM,
        }
        reason_severity = reason_severity_map.get(reason, Severity.LOW)

        # Return the maximum severity
        severity_order = [Severity.LOW, Severity.MEDIUM, Severity.HIGH, Severity.CRITICAL]
        return max([position_severity, time_severity, reason_severity], key=lambda s: severity_order.index(s))

    def _get_suggested_actions(
        self,
        reason: StuckReason,
        strategy_state: StrategyState,
    ) -> list[SuggestedAction]:
        """Get suggested actions from REMEDIATION_MAP.

        May customize actions based on strategy state.
        """
        actions = REMEDIATION_MAP.get(reason, [])

        if not actions:
            # Default fallback
            actions = [
                SuggestedAction(
                    action=AvailableAction.PAUSE,
                    description="Pause strategy for manual investigation",
                    priority=1,
                    is_recommended=True,
                )
            ]

        # Customize actions based on state
        customized_actions = []
        for action in actions:
            # Create a copy with potentially updated params
            params = dict(action.params)

            # Add gas price suggestion for BUMP_GAS actions
            if action.action == AvailableAction.BUMP_GAS:
                if strategy_state.current_gas_price:
                    suggested_gas = int(strategy_state.current_gas_price * 1.25)
                    params["suggested_gas_price"] = suggested_gas
                if strategy_state.pending_tx_hash:
                    params["tx_hash"] = strategy_state.pending_tx_hash

            # Add tx hash for CANCEL_TX actions
            if action.action == AvailableAction.CANCEL_TX:
                if strategy_state.pending_tx_hash:
                    params["tx_hash"] = strategy_state.pending_tx_hash

            customized_actions.append(
                SuggestedAction(
                    action=action.action,
                    description=action.description,
                    priority=action.priority,
                    params=params,
                    is_recommended=action.is_recommended,
                )
            )

        return customized_actions

    def _get_available_actions(self, reason: StuckReason) -> list[AvailableAction]:
        """Get list of available actions based on stuck reason."""
        # All actions that appear in suggested actions for this reason
        suggested = REMEDIATION_MAP.get(reason, [])
        actions = [s.action for s in suggested]

        # Always include PAUSE if not already present
        if AvailableAction.PAUSE not in actions:
            actions.append(AvailableAction.PAUSE)

        # For critical reasons, include EMERGENCY_UNWIND
        if reason == StuckReason.CIRCUIT_BREAKER:
            if AvailableAction.EMERGENCY_UNWIND not in actions:
                actions.append(AvailableAction.EMERGENCY_UNWIND)

        return actions

    def _build_context(
        self,
        strategy_state: StrategyState,
        error_context: ErrorContext | None,
    ) -> dict[str, Any]:
        """Build context dict with transaction, market, and position info."""
        context: dict[str, Any] = {
            "strategy_status": strategy_state.status,
        }

        # Transaction details
        if strategy_state.pending_tx_hash:
            context["transaction"] = {
                "hash": strategy_state.pending_tx_hash,
                "gas_price": strategy_state.pending_tx_gas_price,
                "current_gas_price": strategy_state.current_gas_price,
            }

        # Market conditions
        if strategy_state.current_price is not None:
            context["market"] = {
                "current_price": str(strategy_state.current_price),
                "price_24h_ago": str(strategy_state.price_24h_ago) if strategy_state.price_24h_ago else None,
                "pool_liquidity_usd": str(strategy_state.pool_liquidity_usd)
                if strategy_state.pool_liquidity_usd
                else None,
            }

        # Time stuck
        if strategy_state.stuck_since:
            context["stuck_since"] = strategy_state.stuck_since.isoformat()
            context["seconds_stuck"] = (datetime.now(UTC) - strategy_state.stuck_since).total_seconds()

        if strategy_state.last_successful_action:
            context["last_successful_action"] = strategy_state.last_successful_action.isoformat()

        # Error details
        if error_context:
            context["error"] = {
                "type": error_context.error_type,
                "message": error_context.error_message,
            }
            if error_context.tx_hash:
                context["error"]["tx_hash"] = error_context.tx_hash
            if error_context.revert_reason:
                context["error"]["revert_reason"] = error_context.revert_reason
            if error_context.gas_used is not None:
                context["error"]["gas_used"] = error_context.gas_used
            if error_context.gas_limit is not None:
                context["error"]["gas_limit"] = error_context.gas_limit
            if error_context.slippage_actual is not None:
                context["error"]["slippage_actual"] = str(error_context.slippage_actual)
            if error_context.slippage_max is not None:
                context["error"]["slippage_max"] = str(error_context.slippage_max)

        return context

    def _generate_risk_description(
        self,
        strategy_state: StrategyState,
        position_summary: PositionSummary,
        reason: StuckReason,
        severity: Severity,
    ) -> str:
        """Generate human-readable risk description."""
        parts = []

        # Position at risk
        parts.append(f"Position at risk: ${position_summary.total_value_usd:,.2f} USD")

        # LP exposure
        if position_summary.lp_value_usd > 0:
            parts.append(f"LP exposure: ${position_summary.lp_value_usd:,.2f} USD (subject to impermanent loss)")

        # Borrowed value
        if position_summary.borrowed_value_usd > 0:
            parts.append(f"Borrowed: ${position_summary.borrowed_value_usd:,.2f} USD")
            if position_summary.health_factor:
                parts.append(f"Health factor: {position_summary.health_factor:.2f}")

        # Time stuck
        if strategy_state.stuck_since:
            minutes_stuck = (datetime.now(UTC) - strategy_state.stuck_since).total_seconds() / 60
            parts.append(f"Stuck for: {minutes_stuck:.1f} minutes")

        # Reason-specific risks
        risk_descriptions: dict[StuckReason, str] = {
            StuckReason.GAS_PRICE_BLOCKED: "Transaction pending - position cannot react to market changes.",
            StuckReason.SLIPPAGE_EXCEEDED: "Market volatility high - trades may execute at unfavorable prices.",
            StuckReason.POOL_LIQUIDITY_LOW: "Low liquidity - may not be able to exit position.",
            StuckReason.CIRCUIT_BREAKER: "Circuit breaker triggered - significant losses may have occurred.",
            StuckReason.TRANSACTION_REVERTED: "Transaction failed - intended action did not execute.",
            StuckReason.ORACLE_STALE: "Stale oracle data - protocol may reject transactions.",
        }
        if reason in risk_descriptions:
            parts.append(risk_descriptions[reason])

        return " | ".join(parts)

    def _setup_auto_remediation(
        self,
        reason: StuckReason,
        suggested_actions: list[SuggestedAction],
    ) -> AutoRemediation | None:
        """Set up auto-remediation if the reason is auto-remediable."""
        if reason not in AUTO_REMEDIABLE:
            return None

        # Get the recommended action
        recommended = next(
            (a for a in suggested_actions if a.is_recommended), suggested_actions[0] if suggested_actions else None
        )

        if not recommended:
            return None

        return AutoRemediation(
            enabled=True,
            action=recommended.action,
            trigger_after_seconds=self.AUTO_REMEDIATION_DELAY,
            max_attempts=3,
            current_attempt=0,
            scheduled_at=datetime.now(UTC),
        )
