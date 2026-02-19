"""Cross-Chain Risk Guard for multi-chain strategy execution.

This module provides the CrossChainRiskGuard class for validating multi-chain
intents before execution. It enforces risk limits across chains including:

- Total exposure aggregation across all chains
- Per-chain limits alongside cross-chain limits
- Single bridge transfer limits
- Daily bridging limits
- Minimum balance retention per chain
- In-flight exposure caps
- Bridge protocol allowlist enforcement

Example:
    from almanak.framework.execution.risk_guards import CrossChainRiskGuard, CrossChainRiskConfig

    config = CrossChainRiskConfig(
        max_single_bridge_usd=Decimal("10000"),
        max_daily_bridge_usd=Decimal("100000"),
        min_balance_retention_usd=Decimal("100"),
        max_in_flight_exposure_usd=Decimal("50000"),
        allowed_bridges=["across", "stargate"],
    )

    guard = CrossChainRiskGuard(config)
    result = guard.validate_intent(intent, context)

    if not result.passed:
        print(f"Risk check failed: {result.violations}")
"""

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from ..intents.bridge import BridgeIntent
from ..intents.vocabulary import (
    AnyIntent,
    BorrowIntent,
    HoldIntent,
    IntentSequence,
    SupplyIntent,
    SwapIntent,
    WithdrawIntent,
)

if TYPE_CHECKING:
    from ..state.in_flight import InFlightSummary

logger = logging.getLogger(__name__)


# =============================================================================
# Configuration
# =============================================================================


@dataclass
class CrossChainRiskConfig:
    """Configuration for cross-chain risk limits.

    All limits are configurable to allow strategies to set appropriate
    risk parameters based on their specific requirements.

    Attributes:
        max_single_bridge_usd: Maximum USD value for a single bridge transfer
        max_daily_bridge_usd: Maximum total USD bridged in a 24-hour period
        min_balance_retention_usd: Minimum USD balance to retain on each chain
        min_balance_retention_pct: Minimum percentage of balance to retain (0-1)
        max_in_flight_exposure_usd: Maximum USD in-flight during bridging
        max_total_exposure_usd: Maximum total exposure across all chains
        per_chain_max_exposure_usd: Maximum exposure per chain (dict by chain name)
        allowed_bridges: List of allowed bridge protocol names
        max_position_concentration_pct: Maximum percentage in single position (0-1)
    """

    max_single_bridge_usd: Decimal = field(default_factory=lambda: Decimal("50000"))
    max_daily_bridge_usd: Decimal = field(default_factory=lambda: Decimal("500000"))
    min_balance_retention_usd: Decimal = field(default_factory=lambda: Decimal("100"))
    min_balance_retention_pct: Decimal = field(default_factory=lambda: Decimal("0.01"))
    max_in_flight_exposure_usd: Decimal = field(default_factory=lambda: Decimal("100000"))
    max_total_exposure_usd: Decimal = field(default_factory=lambda: Decimal("10000000"))
    per_chain_max_exposure_usd: dict[str, Decimal] = field(default_factory=dict)
    allowed_bridges: list[str] = field(default_factory=list)
    max_position_concentration_pct: Decimal = field(default_factory=lambda: Decimal("0.5"))

    def to_dict(self) -> dict[str, Any]:
        """Serialize configuration to dictionary."""
        return {
            "max_single_bridge_usd": str(self.max_single_bridge_usd),
            "max_daily_bridge_usd": str(self.max_daily_bridge_usd),
            "min_balance_retention_usd": str(self.min_balance_retention_usd),
            "min_balance_retention_pct": str(self.min_balance_retention_pct),
            "max_in_flight_exposure_usd": str(self.max_in_flight_exposure_usd),
            "max_total_exposure_usd": str(self.max_total_exposure_usd),
            "per_chain_max_exposure_usd": {k: str(v) for k, v in self.per_chain_max_exposure_usd.items()},
            "allowed_bridges": self.allowed_bridges,
            "max_position_concentration_pct": str(self.max_position_concentration_pct),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "CrossChainRiskConfig":
        """Deserialize configuration from dictionary."""
        per_chain = data.get("per_chain_max_exposure_usd", {})
        return cls(
            max_single_bridge_usd=Decimal(str(data.get("max_single_bridge_usd", "50000"))),
            max_daily_bridge_usd=Decimal(str(data.get("max_daily_bridge_usd", "500000"))),
            min_balance_retention_usd=Decimal(str(data.get("min_balance_retention_usd", "100"))),
            min_balance_retention_pct=Decimal(str(data.get("min_balance_retention_pct", "0.01"))),
            max_in_flight_exposure_usd=Decimal(str(data.get("max_in_flight_exposure_usd", "100000"))),
            max_total_exposure_usd=Decimal(str(data.get("max_total_exposure_usd", "10000000"))),
            per_chain_max_exposure_usd={k: Decimal(str(v)) for k, v in per_chain.items()},
            allowed_bridges=data.get("allowed_bridges", []),
            max_position_concentration_pct=Decimal(str(data.get("max_position_concentration_pct", "0.5"))),
        )


# =============================================================================
# Risk Context
# =============================================================================


@dataclass
class ChainBalance:
    """Balance information for a single chain.

    Attributes:
        chain: Chain name
        total_balance_usd: Total USD balance on this chain
        available_balance_usd: Available (non-locked) USD balance
    """

    chain: str
    total_balance_usd: Decimal
    available_balance_usd: Decimal = field(default_factory=lambda: Decimal("0"))

    def __post_init__(self) -> None:
        """Initialize available balance if not set."""
        if self.available_balance_usd == Decimal("0"):
            self.available_balance_usd = self.total_balance_usd


@dataclass
class InFlightTransfer:
    """Represents an in-flight bridge transfer.

    Attributes:
        transfer_id: Unique identifier for the transfer
        token: Token being transferred
        amount_usd: USD value of the transfer
        from_chain: Source chain
        to_chain: Destination chain
        bridge: Bridge protocol used
        initiated_at: When the transfer was initiated
    """

    transfer_id: str
    token: str
    amount_usd: Decimal
    from_chain: str
    to_chain: str
    bridge: str
    initiated_at: datetime = field(default_factory=lambda: datetime.now(UTC))


@dataclass
class BridgeHistoryEntry:
    """A single bridge transfer history entry for daily tracking.

    Attributes:
        timestamp: When the bridge was executed
        amount_usd: USD value bridged
        from_chain: Source chain
        to_chain: Destination chain
        bridge: Bridge protocol used
    """

    timestamp: datetime
    amount_usd: Decimal
    from_chain: str
    to_chain: str
    bridge: str


@dataclass
class RiskContext:
    """Context for risk validation containing current portfolio state.

    This provides all the information the risk guard needs to validate
    intents against current positions and exposure.

    Attributes:
        chain_balances: Balance information per chain
        total_exposure_usd: Current total exposure across all chains
        in_flight_transfers: List of transfers currently in-flight
        bridge_history_24h: Bridge transfers in the last 24 hours
        per_chain_exposure_usd: Current exposure per chain
        timestamp: When this context was created
    """

    chain_balances: dict[str, ChainBalance] = field(default_factory=dict)
    total_exposure_usd: Decimal = field(default_factory=lambda: Decimal("0"))
    in_flight_transfers: list[InFlightTransfer] = field(default_factory=list)
    bridge_history_24h: list[BridgeHistoryEntry] = field(default_factory=list)
    per_chain_exposure_usd: dict[str, Decimal] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))

    @property
    def total_in_flight_usd(self) -> Decimal:
        """Calculate total USD currently in-flight."""
        return sum((t.amount_usd for t in self.in_flight_transfers), Decimal("0"))

    @property
    def total_bridged_24h_usd(self) -> Decimal:
        """Calculate total USD bridged in the last 24 hours."""
        cutoff = datetime.now(UTC) - timedelta(hours=24)
        return sum(
            (e.amount_usd for e in self.bridge_history_24h if e.timestamp >= cutoff),
            Decimal("0"),
        )

    def get_chain_balance(self, chain: str) -> ChainBalance | None:
        """Get balance for a specific chain."""
        return self.chain_balances.get(chain)

    def to_dict(self) -> dict[str, Any]:
        """Serialize context to dictionary."""
        return {
            "chain_balances": {
                k: {
                    "chain": v.chain,
                    "total_balance_usd": str(v.total_balance_usd),
                    "available_balance_usd": str(v.available_balance_usd),
                }
                for k, v in self.chain_balances.items()
            },
            "total_exposure_usd": str(self.total_exposure_usd),
            "in_flight_transfers": [
                {
                    "transfer_id": t.transfer_id,
                    "token": t.token,
                    "amount_usd": str(t.amount_usd),
                    "from_chain": t.from_chain,
                    "to_chain": t.to_chain,
                    "bridge": t.bridge,
                    "initiated_at": t.initiated_at.isoformat(),
                }
                for t in self.in_flight_transfers
            ],
            "total_in_flight_usd": str(self.total_in_flight_usd),
            "total_bridged_24h_usd": str(self.total_bridged_24h_usd),
            "per_chain_exposure_usd": {k: str(v) for k, v in self.per_chain_exposure_usd.items()},
            "timestamp": self.timestamp.isoformat(),
        }


# =============================================================================
# Validation Result
# =============================================================================


@dataclass
class RiskViolation:
    """A single risk limit violation.

    Attributes:
        rule: Name of the risk rule that was violated
        message: Human-readable description of the violation
        current_value: Current value that triggered the violation
        limit_value: The limit that was exceeded
        chain: Chain involved (if applicable)
        severity: Severity level (warning, error)
    """

    rule: str
    message: str
    current_value: Decimal
    limit_value: Decimal
    chain: str | None = None
    severity: str = "error"

    def to_dict(self) -> dict[str, Any]:
        """Serialize violation to dictionary."""
        return {
            "rule": self.rule,
            "message": self.message,
            "current_value": str(self.current_value),
            "limit_value": str(self.limit_value),
            "chain": self.chain,
            "severity": self.severity,
        }


@dataclass
class CrossChainRiskResult:
    """Result of cross-chain risk validation.

    Attributes:
        passed: Whether all risk checks passed
        violations: List of risk violations if any
        warnings: List of non-blocking warnings
        checked_rules: List of rules that were checked
        intent_value_usd: Estimated USD value of the intent
    """

    passed: bool
    violations: list[RiskViolation] = field(default_factory=list)
    warnings: list[RiskViolation] = field(default_factory=list)
    checked_rules: list[str] = field(default_factory=list)
    intent_value_usd: Decimal = field(default_factory=lambda: Decimal("0"))

    def add_violation(self, violation: RiskViolation) -> None:
        """Add a violation and mark as failed."""
        self.passed = False
        if violation.severity == "warning":
            self.warnings.append(violation)
        else:
            self.violations.append(violation)

    def to_dict(self) -> dict[str, Any]:
        """Serialize result to dictionary."""
        return {
            "passed": self.passed,
            "violations": [v.to_dict() for v in self.violations],
            "warnings": [w.to_dict() for w in self.warnings],
            "checked_rules": self.checked_rules,
            "intent_value_usd": str(self.intent_value_usd),
        }

    def format_message(self) -> str:
        """Format result as a human-readable message."""
        if self.passed:
            return "All risk checks passed"

        messages = []
        for v in self.violations:
            messages.append(f"[{v.severity.upper()}] {v.rule}: {v.message}")
        for w in self.warnings:
            messages.append(f"[WARNING] {w.rule}: {w.message}")
        return "\n".join(messages)


# =============================================================================
# Cross-Chain Risk Guard
# =============================================================================


class CrossChainRiskGuard:
    """Validates multi-chain intents before execution.

    CrossChainRiskGuard enforces risk limits across chains to prevent
    dangerous operations. It aggregates positions across chains for
    total exposure calculations and enforces both per-chain and
    cross-chain limits.

    Key validation rules:
    - Maximum single bridge transfer amount
    - Maximum total daily bridge volume
    - Minimum balance retention per chain
    - Maximum in-flight exposure
    - Bridge protocol allowlist
    - Total and per-chain exposure limits

    Example:
        guard = CrossChainRiskGuard(config)

        # Validate a single intent
        result = guard.validate_intent(bridge_intent, context)

        # Validate multiple intents
        result = guard.validate_intents([intent1, intent2], context)

        if not result.passed:
            for violation in result.violations:
                print(f"{violation.rule}: {violation.message}")
    """

    def __init__(self, config: CrossChainRiskConfig | None = None) -> None:
        """Initialize the cross-chain risk guard.

        Args:
            config: Risk configuration (uses defaults if not provided)
        """
        self.config = config or CrossChainRiskConfig()
        logger.info(
            f"CrossChainRiskGuard initialized with "
            f"max_single_bridge=${self.config.max_single_bridge_usd}, "
            f"max_daily_bridge=${self.config.max_daily_bridge_usd}, "
            f"allowed_bridges={self.config.allowed_bridges or 'any'}"
        )

    def validate_intent(
        self,
        intent: AnyIntent | BridgeIntent | IntentSequence,
        context: RiskContext,
        intent_value_usd: Decimal | None = None,
    ) -> CrossChainRiskResult:
        """Validate a single intent or intent sequence against risk limits.

        Args:
            intent: The intent to validate (can be any intent type or sequence)
            context: Current risk context with positions and exposure
            intent_value_usd: Optional pre-calculated USD value of the intent

        Returns:
            CrossChainRiskResult indicating if validation passed
        """
        result = CrossChainRiskResult(passed=True)

        # Handle sequences by validating all intents in the sequence
        if isinstance(intent, IntentSequence):
            for seq_intent in intent.intents:
                seq_result = self.validate_intent(seq_intent, context, intent_value_usd)
                if not seq_result.passed:
                    result.passed = False
                result.violations.extend(seq_result.violations)
                result.warnings.extend(seq_result.warnings)
                result.checked_rules.extend(seq_result.checked_rules)
            return result

        # Route to specific validator based on intent type
        if isinstance(intent, BridgeIntent):
            return self._validate_bridge_intent(intent, context, intent_value_usd)
        elif isinstance(intent, SwapIntent | SupplyIntent | WithdrawIntent | BorrowIntent):
            return self._validate_execution_intent(intent, context, intent_value_usd)
        elif isinstance(intent, HoldIntent):
            # Hold intents always pass
            result.checked_rules.append("hold_passthrough")
            return result
        else:
            # Other intent types - basic validation
            return self._validate_general_intent(intent, context, intent_value_usd)

    def validate_intents(
        self,
        intents: list[AnyIntent | BridgeIntent | IntentSequence],
        context: RiskContext,
    ) -> CrossChainRiskResult:
        """Validate multiple intents against risk limits.

        This validates each intent individually and also checks cumulative
        impact (e.g., multiple bridge intents summing to exceed daily limit).

        Args:
            intents: List of intents to validate
            context: Current risk context with positions and exposure

        Returns:
            CrossChainRiskResult with aggregated validation results
        """
        result = CrossChainRiskResult(passed=True)

        # Track cumulative bridge value for daily limit check
        cumulative_bridge_usd = Decimal("0")

        for intent in intents:
            intent_result = self.validate_intent(intent, context)

            # Aggregate results
            if not intent_result.passed:
                result.passed = False
            result.violations.extend(intent_result.violations)
            result.warnings.extend(intent_result.warnings)
            result.checked_rules.extend(intent_result.checked_rules)
            result.intent_value_usd += intent_result.intent_value_usd

            # Track bridge transfers for cumulative check
            if isinstance(intent, BridgeIntent):
                cumulative_bridge_usd += intent_result.intent_value_usd

        # Check cumulative daily bridge limit
        total_with_pending = context.total_bridged_24h_usd + cumulative_bridge_usd
        if total_with_pending > self.config.max_daily_bridge_usd:
            result.add_violation(
                RiskViolation(
                    rule="cumulative_daily_bridge_limit",
                    message=(
                        f"Cumulative bridge amount ${cumulative_bridge_usd} would exceed "
                        f"daily limit of ${self.config.max_daily_bridge_usd} "
                        f"(already bridged: ${context.total_bridged_24h_usd})"
                    ),
                    current_value=total_with_pending,
                    limit_value=self.config.max_daily_bridge_usd,
                )
            )

        result.checked_rules.append("cumulative_daily_bridge_limit")
        return result

    def _validate_bridge_intent(
        self,
        intent: BridgeIntent,
        context: RiskContext,
        intent_value_usd: Decimal | None = None,
    ) -> CrossChainRiskResult:
        """Validate a bridge intent against bridge-specific risk limits.

        Checks:
        - Single bridge transfer limit
        - Daily bridge volume limit
        - In-flight exposure cap
        - Source chain balance retention
        - Bridge protocol allowlist

        Args:
            intent: Bridge intent to validate
            context: Current risk context
            intent_value_usd: Optional pre-calculated USD value

        Returns:
            CrossChainRiskResult for the bridge intent
        """
        result = CrossChainRiskResult(passed=True)

        # Use provided value or estimate from intent
        amount_usd = intent_value_usd or self._estimate_bridge_value_usd(intent)
        result.intent_value_usd = amount_usd

        # Rule 1: Single bridge transfer limit
        result.checked_rules.append("single_bridge_limit")
        if amount_usd > self.config.max_single_bridge_usd:
            result.add_violation(
                RiskViolation(
                    rule="single_bridge_limit",
                    message=(
                        f"Bridge amount ${amount_usd} exceeds single transfer limit "
                        f"of ${self.config.max_single_bridge_usd}"
                    ),
                    current_value=amount_usd,
                    limit_value=self.config.max_single_bridge_usd,
                    chain=intent.from_chain,
                )
            )

        # Rule 2: Daily bridge volume limit
        result.checked_rules.append("daily_bridge_limit")
        total_bridged_with_current = context.total_bridged_24h_usd + amount_usd
        if total_bridged_with_current > self.config.max_daily_bridge_usd:
            result.add_violation(
                RiskViolation(
                    rule="daily_bridge_limit",
                    message=(
                        f"Bridge amount ${amount_usd} would exceed daily limit "
                        f"of ${self.config.max_daily_bridge_usd} "
                        f"(already bridged: ${context.total_bridged_24h_usd})"
                    ),
                    current_value=total_bridged_with_current,
                    limit_value=self.config.max_daily_bridge_usd,
                )
            )

        # Rule 3: In-flight exposure cap
        result.checked_rules.append("in_flight_exposure_limit")
        total_in_flight_with_current = context.total_in_flight_usd + amount_usd
        if total_in_flight_with_current > self.config.max_in_flight_exposure_usd:
            result.add_violation(
                RiskViolation(
                    rule="in_flight_exposure_limit",
                    message=(
                        f"Bridge amount ${amount_usd} would exceed in-flight exposure limit "
                        f"of ${self.config.max_in_flight_exposure_usd} "
                        f"(currently in-flight: ${context.total_in_flight_usd})"
                    ),
                    current_value=total_in_flight_with_current,
                    limit_value=self.config.max_in_flight_exposure_usd,
                )
            )

        # Rule 4: Minimum balance retention on source chain
        result.checked_rules.append("min_balance_retention")
        source_balance = context.get_chain_balance(intent.from_chain)
        if source_balance:
            remaining_balance = source_balance.available_balance_usd - amount_usd

            # Check absolute minimum
            if remaining_balance < self.config.min_balance_retention_usd:
                result.add_violation(
                    RiskViolation(
                        rule="min_balance_retention",
                        message=(
                            f"Bridge would leave only ${remaining_balance} on {intent.from_chain}, "
                            f"below minimum retention of ${self.config.min_balance_retention_usd}"
                        ),
                        current_value=remaining_balance,
                        limit_value=self.config.min_balance_retention_usd,
                        chain=intent.from_chain,
                    )
                )

            # Check percentage minimum
            if source_balance.total_balance_usd > Decimal("0"):
                remaining_pct = remaining_balance / source_balance.total_balance_usd
                if remaining_pct < self.config.min_balance_retention_pct:
                    result.add_violation(
                        RiskViolation(
                            rule="min_balance_retention_pct",
                            message=(
                                f"Bridge would leave only {remaining_pct:.1%} of balance on {intent.from_chain}, "
                                f"below minimum retention of {self.config.min_balance_retention_pct:.1%}"
                            ),
                            current_value=remaining_pct,
                            limit_value=self.config.min_balance_retention_pct,
                            chain=intent.from_chain,
                        )
                    )

        # Rule 5: Bridge protocol allowlist
        result.checked_rules.append("bridge_allowlist")
        if self.config.allowed_bridges:
            preferred_bridge = intent.preferred_bridge
            if preferred_bridge and preferred_bridge.lower() not in [b.lower() for b in self.config.allowed_bridges]:
                result.add_violation(
                    RiskViolation(
                        rule="bridge_allowlist",
                        message=(
                            f"Bridge '{preferred_bridge}' is not in the allowed list: {self.config.allowed_bridges}"
                        ),
                        current_value=Decimal("0"),
                        limit_value=Decimal("0"),
                    )
                )

        logger.debug(
            f"Bridge intent validation: from={intent.from_chain}, to={intent.to_chain}, "
            f"amount=${amount_usd}, passed={result.passed}"
        )

        return result

    def _validate_execution_intent(
        self,
        intent: SwapIntent | SupplyIntent | WithdrawIntent | BorrowIntent,
        context: RiskContext,
        intent_value_usd: Decimal | None = None,
    ) -> CrossChainRiskResult:
        """Validate execution intents (swap, supply, withdraw, borrow).

        Checks:
        - Total exposure limits
        - Per-chain exposure limits
        - Position concentration limits

        Args:
            intent: Execution intent to validate
            context: Current risk context
            intent_value_usd: Optional pre-calculated USD value

        Returns:
            CrossChainRiskResult for the execution intent
        """
        result = CrossChainRiskResult(passed=True)

        # Use provided value or estimate
        amount_usd = intent_value_usd or self._estimate_intent_value_usd(intent)
        result.intent_value_usd = amount_usd

        # Get chain from intent
        chain = getattr(intent, "chain", None)

        # Rule 1: Total exposure limit
        result.checked_rules.append("total_exposure_limit")
        new_total_exposure = context.total_exposure_usd + amount_usd
        if new_total_exposure > self.config.max_total_exposure_usd:
            result.add_violation(
                RiskViolation(
                    rule="total_exposure_limit",
                    message=(
                        f"Intent amount ${amount_usd} would exceed total exposure limit "
                        f"of ${self.config.max_total_exposure_usd} "
                        f"(current exposure: ${context.total_exposure_usd})"
                    ),
                    current_value=new_total_exposure,
                    limit_value=self.config.max_total_exposure_usd,
                )
            )

        # Rule 2: Per-chain exposure limit
        if chain and chain in self.config.per_chain_max_exposure_usd:
            result.checked_rules.append(f"per_chain_exposure_limit_{chain}")
            chain_limit = self.config.per_chain_max_exposure_usd[chain]
            current_chain_exposure = context.per_chain_exposure_usd.get(chain, Decimal("0"))
            new_chain_exposure = current_chain_exposure + amount_usd

            if new_chain_exposure > chain_limit:
                result.add_violation(
                    RiskViolation(
                        rule=f"per_chain_exposure_limit_{chain}",
                        message=(
                            f"Intent amount ${amount_usd} would exceed {chain} exposure limit "
                            f"of ${chain_limit} (current: ${current_chain_exposure})"
                        ),
                        current_value=new_chain_exposure,
                        limit_value=chain_limit,
                        chain=chain,
                    )
                )

        # Rule 3: Position concentration limit
        result.checked_rules.append("position_concentration_limit")
        if context.total_exposure_usd > Decimal("0"):
            concentration = amount_usd / (context.total_exposure_usd + amount_usd)
            if concentration > self.config.max_position_concentration_pct:
                result.add_violation(
                    RiskViolation(
                        rule="position_concentration_limit",
                        message=(
                            f"Intent would create {concentration:.1%} concentration, "
                            f"exceeding limit of {self.config.max_position_concentration_pct:.1%}"
                        ),
                        current_value=concentration,
                        limit_value=self.config.max_position_concentration_pct,
                        chain=chain,
                    )
                )

        logger.debug(
            f"Execution intent validation: type={type(intent).__name__}, "
            f"chain={chain}, amount=${amount_usd}, passed={result.passed}"
        )

        return result

    def _validate_general_intent(
        self,
        intent: AnyIntent,
        context: RiskContext,
        intent_value_usd: Decimal | None = None,
    ) -> CrossChainRiskResult:
        """Validate general intents with basic risk checks.

        Args:
            intent: Intent to validate
            context: Current risk context
            intent_value_usd: Optional pre-calculated USD value

        Returns:
            CrossChainRiskResult for the intent
        """
        result = CrossChainRiskResult(passed=True)

        amount_usd = intent_value_usd or Decimal("0")
        result.intent_value_usd = amount_usd

        # Basic total exposure check
        result.checked_rules.append("total_exposure_limit")
        new_total_exposure = context.total_exposure_usd + amount_usd
        if new_total_exposure > self.config.max_total_exposure_usd:
            result.add_violation(
                RiskViolation(
                    rule="total_exposure_limit",
                    message=(
                        f"Intent amount ${amount_usd} would exceed total exposure limit "
                        f"of ${self.config.max_total_exposure_usd}"
                    ),
                    current_value=new_total_exposure,
                    limit_value=self.config.max_total_exposure_usd,
                )
            )

        return result

    def _estimate_bridge_value_usd(self, intent: BridgeIntent) -> Decimal:
        """Estimate USD value of a bridge intent.

        This is a placeholder - in production, this would use price oracles.

        Args:
            intent: Bridge intent to estimate value for

        Returns:
            Estimated USD value
        """
        # If amount is "all", we can't estimate without more context
        if intent.is_chained_amount:
            logger.debug("Cannot estimate bridge value for amount='all'")
            return Decimal("0")

        # Simple estimation - in reality this would use price feeds
        # For now, return the amount directly (assuming 1:1 for stablecoins)
        if isinstance(intent.amount, Decimal):
            return intent.amount

        return Decimal("0")

    def _estimate_intent_value_usd(
        self,
        intent: SwapIntent | SupplyIntent | WithdrawIntent | BorrowIntent,
    ) -> Decimal:
        """Estimate USD value of an execution intent.

        Args:
            intent: Intent to estimate value for

        Returns:
            Estimated USD value
        """
        # Check for amount_usd directly (SwapIntent has this)
        if hasattr(intent, "amount_usd") and intent.amount_usd is not None:
            return intent.amount_usd

        # Check for amount field
        if hasattr(intent, "amount"):
            amount = intent.amount
            if isinstance(amount, Decimal):
                return amount

        # Check for collateral_amount (BorrowIntent)
        if hasattr(intent, "collateral_amount"):
            amount = intent.collateral_amount
            if isinstance(amount, Decimal):
                return amount

        return Decimal("0")

    def add_bridge_to_history(
        self,
        amount_usd: Decimal,
        from_chain: str,
        to_chain: str,
        bridge: str,
        context: RiskContext,
    ) -> None:
        """Add a completed bridge to the history for daily tracking.

        Args:
            amount_usd: USD value of the bridge
            from_chain: Source chain
            to_chain: Destination chain
            bridge: Bridge protocol used
            context: Risk context to update
        """
        entry = BridgeHistoryEntry(
            timestamp=datetime.now(UTC),
            amount_usd=amount_usd,
            from_chain=from_chain,
            to_chain=to_chain,
            bridge=bridge,
        )
        context.bridge_history_24h.append(entry)
        logger.debug(f"Added bridge to history: ${amount_usd} from {from_chain} to {to_chain} via {bridge}")

    def add_in_flight_transfer(
        self,
        transfer_id: str,
        token: str,
        amount_usd: Decimal,
        from_chain: str,
        to_chain: str,
        bridge: str,
        context: RiskContext,
    ) -> None:
        """Add a new in-flight transfer for tracking.

        Args:
            transfer_id: Unique identifier for the transfer
            token: Token being transferred
            amount_usd: USD value of the transfer
            from_chain: Source chain
            to_chain: Destination chain
            bridge: Bridge protocol used
            context: Risk context to update
        """
        transfer = InFlightTransfer(
            transfer_id=transfer_id,
            token=token,
            amount_usd=amount_usd,
            from_chain=from_chain,
            to_chain=to_chain,
            bridge=bridge,
        )
        context.in_flight_transfers.append(transfer)
        logger.info(f"Added in-flight transfer {transfer_id}: ${amount_usd} {token} from {from_chain} to {to_chain}")

    def remove_in_flight_transfer(
        self,
        transfer_id: str,
        context: RiskContext,
    ) -> bool:
        """Remove a completed in-flight transfer.

        Args:
            transfer_id: ID of the transfer to remove
            context: Risk context to update

        Returns:
            True if transfer was found and removed
        """
        for i, transfer in enumerate(context.in_flight_transfers):
            if transfer.transfer_id == transfer_id:
                removed = context.in_flight_transfers.pop(i)
                logger.info(f"Removed in-flight transfer {transfer_id}: ${removed.amount_usd}")
                return True
        return False

    def validate_with_in_flight_exposure(
        self,
        intent: AnyIntent | BridgeIntent | IntentSequence,
        context: RiskContext,
        in_flight_summary: "InFlightSummary",
        intent_value_usd: Decimal | None = None,
    ) -> CrossChainRiskResult:
        """Validate an intent considering in-flight exposure from tracker.

        This method integrates with InFlightExposureTracker to ensure
        risk limits consider current_exposure + in_flight_exposure.

        Args:
            intent: The intent to validate
            context: Current risk context with positions and exposure
            in_flight_summary: Summary from InFlightExposureTracker
            intent_value_usd: Optional pre-calculated USD value

        Returns:
            CrossChainRiskResult with in-flight exposure considered
        """
        # First run standard validation
        result = self.validate_intent(intent, context, intent_value_usd)

        # Add in-flight exposure check for total exposure
        result.checked_rules.append("in_flight_total_exposure")
        total_with_in_flight = context.total_exposure_usd + in_flight_summary.total_in_flight_usd

        if total_with_in_flight > self.config.max_total_exposure_usd:
            result.add_violation(
                RiskViolation(
                    rule="in_flight_total_exposure",
                    message=(
                        f"Total exposure including in-flight (${total_with_in_flight}) "
                        f"exceeds limit of ${self.config.max_total_exposure_usd} "
                        f"(current: ${context.total_exposure_usd}, "
                        f"in-flight: ${in_flight_summary.total_in_flight_usd})"
                    ),
                    current_value=total_with_in_flight,
                    limit_value=self.config.max_total_exposure_usd,
                )
            )

        # For bridge intents, add in-flight exposure from tracker to limit check
        if isinstance(intent, BridgeIntent):
            amount_usd = intent_value_usd or self._estimate_bridge_value_usd(intent)

            # Check combined in-flight exposure (context + tracker)
            result.checked_rules.append("combined_in_flight_limit")
            combined_in_flight = context.total_in_flight_usd + in_flight_summary.total_in_flight_usd + amount_usd

            if combined_in_flight > self.config.max_in_flight_exposure_usd:
                result.add_violation(
                    RiskViolation(
                        rule="combined_in_flight_limit",
                        message=(
                            f"Combined in-flight exposure (${combined_in_flight}) "
                            f"would exceed limit of ${self.config.max_in_flight_exposure_usd} "
                            f"(tracker: ${in_flight_summary.total_in_flight_usd}, "
                            f"new: ${amount_usd})"
                        ),
                        current_value=combined_in_flight,
                        limit_value=self.config.max_in_flight_exposure_usd,
                    )
                )

            # Add warning for stale transfers
            if in_flight_summary.stale_transfer_count > 0:
                result.warnings.append(
                    RiskViolation(
                        rule="stale_transfers_warning",
                        message=(
                            f"{in_flight_summary.stale_transfer_count} in-flight "
                            f"transfers are overdue - consider investigating"
                        ),
                        current_value=Decimal(str(in_flight_summary.stale_transfer_count)),
                        limit_value=Decimal("0"),
                        severity="warning",
                    )
                )

        logger.debug(
            f"Validation with in-flight exposure: "
            f"total_exposure=${context.total_exposure_usd}, "
            f"in_flight=${in_flight_summary.total_in_flight_usd}, "
            f"passed={result.passed}"
        )

        return result

    def get_effective_exposure(
        self,
        context: RiskContext,
        in_flight_summary: "InFlightSummary",
    ) -> Decimal:
        """Calculate effective total exposure including in-flight assets.

        This is the total exposure that should be considered for risk
        management: current portfolio value + assets in transit.

        Args:
            context: Current risk context
            in_flight_summary: Summary from InFlightExposureTracker

        Returns:
            Total effective exposure in USD
        """
        return context.total_exposure_usd + in_flight_summary.total_in_flight_usd

    def get_effective_in_flight(
        self,
        context: RiskContext,
        in_flight_summary: "InFlightSummary",
    ) -> Decimal:
        """Calculate total in-flight exposure from both sources.

        Combines in-flight exposure from RiskContext (for backward
        compatibility) and InFlightExposureTracker.

        Args:
            context: Current risk context
            in_flight_summary: Summary from InFlightExposureTracker

        Returns:
            Combined in-flight exposure in USD
        """
        return context.total_in_flight_usd + in_flight_summary.total_in_flight_usd


# =============================================================================
# Exports
# =============================================================================


__all__ = [
    "CrossChainRiskGuard",
    "CrossChainRiskConfig",
    "CrossChainRiskResult",
    "RiskViolation",
    "RiskContext",
    "ChainBalance",
    "InFlightTransfer",
    "BridgeHistoryEntry",
]
