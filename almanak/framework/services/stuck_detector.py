"""Stuck Detection and Classification service.

This module provides automatic detection and classification of stuck strategies,
analyzing various indicators to determine why a strategy has stopped progressing.
"""

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from ..api.timeline import TimelineEvent, TimelineEventType, add_event
from ..models import StuckReason


@dataclass
class PendingTransaction:
    """Information about a pending transaction."""

    tx_hash: str
    nonce: int
    gas_price: int
    submitted_at: datetime
    expected_confirmation_time: datetime | None = None


@dataclass
class BalanceInfo:
    """Balance information for stuck detection."""

    native_balance: Decimal  # ETH, etc. for gas
    token_balances: dict[str, Decimal]
    required_native: Decimal | None = None
    required_tokens: dict[str, Decimal] | None = None


@dataclass
class AllowanceInfo:
    """Token allowance information for stuck detection."""

    token: str
    spender: str
    current_allowance: Decimal
    required_allowance: Decimal


@dataclass
class StrategySnapshot:
    """Snapshot of strategy state for stuck detection.

    This represents all the information needed to detect and classify
    a stuck strategy.
    """

    strategy_id: str
    chain: str
    current_state: str  # e.g., "PREPARING_SWAP", "VALIDATING_LP_OPEN"
    state_entered_at: datetime

    # Transaction context
    pending_transactions: list[PendingTransaction]
    current_gas_price: int | None = None

    # Balance context
    balance_info: BalanceInfo | None = None

    # Allowance context
    allowance_issues: list[AllowanceInfo] | None = None

    # Market context
    current_slippage: Decimal | None = None
    max_allowed_slippage: Decimal | None = None
    pool_liquidity_usd: Decimal | None = None

    # Protocol context
    oracle_last_updated: datetime | None = None
    protocol_paused: bool = False

    # RPC context
    rpc_healthy: bool = True
    last_rpc_error: str | None = None

    # Risk context
    risk_guard_blocked: bool = False
    risk_guard_reason: str | None = None
    circuit_breaker_triggered: bool = False


@dataclass
class StuckDetectionResult:
    """Result of stuck detection analysis."""

    is_stuck: bool
    reason: StuckReason | None = None
    time_in_state_seconds: float = 0
    details: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "is_stuck": self.is_stuck,
            "reason": self.reason.value if self.reason else None,
            "time_in_state_seconds": self.time_in_state_seconds,
            "details": self.details,
        }


class StuckDetector:
    """Detects and classifies stuck strategies.

    The detector analyzes various aspects of strategy state to determine:
    1. Whether the strategy is stuck (in same state too long)
    2. Why it's stuck (classification using StuckReason)

    Detection is performed by checking:
    - Pending transactions and gas prices
    - Token balances and gas availability
    - Token allowances
    - Market conditions (slippage, liquidity)
    - Protocol status (oracle freshness, paused state)
    - RPC health
    - Risk guard and circuit breaker state
    """

    # Default threshold for considering a strategy stuck (10 minutes)
    DEFAULT_STUCK_THRESHOLD_SECONDS = 600

    # Threshold for considering a gas price "blocked" (below 90% of current)
    GAS_PRICE_THRESHOLD_RATIO = 0.9

    # Threshold for considering liquidity "low" (USD)
    LOW_LIQUIDITY_THRESHOLD_USD = Decimal("1000")

    # Threshold for considering oracle data "stale" (seconds)
    ORACLE_STALE_THRESHOLD_SECONDS = 3600

    # Threshold for transaction pending timeout (seconds)
    TX_PENDING_TIMEOUT_SECONDS = 300

    def __init__(
        self,
        stuck_threshold_seconds: int = DEFAULT_STUCK_THRESHOLD_SECONDS,
        emit_events: bool = True,
    ) -> None:
        """Initialize the stuck detector.

        Args:
            stuck_threshold_seconds: Time in seconds before a strategy is
                considered stuck (default 10 minutes).
            emit_events: Whether to emit timeline events when stuck is detected.
        """
        self.stuck_threshold_seconds = stuck_threshold_seconds
        self.emit_events = emit_events

    def detect_stuck(self, snapshot: StrategySnapshot) -> StuckDetectionResult:
        """Detect if a strategy is stuck and classify the reason.

        This is the main entry point for stuck detection. It checks if the
        strategy has been in the same state longer than the threshold, and
        if so, attempts to classify the reason.

        Args:
            snapshot: Current strategy state snapshot

        Returns:
            StuckDetectionResult with stuck status and reason if applicable
        """
        now = datetime.now(UTC)
        time_in_state = (now - snapshot.state_entered_at).total_seconds()

        # Check if strategy has been in state too long
        if time_in_state < self.stuck_threshold_seconds:
            return StuckDetectionResult(
                is_stuck=False,
                time_in_state_seconds=time_in_state,
            )

        # Strategy is stuck - classify the reason
        reason, details = self._classify_stuck_reason(snapshot)

        result = StuckDetectionResult(
            is_stuck=True,
            reason=reason,
            time_in_state_seconds=time_in_state,
            details=details,
        )

        # Emit STUCK_DETECTED event
        if self.emit_events:
            self._emit_stuck_event(snapshot, result)

        return result

    def _classify_stuck_reason(
        self,
        snapshot: StrategySnapshot,
    ) -> tuple[StuckReason, dict[str, Any]]:
        """Classify the reason for being stuck.

        Checks various indicators in order of likelihood and severity.

        Args:
            snapshot: Strategy state snapshot

        Returns:
            Tuple of (StuckReason, details dict)
        """
        # Check risk guard and circuit breaker first (highest priority)
        if snapshot.circuit_breaker_triggered:
            return StuckReason.CIRCUIT_BREAKER, {
                "message": "Circuit breaker has been triggered",
            }

        if snapshot.risk_guard_blocked:
            return StuckReason.RISK_GUARD_BLOCKED, {
                "message": "Risk guard blocked the action",
                "risk_guard_reason": snapshot.risk_guard_reason,
            }

        # Check RPC health
        if not snapshot.rpc_healthy:
            return StuckReason.RPC_FAILURE, {
                "message": "RPC endpoint is unhealthy",
                "last_error": snapshot.last_rpc_error,
            }

        # Check protocol status
        if snapshot.protocol_paused:
            return StuckReason.PROTOCOL_PAUSED, {
                "message": "Target protocol is paused",
            }

        # Check oracle freshness
        if snapshot.oracle_last_updated:
            oracle_age = (datetime.now(UTC) - snapshot.oracle_last_updated).total_seconds()
            if oracle_age > self.ORACLE_STALE_THRESHOLD_SECONDS:
                return StuckReason.ORACLE_STALE, {
                    "message": "Oracle data is stale",
                    "oracle_age_seconds": oracle_age,
                    "threshold_seconds": self.ORACLE_STALE_THRESHOLD_SECONDS,
                }

        # Check pending transactions
        if snapshot.pending_transactions:
            tx_reason, tx_details = self._check_pending_transactions(snapshot)
            if tx_reason:
                return tx_reason, tx_details

        # Check balances
        if snapshot.balance_info:
            balance_reason, balance_details = self._check_balances(snapshot.balance_info)
            if balance_reason:
                return balance_reason, balance_details

        # Check allowances
        if snapshot.allowance_issues:
            for allowance in snapshot.allowance_issues:
                if allowance.current_allowance < allowance.required_allowance:
                    return StuckReason.ALLOWANCE_MISSING, {
                        "message": "Insufficient token allowance",
                        "token": allowance.token,
                        "spender": allowance.spender,
                        "current_allowance": str(allowance.current_allowance),
                        "required_allowance": str(allowance.required_allowance),
                    }

        # Check slippage
        if snapshot.current_slippage and snapshot.max_allowed_slippage:
            if snapshot.current_slippage > snapshot.max_allowed_slippage:
                return StuckReason.SLIPPAGE_EXCEEDED, {
                    "message": "Trade would exceed maximum slippage",
                    "current_slippage": str(snapshot.current_slippage),
                    "max_allowed_slippage": str(snapshot.max_allowed_slippage),
                }

        # Check pool liquidity
        if snapshot.pool_liquidity_usd is not None:
            if snapshot.pool_liquidity_usd < self.LOW_LIQUIDITY_THRESHOLD_USD:
                return StuckReason.POOL_LIQUIDITY_LOW, {
                    "message": "Pool liquidity is too low",
                    "pool_liquidity_usd": str(snapshot.pool_liquidity_usd),
                    "threshold_usd": str(self.LOW_LIQUIDITY_THRESHOLD_USD),
                }

        # Default to UNKNOWN if no specific reason could be determined
        return StuckReason.UNKNOWN, {
            "message": "Could not determine specific stuck reason",
            "state": snapshot.current_state,
            "time_in_state_seconds": (datetime.now(UTC) - snapshot.state_entered_at).total_seconds(),
        }

    def _check_pending_transactions(
        self,
        snapshot: StrategySnapshot,
    ) -> tuple[StuckReason | None, dict[str, Any]]:
        """Check pending transactions for issues.

        Analyzes pending transactions to detect:
        - Gas price blocked (tx gas price below current)
        - Nonce conflicts (gaps or duplicates)
        - Transaction timeout (pending too long)

        Args:
            snapshot: Strategy state snapshot

        Returns:
            Tuple of (StuckReason or None, details dict)
        """
        if not snapshot.pending_transactions:
            return None, {}

        # Check for gas price issues
        if snapshot.current_gas_price:
            for tx in snapshot.pending_transactions:
                threshold = int(snapshot.current_gas_price * self.GAS_PRICE_THRESHOLD_RATIO)
                if tx.gas_price < threshold:
                    return StuckReason.GAS_PRICE_BLOCKED, {
                        "message": "Transaction gas price is too low",
                        "tx_hash": tx.tx_hash,
                        "tx_gas_price": tx.gas_price,
                        "current_gas_price": snapshot.current_gas_price,
                        "threshold_ratio": self.GAS_PRICE_THRESHOLD_RATIO,
                    }

        # Check for nonce conflicts (gaps or duplicates)
        if len(snapshot.pending_transactions) > 1:
            nonces = sorted(tx.nonce for tx in snapshot.pending_transactions)
            # Check for duplicate nonces
            if len(nonces) != len(set(nonces)):
                return StuckReason.NONCE_CONFLICT, {
                    "message": "Multiple transactions with same nonce",
                    "nonces": nonces,
                }
            # Check for gaps in nonces
            for i in range(1, len(nonces)):
                if nonces[i] != nonces[i - 1] + 1:
                    return StuckReason.NONCE_CONFLICT, {
                        "message": "Gap in nonce sequence",
                        "nonces": nonces,
                    }

        # Check for transaction timeout
        now = datetime.now(UTC)
        for tx in snapshot.pending_transactions:
            pending_seconds = (now - tx.submitted_at).total_seconds()
            if pending_seconds > self.TX_PENDING_TIMEOUT_SECONDS:
                return StuckReason.NOT_INCLUDED_TIMEOUT, {
                    "message": "Transaction has been pending too long",
                    "tx_hash": tx.tx_hash,
                    "pending_seconds": pending_seconds,
                    "timeout_threshold": self.TX_PENDING_TIMEOUT_SECONDS,
                }

        return None, {}

    def _check_balances(
        self,
        balance_info: BalanceInfo,
    ) -> tuple[StuckReason | None, dict[str, Any]]:
        """Check balances for issues.

        Analyzes balance information to detect:
        - Insufficient gas (native token balance too low)
        - Insufficient token balance

        Args:
            balance_info: Balance information

        Returns:
            Tuple of (StuckReason or None, details dict)
        """
        # Check native balance for gas
        if balance_info.required_native is not None:
            if balance_info.native_balance < balance_info.required_native:
                return StuckReason.INSUFFICIENT_GAS, {
                    "message": "Insufficient native token for gas",
                    "native_balance": str(balance_info.native_balance),
                    "required_native": str(balance_info.required_native),
                }

        # Check token balances
        if balance_info.required_tokens:
            for token, required_amount in balance_info.required_tokens.items():
                current_balance = balance_info.token_balances.get(token, Decimal("0"))
                if current_balance < required_amount:
                    return StuckReason.INSUFFICIENT_BALANCE, {
                        "message": f"Insufficient balance for {token}",
                        "token": token,
                        "current_balance": str(current_balance),
                        "required_balance": str(required_amount),
                    }

        return None, {}

    def _emit_stuck_event(
        self,
        snapshot: StrategySnapshot,
        result: StuckDetectionResult,
    ) -> None:
        """Emit a STUCK_DETECTED timeline event.

        Args:
            snapshot: Strategy state snapshot
            result: Stuck detection result
        """
        description = f"Strategy stuck in state '{snapshot.current_state}'"
        if result.reason:
            description += f": {result.reason.value}"

        event = TimelineEvent(
            timestamp=datetime.now(UTC),
            event_type=TimelineEventType.STRATEGY_STUCK,
            description=description,
            strategy_id=snapshot.strategy_id,
            chain=snapshot.chain,
            details={
                "reason": result.reason.value if result.reason else None,
                "time_in_state_seconds": result.time_in_state_seconds,
                "state": snapshot.current_state,
                "detection_details": result.details,
            },
        )

        add_event(event)
