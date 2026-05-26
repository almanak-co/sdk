"""Runner models, enums, and protocol definitions.

Extracted from strategy_runner.py for maintainability. All symbols are
re-exported by strategy_runner.py so existing import paths keep working.
"""

from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any, Protocol

from ..intents.vocabulary import AnyIntent, DecideResult
from ..portfolio import PortfolioSnapshot

# =============================================================================
# Exceptions
# =============================================================================


class CriticalCallbackError(Exception):
    """Raised by pre/post-iteration callbacks to signal a fail-closed condition.

    When a pre_iteration_callback raises this exception, the strategy runner
    will stop the loop instead of logging and continuing. This is used by
    safety-critical callbacks like --reset-fork where continuing on failure
    would run the strategy on stale fork state.

    Regular Exception subclasses raised by callbacks are caught and logged
    without stopping the loop (backward compatible behavior).
    """


# =============================================================================
# Intent Helpers
# =============================================================================


def _extract_tokens_from_intent(intent: "AnyIntent") -> list[str]:
    """Extract token symbols from an intent for price pre-fetching.

    Returns a list of token symbols mentioned in the intent. Used to
    pre-populate the price cache when decide() doesn't call market.price().

    Delegates to the shared ``extract_token_symbols`` utility which handles
    all token fields and recurses into ``callback_intents`` for FlashLoanIntent.
    """
    from almanak.framework.runner.token_extraction import extract_token_symbols

    return extract_token_symbols(intent)


# crap-allowlist: VIB-4835 — pre-existing complexity (cc=38, cov=63%) touched only by ``almanak.connectors._strategy_base.protocol_aliases`` import rewrite (legacy ``almanak.framework.connectors.protocol_aliases`` → new ``_strategy_base`` path). Refactor tracked in VIB-4139.
def _format_intent_for_log(intent: "AnyIntent", chain: str = "") -> str:  # noqa: C901
    """Format an intent for user-friendly logging.

    Args:
        intent: The intent to format
        chain: Chain name for protocol display name resolution (e.g., "mantle")

    Returns:
        Human-readable string describing the intent with amounts and tokens
    """
    from almanak.connectors._strategy_base.protocol_aliases import display_protocol

    from ..utils.log_formatters import (
        format_intent_type_emoji,
        format_percentage,
        format_usd,
    )

    intent_type = intent.intent_type.value
    emoji_type = format_intent_type_emoji(intent_type)

    def _display(protocol: str | None) -> str:
        """Resolve protocol to display name if chain context is available."""
        if not protocol:
            return ""
        return display_protocol(chain, protocol) if chain else protocol

    # SwapIntent
    if hasattr(intent, "from_token") and hasattr(intent, "to_token"):
        from_token = intent.from_token
        to_token = intent.to_token

        if hasattr(intent, "amount_usd") and intent.amount_usd:
            amount_str = format_usd(intent.amount_usd)
        elif hasattr(intent, "amount") and intent.amount:
            if intent.amount == "all":
                amount_str = "ALL"
            else:
                amount_str = f"{intent.amount}"
        else:
            amount_str = "N/A"

        slippage = getattr(intent, "max_slippage", None)
        slippage_str = f" (slippage: {format_percentage(slippage)})" if slippage else ""

        protocol = getattr(intent, "protocol", None)
        display_name = _display(protocol)
        protocol_str = f" via {display_name}" if display_name else ""

        return f"{emoji_type}: {amount_str} {from_token} → {to_token}{slippage_str}{protocol_str}"

    # SupplyIntent
    if intent_type == "SUPPLY":
        token = getattr(intent, "token", "")
        amount = getattr(intent, "amount", None)
        amount_usd = getattr(intent, "amount_usd", None)
        protocol = _display(getattr(intent, "protocol", ""))

        if amount_usd:
            amount_str = format_usd(amount_usd)
        elif amount:
            amount_str = f"{amount} {token}"
        else:
            amount_str = f"N/A {token}"

        collateral = getattr(intent, "as_collateral", True)
        collateral_str = " (as collateral)" if collateral else ""

        return f"{emoji_type}: {amount_str} to {protocol}{collateral_str}"

    # BorrowIntent
    if intent_type == "BORROW":
        borrow_token = getattr(intent, "borrow_token", "")
        borrow_amount = getattr(intent, "borrow_amount", None)
        collateral_token = getattr(intent, "collateral_token", "")
        collateral_amount = getattr(intent, "collateral_amount", None)
        protocol = _display(getattr(intent, "protocol", ""))

        if borrow_amount:
            amount_str = f"{borrow_amount} {borrow_token}"
        else:
            amount_str = f"N/A {borrow_token}"

        collateral_str = ""
        if collateral_amount == "all":
            collateral_str = f" (collateral: ALL {collateral_token})"
        elif collateral_amount:
            collateral_str = f" (collateral: {collateral_amount} {collateral_token})"

        return f"{emoji_type}: {amount_str} from {protocol}{collateral_str}"

    # WithdrawIntent
    if intent_type == "WITHDRAW":
        token = getattr(intent, "token", "")
        amount = getattr(intent, "amount", None)
        protocol = _display(getattr(intent, "protocol", ""))

        if amount == "all":
            amount_str = f"ALL {token}"
        elif amount:
            amount_str = f"{amount} {token}"
        else:
            amount_str = f"N/A {token}"

        return f"{emoji_type}: {amount_str} from {protocol}"

    # RepayIntent
    if intent_type == "REPAY":
        token = getattr(intent, "token", "")
        amount = getattr(intent, "amount", None)
        repay_full = getattr(intent, "repay_full", False)
        protocol = _display(getattr(intent, "protocol", ""))

        if repay_full:
            amount_str = f"FULL {token}"
        elif amount == "all":
            amount_str = f"ALL {token}"
        elif amount:
            amount_str = f"{amount} {token}"
        else:
            amount_str = f"N/A {token}"

        return f"{emoji_type}: {amount_str} to {protocol}"

    # LPOpenIntent
    if intent_type == "LP_OPEN":
        pool = getattr(intent, "pool", "")
        amount0 = getattr(intent, "amount0", Decimal("0"))
        amount1 = getattr(intent, "amount1", Decimal("0"))
        range_lower = getattr(intent, "range_lower", None)
        range_upper = getattr(intent, "range_upper", None)
        protocol = _display(getattr(intent, "protocol", ""))

        range_str = ""
        if range_lower and range_upper:
            range_str = f" [{range_lower:.0f} - {range_upper:.0f}]"

        return f"{emoji_type}: {pool} ({amount0}, {amount1}){range_str} via {protocol}"

    # LPCloseIntent
    if intent_type == "LP_CLOSE":
        position_id = getattr(intent, "position_id", "")
        protocol = _display(getattr(intent, "protocol", ""))
        return f"{emoji_type}: position {position_id[:8]}... via {protocol}"

    # PerpOpenIntent
    if intent_type == "PERP_OPEN":
        market = getattr(intent, "market", "")
        direction = getattr(intent, "direction", "")
        size_usd = getattr(intent, "size_usd", None)
        leverage = getattr(intent, "leverage", None)
        protocol = _display(getattr(intent, "protocol", ""))

        size_str = format_usd(size_usd) if size_usd else "N/A"
        leverage_str = f" ({leverage}x)" if leverage else ""

        return f"{emoji_type}: {direction} {market} {size_str}{leverage_str} via {protocol}"

    # PerpCloseIntent
    if intent_type == "PERP_CLOSE":
        market = getattr(intent, "market", "")
        position_id = getattr(intent, "position_id", "")
        protocol = _display(getattr(intent, "protocol", ""))
        return f"{emoji_type}: {market} position {position_id[:8] if position_id else 'N/A'}... via {protocol}"

    # BridgeIntent
    if intent_type == "BRIDGE":
        token = getattr(intent, "token", "")
        amount = getattr(intent, "amount", None)
        from_chain = getattr(intent, "from_chain", "")
        to_chain = getattr(intent, "to_chain", "")

        if amount == "all":
            amount_str = f"ALL {token}"
        elif amount:
            amount_str = f"{amount} {token}"
        else:
            amount_str = f"N/A {token}"

        return f"{emoji_type}: {amount_str} {from_chain} → {to_chain}"

    # HoldIntent
    if intent_type == "HOLD":
        reason = getattr(intent, "reason", "No action")
        return f"{emoji_type}: {reason}"

    # Default fallback
    return f"{emoji_type} (id={intent.intent_id[:8]}...)"


# =============================================================================
# Enums and Data Classes
# =============================================================================


class IterationStatus(StrEnum):
    """Status of a strategy iteration."""

    SUCCESS = "SUCCESS"
    DRY_RUN = "DRY_RUN"  # Dry run mode - no transactions submitted
    HOLD = "HOLD"  # Strategy decided to hold
    TEARDOWN = "TEARDOWN"  # Strategy is executing teardown
    COMPILATION_FAILED = "COMPILATION_FAILED"
    EXECUTION_FAILED = "EXECUTION_FAILED"
    # Tx landed on-chain but pre/post balance deltas fell outside the
    # intent's expected range (fee-on-transfer token, malicious router,
    # approval skim, oracle corruption). On-chain state already moved —
    # rolling back is not possible — so we mark the iteration failed so
    # the circuit breaker + alerting path fire and the strategy does not
    # confidently keep trading on corrupted accounting.
    RECONCILIATION_FAILED = "RECONCILIATION_FAILED"
    STRATEGY_ERROR = "STRATEGY_ERROR"
    STRATEGY_TIMEOUT = "STRATEGY_TIMEOUT"  # strategy.decide() exceeded time limit
    DATA_ERROR = "DATA_ERROR"
    CIRCUIT_BREAKER_OPEN = "CIRCUIT_BREAKER_OPEN"  # Circuit breaker blocked execution
    # VIB-3157: on-chain execution succeeded but the durable accounting write
    # (ledger / snapshot / metrics) failed. Runner halts the iteration and
    # alerts the operator so the books are reconciled before resuming.
    ACCOUNTING_FAILED = "ACCOUNTING_FAILED"
    # VIB-3754: the runner reported SUCCESS for a non-HOLD intent in live mode
    # but no trade-effective evidence was produced — no on-chain tx_hash, no
    # CLOB order_id, no extracted_data signalling an off-chain order matched.
    # Surfaced as a re-classification ONLY at the iteration_summary log layer
    # (the in-memory IterationResult.status stays SUCCESS so the circuit
    # breaker / metrics / state-persistence wiring is untouched). This keeps
    # operator dashboards from showing a green row that produced nothing.
    EXECUTION_NOOP = "EXECUTION_NOOP"


@dataclass
class IterationResult:
    """Result of a single strategy iteration.

    Attributes:
        status: Outcome status of the iteration
        intent: The intent produced by the strategy (if any)
        execution_result: Result from execution orchestrator (if executed)
        error: Error message (if failed)
        deployment_id: ID of the strategy that ran
        duration_ms: Time taken for the iteration in milliseconds
        timestamp: When the iteration completed
    """

    status: IterationStatus
    intent: AnyIntent | None = None
    execution_result: "Any | None" = None
    error: str | None = None
    deployment_id: str = ""
    duration_ms: float = 0.0
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))
    balance_reconciliation: dict[str, Any] | None = None  # Post-execution balance check

    @property
    def success(self) -> bool:
        """Check if iteration was successful (including DRY_RUN, HOLD, and TEARDOWN)."""
        return self.status in (
            IterationStatus.SUCCESS,
            IterationStatus.DRY_RUN,
            IterationStatus.HOLD,
            IterationStatus.TEARDOWN,
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "status": self.status.value,
            "intent": self.intent.serialize() if self.intent else None,
            "execution_result": self.execution_result.to_dict() if self.execution_result else None,
            "error": self.error,
            "deployment_id": self.deployment_id,
            "duration_ms": self.duration_ms,
            "timestamp": self.timestamp.isoformat(),
            "balance_reconciliation": self.balance_reconciliation,
        }


@dataclass
class ExecutionProgress:
    """Tracks execution progress for resuming after restart.

    Attributes:
        execution_id: Unique ID for this execution sequence
        deployment_id: Strategy that owns this execution
        intents_hash: Hash of serialized intents (to detect changes)
        total_steps: Total number of steps in the sequence
        completed_step_index: Index of last completed step (-1 if none)
        previous_amount_received: Amount from last step (for chaining)
        started_at: When this execution started
        last_updated: When progress was last updated
        serialized_intents: Serialized intent data for resumption
        failed_at_step_index: Index of the step that failed (None if no failure)
        failure_error: Error message from the failed step
    """

    execution_id: str
    deployment_id: str
    intents_hash: str
    total_steps: int
    completed_step_index: int = -1  # -1 means no steps completed
    previous_amount_received: Decimal | None = None
    started_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    last_updated: datetime = field(default_factory=lambda: datetime.now(UTC))
    serialized_intents: list[dict[str, Any]] | None = None
    failed_at_step_index: int | None = None
    failure_error: str | None = None

    @property
    def is_stuck(self) -> bool:
        """Check if execution is stuck (has a failed step that needs retry)."""
        return self.failed_at_step_index is not None

    @property
    def next_step_to_execute(self) -> int:
        """Get the index of the next step to execute (failed step or next after completed)."""
        if self.failed_at_step_index is not None:
            return self.failed_at_step_index
        return self.completed_step_index + 1

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for storage."""
        return {
            "execution_id": self.execution_id,
            "deployment_id": self.deployment_id,
            "intents_hash": self.intents_hash,
            "total_steps": self.total_steps,
            "completed_step_index": self.completed_step_index,
            "previous_amount_received": str(self.previous_amount_received)
            if self.previous_amount_received is not None
            else None,
            "started_at": self.started_at.isoformat(),
            "last_updated": self.last_updated.isoformat(),
            "serialized_intents": self.serialized_intents,
            "failed_at_step_index": self.failed_at_step_index,
            "failure_error": self.failure_error,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ExecutionProgress":
        """Create from dictionary."""
        previous_amount = data.get("previous_amount_received")
        return cls(
            execution_id=data["execution_id"],
            deployment_id=data["deployment_id"],
            intents_hash=data["intents_hash"],
            total_steps=data["total_steps"],
            completed_step_index=data.get("completed_step_index", -1),
            previous_amount_received=Decimal(previous_amount) if previous_amount is not None else None,
            started_at=datetime.fromisoformat(data["started_at"]),
            last_updated=datetime.fromisoformat(data["last_updated"]),
            serialized_intents=data.get("serialized_intents"),
            failed_at_step_index=data.get("failed_at_step_index"),
            failure_error=data.get("failure_error"),
        )


@dataclass
class RunnerConfig:
    """Configuration for the strategy runner.

    Attributes:
        default_interval_seconds: Default interval between iterations
        max_consecutive_errors: Maximum consecutive errors before alerting
        enable_state_persistence: Whether to persist state between iterations
        enable_alerting: Whether to send alerts on errors
        dry_run: If True, compile but don't execute intents
        max_retries: Maximum number of automatic retries per intent (default 3)
        initial_retry_delay: Initial delay between retries in seconds (default 1.0)
        max_retry_delay: Maximum delay between retries in seconds (default 60.0)
        decide_timeout_seconds: Hard timeout for strategy.decide() in seconds (default 30.0)
        allow_unsafe_teardown_fallback: If True, allow inline teardown execution without
            TeardownManager safety features (no loss caps, no slippage escalation, no
            approval gates, no verification). Default False — only enable for local
            development/testing where safety features aren't needed.
        reconciliation_enforcement: If True, post-execution balance reconciliation
            incidents flip the iteration to IterationStatus.RECONCILIATION_FAILED and
            engage the downstream failure handler (circuit breaker, consecutive-errors
            alert, operator card). If False (default, "observation mode"), incidents
            are logged at WARNING and attached to the IterationResult via
            ``balance_reconciliation`` but DO NOT halt the iteration. Default is False
            until block-anchored balance reads ship (VIB-3348): the dual-layer
            balance cache today produces false-positive incidents on confirmed-on-chain
            swaps, so enforcing would halt strategies on a plumbing race rather than on
            real accounting breaches. CLI users can opt in early by setting
            ``ALMANAK_RECONCILIATION_ENFORCEMENT=1``; flip the default back to True
            once the cache race is closed.
    """

    default_interval_seconds: int = 60
    max_consecutive_errors: int = 3
    enable_state_persistence: bool = True
    enable_alerting: bool = True
    dry_run: bool = False
    max_retries: int = 3
    initial_retry_delay: float = 1.0
    max_retry_delay: float = 60.0
    lifecycle_poll_interval: float = 2.0
    decide_timeout_seconds: float = 30.0
    allow_unsafe_teardown_fallback: bool = False
    reconciliation_enforcement: bool = False


# =============================================================================
# Strategy Protocol
# =============================================================================


class StrategyProtocol(Protocol):
    """Protocol defining the interface for strategies.

    Strategies must implement these properties and methods to be
    compatible with the StrategyRunner.
    """

    @property
    def deployment_id(self) -> str:
        """Unique identifier for the strategy."""
        ...

    @property
    def chain(self) -> str:
        """Target blockchain (e.g., 'arbitrum')."""
        ...

    @property
    def wallet_address(self) -> str:
        """Wallet address for the strategy."""
        ...

    def decide(self, market: Any) -> DecideResult:
        """Main decision method that returns an intent, sequence, list, or None."""
        ...

    def create_market_snapshot(self) -> Any:
        """Create a market snapshot for the strategy."""
        ...

    def get_portfolio_snapshot(self, market: Any = None) -> PortfolioSnapshot | None:
        """Get current portfolio value and positions (optional).

        Returns PortfolioSnapshot if implemented, None if not supported.
        """
        ...

    def generate_teardown_intents(self, mode: Any, market: Any = None) -> list:
        """Generate intents to close all positions (optional, checked via hasattr)."""
        ...

    def get_open_positions(self) -> Any:
        """Return open positions for teardown safety validation (optional)."""
        ...


class StatefulActivityProviderProtocol(Protocol):
    """Protocol for copy-trading activity providers with cursor state."""

    def get_state(self) -> dict[str, Any]: ...

    def set_state(self, state: dict[str, Any]) -> None: ...


# Unused import kept for type-checking completeness; suppressed for linter.
__all__ = [
    "CriticalCallbackError",
    "ExecutionProgress",
    "IterationResult",
    "IterationStatus",
    "RunnerConfig",
    "StatefulActivityProviderProtocol",
    "StrategyProtocol",
    "_extract_tokens_from_intent",
    "_format_intent_for_log",
]
