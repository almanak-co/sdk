"""Runner models, enums, and protocol definitions.

Extracted from strategy_runner.py for maintainability. All symbols are
re-exported by strategy_runner.py so existing import paths keep working.
"""

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any, Protocol

from ..execution.submission import SubmissionProvenance, SubmissionTransactionEvidence
from ..intents.vocabulary import AnyIntent, DecideResult
from ..portfolio import PortfolioSnapshot
from .failure_kind import FailureKind

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


def _extract_tokens_from_intent(intent: "AnyIntent", *, default_chain: str | None = None) -> list[str]:
    """Extract token symbols from an intent for price pre-fetching.

    Returns a list of token symbols mentioned in the intent. Used to
    pre-populate the price cache when decide() doesn't call market.price().

    Delegates to the shared ``extract_token_symbols`` utility which handles
    all token fields and recurses into ``callback_intents`` for FlashLoanIntent.
    ``default_chain`` supplies address→symbol resolution context for intents
    that rely on the strategy's default chain and declare none themselves.
    """
    from almanak.framework.runner.token_extraction import extract_token_symbols

    return extract_token_symbols(intent, default_chain=default_chain)


def _display_protocol_for_log(protocol: str | None, chain: str) -> str:
    if not protocol:
        return ""
    if not chain:
        return protocol

    from almanak.connectors._strategy_base.protocol_aliases import display_protocol

    return display_protocol(chain, protocol)


def _format_token_amount_for_log(amount: Any, token: Any) -> str:
    if amount == "all":
        return f"ALL {token}"
    if amount:
        return f"{amount} {token}"
    return f"N/A {token}"


_MISSING_INTENT_FIELD = object()


def _format_swap_intent_for_log(intent: Any, emoji_type: str, chain: str) -> str:
    from ..utils.log_formatters import format_percentage, format_usd

    if hasattr(intent, "amount_usd") and intent.amount_usd:
        amount_str = format_usd(intent.amount_usd)
    elif hasattr(intent, "amount") and intent.amount:
        amount_str = "ALL" if intent.amount == "all" else f"{intent.amount}"
    else:
        amount_str = "N/A"

    slippage = getattr(intent, "max_slippage", None)
    slippage_str = f" (slippage: {format_percentage(slippage)})" if slippage else ""
    display_name = _display_protocol_for_log(getattr(intent, "protocol", None), chain)
    protocol_str = f" via {display_name}" if display_name else ""
    return f"{emoji_type}: {amount_str} {intent.from_token} → {intent.to_token}{slippage_str}{protocol_str}"


def _format_supply_intent_for_log(intent: Any, emoji_type: str, chain: str) -> str:
    from ..utils.log_formatters import format_usd

    token = getattr(intent, "token", "")
    amount = getattr(intent, "amount", None)
    amount_usd = getattr(intent, "amount_usd", None)
    protocol = _display_protocol_for_log(getattr(intent, "protocol", ""), chain)
    if amount_usd:
        amount_str = format_usd(amount_usd)
    elif amount:
        amount_str = f"{amount} {token}"
    else:
        amount_str = f"N/A {token}"

    legacy_collateral = getattr(intent, "as_collateral", _MISSING_INTENT_FIELD)
    collateral = (
        getattr(intent, "use_as_collateral", True) if legacy_collateral is _MISSING_INTENT_FIELD else legacy_collateral
    )
    collateral_str = " (as collateral)" if collateral else ""
    return f"{emoji_type}: {amount_str} to {protocol}{collateral_str}"


def _format_borrow_intent_for_log(intent: Any, emoji_type: str, chain: str) -> str:
    borrow_token = getattr(intent, "borrow_token", "")
    borrow_amount = getattr(intent, "borrow_amount", None)
    collateral_token = getattr(intent, "collateral_token", "")
    collateral_amount = getattr(intent, "collateral_amount", None)
    protocol = _display_protocol_for_log(getattr(intent, "protocol", ""), chain)
    amount_str = f"{borrow_amount} {borrow_token}" if borrow_amount else f"N/A {borrow_token}"
    if collateral_amount == "all":
        collateral_str = f" (collateral: ALL {collateral_token})"
    elif collateral_amount:
        collateral_str = f" (collateral: {collateral_amount} {collateral_token})"
    else:
        collateral_str = ""
    return f"{emoji_type}: {amount_str} from {protocol}{collateral_str}"


def _format_withdraw_intent_for_log(intent: Any, emoji_type: str, chain: str) -> str:
    token = getattr(intent, "token", "")
    amount_str = _format_token_amount_for_log(getattr(intent, "amount", None), token)
    protocol = _display_protocol_for_log(getattr(intent, "protocol", ""), chain)
    return f"{emoji_type}: {amount_str} from {protocol}"


def _format_repay_intent_for_log(intent: Any, emoji_type: str, chain: str) -> str:
    token = getattr(intent, "token", "")
    if getattr(intent, "repay_full", False):
        amount_str = f"FULL {token}"
    else:
        amount_str = _format_token_amount_for_log(getattr(intent, "amount", None), token)
    protocol = _display_protocol_for_log(getattr(intent, "protocol", ""), chain)
    return f"{emoji_type}: {amount_str} to {protocol}"


def _format_lp_open_intent_for_log(intent: Any, emoji_type: str, chain: str) -> str:
    pool = getattr(intent, "pool", "")
    amount0 = getattr(intent, "amount0", Decimal("0"))
    amount1 = getattr(intent, "amount1", Decimal("0"))
    range_lower = getattr(intent, "range_lower", None)
    range_upper = getattr(intent, "range_upper", None)
    protocol = _display_protocol_for_log(getattr(intent, "protocol", ""), chain)
    range_str = f" [{range_lower:.0f} - {range_upper:.0f}]" if range_lower and range_upper else ""
    return f"{emoji_type}: {pool} ({amount0}, {amount1}){range_str} via {protocol}"


def _format_lp_close_intent_for_log(intent: Any, emoji_type: str, chain: str) -> str:
    position_id = getattr(intent, "position_id", "")
    protocol = _display_protocol_for_log(getattr(intent, "protocol", ""), chain)
    return f"{emoji_type}: position {position_id[:8]}... via {protocol}"


def _perp_direction_for_log(intent: Any) -> Any:
    direction = getattr(intent, "direction", None)
    if direction is not None:
        return direction
    if hasattr(intent, "is_long"):
        return "LONG" if intent.is_long else "SHORT"
    return ""


def _format_perp_open_intent_for_log(intent: Any, emoji_type: str, chain: str) -> str:
    from ..utils.log_formatters import format_usd

    market = getattr(intent, "market", "")
    direction = _perp_direction_for_log(intent)
    size_usd = getattr(intent, "size_usd", None)
    leverage = getattr(intent, "leverage", None)
    protocol = _display_protocol_for_log(getattr(intent, "protocol", ""), chain)
    size_str = format_usd(size_usd) if size_usd else "N/A"
    leverage_str = f" ({leverage}x)" if leverage else ""
    return f"{emoji_type}: {direction} {market} {size_str}{leverage_str} via {protocol}"


def _format_perp_close_intent_for_log(intent: Any, emoji_type: str, chain: str) -> str:
    market = getattr(intent, "market", "")
    position_id = getattr(intent, "position_id", "")
    protocol = _display_protocol_for_log(getattr(intent, "protocol", ""), chain)
    return f"{emoji_type}: {market} position {position_id[:8] if position_id else 'N/A'}... via {protocol}"


def _format_bridge_intent_for_log(intent: Any, emoji_type: str, _chain: str) -> str:
    token = getattr(intent, "token", "")
    amount_str = _format_token_amount_for_log(getattr(intent, "amount", None), token)
    from_chain = getattr(intent, "from_chain", "")
    to_chain = getattr(intent, "to_chain", "")
    return f"{emoji_type}: {amount_str} {from_chain} → {to_chain}"


def _format_hold_intent_for_log(intent: Any, emoji_type: str, _chain: str) -> str:
    return f"{emoji_type}: {getattr(intent, 'reason', 'No action')}"


type _IntentLogFormatter = Callable[[Any, str, str], str]

_INTENT_LOG_FORMATTERS: dict[str, _IntentLogFormatter] = {
    "SUPPLY": _format_supply_intent_for_log,
    "BORROW": _format_borrow_intent_for_log,
    "WITHDRAW": _format_withdraw_intent_for_log,
    "REPAY": _format_repay_intent_for_log,
    "LP_OPEN": _format_lp_open_intent_for_log,
    "LP_CLOSE": _format_lp_close_intent_for_log,
    "PERP_OPEN": _format_perp_open_intent_for_log,
    "PERP_CLOSE": _format_perp_close_intent_for_log,
    "BRIDGE": _format_bridge_intent_for_log,
    "HOLD": _format_hold_intent_for_log,
}


def _intent_log_formatter(intent_type: Any) -> _IntentLogFormatter | None:
    return next((formatter for name, formatter in _INTENT_LOG_FORMATTERS.items() if intent_type == name), None)


def _format_intent_for_log(intent: "AnyIntent", chain: str = "") -> str:
    """Format an intent for user-friendly logging.

    Args:
        intent: The intent to format
        chain: Chain name for protocol display name resolution (e.g., "mantle")

    Returns:
        Human-readable string describing the intent with amounts and tokens
    """
    from ..utils.log_formatters import format_intent_type_emoji

    intent_type = intent.intent_type.value
    emoji_type = format_intent_type_emoji(intent_type)
    if hasattr(intent, "from_token") and hasattr(intent, "to_token"):
        return _format_swap_intent_for_log(intent, emoji_type, chain)

    formatter = _intent_log_formatter(intent_type)
    if formatter is not None:
        return formatter(intent, emoji_type, chain)
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
    ASYNC_SETTLEMENT_FAILED = "ASYNC_SETTLEMENT_FAILED"
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
    # VIB-5746: typed failure classification for the circuit-breaker recording
    # path. When set, ``handle_iteration_failure`` uses it instead of inferring
    # from the status string — so a pre-execution safety-guard refusal
    # (``FailureKind.GUARD_REFUSED``) is recognised from a typed pipeline signal,
    # never by matching the error message. ``None`` means "infer from status".
    failure_kind: FailureKind | None = None
    # ALM-2972: machine-readable outcome from the protocol-neutral async
    # settlement barrier. None keeps synchronous connectors on the fast path.
    async_settlement: dict[str, Any] | None = None

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
            "async_settlement": self.async_settlement,
        }


@dataclass
class StepSubmissionEvidence:
    """Durable submission evidence scoped to one execution step."""

    step_index: int
    chain: str
    submission_provenance: SubmissionProvenance = SubmissionProvenance.UNSPECIFIED
    submitted_transaction_ids: list[str] = field(default_factory=list)
    execution_plan_hash: str = ""
    submission_transactions: list[SubmissionTransactionEvidence] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "step_index": self.step_index,
            "chain": self.chain,
            "submission_provenance": self.submission_provenance.value,
            "submitted_transaction_ids": list(self.submitted_transaction_ids),
            "execution_plan_hash": self.execution_plan_hash,
            "submission_transactions": [item.to_dict() for item in self.submission_transactions],
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "StepSubmissionEvidence":
        raw_ids = data.get("submitted_transaction_ids", [])
        ids = [item for item in raw_ids if isinstance(item, str) and item.strip()] if isinstance(raw_ids, list) else []
        return cls(
            step_index=int(data.get("step_index", 0)),
            chain=str(data.get("chain", "")),
            submission_provenance=SubmissionProvenance.parse(data.get("submission_provenance")),
            submitted_transaction_ids=ids,
            execution_plan_hash=str(data.get("execution_plan_hash", "")),
            submission_transactions=[
                item
                for value in (data.get("submission_transactions") or [])
                if (item := SubmissionTransactionEvidence.from_value(value)) is not None
            ],
        )


class ExecutionLane(StrEnum):
    """Durable owner of an execution-progress marker."""

    UNKNOWN = "unknown"
    SINGLE_CHAIN = "single_chain"
    SAME_CHAIN_MULTI_LEG = "same_chain_multi_leg"
    BRIDGE = "bridge"

    @classmethod
    def parse(cls, value: Any) -> "ExecutionLane":
        try:
            return cls(value)
        except (TypeError, ValueError):
            return cls.UNKNOWN


class ExecutionBarrierPhase(StrEnum):
    """Durable no-rebroadcast barrier phase.

    ``LANDED_REPAIR_PENDING`` is intentionally distinct from retryable failure:
    chain work already landed and only the sealed post-land repair pipeline (or
    an explicit operator attestation) may advance it.
    """

    PRE_BROADCAST = "pre_broadcast"
    RETRYABLE = "retryable"
    RECOMPILE_REQUIRED = "recompile_required"
    RECONCILIATION_REQUIRED = "reconciliation_required"
    LANDED_REPAIR_PENDING = "landed_repair_pending"
    COMPLETED = "completed"


@dataclass(frozen=True)
class ExecutionRepairAttestation:
    """Operator evidence attached to a landed-work repair seal."""

    operator: str
    accounting_repair_reference: str
    strategy_state_repair_reference: str
    attested_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def to_dict(self) -> dict[str, str]:
        return {
            "operator": self.operator,
            "accounting_repair_reference": self.accounting_repair_reference,
            "strategy_state_repair_reference": self.strategy_state_repair_reference,
            "attested_at": self.attested_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ExecutionRepairAttestation":
        return cls(
            operator=str(data["operator"]),
            accounting_repair_reference=str(data["accounting_repair_reference"]),
            strategy_state_repair_reference=str(data["strategy_state_repair_reference"]),
            attested_at=datetime.fromisoformat(str(data["attested_at"])),
        )


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
        accounting_pending_step_index: Index of a step whose broadcast is confirmed
            on-chain but whose accounting/state repair did not complete. Means
            "broadcast confirmed on-chain,
            accounting write incomplete — do NOT re-broadcast on resume." Mutually
            exclusive per step with ``failed_at_step_index`` (which means the step
            never broadcast and must be re-executed). Every broadcast lane uses
            this same phase contract.
        reconciliation_required_step_index: Index of a failed step that retained
            one or more submitted transaction hashes but no trustworthy receipt
            set. This is a terminal operator-reconciliation marker: unlike an
            ordinary failure it must never be automatically re-executed, and
            unlike accounting-pending it must not allow later legs to proceed.
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
    accounting_pending_step_index: int | None = None
    reconciliation_required_step_index: int | None = None
    submission_evidence: list[StepSubmissionEvidence] = field(default_factory=list)
    execution_lane: ExecutionLane = ExecutionLane.UNKNOWN
    barrier_phase: ExecutionBarrierPhase | None = None
    repair_attestation: ExecutionRepairAttestation | None = None

    def mark_reconciliation_required(self, step_index: int, error: str) -> None:
        """Monotonically block a step whose submission outcome is uncertain."""
        if self.effective_barrier_phase is ExecutionBarrierPhase.COMPLETED:
            raise ValueError("a completed execution cannot re-enter reconciliation")
        self.barrier_phase = ExecutionBarrierPhase.RECONCILIATION_REQUIRED
        self.reconciliation_required_step_index = step_index
        self.accounting_pending_step_index = None
        self.failed_at_step_index = None
        self.failure_error = error
        self.last_updated = datetime.now(UTC)

    def mark_landed_repair_pending(self, step_index: int, error: str) -> None:
        """Advance a confirmed step to the no-rebroadcast repair phase."""
        if self.effective_barrier_phase is ExecutionBarrierPhase.COMPLETED:
            raise ValueError("a completed execution cannot require landed repair")
        self.barrier_phase = ExecutionBarrierPhase.LANDED_REPAIR_PENDING
        self.accounting_pending_step_index = step_index
        self.reconciliation_required_step_index = None
        self.failed_at_step_index = None
        self.failure_error = error
        self.last_updated = datetime.now(UTC)

    def mark_recompile_required(self, step_index: int, error: str) -> None:
        """Permit only a fresh compile of the exact remaining vocabulary plan."""
        if self.effective_barrier_phase is ExecutionBarrierPhase.COMPLETED:
            raise ValueError("a completed execution cannot require recompilation")
        self.barrier_phase = ExecutionBarrierPhase.RECOMPILE_REQUIRED
        self.failed_at_step_index = step_index
        self.reconciliation_required_step_index = None
        self.accounting_pending_step_index = None
        self.failure_error = error
        self.last_updated = datetime.now(UTC)

    def seal_repaired_step(self, step_index: int, attestation: ExecutionRepairAttestation | None = None) -> None:
        """Seal repaired landed work and select the next exact-plan phase."""
        if self.effective_barrier_phase is not ExecutionBarrierPhase.LANDED_REPAIR_PENDING:
            raise ValueError("only landed-repair-pending work can be repair-sealed")
        if self.accounting_pending_step_index not in (None, step_index):
            raise ValueError("repair step does not match the durable pending step")
        self.completed_step_index = max(self.completed_step_index, step_index)
        self.accounting_pending_step_index = None
        self.reconciliation_required_step_index = None
        self.failure_error = None
        self.repair_attestation = attestation
        if step_index + 1 < self.total_steps:
            # A repaired prefix may resume only by freshly compiling the
            # exact sealed suffix with current prices and allowances. Generic
            # RETRYABLE is a legacy, unbound phase and is deliberately refused
            # by the runner.
            self.barrier_phase = ExecutionBarrierPhase.RECOMPILE_REQUIRED
            self.failed_at_step_index = step_index + 1
        else:
            self.barrier_phase = ExecutionBarrierPhase.COMPLETED
            self.failed_at_step_index = None
            self.intents_hash = "landed-complete"
        self.last_updated = datetime.now(UTC)

    @property
    def effective_barrier_phase(self) -> ExecutionBarrierPhase:
        """Return the explicit phase, or conservatively infer a legacy marker."""
        if self.barrier_phase is not None:
            return self.barrier_phase
        if self.accounting_pending_step_index is not None or self.intents_hash == "landed-accounting-pending":
            return ExecutionBarrierPhase.LANDED_REPAIR_PENDING
        if self.reconciliation_required_step_index is not None:
            return ExecutionBarrierPhase.RECONCILIATION_REQUIRED
        if self.failed_at_step_index is not None:
            return ExecutionBarrierPhase.RETRYABLE
        if self.intents_hash == "landed-complete" or (
            self.total_steps > 0 and self.completed_step_index >= self.total_steps - 1
        ):
            return ExecutionBarrierPhase.COMPLETED
        return ExecutionBarrierPhase.PRE_BROADCAST

    def record_submission_evidence(
        self,
        *,
        step_index: int,
        chain: str,
        submission_provenance: SubmissionProvenance,
        submitted_transaction_ids: list[str] | tuple[str, ...],
        execution_plan_hash: str = "",
        submission_transactions: list[SubmissionTransactionEvidence] | tuple[SubmissionTransactionEvidence, ...] = (),
    ) -> None:
        """Upsert evidence for one step without conflating multi-leg IDs."""
        evidence = StepSubmissionEvidence(
            step_index=step_index,
            chain=chain,
            submission_provenance=submission_provenance,
            submitted_transaction_ids=list(submitted_transaction_ids),
            execution_plan_hash=execution_plan_hash,
            submission_transactions=list(submission_transactions),
        )
        self.submission_evidence = [item for item in self.submission_evidence if item.step_index != step_index]
        self.submission_evidence.append(evidence)
        self.submission_evidence.sort(key=lambda item: item.step_index)

    @property
    def is_stuck(self) -> bool:
        """Whether progress requires retry or operator reconciliation."""
        return self.effective_barrier_phase in {
            ExecutionBarrierPhase.PRE_BROADCAST,
            ExecutionBarrierPhase.RETRYABLE,
            ExecutionBarrierPhase.RECOMPILE_REQUIRED,
            ExecutionBarrierPhase.RECONCILIATION_REQUIRED,
            ExecutionBarrierPhase.LANDED_REPAIR_PENDING,
        }

    @property
    def is_reconciliation_required(self) -> bool:
        """True when chain reconciliation must precede any further broadcast."""
        return self.effective_barrier_phase in {
            ExecutionBarrierPhase.PRE_BROADCAST,
            ExecutionBarrierPhase.RECONCILIATION_REQUIRED,
        }

    @property
    def is_accounting_pending(self) -> bool:
        """True when a broadcast-confirmed step has an incomplete accounting write.

        VIB-5670: distinct from ``is_stuck`` — the broadcast already landed
        on-chain, so the step must NOT be re-executed/re-broadcast; only its
        deferred accounting write remains.
        """
        return self.effective_barrier_phase is ExecutionBarrierPhase.LANDED_REPAIR_PENDING

    @property
    def is_completed(self) -> bool:
        """Whether every sealed step reached its downstream durable boundary."""
        return self.effective_barrier_phase is ExecutionBarrierPhase.COMPLETED

    @property
    def next_step_to_execute(self) -> int:
        """Get the index of the next step to execute.

        VIB-5670: an accounting-pending step already broadcast successfully on
        chain, so it is DONE for broadcast purposes — advance PAST it and never
        re-execute (re-broadcasting would duplicate the on-chain money-move).
        The two markers are mutually exclusive per step, but a RESUME can run
        steps past a still-set pending marker (it is kept for operator replay
        visibility): ``completed_step_index`` may then exceed the pending index,
        so the floor is the max of both — never point back at a step that
        already broadcast (pending) or completed. A ``failed_at_step_index`` at
        or above that floor is a genuinely re-executable later step and retains
        its re-execute semantics; below the floor it can only reference an
        already-broadcast step and must not win.
        """
        if self.accounting_pending_step_index is not None:
            floor = max(self.accounting_pending_step_index, self.completed_step_index) + 1
            if self.failed_at_step_index is not None and self.failed_at_step_index >= floor:
                return self.failed_at_step_index
            return floor
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
            "accounting_pending_step_index": self.accounting_pending_step_index,
            "reconciliation_required_step_index": self.reconciliation_required_step_index,
            "submission_evidence": [item.to_dict() for item in self.submission_evidence],
            "execution_lane": self.execution_lane.value,
            "barrier_phase": self.barrier_phase.value if self.barrier_phase is not None else None,
            "repair_attestation": self.repair_attestation.to_dict() if self.repair_attestation is not None else None,
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
            accounting_pending_step_index=data.get("accounting_pending_step_index"),
            reconciliation_required_step_index=data.get("reconciliation_required_step_index"),
            submission_evidence=[
                StepSubmissionEvidence.from_dict(item)
                for item in (data.get("submission_evidence") or [])
                if isinstance(item, dict)
            ],
            execution_lane=ExecutionLane.parse(data.get("execution_lane")),
            barrier_phase=ExecutionBarrierPhase(data["barrier_phase"]) if data.get("barrier_phase") else None,
            repair_attestation=(
                ExecutionRepairAttestation.from_dict(data["repair_attestation"])
                if isinstance(data.get("repair_attestation"), dict)
                else None
            ),
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
        reconciliation_confirmation_depth: VIB-3350 — how many blocks past the
            confirmed receipt block the chain head must advance before the
            block-pinned post-execution reconciliation read runs. This is a
            *proactive* guard against a lagging RPC/replica that has not yet
            indexed the receipt block (which would otherwise force the read into
            the reactive Unknown-block lag-retry). **Opt-in, default OFF.**
            ``None`` (default) or ``0`` → no wait. A positive int → wait that many
            confirmations on every chain. ``-1`` → use the per-chain recommended
            depth from ``ChainDescriptor.reorg_safe_depth`` (Ethereum 12,
            Polygon 10, Avalanche 5; generic-L2 default 3 otherwise).
            **Operational warning:** a depth larger than the strategy cycle
            interval serializes cycles (Ethereum @ 12 ≈ 2.5 min); async
            reconciliation is a separate design. The wait is always bounded by
            ``reconciliation_confirmation_timeout_seconds`` — on timeout the read
            proceeds anyway (still pinned to the receipt block) and the report is
            flagged ``reconciliation_confirmed=False``.
        reconciliation_confirmation_timeout_seconds: Upper bound (seconds) on the
            confirmation-depth wait above. Ignored when the wait is OFF.
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
    reconciliation_confirmation_depth: int | None = None
    reconciliation_confirmation_timeout_seconds: float = 12.0
    async_settlement_timeout_seconds: int | None = None
    async_settlement_poll_interval_seconds: int | None = None


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
        """Generate intents to close all positions (abstract on IntentStrategy)."""
        ...

    def get_open_positions(self) -> Any:
        """Return open positions for teardown (abstract on IntentStrategy)."""
        ...

    def supports_teardown(self) -> bool:
        """Authoritative teardown opt-in (VIB-5474 / TD-16).

        ``True`` (the ``IntentStrategy`` default) means the framework may
        auto-close this strategy's positions when an operator sends a teardown
        signal. An author returns ``False`` to declare a strategy that must NOT
        be force-closed by the framework (e.g. positions the connector cannot
        safely unwind). The runner honours this — it is no longer dead API.
        """
        ...


def strategy_supports_teardown(strategy: Any) -> bool:
    """Authoritative, default-safe answer to "is this strategy teardown-eligible?".

    Single source of truth for the teardown opt-in gate (VIB-5474 / TD-16),
    replacing the old ``hasattr(strategy, "get_open_positions")`` presence-sniff.

    Resolution:

    * The strategy declares ``supports_teardown()`` (the ``IntentStrategy``
      default returns ``True``) → honour its verdict. The **only** way to become
      ineligible is an explicit, literal ``supports_teardown() -> False``; this
      closes the VIB-5370 trap where an author's opt-out was silently ignored.
    * The method is missing, not callable, or raises → fall back to ``True``.
    * Any non-``False`` return — ``None`` (a forgotten ``return``), ``0``, ``""``,
      a non-bool — is treated as eligible (``True``), NOT as an opt-out. The
      default is **safe**: a position-holding strategy must never be silently
      dropped from teardown eligibility (which would strand on-chain risk) by a
      malformed override. Only a deliberate, literal ``False`` opts out.
    * The strategy itself is ``None`` (absent) → fall back to ``True``;
      ``getattr(None, ...)`` would raise ``AttributeError`` rather than return
      the default, so guard it explicitly.
    """
    if strategy is None:
        return True
    probe = getattr(strategy, "supports_teardown", None)
    if not callable(probe):
        return True
    try:
        verdict = probe()
    except Exception:  # noqa: BLE001 - default-safe: never strand a position-holder
        return True
    # Default-safe: ONLY a literal ``False`` opts out. None/0/""/non-bool → eligible,
    # so a forgotten ``return`` in an override can never silently strand funds.
    return verdict is not False


class StatefulActivityProviderProtocol(Protocol):
    """Protocol for copy-trading activity providers with cursor state."""

    def get_state(self) -> dict[str, Any]: ...

    def set_state(self, state: dict[str, Any]) -> None: ...


# Unused import kept for type-checking completeness; suppressed for linter.
__all__ = [
    "CriticalCallbackError",
    "ExecutionProgress",
    "StepSubmissionEvidence",
    "IterationResult",
    "IterationStatus",
    "RunnerConfig",
    "StatefulActivityProviderProtocol",
    "StrategyProtocol",
    "_extract_tokens_from_intent",
    "_format_intent_for_log",
    "strategy_supports_teardown",
]
