"""Dispatch of off-chain venue bundles, shared by the iteration loop and teardown.

A connector that executes off-chain (an order book, a venue margin account)
registers an execution handler with ``PredictionExecuteRegistry``. Its compiled
bundles carry no transactions, so they must reach that handler — the on-chain
orchestrator would report them as an empty bundle that executed nothing.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from almanak.framework.execution.orchestrator import ExecutionPhase, ExecutionResult
from almanak.framework.execution.submission import SubmissionProvenance

OFFCHAIN_FILLED_SIZE_KEY = "offchain_filled_size"


def build_offchain_handler(*, protocol: str, chain: str, gateway_client: Any, wallet: str | None) -> Any | None:
    """The off-chain execution handler for ``protocol``, or ``None`` when it has none.

    An intent without a protocol (a cash movement with several venues) binds to
    the chain's sole off-chain venue, else to the default prediction protocol.
    """
    from almanak.connectors._strategy_base.compiler_registry import CompilerRegistry
    from almanak.connectors._strategy_base.prediction_execute_registry import PredictionExecuteRegistry

    if gateway_client is None:
        return None
    resolved = protocol or ""
    if not resolved:
        chain_venues = PredictionExecuteRegistry.protocols_for_chain(chain)
        resolved = chain_venues[0] if len(chain_venues) == 1 else CompilerRegistry.default_protocol("PREDICTION") or ""
    if not resolved:
        return None
    return PredictionExecuteRegistry.build_handler(resolved, gateway_client=gateway_client, wallet=wallet or None)


def offchain_handler_factory(*, gateway_client: Any, wallet: str | None) -> Callable[[str, str], Any | None]:
    """A ``(protocol, chain) -> handler | None`` resolver that builds each handler once."""
    handlers: dict[tuple[str, str], Any | None] = {}

    def handler_for(protocol: str, chain: str) -> Any | None:
        key = (protocol, chain)
        if key not in handlers:
            handlers[key] = build_offchain_handler(
                protocol=protocol, chain=chain, gateway_client=gateway_client, wallet=wallet
            )
        return handlers[key]

    return handler_for


def _provenance(clob_result: Any) -> SubmissionProvenance:
    """What a failure means for replay. An off-chain venue submits no transaction:
    a failure the venue itself answered is safe to retry (the venue dedupes),
    an unknown outcome may have executed and must be reconciled first, and a
    failure a handler cannot vouch for stays UNSPECIFIED (fail closed)."""
    if getattr(clob_result, "outcome_unknown", False):
        return SubmissionProvenance.ATTEMPTED
    if not clob_result.success and getattr(clob_result, "venue_answered", False):
        return SubmissionProvenance.NOT_ATTEMPTED
    return SubmissionProvenance.UNSPECIFIED


def offchain_execution_result(clob_result: Any) -> ExecutionResult:
    """Convert an off-chain handler result into the pipeline's ``ExecutionResult``."""
    execution_result = ExecutionResult(
        success=clob_result.success,
        phase=ExecutionPhase.COMPLETE,
        completed_at=datetime.now(UTC),
        error=clob_result.error,
        submission_provenance=_provenance(clob_result),
    )
    execution_result.extracted_data = {
        **(getattr(clob_result, "venue_data", None) or {}),
        "clob_status": clob_result.status.value,
    }
    if clob_result.order_id:
        execution_result.extracted_data["order_id"] = clob_result.order_id
    filled = getattr(clob_result, "filled_size", None)
    if filled:
        # A measured fill, also on a failed result (e.g. a partially closed
        # position): the runner books it when the intent finally fails.
        execution_result.extracted_data[OFFCHAIN_FILLED_SIZE_KEY] = str(filled)
    # requested_size may be absent (e.g. SELL "all"); callers then rely on
    # post-execution balance reads instead of a fill.
    prediction_fill = clob_result.to_prediction_fill()
    if prediction_fill is not None:
        execution_result.prediction_fill = prediction_fill
    return execution_result


def has_offchain_fill(result: Any) -> bool:
    """Whether an off-chain result carries a measured, non-zero fill."""
    extracted = getattr(result, "extracted_data", None)
    return isinstance(extracted, dict) and bool(extracted.get(OFFCHAIN_FILLED_SIZE_KEY))


def offchain_reconciliation_error(result: Any) -> str:
    """The terminal error for an off-chain submission whose outcome the venue did not confirm.

    Carries the reconciliation prefix both retry engines treat as non-retryable,
    so the same submission is never re-sent before the handler reconciles it.
    """
    from almanak.framework.execution.reconciliation import RECONCILIATION_REQUIRED_PREFIX

    original = getattr(result, "error", None) or "the venue did not confirm the outcome"
    return (
        f"{RECONCILIATION_REQUIRED_PREFIX}: the off-chain venue outcome is unknown; refusing automatic replay "
        f"until it is reconciled with the venue: {original}"
    )


__all__ = [
    "OFFCHAIN_FILLED_SIZE_KEY",
    "build_offchain_handler",
    "has_offchain_fill",
    "offchain_execution_result",
    "offchain_handler_factory",
    "offchain_reconciliation_error",
]
