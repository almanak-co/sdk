"""Mutable state passed through ``ExecutionOrchestrator.execute`` phases.

Phase 3a of the coverage-improvement plan extracts the 9-step pipeline that
was previously inlined in ``ExecutionOrchestrator.execute`` into seven phase
helpers. This dataclass carries the values the helpers would otherwise need
to thread through each other as positional arguments.

The dataclass is intentionally mutable: each phase updates the fields it
owns. See ``docs/internal/coverage-improvement-plan.md`` section 8 for the
plan and ``orchestrator.py`` for the phases themselves.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ..models.reproduction_bundle import ActionBundle
    from .interfaces import (
        SignedTransaction,
        SubmissionResult,
        TransactionReceipt,
        UnsignedTransaction,
    )
    from .orchestrator import ExecutionContext, ExecutionResult
    from .session import ExecutionSession


@dataclass
class ExecutionPipelineState:
    """Mutable state threaded through orchestrator pipeline phases.

    Each phase helper on ``ExecutionOrchestrator`` reads and mutates the
    fields it owns. The phases in order and the fields they own:

    - ``_phase_build``: ``action_bundle`` (refreshed), ``unsigned_txs``,
      may mutate ``result`` (gas warnings).
    - ``_phase_validate``: reads ``unsigned_txs``, may short-circuit ``result``.
    - ``_phase_simulate``: reads/mutates ``unsigned_txs`` (gas estimates),
      may short-circuit ``result``.
    - ``_phase_gas``: reads/mutates ``unsigned_txs`` (gas prices), may
      short-circuit ``result``.
    - ``_phase_sign``: reads/mutates ``unsigned_txs`` (nonces),
      sets ``signed_txs``.
    - ``_phase_submit_and_confirm``: reads ``signed_txs``, sets
      ``submission_results``, ``receipts``, ``use_sequential``.
    - ``_phase_enrich``: reads ``receipts``, mutates ``result``.

    Attributes:
        action_bundle: The ActionBundle being executed. May be replaced by
            ``_phase_build`` after ``refresh_deferred_bundle`` runs.
        context: Execution context (deployment_id, chain, wallet, dry_run, ...).
        result: ExecutionResult accumulator. Phases mutate this in place;
            the driver returns either the short-circuit result or this final
            accumulator.
        session: Execution session for crash-recovery checkpoints. ``None``
            when the orchestrator has no session store.
        unsigned_txs: Unsigned transactions after Step 1 (build). Set by
            ``_phase_build`` and mutated by simulate/gas/nonce phases.
        signed_txs: Signed transactions after Step 5. Set by ``_phase_sign``.
        submission_results: Results from submit / submit_sequential. Set by
            ``_phase_submit_and_confirm``.
        receipts: Transaction receipts. Set by either the sequential path
            (inside the submitter) or the parallel path (``get_receipts``).
        use_sequential: ``True`` when we took the sequential submit path,
            which pre-populates ``receipts`` and skips the parallel
            ``get_receipts`` call.
    """

    action_bundle: ActionBundle
    context: ExecutionContext
    result: ExecutionResult
    session: ExecutionSession | None = None
    unsigned_txs: list[UnsignedTransaction] | None = None
    signed_txs: list[SignedTransaction] | None = None
    submission_results: list[SubmissionResult] | None = None
    receipts: list[TransactionReceipt] | None = None
    use_sequential: bool = False

    # Scratch space for _phase_simulate to publish state_overrides to
    # downstream phases if we ever need to (currently unused, kept for
    # symmetry with the pre-refactor local variable naming).
    extras: dict[str, Any] = field(default_factory=dict)
