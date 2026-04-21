"""Tests for ExecutionPipelineState dataclass (Phase 3a).

Verifies construction defaults and field mutation semantics of the mutable
state object that threads through ``ExecutionOrchestrator.execute``'s phase
helpers.
"""

from __future__ import annotations

from almanak.framework.execution._pipeline_state import ExecutionPipelineState
from almanak.framework.execution.orchestrator import (
    ExecutionContext,
    ExecutionPhase,
    ExecutionResult,
)
from almanak.framework.models.reproduction_bundle import ActionBundle


def _make_state() -> ExecutionPipelineState:
    bundle = ActionBundle(intent_type="SWAP", transactions=[])
    context = ExecutionContext(
        strategy_id="s",
        intent_id="i",
        chain="arbitrum",
        wallet_address="0x00",
    )
    result = ExecutionResult(success=False, phase=ExecutionPhase.VALIDATION)
    return ExecutionPipelineState(
        action_bundle=bundle,
        context=context,
        result=result,
    )


class TestExecutionPipelineStateDefaults:
    def test_default_fields_are_none_except_required(self):
        state = _make_state()
        assert state.session is None
        assert state.unsigned_txs is None
        assert state.signed_txs is None
        assert state.submission_results is None
        assert state.receipts is None
        assert state.use_sequential is False
        assert state.extras == {}

    def test_required_fields_are_set(self):
        state = _make_state()
        assert state.action_bundle.intent_type == "SWAP"
        assert state.context.strategy_id == "s"
        assert state.result.phase == ExecutionPhase.VALIDATION


class TestExecutionPipelineStateMutation:
    def test_can_update_phase_owned_fields(self):
        state = _make_state()

        state.unsigned_txs = []
        state.signed_txs = []
        state.submission_results = []
        state.receipts = []
        state.use_sequential = True
        state.extras["note"] = "simulate"

        assert state.unsigned_txs == []
        assert state.signed_txs == []
        assert state.submission_results == []
        assert state.receipts == []
        assert state.use_sequential is True
        assert state.extras == {"note": "simulate"}

    def test_result_mutation_is_visible(self):
        state = _make_state()
        state.result.success = True
        state.result.phase = ExecutionPhase.COMPLETE
        assert state.result.success is True
        assert state.result.phase == ExecutionPhase.COMPLETE
