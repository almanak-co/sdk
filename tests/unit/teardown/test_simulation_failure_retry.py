"""Simulation causes survive dispatch and never justify wider price tolerance."""

from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.execution.interfaces import SimulationResult
from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.execution.submission import SubmissionProvenance
from almanak.framework.teardown.config import TeardownConfig
from almanak.framework.teardown.slippage_manager import EscalatingSlippageManager
from almanak.framework.teardown.teardown_manager import TeardownManager
from tests.unit.execution.test_orchestrator_phases import _make_state


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "simulation,status,retries",
    [
        (SimulationResult(success=False, simulated=False), "failed_non_retryable", 1),
        (
            SimulationResult(success=False, simulated=False, failure_kind="transient", revert_reason="RPC deadline"),
            "failed_rpc_unreachable",
            3,
        ),
        (
            SimulationResult(
                success=False, simulated=False, failure_kind="unavailable", revert_reason="missing backend"
            ),
            "failed_non_retryable",
            1,
        ),
        (SimulationResult(success=False, revert_reason="unknown failure"), "failed_non_retryable", 1),
        (SimulationResult(success=False, revert_reason="Anvil Fork Error"), "failed_rpc_unreachable", 3),
        (SimulationResult(success=False, revert_reason="RPC timeout"), "failed_rpc_unreachable", 3),
    ],
)
async def test_simulation_to_teardown_ladder(simulation, status, retries):
    orchestrator = ExecutionOrchestrator(
        signer=MagicMock(), submitter=MagicMock(), simulator=MagicMock(), chain="arbitrum"
    )
    orchestrator.simulator.simulate = AsyncMock(return_value=simulation)
    manager = EscalatingSlippageManager(config=TeardownConfig(retry_delay_seconds=0))
    dispatch = MagicMock()
    dispatch._describe_attempt_error.side_effect = lambda error: error
    slippages = []

    async def execute(intent, slippage):
        slippages.append(slippage)
        state = _make_state(orchestrator)
        state.context.simulation_enabled = True
        state.result.submission_provenance = SubmissionProvenance.NOT_ATTEMPTED
        state.unsigned_txs = []
        result = await orchestrator._phase_simulate(state)
        assert result is not None and not result.success
        # The gateway carries this same JSON-compatible execution_evidence payload.
        assert result.extracted_data["execution_evidence"]["simulation"] == simulation.to_dict()
        if simulation.revert_reason:
            assert simulation.revert_reason in result.error
        return TeardownManager._failed_execution_attempt(dispatch, result, slippage, 0, 1)

    result = await manager.execute_with_escalation(
        intent=MagicMock(), position_value=Decimal("100"), execute_func=execute, intent_slippage=Decimal("0.005")
    )
    assert result.status == status
    assert slippages == [Decimal("0.005")] * retries
    orchestrator.submitter.submit.assert_not_called()


def test_structured_simulation_failure_survives_serialization():
    original = SimulationResult(success=False, simulated=False, failure_kind="transient", revert_reason="RPC deadline")
    assert SimulationResult.from_dict(original.to_dict()) == original


@pytest.mark.asyncio
@pytest.mark.parametrize("recoverable,kind", [(True, "transient"), (False, "unavailable")])
async def test_backend_exception_preserves_typed_cause(recoverable, kind):
    from almanak.framework.execution.interfaces import SimulationError

    orchestrator = ExecutionOrchestrator(
        signer=MagicMock(), submitter=MagicMock(), simulator=MagicMock(), chain="arbitrum"
    )
    orchestrator.simulator.simulate = AsyncMock(side_effect=SimulationError("backend failed", recoverable=recoverable))
    state = _make_state(orchestrator)
    state.context.simulation_enabled = True
    state.unsigned_txs = []
    result = await orchestrator._phase_simulate(state)
    assert not result.success
    assert result.extracted_data["execution_evidence"]["simulation"]["failure_kind"] == kind
    assert "backend failed" in result.error


@pytest.mark.asyncio
async def test_exhausted_recoverable_backends_retry_without_widening():
    from almanak.framework.execution.interfaces import SimulationError
    from almanak.framework.execution.simulator.fallback import FallbackSimulator

    backends = [MagicMock(), MagicMock()]
    for backend in backends:
        backend.supports_chain.return_value = True
        backend.simulate = AsyncMock(side_effect=SimulationError("RPC timeout", recoverable=True))
    orchestrator = ExecutionOrchestrator(
        signer=MagicMock(),
        submitter=MagicMock(),
        simulator=FallbackSimulator(primary=backends[0], secondary=backends[1]),
        chain="arbitrum",
    )
    manager = EscalatingSlippageManager(config=TeardownConfig(retry_delay_seconds=0))
    dispatch = MagicMock()
    dispatch._describe_attempt_error.side_effect = lambda error: error
    slippages = []

    async def execute(intent, slippage):
        slippages.append(slippage)
        state = _make_state(orchestrator)
        state.context.simulation_enabled = True
        state.result.submission_provenance = SubmissionProvenance.NOT_ATTEMPTED
        state.unsigned_txs = [MagicMock()]
        result = await orchestrator._phase_simulate(state)
        assert result.extracted_data["execution_evidence"]["simulation"]["failure_kind"] == "transient"
        return TeardownManager._failed_execution_attempt(dispatch, result, slippage, 0, 1)

    result = await manager.execute_with_escalation(
        intent=MagicMock(), position_value=Decimal("100"), execute_func=execute, intent_slippage=Decimal("0.005")
    )
    assert result.status == "failed_rpc_unreachable"
    assert slippages == [Decimal("0.005")] * 3
    assert all(backend.simulate.await_count == 3 for backend in backends)
    orchestrator.submitter.submit.assert_not_called()
