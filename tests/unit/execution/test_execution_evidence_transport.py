"""Pre-submit observations must survive gateway transport without inventing receipts."""

import json
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from almanak.framework.execution._pipeline_state import ExecutionPipelineState
from almanak.framework.execution.gateway_orchestrator import _execution_result_from_proto
from almanak.framework.execution.orchestrator import (
    ExecutionContext,
    ExecutionOrchestrator,
    ExecutionPhase,
    ExecutionResult,
)
from almanak.framework.execution.submission import execution_plan_hash
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.execution_service import _execution_evidence_bytes
from tests.unit.connectors.uniswap_v4.test_operation_contract import WALLET, Gateway, _compile_bundle


@pytest.mark.asyncio
@pytest.mark.parametrize("stale", [False, True])
async def test_real_v4_validator_observations_survive_gateway_wire(stale):
    gateway = Gateway()
    if stale:
        gateway.quote_time -= 301
    bundle = _compile_bundle(gateway)
    before = deepcopy(bundle)
    if stale:
        gateway.head += 1000
        gateway.elapsed = 301
    signer = MagicMock()
    signer.address = WALLET
    orchestrator = ExecutionOrchestrator(
        signer=signer,
        submitter=MagicMock(),
        simulator=MagicMock(),
        chain="base",
        managed_fork=False,
        operation_observer_factory=lambda: gateway,
    )
    context = ExecutionContext(chain="base", wallet_address=WALLET)
    result = ExecutionResult(success=False, phase=ExecutionPhase.VALIDATION)
    state = ExecutionPipelineState(action_bundle=bundle, context=context, result=result)
    refusal = await orchestrator._validate_connector_operation(state)
    assert (refusal is not None) == stale
    evidence = result.extracted_data["execution_evidence"]
    observation = evidence["connector_validation"][0]
    if stale:
        assert observation["code"] == "quote_stale"
        assert observation["observation"]["quote"]["number"] == 10
        assert observation["observation"]["head"]["number"] == 1010
    else:
        assert observation["status"] == "accepted"
        assert observation["observations"][0]["freshness"]["quote"]["number"] == 10
    plan_hash = execution_plan_hash(bundle)
    response = gateway_pb2.ExecutionResult(
        success=False,
        execution_plan_hash=plan_hash,
        execution_evidence_json=_execution_evidence_bytes(result),
    )
    transmitted = gateway_pb2.ExecutionResult.FromString(response.SerializeToString())
    received = _execution_result_from_proto(transmitted, chain="base", expected_plan_hash=plan_hash)
    assert received.extracted_data["execution_evidence"] == evidence
    assert received.tx_hashes == []
    assert bundle == before


@pytest.mark.parametrize("payload", [b"broken", b"[]", b'{"schema_version":true}', b'{"schema_version":2}'])
def test_malformed_evidence_is_unmeasured_and_does_not_invent_chain_failure(payload):
    response = gateway_pb2.ExecutionResult(
        success=False,
        execution_plan_hash="plan",
        execution_evidence_json=payload,
        tx_hashes=["0x" + "ab" * 32],
    )
    received = _execution_result_from_proto(response, chain="base", expected_plan_hash="plan")
    assert received.tx_hashes == ["0x" + "ab" * 32]
    assert not received.success
    assert "execution_evidence" not in received.extracted_data
    assert any("observations unavailable" in warning for warning in received.extraction_warnings)


def test_evidence_for_another_plan_cannot_be_attached():
    response = gateway_pb2.ExecutionResult(
        success=False,
        execution_plan_hash="other",
        execution_evidence_json=b'{"schema_version":1}',
    )
    received = _execution_result_from_proto(response, chain="base", expected_plan_hash="expected")
    assert not received.extracted_data
    assert received.extraction_warnings


def test_simulation_call_coverage_round_trips_as_observations_not_receipts():
    evidence = {
        "schema_version": 1,
        "simulation": {"success": True, "simulated": True, "evidence": {"evaluated_indices": [0, 1, 2]}},
    }
    result = SimpleNamespace(extracted_data={"execution_evidence": evidence})
    assert json.loads(_execution_evidence_bytes(result)) == evidence
