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


@pytest.mark.asyncio
@pytest.mark.parametrize("current", [0, 1, 2**256 - 1])
@pytest.mark.parametrize("revoked", [False, True])
async def test_allowance_read_and_plan_survive_gateway_and_ledger(current, revoked):
    from almanak.framework.observability.ledger import deserialize_extracted_data, serialize_extracted_data
    from tests.unit.connectors.uniswap_v4.test_approval_planning import AllowanceGateway, compile_operation
    from tests.unit.execution.test_compiler_evidence_retention import enrich

    gateway = AllowanceGateway()
    gateway.allowance = 2**256 - 1 if revoked else current
    bundle = compile_operation(gateway, "swap")
    original = deepcopy(bundle)
    gateway.allowance = current
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
    result = ExecutionResult(success=False, phase=ExecutionPhase.VALIDATION)
    state = ExecutionPipelineState(
        action_bundle=bundle,
        context=ExecutionContext(chain="base", wallet_address=WALLET),
        result=result,
    )
    refusal = await orchestrator._validate_connector_operation(state)
    rejected = revoked and current < 2**256 - 1
    assert (refusal is not None) == rejected
    assert bundle == original
    assert signer.mock_calls == []
    response = gateway_pb2.ExecutionResult(
        success=False,
        execution_plan_hash=execution_plan_hash(bundle),
        execution_evidence_json=_execution_evidence_bytes(result),
    )
    received = _execution_result_from_proto(
        gateway_pb2.ExecutionResult.FromString(response.SerializeToString()),
        chain="base",
        expected_plan_hash=execution_plan_hash(bundle),
    )
    enrich("uniswap_v4", bundle.metadata, received)
    restored = deserialize_extracted_data(serialize_extracted_data(received.extracted_data))
    compile_checks = restored["compiler_evidence"]["v4_approval_checks"]
    validation = restored["execution_evidence"]["connector_validation"][0]
    if rejected:
        assert validation["code"] == "INSUFFICIENT_ERC20_ALLOWANCE"
        observed = validation["observation"]["approval_checks"][0]
        assert observed["current_raw"] == str(current)
        assert observed["return_data"] == "0x" + current.to_bytes(32, "big").hex()
        assert observed["block_number"] == gateway.head
        assert observed["decision"] == "refuse_insufficient"
        assert compile_checks[0]["current_raw"] == str(2**256 - 1)
        assert received.tx_hashes == []
        return
    checks = validation["observations"][0]["approval_checks"]
    assert compile_checks[0]["current_raw"] == str(current)
    assert checks[0]["approval_amounts_raw"] == compile_checks[0]["approval_amounts_raw"]
    assert checks[0]["current_raw"] == (str(current) if current == 2**256 - 1 else None)
    assert checks[0]["permit2_call_data"] == bundle.transactions[-2]["data"]
    assert received.tx_hashes == []
