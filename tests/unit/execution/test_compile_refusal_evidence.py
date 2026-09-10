"""A quote refusal remains auditable without implying on-chain execution."""

import json
from copy import deepcopy
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import pytest

from almanak.connectors.uniswap_v4.adapter import SwapResult, UniswapV4Adapter
from almanak.connectors.uniswap_v4.compiler import UniswapV4Compiler
from almanak.framework.intents.compiler_models import CompilationStatus
from almanak.framework.intents.state_machine import IntentStateMachine, RetryConfig, StateMachineConfig
from almanak.framework.intents.vocabulary import SwapIntent
from almanak.framework.observability.ledger import build_ledger_entry, deserialize_extracted_data
from almanak.framework.state.backends.sqlite import SQLiteConfig, SQLiteStore


def refusal():
    return {
        "schema_version": 1,
        "status": "refused",
        "reason": "IMPACT_TOO_HIGH",
        "amount_in_raw": "1550155",
        "oracle_estimate_raw": "1161919028860569715142",
        "quote_amount_raw": "1040977203114825908270",
        "price_impact": "0.1040879981665717483543029921",
        "max_price_impact": "0.10",
        "quote_block": 59028226,
        "chain": "robinhood",
        "pool_id": "0x1c26d4b49998cec45d0fc5cfc7b4b1bb8e05680c99d4990d51a522355b076032",
    }


def compile_refusal():
    intent = SwapIntent(
        from_token="0x5fc5360d0400a0fd4f2af552add042d716f1d168",
        to_token="0x3df3644bcf4ce0d993e18c86c3080e53bfea06f1",
        amount=Decimal("1.550155"),
        protocol="uniswap_v4",
        chain="robinhood",
        intent_id="refused-intent",
    )
    decision = refusal()
    adapter = UniswapV4Adapter.__new__(UniswapV4Adapter)
    adapter.chain = "robinhood"
    adapter.swap_exact_input = Mock(
        return_value=SwapResult(
            success=False,
            transactions=[],
            error="Price impact too high",
            price_impact_check=decision,
        )
    )
    ctx = SimpleNamespace(
        chain="robinhood",
        price_oracle={},
        max_price_impact_pct=Decimal("0.10"),
        permission_discovery=False,
        using_placeholders=False,
    )
    with patch.object(UniswapV4Compiler, "_adapter", return_value=adapter):
        result = UniswapV4Compiler().compile_swap(ctx, intent)
    return intent, result, decision


@pytest.mark.asyncio
async def test_actual_refusal_survives_adapter_compiler_state_machine_and_sqlite(tmp_path):
    intent, compiled, original = compile_refusal()
    assert compiled.status == CompilationStatus.FAILED
    assert compiled.action_bundle is None and compiled.transactions == []
    assert compiled.is_safety_refusal
    machine = IntentStateMachine(intent, Mock(compile=Mock(return_value=compiled)))
    step = machine.step()
    assert not step.needs_execution and step.action_bundle is None
    evidence = machine.compilation_evidence
    original["status"] = "changed"
    assert evidence["compiler_evidence"]["price_impact_check"] == refusal()
    entry = build_ledger_entry(
        deployment_id="deployment:refusal",
        cycle_id="cycle",
        intent=intent,
        result=None,
        success=False,
        error=compiled.error,
        chain="robinhood",
        compilation_evidence=evidence,
    )
    store = SQLiteStore(SQLiteConfig(db_path=str(tmp_path / "refusal.db")))
    await store.initialize()
    try:
        await store.save_ledger_entry(entry)
        saved = await store.get_ledger_entry_by_id(entry.id)
        data = deserialize_extracted_data(saved["extracted_data_json"])
        assert data["compiler_evidence"]["price_impact_check"] == refusal()
        assert "sub_transactions" not in data
        assert not saved["tx_hash"] and not saved["success"]
        assert not saved["amount_out"]
    finally:
        await store.close()


@pytest.mark.parametrize("mismatch", ["intent", "success", "execution"])
def test_compilation_evidence_cannot_be_attached_to_another_intent_or_execution(mismatch):
    intent, compiled, _ = compile_refusal()
    evidence = deepcopy(compiled.compilation_evidence)
    if mismatch == "intent":
        evidence["intent_id"] = "another-intent"
    with pytest.raises(ValueError):
        build_ledger_entry(
            deployment_id="deployment:refusal",
            cycle_id="cycle",
            intent=intent,
            result=SimpleNamespace() if mismatch == "execution" else None,
            success=mismatch == "success",
            compilation_evidence=evidence,
        )


def test_gateway_failure_returns_evidence_without_an_executable_bundle():
    from almanak.gateway.services.execution_service import ExecutionServiceServicer

    _, compiled, _ = compile_refusal()
    service = ExecutionServiceServicer.__new__(ExecutionServiceServicer)
    response = service._build_compilation_response(compiled, "SWAP")
    response = type(response).FromString(response.SerializeToString())
    assert not response.success and not response.action_bundle
    assert response.is_safety_refusal
    assert json.loads(response.compilation_evidence)["compiler_evidence"]["price_impact_check"] == refusal()


@pytest.mark.asyncio
async def test_gateway_compile_api_exposes_refusal_without_calling_execute():
    from almanak.framework.execution.gateway_orchestrator import GatewayCompilationError, GatewayExecutionOrchestrator
    from almanak.gateway.services.execution_service import ExecutionServiceServicer

    intent, compiled, _ = compile_refusal()
    service = ExecutionServiceServicer.__new__(ExecutionServiceServicer)
    client = Mock()
    client.execution.CompileIntent.return_value = service._build_compilation_response(compiled, "SWAP")
    orchestrator = GatewayExecutionOrchestrator(client, chain="robinhood", wallet_address="0x" + "11" * 20)
    with pytest.raises(GatewayCompilationError) as exc:
        await orchestrator.compile_intent(intent)
    assert exc.value.compilation_evidence == compiled.compilation_evidence
    client.execution.Execute.assert_not_called()


def test_new_compile_exception_does_not_reuse_a_prior_refusal():
    intent, compiled, _ = compile_refusal()
    compiler = Mock(compile=Mock(side_effect=[compiled, RuntimeError("RPC unavailable")]))
    machine = IntentStateMachine(intent, compiler)
    machine._handle_preparing()
    assert machine.compilation_evidence is not None
    machine._handle_preparing()
    assert machine.compilation_evidence is None and not machine.refused_by_safety_guard


@pytest.mark.asyncio
@pytest.mark.parametrize("earlier_execution", [False, True])
async def test_runner_writes_refused_compile_separately_from_previous_execution(earlier_execution):
    from almanak.framework.runner.strategy_runner import SingleChainExecutionState, StrategyRunner

    intent, compiled, _ = compile_refusal()
    machine = IntentStateMachine(
        intent,
        Mock(compile=Mock(return_value=compiled)),
        config=StateMachineConfig(retry_config=RetryConfig(max_retries=0)),
    )
    machine.step()
    machine.step()
    assert machine.is_complete and not machine.success
    strategy = SimpleNamespace(deployment_id="deployment:refusal", chain="robinhood")
    state = SingleChainExecutionState(
        strategy=strategy,
        intent=intent,
        start_time=datetime.now(UTC),
        deployment_id=strategy.deployment_id,
        record_metrics=False,
    )
    state.state_machine = machine
    if earlier_execution:
        state.last_execution_result = SimpleNamespace(
            error="earlier revert", transaction_results=[SimpleNamespace(tx_hash="0xearlier")]
        )
        state.failed_attempt_ledger_id = "immutable-earlier-attempt"
    runner = StrategyRunner.__new__(StrategyRunner)
    runner._write_ledger_entry = AsyncMock(return_value="compile-refusal")
    runner._single_chain_persist_failed_attempt = AsyncMock()
    runner._merge_oracle_for_ledger = Mock(return_value={})
    runner._emit_execution_timeline_event = Mock()
    runner._notify_intent_executed = Mock()
    runner._invoke_optional_hook = Mock()
    runner._calculate_duration_ms = Mock(return_value=1)
    runner._handle_execution_error = AsyncMock()
    runner.balance_provider = None
    with patch("almanak.framework.runner.strategy_runner.diagnose_revert", new=AsyncMock()):
        await runner._single_chain_handle_failure(state)
    call = runner._write_ledger_entry.call_args.kwargs
    assert call["result"] is None and call["success"] is False
    assert call["compilation_evidence"] == compiled.compilation_evidence
    runner._single_chain_persist_failed_attempt.assert_not_called()
    timeline = runner._emit_execution_timeline_event.call_args.kwargs
    assert timeline["related_ledger_entry_id"] == "compile-refusal"
    assert timeline["result"].error == machine.error
    assert not getattr(timeline["result"], "transaction_results", None)
    if earlier_execution:
        assert state.last_execution_result.error == "earlier revert"
        assert state.last_execution_result.transaction_results[0].tx_hash == "0xearlier"


def test_gateway_client_error_preserves_guard_binding_and_accepts_old_gateway():
    from almanak.framework.execution.gateway_orchestrator import GatewayCompilationError
    from almanak.gateway.proto import gateway_pb2

    intent, compiled, _ = compile_refusal()
    response = gateway_pb2.CompilationResult(
        success=False,
        error="refused",
        is_safety_refusal=True,
        compilation_evidence=json.dumps(compiled.compilation_evidence).encode(),
    )
    error = GatewayCompilationError(response, intent.intent_id)
    assert error.is_safety_refusal and error.compilation_evidence == compiled.compilation_evidence
    with pytest.raises(ValueError, match="does not match"):
        GatewayCompilationError(response, "different-intent")
    old = GatewayCompilationError(gateway_pb2.CompilationResult(success=False, error="old"), intent.intent_id)
    assert old.compilation_evidence is None and not old.is_safety_refusal


@pytest.mark.parametrize("invalid", [Decimal("1.2"), float("nan"), object()])
def test_unserializable_diagnostics_preserve_original_safety_refusal(invalid, caplog):
    from almanak.gateway.services.execution_service import ExecutionServiceServicer

    _, compiled, _ = compile_refusal()
    compiled.compiler_evidence["unexpected"] = invalid
    service = ExecutionServiceServicer.__new__(ExecutionServiceServicer)
    response = service._build_compilation_response(compiled, "SWAP")
    response = type(response).FromString(response.SerializeToString())
    assert not response.success and not response.action_bundle
    assert response.error == compiled.error
    assert response.error_code == "COMPILATION_FAILED"
    assert response.is_safety_refusal
    assert response.compilation_evidence == b""
    assert "Compilation observations could not be serialized" in caplog.text
