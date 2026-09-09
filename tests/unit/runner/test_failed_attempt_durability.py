"""Confirmed reverts retain costs once, independently of a successful retry."""

import json
from copy import deepcopy
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from almanak.framework.execution.failed_attempt import confirmed_failed_attempt_id
from almanak.framework.execution.interfaces import TransactionReceipt
from almanak.framework.execution.orchestrator import ExecutionPhase, ExecutionResult, TransactionResult
from almanak.framework.execution.submission import SubmissionProvenance
from almanak.framework.intents.vocabulary import LPCloseIntent
from almanak.framework.runner.strategy_runner import SingleChainExecutionState, StrategyRunner
from almanak.framework.state.backends.sqlite import SQLiteConfig, SQLiteStore


def result_from_real_receipt():
    raw = json.loads((Path(__file__).parents[2] / "fixtures/accounting/v4_failed_close_receipt.json").read_text())[
        "receipt"
    ]
    receipt = TransactionReceipt(
        tx_hash=raw["transactionHash"],
        block_number=int(raw["blockNumber"], 16),
        block_hash=raw["blockHash"],
        gas_used=int(raw["gasUsed"], 16),
        effective_gas_price=int(raw["effectiveGasPrice"], 16),
        status=int(raw["status"], 16),
        logs=raw["logs"],
        from_address=raw["from"],
        to_address=raw["to"],
        l1_fee_wei=int(raw["l1Fee"], 16),
    )
    return ExecutionResult(
        success=False,
        phase=ExecutionPhase.CONFIRMATION,
        transaction_results=[
            TransactionResult(
                tx_hash=receipt.tx_hash,
                success=False,
                receipt=receipt,
                gas_used=receipt.gas_used,
                gas_cost_wei=receipt.gas_cost_wei,
            )
        ],
        total_gas_used=receipt.gas_used,
        total_gas_cost_wei=receipt.gas_cost_wei,
        submission_provenance=SubmissionProvenance.ATTEMPTED,
        error="MinimumAmountInsufficient",
    )


def runner_state(store, result):
    runner = StrategyRunner.__new__(StrategyRunner)
    runner.state_manager = store
    runner.config = SimpleNamespace(chain="base")
    runner._is_live_mode = lambda: True
    runner._derive_execution_mode = lambda: "live"
    runner._maybe_enrich_result_with_runner_hooks = Mock()
    runner._maybe_save_ledger_with_registry = AsyncMock(return_value=False)
    runner._emit_position_event_for_intent = AsyncMock()
    runner._merge_oracle_for_ledger = lambda *args, **kwargs: {"ETH": Decimal("2500")}
    strategy = SimpleNamespace(
        deployment_id="deployment:failed-attempt-test",
        chain="base",
        wallet_address=result.transaction_results[0].receipt.from_address,
    )
    intent = LPCloseIntent(protocol="uniswap_v4", position_id="3027857", pool="WETH/USDC/500", chain="base")
    state = SingleChainExecutionState(
        strategy=strategy, intent=intent, start_time=datetime.now(UTC), deployment_id=strategy.deployment_id
    )
    state.last_execution_result = result
    state.last_execution_context = SimpleNamespace(protocol="uniswap_v4", chain="base")
    return runner, state


@pytest.mark.asyncio
async def test_real_revert_sqlite_replay_is_immutable_and_not_an_lp_event(tmp_path):
    store = SQLiteStore(SQLiteConfig(db_path=str(tmp_path / "state.db")))
    await store.initialize()
    try:
        result = result_from_real_receipt()
        runner, state = runner_state(store, result)
        await runner._single_chain_persist_failed_attempt(state, result)
        first = await store.get_ledger_entry_by_id(state.failed_attempt_ledger_id)
        evidence = json.loads(first["extracted_data_json"])
        assert first["success"] == 0 and first["gas_used"] == result.total_gas_used
        from almanak.framework.accounting.accountant_test import _cell_g11_failed_intents

        assert _cell_g11_failed_intents([first]).status == "PASS"
        assert Decimal(first["gas_usd"]) == Decimal(result.total_gas_cost_wei) * Decimal("2500") / 10**18
        assert evidence["failed_attempt"]["receipts"][0]["l1_fee_wei"] == str(
            result.transaction_results[0].receipt.l1_fee_wei
        )
        assert "compiler_evidence" not in evidence  # Actual failed quote was not captured.
        assert "lp_close_data" not in evidence
        runner._emit_position_event_for_intent.assert_not_awaited()
        runner2, state2 = runner_state(store, deepcopy(result))
        state2.intent = state.intent
        runner2._merge_oracle_for_ledger = lambda *a, **k: {"ETH": Decimal("9000")}
        await runner2._single_chain_persist_failed_attempt(state2, state2.last_execution_result)
        assert await store.get_ledger_entry_by_id(state2.failed_attempt_ledger_id) == first
        assert store._conn.execute("SELECT count(*) FROM transaction_ledger").fetchone()[0] == 1
        assert store._conn.execute("SELECT count(*) FROM accounting_events").fetchone()[0] == 0
        altered = deepcopy(result)
        altered.transaction_results[0].receipt.l1_fee_wei += 1
        altered.extracted_data = {}
        runner3, state3 = runner_state(store, altered)
        state3.intent = state.intent
        with pytest.raises(RuntimeError, match="replay barrier retained"):
            await runner3._single_chain_persist_failed_attempt(state3, altered)
        assert await store.get_ledger_entry_by_id(state.failed_attempt_ledger_id) == first
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_synthetic_compiler_evidence_and_unmeasured_l1_are_retained(tmp_path):
    store = SQLiteStore(SQLiteConfig(db_path=str(tmp_path / "state.db")))
    await store.initialize()
    try:
        result = result_from_real_receipt()
        result.transaction_results[0].receipt.l1_fee_wei = None
        result.total_gas_cost_wei = result.transaction_results[0].receipt.gas_cost_wei
        result.transaction_results[0].gas_cost_wei = result.total_gas_cost_wei
        runner, state = runner_state(store, result)
        artifact = {"schema_version": 1, "quote_block": 123, "quote_block_hash": "synthetic-test-only"}
        state.last_bundle_metadata = {"v4_operation": artifact}
        await runner._single_chain_persist_failed_attempt(state, result)
        artifact["quote_block"] = 456
        saved = await store.get_ledger_entry_by_id(state.failed_attempt_ledger_id)
        data = json.loads(saved["extracted_data_json"])
        assert data["compiler_evidence"]["v4_operation"]["quote_block"] == 123
        assert data["failed_attempt"]["receipts"][0]["l1_fee_wei"] is None
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_failed_persistence_retains_barrier_and_never_authorizes_retry():
    store = SimpleNamespace(get_ledger_entry_by_id=AsyncMock(return_value=None))
    result = result_from_real_receipt()
    runner, state = runner_state(store, result)
    state.replay_barrier = object()
    runner._write_ledger_entry = AsyncMock(side_effect=RuntimeError("disk full"))
    runner._single_chain_seal_broadcast_marker = AsyncMock()
    with pytest.raises(RuntimeError, match="RECONCILIATION_REQUIRED"):
        await runner._single_chain_persist_failed_attempt(state, result)
    assert state.failed_attempt_ledger_id is None
    kwargs = runner._single_chain_seal_broadcast_marker.await_args.kwargs
    assert kwargs["marker"] is state.replay_barrier
    assert kwargs["reconciliation_error"] and kwargs.get("recompile_error") is None
    assert kwargs["submitted_hashes"] == (result.transaction_results[0].tx_hash,)


def test_identity_refuses_success_and_incomplete_and_scopes_chain_deployment():
    result = result_from_real_receipt()
    original = confirmed_failed_attempt_id(result, deployment_id="a", chain="base")
    assert original
    assert original != confirmed_failed_attempt_id(result, deployment_id="b", chain="base")
    assert original != confirmed_failed_attempt_id(result, deployment_id="a", chain="arbitrum")
    result.success = True
    assert confirmed_failed_attempt_id(result, deployment_id="a", chain="base") is None
    result.success = False
    result.transaction_results[0].receipt = None
    assert confirmed_failed_attempt_id(result, deployment_id="a", chain="base") is None


@pytest.mark.asyncio
async def test_terminal_failure_after_lost_ack_reuses_committed_attempt(tmp_path):
    from unittest.mock import patch

    store = SQLiteStore(SQLiteConfig(db_path=str(tmp_path / "state.db")))
    await store.initialize()
    try:
        result = result_from_real_receipt()
        runner, state = runner_state(store, result)
        await runner._single_chain_persist_failed_attempt(state, result)
        attempt_id = state.failed_attempt_ledger_id
        state.failed_attempt_ledger_id = None  # Commit landed, but acknowledgement was lost.
        state.state_machine = SimpleNamespace(error="exhausted retries", retry_count=2, refused_by_safety_guard=False)
        runner._write_ledger_entry = AsyncMock(side_effect=AssertionError("must not insert duplicate"))
        runner._emit_execution_timeline_event = Mock()
        runner.balance_provider = Mock()
        runner._handle_execution_error = AsyncMock()
        runner._notify_intent_executed = Mock()
        runner._invoke_optional_hook = Mock()
        runner._record_failure = Mock()
        runner._calculate_duration_ms = lambda _: 0
        with patch(
            "almanak.framework.runner.strategy_runner.diagnose_revert",
            AsyncMock(return_value=SimpleNamespace(format=lambda: "revert")),
        ):
            await runner._single_chain_handle_failure(state)
        runner._write_ledger_entry.assert_not_awaited()
        assert state.failed_attempt_ledger_id == attempt_id
        assert store._conn.execute("SELECT count(*) FROM transaction_ledger").fetchone()[0] == 1
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_strict_gateway_and_facade_lookup_never_treat_unavailable_as_missing():
    from almanak.framework.state.gateway_state_manager import GatewayStateManager
    from almanak.framework.state.state_manager import StateManager

    gateway = GatewayStateManager.__new__(GatewayStateManager)
    gateway._timeout = 1
    gateway._client = SimpleNamespace(
        state=SimpleNamespace(GetLedgerEntry=Mock(side_effect=RuntimeError("unavailable")))
    )
    with pytest.raises(RuntimeError, match="unavailable"):
        await gateway.get_ledger_entry_by_id("attempt", strict=True)
    facade = StateManager.__new__(StateManager)
    facade._warm = gateway
    facade._initialized = True
    with pytest.raises(RuntimeError, match="unavailable"):
        await facade.get_ledger_entry_by_id("attempt", strict=True)
    gateway._client.state.GetLedgerEntry = Mock(return_value=SimpleNamespace(found=False))
    assert await facade.get_ledger_entry_by_id("attempt", strict=True) is None


@pytest.mark.parametrize("native_input", [False, True])
@pytest.mark.parametrize("external_payer", [False, True])
def test_native_retry_gas_is_normalized_once_without_changing_success_cost(native_input, external_payer):
    from almanak.framework.intents.vocabulary import SwapIntent
    from almanak.framework.runner.reconciliation import BalanceSnapshot, build_reconciliation_report
    from almanak.framework.runner.runner_state import _resolve_gas_context

    failed = result_from_real_receipt()
    prior = failed.transaction_results[0].receipt.to_dict()
    success = deepcopy(failed)
    success.success = True
    receipt = success.transaction_results[0].receipt
    receipt.status = 1
    receipt.tx_hash = "0x" + "ab" * 32
    success.transaction_results[0].tx_hash = receipt.tx_hash
    success.transaction_results[0].success = True
    wallet = receipt.from_address
    if external_payer:
        wallet = "0x" + "12" * 20
    amount_in, amount_out = (Decimal("0.001"), Decimal("3.1")) if native_input else (Decimal("3.1"), Decimal("0.001"))
    success.swap_amounts = SimpleNamespace(amount_in_decimal=amount_in, amount_out_decimal=amount_out)
    intent = SwapIntent(
        from_token="ETH" if native_input else "USDC",
        to_token="USDC" if native_input else "ETH",
        amount=amount_in,
        max_slippage=Decimal("0.00000001"),
        chain="base",
    )
    original_cost = success.total_gas_cost_wei
    token, gas = _resolve_gas_context(intent, success, wallet_address=wallet, prior_attempt_receipts=(prior, prior))
    assert gas == (Decimal(0) if external_payer else Decimal(2 * original_cost) / 10**18)
    assert success.total_gas_cost_wei == original_cost
    pre = {"ETH": Decimal("1"), "USDC": Decimal("100")}
    post = dict(pre)
    post[intent.from_token] -= amount_in
    post[intent.to_token] += amount_out
    post["ETH"] -= gas
    report = build_reconciliation_report(
        pre=BalanceSnapshot(timestamp=datetime.now(UTC), balances=pre),
        post=BalanceSnapshot(timestamp=datetime.now(UTC), balances=post),
        intent=intent,
        execution_result=success,
        gas_token=token,
        gas_cost_native=gas,
    )
    assert not report.incident
    if not external_payer:
        _, current_only = _resolve_gas_context(intent, success, wallet_address=wallet)
        wrong = build_reconciliation_report(
            pre=BalanceSnapshot(timestamp=datetime.now(UTC), balances=pre),
            post=BalanceSnapshot(timestamp=datetime.now(UTC), balances=post),
            intent=intent,
            execution_result=success,
            gas_token=token,
            gas_cost_native=current_only,
        )
        assert wrong.incident


def test_prior_retry_missing_l1_or_conflicting_receipt_never_becomes_zero():
    from almanak.framework.runner.runner_state import _wallet_receipt_gas_cost

    result = result_from_real_receipt()
    current = result.transaction_results[0].receipt
    prior = current.to_dict()
    prior["tx_hash"] = "0x" + "aa" * 32
    prior["l1_fee_wei"] = None
    assert _wallet_receipt_gas_cost(result, current.from_address, chain="base", prior_attempt_receipts=(prior,)) is None
    prior = current.to_dict()
    prior["gas_used"] += 1
    assert _wallet_receipt_gas_cost(result, current.from_address, chain="base", prior_attempt_receipts=(prior,)) is None


@pytest.mark.asyncio
async def test_strict_sqlite_lookup_refuses_missing_connection():
    store = SQLiteStore.__new__(SQLiteStore)
    store._initialized = True
    store._conn = None
    with pytest.raises(RuntimeError, match="no SQLite connection"):
        await store.get_ledger_entry_by_id("attempt", strict=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("saved_lineage", [None, "different-intent"])
async def test_lost_ack_replay_refuses_missing_or_changed_saved_lineage(tmp_path, saved_lineage):
    store = SQLiteStore(SQLiteConfig(db_path=str(tmp_path / "state.db")))
    await store.initialize()
    try:
        result = result_from_real_receipt()
        runner, state = runner_state(store, result)
        await runner._single_chain_persist_failed_attempt(state, result)
        existing = await store.get_ledger_entry_by_id(state.failed_attempt_ledger_id)
        data = json.loads(existing["extracted_data_json"])
        if saved_lineage is None:
            data.pop("execution_intent_id")
        else:
            data["execution_intent_id"] = saved_lineage
        store._conn.execute(
            "UPDATE transaction_ledger SET extracted_data_json=? WHERE id=?", (json.dumps(data), existing["id"])
        )
        store._conn.commit()
        state.failed_attempt_ledger_id = None
        runner._write_ledger_entry = AsyncMock(side_effect=AssertionError("conflict must never overwrite"))
        with pytest.raises(RuntimeError, match="replay barrier retained"):
            await runner._single_chain_persist_failed_attempt(state, result)
        runner._write_ledger_entry.assert_not_awaited()
        assert state.failed_attempt_ledger_id is None
    finally:
        await store.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("successful", [False, True])
async def test_actual_ledger_writer_stamps_original_intent_for_both_outcomes(tmp_path, successful):
    store = SQLiteStore(SQLiteConfig(db_path=str(tmp_path / "state.db")))
    await store.initialize()
    try:
        result = result_from_real_receipt()
        runner, state = runner_state(store, result)
        result.success = successful
        if successful:
            result.transaction_results[0].receipt.status = 1
            result.transaction_results[0].success = True
        ledger_id = await runner._write_ledger_entry(state.strategy, state.intent, result, successful)
        entry = await store.get_ledger_entry_by_id(ledger_id)
        assert json.loads(entry["extracted_data_json"])["execution_intent_id"] == state.intent.intent_id
    finally:
        await store.close()


def test_lineage_retention_refuses_result_reuse_and_missing_failed_identity():
    from almanak.framework.execution.failed_attempt import retain_execution_intent_id

    result = result_from_real_receipt()
    retain_execution_intent_id(result, SimpleNamespace(intent_id="original"), required=True)
    with pytest.raises(ValueError, match="different logical intent"):
        retain_execution_intent_id(result, SimpleNamespace(intent_id="another"), required=True)
    with pytest.raises(ValueError, match="no logical intent"):
        retain_execution_intent_id(result, SimpleNamespace(intent_id=None), required=True)


@pytest.mark.asyncio
async def test_real_shaped_v4_compiler_tuple_evidence_survives_json_replay(tmp_path):
    store = SQLiteStore(SQLiteConfig(db_path=str(tmp_path / "state.db")))
    await store.initialize()
    try:
        result = result_from_real_receipt()
        runner, state = runner_state(store, result)
        artifact = {
            "schema_version": 1,
            "operation": "lp_close",
            "quote_block": 123,
            "deployment_code": (("0x" + "12" * 20, "0x" + "ab" * 32),),
            "pool_key": {"fee": 500, "tick_spacing": 10},
        }
        state.last_bundle_metadata = {"v4_operation": artifact}
        await runner._single_chain_persist_failed_attempt(state, result)
        original = await store.get_ledger_entry_by_id(state.failed_attempt_ledger_id)
        saved = json.loads(original["extracted_data_json"])
        assert isinstance(saved["compiler_evidence"]["v4_operation"]["deployment_code"], list)
        assert isinstance(result.extracted_data["compiler_evidence"]["v4_operation"]["deployment_code"], tuple)
        state.failed_attempt_ledger_id = None
        await runner._single_chain_persist_failed_attempt(state, result)
        assert await store.get_ledger_entry_by_id(state.failed_attempt_ledger_id) == original
        # A changed code hash remains a conflict after serialization normalization.
        artifact["deployment_code"] = (("0x" + "12" * 20, "0x" + "cd" * 32),)
        result.extracted_data.pop("compiler_evidence")
        state.failed_attempt_ledger_id = None
        with pytest.raises(RuntimeError, match="replay barrier retained"):
            await runner._single_chain_persist_failed_attempt(state, result)
        assert await store.get_ledger_entry_by_id(original["id"]) == original
    finally:
        await store.close()


@pytest.mark.parametrize("representation", ["unprefixed", "upper_unprefixed", "upper_prefixed", "mixed_prefixed"])
def test_failed_attempt_id_is_stable_across_valid_evm_hash_representations(representation):
    result = result_from_real_receipt()
    expected = confirmed_failed_attempt_id(result, deployment_id="same", chain="base")
    raw = result.transaction_results[0].tx_hash[2:]
    variants = {
        "unprefixed": raw,
        "upper_unprefixed": raw.upper(),
        "upper_prefixed": "0X" + raw.upper(),
        "mixed_prefixed": "0x" + "".join(c.upper() if i % 2 else c for i, c in enumerate(raw)),
    }
    value = variants[representation]
    result.transaction_results[0].tx_hash = value
    result.transaction_results[0].receipt.tx_hash = value
    assert confirmed_failed_attempt_id(result, deployment_id="same", chain="base") == expected


@pytest.mark.parametrize("malformed", ["0x1234", "z" * 64, "0x" + "1" * 63, "0x" + "1" * 65])
def test_failed_attempt_id_refuses_malformed_evm_hash_even_if_receipt_echoes_it(malformed):
    result = result_from_real_receipt()
    result.transaction_results[0].tx_hash = malformed
    result.transaction_results[0].receipt.tx_hash = malformed
    assert confirmed_failed_attempt_id(result, deployment_id="same", chain="base") is None


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid_scope", ["malformed_hash", "unknown_chain"])
async def test_retryable_but_invalid_attempt_identity_retains_barrier(invalid_scope):
    result = result_from_real_receipt()
    store = SimpleNamespace(get_ledger_entry_by_id=AsyncMock(return_value=None))
    runner, state = runner_state(store, result)
    if invalid_scope == "malformed_hash":
        result.transaction_results[0].tx_hash = "0x1234"
        result.transaction_results[0].receipt.tx_hash = "0x1234"
    else:
        state.strategy.chain = "unconfigured-chain"
    state.replay_barrier = object()
    runner._write_ledger_entry = AsyncMock()
    runner._single_chain_seal_broadcast_marker = AsyncMock()
    with pytest.raises(RuntimeError, match="RECONCILIATION_REQUIRED"):
        await runner._single_chain_persist_failed_attempt(state, result)
    runner._write_ledger_entry.assert_not_awaited()
    store.get_ledger_entry_by_id.assert_not_awaited()
    assert state.failed_attempt_ledger_id is None
    marker_call = runner._single_chain_seal_broadcast_marker.await_args.kwargs
    assert marker_call["marker"] is state.replay_barrier
    assert marker_call["reconciliation_error"]
    assert marker_call.get("recompile_error") is None


@pytest.mark.asyncio
async def test_gateway_found_ledger_identity_preserves_durable_evidence():
    from almanak.framework.state.gateway_state_manager import GatewayStateManager
    from almanak.gateway.proto import gateway_pb2

    evidence = b'{"failed_attempt":{"ledger_entry_id":"attempt"}}'
    response = gateway_pb2.GetLedgerEntryResponse(
        found=True,
        entry=gateway_pb2.LedgerEntryData(
            id="attempt",
            deployment_id="deployment:test",
            timestamp=123,
            success=False,
            gas_used=21000,
            gas_usd="0.01",
            extracted_data_json=evidence,
        ),
    )
    gateway = GatewayStateManager.__new__(GatewayStateManager)
    gateway._timeout = 1
    rpc = Mock(return_value=response)
    gateway._client = SimpleNamespace(state=SimpleNamespace(GetLedgerEntry=rpc))
    row = await gateway.get_ledger_entry_by_id("attempt", strict=True)
    assert row["id"] == "attempt" and row["deployment_id"] == "deployment:test"
    assert row["success"] is False and row["gas_used"] == 21000 and row["gas_usd"] == "0.01"
    assert row["extracted_data_json"] == evidence.decode()
    assert row["timestamp"] == datetime.fromtimestamp(123, UTC).isoformat()
    assert rpc.call_args.args[0].ledger_entry_id == "attempt"


@pytest.mark.asyncio
async def test_gateway_nonstrict_ledger_lookup_degrades_but_strict_lookup_raises():
    from almanak.framework.state.gateway_state_manager import GatewayStateManager

    gateway = GatewayStateManager.__new__(GatewayStateManager)
    gateway._timeout = 1
    rpc = Mock(side_effect=RuntimeError("backend unavailable"))
    gateway._client = SimpleNamespace(state=SimpleNamespace(GetLedgerEntry=rpc))
    assert await gateway.get_ledger_entry_by_id("attempt", strict=False) is None
    with pytest.raises(RuntimeError, match="backend unavailable"):
        await gateway.get_ledger_entry_by_id("attempt", strict=True)
    assert rpc.call_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("strict", [False, True])
async def test_unsupported_facade_ledger_reader_preserves_strict_failure(strict):
    from almanak.framework.state.state_manager import StateManager

    facade = StateManager.__new__(StateManager)
    facade._initialized = True
    facade._warm = SimpleNamespace()
    if strict:
        with pytest.raises(RuntimeError, match="does not support ledger identity lookup"):
            await facade.get_ledger_entry_by_id("attempt", strict=True)
    else:
        assert await facade.get_ledger_entry_by_id("attempt", strict=False) is None
