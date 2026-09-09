"""Native closure needs complete chain/receipt accounting provenance, never a gas exemption."""

import json
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from almanak.framework.teardown.models import PositionInfo, PositionType
from almanak.framework.teardown.native_inventory_closure import (
    capture_native_exit,
    complete_native_exit,
    read_native_inventory,
)

WALLET = "0x" + "1" * 40
TX = "0x" + "2" * 64
H0, H1 = "0x" + "a" * 64, "0x" + "b" * 64
AMOUNT = 10**15
BASELINE = 100 * 10**18
GAS = 21000 * 100 + 7


@pytest.fixture
def scenario():
    position = PositionInfo(PositionType.TOKEN, "held", "base", "uniswap_v4", Decimal(3), details={"token": "ETH"})
    strategy = SimpleNamespace(deployment_id="deployment:test", chain="base", wallet_address=WALLET)
    intent = SimpleNamespace(from_token="ETH", amount=Decimal(AMOUNT) / 10**18, chain="base")
    transaction = dict(hash=TX, **{"from": WALLET}, blockNumber="0x65", blockHash=H1, nonce="0x8", value=hex(AMOUNT))
    receipt = dict(
        transactionHash=TX,
        **{"from": WALLET},
        blockNumber="0x65",
        blockHash=H1,
        status="0x1",
        gasUsed="0x5208",
        effectiveGasPrice="0x64",
        l1Fee="0x7",
    )
    state = {"end_nonce": 9, "end_balance": BASELINE - AMOUNT - GAS, "hash1": H1}
    gateway = MagicMock()

    def call(request, **kwargs):
        args = json.loads(request.params)
        if request.method == "eth_getBlockByNumber":
            result = (
                {"number": "0x64", "hash": H0}
                if args[0] == "0x64" or (args[0] == "latest" and not state.get("landed"))
                else {"number": "0x65", "hash": state["hash1"]}
            )
        elif request.method == "eth_getCode":
            result = state.get("code", "0x")
        elif request.method == "eth_getTransactionCount":
            result = "0x8" if args[1] == "0x64" else hex(state["end_nonce"])
        elif request.method == "eth_getTransactionByHash":
            result = transaction
        elif request.method == "eth_getTransactionReceipt":
            result = receipt
            state["landed"] = True
        else:
            raise AssertionError(request.method)
        return SimpleNamespace(success=True, result=json.dumps(result))

    gateway.rpc.Call.side_effect = call
    gateway.query_native_balance.side_effect = lambda **kw: BASELINE if kw["block"] == 100 else state["end_balance"]
    result = SimpleNamespace(
        success=True,
        swap_amounts=SimpleNamespace(amount_in_decimal_resolved=True, token_in="ETH", amount_in=AMOUNT),
        transaction_results=[SimpleNamespace(tx_hash=TX)],
    )
    anchor = capture_native_exit(
        strategy=strategy,
        intent=intent,
        positions=[position],
        tracked={"ETH": Decimal(AMOUNT) / 10**18},
        gateway=gateway,
    )
    return SimpleNamespace(
        position=position,
        strategy=strategy,
        intent=intent,
        transaction=transaction,
        receipt=receipt,
        state=state,
        gateway=gateway,
        result=result,
        anchor=anchor,
    )


def test_exact_fill_proves_closure_while_preserving_100_eth_gas_principal(scenario):
    s = scenario
    proof = complete_native_exit(s.anchor, s.result, {}, s.gateway)
    assert proof.gas_paid == GAS
    assert proof.terminal_balance > 99 * 10**18
    check = proof.verify(s.position, WALLET, s.gateway, deployment_id=s.strategy.deployment_id, tracked={})
    assert check.closed and not check.unmeasured


@pytest.mark.parametrize(
    "fault",
    [
        "missing_l1",
        "wrong_sender",
        "wrong_receipt_hash",
        "failed_receipt",
        "extra_nonce",
        "missing_transaction",
        "duplicate_transaction",
        "partial_fill",
        "wrapped_identity",
        "unresolved_input",
        "partial_disposal",
        "missing_accounting",
        "refund",
        "reorg",
    ],
)
def test_incomplete_or_conflicting_proof_refuses(scenario, fault):
    s = scenario
    tracked = {}
    if fault == "missing_l1":
        s.receipt.pop("l1Fee")
    elif fault == "wrong_sender":
        s.transaction["from"] = "0x" + "3" * 40
    elif fault == "wrong_receipt_hash":
        s.receipt["transactionHash"] = "0x" + "3" * 64
    elif fault == "failed_receipt":
        s.receipt["status"] = "0x0"
    elif fault == "extra_nonce":
        s.state["end_nonce"] = 10
    elif fault == "missing_transaction":
        s.result.transaction_results = []
    elif fault == "duplicate_transaction":
        s.result.transaction_results *= 2
    elif fault == "partial_fill":
        s.result.swap_amounts.amount_in -= 1
    elif fault == "wrapped_identity":
        s.result.swap_amounts.token_in = "WETH"
    elif fault == "unresolved_input":
        s.result.swap_amounts.amount_in_decimal_resolved = False
    elif fault == "partial_disposal":
        tracked = {"ETH": Decimal("0.000001")}
    elif fault == "missing_accounting":
        tracked = None
    elif fault == "refund":
        s.state["end_balance"] += 1
    elif fault == "reorg":
        s.state["hash1"] = H0
    with pytest.raises(ValueError):
        complete_native_exit(s.anchor, s.result, tracked, s.gateway)


def test_stale_proof_requires_chain_anchor_revalidation(scenario):
    s = scenario
    proof = complete_native_exit(s.anchor, s.result, {}, s.gateway)
    s.state["hash1"] = H0
    with pytest.raises(ValueError, match="reorg"):
        proof.verify(s.position, WALLET, s.gateway, deployment_id=s.strategy.deployment_id, tracked={})


@pytest.mark.parametrize("fault", ["wrong_position", "wrong_wallet"])
def test_proof_cannot_be_transferred_to_another_identity(scenario, fault):
    s = scenario
    proof = complete_native_exit(s.anchor, s.result, {}, s.gateway)
    if fault == "wrong_position":
        s.position.position_id = "another"
    wallet = "0x" + "3" * 40 if fault == "wrong_wallet" else WALLET
    with pytest.raises(ValueError, match="identity"):
        proof.verify(s.position, wallet, s.gateway, deployment_id=s.strategy.deployment_id, tracked={})


@pytest.mark.parametrize("fault", ["no_inventory", "excess_amount", "multiple_positions", "no_gas"])
def test_capture_refuses_unattributed_or_ambiguous_inventory(scenario, fault):
    s = scenario
    tracked = {"ETH": Decimal(AMOUNT) / 10**18}
    positions = [s.position]
    if fault == "no_inventory":
        tracked = None
    elif fault == "excess_amount":
        s.intent.amount *= 2
    elif fault == "multiple_positions":
        positions *= 2
    elif fault == "no_gas":
        s.gateway.query_native_balance.side_effect = None
        s.gateway.query_native_balance.return_value = AMOUNT
    with pytest.raises(ValueError):
        capture_native_exit(
            strategy=s.strategy, intent=s.intent, positions=positions, tracked=tracked, gateway=s.gateway
        )


@pytest.mark.parametrize("fault", ["backend", "deployment", "chain", "wallet", "pool"])
def test_inventory_provenance_refuses_wrong_scope(fault):
    event = {
        "deployment_id": "deployment:test",
        "chain": "base",
        "wallet_address": WALLET,
        "event_type": "SWAP",
        "payload_json": "{}",
    }
    if fault == "deployment":
        event["deployment_id"] = "another"
    if fault == "chain":
        event["chain"] = "arbitrum"
    if fault == "wallet":
        event["wallet_address"] = "0x" + "3" * 40
    if fault == "pool":
        event["payload_json"] = json.dumps({"swap_position_key": "swap:arbitrum:" + WALLET})
    manager = MagicMock()
    manager.read_accounting_events_measured.return_value = ([event], fault != "backend")
    manager.read_ledger_entries_measured.return_value = ([], True)
    if fault == "backend":
        assert read_native_inventory(manager, "deployment:test", "base", WALLET) is None
    else:
        with pytest.raises(ValueError):
            read_native_inventory(manager, "deployment:test", "base", WALLET)


def test_restart_has_no_in_memory_proof_to_certify(scenario):
    from almanak.framework.teardown.teardown_manager import TeardownManager

    manager = TeardownManager()
    assert manager._native_closure_check(scenario.position, WALLET, scenario.gateway) is None


@pytest.mark.parametrize("code", ["0x6000", "0xef0100" + "1" * 40])
def test_contract_or_delegated_sender_is_not_eoa_proof(scenario, code):
    s = scenario
    s.state["code"] = code
    with pytest.raises(ValueError, match="EOA"):
        capture_native_exit(
            strategy=s.strategy,
            intent=s.intent,
            positions=[s.position],
            tracked={"ETH": Decimal(AMOUNT) / 10**18},
            gateway=s.gateway,
        )


@pytest.mark.asyncio
async def test_manager_and_post_reconciliation_share_chain_bound_proof(scenario):
    from almanak.framework.teardown.plan_a_reconciliation import reconcile_known_positions_against_chain
    from almanak.framework.teardown.teardown_manager import TeardownManager

    s = scenario
    proof = complete_native_exit(s.anchor, s.result, {}, s.gateway)
    manager = TeardownManager()
    manager._native_closure_proofs = {s.anchor.key: proof}
    from dataclasses import replace

    manager.runner_helpers = replace(manager.runner_helpers, get_native_closure_inventory=lambda *args: {})
    manager._teardown_gateway_client = lambda: s.gateway
    manager._teardown_rpc_url = lambda: None
    manager._teardown_wallet_address = lambda strategy: WALLET
    summary = SimpleNamespace(positions=[s.position])
    verification = await manager._verify_closure_detailed(
        s.strategy, pre_execution_positions=summary, close_receipt_block=101
    )
    assert verification.all_closed
    assert verification.positions_closed == 1
    report = await reconcile_known_positions_against_chain(
        summary=summary,
        gateway_client=s.gateway,
        market=None,
        wallet_address=WALLET,
        phase="post",
        token_closure_authority=lambda position, wallet, gateway: manager._native_closure_check(
            position, wallet, gateway, deployment_id=s.strategy.deployment_id, strategy=s.strategy
        ),
    )
    assert len(report.diverged) == 1
    assert not report.confirmed


def test_explicit_gateway_confirmed_anvil_has_execution_only_fee_model(scenario):
    s = scenario
    s.strategy._gateway_network = "anvil"
    original = s.gateway.rpc.Call.side_effect

    def call(request, **kwargs):
        if request.method == "web3_clientVersion":
            assert request.network == "anvil"
            return SimpleNamespace(success=True, result=json.dumps("anvil/v1.3.1"))
        return original(request, **kwargs)

    s.gateway.rpc.Call.side_effect = call
    anchor = capture_native_exit(
        strategy=s.strategy,
        intent=s.intent,
        positions=[s.position],
        tracked={"ETH": Decimal(AMOUNT) / 10**18},
        gateway=s.gateway,
    )
    assert anchor.managed_fork is True
    s.receipt.pop("l1Fee")
    s.state["end_balance"] += 7
    proof = complete_native_exit(anchor, s.result, {}, s.gateway)
    assert proof.gas_paid == GAS - 7
    assert proof.verify(s.position, WALLET, s.gateway, deployment_id=s.strategy.deployment_id, tracked={}).closed


@pytest.mark.parametrize(
    "declared,success,version,expected",
    [
        ("mainnet", True, "anvil/v1.3.1", False),
        ("anvil", False, "anvil/v1.3.1", False),
        ("anvil", True, "Geth/v1.16", False),
        ("anvil", True, "not-anvil/v1", False),
        ("anvil", True, "anvil/v1.3.1", True),
    ],
)
def test_fork_fee_exception_requires_declaration_gateway_and_node(declared, success, version, expected):
    from almanak.framework.execution.fork_signal import gateway_confirms_managed_fork

    gateway = MagicMock()
    gateway.rpc.Call.return_value = SimpleNamespace(success=success, result=json.dumps(version))
    assert gateway_confirms_managed_fork(gateway, "base", declared_network=declared) is expected
    if declared == "mainnet":
        gateway.rpc.Call.assert_not_called()
    else:
        request = gateway.rpc.Call.call_args.args[0]
        assert request.network == "anvil"
        assert request.method == "web3_clientVersion"


def test_unmodelled_operator_fee_cannot_certify_closure(scenario):
    s = scenario
    s.receipt["operatorFee"] = "0x1"
    with pytest.raises(ValueError, match="operator"):
        complete_native_exit(s.anchor, s.result, {}, s.gateway)


@pytest.mark.parametrize("fault", ["deployment", "token", "position_type"])
def test_proof_requires_deployment_and_native_asset_identity(scenario, fault):
    s = scenario
    proof = complete_native_exit(s.anchor, s.result, {}, s.gateway)
    deployment = s.strategy.deployment_id
    if fault == "deployment":
        deployment = "deployment:other"
    if fault == "token":
        s.position.details["token"] = "WETH"
    if fault == "position_type":
        s.position.position_type = PositionType.LP
    with pytest.raises(ValueError, match="identity"):
        proof.verify(s.position, WALLET, s.gateway, deployment_id=deployment, tracked={})


@pytest.mark.parametrize("fault", ["nonce", "balance", "inventory", "unmeasured_inventory"])
def test_later_native_activity_invalidates_previously_valid_proof(scenario, fault):
    s = scenario
    proof = complete_native_exit(s.anchor, s.result, {}, s.gateway)
    tracked = {}
    if fault == "nonce":
        s.state["end_nonce"] += 1
    if fault == "balance":
        s.state["end_balance"] += AMOUNT
    if fault == "inventory":
        tracked = {"ETH": Decimal("0.1")}
    if fault == "unmeasured_inventory":
        tracked = None
    with pytest.raises(ValueError):
        proof.verify(s.position, WALLET, s.gateway, deployment_id=s.strategy.deployment_id, tracked=tracked)


def test_receipt_reorg_during_terminal_balance_read_cannot_rebind_proof(scenario):
    s = scenario
    original = s.gateway.query_native_balance.side_effect

    def balance(**kwargs):
        measured = original(**kwargs)
        if kwargs["block"] == 101:
            s.state["hash1"] = H0
        return measured

    s.gateway.query_native_balance.side_effect = balance
    with pytest.raises(ValueError, match="reorg"):
        complete_native_exit(s.anchor, s.result, {}, s.gateway)


def test_measured_ledger_unwrap_is_included_in_native_inventory():
    manager = MagicMock()
    manager.read_accounting_events_measured.return_value = ([], True)
    ledger = {
        "id": "unwrap",
        "deployment_id": "deployment:test",
        "chain": "base",
        "success": True,
        "intent_type": "UNWRAP_NATIVE",
        "token_in": "WETH",
        "amount_in": "0.002",
        "token_out": "ETH",
        "amount_out": "0.002",
        "timestamp": "2026-09-08T17:00:00+00:00",
    }
    manager.read_ledger_entries_measured.return_value = ([ledger], True)
    inventory = read_native_inventory(manager, "deployment:test", "base", WALLET)
    assert inventory["ETH"] == Decimal("0.002")


@pytest.mark.parametrize("measured,rows", [(False, []), (True, None)])
def test_absent_ledger_provenance_is_not_zero_inventory(measured, rows):
    manager = MagicMock()
    manager.read_accounting_events_measured.return_value = ([], True)
    manager.read_ledger_entries_measured.return_value = (rows, measured)
    assert read_native_inventory(manager, "deployment:test", "base", WALLET) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("change", [None, "reacquired", "nonce", "balance", "unmeasured", "reorg"])
async def test_final_native_revalidation_revokes_prior_td14_proof(scenario, change):
    from dataclasses import replace

    from almanak.framework.teardown.teardown_manager import TeardownManager

    s = scenario
    proof = complete_native_exit(s.anchor, s.result, {}, s.gateway)
    manager = TeardownManager()
    manager._native_closure_proofs = {s.anchor.key: proof}
    inventory = {}
    manager.runner_helpers = replace(manager.runner_helpers, get_native_closure_inventory=lambda *args: inventory)
    manager._teardown_gateway_client = lambda: s.gateway
    manager._teardown_rpc_url = lambda: None
    manager._teardown_wallet_address = lambda strategy: WALLET
    manager._fresh_post_execution_market = lambda strategy, market: None
    summary = SimpleNamespace(positions=[s.position])
    prior = await manager._verify_closure_detailed(s.strategy, pre_execution_positions=summary, close_receipt_block=101)
    assert prior.verification_status.value == "chain_verified"
    if change == "reacquired":
        s.state["end_nonce"] += 1
        s.state["end_balance"] += AMOUNT
        inventory = {"ETH": Decimal("0.001")}
    elif change == "nonce":
        s.state["end_nonce"] += 1
    elif change == "balance":
        s.state["end_balance"] += AMOUNT
    elif change == "unmeasured":
        inventory = None
    elif change == "reorg":
        s.state["hash1"] = H0
    final = await manager.verify_closure_against_chain(
        s.strategy, verification=prior, pre_execution_positions=summary, market=None
    )
    if change is None:
        assert final.all_closed
        assert final.verification_status.value == "chain_verified"
        assert s.anchor.key in final.hook_proven_position_keys
    else:
        assert not final.all_closed
        assert final.verification_status.value == "unverified"
        assert final.positions_closed == 0
        assert s.anchor.key not in final.hook_proven_position_keys


@pytest.mark.asyncio
async def test_final_burned_nft_unreadable_preserves_td14_proof(scenario, monkeypatch):
    from unittest.mock import AsyncMock

    from almanak.connectors._strategy_base.teardown_post_condition import ClosureCheckResult
    from almanak.framework.teardown import plan_a_reconciliation, teardown_manager

    s = scenario
    position = PositionInfo(PositionType.LP, "123", "base", "uniswap_v3", Decimal(3))
    manager = teardown_manager.TeardownManager()
    manager._teardown_gateway_client = lambda: s.gateway
    manager._teardown_rpc_url = lambda: None
    manager._teardown_wallet_address = lambda strategy: WALLET
    manager._fresh_post_execution_market = lambda strategy, market: None
    monkeypatch.setattr(
        teardown_manager,
        "_resolve_and_run_post_condition",
        lambda *args, **kwargs: ClosureCheckResult(closed=True, protocol="uniswap_v3", position_id="123"),
    )
    monkeypatch.setattr(
        plan_a_reconciliation,
        "_reconcile_one",
        AsyncMock(return_value=(plan_a_reconciliation.ReconciliationVerdict.UNVERIFIABLE, "burned NFT not found")),
    )
    summary = SimpleNamespace(positions=[position])
    prior = await manager._verify_closure_detailed(s.strategy, pre_execution_positions=summary)
    assert prior.verification_status.value == "chain_verified"
    final = await manager.verify_closure_against_chain(
        s.strategy, verification=prior, pre_execution_positions=summary, market=None
    )
    assert final.all_closed
    assert final.verification_status.value == "chain_verified"
    assert final.hook_proven_position_keys == prior.hook_proven_position_keys


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault",
    [
        None,
        "pre_zero",
        "other_position",
        "other_chain",
        "other_protocol",
        "other_deployment",
        "missing_proof",
        "stale_proof",
    ],
)
async def test_native_pre_unknown_resolved_only_by_exact_fresh_full_flow_proof(scenario, fault):
    from dataclasses import replace

    from almanak.framework.teardown.plan_a_reconciliation import (
        ReconciliationVerdict,
        reconcile_known_positions_against_chain,
    )
    from almanak.framework.teardown.teardown_manager import TeardownManager

    s = scenario
    s.position.details.update(asset="ETH", address="0x" + "0" * 40, token="ETH")
    from almanak.framework.intents import Intent
    from almanak.framework.teardown.completeness import check_intent_coverage

    actual_sale = Intent.swap(from_token="ETH", to_token="USDC", amount=Decimal(AMOUNT) / 10**18, protocol="uniswap_v4")
    coverage = check_intent_coverage([s.position], [actual_sale])
    assert coverage.complete
    summary = SimpleNamespace(positions=[s.position], deployment_id=s.strategy.deployment_id)
    pre = await reconcile_known_positions_against_chain(
        summary=summary, gateway_client=s.gateway, market=None, wallet_address=WALLET
    )
    assert pre.entries[0].verdict is ReconciliationVerdict.UNVERIFIABLE
    original_pre = pre
    if fault == "pre_zero":
        pre = replace(pre, entries=(replace(pre.entries[0], verdict=ReconciliationVerdict.DIVERGED_CLOSED),))
    elif fault in {"other_position", "other_chain", "other_protocol"}:
        changes = {
            "other_position": {"position_id": "uncovered"},
            "other_chain": {"chain": "arbitrum"},
            "other_protocol": {"protocol": "uniswap_v3"},
        }
        pre = replace(pre, entries=(*pre.entries, replace(pre.entries[0], **changes[fault])))
    elif fault == "other_deployment":
        pre = replace(pre, deployment_id="deployment:other")

    proof = complete_native_exit(s.anchor, s.result, {}, s.gateway)
    manager = TeardownManager()
    manager._native_closure_proofs = {s.anchor.key: proof}
    manager.runner_helpers = replace(manager.runner_helpers, get_native_closure_inventory=lambda *args: {})
    manager._teardown_gateway_client = lambda: s.gateway
    manager._teardown_rpc_url = lambda: None
    manager._teardown_wallet_address = lambda strategy: WALLET
    manager._fresh_post_execution_market = lambda strategy, market: None
    prior = await manager._verify_closure_detailed(s.strategy, pre_execution_positions=summary, close_receipt_block=101)
    assert prior.verification_status.value == "chain_verified"
    if fault == "stale_proof":
        s.state["end_nonce"] += 1
    elif fault == "missing_proof":
        manager._native_closure_proofs = {}
    final = await manager.verify_closure_against_chain(
        s.strategy,
        verification=prior,
        pre_execution_positions=summary,
        market=None,
        pre_teardown_reconciliation=pre,
    )
    final, completeness_error = manager._apply_execute_completeness(final, coverage, "")
    assert completeness_error == ""
    assert original_pre.entries[0].verdict is ReconciliationVerdict.UNVERIFIABLE
    if fault is None:
        assert final.all_closed and not final.closure_unknown
        assert final.verification_status.value == "chain_verified"
    else:
        assert final.verification_status.value == "unverified"
    if fault == "stale_proof":
        assert not final.all_closed
        assert s.anchor.key not in final.hook_proven_position_keys


@pytest.mark.parametrize("token", ["ETH", "eth", "0x" + "0" * 40, "0x" + "e" * 40, "0x" + "E" * 40])
def test_completion_accepts_the_same_measured_native_aliases_as_capture(scenario, token):
    from almanak.framework.teardown.native_inventory_closure import native_input

    s = scenario
    assert native_input(token, "base")
    s.result.swap_amounts.token_in = token
    proof = complete_native_exit(s.anchor, s.result, {}, s.gateway)
    assert proof.verify(s.position, WALLET, s.gateway, deployment_id=s.strategy.deployment_id, tracked={}).closed
    s.result.swap_amounts.amount_in_decimal_resolved = False
    with pytest.raises(ValueError, match="identity is unmeasured"):
        complete_native_exit(s.anchor, s.result, {}, s.gateway)


@pytest.mark.parametrize("token", ["WETH", "0x4200000000000000000000000000000000000006", "AVAX", None, ""])
def test_completion_rejects_wrapped_wrong_chain_and_missing_native_identity(scenario, token):
    s = scenario
    s.result.swap_amounts.token_in = token
    with pytest.raises(ValueError, match="identity is unmeasured"):
        complete_native_exit(s.anchor, s.result, {}, s.gateway)


@pytest.mark.parametrize("field", ["transaction", "receipt", "block"])
def test_native_exit_accepts_equivalent_block_hash_case(scenario, field):
    s = scenario
    if field == "block":
        s.state["hash1"] = H1.upper()
    else:
        getattr(s, field)["blockHash"] = H1.upper()
    proof = complete_native_exit(s.anchor, s.result, {}, s.gateway)
    assert proof.gas_paid == GAS
    assert proof.verify(s.position, WALLET, s.gateway, deployment_id=s.strategy.deployment_id, tracked={}).closed
