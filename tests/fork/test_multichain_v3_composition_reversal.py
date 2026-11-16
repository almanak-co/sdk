"""A Base V3 reverse fault preserves its debt while Ethereum closes independently."""

import hashlib
import json
import time
from pathlib import Path
from uuid import uuid4

import pytest

from almanak.connectors.aave_v3.observations import Ownership as LendingOwnership
from almanak.connectors.uniswap_v3.observations import Ownership as ExchangeOwnership
from almanak.supervisor_process import running
from almanak.work import outcome_chain, work_for
from almanak.workspace import read as read_workspace
from tests.strategy_fork import harness, supervised

ROOT = Path(__file__).parents[2]
REFERENCE = "ethereum_base_borrow_and_trade"
FAILED_TRADE = "base_trade"
HEALTHY_TRADE = "ethereum_trade"
EXPECTED_REVISION_REFUSAL = {
    "error": "revision",
    "message": "contract runtime length differs from the compiled revision",
}
pytestmark = [pytest.mark.fork, pytest.mark.strategy_fork]


def scoped(items, chain, chain_of):
    return tuple(item for item in items if chain_of(item) == chain)


def counter_inventory(owned):
    return next(
        value
        for value in (owned.inventory0, owned.inventory1)
        if value.asset != owned.target.funding_asset
    )


def assert_base_obligations(owned):
    positions = {str(item.target.allocation): item for item in owned}
    assert set(positions) == {"base_collateral", "base_loan", FAILED_TRADE}
    collateral = positions["base_collateral"]
    loan = positions["base_loan"]
    trade = positions[FAILED_TRADE]
    assert isinstance(collateral, LendingOwnership)
    assert collateral.supply_scaled > 0 and collateral.collateral_enabled
    assert isinstance(loan, LendingOwnership) and loan.debt_scaled > 0
    assert isinstance(trade, ExchangeOwnership)
    assert trade.entry_complete and counter_inventory(trade).amount > 0
    return counter_inventory(trade).amount


def assert_failed_state(held, failed, assertions):
    failed_chain = held.config.target(FAILED_TRADE).fund.chain
    healthy_chain = held.config.target(HEALTHY_TRADE).fund.chain
    assert failed_chain != healthy_chain and failed.mode == "teardown"
    assert failed.deployment_id == held.deployment_id
    assert failed.config == held.config

    held_inventory = scoped(held.inventory, failed_chain, lambda item: item.key.chain)
    failed_inventory = scoped(failed.inventory, failed_chain, lambda item: item.key.chain)
    held_owned = scoped(held.owned, failed_chain, lambda item: item.target.fund.chain)
    failed_owned = scoped(failed.owned, failed_chain, lambda item: item.target.fund.chain)
    held_outcomes = scoped(held.outcomes, failed_chain, outcome_chain)
    failed_outcomes = scoped(failed.outcomes, failed_chain, outcome_chain)
    assert (failed_inventory, failed_owned, failed_outcomes) == (
        held_inventory,
        held_owned,
        held_outcomes,
    )
    retained_counter = assert_base_obligations(failed_owned)

    failed_work = work_for(failed, failed_chain)
    assert failed_work.pending is None and failed_work.plan is None
    if failed_work.decision is not None:
        assert failed_work.decision.intent.kind == "exit_exchange"
    assertions.assert_independent_closed(failed, failed_chain, healthy_chain, None)
    identities = tuple((outcome_chain(item), item.tx_hash) for item in failed.outcomes)
    assert len(set(identities)) == len(identities)
    return failed_chain, healthy_chain, retained_counter


def retained_chain_refusal(state, failed_chain, healthy_chain):
    failed = work_for(state, failed_chain)
    fault = failed.fault
    return (
        state.mode == "teardown"
        and not failed.closed
        and work_for(state, healthy_chain).closed
        and fault is not None
        and (fault.kind, fault.message, fault.invalidated)
        == (
            EXPECTED_REVISION_REFUSAL["error"],
            EXPECTED_REVISION_REFUSAL["message"],
            False,
        )
    )


def await_chain_refusal(root, failed_chain, healthy_chain):
    """Capture the durable nonfatal fault before a later successful audit clears it."""
    deadline = time.monotonic() + supervised.PHASE_TIMEOUT
    while time.monotonic() < deadline:
        state = supervised.loaded_checkpoint(root / "state.sqlite")
        coordinator = read_workspace(root)["processes"].get("coordinator")
        if state is not None and retained_chain_refusal(state, failed_chain, healthy_chain):
            assert running(coordinator), "chain-local revision refusal stopped the coordinator"
            return state
        if not running(coordinator):
            raise AssertionError("coordinator stopped before retaining the chain-local refusal")
        time.sleep(0.1)
    raise AssertionError("multichain teardown did not retain the required Base revision refusal")


def supervise_multichain_reversal(root, record, follower, assertions, fault):
    """Retain a nonfatal Base fault while the same coordinator closes Ethereum."""
    held = supervised.hold(root, assertions, record, "entered_and_held")
    supervised.control_command(root, "pause", record)
    record["run_exit_code"] = follower.wait(timeout=supervised.CONTROL_TIMEOUT)
    assert record["run_exit_code"] == 0, "almanak run reported a failed workspace"
    record["phases"].append("actual_reference_borrowed_and_traded")
    failed_chain = held.config.target(FAILED_TRADE).fund.chain
    healthy_chain = held.config.target(HEALTHY_TRADE).fund.chain

    def refuse():
        supervised.control_command(root, "resume", record)
        supervised.control_command(root, "teardown", record)
        failed = await_chain_refusal(root, failed_chain, healthy_chain)
        chain_fault = work_for(failed, failed_chain).fault
        assert chain_fault is not None
        return failed, {
            "refused_workspace_stage": read_workspace(root)["stage"],
            "refused_coordinator_running": True,
            "refused_chain_fault": chain_fault.model_dump(mode="json"),
        }

    proof = fault(supervised.fork_handle(root), held, refuse)
    record["phases"].append(
        "base_router_refused_while_ethereum_closed_and_coordinator_remained_running"
    )
    supervised.drive_external(root, assertions)
    closed = supervised.await_checkpoint(root, lambda state: state.mode == "closed")
    assert closed.deployment_id == held.deployment_id
    assertions.assert_closed(supervised.fork_handle(root), closed)
    proof["restored_router_completed_reference"] = True
    record["phases"].append("restored_exact_router_reversed_repaid_and_withdrew")
    record["phases"].append("closed")
    supervised.control_command(root, "shutdown", record)
    record["phases"].append("workspace_shutdown")


def execute_fault(nodes, held, refuse, assertions, proof):
    """Replace the exact Base router and retain the independently closed Ethereum side."""
    target = held.config.target(FAILED_TRADE)
    node = nodes.for_chain(target.fund.chain)
    router = target.contracts.router
    original = node.rpc("eth_getCode", [router, "latest"])
    assert original != "0x"
    node._validate_origin()
    replacement = "0x60006000fd"
    node.rpc("anvil_setCode", [router, replacement])
    try:
        failed, refusal = refuse()
        failed_chain, healthy_chain, retained_counter = assert_failed_state(
            held, failed, assertions
        )
        proof.update(
            failed_chain=str(failed_chain.chain_id),
            independently_closed_chain=str(healthy_chain.chain_id),
            router=router,
            original_code_sha256=hashlib.sha256(bytes.fromhex(original[2:])).hexdigest(),
            injected_code=replacement,
            deployment_id=failed.deployment_id,
            retained_counter_inventory=retained_counter,
            **refusal,
            failed_checkpoint=failed.model_dump(mode="json"),
        )
    finally:
        node.rpc("anvil_setCode", [router, original])
        assert node.rpc("eth_getCode", [router, "latest"]) == original


def reversal_hook(assertions, proof):
    def fault(nodes, held, refuse):
        execute_fault(nodes, held, refuse, assertions, proof)
        return proof

    return fault


def assert_reversal_proof(proof):
    assert proof.get("injected_code") == "0x60006000fd"
    assert proof.get("failed_checkpoint", {}).get("mode") == "teardown"
    assert proof.get("refused_workspace_stage") == "running"
    assert proof.get("refused_coordinator_running") is True
    assert proof.get("refused_chain_fault") == {
        "kind": EXPECTED_REVISION_REFUSAL["error"],
        "message": EXPECTED_REVISION_REFUSAL["message"],
        "invalidated": False,
    }
    assert proof.get("failed_chain") == "eip155:8453"
    assert proof.get("independently_closed_chain") == "eip155:1"
    assert proof.get("deployment_id")
    assert proof.get("retained_counter_inventory", 0) > 0
    assert proof.get("restored_router_completed_reference") is True


def test_base_router_failure_preserves_obligations_while_ethereum_closes(
    monkeypatch, request, tmp_path
):
    case = next(
        item
        for item in harness.discover(ROOT / "strategies", ROOT / "tests/strategy_fork/scenarios")
        if item.name == REFERENCE
    )
    assertions = harness.load_assertions(case)
    proof = {}
    monkeypatch.setattr(supervised, "supervised_fault", reversal_hook(assertions, proof))
    monkeypatch.setattr(supervised, "supervise_reversal", supervise_multichain_reversal)
    artifact = ROOT / "artifacts/multichain-v3-composed-reversal" / uuid4().hex
    artifact.mkdir(parents=True)
    try:
        harness.run_case(
            case, request.config.getoption("--fork-env").resolve(), {}, tmp_path, artifact
        )
        assert_reversal_proof(proof)
    finally:
        (artifact / "reversal-proof.json").write_text(json.dumps(proof, indent=2) + "\n")
