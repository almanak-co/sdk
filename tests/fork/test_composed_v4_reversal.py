"""A failed V4 reversal retains live debt until the exact fork router is restored."""

import hashlib
import json
from pathlib import Path
from uuid import uuid4

import pytest

from tests.fork.support import CLI, request_teardown, resume, wait_state
from tests.strategy_fork import harness, supervised

ROOT = Path(__file__).parents[2]
pytestmark = [pytest.mark.fork, pytest.mark.strategy_fork]
COMPOSED_V4_REFERENCES = {
    "base_borrow_and_trade_v4": "eip155:8453",
    "arbitrum_aave_borrow_and_trade_v4": "eip155:42161",
    "optimism_aave_borrow_and_trade_v4": "eip155:10",
}


def execute_fault(node, held, refuse, proof):
    trade = held.config.target("trade")
    router = trade.contracts.router
    original = node.rpc("eth_getCode", [router, "latest"])
    assert original != "0x"
    node._validate_origin()
    replacement = "0x60006000fd"
    node.rpc("anvil_setCode", [router, replacement])
    try:
        failed, refusal = refuse()
        assert_failed_inventory(held, failed)
        proof.update(
            router=router,
            failed_chain=str(trade.fund.chain.chain_id),
            original_code_sha256=hashlib.sha256(bytes.fromhex(original[2:])).hexdigest(),
            injected_code=replacement,
            **refusal,
            failed_checkpoint=failed.model_dump(mode="json"),
        )
    finally:
        node.rpc("anvil_setCode", [router, original])
        assert node.rpc("eth_getCode", [router, "latest"]) == original


def refuse_fork(launch, journal, arguments):
    running = resume(launch, journal, arguments)
    request_teardown(arguments)
    code = running.wait(timeout=180)
    assert code not in (0, -9, 130)
    return journal.load(), {"refused_process_exit_code": code}


def assert_failed_inventory(held, failed):
    assert failed.mode == "teardown"
    assert_unchanged_effects(held, failed)
    assert failed.pending is None and failed.plan is None
    assert failed.decision.intent.kind == "exit_exchange"
    assert_reversal_memory(failed)
    assert_retained_lending(failed)


def assert_unchanged_effects(held, failed):
    assert failed.outcomes == held.outcomes
    assert failed.inventory == held.inventory and failed.owned == held.owned


def assert_reversal_memory(failed):
    assert failed.memory.values["traded"] and failed.memory.values["exiting"]


def assert_retained_lending(failed):
    collateral, loan = failed.owned[:2]
    assert collateral.supply_scaled > 0 and collateral.collateral_enabled
    assert loan.debt_scaled > 0


def reversal_lifecycle(case, node, assertions, journal, arguments, launch, phases, proof):
    running = launch([CLI, "service", "start", "--strategy", str(case.strategy), *arguments])
    held = wait_state(journal, assertions.entered, running)
    assertions.assert_holding(node, held)
    harness.stop_runtime(running)
    phases.append("actual_reference_borrowed_and_traded")
    execute_fault(node, held, lambda: refuse_fork(launch, journal, arguments), proof)
    phases.append("changed_router_refused_without_repayment_or_collateral_release")
    running = resume(launch, journal, arguments)
    closed = wait_state(journal, lambda state: state.mode == "closed", running, timeout=600)
    assert running.wait(timeout=20) == 0
    assertions.assert_closed(node, closed)
    proof["restored_router_completed_reference"] = True
    phases.append("restored_exact_router_reversed_repaid_and_withdrew")


def reversal_hooks(proof):
    def lifecycle(case, node, assertions, journal, arguments, launch, phases):
        return reversal_lifecycle(case, node, assertions, journal, arguments, launch, phases, proof)

    def fault(node, held, refuse):
        execute_fault(node, held, refuse, proof)
        return proof

    return lifecycle, fault


def assert_refusal_evidence(proof, artifact=None, expected_chain=None):
    expected = {
        "error": "authority",
        "message": "Uniswap code differs from the verified implementation",
    }
    gateway_error = proof.get("refused_gateway_error")
    if gateway_error is None and artifact is not None:
        lines = [
            line
            for path in artifact.rglob("process-*.log")
            for line in path.read_text().splitlines()
        ]
        gateway_error = expected if json.dumps(expected) in lines else None
    assert proof.get("injected_code") == "0x60006000fd"
    assert proof.get("failed_checkpoint", {}).get("mode") == "teardown"
    assert proof.get("refused_workspace_stage") == "failed"
    if expected_chain is not None:
        assert proof.get("failed_chain") == expected_chain
    assert gateway_error == expected
    assert proof.get("restored_router_completed_reference") is True


@pytest.mark.parametrize("reference", COMPOSED_V4_REFERENCES)
def test_router_failure_preserves_lending_obligations(monkeypatch, request, tmp_path, reference):
    case = next(
        item
        for item in harness.discover(ROOT / "strategies", ROOT / "tests/strategy_fork/scenarios")
        if item.name == reference
    )
    proof = {}

    lifecycle, fault = reversal_hooks(proof)
    monkeypatch.setattr(harness, "lifecycle", lifecycle)
    monkeypatch.setattr(supervised, "supervised_fault", fault)
    artifact = ROOT / "artifacts/v4-composed-reversal-fault" / uuid4().hex
    artifact.mkdir(parents=True)
    try:
        harness.run_case(
            case, request.config.getoption("--fork-env").resolve(), {}, tmp_path, artifact
        )
        assert_refusal_evidence(proof, artifact, COMPOSED_V4_REFERENCES[reference])
    finally:
        (artifact / "reversal-proof.json").write_text(json.dumps(proof, indent=2) + "\n")
