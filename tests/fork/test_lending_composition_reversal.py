"""Retain actual lending obligations while the configured reverse trade is unavailable."""

import hashlib
import json
from pathlib import Path
from uuid import uuid4

import pytest

from tests.fork.support import CLI, request_teardown, resume, wait_state
from tests.strategy_fork import harness, supervised

ROOT = Path(__file__).parents[2]
pytestmark = [pytest.mark.fork, pytest.mark.strategy_fork]


def execute_fault(node, held, refuse, proof):
    """Replace the router while refuse() drives teardown to a retained refusal."""
    target = held.config.target("trade")
    router = target.pool.pool if target.pool.kind == "curve" else target.contracts.router
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
    assert failed.outcomes == held.outcomes
    assert failed.inventory == held.inventory and failed.owned == held.owned
    assert_reverse_trade_pending(failed)
    assert_lending_obligations(failed)


def assert_reverse_trade_pending(failed):
    assert failed.pending is None and failed.plan is None
    if failed.decision is not None:
        assert failed.decision.intent.kind == "exit_exchange"


def assert_lending_obligations(failed):
    assert failed.memory.values["traded"]
    positions = {str(position.target.allocation): position for position in failed.owned}
    collateral, loan = positions["collateral"], positions["loan"]
    assert collateral_units(collateral) > 0
    assert has_borrowed_position(loan)
    assert_lender_position(positions)


def collateral_units(position):
    return position.supply_scaled if hasattr(position, "supply_scaled") else position.collateral


def has_borrowed_position(loan):
    if hasattr(loan, "borrow_shares"):
        return loan.borrow_shares > 0
    if hasattr(loan, "debt_scaled"):
        return loan.debt_scaled > 0
    return loan.principal < 0


def assert_lender_position(positions):
    if "earn" in positions:
        earn = positions["earn"]
        assert (earn.supply_shares if hasattr(earn, "supply_shares") else earn.principal) > 0


def reversal_lifecycle(case, node, assertions, journal, arguments, launch, phases, proof):
    running = launch([CLI, "service", "start", "--strategy", str(case.strategy), *arguments])
    held = wait_state(
        journal, assertions.entered, running, timeout=getattr(assertions, "entry_timeout", 180)
    )
    assertions.assert_holding(node, held)
    harness.stop_runtime(running, timeout=getattr(assertions, "process_timeout", 20))
    phases.append("actual_reference_borrowed_and_traded")
    execute_fault(node, held, lambda: refuse_fork(launch, journal, arguments), proof)
    phases.append("changed_router_refused_without_repayment_or_collateral_release")
    running = resume(launch, journal, arguments)
    closed = wait_state(
        journal,
        lambda state: state.mode == "closed",
        running,
        timeout=max(600, getattr(assertions, "teardown_timeout", 600)),
    )
    assert running.wait(timeout=getattr(assertions, "process_timeout", 20)) == 0
    assertions.assert_closed(node, closed)
    proof["restored_router_completed_reference"] = True
    phases.append("restored_exact_router_reversed_repaid_and_withdrew")


def reversal_hooks(proof):
    """Return equivalent fault hooks for legacy and public supervised references."""

    def lifecycle(case, node, assertions, journal, arguments, launch, phases):
        return reversal_lifecycle(case, node, assertions, journal, arguments, launch, phases, proof)

    def fault(node, held, refuse):
        execute_fault(node, held, refuse, proof)
        return proof

    return lifecycle, fault


def assert_reversal_proof(proof):
    """A normal lifecycle or empty artifact cannot satisfy the reversal obligation."""
    assert proof.get("injected_code") == "0x60006000fd"
    assert proof.get("failed_checkpoint", {}).get("mode") == "teardown"
    assert proof.get("restored_router_completed_reference") is True


@pytest.mark.parametrize(
    "reference",
    (
        "ethereum_spark_curve_borrow_and_trade",
        "ethereum_compound_curve_borrow_and_trade",
        "base_compound_borrow_and_trade",
        "arbitrum_compound_borrow_and_trade",
        "ethereum_morpho_curve_borrow_and_trade",
        "base_morpho_borrow_and_trade",
        "arbitrum_morpho_borrow_and_trade",
        "robinhood_morpho_borrow_and_trade_v3",
        "avalanche_morpho_borrow_and_trade_v4",
        "optimism_compound_curve_borrow_and_trade",
        "optimism_morpho_curve_borrow_and_trade",
    ),
)
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
    artifact = ROOT / "artifacts/lending-composed-reversal-fault" / (reference + "-" + uuid4().hex)
    artifact.mkdir(parents=True)
    try:
        harness.run_case(
            case, request.config.getoption("--fork-env").resolve(), {}, tmp_path, artifact
        )
        assert_reversal_proof(proof)
    finally:
        (artifact / "reversal-proof.json").write_text(json.dumps(proof, indent=2) + "\n")
