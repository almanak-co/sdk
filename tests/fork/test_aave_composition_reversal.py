"""A refused fork reversal preserves debt until the exact trading contract is restored."""

import hashlib
import json
from pathlib import Path
from uuid import uuid4

import pytest

from almanak.connectors.aave_v4.models import Ownership as V4Ownership
from almanak.connectors.gmx_v2.exchange_domain import ExchangeTarget as GmxTarget
from almanak.connectors.morpho_blue.observations import Ownership as MorphoOwnership
from tests.fork.support import CLI, request_teardown, resume, wait_state
from tests.strategy_fork import harness, supervised

ROOT = Path(__file__).parents[2]
pytestmark = [pytest.mark.fork, pytest.mark.strategy_fork]


def execute_fault(node, held, refuse, proof):
    """Replace the router with reverting code; refuse() drives teardown to the refusal."""
    target = held.config.target("trade")
    router = reversal_contract(target)
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


def reversal_contract(target):
    if isinstance(target, GmxTarget):
        return target.market.contracts.exchange_router
    return target.pool.pool if target.pool.kind == "curve" else target.contracts.router


def assert_failed_inventory(held, failed):
    assert failed.mode == "teardown"
    assert failed.outcomes == held.outcomes
    assert_inventory_unchanged(held, failed)
    assert failed.pending is None
    assert failed.plan is None
    assert_refused_debt(failed)


def assert_refused_debt(failed):
    asynchronous = isinstance(failed.config.target("trade"), GmxTarget)
    if asynchronous:
        assert_executed_request(failed)
    else:
        assert failed.memory.values["traded"]
    assert_refused_decision(failed, asynchronous)
    assert_lending_obligations(failed)


def assert_executed_request(failed):
    assert failed.memory.values["phase"] == 3
    assert int(failed.memory.values["trade_output"]["amount"]) > 0
    assert len(failed.requests) == 1 and failed.requests[0].status == "executed"


def assert_refused_decision(failed, asynchronous):
    if failed.decision is not None:
        expected = "swap_exact_input" if asynchronous else "exit_exchange"
        assert failed.decision.intent.kind == expected


def assert_inventory_unchanged(held, failed):
    assert failed.inventory == held.inventory
    assert failed.owned == held.owned


def assert_lending_obligations(failed):
    collateral, loan = failed.owned[:2]
    if isinstance(collateral, MorphoOwnership):
        assert isinstance(loan, MorphoOwnership)
        assert collateral.collateral > 0 and loan.borrow_shares > 0
        return
    assert collateral.collateral_enabled
    assert_open_loan(collateral, loan)


def assert_open_loan(collateral, loan):
    if isinstance(collateral, V4Ownership):
        assert isinstance(loan, V4Ownership)
        assert collateral.shares > 0 and loan.drawn_shares > 0
        return
    assert collateral.supply_scaled > 0 and loan.debt_scaled > 0


def reversal_lifecycle(case, node, assertions, journal, arguments, launch, phases, proof):
    running = launch([CLI, "service", "start", "--strategy", str(case.strategy), *arguments])
    harness.drive_external(assertions, node, journal, running)
    held = wait_state(
        journal, assertions.entered, running, timeout=getattr(assertions, "entry_timeout", 180)
    )
    assertions.assert_holding(node, held)
    harness.stop_runtime(running, timeout=getattr(assertions, "process_timeout", 20))
    phases.append("actual_reference_borrowed_and_traded")
    execute_fault(node, held, lambda: refuse_fork(launch, journal, arguments), proof)
    phases.append("changed_router_refused_without_repayment_or_collateral_release")
    running = resume(launch, journal, arguments)
    harness.drive_external(assertions, node, journal, running)
    closed = wait_state(journal, lambda state: state.mode == "closed", running, timeout=600)
    assert running.wait(timeout=getattr(assertions, "process_timeout", 20)) == 0
    assertions.assert_closed(node, closed)
    proof["restored_router_completed_reference"] = True
    phases.append("restored_exact_router_reversed_repaid_and_withdrew")


def assert_reversal_proof(proof):
    """A pass without fault evidence is a bypass, not a proof; refuse to accept it."""
    assert proof.get("injected_code") == "0x60006000fd"
    assert proof.get("failed_checkpoint", {}).get("mode") == "teardown"
    assert proof.get("restored_router_completed_reference") is True


@pytest.mark.parametrize(
    "reference",
    (
        "arbitrum_aave_borrow_and_trade",
        "optimism_aave_borrow_and_trade",
        "bnb_aave_borrow_and_trade",
        "polygon_aave_borrow_and_trade",
        "avalanche_aave_borrow_and_trade",
        "ethereum_aave_v4_borrow_and_trade",
        "avalanche_aave_v4_borrow_and_trade",
        "base_aave_curve_borrow_and_trade",
        "arbitrum_aave_curve_borrow_and_trade",
        "base_aave_pancake_borrow_and_trade",
        "ethereum_aave_pancake_borrow_and_trade",
        "arbitrum_aave_pancake_borrow_and_trade",
        "bnb_aave_pancake_borrow_and_trade",
        "base_aave_aerodrome_stable_borrow_and_trade",
        "base_aave_aerodrome_volatile_borrow_and_trade",
        "base_aave_slipstream_borrow_and_trade",
        "avalanche_aave_borrow_and_trade_v3",
        "bnb_aave_borrow_and_trade_v3",
        "polygon_aave_borrow_and_trade_v3",
        "bnb_aave_borrow_and_trade_v4",
        "polygon_aave_borrow_and_trade_v4",
        "robinhood_morpho_borrow_and_trade_v4",
        "arbitrum_aave_borrow_and_trade_gmx",
        "avalanche_aave_borrow_and_trade_gmx",
    ),
)
def test_router_failure_preserves_lending_obligations(monkeypatch, request, tmp_path, reference):
    case = next(
        item
        for item in harness.discover(ROOT / "strategies", ROOT / "tests/strategy_fork/scenarios")
        if item.name == reference
    )
    proof: dict = {}

    def lifecycle(case, node, assertions, journal, arguments, launch, phases):
        return reversal_lifecycle(case, node, assertions, journal, arguments, launch, phases, proof)

    def fault(node, held, refuse):
        execute_fault(node, held, refuse, proof)
        return proof

    monkeypatch.setattr(harness, "lifecycle", lifecycle)
    monkeypatch.setattr(supervised, "supervised_fault", fault)
    artifact = ROOT / "artifacts/aave-composed-reversal-fault" / (reference + "-" + uuid4().hex)
    artifact.mkdir(parents=True)
    try:
        harness.run_case(
            case, request.config.getoption("--fork-env").resolve(), {}, tmp_path, artifact
        )
        assert_reversal_proof(proof)
    finally:
        (artifact / "reversal-proof.json").write_text(json.dumps(proof, indent=2) + "\n")
