"""A Base V3 reverse fault retains the actual reference's lending obligations."""

import hashlib
import json
from pathlib import Path
from uuid import uuid4

import pytest

from almanak.caip import ChainId
from almanak.connectors.aave_v3.observations import Ownership as LendingOwnership
from almanak.connectors.uniswap_v3.models import Target as ExchangeTarget
from almanak.connectors.uniswap_v3.observations import Ownership as ExchangeOwnership
from tests.strategy_fork import harness, supervised

ROOT = Path(__file__).parents[2]
REFERENCE = "base_borrow_and_trade"
EXPECTED_REVISION_REFUSAL = {
    "error": "revision",
    "message": "contract runtime length differs from the compiled revision",
}
pytestmark = [pytest.mark.fork, pytest.mark.strategy_fork]


def counter_inventory(owned):
    return next(
        value
        for value in (owned.inventory0, owned.inventory1)
        if value.asset != owned.target.funding_asset
    )


def assert_failed_state(held, failed):
    assert failed.mode == "teardown"
    assert failed.deployment_id == held.deployment_id
    assert failed.config == held.config
    assert failed.outcomes == held.outcomes
    assert failed.inventory == held.inventory and failed.owned == held.owned
    assert failed.pending is None and failed.plan is None
    if failed.decision is not None:
        assert failed.decision.intent.kind == "exit_exchange"
    assert failed.memory.values["traded"]

    positions = {str(position.target.allocation): position for position in failed.owned}
    assert set(positions) == {"collateral", "loan", "trade"}
    collateral, loan, trade = (positions[name] for name in ("collateral", "loan", "trade"))
    assert isinstance(collateral, LendingOwnership)
    assert collateral.supply_scaled > 0 and collateral.collateral_enabled
    assert isinstance(loan, LendingOwnership) and loan.debt_scaled > 0
    assert isinstance(trade, ExchangeOwnership) and trade.entry_complete
    assert isinstance(trade.target, ExchangeTarget)
    assert trade.target.fund.chain.chain_id == ChainId("eip155:8453")
    retained_counter = counter_inventory(trade).amount
    assert retained_counter > 0

    outcomes = tuple(str(outcome.tx_hash) for outcome in failed.outcomes)
    assert len(outcomes) == len(set(outcomes))
    return retained_counter, outcomes


def execute_fault(node, held, refuse, proof):
    target = held.config.target("trade")
    assert isinstance(target, ExchangeTarget)
    assert target.fund.chain.chain_id == ChainId("eip155:8453")
    router = target.contracts.router
    original = node.rpc("eth_getCode", [router, "latest"])
    assert original != "0x"
    node._validate_origin()
    replacement = "0x60006000fd"
    node.rpc("anvil_setCode", [router, replacement])
    try:
        failed, refusal = refuse()
        retained_counter, outcomes = assert_failed_state(held, failed)
        proof.update(
            router=router,
            original_code_sha256=hashlib.sha256(bytes.fromhex(original[2:])).hexdigest(),
            injected_code=replacement,
            deployment_id=failed.deployment_id,
            retained_counter_inventory=retained_counter,
            retained_outcome_ids=list(outcomes),
            **refusal,
            failed_checkpoint=failed.model_dump(mode="json"),
        )
    finally:
        node.rpc("anvil_setCode", [router, original])
        assert node.rpc("eth_getCode", [router, "latest"]) == original


def reversal_hook(proof):
    def fault(node, held, refuse):
        execute_fault(node, held, refuse, proof)
        return proof

    return fault


def assert_reversal_proof(proof):
    assert proof.get("injected_code") == "0x60006000fd"
    assert proof.get("failed_checkpoint", {}).get("mode") == "teardown"
    assert proof.get("refused_workspace_stage") == "failed"
    assert proof.get("refused_gateway_error") == EXPECTED_REVISION_REFUSAL
    assert proof.get("deployment_id")
    assert proof.get("retained_counter_inventory", 0) > 0
    outcomes = proof.get("retained_outcome_ids", ())
    assert outcomes and len(outcomes) == len(set(outcomes))
    assert proof.get("restored_router_completed_reference") is True


def test_base_borrow_and_trade_router_failure_preserves_lending_obligations(
    monkeypatch, request, tmp_path
):
    case = next(
        item
        for item in harness.discover(ROOT / "strategies", ROOT / "tests/strategy_fork/scenarios")
        if item.name == REFERENCE
    )
    proof = {}
    monkeypatch.setattr(supervised, "supervised_fault", reversal_hook(proof))
    artifact = ROOT / "artifacts/base-v3-composed-reversal" / uuid4().hex
    artifact.mkdir(parents=True)
    try:
        harness.run_case(
            case, request.config.getoption("--fork-env").resolve(), {}, tmp_path, artifact
        )
        assert_reversal_proof(proof)
    finally:
        (artifact / "reversal-proof.json").write_text(json.dumps(proof, indent=2) + "\n")
