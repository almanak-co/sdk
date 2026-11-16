"""A failed V3 reverse trade retains standalone exchange obligations until restore."""

import hashlib
import json
from pathlib import Path
from uuid import uuid4

import pytest

from tests.strategy_fork import harness, supervised

ROOT = Path(__file__).parents[2]
REFERENCES = (
    "uniswap_v3_ethereum_trading",
    "uniswap_v3_ethereum_lp",
    "uniswap_v3_base_trading",
    "uniswap_v3_base_lp",
    "uniswap_v3_arbitrum_trading",
    "uniswap_v3_arbitrum_lp",
    "uniswap_v3_optimism_trading",
    "uniswap_v3_optimism_lp",
)
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


def assert_failed_inventory(held, failed):
    assert failed.mode == "teardown"
    assert failed.outcomes == held.outcomes
    assert failed.inventory == held.inventory and failed.owned == held.owned
    assert failed.pending is None and failed.plan is None
    if failed.decision is not None:
        assert failed.decision.intent.kind == "exit_exchange"
    assert len(failed.owned) == 1
    owned = failed.owned[0]
    assert owned.entry_complete and counter_inventory(owned).amount > 0
    if owned.target.range is not None:
        assert owned.position is not None


def execute_fault(node, held, refuse, proof):
    """Replace the exact configured router while teardown reaches a retained refusal."""
    router = held.config.target("position").contracts.router
    original = node.rpc("eth_getCode", [router, "latest"])
    assert original != "0x"
    node._validate_origin()
    replacement = "0x60006000fd"
    node.rpc("anvil_setCode", [router, replacement])
    try:
        failed, refusal = refuse()
        assert_failed_inventory(held, failed)
        owned = failed.owned[0]
        proof.update(
            router=router,
            original_code_sha256=hashlib.sha256(bytes.fromhex(original[2:])).hexdigest(),
            injected_code=replacement,
            retained_counter_inventory=counter_inventory(owned).amount,
            retained_nft=str(owned.position) if owned.target.range is not None else None,
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
    """Require the exact failure caused by the injected short V3 router code."""
    assert proof.get("injected_code") == "0x60006000fd"
    assert proof.get("failed_checkpoint", {}).get("mode") == "teardown"
    assert proof.get("refused_workspace_stage") == "failed"
    assert proof.get("refused_gateway_error") == EXPECTED_REVISION_REFUSAL
    assert proof.get("retained_counter_inventory", 0) > 0
    assert proof.get("restored_router_completed_reference") is True


@pytest.mark.parametrize("reference", REFERENCES)
def test_router_failure_retains_exchange_obligations(monkeypatch, request, tmp_path, reference):
    case = next(
        item
        for item in harness.discover(ROOT / "strategies", ROOT / "tests/strategy_fork/scenarios")
        if item.name == reference
    )
    proof = {}
    monkeypatch.setattr(supervised, "supervised_fault", reversal_hook(proof))
    artifact = ROOT / "artifacts/uniswap-v3-reversal-fault" / f"{reference}-{uuid4().hex}"
    artifact.mkdir(parents=True)
    try:
        harness.run_case(
            case, request.config.getoption("--fork-env").resolve(), {}, tmp_path, artifact
        )
        assert_reversal_proof(proof)
    finally:
        (artifact / "reversal-proof.json").write_text(json.dumps(proof, indent=2) + "\n")
