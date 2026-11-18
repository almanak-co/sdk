"""A rejected V4 exit retains standalone trading inventory or the owned LP NFT."""

import hashlib
import json
from pathlib import Path
from uuid import uuid4

import pytest

from tests.strategy_fork import harness, supervised

ROOT = Path(__file__).parents[2]
REFERENCES = (
    "uniswap_v4_trading",
    "uniswap_v4_lp",
    "uniswap_v4_base_trading",
    "uniswap_v4_base_lp",
    "uniswap_v4_arbitrum_trading",
    "uniswap_v4_arbitrum_lp",
    "uniswap_v4_optimism_trading",
    "uniswap_v4_optimism_lp",
)
EXPECTED_REVISION_REFUSAL = {
    "error": "authority",
    "message": "Uniswap code differs from the verified implementation",
}
pytestmark = [pytest.mark.fork, pytest.mark.strategy_fork]


def counter_inventory(owned):
    return next(
        value
        for value in (owned.inventory0, owned.inventory1)
        if value.asset != owned.target.funding_currency
    )


def assert_failed_inventory(held, failed):
    assert failed.mode == "teardown" and failed.config == held.config
    assert failed.outcomes == held.outcomes
    assert failed.inventory == held.inventory and failed.owned == held.owned
    assert failed.pending is None and failed.plan is None
    if failed.decision is not None:
        assert failed.decision.intent.kind == "exit_exchange"
    assert len(failed.owned) == 1
    owned = failed.owned[0]
    assert owned.entry_complete
    if owned.target.range is None:
        assert owned.position is None and counter_inventory(owned).amount > 0
    else:
        # A minted position remains an exit obligation even when no loose
        # counter token remains after depositing into the pool.
        assert owned.position is not None and owned.position.liquidity > 0


def execute_fault(node, held, refuse, proof):
    assert len(held.owned) == 1
    router = held.owned[0].target.contracts.router
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
            retained_nft=str(owned.position.nft) if owned.position is not None else None,
            retained_liquidity=owned.position.liquidity if owned.position is not None else 0,
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
    assert proof.get("retained_counter_inventory", 0) > 0 or (
        proof.get("retained_nft") is not None and proof.get("retained_liquidity", 0) > 0
    )
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
    artifact = ROOT / "artifacts/uniswap-v4-reversal-fault" / f"{reference}-{uuid4().hex}"
    artifact.mkdir(parents=True)
    try:
        harness.run_case(
            case, request.config.getoption("--fork-env").resolve(), {}, tmp_path, artifact
        )
        assert_reversal_proof(proof)
    finally:
        (artifact / "reversal-proof.json").write_text(json.dumps(proof, indent=2) + "\n")
