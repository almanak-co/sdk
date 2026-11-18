"""Actual V4 references must exercise a causal router refusal and restored exit."""

import json
import shutil
import uuid
from pathlib import Path
from types import SimpleNamespace

import pytest
from eth_utils import keccak

from almanak.caip import AssetType, ChainId
from almanak.configuration.unresolved import read_inputs
from almanak.connectors.identifiers import ConnectorId
from almanak.connectors.uniswap_v4 import deployments
from almanak.gateway.ethereum import DataError
from almanak.gateway.permission_models import ContractIdentity
from almanak.models import AssetAmount, Chain
from tests.fork import test_uniswap_v4_reversal as reversal
from tests.strategy_fork import harness, supervised
from tests.strategy_fork.uniswap_assertions import assert_closed
from tests.uniswap_support import live_state

ROOT = Path(__file__).parents[1]


def test_fault_references_cover_actual_trading_and_lp_on_each_chain():
    cases = {
        case.name: case
        for case in harness.discover(ROOT / "strategies", ROOT / "tests/strategy_fork/scenarios")
    }
    observed = []
    for reference in reversal.REFERENCES:
        author = read_inputs(ROOT / "strategies" / reference / "config.yaml").inputs
        assert len(author.markets) == len(author.strategy.allocations) == 1
        market = next(iter(author.markets.values()))
        allocation = next(iter(author.strategy.allocations.values()))
        assert ConnectorId(market.connector) == ConnectorId("uniswap_v4")
        observed.append((market.chain, allocation.range is not None))
        assert harness.load_assertions(cases[reference]).assert_closed is assert_closed
    assert len(observed) == len(set(observed)) == 8
    assert set(observed) == {
        (ChainId(chain), lp)
        for chain in ("eip155:1", "eip155:8453", "eip155:42161", "eip155:10")
        for lp in (False, True)
    }


@pytest.mark.parametrize(
    "chain,name",
    (
        ("eip155:1", "ethereum"),
        ("eip155:8453", "base"),
        ("eip155:42161", "arbitrum"),
        ("eip155:10", "optimism"),
    ),
)
def test_injected_router_has_the_exact_verified_identity_refusal(chain, name):
    roles = deployments.NETWORK_ROLES[chain]
    contracts = {k: v for k, v in roles.items() if k not in ("pool_manager", "state_view")}
    target = SimpleNamespace(
        pool=SimpleNamespace(
            chain=Chain(chain_id=ChainId(chain), environment="mainnet"),
            pool_manager=roles["pool_manager"],
            state_view=roles["state_view"],
            currency0=AssetType(f"{chain}/erc20:0x" + "11" * 20),
            currency1=AssetType(f"{chain}/erc20:0x" + "22" * 20),
        ),
        contracts=SimpleNamespace(model_dump=lambda: contracts),
    )
    pinned = json.loads((deployments.ARTIFACTS / f"{name}-protocol.json").read_text())
    identities = tuple(ContractIdentity.model_validate(i) for i in pinned.values())
    deployments.verify_identities(target, identities)
    changed = tuple(
        i.model_copy(update={"code_hash": "0x" + keccak(bytes.fromhex("60006000fd")).hex()})
        if i.address == roles["router"]
        else i
        for i in identities
    )
    assert changed != identities
    with pytest.raises(DataError) as refused:
        deployments.verify_identities(target, changed)
    assert {"error": refused.value.kind, "message": str(refused.value)} == (
        reversal.EXPECTED_REVISION_REFUSAL
    )


class FailedCheckpoint(SimpleNamespace):
    def model_dump(self, *, mode):
        assert mode == "json"
        return {"mode": self.mode, "refused": True}


class Node:
    def __init__(self):
        self.code = "0x6001"
        self.origin_validated = False

    def _validate_origin(self):
        self.origin_validated = True

    def rpc(self, method, arguments):
        if method == "eth_getCode":
            assert arguments == ["0xrouter", "latest"]
            return self.code
        assert method == "anvil_setCode" and arguments[0] == "0xrouter"
        self.code = arguments[1]


def checkpoints(lp):
    target = SimpleNamespace(
        funding_asset="funding",
        funding_currency="funding",
        range=SimpleNamespace(lower=1, upper=2) if lp else None,
        contracts=SimpleNamespace(router="0xrouter"),
    )
    owned = SimpleNamespace(
        target=target,
        inventory0=SimpleNamespace(asset="funding", amount=4),
        inventory1=SimpleNamespace(asset="counter", amount=0 if lp else 3),
        entry_complete=True,
        position=SimpleNamespace(nft="eip155:1/erc721:0xposition/7", liquidity=9) if lp else None,
    )
    held = SimpleNamespace(
        config=SimpleNamespace(), outcomes=("entry",), inventory=("inventory",), owned=(owned,)
    )
    failed = FailedCheckpoint(
        mode="teardown",
        config=held.config,
        outcomes=held.outcomes,
        inventory=held.inventory,
        owned=held.owned,
        pending=None,
        plan=None,
        decision=SimpleNamespace(intent=SimpleNamespace(kind="exit_exchange")),
    )
    return held, failed


@pytest.mark.parametrize(
    "funding", (None, 0, 1), ids=("default-currency0", "currency0", "currency1")
)
def test_retained_counter_uses_the_effective_typed_funding_currency(funding):
    held, failed = checkpoints(False)
    owned = failed.owned[0]
    target = live_state("trading").owned[0].target
    funding_asset = None if funding is None else target.assets[funding]
    owned.target = target.model_copy(update={"funding_asset": funding_asset})
    amounts = tuple(
        AssetAmount(asset=asset, amount=5 if asset == owned.target.funding_currency else 0)
        for asset in target.assets
    )
    owned.inventory0, owned.inventory1 = amounts
    retained = reversal.counter_inventory(owned)
    assert retained.asset == owned.target.counter_currency
    assert retained.amount == 0
    with pytest.raises(AssertionError):
        reversal.assert_failed_inventory(held, failed)


def invoke_provider_test(reference, monkeypatch, tmp_path, run_supervised_case):
    # tmp_path.name is truncated for parametrized tests and repeats across xdist
    # workers, while the artifact directory is shared, so the token must be unique.
    token = "offline-" + uuid.uuid4().hex
    monkeypatch.setattr(reversal, "uuid4", lambda: SimpleNamespace(hex=token))
    monkeypatch.setattr(supervised, "run_supervised_case", run_supervised_case)
    request = SimpleNamespace(
        config=SimpleNamespace(getoption=lambda option: tmp_path / "unused.env")
    )
    artifact = ROOT / "artifacts/uniswap-v4-reversal-fault" / f"{reference}-{token}"
    try:
        reversal.test_router_failure_retains_exchange_obligations(
            monkeypatch, request, tmp_path, reference
        )
        return json.loads((artifact / "reversal-proof.json").read_text())
    finally:
        shutil.rmtree(artifact, ignore_errors=True)


@pytest.mark.parametrize("reference", reversal.REFERENCES)
def test_actual_provider_entrypoint_installs_and_consumes_the_fault(
    monkeypatch, tmp_path, reference
):
    captured = []

    def run_supervised_case(case, operator, env_file, temporary, artifacts, operator_mode):
        assert case.name == reference and operator.name == f"{reference}.fork.yaml"
        assert operator_mode == "configured"
        held, failed = checkpoints(reference.endswith("_lp"))
        proof = supervised.supervised_fault(
            Node(),
            held,
            lambda: (
                failed,
                {
                    "refused_workspace_stage": "failed",
                    "refused_gateway_error": reversal.EXPECTED_REVISION_REFUSAL,
                },
            ),
        )
        proof["restored_router_completed_reference"] = True
        captured.append(proof.copy())

    retained = invoke_provider_test(reference, monkeypatch, tmp_path, run_supervised_case)
    assert retained == captured[0]
    assert (retained["retained_nft"] is not None) is reference.endswith("_lp")


def test_provider_entrypoint_refuses_an_ordinary_lifecycle_bypass(monkeypatch, tmp_path):
    with pytest.raises(AssertionError):
        invoke_provider_test(reversal.REFERENCES[0], monkeypatch, tmp_path, lambda *args: None)


@pytest.mark.parametrize("lp", (False, True))
@pytest.mark.parametrize("mutation", ("inventory", "outcome", "pending", "obligation"))
def test_fault_rejects_changed_effects_or_missing_obligations(lp, mutation):
    held, failed = checkpoints(lp)
    if mutation == "inventory":
        failed.inventory = ()
    elif mutation == "outcome":
        failed.outcomes = (*held.outcomes, "extra")
    elif mutation == "pending":
        failed.pending = "unproved-pending"
    elif lp:
        failed.owned[0].position = None
    else:
        failed.owned[0].inventory1.amount = 0
    node = Node()
    with pytest.raises(AssertionError):
        reversal.execute_fault(node, held, lambda: (failed, {}), {})
    assert node.origin_validated and node.code == "0x6001"


@pytest.mark.parametrize("mutation", ("missing", "transport", "wrong-revision", "running"))
def test_proof_rejects_noncausal_refusals(mutation):
    held, failed = checkpoints(False)
    refusal = {
        "refused_workspace_stage": "failed",
        "refused_gateway_error": reversal.EXPECTED_REVISION_REFUSAL,
    }
    if mutation == "missing":
        refusal.pop("refused_gateway_error")
    elif mutation == "transport":
        refusal["refused_gateway_error"] = {"error": "gateway_unavailable", "message": "timeout"}
    elif mutation == "wrong-revision":
        refusal["refused_gateway_error"] = {"error": "revision", "message": "another refusal"}
    else:
        refusal["refused_workspace_stage"] = "running"
    proof = {}
    reversal.execute_fault(Node(), held, lambda: (failed, refusal), proof)
    proof["restored_router_completed_reference"] = True
    with pytest.raises(AssertionError):
        reversal.assert_reversal_proof(proof)


def test_refusal_exception_restores_router_code():
    node = Node()
    held, _ = checkpoints(True)

    def refuse():
        raise RuntimeError("synthetic refusal failure")

    with pytest.raises(RuntimeError, match="synthetic refusal failure"):
        reversal.execute_fault(node, held, refuse, {})
    assert node.origin_validated and node.code == "0x6001"
