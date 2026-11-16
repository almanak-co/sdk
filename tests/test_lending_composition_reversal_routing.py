"""The provider reversal entrypoints install faults on public supervised routes."""

import json
import shutil
import uuid
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak.configuration.unresolved import read_inputs
from almanak.connectors.identifiers import ConnectorId
from tests.fork import test_composed_v4_reversal as v4_reversal
from tests.fork import test_lending_composition_reversal as lending_reversal
from tests.qa.composed_catalog import COMPOSED_REFERENCES
from tests.strategy_fork import harness, supervised

ROOT = Path(__file__).parents[1]
GATEWAY_ERROR = {
    "error": "authority",
    "message": "Uniswap code differs from the verified implementation",
}


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
        assert method == "anvil_setCode"
        assert arguments[0] == "0xrouter"
        self.code = arguments[1]


def checkpoint_pair():
    collateral = SimpleNamespace(target=SimpleNamespace(allocation="collateral"), supply_scaled=7)
    loan = SimpleNamespace(target=SimpleNamespace(allocation="loan"), debt_scaled=3)
    held = SimpleNamespace(
        config=SimpleNamespace(
            target=lambda name: SimpleNamespace(pool=SimpleNamespace(kind="curve", pool="0xrouter"))
        ),
        outcomes=("entry",),
        inventory=("inventory",),
        owned=(collateral, loan),
    )
    failed = FailedCheckpoint(
        mode="teardown",
        outcomes=held.outcomes,
        inventory=held.inventory,
        owned=held.owned,
        pending=None,
        plan=None,
        decision=SimpleNamespace(intent=SimpleNamespace(kind="exit_exchange")),
        memory=SimpleNamespace(values={"traded": True}),
    )
    return held, failed


def v4_checkpoint_pair(chain="eip155:8453"):
    collateral = SimpleNamespace(supply_scaled=7, collateral_enabled=True)
    loan = SimpleNamespace(debt_scaled=3)
    held = SimpleNamespace(
        config=SimpleNamespace(
            target=lambda name: SimpleNamespace(
                contracts=SimpleNamespace(router="0xrouter"),
                fund=SimpleNamespace(chain=SimpleNamespace(chain_id=chain)),
            )
        ),
        outcomes=("entry",),
        inventory=("inventory",),
        owned=(collateral, loan),
    )
    failed = FailedCheckpoint(
        mode="teardown",
        outcomes=held.outcomes,
        inventory=held.inventory,
        owned=held.owned,
        pending=None,
        plan=None,
        decision=SimpleNamespace(intent=SimpleNamespace(kind="exit_exchange")),
        memory=SimpleNamespace(values={"traded": True, "exiting": True}),
    )
    return held, failed


def refusal(module, chain="eip155:8453"):
    held, failed = v4_checkpoint_pair(chain) if module is v4_reversal else checkpoint_pair()
    evidence = {
        "refused_workspace_stage": "failed",
        "refused_diagnostic": "Coordinator stopped; inspect its log and explicitly resume.",
    }
    if module is v4_reversal:
        evidence["refused_gateway_error"] = GATEWAY_ERROR
    return held, lambda: (failed, evidence)


def artifact_path(module, reference, token):
    if module is v4_reversal:
        return ROOT / "artifacts/v4-composed-reversal-fault" / token
    return ROOT / "artifacts/lending-composed-reversal-fault" / f"{reference}-{token}"


def invoke_provider_test(module, reference, monkeypatch, tmp_path, run_supervised_case):
    # tmp_path.name is truncated for parametrized tests and repeats across xdist
    # workers, while the artifact directory is shared, so the token must be unique.
    token = "offline-" + uuid.uuid4().hex
    monkeypatch.setattr(module, "uuid4", lambda: SimpleNamespace(hex=token))
    monkeypatch.setattr(supervised, "run_supervised_case", run_supervised_case)
    request = SimpleNamespace(
        config=SimpleNamespace(getoption=lambda option: tmp_path / "unused.env")
    )
    artifact = artifact_path(module, reference, token)
    try:
        if module is v4_reversal:
            module.test_router_failure_preserves_lending_obligations(
                monkeypatch, request, tmp_path, reference
            )
        else:
            module.test_router_failure_preserves_lending_obligations(
                monkeypatch, request, tmp_path, reference
            )
        return json.loads((artifact / "reversal-proof.json").read_text())
    finally:
        shutil.rmtree(artifact, ignore_errors=True)


@pytest.mark.parametrize(
    ("module", "reference"),
    (
        (lending_reversal, "ethereum_spark_curve_borrow_and_trade"),
        (lending_reversal, "robinhood_morpho_borrow_and_trade_v3"),
        *((v4_reversal, reference) for reference in v4_reversal.COMPOSED_V4_REFERENCES),
    ),
)
def test_provider_entrypoint_installs_and_consumes_supervised_fault(
    monkeypatch, tmp_path, module, reference
):
    captured = []

    def run_supervised_case(case, operator, env_file, temporary, artifacts, operator_mode):
        assert case.name == reference
        assert operator.name == f"{reference}.fork.yaml"
        assert operator_mode == "configured"
        fault = supervised.supervised_fault
        assert callable(fault)
        held, refuse = refusal(
            module, v4_reversal.COMPOSED_V4_REFERENCES.get(reference, "eip155:8453")
        )
        proof = fault(Node(), held, refuse)
        proof["restored_router_completed_reference"] = True
        captured.append(proof.copy())

    retained = invoke_provider_test(module, reference, monkeypatch, tmp_path, run_supervised_case)
    assert retained == captured[0]
    assert retained["failed_checkpoint"]["mode"] == "teardown"


@pytest.mark.parametrize(
    ("module", "reference"),
    (
        (lending_reversal, "robinhood_morpho_borrow_and_trade_v3"),
        *((v4_reversal, reference) for reference in v4_reversal.COMPOSED_V4_REFERENCES),
    ),
)
def test_provider_entrypoint_refuses_ordinary_supervised_lifecycle_bypass(
    monkeypatch, tmp_path, module, reference
):
    def ordinary_lifecycle(case, operator, env_file, temporary, artifacts, operator_mode):
        assert case.name == reference

    with pytest.raises(AssertionError):
        invoke_provider_test(module, reference, monkeypatch, tmp_path, ordinary_lifecycle)


def test_supervised_fault_requires_refusal_evidence_and_restored_closure():
    proof = {}
    _, fault = lending_reversal.reversal_hooks(proof)
    node = Node()
    held, failed = checkpoint_pair()

    returned = fault(
        node,
        held,
        lambda: (failed, {"refused_workspace_stage": "running", "refused_diagnostic": "fixed"}),
    )
    assert returned is proof
    assert node.origin_validated and node.code == "0x6001"
    with pytest.raises(AssertionError):
        lending_reversal.assert_reversal_proof(proof)

    proof["restored_router_completed_reference"] = True
    lending_reversal.assert_reversal_proof(proof)
    assert proof["failed_checkpoint"] == {"mode": "teardown", "refused": True}


def test_v4_supervised_fault_requires_the_exact_gateway_refusal_and_restore():
    proof = {}
    _, fault = v4_reversal.reversal_hooks(proof)
    node = Node()
    held, refuse = refusal(v4_reversal)
    returned = fault(node, held, refuse)
    assert returned is proof
    with pytest.raises(AssertionError):
        v4_reversal.assert_refusal_evidence(proof)

    proof["restored_router_completed_reference"] = True
    v4_reversal.assert_refusal_evidence(proof, expected_chain="eip155:8453")


@pytest.mark.parametrize(
    "changes",
    (
        {"refused_workspace_stage": "running"},
        {"refused_gateway_error": {"error": "gateway_unavailable", "message": "timeout"}},
        {"failed_chain": "eip155:10"},
    ),
)
def test_v4_proof_refuses_wrong_failure_cause_or_chain(changes):
    proof = {}
    _, fault = v4_reversal.reversal_hooks(proof)
    held, refuse = refusal(v4_reversal)
    fault(Node(), held, refuse)
    proof["restored_router_completed_reference"] = True
    proof.update(changes)
    with pytest.raises(AssertionError):
        v4_reversal.assert_refusal_evidence(proof, expected_chain="eip155:8453")


@pytest.mark.parametrize(("reference", "chain"), tuple(v4_reversal.COMPOSED_V4_REFERENCES.items()))
def test_v4_reversal_selection_has_exact_typed_composed_scope(reference, chain):
    case = next(
        item
        for item in harness.discover(ROOT / "strategies", ROOT / "tests/strategy_fork/scenarios")
        if item.name == reference
    )
    author = read_inputs(case.config).inputs
    assert {
        (ConnectorId(str(market.connector)), str(market.chain))
        for market in author.markets.values()
    } == {
        (ConnectorId("aave_v3"), chain),
        (ConnectorId("uniswap_v4"), chain),
    }
    assert COMPOSED_REFERENCES[reference] == (
        (ConnectorId("aave_v3"), ("supply", "borrow", "repay", "withdraw")),
        (ConnectorId("uniswap_v4"), ("swap_exact_input", "exit_exchange")),
    )


@pytest.mark.parametrize("module", (lending_reversal, v4_reversal))
def test_refusal_exception_always_restores_the_exact_router_code(module):
    proof = {}
    _, fault = module.reversal_hooks(proof)
    node = Node()
    held, _ = refusal(module)

    def failed_refusal():
        raise RuntimeError("synthetic refusal failure")

    with pytest.raises(RuntimeError, match="synthetic refusal failure"):
        fault(node, held, failed_refusal)
    assert node.origin_validated and node.code == "0x6001"
    assert proof == {}
