"""The standalone V3 provider entrypoint must install and consume its fault."""

import json
import shutil
import uuid
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak.caip import ChainId
from almanak.configuration.unresolved import read_inputs
from almanak.connectors.identifiers import ConnectorId
from almanak.connectors.uniswap_v3 import deployments
from almanak.gateway.ethereum import DataError
from tests.fork import test_uniswap_v3_reversal as reversal
from tests.strategy_fork import harness, supervised
from tests.strategy_fork.v3_pool_assertions import assert_closed

ROOT = Path(__file__).parents[1]


def test_router_fault_references_cover_trading_and_lp_on_each_selected_chain():
    cases = {
        case.name: case
        for case in harness.discover(ROOT / "strategies", ROOT / "tests/strategy_fork/scenarios")
    }
    observed = []
    for reference in reversal.REFERENCES:
        author = read_inputs(ROOT / "strategies" / reference / "config.yaml").inputs
        assert len(author.markets) == 1
        market = author.markets["pool"]
        assert ConnectorId(market.connector) == ConnectorId("uniswap_v3")
        allocation = author.strategy.allocations["position"]
        observed.append((market.chain, allocation.range is not None))
        scenario = harness.load_assertions(cases[reference])
        assert scenario.crash_operation == "swap"
        assert scenario.assert_closed is assert_closed
    assert len(observed) == len(set(observed)) == 8
    assert set(observed) == {
        (ChainId(chain), lp)
        for chain in ("eip155:1", "eip155:8453", "eip155:42161", "eip155:10")
        for lp in (False, True)
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
        assert method == "anvil_setCode" and arguments[0] == "0xrouter"
        self.code = arguments[1]


def checkpoint_pair(*, lp):
    target = SimpleNamespace(
        funding_asset="funding",
        range=SimpleNamespace(lower=1, upper=2) if lp else None,
        contracts=SimpleNamespace(router="0xrouter"),
    )
    owned = SimpleNamespace(
        target=target,
        inventory0=SimpleNamespace(asset="funding", amount=4),
        inventory1=SimpleNamespace(asset="counter", amount=3),
        entry_complete=True,
        position="eip155:1/erc721:0xposition/7" if lp else None,
    )
    held = SimpleNamespace(
        config=SimpleNamespace(target=lambda name: target),
        outcomes=("entry",),
        inventory=("inventory",),
        owned=(owned,),
    )
    failed = FailedCheckpoint(
        mode="teardown",
        outcomes=held.outcomes,
        inventory=held.inventory,
        owned=held.owned,
        pending=None,
        plan=None,
        decision=SimpleNamespace(intent=SimpleNamespace(kind="exit_exchange")),
    )
    return held, failed


def test_injected_router_code_has_the_retained_gateway_revision_error():
    with pytest.raises(DataError) as refused:
        deployments.verify_code("router", "0x60006000fd")
    assert {
        "error": refused.value.kind,
        "message": str(refused.value),
    } == reversal.EXPECTED_REVISION_REFUSAL


def invoke_provider_test(reference, monkeypatch, tmp_path, run_supervised_case):
    # tmp_path.name is truncated for parametrized tests and repeats across xdist
    # workers, while the artifact directory is shared, so the token must be unique.
    token = "offline-" + uuid.uuid4().hex
    monkeypatch.setattr(reversal, "uuid4", lambda: SimpleNamespace(hex=token))
    monkeypatch.setattr(supervised, "run_supervised_case", run_supervised_case)
    request = SimpleNamespace(
        config=SimpleNamespace(getoption=lambda option: tmp_path / "unused.env")
    )
    artifact = ROOT / "artifacts/uniswap-v3-reversal-fault" / f"{reference}-{token}"
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
        assert case.name == reference
        assert operator.name == f"{reference}.fork.yaml"
        assert operator_mode == "configured"
        fault = supervised.supervised_fault
        assert callable(fault)
        held, failed = checkpoint_pair(lp=reference.endswith("_lp"))
        proof = fault(
            Node(),
            held,
            lambda: (
                failed,
                {
                    "refused_workspace_stage": "failed",
                    "refused_diagnostic": "Coordinator stopped after the injected refusal.",
                    "refused_gateway_error": reversal.EXPECTED_REVISION_REFUSAL,
                },
            ),
        )
        proof["restored_router_completed_reference"] = True
        captured.append(proof.copy())

    retained = invoke_provider_test(reference, monkeypatch, tmp_path, run_supervised_case)
    assert retained == captured[0]
    assert retained["failed_checkpoint"] == {"mode": "teardown", "refused": True}
    assert (retained["retained_nft"] is not None) is reference.endswith("_lp")


def test_provider_entrypoint_refuses_an_ordinary_lifecycle_bypass(monkeypatch, tmp_path):
    def ordinary_lifecycle(case, operator, env_file, temporary, artifacts, operator_mode):
        assert case.name == reversal.REFERENCES[0]

    with pytest.raises(AssertionError):
        invoke_provider_test(reversal.REFERENCES[0], monkeypatch, tmp_path, ordinary_lifecycle)


@pytest.mark.parametrize(
    "refusal",
    (
        pytest.param({"refused_workspace_stage": "failed"}, id="missing-structured-refusal"),
        pytest.param(
            {
                "refused_workspace_stage": "failed",
                "refused_gateway_error": {
                    "error": "revision",
                    "message": "contract code differs from the compiled V3 revision",
                },
            },
            id="wrong-revision-refusal",
        ),
        pytest.param(
            {
                "refused_workspace_stage": "failed",
                "refused_gateway_error": {
                    "error": "gateway_unavailable",
                    "message": "synthetic transport interruption",
                },
            },
            id="transient-transport-error",
        ),
        pytest.param(
            {
                "refused_workspace_stage": "running",
                "refused_gateway_error": reversal.EXPECTED_REVISION_REFUSAL,
            },
            id="nonfailed-workspace",
        ),
    ),
)
def test_provider_entrypoint_requires_the_exact_injected_revision_refusal(
    monkeypatch, tmp_path, refusal
):
    def run_supervised_case(case, operator, env_file, temporary, artifacts, operator_mode):
        held, failed = checkpoint_pair(lp=False)
        proof = supervised.supervised_fault(Node(), held, lambda: (failed, refusal))
        proof["restored_router_completed_reference"] = True

    with pytest.raises(AssertionError):
        invoke_provider_test(reversal.REFERENCES[0], monkeypatch, tmp_path, run_supervised_case)


def test_refusal_exception_restores_the_exact_router_code():
    proof = {}
    node = Node()
    held, _ = checkpoint_pair(lp=True)

    def failed_refusal():
        raise RuntimeError("synthetic refusal failure")

    with pytest.raises(RuntimeError, match="synthetic refusal failure"):
        reversal.reversal_hook(proof)(node, held, failed_refusal)
    assert node.origin_validated and node.code == "0x6001"
    assert proof == {}
