"""The Base V3 recovery and reversal selectors bind the actual public reference."""

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
from tests.fork import test_aave_composition_recovery as recovery
from tests.fork import test_base_v3_composition_reversal as reversal
from tests.strategy_fork import harness, supervised

ROOT = Path(__file__).parents[1]
BASE = SimpleNamespace(chain_id=ChainId("eip155:8453"))


class Lending(SimpleNamespace):
    pass


class ExchangeTarget(SimpleNamespace):
    pass


class Exchange(SimpleNamespace):
    pass


class FailedCheckpoint(SimpleNamespace):
    def model_dump(self, *, mode):
        assert mode == "json"
        return {"mode": self.mode, "reference": reversal.REFERENCE}


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


def target(allocation):
    return ExchangeTarget(
        allocation=allocation,
        fund=SimpleNamespace(chain=BASE),
        contracts=SimpleNamespace(router="0xrouter"),
        funding_asset="funding",
    )


def checkpoint_pair():
    targets = {name: target(name) for name in ("collateral", "loan", "trade")}
    collateral = Lending(
        target=targets["collateral"],
        supply_scaled=7,
        debt_scaled=0,
        collateral_enabled=True,
    )
    loan = Lending(target=targets["loan"], supply_scaled=0, debt_scaled=3, collateral_enabled=False)
    trade = Exchange(
        target=targets["trade"],
        inventory0=SimpleNamespace(asset="funding", amount=2),
        inventory1=SimpleNamespace(asset="counter", amount=4),
        entry_complete=True,
    )
    config = SimpleNamespace(target=targets.__getitem__)
    outcomes = tuple(SimpleNamespace(tx_hash=f"0x{index:064x}") for index in range(1, 4))
    held = SimpleNamespace(
        deployment_id="deployment-1",
        config=config,
        outcomes=outcomes,
        inventory=("cash",),
        owned=(collateral, loan, trade),
    )
    failed = FailedCheckpoint(
        mode="teardown",
        deployment_id=held.deployment_id,
        config=config,
        outcomes=outcomes,
        inventory=held.inventory,
        owned=held.owned,
        pending=None,
        plan=None,
        decision=SimpleNamespace(intent=SimpleNamespace(kind="exit_exchange")),
        memory=SimpleNamespace(values={"traded": True}),
    )
    return held, failed


def refusal(failed, **changes):
    evidence = {
        "refused_workspace_stage": "failed",
        "refused_gateway_error": reversal.EXPECTED_REVISION_REFUSAL,
    }
    evidence.update(changes)
    return failed, evidence


def test_actual_reference_binds_base_aave_v3_and_uniswap_v3():
    cases = {
        case.name: case
        for case in harness.discover(ROOT / "strategies", ROOT / "tests/strategy_fork/scenarios")
    }
    case = cases[reversal.REFERENCE]
    author = read_inputs(case.config).inputs
    observed = {
        (name, ConnectorId(market.connector), market.chain)
        for name, market in author.markets.items()
    }
    assert observed == {
        ("collateral", ConnectorId("aave_v3"), ChainId("eip155:8453")),
        ("loan", ConnectorId("aave_v3"), ChainId("eip155:8453")),
        ("pool", ConnectorId("uniswap_v3"), ChainId("eip155:8453")),
    }
    assert recovery.RECOVERY_REFERENCES.count(reversal.REFERENCE) == 1
    assert harness.load_assertions(case).crash_operation == "swap"
    assert (ROOT / "examples/operators/base_borrow_and_trade.fork.yaml").is_file()


@pytest.mark.parametrize("profile", ("eoa", "safe_roles"))
def test_recovery_entrypoint_routes_both_profiles_through_actual_exercise(
    monkeypatch, tmp_path, profile
):
    safe_config = SimpleNamespace(profile="safe")
    eoa_config = SimpleNamespace(profile="eoa")
    captured = {}
    monkeypatch.setattr(recovery, "uuid4", lambda: SimpleNamespace(hex="offline"))
    monkeypatch.setattr(recovery, "case_fork_config", lambda case, env, directory: safe_config)
    monkeypatch.setattr(recovery, "eoa_configuration", lambda config: eoa_config)
    monkeypatch.setattr(recovery, "resolve_config", lambda config, env, cache: config)

    def exercise(case, config, env, temporary, record):
        captured.update(case=case, config=config, record=record, temporary=temporary)

    monkeypatch.setattr(recovery, "exercise", exercise)
    monkeypatch.setattr(recovery, "retain", lambda temporary, artifact, record: None)
    request = SimpleNamespace(
        config=SimpleNamespace(getoption=lambda option: tmp_path / "unused.env")
    )
    artifact = (
        ROOT / "artifacts/aave-composition-recovery" / f"base_borrow_and_trade-{profile}-offline"
    )
    try:
        recovery.test_composed_borrow_recovers_all_privileged_boundaries(
            request, tmp_path, profile, reversal.REFERENCE
        )
    finally:
        shutil.rmtree(artifact, ignore_errors=True)
    assert captured["case"].name == reversal.REFERENCE
    assert captured["config"] is (eoa_config if profile == "eoa" else safe_config)
    assert captured["record"] == {
        "strategy": reversal.REFERENCE,
        "account_profile": profile,
        "status": "passed",
        "phases": [],
    }


def test_short_router_code_has_exact_v3_revision_error():
    with pytest.raises(DataError) as refused:
        deployments.verify_code("router", "0x60006000fd")
    assert {"error": refused.value.kind, "message": str(refused.value)} == (
        reversal.EXPECTED_REVISION_REFUSAL
    )


def invoke_reversal(monkeypatch, tmp_path, run_supervised_case):
    # tmp_path.name is truncated for parametrized tests and repeats across xdist
    # workers, while the artifact directory is shared, so the token must be unique.
    token = "offline-" + uuid.uuid4().hex
    monkeypatch.setattr(reversal, "uuid4", lambda: SimpleNamespace(hex=token))
    monkeypatch.setattr(supervised, "run_supervised_case", run_supervised_case)
    request = SimpleNamespace(
        config=SimpleNamespace(getoption=lambda option: tmp_path / "unused.env")
    )
    artifact = ROOT / "artifacts/base-v3-composed-reversal" / token
    try:
        reversal.test_base_borrow_and_trade_router_failure_preserves_lending_obligations(
            monkeypatch, request, tmp_path
        )
        return json.loads((artifact / "reversal-proof.json").read_text())
    finally:
        shutil.rmtree(artifact, ignore_errors=True)


def test_reversal_entrypoint_installs_and_consumes_exact_fault(monkeypatch, tmp_path):
    monkeypatch.setattr(reversal, "LendingOwnership", Lending)
    monkeypatch.setattr(reversal, "ExchangeOwnership", Exchange)
    monkeypatch.setattr(reversal, "ExchangeTarget", ExchangeTarget)
    captured = []

    def run_supervised_case(case, operator, env_file, temporary, artifacts, operator_mode):
        assert case.name == reversal.REFERENCE
        assert operator.name == f"{reversal.REFERENCE}.fork.yaml"
        assert operator_mode == "configured"
        held, failed = checkpoint_pair()
        proof = supervised.supervised_fault(Node(), held, lambda: refusal(failed))
        proof["restored_router_completed_reference"] = True
        captured.append(proof.copy())

    retained = invoke_reversal(monkeypatch, tmp_path, run_supervised_case)
    assert retained == captured[0]
    assert retained["failed_checkpoint"] == {
        "mode": "teardown",
        "reference": reversal.REFERENCE,
    }


@pytest.mark.parametrize(
    "changes",
    (
        {"refused_gateway_error": None},
        {"refused_gateway_error": {"error": "gateway_unavailable", "message": "timeout"}},
        {"refused_gateway_error": {"error": "revision", "message": "another revision"}},
        {"refused_workspace_stage": "running"},
    ),
)
def test_reversal_refuses_noncausal_or_nonfailed_errors(monkeypatch, tmp_path, changes):
    monkeypatch.setattr(reversal, "LendingOwnership", Lending)
    monkeypatch.setattr(reversal, "ExchangeOwnership", Exchange)
    monkeypatch.setattr(reversal, "ExchangeTarget", ExchangeTarget)

    def run_supervised_case(case, operator, env_file, temporary, artifacts, operator_mode):
        held, failed = checkpoint_pair()
        proof = supervised.supervised_fault(Node(), held, lambda: refusal(failed, **changes))
        proof["restored_router_completed_reference"] = True

    with pytest.raises(AssertionError):
        invoke_reversal(monkeypatch, tmp_path, run_supervised_case)


def test_reversal_refuses_an_ordinary_lifecycle_bypass(monkeypatch, tmp_path):
    def ordinary_lifecycle(case, operator, env_file, temporary, artifacts, operator_mode):
        assert case.name == reversal.REFERENCE

    with pytest.raises(AssertionError):
        invoke_reversal(monkeypatch, tmp_path, ordinary_lifecycle)


def test_reversal_restores_router_when_refusal_raises(monkeypatch):
    monkeypatch.setattr(reversal, "ExchangeTarget", ExchangeTarget)
    proof = {}
    node = Node()
    held, _ = checkpoint_pair()

    def failed_refusal():
        raise RuntimeError("synthetic refusal failure")

    with pytest.raises(RuntimeError, match="synthetic refusal failure"):
        reversal.execute_fault(node, held, failed_refusal, proof)
    assert node.origin_validated and node.code == "0x6001"
    assert proof == {}
