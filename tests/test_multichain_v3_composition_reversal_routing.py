"""The actual multichain V3 reference must consume its adverse-exit hook."""

import json
import shutil
import uuid
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak.caip import ChainId
from almanak.connectors.uniswap_v3 import deployments
from almanak.gateway.ethereum import DataError
from almanak.models import Chain
from almanak.work import ChainFault, work_for
from tests.fork import test_multichain_v3_composition_reversal as reversal
from tests.non_evm_support import Adapter, Environment
from tests.strategy_fork import supervised

ROOT = Path(__file__).parents[1]
ETHEREUM = Chain(chain_id=ChainId("eip155:1"), environment="anvil:ethereum")
BASE = Chain(chain_id=ChainId("eip155:8453"), environment="anvil:base")


class Lending(SimpleNamespace):
    pass


class Exchange(SimpleNamespace):
    pass


class Checkpoint(SimpleNamespace):
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


class Nodes:
    def __init__(self):
        self.node = Node()

    def for_chain(self, chain):
        assert chain == BASE
        return self.node


def target(allocation, chain):
    return SimpleNamespace(
        allocation=allocation,
        fund=SimpleNamespace(chain=chain),
        contracts=SimpleNamespace(router="0xrouter"),
        funding_asset=f"{chain.chain_id}/erc20:funding",
    )


def outcome(name, chain, index):
    return SimpleNamespace(
        name=name,
        tx_hash="0x" + f"{index:064x}",
        operation=SimpleNamespace(target=SimpleNamespace(fund=SimpleNamespace(chain=chain))),
    )


def checkpoints():
    targets = {
        "base_collateral": target("base_collateral", BASE),
        "base_loan": target("base_loan", BASE),
        reversal.FAILED_TRADE: target(reversal.FAILED_TRADE, BASE),
        reversal.HEALTHY_TRADE: target(reversal.HEALTHY_TRADE, ETHEREUM),
    }
    config = SimpleNamespace(target=targets.__getitem__)
    collateral = Lending(
        target=targets["base_collateral"], supply_scaled=9, debt_scaled=0, collateral_enabled=True
    )
    loan = Lending(
        target=targets["base_loan"], supply_scaled=0, debt_scaled=4, collateral_enabled=False
    )
    trade = Exchange(
        target=targets[reversal.FAILED_TRADE],
        inventory0=SimpleNamespace(asset=targets[reversal.FAILED_TRADE].funding_asset, amount=1),
        inventory1=SimpleNamespace(asset="eip155:8453/erc20:counter", amount=3),
        entry_complete=True,
    )
    ethereum_owned = SimpleNamespace(target=target("ethereum_collateral", ETHEREUM))
    base_inventory = SimpleNamespace(key=SimpleNamespace(chain=BASE), amount=3)
    ethereum_inventory = SimpleNamespace(key=SimpleNamespace(chain=ETHEREUM), amount=5)
    entered = (outcome("ethereum-entry", ETHEREUM, 1), outcome("base-entry", BASE, 2))
    held = Checkpoint(
        mode="running",
        deployment_id="deployment-1",
        config=config,
        inventory=(ethereum_inventory, base_inventory),
        owned=(ethereum_owned, collateral, loan, trade),
        outcomes=entered,
        work=(
            SimpleNamespace(
                chain=ETHEREUM, closed=False, pending=None, plan=None, decision=None, fault=None
            ),
            SimpleNamespace(
                chain=BASE, closed=False, pending=None, plan=None, decision=None, fault=None
            ),
        ),
    )
    failed = Checkpoint(
        mode="teardown",
        deployment_id=held.deployment_id,
        config=config,
        inventory=held.inventory,
        owned=held.owned,
        outcomes=(
            *entered,
            outcome("ethereum-exit", ETHEREUM, 3),
            outcome("ethereum-repay", ETHEREUM, 4),
            outcome("ethereum-withdraw", ETHEREUM, 5),
        ),
        work=(
            SimpleNamespace(
                chain=ETHEREUM, closed=True, pending=None, plan=None, decision=None, fault=None
            ),
            SimpleNamespace(
                chain=BASE,
                closed=False,
                pending=None,
                plan=None,
                decision=SimpleNamespace(intent=SimpleNamespace(kind="exit_exchange")),
                fault=SimpleNamespace(
                    kind="revision",
                    message="contract runtime length differs from the compiled revision",
                    invalidated=False,
                ),
            ),
        ),
    )
    return held, failed


class Assertions:
    def __init__(self):
        self.independent_calls = []

    def assert_independent_closed(self, state, failed, healthy, pending):
        assert state.mode == "teardown" and pending is None
        assert next(item for item in state.work if item.chain == failed).closed is False
        assert next(item for item in state.work if item.chain == healthy).closed is True
        self.independent_calls.append((failed, healthy))


def refusal(failed, **changes):
    evidence = {
        "refused_workspace_stage": "running",
        "refused_coordinator_running": True,
        "refused_chain_fault": {
            "kind": "revision",
            "message": "contract runtime length differs from the compiled revision",
            "invalidated": False,
        },
    }
    evidence.update(changes)
    return failed, evidence


def test_short_router_code_has_the_exact_retained_v3_revision_error():
    with pytest.raises(DataError) as refused:
        deployments.verify_code("router", "0x60006000fd")
    assert {"error": refused.value.kind, "message": str(refused.value)} == (
        reversal.EXPECTED_REVISION_REFUSAL
    )


def test_fault_preserves_base_identities_and_closes_ethereum(monkeypatch):
    monkeypatch.setattr(reversal, "LendingOwnership", Lending)
    monkeypatch.setattr(reversal, "ExchangeOwnership", Exchange)
    held, failed = checkpoints()
    assertions = Assertions()
    proof = {}
    nodes = Nodes()
    reversal.execute_fault(nodes, held, lambda: refusal(failed), assertions, proof)
    proof["restored_router_completed_reference"] = True
    reversal.assert_reversal_proof(proof)
    assert assertions.independent_calls == [(BASE, ETHEREUM)]
    assert nodes.node.origin_validated and nodes.node.code == "0x6001"


def test_chain_refusal_wait_requires_alive_coordinator_and_exact_fault(monkeypatch, tmp_path):
    _, failed = checkpoints()
    monkeypatch.setattr(supervised, "loaded_checkpoint", lambda database: failed)
    monkeypatch.setattr(
        reversal,
        "read_workspace",
        lambda root: {"processes": {"coordinator": {"pid": 123}}},
    )
    monkeypatch.setattr(reversal, "running", lambda process: process["pid"] == 123)
    assert reversal.await_chain_refusal(tmp_path, BASE, ETHEREUM) is failed


def test_chain_refusal_wait_rejects_stopped_coordinator(monkeypatch, tmp_path):
    held, _ = checkpoints()
    monkeypatch.setattr(supervised, "loaded_checkpoint", lambda database: held)
    monkeypatch.setattr(
        reversal,
        "read_workspace",
        lambda root: {"processes": {"coordinator": {"pid": 123}}},
    )
    monkeypatch.setattr(reversal, "running", lambda process: False)
    with pytest.raises(AssertionError, match="stopped before retaining"):
        reversal.await_chain_refusal(tmp_path, BASE, ETHEREUM)


@pytest.mark.parametrize("mutation", ("base-inventory", "duplicate-outcome", "healthy-open"))
def test_fault_refuses_incomplete_or_duplicated_independent_closure(monkeypatch, mutation):
    monkeypatch.setattr(reversal, "LendingOwnership", Lending)
    monkeypatch.setattr(reversal, "ExchangeOwnership", Exchange)
    held, failed = checkpoints()
    assertions = Assertions()
    if mutation == "base-inventory":
        changed = SimpleNamespace(key=SimpleNamespace(chain=BASE), amount=2)
        failed.inventory = (failed.inventory[0], changed)
    elif mutation == "duplicate-outcome":
        failed.outcomes = (*failed.outcomes, failed.outcomes[-1])
    else:
        failed.work[0].closed = False
    with pytest.raises(AssertionError):
        reversal.execute_fault(Nodes(), held, lambda: refusal(failed), assertions, {})


@pytest.mark.parametrize(
    "changes",
    (
        {"refused_chain_fault": None},
        {
            "refused_chain_fault": {
                "kind": "gateway_unavailable",
                "message": "timeout",
                "invalidated": False,
            }
        },
        {"refused_workspace_stage": "failed", "refused_coordinator_running": False},
    ),
)
def test_proof_refuses_noncausal_or_nonfailed_rejections(monkeypatch, changes):
    monkeypatch.setattr(reversal, "LendingOwnership", Lending)
    monkeypatch.setattr(reversal, "ExchangeOwnership", Exchange)
    held, failed = checkpoints()
    proof = {}
    reversal.execute_fault(Nodes(), held, lambda: refusal(failed, **changes), Assertions(), proof)
    proof["restored_router_completed_reference"] = True
    with pytest.raises(AssertionError):
        reversal.assert_reversal_proof(proof)


def invoke_provider_test(monkeypatch, tmp_path, run_supervised_case):
    # tmp_path.name is truncated for parametrized tests and repeats across xdist
    # workers, while the artifact directory is shared, so the token must be unique.
    token = "offline-" + uuid.uuid4().hex
    monkeypatch.setattr(reversal, "uuid4", lambda: SimpleNamespace(hex=token))
    monkeypatch.setattr(supervised, "run_supervised_case", run_supervised_case)
    request = SimpleNamespace(
        config=SimpleNamespace(getoption=lambda option: tmp_path / "unused.env")
    )
    artifact = ROOT / "artifacts/multichain-v3-composed-reversal" / token
    try:
        reversal.test_base_router_failure_preserves_obligations_while_ethereum_closes(
            monkeypatch, request, tmp_path
        )
        return json.loads((artifact / "reversal-proof.json").read_text())
    finally:
        shutil.rmtree(artifact, ignore_errors=True)


def test_actual_provider_entrypoint_installs_and_consumes_fault(monkeypatch, tmp_path):
    monkeypatch.setattr(reversal, "LendingOwnership", Lending)
    monkeypatch.setattr(reversal, "ExchangeOwnership", Exchange)
    assertions = Assertions()
    monkeypatch.setattr(reversal.harness, "load_assertions", lambda case: assertions)
    captured = []

    def run_supervised_case(case, operator, env_file, temporary, artifacts, operator_mode):
        assert case.name == reversal.REFERENCE
        assert operator.name == f"{reversal.REFERENCE}.fork.yaml"
        assert operator_mode == "configured"
        held, failed = checkpoints()
        fault = supervised.supervised_fault
        assert callable(fault)
        assert supervised.supervise_reversal is reversal.supervise_multichain_reversal
        proof = fault(Nodes(), held, lambda: refusal(failed))
        proof["restored_router_completed_reference"] = True
        captured.append(proof.copy())

    retained = invoke_provider_test(monkeypatch, tmp_path, run_supervised_case)
    assert retained == captured[0]
    assert retained["failed_checkpoint"] == {
        "mode": "teardown",
        "reference": reversal.REFERENCE,
    }


def test_provider_entrypoint_refuses_an_ordinary_lifecycle_bypass(monkeypatch, tmp_path):
    def ordinary_lifecycle(case, operator, env_file, temporary, artifacts, operator_mode):
        assert case.name == reversal.REFERENCE

    with pytest.raises(AssertionError):
        invoke_provider_test(monkeypatch, tmp_path, ordinary_lifecycle)


@pytest.fixture
def runtime_environment(tmp_path, monkeypatch):
    monkeypatch.setattr("pathlib.Path.home", lambda: tmp_path)
    environment = Environment(tmp_path, monkeypatch)
    try:
        yield environment
    finally:
        environment.stop()


def test_real_runtime_retains_nonfatal_fault_and_continues_other_chain(
    runtime_environment, monkeypatch
):
    environment = runtime_environment
    failed, healthy = environment.ledgers[1], environment.ledgers[0]
    entered = environment.until(
        lambda state: (
            len(state.outcomes) == state.delivered == 2
            and all(item.units == 1 for item in state.owned)
        )
    )
    environment.runtime.journal.teardown()
    original_guard = Adapter.guard
    injected = True

    def revision_guard(adapter):
        if injected and adapter.ledger is failed:
            raise DataError(
                reversal.EXPECTED_REVISION_REFUSAL["error"],
                reversal.EXPECTED_REVISION_REFUSAL["message"],
            )
        original_guard(adapter)

    monkeypatch.setattr(Adapter, "guard", revision_guard)
    refused = environment.until(
        lambda state: reversal.retained_chain_refusal(
            state, failed.target.fund.chain, healthy.target.fund.chain
        )
    )
    assert environment.runtime.closed is False
    assert work_for(refused, healthy.target.fund.chain).closed
    assert work_for(refused, failed.target.fund.chain).fault == ChainFault(
        kind="revision",
        message="contract runtime length differs from the compiled revision",
        invalidated=False,
    )
    assert tuple(item for item in refused.outcomes if item.operation.target == failed.target) == (
        next(item for item in entered.outcomes if item.operation.target == failed.target),
    )

    injected = False
    closed = environment.until(lambda state: state.mode == "closed")
    assert all(item.closed and item.fault is None for item in closed.work)
    assert (failed.units, healthy.units) == (0, 0)
    assert len({item.identity for item in closed.outcomes}) == len(closed.outcomes)
